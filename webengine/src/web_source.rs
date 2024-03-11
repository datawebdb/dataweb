use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::{
    common::Result,
    datasource::TableProvider,
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        project_schema, stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType,
        ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::expr_to_sql::filter_expr_to_sql;
use crate::{
    expr_to_sql::{map_filter_exprs, map_projection},
    utils::get_flight_client,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EntityScanRequest {
    /// A Sql template string, e.g. "select {info} from {entity}"
    pub sql: String,
    pub return_arrow_schema: Option<Arc<Schema>>,
}

impl TryFrom<EntityScanRequest> for bytes::Bytes {
    type Error = DataFusionError;

    fn try_from(value: EntityScanRequest) -> std::prelude::v1::Result<Self, Self::Error> {
        let js = serde_json::to_vec(&value).map_err(|e| {
            DataFusionError::Execution(format!(
                "Unexpected error serializing EntityScanRequest! {e}"
            ))
        })?;
        Ok(bytes::Bytes::from_iter(js))
    }
}

/// Implements a DataFusion [TableProvider] for a given Entity in the DataWeb.
pub struct DataWebEntity {
    pub entity_name: String,
    pub schema: SchemaRef,
    /// The Relay to which requests are originated
    pub local_relay_endpoint: Arc<String>,
    /// x509 certificate used for authenticating as a client with Relays
    pub client_cert: Arc<Vec<u8>>,
    /// private key of the x509 client certificate
    pub client_key: Arc<Vec<u8>>,
    /// CAcert bundle used to verify other flight servers when making a request as a client
    pub ca_cert: Arc<Vec<u8>>,
}

impl DataWebEntity {
    /// Executes a get_flight_info request to the local Relay and returns
    /// the list of flight endpoints for the [EntityScanRequest]
    async fn get_flight_info(&self, entity_scan_request: EntityScanRequest) -> Result<FlightInfo> {
        let mut client = get_flight_client(
            self.client_cert.clone(),
            self.client_key.clone(),
            self.ca_cert.clone(),
            self.local_relay_endpoint.as_ref(),
        )
        .await?;

        let req: bytes::Bytes = entity_scan_request.try_into()?;
        let resp = client
            .get_flight_info(FlightDescriptor::new_cmd(req))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(resp)
    }
}

#[async_trait]
impl TableProvider for DataWebEntity {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;

        let proj_str = map_projection(&self.entity_name, projected_schema.clone());

        let filter_str = map_filter_exprs(&self.entity_name, filters);

        let template = if let Some(l) = limit {
            format!(
                "select {proj_str} from {} {filter_str} limit {l}",
                self.entity_name
            )
        } else {
            format!("select {proj_str} from {} {filter_str}", self.entity_name)
        };

        let entity_scan_req = EntityScanRequest {
            sql: template,
            return_arrow_schema: Some(projected_schema.clone()),
        };

        debug!("Created request: {:?}", entity_scan_req);

        // For large (particularly deep) webs this request could be slow, as our request
        // must propagate to every relay in the web, and back again. If the average latency
        // of a get_flight_info request is S, the depth of the web is N, we would expect
        // the total latency to be at least N*S.
        let info = self.get_flight_info(entity_scan_req).await?;

        debug!("Got info from {} sources!", info.endpoint.len());

        Ok(Arc::new(WebEntityScan {
            scan_endpoints: info.endpoint,
            projected_schema,
            local_relay_endpoint: self.local_relay_endpoint.clone(),
            client_cert: self.client_cert.clone(),
            client_key: self.client_key.clone(),
            ca_cert: self.ca_cert.clone(),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| match filter_expr_to_sql(&self.entity_name, f) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(e) => {
                    error!("Got unsupported filter expr {e}");
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct WebEntityScan {
    /// Each FlightEndpoint represents a DataSource on some Relay in the Web which must be scanned
    pub scan_endpoints: Vec<FlightEndpoint>,
    pub projected_schema: SchemaRef,
    /// The Relay to which requests are originated
    pub local_relay_endpoint: Arc<String>,
    /// x509 certificate used for authenticating as a client with Relays
    pub client_cert: Arc<Vec<u8>>,
    /// private key of the x509 client certificate
    pub client_key: Arc<Vec<u8>>,
    /// CAcert bundle used to verify other flight servers when making a request as a client
    pub ca_cert: Arc<Vec<u8>>,
}

impl DisplayAs for WebEntityScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "WebEntityScan")
    }
}

impl ExecutionPlan for WebEntityScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(self.scan_endpoints.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let endpoint = self
            .scan_endpoints
            .get(partition)
            .ok_or(DataFusionError::Execution(format!(
                "WebEntityScan has only {} partitions, but called on partition {}!",
                self.scan_endpoints.len(),
                partition
            )))?;

        let relay_endpoint = endpoint
            .location
            .first()
            .ok_or(DataFusionError::Execution(
                "WebEntityScan got a FlightEndpoint with no location!".to_string(),
            ))?
            .uri
            .clone();

        let ticket = endpoint
            .ticket
            .as_ref()
            .ok_or(DataFusionError::Execution(
                "WebEntityScan got a FlightEndpoint with no ticket!".to_string(),
            ))?
            .clone();

        let client_cert = self.client_cert.clone();
        let client_key = self.client_key.clone();
        let cacert = self.ca_cert.clone();

        let init_stream_bundle = InitStreamBundle {
            client_cert,
            client_key,
            cacert,
            ticket,
            relay_endpoint,
        };
        let stream = futures::stream::try_unfold(
            (None, Some(init_stream_bundle)),
            |(maybe_stream, maybe_init_stream)| async move {
                let mut stream = match (maybe_stream, maybe_init_stream) {
                    (Some(stream), None) => stream,
                    (None, Some(init_stream_bundle)) => {
                        let mut client = get_flight_client(
                            init_stream_bundle.client_cert,
                            init_stream_bundle.client_key,
                            init_stream_bundle.cacert,
                            &init_stream_bundle.relay_endpoint,
                        )
                        .await?;

                        client
                            .do_get(init_stream_bundle.ticket)
                            .await
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                    }
                    _ => unreachable!(),
                };
                let maybe_batch = stream
                    .next()
                    .await
                    .transpose()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                match maybe_batch {
                    Some(batch) => Ok::<_, DataFusionError>(Some((batch, (Some(stream), None)))),
                    None => Ok(None),
                }
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
            is_exact: false,
        }
    }
}

struct InitStreamBundle {
    client_cert: Arc<Vec<u8>>,
    client_key: Arc<Vec<u8>>,
    cacert: Arc<Vec<u8>>,
    ticket: Ticket,
    relay_endpoint: String,
}
