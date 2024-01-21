use std::env;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;

use arrow_schema::Schema;
use async_trait::async_trait;

use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use tracing::debug;

use crate::error::{MeshError, Result};
use crate::model::data_stores::options::flight_sql::{
    FlightSQLAuth, FlightSQLSource, FlightSqlConnection,
};
use crate::model::query::Query;

use super::QueryRunner;

/// [BasicFlightSQLAuth][crate::model::data_stores::options::flight_sql::BasicFlightSQLAuth],
/// but with the password resolved by looking up the referenced env variable.
pub struct ResolvedBasicAuth {
    pub username: String,
    /// This is now the plaintext password itself.
    pub password: String,
}

/// Provides [QueryRunner] impl leveraging an external Trino cluster
/// as the execution engine.
pub struct FlightSQLRunner {
    /// Fully configured endpoint to which the runner should connect and run the [Query]
    /// against.
    pub endpoint: Endpoint,
    /// If the FlightSQL requires basic username and password auth, this is set to Some
    /// and the runner must complete a pre-query handshake to obtain an auth token at
    /// execution time.
    pub basic_auth: Option<ResolvedBasicAuth>,
}

impl TryFrom<(FlightSqlConnection, FlightSQLSource)> for FlightSQLRunner {
    type Error = MeshError;

    fn try_from(value: (FlightSqlConnection, FlightSQLSource)) -> Result<Self> {
        let (con, _source) = value;

        let endpoint = tonic::transport::Channel::from_shared(con.endpoint)?;
        let (endpoint, basic_auth) = match con.auth {
            FlightSQLAuth::Basic(basic) => {
                let password = env::var(&basic.password).map_err(|_e| {
                    MeshError::Internal(format!(
                        "Expected FlightSQL basic auth password to be set in {} \
                    env variable, but it is unset!",
                        basic.password
                    ))
                })?;
                (
                    endpoint,
                    Some(ResolvedBasicAuth {
                        username: basic.username,
                        password,
                    }),
                )
            }
            FlightSQLAuth::PKI(pki) => (
                endpoint.tls_config(
                    ClientTlsConfig::new()
                        .identity(Identity::from_pem(
                            pki.client_cert_file.as_str(),
                            pki.client_key_file.as_str(),
                        ))
                        .ca_certificate(Certificate::from_pem(pki.ca_cert_bundle.as_str())),
                )?,
                None,
            ),
            FlightSQLAuth::Unsecured => (endpoint, None),
        };
        Ok(FlightSQLRunner {
            endpoint,
            basic_auth,
        })
    }
}

#[async_trait]
impl QueryRunner for FlightSQLRunner {
    async fn execute_stream(&mut self, query: Query) -> Result<SendableRecordBatchStream> {
        debug!("Executing {query:?} on FlightSQLRunner");
        debug!("Connecting to FlightSQL endpoint {:?}", self.endpoint);
        let channel = self.endpoint.connect().await?;
        let mut client = FlightSqlServiceClient::new(channel);

        if let Some(auth) = &self.basic_auth {
            debug!("Handshaking with basic auth...");
            client
                .handshake(auth.username.as_str(), auth.password.as_str())
                .await?;
        }

        let mut stmt = client.prepare(query.sql, None).await?;

        let flight_info = stmt.execute().await?;

        let mut flight_data_streams = Vec::with_capacity(flight_info.endpoint.len());
        for endpoint in flight_info.endpoint {
            let ticket = endpoint.ticket.ok_or(DataFusionError::Execution(
                "FlightEndpoint missing ticket!".to_string(),
            ))?;
            let flight_data = client.do_get(ticket).await?;
            flight_data_streams.push(flight_data.map_err(FlightError::Tonic));
        }

        let flight_data_stream = futures::stream::select_all(flight_data_streams);
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);

        match query.return_schema {
            Some(schema) => {
                let schema: Arc<arrow_schema::Schema> = Arc::new(schema);
                let schema_clone1 = schema.clone();
                let schema_clone2 = schema.clone();
                let stream = record_batch_stream
                    .map(move |maybe_batch| match maybe_batch {
                        Ok(rb) => rb
                            .columns()
                            .iter()
                            .zip(schema_clone1.fields())
                            .map(|(arr, field)| {
                                arrow::compute::cast(arr, field.data_type())
                                    .map_err(DataFusionError::from)
                            })
                            .collect::<Result<Vec<_>, DataFusionError>>(),
                        Err(e) => Err(DataFusionError::External(Box::new(e))),
                    })
                    .map(move |maybe_converted| match maybe_converted {
                        Ok(arrays) => RecordBatch::try_new(schema_clone2.clone(), arrays)
                            .map_err(DataFusionError::from),
                        Err(e) => Err(e),
                    });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
            None => {
                let mut peek = record_batch_stream.peekable();
                let schema = if let Some(batch) = Pin::new(&mut peek).peek().await {
                    match batch {
                        Ok(batch) => batch.schema(),
                        Err(e) => return Err(MeshError::RemoteError(e.to_string())),
                    }
                } else {
                    // There is no data from which to infer a schema and no explicit schema was set.
                    Arc::new(Schema::empty())
                };

                let stream = peek.map_err(|e| DataFusionError::External(Box::new(e)));
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }
}
