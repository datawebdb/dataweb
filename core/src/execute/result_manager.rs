use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{flight_descriptor, FlightClient, FlightData, FlightDescriptor, SchemaAsIpc};
use datafusion::arrow::datatypes::Schema;

use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::AsyncArrowWriter;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::path::Path;
use object_store::ObjectStore;

use arrow_flight::flight_service_client::FlightServiceClient;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use url::Url;
use uuid::Uuid;

use crate::error::{MeshError, Result};
use crate::model::data_stores::options::file_directory::FileDirectorySource;
use crate::model::data_stores::options::SupportedObjectStore;
use crate::model::relay::Relay;

use futures::{Stream, StreamExt, TryStreamExt};

use super::data_stores::initialize_object_store;

/// Manages storing and retrieving query results in an [ObjectStore] as a [Stream]
/// and sending [RecordBatch] streams to remote flight services
pub struct ResultManager {
    object_store: Arc<dyn ObjectStore>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
    cacert_pem: Vec<u8>,
}

impl ResultManager {
    pub fn try_initialize(
        store_type: SupportedObjectStore,
        source: FileDirectorySource,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
        cacert_pem: Vec<u8>,
    ) -> Result<Self> {
        let object_store = initialize_object_store(store_type, &source)?;
        Ok(Self {
            object_store,
            client_cert_pem,
            client_key_pem,
            cacert_pem,
        })
    }

    pub async fn write_task_result<S>(
        &self,
        task_id: &Uuid,
        mut rb_stream: Pin<Box<S>>,
        schema: Arc<Schema>,
    ) -> Result<()>
    where
        S: Stream<Item = std::result::Result<RecordBatch, DataFusionError>>
            + Send
            + 'static
            + ?Sized,
    {
        let path = Path::parse(format!("task_{}/result.parquet", task_id))?;
        let (_, multipart) = self.object_store.put_multipart(&path).await?;
        let mut writer =
            AsyncArrowWriter::try_new(multipart, schema, 10485760, None).map_err(|e| {
                MeshError::Internal(format!(
                    "Parquet serialization error in task serialization! {e}"
                ))
            })?;
        while let Some(batch) = rb_stream.next().await.transpose()? {
            writer.write(&batch).await.map_err(|e| {
                MeshError::Internal(format!(
                    "Parquet serialization error in task serialization! {e}"
                ))
            })?;
        }

        writer.close().await.map_err(|e| {
            MeshError::Internal(format!(
                "Parquet serialization error in task serialization! {e}"
            ))
        })?;

        Ok(())
    }

    pub async fn get_task_result(&self, task_id: Uuid) -> Result<SendableRecordBatchStream> {
        let url = &Url::parse("results://results")?;
        let path_str = format!("{url}/task_{}/result.parquet", task_id);
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(url, self.object_store.clone());
        let df = ctx
            .read_parquet(path_str, ParquetReadOptions::default())
            .await?;
        Ok(df.execute_stream().await?)
    }

    /// Sends a stream of RecordBatches using Flight to the originating remote [Relay]
    pub async fn send_result_flight<S>(
        &self,
        local_task_id: &Uuid,
        origin_task_id: &Uuid,
        rb_stream: Pin<Box<S>>,
        schema: Arc<Schema>,
        relay: Relay,
    ) -> Result<()>
    where
        S: Stream<Item = std::result::Result<RecordBatch, DataFusionError>>
            + Send
            + 'static
            + ?Sized,
    {
        let channel = tonic::transport::Channel::from_shared(relay.flight_endpoint)?
            .tls_config(
                ClientTlsConfig::new()
                    .identity(Identity::from_pem(
                        &self.client_cert_pem,
                        &self.client_key_pem,
                    ))
                    .ca_certificate(Certificate::from_pem(&self.cacert_pem)),
            )?
            .connect()
            .await?;
        let svc_client = FlightServiceClient::new(channel);
        let mut client = FlightClient::new_from_inner(svc_client);

        // add an initial FlightData message that sends schema
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(&schema, &options);

        let mut first_flight = FlightData::from(schema_flight_data);
        first_flight.flight_descriptor = Some(FlightDescriptor {
            r#type: flight_descriptor::DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec![local_task_id.to_string(), origin_task_id.to_string()],
        });

        // Chain the data stream behind the initial metadata message
        let flight_data_stream = futures::stream::once(async { Ok(first_flight) }).chain(
            FlightDataEncoderBuilder::new()
                .build(rb_stream.map_err(|e| FlightError::ExternalError(Box::new(e)))),
        );

        let mut resp = client.do_put(flight_data_stream).await.map_err(|e| {
            MeshError::RemoteError(format!("error in do_put to relay {}: {}", relay.id, e))
        })?;

        while let Some(_put_resp) = resp.next().await.transpose().map_err(|e| {
            MeshError::RemoteError(format!("error in do_put to relay {}: {}", relay.id, e))
        })? {
            // nothing to do with response for now
        }
        Ok(())
    }
}
