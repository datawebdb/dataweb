use std::sync::Arc;

use arrow_flight::{flight_service_client::FlightServiceClient, FlightClient};
use datafusion::error::{DataFusionError, Result};
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tracing::debug;

pub async fn get_flight_client(
    client_cert: Arc<Vec<u8>>,
    client_key: Arc<Vec<u8>>,
    ca_cert: Arc<Vec<u8>>,
    endpoint: &str,
) -> Result<FlightClient> {
    debug!("Connecting to flight endpoint {endpoint}");
    let channel = tonic::transport::Channel::from_shared(endpoint.to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .tls_config(
            ClientTlsConfig::new()
                .identity(Identity::from_pem(
                    client_cert.as_ref(),
                    client_key.as_ref(),
                ))
                .ca_certificate(Certificate::from_pem(ca_cert.as_ref())),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .connect()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let svc_client = FlightServiceClient::new(channel);
    Ok(FlightClient::new_from_inner(svc_client))
}
