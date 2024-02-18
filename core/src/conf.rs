use crate::{
    error::Result, messaging::MessageBrokerOptions,
    model::data_stores::options::SupportedObjectStore,
};
use std::{env, io::Read};

/// Initializes and Holds envrionment variable settings which
/// control system behavior. Panics if any required setting
/// is not set.
#[derive(Debug)]
pub struct EnvConfigSettings {
    pub relay_name: String,
    pub rest_url: String,
    pub rest_port: String,
    pub flight_addr: String,
    pub ca_cert_file: String,
    pub direct_tls: bool,
    pub server_cert_file: String,
    pub server_key_file: String,
    pub client_cert_header: Option<String>,
    pub client_cert_file: String,
    pub client_key_file: String,
    pub msg_broker_opts: MessageBrokerOptions,
    pub db_url: String,
    pub result_object_store: SupportedObjectStore,
    pub result_bucket: Option<String>,
    pub result_region: Option<String>,
    pub result_prefix: Option<String>,
}

impl EnvConfigSettings {
    pub fn init() -> Self {
        let relay_name = env::var("RELAY_NAME").expect("RELAY_NAME must be set");
        let rest_url = env::var("REST_SERVICE_URL").expect("REST_SERVICE_URL must be set");
        let rest_port = env::var("REST_SERVICE_PORT").expect("REST_SERVICE_PORT must be set");
        let result_prefix = env::var("RESULT_SOURCE_PFX").ok();
        let result_bucket = env::var("RESULT_SOURCE_BUCKET").ok();
        let result_region = env::var("RESULT_SOURCE_REGION").ok();
        let result_object_store = env::var("RESULT_SOURCE_OBJECT_STORE")
            .expect("RESULT_SOURCE_OBJECT_STORE must be set")
            .try_into()
            .expect("RESULT_SOURCE_OBJECT_STORE is invalid!");
        let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let flight_addr =
            env::var("FLIGHT_SERVICE_ENDPOINT").expect("FLIGHT_SERVICE_ENDPOINT must be set");
        let ca_cert_file = env::var("CA_CERT_FILE").expect("CA_CERT_FILE must be set");
        let client_cert_file = env::var("CLIENT_CERT_FILE").expect("CLIENT_CERT_FILE must be set");
        let client_key_file = env::var("CLIENT_KEY_FILE").expect("CLIENT_KEY_FILE must be set");

        let direct_tls = env::var("DIRECT_TLS")
            .unwrap_or("false".to_string())
            .parse::<bool>()
            .expect("Unable to parse DIRECT_TLS configuration as boolean!");

        let (server_key_file, server_cert_file, client_cert_header) = if direct_tls {
            (
                env::var("SERVER_KEY_FILE")
                    .expect("SERVER_KEY_FILE must be set when DIRECT_TLS is true"),
                env::var("SERVER_CERT_FILE")
                    .expect("SERVER_CERT_FILE must be set when DIRECT_TLS is true"),
                None,
            )
        } else {
            (
                "".to_string(),
                "".to_string(),
                Some(
                    env::var("CLIENT_CERT_HEADER")
                        .expect("CLIENT_CERT_HEADER must be set when DIRECT_TLS is false"),
                ),
            )
        };

        let msg_broker_opts = serde_json::from_str(
            env::var("MSG_BROKER_OPTS")
                .expect("MSG_BROKER_OPTS must be set")
                .as_str(),
        )
        .expect("MSG_BROKER_OPTS could not be parsed as json");

        Self {
            relay_name,
            rest_url,
            rest_port,
            flight_addr,
            ca_cert_file,
            direct_tls,
            server_cert_file,
            server_key_file,
            client_cert_header,
            client_cert_file,
            client_key_file,
            msg_broker_opts,
            db_url,
            result_object_store,
            result_bucket,
            result_region,
            result_prefix,
        }
    }

    pub fn read_client_cert(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.client_cert_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn read_client_key(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.client_key_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn read_server_cert(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.server_cert_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn read_server_key(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.server_key_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Reads certificate and key pem files into a buffer which can be used to construct a rustls Identity
    pub fn read_client_identity_pem(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.client_cert_file)?.read_to_end(&mut buf)?;
        std::fs::File::open(&self.client_key_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }

    /// Reads certificate and key pem files into a buffer which can be used to construct a rustls Identity
    pub fn read_client_cacert_pem(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(&self.ca_cert_file)?.read_to_end(&mut buf)?;
        Ok(buf)
    }
}
