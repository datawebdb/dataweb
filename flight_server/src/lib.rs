use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;

use flight::FlightRelay;

use mesh::{conf::EnvConfigSettings, crud::run_migrations};

use mesh::error::MeshError;
use mesh::execute::result_manager::ResultManager;
use mesh::model::data_stores::options::file_directory::FileDirectorySource;
use mesh::model::data_stores::options::SourceFileType;
use mesh::pki::parse_certificate;

use rustls_pemfile::certs;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

use arrow_flight::flight_service_server::FlightServiceServer;

mod flight;

pub async fn run() -> Result<(), MeshError> {
    let env_conf = EnvConfigSettings::init();

    run_migrations(&env_conf.db_url);
    let config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&env_conf.db_url);
    let db_pool = Pool::builder()
        .build(config)
        .await
        .expect("pool failed to start");

    let result_source = FileDirectorySource {
        bucket: env_conf.result_bucket.clone(),
        region: env_conf.result_region.clone(),
        prefix: env_conf.result_prefix.clone(),
        file_type: SourceFileType::Parquet,
    };

    let result_manager = Arc::new(
        ResultManager::try_initialize(
            env_conf.result_object_store.clone(),
            result_source,
            env_conf.read_client_cert().unwrap(),
            env_conf.read_client_key().unwrap(),
            env_conf.read_client_cacert_pem().unwrap(),
        )
        .expect("Failed to initialize result manager!"),
    );

    let client_cert =
        &mut BufReader::new(File::open(&env_conf.client_cert_file).unwrap_or_else(|_| {
            panic!("Unable to open {} with error:", env_conf.client_cert_file)
        }));
    let client_cert = rustls::Certificate(certs(client_cert).unwrap().remove(0));

    let (fingerprint, _subject, _issuer) =
        parse_certificate(&client_cert).expect("Failed to parse own cert!");

    let ca_cert = Arc::new(env_conf.read_client_cacert_pem()?);

    let client_cert = Arc::new(env_conf.read_client_cert()?);
    let client_key = Arc::new(env_conf.read_client_key()?);

    let addr = env_conf
        .flight_addr
        .parse()
        .expect("Error parsing flight_addr as socket_address");

    let flight_service = FlightRelay {
        db_pool,
        result_manager,
        client_cert,
        client_key,
        ca_cert: ca_cert.clone(),
        local_fingerprint: Arc::new(fingerprint),
        client_cert_header: env_conf.client_cert_header.clone(),
    };
    let flight_svc = FlightServiceServer::new(flight_service);

    if env_conf.direct_tls {
        let tls_config = ServerTlsConfig::new()
            .client_ca_root(Certificate::from_pem(ca_cert.as_ref()))
            .client_auth_optional(false)
            .identity(Identity::from_pem(
                env_conf.read_server_cert().unwrap(),
                env_conf.read_server_key().unwrap(),
            ));
        Server::builder().tls_config(tls_config)?
    } else {
        Server::builder()
    }
    .add_service(flight_svc)
    .serve(addr)
    .await
    .expect("Failed to create flight service!");

    Ok(())
}
