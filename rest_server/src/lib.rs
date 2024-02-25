use std::any::Any;

use std::env;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;

use actix_web::dev::Extensions;
use actix_web::rt::net::TcpStream;
use actix_web::{web, App, HttpServer};

use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;

use mesh::conf::EnvConfigSettings;

use mesh::crud::PgDb;
use mesh::execute::result_manager::ResultManager;
use mesh::messaging::MessageBrokerOptions;
use mesh::model::data_stores::options::file_directory::FileDirectorySource;
use mesh::model::data_stores::options::SourceFileType;

use actix_tls::accept::rustls_0_21::{reexports::ServerConfig, TlsStream};
use mesh::model::user::{NewUser, UserAttributes};
use mesh::pki::parse_certificate;
use rustls::server::AllowAnyAnonymousOrAuthenticatedClient;
use rustls::{Certificate, PrivateKey, RootCertStore};
use rustls_pemfile::{certs, pkcs8_private_keys};
use tracing::info;

mod admin;
mod error;
mod query;
mod utils;

type DbPool = Pool<AsyncPgConnection>;

#[allow(dead_code)] // it is debug printed
#[derive(Debug, Clone)]
struct ConnectionInfo {
    bind: SocketAddr,
    peer: SocketAddr,
    ttl: Option<u32>,
}

fn get_client_cert(connection: &dyn Any, data: &mut Extensions) {
    if let Some(tls_socket) = connection.downcast_ref::<TlsStream<TcpStream>>() {
        let (socket, tls_session) = tls_socket.get_ref();

        data.insert(ConnectionInfo {
            bind: socket.local_addr().unwrap(),
            peer: socket.peer_addr().unwrap(),
            ttl: socket.ttl().ok(),
        });

        if let Some(certs) = tls_session.peer_certificates() {
            // insert a `rustls::Certificate` into request data
            data.insert(certs.first().unwrap().clone());
        }
    } else if let Some(socket) = connection.downcast_ref::<TcpStream>() {
        data.insert(ConnectionInfo {
            bind: socket.local_addr().unwrap(),
            peer: socket.peer_addr().unwrap(),
            ttl: socket.ttl().ok(),
        });
    } else {
        unreachable!("socket should be TLS or plaintext");
    }
}

/// Creates a [ServerConfig] for an actix-web server running rustls and parses the server_cert_file as a [Certificate]
fn rustls_config(
    cacert_file: &str,
    server_cert_file: &str,
    server_key_file: &str,
) -> std::io::Result<(Certificate, ServerConfig)> {
    let mut cert_store = RootCertStore::empty();

    // import CA cert
    let ca_cert = &mut BufReader::new(
        File::open(cacert_file)
            .unwrap_or_else(|_| panic!("Unable to open {cacert_file} with error:")),
    );
    let all_certs = certs(ca_cert).unwrap();
    for cert in all_certs {
        cert_store
            .add(&Certificate(cert))
            .expect("CA cert could not be added to store!")
    }

    // set up client authentication requirements
    let client_auth = AllowAnyAnonymousOrAuthenticatedClient::new(cert_store);
    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(Arc::new(client_auth));

    // import server cert and key
    let cert_file = &mut BufReader::new(
        File::open(server_cert_file)
            .unwrap_or_else(|_| panic!("Unable to open {server_cert_file} with error:")),
    );
    let key_file = &mut BufReader::new(
        File::open(server_key_file)
            .unwrap_or_else(|_| panic!("Unable to open {server_key_file} with error:")),
    );

    let mut cert_chain: Vec<_> = certs(cert_file)
        .unwrap()
        .into_iter()
        .map(Certificate)
        .collect();
    let mut keys: Vec<PrivateKey> = pkcs8_private_keys(key_file)
        .unwrap()
        .into_iter()
        .map(PrivateKey)
        .collect();
    let config = config
        .with_single_cert(cert_chain.clone(), keys.remove(0))
        .unwrap();
    Ok((cert_chain.remove(0), config))
}

async fn register_default_admin(cert_pem: String, pool: &DbPool) {
    let mut db = PgDb::try_from_pool(pool)
        .await
        .expect("Could not get connection from pool");
    let cert = &mut BufReader::new(
        File::open(&cert_pem).unwrap_or_else(|_| panic!("Unable to open {} with error:", cert_pem)),
    );
    let cert = Certificate(certs(cert).unwrap().remove(0));
    let (x509_sha256, x509_subject, x509_issuer) = parse_certificate(&cert).unwrap();
    let newuser = NewUser {
        x509_sha256,
        x509_subject,
        x509_issuer,
        attributes: UserAttributes::new().with_is_admin(true),
    };

    db.upsert_user_by_fingerprint(&newuser)
        .await
        .expect("Failed to register default_admin");
}

pub async fn run(in_memory_msg_opts: Option<MessageBrokerOptions>) -> std::io::Result<()> {
    let env_config = EnvConfigSettings::init();

    let result_source = FileDirectorySource {
        bucket: env_config.result_bucket.clone(),
        region: env_config.result_region.clone(),
        prefix: env_config.result_prefix.clone(),
        file_type: SourceFileType::Parquet,
    };

    let result_manager = Arc::new(
        ResultManager::try_initialize(
            env_config.result_object_store.clone(),
            result_source,
            env_config.read_client_cert().unwrap(),
            env_config.read_client_key().unwrap(),
            env_config.read_client_cacert_pem().unwrap(),
        )
        .expect("Failed to initialize result manager!"),
    );

    let client_cert = &mut BufReader::new(
        File::open(&env_config.client_cert_file).unwrap_or_else(|_| {
            panic!("Unable to open {} with error:", env_config.client_cert_file)
        }),
    );
    let client_cert = Certificate(certs(client_cert).unwrap().remove(0));

    let (fingerprint, _subject, _issuer) =
        parse_certificate(&client_cert).expect("Failed to parse own cert!");

    let diesel_config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(env_config.db_url);
    let pool = Pool::builder()
        .build(diesel_config)
        .await
        .expect("pool failed to start");

    if let Ok(default_admin) = env::var("DEFAULT_RELAY_ADMIN") {
        info!("Attempting to register default_admin user with identity {default_admin}");
        register_default_admin(default_admin, &pool).await;
    }

    let local_relay_fingerprint = Arc::new(fingerprint);
    let message_options = match in_memory_msg_opts {
        Some(opts) => opts,
        None => env_config.msg_broker_opts,
    };

    let base_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(message_options.clone()))
            .app_data(web::Data::new(result_manager.clone()))
            .app_data(web::Data::new(local_relay_fingerprint.clone()))
            .app_data(web::Data::new(env_config.client_cert_header.clone()))
            .service(query::route::query)
            .service(query::route::get_query_results)
            .service(admin::route::apply)
    });

    if env_config.direct_tls {
        let (_cert, config) = rustls_config(
            env_config.ca_cert_file.as_str(),
            env_config.server_cert_file.as_str(),
            env_config.server_key_file.as_str(),
        )?;
        base_server.on_connect(get_client_cert).bind_rustls_021(
            (
                env_config.rest_url,
                env_config
                    .rest_port
                    .parse()
                    .expect("Unable to parse REST_SERVICE_PORT as a port"),
            ),
            config,
        )?
    } else {
        base_server.bind((
            env_config.rest_url,
            env_config
                .rest_port
                .parse()
                .expect("Unable to parse REST_SERVICE_PORT as a port"),
        ))?
    }
    .run()
    .await
}
