use std::{collections::HashMap, env, fs::File, io::Read};

use clap::{Parser, Subcommand};

use mesh::{error::Result, model::{config_commands::{local_data::{DataConnectionsDeclaration, DataFieldsDeclaration, DataSourcesDeclaration, ResolvedDataConnectionsDeclaration}, remote_mapping::{EntityMapDecl, PeerRelayMappingsDeclaration, RemoteInfoMappingsDeclaration, RemoteMappingsDeclaration}, ConfigCommand, ConfigObject, DefaultPermissionDeclaration, ResolvedConfigCommand, ResolvedConfigObject}, data_stores::{options::{trino::{TrinoConnection, TrinoSource}, ConnectionOptions, SourceOptions}, DataSource}, mappings::Transformation, query::{InfoSubstitution, SubstitutionBlocks}}};
use process::apply;

mod process;

/// relayctl cli app
#[derive(Debug, Parser)]
#[clap(name = "relayctl", version)]
pub struct Relayctl {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Parse a YAML file and send to Relay to apply desired state
    Apply {
        /// Path to the config command. Can be a directory of YAML files or a single YAML file.
        #[clap(long, short = 'f')]
        filepath: std::path::PathBuf,
    },
}

/// Reads certificate and key pem files into the same buffer and constructs a
/// [Identity][reqwest::Identity]
fn read_identity() -> Result<reqwest::Identity> {
    let mut iden_pem = Vec::new();

    let client_cert_file = env::var("CLIENT_CERT_FILE").expect("CLIENT_CERT_FILE must be set");
    let client_key_file = env::var("CLIENT_KEY_FILE").expect("CLIENT_KEY_FILE must be set");

    std::fs::File::open(&client_cert_file)
        .expect(&format!("Could not open {client_cert_file}"))
        .read_to_end(&mut iden_pem)
        .expect(&format!("Could not read {client_cert_file}"));

    std::fs::File::open(&client_key_file)
        .expect(&format!("Could not open {client_key_file}"))
        .read_to_end(&mut iden_pem)
        .expect(&format!("Could not read {client_key_file}"));

    Ok(reqwest::Identity::from_pem(&iden_pem).expect("could not parse client cert and key"))
}

fn get_reqw_client() -> Result<reqwest::Client> {
    let ca_cert_file = env::var("CA_CERT_FILE").expect("CA_CERT_FILE must be set");
    let mut cacert = Vec::new();

    std::fs::File::open(&ca_cert_file)
        .expect(&format!("Could not open {ca_cert_file}"))
        .read_to_end(&mut cacert)
        .expect(&format!("Could not read {ca_cert_file}"));

    let identity = read_identity()?;
    let mut client = reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity);

    let cert = reqwest::Certificate::from_pem(&cacert).expect("Could not parse cacert");
    client = client.add_root_certificate(cert);
    Ok(client.build().expect("client build err"))
}

#[tokio::main]
async fn main() -> Result<()> {

    let args = Relayctl::parse();

    match args.command {
        Command::Apply { filepath } => {
            let client = get_reqw_client()?;
            let relay_endpoint = env::var("RELAY_ENDPOINT").expect("RELAY_ENDPOINT must be set");
            apply(filepath, client, relay_endpoint).await?
        }
    }

    Ok(())
}
