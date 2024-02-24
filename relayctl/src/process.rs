use std::io::Read;

use mesh::error::{MeshError, Result};

use mesh::model::config_commands::entity::{
    EntityDeclaration, ResolvedEntityDeclaration, ResolvedInformationDeclaration,
};
use mesh::model::config_commands::relay::{PeerRelayDeclaration, ResolvedPeerRelayDeclaration};
use mesh::model::config_commands::user::{ResolvedUserDeclaration, UserDeclaration};
use mesh::model::config_commands::{ConfigCommand, ConfigObject, ResolvedConfigObject};
use reqwest::Client;

pub(crate) async fn apply(
    path: std::path::PathBuf,
    mut client: Client,
    relay_endpoint: String,
) -> Result<()> {
    for filepath in walk_directory(path) {
        let command = match try_read_as_config_command(&filepath) {
            Ok(cmd) => cmd,
            Err(e) => {
                println!(
                    "Unable to parse config file at {} with error {e}",
                    filepath.to_string_lossy()
                );
                continue;
            }
        };
        match apply_command(command, &mut client, &relay_endpoint).await {
            Ok(()) => println!("{} applied!", filepath.to_string_lossy()),
            Err(e) => {
                println!(
                    "Unable to apply config file at {} with error {e}",
                    filepath.to_string_lossy()
                );
                continue;
            }
        }
    }
    Ok(())
}

pub async fn apply_command(
    command: ConfigCommand,
    client: &mut Client,
    relay_endpoint: &str,
) -> Result<()> {
    let config_object = match command.config_object {
        ConfigObject::Entity(entity) => ResolvedConfigObject::Entity(resolve_entity_decl(entity)?),
        ConfigObject::PeerRelay(relay) => {
            ResolvedConfigObject::PeerRelay(resolve_relay_decl(relay)?)
        }
        ConfigObject::User(user) => ResolvedConfigObject::User(resolve_user_decl(user)?),
        ConfigObject::LocalData(local_data) => ResolvedConfigObject::LocalData(local_data),
        ConfigObject::LocalMapping(local_mapping) => {
            ResolvedConfigObject::LocalMapping(local_mapping)
        }
        ConfigObject::RemoteMapping(remote_mapping) => {
            ResolvedConfigObject::RemoteMapping(remote_mapping)
        }
    };

    let r = client
        .post(format!("{relay_endpoint}/admin/apply"))
        .json(&config_object)
        .send()
        .await
        .map_err(|e| MeshError::RemoteError(e.to_string()))?;

    match r.text().await {
        Ok(s) => println!("Response from remote: {s}"),
        Err(e) => {
            println!("Failed to parse response as text with e {e}");
        }
    }

    Ok(())
}

fn try_read_as_config_command(path: &std::path::Path) -> Result<ConfigCommand> {
    let f = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(f);
    serde_yaml::from_reader(reader).map_err(|e| MeshError::SerDe(e.to_string()))
}

fn walk_directory(dir: std::path::PathBuf) -> impl Iterator<Item = std::path::PathBuf> {
    let walk = walkdir::WalkDir::new(dir);
    walk.into_iter()
        .filter(|r| match r {
            Err(e) => {
                println!("Error walking directory: {}", e);
                false
            }
            Ok(p) => !p.path().is_dir(),
        })
        .map(|r| {
            let entry = r.unwrap();
            println!("Processing file: {:?}", entry.path());
            entry.path().to_owned()
        })
}

fn resolve_entity_decl(entity: EntityDeclaration) -> Result<ResolvedEntityDeclaration> {
    let mut resolved_info = vec![];

    for info_decl in &entity.information {
        let arrow_dtype = match serde_json::from_str(info_decl.arrow_dtype.as_str()) {
            Ok(dtype) => dtype,
            Err(_) => {
                let modified = format!("\"{}\"", info_decl.arrow_dtype);
                serde_json::from_str(modified.as_str())?
            }
        };
        resolved_info.push(ResolvedInformationDeclaration {
            name: info_decl.name.clone(),
            arrow_dtype,
        })
    }

    Ok(ResolvedEntityDeclaration {
        name: entity.name,
        information: resolved_info,
    })
}

fn resolve_user_decl(user: UserDeclaration) -> Result<ResolvedUserDeclaration> {
    let cert_path = &user.x509_cert;
    let mut buf = Vec::new();
    std::fs::File::open(cert_path)?.read_to_end(&mut buf)?;
    Ok(ResolvedUserDeclaration {
        x509_cert: buf,
        attributes: user.attributes,
        permissions: user.permissions,
    })
}

fn resolve_relay_decl(relay: PeerRelayDeclaration) -> Result<ResolvedPeerRelayDeclaration> {
    let cert_path = &relay.x509_cert;
    let mut buf = Vec::new();
    std::fs::File::open(cert_path)?.read_to_end(&mut buf)?;
    Ok(ResolvedPeerRelayDeclaration {
        name: relay.name,
        x509_cert: buf,
        permissions: relay.permissions,
        rest_endpoint: relay.rest_endpoint,
        flight_endpoint: relay.flight_endpoint,
    })
}
