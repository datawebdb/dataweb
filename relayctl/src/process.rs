use std::io::Read;
use std::iter;

use itertools::Itertools;
use mesh::error::{MeshError, Result};

use mesh::model::config_commands::entity::{
    EntityDeclaration, ResolvedEntityDeclaration, ResolvedInformationDeclaration,
};
use mesh::model::config_commands::relay::{PeerRelayDeclaration, ResolvedPeerRelayDeclaration};
use mesh::model::config_commands::user::{ResolvedUserDeclaration, UserDeclaration};
use mesh::model::config_commands::{
    ConfigCommand, ConfigObject, ResolvedConfigCommand, ResolvedConfigObject,
};
use reqwest::Client;
use serde::Deserialize;

pub(crate) async fn apply(
    path: std::path::PathBuf,
    mut client: Client,
    relay_endpoint: String,
) -> Result<()> {
    for (filepath, cmd) in parse_directory(path)? {
        match apply_command(cmd, &mut client, &relay_endpoint).await {
            Ok(()) => println!("{} applied!", filepath),
            Err(e) => {
                println!("Unable to apply config file at {} with error {e}", filepath);
                continue;
            }
        }
    }
    Ok(())
}

/// Parses all objects found by recursively walking the provided path. Resolves the objects and
/// yields them in order of their apply_precedence, see [ResolvedConfigObject] for details.
fn parse_directory(
    path: std::path::PathBuf,
) -> Result<impl Iterator<Item = (String, ResolvedConfigCommand)>> {
    let mut resolved_cmds = walk_directory(path)
        .filter_map(|filepath| match try_read_as_config_command(&filepath) {
            Ok(cmds) => Some(
                iter::repeat(filepath.to_string_lossy().to_string())
                    .zip(cmds)
                    .collect_vec(),
            ),
            Err(e) => {
                println!(
                    "Unable to parse file at {} with error {e}",
                    filepath.to_string_lossy()
                );
                None
            }
        })
        .flatten()
        .filter_map(|(filepath, cmd)| match resolve_command(cmd) {
            Ok(resolved) => Some((filepath, resolved)),
            Err(e) => {
                println!(
                    "Unable to resolve config object {} with error {e}",
                    filepath
                );
                None
            }
        })
        .collect::<Vec<_>>();
    resolved_cmds.sort_by_key(|(_, cmd)| cmd.config_object.apply_precedence());
    Ok(resolved_cmds.into_iter())
}

fn resolve_command(command: ConfigCommand) -> Result<ResolvedConfigCommand> {
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

    Ok(ResolvedConfigCommand {
        api_version: command.api_version,
        config_object,
    })
}

pub async fn apply_command(
    command: ResolvedConfigCommand,
    client: &mut Client,
    relay_endpoint: &str,
) -> Result<()> {
    let r = client
        .post(format!("{relay_endpoint}/admin/apply"))
        .json(&command)
        .send()
        .await
        .map_err(|e| MeshError::RemoteError(e.to_string()))?;

    match r.text().await {
        Ok(_) => (),
        Err(e) => {
            println!("Failed to parse response as text with e {e}");
        }
    }

    Ok(())
}

fn try_read_as_config_command(path: &std::path::Path) -> Result<impl Iterator<Item=ConfigCommand>> {
    let f = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(f);
    let filename = path.to_string_lossy().to_string();
    let mut obj = vec![];
    for document in serde_yaml::Deserializer::from_reader(reader){
        match serde_yaml::with::singleton_map_recursive::deserialize(document){
            Ok(cmd) => obj.push(cmd),
            Err(e) => {
                println!("Unable to deserialize object as YAML from file {} with error {}",
                 filename,
                 e,
                 );
            }
        }
    }
    Ok(obj.into_iter())
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
    let cert_path = &user.x509_cert_file;
    let mut buf = Vec::new();
    std::fs::File::open(cert_path)?.read_to_end(&mut buf)?;
    Ok(ResolvedUserDeclaration {
        x509_cert: buf,
        attributes: user.attributes,
        permissions: user.permissions,
    })
}

fn resolve_relay_decl(relay: PeerRelayDeclaration) -> Result<ResolvedPeerRelayDeclaration> {
    let cert_path = &relay.x509_cert_file;
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
