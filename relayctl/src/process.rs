use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;

use mesh::crud::PgDb;

use mesh::error::{MeshError, Result};

use mesh::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
use mesh::model::entity::ArrowDataType;
use mesh::model::mappings::{Mapping, NewRemoteEntityMapping, RemoteInfoMapping};
use mesh::model::query::SubstitutionBlocks;
use mesh::model::relay::NewRelay;
use mesh::model::user::NewUser;
use mesh::model::{
    data_stores::{NewDataField, NewDataSource},
    entity::NewInformation,
};
use mesh::pki::{load_certificate_from_reader, parse_certificate};
use serde::Deserialize;

use crate::models::{
    DataConnectionsDeclaration, EntityDeclaration, LocalMappingDeclaration, PeerRelayDeclaration,
    RemoteMappingsDeclaration, UserDeclaration,
};

fn try_read_as_config_object<T: for<'a> Deserialize<'a>>(path: &std::path::Path) -> Result<Box<T>> {
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

async fn process_entity_decl(db: &mut PgDb<'_>, entity_decl: EntityDeclaration) -> Result<()> {
    let entity = db.create_entity_if_not_exist(&entity_decl.name).await?;
    for info_decl in entity_decl.information {
        let new_info = NewInformation {
            name: info_decl.name,
            arrow_dtype: {
                match serde_json::from_str(info_decl.arrow_dtype.as_str()) {
                    Ok(inner) => ArrowDataType { inner },
                    Err(_) => {
                        let modified = format!("\"{}\"", info_decl.arrow_dtype);
                        let inner = serde_json::from_str(modified.as_str())?;
                        ArrowDataType { inner }
                    }
                }
            },
            entity_id: entity.id,
        };
        db.upsert_information(&new_info).await?;
    }
    Ok(())
}

pub(crate) async fn process_entity_configs(
    db: &mut PgDb<'_>,
    entity_config_dir: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(entity_config_dir) {
        match try_read_as_config_object::<EntityDeclaration>(&path) {
            Ok(entity_decl) => {
                process_entity_decl(db, *entity_decl).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as Entity config with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}

async fn process_data_decl(db: &mut PgDb<'_>, data_decl: DataConnectionsDeclaration) -> Result<()> {
    let data_con = db
        .upsert_connection(&data_decl.name, data_decl.connection_options)
        .await?;
    for source_decl in data_decl.data_sources {
        let new_source = NewDataSource {
            name: source_decl.name,
            source_sql: source_decl.source_sql,
            data_connection_id: data_con.id,
            source_options: source_decl.source_options,
        };
        let source = db.upsert_source(&new_source).await?;
        for field_decl in source_decl.fields {
            let new_field = NewDataField {
                name: field_decl.name,
                data_source_id: source.id,
                path: field_decl.path,
            };
            db.upsert_field(&new_field).await?;
        }

        let default_permissions = source_decl.default_permission;
        let source_permission = SourcePermission {
            columns: ColumnPermission {
                allowed_columns: HashSet::from_iter(default_permissions.allowed_columns),
            },
            rows: RowPermission {
                allowed_rows: default_permissions.allowed_rows,
            },
        };

        db.upsert_default_source_permission(&source.id, &source_permission)
            .await?;
    }
    Ok(())
}

pub(crate) async fn process_local_data_configs(
    db: &mut PgDb<'_>,
    local_data_config: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(local_data_config) {
        match try_read_as_config_object::<DataConnectionsDeclaration>(&path) {
            Ok(data_decl) => {
                process_data_decl(db, *data_decl).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as Data Connections config with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}

async fn process_local_mapping_decl(
    db: &mut PgDb<'_>,
    map_decl: LocalMappingDeclaration,
) -> Result<()> {
    let entity = db.get_entity(&map_decl.entity_name).await?;
    let mut all_mappings = vec![];
    for data_con_map_decl in map_decl.mappings {
        let data_con = db.get_connection(&data_con_map_decl.data_con_name).await?;
        for source_map_decl in data_con_map_decl.source_mappings {
            let source = db
                .get_source(&source_map_decl.data_source_name, &data_con.id)
                .await?;
            for field_map_decl in source_map_decl.field_mappings {
                let field = db.get_field(&field_map_decl.field, &source.id).await?;
                let information = db.get_information(&field_map_decl.info, &entity.id).await?;
                let map = Mapping {
                    information_id: information.id,
                    data_field_id: field.id,
                    transformation: field_map_decl.transformation,
                };
                all_mappings.push(map);
            }
        }
    }
    db.create_mapping(&all_mappings).await?;
    Ok(())
}

pub(crate) async fn process_local_mapping_configs(
    db: &mut PgDb<'_>,
    local_mapping_configs: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(local_mapping_configs) {
        match try_read_as_config_object::<LocalMappingDeclaration>(&path) {
            Ok(map_decl) => {
                process_local_mapping_decl(db, *map_decl).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as Local Mapping config with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}

async fn process_relay_decl(
    db: &mut PgDb<'_>,
    relay_decls: Vec<PeerRelayDeclaration>,
) -> Result<()> {
    for relay_decl in relay_decls {
        let mut cert_reader =
            BufReader::new(File::open(&relay_decl.x509_cert_file).map_err(|e| {
                MeshError::SerDe(format!(
                    "error with file {}: {e}",
                    relay_decl.x509_cert_file
                ))
            })?);
        let mut certs = load_certificate_from_reader(&mut cert_reader)?;
        if certs.is_empty() {
            return Err(MeshError::Internal(format!(
                "No certs were found in {} for peer relay {}! \
            Please pass a file containing exactly one certficate.",
                relay_decl.x509_cert_file, relay_decl.name
            )));
        } else if certs.len() > 1 {
            return Err(MeshError::Internal(format!(
                "More than 1 cert found in {} for peer relay {}! \
            Please pass a file containing exactly one certficate.",
                relay_decl.x509_cert_file, relay_decl.name
            )));
        }
        let cert = certs.remove(0);
        let (fingerprint, subject_dn, issuer_dn) = parse_certificate(&cert)?;
        let new_relay = NewRelay {
            name: relay_decl.name,
            rest_endpoint: relay_decl.rest_endpoint,
            flight_endpoint: relay_decl.flight_endpoint,
            x509_sha256: fingerprint,
            x509_subject: subject_dn,
            x509_issuer: issuer_dn,
        };
        let relay = db.upsert_relay(&new_relay).await?;
        if let Some(permissions) = relay_decl.permissions {
            for permission in permissions {
                let data_con = db.get_connection(&permission.data_con_name).await?;
                for source_permission_decl in permission.source_permissions {
                    let source = db
                        .get_source(&source_permission_decl.data_source_name, &data_con.id)
                        .await?;
                    let source_permission = SourcePermission {
                        columns: ColumnPermission {
                            allowed_columns: HashSet::from_iter(
                                source_permission_decl.allowed_columns,
                            ),
                        },
                        rows: RowPermission {
                            allowed_rows: source_permission_decl.allowed_rows,
                        },
                    };
                    db.upsert_relay_source_permission(&relay.id, &source.id, &source_permission)
                        .await?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn process_relay_configs(
    db: &mut PgDb<'_>,
    relay_configs: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(relay_configs) {
        match try_read_as_config_object::<Vec<PeerRelayDeclaration>>(&path) {
            Ok(relay_decls) => {
                process_relay_decl(db, *relay_decls).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as peer relay config with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}

async fn process_remote_map_decl(
    db: &mut PgDb<'_>,
    remote_map_decl: RemoteMappingsDeclaration,
) -> Result<()> {
    let entity = db.get_entity(&remote_map_decl.entity_name).await?;
    for peer_map in remote_map_decl.mappings {
        let relay = db.get_relay_by_name(&peer_map.relay_name).await?;
        let entity_map = match &peer_map.entity_map {
            Some(map) => NewRemoteEntityMapping {
                sql: map.sql.clone(),
                substitution_blocks: map.substitution_blocks.clone(),
                relay_id: relay.id,
                entity_id: entity.id,
                remote_entity_name: peer_map.remote_entity_name.clone(),
                needs_subquery_transformation: true,
            },
            None => NewRemoteEntityMapping {
                sql: "".to_string(),
                substitution_blocks: SubstitutionBlocks {
                    info_substitutions: HashMap::new(),
                    source_substitutions: HashMap::new(),
                    num_capture_braces: 1,
                },
                relay_id: relay.id,
                entity_id: entity.id,
                remote_entity_name: peer_map.remote_entity_name.clone(),
                needs_subquery_transformation: false,
            },
        };

        let entity_map = db.upsert_remote_entity_mapping(&entity_map).await?;
        for map in peer_map.relay_mappings {
            let local_info = db.get_information(&map.local_info, &entity.id).await?;
            let newmap = RemoteInfoMapping {
                remote_entity_mapping_id: entity_map.id,
                information_id: local_info.id,
                info_mapped_name: map.info_mapped_name,
                literal_derived_field: map.literal_derived_field,
                transformation: map.transformation,
            };
            db.upsert_remote_info_mapping(&newmap).await?;
        }
    }
    Ok(())
}

pub(crate) async fn process_remote_mapping_configs(
    db: &mut PgDb<'_>,
    remote_mapping_configs: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(remote_mapping_configs) {
        match try_read_as_config_object::<RemoteMappingsDeclaration>(&path) {
            Ok(remote_map_decl) => {
                process_remote_map_decl(db, *remote_map_decl).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as Remote Mapping configs with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}

async fn process_user_decls(db: &mut PgDb<'_>, user_decls: Vec<UserDeclaration>) -> Result<()> {
    for user_decl in user_decls {
        let mut cert_reader = BufReader::new(File::open(&user_decl.x509_cert_file)?);
        let mut certs = load_certificate_from_reader(&mut cert_reader)?;
        if certs.is_empty() {
            return Err(MeshError::Internal(format!(
                "No certs were found in {} of user declaration! \
            Please pass a file containing exactly one certficate.",
                user_decl.x509_cert_file
            )));
        } else if certs.len() > 1 {
            return Err(MeshError::Internal(format!(
                "More than 1 cert found in {} of user declaration! \
            Please pass a file containing exactly one certficate.",
                user_decl.x509_cert_file
            )));
        }
        let cert = certs.remove(0);
        let (fingerprint, subject_dn, issuer_dn) = parse_certificate(&cert)?;
        let new_user = NewUser {
            x509_sha256: fingerprint,
            x509_subject: subject_dn,
            x509_issuer: issuer_dn,
        };
        let user = db.upsert_user_by_fingerprint(&new_user).await?;
        if let Some(permissions) = user_decl.permissions {
            for permission in permissions {
                let data_con = db.get_connection(&permission.data_con_name).await?;
                for source_permission_decl in permission.source_permissions {
                    let source = db
                        .get_source(&source_permission_decl.data_source_name, &data_con.id)
                        .await?;
                    let source_permission = SourcePermission {
                        columns: ColumnPermission {
                            allowed_columns: HashSet::from_iter(
                                source_permission_decl.allowed_columns,
                            ),
                        },
                        rows: RowPermission {
                            allowed_rows: source_permission_decl.allowed_rows,
                        },
                    };
                    db.upsert_user_source_permission(&user.id, &source.id, &source_permission)
                        .await?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn process_user_configs(
    db: &mut PgDb<'_>,
    user_mapping_configs: std::path::PathBuf,
) -> Result<()> {
    for path in walk_directory(user_mapping_configs) {
        match try_read_as_config_object::<Vec<UserDeclaration>>(&path) {
            Ok(user_decl) => {
                process_user_decls(db, *user_decl).await?;
            }
            Err(e) => {
                println!(
                    "Could not parse {:?} as Remote Mapping configs with error {}",
                    path, e
                );
            }
        }
    }
    Ok(())
}
