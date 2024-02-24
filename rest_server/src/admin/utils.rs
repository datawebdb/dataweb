use std::collections::{HashMap, HashSet};

use std::io::BufReader;

use mesh::crud::PgDb;

use mesh::error::{MeshError, Result};

use mesh::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
use mesh::model::config_commands::entity::ResolvedEntityDeclaration;
use mesh::model::config_commands::local_data::ResolvedDataConnectionsDeclaration;
use mesh::model::config_commands::local_mapping::ResolvedLocalMappingDeclaration;
use mesh::model::config_commands::relay::ResolvedPeerRelayDeclaration;
use mesh::model::config_commands::remote_mapping::ResolvedRemoteMappingsDeclaration;
use mesh::model::config_commands::user::ResolvedUserDeclaration;
use mesh::model::config_commands::ResolvedConfigObject;
use mesh::model::entity::ArrowDataType;
use mesh::model::mappings::{Mapping, NewRemoteEntityMapping, RemoteInfoMapping};
use mesh::model::query::SubstitutionBlocks;
use mesh::model::relay::NewRelay;
use mesh::model::user::{NewUser, UserAttributes};
use mesh::model::{
    data_stores::{NewDataField, NewDataSource},
    entity::NewInformation,
};
use mesh::pki::{load_certificate_from_reader, parse_certificate};


/// Parses declaritive configuration object and updates the database state as appropriate.
pub async fn process_config_obj(db: &mut PgDb<'_>, config_obj: ResolvedConfigObject) -> Result<()> {
    match config_obj {
        ResolvedConfigObject::Entity(entity_decl) => process_entity_decl(db, entity_decl).await?,
        ResolvedConfigObject::LocalData(data_decl) => process_data_decl(db, data_decl).await?,
        ResolvedConfigObject::LocalMapping(map_decl) => {
            process_local_mapping_decl(db, map_decl).await?
        }
        ResolvedConfigObject::PeerRelay(relay_decl) => process_relay_decl(db, relay_decl).await?,
        ResolvedConfigObject::RemoteMapping(remote_map_decl) => {
            process_remote_map_decl(db, remote_map_decl).await?
        }
        ResolvedConfigObject::User(user_decl) => process_user_decls(db, user_decl).await?,
    }
    Ok(())
}

async fn process_entity_decl(db: &mut PgDb<'_>, entity_decl: ResolvedEntityDeclaration) -> Result<()> {
    let entity = db.create_entity_if_not_exist(&entity_decl.name).await?;
    for info_decl in entity_decl.information {
        let new_info = NewInformation {
            name: info_decl.name,
            arrow_dtype: ArrowDataType { inner: info_decl.arrow_dtype },
            entity_id: entity.id,
        };
        db.upsert_information(&new_info).await?;
    }
    Ok(())
}

async fn process_data_decl(db: &mut PgDb<'_>, data_decl: ResolvedDataConnectionsDeclaration) -> Result<()> {
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

async fn process_local_mapping_decl(
    db: &mut PgDb<'_>,
    map_decl: ResolvedLocalMappingDeclaration,
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

async fn process_relay_decl(db: &mut PgDb<'_>, relay_decl: ResolvedPeerRelayDeclaration) -> Result<()> {
    let mut cert_reader = BufReader::new(relay_decl.x509_cert.as_slice());
    let mut certs = load_certificate_from_reader(&mut cert_reader)?;

    if certs.is_empty() {
        return Err(MeshError::Internal(format!(
            "No certs were found for peer relay {}! \
        Please pass a file containing exactly one certficate.",
            relay_decl.name
        )));
    } else if certs.len() > 1 {
        return Err(MeshError::Internal(format!(
            "More than 1 cert found for peer relay {}! \
        Please pass a file containing exactly one certficate.",
            relay_decl.name
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
                        allowed_columns: HashSet::from_iter(source_permission_decl.allowed_columns),
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
    Ok(())
}

async fn process_remote_map_decl(
    db: &mut PgDb<'_>,
    remote_map_decl: ResolvedRemoteMappingsDeclaration,
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

async fn process_user_decls(db: &mut PgDb<'_>, user_decl: ResolvedUserDeclaration) -> Result<()> {
    let mut cert_reader = BufReader::new(user_decl.x509_cert.as_slice());
    let mut certs = load_certificate_from_reader(&mut cert_reader)?;
    if certs.is_empty() {
        return Err(MeshError::Internal("No certs were found in user declaration! \
        Please pass a file containing exactly one certficate.".to_string()));
    } else if certs.len() > 1 {
        return Err(MeshError::Internal("More than 1 cert found in user declaration! \
        Please pass a file containing exactly one certficate.".to_string()));
    }
    let cert = certs.remove(0);
    let (fingerprint, subject_dn, issuer_dn) = parse_certificate(&cert)?;
    let is_admin = user_decl
        .attributes
        .get("is_admin")
        .unwrap_or(&"false".to_string())
        .parse::<bool>()
        .map_err(|_| {
            MeshError::Internal(
                "Got invalid value for UserAttribute.is_admin, not parsable as bool!".to_string(),
            )
        })?;
    let new_user = NewUser {
        x509_sha256: fingerprint,
        x509_subject: subject_dn,
        x509_issuer: issuer_dn,
        attributes: UserAttributes { is_admin },
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
                        allowed_columns: HashSet::from_iter(source_permission_decl.allowed_columns),
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
    Ok(())
}
