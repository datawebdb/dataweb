use std::collections::HashMap;

use crate::error::Result;
use crate::model::data_stores::DataField;
use crate::model::query::{
    default_scope, InfoSubstitution, OriginatorEntityMapping, OriginatorInfoMapping,
    OriginatorMappings, RawQueryRequest, ScopedOriginatorMappings,
};
use crate::model::relay::Relay;
use crate::model::user::User;
use crate::{
    error::MeshError,
    model::query::{SourceSubstitution, SubstitutionBlocks},
};

use itertools::Itertools;
use tracing::debug;
use uuid::Uuid;

use super::map_local::substitute_and_transform_info;
use super::utils::compose_derived_source_substitution;
use super::LocaltoRemoteNameHashMap;

/// Substitutes appropriate [Entity][crate::model::entity::Entity] and
/// [Information][crate::model::entity::Information] names for a specific remote relay
/// for a [SubstitutionBlocks] object sent to the local relay. Also applies relevant
/// [Transformations][crate::model::mappings::Transformation] to the request SQL.
pub(crate) fn map_remote_request(
    raw_request: &RawQueryRequest,
    relay: &Relay,
    originating_relay: Option<Relay>,
    requesting_user: &User,
    request_uuid: &Uuid,
    name_map: &LocaltoRemoteNameHashMap,
) -> Result<RawQueryRequest> {
    let substitution_blocks = &raw_request.substitution_blocks;
    let mut sql = raw_request.sql.clone();
    let mut out_blocks = SubstitutionBlocks {
        info_substitutions: HashMap::with_capacity(substitution_blocks.info_substitutions.len()),
        source_substitutions: HashMap::with_capacity(
            substitution_blocks.source_substitutions.len(),
        ),
        num_capture_braces: substitution_blocks.num_capture_braces,
    };

    let left_capture = &"{".repeat(substitution_blocks.num_capture_braces);
    let right_capture = &"}".repeat(substitution_blocks.num_capture_braces);

    debug!("Mapping SQL template for remote...{sql}");

    let new_originator_mappings;
    (sql, new_originator_mappings) = map_source_substitutions(
        sql,
        left_capture,
        right_capture,
        &mut out_blocks,
        relay,
        substitution_blocks,
        name_map,
    )?;

    debug!("Source substitutions applied: {sql}");

    sql = map_info_substitutions(
        sql,
        left_capture,
        right_capture,
        &mut out_blocks,
        relay,
        &substitution_blocks.info_substitutions,
        name_map,
        &raw_request.originator_mappings,
    )?;

    debug!("Info substitutions applied: {sql}");

    let originator_mappings = map_scoped_originator_mappings(
        &raw_request.originator_mappings,
        relay,
        name_map,
        &out_blocks,
        new_originator_mappings,
    )?;

    Ok(RawQueryRequest {
        sql,
        substitution_blocks: out_blocks,
        request_uuid: Some(*request_uuid),
        requesting_user: Some(requesting_user.clone()),
        originating_relay,
        originating_task_id: raw_request.originating_task_id,
        originator_mappings,
        return_arrow_schema: raw_request.return_arrow_schema.clone(),
    })
}

fn map_source_substitutions(
    mut replaced: String,
    left_capture: &String,
    right_capture: &String,
    out_blocks: &mut SubstitutionBlocks,
    relay: &Relay,
    in_blocks: &SubstitutionBlocks,
    name_map: &LocaltoRemoteNameHashMap,
) -> Result<(String, Option<ScopedOriginatorMappings>)> {
    let mut new_originator_mappings = None;
    for (key, source_sub) in in_blocks.source_substitutions.iter() {
        match source_sub {
            SourceSubstitution::AllSourcesWith(entity_list) => {
                for entity_name in entity_list {
                    let entity_map = name_map
                        .get(entity_name.as_str())
                        .ok_or(MeshError::InvalidQuery(format!(
                            "Missing remote mapping for local entity {entity_name} for relay \
                                {} which is required to execute this query!",
                            relay.id
                        )))?
                        .0;
                    if entity_map.needs_subquery_transformation {
                        let pattern = format!("{left_capture}{key}{right_capture}");
                        (replaced, new_originator_mappings) = compose_derived_source_substitution(
                            replaced,
                            in_blocks,
                            left_capture,
                            right_capture,
                            pattern,
                            entity_map,
                            out_blocks,
                        );
                    } else {
                        out_blocks.source_substitutions.insert(
                            key.clone(),
                            SourceSubstitution::AllSourcesWith(vec![entity_map
                                .remote_entity_name
                                .clone()]),
                        );
                    }
                }
            }
            SourceSubstitution::SourceList(_) => {
                return Err(MeshError::Internal(
                    "Remote mapping source list is unimplemented!".into(),
                ))
            }
        }
    }
    Ok((replaced, new_originator_mappings))
}

#[allow(clippy::too_many_arguments)]
fn map_info_substitutions(
    mut replaced: String,
    left_capture: &str,
    right_capture: &str,
    out_blocks: &mut SubstitutionBlocks,
    relay: &Relay,
    info_substitutions: &HashMap<String, InfoSubstitution>,
    name_map: &LocaltoRemoteNameHashMap,
    originator_mappings: &Option<ScopedOriginatorMappings>,
) -> Result<String> {
    for (key, info_sub) in info_substitutions.iter() {
        let (entity_name, info_name_map) =
            name_map
                .get(info_sub.entity_name.as_str())
                .ok_or(MeshError::InvalidQuery(format!(
                    "Missing remote mapping for local entity {} for relay \
                {} which is required to execute this query!",
                    info_sub.entity_name.as_str(),
                    relay.id
                )))?;
        debug!("Got info_name_map {:?}", info_name_map);
        debug!("Getting key {:?}", info_sub.info_name);
        let remote_info_map =
            info_name_map
                .get(&info_sub.info_name.as_str())
                .ok_or(MeshError::InvalidQuery(format!(
                    "Missing remote mapping for local info {}.{} for relay \
                {} which is required to execute this query!",
                    info_sub.entity_name, info_sub.info_name, relay.id
                )))?;
        if remote_info_map.literal_derived_field {
            // When literal_derived_field is true, that means the entity mapping created
            // a derived field which the remote relay won't recognize. Thus, the local
            // relay needs to substitute the appropriate literal field identifier into
            // the template prior to forwarding the request.
            //
            // No need to add anything to out_blocks here since we've already executed the
            // appropriate substitution.

            let derived_field = DataField {
                id: Uuid::new_v4(),
                name: format!("_derived_{}_", &remote_info_map.info_mapped_name),
                data_source_id: Uuid::new_v4(),
                path: remote_info_map.info_mapped_name.clone(),
            };
            replaced = substitute_and_transform_info(
                replaced,
                key,
                left_capture,
                right_capture,
                originator_mappings,
                remote_info_map.info_mapped_name.clone(),
                info_sub,
                &derived_field,
            )?;
        } else {
            let info_name = remote_info_map.info_mapped_name.to_string();
            let mapped_info = InfoSubstitution {
                entity_name: entity_name.remote_entity_name.to_string(),
                info_name,
                scope: info_sub.scope.clone(),
                include_info: info_sub.include_info,
                exclude_info_alias: info_sub.exclude_info_alias,
                include_data_field: info_sub.include_data_field,
            };
            if out_blocks.info_substitutions.contains_key(key) {
                // This is an internal error since it violates a constraint enforced
                // by the source_subtitution function.
                return Err(MeshError::Internal(format!(
                    "Invalid info_substitution {} \
                substitution key conflict detected.",
                    key
                )));
            } else {
                out_blocks
                    .info_substitutions
                    .insert(key.clone(), mapped_info);
            }
        }
    }

    Ok(replaced)
}

/// Maps all [OriginatorMappings] for all scopes from local->originator to remote-originator
/// for the given remote [Relay]. If a [RemoteEntityMapping][crate::model::mappings::RemoteEntityMapping]
/// was injected, any new scopes are passed in as new_originator_mappings and must be joined into
/// the final output.
fn map_scoped_originator_mappings(
    scoped_originator_mappings: &Option<ScopedOriginatorMappings>,
    relay: &Relay,
    name_map: &LocaltoRemoteNameHashMap,
    out_blocks: &SubstitutionBlocks,
    new_originator_mappings: Option<ScopedOriginatorMappings>,
) -> Result<Option<ScopedOriginatorMappings>> {
    let mut inner = HashMap::new();
    match scoped_originator_mappings {
        Some(scoped_mappings) => {
            for (scope, mappings) in scoped_mappings.inner.iter() {
                inner.insert(
                    scope.clone(),
                    map_originator_mappings(scope, Some(mappings), relay, name_map, out_blocks)?,
                );
            }
        }
        None => {
            // When ScopedOriginatorMappings is None, this means the local Relay is processing a request directly
            // from an end user. It is an invariant that the only scope for all InfoSubstitutions at this stage is
            // the default_scope.
            inner.insert(
                default_scope(),
                map_originator_mappings(&default_scope(), None, relay, name_map, out_blocks)?,
            );
        }
    }

    if let Some(new_mappings) = new_originator_mappings {
        inner.extend(new_mappings.inner);
    }

    Ok(Some(ScopedOriginatorMappings { inner }))
}

fn map_originator_mappings(
    scope: &String,
    originator_mappings: Option<&OriginatorMappings>,
    relay: &Relay,
    name_map: &LocaltoRemoteNameHashMap,
    out_blocks: &SubstitutionBlocks,
) -> Result<OriginatorMappings> {
    match &originator_mappings {
        Some(mappings) => {
            // Update from local->originating to remote->originating, where "remote" is the relay to which
            // the request under construction will be sent. This is accomplished by applying our
            // Local->remote name mappings to the Keys of the existing Local->orig name mappings while leaving
            // the values as-is.
            let mappings: Result<HashMap<_, _>> = mappings
                .inner
                .iter()
                .map(|(local_entity, orig_entity_mapping)| {
                    let (remote_entity, info_map) = name_map
                        .get(local_entity.as_str())
                        .ok_or(MeshError::InvalidQuery(format!(
                            "Missing remote mapping for local entity {} for relay \
                            {} which is required to execute this query!",
                            local_entity, relay.id
                            )))?;
                    let remote_entity = remote_entity.remote_entity_name.to_string();
                    let new_orig_info_map: Result<HashMap<_, _>> = orig_entity_mapping
                        .originator_info_map
                        .iter()
                        .map(|(local_info, orig_info)| {
                            let remote_info = info_map
                                .get(local_info.as_str())
                                .ok_or(MeshError::InvalidQuery(format!(
                                    "Missing remote mapping for local entity {} and info {} for relay \
                                    {} which is required to execute this query!",
                                    local_entity, local_info, relay.id
                                    )))?;
                            // composing local_to_originator with remote_to_local gives remote_to_originator transformation.
                            let local_to_originator = &orig_info.transformation;
                            let remote_to_local = remote_info.transformation.invert();
                            let remote_to_originator = local_to_originator.compose(&remote_to_local);
                            debug!("Updated local to originator transformation: {:?} to remote to originator: {:?}", local_to_originator, remote_to_originator);
                            let new_orig_info_map = OriginatorInfoMapping{
                                originator_info_name: orig_info.originator_info_name.clone(),
                                transformation: remote_to_originator,
                            };
                            Ok((remote_info.info_mapped_name.to_string(), new_orig_info_map))
                        })
                        // If we fully mapped and resolved a derived field, it must be removed from originator mappings here.
                        // To accomplish this, we filter out any remote_entity/remote_info that is not included in the outgoing
                        // info_substitutions.
                        .filter_ok(|(remote_info, _)| {
                            out_blocks
                                .info_substitutions
                                .values()
                                .any(|sub| {
                                    sub.entity_name == *remote_entity
                                        && sub.info_name == **remote_info
                                            && sub.scope == *scope
                                })
                        })
                        .collect();
                    let new_orig_entity_map = OriginatorEntityMapping{
                        originator_entity_name: orig_entity_mapping.originator_entity_name.clone(),
                        originator_info_map: new_orig_info_map?
                    };
                    Ok((remote_entity, new_orig_entity_map))
                })
                .collect();

            Ok(OriginatorMappings { inner: mappings? })
        }
        None => {
            // Here we are flipping the direction of k->v to v->k for both the inner and outer HashMaps.
            // This gives the remote Relay a mapping from its local names to the names of the originating relay
            // which in this case the originating relay == the local relay.
            let inner: HashMap<_, _> = name_map
                .iter()
                .map(
                    |(originating_entity, (remote_entity, orig_to_remote_info))| {
                        let originator_info_map: HashMap<_, _> = orig_to_remote_info
                            .iter()
                            .map(|(orig, map)| {
                                let orig_info_map = OriginatorInfoMapping {
                                    originator_info_name: orig.to_string(),
                                    transformation: map.transformation.invert(),
                                };
                                (map.info_mapped_name.to_string(), orig_info_map)
                            })
                            // This filter removes any alias mapping that is not relevant to the query being forwarded
                            // e.g. if the entity/info name are not contained in any outgoing info_substition,
                            // don't include in the forwarded originator mappings
                            .filter(|(remote_info, _)| {
                                out_blocks.info_substitutions.values().any(|sub| {
                                    sub.entity_name == *remote_entity.remote_entity_name
                                        && sub.info_name == **remote_info
                                        && sub.scope == *scope
                                })
                            })
                            .collect();
                        let orig_entity_map = OriginatorEntityMapping {
                            originator_entity_name: originating_entity.to_string(),
                            originator_info_map,
                        };
                        (
                            remote_entity.remote_entity_name.to_string(),
                            orig_entity_map,
                        )
                    },
                )
                .filter(|(_, orig_entity_map)| !orig_entity_map.originator_info_map.is_empty())
                .collect();
            Ok(OriginatorMappings { inner })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use crate::error::Result;
    use crate::execute::map_remote::map_scoped_originator_mappings;
    use crate::model::query::ScopedOriginatorMappings;
    use crate::{
        execute::map_remote::{map_info_substitutions, map_source_substitutions},
        model::{
            mappings::{RemoteEntityMapping, RemoteInfoMapping, Transformation},
            query::{
                InfoSubstitution, OriginatorEntityMapping, OriginatorInfoMapping,
                OriginatorMappings, SourceSubstitution, SubstitutionBlocks,
            },
            relay::Relay,
        },
    };

    /// Values which typically would come from application state (the database), but
    /// for the purposes of unit tests, we just make up some values.
    type SomeValues = (
        String,
        HashMap<String, InfoSubstitution>,
        HashMap<String, SourceSubstitution>,
        Relay,
        ScopedOriginatorMappings,
    );

    fn make_up_some_values() -> SomeValues {
        let mut info_substitutions = HashMap::new();
        info_substitutions.insert(
            "info".to_string(),
            InfoSubstitution {
                entity_name: "test".to_string(),
                info_name: "test".to_string(),
                scope: "origin".to_string(),
                include_info: true,
                exclude_info_alias: false,
                include_data_field: true,
            },
        );

        info_substitutions.insert(
            "info2".to_string(),
            InfoSubstitution {
                entity_name: "test".to_string(),
                info_name: "test".to_string(),
                scope: "origin".to_string(),
                include_info: false,
                exclude_info_alias: false,
                include_data_field: true,
            },
        );

        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "source".to_string(),
            SourceSubstitution::AllSourcesWith(vec!["test".to_string()]),
        );

        let relay = Relay {
            id: Uuid::new_v4(),
            rest_endpoint: "test".to_string(),
            name: "test".to_string(),
            flight_endpoint: "test".to_string(),
            x509_sha256: "test".to_string(),
            x509_subject: "test".to_string(),
            x509_issuer: "test".to_string(),
        };

        let mut originator_info_map = HashMap::new();
        originator_info_map.insert(
            "test".to_string(),
            OriginatorInfoMapping {
                originator_info_name: "test_orig".to_string(),
                transformation: Transformation {
                    other_to_local_info: "{v}/100".to_string(),
                    local_info_to_other: "{v}*100".to_string(),
                    replace_from: "{v}".to_string(),
                },
            },
        );

        let mut inner = HashMap::new();
        inner.insert(
            "test".to_string(),
            OriginatorEntityMapping {
                originator_entity_name: "test_orig".to_string(),
                originator_info_map,
            },
        );

        let alias_mapping = OriginatorMappings { inner };

        let scoped_alias_mapping = ScopedOriginatorMappings {
            inner: HashMap::from_iter(vec![("origin".to_string(), alias_mapping)]),
        };

        let replaced = "select {info} from {source} where {info2}=0.1".to_string();

        (
            replaced,
            info_substitutions,
            source_substitutions,
            relay,
            scoped_alias_mapping,
        )
    }

    #[test]
    fn test_map_originator_mappings() -> Result<()> {
        let (_replaced, _info_substitutions, _source_substitutions, relay, alias_mapping) =
            make_up_some_values();

        let entity_map = RemoteEntityMapping {
            id: Uuid::new_v4(),
            sql: "{test_remote}".to_string(),
            substitution_blocks: SubstitutionBlocks {
                info_substitutions: HashMap::new(),
                source_substitutions: HashMap::from_iter(vec![(
                    "test_remote".to_string(),
                    SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
                )]),
                num_capture_braces: 1,
            },
            relay_id: Uuid::new_v4(),
            entity_id: Uuid::new_v4(),
            remote_entity_name: "test_remote".to_string(),
            needs_subquery_transformation: false,
        };

        let map = RemoteInfoMapping {
            information_id: Uuid::new_v4(),
            info_mapped_name: "test_remote".to_string(),
            literal_derived_field: false,
            transformation: Transformation {
                other_to_local_info: "{v}/10".to_string(),
                local_info_to_other: "{v}*10".to_string(),
                replace_from: "{v}".to_string(),
            },
            remote_entity_mapping_id: Uuid::new_v4(),
        };
        let mut info_map = HashMap::new();
        info_map.insert("test", &map);
        let mut name_map = HashMap::new();
        name_map.insert("test", (&entity_map, info_map));

        let out_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::from_iter(vec![
                (
                    "info".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: true,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
                (
                    "info2".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: false,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
            ]),
            source_substitutions: HashMap::from_iter(vec![(
                "source".to_string(),
                SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
            )]),
            num_capture_braces: 1,
        };

        let orig_mappings = map_scoped_originator_mappings(
            &Some(alias_mapping),
            &relay,
            &name_map,
            &out_blocks,
            None,
        )?;

        let mut originator_info_map = HashMap::new();
        originator_info_map.insert(
            "test_remote".to_string(),
            OriginatorInfoMapping {
                originator_info_name: "test_orig".to_string(),
                transformation: Transformation {
                    other_to_local_info: "({v}/100)*10".to_string(),
                    local_info_to_other: "({v}/10)*100".to_string(),
                    replace_from: "{v}".to_string(),
                },
            },
        );

        let mut inner = HashMap::new();
        inner.insert(
            "test_remote".to_string(),
            OriginatorEntityMapping {
                originator_entity_name: "test_orig".to_string(),
                originator_info_map,
            },
        );
        let expected = ScopedOriginatorMappings {
            inner: HashMap::from_iter(vec![("origin".to_string(), OriginatorMappings { inner })]),
        };

        assert_eq!(orig_mappings, Some(expected));

        let orig_mappings =
            map_scoped_originator_mappings(&None, &relay, &name_map, &out_blocks, None)?;

        let mut originator_info_map = HashMap::new();
        originator_info_map.insert(
            "test_remote".to_string(),
            OriginatorInfoMapping {
                originator_info_name: "test".to_string(),
                transformation: Transformation {
                    other_to_local_info: "{v}*10".to_string(),
                    local_info_to_other: "{v}/10".to_string(),
                    replace_from: "{v}".to_string(),
                },
            },
        );

        let mut inner = HashMap::new();
        inner.insert(
            "test_remote".to_string(),
            OriginatorEntityMapping {
                originator_entity_name: "test".to_string(),
                originator_info_map,
            },
        );
        let expected = ScopedOriginatorMappings {
            inner: HashMap::from_iter(vec![("origin".to_string(), OriginatorMappings { inner })]),
        };

        assert_eq!(orig_mappings, Some(expected));

        Ok(())
    }

    #[test]
    fn test_map_source_substitutions() -> Result<()> {
        let (mut replaced, info_substitutions, source_substitutions, relay, _alias_mapping) =
            make_up_some_values();

        let entity_map = RemoteEntityMapping {
            id: Uuid::new_v4(),
            sql: "{test_remote}".to_string(),
            substitution_blocks: SubstitutionBlocks {
                info_substitutions: HashMap::new(),
                source_substitutions: HashMap::from_iter(vec![(
                    "test_remote".to_string(),
                    SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
                )]),
                num_capture_braces: 1,
            },
            relay_id: Uuid::new_v4(),
            entity_id: Uuid::new_v4(),
            remote_entity_name: "test_remote".to_string(),
            needs_subquery_transformation: false,
        };

        let map = RemoteInfoMapping {
            information_id: Uuid::new_v4(),
            info_mapped_name: "test_remote".to_string(),
            literal_derived_field: false,
            transformation: Transformation {
                other_to_local_info: "{v}/10".to_string(),
                local_info_to_other: "{v}*10".to_string(),
                replace_from: "{v}".to_string(),
            },
            remote_entity_mapping_id: Uuid::new_v4(),
        };

        let mut info_map = HashMap::new();
        info_map.insert("test", &map);
        let mut name_map = HashMap::new();
        name_map.insert("test", (&entity_map, info_map));

        let mut out_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions: HashMap::new(),
            num_capture_braces: 1,
        };

        let in_blocks = SubstitutionBlocks {
            info_substitutions,
            source_substitutions,
            num_capture_braces: 1,
        };

        (replaced, _) = map_source_substitutions(
            replaced,
            &"{".to_string(),
            &"}".to_string(),
            &mut out_blocks,
            &relay,
            &in_blocks,
            &name_map,
        )?;

        let expected_sql = "select {info} from {source} where {info2}=0.1";
        assert_eq!(replaced, expected_sql);

        let expected = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions: HashMap::from_iter(vec![(
                "source".to_string(),
                SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
            )]),
            num_capture_braces: 1,
        };

        assert_eq!(out_blocks, expected);

        Ok(())
    }

    #[test]
    fn test_map_info_substitutions() -> Result<()> {
        let (replaced, info_substitutions, _source_substitutions, relay, _alias_mapping) =
            make_up_some_values();

        let entity_map = RemoteEntityMapping {
            id: Uuid::new_v4(),
            sql: "{test_remote}".to_string(),
            substitution_blocks: SubstitutionBlocks {
                info_substitutions: HashMap::new(),
                source_substitutions: HashMap::from_iter(vec![(
                    "test_remote".to_string(),
                    SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
                )]),
                num_capture_braces: 1,
            },
            relay_id: Uuid::new_v4(),
            entity_id: Uuid::new_v4(),
            remote_entity_name: "test_remote".to_string(),
            needs_subquery_transformation: false,
        };

        let map = RemoteInfoMapping {
            information_id: Uuid::new_v4(),
            info_mapped_name: "test_remote".to_string(),
            literal_derived_field: false,
            transformation: Transformation {
                other_to_local_info: "{v}/10".to_string(),
                local_info_to_other: "{v}*10".to_string(),
                replace_from: "{v}".to_string(),
            },
            remote_entity_mapping_id: Uuid::new_v4(),
        };
        let mut info_map = HashMap::new();
        info_map.insert("test", &map);
        let mut name_map = HashMap::new();
        name_map.insert("test", (&entity_map, info_map));

        let mut out_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions: HashMap::new(),
            num_capture_braces: 1,
        };

        map_info_substitutions(
            replaced,
            &"{".to_string(),
            &"}".to_string(),
            &mut out_blocks,
            &relay,
            &info_substitutions,
            &name_map,
            &None,
        )?;

        let expected = SubstitutionBlocks {
            info_substitutions: HashMap::from_iter(vec![
                (
                    "info".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: true,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
                (
                    "info2".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: false,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
            ]),
            source_substitutions: HashMap::new(),
            num_capture_braces: 1,
        };

        assert_eq!(out_blocks, expected);

        Ok(())
    }

    #[test]
    fn test_map_remote() -> Result<()> {
        let (mut replaced, info_substitutions, source_substitutions, relay, _alias_mapping) =
            make_up_some_values();

        let entity_map = RemoteEntityMapping {
            id: Uuid::new_v4(),
            sql: "{test_remote}".to_string(),
            substitution_blocks: SubstitutionBlocks {
                info_substitutions: HashMap::new(),
                source_substitutions: HashMap::from_iter(vec![(
                    "test_remote".to_string(),
                    SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
                )]),
                num_capture_braces: 1,
            },
            relay_id: Uuid::new_v4(),
            entity_id: Uuid::new_v4(),
            remote_entity_name: "test_remote".to_string(),
            needs_subquery_transformation: false,
        };

        let map = RemoteInfoMapping {
            information_id: Uuid::new_v4(),
            info_mapped_name: "test_remote".to_string(),
            literal_derived_field: false,
            transformation: Transformation {
                other_to_local_info: "{v}/10".to_string(),
                local_info_to_other: "{v}*10".to_string(),
                replace_from: "{v}".to_string(),
            },
            remote_entity_mapping_id: Uuid::new_v4(),
        };
        let mut info_map = HashMap::new();
        info_map.insert("test", &map);
        let mut name_map = HashMap::new();
        name_map.insert("test", (&entity_map, info_map));
        let mut out_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions: HashMap::new(),
            num_capture_braces: 1,
        };

        let in_blocks = SubstitutionBlocks {
            info_substitutions,
            source_substitutions,
            num_capture_braces: 1,
        };

        (replaced, _) = map_source_substitutions(
            replaced,
            &"{".to_string(),
            &"}".to_string(),
            &mut out_blocks,
            &relay,
            &in_blocks,
            &name_map,
        )?;

        replaced = map_info_substitutions(
            replaced,
            &"{".to_string(),
            &"}".to_string(),
            &mut out_blocks,
            &relay,
            &in_blocks.info_substitutions,
            &name_map,
            &None,
        )?;

        let expected_sql = "select {info} from {source} where {info2}=0.1";
        assert_eq!(replaced, expected_sql);

        let expected = SubstitutionBlocks {
            info_substitutions: HashMap::from_iter(vec![
                (
                    "info".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: true,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
                (
                    "info2".to_string(),
                    InfoSubstitution {
                        entity_name: "test_remote".to_string(),
                        info_name: "test_remote".to_string(),
                        scope: "origin".to_string(),
                        include_info: false,
                        exclude_info_alias: false,
                        include_data_field: true,
                    },
                ),
            ]),
            source_substitutions: HashMap::from_iter(vec![(
                "source".to_string(),
                SourceSubstitution::AllSourcesWith(vec!["test_remote".to_string()]),
            )]),
            num_capture_braces: 1,
        };

        assert_eq!(out_blocks, expected);

        Ok(())
    }
}
