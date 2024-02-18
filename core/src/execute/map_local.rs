use std::collections::HashMap;

use crate::error::Result;

use crate::model::access_control::SourcePermission;
use crate::model::mappings::Transformation;
use crate::model::query::{InfoSubstitution, ScopedOriginatorMappings};

use crate::{
    error::MeshError,
    model::{
        data_stores::{DataConnection, DataField, DataSource},
        entity::{Entity, Information},
        mappings::Mapping,
        query::{SourceSubstitution, SubstitutionBlocks},
    },
};

/// Substitutes appropriate table names and fields for a specific source
/// into a sql template
pub(crate) fn map_sql(
    sql: &str,
    _con: &DataConnection,
    source: &DataSource,
    mappings: &[(Entity, Information, DataField, Mapping)],
    substitution_blocks: &SubstitutionBlocks,
    alias_mappings: &Option<ScopedOriginatorMappings>,
    permission: SourcePermission,
) -> Result<String> {
    let left_capture = &"{".repeat(substitution_blocks.num_capture_braces);
    let right_capture = &"}".repeat(substitution_blocks.num_capture_braces);

    let mut replaced = sql.to_string();

    replaced = apply_source_substitutions(
        replaced,
        left_capture,
        right_capture,
        source,
        &substitution_blocks.source_substitutions,
        &permission,
    );

    let mut info_map_lookup = HashMap::with_capacity(mappings.len());
    for (entity, info, field, map) in mappings.iter() {
        info_map_lookup.insert((entity.name.as_str(), info.name.as_str()), (field, map));
    }

    replaced = apply_info_substitutions(
        replaced,
        left_capture,
        right_capture,
        &substitution_blocks.info_substitutions,
        &info_map_lookup,
        alias_mappings,
        &permission,
    )?;

    Ok(replaced)
}

fn apply_source_substitutions(
    mut replaced: String,
    left_capture: &String,
    right_capture: &String,
    source: &DataSource,
    source_substitutions: &HashMap<String, SourceSubstitution>,
    permission: &SourcePermission,
) -> String {
    let allowed_source = format!(
        "(select {} from ({}) where {})",
        &permission
            .columns
            .allowed_columns
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(","),
        &source.source_sql,
        &permission.rows.allowed_rows,
    );
    for (source_key, _) in source_substitutions.iter() {
        let pattern = format!("{left_capture}{source_key}{right_capture}");
        replaced = replaced.replace(pattern.as_str(), &allowed_source);
    }
    replaced
}

fn get_originator_alias_and_transform<'a>(
    scoped_alias_mappings: &'a Option<ScopedOriginatorMappings>,
    block: &'a InfoSubstitution,
) -> Result<(String, Option<&'a Transformation>)> {
    let out = match scoped_alias_mappings {
        Some(scoped_alias_mappings) => {
            // Here we are mapping our local info name back to the originator's info name.
            // Since the originator could be multiple hops away, we use the alias mappings included
            // in the request from our peered relay. Note that a single entity/information name may
            // have different mappings depending on the scope of the InfoSubstitution.
            match scoped_alias_mappings.inner.get(&block.scope) {
                Some(alias_map) => match alias_map.inner.get(&block.entity_name) {
                    Some(entity_map) => {
                        match entity_map.originator_info_map.get(&block.info_name) {
                            Some(info_map) => {
                                let orig_alias = info_map.originator_info_name.clone();
                                let orig_transform = &info_map.transformation;
                                (orig_alias, Some(orig_transform))
                            }
                            None => {
                                if block.include_info {
                                    return Err(MeshError::InvalidQuery(format!(
                                        "Originator Entity Mapping \
                                        for {} is missing Info mapping for {} \
                                        with scope {}.",
                                        block.entity_name, block.info_name, block.scope
                                    )));
                                }
                                ("".to_string(), None)
                            }
                        }
                    }
                    None => {
                        if block.include_info {
                            return Err(MeshError::InvalidQuery(format!(
                                "Originator Entity Mapping for \
                                {} is missing with scope {}",
                                block.entity_name, block.scope
                            )));
                        }
                        ("".to_string(), None)
                    }
                },
                None => {
                    return Err(MeshError::InvalidQuery(format!(
                        "InfoSubstitution scope {} is not found \
                        in OriginatorMappings!",
                        block.scope
                    )))
                }
            }
        }
        None => (block.info_name.clone(), None),
    };
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn substitute_and_transform_info(
    mut replaced: String,
    info_key: &String,
    left_capture: &String,
    right_capture: &String,
    scoped_alias_mappings: &Option<ScopedOriginatorMappings>,
    transformed_info_sql: String,
    block: &InfoSubstitution,
    field: &DataField,
) -> Result<String> {
    let pattern = format!("{left_capture}{info_key}{right_capture}");

    let (orig_alias, orig_transform) =
        get_originator_alias_and_transform(scoped_alias_mappings, block)?;
    let field_and_info_sql = if block.include_data_field & block.include_info {
        // Here we compose physical data model -> local data model -> originator data model
        let orig_transformed_info_sql = if let Some(t) = orig_transform {
            t.local_info_to_other
                .replace(&t.replace_from, &format!("({transformed_info_sql})"))
        } else {
            transformed_info_sql
        };

        if block.exclude_info_alias {
            format!("{}, {}", field.path, orig_transformed_info_sql)
        } else {
            format!(
                "{}, {} as {}",
                field.path, orig_transformed_info_sql, orig_alias
            )
        }
    } else if block.include_data_field {
        field.path.to_string()
    } else if block.include_info {
        // Here we compose physical data model -> local data model -> originator data model
        let orig_transformed_info_sql = if let Some(t) = orig_transform {
            t.local_info_to_other
                .replace(&t.replace_from, &transformed_info_sql)
        } else {
            transformed_info_sql
        };
        if block.exclude_info_alias {
            orig_transformed_info_sql
        } else {
            format!("{} as {}", orig_transformed_info_sql, orig_alias)
        }
    } else {
        return Err(MeshError::InvalidQuery(format!("Information Substitution must have at least one of include_data_field or include info set to true for {pattern}")));
    };
    replaced = replaced.replace(pattern.as_str(), &field_and_info_sql);
    Ok(replaced)
}

fn apply_null_substitution(
    alias_mappings: &Option<ScopedOriginatorMappings>,
    block: &InfoSubstitution,
    replaced: String,
    pattern: String,
) -> Result<String> {
    let (orig_alias, _) = get_originator_alias_and_transform(alias_mappings, block)?;
    let null_replacement = if block.exclude_info_alias {
        "NULL".to_string()
    } else {
        format!("NULL as {orig_alias}")
    };

    Ok(replaced.replace(&pattern, &null_replacement))
}

fn apply_info_substitutions(
    mut replaced: String,
    left_capture: &String,
    right_capture: &String,
    info_substitutions: &HashMap<String, InfoSubstitution>,
    info_map_lookup: &HashMap<(&str, &str), (&DataField, &Mapping)>,
    alias_mappings: &Option<ScopedOriginatorMappings>,
    permission: &SourcePermission,
) -> Result<String> {
    for (info_key, block) in info_substitutions.iter() {
        let lookup_key = (block.entity_name.as_str(), block.info_name.as_str());
        let (field, map) = match info_map_lookup.get(&lookup_key) {
            Some(info_map) => info_map,
            None => {
                // if the requested DataField is not mapped, then we replace it with a literal null.
                let pattern = format!("{left_capture}{info_key}{right_capture}");
                replaced = apply_null_substitution(alias_mappings, block, replaced, pattern)?;
                continue;
            }
        };

        let transform = &map.transformation;
        let transformed_info_sql = transform
            .other_to_local_info
            .replace(&transform.replace_from, &field.path);

        if permission.columns.allowed_columns.contains(&field.path) {
            replaced = substitute_and_transform_info(
                replaced,
                info_key,
                left_capture,
                right_capture,
                alias_mappings,
                transformed_info_sql,
                block,
                field,
            )?;
        } else {
            // if the requested DataField is disallowed, then we replace it with a literal null.
            let pattern = format!("{left_capture}{info_key}{right_capture}");
            replaced = apply_null_substitution(alias_mappings, block, replaced, pattern)?;
        }
    }
    Ok(replaced)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use uuid::Uuid;

    use crate::{
        error::Result,
        execute::map_local::{apply_info_substitutions, apply_source_substitutions},
        model::{
            access_control::{ColumnPermission, RowPermission, SourcePermission},
            data_stores::{
                options::{trino::TrinoSource, SourceOptions},
                DataField, DataSource,
            },
            mappings::{Mapping, Transformation},
            query::{
                InfoSubstitution, OriginatorEntityMapping, OriginatorInfoMapping,
                OriginatorMappings, ScopedOriginatorMappings, SourceSubstitution,
            },
        },
    };

    /// Values which typically would come from application state (the database), but
    /// for the purposes of unit tests, we just make up some values.
    type SomeValues = (
        String,
        String,
        String,
        HashMap<String, InfoSubstitution>,
        HashMap<String, SourceSubstitution>,
        DataSource,
        DataField,
        Mapping,
        ScopedOriginatorMappings,
    );

    fn make_up_some_values() -> SomeValues {
        let replaced = "select {info} from {source} where {info2}=0.1".to_string();
        let left_capture = "{".to_string();
        let right_capture = "}".to_string();

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

        let source = DataSource {
            id: Uuid::new_v4(),
            name: "test".to_string(),
            source_sql: "test".to_string(),
            data_connection_id: Uuid::new_v4(),
            source_options: SourceOptions::Trino(TrinoSource {}),
        };
        let field = DataField {
            id: Uuid::new_v4(),
            name: "field".to_string(),
            data_source_id: Uuid::new_v4(),
            path: "field_path".to_string(),
        };
        let map = Mapping {
            information_id: Uuid::new_v4(),
            data_field_id: Uuid::new_v4(),
            transformation: Transformation {
                other_to_local_info: "{v}/10".to_string(),
                local_info_to_other: "{v}*10".to_string(),
                replace_from: "{v}".to_string(),
            },
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

        (
            replaced,
            left_capture,
            right_capture,
            info_substitutions,
            source_substitutions,
            source,
            field,
            map,
            scoped_alias_mapping,
        )
    }

    #[test]
    fn test_simple_info_substitution() -> Result<()> {
        let (
            replaced,
            left_capture,
            right_capture,
            info_substitution,
            _source_substitution,
            _source,
            field,
            map,
            _alias_mapping,
        ) = make_up_some_values();

        let permission = SourcePermission {
            columns: ColumnPermission {
                allowed_columns: HashSet::from_iter(vec!["field_path".to_string()]),
            },
            rows: RowPermission {
                allowed_rows: "col1 is not null".to_string(),
            },
        };

        let mut info_map_lookup = HashMap::new();
        info_map_lookup.insert(("test", "test"), (&field, &map));
        let replaced = apply_info_substitutions(
            replaced,
            &left_capture,
            &right_capture,
            &info_substitution,
            &info_map_lookup,
            &None,
            &permission,
        )?;
        assert_eq!(
            replaced,
            "select field_path, \
        field_path/10 as test \
         from {source} \
         where field_path=0.1"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_multihop_info_substitution() -> Result<()> {
        let (
            replaced,
            left_capture,
            right_capture,
            info_substitution,
            _source_substitution,
            _source,
            field,
            map,
            alias_mapping,
        ) = make_up_some_values();

        let permission = SourcePermission {
            columns: ColumnPermission {
                allowed_columns: HashSet::from_iter(vec!["field_path".to_string()]),
            },
            rows: RowPermission {
                allowed_rows: "col1 is not null".to_string(),
            },
        };

        let mut info_map_lookup = HashMap::new();
        info_map_lookup.insert(("test", "test"), (&field, &map));
        let replaced = apply_info_substitutions(
            replaced,
            &left_capture,
            &right_capture,
            &info_substitution,
            &info_map_lookup,
            &Some(alias_mapping),
            &permission,
        )?;
        assert_eq!(
            replaced,
            "select field_path, \
            (field_path/10)*100 as test_orig \
             from {source} \
             where field_path=0.1"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_simple_source_substitution() -> Result<()> {
        let (
            replaced,
            left_capture,
            right_capture,
            _info_substitution,
            source_substitution,
            source,
            _field,
            _map,
            _alias_mapping,
        ) = make_up_some_values();

        let permission = SourcePermission {
            columns: ColumnPermission {
                allowed_columns: HashSet::from_iter(vec!["field_path".to_string()]),
            },
            rows: RowPermission {
                allowed_rows: "col1 is not null".to_string(),
            },
        };
        let replaced = apply_source_substitutions(
            replaced,
            &left_capture,
            &right_capture,
            &source,
            &source_substitution,
            &permission,
        );
        assert_eq!(
            replaced,
            "select {info} \
        from (select field_path from (test) where col1 is not null) \
        where {info2}=0.1"
                .to_string()
        );
        Ok(())
    }

    #[test]
    fn test_map_sql() -> Result<()> {
        let (
            replaced,
            left_capture,
            right_capture,
            info_substitution,
            source_substitution,
            source,
            field,
            map,
            alias_mapping,
        ) = make_up_some_values();

        let permission = SourcePermission {
            columns: ColumnPermission {
                allowed_columns: HashSet::from_iter(vec!["field_path".to_string()]),
            },
            rows: RowPermission {
                allowed_rows: "col1 is not null".to_string(),
            },
        };
        let replaced = apply_source_substitutions(
            replaced,
            &left_capture,
            &right_capture,
            &source,
            &source_substitution,
            &permission,
        );

        let mut info_map_lookup = HashMap::new();
        info_map_lookup.insert(("test", "test"), (&field, &map));
        let replaced = apply_info_substitutions(
            replaced,
            &left_capture,
            &right_capture,
            &info_substitution,
            &info_map_lookup,
            &Some(alias_mapping.clone()),
            &permission,
        )?;

        assert_eq!(
            replaced,
            "select field_path, \
        (field_path/10)*100 as test_orig \
        from (select field_path from (test) where col1 is not null) \
        where field_path=0.1"
                .to_string()
        );

        Ok(())
    }
}
