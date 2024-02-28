use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{GroupByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, WildcardAdditionalOptions};
use datafusion::sql::sqlparser::ast::{visit_relations, visit_relations_mut};
use datafusion::sql::sqlparser::dialect::AnsiDialect;
use datafusion::sql::sqlparser::parser::Parser;
use object_store::local;

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

use super::visit_table_factor_mut;

/// Substitutes appropriate table names and fields for a specific source
/// into an ast
pub(crate) fn map_sql(
    mut statement: Statement,
    _con: &DataConnection,
    source: &DataSource,
    mappings: &[(Entity, Information, DataField, Mapping)],
    alias_mappings: &Option<ScopedOriginatorMappings>,
    permission: SourcePermission,
) -> Result<Statement> {

    apply_source_substitutions(
        &mut statement,
        source,
        &permission,
    );

    // let mut info_map_lookup = HashMap::with_capacity(mappings.len());
    // for (entity, info, field, map) in mappings.iter() {
    //     info_map_lookup.insert((entity.name.as_str(), info.name.as_str()), (field, map));
    // }

    // replaced = apply_info_substitutions(
    //     replaced,
    //     &info_map_lookup,
    //     alias_mappings,
    //     &permission,
    // )?;

    Ok(statement)
}

/// Modifies [Statement] in place, rewriting references to an [Entity] to be in terms of the passed
/// [DataSource]
fn apply_source_substitutions(
    statement: &mut Statement,
    source: &DataSource,
    permission: &SourcePermission,
) -> Result<()>{
    let source_sql = &source.source_sql;
    let mut parser = Parser::new(&AnsiDialect {})
        .try_with_sql(&format!("({source_sql})"))
        .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;
    let local_table = parser.parse_table_factor()
        .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

    println!("LocalTable: {local_table:?}");

    visit_table_factor_mut(statement, |table| {
        match table{
            TableFactor::Table { alias, .. } => {
                let substitute_local = 
                match &local_table{
                    TableFactor::Derived { lateral, subquery, .. } => {
                        TableFactor::Derived { lateral: lateral.to_owned(), subquery: subquery.clone(), alias: alias.clone() }
                    },
                    TableFactor::Table { name, args, with_hints, version, partitions, .. } => {
                        TableFactor::Table {name: name.clone(), args: args.clone(), with_hints: with_hints.clone(), alias: alias.clone(), version: version.clone(), partitions: partitions.clone()} 
                    },
                    _ => todo!()
                };
                *table = substitute_local;
            },
            _ => (),
        };
        std::ops::ControlFlow::<()>::Continue(())
    });
    Ok(())
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

    use std::collections::HashSet;

    use datafusion::sql::sqlparser::{dialect::AnsiDialect, parser::Parser};
    use uuid::Uuid;
    use crate::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
    use crate::model::data_stores::options::SourceOptions;

    use crate::{error::{MeshError, Result}, execute::validation::validate_sql_template, model::data_stores::{options::trino::TrinoSource, DataSource}};

    use super::apply_source_substitutions;

    #[test]
    fn source_substitution_test() -> Result<()> {

        let sql = "select foo, bar from (select * from tablename);";
        let dialect = AnsiDialect {};

        let mut ast = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

        let mut statement = ast.remove(0);

        apply_source_substitutions(
            &mut statement, 
            &DataSource{
                id: Uuid::new_v4(),
                name: "test".to_string(),
                source_sql: "select * from test".to_string(),
                data_connection_id: Uuid::new_v4(),
                source_options: SourceOptions::Trino(TrinoSource{}),
            }, 
            &SourcePermission{ columns: ColumnPermission{
                allowed_columns: HashSet::new(),
            }, rows: RowPermission{ allowed_rows: "true".to_string() } })?;

        assert_eq!(
            statement.to_string(),
            "SELECT foo, bar FROM (SELECT * FROM (SELECT * FROM test))".to_string()
        );

        Ok(())
    }
}