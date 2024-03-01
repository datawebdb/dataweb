use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{
    visit_expressions_mut, visit_statements_mut, Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, WildcardAdditionalOptions
};
use datafusion::sql::sqlparser::ast::{visit_relations, visit_relations_mut};
use datafusion::sql::sqlparser::dialect::AnsiDialect;
use datafusion::sql::sqlparser::parser::Parser;
use itertools::Itertools;
use object_store::local;
use tracing::debug;

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
    apply_source_substitutions(&mut statement, source, &permission)?;

    let mut info_map_lookup = HashMap::with_capacity(mappings.len());
    for (entity, info, field, map) in mappings.iter() {
        info_map_lookup.insert(info.name.as_str(), (field, map));
    }

    apply_info_substitutions(
        &mut statement,
        &info_map_lookup,
        alias_mappings,
        &permission,
    )?;

    Ok(statement)
}

/// Applies the [SourcePermission] to [TableFactor] returning a new [TableFactor] which only allows
/// access to the specified columns and rows.
fn apply_source_permission(
    table: TableFactor,
    permission: &SourcePermission,
) -> Result<TableFactor> {
    let mut parser = Parser::new(&AnsiDialect {}).try_with_sql(&permission.rows.allowed_rows)?;
    let allowed_rows_expr = parser.parse_expr()?;

    let alias = match &table {
        TableFactor::Derived { alias, .. } => alias.clone(),
        TableFactor::Table { alias, .. } => alias.clone(),
        _ => {
            return Err(MeshError::InvalidQuery(format!(
                "Table Facetor {table} is not supported."
            )))
        }
    };

    let permission_subquery = datafusion::sql::sqlparser::ast::Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: permission
                .columns
                .allowed_columns
                .iter()
                .map(|c| {
                    let mut parser = Parser::new(&AnsiDialect {}).try_with_sql(c)?;
                    let mut idens = parser.parse_identifiers()?;
                    let expr = if idens.len() == 0 {
                        return Err(MeshError::InvalidQuery(format!(
                            "Failed to parse column identifier {c}"
                        )));
                    } else if idens.len() == 1 {
                        Expr::Identifier(idens.remove(0))
                    } else {
                        Expr::CompoundIdentifier(idens)
                    };
                    Ok(SelectItem::UnnamedExpr(expr))
                })
                .collect::<Result<Vec<_>>>()?,
            into: None,
            from: vec![TableWithJoins {
                relation: table,
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: Some(allowed_rows_expr),
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
        }))),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
        locks: vec![],
    };

    let subquery = Box::new(permission_subquery);

    Ok(TableFactor::Derived {
        lateral: false,
        subquery,
        alias,
    })
}

/// Modifies [Statement] in place, rewriting references to an [Entity] to be in terms of the passed
/// [DataSource]
fn apply_source_substitutions(
    statement: &mut Statement,
    source: &DataSource,
    permission: &SourcePermission,
) -> Result<()> {
    let source_sql = &source.source_sql;
    let mut parser = Parser::new(&AnsiDialect {})
        .try_with_sql(&format!("({source_sql})"))?;
    let local_table = parser
        .parse_table_factor()?;

    let controlled_table = apply_source_permission(local_table, permission)?;

    visit_table_factor_mut(statement, |table| {
        match table {
            TableFactor::Table { alias, .. } => {
                let substitute_local = match &controlled_table {
                    TableFactor::Derived {
                        lateral, subquery, ..
                    } => TableFactor::Derived {
                        lateral: lateral.to_owned(),
                        subquery: subquery.clone(),
                        alias: alias.clone(),
                    },
                    TableFactor::Table {
                        name,
                        args,
                        with_hints,
                        version,
                        partitions,
                        ..
                    } => TableFactor::Table {
                        name: name.clone(),
                        args: args.clone(),
                        with_hints: with_hints.clone(),
                        alias: alias.clone(),
                        version: version.clone(),
                        partitions: partitions.clone(),
                    },
                    _ => todo!(),
                };
                *table = substitute_local;
            }
            _ => (),
        };
        std::ops::ControlFlow::<()>::Continue(())
    });
    Ok(())
}

fn get_originator_alias_and_transform<'a>(
    scoped_alias_mappings: &'a Option<ScopedOriginatorMappings>,
    info_name: &str,
    scope: &str,
) -> Result<(String, Option<&'a Transformation>)> {
    let out = match scoped_alias_mappings {
        Some(scoped_alias_mappings) => {
            // Here we are mapping our local info name back to the originator's info name.
            // Since the originator could be multiple hops away, we use the alias mappings included
            // in the request from our peered relay. Note that a single entity/information name may
            // have different mappings depending on the scope of the InfoSubstitution.
            match scoped_alias_mappings.inner.get(scope) {
                // The usage of .next() here is a hack exploiting the fact we have forced there
                // to only ever be one Entity in the originator mappings. This should be refactored
                // so that OriginatorEntityMapping level does not exist any more.
                Some(alias_map) => match alias_map.inner.values().next() {
                    Some(entity_map) => {
                        match entity_map.originator_info_map.get(info_name) {
                            Some(info_map) => {
                                let orig_alias = info_map.originator_info_name.clone();
                                let orig_transform = &info_map.transformation;
                                (orig_alias, Some(orig_transform))
                            }
                            None => {
                                return Err(MeshError::InvalidQuery(format!(
                                    "Originator Entity Mapping \
                                    is missing Info mapping for {} \
                                    with scope {}.",
                                    info_name, scope
                                )));

                            }
                        }
                    }
                    None => {
                        return Err(MeshError::InvalidQuery(format!(
                            "Originator Entity Mapping \
                            is missing with scope {}",
                            scope
                        )));
                    }
                },
                None => {
                    return Err(MeshError::InvalidQuery(format!(
                        "InfoSubstitution scope {} is not found \
                        in OriginatorMappings!",
                        scope
                    )))
                }
            }
        }
        None => (info_name.to_string(), None),
    };
    Ok(out)
}

pub(crate) fn substitute_and_transform_info(
    expr: &mut Expr,
    scoped_alias_mappings: &Option<ScopedOriginatorMappings>,
    transformed_info_sql: String,
    info_name: &str,
    scope: &str,
    field: &DataField,
) -> Result<()> {

    let (orig_alias, orig_transform) =
        get_originator_alias_and_transform(scoped_alias_mappings, info_name, scope)?;
    let expr_sql = 
    if let Some(t) = orig_transform {
            t.local_info_to_other
                .replace(&t.replace_from, &transformed_info_sql)
        } else {
            transformed_info_sql
    };

    let mut parser = Parser::new(&AnsiDialect {})
        .try_with_sql(&expr_sql)?;
    let transformed_expr = parser
        .parse_expr()?;

    *expr = transformed_expr;
    
    Ok(())
}

fn apply_null_substitution(
    alias_mappings: &Option<ScopedOriginatorMappings>,
    block: &InfoSubstitution,
    replaced: String,
    pattern: String,
) -> Result<String> {
    // let (orig_alias, _) = get_originator_alias_and_transform(alias_mappings, block)?;
    // let null_replacement = if block.exclude_info_alias {
    //     "NULL".to_string()
    // } else {
    //     format!("NULL as {orig_alias}")
    // };

    // Ok(replaced.replace(&pattern, &null_replacement))
    Ok("".to_string())
}

fn apply_info_substitutions(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, (&DataField, &Mapping)>,
    alias_mappings: &Option<ScopedOriginatorMappings>,
    permission: &SourcePermission,
) -> Result<()> {

    let r = visit_expressions_mut(statement, |expr| {
        let info_name = match &expr{
            Expr::Identifier(iden) => {
                iden.to_string()
            },
            Expr::CompoundIdentifier(idens) => {
                idens.iter()
                .join(".")
            },
            _ => return std::ops::ControlFlow::Continue(())
        };

        let (field, map) = match info_map_lookup.get(info_name.as_str()) {
            Some(info_map) => info_map,
            None => {
                // if the requested DataField is not mapped, then we replace it with a literal null.
                debug!("Info {info_name} is not mapped in query");
                //apply_null_substitution(alias_mappings, block, replaced, pattern)?;
                return std::ops::ControlFlow::Continue(())
            }
        };

        let transform = &map.transformation;
        let transformed_info_sql = transform
            .other_to_local_info
            .replace(&transform.replace_from, &field.path);

        if permission.columns.allowed_columns.contains(&field.path) {
            // Short circuit on error and immediately return error
            if let Err(e) = substitute_and_transform_info(
                expr, 
                alias_mappings, 
                transformed_info_sql, 
                info_name.as_str(), 
                "", 
                field){
                    return std::ops::ControlFlow::<Result<()>>::Break(Err(e))
                }
        } else {
            // if the requested DataField is disallowed, then we replace it with a literal null.
            //replaced = apply_null_substitution(alias_mappings, block, replaced, pattern)?;
        }

        std::ops::ControlFlow::Continue(())
    });

    // Raise error if traversal short circuited with an error
    if let std::ops::ControlFlow::Break(e) = r{
        return e
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use crate::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
    use crate::model::data_stores::options::SourceOptions;
    use datafusion::sql::sqlparser::{dialect::AnsiDialect, parser::Parser};
    use uuid::Uuid;

    use crate::{
        error::{MeshError, Result},
        execute::validation::validate_sql_template,
        model::data_stores::{options::trino::TrinoSource, DataSource},
    };

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
            &DataSource {
                id: Uuid::new_v4(),
                name: "test".to_string(),
                source_sql: "select * from test".to_string(),
                data_connection_id: Uuid::new_v4(),
                source_options: SourceOptions::Trino(TrinoSource {}),
            },
            &SourcePermission {
                columns: ColumnPermission {
                    allowed_columns: HashSet::from_iter(
                        vec!["alias1.col1", "col2"].iter().map(|s| s.to_string()),
                    ),
                },
                rows: RowPermission {
                    allowed_rows: "col1='123'".to_string(),
                },
            },
        )?;

        assert!(
            (statement.to_string() ==
            "SELECT foo, bar FROM (SELECT * FROM (SELECT col2, alias1.col1 FROM (SELECT * FROM test) WHERE col1 = '123'))".to_string())
            ||
            (statement.to_string() ==
            "SELECT foo, bar FROM (SELECT * FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123'))".to_string())
        );

        Ok(())
    }
}
