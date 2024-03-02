use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{
    visit_expressions_mut, Expr, Statement, TableFactor, TableWithJoins,
};

use crate::error::Result;

use crate::model::access_control::SourcePermission;

use crate::model::query::{InfoSubstitution, ScopedOriginatorMappings};

use crate::{
    error::MeshError,
    model::{
        data_stores::{DataConnection, DataField, DataSource},
        entity::{Entity, Information},
        mappings::Mapping,
    },
};

use super::parse_utils::{
    iden_str_to_select_item, null_lit_expr, parse_sql_as_expr, parse_sql_as_table_factor,
    projected_filtered_query,
};
use super::visit_table_factor_mut;

/// Substitutes appropriate table names and fields for a specific source
/// into an ast
pub(crate) fn map_sql(
    mut statement: Statement,
    _con: &DataConnection,
    source: &DataSource,
    mappings: &[(Entity, Information, DataField, Mapping)],
    permission: SourcePermission,
) -> Result<Statement> {
    apply_source_substitutions(&mut statement, source, &permission)?;

    let entity_name = &mappings[0].0.name;
    let mut info_map_lookup = HashMap::with_capacity(mappings.len());
    for (_, info, field, map) in mappings.iter() {
        info_map_lookup.insert(info.name.as_str(), (field, map));
    }

    apply_info_substitutions(&mut statement, &info_map_lookup, &permission, entity_name)?;

    Ok(statement)
}

/// Applies the [SourcePermission] to [TableFactor] returning a new [TableFactor] which only allows
/// access to the specified columns and rows.
fn apply_source_permission(
    table: TableFactor,
    permission: &SourcePermission,
) -> Result<TableFactor> {
    let selection = parse_sql_as_expr(&permission.rows.allowed_rows)?;

    let alias = match &table {
        TableFactor::Derived { alias, .. } => alias.clone(),
        TableFactor::Table { alias, .. } => alias.clone(),
        _ => {
            return Err(MeshError::InvalidQuery(format!(
                "Table Facetor {table} is not supported."
            )))
        }
    };

    let projection = permission
        .columns
        .allowed_columns
        .iter()
        .map(|c| iden_str_to_select_item(c))
        .collect::<Result<Vec<_>>>()?;

    let from = vec![TableWithJoins {
        relation: table,
        joins: vec![],
    }];

    let permission_subquery = projected_filtered_query(projection, from, Some(selection));
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
    let local_table = parse_sql_as_table_factor(source_sql)?;

    let controlled_table = apply_source_permission(local_table, permission)?;

    visit_table_factor_mut(statement, |table| {
        if let TableFactor::Table { alias, .. } = table {
            // Subsitute in the alias for the table being replaced into the inner derived table
            *table = match &controlled_table {
                TableFactor::Derived {
                    lateral, subquery, ..
                } => TableFactor::Derived {
                    lateral: *lateral,
                    subquery: subquery.clone(),
                    alias: alias.clone(),
                },
                _ => unreachable!(),
            };
        };
        std::ops::ControlFlow::<()>::Continue(())
    });
    Ok(())
}

pub(crate) fn substitute_and_transform_info(
    _replaced: String,
    _info_key: &str,
    _left_capture: &str,
    _right_capture: &str,
    _scoped_alias_mappings: &Option<ScopedOriginatorMappings>,
    _transformed_info_sql: String,
    _block: &InfoSubstitution,
    _field: &DataField,
) -> Result<String> {
    //deleteme
    todo!()
}

fn apply_info_substitutions(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, (&DataField, &Mapping)>,
    permission: &SourcePermission,
    entity_name: &str,
) -> Result<()> {
    let r = visit_expressions_mut(statement, |expr| {
        let info_name = match &expr {
            Expr::CompoundIdentifier(idens) => {
                if idens.len() != 2 {
                    return std::ops::ControlFlow::Continue(());
                }
                match (idens.first(), idens.get(1)) {
                    (Some(ent), Some(info)) => {
                        if ent.value == entity_name {
                            &info.value
                        } else {
                            return std::ops::ControlFlow::Continue(());
                        }
                    }
                    _ => return std::ops::ControlFlow::Continue(()),
                }
            }
            _ => return std::ops::ControlFlow::Continue(()),
        };

        let (field, map) = match info_map_lookup.get(info_name.as_str()) {
            Some(info_map) => info_map,
            None => {
                *expr = null_lit_expr();
                return std::ops::ControlFlow::Continue(());
            }
        };

        let transform = &map.transformation;
        let transformed_info_sql = transform
            .other_to_local_info
            .replace(&transform.replace_from, &field.path);

        if permission.columns.allowed_columns.contains(&field.path) {
            // Short circuit on error and immediately return error
            match parse_sql_as_expr(&transformed_info_sql) {
                Ok(transformed_expr) => *expr = transformed_expr,
                Err(e) => return std::ops::ControlFlow::Break(Err(e)),
            };
        } else {
            *expr = null_lit_expr();
        }

        std::ops::ControlFlow::Continue(())
    });

    // Raise error if traversal short circuited with an error
    if let std::ops::ControlFlow::Break(e) = r {
        return e;
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use crate::execute::planning::EntityContext;
    use crate::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
    use crate::model::data_stores::options::SourceOptions;
    use crate::model::data_stores::DataField;
    use crate::model::mappings::{Mapping, Transformation};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::sql::planner::SqlToRel;
    use datafusion::sql::sqlparser::{dialect::AnsiDialect, parser::Parser};
    use datafusion_federation_sql::query_to_sql;
    use uuid::Uuid;

    use crate::{
        error::{MeshError, Result},
        model::data_stores::{options::trino::TrinoSource, DataSource},
    };

    use super::{apply_info_substitutions, apply_source_substitutions};

    #[test]
    fn test_source_substitution() -> Result<()> {
        let sql = "select foo, bar from (select * from entityname);";
        let dialect = AnsiDialect {};

        let mut ast = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

        let statement = ast.remove(0);

        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Utf8, false),
            Field::new("bar", DataType::UInt8, false),
        ]));

        let context_provider = EntityContext::new("entityname", schema);
        let sql_to_rel = SqlToRel::new(&context_provider);
        let logical_plan = sql_to_rel.sql_statement_to_plan(statement)?;
        let mut statement = query_to_sql(&logical_plan)?;

        println!("Round trip statement: {statement}");

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

        println!("Post sub statement {statement}");

        assert!(
            (statement.to_string() ==
            "SELECT `entityname`.`foo`, `entityname`.`bar` FROM (SELECT col2, alias1.col1 FROM (SELECT * FROM test) WHERE col1 = '123')".to_string())
            ||
            (statement.to_string() ==
            "SELECT `entityname`.`foo`, `entityname`.`bar` FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_info_substitution() -> Result<()> {
        let sql = "SELECT `entityname`.`foo`, `entityname`.`bar` FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')";
        let dialect = AnsiDialect {};

        let mut ast = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

        let mut statement = ast.remove(0);

        let foo_field = DataField {
            id: Uuid::new_v4(),
            name: "foo".to_string(),
            data_source_id: Uuid::new_v4(),
            path: "field.path".to_string(),
        };

        let foo_map = Mapping {
            information_id: Uuid::new_v4(),
            data_field_id: Uuid::new_v4(),
            transformation: Transformation {
                other_to_local_info: "{v}/100".to_string(),
                local_info_to_other: "{v}*100".to_string(),
                replace_from: "{v}".to_string(),
            },
        };

        let info_map_lookup = HashMap::from_iter(vec![("foo", (&foo_field, &foo_map))]);

        apply_info_substitutions(
            &mut statement,
            &info_map_lookup,
            &SourcePermission {
                columns: ColumnPermission {
                    allowed_columns: HashSet::from_iter(
                        vec!["field.path"].iter().map(|s| s.to_string()),
                    ),
                },
                rows: RowPermission {
                    allowed_rows: "col1='123'".to_string(),
                },
            },
            "entityname",
        )?;

        println!("Post sub statement {statement}");

        assert_eq!(
            statement.to_string() ,
            "SELECT field.path / 100, NULL FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')".to_string()
        );

        Ok(())
    }
}
