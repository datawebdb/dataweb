use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{Statement, TableFactor, TableWithJoins};

use crate::error::Result;

use crate::model::access_control::SourcePermission;

use crate::{
    error::MeshError,
    model::{
        data_stores::{DataField, DataSource},
        mappings::Mapping,
    },
};

use super::parse_utils::{
    apply_aliases, apply_col_iden_mapping, iden_str_to_select_item, parse_sql_as_expr, parse_sql_as_table_factor, projected_filtered_query, substitute_table_factor
};

/// Substitutes appropriate table names and fields for a specific source
/// into an ast
pub(crate) fn map_sql(
    mut statement: Statement,
    entity_name: &str,
    source: &DataSource,
    info_map_lookup: &HashMap<&str, (&DataField, &Mapping)>,
    permission: SourcePermission,
) -> Result<Statement> {
    apply_source_substitutions(&mut statement, source, &permission)?;
    apply_aliases(&mut statement, entity_name)?;
    apply_info_substitutions(&mut statement, info_map_lookup, &permission, entity_name)?;

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

    substitute_table_factor(statement, controlled_table)?;
    Ok(())
}

fn apply_info_substitutions(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, (&DataField, &Mapping)>,
    permission: &SourcePermission,
    entity_name: &str,
) -> Result<()> {
    // Apply permission to info mappings to filter out disallowed columns
    let allowed_cols = &permission.columns.allowed_columns;
    let filtered_map = info_map_lookup
        .iter()
        .filter_map(|(info, (df, map))| {
            let col = &df.path;
            if allowed_cols.get(col).is_some() {
                let transform = &map.transformation;
                Some((
                    *info,
                    transform
                        .other_to_local_info
                        .replace(&transform.replace_from, col),
                ))
            } else {
                None
            }
        })
        .collect::<HashMap<_, _>>();

    apply_col_iden_mapping(statement, &filtered_map, entity_name)?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use crate::execute::planning::EntityContext;
    use crate::execute::validation::logical_round_trip;
    use crate::model::access_control::{ColumnPermission, RowPermission, SourcePermission};
    use crate::model::data_stores::options::SourceOptions;
    use crate::model::data_stores::DataField;
    use crate::model::mappings::{Mapping, Transformation};
    use arrow_schema::{DataType, Field, Schema};

    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    use uuid::Uuid;

    use crate::{
        error::{MeshError, Result},
        model::data_stores::{options::trino::TrinoSource, DataSource},
    };

    use super::{apply_info_substitutions, apply_source_substitutions};

    #[test]
    fn test_source_substitution() -> Result<()> {
        let sql = "select foo, bar from (select * from entityname);";
        let dialect = GenericDialect {};

        let mut ast = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

        let statement = ast.remove(0);

        let schema = Arc::new(Schema::new(vec![
            Field::new("foo", DataType::Utf8, false),
            Field::new("bar", DataType::UInt8, false),
        ]));

        let context = EntityContext::new("entityname", schema);
        let (mut statement, _) = logical_round_trip(statement, context)?;

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
                        vec!["alias1.col1"].iter().map(|s| s.to_string()),
                    ),
                },
                rows: RowPermission {
                    allowed_rows: "col1='123'".to_string(),
                },
            },
        )?;

        println!("Post sub statement {statement}");

        assert_eq!(
            statement.to_string(),
            concat!(
            r#"SELECT "entityname"."foo", "entityname"."bar" "#, 
            r#"FROM (SELECT "entityname"."foo", "entityname"."bar" "#, 
            r#"FROM (SELECT alias1.col1 FROM (SELECT * FROM test) WHERE col1 = '123'))"#)
        );

        Ok(())
    }

    #[test]
    fn test_info_substitution() -> Result<()> {
        let sql = "SELECT \"entityname\".\"foo\", \"entityname\".\"bar\" FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')";
        let dialect = GenericDialect {};

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
