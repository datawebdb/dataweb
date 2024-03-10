use std::collections::HashMap;

use crate::error::Result;

use crate::model::mappings::{RemoteEntityMapping, RemoteInfoMapping};

use datafusion::sql::sqlparser::ast::Statement;
use tracing::debug;

use super::parse_utils::{
    apply_aliases, apply_col_iden_mapping, parse_sql_as_table_factor, substitute_table_factor
};

/// Substitutes appropriate [Entity][crate::model::entity::Entity] and
/// [Information][crate::model::entity::Information] names for a specific remote relay
/// for a [SubstitutionBlocks] object sent to the local relay. Also applies relevant
/// [Transformations][crate::model::mappings::Transformation] to the request SQL.
pub(crate) fn map_remote_request(
    mut statement: Statement,
    entity_name: &str,
    entity_map: &RemoteEntityMapping,
    info_map_lookup: &HashMap<&str, &RemoteInfoMapping>,
) -> Result<Statement> {

    apply_source_substitutions(&mut statement, entity_map)?;
    apply_aliases(&mut statement, entity_name)?;
    apply_info_substitutions(&mut statement, info_map_lookup, entity_name)?;

    Ok(statement)
}

fn apply_source_substitutions(
    statement: &mut Statement,
    source: &RemoteEntityMapping,
) -> Result<()> {
    let source_sql = &source.sql;
    debug!("Substituting {source_sql} for remote");
    let remote_table = parse_sql_as_table_factor(source_sql)?;

    substitute_table_factor(statement, remote_table)?;
    Ok(())
}

fn apply_info_substitutions(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, &RemoteInfoMapping>,
    entity_name: &str,
) -> Result<()> {
    let filtered_map = info_map_lookup
        .iter()
        .map(|(info, map)| {
            let transform = &map.transformation;
            (
                *info,
                transform
                    .other_to_local_info
                    .replace(&transform.replace_from, &map.info_mapped_name),
            )
        })
        .collect::<HashMap<_, _>>();

    apply_col_iden_mapping(statement, &filtered_map, entity_name)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::execute::planning::EntityContext;

    use crate::model::mappings::{RemoteEntityMapping, RemoteInfoMapping, Transformation};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::sql::planner::SqlToRel;
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    use datafusion_sql_writer::from_df_plan;
    use uuid::Uuid;

    use crate::error::{MeshError, Result};

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

        let context_provider = EntityContext::new("entityname", schema);
        let sql_to_rel = SqlToRel::new(&context_provider);
        let logical_plan = sql_to_rel.sql_statement_to_plan(statement)?;
        let mut statement = from_df_plan(&logical_plan, Arc::new(dialect))?;

        println!("Round trip statement: {statement}");

        apply_source_substitutions(
            &mut statement,
            &RemoteEntityMapping {
                id: Uuid::new_v4(),
                sql: "select * from test".to_string(),
                relay_id: Uuid::new_v4(),
                entity_id: Uuid::new_v4(),
                remote_entity_name: "test".to_string(),
            },
        )?;

        println!("Post sub statement {statement}");

        assert_eq!(
            statement.to_string(),
            "SELECT `entityname`.`foo`, `entityname`.`bar` FROM (SELECT * FROM test)".to_string()
        );

        Ok(())
    }

    #[test]
    fn test_info_substitution() -> Result<()> {
        let sql = "SELECT `entityname`.`foo`, `entityname`.`bar` FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')";
        let dialect = GenericDialect {};

        let mut ast = Parser::parse_sql(&dialect, &sql)
            .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

        let mut statement = ast.remove(0);

        let remote_foo_map = RemoteInfoMapping {
            remote_entity_mapping_id: Uuid::new_v4(),
            information_id: Uuid::new_v4(),
            info_mapped_name: "remote_info".to_string(),
            transformation: Transformation {
                other_to_local_info: "{v}/100".to_string(),
                replace_from: "{v}".to_string(),
            },
        };

        let info_map_lookup = HashMap::from_iter(vec![("foo", &remote_foo_map)]);

        apply_info_substitutions(&mut statement, &info_map_lookup, "entityname")?;

        println!("Post sub statement {statement}");

        assert_eq!(
            statement.to_string() ,
            "SELECT remote_info / 100, NULL FROM (SELECT alias1.col1, col2 FROM (SELECT * FROM test) WHERE col1 = '123')".to_string()
        );

        Ok(())
    }
}
