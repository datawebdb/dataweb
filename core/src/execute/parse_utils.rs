use std::collections::HashMap;

use datafusion::sql::sqlparser::{
    ast::{
        visit_expressions_mut, Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr,
        Statement, TableFactor, TableWithJoins,
    },
    dialect::AnsiDialect,
    parser::Parser,
};

use crate::error::{MeshError, Result};

use super::visit_table_factor_mut;

static DIALECT: AnsiDialect = AnsiDialect {};

pub(crate) fn parse_sql_as_expr(sql: &str) -> Result<Expr> {
    let mut parser = Parser::new(&DIALECT).try_with_sql(sql)?;
    Ok(parser.parse_expr()?)
}

pub(crate) fn null_lit_expr() -> Expr {
    Expr::Value(datafusion::sql::sqlparser::ast::Value::Null)
}

pub(crate) fn parse_sql_as_table_factor(sql: &str) -> Result<TableFactor> {
    let mut parser = Parser::new(&DIALECT).try_with_sql(&format!("({sql})"))?;
    Ok(parser.parse_table_factor()?)
}

/// Creates a simple SELECT query with only projections and filters
pub(crate) fn projected_filtered_query(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
) -> Query {
    datafusion::sql::sqlparser::ast::Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: vec![],
            selection,
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
        limit_by: vec![],
        for_clause: None,
    }
}

pub(crate) fn parse_sql_as_identifiers(sql: &str) -> Result<Vec<Ident>> {
    let mut parser = Parser::new(&DIALECT).try_with_sql(sql)?;
    Ok(parser.parse_identifiers()?)
}

/// Parses a column identifier in a SELECT statment into a SelectItem
/// E.g. "column.name"
pub(crate) fn iden_str_to_select_item(iden: &str) -> Result<SelectItem> {
    let mut idens = parse_sql_as_identifiers(iden)?;
    let expr = if idens.is_empty() {
        return Err(MeshError::InvalidQuery(format!(
            "Failed to parse column identifier {iden}"
        )));
    } else if idens.len() == 1 {
        Expr::Identifier(idens.remove(0))
    } else {
        Expr::CompoundIdentifier(idens)
    };
    Ok(SelectItem::UnnamedExpr(expr))
}

/// Rewrites [Statement] replacing all TableFactor::Table instances with the provided new_table
pub(crate) fn substitute_table_factor(
    statement: &mut Statement,
    new_table: TableFactor,
) -> Result<()> {
    visit_table_factor_mut(statement, |table| {
        if let TableFactor::Table { alias, .. } = table {
            // Subsitute in the alias for the table being replaced into the inner derived table
            *table = match &new_table {
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

/// Checks if the passed expression is a column identifier which is a piece of information about the passed
/// entity_name. If so, returns the portion of the iden that refers to the information.
///
/// Expressions are assumed to be normalized by logical planning into a two part identifier like
/// `Entity Name`.`Info Name`.
fn maybe_extract_info<'a>(expr: &'a Expr, entity_name: &'a str) -> Option<&'a String> {
    match &expr {
        Expr::CompoundIdentifier(idens) => {
            if idens.len() != 2 {
                return None;
            }
            match (idens.first(), idens.get(1)) {
                (Some(ent), Some(info)) => {
                    if ent.value == entity_name {
                        Some(&info.value)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Rewrites [Statement] replacing all Expr::CompoundIdentifiers for the passed entity_name.
/// These are assumed to be normalized by logical planning into a two part identifier like
/// `Entity Name`.`Info Name`.
pub(crate) fn apply_col_iden_mapping(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, String>,
    entity_name: &str,
) -> Result<()> {
    let r = visit_expressions_mut(statement, |expr| {
        let info_name = match maybe_extract_info(expr, entity_name) {
            Some(i) => i,
            None => return std::ops::ControlFlow::Continue(()),
        };

        let transformed_info_sql = match info_map_lookup.get(info_name.as_str()) {
            Some(info_map) => info_map,
            None => {
                *expr = null_lit_expr();
                return std::ops::ControlFlow::Continue(());
            }
        };

        match parse_sql_as_expr(transformed_info_sql) {
            Ok(transformed_expr) => *expr = transformed_expr,
            Err(e) => return std::ops::ControlFlow::Break(Err(e)),
        };

        std::ops::ControlFlow::Continue(())
    });

    // Raise error if traversal short circuited with an error
    if let std::ops::ControlFlow::Break(e) = r {
        return e;
    }

    Ok(())
}
