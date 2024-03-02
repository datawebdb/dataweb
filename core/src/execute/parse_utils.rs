use datafusion::sql::sqlparser::{ast::{Expr, GroupByExpr, Ident, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins}, dialect::AnsiDialect, parser::Parser};

use crate::error::{MeshError, Result};

static DIALECT: AnsiDialect = AnsiDialect {};

pub(crate) fn parse_sql_as_expr(
    sql: &str,
) -> Result<Expr> {

    let mut parser = Parser::new(&DIALECT)
        .try_with_sql(&sql)?;
    Ok(parser
        .parse_expr()?)
}

pub(crate) fn null_lit_expr() -> Expr{
    Expr::Value(datafusion::sql::sqlparser::ast::Value::Null)
}

pub(crate) fn parse_sql_as_table_factor(
    sql: &str
) -> Result<TableFactor>{
    let mut parser = Parser::new(&DIALECT)
        .try_with_sql(&format!("({sql})"))?;
    Ok(parser
        .parse_table_factor()?)
}

/// Creates a simple SELECT query with only projections and filters
pub(crate) fn projected_filtered_query(
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
) -> Query{
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

pub(crate) fn parse_sql_as_identifiers(sql: &str) -> Result<Vec<Ident>>{
    let mut parser = Parser::new(&DIALECT).try_with_sql(sql)?;
    Ok(parser.parse_identifiers()?)
}


/// Parses a column identifier in a SELECT statment into a SelectItem
/// E.g. "column.name"
pub(crate) fn iden_str_to_select_item(iden: &str) -> Result<SelectItem>{
    let mut idens = parse_sql_as_identifiers(iden)?;
    let expr = if idens.len() == 0 {
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