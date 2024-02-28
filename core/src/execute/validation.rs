use std::collections::HashSet;

use crate::error::Result;

use crate::model::query::RawQueryRequest;

use crate::error::MeshError;

use datafusion::sql::sqlparser::ast::{
    Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, ListAggOnOverflow, Select,
    SelectItem, SetExpr, Statement, TableFactor, WindowFrameBound, WindowSpec, WindowType,
};
use datafusion::sql::sqlparser::dialect::AnsiDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// Uses sqlparser-rs to impose constraints on the provided sql template.
/// For example, specifying a raw table name instead of using a
/// [SourceSubstitution][crate::model::query::SourceSubstitution] is disallowed as this would enable crafting
/// queries to bypass access controls.
pub fn validate_sql_template(raw_request: &RawQueryRequest) -> Result<Statement> {
    let sql = &raw_request.sql;

    if sql.len() > 1_000_000 {
        return Err(MeshError::InvalidQuery(
            "SQL query template string exceeds maximum length of 1,000,000 characters! \
            Either simplify query or break into multiple parts."
                .into(),
        ));
    }

    let dialect = AnsiDialect {};

    let mut ast = Parser::parse_sql(&dialect, &sql)
        .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

    if ast.len() != 1 {
        return Err(MeshError::InvalidQuery(format!(
            "Each query must contain exactly one statement. Found: {}",
            ast.len()
        )));
    }

    let statement = ast.remove(0);

    match &statement {
        Statement::Query(q) => {
            validate_query_statement(q, raw_request)?
        }
        _ => {
            return Err(MeshError::InvalidQuery(format!(
                "SQL templates may only contain read-only \
            queries (e.g. select statements), found statement: {statement}"
            )))
        }
    }
    

    Ok(statement)
}

fn validate_expr(
    expr: &Expr,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    match expr {
        Expr::Identifier(_) => (),
        Expr::CompoundIdentifier(_) => (),
        Expr::JsonAccess { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::CompositeAccess { .. } => {
            return Err(MeshError::InvalidQuery(
                "Composite access query expressions are not allowed".into(),
            ))
        }
        Expr::IsFalse(inner) => validate_expr(inner.as_ref(), raw_request)?,
        Expr::IsNotFalse(inner) => {
            validate_expr(inner.as_ref(), raw_request)?
        }
        Expr::IsNotTrue(inner) => {
            validate_expr(inner.as_ref(), raw_request)?
        }
        Expr::IsTrue(inner) => validate_expr(inner.as_ref(), raw_request)?,
        Expr::IsNotNull(inner) => {
            validate_expr(inner.as_ref(), raw_request)?
        }
        Expr::IsNull(inner) => validate_expr(inner.as_ref(), raw_request)?,
        Expr::IsUnknown(inner) => {
            validate_expr(inner.as_ref(), raw_request)?
        }
        Expr::IsNotUnknown(inner) => {
            validate_expr(inner.as_ref(), raw_request)?
        }
        Expr::IsDistinctFrom(left, right) => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::IsNotDistinctFrom(left, right) => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::InList { expr, list, .. } => {
            validate_expr(expr.as_ref(), raw_request)?;
            for list_inner in list.iter() {
                validate_expr(list_inner, raw_request)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_query_statement(
                subquery.as_ref(),
                raw_request,
            )?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(array_expr.as_ref(), raw_request)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(low.as_ref(), raw_request)?;
            validate_expr(high.as_ref(), raw_request)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(pattern.as_ref(), raw_request)?;
        }
        Expr::ILike { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(pattern.as_ref(), raw_request)?;
        }
        Expr::SimilarTo { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(pattern.as_ref(), raw_request)?;
        }
        Expr::AnyOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::AllOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request)?;
            validate_expr(right.as_ref(), raw_request)?;
        }
        Expr::UnaryOp { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::Cast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::TryCast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::SafeCast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::AtTimeZone { timestamp, .. } => {
            validate_expr(timestamp.as_ref(), raw_request)?
        }
        Expr::Extract { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::Ceil { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::Floor { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request)?
        }
        Expr::Position { expr, r#in } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(r#in.as_ref(), raw_request)?;
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_expr(expr.as_ref(), raw_request)?;
            if let Some(from_expr) = substring_from {
                validate_expr(from_expr.as_ref(), raw_request)?;
            }
            if let Some(for_expr) = substring_for {
                validate_expr(for_expr.as_ref(), raw_request)?;
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_where: _,
            ..
        } => {
            validate_expr(expr.as_ref(), raw_request)?;
            if let Some(what_expr) = trim_what {
                validate_expr(what_expr.as_ref(), raw_request)?;
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(overlay_what.as_ref(), raw_request)?;
            validate_expr(overlay_from.as_ref(), raw_request)?;
            if let Some(for_expr) = overlay_for {
                validate_expr(for_expr.as_ref(), raw_request)?;
            }
        }
        Expr::Collate { .. } => {
            return Err(MeshError::InvalidQuery(
                "collation expressions are not allowed".to_string(),
            ))
        }
        Expr::Nested(inner) => validate_expr(inner.as_ref(), raw_request)?,
        Expr::Value(_) => (),
        Expr::IntroducedString { .. } => {
            return Err(MeshError::InvalidQuery(
                "introduced string expressions are not allowed".to_string(),
            ))
        }
        Expr::TypedString { .. } => (),
        Expr::MapAccess { column, keys } => {
            validate_expr(column.as_ref(), raw_request)?;
            for key_expr in keys.iter() {
                validate_expr(key_expr, raw_request)?;
            }
        }
        Expr::Function(fun) => {
            for arg in fun.args.iter() {
                match arg {
                    FunctionArg::Named { arg, .. } => match arg {
                        FunctionArgExpr::Expr(expr) => {
                            validate_expr(expr, raw_request)?
                        }
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                    FunctionArg::Unnamed(arg) => match arg {
                        FunctionArgExpr::Expr(expr) => {
                            validate_expr(expr, raw_request)?
                        }
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                }
            }

            if let Some(window) = &fun.over {
                match &window {
                    WindowType::WindowSpec(spec) => {
                        validate_window_spec(spec, raw_request)?
                    }
                    WindowType::NamedWindow(_) => (),
                }
            }

            for order_by in fun.order_by.iter() {
                validate_expr(&order_by.expr, raw_request)?;
            }
        }
        Expr::AggregateExpressionWithFilter { expr, filter } => {
            validate_expr(expr.as_ref(), raw_request)?;
            validate_expr(filter.as_ref(), raw_request)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                validate_expr(expr.as_ref(), raw_request)?;
            }

            for expr in conditions.iter() {
                validate_expr(expr, raw_request)?;
            }

            for expr in results.iter() {
                validate_expr(expr, raw_request)?;
            }

            if let Some(expr) = else_result {
                validate_expr(expr.as_ref(), raw_request)?;
            }
        }
        Expr::Exists { subquery, .. } => validate_query_statement(
            subquery.as_ref(),
            raw_request,
        )?,
        Expr::Subquery(subquery) => validate_query_statement(
            subquery.as_ref(),
            raw_request,
        )?,
        Expr::ArraySubquery(subquery) => validate_query_statement(
            subquery.as_ref(),
            raw_request,
        )?,
        Expr::ListAgg(listagg) => {
            validate_expr(&listagg.expr, raw_request)?;
            if let Some(expr) = &listagg.separator {
                validate_expr(expr.as_ref(), raw_request)?;
            }
            for orderby in &listagg.within_group {
                validate_expr(&orderby.expr, raw_request)?;
            }
            if let Some(overflow) = &listagg.on_overflow {
                match &overflow {
                    ListAggOnOverflow::Error => (),
                    ListAggOnOverflow::Truncate { filler, .. } => {
                        if let Some(expr) = filler {
                            validate_expr(expr, raw_request)?;
                        }
                    }
                }
            }
        }
        Expr::ArrayAgg(arrayagg) => {
            validate_expr(&arrayagg.expr, raw_request)?;
            if let Some(orderbys) = &arrayagg.order_by {
                for orderby in orderbys.iter() {
                    validate_expr(&orderby.expr, raw_request)?;
                }
            }
            if let Some(expr) = &arrayagg.limit {
                validate_expr(expr.as_ref(), raw_request)?;
            }
        }
        Expr::GroupingSets(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request)?;
                }
            }
        }
        Expr::Cube(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request)?;
                }
            }
        }
        Expr::Rollup(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request)?;
                }
            }
        }
        Expr::Tuple(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr, raw_request)?;
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            validate_expr(obj.as_ref(), raw_request)?;
            for expr in indexes.iter() {
                validate_expr(expr, raw_request)?;
            }
        }
        Expr::Array(arr) => {
            for expr in arr.elem.iter() {
                validate_expr(expr, raw_request)?;
            }
        }
        Expr::Interval(interval) => {
            validate_expr(interval.value.as_ref(), raw_request)?;
        }
        Expr::MatchAgainst { .. } => {
            return Err(MeshError::InvalidQuery(
                "MatchAgainst query expressions are not allowed".into(),
            ))
        }
    }
    Ok(())
}

fn validate_window_frame_bound(
    bound: &WindowFrameBound,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    match &bound {
        WindowFrameBound::Preceding(Some(expr)) => {
            validate_expr(expr, raw_request)?
        }
        WindowFrameBound::Following(Some(expr)) => {
            validate_expr(expr, raw_request)?
        }
        WindowFrameBound::CurrentRow
        | WindowFrameBound::Preceding(None)
        | WindowFrameBound::Following(None) => (),
    }

    Ok(())
}

fn validate_window_spec(
    spec: &WindowSpec,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    for expr in spec.partition_by.iter() {
        validate_expr(expr, raw_request)?;
    }

    for orderby in spec.order_by.iter() {
        validate_expr(&orderby.expr, raw_request)?;
    }

    if let Some(frame) = &spec.window_frame {
        validate_window_frame_bound(&frame.start_bound, raw_request)?;
        if let Some(end_bound) = &frame.end_bound {
            validate_window_frame_bound(end_bound, raw_request)?;
        }
    }

    Ok(())
}

fn validate_select_setexpr(
    select: &Select,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    if let Some(distinct) = &select.distinct {
        match distinct {
            Distinct::Distinct => (),
            Distinct::On(exprs) => {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request)?;
                }
            }
        }
    }

    if let Some(top) = &select.top {
        if let Some(expr) = &top.quantity {
            validate_expr(expr, raw_request)?;
        }
    }

    for proj in select.projection.iter() {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                validate_expr(expr, raw_request)?
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                validate_expr(expr, raw_request)?
            }
            SelectItem::Wildcard(_) => (),
            SelectItem::QualifiedWildcard(_, _) => (),
        }
    }

    if select.into.is_some() {
        return Err(MeshError::InvalidQuery(
            "SELECT INTO is not supported!".into(),
        ));
    }

    for table in select.from.iter() {
        match &table.relation {
            TableFactor::Table { name, args, .. } => {
                if args.is_some() {
                    return Err(MeshError::InvalidQuery(
                        "Table valued functions are not allowed!".into(),
                    ));
                }
                if name.0.len() > 1 {
                    return Err(MeshError::InvalidQuery(format!(
                        "Explicit table paths are not allowed! Found: {}",
                        name
                    )));
                }

            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias: _,
            } => {
                if *lateral {
                    return Err(MeshError::NotImplemented(
                        "Lateral subqueries are not supported".to_owned(),
                    ));
                }
                validate_query_statement(subquery, raw_request)?;
            }
            _ => {
                return Err(MeshError::InvalidQuery(format!(
                    "only explicit source substitutions are allowed but found {}",
                    table.relation
                )))
            }
        }
    }

    if !&select.lateral_views.is_empty() {
        return Err(MeshError::InvalidQuery(
            "Lateral views are not allowed!".to_string(),
        ));
    }

    if let Some(expr) = &select.selection {
        validate_expr(expr, raw_request)?;
    }

    match &select.group_by {
        GroupByExpr::All => (),
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr, raw_request)?;
            }
        }
    }

    if !select.cluster_by.is_empty() {
        return Err(MeshError::InvalidQuery(
            "Cluster by clause is not allowed!".to_string(),
        ));
    }

    if !select.distribute_by.is_empty() {
        return Err(MeshError::InvalidQuery(
            "Distribute by clause is not allowed!".to_string(),
        ));
    }

    for expr in &select.sort_by {
        validate_expr(expr, raw_request)?;
    }

    if let Some(expr) = &select.having {
        validate_expr(expr, raw_request)?;
    }

    for window in select.named_window.iter() {
        validate_window_spec(&window.1, raw_request)?;
    }

    if select.qualify.is_some() {
        return Err(MeshError::InvalidQuery(
            "Qualify clause is not allowed!".to_string(),
        ));
    }

    Ok(())
}

fn validate_query_body(
    body: &SetExpr,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    match body {
        SetExpr::Select(s) => {
            validate_select_setexpr(s.as_ref(), raw_request)?
        }
        SetExpr::Query(q) => {
            validate_query_statement(q.as_ref(), raw_request)?
        }
        SetExpr::SetOperation { left, right, .. } => {
            validate_query_body(left.as_ref(), raw_request)?;
            validate_query_body(right.as_ref(), raw_request)?;
        }
        SetExpr::Values(values) => {
            for exprs in values.rows.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request)?;
                }
            }
        }
        _ => {
            return Err(MeshError::InvalidQuery(format!(
                "query body {} is not allowed",
                body
            )))
        }
    }
    Ok(())
}

/// Processes a single Statement::Query recursively, returning an Err if
/// any constraint is violated. Called via [validate_sql_template].
fn validate_query_statement(
    query: &datafusion::sql::sqlparser::ast::Query,
    raw_request: &RawQueryRequest,
) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in with.cte_tables.iter() {
            validate_query_statement(&cte.query, raw_request)?;
        }
    }
    validate_query_body(query.body.as_ref(), raw_request)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::Schema;
    use uuid::Uuid;

    use crate::error::Result;
    use crate::model::query::{
        InfoSubstitution, RawQueryRequest, SourceSubstitution, SubstitutionBlocks,
    };

    use super::validate_sql_template;

    #[test]
    fn insert_into_test() -> Result<()> {
        let sql = "insert into user_tables values (1,2)".to_string();

        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_tables".to_string(),
            SourceSubstitution::AllSourcesWith(vec!["test".to_string()]),
        );

        let substitution_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions,
            num_capture_braces: 1,
        };
        let raw_request = RawQueryRequest {
            sql,
            substitution_blocks,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            originator_mappings: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql_template(&raw_request)
            .expect_err("Query should have failed validation!")
            .to_string();

        assert_eq!("invalid query: SQL templates may only contain read-only queries (e.g. select statements), \
        found statement: INSERT INTO user_tables VALUES (1, 2)", err_msg);
        Ok(())
    }

    #[test]
    fn escape_query_test() -> Result<()> {
        let sql = "with user_tables as (select * from user_tables) select * from user_tables; select * from user_tables".to_string();

        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_tables".to_string(),
            SourceSubstitution::AllSourcesWith(vec!["test".to_string()]),
        );

        let substitution_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions,
            num_capture_braces: 1,
        };
        let raw_request = RawQueryRequest {
            sql,
            substitution_blocks,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            originator_mappings: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql_template(&raw_request)
            .expect_err("Query should have failed validation!")
            .to_string();

        assert_eq!(
            "invalid query: Each query must contain exactly one statement. Found: 2",
            err_msg
        );
        Ok(())
    }

    #[test]
    fn max_query_length_test() -> Result<()> {
        let statement = "select * from table;".to_string();
        let mut sql = statement.clone();
        for _ in 0..50000 {
            sql.insert_str(0, statement.as_str());
        }

        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_tables".to_string(),
            SourceSubstitution::AllSourcesWith(vec!["test".to_string()]),
        );

        let substitution_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions,
            num_capture_braces: 1,
        };
        let raw_request = RawQueryRequest {
            sql,
            substitution_blocks,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            originator_mappings: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql_template(&raw_request)
            .expect_err("Query should have failed validation!")
            .to_string();

        assert_eq!(
            "invalid query: SQL query template string exceeds maximum length of \
        1,000,000 characters! Either simplify query or break into multiple parts.",
            err_msg
        );
        Ok(())
    }
}
