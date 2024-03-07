use crate::crud::PgDb;
use crate::error::Result;

use crate::execute::utils::information_to_schema;
use crate::model::query::RawQueryRequest;

use crate::error::MeshError;

use datafusion::sql::planner::SqlToRel;
use datafusion::sql::sqlparser::ast::{
    visit_relations, Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, ListAggOnOverflow,
    Select, SelectItem, SetExpr, Statement, TableFactor, WindowFrameBound, WindowSpec, WindowType,
};
use datafusion::sql::sqlparser::dialect::AnsiDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_federation_sql::query_to_sql;

use super::planning::EntityContext;

static MAX_QUERY_LENGTH: usize = 1_000_000;

/// Uses sqlparser-rs to impose constraints on the provided sql.
pub fn validate_sql(sql: &str) -> Result<(String, Statement)> {
    if sql.len() > MAX_QUERY_LENGTH {
        return Err(MeshError::InvalidQuery(format!(
            "SQL string exceeds maximum length of {MAX_QUERY_LENGTH} characters! \
            Either simplify query or break into multiple parts."
        ),
        ));
    }

    let dialect = AnsiDialect {};

    let mut ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

    if ast.len() != 1 {
        return Err(MeshError::InvalidQuery(format!(
            "Each query must contain exactly one statement. Found: {}",
            ast.len()
        )));
    }

    let statement = ast.remove(0);

    match &statement {
        Statement::Query(q) => validate_query_statement(q)?,
        _ => {
            return Err(MeshError::InvalidQuery(format!(
                "SQL templates may only contain read-only \
            queries (e.g. select statements), found statement: {statement}"
            )))
        }
    }

    let entity = get_entity_for_statement(&statement)?;
    

    Ok((entity, statement))
}

/// Uses datafusion to logically plan the [Statement], ultimately converting back to
/// a [Statement] which has alias names resolved, columns fully qualified, ect.
pub fn logical_round_trip(statement: Statement, context: EntityContext) -> Result<Statement>{
    let sql_to_rel = SqlToRel::new(&context);
    let logical_plan = sql_to_rel.sql_statement_to_plan(statement)?;
    Ok(query_to_sql(&logical_plan)?)
}

/// Each [Statement] should only reference a single [Entity]. Verifies this is the case
/// and returns the name of that Entity.
fn get_entity_for_statement(statement: &Statement) -> Result<String> {
    let mut entities = vec![];
    visit_relations(statement, |relation| {
        let entity = relation.to_string();
        if !entities.contains(&entity) {
            entities.push(entity);
        }
        std::ops::ControlFlow::<()>::Continue(())
    });

    if entities.len() != 1 {
        return Err(MeshError::InvalidQuery(
            "There must be exactly one entity per query.".to_string(),
        ));
    }

    Ok(entities.remove(0))
}

fn validate_expr(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Identifier(_) => (),
        Expr::CompoundIdentifier(_) => (),
        Expr::JsonAccess { left, right, .. } => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::CompositeAccess { .. } => {
            return Err(MeshError::InvalidQuery(
                "Composite access query expressions are not allowed".into(),
            ))
        }
        Expr::IsFalse(inner) => validate_expr(inner.as_ref())?,
        Expr::IsNotFalse(inner) => validate_expr(inner.as_ref())?,
        Expr::IsNotTrue(inner) => validate_expr(inner.as_ref())?,
        Expr::IsTrue(inner) => validate_expr(inner.as_ref())?,
        Expr::IsNotNull(inner) => validate_expr(inner.as_ref())?,
        Expr::IsNull(inner) => validate_expr(inner.as_ref())?,
        Expr::IsUnknown(inner) => validate_expr(inner.as_ref())?,
        Expr::IsNotUnknown(inner) => validate_expr(inner.as_ref())?,
        Expr::IsDistinctFrom(left, right) => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::IsNotDistinctFrom(left, right) => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::InList { expr, list, .. } => {
            validate_expr(expr.as_ref())?;
            for list_inner in list.iter() {
                validate_expr(list_inner)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            validate_expr(expr.as_ref())?;
            validate_query_statement(subquery.as_ref())?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            validate_expr(expr.as_ref())?;
            validate_expr(array_expr.as_ref())?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr.as_ref())?;
            validate_expr(low.as_ref())?;
            validate_expr(high.as_ref())?;
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr(expr.as_ref())?;
            validate_expr(pattern.as_ref())?;
        }
        Expr::ILike { expr, pattern, .. } => {
            validate_expr(expr.as_ref())?;
            validate_expr(pattern.as_ref())?;
        }
        Expr::SimilarTo { expr, pattern, .. } => {
            validate_expr(expr.as_ref())?;
            validate_expr(pattern.as_ref())?;
        }
        Expr::AnyOp { left, right, .. } => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::AllOp { left, right, .. } => {
            validate_expr(left.as_ref())?;
            validate_expr(right.as_ref())?;
        }
        Expr::UnaryOp { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::Cast { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::TryCast { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::SafeCast { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::AtTimeZone { timestamp, .. } => validate_expr(timestamp.as_ref())?,
        Expr::Extract { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::Ceil { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::Floor { expr, .. } => validate_expr(expr.as_ref())?,
        Expr::Position { expr, r#in } => {
            validate_expr(expr.as_ref())?;
            validate_expr(r#in.as_ref())?;
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_expr(expr.as_ref())?;
            if let Some(from_expr) = substring_from {
                validate_expr(from_expr.as_ref())?;
            }
            if let Some(for_expr) = substring_for {
                validate_expr(for_expr.as_ref())?;
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_where: _,
            ..
        } => {
            validate_expr(expr.as_ref())?;
            if let Some(what_expr) = trim_what {
                validate_expr(what_expr.as_ref())?;
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            validate_expr(expr.as_ref())?;
            validate_expr(overlay_what.as_ref())?;
            validate_expr(overlay_from.as_ref())?;
            if let Some(for_expr) = overlay_for {
                validate_expr(for_expr.as_ref())?;
            }
        }
        Expr::Collate { .. } => {
            return Err(MeshError::InvalidQuery(
                "collation expressions are not allowed".to_string(),
            ))
        }
        Expr::Nested(inner) => validate_expr(inner.as_ref())?,
        Expr::Value(_) => (),
        Expr::IntroducedString { .. } => {
            return Err(MeshError::InvalidQuery(
                "introduced string expressions are not allowed".to_string(),
            ))
        }
        Expr::TypedString { .. } => (),
        Expr::MapAccess { column, keys } => {
            validate_expr(column.as_ref())?;
            for key_expr in keys.iter() {
                validate_expr(key_expr)?;
            }
        }
        Expr::Function(fun) => {
            for arg in fun.args.iter() {
                match arg {
                    FunctionArg::Named { arg, .. } => match arg {
                        FunctionArgExpr::Expr(expr) => validate_expr(expr)?,
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                    FunctionArg::Unnamed(arg) => match arg {
                        FunctionArgExpr::Expr(expr) => validate_expr(expr)?,
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                }
            }

            if let Some(window) = &fun.over {
                match &window {
                    WindowType::WindowSpec(spec) => validate_window_spec(spec)?,
                    WindowType::NamedWindow(_) => (),
                }
            }

            for order_by in fun.order_by.iter() {
                validate_expr(&order_by.expr)?;
            }
        }
        Expr::AggregateExpressionWithFilter { expr, filter } => {
            validate_expr(expr.as_ref())?;
            validate_expr(filter.as_ref())?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                validate_expr(expr.as_ref())?;
            }

            for expr in conditions.iter() {
                validate_expr(expr)?;
            }

            for expr in results.iter() {
                validate_expr(expr)?;
            }

            if let Some(expr) = else_result {
                validate_expr(expr.as_ref())?;
            }
        }
        Expr::Exists { subquery, .. } => validate_query_statement(subquery.as_ref())?,
        Expr::Subquery(subquery) => validate_query_statement(subquery.as_ref())?,
        Expr::ArraySubquery(subquery) => validate_query_statement(subquery.as_ref())?,
        Expr::ListAgg(listagg) => {
            validate_expr(&listagg.expr)?;
            if let Some(expr) = &listagg.separator {
                validate_expr(expr.as_ref())?;
            }
            for orderby in &listagg.within_group {
                validate_expr(&orderby.expr)?;
            }
            if let Some(overflow) = &listagg.on_overflow {
                match &overflow {
                    ListAggOnOverflow::Error => (),
                    ListAggOnOverflow::Truncate { filler, .. } => {
                        if let Some(expr) = filler {
                            validate_expr(expr)?;
                        }
                    }
                }
            }
        }
        Expr::ArrayAgg(arrayagg) => {
            validate_expr(&arrayagg.expr)?;
            if let Some(orderbys) = &arrayagg.order_by {
                for orderby in orderbys.iter() {
                    validate_expr(&orderby.expr)?;
                }
            }
            if let Some(expr) = &arrayagg.limit {
                validate_expr(expr.as_ref())?;
            }
        }
        Expr::GroupingSets(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr)?;
                }
            }
        }
        Expr::Cube(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr)?;
                }
            }
        }
        Expr::Rollup(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr)?;
                }
            }
        }
        Expr::Tuple(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr)?;
            }
        }
        Expr::ArrayIndex { obj, indexes } => {
            validate_expr(obj.as_ref())?;
            for expr in indexes.iter() {
                validate_expr(expr)?;
            }
        }
        Expr::Array(arr) => {
            for expr in arr.elem.iter() {
                validate_expr(expr)?;
            }
        }
        Expr::Interval(interval) => {
            validate_expr(interval.value.as_ref())?;
        }
        Expr::MatchAgainst { .. } => {
            return Err(MeshError::InvalidQuery(
                "MatchAgainst query expressions are not allowed".into(),
            ))
        }
        Expr::RLike {
            negated: _,
            expr: _,
            pattern: _,
            regexp: _,
        } => todo!(),
        Expr::Convert {
            expr: _,
            data_type: _,
            charset: _,
            target_before_value: _,
        } => todo!(),
        Expr::Struct {
            values: _,
            fields: _,
        } => todo!(),
        Expr::Named { expr: _, name: _ } => todo!(),
    }
    Ok(())
}

fn validate_window_frame_bound(
    bound: &WindowFrameBound,
    
) -> Result<()> {
    match &bound {
        WindowFrameBound::Preceding(Some(expr)) => validate_expr(expr)?,
        WindowFrameBound::Following(Some(expr)) => validate_expr(expr)?,
        WindowFrameBound::CurrentRow
        | WindowFrameBound::Preceding(None)
        | WindowFrameBound::Following(None) => (),
    }

    Ok(())
}

fn validate_window_spec(spec: &WindowSpec) -> Result<()> {
    for expr in spec.partition_by.iter() {
        validate_expr(expr)?;
    }

    for orderby in spec.order_by.iter() {
        validate_expr(&orderby.expr)?;
    }

    if let Some(frame) = &spec.window_frame {
        validate_window_frame_bound(&frame.start_bound)?;
        if let Some(end_bound) = &frame.end_bound {
            validate_window_frame_bound(end_bound)?;
        }
    }

    Ok(())
}

fn validate_select_setexpr(select: &Select) -> Result<()> {
    if let Some(distinct) = &select.distinct {
        match distinct {
            Distinct::Distinct => (),
            Distinct::On(exprs) => {
                for expr in exprs.iter() {
                    validate_expr(expr)?;
                }
            }
        }
    }

    if let Some(top) = &select.top {
        if let Some(expr) = &top.quantity {
            validate_expr(expr)?;
        }
    }

    for proj in select.projection.iter() {
        match proj {
            SelectItem::UnnamedExpr(expr) => validate_expr(expr)?,
            SelectItem::ExprWithAlias { expr, .. } => validate_expr(expr)?,
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
                validate_query_statement(subquery)?;
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
        validate_expr(expr)?;
    }

    match &select.group_by {
        GroupByExpr::All => (),
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr)?;
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
        validate_expr(expr)?;
    }

    if let Some(expr) = &select.having {
        validate_expr(expr)?;
    }

    for window in select.named_window.iter() {
        validate_window_spec(&window.1)?;
    }

    if select.qualify.is_some() {
        return Err(MeshError::InvalidQuery(
            "Qualify clause is not allowed!".to_string(),
        ));
    }

    Ok(())
}

fn validate_query_body(body: &SetExpr) -> Result<()> {
    match body {
        SetExpr::Select(s) => validate_select_setexpr(s.as_ref())?,
        SetExpr::Query(q) => validate_query_statement(q.as_ref())?,
        SetExpr::SetOperation { left, right, .. } => {
            validate_query_body(left.as_ref())?;
            validate_query_body(right.as_ref())?;
        }
        SetExpr::Values(values) => {
            for exprs in values.rows.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr)?;
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
/// any constraint is violated. Called via [logical_round_trip].
fn validate_query_statement(
    query: &datafusion::sql::sqlparser::ast::Query,
    
) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in with.cte_tables.iter() {
            validate_query_statement(&cte.query)?;
        }
    }
    validate_query_body(query.body.as_ref())?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use arrow_schema::Schema;

    use crate::error::Result;
    use crate::execute::validation::validate_sql;
    use crate::model::query::RawQueryRequest;

    use super::logical_round_trip;

    #[test]
    fn insert_into_test() -> Result<()> {
        let sql = "insert into user_tables values (1,2)".to_string();

        let raw_request = RawQueryRequest {
            sql,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql(&raw_request.sql)
            .expect_err("Query should have failed validation!")
            .to_string();

        assert_eq!("invalid query: SQL templates may only contain read-only queries (e.g. select statements), \
        found statement: INSERT INTO user_tables VALUES (1, 2)", err_msg);
        Ok(())
    }

    #[test]
    fn escape_query_test() -> Result<()> {
        let sql = "with user_tables as (select * from user_tables) select * from user_tables; select * from user_tables".to_string();

        let raw_request = RawQueryRequest {
            sql,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql(&raw_request.sql)
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

        let raw_request = RawQueryRequest {
            sql,
            request_uuid: None,
            requesting_user: None,
            originating_relay: None,
            originating_task_id: None,
            return_arrow_schema: Some(Schema::empty()),
        };

        let err_msg = validate_sql(&raw_request.sql)
            .expect_err("Query should have failed validation!")
            .to_string();

        assert_eq!(
            "invalid query: SQL string exceeds maximum length of \
        1000000 characters! Either simplify query or break into multiple parts.",
            err_msg
        );
        Ok(())
    }
}
