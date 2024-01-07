use std::collections::HashSet;

use crate::error::Result;

use crate::model::query::RawQueryRequest;

use crate::error::MeshError;

use sqlparser::ast::{
    Distinct, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, ListAggOnOverflow, Select,
    SelectItem, SetExpr, Statement, TableFactor, WindowFrameBound, WindowSpec, WindowType,
};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

/// Uses sqlparser-rs to impose constraints on the provided sql template.
/// For example, specifying a raw table name instead of using a
/// [SourceSubstitution][crate::model::query::SourceSubstitution] is disallowed as this would enable crafting
/// queries to bypass access controls.
pub fn validate_sql_template(raw_request: &RawQueryRequest) -> Result<()> {
    let sql = &raw_request.sql;

    if sql.len() > 1_000_000 {
        return Err(MeshError::InvalidQuery(
            "SQL query template string exceeds maximum length of 1,000,000 characters! \
            Either simplify query or break into multiple parts."
                .into(),
        ));
    }
    let blocks = &raw_request.substitution_blocks;

    if blocks.source_substitutions.is_empty() {
        return Err(MeshError::InvalidQuery(
            "No source substitution provided for query!".into(),
        ));
    } else if blocks.source_substitutions.len() > 1 {
        return Err(MeshError::NotImplemented("Queries with logic spanning multiple sources (joins, subqueries) is not supported yet!".into()));
    }

    if blocks.num_capture_braces == 0 || blocks.num_capture_braces > 10 {
        return Err(MeshError::InvalidQuery(
            "Num capture braces must be between 1 and 10. E.g. a value of 3 \
            means capture groups are specified such as {{{capture_me}}}."
                .into(),
        ));
    }

    check_duplicate_keys(raw_request)?;

    let left_cap = regex::escape(&"{".repeat(blocks.num_capture_braces));
    let right_cap = regex::escape(&"}".repeat(blocks.num_capture_braces));

    let re = regex::Regex::new(format!("({left_cap}.*?{right_cap})").as_str()).unwrap();

    // This regex literally quotes all substitution inner values so that sqlparser will parse them
    // e.g. select * from {source} -> select * from "{source}" and in the AST
    // we will see "{source}" as a table name.
    let lit_quoted_sql = re.replace_all(sql, "\"$1\"");

    let dialect = AnsiDialect {};

    let ast = Parser::parse_sql(&dialect, &lit_quoted_sql)
        .map_err(|e| MeshError::InvalidQuery(format!("sqlparser syntax error: {e}")))?;

    if ast.len() != 1 {
        return Err(MeshError::InvalidQuery(format!(
            "SQL templates must contain exactly one statement. Found: {}",
            ast.len()
        )));
    }

    for statement in ast {
        match statement {
            Statement::Query(q) => {
                validate_query_statement(q.as_ref(), raw_request, HashSet::new())?
            }
            _ => {
                return Err(MeshError::InvalidQuery(format!(
                    "SQL templates may only contain read-only \
             queries (e.g. select statements), found statement: {statement}"
                )))
            }
        }
    }

    Ok(())
}

/// Checks if any of the [SubstitutionBlocks][crate::model::query::SubstitutionBlocks]
/// hashmaps share any duplicate keys. This is an error since it introduces ambiguity
/// over how a specific part of the SQL template should be treated.
fn check_duplicate_keys(raw_request: &RawQueryRequest) -> Result<()> {
    let info_subs = &raw_request.substitution_blocks.info_substitutions;
    let source_subs = &raw_request.substitution_blocks.source_substitutions;
    let mut key_set = HashSet::with_capacity(info_subs.len() + source_subs.len());
    for key in info_subs.keys().chain(source_subs.keys()) {
        if !key_set.insert(key) {
            return Err(MeshError::InvalidQuery(format!(
                "Found duplicate substitution key {key}"
            )));
        }
    }
    Ok(())
}

/// Every table name in a SQL template must either by a [SourceSubstitution][crate::model::query::SourceSubstitution]
/// or an alias to a SourceSubstitution. This function checks these two conditions for a given table name,
/// a set of all currently in scope aliases to existing SourceSubstitution, and a [RawQueryRequest].
fn validate_table_name(
    name: &str,
    raw_request: &RawQueryRequest,
    in_scope_table_aliases: &HashSet<String>,
) -> Result<()> {
    let err = Err(MeshError::InvalidQuery(format!("Found table identifier \"{name}\" which is neither \
    an in scope table alias nor a SourceSubstitution. Specifying table names directly is not allowed. \
    Use an explicit SourceList within a SourceSubstitution instead.")));

    if in_scope_table_aliases.contains(name) {
        return Ok(());
    }

    // If the table identifier is not an alias, it must be a valid SourceSubstitution:
    // 1. It must start and end with num_capture_braces i.e. {{name}}
    // 2. The remaining inner string must be a key in the source_subtitutions map
    // Otherwise, the query is invalid.
    let num_braces = raw_request.substitution_blocks.num_capture_braces;
    let mut char_vec: Vec<_> = name.chars().collect();
    let initial_len = char_vec.len();
    if initial_len < num_braces * 2 {
        return err;
    }
    for _ in 0..num_braces {
        let end_idx = char_vec.len() - 1;
        if !(char_vec.remove(end_idx) == '}' && char_vec.remove(0) == '{') {
            return err;
        }
    }

    let name_without_braces: String = char_vec.into_iter().collect();

    if raw_request
        .substitution_blocks
        .source_substitutions
        .contains_key(&name_without_braces)
    {
        return Ok(());
    }

    err
}

fn validate_expr(
    expr: &Expr,
    raw_request: &RawQueryRequest,
    in_scope_table_aliases: &HashSet<String>,
) -> Result<()> {
    match expr {
        Expr::Identifier(_) => (),
        Expr::CompoundIdentifier(_) => (),
        Expr::JsonAccess { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::CompositeAccess { .. } => {
            return Err(MeshError::InvalidQuery(
                "Composite access query expressions are not allowed".into(),
            ))
        }
        Expr::IsFalse(inner) => validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?,
        Expr::IsNotFalse(inner) => {
            validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::IsNotTrue(inner) => {
            validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::IsTrue(inner) => validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?,
        Expr::IsNotNull(inner) => {
            validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::IsNull(inner) => validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?,
        Expr::IsUnknown(inner) => {
            validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::IsNotUnknown(inner) => {
            validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::IsDistinctFrom(left, right) => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::IsNotDistinctFrom(left, right) => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::InList { expr, list, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            for list_inner in list.iter() {
                validate_expr(list_inner, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_query_statement(
                subquery.as_ref(),
                raw_request,
                in_scope_table_aliases.clone(),
            )?;
        }
        Expr::InUnnest {
            expr, array_expr, ..
        } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(array_expr.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(low.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(high.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(pattern.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::ILike { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(pattern.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::SimilarTo { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(pattern.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::RLike { expr, pattern, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(pattern.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::AnyOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::AllOp { left, right, .. } => {
            validate_expr(left.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(right.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::UnaryOp { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::Cast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::TryCast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::SafeCast { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::AtTimeZone { timestamp, .. } => {
            validate_expr(timestamp.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::Extract { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::Ceil { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::Floor { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::Position { expr, r#in } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(r#in.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            if let Some(from_expr) = substring_from {
                validate_expr(from_expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
            if let Some(for_expr) = substring_for {
                validate_expr(for_expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            if let Some(what_expr) = trim_what {
                validate_expr(what_expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
            if let Some(char_exprs) = trim_characters {
                for char_expr in char_exprs.iter() {
                    validate_expr(char_expr, raw_request, in_scope_table_aliases)?;
                }
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(overlay_what.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(overlay_from.as_ref(), raw_request, in_scope_table_aliases)?;
            if let Some(for_expr) = overlay_for {
                validate_expr(for_expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Collate { .. } => {
            return Err(MeshError::InvalidQuery(
                "collation expressions are not allowed".to_string(),
            ))
        }
        Expr::Nested(inner) => validate_expr(inner.as_ref(), raw_request, in_scope_table_aliases)?,
        Expr::Value(_) => (),
        Expr::IntroducedString { .. } => {
            return Err(MeshError::InvalidQuery(
                "introduced string expressions are not allowed".to_string(),
            ))
        }
        Expr::TypedString { .. } => (),
        Expr::MapAccess { column, keys } => {
            validate_expr(column.as_ref(), raw_request, in_scope_table_aliases)?;
            for key_expr in keys.iter() {
                validate_expr(key_expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Function(fun) => {
            for arg in fun.args.iter() {
                match arg {
                    FunctionArg::Named { arg, .. } => match arg {
                        FunctionArgExpr::Expr(expr) => {
                            validate_expr(expr, raw_request, in_scope_table_aliases)?
                        }
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                    FunctionArg::Unnamed(arg) => match arg {
                        FunctionArgExpr::Expr(expr) => {
                            validate_expr(expr, raw_request, in_scope_table_aliases)?
                        }
                        FunctionArgExpr::Wildcard => (),
                        FunctionArgExpr::QualifiedWildcard(_) => (),
                    },
                }
            }
            if let Some(filter) = &fun.filter {
                validate_expr(filter.as_ref(), raw_request, in_scope_table_aliases)?;
            }

            if let Some(window) = &fun.over {
                match &window {
                    WindowType::WindowSpec(spec) => {
                        validate_window_spec(spec, raw_request, in_scope_table_aliases)?
                    }
                    WindowType::NamedWindow(_) => (),
                }
            }

            for order_by in fun.order_by.iter() {
                validate_expr(&order_by.expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::AggregateExpressionWithFilter { expr, filter } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            validate_expr(filter.as_ref(), raw_request, in_scope_table_aliases)?;
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(expr) = operand {
                validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }

            for expr in conditions.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }

            for expr in results.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }

            if let Some(expr) = else_result {
                validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Exists { subquery, .. } => validate_query_statement(
            subquery.as_ref(),
            raw_request,
            in_scope_table_aliases.clone(),
        )?,
        Expr::Subquery(subquery) => validate_query_statement(
            subquery.as_ref(),
            raw_request,
            in_scope_table_aliases.clone(),
        )?,
        Expr::ArraySubquery(subquery) => validate_query_statement(
            subquery.as_ref(),
            raw_request,
            in_scope_table_aliases.clone(),
        )?,
        Expr::ListAgg(listagg) => {
            validate_expr(&listagg.expr, raw_request, in_scope_table_aliases)?;
            if let Some(expr) = &listagg.separator {
                validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
            for orderby in &listagg.within_group {
                validate_expr(&orderby.expr, raw_request, in_scope_table_aliases)?;
            }
            if let Some(overflow) = &listagg.on_overflow {
                match &overflow {
                    ListAggOnOverflow::Error => (),
                    ListAggOnOverflow::Truncate { filler, .. } => {
                        if let Some(expr) = filler {
                            validate_expr(expr, raw_request, in_scope_table_aliases)?;
                        }
                    }
                }
            }
        }
        Expr::ArrayAgg(arrayagg) => {
            validate_expr(&arrayagg.expr, raw_request, in_scope_table_aliases)?;
            if let Some(orderbys) = &arrayagg.order_by {
                for orderby in orderbys.iter() {
                    validate_expr(&orderby.expr, raw_request, in_scope_table_aliases)?;
                }
            }
            if let Some(expr) = &arrayagg.limit {
                validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::GroupingSets(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request, in_scope_table_aliases)?;
                }
            }
        }
        Expr::Cube(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request, in_scope_table_aliases)?;
                }
            }
        }
        Expr::Rollup(exprs) => {
            for exprs in exprs.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request, in_scope_table_aliases)?;
                }
            }
        }
        Expr::Tuple(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Struct { values, .. } => {
            for expr in values.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Named { expr, .. } => {
            validate_expr(expr.as_ref(), raw_request, in_scope_table_aliases)?
        }
        Expr::ArrayIndex { obj, indexes } => {
            validate_expr(obj.as_ref(), raw_request, in_scope_table_aliases)?;
            for expr in indexes.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Array(arr) => {
            for expr in arr.elem.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
            }
        }
        Expr::Interval(interval) => {
            validate_expr(interval.value.as_ref(), raw_request, in_scope_table_aliases)?;
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
    in_scope_table_aliases: &HashSet<String>,
) -> Result<()> {
    match &bound {
        WindowFrameBound::Preceding(Some(expr)) => {
            validate_expr(expr, raw_request, in_scope_table_aliases)?
        }
        WindowFrameBound::Following(Some(expr)) => {
            validate_expr(expr, raw_request, in_scope_table_aliases)?
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
    in_scope_table_aliases: &HashSet<String>,
) -> Result<()> {
    for expr in spec.partition_by.iter() {
        validate_expr(expr, raw_request, in_scope_table_aliases)?;
    }

    for orderby in spec.order_by.iter() {
        validate_expr(&orderby.expr, raw_request, in_scope_table_aliases)?;
    }

    if let Some(frame) = &spec.window_frame {
        validate_window_frame_bound(&frame.start_bound, raw_request, in_scope_table_aliases)?;
        if let Some(end_bound) = &frame.end_bound {
            validate_window_frame_bound(end_bound, raw_request, in_scope_table_aliases)?;
        }
    }

    Ok(())
}

fn validate_select_setexpr(
    select: &Select,
    raw_request: &RawQueryRequest,
    in_scope_table_aliases: &mut HashSet<String>,
) -> Result<()> {
    if let Some(distinct) = &select.distinct {
        match distinct {
            Distinct::Distinct => (),
            Distinct::On(exprs) => {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request, in_scope_table_aliases)?;
                }
            }
        }
    }

    if let Some(top) = &select.top {
        if let Some(expr) = &top.quantity {
            validate_expr(expr, raw_request, in_scope_table_aliases)?;
        }
    }

    for proj in select.projection.iter() {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                validate_expr(expr, raw_request, in_scope_table_aliases)?
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                validate_expr(expr, raw_request, in_scope_table_aliases)?
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
                let str_name = &name
                    .0
                    .first()
                    .ok_or(MeshError::InvalidQuery(
                        "Found empty table name!".to_string(),
                    ))?
                    .value;
                validate_table_name(str_name, raw_request, in_scope_table_aliases)?;
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    return Err(MeshError::NotImplemented(
                        "Lateral subqueries are not supported".to_owned(),
                    ));
                }
                validate_query_statement(subquery, raw_request, in_scope_table_aliases.clone())?;
                match alias {
                    Some(a) => in_scope_table_aliases.insert(a.name.to_string()),
                    _ => false,
                };
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
        validate_expr(expr, raw_request, in_scope_table_aliases)?;
    }

    match &select.group_by {
        GroupByExpr::All => (),
        GroupByExpr::Expressions(exprs) => {
            for expr in exprs.iter() {
                validate_expr(expr, raw_request, in_scope_table_aliases)?;
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
        validate_expr(expr, raw_request, in_scope_table_aliases)?;
    }

    if let Some(expr) = &select.having {
        validate_expr(expr, raw_request, in_scope_table_aliases)?;
    }

    for window in select.named_window.iter() {
        validate_window_spec(&window.1, raw_request, in_scope_table_aliases)?;
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
    in_scope_table_aliases: HashSet<String>,
) -> Result<()> {
    match body {
        SetExpr::Select(s) => {
            validate_select_setexpr(s.as_ref(), raw_request, &mut in_scope_table_aliases.clone())?
        }
        SetExpr::Query(q) => {
            validate_query_statement(q.as_ref(), raw_request, in_scope_table_aliases.clone())?
        }
        SetExpr::SetOperation { left, right, .. } => {
            validate_query_body(left.as_ref(), raw_request, in_scope_table_aliases.clone())?;
            validate_query_body(right.as_ref(), raw_request, in_scope_table_aliases.clone())?;
        }
        SetExpr::Values(values) => {
            for exprs in values.rows.iter() {
                for expr in exprs.iter() {
                    validate_expr(expr, raw_request, &in_scope_table_aliases)?;
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
    query: &sqlparser::ast::Query,
    raw_request: &RawQueryRequest,
    mut in_scope_table_aliases: HashSet<String>,
) -> Result<()> {
    if let Some(with) = &query.with {
        for cte in with.cte_tables.iter() {
            if let Some(from) = &cte.from {
                validate_table_name(&from.value, raw_request, &in_scope_table_aliases)?;
            }
            in_scope_table_aliases.insert(cte.alias.name.value.clone());
            validate_query_statement(&cte.query, raw_request, in_scope_table_aliases.clone())?;
        }
    }

    validate_query_body(query.body.as_ref(), raw_request, in_scope_table_aliases)?;

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
    fn block_cte_bypass() -> Result<()> {
        // Validate that we block literal table names.
        let sql =
            "with a as (select * from {user_tables}) select * from user_data_folder".to_string();
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

        assert_eq!("invalid query: Found table identifier \"user_data_folder\" which is neither an \
        in scope table alias nor a SourceSubstitution. Specifying table names directly is not allowed. \
        Use an explicit SourceList within a SourceSubstitution instead.", err_msg);

        Ok(())
    }

    #[test]
    fn correct_literal_table_by_substitution() -> Result<()> {
        // Validate specifying a literal table by source substitution works as expected
        let sql = "with a as (select * from {user_data_folder}) select * from {user_data_folder}"
            .to_string();
        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_data_folder".to_string(),
            SourceSubstitution::SourceList(vec![Uuid::new_v4()]),
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

        validate_sql_template(&raw_request)?;
        Ok(())
    }

    #[test]
    fn correct_literal_table_by_substitution_multi_brace() -> Result<()> {
        // If string literals need to use { or }, you can optionally use more than 1 to define capture group
        let sql = "with a as (select * from {{{{{user_data_folder}}}}}) select * from {{{{{user_data_folder}}}}} where '{'='{'"
            .to_string();
        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_data_folder".to_string(),
            SourceSubstitution::SourceList(vec![Uuid::new_v4()]),
        );
        let substitution_blocks = SubstitutionBlocks {
            info_substitutions: HashMap::new(),
            source_substitutions,
            num_capture_braces: 5,
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

        validate_sql_template(&raw_request)?;
        Ok(())
    }

    #[test]
    fn allow_valid_cte() -> Result<()> {
        // As long as user_data_folder is referencing an in scope table alias, the query is OK.
        let sql =
            "with user_data_folder as (select * from {user_tables}) select * from user_data_folder"
                .to_string();
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

        validate_sql_template(&raw_request)?;
        Ok(())
    }

    #[test]
    fn allow_valid_subquery() -> Result<()> {
        // As long as user_data_folder is referencing an in scope table alias, the query is OK.
        let sql =
            "with a as (select * from (select * from {user_tables}) b where col in (select col from b)) select * from a"
                .to_string();
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

        validate_sql_template(&raw_request)?;
        Ok(())
    }

    #[test]
    fn block_subquery_alias_leak() -> Result<()> {
        // As long as user_data_folder is referencing an in scope table alias, the query is OK.
        let sql =
            "with a as (select * from (select * from {user_tables}) b where col in (select col from b)) select * from a,b"
                .to_string();
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

        assert_eq!("invalid query: Found table identifier \"b\" which is neither an \
        in scope table alias nor a SourceSubstitution. Specifying table names directly is not allowed. \
        Use an explicit SourceList within a SourceSubstitution instead.", err_msg);

        Ok(())
    }

    #[test]
    fn allow_parenthesis() -> Result<()> {
        // As long as user_data_folder is referencing an in scope table alias, the query is OK.
        let sql = "select * from (select * from {user_tables})".to_string();
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

        validate_sql_template(&raw_request)?;
        Ok(())
    }

    #[test]
    fn validate_cte_alias_scope() -> Result<()> {
        // Here we make sure CTE aliases which are out of scope do not leak up into the outer scope.
        // I.e. our validation logic does not get confused and think user_data_folder is the alias
        // created within a subquery
        let sql = "with a as (select * from {user_tables}) select * from user_data_folder user_data_folder \
        where (with user_data_folder as (select 1) select 1)".to_string();

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

        assert_eq!("invalid query: Found table identifier \"user_data_folder\" which is neither an \
        in scope table alias nor a SourceSubstitution. Specifying table names directly is not allowed. \
        Use an explicit SourceList within a SourceSubstitution instead.", err_msg);

        Ok(())
    }

    #[test]
    fn validate_duplicate_keys() -> Result<()> {
        // Duplicate keys accross info, value, or source subtitution should be blocked
        let sql = "with a as (select * from {user_tables}) select * from user_data_folder user_data_folder \
        where (with user_data_folder as (select 1) select 1)".to_string();

        let mut source_substitutions = HashMap::new();
        source_substitutions.insert(
            "user_tables".to_string(),
            SourceSubstitution::AllSourcesWith(vec!["test".to_string()]),
        );

        let mut info_substitutions = HashMap::new();
        info_substitutions.insert(
            "user_tables".to_string(),
            InfoSubstitution {
                entity_name: "test".to_string(),
                info_name: "test".to_string(),
                scope: "origin".to_string(),
                include_data_field: false,
                exclude_info_alias: false,
                include_info: true,
            },
        );

        let substitution_blocks = SubstitutionBlocks {
            info_substitutions,
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
            "invalid query: Found duplicate substitution key user_tables",
            err_msg
        );
        Ok(())
    }

    #[test]
    fn mismatched_substitution_keys() -> Result<()> {
        // The user_tables name matches but does not have capture braces so should fail
        let sql = "select * from user_tables".to_string();

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
            "invalid query: Found table identifier \"user_tables\" which is neither \
        an in scope table alias nor a SourceSubstitution. Specifying table names \
        directly is not allowed. Use an explicit SourceList within a SourceSubstitution instead.",
            err_msg
        );

        Ok(())
    }

    #[test]
    fn drop_table_test() -> Result<()> {
        let sql = "drop table {user_tables}".to_string();

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
            "invalid query: SQL templates may only contain read-only queries \
        (e.g. select statements), found statement: DROP TABLE \"{user_tables}\"",
            err_msg
        );

        Ok(())
    }

    #[test]
    fn insert_into_test() -> Result<()> {
        let sql = "insert into {user_tables} values (1,2)".to_string();

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
        found statement: INSERT INTO \"{user_tables}\" VALUES (1, 2)", err_msg);
        Ok(())
    }

    #[test]
    fn escape_query_test() -> Result<()> {
        let sql = "with user_tables as (select * from {user_tables}) select * from user_tables; select * from user_tables".to_string();

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
            "invalid query: SQL templates must contain exactly one statement. Found: 2",
            err_msg
        );
        Ok(())
    }

    #[test]
    fn escape_query_test2() -> Result<()> {
        let sql = "with user_tables as (select * from {user_tables}) select * from user_tables; select * from {user_tables}".to_string();

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
            "invalid query: SQL templates must contain exactly one statement. Found: 2",
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
