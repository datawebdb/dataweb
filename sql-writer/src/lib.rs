use std::sync::Arc;

use ast_builder::DerivedRelationBuilder;
use chrono::{NaiveDate, NaiveDateTime};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{JoinConstraint, JoinType, Like};
use datafusion::sql::sqlparser::ast::{
    Function, FunctionArg, Ident, JoinOperator, OrderByExpr, SelectItem,
};
use datafusion::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
    sql::sqlparser::ast::{self, DataType as SQLDataType, Expr as SQLExpr},
};

use datafusion::common::{internal_err, not_impl_err, DFSchema};
use datafusion::common::{Column, DFSchemaRef};
#[allow(unused_imports)]
use datafusion::logical_expr::aggregate_function;
use datafusion::logical_expr::expr::{
    AggregateFunctionDefinition, Alias, BinaryExpr, Case, Cast, InList,
    ScalarFunction as DFScalarFunction, WindowFunction,
};
use datafusion::logical_expr::{Between, LogicalPlan, Operator};
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::dialect::{
    Dialect, GenericDialect, PostgreSqlDialect, SQLiteDialect,
};

mod ast_builder;
use crate::ast_builder::{
    BuilderError, QueryBuilder, RelationBuilder, SelectBuilder, TableRelationBuilder,
    TableWithJoinsBuilder,
};

/// The number of days from day 1 CE (0001-1-1) to Unix Epoch (1970-01-01)
static DAYS_FROM_CE_TO_UNIX_EPOCH: i32 = 719_163;

pub fn from_df_plan(plan: &LogicalPlan, dialect: Arc<dyn Dialect>) -> Result<ast::Statement> {
    query_to_sql(plan, dialect)
}

pub fn from_df_expr(expr: &Expr, dialect: Arc<dyn Dialect>) -> Result<SQLExpr> {
    let _schema = DFSchema::empty();
    expr_to_sql(expr, dialect)
}

fn query_to_sql(plan: &LogicalPlan, dialect: Arc<dyn Dialect>) -> Result<ast::Statement> {
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_) => {
            let query_builder = QueryBuilder::default();
            let mut select_builder = SelectBuilder::default();
            select_builder.push_from(TableWithJoinsBuilder::default());
            let relation_builder = RelationBuilder::default();
            select_to_sql(
                plan,
                query_builder,
                select_builder,
                relation_builder,
                dialect,
            )
        }
        LogicalPlan::Dml(_) => dml_to_sql(plan),
        LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Extension(_)
        | LogicalPlan::Prepare(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::Unnest(_) => Err(DataFusionError::NotImplemented(
            "Unsupported operator: {plan:?}".to_string(),
        )),
    }
}

fn finalize_builders(
    mut query: QueryBuilder,
    mut select: SelectBuilder,
    relation: RelationBuilder,
) -> Result<ast::Statement> {
    let mut twj = select.pop_from().unwrap();
    twj.relation(relation);
    select.push_from(twj);

    let body = ast::SetExpr::Select(Box::new(select.build().map_err(builder_error_to_df)?));
    let query = query
        .body(Box::new(body))
        .build()
        .map_err(builder_error_to_df)?;

    Ok(ast::Statement::Query(Box::new(query)))
}

fn select_to_sql(
    plan: &LogicalPlan,
    mut query: QueryBuilder,
    mut select: SelectBuilder,
    mut relation: RelationBuilder,
    dialect: Arc<dyn Dialect>,
) -> Result<ast::Statement> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let mut builder = TableRelationBuilder::default();
            builder.name(ast::ObjectName(vec![new_ident(
                scan.table_name.table().to_string(),
                dialect,
            )]));
            relation.table(builder);

            finalize_builders(query, select, relation)
        }
        LogicalPlan::Projection(p) => {
            // A second projection implies a derived tablefactor
            if !select.already_projected() {
                // Special handling when projecting an agregation plan
                if let LogicalPlan::Aggregate(agg) = p.input.as_ref() {
                    // Currently assuming projection is always group bys first, then aggregations
                    let n_group_bys = agg.group_expr.len();
                    let mut items = p
                        .expr
                        .iter()
                        .take(n_group_bys)
                        .map(|e| select_item_to_sql(e, p.input.schema(), 0, dialect.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    let proj_aggs = p
                        .expr
                        .iter()
                        .skip(n_group_bys)
                        .zip(agg.aggr_expr.iter())
                        .map(|(proj, agg_exp)| {
                            let sql_agg_expr =
                                select_item_to_sql(agg_exp, p.input.schema(), 0, dialect.clone())?;
                            let maybe_aliased = if let Expr::Alias(Alias { name, .. }) = proj {
                                if let SelectItem::UnnamedExpr(aggregation_fun) = sql_agg_expr {
                                    SelectItem::ExprWithAlias {
                                        expr: aggregation_fun,
                                        alias: Ident {
                                            value: name.to_string(),
                                            quote_style: None,
                                        },
                                    }
                                } else {
                                    sql_agg_expr
                                }
                            } else {
                                sql_agg_expr
                            };
                            Ok(maybe_aliased)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    items.extend(proj_aggs);
                    select.projection(items);
                    select.group_by(ast::GroupByExpr::Expressions(
                        agg.group_expr
                            .iter()
                            .map(|expr| expr_to_sql(expr, dialect.clone()))
                            .collect::<Result<Vec<_>>>()?,
                    ));
                    select_to_sql(agg.input.as_ref(), query, select, relation, dialect.clone())
                } else {
                    let items = p
                        .expr
                        .iter()
                        .map(|e| select_item_to_sql(e, p.input.schema(), 0, dialect.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    select.projection(items);
                    select_to_sql(p.input.as_ref(), query, select, relation, dialect.clone())
                }
            } else {
                let mut derived_builder = DerivedRelationBuilder::default();
                derived_builder.lateral(false).alias(None).subquery({
                    let inner_statment = query_to_sql(plan, dialect.clone())?;
                    if let ast::Statement::Query(inner_query) = inner_statment {
                        inner_query
                    } else {
                        return internal_err!(
                            "Subquery must be a Query, but found {inner_statment:?}"
                        );
                    }
                });
                relation.derived(derived_builder);
                finalize_builders(query, select, relation)
            }
        }
        LogicalPlan::Filter(filter) => {
            let filter_expr = expr_to_sql(&filter.predicate, dialect.clone())?;

            select.selection(Some(filter_expr));

            select_to_sql(
                filter.input.as_ref(),
                query,
                select,
                relation,
                dialect.clone(),
            )
        }
        LogicalPlan::Limit(limit) => {
            if let Some(fetch) = limit.fetch {
                query.limit(Some(ast::Expr::Value(ast::Value::Number(
                    fetch.to_string(),
                    false,
                ))));
            }

            select_to_sql(
                limit.input.as_ref(),
                query,
                select,
                relation,
                dialect.clone(),
            )
        }
        LogicalPlan::Sort(sort) => {
            query.order_by(sort_to_sql(
                sort.expr.clone(),
                sort.input.schema(),
                0,
                dialect.clone(),
            )?);

            select_to_sql(
                sort.input.as_ref(),
                query,
                select,
                relation,
                dialect.clone(),
            )
        }
        LogicalPlan::Aggregate(_agg) => {
            not_impl_err!("Unsupported aggregation plan not following a projection: {plan:?}")
        }
        LogicalPlan::Distinct(_distinct) => {
            not_impl_err!("Unsupported Distinct plan: {plan:?}")
        }
        LogicalPlan::Join(join) => {
            match join.join_constraint {
                JoinConstraint::On => {}
                JoinConstraint::Using => {
                    return not_impl_err!("Unsupported join constraint: {:?}", join.join_constraint)
                }
            }

            // parse filter if exists
            let _in_join_schema = join.left.schema().join(join.right.schema())?;
            let join_filter = match &join.filter {
                Some(filter) => Some(expr_to_sql(filter, dialect.clone())?),
                None => None,
            };

            // map join.on to `l.a = r.a AND l.b = r.b AND ...`
            let eq_op = ast::BinaryOperator::Eq;
            let join_on = join_conditions_to_sql(
                &join.on,
                eq_op,
                join.left.schema(),
                join.right.schema(),
                dialect.clone(),
            )?;

            // Merge `join_on` and `join_filter`
            let join_expr = match (join_filter, join_on) {
                (Some(filter), Some(on)) => Some(and_op_to_sql(filter, on)),
                (Some(filter), None) => Some(filter),
                (None, Some(on)) => Some(on),
                (None, None) => None,
            };
            let _join_constraint = match join_expr {
                Some(expr) => ast::JoinConstraint::On(expr),
                None => ast::JoinConstraint::None,
            };

            not_impl_err!("to do!")

            // let mut right_relation = RelationBuilder::default();

            // let left_statement = select_to_sql(join.left.as_ref(), query, select, relation, dialect.clone())?;
            // let right_statment = select_to_sql(
            //     join.right.as_ref(),
            //     query,
            //     select,
            //     right_relation,
            //     dialect.clone(),
            // )?;

            // let ast_join = ast::Join {
            //     relation: right_relation.build().map_err(builder_error_to_df)?,
            //     join_operator: join_operator_to_sql(join.join_type, join_constraint),
            // };
            // let mut from = select.pop_from().unwrap();
            // from.push_join(ast_join);
            // select.push_from(from);

            // finalize_builders(query, select, relation)
        }
        LogicalPlan::SubqueryAlias(plan_alias) => {
            // Handle bottom-up to allocate relation
            relation.alias(Some(new_table_alias(
                plan_alias.alias.table().to_string(),
                dialect.clone(),
            )));
            select_to_sql(
                plan_alias.input.as_ref(),
                query,
                select,
                relation,
                dialect.clone(),
            )
        }
        LogicalPlan::Union(_union) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Window(_window) => {
            not_impl_err!("Unsupported operator: {plan:?}")
        }
        LogicalPlan::Extension(_) => not_impl_err!("Unsupported operator: {plan:?}"),
        _ => not_impl_err!("Unsupported operator: {plan:?}"),
    }
}

fn select_item_to_sql(
    expr: &Expr,
    _schema: &DFSchemaRef,
    _col_ref_offset: usize,
    dialect: Arc<dyn Dialect>,
) -> Result<ast::SelectItem> {
    match expr {
        Expr::Alias(Alias { expr, name, .. }) => {
            let inner = expr_to_sql(expr, dialect.clone())?;

            Ok(ast::SelectItem::ExprWithAlias {
                expr: inner,
                alias: new_ident(name.to_string(), dialect.clone()),
            })
        }
        _ => {
            let inner = expr_to_sql(expr, dialect.clone())?;

            Ok(ast::SelectItem::UnnamedExpr(inner))
        }
    }
}

fn expr_to_sql(expr: &Expr, dialect: Arc<dyn Dialect>) -> Result<SQLExpr> {
    match expr {
        Expr::InList(InList {
            expr,
            list: _,
            negated: _,
        }) => {
            not_impl_err!("Unsupported InList expression: {expr:?}")
        }
        Expr::ScalarFunction(DFScalarFunction { .. }) => {
            not_impl_err!("Unsupported ScalarFunction expression: {expr:?}")
        }
        Expr::Between(Between {
            expr,
            negated: _,
            low: _,
            high: _,
        }) => {
            not_impl_err!("Unsupported Between expression: {expr:?}")
        }
        Expr::Column(col) => col_to_sql(col, dialect.clone()),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let l = expr_to_sql(left.as_ref(), dialect.clone())?;
            let r = expr_to_sql(right.as_ref(), dialect.clone())?;
            let op = op_to_sql(op)?;

            // Nested ensures order is perseved as specified in the LogicalPlan Expression tree vs
            // typical arithmentic precedence.
            // For example, col + 1 / 2 is not the same as (col + 1) / 2
            Ok(SQLExpr::Nested(Box::new(binary_op_to_sql(l, r, op))))
        }
        Expr::Case(Case {
            expr,
            when_then_expr: _,
            else_expr: _,
        }) => {
            not_impl_err!("Unsupported Case expression: {expr:?}")
        }
        Expr::Cast(Cast { expr, data_type }) => {
            let inner_expr = expr_to_sql(expr.as_ref(), dialect.clone())?;
            Ok(SQLExpr::Cast {
                expr: Box::new(inner_expr),
                data_type: df_to_sql_data_type(data_type)?,
                format: None,
            })
        }
        Expr::Literal(value) => Ok(scalar_to_sql(value)?),
        Expr::Alias(Alias { expr, name: _, .. }) => expr_to_sql(expr, dialect.clone()),
        Expr::WindowFunction(WindowFunction {
            fun: _,
            args: _,
            partition_by: _,
            order_by: _,
            window_frame: _,
        }) => {
            not_impl_err!("Unsupported WindowFunction expression: {expr:?}")
        }
        Expr::Like(Like {
            negated: _,
            expr,
            pattern: _,
            escape_char: _,
            case_insensitive: _,
        }) => {
            not_impl_err!("Unsupported Like expression: {expr:?}")
        }
        Expr::ScalarVariable(_, _) => {
            not_impl_err!("Unsupported ScalarVariable expression: {expr:?}")
        }
        Expr::SimilarTo(_) => not_impl_err!("Unsupported SimilarTo expression: {expr:?}"),
        Expr::Not(_) => not_impl_err!("Unsupported Not expression: {expr:?}"),
        Expr::IsNotNull(_) => not_impl_err!("Unsupported IsNotNull expression: {expr:?}"),
        Expr::IsNull(_) => not_impl_err!("Unsupported IsNull expression: {expr:?}"),
        Expr::IsTrue(_) => not_impl_err!("Unsupported IsTrue expression: {expr:?}"),
        Expr::IsFalse(_) => not_impl_err!("Unsupported IsFalse expression: {expr:?}"),
        Expr::IsUnknown(_) => not_impl_err!("Unsupported IsUnknown expression: {expr:?}"),
        Expr::IsNotTrue(_) => not_impl_err!("Unsupported IsNotTrue expression: {expr:?}"),
        Expr::IsNotFalse(_) => not_impl_err!("Unsupported IsNotFalse expression: {expr:?}"),
        Expr::IsNotUnknown(_) => not_impl_err!("Unsupported IsNotUnknown expression: {expr:?}"),
        Expr::Negative(_) => not_impl_err!("Unsupported Negative expression: {expr:?}"),
        Expr::GetIndexedField(_) => {
            not_impl_err!("Unsupported GetIndexedField expression: {expr:?}")
        }
        Expr::TryCast(_) => not_impl_err!("Unsupported TryCast expression: {expr:?}"),
        Expr::Sort(_) => not_impl_err!("Unsupported Sort expression: {expr:?}"),
        Expr::AggregateFunction(agg) => {
            let func_name = if let AggregateFunctionDefinition::BuiltIn(built_in) = &agg.func_def {
                built_in.name()
            } else {
                return not_impl_err!("Only built in agg functions are supported, got {agg:?}");
            };

            let args = agg
                .args
                .iter()
                .map(|e| {
                    if matches!(e, Expr::Wildcard { qualifier: None }) {
                        Ok(FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                    } else {
                        expr_to_sql(e, dialect.clone())
                            .map(|e| FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(SQLExpr::Function(Function {
                name: ast::ObjectName(vec![Ident {
                    value: func_name.to_string(),
                    quote_style: None,
                }]),
                args,
                filter: None,
                null_treatment: None,
                over: None,
                distinct: false,
                special: false,
                order_by: vec![],
            }))
        }
        Expr::Exists(_) => not_impl_err!("Unsupported Exists expression: {expr:?}"),
        Expr::InSubquery(_) => not_impl_err!("Unsupported InSubquery expression: {expr:?}"),
        Expr::ScalarSubquery(_) => not_impl_err!("Unsupported ScalarSubquery expression: {expr:?}"),
        Expr::Wildcard { qualifier: _ } => {
            not_impl_err!("Unsupported Wildcard expression: {expr:?}")
        }
        Expr::GroupingSet(_) => not_impl_err!("Unsupported GroupingSet expression: {expr:?}"),
        Expr::Placeholder(_) => not_impl_err!("Unsupported Placeholder expression: {expr:?}"),
        Expr::OuterReferenceColumn(_, _) => {
            not_impl_err!("Unsupported OuterReferenceColumn expression: {expr:?}")
        }
    }
}

fn df_to_sql_data_type(data_type: &DataType) -> Result<SQLDataType> {
    match data_type {
        DataType::Null => not_impl_err!("Unsupported DataType: conversion: {data_type:?}"),
        DataType::Boolean => Ok(SQLDataType::Bool),
        DataType::Int8 => Ok(SQLDataType::TinyInt(None)),
        DataType::Int16 => Ok(SQLDataType::SmallInt(None)),
        DataType::Int32 => Ok(SQLDataType::Integer(None)),
        DataType::Int64 => Ok(SQLDataType::BigInt(None)),
        DataType::UInt8 => Ok(SQLDataType::UnsignedTinyInt(None)),
        DataType::UInt16 => Ok(SQLDataType::UnsignedSmallInt(None)),
        DataType::UInt32 => Ok(SQLDataType::UnsignedInteger(None)),
        DataType::UInt64 => Ok(SQLDataType::UnsignedBigInt(None)),
        DataType::Float16 => not_impl_err!("Unsupported DataType: conversion: {data_type:?}"),
        DataType::Float32 => Ok(SQLDataType::Float(None)),
        DataType::Float64 => Ok(SQLDataType::Double),
        DataType::Timestamp(_, _) => {
            not_impl_err!("Unsupported DataType: conversion: {data_type:?}")
        }
        DataType::Date32 => Ok(SQLDataType::Date),
        DataType::Date64 => Ok(SQLDataType::Datetime(None)),
        DataType::Time32(_) => todo!(),
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
        DataType::Binary => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Utf8 => todo!(),
        DataType::LargeUtf8 => todo!(),
        DataType::List(_) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}

fn sort_to_sql(
    sort_exprs: Vec<Expr>,
    _schema: &DFSchemaRef,
    _col_ref_offset: usize,
    dialect: Arc<dyn Dialect>,
) -> Result<Vec<OrderByExpr>> {
    sort_exprs
        .iter()
        .map(|expr: &Expr| match expr {
            Expr::Sort(sort_expr) => {
                let col = expr_to_sql(&sort_expr.expr, dialect.clone())?;
                Ok(OrderByExpr {
                    asc: Some(sort_expr.asc),
                    expr: col,
                    nulls_first: Some(sort_expr.nulls_first),
                })
            }
            _ => Err(DataFusionError::Plan("Expecting Sort expr".to_string())),
        })
        .collect::<Result<Vec<_>>>()
}

fn op_to_sql(op: &Operator) -> Result<ast::BinaryOperator> {
    match op {
        Operator::Eq => Ok(ast::BinaryOperator::Eq),
        Operator::NotEq => Ok(ast::BinaryOperator::NotEq),
        Operator::Lt => Ok(ast::BinaryOperator::Lt),
        Operator::LtEq => Ok(ast::BinaryOperator::LtEq),
        Operator::Gt => Ok(ast::BinaryOperator::Gt),
        Operator::GtEq => Ok(ast::BinaryOperator::GtEq),
        Operator::Plus => Ok(ast::BinaryOperator::Plus),
        Operator::Minus => Ok(ast::BinaryOperator::Minus),
        Operator::Multiply => Ok(ast::BinaryOperator::Multiply),
        Operator::Divide => Ok(ast::BinaryOperator::Divide),
        Operator::Modulo => Ok(ast::BinaryOperator::Modulo),
        Operator::And => Ok(ast::BinaryOperator::And),
        Operator::Or => Ok(ast::BinaryOperator::Or),
        Operator::IsDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::IsNotDistinctFrom => not_impl_err!("unsupported operation: {op:?}"),
        Operator::RegexMatch => Ok(ast::BinaryOperator::PGRegexMatch),
        Operator::RegexIMatch => Ok(ast::BinaryOperator::PGRegexIMatch),
        Operator::RegexNotMatch => Ok(ast::BinaryOperator::PGRegexNotMatch),
        Operator::RegexNotIMatch => Ok(ast::BinaryOperator::PGRegexNotIMatch),
        Operator::BitwiseAnd => Ok(ast::BinaryOperator::BitwiseAnd),
        Operator::BitwiseOr => Ok(ast::BinaryOperator::BitwiseOr),
        Operator::BitwiseXor => Ok(ast::BinaryOperator::BitwiseXor),
        Operator::BitwiseShiftRight => Ok(ast::BinaryOperator::PGBitwiseShiftRight),
        Operator::BitwiseShiftLeft => Ok(ast::BinaryOperator::PGBitwiseShiftLeft),
        Operator::StringConcat => Ok(ast::BinaryOperator::StringConcat),
        Operator::AtArrow => not_impl_err!("unsupported operation: {op:?}"),
        Operator::ArrowAt => not_impl_err!("unsupported operation: {op:?}"),
    }
}

/// DataFusion ScalarValues sometimes require a SQLExpr to construct.
/// For example ScalarValue::Date32(d) corresponds to the SqlExpr CAST('datestr' as DATE)
fn scalar_to_sql(v: &ScalarValue) -> Result<SQLExpr> {
    match v {
        ScalarValue::Null => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Boolean(Some(b)) => Ok(SQLExpr::Value(ast::Value::Boolean(b.to_owned()))),
        ScalarValue::Boolean(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Float32(Some(f)) => {
            Ok(SQLExpr::Value(ast::Value::Number(f.to_string(), false)))
        }
        ScalarValue::Float32(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Float64(Some(f)) => {
            Ok(SQLExpr::Value(ast::Value::Number(f.to_string(), false)))
        }
        ScalarValue::Float64(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Decimal128(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal128(None, ..) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Decimal256(Some(_), ..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Decimal256(None, ..) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Int8(Some(i)) => Ok(SQLExpr::Value(ast::Value::Number(i.to_string(), false))),
        ScalarValue::Int8(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Int16(Some(i)) => Ok(SQLExpr::Value(ast::Value::Number(i.to_string(), false))),
        ScalarValue::Int16(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Int32(Some(i)) => Ok(SQLExpr::Value(ast::Value::Number(i.to_string(), false))),
        ScalarValue::Int32(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Int64(Some(i)) => Ok(SQLExpr::Value(ast::Value::Number(i.to_string(), false))),
        ScalarValue::Int64(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::UInt8(Some(ui)) => {
            Ok(SQLExpr::Value(ast::Value::Number(ui.to_string(), false)))
        }
        ScalarValue::UInt8(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::UInt16(Some(ui)) => {
            Ok(SQLExpr::Value(ast::Value::Number(ui.to_string(), false)))
        }
        ScalarValue::UInt16(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::UInt32(Some(ui)) => {
            Ok(SQLExpr::Value(ast::Value::Number(ui.to_string(), false)))
        }
        ScalarValue::UInt32(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::UInt64(Some(ui)) => {
            Ok(SQLExpr::Value(ast::Value::Number(ui.to_string(), false)))
        }
        ScalarValue::UInt64(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Utf8(Some(str)) => Ok(SQLExpr::Value(ast::Value::SingleQuotedString(
            str.to_string(),
        ))),
        ScalarValue::Utf8(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::LargeUtf8(Some(str)) => Ok(SQLExpr::Value(ast::Value::SingleQuotedString(
            str.to_string(),
        ))),
        ScalarValue::LargeUtf8(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Binary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Binary(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::FixedSizeBinary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(Some(_)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeBinary(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::FixedSizeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::List(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::LargeList(_a) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Date32(Some(d)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(d + DAYS_FROM_CE_TO_UNIX_EPOCH).ok_or(
                DataFusionError::NotImplemented(format!("Date overflow error for {d:?}")),
            )?;
            Ok(SQLExpr::Cast {
                expr: Box::new(SQLExpr::Value(ast::Value::SingleQuotedString(
                    date.to_string(),
                ))),
                data_type: SQLDataType::Date,
                format: None,
            })
        }
        ScalarValue::Date32(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Date64(Some(ms)) => {
            let datetime = NaiveDateTime::from_timestamp_millis(*ms).ok_or(
                DataFusionError::NotImplemented(format!("Datetime overflow error for {ms:?}")),
            )?;
            Ok(SQLExpr::Cast {
                expr: Box::new(SQLExpr::Value(ast::Value::SingleQuotedString(
                    datetime.to_string(),
                ))),
                data_type: SQLDataType::Datetime(None),
                format: None,
            })
        }
        ScalarValue::Date64(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Time32Second(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Second(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Time32Millisecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time32Millisecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Time64Microsecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Microsecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Time64Nanosecond(Some(_t)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Time64Nanosecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::TimestampSecond(Some(_ts), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::TimestampSecond(None, _) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::TimestampMillisecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMillisecond(None, _) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::TimestampMicrosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampMicrosecond(None, _) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::TimestampNanosecond(Some(_ts), _) => {
            not_impl_err!("Unsupported scalar: {v:?}")
        }
        ScalarValue::TimestampNanosecond(None, _) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::IntervalYearMonth(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalYearMonth(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::IntervalDayTime(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalDayTime(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::IntervalMonthDayNano(Some(_i)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::IntervalMonthDayNano(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::DurationSecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationSecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::DurationMillisecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMillisecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::DurationMicrosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationMicrosecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::DurationNanosecond(Some(_d)) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::DurationNanosecond(None) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Struct(Some(_), _) => not_impl_err!("Unsupported scalar: {v:?}"),
        ScalarValue::Struct(None, _) => Ok(SQLExpr::Value(ast::Value::Null)),
        ScalarValue::Dictionary(..) => not_impl_err!("Unsupported scalar: {v:?}"),
    }
}

fn col_to_sql(col: &Column, dialect: Arc<dyn Dialect>) -> Result<ast::Expr> {
    let expr = if let Some(relation) = &col.relation {
        ast::Expr::CompoundIdentifier(
            [relation.table().to_string(), col.name.to_string()]
                .iter()
                .map(|i| new_ident(i.to_string(), dialect.clone()))
                .collect(),
        )
    } else {
        ast::Expr::Identifier(new_ident(col.name.to_string(), dialect.clone()))
    };
    Ok(expr)
}

fn _join_operator_to_sql(join_type: JoinType, constraint: ast::JoinConstraint) -> JoinOperator {
    match join_type {
        JoinType::Inner => JoinOperator::Inner(constraint),
        JoinType::Left => JoinOperator::LeftOuter(constraint),
        JoinType::Right => JoinOperator::RightOuter(constraint),
        JoinType::Full => JoinOperator::FullOuter(constraint),
        JoinType::LeftAnti => JoinOperator::LeftAnti(constraint),
        JoinType::LeftSemi => JoinOperator::LeftSemi(constraint),
        JoinType::RightAnti => JoinOperator::RightAnti(constraint),
        JoinType::RightSemi => JoinOperator::RightSemi(constraint),
    }
}

fn join_conditions_to_sql(
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: ast::BinaryOperator,
    _left_schema: &DFSchemaRef,
    _right_schema: &DFSchemaRef,
    dialect: Arc<dyn Dialect>,
) -> Result<Option<SQLExpr>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<SQLExpr> = vec![];
    for (left, right) in join_conditions {
        // Parse left
        let l = expr_to_sql(left, dialect.clone())?;
        // Parse right
        let r = expr_to_sql(right, dialect.clone())?;
        // AND with existing expression
        exprs.push(binary_op_to_sql(l, r, eq_op.clone()));
    }
    let join_expr: Option<SQLExpr> = exprs.into_iter().reduce(and_op_to_sql);
    Ok(join_expr)
}

pub fn and_op_to_sql(lhs: SQLExpr, rhs: SQLExpr) -> SQLExpr {
    binary_op_to_sql(lhs, rhs, ast::BinaryOperator::And)
}

pub fn binary_op_to_sql(lhs: SQLExpr, rhs: SQLExpr, op: ast::BinaryOperator) -> SQLExpr {
    SQLExpr::BinaryOp {
        left: Box::new(lhs),
        op,
        right: Box::new(rhs),
    }
}

fn new_table_alias(alias: String, dialect: Arc<dyn Dialect>) -> ast::TableAlias {
    ast::TableAlias {
        name: new_ident(alias, dialect.clone()),
        columns: Vec::new(),
    }
}

fn new_ident(str: String, dialect: Arc<dyn Dialect>) -> ast::Ident {
    ast::Ident {
        value: str,
        quote_style: if dialect.is::<PostgreSqlDialect>() {
            Some('"')
        } else if dialect.is::<SQLiteDialect>() || dialect.is::<GenericDialect>() {
            Some('`')
        } else {
            todo!()
        },
    }
}

fn dml_to_sql(_plan: &LogicalPlan) -> Result<ast::Statement> {
    Err(DataFusionError::NotImplemented(
        "dml unsupported".to_string(),
    ))
}

fn builder_error_to_df(e: BuilderError) -> DataFusionError {
    DataFusionError::External(format!("{e}").into())
}
