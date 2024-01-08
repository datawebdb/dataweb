use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use chrono::{NaiveDate, NaiveDateTime};
use datafusion::{
    common::{not_impl_err, Result},
    logical_expr::Expr,
    scalar::ScalarValue,
};

use datafusion::common::DataFusionError;
use tracing::info;

use crate::web_source::InfoSubstitution;

pub fn map_filter_exprs(
    entity_name: &str,
    filters: &[Expr],
    info_subs: &mut HashMap<String, InfoSubstitution>,
) -> String {
    let sql_exprs = filters
        .iter()
        .filter_map(|f| match filter_expr_to_sql(entity_name, f, info_subs) {
            Ok(s) => Some(s),
            Err(e) => {
                info!("Failed to push down filter expr {f} with error {e}");
                None
            }
        })
        .collect::<Vec<_>>();
    if sql_exprs.is_empty() {
        "".to_string()
    } else {
        format!("WHERE {}", sql_exprs.join(" AND "))
    }
}

pub fn filter_expr_to_sql(
    entity_name: &str,
    filter: &Expr,
    info_subs: &mut HashMap<String, InfoSubstitution>,
) -> Result<String> {
    match filter {
        Expr::Alias(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Column(col) => {
            let key = format!("filter_{}", col.name);
            info_subs.insert(
                key.clone(),
                InfoSubstitution {
                    entity_name: entity_name.to_string(),
                    info_name: col.name.to_string(),
                    include_info: true,
                    exclude_info_alias: true,
                    include_data_field: false,
                },
            );
            Ok(format!("{{{}}}", key))
        }
        Expr::ScalarVariable(_, _) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Literal(lit) => scalar_value_to_sql(lit),
        Expr::BinaryExpr(expr) => Ok(format!(
            "({} {} {})",
            filter_expr_to_sql(entity_name, expr.left.as_ref(), info_subs)?,
            expr.op,
            filter_expr_to_sql(entity_name, expr.right.as_ref(), info_subs)?
        )),
        Expr::Like(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::SimilarTo(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Not(expr) => Ok(format!(
            "(NOT {})",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsNotNull(expr) => Ok(format!(
            "({} IS NOT NULL)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsNull(expr) => Ok(format!(
            "({} IS NULL)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsTrue(expr) => Ok(format!(
            "({} IS TRUE)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsFalse(expr) => Ok(format!(
            "({} IS FALSE)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsUnknown(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::IsNotTrue(expr) => Ok(format!(
            "({} IS NOT TRUE)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsNotFalse(expr) => Ok(format!(
            "({} IS NOT FALSE)",
            filter_expr_to_sql(entity_name, expr.as_ref(), info_subs)?
        )),
        Expr::IsNotUnknown(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Negative(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::GetIndexedField(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Between(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Case(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Cast(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::TryCast(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Sort(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::ScalarFunction(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::ScalarUDF(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::AggregateFunction(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::WindowFunction(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::AggregateUDF(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::InList(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Exists(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::InSubquery(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::ScalarSubquery(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Wildcard => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::QualifiedWildcard { qualifier: _ } => {
            not_impl_err!("Got unsupported filter Expr {filter}")
        }
        Expr::GroupingSet(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::Placeholder(_) => not_impl_err!("Got unsupported filter Expr {filter}"),
        Expr::OuterReferenceColumn(_, _) => not_impl_err!("Got unsupported filter Expr {filter}"),
    }
}

fn primative_option_to_string<T: std::fmt::Display + std::fmt::Debug>(
    opt: &Option<T>,
    lit_quoted: bool,
) -> Result<String> {
    match opt {
        Some(prim) => {
            if lit_quoted {
                Ok(format!("'{prim}'"))
            } else {
                Ok(format!("{prim}"))
            }
        }
        None => not_impl_err!("Got unsupported 'None' ScalarValue {opt:?}"),
    }
}

fn scalar_value_to_sql(val: &ScalarValue) -> Result<String> {
    match val {
        ScalarValue::Null => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Boolean(b) => primative_option_to_string(b, false),
        ScalarValue::Float32(f) => primative_option_to_string(f, false),
        ScalarValue::Float64(f) => primative_option_to_string(f, false),
        ScalarValue::Decimal128(_, _, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Decimal256(_, _, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Int8(i) => primative_option_to_string(i, false),
        ScalarValue::Int16(i) => primative_option_to_string(i, false),
        ScalarValue::Int32(i) => primative_option_to_string(i, false),
        ScalarValue::Int64(i) => primative_option_to_string(i, false),
        ScalarValue::UInt8(i) => primative_option_to_string(i, false),
        ScalarValue::UInt16(i) => primative_option_to_string(i, false),
        ScalarValue::UInt32(i) => primative_option_to_string(i, false),
        ScalarValue::UInt64(i) => primative_option_to_string(i, false),
        ScalarValue::Utf8(s) => primative_option_to_string(s, true),
        ScalarValue::LargeUtf8(s) => primative_option_to_string(s, true),
        ScalarValue::Binary(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::FixedSizeBinary(_, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::LargeBinary(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Fixedsizelist(_, _, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::List(_, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Date32(d) => {
            let date = match d {
                Some(days) => NaiveDate::from_num_days_from_ce_opt(days + 719_163),
                None => return not_impl_err!("Got unsupported 'None' ScalarValue {d:?}"),
            }
            .ok_or(DataFusionError::NotImplemented(format!(
                "Date overflow error for {d:?}"
            )))?;
            Ok(format!("date '{}'", date))
        }
        ScalarValue::Date64(d) => {
            let datetime = match d {
                Some(ms) => NaiveDateTime::from_timestamp_millis(*ms),
                None => return not_impl_err!("Got unsupported 'None' ScalarValue {d:?}"),
            }
            .ok_or(DataFusionError::NotImplemented(format!(
                "Date overflow error for {d:?}"
            )))?;
            Ok(format!("date '{}'", datetime))
        }
        ScalarValue::Time32Second(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Time32Millisecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Time64Microsecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Time64Nanosecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::TimestampSecond(_, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::TimestampMillisecond(_, _) => {
            not_impl_err!("Got unsupported ScalarValue {val}")
        }
        ScalarValue::TimestampMicrosecond(_, _) => {
            not_impl_err!("Got unsupported ScalarValue {val}")
        }
        ScalarValue::TimestampNanosecond(_, _) => {
            not_impl_err!("Got unsupported ScalarValue {val}")
        }
        ScalarValue::IntervalYearMonth(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::IntervalDayTime(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::IntervalMonthDayNano(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::DurationSecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::DurationMillisecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::DurationMicrosecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::DurationNanosecond(_) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Struct(_, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
        ScalarValue::Dictionary(_, _) => not_impl_err!("Got unsupported ScalarValue {val}"),
    }
}

/// Computes the appropriate projection string for a given projected [SchemaRef] and inserts
/// the appropraite [InfoSubstitution] for the returned string
pub fn map_projection(
    entity_name: &str,
    projected_schema: SchemaRef,
    info_subs: &mut HashMap<String, InfoSubstitution>,
) -> String {
    projected_schema
        .fields()
        .iter()
        .map(|f| {
            let key = format!("proj_{}", f.name());
            info_subs.insert(
                key.clone(),
                InfoSubstitution {
                    entity_name: entity_name.to_string(),
                    info_name: f.name().to_string(),
                    include_info: true,
                    exclude_info_alias: false,
                    include_data_field: false,
                },
            );
            format!("{{{}}}", key)
        })
        .collect::<Vec<_>>()
        .join(",")
}
