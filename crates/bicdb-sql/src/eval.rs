//! Scalar expression evaluation: SQL value coercion/casting, arithmetic, JSON and interval operators, pg builtin functions (regclass/regtype/GUC/acl), decimals, aggregates, and literal parsing.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use eval::*;`.

mod value_ops1;
pub(crate) use value_ops1::*;
mod value_ops2;
pub(crate) use value_ops2::*;
mod temporal_json_oid;
// Keep the central cast implementation's CFG stable for existing PGO data.
pub(crate) use temporal_json_oid::cast_value_to_pg_type_fast as cast_value_to_pg_type;
pub(crate) use temporal_json_oid::*;
mod ranges_network;
pub(crate) use ranges_network::*;
mod operators_compare;
pub(crate) use operators_compare::*;
mod aggregates;
pub(crate) use aggregates::*;
pub(crate) mod rewrite_helpers;
pub(crate) use rewrite_helpers::*;
mod text_rewrite;
pub(crate) use text_rewrite::*;
// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;
use base64::Engine as _;
use chrono::{
    DateTime, Duration as ChronoDuration, FixedOffset, LocalResult, NaiveDate, Offset, TimeZone,
    Utc,
};
use chrono_tz::Tz;
use hmac::{Hmac, Mac};
use icu_collator::{Collator, CollatorBorrowed};
use sha2::{Digest as ShaDigest, Sha224, Sha256, Sha384, Sha512};
use std::cell::RefCell;
use std::net::Ipv6Addr;

thread_local! {
    static SQL_UUID_V7_CONTEXT: uuid::ContextV7 = const { uuid::ContextV7::new() };
}

type SpaceUsage = (u64, u64, std::collections::BTreeMap<String, u64>);

fn cached_space_usage(root: &std::path::Path) -> SpaceUsage {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<std::path::PathBuf, (std::time::Instant, SpaceUsage)>,
        >,
    > = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    if let Some((when, usage)) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(root)
        .cloned()
        .filter(|(when, _)| when.elapsed() < std::time::Duration::from_secs(5))
    {
        let _ = when;
        return usage;
    }

    let mut categories = std::collections::BTreeMap::<String, u64>::new();
    let mut total = 0_u64;
    let mut allocated = 0_u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                stack.push(path);
                continue;
            }
            if !kind.is_file() {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            // NTFS directory metadata lags for files another handle holds
            // open (the paged store and its WAL): read_dir reports a stale —
            // typically zero — length until the writer flushes or closes. A
            // freshly opened handle reports the live size.
            #[cfg(windows)]
            let meta = match std::fs::File::open(&path).and_then(|file| file.metadata()) {
                Ok(live) => live,
                Err(_) => meta,
            };
            let bytes = meta.len();
            total = total.saturating_add(bytes);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                allocated = allocated.saturating_add(meta.blocks().saturating_mul(512));
            }
            #[cfg(not(unix))]
            {
                allocated = allocated.saturating_add(bytes);
            }
            let name = path.strip_prefix(root).unwrap_or(&path).to_string_lossy();
            let category = if name.starts_with("paged") {
                if name.contains(".wal") {
                    "wal"
                } else {
                    "paged_store"
                }
            } else if name.starts_with("events") {
                "events"
            } else if name.contains("sync") {
                "sync"
            } else if name.starts_with("maintenance") {
                "maintenance"
            } else if name.ends_with(".seg") {
                "segments"
            } else {
                "other"
            };
            *categories.entry(category.to_string()).or_default() = categories
                .get(category)
                .copied()
                .unwrap_or(0)
                .saturating_add(bytes);
        }
    }
    let usage = (total, allocated, categories);
    let mut guard = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if guard.len() >= 64 && !guard.contains_key(root) {
        if let Some(key) = guard.keys().next().cloned() {
            guard.remove(&key);
        }
    }
    guard.insert(
        root.to_path_buf(),
        (std::time::Instant::now(), usage.clone()),
    );
    usage
}

pub(crate) fn anonymous_record_value(
    values: Vec<SqlValue>,
    pg_types: Vec<Option<String>>,
) -> SqlValue {
    SqlValue::Composite(SqlComposite::anonymous(
        values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    pg_types
                        .get(index)
                        .and_then(Clone::clone)
                        .unwrap_or_else(|| "unknown".to_string()),
                    value,
                )
            })
            .collect(),
    ))
}

pub(crate) fn is_row_constructor(function: &Function) -> bool {
    // Structural check on the last name part; rendering the name to a
    // String per call showed up in the TPC-C profile.
    match function.name.0.last() {
        Some(sqlparser::ast::ObjectNamePart::Identifier(ident)) => {
            ident.value.eq_ignore_ascii_case("row")
        }
        _ => false,
    }
}

pub(crate) fn pg_typeof_result(
    declared_type: Option<&String>,
    value: Option<&SqlValue>,
) -> SqlValue {
    declared_type
        .cloned()
        .or_else(|| match value {
            Some(SqlValue::Composite(composite)) => Some(composite.type_name.clone()),
            Some(SqlValue::Json(value)) => value
                .get("$bicdb_array_input")
                .and_then(|input| input.get("declared_type"))
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            Some(value) => Some(projected_value_pg_type(value).to_string()),
            None => None,
        })
        .map(|pg_type| {
            pg_type_regtype_name(&pg_type)
                .and_then(|builtin| pg_format_type(pg_type_oid(&builtin) as i32, -1))
                .unwrap_or(pg_type)
        })
        .map(SqlValue::String)
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn eval_value(record: &Record, expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_date") => {
            Ok(SqlValue::String(unix_now_date_string()))
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_database") => {
            Ok(SqlValue::String("bicdb".to_string()))
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_schema") => {
            Ok(SqlValue::String("public".to_string()))
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::JsonAccess { .. } => {
            FieldRef::from_expr(expr)?.value(record)
        }
        Expr::Value(value) => literal_to_value(value),
        Expr::TypedString(value) => typed_string_to_value(value),
        Expr::Cast {
            expr, data_type, ..
        } => cast_expr_value(eval_value(record, expr)?, expr, data_type, None),
        Expr::Function(function) => eval_record_function_value(record, function),
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
        {
            eval_json_operator_value(eval_value(record, left)?, op, eval_value(record, right)?)
        }
        Expr::BinaryOp { left, op, right } => eval_binary_expr_value(
            left,
            op,
            right,
            eval_value(record, left)?,
            eval_value(record, right)?,
            None,
        ),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => eval_substring_expr(
            expr,
            substring_from.as_deref(),
            substring_for.as_deref(),
            |expr| eval_value(record, expr),
            projected_expr_pg_type(expr, None).as_deref() == Some("bytea"),
        ),
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => eval_overlay_expr(
            expr,
            overlay_what,
            overlay_from,
            overlay_for.as_deref(),
            |expr| eval_value(record, expr),
            projected_expr_pg_type(expr, None).as_deref() == Some("bytea"),
        ),
        Expr::Extract { field, expr, .. } => eval_extract_value(field, eval_value(record, expr)?),
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => eval_trim_expr(
            trim_where.as_ref(),
            trim_what.as_deref(),
            expr,
            trim_characters.as_deref(),
            |expr| eval_value(record, expr),
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => eval_record_case(
            record,
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
        ),
        Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_) => eval_predicate_truth(record, expr)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        Expr::Array(array) => sql_array_value(
            array
                .elem
                .iter()
                .map(|expr| eval_value(record, expr))
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::Tuple(exprs) => Ok(anonymous_record_value(
            exprs
                .iter()
                .map(|expr| eval_value(record, expr))
                .collect::<Result<Vec<_>>>()?,
            exprs
                .iter()
                .map(|expr| projected_expr_pg_type(expr, None))
                .collect(),
        )),
        Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
            root,
            access_chain,
            projected_expr_pg_type(root, None).as_deref() == Some("jsonb"),
            |expr| eval_value(record, expr),
        ),
        Expr::Interval(interval) => {
            interval_literal_value(interval, |expr| eval_value(record, expr))
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => eval_at_time_zone_value(
            eval_value(record, timestamp)?,
            eval_value(record, time_zone)?,
            projected_expr_pg_type(timestamp, None).as_deref(),
        ),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            eval_predicate_truth(record, expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
            eval_unary_minus_expr_value(expr, eval_value(record, expr)?, None)
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "+" => {
            eval_unary_plus_expr_value(expr, eval_value(record, expr)?, None)
        }
        Expr::UnaryOp { op, expr }
            if matches!(
                op,
                UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
            ) || is_geometric_unary_operator(op) =>
        {
            eval_unary_bit_not_expr_value(op, expr, eval_value(record, expr)?, None)
        }
        Expr::Nested(expr) => eval_value(record, expr),
        Expr::Collate { expr, collation } => {
            normalize_column_collation(collation)?;
            eval_value(record, expr)
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported value expression {other}"
        ))),
    }
}

pub(crate) fn eval_record_function_value(record: &Record, function: &Function) -> Result<SqlValue> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    let arg_exprs = function_args(function);
    let args = arg_exprs
        .iter()
        .map(|arg| eval_value(record, arg))
        .collect::<Result<Vec<_>>>()?;
    let arg_types = arg_exprs
        .iter()
        .map(|arg| projected_expr_pg_type(arg, None))
        .collect::<Vec<_>>();
    if is_row_constructor(function) {
        return Ok(anonymous_record_value(args, arg_types));
    }
    if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
        return Ok(pg_typeof_result(
            arg_types.first().and_then(Option::as_ref),
            args.first(),
        ));
    }
    if let Some(value) = eval_network_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_range_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_json_function_call_value(function, &args)? {
        return Ok(value);
    }
    if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_catalog_function_value(&name, &args) {
        return Ok(value);
    }
    if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_compatibility_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    eval_constant_function_value(function)
}

pub(crate) fn is_spatial_predicate_function(function: &Function) -> bool {
    let Ok(name) = object_name(&function.name).map(|name| name.to_ascii_lowercase()) else {
        return false;
    };
    let name = name
        .strip_prefix("public.")
        .or_else(|| name.strip_prefix("pg_catalog."))
        .unwrap_or(&name);
    matches!(name, "st_dwithin" | "st_intersects")
}

pub(crate) fn eval_record_case(
    record: &Record,
    operand: Option<&Expr>,
    conditions: &[sqlparser::ast::CaseWhen],
    else_result: Option<&Expr>,
) -> Result<SqlValue> {
    let operand_value = operand.map(|expr| eval_value(record, expr)).transpose()?;
    for condition in conditions {
        let matched = if let Some(operand_value) = &operand_value {
            values_equal(operand_value, &eval_value(record, &condition.condition)?)
        } else {
            eval_predicate_truth(record, &condition.condition)?.unwrap_or(false)
        };
        if matched {
            return eval_value(record, &condition.result);
        }
    }
    else_result
        .map(|expr| eval_value(record, expr))
        .unwrap_or(Ok(SqlValue::Null))
}

pub(crate) fn eval_constant_expr(expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_date") => {
            Ok(SqlValue::String(unix_now_date_string()))
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_database") => {
            Ok(SqlValue::String("bicdb".to_string()))
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_schema") => {
            Ok(SqlValue::String("public".to_string()))
        }
        Expr::Value(value) => literal_to_value(value),
        Expr::TypedString(value) => typed_string_to_value(value),
        Expr::Nested(expr) => eval_constant_expr(expr),
        Expr::Collate { expr, collation } => {
            normalize_column_collation(collation)?;
            eval_constant_expr(expr)
        }
        Expr::Cast {
            expr, data_type, ..
        } => cast_expr_value(eval_constant_expr(expr)?, expr, data_type, None),
        Expr::Function(function) => eval_constant_function_value(function),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            eval_constant_truth(expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
            eval_unary_minus_expr_value(expr, eval_constant_expr(expr)?, None)
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "+" => {
            eval_unary_plus_expr_value(expr, eval_constant_expr(expr)?, None)
        }
        Expr::UnaryOp { op, expr }
            if matches!(
                op,
                UnaryOperator::BitwiseNot | UnaryOperator::PGPrefixFactorial
            ) || is_geometric_unary_operator(op) =>
        {
            eval_unary_bit_not_expr_value(op, expr, eval_constant_expr(expr)?, None)
        }
        Expr::BinaryOp {
            left: left_expr,
            op,
            right: right_expr,
        } => {
            let left = eval_constant_expr(left_expr)?;
            let right = eval_constant_expr(right_expr)?;
            eval_binary_expr_value(left_expr, op, right_expr, left, right, None)
        }
        Expr::Position { expr, r#in } => eval_position_typed_value(
            eval_constant_expr(expr)?,
            eval_constant_expr(r#in)?,
            projected_expr_pg_type(expr, None).as_deref() == Some("bytea")
                || projected_expr_pg_type(r#in, None).as_deref() == Some("bytea"),
        ),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => eval_substring_expr(
            expr,
            substring_from.as_deref(),
            substring_for.as_deref(),
            eval_constant_expr,
            projected_expr_pg_type(expr, None).as_deref() == Some("bytea"),
        ),
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => eval_overlay_expr(
            expr,
            overlay_what,
            overlay_from,
            overlay_for.as_deref(),
            eval_constant_expr,
            projected_expr_pg_type(expr, None).as_deref() == Some("bytea"),
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => eval_constant_case(operand.as_deref(), conditions, else_result.as_deref()),
        Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_) => eval_constant_truth(expr)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        Expr::Array(array) => sql_array_value(
            array
                .elem
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?,
        ),
        Expr::Tuple(exprs) => Ok(anonymous_record_value(
            exprs
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?,
            exprs
                .iter()
                .map(|expr| projected_expr_pg_type(expr, None))
                .collect(),
        )),
        Expr::CompoundFieldAccess { root, access_chain } => eval_access_chain_expr(
            root,
            access_chain,
            projected_expr_pg_type(root, None).as_deref() == Some("jsonb"),
            eval_constant_expr,
        ),
        Expr::Interval(interval) => interval_literal_value(interval, eval_constant_expr),
        Expr::Extract { field, expr, .. } => eval_extract_value(field, eval_constant_expr(expr)?),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => eval_at_time_zone_value(
            eval_constant_expr(timestamp)?,
            eval_constant_expr(time_zone)?,
            projected_expr_pg_type(timestamp, None).as_deref(),
        ),
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => eval_trim_expr(
            trim_where.as_ref(),
            trim_what.as_deref(),
            expr,
            trim_characters.as_deref(),
            eval_constant_expr,
        ),
        other => Err(SqlError::Unsupported(format!(
            "unsupported constant expression {other}"
        ))),
    }
}

pub(crate) fn normalize_datestyle_setting(value: &str) -> Result<String> {
    let mut output = None;
    let mut order = None;

    for token in value
        .split(|ch: char| ch == ',' || ch.is_whitespace())
        .filter(|token| !token.is_empty())
    {
        match token.to_ascii_lowercase().as_str() {
            "iso" => output = Some("ISO"),
            "sql" => output = Some("SQL"),
            "postgres" => output = Some("Postgres"),
            "german" => output = Some("German"),
            "mdy" => order = Some("MDY"),
            "dmy" => order = Some("DMY"),
            "ymd" => order = Some("YMD"),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "SET datestyle got unsupported value {other}"
                )));
            }
        }
    }

    Ok(format!(
        "{}, {}",
        output.unwrap_or("ISO"),
        order.unwrap_or("MDY")
    ))
}

pub(crate) fn normalize_bool_setting(setting: &str, value: &str) -> Result<String> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok("on".to_string()),
        "off" | "false" | "no" | "0" => Ok("off".to_string()),
        other => Err(SqlError::InvalidSql(format!(
            "SET {setting} expects a boolean value, got {other}"
        ))),
    }
}

pub(crate) fn eval_setting_value(expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Identifier(ident) => Ok(SqlValue::String(ident.value.clone())),
        _ => eval_constant_expr(expr),
    }
}

pub(crate) fn eval_builtin_function_value(function: &Function) -> Result<SqlValue> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    let arg_exprs = function_args(function);
    let arg_types = arg_exprs
        .iter()
        .map(|arg| projected_expr_pg_type(arg, None))
        .collect::<Vec<_>>();
    if geometric_function_pg_type(&name, &arg_types).is_some() {
        let args = arg_exprs
            .iter()
            .map(eval_constant_expr)
            .collect::<Result<Vec<_>>>()?;
        if let Some(value) = eval_geometric_function_value(&name, &args, &arg_types)? {
            return Ok(value);
        }
    }
    let result = execute_builtin_function(function, None, None, None, None)?;
    Ok(first_result_value(result).unwrap_or(SqlValue::Null))
}

pub(crate) fn eval_constant_function_value(function: &Function) -> Result<SqlValue> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    if matches!(
        name.as_str(),
        "has_schema_privilege"
            | "pg_catalog.has_schema_privilege"
            | "has_table_privilege"
            | "pg_catalog.has_table_privilege"
            | "has_function_privilege"
            | "pg_catalog.has_function_privilege"
            | "has_type_privilege"
            | "pg_catalog.has_type_privilege"
            | "obj_description"
            | "pg_catalog.obj_description"
            | "pg_current_wal_lsn"
            | "pg_catalog.pg_current_wal_lsn"
            | "pg_current_wal_insert_lsn"
            | "pg_catalog.pg_current_wal_insert_lsn"
            | "pg_current_wal_flush_lsn"
            | "pg_catalog.pg_current_wal_flush_lsn"
            | "pg_last_wal_receive_lsn"
            | "pg_catalog.pg_last_wal_receive_lsn"
            | "pg_last_wal_replay_lsn"
            | "pg_catalog.pg_last_wal_replay_lsn"
            | "pg_current_snapshot"
            | "pg_catalog.pg_current_snapshot"
            | "txid_current_snapshot"
            | "pg_catalog.txid_current_snapshot"
    ) {
        return Err(SqlError::Unsupported(format!(
            "function {name} requires database context"
        )));
    }
    if matches!(name.as_str(), "coalesce" | "pg_catalog.coalesce") {
        for arg in function_args(function) {
            let value = eval_constant_expr(&arg)?;
            if !matches!(value, SqlValue::Null) {
                return Ok(value);
            }
        }
        return Ok(SqlValue::Null);
    }
    let arg_exprs = function_args(function);
    let args = arg_exprs
        .iter()
        .map(eval_constant_expr)
        .collect::<Result<Vec<_>>>()?;
    let arg_types = arg_exprs
        .iter()
        .map(|arg| projected_expr_pg_type(arg, None))
        .collect::<Vec<_>>();
    if is_row_constructor(function) {
        return Ok(anonymous_record_value(args, arg_types));
    }
    if matches!(name.as_str(), "pg_typeof" | "pg_catalog.pg_typeof") {
        return Ok(pg_typeof_result(
            arg_types.first().and_then(Option::as_ref),
            args.first(),
        ));
    }
    if matches!(
        name.as_str(),
        "pg_wal_lsn_diff" | "pg_catalog.pg_wal_lsn_diff"
    ) {
        require_arg_count("pg_wal_lsn_diff", &args, 2)?;
        if args.iter().any(|value| matches!(value, SqlValue::Null)) {
            return Ok(SqlValue::Null);
        }
        return Ok(SqlValue::String(
            (BigInt::from(pg_lsn_argument(&args[0])?) - BigInt::from(pg_lsn_argument(&args[1])?))
                .to_string(),
        ));
    }
    if let Some(value) = eval_network_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_range_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    if let Some(value) = eval_json_function_call_value(function, &args)? {
        return Ok(value);
    }
    if let Some(value) = crate::eval_xml_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_catalog_function_value(&name, &args) {
        return Ok(value);
    }
    if let Some(value) = eval_compatibility_function_value(&name, &args, Some(&arg_types))? {
        return Ok(value);
    }
    if let Some(value) = eval_spatial_function_value(&name, &args, &arg_types)? {
        return Ok(value);
    }
    eval_builtin_function_value(function)
}

pub(crate) fn eval_constant_case(
    operand: Option<&Expr>,
    conditions: &[sqlparser::ast::CaseWhen],
    else_result: Option<&Expr>,
) -> Result<SqlValue> {
    let operand_value = operand.map(eval_constant_expr).transpose()?;
    for condition in conditions {
        let matched = if let Some(operand_value) = &operand_value {
            values_equal(operand_value, &eval_constant_expr(&condition.condition)?)
        } else {
            eval_constant_truth(&condition.condition)?.unwrap_or(false)
        };
        if matched {
            return eval_constant_expr(&condition.result);
        }
    }
    else_result
        .map(eval_constant_expr)
        .unwrap_or(Ok(SqlValue::Null))
}

pub(crate) fn eval_trim_expr<F>(
    trim_where: Option<&sqlparser::ast::TrimWhereField>,
    trim_what: Option<&Expr>,
    expr: &Expr,
    trim_characters: Option<&[Expr]>,
    mut eval: F,
) -> Result<SqlValue>
where
    F: FnMut(&Expr) -> Result<SqlValue>,
{
    let value = eval(expr)?;
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let characters = if let Some(expr) = trim_what {
        Some(eval(expr)?.to_cell())
    } else if let Some(expr) = trim_characters.and_then(|characters| characters.first()) {
        Some(eval(expr)?.to_cell())
    } else {
        None
    };
    let value = value.to_cell();
    let side = trim_where
        .map(ToString::to_string)
        .unwrap_or_else(|| "BOTH".to_string())
        .to_ascii_uppercase();

    let trimmed = if let Some(characters) = characters {
        match side.as_str() {
            "LEADING" => value
                .trim_start_matches(|ch| characters.contains(ch))
                .to_string(),
            "TRAILING" => value
                .trim_end_matches(|ch| characters.contains(ch))
                .to_string(),
            _ => value.trim_matches(|ch| characters.contains(ch)).to_string(),
        }
    } else {
        match side.as_str() {
            "LEADING" => value.trim_start().to_string(),
            "TRAILING" => value.trim_end().to_string(),
            _ => value.trim().to_string(),
        }
    };
    Ok(SqlValue::String(trimmed))
}

pub(crate) fn eval_constant_truth(expr: &Expr) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(sql_and(
                eval_constant_truth(left)?,
                eval_constant_truth(right)?,
            )),
            BinaryOperator::Or => Ok(sql_or(
                eval_constant_truth(left)?,
                eval_constant_truth(right)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let mut eval = eval_constant_expr;
                if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                    return Ok(truth);
                }
                compare_expr_values(
                    left,
                    op,
                    right,
                    &eval_constant_expr(left)?,
                    &eval_constant_expr(right)?,
                    None,
                )
            }
            BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch
            | BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch => eval_pg_pattern_operator(
                &eval_constant_expr(left)?,
                op,
                &eval_constant_expr(right)?,
            ),
            BinaryOperator::AtArrow | BinaryOperator::ArrowAt => {
                eval_containment_truth(eval_constant_expr(left)?, op, eval_constant_expr(right)?)
            }
            _ if geometric_binary_result_pg_type(
                op,
                projected_expr_pg_type(left, None).as_deref(),
                projected_expr_pg_type(right, None).as_deref(),
            )
            .as_deref()
                == Some("bool")
                || network_binary_result_pg_type(
                    op,
                    projected_expr_pg_type(left, None).as_deref(),
                    projected_expr_pg_type(right, None).as_deref(),
                )
                .as_deref()
                    == Some("bool") =>
            {
                eval_binary_expr_value(
                    left,
                    op,
                    right,
                    eval_constant_expr(left)?,
                    eval_constant_expr(right)?,
                    None,
                )
                .and_then(sql_value_truth)
            }
            _ => Err(SqlError::Unsupported(format!(
                "unsupported boolean operator {op}"
            ))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut eval = eval_constant_expr;
            if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_in_list_truth(
                eval_constant_expr(expr)?,
                list.iter()
                    .map(eval_constant_expr)
                    .collect::<Result<Vec<_>>>()?,
                *negated,
            )
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let mut eval = eval_constant_expr;
            if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_between_truth(
                eval_constant_expr(expr)?,
                eval_constant_expr(low)?,
                eval_constant_expr(high)?,
                *negated,
            )
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => eval_quantified_truth(
            eval_constant_expr(left)?,
            compare_op,
            eval_constant_expr(right)?,
            false,
        ),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => eval_quantified_truth(
            eval_constant_expr(left)?,
            compare_op,
            eval_constant_expr(right)?,
            true,
        ),
        Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(&eval_constant_expr(expr)?))),
        Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(&eval_constant_expr(
            expr,
        )?))),
        Expr::IsDistinctFrom(left, right) => {
            let mut eval = eval_constant_expr;
            if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                return Ok(Some(!not_distinct));
            }
            Ok(Some(!values_not_distinct(
                &eval_constant_expr(left)?,
                &eval_constant_expr(right)?,
            )))
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let mut eval = eval_constant_expr;
            if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                return Ok(Some(not_distinct));
            }
            Ok(Some(values_not_distinct(
                &eval_constant_expr(left)?,
                &eval_constant_expr(right)?,
            )))
        }
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(SqlError::Unsupported(
                    "LIKE ANY is not supported".to_string(),
                ));
            }
            eval_like_values(
                eval_constant_expr(expr)?,
                eval_constant_expr(pattern)?,
                *negated,
                false,
                escape_char.as_ref(),
            )
        }
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(SqlError::Unsupported(
                    "ILIKE ANY is not supported".to_string(),
                ));
            }
            eval_like_values(
                eval_constant_expr(expr)?,
                eval_constant_expr(pattern)?,
                *negated,
                true,
                escape_char.as_ref(),
            )
        }
        Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
            "SIMILAR TO is not supported".to_string(),
        )),
        Expr::IsTrue(expr) => Ok(Some(matches!(eval_constant_truth(expr)?, Some(true)))),
        Expr::IsNotTrue(expr) => Ok(Some(!matches!(eval_constant_truth(expr)?, Some(true)))),
        Expr::IsFalse(expr) => Ok(Some(matches!(eval_constant_truth(expr)?, Some(false)))),
        Expr::IsNotFalse(expr) => Ok(Some(!matches!(eval_constant_truth(expr)?, Some(false)))),
        Expr::IsUnknown(expr) => Ok(Some(eval_constant_truth(expr)?.is_none())),
        Expr::IsNotUnknown(expr) => Ok(Some(eval_constant_truth(expr)?.is_some())),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            Ok(sql_not(eval_constant_truth(expr)?))
        }
        Expr::Value(_) | Expr::Cast { .. } | Expr::Function(_) | Expr::Case { .. } => {
            match eval_constant_expr(expr)? {
                SqlValue::Bool(value) => Ok(Some(value)),
                SqlValue::Null => Ok(None),
                other => Err(SqlError::Unsupported(format!(
                    "boolean expression returned non-boolean {}",
                    other.to_cell()
                ))),
            }
        }
        Expr::Nested(expr) => eval_constant_truth(expr),
        other => Err(SqlError::Unsupported(format!(
            "unsupported boolean expression {other}"
        ))),
    }
}

pub(crate) fn first_result_value(result: SqlResult) -> Option<SqlValue> {
    result.rows.first().and_then(|row| row.first()).cloned()
}

pub fn is_oid_alias_type(pg_type: &str) -> bool {
    matches!(
        pg_type,
        "regproc"
            | "regprocedure"
            | "regoper"
            | "regoperator"
            | "regclass"
            | "regcollation"
            | "regtype"
            | "regrole"
            | "regnamespace"
            | "regconfig"
            | "regdictionary"
    )
}

pub fn oid_alias_type_from_oid(oid: i32) -> Option<&'static str> {
    match oid {
        24 => Some("regproc"),
        2202 => Some("regprocedure"),
        2203 => Some("regoper"),
        2204 => Some("regoperator"),
        2205 => Some("regclass"),
        4191 => Some("regcollation"),
        2206 => Some("regtype"),
        4096 => Some("regrole"),
        4089 => Some("regnamespace"),
        3734 => Some("regconfig"),
        3769 => Some("regdictionary"),
        _ => None,
    }
}

pub fn resolve_oid_alias_value(db: &BicDb, pg_type: &str, value: SqlValue) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    if let Some(oid) = sql_value_i64(&value) {
        let oid = u32::try_from(oid)
            .map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))?;
        return Ok(SqlValue::Int(i64::from(oid)));
    }
    let input = value.to_cell();
    if let Ok(oid) = parse_pg_oid(&input) {
        return Ok(SqlValue::Int(i64::from(oid)));
    }
    let oid = match pg_type {
        "regclass" => resolve_regclass_oid(db, &input).and_then(|oid| u32::try_from(oid).ok()),
        "regtype" => resolve_regtype_oid(db, &input)?,
        "regnamespace" => resolve_regnamespace_oid(db, &input)?,
        "regrole" => resolve_regrole_oid(db, &input)?,
        "regcollation" => resolve_regcollation_oid(&input),
        "regconfig" => resolve_named_oid(&input, REGCONFIG_OBJECTS),
        "regdictionary" => resolve_named_oid(&input, REGDICTIONARY_OBJECTS),
        "regproc" | "regprocedure" => resolve_regprocedure_oid(db, pg_type, &input)?,
        "regoper" | "regoperator" => resolve_regoperator_oid(pg_type, &input)?,
        _ => None,
    };
    oid.map(|oid| SqlValue::Int(i64::from(oid)))
        .ok_or_else(|| undefined_oid_alias(pg_type, &input))
}

fn map_oid_alias_array_json<F>(value: JsonValue, scalar: &F) -> Result<JsonValue>
where
    F: Fn(JsonValue) -> Result<JsonValue>,
{
    match value {
        JsonValue::Array(values) => values
            .into_iter()
            .map(|value| map_oid_alias_array_json(value, scalar))
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),
        JsonValue::Object(mut object) if object.contains_key("$bicdb_array_input") => {
            let envelope = object
                .get_mut("$bicdb_array_input")
                .and_then(JsonValue::as_object_mut)
                .ok_or_else(|| invalid_array_text("catalog-reference array envelope"))?;
            let values = envelope
                .remove("value")
                .ok_or_else(|| invalid_array_text("catalog-reference array value"))?;
            envelope.insert(
                "value".to_string(),
                map_oid_alias_array_json(values, scalar)?,
            );
            Ok(JsonValue::Object(object))
        }
        JsonValue::Null => Ok(JsonValue::Null),
        value => scalar(value),
    }
}

pub fn resolve_oid_alias_array_value(
    db: &BicDb,
    element_type: &str,
    value: SqlValue,
) -> Result<SqlValue> {
    let SqlValue::Json(value) = cast_value_to_array(value, "text")? else {
        unreachable!("array casts return JSON")
    };
    map_oid_alias_array_json(value, &|value| {
        let input = match value {
            JsonValue::String(value) => SqlValue::String(value),
            JsonValue::Number(value) => SqlValue::Int(
                value
                    .as_i64()
                    .ok_or_else(|| SqlError::numeric_value_out_of_range("OID out of range"))?,
            ),
            value => {
                return Err(SqlError::invalid_text_representation(
                    element_type,
                    value.to_string(),
                ));
            }
        };
        let resolved = resolve_oid_alias_value(db, element_type, input)?;
        Ok(JsonValue::from(
            sql_value_i64(&resolved).expect("OID alias resolver returns an integer"),
        ))
    })
    .map(SqlValue::Json)
}

pub fn render_oid_alias_array_value(
    db: &BicDb,
    element_type: &str,
    value: &SqlValue,
) -> Result<SqlValue> {
    let SqlValue::Json(value) = value else {
        return Err(invalid_array_text("catalog-reference array value"));
    };
    map_oid_alias_array_json(value.clone(), &|value| {
        let input = match value {
            JsonValue::String(value) => SqlValue::String(value),
            JsonValue::Number(value) => SqlValue::Int(
                value
                    .as_i64()
                    .ok_or_else(|| SqlError::numeric_value_out_of_range("OID out of range"))?,
            ),
            value => {
                return Err(SqlError::invalid_text_representation(
                    element_type,
                    value.to_string(),
                ));
            }
        };
        render_oid_alias_value(db, element_type, &input).map(JsonValue::String)
    })
    .map(SqlValue::Json)
}

pub fn render_oid_alias_value(db: &BicDb, pg_type: &str, value: &SqlValue) -> Result<String> {
    if matches!(value, SqlValue::Null) {
        return Ok(String::new());
    }
    let resolved = match resolve_oid_alias_value(db, pg_type, value.clone()) {
        Ok(resolved) => resolved,
        Err(_) if matches!(value, SqlValue::String(_)) => return Ok(value.to_cell()),
        Err(error) => return Err(error),
    };
    let oid = sql_value_i64(&resolved).expect("OID alias resolver returns an integer");
    if oid == 0 && matches!(pg_type, "regproc" | "regprocedure" | "regclass" | "regtype") {
        return Ok("-".to_string());
    }
    let rendered = match pg_type {
        "regclass" => resolve_regclass_name(db, oid)?,
        "regtype" => resolve_regtype_name(db, oid)?,
        "regnamespace" => resolve_regnamespace_name(db, oid)?,
        "regrole" => resolve_regrole_name(db, oid)?,
        "regcollation" => resolve_regcollation_name(oid),
        "regconfig" => resolve_named_oid_output(oid, REGCONFIG_OBJECTS),
        "regdictionary" => resolve_named_oid_output(oid, REGDICTIONARY_OBJECTS),
        "regproc" | "regprocedure" => resolve_regprocedure_name(db, pg_type, oid)?,
        "regoper" | "regoperator" => resolve_regoperator_name(pg_type, oid),
        _ => None,
    };
    Ok(rendered.unwrap_or_else(|| oid.to_string()))
}

pub fn oid_alias_numeric_value(db: &BicDb, pg_type: &str, value: &SqlValue) -> Result<u32> {
    let resolved = resolve_oid_alias_value(db, pg_type, value.clone())?;
    let oid = sql_value_i64(&resolved)
        .ok_or_else(|| SqlError::invalid_text_representation(pg_type, value.to_cell()))?;
    u32::try_from(oid).map_err(|_| SqlError::numeric_value_out_of_range("OID out of range"))
}

const REGCONFIG_OBJECTS: &[(u32, &str)] = &[
    (3748, "simple"),
    (810_100, "danish"),
    (810_101, "dutch"),
    (810_102, "english"),
    (810_103, "finnish"),
    (810_104, "french"),
    (810_105, "german"),
    (810_106, "hungarian"),
    (810_107, "italian"),
    (810_108, "norwegian"),
    (810_109, "portuguese"),
    (810_110, "romanian"),
    (810_111, "russian"),
    (810_112, "spanish"),
    (810_113, "swedish"),
    (810_114, "turkish"),
];

pub(crate) fn builtin_regconfig_name(oid: i64) -> Option<&'static str> {
    REGCONFIG_OBJECTS
        .iter()
        .find_map(|(candidate, name)| (i64::from(*candidate) == oid).then_some(*name))
}

const REGDICTIONARY_OBJECTS: &[(u32, &str)] = &[
    (3765, "simple"),
    (810_200, "danish_stem"),
    (810_201, "dutch_stem"),
    (810_202, "english_stem"),
    (810_203, "finnish_stem"),
    (810_204, "french_stem"),
    (810_205, "german_stem"),
    (810_206, "hungarian_stem"),
    (810_207, "italian_stem"),
    (810_208, "norwegian_stem"),
    (810_209, "portuguese_stem"),
    (810_210, "romanian_stem"),
    (810_211, "russian_stem"),
    (810_212, "spanish_stem"),
    (810_213, "swedish_stem"),
    (810_214, "turkish_stem"),
];

const REGOPERATOR_OBJECTS: &[(u32, &str, &str, &str)] = &[
    (96, "=", "int4", "int4"),
    (97, "<", "int4", "int4"),
    (98, "=", "text", "text"),
    (518, "<>", "int4", "int4"),
    (521, ">", "int4", "int4"),
    (522, "<=", "int4", "int4"),
    (525, ">=", "int4", "int4"),
    (551, "+", "int4", "int4"),
    (552, "-", "int4", "int4"),
    (514, "*", "int4", "int4"),
    (528, "/", "int4", "int4"),
    (529, "%", "int4", "int4"),
    (654, "||", "text", "text"),
    (3247, "?", "jsonb", "text"),
];

fn normalize_alias_name(value: &str) -> String {
    let name = value.trim().rsplit('.').next().unwrap_or(value).trim();
    if name.len() >= 2 && name.starts_with('"') && name.ends_with('"') {
        name[1..name.len() - 1].replace("\"\"", "\"")
    } else {
        name.to_ascii_lowercase()
    }
}

fn alias_schema_and_name(value: &str) -> (Option<String>, String) {
    let value = value.trim();
    let mut quoted = false;
    let mut separator = None;
    let mut chars = value.char_indices().peekable();
    while let Some((index, ch)) = chars.next() {
        if ch == '"' {
            if quoted && chars.peek().is_some_and(|(_, next)| *next == '"') {
                chars.next();
            } else {
                quoted = !quoted;
            }
        } else if ch == '.' && !quoted {
            separator = Some(index);
        }
    }
    match separator {
        Some(index) => (
            Some(normalize_alias_name(&value[..index])),
            normalize_alias_name(&value[index + 1..]),
        ),
        None => (None, normalize_alias_name(value)),
    }
}

fn alias_schema_is(schema: Option<&str>, expected: &str) -> bool {
    schema.is_none_or(|schema| schema.eq_ignore_ascii_case(expected))
}

fn resolve_named_oid(value: &str, objects: &[(u32, &str)]) -> Option<u32> {
    let name = normalize_alias_name(value);
    objects
        .iter()
        .find_map(|(oid, candidate)| candidate.eq_ignore_ascii_case(&name).then_some(*oid))
}

fn resolve_builtin_oid_alias(pg_type: &str, value: &str) -> Option<u32> {
    match pg_type {
        "regclass" => {
            let relation = regclass_relation_name(value);
            virtual_catalog_table_oid(&relation).and_then(|oid| u32::try_from(oid).ok())
        }
        "regtype" => pg_type_regtype_name(value)
            .map(|name| pg_type_oid(&name))
            .and_then(|oid| u32::try_from(oid).ok()),
        "regnamespace" => {
            let name = normalize_alias_name(value);
            matches!(
                name.as_str(),
                "public" | "pg_catalog" | "information_schema"
            )
            .then(|| namespace_oid(&name))
            .and_then(|oid| u32::try_from(oid).ok())
        }
        "regrole" => {
            let name = normalize_alias_name(value);
            (normalize_role_name(&name) == current_role_name())
                .then(|| role_oid(&name))
                .and_then(|oid| u32::try_from(oid).ok())
        }
        "regcollation" => resolve_regcollation_oid(value),
        "regconfig" => resolve_named_oid(value, REGCONFIG_OBJECTS),
        "regdictionary" => resolve_named_oid(value, REGDICTIONARY_OBJECTS),
        "regproc" | "regprocedure" => {
            let (name, signature) = split_alias_signature(value);
            if pg_type == "regprocedure" && signature.is_none() {
                return None;
            }
            let (schema, name) = alias_schema_and_name(name);
            if !alias_schema_is(schema.as_deref(), "pg_catalog") {
                return None;
            }
            let nargs = signature.as_ref().map(Vec::len);
            let mut matches = PG_BUILTIN_PROC_ROWS
                .iter()
                .filter(|(_, candidate, _, count)| {
                    *candidate == name && nargs.is_none_or(|nargs| *count == nargs as i64)
                });
            let first = matches
                .next()
                .and_then(|(oid, _, _, _)| u32::try_from(*oid).ok());
            (matches.next().is_none()).then_some(first).flatten()
        }
        "regoper" | "regoperator" => resolve_regoperator_oid(pg_type, value).ok().flatten(),
        _ => None,
    }
}

fn resolve_named_oid_output(oid: i64, objects: &[(u32, &str)]) -> Option<String> {
    objects
        .iter()
        .find_map(|(candidate, name)| (i64::from(*candidate) == oid).then(|| (*name).to_string()))
}

fn resolve_regtype_oid(db: &BicDb, value: &str) -> Result<Option<u32>> {
    let (schema, relation) = alias_schema_and_name(value.trim_end_matches("[]"));
    if alias_schema_is(schema.as_deref(), "pg_catalog") {
        if let Some(pg_type) = pg_type_regtype_name(value) {
            return Ok(u32::try_from(pg_type_oid(&pg_type)).ok());
        }
    }
    if let Some(oid) = pg_user_type_oid_by_name(db, value)?.and_then(|oid| u32::try_from(oid).ok())
    {
        return Ok(Some(oid));
    }
    // Every table also defines its row type, so `'schema.table'::regtype` has
    // to resolve. Outside `public` the stored relation name is the
    // schema-isolated physical one, so match what it encodes too.
    Ok(list_schemas(db)?
        .into_iter()
        .find(|candidate| {
            let logical = crate::eval::rewrite_helpers::logical_relation_name(&candidate.name);
            (candidate.name.eq_ignore_ascii_case(&relation)
                || logical
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&relation)))
                && schema.as_deref().map_or_else(
                    || candidate.schema_name.eq_ignore_ascii_case("public"),
                    |requested| candidate.schema_name.eq_ignore_ascii_case(requested),
                )
        })
        .and_then(|candidate| u32::try_from(candidate.row_type_oid()).ok()))
}

/// How a relation should be spelled in catalog output: the logical name a user
/// wrote, not the schema-isolated physical name it is stored under.
fn relation_display_name(schema: &TableSchema) -> String {
    crate::eval::rewrite_helpers::logical_relation_name(&schema.name)
        .unwrap_or_else(|| schema.name.clone())
}

fn resolve_regtype_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    if let Some(name) = list_schemas(db)?.into_iter().find_map(|schema| {
        let display = relation_display_name(&schema);
        if schema.row_type_oid() == oid {
            Some(if schema.schema_name.eq_ignore_ascii_case("public") {
                display
            } else {
                format!("{}.{}", schema.schema_name, display)
            })
        } else if schema.row_array_type_oid() == oid {
            Some(if schema.schema_name.eq_ignore_ascii_case("public") {
                format!("_{display}")
            } else {
                format!("{}._{}", schema.schema_name, display)
            })
        } else {
            None
        }
    }) {
        return Ok(Some(name));
    }
    let formatted = eval_db_catalog_function_value(
        db,
        "format_type",
        &[SqlValue::Int(oid), SqlValue::Int(-1)],
        None,
        None,
    )?;
    Ok(formatted.and_then(|value| {
        let text = value.to_cell();
        (text != "???").then_some(text)
    }))
}

fn resolve_regnamespace_oid(db: &BicDb, value: &str) -> Result<Option<u32>> {
    let name = normalize_alias_name(value);
    if matches!(
        name.as_str(),
        "public" | "pg_catalog" | "information_schema"
    ) {
        return Ok(u32::try_from(namespace_oid(&name)).ok());
    }
    let exists = list_namespaces(db)?
        .into_iter()
        .any(|namespace| namespace.name == name)
        || list_schemas(db)?
            .into_iter()
            .any(|schema| schema.schema_name == name);
    Ok(exists
        .then(|| namespace_oid(&name))
        .and_then(|oid| u32::try_from(oid).ok()))
}

fn resolve_regnamespace_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    for name in ["public", "pg_catalog", "information_schema"] {
        if namespace_oid(name) == oid {
            return Ok(Some(name.to_string()));
        }
    }
    let mut names = list_namespaces(db)?
        .into_iter()
        .map(|namespace| namespace.name)
        .chain(
            list_schemas(db)?
                .into_iter()
                .map(|schema| schema.schema_name),
        )
        .collect::<BTreeSet<_>>();
    Ok(names.take(
        &names
            .iter()
            .find(|name| namespace_oid(name) == oid)
            .cloned()
            .unwrap_or_default(),
    ))
}

fn resolve_regrole_oid(db: &BicDb, value: &str) -> Result<Option<u32>> {
    let name = normalize_role_name(&normalize_alias_name(value));
    Ok(list_roles(db)?
        .into_iter()
        .find(|role| normalize_role_name(&role.name) == name)
        .and_then(|role| u32::try_from(role_oid(&role.name)).ok()))
}

fn resolve_regrole_name(db: &BicDb, oid: i64) -> Result<Option<String>> {
    Ok(list_roles(db)?
        .into_iter()
        .find(|role| role_oid(&role.name) == oid)
        .map(|role| role.name))
}

fn resolve_regcollation_oid(value: &str) -> Option<u32> {
    let name = normalize_alias_name(value);
    pg_collation_rows().into_iter().find_map(|row| {
        (row.get("collname")?.to_cell() == name)
            .then(|| row.get("oid").and_then(sql_value_i64))
            .flatten()
            .and_then(|oid| u32::try_from(oid).ok())
    })
}

fn resolve_regcollation_name(oid: i64) -> Option<String> {
    pg_collation_rows().into_iter().find_map(|row| {
        (row.get("oid").and_then(sql_value_i64) == Some(oid))
            .then(|| {
                row.get("collname")
                    .map(SqlValue::to_cell)
                    .map(|name| quote_identifier_if_needed(&name))
            })
            .flatten()
    })
}

fn quote_identifier_if_needed(name: &str) -> String {
    let unquoted = name.chars().enumerate().all(|(index, ch)| {
        ch == '_' || ch.is_ascii_lowercase() || (index > 0 && ch.is_ascii_digit())
    });
    if unquoted {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn builtin_proc_arg_types(oid: i64) -> Option<&'static [&'static str]> {
    match oid {
        6342 | 6343 => Some(&["uuid"]),
        6430 => Some(&["interval"]),
        _ => None,
    }
}
