//! Pure JSON/JSONB scalar-function helpers.
//!
//! This module deliberately has no executor dependencies. Function names that
//! it does not recognize return `Ok(None)` so the evaluator can continue
//! dispatching through the other function families.

use crate::{JsonPathElement, PgJsonText, Result, SqlError, SqlValue};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgumentClause,
    FunctionArguments, JsonNullClause,
};

/// Render JSONB using PostgreSQL's canonical text form. Object keys use the
/// JSONB length-then-byte ordering and numeric exponents are expanded.
pub fn postgres_jsonb_text(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Number(value) => postgres_jsonb_number_text(value),
        JsonValue::String(value) => {
            serde_json::to_string(value).expect("JSON string serialization")
        }
        JsonValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(postgres_jsonb_text)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        JsonValue::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| {
                left.len()
                    .cmp(&right.len())
                    .then_with(|| left.as_bytes().cmp(right.as_bytes()))
            });
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}: {}",
                        serde_json::to_string(key).expect("JSON key serialization"),
                        postgres_jsonb_text(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

/// Render the compact JSONB cell representation independently of
/// `serde_json`'s optional `preserve_order` feature. This is intentionally
/// lexicographic to preserve BicDB's historical compact cell encoding; the
/// PostgreSQL wire/text renderer above applies PostgreSQL's length-then-byte
/// object ordering and spacing.
pub(crate) fn compact_jsonb_cell_text(value: &JsonValue) -> String {
    fn append(value: &JsonValue, output: &mut String) {
        match value {
            JsonValue::Null => output.push_str("null"),
            JsonValue::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            JsonValue::Number(value) => output.push_str(&value.to_string()),
            JsonValue::String(value) => output.push_str(
                &serde_json::to_string(value).expect("serializing a JSON string cannot fail"),
            ),
            JsonValue::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    append(value, output);
                }
                output.push(']');
            }
            JsonValue::Object(values) => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                output.push('{');
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    if index != 0 {
                        output.push(',');
                    }
                    output.push_str(
                        &serde_json::to_string(key)
                            .expect("serializing a JSON object key cannot fail"),
                    );
                    output.push(':');
                    append(value, output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    append(value, &mut output);
    output
}

pub fn postgres_jsonb_pretty_text(value: &JsonValue) -> String {
    fn indent(output: &mut String, depth: usize) {
        for _ in 0..depth {
            output.push_str("    ");
        }
    }

    fn append(value: &JsonValue, depth: usize, output: &mut String) {
        match value {
            JsonValue::Array(values) if !values.is_empty() => {
                output.push_str("[\n");
                for (index, value) in values.iter().enumerate() {
                    indent(output, depth + 1);
                    append(value, depth + 1, output);
                    if index + 1 != values.len() {
                        output.push(',');
                    }
                    output.push('\n');
                }
                indent(output, depth);
                output.push(']');
            }
            JsonValue::Object(values) if !values.is_empty() => {
                let mut entries = values.iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| {
                    left.len()
                        .cmp(&right.len())
                        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
                });
                let entry_count = entries.len();
                output.push_str("{\n");
                for (index, (key, value)) in entries.into_iter().enumerate() {
                    indent(output, depth + 1);
                    output.push_str(&serde_json::to_string(key).expect("JSON key serialization"));
                    output.push_str(": ");
                    append(value, depth + 1, output);
                    if index + 1 != entry_count {
                        output.push(',');
                    }
                    output.push('\n');
                }
                indent(output, depth);
                output.push('}');
            }
            _ => output.push_str(&postgres_jsonb_text(value)),
        }
    }

    let mut output = String::new();
    append(value, 0, &mut output);
    output
}

pub(crate) fn jsonb_index_key_token(key: &str) -> String {
    format!("k{}:{key}", key.len())
}

pub(crate) fn jsonb_index_terms(value: &SqlValue) -> Result<Vec<String>> {
    fn append(value: &JsonValue, terms: &mut std::collections::BTreeSet<String>) {
        match value {
            JsonValue::Object(object) => {
                for (key, value) in object {
                    terms.insert(jsonb_index_key_token(key));
                    append(value, terms);
                }
            }
            JsonValue::Array(values) => {
                for value in values {
                    if let JsonValue::String(value) = value {
                        terms.insert(jsonb_index_key_token(value));
                    }
                    append(value, terms);
                }
            }
            value => {
                terms.insert(format!("v{}", crate::canonical_json_value_key(value)));
            }
        }
    }

    let SqlValue::Json(value) = value else {
        return Err(argument_type_error("JSONB GIN index", 1, "jsonb"));
    };
    let mut terms = std::collections::BTreeSet::new();
    append(value, &mut terms);
    Ok(terms.into_iter().collect())
}

fn postgres_jsonb_number_text(number: &JsonNumber) -> String {
    const MAX_RENDERED_ZEROES: usize = 131_072;

    let rendered = number.to_string();
    let Some(exponent_offset) = rendered.find(['e', 'E']) else {
        return normalize_jsonb_negative_zero(rendered);
    };
    let mantissa = &rendered[..exponent_offset];
    let Ok(exponent) = rendered[exponent_offset + 1..].parse::<i64>() else {
        return rendered;
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let digits = format!("{whole}{fraction}");
    let Ok(fraction_len) = i64::try_from(fraction.len()) else {
        return rendered;
    };
    let Some(scale) = fraction_len.checked_sub(exponent) else {
        return rendered;
    };
    let magnitude = if scale <= 0 {
        if digits.chars().all(|character| character == '0') {
            return "0".to_string();
        }
        let Ok(zeroes) = usize::try_from(scale.unsigned_abs()) else {
            return rendered;
        };
        if zeroes > MAX_RENDERED_ZEROES {
            return rendered;
        }
        format!("{}{}", digits.trim_start_matches('0'), "0".repeat(zeroes))
    } else {
        let Ok(scale) = usize::try_from(scale) else {
            return rendered;
        };
        if digits.chars().all(|character| character == '0') {
            if scale > MAX_RENDERED_ZEROES {
                return rendered;
            }
            return format!("0.{}", "0".repeat(scale));
        }
        if digits.len() > scale {
            let split = digits.len() - scale;
            let integer = digits[..split].trim_start_matches('0');
            format!(
                "{}.{}",
                if integer.is_empty() { "0" } else { integer },
                &digits[split..]
            )
        } else {
            let zeroes = scale - digits.len();
            if zeroes > MAX_RENDERED_ZEROES {
                return rendered;
            }
            format!("0.{}{}", "0".repeat(zeroes), digits)
        }
    };
    normalize_jsonb_negative_zero(if negative {
        format!("-{magnitude}")
    } else {
        magnitude
    })
}

fn normalize_jsonb_negative_zero(rendered: String) -> String {
    if let Some(unsigned) = rendered.strip_prefix('-') {
        if unsigned
            .chars()
            .all(|character| matches!(character, '0' | '.'))
        {
            return unsigned.to_string();
        }
    }
    rendered
}

/// Evaluate JSON/JSONB functions that only depend on their argument values.
pub(crate) fn eval_json_function_value(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if let Some(value) = crate::eval_jsonpath_function_value(name, args)? {
        return Ok(Some(value));
    }
    let value = match name {
        "json_build_object" => build_object(args, false)?,
        "jsonb_build_object" => build_object(args, true)?,
        "json_build_array" => build_array(args, false)?,
        "jsonb_build_array" => build_array(args, true)?,
        "array_to_json" => array_to_json(name, args)?,
        "json_object" => json_object(name, args)?,
        "row_to_json" => {
            if !matches!(args.len(), 1 | 2) {
                return Err(data_error(
                    "42883",
                    format!("function {name} expects one or two arguments"),
                ));
            }
            if matches!(args[0], SqlValue::Null) {
                SqlValue::Null
            } else {
                let SqlValue::Composite(composite) = &args[0] else {
                    return Err(argument_type_error(name, 1, "record"));
                };
                let pretty = match args.get(1) {
                    None => false,
                    Some(SqlValue::Bool(value)) => *value,
                    Some(SqlValue::Null) => return Ok(Some(SqlValue::Null)),
                    Some(_) => return Err(argument_type_error(name, 2, "boolean")),
                };
                let fields = composite
                    .fields
                    .iter()
                    .map(|field| (field.name.clone(), field.value.clone()))
                    .collect::<Vec<_>>();
                composite_to_json(&fields, false, pretty)?
            }
        }
        "to_json" | "to_jsonb" => {
            expect_exact_args(name, args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                SqlValue::Null
            } else if name == "to_json" {
                json_text_result(sql_value_to_json_text(&args[0]))?
            } else {
                SqlValue::Json(sql_value_to_json(&args[0]))
            }
        }
        "json_array_length" | "jsonb_array_length" => json_array_length(name, args)?,
        "json_extract_path"
        | "json_extract_path_text"
        | "jsonb_extract_path"
        | "jsonb_extract_path_text" => json_extract_path(name, args)?,
        "json_array_element"
        | "json_array_element_text"
        | "jsonb_array_element"
        | "jsonb_array_element_text"
        | "json_object_field"
        | "json_object_field_text"
        | "jsonb_object_field"
        | "jsonb_object_field_text" => json_field_or_element(name, args)?,
        "json_send" | "jsonb_send" => json_send(name, args)?,
        "bicdb_json_format" => json_format_input(name, args)?,
        name if name.starts_with("bicdb_json_array_query_") => {
            expect_exact_args(name, args, 1)?;
            args[0].clone()
        }
        "json_strip_nulls" => json_strip_nulls(name, args)?,
        "jsonb_object" => jsonb_object(name, args)?,
        "jsonb_set" => jsonb_set(name, args)?,
        "jsonb_set_lax" => jsonb_set_lax(name, args)?,
        "jsonb_insert" => jsonb_insert(name, args)?,
        "jsonb_strip_nulls" => jsonb_strip_nulls(name, args)?,
        "jsonb_pretty" => jsonb_pretty(name, args)?,
        "jsonb_cmp" | "jsonb_eq" | "jsonb_ne" | "jsonb_gt" | "jsonb_ge" | "jsonb_lt"
        | "jsonb_le" => jsonb_compare_function(name, args)?,
        "jsonb_concat" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::Concat)?,
        "jsonb_contains" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::Contains)?,
        "jsonb_contained" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::Contained)?,
        "jsonb_exists" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::Exists)?,
        "jsonb_exists_any" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::ExistsAny)?,
        "jsonb_exists_all" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::ExistsAll)?,
        "jsonb_delete" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::Delete)?,
        "jsonb_delete_path" => jsonb_binary_wrapper(name, args, JsonbBinaryFunction::DeletePath)?,
        "bicdb_is_json" => is_json_predicate(name, args)?,
        _ => return Ok(None),
    };
    Ok(Some(value))
}

pub(crate) fn eval_json_function_call_value(
    function: &Function,
    args: &[SqlValue],
) -> Result<Option<SqlValue>> {
    let name = function.name.to_string().to_ascii_lowercase();
    let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    let normalized_args = normalize_jsonb_literal_arguments(function, bare_name, args)?;
    let args = normalized_args.as_deref().unwrap_or(args);
    if bare_name.ends_with("_encoding_error") {
        return Err(json_output_encoding_error());
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return eval_json_function_value(&name, args);
    };
    let null_clause = arguments.clauses.iter().find_map(|clause| match clause {
        FunctionArgumentClause::JsonNullClause(clause) => Some(clause),
        _ => None,
    });
    let returning = arguments.clauses.iter().find_map(|clause| match clause {
        FunctionArgumentClause::JsonReturningClause(clause) => Some(&clause.data_type),
        _ => None,
    });

    let result = match bare_name {
        "json_array" => {
            let absent_on_null = !matches!(null_clause, Some(JsonNullClause::NullOnNull));
            let values = args
                .iter()
                .filter(|value| !absent_on_null || !matches!(value, SqlValue::Null))
                .cloned()
                .collect::<Vec<_>>();
            build_array(&values, false)?
        }
        "json_object" | "json_object_unique"
            if arguments.args.is_empty()
                || arguments.args.iter().any(|arg| {
                    matches!(
                        arg,
                        FunctionArg::ExprNamed { .. } | FunctionArg::Named { .. }
                    )
                }) =>
        {
            let absent_on_null = matches!(null_clause, Some(JsonNullClause::AbsentOnNull));
            let mut values = Vec::with_capacity(args.len());
            for pair in args.chunks_exact(2) {
                if absent_on_null && matches!(pair[1], SqlValue::Null) {
                    continue;
                }
                values.extend_from_slice(pair);
            }
            build_object_with_unique(&values, false, bare_name == "json_object_unique")?
        }
        "json" | "json_unique" => {
            expect_exact_args(bare_name, args, 1)?;
            let result = match &args[0] {
                SqlValue::Null => SqlValue::Null,
                SqlValue::String(value) if value.starts_with("\\x") => {
                    let bytes = crate::parse_bytea_text(value).map_err(|_| {
                        SqlError::InvalidTextRepresentation(
                            "invalid input syntax for type bytea".to_string(),
                        )
                    })?;
                    let raw = String::from_utf8(bytes).map_err(|_| {
                        data_error("22021", "invalid byte sequence for encoding UTF8")
                    })?;
                    json_text_result(raw)?
                }
                SqlValue::String(value) => json_text_result(value.clone())?,
                SqlValue::JsonText(value) => SqlValue::JsonText(value.clone()),
                _ => return Err(argument_type_error(bare_name, 1, "character string")),
            };
            if bare_name == "json_unique"
                && matches!(&result, SqlValue::JsonText(value) if !json_text_has_unique_keys(value.raw()))
            {
                return Err(data_error("22030", "duplicate JSON object key value"));
            }
            result
        }
        "json_scalar" => {
            expect_exact_args(bare_name, args, 1)?;
            if matches!(args[0], SqlValue::Null) {
                SqlValue::Null
            } else {
                json_text_result(sql_value_to_json_text(&args[0]))?
            }
        }
        "json_serialize" => {
            expect_exact_args(bare_name, args, 1)?;
            match &args[0] {
                SqlValue::Null => SqlValue::Null,
                SqlValue::JsonText(value) => SqlValue::String(value.raw().to_string()),
                SqlValue::Json(value) => SqlValue::String(postgres_jsonb_text(value)),
                _ => return Err(argument_type_error(bare_name, 1, "json or jsonb")),
            }
        }
        _ => return eval_json_function_value(&name, args),
    };
    apply_json_returning(result, returning).map(Some)
}

fn normalize_jsonb_literal_arguments(
    function: &Function,
    name: &str,
    args: &[SqlValue],
) -> Result<Option<Vec<SqlValue>>> {
    let jsonb_positions: &[usize] = match name {
        "jsonb_insert" | "jsonb_set" | "jsonb_set_lax" => &[0, 2],
        "jsonb_contains" | "jsonb_contained" | "jsonb_concat" | "jsonb_cmp" | "jsonb_eq"
        | "jsonb_ne" | "jsonb_gt" | "jsonb_ge" | "jsonb_lt" | "jsonb_le" => &[0, 1],
        "jsonb_exists"
        | "jsonb_exists_any"
        | "jsonb_exists_all"
        | "jsonb_delete"
        | "jsonb_delete_path"
        | "jsonb_array_element"
        | "jsonb_array_element_text"
        | "jsonb_object_field"
        | "jsonb_object_field_text"
        | "jsonb_send" => &[0],
        _ => return Ok(None),
    };
    let expressions = crate::function_args(function);
    let mut normalized = args.to_vec();
    let mut changed = false;
    for &position in jsonb_positions {
        let (Some(Expr::Value(_)), Some(SqlValue::String(raw))) =
            (expressions.get(position), args.get(position))
        else {
            continue;
        };
        normalized[position] = serde_json::from_str(raw)
            .map(SqlValue::Json)
            .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?;
        changed = true;
    }
    Ok(changed.then_some(normalized))
}

pub(crate) fn json_output_encoding_error() -> SqlError {
    SqlError::Unsupported("cannot set JSON encoding for non-bytea output types".to_string())
}

pub(crate) fn apply_json_returning(
    value: SqlValue,
    returning: Option<&DataType>,
) -> Result<SqlValue> {
    let Some(returning) = returning else {
        return Ok(value);
    };
    let pg_type = crate::pg_type_from_data_type(returning)?.0;
    if matches!(pg_type.as_str(), "text" | "varchar" | "bpchar") {
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        return crate::cast_value_with_assignment_semantics(
            SqlValue::String(value.to_cell()),
            returning,
        );
    }
    apply_json_returning_pg_type(value, Some(&pg_type))
}

pub(crate) fn apply_json_returning_pg_type(
    value: SqlValue,
    returning: Option<&str>,
) -> Result<SqlValue> {
    let Some(pg_type) = returning else {
        return Ok(value);
    };
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let raw = value.to_cell();
    match pg_type {
        "json" => json_text_result(raw),
        "jsonb" => serde_json::from_str(&raw)
            .map(SqlValue::Json)
            .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string())),
        "text" | "varchar" | "bpchar" => Ok(SqlValue::String(raw)),
        "bytea" => Ok(SqlValue::String(crate::format_bytea_hex(raw.as_bytes()))),
        _ => Err(SqlError::cannot_coerce(format!(
            "cannot use RETURNING {pg_type} with JSON constructor"
        ))),
    }
}

pub(crate) fn json_function_pg_type(name: &str) -> Option<&'static str> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    if let Some(pg_type) = crate::jsonpath_function_pg_type(name) {
        return Some(pg_type);
    }
    match name {
        "json_build_object"
        | "json_build_array"
        | "json_array"
        | "json"
        | "json_unique"
        | "json_scalar"
        | "array_to_json"
        | "json_object"
        | "json_object_unique"
        | "to_json"
        | "row_to_json"
        | "json_agg"
        | "json_agg_strict"
        | "json_object_agg"
        | "json_object_agg_strict"
        | "json_object_agg_unique"
        | "json_object_agg_unique_strict"
        | "json_strip_nulls" => Some("json"),
        "jsonb_build_object"
        | "jsonb_build_array"
        | "jsonb_object"
        | "to_jsonb"
        | "jsonb_set"
        | "jsonb_set_lax"
        | "jsonb_insert"
        | "jsonb_concat"
        | "jsonb_delete"
        | "jsonb_delete_path"
        | "jsonb_strip_nulls"
        | "jsonb_agg"
        | "jsonb_agg_strict"
        | "jsonb_object_agg"
        | "jsonb_object_agg_strict"
        | "jsonb_object_agg_unique"
        | "jsonb_object_agg_unique_strict" => Some("jsonb"),
        "json_array_length" | "jsonb_array_length" => Some("int4"),
        "json_array_element" | "json_object_field" => Some("json"),
        "jsonb_array_element" | "jsonb_object_field" => Some("jsonb"),
        "json_array_element_text"
        | "json_object_field_text"
        | "jsonb_array_element_text"
        | "jsonb_object_field_text" => Some("text"),
        "json_extract_path" => Some("json"),
        "jsonb_extract_path" => Some("jsonb"),
        "json_extract_path_text" | "jsonb_extract_path_text" => Some("text"),
        "jsonb_pretty" | "json_serialize" => Some("text"),
        "json_send" | "jsonb_send" => Some("bytea"),
        "jsonb_contains" | "jsonb_contained" | "jsonb_exists" | "jsonb_exists_any"
        | "jsonb_exists_all" => Some("bool"),
        "jsonb_eq" | "jsonb_ne" | "jsonb_gt" | "jsonb_ge" | "jsonb_lt" | "jsonb_le" => Some("bool"),
        "jsonb_cmp" => Some("int4"),
        "bicdb_is_json" => Some("bool"),
        "bicdb_json_array_query_json" => Some("json"),
        "bicdb_json_array_query_jsonb" => Some("jsonb"),
        "bicdb_json_array_query_text" => Some("text"),
        "bicdb_json_array_query_varchar" => Some("varchar"),
        "bicdb_json_array_query_bpchar" => Some("bpchar"),
        "bicdb_json_array_query_bytea" => Some("bytea"),
        _ => None,
    }
}

fn is_json_predicate(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 3)?;
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let kind = match &args[1] {
        SqlValue::String(kind) => kind.as_str(),
        _ => return Err(argument_type_error(name, 2, "text")),
    };
    let unique = match args[2] {
        SqlValue::Bool(unique) => unique,
        _ => return Err(argument_type_error(name, 3, "boolean")),
    };
    let (raw, parsed) = match &args[0] {
        SqlValue::String(raw) => match PgJsonText::parse(raw.clone()) {
            Ok(value) => (raw.as_str(), value.parsed().clone()),
            Err(_) => return Ok(SqlValue::Bool(false)),
        },
        SqlValue::JsonText(value) => (value.raw(), value.parsed().clone()),
        SqlValue::Json(value) => return Ok(SqlValue::Bool(json_kind_matches(value, kind))),
        _ => {
            return Err(SqlError::ConstraintViolation {
                sqlstate: "42804",
                message: format!(
                    "cannot use type {} in IS JSON predicate",
                    match args[0] {
                        SqlValue::Int(_) => "integer",
                        SqlValue::Float(_) => "double precision",
                        SqlValue::Bool(_) => "boolean",
                        _ => "unknown",
                    }
                ),
                table: None,
                column: None,
                constraint: None,
            });
        }
    };
    if !json_kind_matches(&parsed, kind) {
        return Ok(SqlValue::Bool(false));
    }
    if unique {
        return Ok(SqlValue::Bool(json_text_has_unique_keys(raw)));
    }
    Ok(SqlValue::Bool(true))
}

fn json_text_has_unique_keys(raw: &str) -> bool {
    let (parseable, _) = crate::sanitize_pg_json_unicode(raw);
    serde_json::from_str::<UniqueJsonValue>(&parseable)
        .map(|value| value.0)
        .unwrap_or(false)
}

fn json_kind_matches(value: &JsonValue, kind: &str) -> bool {
    match kind {
        "value" => true,
        "scalar" => !matches!(value, JsonValue::Array(_) | JsonValue::Object(_)),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

struct UniqueJsonValue(bool);

impl<'de> serde::Deserialize<'de> for UniqueJsonValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = UniqueJsonValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON value")
            }

            fn visit_bool<E>(self, _: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_i64<E>(self, _: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_u64<E>(self, _: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_f64<E>(self, _: f64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_str<E>(self, _: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueJsonValue(true))
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut unique = true;
                while let Some(value) = sequence.next_element::<UniqueJsonValue>()? {
                    unique &= value.0;
                }
                Ok(UniqueJsonValue(unique))
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut keys = std::collections::HashSet::new();
                let mut unique = true;
                while let Some(key) = map.next_key::<String>()? {
                    unique &= keys.insert(key);
                    unique &= map.next_value::<UniqueJsonValue>()?.0;
                }
                Ok(UniqueJsonValue(unique))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

pub(crate) fn json_function_call_pg_type(function: &Function) -> Option<String> {
    if let FunctionArguments::List(arguments) = &function.args {
        if let Some(data_type) = arguments.clauses.iter().find_map(|clause| match clause {
            FunctionArgumentClause::JsonReturningClause(clause) => Some(&clause.data_type),
            _ => None,
        }) {
            return crate::pg_type_from_data_type(data_type)
                .ok()
                .map(|(name, _)| name);
        }
    }
    json_function_pg_type(&function.name.to_string().to_ascii_lowercase()).map(str::to_string)
}

pub(crate) fn json_text_array_path(value: &SqlValue) -> Result<Vec<Option<String>>> {
    json_path("JSON path operator", value)
}

pub(crate) fn composite_to_json(
    fields: &[(String, SqlValue)],
    binary: bool,
    pretty: bool,
) -> Result<SqlValue> {
    if binary {
        let object = fields
            .iter()
            .map(|(field, value)| (field.clone(), sql_value_to_json(value)))
            .collect::<JsonMap<_, _>>();
        return Ok(SqlValue::Json(JsonValue::Object(object)));
    }

    let separator = if pretty { ",\n " } else { "," };
    let entries = fields
        .iter()
        .map(|(field, value)| {
            format!(
                "{}:{}",
                serde_json::to_string(field).expect("JSON field serialization"),
                sql_value_to_json_text(value)
            )
        })
        .collect::<Vec<_>>();
    json_text_result(format!("{{{}}}", entries.join(separator)))
}

fn build_object(args: &[SqlValue], binary: bool) -> Result<SqlValue> {
    build_object_with_unique(args, binary, false)
}

fn build_object_with_unique(args: &[SqlValue], binary: bool, unique: bool) -> Result<SqlValue> {
    if args.len() % 2 != 0 {
        return Err(data_error(
            "22023",
            "argument list must have even number of elements",
        ));
    }

    let mut object = JsonMap::new();
    let mut keys = std::collections::HashSet::new();
    let mut text_entries = Vec::with_capacity(args.len() / 2);
    for (pair_index, pair) in args.chunks_exact(2).enumerate() {
        let key = object_key(&pair[0], pair_index * 2 + 1)?;
        if unique && !keys.insert(key.clone()) {
            return Err(data_error(
                "22030",
                format!("duplicate JSON object key value: \"{key}\""),
            ));
        }
        object.insert(key.clone(), sql_value_to_json(&pair[1]));
        if !binary {
            text_entries.push(format!(
                "{} : {}",
                serde_json::to_string(&key).expect("JSON key serialization"),
                sql_value_to_json_text(&pair[1])
            ));
        }
    }
    if binary {
        Ok(SqlValue::Json(JsonValue::Object(object)))
    } else {
        json_text_result(format!("{{{}}}", text_entries.join(", ")))
    }
}

fn build_array(args: &[SqlValue], binary: bool) -> Result<SqlValue> {
    if binary {
        return Ok(SqlValue::Json(JsonValue::Array(
            args.iter().map(sql_value_to_json).collect(),
        )));
    }
    json_text_result(format!(
        "[{}]",
        args.iter()
            .map(sql_value_to_json_text)
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

fn array_to_json(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 1, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let array = sql_array_json(name, &args[0])?;
    let pretty = match args.get(1) {
        None => false,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => return Err(argument_type_error(name, 2, "boolean")),
    };
    let raw = if pretty {
        pretty_top_level_json_array(array)
    } else {
        serde_json::to_string(array).expect("SQL array JSON serialization")
    };
    json_text_result(raw)
}

fn pretty_top_level_json_array(array: &[JsonValue]) -> String {
    let values = array
        .iter()
        .map(|value| serde_json::to_string(value).expect("SQL array JSON serialization"))
        .collect::<Vec<_>>();
    if values.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", values.join(",\n "))
    }
}

fn json_object(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 1, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }

    let pairs = if args.len() == 1 {
        json_object_pairs_from_single_array(name, sql_array_json(name, &args[0])?)?
    } else {
        let keys = sql_array_json(name, &args[0])?;
        let values = sql_array_json(name, &args[1])?;
        if keys.iter().any(JsonValue::is_array) || values.iter().any(JsonValue::is_array) {
            return Err(data_error("22023", "wrong number of array subscripts"));
        }
        if keys.len() != values.len() {
            return Err(data_error("22023", "mismatched array dimensions"));
        }
        keys.iter().zip(values).collect()
    };

    let mut entries = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        let Some(key) = key.as_str() else {
            if key.is_null() {
                return Err(data_error("22023", "null value not allowed for object key"));
            }
            return Err(argument_type_error(name, 1, "text[]"));
        };
        let value = match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::String(value) => {
                serde_json::to_string(value).expect("JSON object value serialization")
            }
            _ => return Err(argument_type_error(name, 1, "text[]")),
        };
        entries.push(format!(
            "{} : {value}",
            serde_json::to_string(key).expect("JSON object key serialization")
        ));
    }
    json_text_result(format!("{{{}}}", entries.join(", ")))
}

fn jsonb_object(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 1, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let pairs = if args.len() == 1 {
        json_object_pairs_from_single_array(name, sql_array_json(name, &args[0])?)?
    } else {
        let keys = sql_array_json(name, &args[0])?;
        let values = sql_array_json(name, &args[1])?;
        if keys.iter().any(JsonValue::is_array) || values.iter().any(JsonValue::is_array) {
            return Err(data_error("22023", "wrong number of array subscripts"));
        }
        if keys.len() != values.len() {
            return Err(data_error("22023", "mismatched array dimensions"));
        }
        keys.iter().zip(values).collect()
    };
    let mut object = JsonMap::new();
    for (key, value) in pairs {
        let Some(key) = key.as_str() else {
            if key.is_null() {
                return Err(data_error("22023", "null value not allowed for object key"));
            }
            return Err(argument_type_error(name, 1, "text[]"));
        };
        let value = match value {
            JsonValue::Null => JsonValue::Null,
            JsonValue::String(value) => JsonValue::String(value.clone()),
            _ => return Err(argument_type_error(name, 1, "text[]")),
        };
        object.insert(key.to_string(), value);
    }
    Ok(SqlValue::Json(JsonValue::Object(object)))
}

fn json_object_pairs_from_single_array<'a>(
    name: &str,
    values: &'a [JsonValue],
) -> Result<Vec<(&'a JsonValue, &'a JsonValue)>> {
    if values.iter().all(JsonValue::is_array) {
        return values
            .iter()
            .map(|row| {
                let row = row.as_array().expect("checked JSON array row");
                let [key, value] = row.as_slice() else {
                    return Err(data_error("22023", "array must have two columns"));
                };
                Ok((key, value))
            })
            .collect();
    }
    if values.iter().any(JsonValue::is_array) {
        return Err(argument_type_error(name, 1, "text[]"));
    }
    if values.len() % 2 != 0 {
        return Err(data_error(
            "22023",
            "array must have even number of elements",
        ));
    }
    Ok(values
        .chunks_exact(2)
        .map(|pair| (&pair[0], &pair[1]))
        .collect())
}

fn sql_array_json<'a>(name: &str, value: &'a SqlValue) -> Result<&'a [JsonValue]> {
    match value {
        SqlValue::Json(JsonValue::Array(values)) => Ok(values),
        SqlValue::Json(JsonValue::Object(object)) => object
            .get("$bicdb_array_input")
            .and_then(|input| input.get("value"))
            .and_then(JsonValue::as_array)
            .map(Vec::as_slice)
            .ok_or_else(|| argument_type_error(name, 1, "array")),
        _ => Err(argument_type_error(name, 1, "array")),
    }
}

fn json_text_result(raw: String) -> Result<SqlValue> {
    PgJsonText::parse(raw)
        .map(SqlValue::JsonText)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))
}

fn sql_value_to_json_text(value: &SqlValue) -> String {
    match value {
        SqlValue::JsonText(value) => value.raw().to_string(),
        SqlValue::Json(value) => postgres_jsonb_text(value),
        value => sql_value_to_json(value).to_string(),
    }
}

fn json_array_length(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 1)?;
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::JsonText(value) => match value.parsed() {
            JsonValue::Array(values) => Ok(SqlValue::Int(values.len() as i64)),
            _ => Err(data_error(
                "22023",
                "cannot get array length of a non-array",
            )),
        },
        SqlValue::Json(JsonValue::Array(values)) => Ok(SqlValue::Int(values.len() as i64)),
        SqlValue::Json(_) => Err(data_error(
            "22023",
            "cannot get array length of a non-array",
        )),
        _ => Err(argument_type_error(name, 1, "jsonb")),
    }
}

fn json_strip_nulls(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 1, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }

    let SqlValue::JsonText(target) = &args[0] else {
        return Err(argument_type_error(name, 1, "json"));
    };
    if target.has_invalid_unicode_escape() {
        return Err(data_error(
            "22P02",
            "invalid input syntax for type json: unsupported Unicode escape sequence",
        ));
    }
    let strip_in_arrays = match args.get(1) {
        None => false,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => return Err(argument_type_error(name, 2, "boolean")),
    };

    json_text_result(strip_json_text_nulls(target.raw(), strip_in_arrays)?)
}

fn strip_json_text_nulls(raw: &str, strip_in_arrays: bool) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') {
        let entries = json_text_object_entries(trimmed)?
            .into_iter()
            .filter(|(_, value)| value.get().trim() != "null")
            .map(|(key, value)| {
                Ok(format!(
                    "{}:{}",
                    serde_json::to_string(&key).expect("JSON object key serialization"),
                    strip_json_text_nulls(value.get(), strip_in_arrays)?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(format!("{{{}}}", entries.join(",")));
    }
    if trimmed.starts_with('[') {
        let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(trimmed)
            .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?
            .into_iter()
            .filter(|value| !strip_in_arrays || value.get().trim() != "null")
            .map(|value| strip_json_text_nulls(value.get(), strip_in_arrays))
            .collect::<Result<Vec<_>>>()?;
        return Ok(format!("[{}]", values.join(",")));
    }
    Ok(trimmed.to_string())
}

fn json_extract_path(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    if args.is_empty() {
        return Err(data_error("22023", format!("{name} requires an argument")));
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let binary = name.starts_with("jsonb_");
    let as_text = name.ends_with("_text");
    if let SqlValue::JsonText(value) = &args[0] {
        if binary {
            return Err(argument_type_error(name, 1, "jsonb"));
        }
        let path = args[1..]
            .iter()
            .enumerate()
            .map(|(position, element)| match element {
                SqlValue::String(element) => Ok(JsonPathElement::Text(element.clone())),
                _ => Err(argument_type_error(name, position + 2, "text")),
            })
            .collect::<Result<Vec<_>>>()?;
        return json_text_path_result(value, &path, as_text);
    }
    let mut current = match &args[0] {
        SqlValue::Json(value) if binary => value,
        _ => {
            return Err(argument_type_error(
                name,
                1,
                if binary { "jsonb" } else { "json" },
            ));
        }
    };
    for (position, element) in args[1..].iter().enumerate() {
        let SqlValue::String(element) = element else {
            return Err(argument_type_error(name, position + 2, "text"));
        };
        current = match current {
            JsonValue::Object(values) => match values.get(element) {
                Some(value) => value,
                None => return Ok(SqlValue::Null),
            },
            JsonValue::Array(values) => {
                let Ok(index) = element.parse::<i64>() else {
                    return Ok(SqlValue::Null);
                };
                let index = if index < 0 {
                    i64::try_from(values.len()).unwrap_or(i64::MAX) + index
                } else {
                    index
                };
                let Ok(index) = usize::try_from(index) else {
                    return Ok(SqlValue::Null);
                };
                match values.get(index) {
                    Some(value) => value,
                    None => return Ok(SqlValue::Null),
                }
            }
            _ => return Ok(SqlValue::Null),
        };
    }
    if as_text {
        return Ok(match current {
            JsonValue::Null => SqlValue::Null,
            JsonValue::String(value) => SqlValue::String(value.clone()),
            value => SqlValue::String(if binary {
                postgres_jsonb_text(value)
            } else {
                value.to_string()
            }),
        });
    }
    if binary {
        Ok(SqlValue::Json(current.clone()))
    } else {
        Ok(SqlValue::JsonText(PgJsonText::from_value(current.clone())))
    }
}

fn json_field_or_element(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let binary = name.starts_with("jsonb_");
    let array_element = name.contains("array_element");
    let as_text = name.ends_with("_text");
    let path = if array_element {
        let SqlValue::Int(index) = args[1] else {
            return Err(argument_type_error(name, 2, "integer"));
        };
        vec![JsonPathElement::Index(index)]
    } else {
        let SqlValue::String(ref field) = args[1] else {
            return Err(argument_type_error(name, 2, "text"));
        };
        vec![JsonPathElement::Key(field.clone())]
    };
    match &args[0] {
        SqlValue::JsonText(value) if !binary => json_text_path_result(value, &path, as_text),
        SqlValue::Json(_) if binary => Ok(crate::json_extract_path_value(&args[0], &path, as_text)),
        _ => Err(argument_type_error(
            name,
            1,
            if binary { "jsonb" } else { "json" },
        )),
    }
}

fn json_send(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 1)?;
    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::JsonText(value) => Ok(SqlValue::String(crate::format_bytea_hex(
            value.raw().as_bytes(),
        ))),
        SqlValue::Json(value) if name.ends_with("jsonb_send") || name == "jsonb_send" => {
            let mut encoded = vec![1_u8];
            encoded.extend_from_slice(postgres_jsonb_text(value).as_bytes());
            Ok(SqlValue::String(crate::format_bytea_hex(&encoded)))
        }
        _ => Err(argument_type_error(
            name,
            1,
            if name.ends_with("jsonb_send") || name == "jsonb_send" {
                "jsonb"
            } else {
                "json"
            },
        )),
    }
}

fn json_format_input(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 2)?;
    if matches!(args[0], SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let encoded = match args[1] {
        SqlValue::Bool(encoded) => encoded,
        _ => return Err(argument_type_error(name, 2, "boolean")),
    };
    let raw = match &args[0] {
        SqlValue::JsonText(value) if !encoded => return Ok(SqlValue::JsonText(value.clone())),
        SqlValue::Json(value) if !encoded => {
            return Ok(SqlValue::JsonText(PgJsonText::from_value(value.clone())));
        }
        SqlValue::String(value) if value.starts_with("\\x") => {
            let bytes = crate::parse_bytea_text(value).map_err(|_| {
                SqlError::InvalidTextRepresentation(
                    "invalid input syntax for type bytea".to_string(),
                )
            })?;
            String::from_utf8(bytes)
                .map_err(|_| data_error("22021", "invalid byte sequence for encoding UTF8"))?
        }
        SqlValue::String(value) if !encoded => value.clone(),
        SqlValue::String(_) => {
            return Err(data_error(
                "22023",
                "JSON ENCODING clause is only allowed for bytea input type",
            ));
        }
        _ => return Err(argument_type_error(name, 1, "character string or bytea")),
    };
    json_text_result(raw)
}

pub(crate) fn json_text_path_result(
    value: &PgJsonText,
    path: &[JsonPathElement],
    as_text: bool,
) -> Result<SqlValue> {
    let Some(value) = json_text_extract_path(value, path)? else {
        return Ok(SqlValue::Null);
    };
    if !as_text {
        return Ok(SqlValue::JsonText(value));
    }
    Ok(match value.parsed() {
        JsonValue::Null => SqlValue::Null,
        JsonValue::String(value) => SqlValue::String(value.clone()),
        _ => SqlValue::String(value.raw().to_string()),
    })
}

pub(crate) fn json_text_extract_path(
    value: &PgJsonText,
    path: &[JsonPathElement],
) -> Result<Option<PgJsonText>> {
    if value.has_invalid_unicode_escape() {
        return Err(data_error(
            "22P02",
            "invalid input syntax for type json: unsupported Unicode escape sequence",
        ));
    }
    let mut raw = value.raw().to_string();
    for element in path {
        let trimmed = raw.trim_start();
        let selected = match element {
            JsonPathElement::Key(key) if trimmed.starts_with('{') => {
                json_text_object_entries(trimmed)?
                    .into_iter()
                    .rev()
                    .find_map(|(candidate, value)| (candidate == *key).then_some(value))
            }
            JsonPathElement::Index(index) if trimmed.starts_with('[') => {
                json_text_array_element(trimmed, *index)?
            }
            JsonPathElement::Text(key) if trimmed.starts_with('{') => {
                json_text_object_entries(trimmed)?
                    .into_iter()
                    .rev()
                    .find_map(|(candidate, value)| (candidate == *key).then_some(value))
            }
            JsonPathElement::Text(index) if trimmed.starts_with('[') => index
                .parse::<i64>()
                .ok()
                .map(|index| json_text_array_element(trimmed, index))
                .transpose()?
                .flatten(),
            _ => None,
        };
        let Some(selected) = selected else {
            return Ok(None);
        };
        raw = selected.get().to_string();
    }
    PgJsonText::parse(raw)
        .map(Some)
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))
}

fn json_text_array_element(
    raw: &str,
    index: i64,
) -> Result<Option<Box<serde_json::value::RawValue>>> {
    let values = serde_json::from_str::<Vec<Box<serde_json::value::RawValue>>>(raw)
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))?;
    let index = if index < 0 {
        i64::try_from(values.len())
            .ok()
            .and_then(|length| length.checked_add(index))
    } else {
        Some(index)
    };
    Ok(index
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| values.into_iter().nth(index)))
}

struct JsonTextObjectEntries(Vec<(String, Box<serde_json::value::RawValue>)>);

impl<'de> serde::Deserialize<'de> for JsonTextObjectEntries {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = JsonTextObjectEntries;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or_default());
                while let Some(key) = map.next_key::<String>()? {
                    entries.push((key, map.next_value::<Box<serde_json::value::RawValue>>()?));
                }
                Ok(JsonTextObjectEntries(entries))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn json_text_object_entries(raw: &str) -> Result<Vec<(String, Box<serde_json::value::RawValue>)>> {
    serde_json::from_str::<JsonTextObjectEntries>(raw)
        .map(|entries| entries.0)
        .map_err(|error| SqlError::InvalidTextRepresentation(error.to_string()))
}

fn jsonb_set(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 3, 4)?;

    // PostgreSQL's jsonb_set is strict, including its optional boolean.
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }

    let SqlValue::Json(target) = &args[0] else {
        return Err(argument_type_error(name, 1, "jsonb"));
    };
    let path = json_path(name, &args[1])?;
    let SqlValue::Json(new_value) = &args[2] else {
        return Err(argument_type_error(name, 3, "jsonb"));
    };
    let create_if_missing = match args.get(3) {
        None => true,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => return Err(argument_type_error(name, 4, "boolean")),
    };

    if !matches!(target, JsonValue::Object(_) | JsonValue::Array(_)) {
        return Err(data_error("22023", "cannot set path in scalar"));
    }

    // PostgreSQL returns an empty container unchanged before inspecting path
    // elements when creation is disabled.
    let target_is_empty = match target {
        JsonValue::Object(values) => values.is_empty(),
        JsonValue::Array(values) => values.is_empty(),
        _ => false,
    };
    if !create_if_missing && target_is_empty {
        return Ok(SqlValue::Json(target.clone()));
    }
    if path.is_empty() {
        return Ok(SqlValue::Json(target.clone()));
    }

    let mut result = target.clone();
    set_json_path(&mut result, &path, 0, new_value, create_if_missing)?;
    Ok(SqlValue::Json(result))
}

fn jsonb_set_lax(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 3, 5)?;
    if matches!(args[0], SqlValue::Null)
        || matches!(args[1], SqlValue::Null)
        || args
            .get(3)
            .is_some_and(|value| matches!(value, SqlValue::Null))
        || args
            .get(4)
            .is_some_and(|value| matches!(value, SqlValue::Null))
    {
        return Ok(SqlValue::Null);
    }
    if !matches!(args[2], SqlValue::Null) {
        return jsonb_set("jsonb_set", &args[..args.len().min(4)]);
    }
    let treatment = match args.get(4) {
        None => "use_json_null",
        Some(SqlValue::String(value)) => value.as_str(),
        Some(_) => return Err(argument_type_error(name, 5, "text")),
    };
    match treatment.to_ascii_lowercase().as_str() {
        "raise_exception" => Err(data_error("22004", "JSON value must not be null")),
        "use_json_null" => {
            let mut set_args = vec![
                args[0].clone(),
                args[1].clone(),
                SqlValue::Json(JsonValue::Null),
            ];
            if let Some(create) = args.get(3) {
                set_args.push(create.clone());
            }
            jsonb_set("jsonb_set", &set_args)
        }
        "delete_key" => crate::eval_json_delete_path_value(args[0].clone(), args[1].clone()),
        "return_target" => Ok(args[0].clone()),
        _ => Err(data_error(
            "22023",
            format!(
                "null_value_treatment must be one of raise_exception, use_json_null, delete_key, or return_target, not \"{treatment}\""
            ),
        )),
    }
}

fn jsonb_insert(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 3, 4)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let SqlValue::Json(target) = &args[0] else {
        return Err(argument_type_error(name, 1, "jsonb"));
    };
    let path = json_path(name, &args[1])?;
    let SqlValue::Json(new_value) = &args[2] else {
        return Err(argument_type_error(name, 3, "jsonb"));
    };
    let insert_after = match args.get(3) {
        None => false,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => return Err(argument_type_error(name, 4, "boolean")),
    };
    if !matches!(target, JsonValue::Object(_) | JsonValue::Array(_)) {
        return Err(data_error("22023", "cannot set path in scalar"));
    }
    if path.is_empty() {
        return Ok(SqlValue::Json(target.clone()));
    }
    let mut result = target.clone();
    insert_json_path(&mut result, &path, 0, new_value, insert_after)?;
    Ok(SqlValue::Json(result))
}

fn insert_json_path(
    current: &mut JsonValue,
    path: &[Option<String>],
    level: usize,
    new_value: &JsonValue,
    insert_after: bool,
) -> Result<bool> {
    let path_element = path[level].as_deref().ok_or_else(|| {
        data_error(
            "22004",
            format!("path element at position {} is null", level + 1),
        )
    })?;
    let is_last = level + 1 == path.len();
    match current {
        JsonValue::Object(object) => {
            if is_last {
                if !object.contains_key(path_element) {
                    object.insert(path_element.to_string(), new_value.clone());
                    return Ok(true);
                }
                return Ok(false);
            }
            let Some(child) = object.get_mut(path_element) else {
                return Ok(false);
            };
            insert_json_path(child, path, level + 1, new_value, insert_after)
        }
        JsonValue::Array(array) => {
            let raw_index = i64::from(parse_array_index(path_element, level + 1)?);
            let normalized = if raw_index < 0 {
                i64::try_from(array.len()).unwrap_or(i64::MAX) + raw_index
            } else {
                raw_index
            };
            if is_last {
                let index = if normalized < 0 {
                    0
                } else if normalized >= i64::try_from(array.len()).unwrap_or(i64::MAX) {
                    array.len()
                } else {
                    usize::try_from(normalized).unwrap_or(array.len()) + usize::from(insert_after)
                };
                array.insert(index.min(array.len()), new_value.clone());
                return Ok(true);
            }
            let Ok(index) = usize::try_from(normalized) else {
                return Ok(false);
            };
            let Some(child) = array.get_mut(index) else {
                return Ok(false);
            };
            insert_json_path(child, path, level + 1, new_value, insert_after)
        }
        _ => Ok(false),
    }
}

#[derive(Clone, Copy)]
enum JsonbBinaryFunction {
    Concat,
    Contains,
    Contained,
    Exists,
    ExistsAny,
    ExistsAll,
    Delete,
    DeletePath,
}

fn jsonb_binary_wrapper(
    name: &str,
    args: &[SqlValue],
    function: JsonbBinaryFunction,
) -> Result<SqlValue> {
    if matches!(function, JsonbBinaryFunction::Delete) {
        expect_arg_range(name, args, 2, usize::MAX)?;
    } else {
        expect_exact_args(name, args, 2)?;
    }
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    match function {
        JsonbBinaryFunction::Concat => crate::eval_json_concat(&args[0], &args[1])
            .ok_or_else(|| argument_type_error(name, 1, "jsonb")),
        JsonbBinaryFunction::Contains | JsonbBinaryFunction::Contained => {
            let (SqlValue::Json(left), SqlValue::Json(right)) = (&args[0], &args[1]) else {
                return Err(argument_type_error(name, 1, "jsonb"));
            };
            let contains = if matches!(function, JsonbBinaryFunction::Contains) {
                crate::json_contains(left, right)
            } else {
                crate::json_contains(right, left)
            };
            Ok(SqlValue::Bool(contains.unwrap_or(false)))
        }
        JsonbBinaryFunction::Exists => crate::eval_json_existence_value(
            args[0].clone(),
            &BinaryOperator::Question,
            args[1].clone(),
        ),
        JsonbBinaryFunction::ExistsAny => crate::eval_json_existence_value(
            args[0].clone(),
            &BinaryOperator::QuestionPipe,
            args[1].clone(),
        ),
        JsonbBinaryFunction::ExistsAll => crate::eval_json_existence_value(
            args[0].clone(),
            &BinaryOperator::QuestionAnd,
            args[1].clone(),
        ),
        JsonbBinaryFunction::Delete => {
            let keys = if args.len() == 2 {
                args[1].clone()
            } else {
                SqlValue::Json(JsonValue::Array(
                    args[1..]
                        .iter()
                        .enumerate()
                        .map(|(index, value)| match value {
                            SqlValue::String(value) => Ok(JsonValue::String(value.clone())),
                            _ => Err(argument_type_error(name, index + 2, "text")),
                        })
                        .collect::<Result<Vec<_>>>()?,
                ))
            };
            crate::eval_json_delete_value(args[0].clone(), keys)
        }
        JsonbBinaryFunction::DeletePath => {
            crate::eval_json_delete_path_value(args[0].clone(), args[1].clone())
        }
    }
}

fn set_json_path(
    current: &mut JsonValue,
    path: &[Option<String>],
    level: usize,
    new_value: &JsonValue,
    create_if_missing: bool,
) -> Result<bool> {
    let path_element = path[level].as_deref().ok_or_else(|| {
        data_error(
            "22004",
            format!("path element at position {} is null", level + 1),
        )
    })?;
    let is_last = level == path.len() - 1;

    match current {
        JsonValue::Object(object) => {
            if is_last {
                if object.contains_key(path_element) || create_if_missing {
                    object.insert(path_element.to_string(), new_value.clone());
                    return Ok(true);
                }
                return Ok(false);
            }

            let Some(child) = object.get_mut(path_element) else {
                return Ok(false);
            };
            set_json_path(child, path, level + 1, new_value, create_if_missing)
        }
        JsonValue::Array(array) => {
            let raw_index = parse_array_index(path_element, level + 1)?;
            let normalized = if raw_index < 0 {
                array.len() as i64 + i64::from(raw_index)
            } else {
                i64::from(raw_index)
            };

            if is_last {
                if normalized >= 0 && normalized < array.len() as i64 {
                    array[normalized as usize] = new_value.clone();
                    return Ok(true);
                }
                if create_if_missing {
                    if raw_index < 0 {
                        array.insert(0, new_value.clone());
                    } else {
                        array.push(new_value.clone());
                    }
                    return Ok(true);
                }
                return Ok(false);
            }

            if normalized < 0 || normalized >= array.len() as i64 {
                return Ok(false);
            }
            set_json_path(
                &mut array[normalized as usize],
                path,
                level + 1,
                new_value,
                create_if_missing,
            )
        }
        // jsonb_set does not manufacture intermediate containers. An existing
        // scalar below the root therefore leaves the target unchanged.
        _ => Ok(false),
    }
}

fn jsonb_strip_nulls(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_arg_range(name, args, 1, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }

    let SqlValue::Json(target) = &args[0] else {
        return Err(argument_type_error(name, 1, "jsonb"));
    };
    let strip_in_arrays = match args.get(1) {
        None => false,
        Some(SqlValue::Bool(value)) => *value,
        Some(_) => return Err(argument_type_error(name, 2, "boolean")),
    };

    let mut result = target.clone();
    strip_json_nulls(&mut result, strip_in_arrays);
    Ok(SqlValue::Json(result))
}

fn strip_json_nulls(value: &mut JsonValue, strip_in_arrays: bool) {
    match value {
        JsonValue::Object(object) => {
            object.retain(|_, child| !child.is_null());
            for child in object.values_mut() {
                strip_json_nulls(child, strip_in_arrays);
            }
        }
        JsonValue::Array(array) => {
            if strip_in_arrays {
                array.retain(|child| !child.is_null());
            }
            for child in array {
                strip_json_nulls(child, strip_in_arrays);
            }
        }
        _ => {}
    }
}

fn jsonb_pretty(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 1)?;
    let value = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Json(value) => value,
        _ => return Err(argument_type_error(name, 1, "jsonb")),
    };

    Ok(SqlValue::String(postgres_jsonb_pretty_text(value)))
}

fn jsonb_compare_function(name: &str, args: &[SqlValue]) -> Result<SqlValue> {
    expect_exact_args(name, args, 2)?;
    if args.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(SqlValue::Null);
    }
    let (SqlValue::Json(left), SqlValue::Json(right)) = (&args[0], &args[1]) else {
        return Err(argument_type_error(name, 1, "jsonb"));
    };
    let ordering = crate::jsonb_value_ordering(left, right);
    Ok(match name {
        "jsonb_cmp" => SqlValue::Int(match ordering {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        "jsonb_eq" => SqlValue::Bool(ordering.is_eq()),
        "jsonb_ne" => SqlValue::Bool(ordering.is_ne()),
        "jsonb_gt" => SqlValue::Bool(ordering.is_gt()),
        "jsonb_ge" => SqlValue::Bool(ordering.is_ge()),
        "jsonb_lt" => SqlValue::Bool(ordering.is_lt()),
        "jsonb_le" => SqlValue::Bool(ordering.is_le()),
        _ => unreachable!("recognized JSONB comparator"),
    })
}

pub(crate) fn sql_value_to_json(value: &SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Bool(value) => JsonValue::Bool(*value),
        SqlValue::Int(value) => JsonValue::Number(JsonNumber::from(*value)),
        SqlValue::Float(value) => JsonNumber::from_f64(*value)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(non_finite_float(*value).to_string())),
        SqlValue::String(value) => JsonValue::String(value.clone()),
        SqlValue::TsQuery(value) => JsonValue::String(value.to_postgres_text()),
        SqlValue::JsonText(value) => value.parsed().clone(),
        SqlValue::Json(value) => value.clone(),
        SqlValue::Geometry(value) => JsonValue::String(value.to_wkt()),
        SqlValue::Composite(value) => JsonValue::Object(
            value
                .fields
                .iter()
                .map(|field| (field.name.clone(), sql_value_to_json(&field.value)))
                .collect(),
        ),
    }
}

pub(crate) fn json_object_aggregate_key(value: &SqlValue) -> Result<String> {
    object_key(value, 1)
}

fn object_key(value: &SqlValue, position: usize) -> Result<String> {
    match value {
        SqlValue::Null => Err(data_error(
            "22023",
            format!("argument {position}: key must not be null"),
        )),
        SqlValue::Json(_) | SqlValue::JsonText(_) | SqlValue::Composite(_) => Err(data_error(
            "22023",
            "key value must be scalar, not array, composite, or json",
        )),
        SqlValue::Float(value) if !value.is_finite() => Ok(non_finite_float(*value).to_string()),
        value => Ok(value.to_cell()),
    }
}

fn non_finite_float(value: f64) -> &'static str {
    if value.is_nan() {
        "NaN"
    } else if value.is_sign_negative() {
        "-Infinity"
    } else {
        "Infinity"
    }
}

fn json_path(name: &str, value: &SqlValue) -> Result<Vec<Option<String>>> {
    match value {
        SqlValue::Json(JsonValue::Array(elements)) => elements
            .iter()
            .map(|element| match element {
                JsonValue::Null => Ok(None),
                JsonValue::String(value) => Ok(Some(value.clone())),
                _ => Err(argument_type_error(name, 2, "text[]")),
            })
            .collect(),
        SqlValue::String(value) => parse_text_array_path(name, value),
        _ => Err(argument_type_error(name, 2, "text[]")),
    }
}

fn parse_text_array_path(name: &str, value: &str) -> Result<Vec<Option<String>>> {
    let trimmed = value.trim();
    if trimmed.starts_with('[') {
        let parsed = serde_json::from_str::<JsonValue>(trimmed).map_err(|_| {
            SqlError::InvalidTextRepresentation(format!("malformed array literal: \"{value}\""))
        })?;
        return json_path(name, &SqlValue::Json(parsed));
    }
    if trimmed == "{}" {
        return Ok(Vec::new());
    }
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| {
            SqlError::InvalidTextRepresentation(format!("malformed array literal: \"{value}\""))
        })?;
    split_text_array_path(inner).map_err(|message| SqlError::InvalidTextRepresentation(message))
}

fn split_text_array_path(inner: &str) -> std::result::Result<Vec<Option<String>>, String> {
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut element_quoted = false;
    let mut escaped = false;

    let push_element =
        |result: &mut Vec<Option<String>>, current: &mut String, element_quoted: &mut bool| {
            let value = if *element_quoted {
                Some(std::mem::take(current))
            } else {
                let value = std::mem::take(current).trim().to_string();
                if value.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(value)
                }
            };
            result.push(value);
            *element_quoted = false;
        };

    for character in inner.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            element_quoted = true;
            continue;
        }
        if character == ',' && !quoted {
            push_element(&mut result, &mut current, &mut element_quoted);
            continue;
        }
        current.push(character);
    }
    if quoted || escaped {
        return Err(format!("malformed array literal: \"{{{inner}}}\""));
    }
    push_element(&mut result, &mut current, &mut element_quoted);
    Ok(result)
}

fn parse_array_index(value: &str, position: usize) -> Result<i32> {
    value.parse::<i32>().map_err(|_| {
        SqlError::InvalidTextRepresentation(format!(
            "path element at position {position} is not an integer: \"{value}\""
        ))
    })
}

fn expect_exact_args(name: &str, args: &[SqlValue], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(SqlError::InvalidSql(format!(
            "{name} expects {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}

fn expect_arg_range(name: &str, args: &[SqlValue], min: usize, max: usize) -> Result<()> {
    if args.len() < min || args.len() > max {
        return Err(SqlError::InvalidSql(format!(
            "{name} expects between {min} and {max} arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

fn argument_type_error(name: &str, position: usize, expected: &str) -> SqlError {
    SqlError::InvalidSql(format!("{name} argument {position} must be {expected}"))
}

fn data_error(sqlstate: &'static str, message: impl Into<String>) -> SqlError {
    SqlError::ConstraintViolation {
        sqlstate,
        message: message.into(),
        table: None,
        column: None,
        constraint: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn evaluate(name: &str, args: Vec<SqlValue>) -> Result<SqlValue> {
        Ok(eval_json_function_value(name, &args)?.expect("recognized function"))
    }

    #[test]
    fn compact_cells_and_identity_ignore_preserved_insertion_order() {
        let mut nested = JsonMap::new();
        nested.insert("title".to_string(), JsonValue::String("nested".to_string()));
        nested.insert("enabled".to_string(), JsonValue::Null);
        nested.insert("code".to_string(), JsonValue::from(9));
        let mut root = JsonMap::new();
        root.insert("z".to_string(), JsonValue::from(2));
        root.insert("a".to_string(), JsonValue::Object(nested));
        let value = JsonValue::Object(root);

        assert_eq!(
            compact_jsonb_cell_text(&value),
            r#"{"a":{"code":9,"enabled":null,"title":"nested"},"z":2}"#
        );

        let mut equivalent_nested = JsonMap::new();
        equivalent_nested.insert("code".to_string(), JsonValue::from(9));
        equivalent_nested.insert("enabled".to_string(), JsonValue::Null);
        equivalent_nested.insert("title".to_string(), JsonValue::String("nested".to_string()));
        let mut equivalent_root = JsonMap::new();
        equivalent_root.insert("a".to_string(), JsonValue::Object(equivalent_nested));
        equivalent_root.insert("z".to_string(), JsonValue::from(2));

        assert_eq!(
            crate::canonical_json_value_key(&value),
            crate::canonical_json_value_key(&JsonValue::Object(equivalent_root))
        );
    }

    #[test]
    fn exponent_rendering_normalizes_leading_zeroes_and_preserves_zero_scale() {
        for (input, expected) in [
            ("0.001e2", "0.1"),
            ("0.001e-2", "0.00001"),
            ("1.20e-2", "0.0120"),
            ("0.01e4", "100"),
            ("0e-2", "0.00"),
            ("-0e-3", "0.000"),
            ("0e2", "0"),
        ] {
            let value = serde_json::from_str(input).unwrap();
            assert_eq!(postgres_jsonb_text(&value), expected, "input {input}");
        }
    }

    #[test]
    fn dispatches_only_known_names_and_accepts_catalog_qualification() {
        assert_eq!(eval_json_function_value("not_json", &[]).unwrap(), None);
        assert_eq!(
            evaluate("pg_catalog.jsonb_build_array", vec![]).unwrap(),
            SqlValue::Json(json!([]))
        );
    }

    #[test]
    fn builders_distinguish_sql_null_and_json_values() {
        assert_eq!(
            evaluate(
                "jsonb_build_array",
                vec![
                    SqlValue::Int(1),
                    SqlValue::Null,
                    SqlValue::Json(json!({"nested": true})),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!([1, null, {"nested": true}]))
        );
        assert_eq!(
            evaluate(
                "json_build_object",
                vec![
                    SqlValue::String("key".into()),
                    SqlValue::Null,
                    SqlValue::Int(7),
                    SqlValue::Bool(true),
                ],
            )
            .unwrap(),
            SqlValue::JsonText(
                PgJsonText::parse(r#"{"key" : null, "7" : true}"#.to_string()).unwrap()
            )
        );

        let odd = evaluate("jsonb_build_object", vec![SqlValue::String("key".into())]).unwrap_err();
        assert_eq!(odd.sqlstate(), "22023");
        let null_key =
            evaluate("jsonb_build_object", vec![SqlValue::Null, SqlValue::Int(1)]).unwrap_err();
        assert_eq!(null_key.sqlstate(), "22023");
    }

    #[test]
    fn to_json_is_strict_and_does_not_parse_text() {
        assert_eq!(
            evaluate("to_jsonb", vec![SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            evaluate("to_jsonb", vec![SqlValue::String("{\"a\":1}".into())]).unwrap(),
            SqlValue::Json(json!("{\"a\":1}"))
        );
        assert_eq!(
            evaluate("to_json", vec![SqlValue::Json(json!({"a": 1}))]).unwrap(),
            SqlValue::JsonText(PgJsonText::parse(r#"{"a": 1}"#.to_string()).unwrap())
        );
    }

    #[test]
    fn array_length_handles_null_and_rejects_other_json_shapes() {
        assert_eq!(
            evaluate("jsonb_array_length", vec![SqlValue::Json(json!([1, 2, 3]))]).unwrap(),
            SqlValue::Int(3)
        );
        assert_eq!(
            evaluate("jsonb_array_length", vec![SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
        let error = evaluate("jsonb_array_length", vec![SqlValue::Json(json!({}))]).unwrap_err();
        assert_eq!(error.sqlstate(), "22023");
    }

    #[test]
    fn jsonb_set_replaces_creates_and_preserves_missing_intermediates() {
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!({"a": 1})),
                    SqlValue::Json(json!(["a"])),
                    SqlValue::Json(json!(2)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!({"a": 2}))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!({"a": 1})),
                    SqlValue::String("{b}".into()),
                    SqlValue::Json(json!(2)),
                    SqlValue::Bool(false),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!({"a": 1}))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!({"a": 1})),
                    SqlValue::String("{b}".into()),
                    SqlValue::Json(json!(2)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!({"a": 1, "b": 2}))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!({})),
                    SqlValue::String("{a,b}".into()),
                    SqlValue::Json(json!(1)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!({}))
        );
    }

    #[test]
    fn jsonb_set_matches_negative_and_out_of_range_array_indexes() {
        let target = SqlValue::Json(json!([0, 1, 2]));
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    target.clone(),
                    SqlValue::String("{-1}".into()),
                    SqlValue::Json(json!(9)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!([0, 1, 9]))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    target.clone(),
                    SqlValue::String("{99}".into()),
                    SqlValue::Json(json!(9)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!([0, 1, 2, 9]))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    target,
                    SqlValue::String("{-99}".into()),
                    SqlValue::Json(json!(9)),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!([9, 0, 1, 2]))
        );
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!([0, 1, 2])),
                    SqlValue::String("{99}".into()),
                    SqlValue::Json(json!(9)),
                    SqlValue::Bool(false),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!([0, 1, 2]))
        );
    }

    #[test]
    fn jsonb_set_is_strict_and_rejects_scalar_targets() {
        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Null,
                    SqlValue::String("{a}".into()),
                    SqlValue::Json(json!(1)),
                ],
            )
            .unwrap(),
            SqlValue::Null
        );
        let error = evaluate(
            "jsonb_set",
            vec![
                SqlValue::Json(json!(1)),
                SqlValue::String("{}".into()),
                SqlValue::Json(json!(2)),
            ],
        )
        .unwrap_err();
        assert_eq!(error.sqlstate(), "22023");

        assert_eq!(
            evaluate(
                "jsonb_set",
                vec![
                    SqlValue::Json(json!({})),
                    SqlValue::Json(json!([null])),
                    SqlValue::Json(json!(1)),
                    SqlValue::Bool(false),
                ],
            )
            .unwrap(),
            SqlValue::Json(json!({}))
        );
        let null_path_element = evaluate(
            "jsonb_set",
            vec![
                SqlValue::Json(json!({})),
                SqlValue::Json(json!([null])),
                SqlValue::Json(json!(1)),
            ],
        )
        .unwrap_err();
        assert_eq!(null_path_element.sqlstate(), "22004");
    }

    #[test]
    fn strip_nulls_recurses_and_optionally_removes_array_nulls() {
        let target = SqlValue::Json(json!({
            "a": null,
            "b": [{"c": null, "d": 1}, null, [null, 2]]
        }));
        assert_eq!(
            evaluate("jsonb_strip_nulls", vec![target.clone()]).unwrap(),
            SqlValue::Json(json!({"b": [{"d": 1}, null, [null, 2]]}))
        );
        assert_eq!(
            evaluate("jsonb_strip_nulls", vec![target, SqlValue::Bool(true)],).unwrap(),
            SqlValue::Json(json!({"b": [{"d": 1}, [2]]}))
        );
        assert_eq!(
            evaluate("jsonb_strip_nulls", vec![SqlValue::Json(JsonValue::Null)]).unwrap(),
            SqlValue::Json(JsonValue::Null)
        );
    }

    #[test]
    fn pretty_uses_postgresql_four_space_indentation() {
        assert_eq!(
            evaluate(
                "jsonb_pretty",
                vec![SqlValue::Json(json!([{"f1": 1, "f2": null}, 2]))],
            )
            .unwrap(),
            SqlValue::String(
                "[\n    {\n        \"f1\": 1,\n        \"f2\": null\n    },\n    2\n]".into()
            )
        );
        let value = serde_json::from_str(r#"{"aa":1e2,"b":["x",null]}"#).unwrap();
        assert_eq!(
            evaluate("jsonb_pretty", vec![SqlValue::Json(value)]).unwrap(),
            SqlValue::String(
                "{\n    \"b\": [\n        \"x\",\n        null\n    ],\n    \"aa\": 100\n}".into()
            )
        );
    }
}
