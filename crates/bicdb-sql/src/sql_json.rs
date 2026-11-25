//! SQL/JSON query functions: `JSON_VALUE`, `JSON_QUERY` and `JSON_EXISTS`
//! with their PASSING, RETURNING, wrapper, quotes, ON EMPTY and ON ERROR
//! clauses.
//!
//! The parser knows none of these clauses, so each call is rewritten before
//! parsing into a builtin that receives the clauses as a JSON description
//! (`bicdb_json_value(context, path, 'clauses', passing, empty_default,
//! error_default)`), wrapped in a cast to the RETURNING type so the result
//! column carries it. The builtin evaluates the SQL/JSON path with the
//! jsonpath engine and applies PostgreSQL's empty/error behaviour.

use crate::jsonpath::JsonPath;
use crate::*;
use serde_json::{json, Map};

const FUNCTIONS: [(&str, &str); 3] = [
    ("json_value", "bicdb_json_value"),
    ("json_query", "bicdb_json_query"),
    ("json_exists", "bicdb_json_exists"),
];

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$'
}

/// End (exclusive) of the parenthesized argument list opening at `open`.
fn matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut index = open;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// Top-level tokens of an argument list: words, string literals,
/// parenthesized groups, commas and other punctuation.
fn tokens(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        let start = index;
        match byte {
            b'\'' | b'"' => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == byte {
                        if bytes.get(index + 1) == Some(&byte) {
                            index += 2;
                            continue;
                        }
                        index += 1;
                        break;
                    }
                    index += 1;
                }
                // A cast suffix stays with its literal.
                while text[index..].starts_with("::") {
                    index += 2;
                    while index < bytes.len()
                        && (is_word_byte(bytes[index]) || bytes[index] == b'.')
                    {
                        index += 1;
                    }
                }
            }
            b'(' => {
                index = matching_paren(text, index).map_or(bytes.len(), |end| end + 1);
            }
            _ if is_word_byte(byte) => {
                while index < bytes.len() && (is_word_byte(bytes[index]) || bytes[index] == b'.') {
                    index += 1;
                }
                // A function call or type modifier keeps its argument list.
                let mut after = index;
                while after < bytes.len() && bytes[after].is_ascii_whitespace() {
                    after += 1;
                }
                if bytes.get(after) == Some(&b'(') {
                    index = matching_paren(text, after).map_or(bytes.len(), |end| end + 1);
                }
                if text[index..].starts_with("[]") {
                    index += 2;
                }
            }
            b':' if text[index..].starts_with("::") => {
                index += 2;
                while index < bytes.len() && (is_word_byte(bytes[index]) || bytes[index] == b'.') {
                    index += 1;
                }
            }
            _ => index += 1,
        }
        tokens.push((start, index));
    }
    tokens
}

const CLAUSE_WORDS: [&str; 14] = [
    "PASSING",
    "RETURNING",
    "NULL",
    "ERROR",
    "DEFAULT",
    "EMPTY",
    "TRUE",
    "FALSE",
    "UNKNOWN",
    "WITH",
    "WITHOUT",
    "KEEP",
    "OMIT",
    "FORMAT",
];

fn upper(token: &str) -> String {
    token.to_ascii_uppercase()
}

/// The rewritten call for `name(args)`, or `None` when the arguments are
/// not a SQL/JSON function call this rewrite understands.
fn rewrite_call(function: &str, builtin: &str, args: &str) -> Option<String> {
    let spans = tokens(args);
    let token = |index: usize| &args[spans[index].0..spans[index].1];
    // Source text of tokens `from..to`, as written.
    let source = |from: usize, to: usize| args[spans[from].0..spans[to - 1].1].to_string();
    // Context expression: everything before the first top-level comma.
    let comma = (0..spans.len()).position(|index| token(index) == ",")?;
    if comma == 0 {
        return None;
    }
    let context = source(0, comma);
    // Path: a literal or expression up to the first clause keyword.
    let mut index = comma + 1;
    let path_start = index;
    while index < spans.len()
        && !(index > path_start && CLAUSE_WORDS.contains(&upper(token(index)).as_str()))
    {
        index += 1;
    }
    if index == path_start {
        return None;
    }
    let path = source(path_start, index);
    let rest = (index..spans.len())
        .map(|index| (token(index).to_string(), spans[index]))
        .collect::<Vec<_>>();
    let raw = |from: usize, to: usize| args[rest[from].1 .0..rest[to - 1].1 .1].to_string();
    let rest = rest
        .iter()
        .map(|(text, _)| text.clone())
        .collect::<Vec<_>>();
    let mut spec = Map::new();
    let mut passing = Vec::new();
    let mut empty_default = "NULL".to_string();
    let mut error_default = "NULL".to_string();
    let mut returning = None;
    let mut index = 0usize;
    let word = |index: usize| rest.get(index).map(|token| upper(token));
    // A behaviour (`NULL`, `ERROR`, `DEFAULT expr`, `EMPTY [ARRAY|OBJECT]`,
    // `TRUE`, `FALSE`, `UNKNOWN`) followed by `ON EMPTY` / `ON ERROR`.
    while index < rest.len() {
        let current = word(index)?;
        match current.as_str() {
            "PASSING" => {
                index += 1;
                loop {
                    let value_start = index;
                    while index < rest.len() && word(index)? != "AS" {
                        index += 1;
                    }
                    if index == value_start {
                        return None;
                    }
                    let value = raw(value_start, index);
                    let name = rest.get(index + 1)?.clone();
                    index += 2;
                    let name = name.trim_matches('"').to_string();
                    passing.push(format!("'{}', {value}", name.replace('\'', "''")));
                    if rest.get(index).map(String::as_str) == Some(",") {
                        index += 1;
                        continue;
                    }
                    break;
                }
            }
            "RETURNING" => {
                index += 1;
                let kind_start = index;
                while index < rest.len() && !CLAUSE_WORDS.contains(&word(index)?.as_str()) {
                    index += 1;
                }
                if index == kind_start {
                    return None;
                }
                returning = Some(raw(kind_start, index));
            }
            "FORMAT" => {
                // FORMAT JSON [ENCODING UTF8] is the default and only format.
                index += 2;
                if word(index).as_deref() == Some("ENCODING") {
                    index += 2;
                }
            }
            "WITH" | "WITHOUT" => {
                let mut mode = if current == "WITH" {
                    "unconditional"
                } else {
                    "none"
                };
                index += 1;
                match word(index).as_deref() {
                    Some("CONDITIONAL") => {
                        mode = "conditional";
                        index += 1;
                    }
                    Some("UNCONDITIONAL") => index += 1,
                    _ => {}
                }
                if word(index).as_deref() == Some("ARRAY") {
                    index += 1;
                }
                if word(index).as_deref() != Some("WRAPPER") {
                    return None;
                }
                index += 1;
                spec.insert("wrapper".into(), json!(mode));
            }
            "KEEP" | "OMIT" => {
                if word(index + 1).as_deref() != Some("QUOTES") {
                    return None;
                }
                index += 2;
                if word(index).as_deref() == Some("ON") {
                    // ON SCALAR STRING
                    index += 3;
                }
                spec.insert("omit_quotes".into(), json!(current == "OMIT"));
            }
            "NULL" | "ERROR" | "DEFAULT" | "EMPTY" | "TRUE" | "FALSE" | "UNKNOWN" => {
                let mut behaviour = current.to_ascii_lowercase();
                let mut default = None;
                index += 1;
                if current == "DEFAULT" {
                    let value_start = index;
                    while index < rest.len() && word(index)? != "ON" {
                        index += 1;
                    }
                    if index == value_start {
                        return None;
                    }
                    default = Some(raw(value_start, index));
                } else if current == "EMPTY" {
                    behaviour = match word(index).as_deref() {
                        Some("OBJECT") => {
                            index += 1;
                            "empty_object".into()
                        }
                        Some("ARRAY") => {
                            index += 1;
                            "empty_array".into()
                        }
                        _ => "empty_array".into(),
                    };
                }
                if word(index).as_deref() != Some("ON") {
                    return None;
                }
                let target = word(index + 1)?;
                index += 2;
                match target.as_str() {
                    "EMPTY" => {
                        spec.insert("on_empty".into(), json!(behaviour));
                        if let Some(default) = default {
                            empty_default = default;
                        }
                    }
                    "ERROR" => {
                        spec.insert("on_error".into(), json!(behaviour));
                        if let Some(default) = default {
                            error_default = default;
                        }
                    }
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    let returning = returning.unwrap_or_else(|| {
        match function {
            "json_value" => "text",
            "json_query" => "jsonb",
            _ => "boolean",
        }
        .to_string()
    });
    spec.insert("returning".into(), json!(returning));
    let spec = JsonValue::Object(spec).to_string().replace('\'', "''");
    let passing = if passing.is_empty() {
        "NULL".to_string()
    } else {
        format!("jsonb_build_object({})", passing.join(", "))
    };
    Some(format!(
        "CAST({builtin}({context}, {path}, '{spec}', {passing}, {empty_default}, {error_default}) AS {returning})"
    ))
}

/// Rewrite every `JSON_VALUE(...)`, `JSON_QUERY(...)` and `JSON_EXISTS(...)`
/// outside strings, comments and dollar quotes.
pub(crate) fn rewrite_sql_json_functions(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !FUNCTIONS.iter().any(|(name, _)| lower.contains(name)) {
        return None;
    }
    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len());
    let mut copied = 0usize;
    let mut index = 0usize;
    let mut changed = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\'' | b'"' => {
                let quote = bytes[index];
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == quote {
                        if bytes.get(index + 1) == Some(&quote) {
                            index += 2;
                            continue;
                        }
                        break;
                    }
                    index += 1;
                }
                index += 1;
                continue;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            b'$' => {
                if let Some(delimiter) = crate::dollar_quote_delimiter(&sql[index..]) {
                    let body = index + delimiter.len();
                    match sql[body..].find(&delimiter) {
                        Some(end) => index = body + end + delimiter.len(),
                        None => return None,
                    }
                    continue;
                }
            }
            byte if is_word_byte(byte) => {
                let start = index;
                while index < bytes.len() && is_word_byte(bytes[index]) {
                    index += 1;
                }
                if start > 0 && (bytes[start - 1] == b'.' || is_word_byte(bytes[start - 1])) {
                    continue;
                }
                let word = &lower[start..index];
                let Some((function, builtin)) = FUNCTIONS.iter().find(|(name, _)| *name == word)
                else {
                    continue;
                };
                let mut open = index;
                while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                    open += 1;
                }
                if bytes.get(open) != Some(&b'(') {
                    continue;
                }
                let Some(close) = matching_paren(sql, open) else {
                    return None;
                };
                let inner = &sql[open + 1..close];
                // Nested calls inside the arguments are rewritten first.
                let inner = rewrite_sql_json_functions(inner).unwrap_or_else(|| inner.to_string());
                let Some(call) = rewrite_call(function, builtin, &inner) else {
                    continue;
                };
                output.push_str(&sql[copied..start]);
                output.push_str(&call);
                copied = close + 1;
                index = close + 1;
                changed = true;
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    if !changed {
        return None;
    }
    output.push_str(&sql[copied..]);
    Some(output)
}

#[derive(Default)]
struct Clauses {
    returning: String,
    wrapper: String,
    omit_quotes: bool,
    on_empty: Option<String>,
    on_error: Option<String>,
}

fn clauses(spec: &SqlValue) -> Result<Clauses> {
    let spec: JsonValue = serde_json::from_str(&spec.to_cell())
        .map_err(|_| SqlError::InvalidSql("invalid SQL/JSON clause description".into()))?;
    let text = |key: &str| {
        spec.get(key)
            .and_then(JsonValue::as_str)
            .map(str::to_string)
    };
    Ok(Clauses {
        returning: text("returning").unwrap_or_default(),
        wrapper: text("wrapper").unwrap_or_else(|| "none".into()),
        omit_quotes: spec
            .get("omit_quotes")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        on_empty: text("on_empty"),
        on_error: text("on_error"),
    })
}

fn context_json(value: &SqlValue) -> Result<JsonValue> {
    Ok(match value {
        SqlValue::Json(value) => (**value).clone(),
        SqlValue::JsonText(value) => value.parsed().clone(),
        other => serde_json::from_str(&other.to_cell()).map_err(|_| {
            SqlError::invalid_text_representation(
                "json",
                format!(
                    "invalid input syntax for type json: \"{}\"",
                    other.to_cell()
                ),
            )
        })?,
    })
}

fn sql_json_error(sqlstate: &'static str, message: &str) -> SqlError {
    SqlError::data_exception_public(sqlstate, message, None)
}

/// The value a behaviour (`null`, `error`, `default`, `empty_array`, ...)
/// produces, or the error it raises.
fn behaviour_value(
    behaviour: Option<&str>,
    default_behaviour: &str,
    default: &SqlValue,
    error: SqlError,
) -> Result<SqlValue> {
    match behaviour.unwrap_or(default_behaviour) {
        "error" => Err(error),
        "default" => Ok(default.clone()),
        "empty_array" => Ok(SqlValue::json(json!([]).into())),
        "empty_object" => Ok(SqlValue::json(json!({}).into())),
        "true" => Ok(SqlValue::Bool(true)),
        "false" => Ok(SqlValue::Bool(false)),
        _ => Ok(SqlValue::Null),
    }
}

fn cast_to(value: SqlValue, returning: &str) -> Result<SqlValue> {
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }
    let data_type = crate::pg_operators::parse_sql_data_type(returning)?;
    let (pg_type, _) = pg_type_from_data_type(&data_type)?;
    cast_value_to_pg_type(value, &pg_type)
}

/// Evaluate one SQL/JSON query function (`bicdb_json_value`, `_query`,
/// `_exists`). `None` when `name` is not one of them.
pub(crate) fn eval_sql_json_function(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let function = match name {
        "bicdb_json_value" => "json_value",
        "bicdb_json_query" => "json_query",
        "bicdb_json_exists" => "json_exists",
        _ => return Ok(None),
    };
    let [context, path, spec, passing, empty_default, error_default] = args else {
        return Err(SqlError::InvalidSql(format!(
            "{function} expects its SQL/JSON clause arguments"
        )));
    };
    let clauses = clauses(spec)?;
    if matches!(context, SqlValue::Null) || matches!(path, SqlValue::Null) {
        return Ok(Some(SqlValue::Null));
    }
    let target = context_json(context)?;
    let path = JsonPath::parse(&path.to_cell())?;
    let variables = match passing {
        SqlValue::Null => Map::new(),
        value => context_json(value)?
            .as_object()
            .cloned()
            .unwrap_or_default(),
    };
    let items = path.query(&target, &variables, false);
    let on_error = |error: SqlError, default_behaviour: &str| {
        behaviour_value(
            clauses.on_error.as_deref(),
            default_behaviour,
            error_default,
            error,
        )
    };
    let value = match function {
        "json_exists" => match items {
            Ok(items) => SqlValue::Bool(!items.is_empty()),
            Err(error) => match clauses.on_error.as_deref() {
                Some("unknown") => SqlValue::Null,
                behaviour => behaviour_value(behaviour, "false", error_default, error)?,
            },
        },
        "json_value" => {
            let items = match items {
                Ok(items) => items,
                Err(error) => return on_error(error, "null").map(Some),
            };
            match items.as_slice() {
                [] => behaviour_value(
                    clauses.on_empty.as_deref(),
                    "null",
                    empty_default,
                    sql_json_error("22035", "no SQL/JSON item found for specified path"),
                )?,
                [item] if item.is_array() || item.is_object() => {
                    return on_error(
                        sql_json_error(
                            "2203F",
                            "JSON path expression in JSON_VALUE must return single scalar item",
                        ),
                        "null",
                    )
                    .map(Some);
                }
                [item] => {
                    let scalar = match item {
                        JsonValue::Null => SqlValue::Null,
                        JsonValue::String(text) => SqlValue::String(text.clone()),
                        JsonValue::Bool(value) => SqlValue::String(value.to_string()),
                        other => SqlValue::String(other.to_string()),
                    };
                    match cast_to(scalar, &clauses.returning) {
                        Ok(value) => value,
                        Err(error) => return on_error(error, "null").map(Some),
                    }
                }
                _ => {
                    return on_error(
                        sql_json_error(
                            "22034",
                            "JSON path expression in JSON_VALUE must return single scalar item",
                        ),
                        "null",
                    )
                    .map(Some);
                }
            }
        }
        _ => {
            let items = match items {
                Ok(items) => items,
                Err(error) => return on_error(error, "null").map(Some),
            };
            let result = match clauses.wrapper.as_str() {
                "unconditional" if !items.is_empty() => Some(JsonValue::Array(items)),
                // A single item is returned as it is; several are wrapped.
                "conditional" => match items.as_slice() {
                    [] => None,
                    [item] => Some(item.clone()),
                    _ => Some(JsonValue::Array(items)),
                },
                _ => match items.as_slice() {
                    [] => None,
                    [item] => Some(item.clone()),
                    _ => {
                        return on_error(
                            sql_json_error(
                                "22034",
                                "JSON path expression in JSON_QUERY must return single item when no wrapper is requested",
                            ),
                            "null",
                        )
                        .map(Some);
                    }
                },
            };
            let Some(result) = result else {
                return behaviour_value(
                    clauses.on_empty.as_deref(),
                    "null",
                    empty_default,
                    sql_json_error("22035", "no SQL/JSON item found for specified path"),
                )
                .map(Some);
            };
            let value = match (&result, clauses.omit_quotes) {
                (JsonValue::String(text), true) => SqlValue::String(text.clone()),
                _ if matches!(
                    clauses.returning.to_ascii_lowercase().as_str(),
                    "json" | "jsonb"
                ) =>
                {
                    SqlValue::json(result.into())
                }
                _ => SqlValue::String(result.to_string()),
            };
            match cast_to(value, &clauses.returning) {
                Ok(value) => value,
                Err(error) => return on_error(error, "null").map(Some),
            }
        }
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calls_rewrite_to_builtins() {
        let sql = rewrite_sql_json_functions(
            "SELECT JSON_VALUE(doc, '$.a' RETURNING int DEFAULT 0 ON EMPTY), json_exists('{}', '$.x')",
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT CAST(bicdb_json_value(doc, '$.a', '{\"on_empty\":\"default\",\"returning\":\"int\"}', NULL, 0, NULL) AS int), \
             CAST(bicdb_json_exists('{}', '$.x', '{\"returning\":\"boolean\"}', NULL, NULL, NULL) AS boolean)"
        );
        assert!(rewrite_sql_json_functions("SELECT 'json_value(x, y)'").is_none());
        let query = rewrite_sql_json_functions(
            "SELECT JSON_QUERY(j, '$.a[*]' WITH CONDITIONAL ARRAY WRAPPER EMPTY OBJECT ON EMPTY)",
        )
        .unwrap();
        assert!(query.contains("\"wrapper\":\"conditional\""), "{query}");
        assert!(query.contains("\"on_empty\":\"empty_object\""), "{query}");
    }

    fn call(name: &str, context: &str, path: &str, spec: &str) -> Result<Option<SqlValue>> {
        eval_sql_json_function(
            name,
            &[
                SqlValue::String(context.into()),
                SqlValue::String(path.into()),
                SqlValue::String(spec.into()),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Null,
            ],
        )
    }

    #[test]
    fn value_query_and_exists_follow_postgres() {
        let text = r#"{"returning":"text"}"#;
        assert_eq!(
            call("bicdb_json_value", r#"{"a":{"b":5}}"#, "$.a.b", text).unwrap(),
            Some(SqlValue::String("5".into()))
        );
        assert_eq!(
            call("bicdb_json_value", r#"{"a":[1]}"#, "$.a", text).unwrap(),
            Some(SqlValue::Null)
        );
        assert_eq!(
            call(
                "bicdb_json_value",
                r#"{"a":[1,2]}"#,
                "$.a",
                r#"{"returning":"text","on_error":"error"}"#
            )
            .unwrap_err()
            .sqlstate(),
            "2203F"
        );
        let wrapped = call(
            "bicdb_json_query",
            r#"{"a":[1,2]}"#,
            "$.a[*]",
            r#"{"returning":"jsonb","wrapper":"unconditional"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(wrapped.to_cell(), "[1,2]");
        assert_eq!(
            call(
                "bicdb_json_exists",
                r#"{"a":1}"#,
                "$.b",
                r#"{"returning":"boolean"}"#
            )
            .unwrap(),
            Some(SqlValue::Bool(false))
        );
    }
}
