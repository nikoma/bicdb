//! The `hstore` extension type: sets of text keys with text-or-NULL values.
//! Values are stored as PostgreSQL's canonical output text (`"a"=>"1",
//! "b"=>NULL`, pairs ordered by key length and then bytes, one pair per
//! key), so stored values compare and print as PostgreSQL's do. Operators
//! bind to the functions here through the user-defined operator catalog.
use crate::*;

pub(crate) type Pairs = Vec<(String, Option<String>)>;

/// PostgreSQL's hstore input errors: the character at char index `at`, and
/// its byte position, or the end of the string.
fn syntax_error(chars: &[char], at: usize) -> SqlError {
    let message = match chars.get(at) {
        None => "syntax error in hstore: unexpected end of string".to_string(),
        Some(ch) => format!(
            "syntax error in hstore, near \"{ch}\" at position {}",
            chars[..at].iter().map(|ch| ch.len_utf8()).sum::<usize>()
        ),
    };
    SqlError::data_exception("42601", message, None)
}

/// Parses hstore input text (`a=>1, "b c"=>NULL`).
pub(crate) fn parse(input: &str) -> Result<Pairs> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut at = 0;
    let skip_space = |at: &mut usize| {
        while *at < chars.len() && chars[*at].is_whitespace() {
            *at += 1;
        }
    };
    // A quoted string (backslash escapes) or a run of non-space characters
    // up to `=`, `,` or the end; `None` value marks an unquoted NULL.
    let token = |at: &mut usize, is_value: bool| -> Result<(String, bool)> {
        if *at < chars.len() && chars[*at] == '"' {
            *at += 1;
            let mut out = String::new();
            loop {
                let Some(&ch) = chars.get(*at) else {
                    return Err(syntax_error(&chars, *at));
                };
                *at += 1;
                match ch {
                    '\\' => {
                        let Some(&next) = chars.get(*at) else {
                            return Err(syntax_error(&chars, *at));
                        };
                        *at += 1;
                        out.push(next);
                    }
                    '"' => return Ok((out, true)),
                    other => out.push(other),
                }
            }
        }
        let mut out = String::new();
        while let Some(&ch) = chars.get(*at) {
            if ch.is_whitespace() || ch == ',' || (!is_value && ch == '=') {
                break;
            }
            if ch == '\\' {
                *at += 1;
                let Some(&next) = chars.get(*at) else {
                    return Err(syntax_error(&chars, *at));
                };
                out.push(next);
                *at += 1;
                continue;
            }
            out.push(ch);
            *at += 1;
        }
        if out.is_empty() {
            return Err(syntax_error(&chars, *at));
        }
        Ok((out, false))
    };
    let mut pairs = Pairs::new();
    skip_space(&mut at);
    while at < chars.len() {
        let (key, _) = token(&mut at, false)?;
        skip_space(&mut at);
        if chars.get(at) != Some(&'=') {
            return Err(syntax_error(&chars, at));
        }
        if chars.get(at + 1) != Some(&'>') {
            return Err(syntax_error(&chars, at + 1));
        }
        at += 2;
        skip_space(&mut at);
        let (value, quoted) = token(&mut at, true)?;
        let value = (quoted || !value.eq_ignore_ascii_case("null")).then_some(value);
        pairs.push((key, value));
        skip_space(&mut at);
        match chars.get(at) {
            None => break,
            Some(',') => {
                at += 1;
                skip_space(&mut at);
            }
            Some(_) => return Err(syntax_error(&chars, at)),
        }
    }
    Ok(normalize(pairs))
}

/// PostgreSQL's storage order (key length, then bytes) with one pair per key;
/// of duplicate keys the first one written is kept.
pub(crate) fn normalize(pairs: Pairs) -> Pairs {
    let mut unique: Pairs = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        if !unique.iter().any(|(existing, _)| *existing == key) {
            unique.push((key, value));
        }
    }
    unique.sort_by(|left, right| {
        left.0
            .len()
            .cmp(&right.0.len())
            .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
    });
    unique
}

fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

/// PostgreSQL's hstore output text.
pub(crate) fn format(pairs: &Pairs) -> String {
    pairs
        .iter()
        .map(|(key, value)| match value {
            Some(value) => format!("{}=>{}", quote(key), quote(value)),
            None => format!("{}=>NULL", quote(key)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Canonicalizes a value cast to hstore.
pub(crate) fn canonical_value(value: SqlValue) -> Result<SqlValue> {
    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        value => Ok(SqlValue::String(format(&parse(&value.to_cell())?))),
    }
}

fn pairs_arg(value: &SqlValue) -> Result<Pairs> {
    parse(&value.to_cell())
}

fn text_array_arg(value: &SqlValue) -> Result<Vec<Option<String>>> {
    let array = cast_value_to_array(value.clone(), "text")?;
    let SqlValue::Json(json) = array else {
        return Ok(Vec::new());
    };
    let JsonValue::Array(items) = SqlValue::into_json_value(json) else {
        return Ok(Vec::new());
    };
    Ok(items
        .into_iter()
        .map(|item| match item {
            JsonValue::Null => None,
            JsonValue::String(text) => Some(text),
            other => Some(other.to_string()),
        })
        .collect())
}

fn hstore_value(pairs: Pairs) -> SqlValue {
    SqlValue::String(format(&normalize(pairs)))
}

fn lookup<'a>(pairs: &'a Pairs, key: &str) -> Option<&'a Option<String>> {
    pairs
        .iter()
        .find(|(candidate, _)| candidate == key)
        .map(|(_, value)| value)
}

/// `left @> right`: every pair of `right` is in `left`.
fn contains(left: &Pairs, right: &Pairs) -> bool {
    right
        .iter()
        .all(|(key, value)| lookup(left, key).is_some_and(|found| found == value))
}

/// Evaluates the hstore functions; `None` for any other name.
pub(crate) fn eval_hstore_function(name: &str, args: &[SqlValue]) -> Result<Option<SqlValue>> {
    let bare = name.strip_prefix("public.").unwrap_or(name);
    let strict = |args: &[SqlValue]| args.iter().any(|arg| matches!(arg, SqlValue::Null));
    let value = match (bare, args) {
        ("hstore_fetchval", [h, key]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            lookup(&pairs_arg(h)?, &key.to_cell())
                .cloned()
                .flatten()
                .map(SqlValue::String)
                .unwrap_or(SqlValue::Null)
        }
        ("hstore_exist", [h, key]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            SqlValue::Bool(lookup(&pairs_arg(h)?, &key.to_cell()).is_some())
        }
        ("hstore_exists_any" | "hstore_exists_all", [h, keys]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let pairs = pairs_arg(h)?;
            let keys = text_array_arg(keys)?;
            let present = |key: &Option<String>| {
                key.as_deref()
                    .is_some_and(|key| lookup(&pairs, key).is_some())
            };
            let keys = keys.iter().filter(|key| key.is_some());
            SqlValue::Bool(if bare == "hstore_exists_any" {
                keys.clone().any(present)
            } else {
                keys.clone().all(present)
            })
        }
        ("hstore_contains" | "hstore_contained", [left, right]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let (left, right) = (pairs_arg(left)?, pairs_arg(right)?);
            SqlValue::Bool(if bare == "hstore_contains" {
                contains(&left, &right)
            } else {
                contains(&right, &left)
            })
        }
        ("hstore_concat", [left, right]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let mut pairs = pairs_arg(right)?;
            pairs.extend(pairs_arg(left)?);
            hstore_value(pairs)
        }
        ("hstore_delete", [h, key]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let key = key.to_cell();
            hstore_value(
                pairs_arg(h)?
                    .into_iter()
                    .filter(|(candidate, _)| *candidate != key)
                    .collect(),
            )
        }
        ("hstore_delete_array", [h, keys]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let keys = text_array_arg(keys)?;
            hstore_value(
                pairs_arg(h)?
                    .into_iter()
                    .filter(|(candidate, _)| {
                        !keys.iter().any(|key| key.as_deref() == Some(candidate))
                    })
                    .collect(),
            )
        }
        ("hstore_defined", [h, key]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            SqlValue::Bool(lookup(&pairs_arg(h)?, &key.to_cell()).is_some_and(Option::is_some))
        }
        ("hstore_slice", [h, keys]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let keys = text_array_arg(keys)?;
            hstore_value(
                pairs_arg(h)?
                    .into_iter()
                    .filter(|(candidate, _)| {
                        keys.iter().any(|key| key.as_deref() == Some(candidate))
                    })
                    .collect(),
            )
        }
        ("akeys", [h]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            array_value(
                pairs_arg(h)?
                    .into_iter()
                    .map(|(key, _)| SqlValue::String(key))
                    .collect(),
            )
        }
        ("avals", [h]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            array_value(
                pairs_arg(h)?
                    .into_iter()
                    .map(|(_, value)| value.map(SqlValue::String).unwrap_or(SqlValue::Null))
                    .collect(),
            )
        }
        ("hstore_to_array", [h]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            array_value(
                pairs_arg(h)?
                    .into_iter()
                    .flat_map(|(key, value)| {
                        [
                            SqlValue::String(key),
                            value.map(SqlValue::String).unwrap_or(SqlValue::Null),
                        ]
                    })
                    .collect(),
            )
        }
        ("hstore_to_json" | "hstore_to_jsonb", [h]) => {
            if strict(args) {
                return Ok(Some(SqlValue::Null));
            }
            let pairs = pairs_arg(h)?;
            if bare == "hstore_to_json" {
                // json keeps the hstore order and PostgreSQL's spacing.
                let body = pairs
                    .iter()
                    .map(|(key, value)| {
                        format!(
                            "{}: {}",
                            JsonValue::String(key.clone()),
                            value
                                .as_ref()
                                .map(|value| JsonValue::String(value.clone()).to_string())
                                .unwrap_or_else(|| "null".to_string())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                SqlValue::String(format!("{{{body}}}"))
            } else {
                let mut object = serde_json::Map::new();
                for (key, value) in pairs {
                    object.insert(key, value.map(JsonValue::String).unwrap_or(JsonValue::Null));
                }
                SqlValue::Json(JsonValue::Object(object).into())
            }
        }
        ("hstore", [key, value]) => {
            if matches!(key, SqlValue::Null) {
                return Ok(Some(SqlValue::Null));
            }
            let is_array = |value: &SqlValue| matches!(value, SqlValue::Json(json) if json.as_ref().is_array());
            if is_array(key) || is_array(value) {
                let keys = text_array_arg(key)?;
                let values = match value {
                    SqlValue::Null => vec![None; keys.len()],
                    value => text_array_arg(value)?,
                };
                if keys.len() != values.len() {
                    return Err(SqlError::data_exception(
                        "2202E",
                        "arrays must have same bounds",
                        None,
                    ));
                }
                let mut pairs = Pairs::new();
                for (key, value) in keys.into_iter().zip(values) {
                    let Some(key) = key else {
                        return Err(SqlError::data_exception(
                            "22004",
                            "null value not allowed for hstore key",
                            None,
                        ));
                    };
                    pairs.push((key, value));
                }
                hstore_value(pairs)
            } else {
                let value = match value {
                    SqlValue::Null => None,
                    value => Some(value.to_cell()),
                };
                hstore_value(vec![(key.to_cell(), value)])
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(value))
}

/// The SQL functions `CREATE EXTENSION hstore` defines over the native ones,
/// as (signature, definition); `{hstore}` is the type's qualified name.
pub(crate) const SQL_FUNCTIONS: &[(&str, &str)] = &[
    (
        "exist({hstore}, text) RETURNS boolean",
        "SELECT hstore_exist($1, $2)",
    ),
    (
        "defined({hstore}, text) RETURNS boolean",
        "SELECT hstore_defined($1, $2)",
    ),
    (
        "delete({hstore}, text) RETURNS {hstore}",
        "SELECT hstore_delete($1, $2)",
    ),
    (
        "delete({hstore}, text[]) RETURNS {hstore}",
        "SELECT hstore_delete_array($1, $2)",
    ),
    (
        "slice({hstore}, text[]) RETURNS {hstore}",
        "SELECT hstore_slice($1, $2)",
    ),
    (
        "skeys({hstore}) RETURNS SETOF text",
        "SELECT unnest(akeys($1))",
    ),
    (
        "svals({hstore}) RETURNS SETOF text",
        "SELECT unnest(avals($1))",
    ),
    (
        "each(hs {hstore}, OUT key text, OUT value text) RETURNS SETOF record",
        "SELECT * FROM jsonb_each_text(hstore_to_jsonb($1))",
    ),
];

/// The operators `CREATE EXTENSION hstore` defines, as
/// (name, left type, right type, function, result type).
pub(crate) const OPERATORS: &[(&str, &str, &str, &str, &str)] = &[
    ("->", "hstore", "text", "hstore_fetchval", "text"),
    ("?", "hstore", "text", "hstore_exist", "bool"),
    ("?|", "hstore", "text[]", "hstore_exists_any", "bool"),
    ("?&", "hstore", "text[]", "hstore_exists_all", "bool"),
    ("@>", "hstore", "hstore", "hstore_contains", "bool"),
    ("<@", "hstore", "hstore", "hstore_contained", "bool"),
    ("||", "hstore", "hstore", "hstore_concat", "hstore"),
    ("-", "hstore", "text", "hstore_delete", "hstore"),
    ("-", "hstore", "text[]", "hstore_delete_array", "hstore"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_is_canonicalized_like_postgresql() {
        assert_eq!(
            format(&parse("b=>1, aa=>2, \"c d\"=>NULL, e=>\"NULL\"").unwrap()),
            "\"b\"=>\"1\", \"e\"=>\"NULL\", \"aa\"=>\"2\", \"c d\"=>NULL"
        );
        assert_eq!(format(&parse("a=>1, a=>2").unwrap()), "\"a\"=>\"1\"");
        assert_eq!(format(&parse("").unwrap()), "");
        assert!(parse("a=>").is_err());
        assert!(parse("a=1").is_err());
        assert_eq!(
            format(&parse(r#""q\"x"=>"b\\s""#).unwrap()),
            r#""q\"x"=>"b\\s""#
        );
    }
}
