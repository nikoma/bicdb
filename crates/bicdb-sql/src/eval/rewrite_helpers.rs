//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

pub(crate) fn literal_to_value(value: &ValueWithSpan) -> Result<SqlValue> {
    match &value.value {
        Value::Null => Ok(SqlValue::Null),
        Value::Boolean(value) => Ok(SqlValue::Bool(*value)),
        Value::DollarQuotedString(value) => text_value_without_nul(value.value.clone(), "text"),
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::TripleSingleQuotedString(value)
        | Value::TripleDoubleQuotedString(value) => text_value_without_nul(value.clone(), "text"),
        Value::SingleQuotedByteStringLiteral(value)
        | Value::DoubleQuotedByteStringLiteral(value)
        | Value::TripleSingleQuotedByteStringLiteral(value)
        | Value::TripleDoubleQuotedByteStringLiteral(value) => PgBitString::from_bit_text(value)
            .map(|value| SqlValue::String(value.to_bit_text()))
            .map_err(|_| bit_input_error("bit", &format!("b{value}"))),
        Value::HexStringLiteral(value) => PgBitString::from_hex_text(value)
            .map(|value| SqlValue::String(value.to_bit_text()))
            .map_err(|_| bit_input_error("bit", &format!("x{value}"))),
        // Escape (E'...') and Unicode-escape (U&'...') strings: the tokenizer
        // already decoded the value; decoding it again lost backslashes.
        Value::EscapedStringLiteral(value) | Value::UnicodeStringLiteral(value) => {
            text_value_without_nul(value.clone(), "text")
        }
        Value::Number(value, _) => {
            if value.contains(['.', 'e', 'E']) {
                if is_valid_numeric_text(value) {
                    Ok(SqlValue::String(value.clone()))
                } else {
                    value
                        .parse::<f64>()
                        .map(SqlValue::Float)
                        .map_err(|error| SqlError::InvalidSql(error.to_string()))
                }
            } else {
                match value.parse::<i64>() {
                    Ok(value) => Ok(SqlValue::Int(value)),
                    Err(_) if is_valid_numeric_text(value) => Ok(SqlValue::String(value.clone())),
                    Err(error) => Err(SqlError::InvalidSql(error.to_string())),
                }
            }
        }
        other => Err(SqlError::Unsupported(format!(
            "unsupported literal {other}"
        ))),
    }
}

pub(crate) fn typed_string_to_value(value: &TypedString) -> Result<SqlValue> {
    let literal = literal_to_value(&value.value)?;
    if let Some(input) = typed_string_builtin_input(&literal, &value.data_type) {
        return input;
    }
    cast_value(literal, &value.data_type)
}

pub(crate) fn typed_string_to_value_with_db(db: &BicDb, value: &TypedString) -> Result<SqlValue> {
    let literal = literal_to_value(&value.value)?;
    if let Some(input) = typed_string_builtin_input(&literal, &value.data_type) {
        return input;
    }
    cast_value_with_db(db, literal, &value.data_type)
}

/// Typed literals go through their type's input function.
fn typed_string_builtin_input(
    literal: &SqlValue,
    data_type: &DataType,
) -> Option<Result<SqlValue>> {
    let (pg_type, _) = pg_type_from_data_type(data_type).ok()?;
    if pg_type == "xml" && crate::xml::xml_input_is_document() {
        return Some(crate::xml::xml_input_value(literal.clone()));
    }
    text_input_integer_value(literal, &pg_type)
}

/// A string literal (an unknown-typed constant), looking through parentheses.
pub(crate) fn is_string_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => is_string_literal_expr(inner),
        Expr::Value(value) => matches!(
            value.value,
            Value::SingleQuotedString(_)
                | Value::DoubleQuotedString(_)
                | Value::EscapedStringLiteral(_)
                | Value::DollarQuotedString(_)
                | Value::TripleSingleQuotedString(_)
                | Value::TripleDoubleQuotedString(_)
        ),
        _ => false,
    }
}

pub(crate) fn json_path<'a>(value: &'a JsonValue, path: &[String]) -> Option<&'a JsonValue> {
    let mut current = value;
    for part in path {
        current = current.get(part)?;
    }
    Some(current)
}

pub(crate) fn json_path_case_insensitive<'a>(
    value: &'a JsonValue,
    path: &[String],
) -> Option<&'a JsonValue> {
    let mut current = value;
    for part in path {
        current = current.get(part).or_else(|| {
            current.as_object().and_then(|object| {
                object
                    .iter()
                    .find_map(|(key, value)| key.eq_ignore_ascii_case(part).then_some(value))
            })
        })?;
    }
    Some(current)
}

pub(crate) fn json_to_sql_value(value: &JsonValue) -> SqlValue {
    match value {
        JsonValue::Null => SqlValue::Null,
        JsonValue::Bool(value) => SqlValue::Bool(*value),
        JsonValue::Number(value) => value
            .as_i64()
            .map(SqlValue::Int)
            .or_else(|| value.as_f64().map(SqlValue::Float))
            .unwrap_or_else(|| SqlValue::json(JsonValue::Number(value.clone()))),
        JsonValue::String(value) => SqlValue::String(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => SqlValue::json(value.clone()),
    }
}

/// Lowercased function/object name with a borrow fast path: a single
/// unquoted, already-lowercase identifier (every hot routine name)
/// costs zero allocations, versus the four the Display + trim + join +
/// to_ascii_lowercase pipeline paid per expression evaluation.
pub(crate) fn object_name_lowercase(name: &ObjectName) -> Result<Cow<'_, str>> {
    if let [part] = name.0.as_slice() {
        if let sqlparser::ast::ObjectNamePart::Identifier(ident) = part {
            if ident.quote_style.is_none() {
                if ident.value.bytes().any(|b| b.is_ascii_uppercase()) {
                    return Ok(Cow::Owned(ident.value.to_ascii_lowercase()));
                }
                return Ok(Cow::Borrowed(ident.value.as_str()));
            }
        }
    }
    Ok(Cow::Owned(object_name(name)?.to_ascii_lowercase()))
}

thread_local! {
    pub(crate) static SQL_ARG_POOL: RefCell<Vec<Vec<SqlValue>>> = const { RefCell::new(Vec::new()) };
}

/// Reusable argument buffer for function-call evaluation: the Vec's capacity
/// is recycled across calls (evaluation nests strictly, so a per-thread stack
/// of spare buffers suffices), removing one heap allocation per function
/// call evaluated. Returned to the pool on drop, early `?` exits included.
pub(crate) struct PooledArgs(Vec<SqlValue>);

impl PooledArgs {
    pub(crate) fn take() -> Self {
        Self(
            SQL_ARG_POOL
                .with(|pool| pool.borrow_mut().pop())
                .unwrap_or_default(),
        )
    }
}

impl Drop for PooledArgs {
    fn drop(&mut self) {
        let mut buffer = std::mem::take(&mut self.0);
        buffer.clear();
        SQL_ARG_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < 32 {
                pool.push(buffer);
            }
        });
    }
}

impl std::ops::Deref for PooledArgs {
    type Target = Vec<SqlValue>;
    fn deref(&self) -> &Vec<SqlValue> {
        &self.0
    }
}

impl std::ops::DerefMut for PooledArgs {
    fn deref_mut(&mut self) -> &mut Vec<SqlValue> {
        &mut self.0
    }
}

pub(crate) fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident().map_or_else(
                || part.to_string().trim_matches('"').to_string(),
                |ident| {
                    if ident.quote_style.is_some() {
                        ident.value.clone()
                    } else {
                        ident.value.to_ascii_lowercase()
                    }
                },
            )
        })
        .collect()
}

pub(crate) fn object_name(name: &ObjectName) -> Result<String> {
    Ok(object_name_parts(name).join("."))
}

pub(crate) fn schema_name_value(name: &SchemaName) -> Result<String> {
    match name {
        SchemaName::Simple(name) => object_name(name),
        SchemaName::NamedAuthorization(name, _) => object_name(name),
        SchemaName::UnnamedAuthorization(authorization) => Ok(ident_value(authorization)),
    }
}

pub(crate) fn relation_name(name: &ObjectName) -> Result<String> {
    relation_name_from_parts(&object_name_parts(name))
}

/// Parts form of [`relation_name`] for callers holding an already-split,
/// already-normalized dotted name rather than an AST `ObjectName` — the
/// COPY target parsed off the wire. Sharing one implementation keeps the
/// schema encoding identical to INSERT/SELECT; COPY previously bypassed it
/// and silently wrote into `public`.
pub(crate) fn relation_name_from_parts(parts: &[String]) -> Result<String> {
    let direct_name = |name: &str| {
        !name.is_empty()
            && name.len() <= 255
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    };
    match parts {
        [schema, table] if schema == "pg_catalog" => Ok(format!("pg_catalog.{table}")),
        [schema, table] if schema == "information_schema" => {
            Ok(format!("information_schema.{table}"))
        }
        // Durable aggregate projections are virtual relations, not stored
        // collections, so the qualifier must survive rather than being
        // encoded into a physical collection name.
        [schema, table] if schema == "bicdb_projection" => Ok(format!("bicdb_projection.{table}")),
        [schema, table] if schema == "public" && direct_name(table) => Ok(table.clone()),
        // `pg_temp` names the session's own temporary schema.
        [schema, table] if schema == "pg_temp" => match crate::search_path::active_temp_schema() {
            Some(temp) => relation_name_from_parts(&[temp, table.clone()]),
            None => relation_name_from_parts(&["pg_temp_".to_string(), table.clone()]),
        },
        [schema, table] => {
            // Core collections have no schema dimension. Encode a
            // non-public SQL schema, or a quoted public name that cannot be
            // stored directly, into a path-safe, collision-resistant
            // physical relation name instead of silently discarding it. The
            // table component is hex so quoted identifiers cannot inject
            // separators; the fixed schema digest keeps the durable name
            // under the core's 255-byte limit for PostgreSQL-sized names.
            let schema_digest = Sha256::digest(schema.as_bytes());
            Ok(format!(
                "__bicdb_s_{}_{}",
                hex::encode(schema_digest),
                hex::encode(table.as_bytes())
            ))
        }
        [table] if table.starts_with("__bicdb_s_") => Err(SqlError::InvalidSql(
            "relation names beginning with `__bicdb_s_` are reserved for schema isolation"
                .to_string(),
        )),
        [table] if direct_name(table) => Ok(table.clone()),
        [table] => relation_name_from_parts(&["public".to_string(), table.clone()]),
        _ => Ok(parts.join(".")),
    }
}

/// Index metadata retains its logical spelling. Index storage performs its
/// own encoding with the owning table's namespace.
pub(crate) fn index_relation_name(name: &ObjectName) -> Result<String> {
    let parts = object_name_parts(name);
    match parts.as_slice() {
        [index] if index.starts_with("__bicdb_s_") => relation_name_from_parts(&parts),
        [index] => Ok(index.clone()),
        [schema, index] if schema == "public" => Ok(index.clone()),
        _ => relation_name_from_parts(&parts),
    }
}

pub(crate) fn relation_default_alias(name: &ObjectName) -> String {
    let last = object_name_parts(name).last().cloned().unwrap_or_default();
    logical_relation_name(&last).unwrap_or(last)
}

/// Recovers the SQL relation name from a schema-isolated physical name.
///
/// `__bicdb_s_<sha256(schema)>_<hex(table)>` encodes the table half as hex, so
/// the logical name is recoverable even though the schema digest is not. The
/// catalogs know the namespace separately, so that is enough to render a
/// relation the way PostgreSQL would. Without this, `pg_class.relname` for any
/// table outside `public` was the mangled physical name, and casting
/// `'schema.table'::regclass` could not find it at all.
pub(crate) fn logical_relation_name(physical: &str) -> Option<String> {
    const PREFIX: &str = "__bicdb_s_";
    const DIGEST_HEX_LEN: usize = 64;
    let rest = physical.strip_prefix(PREFIX)?;
    let (digest, table_hex) = rest.split_at_checked(DIGEST_HEX_LEN)?;
    if !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let table_hex = table_hex.strip_prefix('_')?;
    // Derived names append a suffix after the hex, e.g. `<encoded>_pkey`. Hex
    // digits never include `_`, so the first one ends the encoded name and the
    // remainder is carried through unchanged.
    let (encoded, suffix) = match table_hex.find('_') {
        Some(at) => (&table_hex[..at], &table_hex[at..]),
        None => (table_hex, ""),
    };
    let bytes = hex::decode(encoded).ok()?;
    let name = String::from_utf8(bytes).ok()?;
    Some(format!("{name}{suffix}"))
}

/// Earlier public views could use logical names that core collections cannot
/// store. Only the public encoding may fall back to those legacy catalog keys.
pub(crate) fn legacy_public_relation_name(physical: &str) -> Option<String> {
    let logical = logical_relation_name(physical)?;
    let encoded = relation_name_from_parts(&["public".to_string(), logical.clone()]).ok()?;
    (encoded == physical).then_some(logical)
}

pub(crate) fn relation_schema_name(name: &ObjectName) -> String {
    let parts = object_name_parts(name);
    match parts.as_slice() {
        [schema, _] => schema.clone(),
        [_, schema, _] => schema.clone(),
        [_] => "public".to_string(),
        [] => "public".to_string(),
        parts => parts
            .get(parts.len().saturating_sub(2))
            .cloned()
            .unwrap_or_else(|| "public".to_string()),
    }
}

pub(crate) fn normalize_object_name(name: &str) -> String {
    normalized_object_name_ref(name).into_owned()
}

/// Borrowing twin of [`normalize_object_name`] for lookup-only callers: names
/// that are already normalized (no qualifier, no quotes, no uppercase — the
/// overwhelmingly common case on hot paths) are returned as-is with no
/// allocation and no reverse character search.
pub(crate) fn normalized_object_name_ref(name: &str) -> Cow<'_, str> {
    if name
        .bytes()
        .all(|b| b != b'.' && b != b'"' && !b.is_ascii_uppercase())
    {
        return Cow::Borrowed(name);
    }
    let last = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
    if last.bytes().all(|b| !b.is_ascii_uppercase()) {
        Cow::Borrowed(last)
    } else {
        Cow::Owned(last.to_ascii_lowercase())
    }
}

pub(crate) fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

/// `top` is an ordinary identifier in both SQL profiles, but the SQL parser
/// always reads `SELECT [ALL | DISTINCT [ON (...)]] top` as the start of a
/// `TOP n` row-limit clause, so a column, parameter or variable named `top`
/// failed to parse as the first select item. Quote that one occurrence; an
/// unquoted `top` folds to the same lower-case name.
pub(crate) fn rewrite_select_top_identifiers(sql: &str) -> Option<String> {
    if !sql
        .as_bytes()
        .windows(3)
        .any(|window| window.eq_ignore_ascii_case(b"top"))
    {
        return None;
    }
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Other,
        // After SELECT, ALL, DISTINCT or DISTINCT ON (...).
        SelectHead,
        // After DISTINCT: ON (...) may follow.
        AfterDistinct,
        // After DISTINCT ON: the parenthesized list follows.
        DistinctOn,
    }
    let bytes = sql.as_bytes();
    let mut state = State::Other;
    let mut replacements = Vec::new();
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if byte.is_ascii_whitespace() {
            idx += 1;
            continue;
        }
        if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
            while idx < bytes.len() && bytes[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
            let mut depth = 0_usize;
            while idx < bytes.len() {
                if bytes[idx] == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                    depth += 1;
                    idx += 2;
                } else if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                    depth -= 1;
                    idx += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    idx += 1;
                }
            }
            continue;
        }
        if byte == b'\'' {
            let backslash_escapes = idx > 0 && bytes[idx - 1].eq_ignore_ascii_case(&b'e') && {
                idx < 2 || !(bytes[idx - 2].is_ascii_alphanumeric() || bytes[idx - 2] == b'_')
            };
            idx += 1;
            while idx < bytes.len() {
                if backslash_escapes && bytes[idx] == b'\\' {
                    idx += 2;
                } else if bytes[idx] == b'\'' {
                    if bytes.get(idx + 1) == Some(&b'\'') {
                        idx += 2;
                    } else {
                        idx += 1;
                        break;
                    }
                } else {
                    idx += 1;
                }
            }
            state = State::Other;
            continue;
        }
        if byte == b'"' {
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'"' {
                    if bytes.get(idx + 1) == Some(&b'"') {
                        idx += 2;
                    } else {
                        idx += 1;
                        break;
                    }
                } else {
                    idx += 1;
                }
            }
            state = State::Other;
            continue;
        }
        if byte == b'$' {
            let tag_end = bytes[idx + 1..]
                .iter()
                .position(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'_'))
                .map(|offset| idx + 1 + offset);
            if let Some(tag_end) = tag_end.filter(|end| {
                bytes[*end] == b'$' && !bytes.get(idx + 1).is_some_and(u8::is_ascii_digit)
            }) {
                let tag = &bytes[idx..=tag_end];
                let body = tag_end + 1;
                idx = bytes[body..]
                    .windows(tag.len())
                    .position(|window| window == tag)
                    .map_or(bytes.len(), |offset| body + offset + tag.len());
            } else {
                idx += 1;
                while idx < bytes.len() && bytes[idx].is_ascii_digit() {
                    idx += 1;
                }
            }
            state = State::Other;
            continue;
        }
        if byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80 {
            let start = idx;
            while idx < bytes.len()
                && (bytes[idx].is_ascii_alphanumeric()
                    || matches!(bytes[idx], b'_' | b'$')
                    || bytes[idx] >= 0x80)
            {
                idx += 1;
            }
            let word = &sql[start..idx];
            state = match state {
                _ if word.eq_ignore_ascii_case("select") => State::SelectHead,
                State::SelectHead if word.eq_ignore_ascii_case("all") => State::SelectHead,
                State::SelectHead if word.eq_ignore_ascii_case("distinct") => State::AfterDistinct,
                State::AfterDistinct if word.eq_ignore_ascii_case("on") => State::DistinctOn,
                State::SelectHead | State::AfterDistinct if word.eq_ignore_ascii_case("top") => {
                    replacements.push(start..idx);
                    State::Other
                }
                _ => State::Other,
            };
            continue;
        }
        if byte == b'(' && state == State::DistinctOn {
            let mut depth = 0_usize;
            while idx < bytes.len() {
                match bytes[idx] {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            idx += 1;
                            break;
                        }
                    }
                    _ => {}
                }
                idx += 1;
            }
            state = State::SelectHead;
            continue;
        }
        state = State::Other;
        idx += 1;
    }
    if replacements.is_empty() {
        return None;
    }
    let mut rewritten = String::with_capacity(sql.len() + 2 * replacements.len());
    let mut copied = 0;
    for range in replacements {
        rewritten.push_str(&sql[copied..range.start]);
        rewritten.push_str("\"top\"");
        copied = range.end;
    }
    rewritten.push_str(&sql[copied..]);
    Some(rewritten)
}

/// `SELECT pg_partition_ancestors(x)` returns a set of OIDs; it runs as
/// `SELECT unnest(bicdb_partition_ancestor_oids(x))`, the partition itself
/// first and then each ancestor, as PostgreSQL returns them.
pub(crate) fn rewrite_partition_ancestor_projection(sql: &str) -> Option<String> {
    if !sql.to_ascii_lowercase().contains("pg_partition_ancestors") {
        return None;
    }
    let tokens = tokens_with_source(&sqlparser::dialect::PostgreSqlDialect {}, sql)?;
    let code = tokens
        .iter()
        .enumerate()
        .filter(|(_, (token, _))| !matches!(token, Token::Whitespace(_)))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let word = |index: usize, value: &str| matches!(&tokens[index].0, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value));
    // Replacement text for token ranges: (start, end exclusive, text).
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    let mut close_after: Vec<usize> = Vec::new();
    // `FROM pg_partition_ancestors(x)` (and `FROM a, pg_partition_ancestors(x)`):
    // the table form, column `relid`.
    let in_from_list = |position: usize| -> bool {
        let mut depth = 0i32;
        for back in (0..position).rev() {
            let token = &tokens[code[back]].0;
            match token {
                Token::RParen => depth += 1,
                Token::LParen => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                }
                _ if depth > 0 => {}
                Token::Word(word) if word.quote_style.is_none() => {
                    let value = word.value.to_ascii_lowercase();
                    if matches!(value.as_str(), "from" | "join") {
                        return true;
                    }
                    if matches!(
                        value.as_str(),
                        "select" | "where" | "on" | "group" | "order" | "having" | "values"
                    ) {
                        return false;
                    }
                }
                _ => {}
            }
        }
        false
    };
    let mut from_edits: Vec<(usize, usize, String)> = Vec::new();
    let mut from_alias_after: Vec<usize> = Vec::new();
    for position in 0..code.len() {
        let index = code[position];
        let (name_start, name_end) = if word(index, "pg_catalog")
            && code
                .get(position + 1)
                .is_some_and(|&next| matches!(tokens[next].0, Token::Period))
        {
            (position, position + 2)
        } else {
            (position, position)
        };
        let Some(&function) = code.get(name_end) else {
            continue;
        };
        if !word(function, "pg_partition_ancestors")
            || !code
                .get(name_end + 1)
                .is_some_and(|&next| matches!(tokens[next].0, Token::LParen))
            || name_start == 0
        {
            continue;
        }
        let previous = &tokens[code[name_start - 1]].0;
        let from_form = matches!(previous, Token::Word(word) if word.value.eq_ignore_ascii_case("from") || word.value.eq_ignore_ascii_case("join"))
            || (matches!(previous, Token::Comma) && in_from_list(name_start));
        if !from_form {
            continue;
        }
        let open = code[name_end + 1];
        let mut depth = 0usize;
        let mut close_position = None;
        for (offset, &candidate) in code[name_end + 1..].iter().enumerate() {
            match tokens[candidate].0 {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close_position = Some(name_end + 1 + offset);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close_position) = close_position else {
            continue;
        };
        // A constant relation name (`FROM pg_partition_ancestors('t')`) is
        // the table function's own form, which returns regclass values.
        if close_position == name_end + 3
            && matches!(tokens[code[name_end + 2]].0, Token::SingleQuotedString(_))
        {
            continue;
        }
        let aliased = code.get(close_position + 1).is_some_and(|&next| {
            matches!(&tokens[next].0, Token::Word(word)
            if word.value.eq_ignore_ascii_case("as")
                || !matches!(
                    word.value.to_ascii_lowercase().as_str(),
                    "where" | "join" | "on" | "group" | "order" | "limit" | "union"
                        | "inner" | "left" | "right" | "full" | "cross" | "having"
                        | "offset" | "except" | "intersect" | "window"
                ))
        });
        from_edits.push((
            code[name_start],
            open,
            "unnest(pg_catalog.bicdb_partition_ancestor_oids".to_string(),
        ));
        from_alias_after.push(code[close_position]);
        if aliased {
            from_alias_after.pop();
            from_alias_after.push(usize::MAX - code[close_position]);
        }
    }
    for position in 0..code.len() {
        let index = code[position];
        if !word(index, "select") {
            continue;
        }
        let mut name_start = position + 1;
        let mut name_end = name_start;
        if code
            .get(name_start)
            .is_some_and(|&next| word(next, "pg_catalog"))
            && code
                .get(name_start + 1)
                .is_some_and(|&next| matches!(tokens[next].0, Token::Period))
        {
            name_end = name_start + 2;
        }
        let Some(&function) = code.get(name_end) else {
            continue;
        };
        if !word(function, "pg_partition_ancestors")
            || !code
                .get(name_end + 1)
                .is_some_and(|&next| matches!(tokens[next].0, Token::LParen))
        {
            continue;
        }
        let open = code[name_end + 1];
        let mut depth = 0usize;
        let mut close = None;
        for &candidate in &code[name_end + 1..] {
            match tokens[candidate].0 {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(candidate);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(close) = close else {
            continue;
        };
        name_start = code[name_start];
        edits.push((
            name_start,
            open,
            "unnest(pg_catalog.bicdb_partition_ancestor_oids".to_string(),
        ));
        close_after.push(close);
    }
    if edits.is_empty() && from_edits.is_empty() {
        return None;
    }
    edits.extend(from_edits);
    let mut out = String::with_capacity(sql.len() + 64);
    let mut index = 0;
    while index < tokens.len() {
        if let Some((_, end, text)) = edits.iter().find(|(start, _, _)| *start == index) {
            out.push_str(text);
            index = *end;
            continue;
        }
        out.push_str(tokens[index].1);
        if close_after.contains(&index) {
            out.push(')');
        }
        if from_alias_after.contains(&index) {
            out.push_str(") AS pg_partition_ancestors(relid)");
        } else if from_alias_after.contains(&(usize::MAX - index)) {
            out.push(')');
        }
        index += 1;
    }
    Some(out)
}

/// `pg_catalog.unnest(...)` is the built-in `unnest`, which the parser only
/// recognizes as a table function (`FROM unnest(...)`) unqualified; psql's
/// describe queries qualify it.
pub(crate) fn rewrite_qualified_unnest_calls(sql: &str) -> Option<String> {
    if !sql.to_ascii_lowercase().contains("pg_catalog.unnest") {
        return None;
    }
    let tokens = tokens_with_source(&sqlparser::dialect::PostgreSqlDialect {}, sql)?;
    let significant = |index: usize| {
        tokens[index..]
            .iter()
            .position(|(token, _)| !matches!(token, Token::Whitespace(_)))
            .map(|offset| index + offset)
    };
    let mut out = String::with_capacity(sql.len());
    let mut changed = false;
    let mut index = 0;
    while index < tokens.len() {
        let (token, source) = &tokens[index];
        if let Token::Word(word) = token {
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("pg_catalog") {
                let dot = index + 1;
                if matches!(tokens.get(dot), Some((Token::Period, _))) {
                    if let Some((Token::Word(name), _)) = tokens.get(dot + 1) {
                        let call = significant(dot + 2);
                        if name.quote_style.is_none()
                            && name.value.eq_ignore_ascii_case("unnest")
                            && call.is_some_and(|call| matches!(tokens[call].0, Token::LParen))
                        {
                            index = dot + 1;
                            changed = true;
                            continue;
                        }
                    }
                }
            }
        }
        out.push_str(source);
        index += 1;
    }
    changed.then_some(out)
}

fn rewrite_named_type_literals(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("name") && !lower.contains("pg_lsn") {
        return None;
    }
    let tokens = tokens_with_source(&PostgreSqlDialect {}, sql)?;
    let mut offset = 0;
    let mut code = Vec::new();
    for (token, source) in &tokens {
        if !matches!(token, Token::Whitespace(_)) {
            code.push((token, offset, offset + source.len()));
        }
        offset += source.len();
    }
    let is_word = |token: &Token, spelling: &str| {
        matches!(token, Token::Word(word) if if word.quote_style.is_none() {
            word.value.eq_ignore_ascii_case(spelling)
        } else {
            word.value == spelling
        })
    };
    let mut edits = Vec::new();
    let mut ddl = false;
    let mut statement_start = true;
    let mut option_depth = 0usize;
    for (index, pair) in code.windows(2).enumerate() {
        let [(token, start, end), (literal, _, literal_end)] = pair else {
            unreachable!("window contains the type and input token");
        };
        if statement_start {
            ddl = is_word(token, "create") || is_word(token, "alter");
        }
        statement_start = matches!(token, Token::SemiColon);
        if ddl
            && matches!(token, Token::LParen)
            && (option_depth > 0
                || index
                    .checked_sub(1)
                    .is_some_and(|i| is_word(code[i].0, "options")))
        {
            option_depth += 1;
        } else if matches!(token, Token::RParen) {
            option_depth = option_depth.saturating_sub(1);
        }
        if option_depth > 0
            || !(is_word(token, "name") || is_word(token, "pg_lsn"))
            || !matches!(
                literal,
                Token::SingleQuotedString(_)
                    | Token::EscapedStringLiteral(_)
                    | Token::DollarQuotedString(_)
            )
        {
            continue;
        }
        let start = if matches!(index.checked_sub(1).map(|i| code[i].0), Some(Token::Period)) {
            let Some((schema, start, _)) = index.checked_sub(2).map(|i| code[i]) else {
                continue;
            };
            if !is_word(schema, "pg_catalog") {
                continue;
            }
            start
        } else {
            *start
        };
        edits.push((
            start,
            *literal_end,
            format!(
                "CAST({} AS {})",
                &sql[*end..*literal_end],
                &sql[start..*end]
            ),
        ));
    }
    if edits.is_empty() {
        return None;
    }
    let mut result = sql.to_string();
    for (start, end, replacement) in edits.into_iter().rev() {
        result.replace_range(start..end, &replacement);
    }
    Some(result)
}

pub(crate) fn rewrite_postgres_parse_compat(sql: &str) -> Option<String> {
    let mut rewritten = sql.to_string();
    let mut changed = false;

    if let Some(next) = super::identity_sequence_options::rewrite(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::engine::flashback::rewrite_flashback_clauses(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_partition_ancestor_projection(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_qualified_unnest_calls(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_select_top_identifiers(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_named_type_literals(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::json_table::rewrite_json_table_clauses(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::sql_json::rewrite_sql_json_functions(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::routine_calls::rewrite_assignment_named_args(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::parse_compat_postfix::rewrite(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::parse_compat_postfix::rewrite_insert_element_targets(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::similar_to::rewrite_order_by_using(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::similar_to::rewrite_substring_similar(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_qualified_substring_calls(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_variadic_builtin_calls(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_update_array_assignments(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_pgvector_l1_operator(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = replace_ascii_case_insensitive(&rewritten, "TRIM(BOTH FROM ", "TRIM(") {
        rewritten = next;
        changed = true;
    }

    // DROP INDEX CONCURRENTLY deliberately keeps mapping to plain DROP
    // INDEX (sqlparser cannot represent it): unlike the build, a drop is a
    // metadata unpublish plus bounded namespace purge, so the semantic gap
    // to PostgreSQL's lock-polite variant is a brief exclusive acquisition,
    // not an hours-long stall. CREATE INDEX CONCURRENTLY, by contrast, is
    // REJECTED — see execute_create_index and
    // docs/create-index-concurrently-design.md.
    if let Some(next) =
        replace_ascii_case_insensitive(&rewritten, "DROP INDEX CONCURRENTLY ", "DROP INDEX ")
    {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = replace_ascii_case_insensitive(&rewritten, "DROP DOMAIN ", "DROP TYPE ") {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_create_domain_optional_as(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_domain_not_null_constraint(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_create_domain_null_constraint(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_postgres_operator_qualifiers(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_on_conflict_index_predicates(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_on_conflict_after_bare_projection(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_record_star_items(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_qualified_current_user_calls(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_postgres_interval_precision_literals(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_postgres_record_function_aliases(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_sql_xml_expressions(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_json_array_query_constructors(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_is_json_predicates(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_json_unique_keys_clauses(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_json_format_clauses(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_weak_lock_strengths(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::quantified_operators::rewrite_operator_quantifiers(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = rewrite_locking_clause_before_limit(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::session::merge::rewrite_merge_parse_compat(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::window_exclusion::rewrite_window_exclusion(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::rows_from::rewrite_rows_from(&rewritten) {
        rewritten = next;
        changed = true;
    }

    if let Some(next) = crate::only_rewrite::rewrite_only_relations(&rewritten) {
        rewritten = next;
        changed = true;
    }

    changed.then_some(rewritten)
}

fn rewrite_qualified_substring_calls(sql: &str) -> Option<String> {
    if !sql
        .as_bytes()
        .windows(6)
        .any(|text| text.eq_ignore_ascii_case(b"substr"))
    {
        return None;
    }
    let tokens = tokens_with_source(&PostgreSqlDialect {}, sql)?;
    let mut offset = 0;
    let mut code = Vec::new();
    for (token, original) in tokens {
        let end = offset + original.len();
        if !matches!(token, Token::Whitespace(_)) {
            code.push((token, offset, end));
        }
        offset = end;
    }
    let mut rewritten = String::new();
    let mut copied = 0;
    for window in code.windows(3) {
        if let [(Token::Period, _, _), (Token::Word(word), start, end), (Token::LParen, _, _)] =
            window
        {
            let name = word.value.to_ascii_lowercase();
            if word.quote_style.is_none() && matches!(name.as_str(), "substring" | "substr") {
                // The generic parser treats a bare SUBSTRING token as the
                // special SQL syntax even after a schema qualifier.
                rewritten.push_str(&sql[copied..*start]);
                rewritten.push('"');
                rewritten.push_str(&name);
                rewritten.push('"');
                copied = *end;
            }
        }
    }
    if copied == 0 {
        return None;
    }
    rewritten.push_str(&sql[copied..]);
    Some(rewritten)
}

/// `FOR KEY SHARE` and `FOR NO KEY UPDATE` are PostgreSQL's weaker row lock
/// strengths, which sqlparser does not parse. They become the next stronger
/// strength it does parse (`FOR SHARE`, `FOR UPDATE`): every conflict the
/// weak lock would take is still taken, so correctness is unchanged and only
/// concurrency can be lower. Works on tokens, so strings, dollar-quoted bodies
/// and comments are left alone.
/// SQL text for one token that reads back as the same token. A token's
/// `Display` writes a decoded string or quoted identifier without escaping
/// its quotes, so it is not valid SQL for values that contain them.
pub(crate) fn token_sql(token: &Token) -> String {
    fn quoted(prefix: &str, value: &str) -> String {
        format!("{prefix}'{}'", value.replace('\'', "''"))
    }
    match token {
        Token::SingleQuotedString(value) | Token::UnicodeStringLiteral(value) => quoted("", value),
        Token::NationalStringLiteral(value) => quoted("N", value),
        Token::EscapedStringLiteral(value) => {
            format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        Token::DoubleQuotedString(value) => format!("\"{}\"", value.replace('"', "\"\"")),
        Token::Word(word) => match word.quote_style {
            Some(quote) => {
                let end = if quote == '[' { ']' } else { quote };
                format!(
                    "{quote}{}{end}",
                    word.value.replace(end, &format!("{end}{end}"))
                )
            }
            None => word.value.clone(),
        },
        token => token.to_string(),
    }
}

/// Each token of `input` with the exact source text it was read from, for
/// rewrites that re-emit tokens: a token's `Display` is not its source text
/// (a doubled quote or an escape-string escape is lost).
pub(crate) fn tokens_with_source<'a>(
    dialect: &dyn sqlparser::dialect::Dialect,
    input: &'a str,
) -> Option<Vec<(Token, &'a str)>> {
    let spanned = Tokenizer::new(dialect, input)
        .tokenize_with_location()
        .ok()?;
    let mut line_starts = vec![0usize];
    line_starts.extend(input.match_indices('\n').map(|(index, _)| index + 1));
    let offset = |location: sqlparser::tokenizer::Location| -> Option<usize> {
        let start = *line_starts.get(usize::try_from(location.line).ok()?.checked_sub(1)?)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        input[start..]
            .char_indices()
            .map(|(index, _)| start + index)
            .chain(std::iter::once(input.len()))
            .nth(column)
    };
    let starts = spanned
        .iter()
        .map(|token| offset(token.span.start))
        .collect::<Option<Vec<_>>>()?;
    if starts.first().is_some_and(|start| *start != 0) {
        return None;
    }
    let mut tokens = Vec::with_capacity(spanned.len());
    for (index, token) in spanned.into_iter().enumerate() {
        let start = starts[index];
        let end = starts.get(index + 1).copied().unwrap_or(input.len());
        if end <= start {
            return None;
        }
        tokens.push((token.token, &input[start..end]));
    }
    Some(tokens)
}

pub(crate) fn rewrite_weak_lock_strengths(input: &str) -> Option<String> {
    let lower = input.to_ascii_lowercase();
    if !lower.contains("key") || !lower.contains("for") {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let tokens: Vec<Token> = source.iter().map(|(token, _)| token.clone()).collect();
    let word = |token: &Token| -> Option<String> {
        match token {
            Token::Word(word) if word.quote_style.is_none() => {
                Some(word.value.to_ascii_uppercase())
            }
            _ => None,
        }
    };
    let next_word = |from: usize| -> Option<(usize, String)> {
        let mut index = from;
        while let Some(token) = tokens.get(index) {
            if matches!(token, Token::Whitespace(_)) {
                index += 1;
                continue;
            }
            return word(token).map(|value| (index, value));
        }
        None
    };
    let mut out = String::with_capacity(input.len());
    let mut changed = false;
    let mut index = 0;
    while index < tokens.len() {
        if word(&tokens[index]).as_deref() == Some("FOR") {
            if let Some((key_index, first)) = next_word(index + 1) {
                let replacement = match first.as_str() {
                    "KEY" => next_word(key_index + 1)
                        .filter(|(_, value)| value == "SHARE")
                        .map(|(end, _)| ("FOR SHARE", end)),
                    "NO" => next_word(key_index + 1)
                        .filter(|(_, value)| value == "KEY")
                        .and_then(|(key, _)| next_word(key + 1))
                        .filter(|(_, value)| value == "UPDATE")
                        .map(|(end, _)| ("FOR UPDATE", end)),
                    _ => None,
                };
                if let Some((text, end)) = replacement {
                    out.push_str(text);
                    index = end + 1;
                    changed = true;
                    continue;
                }
            }
        }
        out.push_str(source[index].1);
        index += 1;
    }
    changed.then_some(out)
}

/// PostgreSQL accepts a locking clause (`FOR UPDATE [OF ...] [NOWAIT | SKIP
/// LOCKED]`, `FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`) either before
/// or after `LIMIT` / `OFFSET` / `FETCH`; sqlparser only parses it after.
/// Queue-claim SQL commonly writes `... FOR UPDATE SKIP LOCKED LIMIT 1`, so
/// move the limit clause in front of the locking clause. The rewrite works on
/// tokens (quotes, dollar-quoted bodies and comments stay intact) and only
/// runs when that exact ordering is present.
pub(crate) fn rewrite_locking_clause_before_limit(input: &str) -> Option<String> {
    let lower = input.to_ascii_lowercase();
    if !lower.contains("for")
        || !(lower.contains("limit") || lower.contains("offset") || lower.contains("fetch"))
    {
        return None;
    }
    let source = tokens_with_source(&PostgreSqlDialect {}, input)?;
    let tokens: Vec<Token> = source.iter().map(|(token, _)| token.clone()).collect();
    let word = |token: &Token| -> Option<String> {
        match token {
            Token::Word(word) if word.quote_style.is_none() => {
                Some(word.value.to_ascii_uppercase())
            }
            _ => None,
        }
    };
    let next_significant = |from: usize| -> Option<usize> {
        (from..tokens.len()).find(|index| !matches!(tokens[*index], Token::Whitespace(_)))
    };
    let mut output: Vec<&str> = Vec::with_capacity(tokens.len() + 2);
    let mut changed = false;
    let mut index = 0;
    while index < tokens.len() {
        let is_lock_start = word(&tokens[index]).as_deref() == Some("FOR")
            && next_significant(index + 1)
                .and_then(|next| word(&tokens[next]))
                .is_some_and(|next| matches!(next.as_str(), "UPDATE" | "SHARE" | "NO" | "KEY"));
        if !is_lock_start {
            output.push(source[index].1);
            index += 1;
            continue;
        }
        // The locking clause (possibly several) runs until a LIMIT / OFFSET /
        // FETCH keyword or the end of the enclosing query at this depth.
        let lock_start = index;
        let mut depth = 0i32;
        let mut cursor = index;
        let mut limit_start = None;
        while cursor < tokens.len() {
            match &tokens[cursor] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                Token::SemiColon if depth == 0 => break,
                token if depth == 0 => {
                    if word(token).is_some_and(|keyword| {
                        matches!(keyword.as_str(), "LIMIT" | "OFFSET" | "FETCH")
                    }) {
                        limit_start = Some(cursor);
                        break;
                    }
                }
                _ => {}
            }
            cursor += 1;
        }
        let Some(limit_start) = limit_start else {
            output.extend(source[lock_start..cursor].iter().map(|(_, text)| *text));
            index = cursor;
            continue;
        };
        // The limit clause ends where the locking clause would have: a closing
        // parenthesis of the enclosing query, a semicolon, or the end of input.
        let mut depth = 0i32;
        let mut limit_end = limit_start;
        while limit_end < tokens.len() {
            match &tokens[limit_end] {
                Token::LParen => depth += 1,
                Token::RParen => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                Token::SemiColon if depth == 0 => break,
                _ => {}
            }
            limit_end += 1;
        }
        let is_space = |index: usize| matches!(tokens[index], Token::Whitespace(_));
        let mut lock_end = limit_start;
        while lock_end > lock_start && is_space(lock_end - 1) {
            lock_end -= 1;
        }
        let mut limit_trim = limit_end;
        while limit_trim > limit_start && is_space(limit_trim - 1) {
            limit_trim -= 1;
        }
        let text = |range: std::ops::Range<usize>| source[range].iter().map(|(_, text)| *text);
        output.extend(text(limit_start..limit_trim));
        output.push(" ");
        output.extend(text(lock_start..lock_end));
        output.extend(text(limit_trim..limit_end));
        changed = true;
        index = limit_end;
    }
    if !changed {
        return None;
    }
    Some(output.concat())
}

pub(crate) fn rewrite_variadic_builtin_calls(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut edits = Vec::<(usize, usize, String)>::new();
    let mut parens = Vec::new();
    let mut index = 0usize;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut dollar_quote = None::<String>;
    while index < bytes.len() {
        if let Some(delimiter) = dollar_quote.as_deref() {
            if bytes[index..].starts_with(delimiter.as_bytes()) {
                index += delimiter.len();
                dollar_quote = None;
            } else {
                index += 1;
            }
            continue;
        }
        if line_comment {
            if bytes[index] == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if single_quoted {
            if bytes[index] == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                    continue;
                }
                single_quoted = false;
            }
            index += 1;
            continue;
        }
        if double_quoted {
            if bytes[index] == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 2;
                    continue;
                }
                double_quoted = false;
            }
            index += 1;
            continue;
        }
        match bytes[index] {
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                line_comment = true;
                index += 2;
                continue;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                block_comment = true;
                index += 2;
                continue;
            }
            b'\'' => single_quoted = true,
            b'"' => double_quoted = true,
            b'$' => {
                if let Some(delimiter) = dollar_quote_delimiter(&input[index..]) {
                    index += delimiter.len();
                    dollar_quote = Some(delimiter);
                    continue;
                }
            }
            b'(' => parens.push(index),
            b')' => {
                parens.pop();
            }
            _ if keyword_matches_at(input, "VARIADIC", index) => {
                let Some(&open) = parens.last() else {
                    index += "VARIADIC".len();
                    continue;
                };
                let mut name_end = open;
                while name_end > 0 && bytes[name_end - 1].is_ascii_whitespace() {
                    name_end -= 1;
                }
                let mut name_start = name_end;
                while name_start > 0
                    && (bytes[name_start - 1].is_ascii_alphanumeric()
                        || matches!(bytes[name_start - 1], b'_' | b'$' | b'.' | b'"'))
                {
                    name_start -= 1;
                }
                let raw_name = input[name_start..name_end].trim();
                let bare_name = raw_name
                    .rsplit('.')
                    .next()
                    .unwrap_or(raw_name)
                    .trim_matches('"')
                    .to_ascii_lowercase();
                if !matches!(bare_name.as_str(), "concat" | "concat_ws" | "format") {
                    // A user function's `VARIADIC array` argument reaches the
                    // parser as a marker-named argument that call binding
                    // (routine_calls) passes as the variadic parameter. A
                    // routine signature (CREATE/DROP/ALTER FUNCTION f(VARIADIC
                    // int[]), ...) keeps its declaration as written.
                    let before = input[..name_start].trim_end();
                    let previous_word = before
                        .rsplit(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                        .next()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let statement_start = before.rfind(';').map_or(0, |at| at + 1);
                    let first_word = input[statement_start..]
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    let signature = matches!(
                        previous_word.as_str(),
                        "function" | "procedure" | "routine" | "aggregate"
                    ) || matches!(
                        first_word.as_str(),
                        "drop" | "alter" | "comment" | "grant" | "revoke" | "security"
                    );
                    if !signature && !raw_name.is_empty() {
                        let mut variadic_end = index + "VARIADIC".len();
                        while bytes.get(variadic_end).is_some_and(u8::is_ascii_whitespace) {
                            variadic_end += 1;
                        }
                        edits.push((
                            index,
                            variadic_end,
                            format!("\"{}\" => ", crate::routine_calls::VARIADIC_ARGUMENT_MARKER),
                        ));
                        index = variadic_end;
                        continue;
                    }
                    index += "VARIADIC".len();
                    continue;
                }
                let mut variadic_end = index + "VARIADIC".len();
                while bytes.get(variadic_end).is_some_and(u8::is_ascii_whitespace) {
                    variadic_end += 1;
                }
                edits.push((name_start, name_end, "bicdb_variadic_call".to_string()));
                edits.push((
                    open + 1,
                    open + 1,
                    format!("'{}', ", raw_name.replace('\'', "''")),
                ));
                edits.push((index, variadic_end, String::new()));
                index = variadic_end;
                continue;
            }
            _ => {}
        }
        index += 1;
    }
    if edits.is_empty() {
        return None;
    }
    edits.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
    let mut output = input.to_string();
    for (start, end, replacement) in edits {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

pub(crate) fn skip_sql_space(input: &str, mut index: usize) -> usize {
    while input
        .as_bytes()
        .get(index)
        .is_some_and(u8::is_ascii_whitespace)
    {
        index += 1;
    }
    index
}

pub(crate) fn sql_identifier_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) == Some(&b'"') {
        let mut index = start + 1;
        while index < bytes.len() {
            if bytes[index] == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 2;
                } else {
                    return Some(index + 1);
                }
            } else {
                index += 1;
            }
        }
        return None;
    }
    if !bytes
        .get(start)
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    {
        return None;
    }
    let mut index = start + 1;
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
    {
        index += 1;
    }
    Some(index)
}

pub(crate) fn matching_sql_bracket(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'[') {
        return None;
    }
    let mut index = start + 1;
    let mut depth = 1usize;
    let mut quote = None;
    while index < bytes.len() {
        if let Some(active) = quote {
            if bytes[index] == active {
                if bytes.get(index + 1) == Some(&active) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match bytes[index] {
            b'\'' | b'"' => quote = Some(bytes[index]),
            b'[' => depth += 1,
            b']' => {
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

pub(crate) fn top_level_slice_colon(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut parens = 0usize;
    let mut brackets = 0usize;
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        if let Some(active) = quote {
            if bytes[index] == active {
                if bytes.get(index + 1) == Some(&active) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        match bytes[index] {
            b'\'' | b'"' => quote = Some(bytes[index]),
            b'(' => parens += 1,
            b')' => parens = parens.saturating_sub(1),
            b'[' => brackets += 1,
            b']' => brackets = brackets.saturating_sub(1),
            b':' if parens == 0 && brackets == 0 => return Some(index),
            _ => {}
        }
        index += 1;
    }
    None
}

pub(crate) fn sql_assignment_rhs_end(input: &str, start: usize) -> usize {
    let bytes = input.as_bytes();
    let mut index = start;
    let mut parens = 0usize;
    let mut brackets = 0usize;
    let mut quote = None;
    while index < bytes.len() {
        if let Some(active) = quote {
            if bytes[index] == active {
                if bytes.get(index + 1) == Some(&active) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if bytes[index] == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        match bytes[index] {
            b'\'' | b'"' => {
                quote = Some(bytes[index]);
                index += 1;
            }
            b'(' => {
                parens += 1;
                index += 1;
            }
            b')' if parens > 0 => {
                parens -= 1;
                index += 1;
            }
            b'[' => {
                brackets += 1;
                index += 1;
            }
            b']' if brackets > 0 => {
                brackets -= 1;
                index += 1;
            }
            b',' | b';' if parens == 0 && brackets == 0 => return index,
            byte if parens == 0
                && brackets == 0
                && (byte.is_ascii_alphabetic() || byte == b'_') =>
            {
                let end = sql_identifier_end(input, index).unwrap_or(index + 1);
                let word = &input[index..end];
                if matches!(
                    word.to_ascii_lowercase().as_str(),
                    "where" | "from" | "returning"
                ) {
                    return index;
                }
                index = end;
            }
            _ => index += 1,
        }
    }
    input.len()
}

pub(crate) fn rewrite_update_array_assignments(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let update = tokens
        .iter()
        .find(|token| token.depth == 0 && token.word.as_deref() == Some("update"))?;
    if tokens
        .iter()
        .filter(|token| token.depth == 0 && token.word.is_some())
        .next()
        .is_none_or(|token| token.start != update.start)
    {
        return None;
    }
    let set = tokens.iter().find(|token| {
        token.depth == 0 && token.start > update.end && token.word.as_deref() == Some("set")
    })?;
    let mut cursor = set.end;
    let mut replacements = Vec::new();
    loop {
        cursor = skip_sql_space(input, cursor);
        if cursor >= input.len() {
            break;
        }
        let Some(target_end) = sql_identifier_end(input, cursor) else {
            break;
        };
        let target = input[cursor..target_end].to_string();
        let mut position = skip_sql_space(input, target_end);
        let mut lowers = Vec::new();
        let mut uppers = Vec::new();
        let mut slices = Vec::new();
        while input.as_bytes().get(position) == Some(&b'[') {
            let close = matching_sql_bracket(input, position)?;
            let subscript = input[position + 1..close].trim();
            if let Some(colon) = top_level_slice_colon(subscript) {
                let lower = subscript[..colon].trim();
                let upper = subscript[colon + 1..].trim();
                lowers.push(if lower.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("({lower})")
                });
                uppers.push(if upper.is_empty() {
                    "NULL".to_string()
                } else {
                    format!("({upper})")
                });
                slices.push("true");
            } else {
                if subscript.is_empty() {
                    return None;
                }
                lowers.push(format!("({subscript})"));
                uppers.push(format!("({subscript})"));
                slices.push("false");
            }
            position = skip_sql_space(input, close + 1);
        }
        if lowers.is_empty() {
            let equals = input[position..].find('=')? + position;
            let end = sql_assignment_rhs_end(input, equals + 1);
            cursor = if input.as_bytes().get(end) == Some(&b',') {
                end + 1
            } else {
                end
            };
            if cursor == end {
                break;
            }
            continue;
        }
        if input.as_bytes().get(position) != Some(&b'=') {
            return None;
        }
        let rhs_start = position + 1;
        let rhs_end = sql_assignment_rhs_end(input, rhs_start);
        let rhs = input[rhs_start..rhs_end].trim();
        if rhs.is_empty() {
            return None;
        }
        replacements.push((
            cursor,
            rhs_end,
            format!(
                "{target} = bicdb_array_assign({target}, ARRAY[{}], ARRAY[{}], ARRAY[{}], ({rhs}))",
                lowers.join(", "),
                uppers.join(", "),
                slices.join(", ")
            ),
        ));
        cursor = if input.as_bytes().get(rhs_end) == Some(&b',') {
            rhs_end + 1
        } else {
            rhs_end
        };
        if cursor == rhs_end {
            break;
        }
    }
    if replacements.is_empty() {
        return None;
    }
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
    }
    Some(output)
}

pub(crate) fn rewrite_create_domain_optional_as(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    if tokens.len() < 4
        || tokens[0].word.as_deref() != Some("create")
        || tokens[1].word.as_deref() != Some("domain")
    {
        return None;
    }
    let name_end_index = if tokens
        .get(3)
        .is_some_and(|token| token.symbol == Some(b'.'))
    {
        4
    } else {
        2
    };
    let next = tokens.get(name_end_index + 1)?;
    if next.word.as_deref() == Some("as") {
        return None;
    }
    let mut output = input.to_string();
    output.insert_str(tokens[name_end_index].end, " AS");
    Some(output)
}

pub(crate) fn rewrite_create_domain_null_constraint(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let top_level_words = tokens
        .iter()
        .filter(|token| token.depth == 0)
        .filter_map(|token| token.word.as_deref())
        .take(2)
        .collect::<Vec<_>>();
    if top_level_words != ["create", "domain"] {
        return None;
    }
    for (index, token) in tokens.iter().enumerate() {
        if token.depth != 0 || token.word.as_deref() != Some("null") {
            continue;
        }
        if tokens.get(index.wrapping_sub(1)).is_some_and(|previous| {
            matches!(previous.word.as_deref(), Some("default" | "not" | "is"))
        }) {
            continue;
        }
        let start = if index >= 2
            && tokens[index - 2].depth == 0
            && tokens[index - 2].word.as_deref() == Some("constraint")
        {
            tokens[index - 2].start
        } else {
            token.start
        };
        let mut output = input.to_string();
        output.replace_range(start..token.end, "");
        return Some(output);
    }
    None
}

pub(crate) fn rewrite_domain_not_null_constraint(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let top_level_words = tokens
        .iter()
        .filter(|token| token.depth == 0)
        .filter_map(|token| token.word.as_deref())
        .take(2)
        .collect::<Vec<_>>();
    if top_level_words != ["create", "domain"] && top_level_words != ["alter", "domain"] {
        return None;
    }
    for (index, token) in tokens.iter().enumerate() {
        if token.depth != 0 || token.word.as_deref() != Some("not") {
            continue;
        }
        let Some(null) = tokens
            .get(index + 1)
            .filter(|token| token.depth == 0 && token.word.as_deref() == Some("null"))
        else {
            continue;
        };
        if tokens
            .get(index.wrapping_sub(1))
            .is_some_and(|previous| matches!(previous.word.as_deref(), Some("set" | "drop")))
        {
            continue;
        }
        let named = index >= 2
            && tokens[index - 2].depth == 0
            && tokens[index - 2].word.as_deref() == Some("constraint");
        let start = if named {
            tokens[index - 2].start
        } else {
            token.start
        };
        let marker = if named {
            let raw_name = input[tokens[index - 1].start..tokens[index - 1].end].trim();
            let name = raw_name
                .strip_prefix('"')
                .and_then(|name| name.strip_suffix('"'))
                .map(|name| name.replace("\"\"", "\""))
                .unwrap_or_else(|| raw_name.to_ascii_lowercase());
            format!("{DOMAIN_NOT_NULL_MARKER}::{name}")
        } else {
            DOMAIN_NOT_NULL_MARKER.to_string()
        };
        let marker = marker.replace('"', "\"\"");
        let mut output = input.to_string();
        output.replace_range(
            start..null.end,
            &format!("CONSTRAINT \"{marker}\" CHECK (VALUE IS NOT NULL)"),
        );
        return Some(output);
    }
    None
}

#[derive(Clone, Debug)]
pub(crate) struct JsonPredicateToken {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) depth: usize,
    pub(crate) word: Option<String>,
    pub(crate) symbol: Option<u8>,
}

fn xml_passing_document<'a>(
    input: &'a str,
    tokens: &[JsonPredicateToken],
    passing: usize,
    end: usize,
) -> &'a str {
    let depth = tokens[passing].depth;
    let by_mode = |start: usize| {
        tokens.get(start..start + 2).is_some_and(|pair| {
            pair.iter().all(|token| token.depth == depth)
                && pair[0].word.as_deref() == Some("by")
                && matches!(pair[1].word.as_deref(), Some("ref" | "value"))
        })
    };
    let (prefix, suffix) = (passing + 1, end.saturating_sub(2));
    let start_byte = if prefix + 2 < end && by_mode(prefix) {
        tokens[prefix + 1].end
    } else {
        tokens[passing].end
    };
    let end_byte = if suffix > prefix && tokens[suffix].start >= start_byte && by_mode(suffix) {
        tokens[suffix].start
    } else {
        tokens[end].start
    };
    input[start_byte..end_byte].trim()
}

fn rewrite_sql_xml_calls(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let mut replacements = Vec::new();
    for (index, function) in tokens.iter().enumerate() {
        let Some(name) = function.word.as_deref() else {
            continue;
        };
        if !matches!(name, "xmlparse" | "xmlserialize" | "xmlexists") {
            continue;
        }
        let Some(open) = tokens
            .get(index + 1)
            .filter(|token| token.depth == function.depth && token.symbol == Some(b'('))
        else {
            continue;
        };
        let Some(close_index) = tokens[index + 2..]
            .iter()
            .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
            .map(|offset| index + 2 + offset)
        else {
            continue;
        };
        let inner_depth = open.depth + 1;
        let replacement = match name {
            "xmlparse" => {
                let Some(mode) = tokens.get(index + 2).filter(|token| {
                    token.depth == inner_depth
                        && matches!(token.word.as_deref(), Some("document" | "content"))
                }) else {
                    continue;
                };
                let end = close_index
                    .checked_sub(2)
                    .filter(|flag| *flag > index + 2)
                    .and_then(|flag| {
                        let first = &tokens[flag];
                        let second = &tokens[flag + 1];
                        (first.depth == inner_depth
                            && second.depth == inner_depth
                            && matches!(first.word.as_deref(), Some("preserve" | "strip"))
                            && second.word.as_deref() == Some("whitespace"))
                        .then_some(first.start)
                    })
                    .unwrap_or(tokens[close_index].start);
                let expression = input[mode.end..end].trim();
                format!(
                    "bicdb_xmlparse_{}(({expression}))",
                    mode.word.as_deref().unwrap()
                )
            }
            "xmlserialize" => {
                let Some(mode) = tokens.get(index + 2).filter(|token| {
                    token.depth == inner_depth
                        && matches!(token.word.as_deref(), Some("document" | "content"))
                }) else {
                    continue;
                };
                let Some(as_index) = tokens[index + 3..close_index]
                    .iter()
                    .rposition(|token| {
                        token.depth == inner_depth && token.word.as_deref() == Some("as")
                    })
                    .map(|offset| index + 3 + offset)
                else {
                    continue;
                };
                let expression = input[mode.end..tokens[as_index].start].trim();
                let mut type_end = tokens[close_index].start;
                let mut indent = false;
                let indent_index = close_index - 1;
                if indent_index > as_index + 1
                    && tokens[indent_index].depth == inner_depth
                    && tokens[indent_index].word.as_deref() == Some("indent")
                {
                    let before = &tokens[indent_index - 1];
                    if before.depth == inner_depth && before.word.as_deref() == Some("no") {
                        type_end = before.start;
                    } else {
                        indent = true;
                        type_end = tokens[indent_index].start;
                    }
                }
                let data_type = input[tokens[as_index].end..type_end].trim();
                let target = data_type.replace('\'', "''");
                format!(
                    "CAST(bicdb_xmlserialize_{}(({expression}), {indent}, '{target}') AS {data_type})",
                    mode.word.as_deref().unwrap()
                )
            }
            "xmlexists" => {
                let Some(passing_index) = tokens[index + 2..close_index]
                    .iter()
                    .position(|token| {
                        token.depth == inner_depth && token.word.as_deref() == Some("passing")
                    })
                    .map(|offset| index + 2 + offset)
                else {
                    continue;
                };
                let path = input[open.end..tokens[passing_index].start].trim();
                let document = xml_passing_document(input, &tokens, passing_index, close_index);
                format!("bicdb_xmlexists(({path}), ({document}))")
            }
            _ => unreachable!(),
        };
        replacements.push((function.start, tokens[close_index].end, replacement));
    }
    if replacements.is_empty() {
        return None;
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    let mut output = input.to_string();
    let mut rightmost = input.len();
    for (start, end, replacement) in replacements.into_iter().rev() {
        if end <= rightmost {
            output.replace_range(start..end, &replacement);
            rightmost = start;
        }
    }
    Some(output)
}

pub(crate) fn rewrite_sql_xml_expressions(input: &str) -> Option<String> {
    let mut output = input.to_string();
    let mut changed = false;
    while let Some(next) = rewrite_sql_xml_calls(&output) {
        output = next;
        changed = true;
    }
    while let Some(next) = rewrite_xml_constructor_once(&output) {
        output = next;
        changed = true;
    }
    while let Some(next) = rewrite_xmltable_once(&output) {
        output = next;
        changed = true;
    }
    while let Some(next) = rewrite_xmlroot_once(&output) {
        output = next;
        changed = true;
    }
    changed.then_some(output)
}

pub(crate) fn rewrite_xmlroot_once(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let (index, function) = tokens.iter().enumerate().rev().find(|(index, token)| {
        token.word.as_deref() == Some("xmlroot")
            && tokens
                .get(index + 1)
                .is_some_and(|next| next.depth == token.depth && next.symbol == Some(b'('))
    })?;
    let open = tokens
        .get(index + 1)
        .filter(|token| token.symbol == Some(b'('))?;
    let close_index = tokens[index + 2..]
        .iter()
        .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
        .map(|offset| index + 2 + offset)?;
    let depth = open.depth + 1;
    let commas = tokens[index + 2..close_index]
        .iter()
        .filter(|token| token.depth == depth && token.symbol == Some(b','))
        .collect::<Vec<_>>();
    let (version_start, standalone_start) = match commas.as_slice() {
        [version] => (*version, None),
        [version, standalone] => (*version, Some(*standalone)),
        _ => return None,
    };
    let document = input[open.end..version_start.start].trim();
    let close = &tokens[close_index];
    let version_end = standalone_start.map_or(close.start, |token| token.start);
    let version_clause = input[version_start.end..version_end].trim();
    let version = strip_case_insensitive_keyword(version_clause, "version")?.trim();
    let version = if version.eq_ignore_ascii_case("no value") {
        "NULL"
    } else {
        version
    };
    let replacement = match standalone_start {
        Some(token) => {
            let clause = input[token.end..close.start].trim();
            let standalone = strip_case_insensitive_keyword(clause, "standalone")?.trim();
            let standalone = match standalone.to_ascii_lowercase().as_str() {
                "no value" => "NULL",
                "yes" => "'yes'",
                "no" => "'no'",
                _ => return None,
            };
            format!("bicdb_xmlroot(({document}), {version}, {standalone})")
        }
        None => format!("bicdb_xmlroot(({document}), {version})"),
    };
    let mut output = input.to_string();
    output.replace_range(function.start..tokens[close_index].end, &replacement);
    Some(output)
}

pub(crate) fn strip_case_insensitive_keyword<'a>(value: &'a str, keyword: &str) -> Option<&'a str> {
    let prefix = value.get(..keyword.len())?;
    prefix
        .eq_ignore_ascii_case(keyword)
        .then(|| &value[keyword.len()..])
}

pub(crate) fn rewrite_xmltable_once(input: &str) -> Option<String> {
    let tokens = json_predicate_tokens(input);
    let (index, function) = tokens.iter().enumerate().find(|(index, token)| {
        token.word.as_deref() == Some("xmltable")
            && tokens
                .get(index + 1)
                .is_some_and(|open| open.depth == token.depth && open.symbol == Some(b'('))
    })?;
    let open = tokens
        .get(index + 1)
        .filter(|token| token.symbol == Some(b'('))?;
    let close_index = tokens[index + 2..]
        .iter()
        .position(|token| token.depth == function.depth && token.symbol == Some(b')'))
        .map(|offset| index + 2 + offset)?;
    let inner_depth = open.depth + 1;
    let mut row_path_start = open.end;
    let mut namespace_expression = "'[]'::jsonb".to_string();
    if tokens
        .get(index + 2)
        .is_some_and(|token| token.word.as_deref() == Some("xmlnamespaces"))
    {
        let namespace_open_index = index + 3;
        let namespace_open = tokens
            .get(namespace_open_index)
            .filter(|token| token.symbol == Some(b'('))?;
        let namespace_close_index = tokens[namespace_open_index + 1..close_index]
            .iter()
            .position(|token| token.depth == namespace_open.depth && token.symbol == Some(b')'))
            .map(|offset| namespace_open_index + 1 + offset)?;
        let separator = tokens
            .get(namespace_close_index + 1)
            .filter(|token| token.depth == inner_depth && token.symbol == Some(b','))?;
        row_path_start = separator.end;
        let namespace_depth = namespace_open.depth + 1;
        let namespace_commas = tokens[namespace_open_index + 1..namespace_close_index]
            .iter()
            .filter(|token| token.depth == namespace_depth && token.symbol == Some(b','))
            .collect::<Vec<_>>();
        let mut starts = vec![namespace_open.end];
        starts.extend(namespace_commas.iter().map(|token| token.end));
        let mut ends = namespace_commas
            .iter()
            .map(|token| token.start)
            .collect::<Vec<_>>();
        ends.push(tokens[namespace_close_index].start);
        let mut namespace_pairs = Vec::new();
        for (start, end) in starts.into_iter().zip(ends) {
            let segment = input[start..end].trim();
            if segment.eq_ignore_ascii_case("no default") {
                continue;
            }
            if segment
                .get(..7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("default"))
            {
                let uri = segment[7..].trim();
                namespace_pairs.push(format!("jsonb_build_array('', ({uri})::text)"));
                continue;
            }
            let as_token = tokens.iter().find(|token| {
                token.start >= start
                    && token.end <= end
                    && token.depth == namespace_depth
                    && token.word.as_deref() == Some("as")
            })?;
            let uri = input[start..as_token.start].trim();
            let prefix = input[as_token.end..end].trim().trim_matches('"');
            if prefix.is_empty() {
                return None;
            }
            namespace_pairs.push(format!(
                "jsonb_build_array('{}', ({uri})::text)",
                prefix.replace('\'', "''")
            ));
        }
        namespace_expression = format!("jsonb_build_array({})", namespace_pairs.join(", "));
    }
    let passing_index = tokens[index + 2..close_index]
        .iter()
        .position(|token| token.depth == inner_depth && token.word.as_deref() == Some("passing"))
        .map(|offset| index + 2 + offset)?;
    let columns_index = tokens[passing_index + 1..close_index]
        .iter()
        .position(|token| token.depth == inner_depth && token.word.as_deref() == Some("columns"))
        .map(|offset| passing_index + 1 + offset)?;
    let row_path = input[row_path_start..tokens[passing_index].start].trim();
    let document = xml_passing_document(input, &tokens, passing_index, columns_index);
    let mut starts = vec![tokens[columns_index].end];
    starts.extend(
        tokens[columns_index + 1..close_index]
            .iter()
            .filter(|token| token.depth == inner_depth && token.symbol == Some(b','))
            .map(|token| token.end),
    );
    let mut ends = tokens[columns_index + 1..close_index]
        .iter()
        .filter(|token| token.depth == inner_depth && token.symbol == Some(b','))
        .map(|token| token.start)
        .collect::<Vec<_>>();
    ends.push(tokens[close_index].start);

    let mut specs = Vec::new();
    let mut extra_arguments = Vec::new();
    let mut aliases = Vec::new();
    for (start, end) in starts.into_iter().zip(ends) {
        let segment_tokens = tokens
            .iter()
            .filter(|token| token.start >= start && token.end <= end && token.depth == inner_depth)
            .collect::<Vec<_>>();
        let name_token = *segment_tokens.first()?;
        let authored_name = input[name_token.start..name_token.end].trim();
        let column_name = match authored_name
            .strip_prefix('"')
            .and_then(|name| name.strip_suffix('"'))
        {
            Some(name) => name.replace("\"\"", "\""),
            None => authored_name.to_ascii_lowercase(),
        };
        let column_sql = format!("\"{}\"", column_name.replace('"', "\"\""));
        let for_ordinality = segment_tokens.windows(2).any(|pair| {
            pair[0].word.as_deref() == Some("for") && pair[1].word.as_deref() == Some("ordinality")
        });
        if for_ordinality {
            specs.push(serde_json::json!({"name": column_name, "ordinality": true}));
            aliases.push(format!("{column_sql} int"));
            continue;
        }
        let boundary = segment_tokens
            .iter()
            .skip(1)
            .find(|token| matches!(token.word.as_deref(), Some("path" | "default" | "not")))
            .map(|token| token.start)
            .unwrap_or(end);
        let data_type = input[name_token.end..boundary].trim();
        if data_type.is_empty() {
            return None;
        }
        let path_token = segment_tokens
            .iter()
            .find(|token| token.word.as_deref() == Some("path"));
        let default_token = segment_tokens
            .iter()
            .find(|token| token.word.as_deref() == Some("default"));
        let not_token = segment_tokens
            .windows(2)
            .find(|pair| {
                pair[0].word.as_deref() == Some("not") && pair[1].word.as_deref() == Some("null")
            })
            .map(|pair| pair[0]);
        let option_end = |start| {
            [path_token.copied(), default_token.copied(), not_token]
                .into_iter()
                .flatten()
                .map(|token| token.start)
                .filter(|position| *position > start)
                .min()
                .unwrap_or(end)
        };
        let path_expression = path_token
            .map(|token| input[token.end..option_end(token.start)].trim().to_string())
            .unwrap_or_else(|| format!("'{}'", column_name.replace('\'', "''")));
        let path_arg = 4 + extra_arguments.len();
        extra_arguments.push(path_expression);
        let pg_type = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(data_type)
            .ok()
            .and_then(|mut parser| parser.parse_data_type().ok())
            .and_then(|ty| pg_type_from_data_type(&ty).ok())
            .map(|(kind, _)| kind);
        let mut spec = serde_json::json!({
            "name": column_name,
            "path_arg": path_arg,
            "xml": pg_type.as_deref() == Some("xml"),
            "numeric": pg_type.as_deref().is_some_and(|kind| {
                matches!(kind, "int2" | "int4" | "int8" | "numeric" | "float4" | "float8")
            }),
            "required": not_token.is_some()
        });
        if let Some(default_token) = default_token {
            let default_end = option_end(default_token.start);
            let expression = input[default_token.end..default_end].trim();
            let default_arg = 4 + extra_arguments.len();
            extra_arguments.push(expression.to_string());
            spec["default_arg"] = serde_json::json!(default_arg);
        }
        specs.push(spec);
        aliases.push(format!("{column_sql} {data_type}"));
    }
    let spec = serde_json::to_string(&specs).ok()?.replace('\'', "''");
    let extras = if extra_arguments.is_empty() {
        String::new()
    } else {
        format!(", {}", extra_arguments.join(", "))
    };
    let after = tokens
        .get(close_index + 1)
        .filter(|token| token.depth == function.depth);
    let alias_index = match after.and_then(|token| token.word.as_deref()) {
        Some("as") => tokens.get(close_index + 2).map(|_| close_index + 2),
        Some(
            "where" | "join" | "inner" | "left" | "right" | "full" | "cross" | "natural" | "on"
            | "using" | "group" | "order" | "limit" | "offset" | "fetch" | "for" | "window"
            | "having" | "union" | "intersect" | "except" | "qualify",
        ) => None,
        _ => after
            .filter(|token| token.word.is_some() || input[token.start..token.end].starts_with('"'))
            .map(|_| close_index + 1),
    };
    let alias_token = alias_index.map(|index| &tokens[index]);
    let (alias, end) = alias_token.map_or(("\"xmltable\"", tokens[close_index].end), |token| {
        (&input[token.start..token.end], token.end)
    });
    let recordset = format!(
        "jsonb_to_recordset(bicdb_xmltable_json(({document}), ({row_path}), '{spec}'::jsonb, {namespace_expression}{extras}))"
    );
    let columns = aliases.join(", ");
    let has_column_aliases = alias_index
        .and_then(|alias_index| tokens.get(alias_index + 1))
        .is_some_and(|token| token.depth == function.depth && token.symbol == Some(b'('));
    let replacement = if has_column_aliases {
        // Keep the COLUMNS clause's names and types for XML evaluation.
        // The outer alias list only renames the projected SQL columns.
        let explicit_lateral = index
            .checked_sub(1)
            .and_then(|previous| tokens.get(previous))
            .is_some_and(|token| {
                token.depth == function.depth && token.word.as_deref() == Some("lateral")
            });
        let lateral = if explicit_lateral { "" } else { "LATERAL " };
        format!("{lateral}(SELECT * FROM {recordset} AS \"xmltable\"({columns})) AS {alias}")
    } else {
        format!("{recordset} AS {alias}({columns})")
    };
    let mut output = input.to_string();
    output.replace_range(function.start..end, &replacement);
    Some(output)
}

/// A `SqlValue` as a literal expression, for inlining an already-computed
/// value into an expression tree. Values without a natural literal form
/// render through their text cell, PostgreSQL's unknown-typed coercion.
pub(crate) fn sql_value_to_literal_expr(value: &SqlValue) -> Expr {
    let literal = match value {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(value) => Value::Boolean(*value),
        SqlValue::Int(value) => Value::Number(value.to_string(), false),
        SqlValue::Float(value) => Value::Number(value.to_string(), false),
        other => Value::SingleQuotedString(other.to_cell()),
    };
    Expr::Value(literal.into())
}

/// A literal expression for an evaluated value.
pub(crate) fn sql_value_literal_expr(value: &SqlValue) -> Expr {
    let value = match value {
        SqlValue::Null => Value::Null,
        SqlValue::Bool(value) => Value::Boolean(*value),
        SqlValue::Int(value) => Value::Number(value.to_string(), false),
        SqlValue::Float(value) => Value::Number(value.to_string(), false),
        other => Value::SingleQuotedString(other.to_cell()),
    };
    Expr::Value(ValueWithSpan {
        value,
        span: sqlparser::tokenizer::Span::empty(),
    })
}
