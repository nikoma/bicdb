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
            .map_err(|_| {
                SqlError::invalid_text_representation(
                    "bit",
                    format!("invalid binary digit in bit string: \"{value}\""),
                )
            }),
        Value::HexStringLiteral(value) => PgBitString::from_hex_text(value)
            .map(|value| SqlValue::String(value.to_bit_text()))
            .map_err(|_| {
                SqlError::invalid_text_representation(
                    "bit",
                    format!("invalid hexadecimal digit in bit string: \"{value}\""),
                )
            }),
        // PostgreSQL escape strings (psql's \l projects array_to_string(.., E'\n')).
        Value::EscapedStringLiteral(value) => {
            text_value_without_nul(unescape_pg_string(value), "text")
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
    cast_value(literal_to_value(&value.value)?, &value.data_type)
}

pub(crate) fn typed_string_to_value_with_db(db: &BicDb, value: &TypedString) -> Result<SqlValue> {
    cast_value_with_db(db, literal_to_value(&value.value)?, &value.data_type)
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
            .unwrap_or_else(|| SqlValue::Json(value.clone().into())),
        JsonValue::String(value) => SqlValue::String(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => SqlValue::Json(value.clone()),
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
    match parts {
        [schema, table] if schema.eq_ignore_ascii_case("pg_catalog") => {
            Ok(format!("pg_catalog.{table}"))
        }
        [schema, table] if schema.eq_ignore_ascii_case("information_schema") => {
            Ok(format!("information_schema.{table}"))
        }
        // Durable aggregate projections are virtual relations, not stored
        // collections, so the qualifier must survive rather than being
        // encoded into a physical collection name.
        [schema, table] if schema.eq_ignore_ascii_case("bicdb_projection") => {
            Ok(format!("bicdb_projection.{table}"))
        }
        [schema, table] if schema.eq_ignore_ascii_case("public") => Ok(table.clone()),
        [schema, table] => {
            // Core collections have no schema dimension. Encode a
            // non-public SQL schema into a path-safe, collision-resistant
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
        [table] => Ok(table.clone()),
        _ => Ok(parts.join(".")),
    }
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

pub(crate) fn rewrite_postgres_parse_compat(sql: &str) -> Option<String> {
    let mut rewritten = sql.to_string();
    let mut changed = false;

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

    changed.then_some(rewritten)
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
            if input[index..].starts_with(delimiter) {
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
            b',' if parens == 0 && brackets == 0 => return index,
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

pub(crate) fn rewrite_sql_xml_expressions(input: &str) -> Option<String> {
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
                let expression = input[mode.end..tokens[close_index].start].trim();
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
                let data_type = input[tokens[as_index].end..tokens[close_index].start].trim();
                format!(
                    "CAST(bicdb_xmlserialize_{}(({expression})) AS {data_type})",
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
                let mut document_start = tokens[passing_index].end;
                let mut cursor = passing_index + 1;
                if tokens.get(cursor).is_some_and(|token| {
                    token.depth == inner_depth && token.word.as_deref() == Some("by")
                }) && tokens.get(cursor + 1).is_some_and(|token| {
                    token.depth == inner_depth && token.word.as_deref() == Some("value")
                }) {
                    document_start = tokens[cursor + 1].end;
                    cursor += 2;
                }
                let _ = cursor;
                let document = input[document_start..tokens[close_index].start].trim();
                format!("bicdb_xmlexists(({path}), ({document}))")
            }
            _ => unreachable!(),
        };
        replacements.push((function.start, tokens[close_index].end, replacement));
    }
    let mut changed = !replacements.is_empty();
    replacements.sort_by_key(|(start, _, _)| *start);
    let mut output = input.to_string();
    for (start, end, replacement) in replacements.into_iter().rev() {
        output.replace_range(start..end, &replacement);
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
    let (index, function) = tokens
        .iter()
        .enumerate()
        .find(|(_, token)| token.word.as_deref() == Some("xmlroot"))?;
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
    if commas.len() != 2 {
        return None;
    }
    let document = input[open.end..commas[0].start].trim();
    let version_clause = input[commas[0].end..commas[1].start].trim();
    let standalone_clause = input[commas[1].end..tokens[close_index].start].trim();
    let version = strip_case_insensitive_keyword(version_clause, "version")?.trim();
    let version = if version.eq_ignore_ascii_case("no value") {
        "NULL"
    } else {
        version
    };
    let standalone = strip_case_insensitive_keyword(standalone_clause, "standalone")?.trim();
    let standalone = if standalone.eq_ignore_ascii_case("no value") {
        "NULL"
    } else {
        match standalone.to_ascii_lowercase().as_str() {
            "yes" => "'yes'",
            "no" => "'no'",
            _ => return None,
        }
    };
    let replacement = format!("bicdb_xmlroot(({document}), {version}, {standalone})");
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
    let (index, function) = tokens
        .iter()
        .enumerate()
        .find(|(_, token)| token.word.as_deref() == Some("xmltable"))?;
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
    let mut document_start = tokens[passing_index].end;
    if let Some(by_index) = tokens[passing_index + 1..columns_index]
        .iter()
        .position(|token| token.depth == inner_depth && token.word.as_deref() == Some("by"))
        .map(|offset| passing_index + 1 + offset)
    {
        if tokens
            .get(by_index + 1)
            .is_some_and(|token| token.word.as_deref() == Some("value"))
        {
            document_start = tokens[by_index + 1].end;
        }
    }
    let document = input[document_start..tokens[columns_index].start].trim();
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
        let column_name = input[name_token.start..name_token.end]
            .trim()
            .trim_matches('"');
        let for_ordinality = segment_tokens
            .iter()
            .any(|token| token.word.as_deref() == Some("ordinality"));
        if for_ordinality {
            specs.push(serde_json::json!({"name": column_name, "ordinality": true}));
            aliases.push(format!("{column_name} int"));
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
            .iter()
            .find(|token| token.word.as_deref() == Some("not"));
        let path_end = default_token
            .map(|token| token.start)
            .or_else(|| not_token.map(|token| token.start))
            .unwrap_or(end);
        let path_expression = path_token
            .map(|token| input[token.end..path_end].trim().to_string())
            .unwrap_or_else(|| format!("'{column_name}'"));
        let path_arg = 4 + extra_arguments.len();
        extra_arguments.push(path_expression);
        let mut spec = serde_json::json!({
            "name": column_name,
            "path_arg": path_arg,
            "xml": data_type.trim().eq_ignore_ascii_case("xml")
        });
        if let Some(default_token) = default_token {
            let default_end = not_token.map(|token| token.start).unwrap_or(end);
            let expression = input[default_token.end..default_end].trim();
            let default_arg = 4 + extra_arguments.len();
            extra_arguments.push(expression.to_string());
            spec["default_arg"] = serde_json::json!(default_arg);
        }
        specs.push(spec);
        aliases.push(format!("{column_name} {data_type}"));
    }
    let spec = serde_json::to_string(&specs).ok()?.replace('\'', "''");
    let extras = if extra_arguments.is_empty() {
        String::new()
    } else {
        format!(", {}", extra_arguments.join(", "))
    };
    let replacement = format!(
        "jsonb_to_recordset(bicdb_xmltable_json(({document}), ({row_path}), '{spec}'::jsonb, {namespace_expression}{extras})) AS xmltable({})",
        aliases.join(", ")
    );
    let mut output = input.to_string();
    output.replace_range(function.start..tokens[close_index].end, &replacement);
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
