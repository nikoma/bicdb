//! Virtual table catalog: table/column/oid registries, pg type oid maps, trigger/routine raw-SQL parsing, virtual row predicate evaluation, projection and FieldRef helpers, generate_series/range/ANN ordering, and predicate truth evaluation.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use virtual_tables::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn index_field_is_expression(field: &IndexField) -> bool {
    matches!(field, IndexField::Lower(_) | IndexField::Trim(_))
}

pub(crate) fn user_collection_names(db: &BicDb) -> Vec<String> {
    let mut names = db
        .collections()
        .into_iter()
        .map(|collection| collection.name)
        .filter(|name| {
            name != SCHEMA_COLLECTION
                && name != DATABASE_COLLECTION
                && name != SEQUENCE_COLLECTION
                && name != VIEW_COLLECTION
                && name != ROUTINE_COLLECTION
                && name != TRIGGER_COLLECTION
                && name != NOTIFICATION_COLLECTION
                && name != ROLE_COLLECTION
                && name != ROLE_MEMBERSHIP_COLLECTION
                && name != PRIVILEGE_COLLECTION
                && name != DEFAULT_PRIVILEGE_COLLECTION
                && name != MIGRATION_VERSION_COLLECTION
                && name != MIGRATION_HISTORY_COLLECTION
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

pub(crate) fn catalog_table_names(db: &BicDb) -> Vec<String> {
    let mut names = user_collection_names(db);
    names.extend(
        list_views(db)
            .unwrap_or_default()
            .into_iter()
            .map(|view| view.name),
    );
    names.extend(graph_virtual_table_names().into_iter().map(str::to_string));
    names.sort();
    names
}

pub(crate) fn graph_virtual_table_names() -> Vec<&'static str> {
    vec![GRAPH_NODES_TABLE, GRAPH_EDGES_TABLE]
}

pub(crate) fn graph_virtual_table_columns(table: &str) -> Vec<ColumnSchema> {
    let columns = match table {
        GRAPH_NODES_TABLE => vec![
            ("projection", "text"),
            ("id", "text"),
            ("label", "text"),
            ("properties", "jsonb"),
        ],
        GRAPH_EDGES_TABLE => vec![
            ("projection", "text"),
            ("id", "text"),
            ("from", "text"),
            ("to", "text"),
            ("label", "text"),
            ("properties", "jsonb"),
            ("timestamp", "int8"),
        ],
        _ => Vec::new(),
    };
    columns
        .into_iter()
        .map(|(name, pg_type)| ColumnSchema {
            name: name.to_string(),
            pg_type: pg_type.to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        })
        .collect()
}

pub(crate) fn default_record_columns() -> Vec<ColumnSchema> {
    vec![
        ColumnSchema {
            name: "id".to_string(),
            pg_type: "text".to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: true,
            hidden: false,
            nullable: false,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        },
        ColumnSchema {
            name: "metadata".to_string(),
            pg_type: "jsonb".to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        },
        ColumnSchema {
            name: "timestamp".to_string(),
            pg_type: "int8".to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        },
        ColumnSchema {
            name: "payload".to_string(),
            pg_type: "bytea".to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        },
        ColumnSchema {
            name: "vector".to_string(),
            pg_type: "vector".to_string(),
            user_type: None,
            collation: None,
            type_modifier: None,
            array_ndims: 0,
            compression: None,
            primary_key: false,
            hidden: false,
            nullable: true,
            vector_dim: None,
            default_sequence: None,
            default_value: None,
            default_expr: None,
            generated_expr: None,
            identity: None,
        },
    ]
}

pub(crate) fn virtual_row<const N: usize>(
    entries: [(&str, SqlValue); N],
) -> BTreeMap<String, SqlValue> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

pub(crate) fn table_relation_oid(schema: &TableSchema) -> i64 {
    let row_type_oid = u32::try_from(schema.row_type_oid()).unwrap_or_default();
    i64::from(row_type_oid ^ 0x4000_0000)
}

pub(crate) fn named_relation_oid(name: &str) -> i64 {
    stable_name_hash_wide(1_500_000_000, name, 200_000_000)
}

pub(crate) fn pg_type_oid(pg_type: &str) -> i64 {
    pg_type_oid_by_name(pg_type).unwrap_or(25) as i64
}

pub(crate) fn pg_type_is_array(pg_type: &str) -> bool {
    builtin_array_element_spec(pg_type).is_some() || pg_array_type_element(pg_type).is_some()
}

fn builtin_array_element_spec(pg_type: &str) -> Option<&'static PgTypeSpec> {
    let mut element_type = pg_type;
    let mut dimensions = 0;
    while let Some(scalar_type) = element_type.strip_suffix("[]") {
        element_type = scalar_type;
        dimensions += 1;
    }
    (dimensions > 0)
        .then(|| pg_type_spec(element_type))
        .flatten()
        .filter(|spec| spec.array_oid.is_some())
}

pub(crate) fn pg_type_name_from_oid(oid: i64) -> Option<&'static str> {
    i32::try_from(oid).ok().and_then(pg_type_name_by_oid)
}

pub(crate) fn pg_type_len(pg_type: &str) -> i64 {
    pg_type_spec(pg_type).map_or(-1, |spec| spec.len as i64)
}

pub(crate) fn pg_type_by_value(pg_type: &str) -> bool {
    pg_type_spec(pg_type).is_some_and(|spec| spec.by_value)
}

pub(crate) fn pg_type_align(pg_type: &str) -> char {
    builtin_array_element_spec(pg_type).map_or_else(
        || pg_type_spec(pg_type).map_or('i', |spec| spec.align),
        |spec| spec.array_alignment(),
    )
}

pub(crate) fn pg_type_storage(pg_type: &str) -> char {
    if builtin_array_element_spec(pg_type).is_some() {
        'x'
    } else {
        pg_type_spec(pg_type).map_or('p', |spec| spec.storage)
    }
}

pub(crate) fn pg_type_category(pg_type: &str) -> char {
    pg_type_spec(pg_type).map_or('U', |spec| spec.category)
}

pub(crate) fn type_collation_oid(pg_type: &str) -> i64 {
    builtin_array_element_spec(pg_type)
        .or_else(|| pg_type_spec(pg_type))
        .map_or(0, |spec| i64::from(spec.collation_oid()))
}

pub(crate) fn primary_index_oid(table_oid: i64) -> i64 {
    80_000 + table_oid
}

pub(crate) fn secondary_index_oid(schema_name: &str, name: &str) -> i64 {
    stable_name_hash_wide(1_000_000, &format!("{schema_name}.{name}"), 1_000_000_000)
}

pub(crate) fn table_row_type_oid(namespace_oid: i64, name: &str) -> i64 {
    stable_name_hash_wide(
        1_100_000_000,
        &format!("{namespace_oid}.{name}"),
        200_000_000,
    )
}

pub(crate) fn table_row_array_type_oid(namespace_oid: i64, name: &str) -> i64 {
    stable_name_hash_wide(
        1_300_000_000,
        &format!("{namespace_oid}.{name}"),
        200_000_000,
    )
}

pub(crate) fn access_method_oid(access_method: &str) -> i64 {
    match access_method.to_ascii_lowercase().as_str() {
        "btree" => 403,
        "hash" => 405,
        "gist" => 783,
        "gin" => 2742,
        "brin" => 3580,
        "spgist" => 4000,
        _ => 0,
    }
}

pub(crate) fn sequence_oid(sequence: &str) -> i64 {
    stable_name_hash_wide(1_700_000_000, sequence, 200_000_000)
}

pub(crate) fn routine_oid(kind: RoutineKind, name: &str) -> i64 {
    let base = match kind {
        RoutineKind::Function => 130_000_000,
        RoutineKind::Procedure => 530_000_000,
    };
    stable_name_hash_wide(base, name, 400_000_000)
}

pub(crate) fn trigger_oid(table: &str, name: &str) -> i64 {
    stable_name_hash(150_000, &format!("{table}.{name}"))
}

pub(crate) fn rewrite_rule_oid(view: &str) -> i64 {
    stable_name_hash(160_000, &format!("{view}._RETURN"))
}

pub(crate) fn namespace_oid(name: &str) -> i64 {
    match name {
        "public" => 2200,
        "pg_catalog" => 11,
        "information_schema" => 13207,
        other => stable_name_hash(80_000, other),
    }
}

pub(crate) fn stable_name_hash(base: i64, name: &str) -> i64 {
    stable_name_hash_wide(base, name, 10_000)
}

pub(crate) fn stable_name_hash_wide(base: i64, name: &str, modulo: i64) -> i64 {
    base + name
        .to_ascii_lowercase()
        .bytes()
        .fold(0_i64, |hash, byte| {
            hash.wrapping_mul(31).wrapping_add(byte as i64)
        })
        .abs()
        % modulo
}

pub(crate) fn routine_language(language: Option<&Ident>, _return_type: &str) -> Result<String> {
    let language = language
        .map(ident_value)
        .unwrap_or_else(|| "sql".to_string())
        .to_ascii_lowercase();
    if language == "sql" || language == "plpgsql" {
        Ok(language)
    } else {
        Err(SqlError::Unsupported(format!(
            "procedural language {language} is not supported"
        )))
    }
}

pub(crate) fn raw_routine_language(sql: &str) -> Result<String> {
    let Some(language_idx) = find_top_level_keyword(sql, "LANGUAGE") else {
        return Ok("sql".to_string());
    };
    let after_language = sql[language_idx + "LANGUAGE".len()..].trim_start();
    let language = after_language
        .split_whitespace()
        .next()
        .ok_or_else(|| SqlError::InvalidSql("CREATE PROCEDURE requires a language".to_string()))?
        .trim_matches(|ch| matches!(ch, '"' | '\'' | ';'))
        .to_ascii_lowercase();
    Ok(language)
}

pub(crate) fn raw_routine_return_type(sql: &str) -> Result<(String, bool)> {
    // PostgreSQL treats every SQL whitespace character identically. BicDB application's
    // generated functions put RETURNS and LANGUAGE on their own lines, so
    // looking for the literal substrings ` returns ` / ` language ` silently
    // classified a trigger function as an ordinary SQL function returning
    // void. Keep the scan at top level so keywords in arguments or bodies do
    // not terminate the declaration.
    let Some(returns_idx) = find_top_level_keyword(sql, "RETURNS") else {
        return Ok(("void".to_string(), false));
    };
    let after_returns = sql[returns_idx + "RETURNS".len()..].trim_start();
    if after_returns.to_ascii_lowercase().starts_with("table") {
        return Ok(("record".to_string(), true));
    }
    let end = [
        "LANGUAGE",
        "IMMUTABLE",
        "STABLE",
        "VOLATILE",
        "AS",
        "COST",
        "ROWS",
        "PARALLEL",
        "STRICT",
        "SECURITY",
        "LEAKPROOF",
        "CALLED",
        "RETURNS",
    ]
    .into_iter()
    .filter_map(|keyword| find_top_level_keyword(after_returns, keyword))
    .min()
    .unwrap_or(after_returns.len());
    let return_type = after_returns[..end].trim();
    if return_type.is_empty() {
        Err(SqlError::InvalidSql(
            "CREATE FUNCTION requires a return type after RETURNS".to_string(),
        ))
    } else {
        let mut normalized = collapse_sql_whitespace(return_type.trim());
        normalized = normalized.trim_matches('"').to_ascii_lowercase();
        if let Some(element_type) = normalized.strip_prefix("setof ") {
            Ok((element_type.trim_matches('"').trim().to_string(), true))
        } else {
            Ok((normalized, false))
        }
    }
}

pub(crate) fn raw_parenthesized_args(sql_after_name: &str) -> Vec<String> {
    let Some(start) = sql_after_name.find('(') else {
        return Vec::new();
    };
    let Some(end) = find_matching_paren_nested(sql_after_name, start) else {
        return Vec::new();
    };
    let args = &sql_after_name[start + 1..end];
    if args.trim().is_empty() {
        Vec::new()
    } else {
        split_top_level_commas_nested(args)
    }
}

pub(crate) fn parse_raw_create_trigger(sql: &str) -> Result<Option<(TriggerSchema, bool)>> {
    let trimmed = trim_sql_statement(sql);
    let Some(mut rest) = strip_prefix_ci(trimmed, "CREATE ") else {
        return Ok(None);
    };
    rest = rest.trim_start();
    let or_replace = if let Some(after) = strip_prefix_ci(rest, "OR REPLACE ") {
        rest = after.trim_start();
        true
    } else {
        false
    };
    let is_constraint = if let Some(after) = strip_prefix_ci(rest, "CONSTRAINT ") {
        rest = after.trim_start();
        true
    } else {
        false
    };
    let Some(after_trigger) = strip_prefix_ci(rest, "TRIGGER ") else {
        return Ok(None);
    };
    let (name, rest) = parse_leading_sql_identifier(after_trigger)?;
    let Some(on_idx) = find_top_level_keyword(rest, "ON") else {
        return Err(SqlError::InvalidSql(
            "CREATE TRIGGER requires ON table".to_string(),
        ));
    };
    let trigger_spec = rest[..on_idx].trim();
    let (timing, event) = raw_trigger_timing_event(trigger_spec)?;
    let after_on = rest[on_idx + "ON".len()..].trim_start();
    let (table_name, rest_after_table) = parse_leading_sql_identifier(after_on)?;
    let (exec_idx, exec_keyword) = find_top_level_keyword(rest_after_table, "EXECUTE FUNCTION")
        .map(|idx| (idx, "EXECUTE FUNCTION"))
        .or_else(|| {
            find_top_level_keyword(rest_after_table, "EXECUTE PROCEDURE")
                .map(|idx| (idx, "EXECUTE PROCEDURE"))
        })
        .ok_or_else(|| {
            SqlError::InvalidSql("CREATE TRIGGER requires EXECUTE FUNCTION".to_string())
        })?;
    let trigger_options = rest_after_table[..exec_idx].to_ascii_lowercase();
    let for_each =
        if trigger_options.contains("for each row") || trigger_options.contains("for row") {
            "row"
        } else if is_constraint {
            // A constraint trigger with no FOR EACH clause is FOR EACH ROW in
            // PostgreSQL (plain triggers default to FOR EACH STATEMENT).
            "row"
        } else {
            "statement"
        };
    // DEFERRABLE INITIALLY DEFERRED queues the trigger to fire at COMMIT.
    // INITIALLY IMMEDIATE (or plain DEFERRABLE) fires at statement end, which
    // is the same point a non-deferred AFTER trigger fires at here.
    let initially_deferred = is_constraint && trigger_options.contains("initially deferred");
    let function_call = rest_after_table[exec_idx + exec_keyword.len()..].trim_start();
    let function_name = raw_trigger_function_name(function_call)?;
    let arguments = raw_trigger_arguments(function_call)?;
    Ok(Some((
        TriggerSchema {
            name: normalize_object_name(&name),
            table_name: normalize_object_name(&table_name),
            function_name,
            arguments,
            definition: trimmed.to_string(),
            event,
            timing,
            for_each: for_each.to_string(),
            enabled: true,
            enabled_mode: TriggerEnabledMode::Origin,
            is_constraint,
            initially_deferred,
        },
        or_replace,
    )))
}

pub(crate) fn raw_trigger_timing_event(spec: &str) -> Result<(String, String)> {
    let spec = spec.trim();
    let lower = spec.to_ascii_lowercase();
    let (timing, event) = if lower.starts_with("before ") {
        ("before", spec["before".len()..].trim())
    } else if lower.starts_with("after ") {
        ("after", spec["after".len()..].trim())
    } else if lower.starts_with("instead of ") {
        ("instead of", spec["instead of".len()..].trim())
    } else {
        return Err(SqlError::InvalidSql(
            "CREATE TRIGGER requires BEFORE, AFTER, or INSTEAD OF".to_string(),
        ));
    };
    let event = event
        .split_whitespace()
        .take_while(|part| !part.eq_ignore_ascii_case("OF"))
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if event.is_empty() {
        return Err(SqlError::InvalidSql(
            "CREATE TRIGGER requires an event".to_string(),
        ));
    }
    Ok((timing.to_string(), event))
}

pub(crate) fn raw_trigger_function_name(function_call: &str) -> Result<String> {
    let end = function_call
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace() || *ch == '(')
        .map(|(idx, _)| idx)
        .unwrap_or(function_call.len());
    let name = function_call[..end].trim();
    if name.is_empty() {
        return Err(SqlError::InvalidSql(
            "CREATE TRIGGER requires a function name".to_string(),
        ));
    }
    if let Some(open) = function_call[end..].find('(') {
        let content_start = end + open + 1;
        if matching_close_paren(function_call, content_start).is_none() {
            return Err(SqlError::InvalidSql(
                "CREATE TRIGGER function arguments are not balanced".to_string(),
            ));
        }
    }
    Ok(normalize_object_name(name))
}

pub(crate) fn raw_trigger_arguments(function_call: &str) -> Result<Vec<String>> {
    let Some(open) = function_call.find('(') else {
        return Ok(Vec::new());
    };
    let Some(close) = find_matching_paren_nested(function_call, open) else {
        return Err(SqlError::InvalidSql(
            "CREATE TRIGGER function arguments are not balanced".to_string(),
        ));
    };
    let body = &function_call[open + 1..close];
    if body.trim().is_empty() {
        return Ok(Vec::new());
    }
    split_top_level_commas_nested(body)
        .into_iter()
        .map(|argument| {
            let argument = argument.trim();
            let literals = parse_sql_string_literals(argument)?;
            if let [literal] = literals.as_slice() {
                Ok(literal.clone())
            } else {
                Ok(argument.trim_matches('"').to_string())
            }
        })
        .collect()
}

pub(crate) fn trigger_event_name(trigger: &CreateTrigger) -> String {
    if trigger.events.len() == 1 {
        match &trigger.events[0] {
            TriggerEvent::Insert => "insert".to_string(),
            TriggerEvent::Update(_) => "update".to_string(),
            TriggerEvent::Delete => "delete".to_string(),
            TriggerEvent::Truncate => "truncate".to_string(),
        }
    } else {
        trigger
            .events
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" or ")
            .to_ascii_lowercase()
    }
}

pub(crate) fn trigger_timing_name(trigger: &CreateTrigger) -> String {
    match trigger.period {
        Some(TriggerPeriod::After) => "after",
        Some(TriggerPeriod::Before) => "before",
        Some(TriggerPeriod::InsteadOf) => "instead of",
        Some(TriggerPeriod::For) | None => "",
    }
    .to_string()
}

pub(crate) fn trigger_for_each_name(trigger: &CreateTrigger) -> String {
    match trigger.trigger_object {
        Some(TriggerObjectKind::For(TriggerObject::Row))
        | Some(TriggerObjectKind::ForEach(TriggerObject::Row)) => "row",
        Some(TriggerObjectKind::For(TriggerObject::Statement))
        | Some(TriggerObjectKind::ForEach(TriggerObject::Statement)) => "statement",
        None => "statement",
    }
    .to_string()
}

pub(crate) fn pg_notify_trigger_call(definition: &str) -> Option<(String, String)> {
    let lower = definition.to_ascii_lowercase();
    let start = lower.find("pg_notify(")? + "pg_notify(".len();
    let end = matching_close_paren(definition, start)?;
    let args = split_top_level_args(&definition[start..end]);
    if args.len() != 2 {
        return None;
    }
    let channel = args[0].trim().trim_matches('\'').to_string();
    if channel.is_empty() {
        return None;
    }
    let payload = args[1].trim();
    let payload_lower = payload.to_ascii_lowercase();
    let column = payload_lower
        .strip_prefix("new.")?
        .split("::")
        .next()?
        .trim()
        .trim_matches('"')
        .to_string();
    if column.is_empty() {
        return None;
    }
    Some((channel, column))
}

pub(crate) fn matching_close_paren(sql: &str, content_start: usize) -> Option<usize> {
    let mut quote = false;
    for (offset, ch) in sql[content_start..].char_indices() {
        if ch == '\'' {
            quote = !quote;
        } else if ch == ')' && !quote {
            return Some(content_start + offset);
        }
    }
    None
}

pub(crate) fn split_top_level_args(args: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut quote = false;
    let mut start = 0;
    for (idx, ch) in args.char_indices() {
        if ch == '\'' {
            quote = !quote;
        } else if ch == ',' && !quote {
            parts.push(args[start..idx].trim().to_string());
            start = idx + 1;
        }
    }
    parts.push(args[start..].trim().to_string());
    parts
}

pub(crate) fn routine_return_type(return_type: &FunctionReturnType) -> Result<(String, bool)> {
    match return_type {
        FunctionReturnType::DataType(data_type) => routine_type_schema(data_type).map(|schema| {
            let rendered = data_type.to_string();
            let normalized = rendered.trim_start().to_ascii_lowercase();
            let returns_set = normalized.starts_with("table(") || normalized.starts_with("table (");
            (schema.pg_type, returns_set)
        }),
        FunctionReturnType::SetOf(data_type) => {
            routine_type_schema(data_type).map(|schema| (schema.pg_type, true))
        }
    }
}

pub(crate) fn routine_return_type_schema(
    return_type: &FunctionReturnType,
) -> Result<(RoutineTypeSchema, String)> {
    let data_type = match return_type {
        FunctionReturnType::DataType(data_type) | FunctionReturnType::SetOf(data_type) => data_type,
    };
    Ok((
        routine_type_schema(data_type)?,
        data_type.to_string().trim().to_string(),
    ))
}

pub(crate) fn routine_type_schema(data_type: &DataType) -> Result<RoutineTypeSchema> {
    let pg_type = match routine_data_type(data_type) {
        Ok(pg_type) => pg_type,
        Err(SqlError::Unsupported(_)) if matches!(data_type, DataType::Custom(_, _)) => {
            collapse_sql_whitespace(&data_type.to_string())
                .trim_matches('"')
                .to_ascii_lowercase()
        }
        Err(error) => return Err(error),
    };
    let type_modifier = if matches!(pg_type.as_str(), "record" | "void" | "trigger") {
        None
    } else {
        pg_type_modifier_from_data_type(data_type)?
    };
    Ok(RoutineTypeSchema {
        pg_type,
        type_modifier,
    })
}

pub(crate) fn routine_argument_type_schemas(
    args: Option<&Vec<sqlparser::ast::OperateFunctionArg>>,
) -> Result<Vec<RoutineTypeSchema>> {
    args.into_iter()
        .flatten()
        .map(|arg| routine_type_schema(&arg.data_type))
        .collect()
}

pub(crate) fn procedure_argument_type_schemas(
    params: Option<&Vec<ProcedureParam>>,
) -> Result<Vec<RoutineTypeSchema>> {
    params
        .into_iter()
        .flatten()
        .map(|param| routine_type_schema(&param.data_type))
        .collect()
}

pub(crate) fn raw_routine_type_schema(declaration: &str) -> Result<RoutineTypeSchema> {
    let normalized = collapse_sql_whitespace(declaration.trim());
    if matches!(normalized.as_str(), "record" | "void" | "trigger") {
        return Ok(RoutineTypeSchema {
            pg_type: normalized,
            type_modifier: None,
        });
    }
    let sql = format!("CREATE TABLE __bicdb_type_probe(value {normalized})");
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let Some(Statement::CreateTable(table)) = statements.pop() else {
        return Err(SqlError::InvalidSql(format!(
            "could not parse routine type {declaration}"
        )));
    };
    let data_type = &table
        .columns
        .first()
        .ok_or_else(|| SqlError::InvalidSql(format!("empty routine type {declaration}")))?
        .data_type;
    routine_type_schema(data_type)
}

pub(crate) fn raw_routine_argument_type_schemas(args: &[String]) -> Result<Vec<RoutineTypeSchema>> {
    args.iter()
        .map(|arg| raw_routine_argument_type_schema(arg))
        .collect()
}

pub(crate) fn raw_routine_argument_type_schema(arg: &str) -> Result<RoutineTypeSchema> {
    let arg = routine_arg_without_default(arg);
    let words = split_sql_words(&arg);
    if words.is_empty() {
        return Err(SqlError::InvalidSql("empty routine argument".to_string()));
    }

    let mut type_start = 0usize;
    if routine_mode_word(&words[0]).is_some() {
        type_start = 1;
        if words[0].eq_ignore_ascii_case("IN")
            && words
                .get(type_start)
                .is_some_and(|word| word.eq_ignore_ascii_case("OUT"))
        {
            type_start += 1;
        }
        if words.len().saturating_sub(type_start) > 1 {
            type_start += 1;
        }
    } else if words
        .get(1)
        .is_some_and(|word| routine_mode_word(word).is_some())
    {
        type_start = 2;
        if words[1].eq_ignore_ascii_case("IN")
            && words
                .get(type_start)
                .is_some_and(|word| word.eq_ignore_ascii_case("OUT"))
        {
            type_start += 1;
        }
    } else if words.len() > 1 && !is_likely_type_word(&words[0]) {
        type_start = 1;
    }

    let declaration = words[type_start..].join(" ");
    if declaration.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "routine argument {arg} is missing a type"
        )));
    }
    raw_routine_type_schema(&declaration)
}

pub(crate) fn routine_data_type(data_type: &DataType) -> Result<String> {
    let rendered = data_type.to_string();
    let normalized = rendered.trim_start().to_ascii_lowercase();
    if normalized.starts_with("table(") || normalized.starts_with("table (") {
        return Ok("record".to_string());
    }
    let object_name = normalize_object_name(&rendered);
    if matches!(object_name.as_str(), "record" | "void") {
        return Ok(object_name);
    }
    Ok(pg_type_from_data_type(data_type)?.0)
}

pub(crate) fn project_virtual_rows(
    projection: &[SelectItem],
    rows: Vec<BTreeMap<String, SqlValue>>,
    fallback_columns: &[String],
) -> Result<SqlResult> {
    if matches!(projection, [SelectItem::Wildcard(_)]) {
        let columns = rows
            .first()
            .map(|row| row.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_else(|| fallback_columns.to_vec());
        let result_rows = rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| virtual_cell(row, column))
                    .collect()
            })
            .collect();
        return Ok(SqlResult::new(columns, result_rows));
    }

    let mut columns = Vec::new();
    let mut exprs = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                columns.push(virtual_field_name(expr)?);
                exprs.push(expr.clone());
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push(expr.clone());
                columns.push(alias.value.clone());
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "unsupported virtual table projection {other}"
                )));
            }
        }
    }
    let result_rows = rows
        .iter()
        .map(|row| {
            exprs
                .iter()
                .map(|expr| eval_virtual_value(row, expr))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SqlResult::new(columns, result_rows))
}

pub(crate) fn eval_virtual_predicate(
    row: &BTreeMap<String, SqlValue>,
    expr: &Expr,
) -> Result<bool> {
    Ok(eval_virtual_truth(row, expr)?.unwrap_or(false))
}

pub(crate) fn eval_virtual_truth(
    row: &BTreeMap<String, SqlValue>,
    expr: &Expr,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(sql_and(
                eval_virtual_truth(row, left)?,
                eval_virtual_truth(row, right)?,
            )),
            BinaryOperator::Or => Ok(sql_or(
                eval_virtual_truth(row, left)?,
                eval_virtual_truth(row, right)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let mut eval = |expr: &Expr| eval_virtual_value(row, expr);
                if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                    return Ok(truth);
                }
                let left = eval_virtual_value(row, left)?;
                let right = eval_virtual_value(row, right)?;
                compare_values(&left, op, &right)
            }
            BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch
            | BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch => {
                let left = eval_virtual_value(row, left)?;
                let right = eval_virtual_value(row, right)?;
                eval_pg_pattern_operator(&left, op, &right)
            }
            _ => Err(SqlError::Unsupported(format!(
                "unsupported virtual WHERE operator {op}"
            ))),
        },
        Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(&eval_virtual_value(
            row, expr,
        )?))),
        Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(&eval_virtual_value(
            row, expr,
        )?))),
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
                eval_virtual_value(row, expr)?,
                eval_virtual_value(row, pattern)?,
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
                eval_virtual_value(row, expr)?,
                eval_virtual_value(row, pattern)?,
                *negated,
                true,
                escape_char.as_ref(),
            )
        }
        Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
            "SIMILAR TO is not supported".to_string(),
        )),
        Expr::Function(function) => match eval_virtual_value(row, expr)? {
            SqlValue::Bool(value) => Ok(Some(value)),
            SqlValue::Null => Ok(None),
            other => Err(SqlError::Unsupported(format!(
                "virtual WHERE function {} returned non-boolean {}",
                function.name,
                other.to_cell()
            ))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut eval = |expr: &Expr| eval_virtual_value(row, expr);
            if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_in_list_truth(
                eval_virtual_value(row, expr)?,
                list.iter()
                    .map(|item| eval_virtual_value(row, item))
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
            let mut eval = |expr: &Expr| eval_virtual_value(row, expr);
            if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_between_truth(
                eval_virtual_value(row, expr)?,
                eval_virtual_value(row, low)?,
                eval_virtual_value(row, high)?,
                *negated,
            )
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => eval_quantified_truth(
            eval_virtual_value(row, left)?,
            compare_op,
            eval_virtual_value(row, right)?,
            false,
        ),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => eval_quantified_truth(
            eval_virtual_value(row, left)?,
            compare_op,
            eval_virtual_value(row, right)?,
            true,
        ),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            Ok(sql_not(eval_virtual_truth(row, expr)?))
        }
        Expr::Nested(expr) => eval_virtual_truth(row, expr),
        _ => Ok(Some(true)),
    }
}

pub(crate) fn eval_virtual_value(
    row: &BTreeMap<String, SqlValue>,
    expr: &Expr,
) -> Result<SqlValue> {
    match expr {
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_date") => {
            Ok(SqlValue::String(unix_now_date_string()))
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(virtual_cell(row, &virtual_field_name(expr)?))
        }
        Expr::Function(function) => {
            let name = object_name(&function.name)?.to_ascii_lowercase();
            let arg_exprs = function_args(function);
            let args = arg_exprs
                .iter()
                .map(|arg| eval_virtual_value(row, arg))
                .collect::<Result<Vec<_>>>()?;
            let arg_types = arg_exprs
                .iter()
                .map(|arg| projected_expr_pg_type(arg, None))
                .collect::<Vec<_>>();
            if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
                .or(eval_range_function_value(&name, &args, &arg_types)?)
            {
                return Ok(value);
            }
            if let Some(value) = eval_json_function_call_value(function, &args)? {
                return Ok(value);
            }
            if let Some(value) = eval_fts_function_value(&name, &args, Some(&arg_types))? {
                return Ok(value);
            }
            eval_catalog_function_value(&name, &args).ok_or_else(|| {
                SqlError::Unsupported(format!(
                    "function {} is not supported on virtual catalog rows",
                    function.name
                ))
            })
        }
        Expr::Cast {
            expr, data_type, ..
        } => cast_value(eval_virtual_value(row, expr)?, data_type),
        Expr::BinaryOp { left, op, right } => eval_binary_value(
            eval_virtual_value(row, left)?,
            op,
            eval_virtual_value(row, right)?,
        ),
        Expr::Like { .. } | Expr::ILike { .. } => eval_virtual_truth(row, expr)
            .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null)),
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            eval_virtual_truth(row, expr)
                .map(sql_not)
                .map(|value| value.map(SqlValue::Bool).unwrap_or(SqlValue::Null))
        }
        Expr::Nested(expr) => eval_virtual_value(row, expr),
        Expr::Collate { expr, collation } => {
            normalize_column_collation(collation)?;
            eval_virtual_value(row, expr)
        }
        _ => eval_constant_expr(expr),
    }
}

pub(crate) fn virtual_field_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| SqlError::Unsupported("empty identifier".to_string())),
        Expr::Value(_) | Expr::Cast { .. } | Expr::Function(_) => Ok(select_expr_column_name(expr)),
        other => Err(SqlError::Unsupported(format!(
            "unsupported virtual field expression {other}"
        ))),
    }
}

pub(crate) fn virtual_cell(row: &BTreeMap<String, SqlValue>, field: &str) -> SqlValue {
    row.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(field))
        .map(|(_, value)| value.clone())
        .unwrap_or(SqlValue::Null)
}

pub(crate) fn virtual_rows_by_oid(
    rows: Vec<BTreeMap<String, SqlValue>>,
) -> BTreeMap<i64, BTreeMap<String, SqlValue>> {
    rows.into_iter()
        .filter_map(|row| sql_value_i64(&virtual_cell(&row, "oid")).map(|oid| (oid, row)))
        .collect()
}

pub(crate) fn apply_virtual_order_by(
    rows: &mut [BTreeMap<String, SqlValue>],
    order_by: Option<&OrderBy>,
) -> Result<()> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Ok(());
    };
    if expressions.is_empty() {
        return Ok(());
    }
    let fields = expressions
        .iter()
        .map(|order| virtual_field_name(&order.expr))
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        expressions
            .iter()
            .zip(&fields)
            .map(|(order, field)| {
                order_value_ordering(
                    &virtual_cell(left, field),
                    &virtual_cell(right, field),
                    &order.options,
                )
            })
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    Ok(())
}

pub(crate) fn apply_virtual_limit(
    rows: &mut Vec<BTreeMap<String, SqlValue>>,
    query: &Query,
) -> Result<()> {
    let mut records = rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            Record::new(idx.to_string()).with_metadata(JsonValue::Object(
                row.iter()
                    .map(|(key, value)| (key.clone(), sql_value_to_json(value.clone())))
                    .collect(),
            ))
        })
        .collect::<Vec<_>>();
    apply_limit(&mut records, query)?;
    let keep = records
        .iter()
        .filter_map(|record| record.id.parse::<usize>().ok())
        .collect::<Vec<_>>();
    *rows = keep
        .into_iter()
        .filter_map(|idx| rows.get(idx).cloned())
        .collect();
    Ok(())
}

pub(crate) fn execute_builtin_function(
    function: &Function,
    db: Option<&BicDb>,
    security_context: Option<&SecurityContext>,
    tx: Option<&bicdb_core::Transaction>,
    session_gucs: Option<&HashMap<String, String>>,
) -> Result<SqlResult> {
    let name = object_name(&function.name)?.to_ascii_lowercase();
    let bare_name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    if matches!(bare_name, "pg_snapshot_xip" | "txid_snapshot_xip") {
        let args = function_args(function)
            .iter()
            .map(eval_constant_expr)
            .collect::<Result<Vec<_>>>()?;
        require_arg_count(bare_name, &args, 1)?;
        let rows = pg_snapshot_xip_values(bare_name, &args[0])?
            .into_iter()
            .map(|value| vec![value])
            .collect();
        return Ok(
            SqlResult::new(vec![bare_name.to_string()], rows).with_column_types(vec![Some(
                if bare_name == "pg_snapshot_xip" {
                    "xid8"
                } else {
                    "int8"
                }
                .to_string(),
            )]),
        );
    }
    if is_builtin_range_type(bare_name)
        || is_builtin_multirange_type(bare_name)
        || matches!(
            bare_name,
            "isempty"
                | "lower"
                | "upper"
                | "lower_inc"
                | "upper_inc"
                | "lower_inf"
                | "upper_inf"
                | "range_merge"
        )
    {
        let arg_exprs = function_args(function);
        let args = arg_exprs
            .iter()
            .map(eval_constant_expr)
            .collect::<Result<Vec<_>>>()?;
        let arg_types = arg_exprs
            .iter()
            .map(|arg| projected_expr_pg_type(arg, None))
            .collect::<Vec<_>>();
        if let Some(value) = eval_network_function_value(&name, &args, &arg_types)?
            .or(eval_range_function_value(&name, &args, &arg_types)?)
        {
            return Ok(
                SqlResult::new(vec![bare_name.to_string()], vec![vec![value]]).with_column_types(
                    vec![projected_expr_pg_type(
                        &Expr::Function(function.clone()),
                        None,
                    )],
                ),
            );
        }
    }
    if matches!(
        bare_name,
        "pg_current_wal_lsn"
            | "pg_current_wal_insert_lsn"
            | "pg_current_wal_flush_lsn"
            | "pg_last_wal_receive_lsn"
            | "pg_last_wal_replay_lsn"
            | "pg_wal_lsn_diff"
    ) {
        let Some(db) = db else {
            return Err(SqlError::Unsupported(format!(
                "function {bare_name} requires database context"
            )));
        };
        let args = function_args(function)
            .iter()
            .map(|arg| match arg {
                Expr::Function(nested) => {
                    let nested_name = object_name(&nested.name)?.to_ascii_lowercase();
                    let nested_bare = nested_name
                        .strip_prefix("pg_catalog.")
                        .unwrap_or(&nested_name);
                    if matches!(
                        nested_bare,
                        "pg_current_wal_lsn"
                            | "pg_current_wal_insert_lsn"
                            | "pg_current_wal_flush_lsn"
                            | "pg_last_wal_receive_lsn"
                            | "pg_last_wal_replay_lsn"
                    ) {
                        return eval_db_catalog_function_value(
                            db,
                            nested_bare,
                            &[],
                            tx.map(Transaction::visibility_watermark),
                            session_gucs,
                        )?
                        .ok_or_else(|| {
                            SqlError::Unsupported(format!(
                                "function {nested_bare} is not supported"
                            ))
                        });
                    }
                    eval_constant_expr(arg)
                }
                _ => eval_constant_expr(arg),
            })
            .collect::<Result<Vec<_>>>()?;
        let value = eval_db_catalog_function_value(
            db,
            bare_name,
            &args,
            tx.map(Transaction::visibility_watermark),
            session_gucs,
        )?
        .ok_or_else(|| SqlError::Unsupported(format!("function {bare_name} is not supported")))?;
        return Ok(
            SqlResult::new(vec![bare_name.to_string()], vec![vec![value]]).with_column_types(vec![
                projected_expr_pg_type(&Expr::Function(function.clone()), None),
            ]),
        );
    }
    if bare_name == "pg_typeof" {
        let arg = function_args(function);
        if arg.len() != 1 {
            return Err(SqlError::InvalidSql(format!(
                "pg_typeof expects 1 argument, got {}",
                arg.len()
            )));
        }
        if let Some(db) = db {
            validate_common_type_expr(db, &arg[0])?;
        }
        let pg_type = db
            .and_then(|db| projected_expr_pg_type_with_db(db, &arg[0]))
            .or_else(|| projected_expr_pg_type(&arg[0], None))
            .unwrap_or_else(|| "unknown".into());
        return Ok(SqlResult::new(
            vec!["pg_typeof".to_string()],
            vec![vec![SqlValue::String(pg_type)]],
        )
        .with_column_types(vec![Some("regtype".to_string())]));
    }
    match name.as_str() {
        "current_database" | "pg_catalog.current_database" => Ok(SqlResult::new(
            vec!["current_database".to_string()],
            vec![vec![SqlValue::String(
                session_gucs
                    .map(current_database_from_gucs)
                    .unwrap_or_else(|| "bicdb".to_string()),
            )]],
        )),
        "current_schema" | "pg_catalog.current_schema" => Ok(SqlResult::new(
            vec!["current_schema".to_string()],
            vec![vec![SqlValue::String("public".to_string())]],
        )),
        "current_schemas" | "pg_catalog.current_schemas" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            let include_implicit = match args.as_slice() {
                [SqlValue::Bool(value)] => *value,
                [other] => {
                    return Err(SqlError::InvalidSql(format!(
                        "current_schemas expects boolean, got {}",
                        other.to_cell()
                    )));
                }
                _ => {
                    return Err(SqlError::InvalidSql(
                        "current_schemas expects one boolean argument".to_string(),
                    ));
                }
            };
            let schemas = if include_implicit {
                vec![
                    SqlValue::String("pg_catalog".to_string()),
                    SqlValue::String("public".to_string()),
                ]
            } else {
                vec![SqlValue::String("public".to_string())]
            };
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![sql_array_value(schemas)?]],
            ))
        }
        "current_user" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::String(
                session_gucs
                    .map(current_user_from_gucs)
                    .unwrap_or_else(current_role_name),
            )]],
        )),
        "session_user" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::String(
                session_gucs
                    .map(session_user_from_gucs)
                    .unwrap_or_else(current_role_name),
            )]],
        )),
        "version" | "pg_catalog.version" => Ok(SqlResult::new(
            vec!["version".to_string()],
            vec![vec![SqlValue::String(match session_gucs {
                Some(session_gucs) => sql_version_banner(session_gucs),
                None => bicdb_version_banner(POSTGRES_COMPATIBILITY_VERSION),
            })]],
        )),
        "bicdb_version" | "pg_catalog.bicdb_version" => Ok(SqlResult::new(
            vec!["bicdb_version".to_string()],
            vec![vec![SqlValue::String(BICDB_VERSION.to_string())]],
        )),
        "current_date" | "pg_catalog.current_date" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::String(unix_now_date_string())]],
        )
        .with_column_types(vec![Some("date".to_string())])),
        "now"
        | "pg_catalog.now"
        | "current_timestamp"
        | "pg_catalog.current_timestamp"
        | "clock_timestamp"
        | "pg_catalog.clock_timestamp" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::String(unix_now_timestamp_string())]],
        )),
        "to_timestamp" | "pg_catalog.to_timestamp" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![eval_to_timestamp(&args)?]],
            ))
        }
        "pg_backend_pid" | "pg_catalog.pg_backend_pid" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::Int(1)]],
        )),
        "pg_is_in_recovery" | "pg_catalog.pg_is_in_recovery" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::Bool(
                db.is_some_and(BicDb::is_replication_standby),
            )]],
        )
        .with_column_types(vec![Some("bool".to_string())])),
        "pg_advisory_lock"
        | "pg_catalog.pg_advisory_lock"
        | "pg_try_advisory_lock"
        | "pg_catalog.pg_try_advisory_lock" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::Bool(true)]],
        )),
        "pg_advisory_unlock" | "pg_catalog.pg_advisory_unlock" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::Bool(true)]],
        )),
        "has_schema_privilege" | "pg_catalog.has_schema_privilege" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![SqlValue::Bool(match db {
                    Some(db) => has_schema_privilege(db, &args)?,
                    None => false,
                })]],
            ))
        }
        "has_table_privilege" | "pg_catalog.has_table_privilege" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![SqlValue::Bool(match db {
                    Some(db) => has_table_privilege(db, &args)?,
                    None => false,
                })]],
            ))
        }
        "has_function_privilege" | "pg_catalog.has_function_privilege" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![match db {
                    Some(db) if args.iter().all(|value| !matches!(value, SqlValue::Null)) => {
                        SqlValue::Bool(has_function_privilege(db, &args)?)
                    }
                    _ => SqlValue::Null,
                }]],
            ))
        }
        "has_type_privilege" | "pg_catalog.has_type_privilege" => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            Ok(SqlResult::new(
                vec![name.replace("pg_catalog.", "")],
                vec![vec![match db {
                    Some(db) if args.iter().all(|value| !matches!(value, SqlValue::Null)) => {
                        SqlValue::Bool(has_type_privilege(db, &args, &current_role_name())?)
                    }
                    _ => SqlValue::Null,
                }]],
            ))
        }
        "obj_description"
        | "pg_catalog.obj_description"
        | "col_description"
        | "pg_catalog.col_description"
        | "pg_get_constraintdef"
        | "pg_catalog.pg_get_constraintdef"
        | "pg_get_indexdef"
        | "pg_catalog.pg_get_indexdef" => Ok(SqlResult::new(
            vec![name.replace("pg_catalog.", "")],
            vec![vec![SqlValue::Null]],
        )),
        other => {
            let args = function_args(function)
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            let arg_types = function_args(function)
                .iter()
                .map(|arg| projected_expr_pg_type(arg, None))
                .collect::<Vec<_>>();
            if is_row_constructor(function) {
                return Ok(SqlResult::new(
                    vec!["row".to_string()],
                    vec![vec![anonymous_record_value(args, arg_types)]],
                )
                .with_column_types(vec![Some("record".to_string())]));
            }
            if matches!(other, "pg_typeof" | "pg_catalog.pg_typeof") {
                let value =
                    pg_typeof_result(arg_types.first().and_then(Option::as_ref), args.first());
                return Ok(
                    SqlResult::new(vec!["pg_typeof".to_string()], vec![vec![value]])
                        .with_column_types(vec![Some("regtype".to_string())]),
                );
            }
            if let Some(value) = eval_network_function_value(other, &args, &arg_types)?
                .or(eval_range_function_value(other, &args, &arg_types)?)
            {
                return Ok(SqlResult::new(
                    vec![name.replace("pg_catalog.", "")],
                    vec![vec![value]],
                )
                .with_column_types(vec![projected_expr_pg_type(
                    &Expr::Function(function.clone()),
                    None,
                )]));
            }
            if let Some(db) = db {
                if let Some(value) = eval_db_catalog_function_value(
                    db,
                    other,
                    &args,
                    tx.map(Transaction::visibility_watermark),
                    session_gucs,
                )? {
                    return Ok(SqlResult::new(
                        vec![name.replace("pg_catalog.", "")],
                        vec![vec![value]],
                    ));
                }
                if let Some(value) = eval_broker_function_value(
                    db,
                    other,
                    &args,
                    BrokerCaller::Sql {
                        context: security_context,
                        superuser: session_gucs
                            .is_some_and(|gucs| session_role_is_superuser(db, gucs)),
                    },
                    tx,
                )? {
                    return Ok(SqlResult::new(
                        vec![name.replace("pg_catalog.", "")],
                        vec![vec![value]],
                    ));
                }
            }
            if let Some(value) = eval_json_function_call_value(function, &args)? {
                return Ok(SqlResult::new(
                    vec![name.replace("pg_catalog.", "")],
                    vec![vec![value]],
                ));
            }
            if let Some(value) = crate::eval_xml_function_value(other, &args, Some(&arg_types))? {
                return Ok(SqlResult::new(
                    vec![name.replace("pg_catalog.", "")],
                    vec![vec![value]],
                ));
            }
            if let Some(value) = eval_fts_function_value(other, &args, Some(&arg_types))? {
                return Ok(SqlResult::new(
                    vec![name.replace("pg_catalog.", "")],
                    vec![vec![value]],
                ));
            }
            eval_catalog_function_value(other, &args)
                .map(|value| {
                    SqlResult::new(vec![name.replace("pg_catalog.", "")], vec![vec![value]])
                })
                .ok_or_else(|| {
                    SqlError::Unsupported(format!("function {other} is not supported without FROM"))
                })
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Projection {
    pub(crate) columns: Vec<String>,
    pub(crate) fields: Vec<ProjectedRecordField>,
}

impl Projection {
    pub(crate) fn from_select_items(
        items: &[SelectItem],
        schema: Option<&TableSchema>,
    ) -> Result<Self> {
        let mut columns = Vec::new();
        let mut fields = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    for field in FieldRef::wildcard(schema) {
                        columns.push(field.name());
                        fields.push(ProjectedRecordField::bind(field, schema));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let field = schema_projected_field(FieldRef::from_expr(expr)?, schema);
                    validate_projected_field(&field, schema)?;
                    columns.push(field.name());
                    fields.push(ProjectedRecordField::bind(field, schema));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let field = schema_projected_field(FieldRef::from_expr(expr)?, schema);
                    validate_projected_field(&field, schema)?;
                    columns.push(alias.value.clone());
                    fields.push(ProjectedRecordField::bind(field, schema));
                }
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "unsupported SELECT projection {other}"
                    )));
                }
            }
        }

        Ok(Self { columns, fields })
    }

    pub(crate) fn row(&self, record: &Record) -> Result<Vec<SqlValue>> {
        self.fields
            .iter()
            .map(|field| field.value(record))
            .collect::<Result<Vec<_>>>()
    }

    /// Per-column logical PostgreSQL type names for this projection, derived from
    /// the bound schema columns. `None` for dynamic/JSON fields with no declared
    /// type (the wire layer then falls back to its value-width heuristic).
    pub(crate) fn column_types(&self) -> Vec<Option<String>> {
        self.fields
            .iter()
            .map(ProjectedRecordField::pg_type)
            .collect()
    }

    pub(crate) fn column_metadata(
        &self,
        db: &BicDb,
        relation: &str,
        schema: Option<&TableSchema>,
    ) -> Vec<SqlColumnMetadata> {
        let Some(schema) = schema else {
            return Vec::new();
        };
        let relation_oid = table_oids(db).get(relation).copied().unwrap_or_default() as i32;
        self.fields
            .iter()
            .map(|field| match field {
                ProjectedRecordField::SchemaColumn { column, .. } => {
                    let attribute_number = schema
                        .columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(&column.name))
                        .and_then(|index| i16::try_from(index + 1).ok())
                        .unwrap_or_default();
                    SqlColumnMetadata {
                        table_oid: relation_oid,
                        attribute_number,
                        type_modifier: column.catalog_typmod() as i32,
                    }
                }
                ProjectedRecordField::Field(_) => SqlColumnMetadata::default(),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ProjectedRecordField {
    SchemaColumn {
        column: ColumnSchema,
        use_record_id_for_primary_key: bool,
    },
    Field(FieldRef),
}

impl ProjectedRecordField {
    pub(crate) fn bind(field: FieldRef, schema: Option<&TableSchema>) -> Self {
        let Some(schema) = schema else {
            return Self::Field(field);
        };
        let builtin_column = match &field {
            FieldRef::Metadata => Some("metadata"),
            FieldRef::Timestamp => Some("timestamp"),
            FieldRef::Payload => Some("payload"),
            FieldRef::Vector => Some("vector"),
            FieldRef::Geometry => Some("geometry"),
            _ => None,
        };
        if let Some(column) = builtin_column
            .and_then(|name| schema.column(name))
            .filter(|column| !column.hidden)
        {
            return Self::SchemaColumn {
                column: column.clone(),
                use_record_id_for_primary_key: column.primary_key,
            };
        }
        match &field {
            FieldRef::PrimaryKey { name, .. } => schema
                .column(name)
                .filter(|column| !column.hidden)
                .map(|column| Self::SchemaColumn {
                    column: column.clone(),
                    use_record_id_for_primary_key: true,
                })
                .unwrap_or(Self::Field(field)),
            FieldRef::Column(name)
            | FieldRef::JsonColumn(name)
            | FieldRef::TypedColumn { name, .. } => schema
                .column(name)
                .filter(|column| !column.hidden)
                .map(|column| Self::SchemaColumn {
                    column: column.clone(),
                    use_record_id_for_primary_key: false,
                })
                .unwrap_or(Self::Field(field)),
            _ => Self::Field(field),
        }
    }

    pub(crate) fn value(&self, record: &Record) -> Result<SqlValue> {
        match self {
            Self::SchemaColumn {
                column,
                use_record_id_for_primary_key,
            } => Ok(record_schema_column_value(
                record,
                column,
                *use_record_id_for_primary_key,
            )),
            Self::Field(field) => field.value(record),
        }
    }

    /// Declared PostgreSQL type name for this projected field, if known from the
    /// schema. Schema-bound columns and primary keys carry their declared type;
    /// dynamic/JSON fields return `None`.
    pub(crate) fn pg_type(&self) -> Option<String> {
        match self {
            Self::SchemaColumn { column, .. } => Some(column.pg_type.clone()),
            Self::Field(FieldRef::PrimaryKey { pg_type, .. }) => Some(pg_type.clone()),
            Self::Field(FieldRef::TypedColumn { pg_type, .. }) => Some(pg_type.clone()),
            Self::Field(_) => None,
        }
    }
}

fn schema_column_projected_pg_type(column: &ColumnSchema) -> String {
    column
        .user_type
        .as_ref()
        .map(UserTypeColumnSchema::formatted_name)
        .unwrap_or_else(|| column.pg_type.clone())
}

fn schema_column_array_delimiter(column: &ColumnSchema) -> Option<char> {
    column
        .user_type
        .as_ref()
        .filter(|user_type| user_type.array)
        .map(UserTypeColumnSchema::scalar_delimiter)
}

pub(crate) fn schema_projected_field(field: FieldRef, schema: Option<&TableSchema>) -> FieldRef {
    let Some(schema) = schema else {
        return field;
    };
    let field = match field {
        FieldRef::Id => {
            if let Some(column) = schema.column("id").filter(|column| !column.hidden) {
                if column.primary_key {
                    FieldRef::PrimaryKey {
                        name: column.name.clone(),
                        pg_type: schema_column_projected_pg_type(column),
                        array_delimiter: schema_column_array_delimiter(column),
                    }
                } else {
                    FieldRef::TypedColumn {
                        name: column.name.clone(),
                        pg_type: schema_column_projected_pg_type(column),
                        array_delimiter: schema_column_array_delimiter(column),
                    }
                }
            } else {
                FieldRef::Id
            }
        }
        FieldRef::MetadataPath(path) => match table_qualified_schema_column(&path, schema) {
            Some(column) => schema
                .column(&column)
                .map(|schema_column| FieldRef::TypedColumn {
                    name: schema_column.name.clone(),
                    pg_type: schema_column_projected_pg_type(schema_column),
                    array_delimiter: schema_column_array_delimiter(schema_column),
                })
                .unwrap_or(FieldRef::Column(column)),
            None => FieldRef::MetadataPath(path),
        },
        FieldRef::Metadata
            if schema
                .column("metadata")
                .is_some_and(|column| is_json_pg_type(&column.pg_type)) =>
        {
            FieldRef::JsonColumn("metadata".to_string())
        }
        FieldRef::Payload => match schema.column("payload") {
            Some(column) if is_json_pg_type(&column.pg_type) => {
                FieldRef::JsonColumn(column.name.clone())
            }
            Some(column) => FieldRef::TypedColumn {
                name: column.name.clone(),
                pg_type: schema_column_projected_pg_type(column),
                array_delimiter: schema_column_array_delimiter(column),
            },
            None => FieldRef::Payload,
        },
        FieldRef::Geometry => schema
            .column("geometry")
            .filter(|column| !column.hidden)
            .map(schema_column_field_ref)
            .unwrap_or(FieldRef::Geometry),
        FieldRef::JsonTextPath(base, path) => {
            FieldRef::JsonTextPath(Box::new(schema_projected_field(*base, Some(schema))), path)
        }
        FieldRef::JsonPath(base, path) => {
            FieldRef::JsonPath(Box::new(schema_projected_field(*base, Some(schema))), path)
        }
        FieldRef::Cast(base, data_type) => FieldRef::Cast(
            Box::new(schema_projected_field(*base, Some(schema))),
            data_type,
        ),
        other => other,
    };
    let column = match field {
        FieldRef::Column(column) | FieldRef::JsonColumn(column) => column,
        FieldRef::TypedColumn { name, .. } => name,
        other => return other,
    };
    let Some(schema_column) = schema.column(&column).filter(|column| !column.hidden) else {
        return FieldRef::Column(column);
    };
    if schema_column.primary_key {
        FieldRef::PrimaryKey {
            name: schema_column.name.clone(),
            pg_type: schema_column_projected_pg_type(schema_column),
            array_delimiter: schema_column_array_delimiter(schema_column),
        }
    } else if is_json_pg_type(&schema_column.pg_type) {
        FieldRef::JsonColumn(schema_column.name.clone())
    } else {
        FieldRef::TypedColumn {
            name: schema_column.name.clone(),
            pg_type: schema_column_projected_pg_type(schema_column),
            array_delimiter: schema_column_array_delimiter(schema_column),
        }
    }
}

pub(crate) fn table_qualified_schema_column(
    path: &[String],
    schema: &TableSchema,
) -> Option<String> {
    let (column, qualifier) = path.split_last()?;
    if qualifier.is_empty() || qualifier[0].eq_ignore_ascii_case("metadata") {
        return None;
    }
    let schema_column = schema.column(column).filter(|column| !column.hidden)?;
    if qualifier_matches_schema(qualifier, schema) {
        Some(schema_column.name.clone())
    } else {
        None
    }
}

pub(crate) fn qualifier_matches_schema(qualifier: &[String], schema: &TableSchema) -> bool {
    let qualifier = qualifier.join(".");
    if qualifier.eq_ignore_ascii_case(&schema.name) {
        return true;
    }
    let qualifier_relation = qualifier.rsplit('.').next().unwrap_or(&qualifier);
    let schema_relation = schema.name.rsplit('.').next().unwrap_or(&schema.name);
    qualifier_relation.eq_ignore_ascii_case(schema_relation)
}

pub(crate) fn ensure_schema_column(table: &str, schema: &TableSchema, column: &str) -> Result<()> {
    if schema.column(column).is_some_and(|column| !column.hidden) || is_builtin_field(column) {
        Ok(())
    } else {
        Err(SqlError::UndefinedColumn {
            table: table.to_string(),
            column: column.to_string(),
        })
    }
}

pub(crate) fn is_builtin_field(column: &str) -> bool {
    matches!(
        column.to_ascii_lowercase().as_str(),
        "id" | "metadata" | "timestamp" | "payload" | "vector" | "geometry"
    )
}

pub(crate) fn validate_projected_field(
    field: &FieldRef,
    schema: Option<&TableSchema>,
) -> Result<()> {
    let Some(schema) = schema else {
        return Ok(());
    };
    match field {
        FieldRef::Id
        | FieldRef::Metadata
        | FieldRef::Timestamp
        | FieldRef::Payload
        | FieldRef::Vector
        | FieldRef::Geometry
        | FieldRef::PrimaryKey { .. }
        | FieldRef::Literal(_) => Ok(()),
        FieldRef::Column(column)
        | FieldRef::JsonColumn(column)
        | FieldRef::TypedColumn { name: column, .. } => {
            ensure_schema_column(&schema.name, schema, column)
        }
        FieldRef::MetadataPath(_) => Ok(()),
        FieldRef::Cast(base, _) => validate_projected_field(base, Some(schema)),
        FieldRef::JsonTextPath(base, _) | FieldRef::JsonPath(base, _) => {
            validate_projected_field(base, Some(schema))
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum FieldRef {
    Id,
    Metadata,
    Timestamp,
    Payload,
    Vector,
    Geometry,
    PrimaryKey {
        name: String,
        pg_type: String,
        array_delimiter: Option<char>,
    },
    Column(String),
    TypedColumn {
        name: String,
        pg_type: String,
        array_delimiter: Option<char>,
    },
    JsonColumn(String),
    MetadataPath(Vec<String>),
    Literal(SqlValue),
    Cast(Box<FieldRef>, DataType),
    JsonTextPath(Box<FieldRef>, Vec<JsonPathElement>),
    JsonPath(Box<FieldRef>, Vec<JsonPathElement>),
}

impl FieldRef {
    fn declared_pg_type(&self) -> Option<String> {
        match self {
            Self::PrimaryKey { pg_type, .. } | Self::TypedColumn { pg_type, .. } => {
                Some(pg_type.clone())
            }
            Self::Cast(_, data_type) => pg_type_from_data_type(data_type)
                .ok()
                .map(|(pg_type, _)| pg_type),
            Self::JsonTextPath(_, _) => Some("text".to_string()),
            _ => None,
        }
    }

    fn declared_array_delimiter(&self) -> Option<char> {
        match self {
            Self::PrimaryKey {
                array_delimiter, ..
            }
            | Self::TypedColumn {
                array_delimiter, ..
            } => *array_delimiter,
            _ => None,
        }
    }

    fn cast_value(&self, value: SqlValue, data_type: &DataType) -> Result<SqlValue> {
        let target_type = pg_type_from_data_type(data_type)
            .ok()
            .map(|(pg_type, _)| pg_type);
        if matches!(
            target_type.as_deref(),
            Some("text" | "varchar" | "bpchar" | "name")
        ) {
            if self.declared_pg_type().as_deref() == Some("bpchar")
                && matches!(target_type.as_deref(), Some("text" | "varchar" | "name"))
            {
                return Ok(match value {
                    SqlValue::String(value) => {
                        SqlValue::String(value.trim_end_matches(' ').to_string())
                    }
                    value => value,
                });
            }
            if let Some(element_type) = self
                .declared_pg_type()
                .as_deref()
                .and_then(|pg_type| pg_type.strip_suffix("[]"))
            {
                return postgres_array_text_value(
                    &value,
                    self.declared_array_delimiter()
                        .or_else(|| pg_type_delimiter(element_type))
                        .unwrap_or(','),
                )
                .map(SqlValue::String);
            }
        }
        cast_value(value, data_type)
    }

    pub(crate) fn wildcard(schema: Option<&TableSchema>) -> Vec<Self> {
        if let Some(schema) = schema {
            return schema
                .columns
                .iter()
                .filter(|column| !column.hidden)
                .map(schema_column_field_ref)
                .collect();
        }
        vec![
            Self::Id,
            Self::Metadata,
            Self::Timestamp,
            Self::Payload,
            Self::Vector,
        ]
    }

    pub(crate) fn from_expr(expr: &Expr) -> Result<Self> {
        Self::from_expr_inner(expr)?
            .ok_or_else(|| SqlError::Unsupported(format!("unsupported field expression {expr}")))
    }

    /// `from_expr` for callers that treat "not a field expression" as a
    /// fallback signal rather than an error. The rendering of the whole
    /// expression into an error message that `from_expr` builds was, per
    /// profile, the single largest source of AST formatting on the TPC-C
    /// hot path, several times per evaluated row.
    pub(crate) fn from_expr_opt(expr: &Expr) -> Option<Self> {
        Self::from_expr_inner(expr).ok().flatten()
    }

    fn from_expr_inner(expr: &Expr) -> Result<Option<Self>> {
        let field = match expr {
            Expr::Identifier(ident) => Self::from_parts(std::slice::from_ref(&ident.value))?,
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect::<Vec<_>>();
                Self::from_parts(&parts)?
            }
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::Arrow | BinaryOperator::LongArrow) =>
            {
                let Some(base) = Self::from_expr_inner(left)? else {
                    return Ok(None);
                };
                let path = json_operator_path(right)?;
                if matches!(op, BinaryOperator::LongArrow) {
                    Self::JsonTextPath(Box::new(base), path)
                } else {
                    Self::JsonPath(Box::new(base), path)
                }
            }
            Expr::JsonAccess { value, path } => {
                let Some(base) = Self::from_expr_inner(value)? else {
                    return Ok(None);
                };
                Self::JsonPath(Box::new(base), json_access_path(path)?)
            }
            Expr::Value(value) => Self::Literal(literal_to_value(value)?),
            Expr::TypedString(value) => Self::Literal(typed_string_to_value(value)?),
            Expr::Cast {
                expr, data_type, ..
            } => {
                let Some(inner) = Self::from_expr_inner(expr)? else {
                    return Ok(None);
                };
                Self::Cast(Box::new(inner), data_type.clone())
            }
            Expr::Nested(expr) => return Self::from_expr_inner(expr),
            Expr::Collate { expr, collation } => {
                normalize_column_collation(collation)?;
                return Self::from_expr_inner(expr);
            }
            _ => return Ok(None),
        };
        Ok(Some(field))
    }

    pub(crate) fn from_parts(parts: &[String]) -> Result<Self> {
        let Some(head) = parts.first() else {
            return Err(SqlError::Unsupported("empty field expression".to_string()));
        };
        match head.to_ascii_lowercase().as_str() {
            "id" if parts.len() == 1 => Ok(Self::Id),
            "metadata" if parts.len() == 1 => Ok(Self::Metadata),
            "metadata" => Ok(Self::MetadataPath(parts[1..].to_vec())),
            "timestamp" if parts.len() == 1 => Ok(Self::Timestamp),
            "payload" if parts.len() == 1 => Ok(Self::Payload),
            "vector" if parts.len() == 1 => Ok(Self::Vector),
            "geometry" if parts.len() == 1 => Ok(Self::Geometry),
            _ if parts.len() == 1 => Ok(Self::Column(parts[0].clone())),
            _ => Ok(Self::MetadataPath(parts.to_vec())),
        }
    }

    pub(crate) fn name(&self) -> String {
        match self {
            Self::Id => "id".to_string(),
            Self::Metadata => "metadata".to_string(),
            Self::Timestamp => "timestamp".to_string(),
            Self::Payload => "payload".to_string(),
            Self::Vector => "vector".to_string(),
            Self::Geometry => "geometry".to_string(),
            Self::PrimaryKey { name, .. } => name.clone(),
            Self::Column(name) | Self::JsonColumn(name) => name.clone(),
            Self::TypedColumn { name, .. } => name.clone(),
            Self::MetadataPath(path) => format!("metadata.{}", path.join(".")),
            Self::Literal(value) => value.to_cell(),
            Self::Cast(base, _) => base.name(),
            Self::JsonTextPath(base, path) => format!(
                "{}->>{}",
                base.name(),
                path.iter()
                    .map(JsonPathElement::label)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
            Self::JsonPath(base, path) => format!(
                "{}->{}",
                base.name(),
                path.iter()
                    .map(JsonPathElement::label)
                    .collect::<Vec<_>>()
                    .join(".")
            ),
        }
    }

    /// Typed-path twin of [`Self::value`]: resolve against a record's cached
    /// flat cell view without materializing the `Value` tree. `None` means the
    /// field shape is not representable from flat cells (nested paths, vector
    /// fallbacks, whole-metadata projections) and the caller must fall back to
    /// the `Record` form for this record.
    pub(crate) fn value_from_stored(
        &self,
        stored: &bicdb_core::StoredRecord,
        cells: &bicdb_core::TypedRow,
    ) -> Option<Result<SqlValue>> {
        fn cell_value(cell: &bicdb_core::TypedCell) -> Option<SqlValue> {
            Some(match cell {
                bicdb_core::TypedCell::Null => SqlValue::Null,
                bicdb_core::TypedCell::Bool(value) => SqlValue::Bool(*value),
                bicdb_core::TypedCell::Int(value) => SqlValue::Int(*value),
                bicdb_core::TypedCell::Float(value) => SqlValue::Float(*value),
                bicdb_core::TypedCell::Number(value) => value
                    .parse::<f64>()
                    .map(SqlValue::Float)
                    .unwrap_or_else(|_| SqlValue::String(value.to_string())),
                bicdb_core::TypedCell::Str(value) => SqlValue::String(value.to_string()),
                // Nested JSON: parse only when actually projected (rare on
                // the hot schema paths).
                bicdb_core::TypedCell::Raw(raw) => match serde_json::from_str(raw) {
                    Ok(value) => json_to_sql_value(&value),
                    Err(_) => return None,
                },
            })
        }
        fn lookup<'a>(
            cells: &'a bicdb_core::TypedRow,
            name: &str,
        ) -> Option<&'a bicdb_core::TypedCell> {
            cells
                .iter()
                .find(|(key, _)| key.as_ref() == name)
                .or_else(|| cells.iter().find(|(key, _)| key.eq_ignore_ascii_case(name)))
                .map(|(_, cell)| cell)
        }
        match self {
            Self::Id => Some(Ok(SqlValue::String(stored.id.clone()))),
            Self::Timestamp => Some(Ok(stored
                .timestamp
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null))),
            Self::PrimaryKey { name, pg_type, .. } => match lookup(cells, name) {
                Some(cell) if is_json_pg_type(pg_type) => {
                    typed_json_cell_to_sql_value(cell).map(Ok)
                }
                Some(cell) => cell_value(cell).map(Ok),
                None if is_json_pg_type(pg_type)
                    && name.eq_ignore_ascii_case("payload")
                    && stored.payload.is_some() =>
                {
                    Some(Ok(payload_bytes_json_value(
                        stored.payload.as_deref().expect("checked above"),
                    )))
                }
                // pk column missing from metadata: needs the record-id cast
                // fallback — take the Record path.
                None => None,
            },
            Self::Column(column) => match lookup(cells, column) {
                Some(cell) => cell_value(cell).map(Ok),
                None => {
                    if is_vector_column(None, column) && stored.vector.is_some() {
                        return None;
                    }
                    Some(Ok(SqlValue::Null))
                }
            },
            Self::TypedColumn { name, pg_type, .. } => match lookup(cells, name) {
                Some(cell) => cell_value(cell).map(|value| {
                    Ok(match value {
                        SqlValue::Json(json) => storage_json_to_sql_value(&json, pg_type),
                        value => value,
                    })
                }),
                None if name.eq_ignore_ascii_case("payload")
                    && pg_type == "bytea"
                    && stored.payload.is_some() =>
                {
                    Some(Ok(SqlValue::String(format_bytea_hex(
                        stored.payload.as_deref().expect("checked above"),
                    ))))
                }
                None => Some(Ok(SqlValue::Null)),
            },
            Self::JsonColumn(column) => match lookup(cells, column) {
                Some(cell) => typed_json_cell_to_sql_value(cell).map(Ok),
                None if column.eq_ignore_ascii_case("payload") && stored.payload.is_some() => {
                    Some(Ok(payload_bytes_json_value(
                        stored.payload.as_deref().expect("checked above"),
                    )))
                }
                None => Some(Ok(SqlValue::Null)),
            },
            Self::MetadataPath(path) if path.len() == 1 => match lookup(cells, &path[0]) {
                Some(cell) => cell_value(cell).map(Ok),
                None => Some(Ok(SqlValue::Null)),
            },
            Self::Literal(value) => Some(Ok(value.clone())),
            Self::Cast(base, data_type) => base
                .value_from_stored(stored, cells)
                .map(|value| value.and_then(|value| base.cast_value(value, data_type))),
            _ => None,
        }
    }

    pub(crate) fn value(&self, record: &Record) -> Result<SqlValue> {
        Ok(match self {
            Self::Id => SqlValue::String(record.id.clone()),
            Self::Metadata => SqlValue::Json(
                record
                    .metadata
                    .get("metadata")
                    .cloned()
                    .unwrap_or_else(|| record.metadata.clone()),
            ),
            Self::Timestamp => record
                .timestamp
                .map(SqlValue::Int)
                .unwrap_or(SqlValue::Null),
            Self::Payload => record
                .payload
                .as_ref()
                .map(|payload| {
                    SqlValue::Json(JsonValue::Array(
                        payload.iter().map(|byte| JsonValue::from(*byte)).collect(),
                    ))
                })
                .or_else(|| {
                    json_path(&record.metadata, &["payload".to_string()]).map(json_to_sql_value)
                })
                .unwrap_or(SqlValue::Null),
            Self::Vector => record
                .vector
                .as_ref()
                .map(|vector| {
                    SqlValue::Json(JsonValue::Array(
                        vector
                            .iter()
                            .filter_map(|value| serde_json::Number::from_f64(*value as f64))
                            .map(JsonValue::Number)
                            .collect(),
                    ))
                })
                .unwrap_or(SqlValue::Null),
            Self::Geometry => record
                .geometry
                .as_ref()
                .cloned()
                .map(SqlValue::Geometry)
                .unwrap_or(SqlValue::Null),
            Self::PrimaryKey { name, pg_type, .. } => {
                if pg_type == "json" {
                    return Ok(json_value_for_key_case_insensitive(&record.metadata, name)
                        .map(|value| storage_json_to_sql_value(value, pg_type))
                        .or_else(|| legacy_payload_json_column_value(record, name))
                        .unwrap_or_else(|| cast_primary_key_record_id(record, pg_type)));
                }
                if pg_type == "jsonb" {
                    return Ok(json_value_for_key_case_insensitive(&record.metadata, name)
                        .cloned()
                        .map(SqlValue::Json)
                        .or_else(|| legacy_payload_json_column_value(record, name))
                        .unwrap_or_else(|| cast_primary_key_record_id(record, pg_type)));
                }
                let stored = json_path(&record.metadata, std::slice::from_ref(name))
                    .map(|value| storage_json_to_sql_value(value, pg_type))
                    .unwrap_or(SqlValue::Null);
                if matches!(stored, SqlValue::Null) {
                    cast_primary_key_record_id(record, pg_type)
                } else {
                    stored
                }
            }
            Self::Column(column) => {
                json_path_case_insensitive(&record.metadata, std::slice::from_ref(column))
                    .map(json_to_sql_value)
                    .or_else(|| {
                        if is_vector_column(None, column) {
                            record
                                .vector
                                .as_ref()
                                .map(|vector| SqlValue::Json(vector_json(vector)))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(SqlValue::Null)
            }
            Self::TypedColumn { name, pg_type, .. } => {
                json_path_case_insensitive(&record.metadata, std::slice::from_ref(name))
                    .map(|value| storage_json_to_sql_value(value, pg_type))
                    .or_else(|| {
                        (name.eq_ignore_ascii_case("payload") && pg_type == "bytea")
                            .then(|| {
                                record
                                    .payload
                                    .as_ref()
                                    .map(|payload| SqlValue::String(format_bytea_hex(payload)))
                            })
                            .flatten()
                    })
                    .unwrap_or(SqlValue::Null)
            }
            Self::JsonColumn(column) => {
                json_value_for_key_case_insensitive(&record.metadata, column)
                    .map(|value| {
                        let stored_type = value
                            .get(TYPED_STORAGE_KEY)
                            .and_then(|stored| stored.get("pg_type"))
                            .and_then(JsonValue::as_str);
                        match stored_type {
                            Some("json") => storage_json_to_sql_value(value, "json"),
                            _ => SqlValue::Json(value.clone()),
                        }
                    })
                    .or_else(|| legacy_payload_json_column_value(record, column))
                    .unwrap_or(SqlValue::Null)
            }
            Self::MetadataPath(path) => json_path(&record.metadata, path)
                .or_else(|| {
                    record
                        .metadata
                        .get("metadata")
                        .and_then(|value| json_path(value, path))
                })
                .map(json_to_sql_value)
                .unwrap_or(SqlValue::Null),
            Self::Literal(value) => value.clone(),
            Self::Cast(base, data_type) => base.cast_value(base.value(record)?, data_type)?,
            Self::JsonTextPath(base, path) => {
                let value = base.value(record)?;
                json_extract_path_value(&value, path, true)
            }
            Self::JsonPath(base, path) => {
                let value = base.value(record)?;
                json_extract_path_value(&value, path, false)
            }
        })
    }
}

pub(crate) fn schema_column_field_ref(column: &ColumnSchema) -> FieldRef {
    if column.primary_key {
        return FieldRef::PrimaryKey {
            name: column.name.clone(),
            pg_type: schema_column_projected_pg_type(column),
            array_delimiter: schema_column_array_delimiter(column),
        };
    }
    if is_json_pg_type(&column.pg_type) {
        return FieldRef::JsonColumn(column.name.clone());
    }
    let name = column.name.as_str();
    if name.eq_ignore_ascii_case("id") {
        FieldRef::TypedColumn {
            name: column.name.clone(),
            pg_type: schema_column_projected_pg_type(column),
            array_delimiter: schema_column_array_delimiter(column),
        }
    } else if name.eq_ignore_ascii_case("metadata") {
        FieldRef::Metadata
    } else if name.eq_ignore_ascii_case("timestamp") {
        FieldRef::Timestamp
    } else if name.eq_ignore_ascii_case("payload") {
        FieldRef::TypedColumn {
            name: column.name.clone(),
            pg_type: schema_column_projected_pg_type(column),
            array_delimiter: schema_column_array_delimiter(column),
        }
    } else if name.eq_ignore_ascii_case("vector") {
        FieldRef::Vector
    } else {
        FieldRef::TypedColumn {
            name: column.name.clone(),
            pg_type: schema_column_projected_pg_type(column),
            array_delimiter: schema_column_array_delimiter(column),
        }
    }
}

pub(crate) fn expression_metadata_paths(expr: &Expr) -> Vec<Vec<String>> {
    let mut paths = Vec::new();
    collect_expression_metadata_paths(expr, &mut paths);
    paths
}

pub(crate) fn json_object_path(path: &[JsonPathElement]) -> Option<Vec<String>> {
    path.iter()
        .map(|element| match element {
            JsonPathElement::Key(key) | JsonPathElement::Text(key) => Some(key.clone()),
            JsonPathElement::Index(_) => None,
        })
        .collect()
}

pub(crate) fn collect_expression_metadata_paths(expr: &Expr, paths: &mut Vec<Vec<String>>) {
    if let Some(field) = FieldRef::from_expr_opt(expr) {
        match field {
            FieldRef::Column(column) | FieldRef::JsonColumn(column) => paths.push(vec![column]),
            FieldRef::MetadataPath(path) => paths.push(path),
            FieldRef::JsonTextPath(base, path) | FieldRef::JsonPath(base, path) => {
                if let Some(path) = json_object_path(&path) {
                    match *base {
                        FieldRef::Column(column) | FieldRef::JsonColumn(column) => {
                            let mut full_path = vec![column];
                            full_path.extend(path);
                            paths.push(full_path);
                        }
                        FieldRef::Metadata => paths.push(path),
                        FieldRef::MetadataPath(mut base_path) => {
                            base_path.extend(path);
                            paths.push(base_path);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            collect_expression_metadata_paths(left, paths);
            collect_expression_metadata_paths(right, paths);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_expression_metadata_paths(expr, paths),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expression_metadata_paths(expr, paths);
            collect_expression_metadata_paths(low, paths);
            collect_expression_metadata_paths(high, paths);
        }
        Expr::InList { expr, list, .. } => {
            collect_expression_metadata_paths(expr, paths);
            for item in list {
                collect_expression_metadata_paths(item, paths);
            }
        }
        Expr::Cast { expr, .. } => collect_expression_metadata_paths(expr, paths),
        _ => {}
    }
}

pub(crate) fn index_field_from_expr(expr: &Expr) -> Result<IndexField> {
    match expr {
        Expr::Function(function) if function_name_is(function, "lower") => {
            let Some(args) = positional_function_args(&function.args) else {
                return Err(SqlError::Unsupported(
                    "lower index expression requires positional arguments".to_string(),
                ));
            };
            if args.len() != 1 {
                return Err(SqlError::Unsupported(format!(
                    "lower index expression expects 1 argument, got {}",
                    args.len()
                )));
            }
            return Ok(IndexField::Lower(Box::new(index_field_from_expr(args[0])?)));
        }
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } if is_default_both_trim(trim_where.as_ref(), trim_what.as_deref(), trim_characters) => {
            return Ok(IndexField::Trim(Box::new(index_field_from_expr(expr)?)));
        }
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } => return index_field_from_expr(expr),
        Expr::Nested(expr) => return index_field_from_expr(expr),
        _ => {}
    }
    let field = FieldRef::from_expr(expr)?;
    index_field_from_field_ref(&field)
}

pub(crate) fn index_field_from_expr_for_schema(
    schema: &TableSchema,
    expr: &Expr,
) -> Result<IndexField> {
    match expr {
        Expr::Function(function) if function_name_is(function, "lower") => {
            let Some(args) = positional_function_args(&function.args) else {
                return Err(SqlError::Unsupported(
                    "lower index expression requires positional arguments".to_string(),
                ));
            };
            if args.len() != 1 {
                return Err(SqlError::Unsupported(format!(
                    "lower index expression expects 1 argument, got {}",
                    args.len()
                )));
            }
            return Ok(IndexField::Lower(Box::new(
                index_field_from_expr_for_schema(schema, args[0])?,
            )));
        }
        Expr::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } if is_default_both_trim(trim_where.as_ref(), trim_what.as_deref(), trim_characters) => {
            return Ok(IndexField::Trim(Box::new(
                index_field_from_expr_for_schema(schema, expr)?,
            )));
        }
        Expr::Cast { expr, .. } | Expr::Collate { expr, .. } | Expr::Nested(expr) => {
            return index_field_from_expr_for_schema(schema, expr);
        }
        Expr::Identifier(ident) => {
            let column = if ident.quote_style.is_some() {
                schema.column(&ident.value)
            } else {
                schema
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&ident.value))
            };
            if let Some(column) = column {
                if column.name.eq_ignore_ascii_case("payload") {
                    return Ok(IndexField::MetadataPath(vec![column.name.clone()]));
                }
                if column.name != ident.value {
                    let mut canonical = ident.clone();
                    canonical.value = column.name.clone();
                    return index_field_from_expr(&Expr::Identifier(canonical))
                        .map(|field| canonical_index_field_for_schema(schema, field));
                }
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(column) = idents
                .last()
                .and_then(|ident| schema.column(&ident.value))
                .filter(|column| column.name.eq_ignore_ascii_case("payload"))
            {
                return Ok(IndexField::MetadataPath(vec![column.name.clone()]));
            }
        }
        _ => {}
    }
    index_field_from_expr(expr).map(|field| canonical_index_field_for_schema(schema, field))
}

pub(crate) fn is_default_both_trim(
    trim_where: Option<&sqlparser::ast::TrimWhereField>,
    trim_what: Option<&Expr>,
    trim_characters: &Option<Vec<Expr>>,
) -> bool {
    let default_direction = trim_where
        .map(|direction| direction.to_string().eq_ignore_ascii_case("both"))
        .unwrap_or(true);
    default_direction
        && trim_what.is_none()
        && trim_characters
            .as_ref()
            .is_none_or(|characters| characters.is_empty())
}

pub(crate) fn index_access_method_name(using: Option<&IndexType>) -> &'static str {
    match using {
        Some(IndexType::Hash) => "hash",
        Some(IndexType::GIN) => "gin",
        Some(IndexType::GiST) => "gist",
        Some(IndexType::SPGiST) => "spgist",
        Some(IndexType::BRIN) => "brin",
        Some(IndexType::BTree) | None => "btree",
        _ => "btree",
    }
}

pub(crate) fn index_supports_ordering(schema: &TableSchema, index_name: &str) -> bool {
    schema
        .indexes
        .iter()
        .find(|index| index.name.eq_ignore_ascii_case(index_name))
        .is_none_or(|index| index.access_method.eq_ignore_ascii_case("btree"))
}

pub(crate) fn is_executable_fts_index(create_index: &CreateIndex, schema: &TableSchema) -> bool {
    matches!(create_index.using, Some(IndexType::GIN | IndexType::GiST))
        && !create_index.unique
        && create_index.columns.len() == 1
        && (is_weighted_fts_expression(&create_index.columns[0].column.expr)
            || projected_expr_pg_type(&create_index.columns[0].column.expr, Some(schema))
                .as_deref()
                == Some("tsvector"))
}

pub(crate) fn is_executable_jsonb_index(create_index: &CreateIndex, schema: &TableSchema) -> bool {
    matches!(create_index.using, Some(IndexType::GIN))
        && !create_index.unique
        && create_index.columns.len() == 1
        && projected_expr_pg_type(&create_index.columns[0].column.expr, Some(schema)).as_deref()
            == Some("jsonb")
}

pub(crate) fn is_executable_array_index(create_index: &CreateIndex, schema: &TableSchema) -> bool {
    matches!(create_index.using, Some(IndexType::GIN))
        && !create_index.unique
        && create_index.columns.len() == 1
        && projected_expr_pg_type(&create_index.columns[0].column.expr, Some(schema))
            .is_some_and(|pg_type| pg_type.ends_with("[]"))
}

pub(crate) fn is_executable_geometric_index(
    create_index: &CreateIndex,
    schema: &TableSchema,
) -> bool {
    if create_index.unique || create_index.columns.is_empty() {
        return false;
    }
    let access_method = index_access_method_name(create_index.using.as_ref());
    create_index.columns.iter().all(|column| {
        projected_expr_pg_type(&column.column.expr, Some(schema)).is_some_and(|pg_type| {
            geometric_index_default_opclass(access_method, &pg_type).is_some()
        })
    })
}

pub(crate) fn full_text_projection_field(index_name: &str) -> String {
    format!("$bicdb_fts_{index_name}")
}

pub(crate) fn jsonb_projection_field(index_name: &str) -> String {
    format!("$bicdb_jsonb_{index_name}")
}

pub(crate) fn array_projection_field(index_name: &str) -> String {
    format!("$bicdb_array_{index_name}")
}

pub(crate) fn geometric_projection_field(index_name: &str, position: usize) -> String {
    format!("$bicdb_geometric_{index_name}_{position}")
}

pub(crate) fn geometric_internal_index_name(index_name: &str, position: usize) -> String {
    format!(
        "bicdb_geometric_internal_{}_{}",
        stable_name_hash_wide(1, index_name, 1_000_000_000),
        position
    )
}

pub(crate) fn is_weighted_fts_expression(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => is_weighted_fts_expression(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::StringConcat,
            right,
        } => is_weighted_fts_expression(left) && is_weighted_fts_expression(right),
        Expr::Function(function) if function_name_is(function, "setweight") => {
            is_setweight_tsvector(function)
        }
        _ => false,
    }
}

pub(crate) fn is_setweight_tsvector(function: &Function) -> bool {
    let Some(args) = positional_function_args(&function.args) else {
        return false;
    };
    if args.len() != 2 || !is_string_literal(args[1], Some(&["A", "B", "C", "D"])) {
        return false;
    }
    let Expr::Function(to_tsvector) = args[0] else {
        return false;
    };
    if !function_name_is(to_tsvector, "to_tsvector") {
        return false;
    }
    let Some(tsvector_args) = positional_function_args(&to_tsvector.args) else {
        return false;
    };
    if tsvector_args.len() != 2 || !is_string_literal(tsvector_args[0], Some(&["english"])) {
        return false;
    }
    is_coalesce_empty_text(tsvector_args[1])
}

pub(crate) fn is_coalesce_empty_text(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    if !function_name_is(function, "coalesce") {
        return false;
    }
    let Some(args) = positional_function_args(&function.args) else {
        return false;
    };
    args.len() == 2
        && FieldRef::from_expr_opt(args[0]).is_some()
        && is_string_literal(args[1], Some(&[""]))
}

pub(crate) fn positional_function_args(args: &FunctionArguments) -> Option<Vec<&Expr>> {
    let FunctionArguments::List(list) = args else {
        return None;
    };
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
            _ => None,
        })
        .collect()
}

pub(crate) fn table_function_expr_args(args: &TableFunctionArgs) -> Result<Vec<Expr>> {
    if args
        .settings
        .as_ref()
        .is_some_and(|settings| !settings.is_empty())
    {
        return Err(SqlError::Unsupported(
            "table function SETTINGS clauses are not supported".to_string(),
        ));
    }
    args.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
            _ => Err(SqlError::Unsupported(
                "table functions support only positional expression arguments".to_string(),
            )),
        })
        .collect()
}

pub(crate) fn is_generate_series_table_function(table: &str) -> bool {
    table.eq_ignore_ascii_case("generate_series")
        || table.eq_ignore_ascii_case("pg_catalog.generate_series")
}

pub(crate) fn is_regexp_split_to_table_function(table: &str) -> bool {
    table.eq_ignore_ascii_case("regexp_split_to_table")
        || table.eq_ignore_ascii_case("pg_catalog.regexp_split_to_table")
}

pub(crate) fn regexp_split_to_table_values(values: &[SqlValue]) -> Result<Vec<SqlValue>> {
    if !(2..=3).contains(&values.len()) {
        return Err(SqlError::InvalidSql(format!(
            "regexp_split_to_table expects 2 or 3 arguments, got {}",
            values.len()
        )));
    }
    if values.iter().any(|value| matches!(value, SqlValue::Null)) {
        return Ok(Vec::new());
    }

    let source = values[0].to_cell();
    let mut pattern = values[1].to_cell();
    let flags = values.get(2).map(SqlValue::to_cell).unwrap_or_default();
    let mut case_insensitive = false;
    let mut multi_line = false;
    let mut dot_matches_new_line = true;
    let mut ignore_whitespace = false;
    for flag in flags.chars() {
        match flag {
            // PostgreSQL's ARE syntax is the default. `c` and `t` explicitly
            // select the defaults, while `e` requests the compatible ERE mode.
            'c' | 'e' | 't' => {}
            'i' => case_insensitive = true,
            'm' | 'n' => {
                multi_line = true;
                dot_matches_new_line = false;
            }
            'p' => dot_matches_new_line = false,
            'q' => pattern = regex::escape(&pattern),
            's' => {
                multi_line = false;
                dot_matches_new_line = true;
            }
            'w' => {
                multi_line = true;
                dot_matches_new_line = true;
            }
            'x' => ignore_whitespace = true,
            'b' => {
                return Err(SqlError::Unsupported(
                    "regexp_split_to_table basic regular-expression mode is not supported"
                        .to_string(),
                ));
            }
            other => {
                return Err(SqlError::invalid_parameter_value(format!(
                    "invalid regular expression option: {other}"
                )));
            }
        }
    }
    let regex = RegexBuilder::new(&pattern)
        .case_insensitive(case_insensitive)
        .multi_line(multi_line)
        .dot_matches_new_line(dot_matches_new_line)
        .ignore_whitespace(ignore_whitespace)
        .build()
        .map_err(|error| {
            SqlError::invalid_parameter_value(format!("invalid regular expression: {error}"))
        })?;

    // PostgreSQL ignores zero-length delimiter matches at the start/end of the
    // input and immediately after a previous delimiter match. `Regex::split`
    // retains some of those empty fields, so materialize the documented
    // behavior directly.
    let mut parts = Vec::new();
    let mut field_start = 0usize;
    let mut previous_match_end = None;
    for matched in regex.find_iter(&source) {
        let start = matched.start();
        let end = matched.end();
        let ignored_empty = start == end
            && (start == 0 || end == source.len() || previous_match_end == Some(start));
        if !ignored_empty {
            parts.push(SqlValue::String(source[field_start..start].to_string()));
            field_start = end;
        }
        previous_match_end = Some(end);
    }
    parts.push(SqlValue::String(source[field_start..].to_string()));
    Ok(parts)
}

pub(crate) fn unnest_source_columns(
    array_count: usize,
    with_offset: bool,
    with_offset_alias: Option<&Ident>,
    with_ordinality: bool,
) -> Vec<String> {
    let mut columns = (0..array_count)
        .map(|idx| {
            if idx == 0 {
                "unnest".to_string()
            } else {
                format!("unnest_{}", idx + 1)
            }
        })
        .collect::<Vec<_>>();
    if with_offset {
        columns.push(
            with_offset_alias
                .map(|alias| alias.value.clone())
                .unwrap_or_else(|| "offset".to_string()),
        );
    }
    if with_ordinality {
        columns.push("ordinality".to_string());
    }
    columns
}

pub(crate) fn table_function_source_columns_for_alias(
    alias: Option<&TableAlias>,
    alias_name: &str,
    source_columns: Vec<String>,
) -> Vec<String> {
    let Some(alias) = alias else {
        return source_columns;
    };
    if !alias.columns.is_empty() || source_columns.len() != 1 {
        return source_columns;
    }
    vec![alias_name.to_string()]
}

pub(crate) fn unnest_values_from_sql(value: SqlValue) -> Result<Vec<SqlValue>> {
    fn flatten(value: &JsonValue, output: &mut Vec<SqlValue>) {
        match value {
            JsonValue::Array(values) => {
                for value in values {
                    flatten(value, output);
                }
            }
            value => output.push(json_to_sql_value(value)),
        }
    }

    fn flatten_array(value: &JsonValue) -> Option<Vec<SqlValue>> {
        let value = match value {
            JsonValue::Array(_) => value,
            JsonValue::Object(object) => {
                let value = object.get("$bicdb_array_input")?.get("value")?;
                value.as_array()?;
                value
            }
            _ => return None,
        };
        let mut output = Vec::new();
        flatten(value, &mut output);
        Some(output)
    }

    match value {
        SqlValue::Null => Ok(Vec::new()),
        SqlValue::Json(value) => flatten_array(&value).ok_or_else(|| {
            SqlError::InvalidSql(format!(
                "UNNEST expects an array argument, got {}",
                SqlValue::Json(value).to_cell()
            ))
        }),
        SqlValue::String(value) => match parse_array_literal(&value) {
            Ok(values) => {
                let mut output = Vec::new();
                for value in &values {
                    flatten(value, &mut output);
                }
                Ok(output)
            }
            Err(error) => parse_pg_int_vector(&value).ok_or(error),
        },
        other => Err(SqlError::InvalidSql(format!(
            "UNNEST expects an array argument, got {}",
            other.to_cell()
        ))),
    }
}

pub(crate) fn parse_pg_int_vector(value: &str) -> Option<Vec<SqlValue>> {
    if value.trim().is_empty() {
        return Some(Vec::new());
    }
    value
        .split_whitespace()
        .map(|part| part.parse::<i64>().ok().map(SqlValue::Int))
        .collect()
}

pub(crate) fn is_one_row_catalog_table_function(table: &str) -> bool {
    let table = table.strip_prefix("pg_catalog.").unwrap_or(table);
    matches!(table.to_ascii_lowercase().as_str(), "pg_control_system")
}

pub(crate) fn validate_zero_arg_table_function(
    table: &str,
    args: &TableFunctionArgs,
) -> Result<()> {
    let args = table_function_expr_args(args)?;
    if !args.is_empty() {
        return Err(SqlError::InvalidSql(format!(
            "{table} expects no arguments, got {}",
            args.len()
        )));
    }
    Ok(())
}

pub(crate) fn estimate_generate_series_rows(args: &TableFunctionArgs) -> Result<usize> {
    Ok(generate_series_bounds(args)?
        .map(|(_, _, _, rows)| rows)
        .unwrap_or_default())
}

pub(crate) fn generate_series_bounds(
    args: &TableFunctionArgs,
) -> Result<Option<(i64, i64, i64, usize)>> {
    let args = table_function_expr_args(args)?;
    if !(2..=3).contains(&args.len()) {
        return Err(SqlError::InvalidSql(format!(
            "generate_series expects 2 or 3 arguments, got {}",
            args.len()
        )));
    }
    let Some(start) = generate_series_i64_arg(&args[0])? else {
        return Ok(None);
    };
    let Some(stop) = generate_series_i64_arg(&args[1])? else {
        return Ok(None);
    };
    let step = if let Some(expr) = args.get(2) {
        let Some(step) = generate_series_i64_arg(expr)? else {
            return Ok(None);
        };
        step
    } else {
        1
    };
    if step == 0 {
        return Err(SqlError::InvalidSql(
            "generate_series step size cannot equal zero".to_string(),
        ));
    }
    let rows = generate_series_row_count(start, stop, step)?;
    Ok(Some((start, stop, step, rows)))
}

pub(crate) fn generate_series_i64_arg(expr: &Expr) -> Result<Option<i64>> {
    let value = eval_constant_expr(expr)?;
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    sql_value_i64(&value).map(Some).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "generate_series argument must be an integer-compatible value, got {}",
            value.to_cell()
        ))
    })
}

pub(crate) fn generate_series_row_count(start: i64, stop: i64, step: i64) -> Result<usize> {
    if (step > 0 && start > stop) || (step < 0 && start < stop) {
        return Ok(0);
    }
    let distance = if step > 0 {
        i128::from(stop) - i128::from(start)
    } else {
        i128::from(start) - i128::from(stop)
    };
    let count = distance / i128::from(step).abs() + 1;
    usize::try_from(count).map_err(|_| {
        SqlError::Unsupported(
            "generate_series result is too large to materialize on this platform".to_string(),
        )
    })
}

pub(crate) fn function_name_is(function: &Function, expected: &str) -> bool {
    // Compare the last name part structurally; rendering the ObjectName to
    // a String per call was a measurable share of the TPC-C hot path.
    if !expected.contains('.') {
        return match function.name.0.last() {
            Some(sqlparser::ast::ObjectNamePart::Identifier(ident)) => {
                ident.value.eq_ignore_ascii_case(expected)
            }
            _ => false,
        };
    }
    let rendered = function.name.to_string();
    rendered.eq_ignore_ascii_case(expected)
        || rendered
            .rsplit('.')
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case(expected))
}

pub(crate) fn is_string_literal(expr: &Expr, allowed: Option<&[&str]>) -> bool {
    let value = match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => value.as_str(),
        _ => return false,
    };
    allowed
        .map(|allowed| {
            allowed
                .iter()
                .any(|candidate| value.eq_ignore_ascii_case(candidate))
        })
        .unwrap_or(true)
}

pub(crate) fn spatial_index_field_from_name(name: &str) -> Result<IndexField> {
    if name.eq_ignore_ascii_case("geometry") {
        Ok(IndexField::Geometry)
    } else {
        Ok(IndexField::MetadataPath(vec![name.to_string()]))
    }
}

pub(crate) fn index_field_from_field_ref(field: &FieldRef) -> Result<IndexField> {
    match field {
        FieldRef::Id => Ok(IndexField::Id),
        FieldRef::Timestamp => Ok(IndexField::Timestamp),
        FieldRef::Geometry => Err(SqlError::Unsupported(
            "cannot create an index on geometry".to_string(),
        )),
        FieldRef::Column(column)
        | FieldRef::JsonColumn(column)
        | FieldRef::TypedColumn { name: column, .. } => {
            Ok(IndexField::MetadataPath(vec![column.clone()]))
        }
        FieldRef::MetadataPath(path) => Ok(IndexField::MetadataPath(path.clone())),
        FieldRef::JsonTextPath(base, path) | FieldRef::JsonPath(base, path) => {
            match base.as_ref() {
                FieldRef::Metadata
                | FieldRef::Column(_)
                | FieldRef::TypedColumn { .. }
                | FieldRef::JsonColumn(_) => json_object_path(path)
                    .map(IndexField::MetadataPath)
                    .ok_or_else(|| {
                        SqlError::Unsupported(
                            "array-index JSON expressions cannot use metadata indexes".to_string(),
                        )
                    }),
                other => Err(SqlError::Unsupported(format!(
                    "cannot index JSON path rooted at {}",
                    other.name()
                ))),
            }
        }
        other => Err(SqlError::Unsupported(format!(
            "cannot create an index on {}",
            other.name()
        ))),
    }
}

pub(crate) fn canonical_index_field_for_schema(
    schema: &TableSchema,
    field: IndexField,
) -> IndexField {
    match field {
        IndexField::MetadataPath(path) if path.len() == 1 => schema
            .column(&path[0])
            .map(|column| IndexField::MetadataPath(vec![column.name.clone()]))
            .unwrap_or(IndexField::MetadataPath(path)),
        IndexField::Lower(inner) => {
            IndexField::Lower(Box::new(canonical_index_field_for_schema(schema, *inner)))
        }
        IndexField::Trim(inner) => {
            IndexField::Trim(Box::new(canonical_index_field_for_schema(schema, *inner)))
        }
        other => other,
    }
}

pub(crate) fn parse_raw_create_spatial_index(
    sql: &str,
) -> Result<Option<(String, String, String)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE SPATIAL INDEX ") {
        return Ok(None);
    }
    if trimmed.contains(';') {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX accepts exactly one statement".to_string(),
        ));
    }
    let rest = trimmed["CREATE SPATIAL INDEX ".len()..].trim();
    let Some((index_name, after_index)) = split_first_ident(rest) else {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX requires an index name".to_string(),
        ));
    };
    let after_index = after_index.trim_start();
    if !after_index.to_ascii_uppercase().starts_with("ON ") {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX requires ON collection(field)".to_string(),
        ));
    }
    let after_on = after_index[2..].trim_start();
    let Some(open_paren) = after_on.find('(') else {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX requires ON collection(field)".to_string(),
        ));
    };
    let table = after_on[..open_paren].trim();
    let field_and_tail = &after_on[open_paren + 1..];
    let Some(close_paren) = field_and_tail.find(')') else {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX requires a closing ')'".to_string(),
        ));
    };
    let field = field_and_tail[..close_paren].trim();
    if !field_and_tail[close_paren + 1..].trim().is_empty()
        || table.is_empty()
        || field.is_empty()
        || field.contains(',')
    {
        return Err(SqlError::InvalidSql(
            "CREATE SPATIAL INDEX supports exactly one field".to_string(),
        ));
    }
    Ok(Some((
        unquote_simple_ident(index_name),
        unquote_simple_ident(table),
        unquote_simple_ident(field),
    )))
}

/// `PACK SPATIAL INDEX <name> [USING HILBERT | STR]` — rebuild a spatial
/// index as a bulk-packed immutable durable base. Returns
/// `(index_name, strategy_keyword)`; the strategy keyword is uppercased and
/// validated by the executor (which owns the strategy enum).
pub(crate) fn parse_raw_pack_spatial_index(sql: &str) -> Result<Option<(String, Option<String>)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("PACK SPATIAL INDEX ") {
        return Ok(None);
    }
    if trimmed.contains(';') {
        return Err(SqlError::InvalidSql(
            "PACK SPATIAL INDEX accepts exactly one statement".to_string(),
        ));
    }
    let rest = trimmed["PACK SPATIAL INDEX ".len()..].trim();
    let Some((index_name, after_index)) = split_first_ident(rest) else {
        return Err(SqlError::InvalidSql(
            "PACK SPATIAL INDEX requires an index name".to_string(),
        ));
    };
    let after_index = after_index.trim();
    if after_index.is_empty() {
        return Ok(Some((unquote_simple_ident(index_name), None)));
    }
    let after_upper = after_index.to_ascii_uppercase();
    let Some(strategy) = after_upper.strip_prefix("USING ") else {
        return Err(SqlError::InvalidSql(
            "PACK SPATIAL INDEX supports only an optional USING <strategy> clause".to_string(),
        ));
    };
    let strategy = strategy.trim();
    if strategy.is_empty() || strategy.split_whitespace().count() != 1 {
        return Err(SqlError::InvalidSql(
            "PACK SPATIAL INDEX USING requires exactly one strategy name".to_string(),
        ));
    }
    Ok(Some((
        unquote_simple_ident(index_name),
        Some(strategy.to_string()),
    )))
}

pub(crate) struct RawCreateIndexSchema {
    pub(crate) name: String,
    pub(crate) table: String,
    pub(crate) expression: String,
    pub(crate) unique: bool,
    pub(crate) access_method: String,
    pub(crate) if_not_exists: bool,
}

pub(crate) fn parse_raw_create_index_on_only(sql: &str) -> Result<Option<RawCreateIndexSchema>> {
    let mut rest = trim_sql_statement(sql);
    let Some(after_create) = strip_prefix_ci(rest, "CREATE ") else {
        return Ok(None);
    };
    rest = after_create.trim_start();
    let unique = if let Some(after_unique) = strip_prefix_ci(rest, "UNIQUE ") {
        rest = after_unique.trim_start();
        true
    } else {
        false
    };
    let Some(after_index) = strip_prefix_ci(rest, "INDEX ") else {
        return Ok(None);
    };
    rest = after_index.trim_start();
    if strip_prefix_ci(rest, "CONCURRENTLY ").is_some() {
        // Same refusal as the parsed path: a silently-blocking "concurrent"
        // build is a lie to the client. docs/create-index-concurrently-design.md.
        return Err(SqlError::Unsupported(
            "CREATE INDEX CONCURRENTLY is not supported yet: BicDB index builds \
             currently serialize writes for the build's duration. Run CREATE INDEX \
             (blocking) in a maintenance window, or track \
             docs/create-index-concurrently-design.md for the non-blocking design."
                .to_string(),
        ));
    }
    let if_not_exists = if let Some(after_if_not_exists) = strip_prefix_ci(rest, "IF NOT EXISTS ") {
        rest = after_if_not_exists.trim_start();
        true
    } else {
        false
    };
    let (index_name, after_name) = parse_leading_policy_identifier(rest)?;
    let Some(after_on) = strip_prefix_ci(after_name.trim_start(), "ON ") else {
        return Ok(None);
    };
    let Some(after_only) = strip_prefix_ci(after_on.trim_start(), "ONLY ") else {
        return Ok(None);
    };
    let (table, mut rest) = parse_leading_policy_identifier(after_only)?;
    rest = rest.trim_start();
    let access_method = if let Some(after_using) = strip_prefix_ci(rest, "USING ") {
        let (method, after_method) = parse_leading_policy_identifier(after_using)?;
        rest = after_method.trim_start();
        normalize_object_name(&method)
    } else {
        "btree".to_string()
    };
    let Some(open) = rest.find('(') else {
        return Err(SqlError::InvalidSql(
            "CREATE INDEX ON ONLY expects an index expression list".to_string(),
        ));
    };
    if !rest[..open].trim().is_empty() {
        return Err(SqlError::Unsupported(format!(
            "CREATE INDEX ON ONLY clause `{}` is not supported before expression list",
            rest[..open].trim()
        )));
    }
    let close = find_matching_paren_nested(rest, open).ok_or_else(|| {
        SqlError::InvalidSql("unterminated CREATE INDEX expression list".to_string())
    })?;
    let mut expression = rest[open + 1..close].trim().to_string();
    let trailing = rest[close + 1..].trim();
    if !trailing.is_empty() {
        expression.push(' ');
        expression.push_str(trailing);
    }
    Ok(Some(RawCreateIndexSchema {
        name: normalize_object_name(&index_name),
        table: normalize_object_name(&table),
        expression,
        unique,
        access_method,
        if_not_exists,
    }))
}

pub(crate) fn split_first_ident(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    if let Some(stripped) = input.strip_prefix('"') {
        let end = stripped.find('"')? + 2;
        return Some((&input[..end], &input[end..]));
    }
    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_ident_char(ch)).then_some(index))
        .unwrap_or(input.len());
    (end > 0).then_some((&input[..end], &input[end..]))
}

pub(crate) fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

pub(crate) fn unquote_simple_ident(value: &str) -> String {
    value
        .trim()
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or_else(|| value.trim())
        .to_string()
}

pub(crate) struct DoExistsGuard {
    pub(crate) negated: bool,
    pub(crate) exists_sql: String,
}

pub(crate) struct DoIfExistsDdl {
    pub(crate) guards: Vec<DoExistsGuard>,
    pub(crate) ddl: String,
}

pub(crate) struct DoRelationCompatibility {
    pub(crate) relation_pairs: Vec<(String, String)>,
}

pub(crate) struct DoCompatibilityViewGrants {
    pub(crate) role: String,
    pub(crate) views: Vec<String>,
}

pub(crate) struct DoApplicationFunctionDependencyGrants {
    pub(crate) role: String,
}

pub(crate) fn parse_raw_do_relation_compatibility(
    sql: &str,
) -> Result<Option<DoRelationCompatibility>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    if !trimmed.to_ascii_uppercase().starts_with("DO ") {
        return Ok(None);
    }
    let pair_marker = "relation_pairs CONSTANT TEXT[][] := ARRAY[";
    let Some(pair_start) = trimmed.find(pair_marker) else {
        return Ok(None);
    };
    if !trimmed.contains("FOREACH relation_pair SLICE 1 IN ARRAY relation_pairs LOOP")
        || !trimmed.contains("'ALTER TABLE public.%I RENAME TO %I'")
        || !trimmed.contains(
            "'CREATE VIEW public.%I WITH (security_invoker = true) AS SELECT * FROM public.%I'",
        )
    {
        return Ok(None);
    }
    let pair_values = &trimmed[pair_start + pair_marker.len()..];
    let pair_end = pair_values.find("]; ").or_else(|| pair_values.find("];\n"));
    let pair_end = pair_end.ok_or_else(|| {
        SqlError::InvalidSql("relation compatibility array is not terminated".to_string())
    })?;
    let values = parse_sql_string_literals(&pair_values[..pair_end])?;
    if values.is_empty() || values.len() % 2 != 0 {
        return Err(SqlError::InvalidSql(
            "relation compatibility array requires old/new relation pairs".to_string(),
        ));
    }
    let mut relation_pairs = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks_exact(2) {
        if !pair.iter().all(|name| {
            !name.is_empty()
                && name.chars().all(is_ident_char)
                && !name.starts_with(|c: char| c.is_ascii_digit())
        }) {
            return Err(SqlError::InvalidSql(
                "relation compatibility names must be unquoted SQL identifiers".to_string(),
            ));
        }
        relation_pairs.push((pair[0].clone(), pair[1].clone()));
    }
    Ok(Some(DoRelationCompatibility { relation_pairs }))
}

pub(crate) fn parse_raw_do_compatibility_view_grants(
    sql: &str,
) -> Result<Option<DoCompatibilityViewGrants>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    if !trimmed.to_ascii_uppercase().starts_with("DO ")
        || !trimmed.contains("FOREACH compatibility_view IN ARRAY ARRAY[")
        || !trimmed.contains("'GRANT SELECT, INSERT, UPDATE, DELETE ON public.%I TO ")
    {
        return Ok(None);
    }
    let role_marker = "WHERE rolname = '";
    let Some(role_start) = trimmed.find(role_marker) else {
        return Ok(None);
    };
    let role_tail = &trimmed[role_start + role_marker.len()..];
    let role_end = role_tail.find('\'').ok_or_else(|| {
        SqlError::InvalidSql("compatibility view grant role is not terminated".to_string())
    })?;
    let role = role_tail[..role_end].to_string();
    if role.is_empty() || !role.chars().all(is_ident_char) {
        return Err(SqlError::InvalidSql(
            "compatibility view grant role must be an SQL identifier".to_string(),
        ));
    }
    let array_marker = "FOREACH compatibility_view IN ARRAY ARRAY[";
    let array_start = trimmed.find(array_marker).expect("checked above") + array_marker.len();
    let array_tail = &trimmed[array_start..];
    let array_end = array_tail.find("] LOOP").ok_or_else(|| {
        SqlError::InvalidSql("compatibility view grant array is not terminated".to_string())
    })?;
    let views = parse_sql_string_literals(&array_tail[..array_end])?;
    if views.is_empty()
        || !views.iter().all(|name| {
            !name.is_empty()
                && name.chars().all(is_ident_char)
                && !name.starts_with(|c: char| c.is_ascii_digit())
        })
    {
        return Err(SqlError::InvalidSql(
            "compatibility view names must be SQL identifiers".to_string(),
        ));
    }
    Ok(Some(DoCompatibilityViewGrants { role, views }))
}

pub(crate) fn parse_raw_do_carrier_function_dependency_grants(
    sql: &str,
) -> Result<Option<DoApplicationFunctionDependencyGrants>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    if !trimmed.starts_with("DO $carrier_grant_function_dependencies$") {
        return Ok(None);
    }
    if !trimmed.contains("runtime_role_oid OID := 'carrier_app'::regrole::oid;")
        || !trimmed.contains("carrier_runtime_function_dependency_work")
        || !trimmed.contains("carrier_runtime_relation_dependency_work")
        || !trimmed.ends_with("$carrier_grant_function_dependencies$")
    {
        return Err(SqlError::Unsupported(
            "BicDB application function-dependency grant block has an unsupported shape"
                .to_string(),
        ));
    }
    Ok(Some(DoApplicationFunctionDependencyGrants {
        role: "carrier_app".to_string(),
    }))
}

pub(crate) fn parse_sql_string_literals(value: &str) -> Result<Vec<String>> {
    let bytes = value.as_bytes();
    let mut values = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'\'' {
            index += 1;
            continue;
        }
        index += 1;
        let mut literal = String::new();
        let mut terminated = false;
        while index < bytes.len() {
            if bytes[index] == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    literal.push('\'');
                    index += 2;
                    continue;
                }
                index += 1;
                terminated = true;
                break;
            }
            literal.push(bytes[index] as char);
            index += 1;
        }
        if !terminated {
            return Err(SqlError::InvalidSql(
                "unterminated SQL string literal".to_string(),
            ));
        }
        values.push(literal);
    }
    Ok(values)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RawAlterDefaultPrivileges {
    pub(crate) schema_name: String,
    pub(crate) object_type: PrivilegeObjectType,
    pub(crate) privileges: Vec<String>,
    pub(crate) grantees: Vec<String>,
    pub(crate) grant: bool,
}

pub(crate) fn parse_raw_alter_default_privileges(
    sql: &str,
) -> Result<Option<RawAlterDefaultPrivileges>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    let normalized = normalize_sql(trimmed);
    let Some(mut rest) = normalized.strip_prefix("alter default privileges ") else {
        return Ok(None);
    };
    let schema_name = if let Some(schema_rest) = rest.strip_prefix("in schema ") {
        let (schema_name, after_schema) = parse_leading_sql_identifier(schema_rest)?;
        if !schema_name.chars().all(is_ident_char) {
            return Err(SqlError::InvalidSql(
                "ALTER DEFAULT PRIVILEGES schema must be an SQL identifier".to_string(),
            ));
        }
        rest = after_schema.trim_start();
        schema_name.to_ascii_lowercase()
    } else {
        "*".to_string()
    };
    let (grant, action_rest, role_separator) = if let Some(grant_rest) = rest.strip_prefix("grant ")
    {
        (true, grant_rest, " to ")
    } else if let Some(revoke_rest) = rest.strip_prefix("revoke ") {
        (false, revoke_rest, " from ")
    } else {
        return Err(SqlError::InvalidSql(
            "ALTER DEFAULT PRIVILEGES requires GRANT or REVOKE".to_string(),
        ));
    };
    let Some((privileges, target_and_roles)) = action_rest.split_once(" on ") else {
        return Err(SqlError::InvalidSql(
            "ALTER DEFAULT PRIVILEGES requires ON".to_string(),
        ));
    };
    let Some((target, roles)) = target_and_roles.split_once(role_separator) else {
        return Err(SqlError::InvalidSql(format!(
            "ALTER DEFAULT PRIVILEGES {} requires {}",
            if grant { "GRANT" } else { "REVOKE" },
            if grant { "TO" } else { "FROM" }
        )));
    };
    let (object_type, allowed) = match target.trim() {
        "tables" => (
            PrivilegeObjectType::Table,
            &[
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
            ][..],
        ),
        "sequences" => (
            PrivilegeObjectType::Sequence,
            &["USAGE", "SELECT", "UPDATE"][..],
        ),
        "functions" | "routines" => (PrivilegeObjectType::Function, &["EXECUTE"][..]),
        other => {
            return Err(SqlError::Unsupported(format!(
                "ALTER DEFAULT PRIVILEGES target {other} is not supported"
            )));
        }
    };
    let requested = privileges
        .split(',')
        .map(|privilege| privilege.trim().to_ascii_uppercase())
        .collect::<Vec<_>>();
    if requested.is_empty() || requested.iter().any(String::is_empty) {
        return Err(SqlError::InvalidSql(
            "ALTER DEFAULT PRIVILEGES requires privileges".to_string(),
        ));
    }
    let privileges = if requested.len() == 1 && requested[0] == "ALL" {
        allowed
            .iter()
            .map(|privilege| (*privilege).to_string())
            .collect()
    } else {
        for privilege in &requested {
            if !allowed.contains(&privilege.as_str()) {
                return Err(SqlError::Unsupported(format!(
                    "ALTER DEFAULT PRIVILEGES privilege {privilege} is not supported for {}",
                    target.trim()
                )));
            }
        }
        requested
    };
    let grantees = roles
        .split(',')
        .map(|role| normalize_role_name(role.trim()))
        .collect::<Vec<_>>();
    if grantees.is_empty()
        || grantees
            .iter()
            .any(|role| role.is_empty() || (role != "public" && !role.chars().all(is_ident_char)))
    {
        return Err(SqlError::InvalidSql(
            "ALTER DEFAULT PRIVILEGES requires role names".to_string(),
        ));
    }
    Ok(Some(RawAlterDefaultPrivileges {
        schema_name,
        object_type,
        privileges,
        grantees,
        grant,
    }))
}

pub(crate) fn split_raw_multi_grantee_revoke(sql: &str) -> Result<Option<Vec<String>>> {
    let trimmed = trim_sql_statement(sql).trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("revoke ") {
        return Ok(None);
    }
    let Some(from_index) = lower.rfind(" from ") else {
        return Ok(None);
    };
    let grantees = &trimmed[from_index + " from ".len()..];
    if !grantees.contains(',') {
        return Ok(None);
    }
    let prefix = &trimmed[..from_index];
    let mut statements = Vec::new();
    for grantee in grantees.split(',') {
        let grantee = grantee.trim();
        let normalized = grantee.trim_matches('"');
        if normalized.is_empty()
            || (!normalized.eq_ignore_ascii_case("public")
                && !normalized.chars().all(is_ident_char))
        {
            return Err(SqlError::InvalidSql(
                "REVOKE FROM requires SQL role identifiers".to_string(),
            ));
        }
        statements.push(format!("{prefix} FROM {grantee}"));
    }
    Ok(Some(statements))
}

pub(crate) struct RawFunctionPrivilegeDdl {
    pub(crate) grant: bool,
    pub(crate) with_grant_option: bool,
    pub(crate) routines: Vec<String>,
    pub(crate) grantees: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct RawTypeIdentity {
    pub(crate) schema_name: String,
    pub(crate) name: String,
}

#[derive(Clone, Debug)]
pub(crate) enum RawAlterUserTypeDdl {
    Owner {
        identity: RawTypeIdentity,
        owner: String,
        domain: bool,
    },
    SetSchema {
        identity: RawTypeIdentity,
        schema_name: String,
        domain: bool,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct RawTypePrivilegeDdl {
    pub(crate) grant: bool,
    pub(crate) types: Vec<RawTypeIdentity>,
    pub(crate) grantees: Vec<String>,
}

pub(crate) fn parse_raw_alter_user_type(sql: &str) -> Result<Option<RawAlterUserTypeDdl>> {
    let statement = trim_sql_statement(sql);
    let (domain, rest) = if let Some(rest) = strip_prefix_ci(statement, "ALTER TYPE ") {
        (false, rest)
    } else if let Some(rest) = strip_prefix_ci(statement, "ALTER DOMAIN ") {
        (true, rest)
    } else {
        return Ok(None);
    };
    let (target, rest) = parse_leading_qualified_sql_identifier(rest)?;
    let identity = raw_type_identity(&target)?;
    let rest = rest.trim_start();
    if let Some(rest) = strip_prefix_ci(rest, "OWNER TO ") {
        let (owner, trailing) = parse_leading_qualified_sql_identifier(rest)?;
        if has_executable_sql(trailing) {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE OWNER TO has trailing tokens".to_string(),
            ));
        }
        let owner = sql_identifier_parts(&owner)?;
        let [owner] = owner.as_slice() else {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE OWNER TO requires a role name".to_string(),
            ));
        };
        return Ok(Some(RawAlterUserTypeDdl::Owner {
            identity,
            owner: owner.clone(),
            domain,
        }));
    }
    if let Some(rest) = strip_prefix_ci(rest, "SET SCHEMA ") {
        let (schema, trailing) = parse_leading_qualified_sql_identifier(rest)?;
        if has_executable_sql(trailing) {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE SET SCHEMA has trailing tokens".to_string(),
            ));
        }
        let schema = sql_identifier_parts(&schema)?;
        let [schema_name] = schema.as_slice() else {
            return Err(SqlError::InvalidSql(
                "ALTER TYPE SET SCHEMA requires a schema name".to_string(),
            ));
        };
        return Ok(Some(RawAlterUserTypeDdl::SetSchema {
            identity,
            schema_name: schema_name.clone(),
            domain,
        }));
    }
    Ok(None)
}

pub(crate) fn parse_raw_type_privilege_ddl(sql: &str) -> Result<Option<RawTypePrivilegeDdl>> {
    let statement = trim_sql_statement(sql);
    let (grant, mut rest, role_keyword) =
        if let Some(rest) = strip_prefix_ci(statement, "GRANT USAGE ON TYPE ") {
            (true, rest, "TO")
        } else if let Some(rest) = strip_prefix_ci(statement, "REVOKE USAGE ON TYPE ") {
            (false, rest, "FROM")
        } else {
            return Ok(None);
        };
    let role_index = find_top_level_keyword(rest, role_keyword).ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "{} USAGE ON TYPE requires {role_keyword}",
            if grant { "GRANT" } else { "REVOKE" }
        ))
    })?;
    let mut target_sql = rest[..role_index].trim();
    rest = rest[role_index + role_keyword.len()..].trim();
    let mut types = Vec::new();
    while !target_sql.is_empty() {
        let (target, trailing) = parse_leading_qualified_sql_identifier(target_sql)?;
        types.push(raw_type_identity(&target)?);
        target_sql = trailing.trim_start();
        if target_sql.is_empty() {
            break;
        }
        target_sql = target_sql.strip_prefix(',').ok_or_else(|| {
            SqlError::InvalidSql("TYPE privilege targets must be comma-separated".to_string())
        })?;
        target_sql = target_sql.trim_start();
    }
    let mut grantees = Vec::new();
    while !rest.is_empty() {
        let (grantee, trailing) = parse_leading_qualified_sql_identifier(rest)?;
        let grantee = sql_identifier_parts(&grantee)?;
        let [grantee] = grantee.as_slice() else {
            return Err(SqlError::InvalidSql(
                "TYPE privilege grantees must be role names".to_string(),
            ));
        };
        grantees.push(grantee.clone());
        rest = trailing.trim_start();
        if rest.is_empty() {
            break;
        }
        rest = rest.strip_prefix(',').ok_or_else(|| {
            SqlError::InvalidSql("TYPE privilege grantees must be comma-separated".to_string())
        })?;
        rest = rest.trim_start();
    }
    if types.is_empty() || grantees.is_empty() {
        return Err(SqlError::InvalidSql(
            "TYPE privileges require targets and grantees".to_string(),
        ));
    }
    Ok(Some(RawTypePrivilegeDdl {
        grant,
        types,
        grantees,
    }))
}

fn raw_type_identity(identifier: &str) -> Result<RawTypeIdentity> {
    let parts = sql_identifier_parts(identifier)?;
    let (schema_name, name) = match parts.as_slice() {
        [name] => ("public".to_string(), name.clone()),
        [schema_name, name] => (schema_name.clone(), name.clone()),
        _ => {
            return Err(SqlError::InvalidSql(format!(
                "invalid user-defined type name {identifier}"
            )));
        }
    };
    Ok(RawTypeIdentity { schema_name, name })
}

pub(crate) fn sql_identifier_parts(identifier: &str) -> Result<Vec<String>> {
    let bytes = identifier.as_bytes();
    let mut parts = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            index += 1;
            let mut part = String::new();
            let mut closed = false;
            while index < bytes.len() {
                if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        part.push('"');
                        index += 2;
                    } else {
                        index += 1;
                        closed = true;
                        break;
                    }
                } else {
                    let ch = identifier[index..].chars().next().ok_or_else(|| {
                        SqlError::InvalidSql("invalid quoted identifier".to_string())
                    })?;
                    part.push(ch);
                    index += ch.len_utf8();
                }
            }
            if !closed {
                return Err(SqlError::InvalidSql(
                    "unterminated quoted identifier".to_string(),
                ));
            }
            parts.push(part);
        } else {
            let start = index;
            while index < bytes.len() && bytes[index] != b'.' {
                index += 1;
            }
            let part = identifier[start..index].trim();
            if part.is_empty() {
                return Err(SqlError::InvalidSql("expected identifier".to_string()));
            }
            parts.push(part.to_ascii_lowercase());
        }
        if index == bytes.len() {
            break;
        }
        if bytes[index] != b'.' {
            return Err(SqlError::InvalidSql(format!(
                "invalid qualified identifier {identifier}"
            )));
        }
        index += 1;
        if index == bytes.len() {
            return Err(SqlError::InvalidSql("expected identifier".to_string()));
        }
    }
    Ok(parts)
}

fn strip_sql_keywords<'a>(mut sql: &'a str, keywords: &[&str]) -> Option<&'a str> {
    for keyword in keywords {
        sql = trim_leading_sql_comments_with_offset(sql).0.trim_start();
        if !keyword_matches_at(sql, keyword, 0) {
            return None;
        }
        sql = &sql[keyword.len()..];
    }
    Some(trim_leading_sql_comments_with_offset(sql).0.trim())
}

pub(crate) fn parse_raw_function_privilege_ddl(
    db: &BicDb,
    sql: &str,
) -> Result<Option<RawFunctionPrivilegeDdl>> {
    let trimmed = trim_sql_statement(sql).trim();
    let (grant, rest, role_keyword) = if let Some(rest) = strip_sql_keywords(trimmed, &["GRANT"]) {
        (true, rest, "TO")
    } else if let Some(rest) = strip_sql_keywords(trimmed, &["REVOKE"]) {
        (false, rest, "FROM")
    } else {
        return Ok(None);
    };
    let Some(rest) = strip_sql_keywords(rest, &["EXECUTE", "ON"])
        .or_else(|| strip_sql_keywords(rest, &["ALL", "ON"]))
    else {
        return Ok(None);
    };
    if strip_sql_keywords(rest, &["FUNCTION"]).is_none()
        && strip_sql_keywords(rest, &["ALL", "FUNCTIONS", "IN", "SCHEMA"]).is_none()
    {
        return Ok(None);
    }
    // Find the final unquoted, top-level keyword. Whitespace is not syntax,
    // and quoted routine/role names or signature types must not split a grant.
    let mut role_index = None;
    let mut offset = 0;
    while let Some(relative) = find_top_level_keyword(&rest[offset..], role_keyword) {
        let index = offset + relative;
        role_index = Some(index);
        offset = index + role_keyword.len();
    }
    let role_index = role_index.ok_or_else(|| {
        SqlError::InvalidSql(format!(
            "{} EXECUTE requires {}",
            if grant { "GRANT" } else { "REVOKE" },
            role_keyword
        ))
    })?;
    let target = rest[..role_index].trim();
    let mut tail = rest[role_index + role_keyword.len()..].trim();
    let mut with_grant_option = false;
    if let Some(index) = find_top_level_keyword(tail, "WITH") {
        if !grant
            || !strip_sql_keywords(&tail[index..], &["WITH", "GRANT", "OPTION"])
                .is_some_and(str::is_empty)
        {
            return Err(SqlError::Unsupported(
                "unsupported function grant options".into(),
            ));
        }
        with_grant_option = true;
        tail = tail[..index].trim();
    }
    let grantees = parse_role_membership_identifiers(tail)?;

    let routines =
        if let Some(schema) = strip_sql_keywords(target, &["ALL", "FUNCTIONS", "IN", "SCHEMA"]) {
            let schema = schema.trim().trim_matches('"');
            if schema.is_empty() || !schema.chars().all(is_ident_char) {
                return Err(SqlError::InvalidSql(
                    "ALL FUNCTIONS IN SCHEMA requires an SQL identifier".to_string(),
                ));
            }
            list_routines(db)?
                .into_iter()
                .filter(|routine| routine.kind == RoutineKind::Function)
                .filter(|routine| routine_schema_name(routine).eq_ignore_ascii_case(schema))
                .map(|routine| normalize_object_name(&routine.name))
                .collect()
        } else if let Some(functions) = strip_sql_keywords(target, &["FUNCTION"]) {
            let name_end = functions.find('(').unwrap_or(functions.len());
            let name = normalize_object_name(functions[..name_end].trim());
            if name.is_empty() || !name.chars().all(is_ident_char) {
                return Err(SqlError::InvalidSql(
                    "FUNCTION privilege requires a routine name".to_string(),
                ));
            }
            vec![name]
        } else {
            return Ok(None);
        };
    Ok(Some(RawFunctionPrivilegeDdl {
        grant,
        with_grant_option,
        routines,
        grantees,
    }))
}

/// Extract the dollar-quoted body of a `DO $tag$ ... $tag$` statement, or
/// `None` when the statement is not a DO block at all. Shared by the IF-EXISTS
/// fast path and the generic PLpgSQL interpreter fallback.
pub(crate) fn raw_do_block_body(sql: &str) -> Result<Option<String>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    let Some(rest) = strip_prefix_ci(trimmed, "DO ") else {
        return Ok(None);
    };
    let rest = rest.trim_start();
    let Some(after_open_dollar) = rest.strip_prefix('$') else {
        return Ok(None);
    };
    let Some(tag_end) = after_open_dollar.find('$') else {
        return Err(SqlError::InvalidSql(
            "unterminated DO block delimiter".to_string(),
        ));
    };
    let delimiter = &rest[..tag_end + 2];
    let body_start = delimiter.len();
    let Some(body_end) = rest[body_start..].rfind(delimiter) else {
        return Err(SqlError::InvalidSql(
            "unterminated DO block body".to_string(),
        ));
    };
    Ok(Some(
        rest[body_start..body_start + body_end].trim().to_string(),
    ))
}

pub(crate) fn parse_raw_do_if_exists_ddl(sql: &str) -> Result<Option<DoIfExistsDdl>> {
    let trimmed = trim_sql_statement(sql);
    let (trimmed, _) = trim_leading_sql_comments_with_offset(trimmed);
    let Some(rest) = strip_prefix_ci(trimmed, "DO ") else {
        return Ok(None);
    };
    let rest = rest.trim_start();
    let Some(after_open_dollar) = rest.strip_prefix('$') else {
        return Ok(None);
    };
    let Some(tag_end) = after_open_dollar.find('$') else {
        return Err(SqlError::InvalidSql(
            "unterminated DO block delimiter".to_string(),
        ));
    };
    let delimiter = &rest[..tag_end + 2];
    let body_start = delimiter.len();
    let Some(body_end) = rest[body_start..].rfind(delimiter) else {
        return Err(SqlError::InvalidSql(
            "unterminated DO block body".to_string(),
        ));
    };
    let trailing = rest[body_start + body_end + delimiter.len()..].trim();
    if !trailing.is_empty() {
        return Err(SqlError::Unsupported(format!(
            "DO block trailing clause `{trailing}` is not supported"
        )));
    }
    let body = rest[body_start..body_start + body_end].trim();
    let Some(body) = strip_prefix_ci(body, "BEGIN") else {
        return Err(SqlError::Unsupported(
            "DO block supports only BEGIN ... END blocks".to_string(),
        ));
    };
    let body = body.trim();
    let body = body
        .strip_suffix(';')
        .unwrap_or(body)
        .trim_end()
        .strip_suffix("END")
        .or_else(|| body.strip_suffix("end"))
        .ok_or_else(|| {
            SqlError::Unsupported("DO block supports only BEGIN ... END blocks".to_string())
        })?
        .trim();
    let Some(mut rest) = strip_prefix_ci(body, "IF ") else {
        return Err(SqlError::Unsupported(
            "DO block supports only IF EXISTS/IF NOT EXISTS guards".to_string(),
        ));
    };
    let mut guards = Vec::new();
    let rest_after_then = loop {
        rest = rest.trim_start();
        let negated = if let Some(after_not) = strip_prefix_ci(rest, "NOT ") {
            rest = after_not.trim_start();
            true
        } else {
            false
        };
        let Some(rest_after_exists) = strip_prefix_ci(rest, "EXISTS") else {
            return Err(SqlError::Unsupported(
                "DO block supports only AND-connected IF EXISTS/IF NOT EXISTS guards".to_string(),
            ));
        };
        let rest_after_exists = rest_after_exists.trim_start();
        let Some(condition_body) = rest_after_exists.strip_prefix('(') else {
            return Err(SqlError::InvalidSql(
                "DO block EXISTS guard requires a SELECT subquery".to_string(),
            ));
        };
        let close = find_matching_closing_paren(condition_body).ok_or_else(|| {
            SqlError::InvalidSql("unterminated DO block EXISTS subquery".to_string())
        })?;
        let exists_sql = condition_body[..close].trim().to_string();
        if !exists_sql.to_ascii_lowercase().starts_with("select ") {
            return Err(SqlError::Unsupported(
                "DO block EXISTS guard supports only SELECT subqueries".to_string(),
            ));
        }
        let normalized_exists = normalize_sql(&exists_sql);
        if !(normalized_exists.contains(" from pg_class ")
            || normalized_exists.contains(" from pg_catalog.pg_class ")
            || normalized_exists.contains(" from pg_constraint ")
            || normalized_exists.contains(" from pg_catalog.pg_constraint ")
            || normalized_exists.contains(" from pg_roles ")
            || normalized_exists.contains(" from pg_catalog.pg_roles ")
            || normalized_exists.contains(" from information_schema.columns ")
            || normalized_exists.contains(" from information_schema.tables "))
        {
            return Err(SqlError::Unsupported(
                "DO block EXISTS guard supports only PostgreSQL catalog and information_schema predicates"
                    .to_string(),
            ));
        }
        guards.push(DoExistsGuard {
            negated,
            exists_sql,
        });

        rest = condition_body[close + 1..].trim_start();
        if let Some(after_then) = strip_prefix_ci(rest, "THEN") {
            break after_then;
        }
        let Some(after_and) = strip_prefix_ci(rest, "AND ") else {
            return Err(SqlError::InvalidSql(
                "DO block IF guard requires AND or THEN".to_string(),
            ));
        };
        rest = after_and;
    };
    let ddl = rest_after_then
        .trim()
        .strip_suffix(';')
        .unwrap_or_else(|| rest_after_then.trim())
        .trim();
    let ddl = ddl
        .strip_suffix("END IF")
        .or_else(|| ddl.strip_suffix("end if"))
        .ok_or_else(|| {
            SqlError::Unsupported(
                "DO block supports only one statement followed by END IF".to_string(),
            )
        })?
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    if ddl.is_empty() {
        return Err(SqlError::InvalidSql(
            "DO block THEN statement is empty".to_string(),
        ));
    }
    // This shortcut handles exactly one SQL statement inside one IF. A second
    // IF/ELSE must go through the PL/pgSQL interpreter, not be swallowed into
    // the first guard's body (which can skip independent constraints).
    let statements = parse_statements(&ddl).map_err(|_| {
        SqlError::Unsupported("DO block needs the PL/pgSQL interpreter".to_string())
    })?;
    if statements.len() != 1 {
        return Err(SqlError::Unsupported(
            "DO block needs the PL/pgSQL interpreter".to_string(),
        ));
    }
    Ok(Some(DoIfExistsDdl { guards, ddl }))
}

pub(crate) fn find_matching_closing_paren(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_string {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' if depth == 0 => return Some(idx),
            b')' => depth -= 1,
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn trim_sql_statement(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim()
}

pub(crate) fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))
        .map(|_| &value[prefix.len()..])
}

pub(crate) fn parse_leading_policy_identifier(value: &str) -> Result<(String, &str)> {
    parse_leading_sql_identifier(value)
}

pub(crate) fn parse_leading_sql_identifier(value: &str) -> Result<(String, &str)> {
    let value = value.trim_start();
    if value.is_empty() {
        return Err(SqlError::InvalidSql("expected identifier".to_string()));
    }
    if let Some(rest) = value.strip_prefix('"') {
        let Some(end) = rest.find('"') else {
            return Err(SqlError::InvalidSql(
                "unterminated quoted identifier".to_string(),
            ));
        };
        return Ok((rest[..end].to_string(), &rest[end + 1..]));
    }
    let end = value
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map(|(idx, _)| idx)
        .unwrap_or(value.len());
    Ok((value[..end].to_string(), &value[end..]))
}

pub(crate) fn parse_leading_qualified_sql_identifier(value: &str) -> Result<(String, &str)> {
    let value = value.trim_start();
    if value.is_empty() {
        return Err(SqlError::InvalidSql("expected identifier".to_string()));
    }
    let bytes = value.as_bytes();
    let mut idx = 0;
    loop {
        if bytes.get(idx) == Some(&b'"') {
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
        } else {
            let start = idx;
            while idx < bytes.len() {
                let ch = bytes[idx] as char;
                if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$') {
                    idx += 1;
                } else {
                    break;
                }
            }
            if idx == start {
                return Err(SqlError::InvalidSql("expected identifier".to_string()));
            }
        }
        if bytes.get(idx) == Some(&b'.') {
            idx += 1;
            continue;
        }
        break;
    }
    Ok((value[..idx].to_string(), &value[idx..]))
}

pub(crate) fn identifier_schema_and_name(identifier: &str) -> (String, String) {
    let parts = identifier
        .split('.')
        .map(|part| part.trim_matches('"').to_string())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [schema, table] => (schema.to_string(), table.to_string()),
        [_, schema, table] => (schema.to_string(), table.to_string()),
        [table] => ("public".to_string(), table.to_string()),
        [] => ("public".to_string(), String::new()),
        parts => (
            parts
                .get(parts.len().saturating_sub(2))
                .cloned()
                .unwrap_or_else(|| "public".to_string()),
            parts.last().cloned().unwrap_or_default(),
        ),
    }
}

pub(crate) fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut idx = 0;
    while idx < bytes.len() {
        let byte = bytes[idx];
        if in_string {
            if byte == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                in_string = false;
            }
            idx += 1;
            continue;
        }
        // Dollar-quoted SQL fragments may contain unmatched parentheses,
        // quotes, and keywords. They are one literal token.
        if byte == b'$' {
            if let Some(delimiter) = dollar_quote_delimiter(&sql[idx..]) {
                let start = idx + delimiter.len();
                let end = sql[start..].find(&delimiter)?;
                idx = start + end + delimiter.len();
                continue;
            }
        }
        if byte == b'"' {
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'"' {
                    idx += 1;
                    if bytes.get(idx) != Some(&b'"') {
                        break;
                    }
                }
                idx += 1;
            }
            continue;
        }
        if byte == b'-' && bytes.get(idx + 1) == Some(&b'-') {
            idx += 2;
            while idx < bytes.len() && bytes[idx] != b'\n' {
                idx += 1;
            }
            continue;
        }
        if byte == b'/' && bytes.get(idx + 1) == Some(&b'*') {
            idx += 2;
            let mut comments = 1;
            while idx < bytes.len() && comments > 0 {
                if bytes[idx] == b'/' && bytes.get(idx + 1) == Some(&b'*') {
                    comments += 1;
                    idx += 2;
                } else if bytes[idx] == b'*' && bytes.get(idx + 1) == Some(&b'/') {
                    comments -= 1;
                    idx += 2;
                } else {
                    idx += 1;
                }
            }
            continue;
        }
        match byte {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && keyword_matches_at(sql, keyword, idx) => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn keyword_matches_at(sql: &str, keyword: &str, idx: usize) -> bool {
    let Some(candidate) = sql.get(idx..idx + keyword.len()) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(keyword) {
        return false;
    }
    let bytes = sql.as_bytes();
    let before_ok = idx == 0
        || bytes
            .get(idx.saturating_sub(1))
            .is_none_or(|byte| !is_sql_identifier_byte(*byte));
    let after_ok = bytes
        .get(idx + keyword.len())
        .is_none_or(|byte| !is_sql_identifier_byte(*byte));
    before_ok && after_ok
}

pub(crate) fn spatial_index_predicate(
    selection: &Expr,
    field: &IndexField,
) -> Result<Option<SpatialIndexPredicate>> {
    for term in and_terms(selection) {
        let Expr::Function(function) = term else {
            continue;
        };
        let name = object_name(&function.name)?.to_ascii_lowercase();
        let name = name
            .strip_prefix("public.")
            .or_else(|| name.strip_prefix("pg_catalog."))
            .unwrap_or(&name);
        let args = function_args(function);
        match name {
            "st_dwithin" if args.len() == 3 => {
                if !expr_matches_index_field(&args[0], field)? {
                    continue;
                }
                let point = spatial_point_constant(&args[1], "ST_DWithin")?;
                let meters =
                    spatial_number("ST_DWithin", &eval_constant_expr(&args[2])?, "meters")?;
                if meters < 0.0 {
                    return Err(SqlError::InvalidSql(
                        "ST_DWithin meters must be non-negative".to_string(),
                    ));
                }
                return Ok(Some(SpatialIndexPredicate::DWithin {
                    lon: point.0,
                    lat: point.1,
                    meters,
                }));
            }
            "st_intersects" if args.len() == 2 => {
                let envelope_arg = if expr_matches_index_field(&args[0], field)? {
                    &args[1]
                } else if expr_matches_index_field(&args[1], field)? {
                    &args[0]
                } else {
                    continue;
                };
                let (min_lon, min_lat, max_lon, max_lat) =
                    spatial_envelope_constant(envelope_arg, "ST_Intersects")?;
                return Ok(Some(SpatialIndexPredicate::IntersectsEnvelope {
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                }));
            }
            _ => {}
        }
    }
    Ok(None)
}

pub(crate) fn expr_matches_index_field(expr: &Expr, field: &IndexField) -> Result<bool> {
    let Ok(expr_field) = FieldRef::from_expr(expr) else {
        return Ok(false);
    };
    let index_field = match expr_field {
        FieldRef::Geometry => IndexField::Geometry,
        FieldRef::Column(column)
        | FieldRef::JsonColumn(column)
        | FieldRef::TypedColumn { name: column, .. } => IndexField::MetadataPath(vec![column]),
        FieldRef::MetadataPath(path) => IndexField::MetadataPath(path),
        FieldRef::JsonTextPath(base, path) | FieldRef::JsonPath(base, path) => match *base {
            FieldRef::Metadata
            | FieldRef::Column(_)
            | FieldRef::TypedColumn { .. }
            | FieldRef::JsonColumn(_) => {
                let Some(path) = json_object_path(&path) else {
                    return Ok(false);
                };
                IndexField::MetadataPath(path)
            }
            _ => return Ok(false),
        },
        _ => return Ok(false),
    };
    Ok(index_field_matches(&index_field, field))
}

pub(crate) fn index_field_lists_match(left: &[IndexField], right: &[IndexField]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| index_field_matches(left, right))
}

pub(crate) fn index_field_matches(left: &IndexField, right: &IndexField) -> bool {
    match (left, right) {
        (IndexField::Id, IndexField::Id)
        | (IndexField::Timestamp, IndexField::Timestamp)
        | (IndexField::Geometry, IndexField::Geometry) => true,
        (IndexField::MetadataPath(left), IndexField::MetadataPath(right)) => {
            metadata_paths_match(left, right)
        }
        (IndexField::Lower(left), IndexField::Lower(right))
        | (IndexField::Trim(left), IndexField::Trim(right)) => index_field_matches(left, right),
        _ => false,
    }
}

pub(crate) fn metadata_paths_match(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

pub(crate) fn spatial_point_constant(expr: &Expr, function: &str) -> Result<(f64, f64)> {
    match spatial_geometry(function, &eval_constant_expr(expr)?)? {
        Geometry::Point(point) => Ok((point.x(), point.y())),
        geometry => Err(SqlError::Unsupported(format!(
            "{function} spatial index lookup requires a POINT constant, got {}",
            spatial_type_name(&geometry)
        ))),
    }
}

pub(crate) fn spatial_envelope_constant(
    expr: &Expr,
    function: &str,
) -> Result<(f64, f64, f64, f64)> {
    match spatial_geometry(function, &eval_constant_expr(expr)?)? {
        Geometry::Envelope(rect) => Ok((rect.min().x, rect.min().y, rect.max().x, rect.max().y)),
        geometry => Err(SqlError::Unsupported(format!(
            "{function} spatial index lookup requires an envelope constant, got {}",
            spatial_type_name(&geometry)
        ))),
    }
}

pub(crate) fn equality_value(selection: &Expr, field: &IndexField) -> Result<Option<IndexValue>> {
    for term in and_terms(selection) {
        let Some((term_field, op, value)) = comparison_term(term)? else {
            continue;
        };
        if index_field_matches(&term_field, field) && op == BinaryOperator::Eq {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

pub(crate) fn range_bounds(
    selection: &Expr,
    field: &IndexField,
) -> Result<Option<(Option<IndexValue>, Option<IndexValue>)>> {
    let mut lower = None;
    let mut upper = None;
    let mut matched = false;
    for term in and_terms(selection) {
        let Some((term_field, op, value)) = comparison_term(term)? else {
            continue;
        };
        if !index_field_matches(&term_field, field) {
            continue;
        }
        match op {
            BinaryOperator::Gt | BinaryOperator::GtEq => {
                lower = Some(value);
                matched = true;
            }
            BinaryOperator::Lt | BinaryOperator::LtEq => {
                upper = Some(value);
                matched = true;
            }
            _ => {}
        }
    }
    Ok(matched.then_some((lower, upper)))
}

pub(crate) fn update_dynamic_range_bound(
    op: BinaryOperator,
    value: SqlValue,
    lower: &mut Option<IndexValue>,
    upper: &mut Option<IndexValue>,
    has_null_bound: &mut bool,
) -> Result<()> {
    if matches!(value, SqlValue::Null) {
        *has_null_bound = true;
        return Ok(());
    }
    let value = index_value_from_sql(value)?;
    match op {
        BinaryOperator::Gt | BinaryOperator::GtEq => {
            if lower.as_ref().is_none_or(|existing| &value > existing) {
                *lower = Some(value);
            }
        }
        BinaryOperator::Lt | BinaryOperator::LtEq => {
            if upper.as_ref().is_none_or(|existing| &value < existing) {
                *upper = Some(value);
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn update_dynamic_sql_range_bound(
    op: BinaryOperator,
    value: SqlValue,
    lower: &mut Option<SqlValue>,
    upper: &mut Option<SqlValue>,
    has_null_bound: &mut bool,
) {
    if matches!(value, SqlValue::Null) {
        *has_null_bound = true;
        return;
    }
    match op {
        BinaryOperator::Gt | BinaryOperator::GtEq => {
            if lower.as_ref().is_none_or(|existing| {
                value_ordering(&value, existing)
                    .is_some_and(|ordering| ordering == Ordering::Greater)
            }) {
                *lower = Some(value);
            }
        }
        BinaryOperator::Lt | BinaryOperator::LtEq => {
            if upper.as_ref().is_none_or(|existing| {
                value_ordering(&value, existing).is_some_and(|ordering| ordering == Ordering::Less)
            }) {
                *upper = Some(value);
            }
        }
        _ => {}
    }
}

pub(crate) fn and_terms(expr: &Expr) -> Vec<&Expr> {
    let mut terms = Vec::new();
    collect_and_terms(expr, &mut terms);
    terms
}

pub(crate) fn collect_and_terms<'a>(expr: &'a Expr, terms: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_and_terms(left, terms);
            collect_and_terms(right, terms);
        }
        Expr::Nested(expr) => collect_and_terms(expr, terms),
        other => terms.push(other),
    }
}

pub(crate) fn predicate_references_only_columns(expr: &Expr, columns: &BTreeSet<String>) -> bool {
    let mut references = Vec::new();
    if !collect_predicate_column_references(expr, &mut references) || references.is_empty() {
        return false;
    }
    references
        .iter()
        .all(|reference| column_reference_matches(reference, columns))
}

pub(crate) fn collect_predicate_column_references(
    expr: &Expr,
    references: &mut Vec<Vec<String>>,
) -> bool {
    match expr {
        Expr::Identifier(ident) => {
            references.push(vec![ident.value.clone()]);
            true
        }
        Expr::CompoundIdentifier(idents) => {
            references.push(idents.iter().map(|ident| ident.value.clone()).collect());
            true
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_predicate_column_references(left, references)
                && collect_predicate_column_references(right, references)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => collect_predicate_column_references(expr, references),
        Expr::Function(_) => false,
        Expr::Like {
            expr,
            pattern,
            escape_char: _,
            ..
        }
        | Expr::ILike {
            expr,
            pattern,
            escape_char: _,
            ..
        } => {
            collect_predicate_column_references(expr, references)
                && collect_predicate_column_references(pattern, references)
        }
        Expr::InList { expr, list, .. } => {
            collect_predicate_column_references(expr, references)
                && list
                    .iter()
                    .all(|item| collect_predicate_column_references(item, references))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_predicate_column_references(expr, references)
                && collect_predicate_column_references(low, references)
                && collect_predicate_column_references(high, references)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            collect_predicate_column_references(left, references)
                && collect_predicate_column_references(right, references)
        }
        Expr::JsonAccess { value, .. } => collect_predicate_column_references(value, references),
        Expr::Position { expr, r#in } => {
            collect_predicate_column_references(expr, references)
                && collect_predicate_column_references(r#in, references)
        }
        Expr::Extract { expr, .. } => collect_predicate_column_references(expr, references),
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            collect_predicate_column_references(expr, references)
                && trim_what
                    .as_ref()
                    .is_none_or(|expr| collect_predicate_column_references(expr, references))
                && trim_characters.as_ref().is_none_or(|exprs| {
                    exprs
                        .iter()
                        .all(|expr| collect_predicate_column_references(expr, references))
                })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_none_or(|expr| collect_predicate_column_references(expr, references))
                && conditions.iter().all(|condition| {
                    collect_predicate_column_references(&condition.condition, references)
                        && collect_predicate_column_references(&condition.result, references)
                })
                && else_result
                    .as_ref()
                    .is_none_or(|expr| collect_predicate_column_references(expr, references))
        }
        Expr::Array(array) => array
            .elem
            .iter()
            .all(|expr| collect_predicate_column_references(expr, references)),
        Expr::Value(_) | Expr::TypedString(_) => true,
        _ => false,
    }
}

pub(crate) fn column_reference_matches(reference: &[String], columns: &BTreeSet<String>) -> bool {
    if reference.len() < 2 {
        return false;
    }
    let qualified = format!(
        "{}.{}",
        reference[reference.len() - 2],
        reference[reference.len() - 1]
    );
    columns
        .iter()
        .any(|column| column.eq_ignore_ascii_case(&qualified))
}

pub(crate) fn comparison_term(
    expr: &Expr,
) -> Result<Option<(IndexField, BinaryOperator, IndexValue)>> {
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };
    if !matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
    ) {
        return Ok(None);
    }
    if let Ok(field) = index_field_from_expr(left) {
        return Ok(Some((
            field,
            op.clone(),
            index_value_from_sql(eval_constant_expr(right)?)?,
        )));
    }
    if let Ok(field) = index_field_from_expr(right) {
        return Ok(Some((
            field,
            reverse_comparison(op),
            index_value_from_sql(eval_constant_expr(left)?)?,
        )));
    }
    Ok(None)
}

pub(crate) fn reverse_comparison(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other.clone(),
    }
}

pub(crate) fn reverse_range_statistics_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    Some(match op {
        BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::PGOverlap => op.clone(),
        BinaryOperator::AtArrow => BinaryOperator::ArrowAt,
        BinaryOperator::ArrowAt => BinaryOperator::AtArrow,
        BinaryOperator::PGBitwiseShiftLeft => BinaryOperator::PGBitwiseShiftRight,
        BinaryOperator::PGBitwiseShiftRight => BinaryOperator::PGBitwiseShiftLeft,
        BinaryOperator::AndLt => BinaryOperator::AndGt,
        BinaryOperator::AndGt => BinaryOperator::AndLt,
        BinaryOperator::Custom(operator) if operator == "-|-" => op.clone(),
        _ => return None,
    })
}

pub(crate) fn reverse_network_statistics_operator(op: &BinaryOperator) -> Option<BinaryOperator> {
    Some(match op {
        BinaryOperator::Eq | BinaryOperator::NotEq | BinaryOperator::PGOverlap => op.clone(),
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::PGBitwiseShiftLeft => BinaryOperator::PGBitwiseShiftRight,
        BinaryOperator::PGBitwiseShiftRight => BinaryOperator::PGBitwiseShiftLeft,
        BinaryOperator::Custom(operator) if operator == "<<=" => {
            BinaryOperator::Custom(">>=".to_string())
        }
        BinaryOperator::Custom(operator) if operator == ">>=" => {
            BinaryOperator::Custom("<<=".to_string())
        }
        _ => return None,
    })
}

pub(crate) fn network_default_selectivity(op: &BinaryOperator) -> Option<f64> {
    match op.to_string().as_str() {
        "&&" => Some(0.01),
        "<<" | "<<=" | ">>" | ">>=" => Some(0.005),
        _ => None,
    }
}

/// Match the `WHERE <equality prefix> ORDER BY <next index field> LIMIT k` shape
/// to an index, returning `(index_name, prefix, descending, limit)` for a bounded
/// ordered scan. Shared by the plan-path candidate and the row-path fast path.
///
/// Gated for correctness: exactly one ORDER BY term (a secondary key would break
/// ties a single-field bounded scan can't see); a concrete LIMIT (no OFFSET); and
/// the WHERE must be PURELY a conjunction of `column = literal` equalities that
/// form a contiguous prefix of the index, with the ORDER BY field the very next
/// index field. The "purely equalities forming the whole prefix" rule guarantees
/// there is no residual filter that could drop a top-`limit` row.
pub(crate) fn match_prefix_ordered_index(
    selection: &Expr,
    query: &Query,
    indexes: &[IndexDefinition],
) -> Result<Option<(String, Vec<IndexValue>, bool, usize)>> {
    let order_terms = match query.order_by.as_ref().map(|order_by| &order_by.kind) {
        Some(OrderByKind::Expressions(expressions)) => expressions.len(),
        _ => 0,
    };
    if order_terms != 1 {
        return Ok(None);
    }
    // This is an optional optimization: anything that isn't a clean, indexable
    // shape — a non-column ORDER BY (e.g. a CASE expr), a non-literal LIMIT, a
    // non-equality WHERE term — must fall back (Ok(None)), never error, since the
    // row path handles those queries correctly on its own.
    let Some((order_field, descending)) = first_order_field(query).ok().flatten() else {
        return Ok(None);
    };
    let Some(limit) = limit_for_plan(query).ok().flatten() else {
        return Ok(None);
    };
    let mut equalities = Vec::new();
    for term in and_terms(selection) {
        match comparison_term(term) {
            Ok(Some((field, BinaryOperator::Eq, value))) => equalities.push((field, value)),
            _ => return Ok(None),
        }
    }
    if equalities.is_empty() {
        return Ok(None);
    }
    for index in indexes {
        let mut prefix = Vec::with_capacity(equalities.len());
        for field in &index.fields {
            let Some((_, value)) = equalities
                .iter()
                .find(|(equality_field, _)| index_field_matches(equality_field, field))
            else {
                break;
            };
            prefix.push(value.clone());
        }
        let k = prefix.len();
        if k != equalities.len() || k >= index.fields.len() {
            continue;
        }
        if !index_field_matches(&index.fields[k], &order_field) {
            continue;
        }
        return Ok(Some((index.name.clone(), prefix, descending, limit)));
    }
    Ok(None)
}

pub(crate) fn first_order_field(query: &Query) -> Result<Option<(IndexField, bool)>> {
    let Some(order_by) = &query.order_by else {
        return Ok(None);
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Ok(None);
    };
    let [order] = expressions.as_slice() else {
        // The bounded index path only guarantees one ordering key. Fall back to
        // the materialized multi-key sorter when secondary keys are present.
        return Ok(None);
    };
    if VectorOrder::from_expr(&order.expr)?.is_some() {
        return Ok(None);
    }
    let Ok(field) = index_field_from_expr(&order.expr) else {
        return Ok(None);
    };
    Ok(Some((field, order.options.asc == Some(false))))
}

pub(crate) fn ann_vector_order(query: &Query) -> Result<Option<(VectorOrder, usize)>> {
    let Some(order_by) = &query.order_by else {
        return Ok(None);
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Ok(None);
    };
    let [order] = expressions.as_slice() else {
        // ANN LIMIT pushdown cannot preserve a secondary ordering key.
        return Ok(None);
    };
    if order.options.asc == Some(false) {
        return Ok(None);
    }
    let Some(vector_order) = VectorOrder::from_expr(&order.expr)? else {
        return Ok(None);
    };
    let Some(limit) = limit_for_plan(query)? else {
        return Ok(None);
    };
    Ok(Some((vector_order, limit)))
}

pub(crate) fn limit_for_plan(query: &Query) -> Result<Option<usize>> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(None);
    };
    match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            ..
        } => Ok(optional_count_expr(limit)?),
        LimitClause::OffsetCommaLimit { .. }
        | LimitClause::LimitOffset {
            offset: Some(_), ..
        } => Ok(None),
        LimitClause::LimitOffset { limit: None, .. } => Ok(None),
    }
}

pub(crate) fn index_value_from_sql(value: SqlValue) -> Result<IndexValue> {
    Ok(match value {
        SqlValue::Null => IndexValue::Null,
        SqlValue::Bool(value) => IndexValue::Bool(value),
        SqlValue::Int(value) => IndexValue::Int(value),
        SqlValue::Float(value) => IndexValue::Float(ordered_f64(value)),
        SqlValue::String(value) => IndexValue::String(value),
        SqlValue::TsQuery(value) => IndexValue::String(
            serde_json::to_string(&value).expect("tsquery index value is serializable"),
        ),
        SqlValue::JsonText(value) => IndexValue::String(value.raw().to_string()),
        SqlValue::Json(JsonValue::Null) => IndexValue::Null,
        SqlValue::Json(JsonValue::Bool(value)) => IndexValue::Bool(value),
        SqlValue::Json(JsonValue::Number(value)) => value
            .as_i64()
            .map(IndexValue::Int)
            .or_else(|| {
                value
                    .as_f64()
                    .map(|value| IndexValue::Float(ordered_f64(value)))
            })
            .unwrap_or(IndexValue::Null),
        SqlValue::Json(JsonValue::String(value)) => IndexValue::String(value),
        SqlValue::Json(value @ (JsonValue::Array(_) | JsonValue::Object(_))) => {
            IndexValue::String(value.to_string())
        }
        SqlValue::Geometry(value) => IndexValue::String(value.to_wkt()),
        SqlValue::Composite(value) => {
            return Err(SqlError::Unsupported(format!(
                "composite type {} requires a typed composite index key",
                value.type_name
            )));
        }
    })
}

pub(crate) fn sql_value_from_index_value(value: &IndexValue) -> SqlValue {
    match value {
        IndexValue::Null => SqlValue::Null,
        IndexValue::Bool(value) => SqlValue::Bool(*value),
        IndexValue::Int(value) => SqlValue::Int(*value),
        IndexValue::Float(value) => SqlValue::Float(f64::from_bits(decode_ordered_f64(*value))),
        IndexValue::String(value) => SqlValue::String(value.clone()),
    }
}

pub(crate) fn decode_ordered_f64(value: u64) -> u64 {
    if value & (1 << 63) == 0 {
        !value
    } else {
        value ^ (1 << 63)
    }
}

pub(crate) fn index_value_label(value: &IndexValue) -> String {
    match value {
        IndexValue::Null => "NULL".to_string(),
        IndexValue::Bool(value) => value.to_string(),
        IndexValue::Int(value) => value.to_string(),
        IndexValue::Float(value) => format!("float:{value}"),
        IndexValue::String(value) => format!("'{value}'"),
    }
}

pub(crate) fn estimate_index_lookup_rows(
    index: &IndexDefinition,
    prefix: &[IndexValue],
    stats: Option<&TableStatistics>,
) -> Option<usize> {
    let stats = stats?;
    if prefix.is_empty() {
        return Some(stats.row_count);
    }
    if prefix.len() == 1 {
        let column = table_column_stats(stats, &index.fields[0])?;
        if let Some(common) = column
            .most_common
            .iter()
            .find(|common| common.value == prefix[0])
        {
            return Some(common.count);
        }
        let distinct = column.distinct_count.max(1);
        return Some(
            stats
                .row_count
                .saturating_sub(column.null_count)
                .saturating_div(distinct)
                .max(1),
        );
    }

    let mut estimate = stats.row_count.max(1);
    for (field, value) in index.fields.iter().zip(prefix) {
        let Some(column) = table_column_stats(stats, field) else {
            continue;
        };
        let field_estimate = column
            .most_common
            .iter()
            .find(|common| common.value == *value)
            .map(|common| common.count)
            .unwrap_or_else(|| {
                stats
                    .row_count
                    .saturating_sub(column.null_count)
                    .saturating_div(column.distinct_count.max(1))
                    .max(1)
            });
        estimate = estimate.min(field_estimate);
    }

    if let Some(index_stats) = stats.indexes.get(&index.name) {
        estimate = estimate.min(
            index_stats
                .indexed_rows
                .saturating_div(index_stats.distinct_keys.max(1))
                .max(1),
        );
    }
    Some(estimate)
}

pub(crate) fn estimate_index_range_rows(
    field: &IndexField,
    lower: Option<&IndexValue>,
    upper: Option<&IndexValue>,
    stats: Option<&TableStatistics>,
) -> Option<usize> {
    let stats = stats?;
    let column = table_column_stats(stats, field)?;
    let non_null = stats.row_count.saturating_sub(column.null_count);
    if non_null == 0 {
        return Some(0);
    }
    if let Some(typed) = column
        .typed
        .as_ref()
        .filter(|typed| !typed.histogram_keys.is_empty())
    {
        let lower = lower.and_then(|value| match value {
            IndexValue::String(value) if value.starts_with("\0bicdb:typed:") => {
                Some(value.as_str())
            }
            _ => None,
        });
        let upper = upper.and_then(|value| match value {
            IndexValue::String(value) if value.starts_with("\0bicdb:typed:") => {
                Some(value.as_str())
            }
            _ => None,
        });
        if lower.is_some() || upper.is_some() {
            let first = lower.map_or(0, |lower| {
                typed
                    .histogram_keys
                    .partition_point(|value| value.as_str() < lower)
            });
            let last = upper.map_or(typed.histogram_keys.len(), |upper| {
                typed
                    .histogram_keys
                    .partition_point(|value| value.as_str() <= upper)
            });
            let sampled = last.saturating_sub(first);
            if sampled == 0 {
                return Some(0);
            }
            return Some(
                sampled
                    .saturating_mul(non_null)
                    .saturating_add(typed.histogram_keys.len() - 1)
                    .saturating_div(typed.histogram_keys.len())
                    .max(1),
            );
        }
    }
    let covers_min = match (lower, column.min.as_ref()) {
        (Some(lower), Some(min)) => lower <= min,
        (Some(_), None) => false,
        (None, _) => true,
    };
    let covers_max = match (upper, column.max.as_ref()) {
        (Some(upper), Some(max)) => upper >= max,
        (Some(_), None) => false,
        (None, _) => true,
    };
    if covers_min && covers_max {
        return Some(non_null);
    }
    Some(match (lower, upper) {
        (Some(_), Some(_)) => (non_null / 3).max(1),
        (Some(_), None) | (None, Some(_)) => (non_null / 2).max(1),
        (None, None) => non_null,
    })
}

pub(crate) fn table_column_stats<'a>(
    stats: &'a TableStatistics,
    field: &IndexField,
) -> Option<&'a bicdb_core::ColumnStatistics> {
    stats
        .columns
        .values()
        .find(|column| index_field_matches(&column.field, field))
}

pub(crate) fn ordered_f64(value: f64) -> u64 {
    let value = if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    };
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

pub(crate) fn eval_predicate_truth(record: &Record, expr: &Expr) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(sql_and(
                eval_predicate_truth(record, left)?,
                eval_predicate_truth(record, right)?,
            )),
            BinaryOperator::Or => Ok(sql_or(
                eval_predicate_truth(record, left)?,
                eval_predicate_truth(record, right)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let mut eval = |expr: &Expr| eval_value(record, expr);
                if let Some(truth) = eval_tuple_comparison(left, op, right, &mut eval)? {
                    return Ok(truth);
                }
                let left = eval_value(record, left)?;
                let right = eval_value(record, right)?;
                compare_values(&left, op, &right)
            }
            BinaryOperator::PGLikeMatch
            | BinaryOperator::PGILikeMatch
            | BinaryOperator::PGNotLikeMatch
            | BinaryOperator::PGNotILikeMatch
            | BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch => {
                let left = eval_value(record, left)?;
                let right = eval_value(record, right)?;
                eval_pg_pattern_operator(&left, op, &right)
            }
            BinaryOperator::AtArrow | BinaryOperator::ArrowAt => {
                let left = eval_value(record, left)?;
                let right = eval_value(record, right)?;
                eval_containment_truth(left, op, right)
            }
            _ => Err(SqlError::Unsupported(format!(
                "unsupported WHERE operator {op}"
            ))),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut eval = |expr: &Expr| eval_value(record, expr);
            if let Some(truth) = eval_tuple_in_list_truth(expr, list, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_in_list_truth(
                eval_value(record, expr)?,
                list.iter()
                    .map(|candidate| eval_value(record, candidate))
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
            let mut eval = |expr: &Expr| eval_value(record, expr);
            if let Some(truth) = eval_tuple_between_truth(expr, low, high, *negated, &mut eval)? {
                return Ok(truth);
            }
            eval_between_truth(
                eval_value(record, expr)?,
                eval_value(record, low)?,
                eval_value(record, high)?,
                *negated,
            )
        }
        Expr::AnyOp {
            left,
            compare_op,
            right,
            ..
        } => eval_quantified_truth(
            eval_value(record, left)?,
            compare_op,
            eval_value(record, right)?,
            false,
        ),
        Expr::AllOp {
            left,
            compare_op,
            right,
        } => eval_quantified_truth(
            eval_value(record, left)?,
            compare_op,
            eval_value(record, right)?,
            true,
        ),
        Expr::IsNull(expr) => Ok(Some(value_is_null_predicate(&eval_value(record, expr)?))),
        Expr::IsNotNull(expr) => Ok(Some(value_is_not_null_predicate(&eval_value(
            record, expr,
        )?))),
        Expr::IsDistinctFrom(left, right) => {
            let mut eval = |expr: &Expr| eval_value(record, expr);
            if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                return Ok(Some(!not_distinct));
            }
            Ok(Some(!values_not_distinct(
                &eval_value(record, left)?,
                &eval_value(record, right)?,
            )))
        }
        Expr::IsNotDistinctFrom(left, right) => {
            let mut eval = |expr: &Expr| eval_value(record, expr);
            if let Some(not_distinct) = eval_tuple_not_distinct(left, right, &mut eval)? {
                return Ok(Some(not_distinct));
            }
            Ok(Some(values_not_distinct(
                &eval_value(record, left)?,
                &eval_value(record, right)?,
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
                eval_value(record, expr)?,
                eval_value(record, pattern)?,
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
                eval_value(record, expr)?,
                eval_value(record, pattern)?,
                *negated,
                true,
                escape_char.as_ref(),
            )
        }
        Expr::SimilarTo { .. } => Err(SqlError::Unsupported(
            "SIMILAR TO is not supported".to_string(),
        )),
        Expr::IsTrue(expr) => Ok(Some(matches!(
            eval_predicate_truth(record, expr)?,
            Some(true)
        ))),
        Expr::IsNotTrue(expr) => Ok(Some(!matches!(
            eval_predicate_truth(record, expr)?,
            Some(true)
        ))),
        Expr::IsFalse(expr) => Ok(Some(matches!(
            eval_predicate_truth(record, expr)?,
            Some(false)
        ))),
        Expr::IsNotFalse(expr) => Ok(Some(!matches!(
            eval_predicate_truth(record, expr)?,
            Some(false)
        ))),
        Expr::IsUnknown(expr) => Ok(Some(eval_predicate_truth(record, expr)?.is_none())),
        Expr::IsNotUnknown(expr) => Ok(Some(eval_predicate_truth(record, expr)?.is_some())),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Case { .. } => {
            match eval_value(record, expr)? {
                SqlValue::Bool(value) => Ok(Some(value)),
                SqlValue::Null => Ok(None),
                other => Err(SqlError::Unsupported(format!(
                    "WHERE expression {expr} returned non-boolean {}",
                    other.to_cell()
                ))),
            }
        }
        Expr::Function(function) => match eval_value(record, expr)? {
            SqlValue::Bool(value) => Ok(Some(value)),
            SqlValue::Null => Ok(None),
            other => Err(SqlError::Unsupported(format!(
                "WHERE function {} returned non-boolean {}",
                function.name,
                other.to_cell()
            ))),
        },
        Expr::UnaryOp { op, expr } if op.to_string().eq_ignore_ascii_case("NOT") => {
            Ok(sql_not(eval_predicate_truth(record, expr)?))
        }
        Expr::Nested(expr) => eval_predicate_truth(record, expr),
        other => Err(SqlError::Unsupported(format!(
            "unsupported WHERE expression {other}"
        ))),
    }
}
