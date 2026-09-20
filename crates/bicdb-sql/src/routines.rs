//! PL/pgSQL routine interpreter support: routine expression binding, routine body parsing (IF/LOOP/cursor block matching), RoutineFrame variable handling, routine persistence, plus sequence load/save and notification helpers.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use routines::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) fn routine_record_value(columns: &[String], row: &[SqlValue]) -> SqlValue {
    let values = columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| (column.clone(), sql_value_to_json(value.clone())))
        .collect::<JsonMap<String, JsonValue>>();
    SqlValue::Json(JsonValue::Object(values))
}

pub(crate) fn routine_exception_matches(handler: &RoutineExceptionHandler, sqlstate: &str) -> bool {
    handler.conditions.iter().any(|condition| match condition {
        RoutineExceptionCondition::Others => true,
        RoutineExceptionCondition::SqlState(state) => state.eq_ignore_ascii_case(sqlstate),
    })
}

pub(crate) fn routine_language_is_plpgsql(routine: &RoutineSchema) -> bool {
    routine
        .language
        .trim()
        .trim_matches('\'')
        .eq_ignore_ascii_case("plpgsql")
}

pub(crate) fn compile_routine_ir(routine: &RoutineSchema) -> Result<RoutineIR> {
    if routine
        .language
        .trim()
        .trim_matches('\'')
        .eq_ignore_ascii_case("sql")
    {
        let params = parse_routine_params(&routine.args)?;
        let body = plpgsql_body_from_definition(&routine.definition)?;
        let statements = parse_statements(&body)?
            .into_iter()
            .map(RoutineStmt::Sql)
            .collect::<Vec<_>>();
        let symbol_names = routine_symbol_names(&params, &[], &statements, &[]);
        return Ok(RoutineIR {
            params,
            symbol_names,
            declarations: Vec::new(),
            statements,
            exception_handlers: Vec::new(),
        });
    }
    if !routine_language_is_plpgsql(routine) {
        return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
            "stored routine {} uses unsupported language {}",
            routine.name, routine.language
        )));
    }
    let mut params = parse_routine_params(&routine.args)?;
    if let Some(index) = find_top_level_keyword(&routine.definition, "RETURNS") {
        let declaration = routine.definition[index + 7..].trim_start();
        if strip_prefix_ci(declaration, "TABLE").is_some() {
            for output in raw_parenthesized_args(declaration) {
                params.push(parse_routine_param(&format!("OUT {output}"), params.len())?);
            }
        }
    }
    let body = plpgsql_body_from_definition(&routine.definition)?;
    let (mut declarations, mut statements, mut exception_handlers) = parse_plpgsql_body(&body)?;
    let symbol_names =
        routine_symbol_names(&params, &declarations, &statements, &exception_handlers);
    bind_routine_expressions(
        &symbol_names,
        &mut declarations,
        &mut statements,
        &mut exception_handlers,
    );
    Ok(RoutineIR {
        params,
        symbol_names,
        declarations,
        statements,
        exception_handlers,
    })
}

pub(crate) fn compile_cached_routine_ir(routine: &RoutineSchema) -> Result<Arc<RoutineIR>> {
    let key = routine_ir_cache_key(routine);
    if let Some(cached) = ROUTINE_IR_CACHE.with(|cache| cache.borrow().get(&key).cloned()) {
        return Ok(cached);
    }
    let ir = Arc::new(compile_routine_ir(routine)?);
    ROUTINE_IR_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, ir.clone());
    });
    Ok(ir)
}

pub(crate) fn routine_ir_cache_key(routine: &RoutineSchema) -> RoutineIrCacheKey {
    RoutineIrCacheKey {
        kind: routine.kind,
        name: routine.name.to_ascii_lowercase(),
        args_hash: hash_routine_args(&routine.args),
        args_len: routine.args.len(),
        return_type: routine.return_type.to_ascii_lowercase(),
        returns_set: routine.returns_set,
        language: routine.language.to_ascii_lowercase(),
        definition_hash: sql_profile_hash(&routine.definition),
        definition_len: routine.definition.len(),
    }
}

pub(crate) fn routine_symbol_names(
    params: &[RoutineParam],
    declarations: &[RoutineDecl],
    statements: &[RoutineStmt],
    handlers: &[RoutineExceptionHandler],
) -> Vec<String> {
    let mut names = routine_symbol_names_for_params(params);
    let mut seen = names.iter().cloned().collect::<BTreeSet<_>>();
    for declaration in declarations {
        collect_routine_decl_symbols(declaration, &mut names, &mut seen);
    }
    collect_routine_stmt_symbols(statements, &mut names, &mut seen);
    for handler in handlers {
        collect_routine_stmt_symbols(&handler.statements, &mut names, &mut seen);
    }
    names
}

pub(crate) fn routine_symbol_names_for_params(params: &[RoutineParam]) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = BTreeSet::new();
    for param in params {
        add_routine_symbol(&format!("${}", param.index + 1), &mut names, &mut seen);
        if let Some(name) = &param.name {
            add_routine_symbol(name, &mut names, &mut seen);
        }
    }
    names
}

pub(crate) fn collect_routine_decl_symbols(
    declaration: &RoutineDecl,
    names: &mut Vec<String>,
    seen: &mut BTreeSet<String>,
) {
    match declaration {
        RoutineDecl::Alias { name, .. } | RoutineDecl::Variable { name, .. } => {
            add_routine_symbol(name, names, seen);
        }
        RoutineDecl::Cursor { .. } => {}
    }
}

pub(crate) fn collect_routine_stmt_symbols(
    statements: &[RoutineStmt],
    names: &mut Vec<String>,
    seen: &mut BTreeSet<String>,
) {
    for statement in statements {
        match statement {
            RoutineStmt::Assignment { target, .. } => {
                collect_routine_assignment_target_symbol(target, names, seen);
            }
            RoutineStmt::SelectInto { targets, .. } | RoutineStmt::SqlInto { targets, .. } => {
                for target in targets {
                    add_routine_symbol(target, names, seen);
                }
            }
            RoutineStmt::Perform { .. } => {}
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                collect_routine_stmt_symbols(then_body, names, seen);
                collect_routine_stmt_symbols(else_body, names, seen);
            }
            RoutineStmt::ForLoop { iterator, body, .. } => {
                add_routine_symbol(iterator, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::ForeachLoop { target, body, .. } => {
                add_routine_symbol(target, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::QueryForLoop { target, body, .. } => {
                add_routine_symbol(target, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::FetchCursor { targets, .. } => {
                for target in targets {
                    add_routine_symbol(target, names, seen);
                }
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::Null
            | RoutineStmt::Sql(_)
            | RoutineStmt::DynamicExecute(_)
            | RoutineStmt::OpenCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::RaiseException { .. }
            | RoutineStmt::ReturnQuery(_)
            | RoutineStmt::Return(_) => {}
        }
    }
}

pub(crate) fn collect_routine_assignment_target_symbol(
    target: &RoutineAssignmentTarget,
    names: &mut Vec<String>,
    seen: &mut BTreeSet<String>,
) {
    match target {
        RoutineAssignmentTarget::Variable(name) => add_routine_symbol(name, names, seen),
        RoutineAssignmentTarget::ArrayElement { array_name, .. } => {
            add_routine_symbol(array_name, names, seen);
        }
    }
}

pub(crate) fn add_routine_symbol(name: &str, names: &mut Vec<String>, seen: &mut BTreeSet<String>) {
    let normalized = normalize_object_name(name);
    if !normalized.is_empty() && seen.insert(normalized.clone()) {
        names.push(normalized);
    }
}

pub(crate) fn bind_routine_expressions(
    symbol_names: &[String],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) {
    let array_vars = declarations
        .iter()
        .filter_map(|declaration| match declaration {
            RoutineDecl::Variable {
                name,
                pg_type: Some(pg_type),
                ..
            } if pg_type.ends_with("[]") => Some(name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let scope = BoundExprScope::new(&[], symbol_names).with_array_vars(&array_vars);
    for declaration in declarations {
        if let RoutineDecl::Variable {
            default_expr: Some(expr),
            ..
        } = declaration
        {
            expr.bind(&scope);
        }
    }
    bind_routine_statement_exprs(statements, &scope);
    for handler in handlers {
        bind_routine_statement_exprs(&mut handler.statements, &scope);
    }
}

pub(crate) fn bind_routine_statement_exprs(statements: &mut [RoutineStmt], scope: &BoundExprScope) {
    for statement in statements {
        match statement {
            RoutineStmt::Assignment { target, expr } => {
                bind_routine_assignment_target_exprs(target, scope);
                expr.bind(scope);
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                condition.bind(scope);
                bind_routine_statement_exprs(then_body, scope);
                bind_routine_statement_exprs(else_body, scope);
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                lower.bind(scope);
                upper.bind(scope);
                bind_routine_statement_exprs(body, scope);
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                array.bind(scope);
                bind_routine_statement_exprs(body, scope);
            }
            RoutineStmt::QueryForLoop { body, .. } => {
                bind_routine_statement_exprs(body, scope);
            }
            RoutineStmt::Return(Some(expr)) => expr.bind(scope),
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                if let Some(detail) = detail {
                    detail.bind(scope);
                }
                for argument in arguments {
                    argument.bind(scope);
                }
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::Null
            | RoutineStmt::SelectInto { .. }
            | RoutineStmt::Perform { .. }
            | RoutineStmt::Sql(_)
            | RoutineStmt::SqlInto { .. }
            | RoutineStmt::OpenCursor { .. }
            | RoutineStmt::FetchCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::ReturnQuery(_)
            | RoutineStmt::Return(None) => {}
            // Dynamic SQL is evaluated against the materialized routine frame at
            // execution time; binding it as a row expression loses routine vars.
            RoutineStmt::DynamicExecute(_) => {}
        }
    }
}

pub(crate) fn bind_routine_assignment_target_exprs(
    target: &mut RoutineAssignmentTarget,
    scope: &BoundExprScope,
) {
    if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
        index.bind(scope);
    }
}

pub(crate) fn hash_routine_args(args: &[String]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    args.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn parse_routine_params(args: &[String]) -> Result<Vec<RoutineParam>> {
    args.iter()
        .enumerate()
        .map(|(index, arg)| parse_routine_param(arg, index))
        .collect()
}

pub(crate) fn parse_routine_param(arg: &str, index: usize) -> Result<RoutineParam> {
    let default_expr = routine_arg_default_expr(arg)?;
    let arg = routine_arg_without_default(arg);
    let words = split_sql_words(&arg);
    if words.is_empty() {
        return Err(SqlError::InvalidSql("empty routine argument".to_string()));
    }

    let mut mode = RoutineArgMode::In;
    let mut name = None;
    if routine_mode_word(&words[0]).is_some() {
        let mut pos = 0usize;
        mode = routine_mode_word(&words[pos]).unwrap_or(RoutineArgMode::In);
        pos += 1;
        if mode == RoutineArgMode::In
            && words
                .get(pos)
                .is_some_and(|word| word.eq_ignore_ascii_case("OUT"))
        {
            mode = RoutineArgMode::InOut;
            pos += 1;
        }
        if pos + 1 < words.len() {
            name = Some(unquote_identifier_word(&words[pos]));
        }
    } else if words.len() >= 2 && routine_mode_word(&words[1]).is_some() {
        name = Some(unquote_identifier_word(&words[0]));
        mode = routine_mode_word(&words[1]).unwrap_or(RoutineArgMode::In);
        if mode == RoutineArgMode::In
            && words
                .get(2)
                .is_some_and(|word| word.eq_ignore_ascii_case("OUT"))
        {
            mode = RoutineArgMode::InOut;
        }
    } else if words.len() >= 2 && !is_likely_type_word(&words[0]) {
        name = Some(unquote_identifier_word(&words[0]));
    }
    let type_schema = raw_routine_argument_type_schema(&arg)
        .ok()
        .map(|mut schema| {
            // PostgreSQL routine argument signatures discard type modifiers.
            schema.type_modifier = None;
            schema
        });
    Ok(RoutineParam {
        type_schema,
        name,
        mode,
        index,
        default_expr,
    })
}

pub(crate) fn routine_arg_default_expr(arg: &str) -> Result<Option<Expr>> {
    let default = if let Some(index) = find_top_level_keyword(arg, "DEFAULT") {
        Some(&arg[index + "DEFAULT".len()..])
    } else if let Some(index) = find_top_level_operator(arg, "=") {
        Some(&arg[index + 1..])
    } else {
        None
    };
    default
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(parse_routine_expr)
        .transpose()
}

pub(crate) fn routine_arg_without_default(arg: &str) -> String {
    let default_idx = find_top_level_keyword(arg, "DEFAULT")
        .or_else(|| find_top_level_operator(arg, "="))
        .unwrap_or(arg.len());
    arg[..default_idx].trim().to_string()
}

pub(crate) fn routine_mode_word(word: &str) -> Option<RoutineArgMode> {
    match word.trim_matches('"').to_ascii_lowercase().as_str() {
        "in" => Some(RoutineArgMode::In),
        "out" => Some(RoutineArgMode::Out),
        "inout" => Some(RoutineArgMode::InOut),
        _ => None,
    }
}

pub(crate) fn is_likely_type_word(word: &str) -> bool {
    let lower = word
        .trim_matches('"')
        .trim_end_matches("[]")
        .split('(')
        .next()
        .unwrap_or(word)
        .to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "bigint"
            | "boolean"
            | "bool"
            | "date"
            | "decimal"
            | "double"
            | "float"
            | "integer"
            | "int"
            | "int2"
            | "int4"
            | "int8"
            | "json"
            | "jsonb"
            | "numeric"
            | "real"
            | "record"
            | "smallint"
            | "text"
            | "time"
            | "timestamp"
            | "timestamptz"
            | "uuid"
            | "varchar"
            | "void"
    )
}

pub(crate) fn split_sql_words(value: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut start = None;
    let mut in_double_quote = false;
    for (idx, ch) in value.char_indices() {
        if ch == '"' {
            in_double_quote = !in_double_quote;
            start.get_or_insert(idx);
            continue;
        }
        if ch.is_whitespace() && !in_double_quote {
            if let Some(word_start) = start.take() {
                words.push(value[word_start..idx].to_string());
            }
        } else {
            start.get_or_insert(idx);
        }
    }
    if let Some(word_start) = start {
        words.push(value[word_start..].to_string());
    }
    words
}

pub(crate) fn unquote_identifier_word(word: &str) -> String {
    word.trim().trim_matches('"').to_string()
}

pub(crate) fn plpgsql_body_from_definition(definition: &str) -> Result<String> {
    let Some(as_idx) = find_top_level_keyword(definition, "AS") else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "routine definition does not contain an AS body".to_string(),
        ));
    };
    let rest = definition[as_idx + "AS".len()..].trim_start();
    if let Some(delimiter) = dollar_quote_delimiter(rest) {
        let body_start = delimiter.len();
        let Some(body_end) = rest[body_start..].find(&delimiter) else {
            return Err(SqlError::InvalidSql(
                "unterminated routine dollar-quoted body".to_string(),
            ));
        };
        return Ok(rest[body_start..body_start + body_end].to_string());
    }
    if let Some(body) = parse_single_quoted_sql_string(rest)? {
        return Ok(body);
    }
    Err(SqlError::UnsupportedPlpgsqlFeature(
        "routine body must be single-quoted or dollar-quoted".to_string(),
    ))
}

pub(crate) fn parse_single_quoted_sql_string(sql: &str) -> Result<Option<String>> {
    let Some(rest) = sql.strip_prefix('\'') else {
        return Ok(None);
    };
    let bytes = rest.as_bytes();
    let mut idx = 0usize;
    let mut out = String::new();
    while idx < bytes.len() {
        if bytes[idx] == b'\'' {
            if bytes.get(idx + 1) == Some(&b'\'') {
                out.push('\'');
                idx += 2;
                continue;
            }
            return Ok(Some(out));
        }
        out.push(bytes[idx] as char);
        idx += 1;
    }
    Err(SqlError::InvalidSql(
        "unterminated routine single-quoted body".to_string(),
    ))
}

pub(crate) fn parse_plpgsql_body(
    body: &str,
) -> Result<(
    Vec<RoutineDecl>,
    Vec<RoutineStmt>,
    Vec<RoutineExceptionHandler>,
)> {
    let body = trim_sql_statement(body);
    let Some(begin_idx) = find_top_level_keyword(body, "BEGIN") else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "routine body must contain a BEGIN block".to_string(),
        ));
    };
    let declarations_sql = body[..begin_idx]
        .trim()
        .strip_prefix("DECLARE")
        .or_else(|| body[..begin_idx].trim().strip_prefix("declare"))
        .unwrap_or(body[..begin_idx].trim())
        .trim();
    let block_sql = strip_plpgsql_final_end(&body[begin_idx + "BEGIN".len()..])?;
    let (block_sql, exception_handlers) = split_plpgsql_exception_block(block_sql)?;
    let declarations = split_sql_statements(declarations_sql)
        .into_iter()
        .filter(|decl| !decl.trim().is_empty())
        .map(|decl| parse_routine_decl(&decl))
        .collect::<Result<Vec<_>>>()?;
    let statements = parse_routine_statements(block_sql)?;
    validate_routine_continue(&statements, false)?;
    for handler in &exception_handlers {
        validate_routine_continue(&handler.statements, false)?;
    }
    Ok((declarations, statements, exception_handlers))
}

pub(crate) fn strip_plpgsql_final_end(value: &str) -> Result<&str> {
    let mut value = value.trim();
    value = value.strip_suffix(';').unwrap_or(value).trim_end();
    if value.len() >= 3 && value[value.len() - 3..].eq_ignore_ascii_case("END") {
        return Ok(value[..value.len() - 3].trim());
    }
    Err(SqlError::UnsupportedPlpgsqlFeature(
        "routine body must end with END".to_string(),
    ))
}

pub(crate) fn split_plpgsql_exception_block(
    block_sql: &str,
) -> Result<(&str, Vec<RoutineExceptionHandler>)> {
    let Some(exception_idx) = find_plpgsql_exception_clause(block_sql) else {
        return Ok((block_sql, Vec::new()));
    };
    let main_sql = block_sql[..exception_idx].trim();
    let handlers_sql = block_sql[exception_idx + "EXCEPTION".len()..].trim();
    Ok((main_sql, parse_routine_exception_handlers(handlers_sql)?))
}

pub(crate) fn find_plpgsql_exception_clause(block_sql: &str) -> Option<usize> {
    let mut search_start = 0usize;
    while search_start < block_sql.len() {
        let relative = find_top_level_keyword(&block_sql[search_start..], "EXCEPTION")?;
        let exception_idx = search_start + relative;
        let after_exception = &block_sql[exception_idx + "EXCEPTION".len()..];
        if strip_prefix_ci(after_exception.trim_start(), "WHEN").is_some() {
            return Some(exception_idx);
        }
        search_start = exception_idx + "EXCEPTION".len();
    }
    None
}

pub(crate) fn parse_routine_exception_handlers(sql: &str) -> Result<Vec<RoutineExceptionHandler>> {
    let mut handlers = Vec::new();
    let mut rest = sql.trim();
    while !rest.is_empty() {
        let Some(after_when) = strip_prefix_ci(rest, "WHEN") else {
            return Err(SqlError::UnsupportedPlpgsqlFeature(
                "EXCEPTION blocks must contain WHEN handlers".to_string(),
            ));
        };
        let Some(then_idx) = find_top_level_keyword(after_when, "THEN") else {
            return Err(SqlError::InvalidSql(
                "EXCEPTION WHEN handler requires THEN".to_string(),
            ));
        };
        let conditions = parse_routine_exception_conditions(&after_when[..then_idx])?;
        let after_then = after_when[then_idx + "THEN".len()..].trim();
        let next_when = find_top_level_keyword(after_then, "WHEN");
        let (handler_sql, remaining) = if let Some(idx) = next_when {
            (&after_then[..idx], &after_then[idx..])
        } else {
            (after_then, "")
        };
        handlers.push(RoutineExceptionHandler {
            conditions,
            statements: parse_routine_statements(handler_sql)?,
        });
        rest = remaining.trim();
    }
    Ok(handlers)
}

pub(crate) fn parse_routine_exception_conditions(
    sql: &str,
) -> Result<Vec<RoutineExceptionCondition>> {
    split_top_level_keyword(sql, "OR")
        .into_iter()
        .map(|condition| routine_exception_condition(condition.trim()))
        .collect()
}

pub(crate) fn routine_exception_condition(condition: &str) -> Result<RoutineExceptionCondition> {
    let normalized = normalize_object_name(condition);
    match normalized.as_str() {
        "others" => Ok(RoutineExceptionCondition::Others),
        "duplicate_object" => Ok(RoutineExceptionCondition::SqlState("42710".to_string())),
        "serialization_failure" => Ok(RoutineExceptionCondition::SqlState("40001".to_string())),
        "deadlock_detected" => Ok(RoutineExceptionCondition::SqlState("40P01".to_string())),
        "too_many_rows" => Ok(RoutineExceptionCondition::SqlState("P0003".to_string())),
        "no_data_found" => Ok(RoutineExceptionCondition::SqlState("P0002".to_string())),
        _ => {
            if let Some(value) = strip_prefix_ci(condition, "SQLSTATE") {
                let value = value.trim();
                if let Some(state) = parse_single_quoted_sql_string(value)? {
                    return Ok(RoutineExceptionCondition::SqlState(state));
                }
            }
            Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "EXCEPTION condition {condition} is not implemented"
            )))
        }
    }
}

pub(crate) fn parse_routine_decl(declaration: &str) -> Result<RoutineDecl> {
    let declaration = declaration.trim();
    if declaration.is_empty() {
        return Err(SqlError::InvalidSql(
            "empty routine declaration".to_string(),
        ));
    }
    let (name, rest) = parse_leading_sql_identifier(declaration)?;
    let rest = rest.trim_start();
    if let Some(cursor_rest) = strip_prefix_ci(rest, "CURSOR") {
        let cursor_rest = cursor_rest.trim_start();
        let Some(query_sql) = strip_prefix_ci(cursor_rest, "FOR") else {
            return Err(SqlError::UnsupportedPlpgsqlFeature(
                "cursor declarations must use CURSOR FOR query".to_string(),
            ));
        };
        return Ok(RoutineDecl::Cursor {
            name: normalize_object_name(&name),
            query: parse_single_query(query_sql.trim())?,
        });
    }
    if let Some(alias_rest) = strip_prefix_ci(rest, "ALIAS FOR ") {
        let position = parse_plpgsql_position(alias_rest.trim())?;
        return Ok(RoutineDecl::Alias {
            name: normalize_object_name(&name),
            position,
        });
    }
    let assignment = find_top_level_operator(rest, ":=").map(|idx| (idx, idx + 2));
    let default = find_top_level_keyword(rest, "DEFAULT").map(|idx| (idx, idx + "DEFAULT".len()));
    let initializer = match (assignment, default) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (left, right) => left.or(right),
    };
    let type_declaration = rest[..initializer.map_or(rest.len(), |(start, _)| start)].trim();
    let type_schema = raw_routine_type_schema(type_declaration).ok();
    let pg_type = type_schema.as_ref().map(|schema| schema.pg_type.clone());
    let type_modifier = type_schema.and_then(|schema| schema.type_modifier);
    let default_expr = initializer
        .map(|(_, expr_start)| &rest[expr_start..])
        .map(|expr| parse_routine_expr(expr.trim()))
        .map(|result| result.map(RoutineExpr::unbound))
        .transpose()?;
    Ok(RoutineDecl::Variable {
        name: normalize_object_name(&name),
        pg_type,
        type_modifier,
        default_expr,
    })
}

pub(crate) fn parse_plpgsql_position(value: &str) -> Result<usize> {
    let token = value.split_whitespace().next().ok_or_else(|| {
        SqlError::InvalidSql("ALIAS FOR requires a positional argument".to_string())
    })?;
    let number = token
        .trim_start_matches('$')
        .parse::<usize>()
        .map_err(|_| {
            SqlError::InvalidSql(format!("invalid routine positional argument {token}"))
        })?;
    if number == 0 {
        return Err(SqlError::InvalidSql(
            "routine positional arguments are 1-based".to_string(),
        ));
    }
    Ok(number)
}

pub(crate) fn parse_routine_statements(block: &str) -> Result<Vec<RoutineStmt>> {
    let mut statements = Vec::new();
    let mut rest = block.trim();
    while !rest.is_empty() {
        let (statement, consumed) = next_plpgsql_statement(rest)?;
        let statement = statement.trim().trim_end_matches(';').trim();
        if !statement.is_empty() {
            statements.push(parse_routine_statement(statement)?);
        }
        rest = rest[consumed..].trim_start();
        rest = rest.strip_prefix(';').unwrap_or(rest).trim_start();
    }
    Ok(statements)
}

pub(crate) fn trim_leading_sql_comments_with_offset(mut value: &str) -> (&str, usize) {
    let mut consumed = 0usize;
    loop {
        let trimmed = value.trim_start();
        consumed += value.len() - trimmed.len();
        value = trimmed;
        if let Some(rest) = value.strip_prefix("--") {
            let Some(newline_idx) = rest.find('\n') else {
                return ("", consumed + value.len());
            };
            let line_len = "--".len() + newline_idx + 1;
            consumed += line_len;
            value = &value[line_len..];
            continue;
        }
        if let Some(rest) = value.strip_prefix("/*") {
            let Some(end_idx) = rest.find("*/") else {
                return (value, consumed);
            };
            let block_len = "/*".len() + end_idx + "*/".len();
            consumed += block_len;
            value = &value[block_len..];
            continue;
        }
        return (value, consumed);
    }
}

pub(crate) fn next_plpgsql_statement(value: &str) -> Result<(&str, usize)> {
    let original = value;
    let (value, leading_len) = trim_leading_sql_comments_with_offset(original);
    if value.is_empty() {
        return Ok(("", original.len()));
    }
    if value
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF"))
        && value.get(2..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        let end = matching_end_if(value)?;
        return Ok((&original[leading_len..leading_len + end], leading_len + end));
    }
    if value
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOR"))
        && value.get(3..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        let end = matching_end_loop(value)?;
        return Ok((&original[leading_len..leading_len + end], leading_len + end));
    }
    if value
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOREACH"))
        && value.get(7..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        let end = matching_end_loop(value)?;
        return Ok((&original[leading_len..leading_len + end], leading_len + end));
    }
    let end = find_top_level_statement_semicolon(value).unwrap_or(value.len());
    Ok((&original[leading_len..leading_len + end], leading_len + end))
}

// Control words count only at procedural statement boundaries. SQL DDL's
// IF [NOT] EXISTS and SELECT ... FOR UPDATE are not nested PL/pgSQL blocks.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RoutineBoundary {
    If,
    EndIf,
    Loop,
    EndLoop,
    Else,
    Elsif,
}

fn routine_boundaries(value: &str) -> Result<Vec<(usize, usize, RoutineBoundary)>> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, value)
        .tokenize_with_location()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
    let tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut lines = vec![0];
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'\n' {
            lines.push(index + 1);
        }
    }
    let offset = |location: sqlparser::tokenizer::Location| {
        let line = lines[(location.line as usize).saturating_sub(1)];
        line + value[line..]
            .char_indices()
            .nth((location.column as usize).saturating_sub(1))
            .map_or(value.len() - line, |(index, _)| index)
    };
    let word = |index: usize| -> String {
        match tokens.get(index).map(|token| &token.token) {
            Some(Token::Word(word)) if word.quote_style.is_none() => {
                word.value.to_ascii_uppercase()
            }
            _ => String::new(),
        }
    };
    let mut events = Vec::new();
    let mut start = true;
    let mut loop_header = false;
    let mut case_depth = 0usize;
    let mut index = 0;
    while index < tokens.len() {
        let current = word(index);
        if current == "CASE" {
            case_depth += 1;
        } else if case_depth > 0 && current == "END" {
            case_depth -= 1;
        } else if case_depth == 0 {
            let next = word(index + 1);
            let event = if current == "END" && (next == "IF" || next == "LOOP") {
                let kind = if next == "IF" {
                    RoutineBoundary::EndIf
                } else {
                    RoutineBoundary::EndLoop
                };
                let begin = offset(tokens[index].span.start);
                index += 1;
                events.push((begin, offset(tokens[index].span.end), kind));
                start = false;
                index += 1;
                continue;
            } else if start && current == "IF" {
                Some(RoutineBoundary::If)
            } else if start && (current == "FOR" || current == "FOREACH") {
                loop_header = true;
                Some(RoutineBoundary::Loop)
            } else if start && current == "ELSIF" {
                Some(RoutineBoundary::Elsif)
            } else if start && current == "ELSE" {
                Some(RoutineBoundary::Else)
            } else {
                None
            };
            if let Some(kind) = event {
                events.push((
                    offset(tokens[index].span.start),
                    offset(tokens[index].span.end),
                    kind,
                ));
            }
            let starts_body = current == "THEN"
                || current == "ELSE"
                || current == "BEGIN"
                || (current == "LOOP" && loop_header);
            if current == "LOOP" && loop_header {
                loop_header = false;
            }
            start = starts_body || matches!(tokens[index].token, Token::SemiColon);
        }
        index += 1;
    }
    Ok(events)
}

pub(crate) fn matching_end_if(value: &str) -> Result<usize> {
    matching_routine_end(
        value,
        RoutineBoundary::If,
        RoutineBoundary::EndIf,
        "IF statement is missing END IF",
    )
}

pub(crate) fn matching_end_loop(value: &str) -> Result<usize> {
    matching_routine_end(
        value,
        RoutineBoundary::Loop,
        RoutineBoundary::EndLoop,
        "FOR loop is missing END LOOP",
    )
}

fn matching_routine_end(
    value: &str,
    open: RoutineBoundary,
    close: RoutineBoundary,
    error: &str,
) -> Result<usize> {
    let mut depth = 0usize;
    for (_, end, kind) in routine_boundaries(value)? {
        if kind == open {
            depth += 1;
        } else if kind == close {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Ok(end);
            }
        }
    }
    Err(SqlError::UnsupportedPlpgsqlFeature(error.to_string()))
}

pub(crate) fn find_top_level_statement_semicolon(value: &str) -> Option<usize> {
    let bytes = value.as_bytes();
    let mut idx = 0usize;
    let mut depth = 0i32;
    let mut single_quoted = false;
    let mut double_quoted = false;
    while idx < bytes.len() {
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                single_quoted = false;
            }
            idx += 1;
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }
        match bytes[idx] {
            b'\'' => single_quoted = true,
            b'"' => double_quoted = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b';' if depth == 0 => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn parse_routine_statement(statement: &str) -> Result<RoutineStmt> {
    let (statement, _) = trim_leading_sql_comments_with_offset(statement);
    let statement = statement.trim();
    if statement.is_empty() {
        return Err(SqlError::InvalidSql("empty routine statement".to_string()));
    }
    if keyword_matches_at(statement, "CONTINUE", 0) {
        let rest = statement["CONTINUE".len()..].trim();
        if rest.is_empty() {
            return Ok(RoutineStmt::ContinueLoop);
        }
        if keyword_matches_at(rest, "WHEN", 0) {
            return Ok(RoutineStmt::If {
                condition: RoutineExpr::unbound(parse_routine_expr(rest[4..].trim())?),
                then_body: vec![RoutineStmt::ContinueLoop],
                else_body: Vec::new(),
            });
        }
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "labeled CONTINUE is not implemented".into(),
        ));
    }
    if statement.eq_ignore_ascii_case("NULL") {
        return Ok(RoutineStmt::Null);
    }
    if statement
        .get(..2)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("IF"))
        && statement.get(2..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        return parse_routine_if(statement);
    }
    if statement
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOR"))
        && statement.get(3..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        return parse_routine_for_loop(statement);
    }
    if statement
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("FOREACH"))
        && statement.get(7..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        return parse_routine_foreach_loop(statement);
    }
    if statement
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("LOOP"))
        && statement.get(4..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == ';')
        })
    {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "LOOP blocks are not implemented".to_string(),
        ));
    }
    if let Some(rest) = strip_prefix_ci(statement, "RAISE") {
        return parse_routine_raise(rest.trim());
    }
    if let Some(rest) = strip_prefix_ci(statement, "PERFORM ") {
        // PERFORM expr is SELECT expr with the rows thrown away. It still sets
        // FOUND, which is how a trigger body asks "did the check function run
        // against a real row".
        return Ok(RoutineStmt::Perform {
            query: parse_single_query(&format!("SELECT {}", rest.trim()))?,
        });
    }
    let lower = statement.to_ascii_lowercase();
    for (keyword, feature) in [("exception", "EXCEPTION blocks")] {
        if lower.starts_with(keyword) || lower.contains(&format!(" {keyword}")) {
            return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                "{feature} are not implemented"
            )));
        }
    }
    if let Some(rest) = strip_prefix_ci(statement, "EXECUTE") {
        let rest = rest.trim();
        if rest.is_empty() {
            return Err(SqlError::InvalidSql(
                "dynamic EXECUTE requires a SQL expression".to_string(),
            ));
        }
        return Ok(RoutineStmt::DynamicExecute(RoutineExpr::unbound(
            parse_routine_expr(rest)?,
        )));
    }
    if let Some(rest) = strip_prefix_ci(statement, "RETURN QUERY") {
        return Ok(RoutineStmt::ReturnQuery(parse_single_query(rest.trim())?));
    }
    if let Some(rest) = strip_prefix_ci(statement, "RETURN") {
        let rest = rest.trim();
        if rest.to_ascii_lowercase().starts_with("next") {
            return Err(SqlError::UnsupportedPlpgsqlFeature(
                "RETURN NEXT is not implemented".to_string(),
            ));
        }
        return Ok(RoutineStmt::Return(
            (!rest.is_empty())
                .then(|| parse_routine_expr(rest))
                .map(|result| result.map(RoutineExpr::unbound))
                .transpose()?,
        ));
    }
    if let Some(rest) = strip_prefix_ci(statement, "OPEN") {
        let name = parse_single_routine_identifier(rest.trim(), "OPEN")?;
        return Ok(RoutineStmt::OpenCursor { name });
    }
    if let Some(rest) = strip_prefix_ci(statement, "FETCH") {
        return parse_routine_fetch(rest.trim());
    }
    if let Some(rest) = strip_prefix_ci(statement, "CLOSE") {
        let name = parse_single_routine_identifier(rest.trim(), "CLOSE")?;
        return Ok(RoutineStmt::CloseCursor { name });
    }
    if let Some(idx) = find_top_level_operator(statement, ":=") {
        let target = parse_routine_assignment_target(statement[..idx].trim())?;
        let expr = RoutineExpr::unbound(parse_routine_expr(statement[idx + 2..].trim())?);
        return Ok(RoutineStmt::Assignment { target, expr });
    }
    if routine_query_statement(statement) {
        if let Some((sql, targets, strict)) = extract_query_into(statement)? {
            return Ok(RoutineStmt::SelectInto {
                query: parse_single_query(&sql)?,
                targets,
                strict,
            });
        }
    }
    if let Some((sql, targets)) = extract_returning_into(statement)? {
        return Ok(RoutineStmt::SqlInto {
            statement: parse_single_statement(&sql)?,
            targets,
        });
    }
    Ok(RoutineStmt::Sql(parse_single_statement(statement)?))
}

pub(crate) fn parse_routine_raise(statement: &str) -> Result<RoutineStmt> {
    let Some(rest) = strip_prefix_ci(statement, "EXCEPTION") else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "only RAISE EXCEPTION is implemented".to_string(),
        ));
    };
    let rest = rest.trim();
    let using_idx = find_top_level_keyword(rest, "USING");
    let (message_and_arguments, using_options) = using_idx
        .map(|idx| (&rest[..idx], Some(rest[idx + "USING".len()..].trim())))
        .unwrap_or((rest, None));
    let parts = split_top_level_commas_nested(message_and_arguments);
    let mut message = if message_and_arguments.trim().is_empty() {
        None
    } else {
        Some(
            parse_single_quoted_sql_string(parts[0].trim())?.ok_or_else(|| {
                SqlError::UnsupportedPlpgsqlFeature(
                    "RAISE EXCEPTION message must be a string literal".into(),
                )
            })?,
        )
    };
    let mut arguments = parts
        .iter()
        .skip(1)
        .map(|argument| parse_routine_expr(argument.trim()).map(RoutineExpr::unbound))
        .collect::<Result<Vec<_>>>()?;
    let mut sqlstate = "P0001".to_string();
    let mut detail = None;
    let mut seen = std::collections::HashSet::new();
    if let Some(options) = using_options {
        for option in split_top_level_commas_nested(options) {
            let equals_idx = find_top_level_operator(&option, "=").ok_or_else(|| {
                SqlError::InvalidSql("RAISE EXCEPTION USING option requires =".into())
            })?;
            let name = normalize_object_name(option[..equals_idx].trim());
            if !seen.insert(name.clone()) {
                return Err(SqlError::InvalidSql(format!(
                    "duplicate RAISE EXCEPTION USING {name}"
                )));
            }
            let value = option[equals_idx + 1..].trim();
            if name == "detail" {
                detail = Some(RoutineExpr::unbound(parse_routine_expr(value)?));
                continue;
            }
            if name == "message" {
                if message.is_some() {
                    return Err(SqlError::InvalidSql(
                        "RAISE EXCEPTION message specified twice".into(),
                    ));
                }
                message = Some("%".into());
                arguments.push(RoutineExpr::unbound(parse_routine_expr(value)?));
                continue;
            }
            let parsed = parse_single_quoted_sql_string(option[equals_idx + 1..].trim())?
                .ok_or_else(|| {
                    SqlError::UnsupportedPlpgsqlFeature(format!(
                        "RAISE EXCEPTION {name} must be a string literal"
                    ))
                })?;
            match name.as_str() {
                "errcode" => {
                    if parsed.len() != 5 || !parsed.bytes().all(|byte| byte.is_ascii_alphanumeric())
                    {
                        return Err(SqlError::InvalidSql(
                            "RAISE EXCEPTION ERRCODE must be a five-character SQLSTATE".into(),
                        ));
                    }
                    sqlstate = parsed.to_ascii_uppercase();
                }
                _ => {
                    return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
                        "RAISE EXCEPTION USING {name} is not implemented"
                    )))
                }
            }
        }
    }
    let message =
        message.ok_or_else(|| SqlError::InvalidSql("RAISE EXCEPTION requires a message".into()))?;
    Ok(RoutineStmt::RaiseException {
        message,
        arguments,
        detail,
        sqlstate,
    })
}

pub(crate) fn format_routine_raise_message(template: &str, arguments: &[SqlValue]) -> String {
    let mut result = String::with_capacity(template.len());
    let mut argument_index = 0usize;
    let mut chars = template.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            result.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            result.push('%');
            continue;
        }
        if let Some(argument) = arguments.get(argument_index) {
            let text = match argument {
                SqlValue::Null => "<NULL>".to_string(),
                value => sql_value_text(value)
                    .unwrap_or_else(|| sql_value_to_json(value.clone()).to_string()),
            };
            result.push_str(&text);
            argument_index += 1;
        } else {
            result.push('%');
        }
    }
    result
}

pub(crate) fn parse_routine_for_loop(statement: &str) -> Result<RoutineStmt> {
    let inner = statement
        .trim()
        .strip_prefix("FOR")
        .or_else(|| statement.trim().strip_prefix("for"))
        .ok_or_else(|| SqlError::InvalidSql("FOR loop expected".to_string()))?
        .trim();
    let Some(in_idx) = find_top_level_keyword(inner, "IN") else {
        return Err(SqlError::InvalidSql("FOR loop requires IN".to_string()));
    };
    let iterator = normalize_object_name(inner[..in_idx].trim());
    if iterator.is_empty() {
        return Err(SqlError::InvalidSql(
            "FOR loop requires an iterator variable".to_string(),
        ));
    }
    let after_in = inner[in_idx + "IN".len()..].trim();
    let Some(loop_idx) = find_top_level_keyword(after_in, "LOOP") else {
        return Err(SqlError::InvalidSql("FOR loop requires LOOP".to_string()));
    };
    let range_sql = after_in[..loop_idx].trim();
    if strip_prefix_ci(range_sql, "REVERSE").is_some() {
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "REVERSE FOR loops are not implemented".to_string(),
        ));
    }
    let body = after_in[loop_idx + "LOOP".len()..]
        .trim()
        .strip_suffix("END LOOP")
        .or_else(|| {
            after_in[loop_idx + "LOOP".len()..]
                .trim()
                .strip_suffix("end loop")
        })
        .ok_or_else(|| SqlError::InvalidSql("FOR loop requires END LOOP".to_string()))?
        .trim();
    let body = parse_routine_statements(body)?;
    if let Some(range_idx) = find_top_level_operator(range_sql, "..") {
        let lower = parse_routine_expr(range_sql[..range_idx].trim())?;
        let upper = parse_routine_expr(range_sql[range_idx + 2..].trim())?;
        Ok(RoutineStmt::ForLoop {
            iterator,
            lower: RoutineExpr::unbound(lower),
            upper: RoutineExpr::unbound(upper),
            body,
        })
    } else {
        Ok(RoutineStmt::QueryForLoop {
            target: iterator,
            query: parse_single_query(range_sql)?,
            body,
        })
    }
}

pub(crate) fn parse_routine_foreach_loop(statement: &str) -> Result<RoutineStmt> {
    let inner = strip_prefix_ci(statement.trim(), "FOREACH")
        .ok_or_else(|| SqlError::InvalidSql("FOREACH loop expected".to_string()))?
        .trim();
    let Some(in_idx) = find_top_level_keyword(inner, "IN") else {
        return Err(SqlError::InvalidSql(
            "FOREACH loop requires IN ARRAY".to_string(),
        ));
    };
    let target_clause = inner[..in_idx].trim();
    let target_words = split_sql_words(target_clause);
    let (target, slice) = match target_words.as_slice() {
        [target] => (normalize_object_name(&unquote_identifier_word(target)), 0),
        [target, slice_keyword, slice] if slice_keyword.eq_ignore_ascii_case("SLICE") => {
            let slice = slice.parse::<usize>().map_err(|_| {
                SqlError::InvalidSql("FOREACH SLICE must be a nonnegative integer".to_string())
            })?;
            (
                normalize_object_name(&unquote_identifier_word(target)),
                slice,
            )
        }
        _ => {
            return Err(SqlError::InvalidSql(
                "FOREACH requires a target and optional SLICE count".to_string(),
            ));
        }
    };
    if target.is_empty() {
        return Err(SqlError::InvalidSql(
            "FOREACH loop requires a target variable".to_string(),
        ));
    }
    let after_in = inner[in_idx + "IN".len()..].trim();
    let after_array = strip_prefix_ci(after_in, "ARRAY")
        .ok_or_else(|| SqlError::InvalidSql("FOREACH loop requires IN ARRAY".to_string()))?
        .trim();
    let Some(loop_idx) = find_top_level_keyword(after_array, "LOOP") else {
        return Err(SqlError::InvalidSql(
            "FOREACH loop requires LOOP".to_string(),
        ));
    };
    let array = parse_routine_expr(after_array[..loop_idx].trim())?;
    let body = after_array[loop_idx + "LOOP".len()..]
        .trim()
        .strip_suffix("END LOOP")
        .or_else(|| {
            after_array[loop_idx + "LOOP".len()..]
                .trim()
                .strip_suffix("end loop")
        })
        .ok_or_else(|| SqlError::InvalidSql("FOREACH loop requires END LOOP".to_string()))?
        .trim();
    Ok(RoutineStmt::ForeachLoop {
        target,
        slice,
        array: RoutineExpr::unbound(array),
        body: parse_routine_statements(body)?,
    })
}

pub(crate) fn parse_routine_fetch(rest: &str) -> Result<RoutineStmt> {
    let Some(into_idx) = find_top_level_keyword(rest, "INTO") else {
        return Err(SqlError::InvalidSql(
            "FETCH requires INTO targets".to_string(),
        ));
    };
    let name = parse_single_routine_identifier(rest[..into_idx].trim(), "FETCH")?;
    let targets = routine_target_names(rest[into_idx + "INTO".len()..].trim())?;
    Ok(RoutineStmt::FetchCursor { name, targets })
}

pub(crate) fn parse_routine_assignment_target(target: &str) -> Result<RoutineAssignmentTarget> {
    if !target.contains('[') {
        return Ok(RoutineAssignmentTarget::Variable(normalize_object_name(
            target,
        )));
    }
    let expr = parse_routine_expr(target)?;
    let Expr::CompoundFieldAccess { root, access_chain } = expr else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
            "routine assignment target {target} is not a supported array element"
        )));
    };
    let Expr::Identifier(ident) = root.as_ref() else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
            "routine assignment target {target} must use a simple array variable"
        )));
    };
    let [AccessExpr::Subscript(Subscript::Index { index })] = access_chain.as_slice() else {
        return Err(SqlError::UnsupportedPlpgsqlFeature(format!(
            "routine assignment target {target} supports only single array indexes"
        )));
    };
    Ok(RoutineAssignmentTarget::ArrayElement {
        array_name: normalize_object_name(&ident.value),
        index: RoutineExpr::unbound(index.clone()),
    })
}

pub(crate) fn parse_single_routine_identifier(value: &str, statement: &str) -> Result<String> {
    let words = split_sql_words(value);
    let [name] = words.as_slice() else {
        return Err(SqlError::InvalidSql(format!(
            "{statement} requires exactly one cursor name"
        )));
    };
    Ok(normalize_object_name(&unquote_identifier_word(name)))
}

pub(crate) fn parse_routine_if(statement: &str) -> Result<RoutineStmt> {
    let inner = statement
        .trim()
        .strip_prefix("IF")
        .or_else(|| statement.trim().strip_prefix("if"))
        .ok_or_else(|| SqlError::InvalidSql("IF statement expected".to_string()))?;
    let inner = inner.trim();
    let Some(then_idx) = find_top_level_keyword(inner, "THEN") else {
        return Err(SqlError::InvalidSql(
            "IF statement requires THEN".to_string(),
        ));
    };
    let condition = parse_check_expression(strip_balanced_outer_parens(&inner[..then_idx]))?;
    let body = inner[then_idx + "THEN".len()..].trim();
    let body = body
        .strip_suffix("END IF")
        .or_else(|| body.strip_suffix("end if"))
        .ok_or_else(|| SqlError::InvalidSql("IF statement requires END IF".to_string()))?
        .trim();
    routine_if_from_condition_and_body(condition, body)
}

pub(crate) fn routine_if_from_condition_and_body(
    condition: Expr,
    body: &str,
) -> Result<RoutineStmt> {
    let (then_sql, else_body) = match find_plpgsql_if_branch(body)? {
        Some((idx, PlpgsqlIfBranch::Elsif)) => {
            let nested = parse_routine_elsif_clause(&body[idx + "ELSIF".len()..])?;
            (&body[..idx], vec![nested])
        }
        Some((idx, PlpgsqlIfBranch::Else)) => (
            &body[..idx],
            parse_routine_statements(&body[idx + "ELSE".len()..])?,
        ),
        None => (body, Vec::new()),
    };
    Ok(RoutineStmt::If {
        condition: RoutineExpr::unbound(condition),
        then_body: parse_routine_statements(then_sql)?,
        else_body,
    })
}

pub(crate) fn parse_routine_elsif_clause(statement: &str) -> Result<RoutineStmt> {
    let statement = statement.trim();
    let Some(then_idx) = find_top_level_keyword(statement, "THEN") else {
        return Err(SqlError::InvalidSql(
            "ELSIF statement requires THEN".to_string(),
        ));
    };
    let condition = parse_check_expression(strip_balanced_outer_parens(&statement[..then_idx]))?;
    let body = statement[then_idx + "THEN".len()..].trim();
    routine_if_from_condition_and_body(condition, body)
}

pub(crate) fn find_plpgsql_if_branch(value: &str) -> Result<Option<(usize, PlpgsqlIfBranch)>> {
    let mut depth = 0usize;
    for (start, _, kind) in routine_boundaries(value)? {
        match kind {
            RoutineBoundary::If => depth += 1,
            RoutineBoundary::EndIf => depth = depth.saturating_sub(1),
            RoutineBoundary::Else if depth == 0 => return Ok(Some((start, PlpgsqlIfBranch::Else))),
            RoutineBoundary::Elsif if depth == 0 => {
                return Ok(Some((start, PlpgsqlIfBranch::Elsif)))
            }
            _ => {}
        }
    }
    Ok(None)
}

pub(crate) fn routine_query_statement(statement: &str) -> bool {
    let statement = statement.trim_start();
    statement
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"))
        && statement.get(6..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
        || statement
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WITH"))
            && statement.get(4..).is_some_and(|tail| {
                tail.chars()
                    .next()
                    .is_none_or(|ch| ch.is_whitespace() || ch == '(')
            })
}

pub(crate) fn extract_query_into(statement: &str) -> Result<Option<(String, Vec<String>, bool)>> {
    let mut search_start = 0usize;
    while let Some(relative_idx) = find_top_level_keyword(&statement[search_start..], "INTO") {
        let into_idx = search_start + relative_idx;
        let after_into = &statement[into_idx + "INTO".len()..];
        let target_end = first_top_level_keyword_index(
            after_into,
            &[
                "FROM",
                "WHERE",
                "GROUP BY",
                "HAVING",
                "ORDER BY",
                "LIMIT",
                "OFFSET",
                "UNION",
                "INTERSECT",
                "EXCEPT",
            ],
        )
        .unwrap_or(after_into.len());
        let raw_targets = after_into[..target_end].trim_start();
        let strict = raw_targets
            .get(..6)
            .is_some_and(|head| head.eq_ignore_ascii_case("STRICT"))
            && raw_targets
                .get(6..)
                .is_some_and(|tail| tail.starts_with(char::is_whitespace));
        let targets = routine_target_names(if strict {
            raw_targets[6..].trim_start()
        } else {
            raw_targets
        })?;
        let sql = format!(
            "{} {}",
            statement[..into_idx].trim_end(),
            after_into[target_end..].trim_start()
        )
        .trim()
        .to_string();
        if parse_single_query(&sql).is_ok() {
            return Ok(Some((sql, targets, strict)));
        }
        search_start = into_idx + "INTO".len();
    }
    Ok(None)
}

pub(crate) fn extract_returning_into(statement: &str) -> Result<Option<(String, Vec<String>)>> {
    let Some(returning_idx) = find_top_level_keyword(statement, "RETURNING") else {
        return Ok(None);
    };
    let after_returning = &statement[returning_idx + "RETURNING".len()..];
    let Some(into_rel_idx) = find_top_level_keyword(after_returning, "INTO") else {
        return Ok(None);
    };
    let into_idx = returning_idx + "RETURNING".len() + into_rel_idx;
    let targets = routine_target_names(&statement[into_idx + "INTO".len()..])?;
    let sql = statement[..into_idx].trim_end().to_string();
    Ok(Some((sql, targets)))
}

pub(crate) fn routine_target_names(targets: &str) -> Result<Vec<String>> {
    let names = split_top_level_commas_nested(targets)
        .into_iter()
        .map(|target| normalize_object_name(target.trim()))
        .filter(|target| !target.is_empty())
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Err(SqlError::InvalidSql(
            "INTO requires at least one target".to_string(),
        ));
    }
    Ok(names)
}

pub(crate) fn parse_routine_expr(expr: &str) -> Result<Expr> {
    let sql = format!("SELECT {expr}");
    let statements = parse_statements(&sql)?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(SqlError::InvalidSql(format!(
            "invalid routine expression {expr}"
        )));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SqlError::InvalidSql(format!(
            "invalid routine expression {expr}"
        )));
    };
    let [item] = select.projection.as_slice() else {
        return Err(SqlError::InvalidSql(format!(
            "invalid routine expression {expr}"
        )));
    };
    let (expr, _) = select_item_expr_and_alias(item)?;
    Ok(expr.clone())
}

pub(crate) fn parse_single_query(sql: &str) -> Result<Query> {
    let statement = parse_single_statement(sql)?;
    let Statement::Query(query) = statement else {
        return Err(SqlError::InvalidSql(format!(
            "expected SELECT statement, got {sql}"
        )));
    };
    Ok(*query)
}

pub(crate) fn parse_single_statement(sql: &str) -> Result<Statement> {
    let statements = parse_statements(sql)?;
    let [statement] = statements.as_slice() else {
        return Err(SqlError::InvalidSql(format!(
            "expected one SQL statement, got {}",
            statements.len()
        )));
    };
    Ok(statement.clone())
}

pub(crate) fn first_top_level_keyword_index(sql: &str, keywords: &[&str]) -> Option<usize> {
    keywords
        .iter()
        .filter_map(|keyword| find_top_level_keyword(sql, keyword))
        .min()
}

pub(crate) fn split_top_level_keyword(sql: &str, keyword: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = sql.trim();
    while let Some(idx) = find_top_level_keyword(rest, keyword) {
        parts.push(rest[..idx].trim().to_string());
        rest = rest[idx + keyword.len()..].trim();
    }
    if !rest.is_empty() {
        parts.push(rest.to_string());
    }
    parts
}

pub(crate) fn find_top_level_operator(sql: &str, operator: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut idx = 0usize;
    let mut depth = 0i32;
    let mut single_quoted = false;
    let mut double_quoted = false;
    while idx < bytes.len() {
        if single_quoted {
            if bytes[idx] == b'\'' {
                if bytes.get(idx + 1) == Some(&b'\'') {
                    idx += 2;
                    continue;
                }
                single_quoted = false;
            }
            idx += 1;
            continue;
        }
        if double_quoted {
            if bytes[idx] == b'"' {
                double_quoted = false;
            }
            idx += 1;
            continue;
        }
        match bytes[idx] {
            b'\'' => single_quoted = true,
            b'"' => double_quoted = true,
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && sql[idx..].starts_with(operator) => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    None
}

pub(crate) fn list_routines(db: &BicDb) -> Result<Vec<RoutineSchema>> {
    let records = match db.scan_collection(ROUTINE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut routines = records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<RoutineSchema>(record.metadata)
                .map(RoutineSchema::with_legacy_security_metadata)
                .map_err(SqlError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    routines.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(routines)
}

pub(crate) fn save_routine(db: &mut BicDb, routine: &RoutineSchema) -> Result<()> {
    ensure_routine_collection(db)?;
    db.insert(
        ROUTINE_COLLECTION,
        Record::new(routine_key(routine.kind, &routine.name))
            .with_metadata(serde_json::to_value(routine)?),
    )?;
    Ok(())
}

pub(crate) fn delete_routine(db: &mut BicDb, kind: RoutineKind, name: &str) -> Result<bool> {
    match db.delete(ROUTINE_COLLECTION, &routine_key(kind, name)) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn trigger_key(table: &str, name: &str) -> String {
    format!(
        "{}:{}",
        normalize_object_name(table),
        normalize_object_name(name)
    )
}

pub(crate) fn load_trigger(db: &BicDb, table: &str, name: &str) -> Result<Option<TriggerSchema>> {
    match db.get(TRIGGER_COLLECTION, &trigger_key(table, name)) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) | Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn find_trigger(
    db: &BicDb,
    name: &str,
    table_name: Option<&str>,
) -> Result<Option<TriggerSchema>> {
    if let Some(table_name) = table_name {
        return load_trigger(db, table_name, name);
    }
    Ok(list_triggers(db)?
        .into_iter()
        .find(|trigger| trigger.name.eq_ignore_ascii_case(name)))
}

pub(crate) fn list_triggers(db: &BicDb) -> Result<Vec<TriggerSchema>> {
    let records = match db.scan_collection(TRIGGER_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut triggers = records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<TriggerSchema>(record.metadata).map_err(SqlError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    triggers.sort_by(|left, right| {
        left.table_name
            .cmp(&right.table_name)
            .then(left.name.cmp(&right.name))
    });
    Ok(triggers)
}

pub(crate) fn save_trigger(db: &mut BicDb, trigger: &TriggerSchema) -> Result<()> {
    ensure_trigger_collection(db)?;
    db.insert(
        TRIGGER_COLLECTION,
        Record::new(trigger_key(&trigger.table_name, &trigger.name))
            .with_metadata(serde_json::to_value(trigger)?),
    )?;
    Ok(())
}

pub(crate) fn save_trigger_if_missing(
    db: &mut BicDb,
    trigger: TriggerSchema,
    or_replace: bool,
) -> Result<()> {
    let exists = load_trigger(db, &trigger.table_name, &trigger.name)?.is_some();
    if exists && !or_replace {
        return Err(SqlError::InvalidSql(format!(
            "trigger \"{}\" for relation \"{}\" already exists",
            trigger.name, trigger.table_name
        )));
    }
    save_trigger(db, &trigger)
}

pub(crate) fn delete_trigger(db: &mut BicDb, name: &str, table_name: Option<&str>) -> Result<bool> {
    if let Some(table_name) = table_name {
        return match db.delete(TRIGGER_COLLECTION, &trigger_key(table_name, name)) {
            Ok(existed) => Ok(existed),
            Err(BicDbError::CollectionNotFound(_)) => Ok(false),
            Err(error) => Err(error.into()),
        };
    }
    for trigger in list_triggers(db)? {
        if trigger.name.eq_ignore_ascii_case(name) {
            return delete_trigger(db, name, Some(&trigger.table_name));
        }
    }
    Ok(false)
}

pub(crate) fn list_notifications(db: &BicDb) -> Result<Vec<NotificationRecord>> {
    let records = match db.scan_collection(NOTIFICATION_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut notifications = records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<NotificationRecord>(record.metadata).map_err(SqlError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    notifications.sort_by_key(|notification| notification.id);
    Ok(notifications)
}

pub(crate) fn next_notification_id(db: &BicDb) -> Result<i64> {
    Ok(list_notifications(db)?
        .into_iter()
        .map(|notification| notification.id)
        .max()
        .unwrap_or(0)
        + 1)
}

pub(crate) fn save_notification(db: &mut BicDb, notification: NotificationRecord) -> Result<()> {
    ensure_notification_collection(db)?;
    db.insert(
        NOTIFICATION_COLLECTION,
        Record::new(notification.id.to_string()).with_metadata(serde_json::to_value(notification)?),
    )?;
    Ok(())
}

pub(crate) fn load_sequence(db: &BicDb, sequence: &str) -> Result<Option<SequenceSchema>> {
    match db.get(SEQUENCE_COLLECTION, sequence) {
        Ok(Some(record)) => Ok(Some(serde_json::from_value(record.metadata.clone())?)),
        Ok(None) => Ok(None),
        Err(BicDbError::CollectionNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn load_sequence_required(db: &BicDb, sequence: &str) -> Result<SequenceSchema> {
    load_sequence(db, sequence)?
        .ok_or_else(|| SqlError::InvalidSql(format!("relation \"{sequence}\" does not exist")))
}

pub(crate) fn advance_sequence(sequence: &mut SequenceSchema) -> Result<i64> {
    let value = if sequence.is_called {
        sequence
            .last_value
            .checked_add(sequence.increment_by)
            .ok_or_else(|| {
                SqlError::sequence_generator_limit(format!(
                    "nextval: reached {} value of sequence \"{}\" ({})",
                    if sequence.increment_by >= 0 {
                        "maximum"
                    } else {
                        "minimum"
                    },
                    sequence.name,
                    if sequence.increment_by >= 0 {
                        sequence.max_value
                    } else {
                        sequence.min_value
                    }
                ))
            })?
    } else {
        sequence.last_value
    };
    if value > sequence.max_value || value < sequence.min_value {
        if sequence.cycle {
            sequence.last_value = if sequence.increment_by >= 0 {
                sequence.min_value
            } else {
                sequence.max_value
            };
        } else {
            return Err(SqlError::sequence_generator_limit(format!(
                "nextval: reached {} value of sequence \"{}\" ({})",
                if sequence.increment_by >= 0 {
                    "maximum"
                } else {
                    "minimum"
                },
                sequence.name,
                if sequence.increment_by >= 0 {
                    sequence.max_value
                } else {
                    sequence.min_value
                }
            )));
        }
    } else {
        sequence.last_value = value;
    }
    sequence.is_called = true;
    Ok(sequence.last_value)
}

pub(crate) fn list_sequences(db: &BicDb) -> Result<Vec<SequenceSchema>> {
    let records = match db.scan_collection(SEQUENCE_COLLECTION) {
        Ok(records) => records,
        Err(BicDbError::CollectionNotFound(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut sequences = records
        .into_iter()
        .map(|record| {
            serde_json::from_value::<SequenceSchema>(record.metadata).map_err(SqlError::from)
        })
        .collect::<Result<Vec<_>>>()?;
    sequences.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sequences)
}

pub(crate) fn save_sequence(db: &mut BicDb, sequence: &SequenceSchema) -> Result<()> {
    ensure_sequence_collection(db)?;
    db.insert(
        SEQUENCE_COLLECTION,
        Record::new(sequence.name.clone()).with_metadata(serde_json::to_value(sequence)?),
    )?;
    Ok(())
}

pub(crate) fn create_sequence_if_missing(
    db: &mut BicDb,
    sequence: SequenceSchema,
    if_not_exists: bool,
) -> Result<()> {
    if load_sequence(db, &sequence.name)?.is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::InvalidSql(format!(
            "relation \"{}\" already exists",
            sequence.name
        )));
    }
    save_sequence(db, &sequence)
}

pub(crate) fn delete_sequence(db: &mut BicDb, sequence: &str) -> Result<bool> {
    match db.delete(SEQUENCE_COLLECTION, sequence) {
        Ok(existed) => Ok(existed),
        Err(BicDbError::CollectionNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn alter_table_add_column(
    db: &mut BicDb,
    table: &str,
    schema: &mut TableSchema,
    column_def: &ColumnDef,
    if_not_exists: bool,
) -> Result<()> {
    let mut column = column_schema_from_def(db, column_def)?;
    if schema.column(&column.name).is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::InvalidSql(format!(
            "column \"{}\" of relation \"{}\" already exists",
            column.name, table
        )));
    }
    let default = column_default_value(column_def)?;
    column.default_value = default.clone();
    let column_name = column.name.clone();
    let column_nullable = column.nullable;
    let mut updated_schema = schema.clone();
    updated_schema.add_column(column);
    if !column_nullable && default.is_none() {
        for record in db.scan_collection(table)? {
            if matches!(
                record_column_value(&record, &updated_schema, &column_name),
                SqlValue::Null
            ) {
                return Err(constraint_violation(
                    "23502",
                    format!(
                        "column \"{}\" of relation \"{}\" contains null values",
                        column_name, table
                    ),
                    Some(table.to_string()),
                    Some(column_name.clone()),
                    Some(not_null_constraint_name(table, &column_name)),
                ));
            }
        }
    }
    if let Some(default) = default {
        for mut record in db.scan_collection(table)? {
            set_record_column(
                &mut record,
                Some(&updated_schema),
                &column_name,
                default.clone(),
            )?;
            db.insert(table, record)?;
        }
    }
    *schema = updated_schema;
    save_schema(db, schema)
}

pub(crate) fn alter_table_drop_column(
    db: &mut BicDb,
    table: &str,
    schema: &mut TableSchema,
    column: &str,
    if_exists: bool,
) -> Result<()> {
    let Some(idx) = schema
        .columns
        .iter()
        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
    else {
        if if_exists {
            return Ok(());
        }
        return Err(SqlError::InvalidSql(format!(
            "column \"{column}\" of relation \"{table}\" does not exist"
        )));
    };
    if schema.columns[idx].primary_key {
        return Err(SqlError::Unsupported(
            "ALTER TABLE DROP COLUMN for primary key columns is not supported".to_string(),
        ));
    }
    let actual = schema.columns[idx].name.clone();
    schema.columns.remove(idx);
    schema.constraints.retain(|constraint| match constraint {
        ConstraintSchema::Unique { columns, .. } | ConstraintSchema::ForeignKey { columns, .. } => {
            !columns
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&actual))
        }
        ConstraintSchema::Check { expression, .. } => !expression
            .split(|ch: char| !ch.is_alphanumeric() && ch != '_')
            .any(|token| token.eq_ignore_ascii_case(&actual)),
        ConstraintSchema::Exclusion {
            equal_columns,
            range,
            predicate,
            ..
        } => {
            !equal_columns
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&actual))
                && !range.as_ref().is_some_and(|range| {
                    range.start_column.eq_ignore_ascii_case(&actual)
                        || range.end_column.eq_ignore_ascii_case(&actual)
                })
                && !predicate.as_ref().is_some_and(|expression| {
                    expression
                        .split(|ch: char| !ch.is_alphanumeric() && ch != '_')
                        .any(|token| token.eq_ignore_ascii_case(&actual))
                })
        }
    });
    schema.indexes.retain(|index| {
        !index
            .expression
            .split(',')
            .map(|part| part.trim().trim_matches('"'))
            .any(|candidate| candidate.eq_ignore_ascii_case(&actual))
    });
    for mut record in db.scan_collection(table)? {
        remove_record_column(&mut record, &actual);
        db.insert(table, record)?;
    }
    save_schema(db, schema)
}

pub(crate) fn alter_table_rename_column(
    db: &mut BicDb,
    table: &str,
    schema: &mut TableSchema,
    old_column: &str,
    new_column: &str,
) -> Result<()> {
    if schema.column(new_column).is_some() {
        return Err(SqlError::InvalidSql(format!(
            "column \"{new_column}\" of relation \"{table}\" already exists"
        )));
    }
    let Some(idx) = schema
        .columns
        .iter()
        .position(|candidate| candidate.name.eq_ignore_ascii_case(old_column))
    else {
        return Err(SqlError::InvalidSql(format!(
            "column \"{old_column}\" of relation \"{table}\" does not exist"
        )));
    };
    if schema.columns[idx].primary_key {
        return Err(SqlError::Unsupported(
            "ALTER TABLE RENAME COLUMN for primary key columns is not supported".to_string(),
        ));
    }
    let old_actual = schema.columns[idx].name.clone();
    schema.columns[idx].name = new_column.to_string();
    rename_column_references(schema, &old_actual, new_column);
    for mut record in db.scan_collection(table)? {
        rename_record_column(&mut record, &old_actual, new_column);
        db.insert(table, record)?;
    }
    save_schema(db, schema)
}

pub(crate) fn alter_table_rename_table(
    db: &mut BicDb,
    old_table: &str,
    new_table: &str,
    schema: &mut TableSchema,
    track_undo: bool,
) -> Result<Option<DdlUndo>> {
    if user_collection_names(db)
        .iter()
        .any(|name| name.eq_ignore_ascii_case(new_table))
    {
        return Err(SqlError::InvalidSql(format!(
            "relation \"{new_table}\" already exists"
        )));
    }
    let primary_key_name = schema
        .primary_key_column()
        .map(|_| schema.primary_key_constraint_name());
    let records = db.scan_collection(old_table)?;
    let indexes = db
        .index_definitions()
        .into_iter()
        .filter(|index| index.collection.eq_ignore_ascii_case(old_table))
        .collect::<Vec<_>>();
    let sequences = list_sequences(db)?
        .into_iter()
        .filter(|sequence| sequence.owned_by_table.as_deref() == Some(old_table))
        .collect::<Vec<_>>();
    let undo = track_undo.then(|| DdlUndo::RestoreRenamedTable {
        old_table: old_table.to_string(),
        new_table: new_table.to_string(),
        records: records.clone(),
        indexes: indexes.clone(),
        schema: schema.clone(),
        sequences: sequences.clone(),
    });

    let result = (|| -> Result<()> {
        db.create_collection(new_table)?;
        db.batch_insert(new_table, records)?;
        db.drop_collection(old_table)?;
        for index in &indexes {
            db.create_index(IndexDefinition {
                name: index.name.clone(),
                collection: new_table.to_string(),
                fields: index.fields.clone(),
                unique: index.unique,
                kind: index.kind.clone(),
                predicate: None,
                exclusion: None,
            })?;
        }
        delete_schema(db, old_table)?;
        schema.name = new_table.to_string();
        if schema.primary_key_name.is_none() {
            schema.primary_key_name = primary_key_name;
        }
        for sequence in &sequences {
            let mut sequence = sequence.clone();
            sequence.owned_by_table = Some(new_table.to_string());
            save_sequence(db, &sequence)?;
        }
        save_schema(db, schema)
    })();

    if let Err(error) = result {
        if let Some(undo) = undo.clone() {
            apply_rename_table_undo(db, undo)?;
        }
        return Err(error);
    }

    Ok(undo)
}

pub(crate) fn restore_table_state(
    db: &mut BicDb,
    table: &str,
    schema: &TableSchema,
    records: Vec<Record>,
) -> Result<()> {
    let restore_ids = records
        .iter()
        .map(|record| record.id.clone())
        .collect::<BTreeSet<_>>();
    for record in db.scan_collection(table)? {
        if !restore_ids.contains(&record.id) {
            db.delete(table, &record.id)?;
        }
    }
    for record in records {
        db.insert(table, record)?;
    }
    save_schema(db, schema)
}

fn validate_routine_continue(statements: &[RoutineStmt], in_loop: bool) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::ContinueLoop if !in_loop => {
                return Err(SqlError::InvalidSql(
                    "CONTINUE cannot be used outside a loop".into(),
                ))
            }
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                validate_routine_continue(then_body, in_loop)?;
                validate_routine_continue(else_body, in_loop)?;
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. } => validate_routine_continue(body, true)?,
            _ => {}
        }
    }
    Ok(())
}
