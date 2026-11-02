//! PL/pgSQL routine interpreter support: routine expression binding, routine body parsing (IF/LOOP/cursor block matching), RoutineFrame variable handling, routine persistence, plus sequence load/save and notification helpers.
//!
//! Extracted verbatim from `lib.rs` (phase-1 mechanical module split).
//! Moved items were bumped to `pub(crate)` so existing call sites keep
//! resolving; `lib.rs` re-exports this module via `pub use routines::*;`.

// Glue import: bring every crate-root item (including the root's private
// imports and the other split modules' re-exports) into scope so the moved
// code compiles unchanged.
use crate::*;

pub(crate) mod nested_blocks;
pub(crate) mod parameterized_cursors;
mod procedural_case;

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
    let language = routine.language.trim().trim_matches('\'');
    language.eq_ignore_ascii_case("plpgsql")
        || oracle::preview_routine_language_version(language).is_some()
}

pub(crate) fn compile_routine_ir(routine: &RoutineSchema) -> Result<RoutineIR> {
    compile_routine_ir_in_catalog(routine, None)
}

fn compile_routine_ir_in_catalog(routine: &RoutineSchema, db: Option<&BicDb>) -> Result<RoutineIR> {
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
            oracle_date_return: false,
            oracle_statement_rollback: false,
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
    parameterized_cursors::validate_capabilities(
        &routine.language,
        &declarations,
        &statements,
        &exception_handlers,
    )?;
    let oracle_date_return = if oracle::typed_preview_routine_language(&routine.language) {
        nested_blocks::prepare_declared_types(&mut statements, &mut exception_handlers)?;
        oracle::routine_arithmetic::prepare_declared_types(routine, &mut params, &mut declarations)?
    } else {
        false
    };
    if oracle::preview_routine_language_version(&routine.language).is_some() {
        oracle::routines::lower_datetime_trunc(
            &mut params,
            &mut declarations,
            &mut statements,
            &mut exception_handlers,
        );
    }
    if matches!(
        oracle::preview_routine_language_version(&routine.language),
        Some(2 | 3 | 4 | 5 | 6 | 7 | 8 | 9)
    ) {
        oracle::routines::bind_function_namespace(
            routine,
            &mut params,
            &mut declarations,
            &mut statements,
            &mut exception_handlers,
        )?;
    }
    if oracle::typed_preview_routine_language(&routine.language) {
        oracle::routine_arithmetic::lower(
            db.ok_or_else(|| {
                SqlError::Unsupported(
                    "typed Oracle routine compilation requires catalog context".into(),
                )
            })?,
            matches!(
                oracle::preview_routine_language_version(&routine.language),
                Some(4 | 5 | 6 | 7 | 8 | 9)
            ),
            oracle::preview_routine_language_version(&routine.language)
                .is_some_and(|version| version >= 7),
            oracle::preview_routine_language_version(&routine.language)
                .is_some_and(|version| version >= 8),
            oracle::preview_routine_language_version(&routine.language)
                .is_some_and(|version| version >= 9),
            &mut params,
            &mut declarations,
            &mut statements,
            &mut exception_handlers,
        )?;
    }
    let symbol_names =
        routine_symbol_names(&params, &declarations, &statements, &exception_handlers);
    bind_routine_expressions(
        &params,
        oracle::preview_routine_language_version(&routine.language).is_none(),
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
        oracle_date_return,
        oracle_statement_rollback: oracle::preview_routine_language_version(&routine.language)
            .is_some_and(|version| version >= 6),
    })
}

pub(crate) fn compile_cached_routine_ir(routine: &RoutineSchema) -> Result<Arc<RoutineIR>> {
    compile_cached_routine_ir_in_catalog(routine, None)
}

pub(crate) fn routine_compilation_scope(db: &BicDb) -> RoutineCompilationScope {
    (
        db.instance_id(),
        db.collection_generation(ROUTINE_COLLECTION),
        db.collection_generation(SCHEMA_COLLECTION),
        db.collection_generation(VIEW_COLLECTION),
        db.collection_generation(USER_TYPE_COLLECTION),
        db.index_generation(),
    )
}

pub(crate) fn compile_cached_routine_ir_in_catalog(
    routine: &RoutineSchema,
    db: Option<&BicDb>,
) -> Result<Arc<RoutineIR>> {
    let mut key = routine_ir_cache_key(routine);
    if oracle::typed_preview_routine_language(&routine.language) {
        key.catalog_scope = Some(routine_compilation_scope(db.ok_or_else(|| {
            SqlError::Unsupported(
                "typed Oracle routine compilation requires catalog context".into(),
            )
        })?));
    }
    if let Some(cached) = ROUTINE_IR_CACHE.with(|cache| cache.borrow().get(&key).cloned()) {
        return Ok(cached);
    }
    let ir = Arc::new(compile_routine_ir_in_catalog(routine, db)?);
    ROUTINE_IR_CACHE.with(|cache| {
        cache.borrow_mut().insert(key, ir.clone());
    });
    Ok(ir)
}

pub(crate) fn routine_ir_cache_key(routine: &RoutineSchema) -> RoutineIrCacheKey {
    RoutineIrCacheKey {
        kind: routine.kind,
        name: routine.name.clone(),
        args: routine.args.clone(),
        return_type: routine.return_type.clone(),
        returns_set: routine.returns_set,
        language: routine.language.to_ascii_lowercase(),
        definition: routine.definition.clone(),
        catalog_scope: None,
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
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                for declaration in declarations {
                    collect_routine_decl_symbols(declaration, names, seen);
                }
                collect_routine_stmt_symbols(body, names, seen);
                for handler in handlers {
                    collect_routine_stmt_symbols(&handler.statements, names, seen);
                }
            }
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
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for arm in arms {
                    collect_routine_stmt_symbols(arm, names, seen);
                }
                if let Some(body) = else_body {
                    collect_routine_stmt_symbols(body, names, seen);
                }
            }
            RoutineStmt::ForLoop { iterator, body, .. } => {
                add_routine_symbol(iterator, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::ForeachLoop { target, body, .. } => {
                add_routine_symbol(target, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::QueryForLoop { target, body, .. }
            | RoutineStmt::CursorForLoop { target, body, .. } => {
                add_routine_symbol(target, names, seen);
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::Loop { body } | RoutineStmt::WhileLoop { body, .. } => {
                collect_routine_stmt_symbols(body, names, seen);
            }
            RoutineStmt::FetchCursor { targets, .. } => {
                for target in targets {
                    add_routine_symbol(target, names, seen);
                }
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::ExitLoop
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
        RoutineAssignmentTarget::Variable(name) => {
            add_routine_symbol(name.split('.').next().unwrap_or(name), names, seen)
        }
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
    params: &[RoutineParam],
    declared_types: bool,
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
    let mut types = Vec::new();
    if declared_types {
        for param in params {
            if let Some(kind) = &param.type_schema {
                types.push((format!("${}", param.index + 1), kind.pg_type.clone()));
                if let Some(name) = &param.name {
                    types.push((name.clone(), kind.pg_type.clone()));
                }
            }
        }
        for declaration in declarations.iter() {
            match declaration {
                RoutineDecl::Variable {
                    name,
                    pg_type: Some(kind),
                    ..
                } => types.push((name.clone(), kind.clone())),
                RoutineDecl::Alias { name, position } => {
                    if let Some((_, kind)) =
                        types.iter().find(|(key, _)| key == &format!("${position}"))
                    {
                        types.push((name.clone(), kind.clone()));
                    }
                }
                _ => {}
            }
        }
    }
    let scope = BoundExprScope::new(&[], symbol_names)
        .with_array_vars(&array_vars)
        .with_var_types(&types);
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
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                let types = declarations
                    .iter()
                    .filter_map(|decl| match decl {
                        RoutineDecl::Variable {
                            name,
                            pg_type: Some(kind),
                            ..
                        } => Some((name.clone(), kind.clone())),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let mut local_scope = scope.clone().with_var_types(&types);
                for declaration in declarations.iter() {
                    let (name, kind) = match declaration {
                        RoutineDecl::Variable { name, pg_type, .. } => (name, pg_type.clone()),
                        RoutineDecl::Alias { name, position } => {
                            let kind = scope
                                .exact_vars
                                .get(&format!("${position}"))
                                .and_then(|id| scope.var_types.as_ref()?.get(id))
                                .cloned();
                            (name, kind)
                        }
                        RoutineDecl::Cursor { .. } => continue,
                    };
                    if let Some(id) = local_scope.exact_vars.get(name).copied() {
                        Arc::make_mut(&mut local_scope.array_vars).remove(&id);
                        if let Some(kind) = kind {
                            if kind.ends_with("[]") {
                                Arc::make_mut(&mut local_scope.array_vars).insert(id);
                            }
                            let types = local_scope
                                .var_types
                                .get_or_insert_with(|| Arc::new(FxHashMap::default()));
                            Arc::make_mut(types).insert(id, kind);
                        }
                    }
                }
                for declaration in declarations {
                    if let RoutineDecl::Variable {
                        default_expr: Some(expr),
                        ..
                    } = declaration
                    {
                        expr.bind(&local_scope);
                    }
                }
                bind_routine_statement_exprs(body, &local_scope);
                for handler in handlers {
                    bind_routine_statement_exprs(&mut handler.statements, &local_scope);
                }
            }
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
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                expression.bind(scope);
                for arm in arms {
                    bind_routine_statement_exprs(arm, scope);
                }
                if let Some(body) = else_body {
                    bind_routine_statement_exprs(body, scope);
                }
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
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    arg.expr.bind(scope);
                }
                bind_routine_statement_exprs(body, scope);
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    arg.expr.bind(scope);
                }
            }
            RoutineStmt::QueryForLoop { body, .. } | RoutineStmt::Loop { body } => {
                bind_routine_statement_exprs(body, scope);
            }
            RoutineStmt::WhileLoop { condition, body } => {
                condition.bind(scope);
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
            | RoutineStmt::ExitLoop
            | RoutineStmt::Null
            | RoutineStmt::SelectInto { .. }
            | RoutineStmt::Perform { .. }
            | RoutineStmt::Sql(_)
            | RoutineStmt::SqlInto { .. }
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
    let details = raw_routine_argument_type_details(&arg).ok();
    let declared_date = details.as_ref().is_some_and(|(_, date)| *date);
    let type_schema = details.map(|(schema, _)| schema).map(|mut schema| {
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
        declared_date,
        oracle_date: false,
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
    let (declarations, statements, exception_handlers) = parse_plpgsql_block(body)?;
    validate_routine_continue(&statements, false)?;
    for handler in &exception_handlers {
        validate_routine_continue(&handler.statements, false)?;
    }
    Ok((declarations, statements, exception_handlers))
}

fn parse_plpgsql_block(
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
    let mut depth = 0usize;
    for (start, _, event) in routine_boundaries(block_sql).ok()? {
        match event {
            RoutineBoundary::Begin
            | RoutineBoundary::If
            | RoutineBoundary::Loop
            | RoutineBoundary::Case => depth += 1,
            RoutineBoundary::End
            | RoutineBoundary::EndIf
            | RoutineBoundary::EndLoop
            | RoutineBoundary::EndCase => depth = depth.saturating_sub(1),
            RoutineBoundary::Exception if depth == 0 => return Some(start),
            _ => {}
        }
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
        let mut depth = 0usize;
        let next_when = routine_boundaries(after_then)?
            .into_iter()
            .find_map(|(start, _, kind)| {
                match kind {
                    RoutineBoundary::Begin
                    | RoutineBoundary::If
                    | RoutineBoundary::Loop
                    | RoutineBoundary::Case => depth += 1,
                    RoutineBoundary::End
                    | RoutineBoundary::EndIf
                    | RoutineBoundary::EndLoop
                    | RoutineBoundary::EndCase => depth = depth.saturating_sub(1),
                    RoutineBoundary::When if depth == 0 => return Some(start),
                    _ => {}
                }
                None
            });
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
        "case_not_found" => Ok(RoutineExceptionCondition::SqlState("20000".to_string())),
        "division_by_zero" | "zero_divide" => {
            Ok(RoutineExceptionCondition::SqlState("22012".into()))
        }
        "unique_violation" | "dup_val_on_index" => {
            Ok(RoutineExceptionCondition::SqlState("23505".into()))
        }
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
        let (params, cursor_rest) = parameterized_cursors::parse_parameters(cursor_rest)?;
        let Some(query_sql) = strip_prefix_ci(cursor_rest.trim_start(), "FOR") else {
            return Err(SqlError::UnsupportedPlpgsqlFeature(
                "cursor declarations must use CURSOR FOR query".to_string(),
            ));
        };
        return Ok(RoutineDecl::Cursor {
            name: normalize_object_name(&name),
            params,
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
    let details = raw_routine_type_details(type_declaration).ok();
    let declared_date = details.as_ref().is_some_and(|(_, date)| *date);
    let type_schema = details.map(|(schema, _)| schema);
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
        declared_date,
        oracle_date: false,
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
    if starts_with_plpgsql_word(value, "BEGIN") || starts_with_plpgsql_word(value, "DECLARE") {
        let end = matching_routine_end(
            value,
            RoutineBoundary::Begin,
            RoutineBoundary::End,
            "block is missing END",
        )?;
        return Ok((&original[leading_len..leading_len + end], leading_len + end));
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
    if starts_with_plpgsql_word(value, "CASE") {
        let end = plpgsql_case_statement(value)?.end;
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
    // `WHILE cond LOOP ... END LOOP` and a bare `LOOP ... END LOOP` are one statement each.
    let opens = |keyword: &str, extra: char| {
        value
            .get(..keyword.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(keyword))
            && value.get(keyword.len()..).is_some_and(|tail| {
                tail.chars()
                    .next()
                    .is_none_or(|ch| ch.is_whitespace() || ch == extra)
            })
    };
    if opens("WHILE", '(') || opens("LOOP", ';') {
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
    Begin,
    End,
    Exception,
    If,
    EndIf,
    Loop,
    EndLoop,
    Case,
    EndCase,
    When,
    Then,
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
    let mut case_header = false;
    let mut index = 0;
    while index < tokens.len() {
        let current = word(index);
        // SQL CASE expressions have a bare END. Procedural CASE starts at a
        // statement boundary and ends with END CASE. Neither exposes its SQL
        // WHEN/ELSE keywords as procedural branch delimiters.
        if current == "CASE" && (case_depth > 0 || !start) {
            case_depth += 1;
        } else if case_depth > 0 {
            if current == "END" {
                case_depth -= 1;
            }
        } else {
            let next = word(index + 1);
            if current == "END" && matches!(next.as_str(), "IF" | "LOOP" | "CASE") {
                let kind = match next.as_str() {
                    "IF" => RoutineBoundary::EndIf,
                    "LOOP" => RoutineBoundary::EndLoop,
                    _ => RoutineBoundary::EndCase,
                };
                let begin = offset(tokens[index].span.start);
                index += 1;
                events.push((begin, offset(tokens[index].span.end), kind));
                start = false;
                index += 1;
                continue;
            }
            let event = if start && current == "BEGIN" {
                Some(RoutineBoundary::Begin)
            } else if current == "END" {
                Some(RoutineBoundary::End)
            } else if start && current == "EXCEPTION" {
                Some(RoutineBoundary::Exception)
            } else if start && current == "IF" {
                Some(RoutineBoundary::If)
            } else if start && current == "CASE" {
                case_header = true;
                Some(RoutineBoundary::Case)
            } else if current == "WHEN" && (start || case_header) {
                case_header = false;
                Some(RoutineBoundary::When)
            } else if current == "THEN" {
                Some(RoutineBoundary::Then)
            } else if start && matches!(current.as_str(), "FOR" | "FOREACH" | "WHILE") {
                loop_header = true;
                Some(RoutineBoundary::Loop)
            } else if start && current == "LOOP" {
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
            let starts_body = matches!(current.as_str(), "THEN" | "ELSE" | "BEGIN" | "EXCEPTION")
                || event == Some(RoutineBoundary::Case)
                || (current == "LOOP" && (loop_header || start));
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
    if starts_with_plpgsql_word(statement, "BEGIN")
        || starts_with_plpgsql_word(statement, "DECLARE")
    {
        let (declarations, body, handlers) = parse_plpgsql_block(statement)?;
        return Ok(RoutineStmt::Block {
            declarations,
            body,
            handlers,
        });
    }
    if keyword_matches_at(statement, "CASE", 0) {
        return procedural_case::parse(statement);
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
        return parse_routine_plain_loop(statement);
    }
    if statement
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WHILE"))
        && statement.get(5..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|ch| ch.is_whitespace() || ch == '(')
        })
    {
        return parse_routine_while_loop(statement);
    }
    if keyword_matches_at(statement, "EXIT", 0) {
        let rest = statement["EXIT".len()..]
            .trim()
            .trim_end_matches(';')
            .trim();
        if rest.is_empty() {
            return Ok(RoutineStmt::ExitLoop);
        }
        if keyword_matches_at(rest, "WHEN", 0) {
            return Ok(RoutineStmt::If {
                condition: RoutineExpr::unbound(parse_routine_expr(rest[4..].trim())?),
                then_body: vec![RoutineStmt::ExitLoop],
                else_body: Vec::new(),
            });
        }
        return Err(SqlError::UnsupportedPlpgsqlFeature(
            "EXIT with a label is not implemented".to_string(),
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
        let (name, arguments) = parameterized_cursors::parse_call(rest.trim())?;
        return Ok(RoutineStmt::OpenCursor { name, arguments });
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
    if !starts_with_plpgsql_word(statement, "EXCEPTION") {
        if let Ok(RoutineExceptionCondition::SqlState(sqlstate)) =
            routine_exception_condition(statement)
        {
            return Ok(RoutineStmt::RaiseException {
                message: normalize_object_name(statement),
                arguments: Vec::new(),
                detail: None,
                sqlstate,
                error_fields: Box::default(),
            });
        }
    }
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
    let mut error_fields = RaiseErrorFields::default();
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
                "constraint" => error_fields.constraint = Some(parsed),
                "table" => error_fields.table = Some(parsed),
                "column" => error_fields.column = Some(parsed),
                // Reported by PostgreSQL only as diagnostic fields BicDB does
                // not send; accepted so the routine compiles.
                "schema" | "datatype" | "hint" => {}
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
        error_fields: Box::new(error_fields),
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
        let tokens = Tokenizer::new(&PostgreSqlDialect {}, range_sql)
            .tokenize()
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_)))
            .collect::<Vec<_>>();
        if matches!(tokens.as_slice(), [Token::Word(_)])
            || matches!(tokens.as_slice(), [Token::Word(word), Token::LParen, ..] if word.keyword != sqlparser::keywords::Keyword::SELECT)
        {
            let (cursor, arguments) = parameterized_cursors::parse_call(range_sql)?;
            return Ok(RoutineStmt::CursorForLoop {
                target: iterator,
                cursor,
                arguments,
                body,
            });
        }
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

/// The statements between a loop's `LOOP` keyword and its closing `END LOOP`.
fn routine_loop_body(after_loop: &str) -> Result<Vec<RoutineStmt>> {
    let body = after_loop.trim().trim_end_matches(';').trim_end();
    let lower = body.to_ascii_lowercase();
    let Some(end) = lower.strip_suffix("end loop").map(str::len) else {
        return Err(SqlError::InvalidSql("LOOP requires END LOOP".to_string()));
    };
    parse_routine_statements(body[..end].trim())
}

pub(crate) fn parse_routine_plain_loop(statement: &str) -> Result<RoutineStmt> {
    let after = strip_prefix_ci(statement.trim(), "LOOP")
        .ok_or_else(|| SqlError::InvalidSql("LOOP expected".to_string()))?;
    Ok(RoutineStmt::Loop {
        body: routine_loop_body(after)?,
    })
}

pub(crate) fn parse_routine_while_loop(statement: &str) -> Result<RoutineStmt> {
    let inner = strip_prefix_ci(statement.trim(), "WHILE")
        .ok_or_else(|| SqlError::InvalidSql("WHILE loop expected".to_string()))?
        .trim();
    let Some(loop_idx) = find_top_level_keyword(inner, "LOOP") else {
        return Err(SqlError::InvalidSql("WHILE loop requires LOOP".to_string()));
    };
    Ok(RoutineStmt::WhileLoop {
        condition: RoutineExpr::unbound(parse_routine_expr(inner[..loop_idx].trim())?),
        body: routine_loop_body(&inner[loop_idx + "LOOP".len()..])?,
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
        if let Expr::CompoundIdentifier(parts) = parse_routine_expr(target)? {
            return Ok(RoutineAssignmentTarget::Variable(
                parts
                    .iter()
                    .map(|part| normalize_object_name(&part.value))
                    .collect::<Vec<_>>()
                    .join("."),
            ));
        }
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
            RoutineBoundary::Begin
            | RoutineBoundary::If
            | RoutineBoundary::Loop
            | RoutineBoundary::Case => depth += 1,
            RoutineBoundary::End
            | RoutineBoundary::EndIf
            | RoutineBoundary::EndLoop
            | RoutineBoundary::EndCase => depth = depth.saturating_sub(1),
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
            _ if depth == 0 && bytes[idx..].starts_with(operator.as_bytes()) => return Some(idx),
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
    require_user_relation_name(&sequence.name)?;
    if load_sequence(db, &sequence.name)?.is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(SqlError::InvalidSql(format!(
            "relation \"{}\" already exists",
            sequence.name
        )));
    }
    if relation_exists(db, &sequence.name) {
        return Err(SqlError::InvalidSql(format!(
            "relation {} already exists",
            sequence.name
        )));
    }
    require_unused_relation_oid(db, sequence_oid(&sequence.name))?;
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
    let mut unique_definition = column_def.clone();
    unique_definition
        .options
        .retain(|option| matches!(option.option, ColumnOption::Unique(_)));
    let unique_constraints = column_constraints_from_def(
        table,
        &unique_definition,
        &column,
        oracle::default_unique_null_policy(db)?,
    )?;
    let added_unique = !unique_constraints.is_empty();
    let mut updated_schema = schema.clone();
    updated_schema.add_column(column);
    if !column_nullable {
        for record in db.scan_collection(table)? {
            if matches!(
                default.clone().unwrap_or_else(|| record_column_value(
                    &record,
                    &updated_schema,
                    &column_name
                )),
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
    if !unique_constraints.is_empty() {
        let mut candidates = db.scan_collection(table)?;
        if let Some(default) = default.as_ref() {
            for record in &mut candidates {
                set_record_column(record, Some(&updated_schema), &column_name, default.clone())?;
            }
        }
        for constraint in &unique_constraints {
            if table_constraint_name_exists(&updated_schema, constraint_name(constraint)) {
                return Err(SqlError::duplicate_constraint(
                    table,
                    constraint_name(constraint),
                ));
            }
            validate_constraint_columns(db, table, &updated_schema, constraint)?;
            let (ConstraintSchema::Unique { name, columns, .. }
            | ConstraintSchema::UniqueWithNullPolicy { name, columns, .. }) = constraint
            else {
                unreachable!("unique column constraints");
            };
            validate_unique_key_records(
                table,
                &updated_schema,
                name,
                columns,
                &candidates,
                false,
                constraint.unique_null_policy(),
            )?;
        }
        updated_schema.constraints.extend(unique_constraints);
        validate_unique_constraint_index_compatibility(db, &updated_schema)?;
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
    if added_unique {
        ensure_unique_constraint_indexes_for_schema(db, &updated_schema)?;
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
        ConstraintSchema::Unique { columns, .. }
        | ConstraintSchema::UniqueWithNullPolicy { columns, .. }
        | ConstraintSchema::ForeignKey { columns, .. } => !columns
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&actual)),
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
    require_user_relation_name(new_table)?;
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
    // Outside a transaction block the rename is catalog-only: rows, indexes
    // and flashback history stay where they are. Inside one, the copy path
    // below provides the undo image; it is also the fallback when the
    // storage layer cannot rename this collection in place.
    if !track_undo {
        match db.rename_collection(old_table, new_table) {
            Ok(()) => {
                let sequences = list_sequences(db)?
                    .into_iter()
                    .filter(|sequence| sequence.owned_by_table.as_deref() == Some(old_table))
                    .collect::<Vec<_>>();
                delete_schema(db, old_table)?;
                schema.name = new_table.to_string();
                if schema.primary_key_name.is_none() {
                    schema.primary_key_name = primary_key_name;
                }
                for mut sequence in sequences {
                    sequence.owned_by_table = Some(new_table.to_string());
                    save_sequence(db, &sequence)?;
                }
                save_schema(db, schema)?;
                sql_index_definitions_cache_clear();
                return Ok(None);
            }
            Err(BicDbError::Unsupported(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
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
                nulls_not_distinct: false,
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
            RoutineStmt::Block { body, handlers, .. } => {
                validate_routine_continue(body, in_loop)?;
                for handler in handlers {
                    validate_routine_continue(&handler.statements, in_loop)?;
                }
            }
            RoutineStmt::ContinueLoop if !in_loop => {
                return Err(SqlError::InvalidSql(
                    "CONTINUE cannot be used outside a loop".into(),
                ))
            }
            RoutineStmt::ExitLoop if !in_loop => {
                return Err(SqlError::InvalidSql(
                    "EXIT cannot be used outside a loop".into(),
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
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for arm in arms {
                    validate_routine_continue(arm, in_loop)?;
                }
                if let Some(body) = else_body {
                    validate_routine_continue(body, in_loop)?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body }
            | RoutineStmt::WhileLoop { body, .. } => validate_routine_continue(body, true)?,
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod cursor_loop_tests {
    use super::*;

    #[test]
    fn bound_cursor_loops_rebind_parameters_close_and_support_loop_control() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE cursor_values(k INT PRIMARY KEY,bucket INT,n INT); INSERT INTO cursor_values VALUES(1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,2,8)").unwrap();
        session
            .execute(
                r#"CREATE FUNCTION cursor_control(p_bucket INT) RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values WHERE bucket=p_bucket ORDER BY k;
                    total INT := 0; first_value INT;
            BEGIN
                FOR r IN c LOOP
                    IF r.n=2 THEN CONTINUE; END IF;
                    total:=total+r.n;
                    EXIT WHEN r.n>=3;
                END LOOP;
                OPEN c; FETCH c INTO first_value; CLOSE c;
                RETURN total*10+coalesce(first_value,0);
            END$$"#,
            )
            .unwrap();
        for (bucket, expected) in [(1, 41), (2, 88), (3, 0), (1, 41)] {
            let rows = session
                .execute(&format!("SELECT cursor_control({bucket})"))
                .unwrap();
            assert_eq!(rows.rows[0][0], SqlValue::Int(expected));
        }
        session
            .execute(
                r#"CREATE FUNCTION cursor_found(p_bucket INT) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values WHERE bucket=p_bucket;
            BEGIN FOR r IN c LOOP NULL; END LOOP; RETURN found; END$$"#,
            )
            .unwrap();
        for (bucket, expected) in [(1, true), (3, false), (2, true)] {
            assert_eq!(
                session
                    .execute(&format!("SELECT cursor_found({bucket})"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::Bool(expected)
            );
        }
    }

    #[test]
    fn cursor_loop_errors_close_owned_cursors_and_preserve_prior_opens() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE cursor_values(n INT); INSERT INTO cursor_values VALUES(7),(9)")
            .unwrap();
        session
            .execute(
                r#"CREATE FUNCTION cursor_error_recovery() RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values ORDER BY n; v INT; z INT;
            BEGIN
                FOR r IN c LOOP z:=1/(r.n-r.n); END LOOP;
                RETURN 0;
            EXCEPTION WHEN SQLSTATE '22012' THEN
                OPEN c; FETCH c INTO v; CLOSE c; RETURN v;
            END$$"#,
            )
            .unwrap();
        session
            .execute(
                r#"CREATE FUNCTION cursor_prior_open() RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values ORDER BY n; v INT;
            BEGIN OPEN c; FOR r IN c LOOP NULL; END LOOP; RETURN 0;
            EXCEPTION WHEN SQLSTATE '42P03' THEN FETCH c INTO v; CLOSE c; RETURN v;
            END$$"#,
            )
            .unwrap();
        for function in ["cursor_error_recovery", "cursor_prior_open"] {
            for _ in 0..2 {
                assert_eq!(
                    session
                        .execute(&format!("SELECT {function}()"))
                        .unwrap()
                        .rows[0][0],
                    SqlValue::Int(7)
                );
            }
        }
    }

    #[test]
    fn nested_bound_cursors_resolve_outer_record_parameters() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE cursor_values(bucket INT,n INT); INSERT INTO cursor_values VALUES(1,2),(1,3),(2,5)").unwrap();
        session.execute(r#"CREATE FUNCTION nested_cursors() RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE parents CURSOR FOR SELECT DISTINCT bucket FROM cursor_values ORDER BY bucket;
                    children CURSOR FOR SELECT n FROM cursor_values WHERE bucket=r.bucket ORDER BY n;
                    total INT:=0;
            BEGIN
                FOR r IN parents LOOP
                    FOR child IN children LOOP total:=total+r.bucket*child.n; END LOOP;
                END LOOP;
                RETURN total;
            END$$"#).unwrap();
        for _ in 0..2 {
            assert_eq!(
                session.execute("SELECT nested_cursors()").unwrap().rows[0][0],
                SqlValue::Int(15)
            );
        }
    }

    #[test]
    fn quoted_cursor_parameters_resolve_after_row_columns() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE cursor_values(bucket INT,n INT); INSERT INTO cursor_values VALUES(1,5),(2,9)").unwrap();
        session
            .execute(
                r#"CREATE FUNCTION quoted_cursor("P_BUCKET" INT) RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values WHERE bucket="P_BUCKET";
                    total INT:=0;
            BEGIN FOR r IN c LOOP total:=total+r.n; END LOOP; RETURN total; END$$"#,
            )
            .unwrap();
        session
            .execute(
                r#"CREATE FUNCTION quoted_column("BUCKET" INT) RETURNS INT LANGUAGE plpgsql AS $$
            DECLARE c CURSOR FOR SELECT n FROM cursor_values WHERE "bucket"=1;
                    total INT:=0;
            BEGIN FOR r IN c LOOP total:=total+r.n; END LOOP; RETURN total; END$$"#,
            )
            .unwrap();
        for (query, expected) in [
            ("SELECT quoted_cursor(1)", 5),
            ("SELECT quoted_cursor(2)", 9),
            ("SELECT quoted_column(2)", 5),
        ] {
            assert_eq!(
                session.execute(query).unwrap().rows[0][0],
                SqlValue::Int(expected)
            );
        }
    }
}

#[cfg(test)]
mod temporal_type_tests {
    use crate::*;

    #[test]
    fn postgres_routines_preserve_declared_calendar_and_temporal_operator_types() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(directory.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for ddl in [
            "CREATE FUNCTION native_month(p_stamp TIMESTAMP,p_step INTERVAL) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN p_stamp+p_step; END$$",
            "CREATE FUNCTION native_local(p_stamp TIMESTAMP) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$DECLARE p ALIAS FOR $1; shifted TIMESTAMP:=p+INTERVAL '1 month'; step INTERVAL:=INTERVAL '1 day'; BEGIN RETURN shifted+step; END$$",
            "CREATE FUNCTION native_date_shift(p_day DATE,p_days INT) RETURNS DATE LANGUAGE plpgsql AS $$BEGIN RETURN p_day+p_days; END$$",
            "CREATE FUNCTION native_date_month(p_day DATE,p_step INTERVAL) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN p_day+p_step; END$$",
            "CREATE FUNCTION native_interval_literal() RETURNS INTERVAL LANGUAGE plpgsql AS $$BEGIN RETURN INTERVAL '1 month'; END$$",
            "CREATE FUNCTION native_delta(p_left TIMESTAMP,p_right TIMESTAMP) RETURNS INTERVAL LANGUAGE plpgsql AS $$BEGIN RETURN p_left-p_right; END$$",
        ] {session.execute(ddl).unwrap();}
        for (sql,expected) in [
            ("SELECT native_month(TIMESTAMP '2024-02-29 12:34:56.9',INTERVAL '1 month')","2024-03-29 12:34:56.9"),
            ("SELECT native_local(TIMESTAMP '2024-02-29 12:34:56.9')","2024-03-30 12:34:56.9"),
            ("SELECT native_date_shift(DATE '2024-02-29',1)","2024-03-01"),
            ("SELECT native_date_month(DATE '2024-02-29',INTERVAL '1 month')","2024-03-29 00:00:00"),
            ("SELECT native_delta(TIMESTAMP '2024-03-01 00:00:01.123456',TIMESTAMP '2024-03-01 00:00:00.123455')","00:00:01.000001"),
        ] {assert_eq!(session.execute(sql).unwrap().rows[0][0].to_cell(),expected,"{sql}");}
        for sql in [
            "SELECT native_month(NULL,INTERVAL '1 month')",
            "SELECT native_month(TIMESTAMP '2024-02-29 12:34:56.9',NULL)",
        ] {
            assert_eq!(session.execute(sql).unwrap().rows[0][0], SqlValue::Null);
        }
        let default_interval =
            session.execute("SELECT INTERVAL '1 month'").unwrap().rows[0][0].clone();
        assert_eq!(
            session
                .execute("SELECT native_interval_literal()")
                .unwrap()
                .rows[0][0],
            default_interval
        );
        session.execute("SET intervalstyle=iso_8601").unwrap();
        assert_eq!(
            session
                .execute("SELECT native_interval_literal()")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "P1M"
        );
        session.execute("RESET intervalstyle").unwrap();
        session.execute("DO $$DECLARE stamp TIMESTAMP:=TIMESTAMP '2024-02-29 12:34:56.9'; shifted TIMESTAMP:=stamp+INTERVAL '1 month'; BEGIN IF shifted<>TIMESTAMP '2024-03-29 12:34:56.9' THEN RAISE EXCEPTION 'native temporal type was lost'; END IF; END$$").unwrap();
    }
}

fn starts_with_plpgsql_word(value: &str, word: &str) -> bool {
    value
        .get(..word.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(word))
        && value[word.len()..]
            .chars()
            .next()
            .is_none_or(|ch| ch.is_whitespace() || ch == '(')
}

/// Byte ranges of a PL/pgSQL CASE statement (`value` starts with CASE).
struct PlpgsqlCaseStatement {
    /// The simple-CASE operand, empty for a searched CASE.
    operand: std::ops::Range<usize>,
    /// (WHEN condition, THEN body) per clause.
    clauses: Vec<(std::ops::Range<usize>, std::ops::Range<usize>)>,
    else_body: Option<std::ops::Range<usize>>,
    /// Offset just past END CASE.
    end: usize,
}

/// Splits a CASE statement at its own WHEN / THEN / ELSE / END CASE, skipping
/// the keywords of nested CASE expressions and statements, IF and LOOP blocks
/// and BEGIN ... EXCEPTION WHEN blocks.
fn plpgsql_case_statement(value: &str) -> Result<PlpgsqlCaseStatement> {
    #[derive(PartialEq)]
    enum Block {
        Case,
        If,
        Loop,
        Begin,
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, value)
        .tokenize_with_location()
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?
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
    if word(0) != "CASE" {
        return Err(SqlError::InvalidSql("CASE statement expected".to_string()));
    }
    let start_of = |index: usize| offset(tokens[index].span.start);
    let end_of = |index: usize| offset(tokens[index].span.end);
    let mut stack: Vec<Block> = Vec::new();
    let mut operand_end = None;
    let mut clauses = Vec::new();
    let mut when_start: Option<usize> = None;
    let mut body_start: Option<(std::ops::Range<usize>, usize)> = None;
    let mut else_start: Option<usize> = None;
    let mut start = false;
    let mut loop_header = false;
    let mut index = 1;
    let close_body = |clauses: &mut Vec<_>,
                      body_start: &mut Option<(std::ops::Range<usize>, usize)>,
                      at: usize| {
        if let Some((condition, body)) = body_start.take() {
            clauses.push((condition, body..at));
        }
    };
    while index < tokens.len() {
        let current = word(index);
        if stack.is_empty() {
            match current.as_str() {
                "WHEN" => {
                    if operand_end.is_none() {
                        operand_end = Some(start_of(index));
                    }
                    close_body(&mut clauses, &mut body_start, start_of(index));
                    when_start = Some(end_of(index));
                    index += 1;
                    start = false;
                    continue;
                }
                "THEN" if when_start.is_some() => {
                    let condition = when_start.take().unwrap()..start_of(index);
                    body_start = Some((condition, end_of(index)));
                    index += 1;
                    start = true;
                    continue;
                }
                "ELSE" if when_start.is_none() && body_start.is_some() => {
                    close_body(&mut clauses, &mut body_start, start_of(index));
                    else_start = Some(end_of(index));
                    index += 1;
                    start = true;
                    continue;
                }
                "END" if word(index + 1) == "CASE" => {
                    let at = start_of(index);
                    close_body(&mut clauses, &mut body_start, at);
                    if clauses.is_empty() {
                        return Err(SqlError::InvalidSql(
                            "CASE statement requires at least one WHEN".to_string(),
                        ));
                    }
                    return Ok(PlpgsqlCaseStatement {
                        operand: end_of(0)..operand_end.unwrap_or(end_of(0)),
                        clauses,
                        else_body: else_start.map(|begin| begin..at),
                        end: end_of(index + 1),
                    });
                }
                _ => {}
            }
        }
        match current.as_str() {
            "CASE" => stack.push(Block::Case),
            "END" => {
                let next = word(index + 1);
                match next.as_str() {
                    "CASE" | "IF" | "LOOP" => {
                        stack.pop();
                        index += 1;
                    }
                    _ => {
                        stack.pop();
                    }
                }
            }
            "IF" if start => stack.push(Block::If),
            "FOR" | "FOREACH" | "WHILE" if start => {
                loop_header = true;
                stack.push(Block::Loop);
            }
            "LOOP" if start && !loop_header => stack.push(Block::Loop),
            "BEGIN" if start => stack.push(Block::Begin),
            _ => {}
        }
        let starts_body = matches!(current.as_str(), "THEN" | "ELSE" | "BEGIN" | "LOOP");
        if current == "LOOP" {
            loop_header = false;
        }
        start = starts_body || matches!(tokens[index].token, Token::SemiColon);
        index += 1;
    }
    Err(SqlError::InvalidSql(
        "CASE statement is missing END CASE".to_string(),
    ))
}
