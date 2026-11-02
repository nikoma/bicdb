//! Bound cursor formal arguments have query-local scope and OPEN-time values.
use super::*;

pub(super) fn parse_parameters(rest: &str) -> Result<(Vec<RoutineParam>, &str)> {
    if !rest.starts_with('(') {
        return Ok((Vec::new(), rest));
    }
    let close = find_matching_paren_nested(rest, 0)
        .ok_or_else(|| SqlError::InvalidSql("unterminated cursor parameter list".into()))?;
    let mut names = BTreeSet::new();
    let mut params = Vec::new();
    let body = rest[1..close].trim();
    if !body.is_empty() {
        for (index, declaration) in split_top_level_commas_nested(body).iter().enumerate() {
            let declaration = match find_top_level_operator(declaration, ":=") {
                Some(at) => format!("{} DEFAULT {}", &declaration[..at], &declaration[at + 2..]),
                None => declaration.clone(),
            };
            let mut param = parse_routine_param(&declaration, index)?;
            let name = param.name.as_ref().ok_or_else(|| {
                SqlError::InvalidSql("cursor parameter requires a name and type".into())
            })?;
            let name = normalize_object_name(name);
            if !names.insert(name.clone()) {
                return Err(SqlError::InvalidSql(
                    "duplicate cursor parameter name".into(),
                ));
            }
            if param.mode != RoutineArgMode::In || param.type_schema.is_none() {
                return Err(SqlError::UnsupportedPlpgsqlFeature(
                    "cursor parameters require supported IN types".into(),
                ));
            }
            param.name = Some(name);
            params.push(param);
        }
    }
    // A formal is not denotable while another formal's default is parsed,
    // even if an enclosing variable has the same spelling (Oracle PLS-00227).
    for param in &params {
        if let Some(default) = &param.default_expr {
            let referenced = sqlparser::ast::visit_expressions(default, |expr| {
                if matches!(expr, Expr::Identifier(ident) if names.contains(&normalize_object_name(&ident.value)))
                {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
            if referenced.is_break() {
                return Err(SqlError::InvalidSql(
                    "cursor formal is not denotable in a parameter default".into(),
                ));
            }
        }
    }
    Ok((params, &rest[close + 1..]))
}

pub(super) fn parse_call(source: &str) -> Result<(String, Vec<RoutineCursorArgument>)> {
    let source = source.trim();
    let (name, rest) = if source.starts_with('"') {
        parse_leading_sql_identifier(source)?
    } else {
        let end = source
            .find(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '$')
            .unwrap_or(source.len());
        if end == 0 {
            return Err(SqlError::InvalidSql("expected cursor identifier".into()));
        }
        (source[..end].to_string(), &source[end..])
    };
    let rest = rest.trim();
    let mut arguments = Vec::new();
    if !rest.is_empty() {
        if !rest.starts_with('(') {
            return Err(SqlError::InvalidSql("expected cursor argument list".into()));
        }
        let close = find_matching_paren_nested(rest, 0)
            .ok_or_else(|| SqlError::InvalidSql("unterminated cursor argument list".into()))?;
        if !rest[close + 1..].trim().is_empty() {
            return Err(SqlError::InvalidSql(
                "unexpected text after cursor arguments".into(),
            ));
        }
        let body = rest[1..close].trim();
        if !body.is_empty() {
            let mut named = false;
            for argument in split_top_level_commas_nested(body) {
                let separator = find_top_level_operator(&argument, "=>")
                    .or_else(|| find_top_level_operator(&argument, ":="));
                let (name, expression) = if let Some(at) = separator {
                    named = true;
                    (
                        Some(parse_single_routine_identifier(
                            argument[..at].trim(),
                            "cursor argument",
                        )?),
                        argument[at + 2..].trim(),
                    )
                } else {
                    if named {
                        return Err(SqlError::InvalidSql(
                            "positional cursor argument follows a named argument".into(),
                        ));
                    }
                    (None, argument.trim())
                };
                arguments.push(RoutineCursorArgument {
                    name,
                    expr: RoutineExpr::unbound(parse_routine_expr(expression)?),
                });
            }
        }
    }
    Ok((normalize_object_name(&name), arguments))
}

pub(crate) fn prepare_parameters(params: &mut [RoutineParam]) -> Result<()> {
    for param in params {
        if param.declared_date {
            param.oracle_date = true;
            param.type_schema = Some(raw_routine_type_schema("timestamp")?);
        }
    }
    Ok(())
}

impl SqlSession<'_> {
    pub(crate) fn execute_parameterized_cursor_query(
        &mut self,
        frame: &mut RoutineFrame,
        cursor_name: &str,
        query: &Query,
        params: &[RoutineParam],
        arguments: &[RoutineCursorArgument],
    ) -> Result<SqlResult> {
        if params.is_empty() && arguments.is_empty() {
            return self.execute_routine_query_unowned(frame, query);
        }
        // Validate the complete argument mapping before evaluating expressions.
        let mut supplied = vec![None; params.len()];
        let mut positional = 0;
        for (actual, argument) in arguments.iter().enumerate() {
            let index = if let Some(name) = &argument.name {
                params
                    .iter()
                    .position(|param| param.name.as_ref() == Some(name))
                    .ok_or_else(|| {
                        SqlError::InvalidSql(format!("unknown cursor parameter {name}"))
                    })?
            } else {
                let index = positional;
                positional += 1;
                index
            };
            let slot = supplied
                .get_mut(index)
                .ok_or_else(|| SqlError::InvalidSql("too many cursor arguments".into()))?;
            if slot.replace(actual).is_some() {
                return Err(SqlError::InvalidSql(
                    "cursor parameter supplied more than once".into(),
                ));
            }
        }
        if params
            .iter()
            .zip(&supplied)
            .any(|(param, arg)| arg.is_none() && param.default_expr.is_none())
        {
            return Err(SqlError::InvalidSql(
                "missing required cursor argument".into(),
            ));
        }
        if !frame.oracle_statement_rollback {
            validate_native_parameter_columns(self.db_ref(), query, params)?;
        }
        // Actuals are evaluated once in source order, in the caller's scope.
        let actual_values = arguments
            .iter()
            .map(|arg| self.eval_routine_expr(frame, &arg.expr))
            .collect::<Result<Vec<_>>>()?;
        let mut values = Vec::with_capacity(params.len());
        for (param, supplied) in params.iter().zip(supplied) {
            let value = match supplied {
                Some(index) => actual_values[index].clone(),
                None => self.eval_routine_expr(
                    frame,
                    &RoutineExpr::unbound(param.default_expr.clone().expect("validated default")),
                )?,
            };
            let value = if param.oracle_date {
                crate::pg_scalar_extra::oracle_date_coerce(value)?
            } else {
                value
            };
            let value = coerce_routine_assignment(
                value,
                param
                    .type_schema
                    .as_ref()
                    .expect("validated parameter type"),
            )?;
            values.push(value);
        }
        // Keep query-local names/types out of the caller and loop body, without
        // cloning materialized cursor row sets. Restore on query errors too.
        let saved_values = frame.materialized_values();
        let saved_slots = std::rc::Rc::clone(&frame.slot_values);
        let saved_types = Arc::clone(&frame.assignment_types);
        let saved_dates = frame.oracle_dates.clone();
        let mut qualified = JsonMap::new();
        for (param, value) in params.iter().zip(values) {
            let name = param.name.as_ref().expect("named parameter");
            qualified.insert(name.clone(), sql_value_to_json(value.clone()));
            frame.set(name, value);
            Arc::make_mut(&mut frame.assignment_types).insert(
                name.clone(),
                param.type_schema.clone().expect("parameter type"),
            );
            if let Some(dates) = &mut frame.oracle_dates {
                Arc::make_mut(dates).remove(name);
            }
            if param.oracle_date {
                Arc::make_mut(
                    frame
                        .oracle_dates
                        .get_or_insert_with(|| Arc::new(FxHashSet::default())),
                )
                .insert(name.clone());
            }
        }
        if !params.is_empty() {
            frame.set(cursor_name, SqlValue::Json(JsonValue::Object(qualified)));
        }
        let result = self.execute_routine_query_unowned(frame, query);
        frame.values = saved_values;
        frame.slot_values = saved_slots;
        frame.assignment_types = saved_types;
        frame.oracle_dates = saved_dates;
        frame.values_dirty = false;
        frame.full_resync = false;
        frame.dirty_slots.fill(false);
        result
    }
}

pub(crate) fn validate_capabilities(
    language: &str,
    declarations: &[RoutineDecl],
    statements: &[RoutineStmt],
    handlers: &[RoutineExceptionHandler],
) -> Result<()> {
    for decl in declarations {
        if let RoutineDecl::Cursor { params, .. } = decl {
            if !params.is_empty()
                && oracle::preview_routine_language_version(language)
                    .is_some_and(|version| version < 8)
            {
                return Err(SqlError::UnsupportedPlpgsqlFeature(
                    "parameterized Oracle cursors require routine compiler version 8".into(),
                ));
            }
            if oracle::preview_routine_language_version(language).is_none()
                && params.iter().any(|param| param.default_expr.is_some())
            {
                return Err(SqlError::InvalidSql(
                    "native cursor formal parameters do not accept defaults".into(),
                ));
            }
        }
    }
    for stmt in statements {
        match stmt {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => validate_capabilities(language, declarations, body, handlers)?,
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                validate_capabilities(language, &[], then_body, &[])?;
                validate_capabilities(language, &[], else_body, &[])?;
            }
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for body in arms {
                    validate_capabilities(language, &[], body, &[])?;
                }
                if let Some(body) = else_body {
                    validate_capabilities(language, &[], body, &[])?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::WhileLoop { body, .. }
            | RoutineStmt::Loop { body } => validate_capabilities(language, &[], body, &[])?,
            _ => {}
        }
    }
    for handler in handlers {
        validate_capabilities(language, &[], &handler.statements, &[])?;
    }
    Ok(())
}

fn validate_native_parameter_columns(
    db: &BicDb,
    query: &Query,
    params: &[RoutineParam],
) -> Result<()> {
    struct Check<'a> {
        db: &'a BicDb,
        names: BTreeSet<String>,
        queries: Vec<Query>,
    }
    impl sqlparser::ast::Visitor for Check<'_> {
        type Break = SqlError;
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<SqlError> {
            self.queries.push(query.clone());
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<SqlError> {
            self.queries.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<SqlError> {
            let Expr::Identifier(ident) = expr else {
                return ControlFlow::Continue(());
            };
            if !self.names.contains(&normalize_object_name(&ident.value)) {
                return ControlFlow::Continue(());
            }
            for query in self.queries.iter().rev() {
                let mut probe = query.clone();
                let SetExpr::Select(select) = probe.body.as_mut() else {
                    continue;
                };
                select.projection = vec![SelectItem::UnnamedExpr(expr.clone())];
                if infer_query_result_types(self.db, &probe.to_string())
                    .ok()
                    .flatten()
                    .and_then(|types| types.into_iter().next())
                    .flatten()
                    .is_some()
                {
                    return ControlFlow::Break(SqlError::data_exception("42702", format!("column reference {} is ambiguous between a cursor parameter and a column", ident.value), None));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut check = Check {
        db,
        names: params.iter().filter_map(|p| p.name.clone()).collect(),
        queries: Vec::new(),
    };
    match sqlparser::ast::Visit::visit(query, &mut check) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_reference_fixture(key: &str, native: bool) {
        let fixture: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/parameterized-cursor-contracts.json"
        ))
        .unwrap();
        let plan = &fixture[key];
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        if !native {
            oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        }
        let mut session = SqlSession::new(&mut db);
        let execute = |session: &mut SqlSession<'_>, source: &str| -> Result<SqlResult> {
            let sql = if native {
                source.to_string()
            } else {
                oracle::lower_profile_sql(session.db_ref(), source, oracle::ORACLE_PREVIEW_PROFILE)?
            };
            session.execute(&sql)
        };
        for object in plan["setup"].as_array().unwrap() {
            if object["backend"] != 0 {
                continue;
            }
            let sql = object["sql"].as_str().unwrap();
            execute(&mut session, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            for sql in object["inserts"].as_array().unwrap() {
                execute(&mut session, sql.as_str().unwrap()).unwrap();
            }
        }
        for case in plan["cases"].as_array().unwrap() {
            let sql = case["sql"].as_str().unwrap();
            if let Some(expected) = case.get("expected_error") {
                assert_eq!(
                    execute(&mut session, sql).unwrap_err().sqlstate(),
                    expected["bicdb"].as_str().unwrap(),
                    "{sql}"
                );
                continue;
            }
            let result = execute(&mut session, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(result.rows.len(), 1, "{sql}");
            let expected = &case["expected"][0][0];
            if expected.is_null() {
                assert_eq!(result.rows[0][0], SqlValue::Null, "{sql}");
            } else {
                assert_eq!(
                    result.rows[0][0].to_cell(),
                    expected
                        .as_str()
                        .or_else(|| expected["number"].as_str())
                        .unwrap(),
                    "{sql}"
                );
            }
        }
    }

    #[test]
    fn postgres_parameterized_cursor_reference_contracts() {
        run_reference_fixture("postgres", true);
    }

    #[test]
    fn oracle_parameterized_cursor_reference_contracts() {
        run_reference_fixture("oracle", false);
    }

    #[test]
    fn oracle_number_arithmetic_preserves_reference_precision_and_native_results() {
        run_reference_fixture("number_arithmetic", false);
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for language in ["plpgsql", oracle::PREVIEW_ROUTINE_LANGUAGE_V7] {
            session.execute(&format!("CREATE OR REPLACE FUNCTION legacy_number(p_left NUMERIC,p_right NUMERIC) RETURNS NUMERIC LANGUAGE {language} AS $$BEGIN RETURN p_left*p_right; END$$")).unwrap();
            assert_eq!(session.execute("SELECT legacy_number(123456789012345678901234567890,123456789012345678901234567890)").unwrap().rows[0][0].to_cell(), "15241578753238836750495351562536198787501905199875019052100");
        }
    }

    #[test]
    fn cursor_declarations_reject_duplicate_names_modes_and_invalid_defaults() {
        for declaration in [
            "c CURSOR(x INT,x TEXT) FOR SELECT x",
            "c CURSOR(x OUT INT) FOR SELECT x",
            "c CURSOR(x INT DEFAULT 2,y INT DEFAULT x) FOR SELECT x+y",
        ] {
            assert!(parse_routine_decl(declaration).is_err(), "{declaration}");
        }
        assert!(parse_call("c(x=>1,2)").is_err());
        assert!(parse_call("c(1) trailing").is_err());
    }

    #[test]
    fn older_oracle_compilers_reject_new_cursor_definitions() {
        let declarations = vec![parse_routine_decl("c CURSOR(x INT) FOR SELECT x").unwrap()];
        assert!(validate_capabilities(
            oracle::PREVIEW_ROUTINE_LANGUAGE_V7,
            &declarations,
            &[],
            &[]
        )
        .is_err());
        assert!(
            validate_capabilities(oracle::PREVIEW_ROUTINE_LANGUAGE, &declarations, &[], &[])
                .is_ok()
        );
        let defaults = vec![parse_routine_decl("c CURSOR(x INT DEFAULT 2) FOR SELECT x").unwrap()];
        assert_eq!(
            validate_capabilities("plpgsql", &defaults, &[], &[])
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
    }
}
