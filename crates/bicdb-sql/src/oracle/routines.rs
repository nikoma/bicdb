//! Declared-type overload selection for explicitly versioned preview routines.
use super::*;

pub(crate) fn function_namespace(routine: &RoutineSchema) -> Result<String> {
    let statements = if routine.kind == RoutineKind::Procedure {
        if !preview_routine_language_version(&routine.language).is_some_and(|v| v >= 12) {
            return Err(SqlError::Unsupported(
                "Oracle preview procedures require compiler version 12".into(),
            ));
        }
        let function = super::procedures::header(&routine.definition)?
            .ok_or_else(|| SqlError::InvalidSql("expected a procedure definition".into()))?;
        vec![Statement::CreateFunction(function)]
    } else if routine.kind == RoutineKind::Function {
        Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition)
            .map_err(|error| SqlError::InvalidSql(error.to_string()))?
    } else {
        return Err(SqlError::Unsupported(
            "Oracle preview requires a function or procedure definition".into(),
        ));
    };
    let [Statement::CreateFunction(function)] = statements.as_slice() else {
        return Err(SqlError::InvalidSql(
            "preview function requires one CREATE FUNCTION definition".into(),
        ));
    };
    if normalize_object_name(&object_name(&function.name)?) != routine.name
        || !function.language.as_ref().is_some_and(|language| {
            preview_routine_language_version(&ident_value(language))
                == preview_routine_language_version(&routine.language)
                && matches!(
                    preview_routine_language_version(&routine.language),
                    Some(
                        2 | 3
                            | 4
                            | 5
                            | 6
                            | 7
                            | 8
                            | 9
                            | 10
                            | 11
                            | 12
                            | 13
                            | 14
                            | 15
                            | 16
                            | 17
                            | 18
                            | 19
                    )
                )
        })
    {
        return Err(SqlError::InvalidSql(
            "preview function definition identity does not match its metadata".into(),
        ));
    }
    Ok(relation_schema_name(&function.name))
}

fn bind_relations<T: sqlparser::ast::VisitMut>(
    db: Option<&BicDb>,
    node: &mut T,
    schema: &str,
    calls: bool,
) -> Result<()> {
    super::rownum::lower(node)?;
    super::dual::lower(node, false)?;
    if super::syntax::enabled() {
        let db = db.ok_or_else(|| unsupported("Oracle grammar binding requires a catalog"))?;
        super::syntax::rewrite(db, node, Some(schema))?;
    }
    if calls {
        namespaces::qualify_with_calls(node, schema);
    } else {
        namespaces::qualify(node, schema);
    }
    if super::syntax::enabled() {
        let db = db.ok_or_else(|| unsupported("Oracle grammar binding requires a catalog"))?;
        super::lower_query_locks(db, node)?;
    }
    Ok(())
}

fn bind_namespace_statements(
    db: Option<&BicDb>,
    statements: &mut [RoutineStmt],
    schema: &str,
    calls: bool,
) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                for declaration in declarations {
                    super::collections::bind_declaration_type(declaration, schema);
                    match declaration {
                        RoutineDecl::Variable {
                            default_expr: Some(expr),
                            ..
                        } => bind_relations(db, &mut expr.expr, schema, calls)?,
                        RoutineDecl::Cursor { query, params, .. } => {
                            for param in params {
                                if let Some(expr) = &mut param.default_expr {
                                    bind_relations(db, expr, schema, calls)?;
                                }
                            }
                            bind_relations(db, query, schema, calls)?;
                        }
                        _ => {}
                    }
                }
                bind_namespace_statements(db, body, schema, calls)?;
                for handler in handlers {
                    bind_namespace_statements(db, &mut handler.statements, schema, calls)?;
                }
            }
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    bind_relations(db, &mut index.expr, schema, calls)?;
                }
                bind_relations(db, &mut expr.expr, schema, calls)?;
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                bind_relations(db, &mut condition.expr, schema, calls)?;
                bind_namespace_statements(db, then_body, schema, calls)?;
                bind_namespace_statements(db, else_body, schema, calls)?;
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                bind_relations(db, &mut expression.expr, schema, calls)?;
                for arm in arms {
                    bind_namespace_statements(db, arm, schema, calls)?;
                }
                if let Some(body) = else_body {
                    bind_namespace_statements(db, body, schema, calls)?;
                }
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                bind_relations(db, &mut lower.expr, schema, calls)?;
                bind_relations(db, &mut upper.expr, schema, calls)?;
                bind_namespace_statements(db, body, schema, calls)?;
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                bind_relations(db, &mut array.expr, schema, calls)?;
                bind_namespace_statements(db, body, schema, calls)?;
            }
            RoutineStmt::WhileLoop { condition, body } => {
                bind_relations(db, &mut condition.expr, schema, calls)?;
                bind_namespace_statements(db, body, schema, calls)?;
            }
            RoutineStmt::QueryForLoop { query, body, .. } => {
                bind_relations(db, query, schema, calls)?;
                bind_namespace_statements(db, body, schema, calls)?;
            }
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    bind_relations(db, &mut arg.expr.expr, schema, calls)?;
                }
                bind_namespace_statements(db, body, schema, calls)?;
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    bind_relations(db, &mut arg.expr.expr, schema, calls)?;
                }
            }
            RoutineStmt::Loop { body } => bind_namespace_statements(db, body, schema, calls)?,
            RoutineStmt::SelectInto { query, .. }
            | RoutineStmt::Perform { query }
            | RoutineStmt::ReturnQuery(query) => bind_relations(db, query, schema, calls)?,
            RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                bind_relations(db, statement, schema, calls)?
            }
            RoutineStmt::Return(Some(expr)) => bind_relations(db, &mut expr.expr, schema, calls)?,
            RoutineStmt::OracleBuiltin { arguments, .. } => {
                for arg in arguments {
                    bind_relations(db, &mut arg.expr, schema, calls)?;
                }
            }
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    bind_relations(db, &mut arg.expr, schema, calls)?;
                }
                if let Some(detail) = detail {
                    bind_relations(db, &mut detail.expr, schema, calls)?;
                }
            }
            RoutineStmt::OracleDynamicExecute(dynamic) => {
                bind_relations(db, &mut dynamic.command.expr, schema, calls)?;
                for bind in &mut dynamic.using {
                    bind_relations(db, &mut bind.expr, schema, calls)?;
                }
                dynamic.namespace = Some(schema.into());
            }
            RoutineStmt::DynamicExecute(_) => {
                return Err(SqlError::Unsupported(
                    "Oracle preview routine v2 dynamic SQL namespace binding is not implemented"
                        .into(),
                ))
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::Rethrow
            | RoutineStmt::ExitLoop
            | RoutineStmt::Null
            | RoutineStmt::FetchCursor { .. }
            | RoutineStmt::CloseCursor { .. }
            | RoutineStmt::Return(None) => {}
        }
    }
    Ok(())
}

pub(crate) fn bind_function_namespace(
    db: Option<&BicDb>,
    routine: &RoutineSchema,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    let schema = function_namespace(routine)?;
    let calls = preview_routine_language_version(&routine.language).is_some_and(|v| v >= 12);
    if routine.schema != schema {
        return Err(SqlError::InvalidSql(
            "preview function namespace does not match its definition".into(),
        ));
    }
    for param in params {
        if let Some(kind) = &mut param.type_schema {
            super::collections::bind_routine_type_name(&mut kind.pg_type, &schema);
        }
        if let Some(expr) = &mut param.default_expr {
            bind_relations(db, expr, &schema, calls)?;
        }
    }
    bind_block_namespace(db, declarations, statements, handlers, &schema, calls)
}

pub(crate) fn bind_block_namespace(
    db: Option<&BicDb>,
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
    schema: &str,
    calls: bool,
) -> Result<()> {
    for declaration in declarations {
        super::collections::bind_declaration_type(declaration, schema);
        match declaration {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => bind_relations(db, &mut expr.expr, &schema, calls)?,
            RoutineDecl::Cursor { query, params, .. } => {
                for param in params {
                    if let Some(expr) = &mut param.default_expr {
                        bind_relations(db, expr, &schema, calls)?;
                    }
                }
                bind_relations(db, query, &schema, calls)?;
            }
            _ => {}
        }
    }
    bind_namespace_statements(db, statements, &schema, calls)?;
    for handler in handlers {
        bind_namespace_statements(db, &mut handler.statements, &schema, calls)?;
    }
    Ok(())
}

type Types = BTreeMap<String, String>;

fn input_type(expr: &Expr, types: &Types) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => types.get(&normalize_object_name(&ident.value)).cloned(),
        Expr::Value(value) if matches!(value.value, Value::Placeholder(_)) => {
            let Value::Placeholder(name) = &value.value else {
                unreachable!()
            };
            types.get(&normalize_object_name(name)).cloned()
        }
        Expr::Nested(inner) => input_type(inner, types),
        Expr::Function(function)
            if object_name_lowercase(&function.name)
                .is_ok_and(|name| matches!(name.as_ref(), "trunc" | "bicdb_oracle_trunc_date")) =>
        {
            function_args(function)
                .first()
                .and_then(|arg| input_type(arg, types))
        }
        _ => projected_expr_pg_type(expr, None),
    }
}

fn lower_expression(expr: &mut Expr, types: &Types) {
    struct Lower<'a> {
        types: &'a Types,
        query_depth: usize,
    }
    impl sqlparser::ast::VisitorMut for Lower<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.query_depth -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            // Embedded SQL has its own row namespace. A same-named column must
            // not inherit a procedural variable's type.
            if self.query_depth != 0 {
                return ControlFlow::Continue(());
            }
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|name| name == "trunc")
                    && function_args(function).first().is_some_and(|arg| {
                        matches!(
                            input_type(arg, self.types).as_deref(),
                            Some("date" | "timestamp")
                        )
                    })
                {
                    function.name = ObjectName::from(vec![Ident::new("bicdb_oracle_trunc_date")]);
                }
            }
            ControlFlow::Continue(())
        }
    }
    let _ = sqlparser::ast::VisitMut::visit(
        expr,
        &mut Lower {
            types,
            query_depth: 0,
        },
    );
}

fn lower_statements(statements: &mut [RoutineStmt], types: &Types) {
    for statement in statements {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                let mut local_types = types.clone();
                for decl in declarations.iter() {
                    match decl {
                        RoutineDecl::Variable {
                            name,
                            pg_type: Some(kind),
                            ..
                        } => {
                            local_types.insert(normalize_object_name(name), kind.clone());
                        }
                        RoutineDecl::Alias { name, position } => {
                            if let Some(kind) = types.get(&format!("${position}")) {
                                local_types.insert(normalize_object_name(name), kind.clone());
                            }
                        }
                        _ => {}
                    }
                }
                for decl in declarations {
                    match decl {
                        RoutineDecl::Variable {
                            default_expr: Some(expr),
                            ..
                        } => lower_expression(&mut expr.expr, &local_types),
                        RoutineDecl::Cursor { params, .. } => {
                            for param in params {
                                if let Some(expr) = &mut param.default_expr {
                                    lower_expression(expr, &local_types);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                lower_statements(body, &local_types);
                for handler in handlers {
                    lower_statements(&mut handler.statements, &local_types);
                }
            }
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    lower_expression(&mut index.expr, types);
                }
                lower_expression(&mut expr.expr, types);
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                lower_expression(&mut condition.expr, types);
                lower_statements(then_body, types);
                lower_statements(else_body, types);
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                lower_expression(&mut expression.expr, types);
                for arm in arms {
                    lower_statements(arm, types);
                }
                if let Some(body) = else_body {
                    lower_statements(body, types);
                }
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                lower_expression(&mut lower.expr, types);
                lower_expression(&mut upper.expr, types);
                lower_statements(body, types);
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                lower_expression(&mut array.expr, types);
                lower_statements(body, types);
            }
            RoutineStmt::WhileLoop { condition, body } => {
                lower_expression(&mut condition.expr, types);
                lower_statements(body, types);
            }
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    lower_expression(&mut arg.expr.expr, types);
                }
                lower_statements(body, types);
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    lower_expression(&mut arg.expr.expr, types);
                }
            }
            RoutineStmt::QueryForLoop { body, .. } | RoutineStmt::Loop { body } => {
                lower_statements(body, types)
            }
            RoutineStmt::OracleDynamicExecute(dynamic) => {
                lower_expression(&mut dynamic.command.expr, types);
                for bind in &mut dynamic.using {
                    lower_expression(&mut bind.expr, types);
                }
            }
            RoutineStmt::Return(Some(expr)) | RoutineStmt::DynamicExecute(expr) => {
                lower_expression(&mut expr.expr, types)
            }
            RoutineStmt::OracleBuiltin { arguments, .. } => {
                for arg in arguments {
                    lower_expression(&mut arg.expr, types);
                }
            }
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    lower_expression(&mut arg.expr, types);
                }
                if let Some(detail) = detail {
                    lower_expression(&mut detail.expr, types);
                }
            }
            RoutineStmt::ContinueLoop
            | RoutineStmt::Rethrow
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
        }
    }
}

pub(crate) fn lower_datetime_trunc(
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) {
    let mut types = Types::new();
    for param in params.iter() {
        if let Some(schema) = &param.type_schema {
            types.insert(format!("${}", param.index + 1), schema.pg_type.clone());
            if let Some(name) = &param.name {
                types.insert(normalize_object_name(name), schema.pg_type.clone());
            }
        }
    }
    for declaration in declarations.iter() {
        match declaration {
            RoutineDecl::Variable {
                name,
                pg_type: Some(kind),
                ..
            } => {
                types.insert(normalize_object_name(name), kind.clone());
            }
            RoutineDecl::Alias { name, position } => {
                if let Some(kind) = types.get(&format!("${position}")).cloned() {
                    types.insert(normalize_object_name(name), kind);
                }
            }
            _ => {}
        }
    }
    for param in params {
        if let Some(expr) = &mut param.default_expr {
            lower_expression(expr, &types);
        }
    }
    for declaration in declarations {
        match declaration {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => lower_expression(&mut expr.expr, &types),
            RoutineDecl::Cursor { params, .. } => {
                for param in params {
                    if let Some(expr) = &mut param.default_expr {
                        lower_expression(expr, &types);
                    }
                }
            }
            _ => {}
        }
    }
    lower_statements(statements, &types);
    for handler in handlers {
        lower_statements(&mut handler.statements, &types);
    }
}
