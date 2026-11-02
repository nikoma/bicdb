//! Declared-type overload selection for explicitly versioned preview routines.
use super::*;

pub(crate) fn function_namespace(routine: &RoutineSchema) -> Result<String> {
    if routine.kind != RoutineKind::Function {
        return Err(SqlError::Unsupported(
            "Oracle preview static routine compiler currently supports function definitions".into(),
        ));
    }
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &routine.definition)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))?;
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
                    Some(2 | 3 | 4 | 5 | 6 | 7 | 8)
                )
        })
    {
        return Err(SqlError::InvalidSql(
            "preview function definition identity does not match its metadata".into(),
        ));
    }
    Ok(relation_schema_name(&function.name))
}

fn bind_relations<T: sqlparser::ast::VisitMut>(node: &mut T, schema: &str) -> Result<()> {
    super::rownum::lower(node)?;
    super::dual::lower(node, false)?;
    namespaces::qualify(node, schema);
    Ok(())
}

fn bind_namespace_statements(statements: &mut [RoutineStmt], schema: &str) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                for declaration in declarations {
                    match declaration {
                        RoutineDecl::Variable {
                            default_expr: Some(expr),
                            ..
                        } => bind_relations(&mut expr.expr, schema)?,
                        RoutineDecl::Cursor { query, params, .. } => {
                            for param in params {
                                if let Some(expr) = &mut param.default_expr {
                                    bind_relations(expr, schema)?;
                                }
                            }
                            bind_relations(query, schema)?;
                        }
                        _ => {}
                    }
                }
                bind_namespace_statements(body, schema)?;
                for handler in handlers {
                    bind_namespace_statements(&mut handler.statements, schema)?;
                }
            }
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    bind_relations(&mut index.expr, schema)?;
                }
                bind_relations(&mut expr.expr, schema)?;
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                bind_relations(&mut condition.expr, schema)?;
                bind_namespace_statements(then_body, schema)?;
                bind_namespace_statements(else_body, schema)?;
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                bind_relations(&mut expression.expr, schema)?;
                for arm in arms {
                    bind_namespace_statements(arm, schema)?;
                }
                if let Some(body) = else_body {
                    bind_namespace_statements(body, schema)?;
                }
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                bind_relations(&mut lower.expr, schema)?;
                bind_relations(&mut upper.expr, schema)?;
                bind_namespace_statements(body, schema)?;
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                bind_relations(&mut array.expr, schema)?;
                bind_namespace_statements(body, schema)?;
            }
            RoutineStmt::WhileLoop { condition, body } => {
                bind_relations(&mut condition.expr, schema)?;
                bind_namespace_statements(body, schema)?;
            }
            RoutineStmt::QueryForLoop { query, body, .. } => {
                bind_relations(query, schema)?;
                bind_namespace_statements(body, schema)?;
            }
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    bind_relations(&mut arg.expr.expr, schema)?;
                }
                bind_namespace_statements(body, schema)?;
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    bind_relations(&mut arg.expr.expr, schema)?;
                }
            }
            RoutineStmt::Loop { body } => bind_namespace_statements(body, schema)?,
            RoutineStmt::SelectInto { query, .. }
            | RoutineStmt::Perform { query }
            | RoutineStmt::ReturnQuery(query) => bind_relations(query, schema)?,
            RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                bind_relations(statement, schema)?
            }
            RoutineStmt::Return(Some(expr)) => bind_relations(&mut expr.expr, schema)?,
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    bind_relations(&mut arg.expr, schema)?;
                }
                if let Some(detail) = detail {
                    bind_relations(&mut detail.expr, schema)?;
                }
            }
            RoutineStmt::DynamicExecute(_) => {
                return Err(SqlError::Unsupported(
                    "Oracle preview routine v2 dynamic SQL namespace binding is not implemented"
                        .into(),
                ))
            }
            RoutineStmt::ContinueLoop
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
    routine: &RoutineSchema,
    params: &mut [RoutineParam],
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    let schema = function_namespace(routine)?;
    if routine.schema != schema {
        return Err(SqlError::InvalidSql(
            "preview function namespace does not match its definition".into(),
        ));
    }
    for param in params {
        if let Some(expr) = &mut param.default_expr {
            bind_relations(expr, &schema)?;
        }
    }
    bind_block_namespace(declarations, statements, handlers, &schema)
}

pub(crate) fn bind_block_namespace(
    declarations: &mut [RoutineDecl],
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
    schema: &str,
) -> Result<()> {
    for declaration in declarations {
        match declaration {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => bind_relations(&mut expr.expr, &schema)?,
            RoutineDecl::Cursor { query, params, .. } => {
                for param in params {
                    if let Some(expr) = &mut param.default_expr {
                        bind_relations(expr, &schema)?;
                    }
                }
                bind_relations(query, &schema)?;
            }
            _ => {}
        }
    }
    bind_namespace_statements(statements, &schema)?;
    for handler in handlers {
        bind_namespace_statements(&mut handler.statements, &schema)?;
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
            RoutineStmt::Return(Some(expr)) | RoutineStmt::DynamicExecute(expr) => {
                lower_expression(&mut expr.expr, types)
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
