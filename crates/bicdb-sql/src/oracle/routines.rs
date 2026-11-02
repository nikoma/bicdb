//! Declared-type overload selection for explicitly versioned preview routines.
use super::*;

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
            RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body } => lower_statements(body, types),
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
            | RoutineStmt::OpenCursor { .. }
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
        if let RoutineDecl::Variable {
            default_expr: Some(expr),
            ..
        } = declaration
        {
            lower_expression(&mut expr.expr, &types);
        }
    }
    lower_statements(statements, &types);
    for handler in handlers {
        lower_statements(&mut handler.statements, &types);
    }
}
