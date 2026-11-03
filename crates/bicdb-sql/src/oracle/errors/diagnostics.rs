//! Resolve PL/SQL diagnostic references without freezing session state.
use super::*;

#[derive(Clone, Debug)]
pub(crate) struct Diagnostics {
    use_code: bool,
    use_message: bool,
}

fn oracle_identity(id: &Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_uppercase()
    }
}

fn rewrite<T: sqlparser::ast::VisitMut>(
    node: &mut T,
    protected: &BTreeSet<String>,
    symbols: &mut Diagnostics,
) {
    struct Rewrite<'a> {
        protected: &'a BTreeSet<String>,
        symbols: &'a mut Diagnostics,
        queries: usize,
    }
    impl sqlparser::ast::VisitorMut for Rewrite<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.queries += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
            self.queries -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
            if self.queries != 0 {
                return ControlFlow::Continue(());
            }
            let name = match expr {
                Expr::Identifier(id) => oracle_identity(id),
                Expr::Function(f)
                    if f.name.0.len() == 1
                        && matches!(&f.args,FunctionArguments::List(list) if list.args.is_empty() && list.clauses.is_empty() && list.duplicate_treatment.is_none()) =>
                {
                    if f.filter.is_some() || f.over.is_some() {
                        return ControlFlow::Continue(());
                    }
                    f.name.0[0]
                        .as_ident()
                        .map(oracle_identity)
                        .unwrap_or_default()
                }
                _ => return ControlFlow::Continue(()),
            };
            if self.protected.contains(&name) {
                return ControlFlow::Continue(());
            }
            let function = match name.as_str() {
                "SQLCODE" => {
                    self.symbols.use_code = true;
                    "bicdb_oracle_current_sqlcode()"
                }
                "SQLERRM" => {
                    self.symbols.use_message = true;
                    "bicdb_oracle_current_sqlerrm()"
                }
                _ => return ControlFlow::Continue(()),
            };
            *expr = expression(function).expect("static diagnostic builtin syntax");
            ControlFlow::Continue(())
        }
    }
    let _ = node.visit(&mut Rewrite {
        protected,
        symbols,
        queries: 0,
    });
}

fn scope_names(declarations: &[RoutineDecl], outer: &BTreeSet<String>) -> BTreeSet<String> {
    let mut names = outer.clone();
    for decl in declarations {
        let name = match decl {
            RoutineDecl::Variable { source_name, .. } => source_name,
            RoutineDecl::Alias { name, .. } | RoutineDecl::Cursor { name, .. } => name,
        };
        names.insert(name.clone());
    }
    names
}

fn visit_scope(
    declarations: &mut [RoutineDecl],
    body: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
    outer: &BTreeSet<String>,
    symbols: &mut Diagnostics,
) {
    let names = scope_names(declarations, outer);
    for decl in declarations {
        match decl {
            RoutineDecl::Variable {
                default_expr: Some(expr),
                ..
            } => rewrite(&mut expr.expr, &names, symbols),
            RoutineDecl::Cursor { params, .. } => {
                for p in params {
                    if let Some(expr) = &mut p.default_expr {
                        rewrite(expr, &names, symbols);
                    }
                }
            }
            _ => {}
        }
    }
    visit_body(body, &names, symbols);
    for handler in handlers {
        visit_body(&mut handler.statements, &names, symbols);
    }
}

fn visit_body(body: &mut [RoutineStmt], names: &BTreeSet<String>, symbols: &mut Diagnostics) {
    for statement in body {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => visit_scope(declarations, body, handlers, names, symbols),
            RoutineStmt::Assignment { target, expr } => {
                if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                    rewrite(&mut index.expr, names, symbols);
                }
                rewrite(&mut expr.expr, names, symbols);
            }
            RoutineStmt::If {
                condition,
                then_body,
                else_body,
            } => {
                rewrite(&mut condition.expr, names, symbols);
                visit_body(then_body, names, symbols);
                visit_body(else_body, names, symbols);
            }
            RoutineStmt::Case {
                expression,
                arms,
                else_body,
            } => {
                rewrite(&mut expression.expr, names, symbols);
                for arm in arms {
                    visit_body(arm, names, symbols);
                }
                if let Some(body) = else_body {
                    visit_body(body, names, symbols);
                }
            }
            RoutineStmt::ForLoop {
                lower, upper, body, ..
            } => {
                rewrite(&mut lower.expr, names, symbols);
                rewrite(&mut upper.expr, names, symbols);
                visit_body(body, names, symbols);
            }
            RoutineStmt::ForeachLoop { array, body, .. } => {
                rewrite(&mut array.expr, names, symbols);
                visit_body(body, names, symbols);
            }
            RoutineStmt::WhileLoop { condition, body } => {
                rewrite(&mut condition.expr, names, symbols);
                visit_body(body, names, symbols);
            }
            RoutineStmt::QueryForLoop { body, .. } | RoutineStmt::Loop { body } => {
                visit_body(body, names, symbols)
            }
            RoutineStmt::CursorForLoop {
                arguments, body, ..
            } => {
                for arg in arguments {
                    rewrite(&mut arg.expr.expr, names, symbols);
                }
                visit_body(body, names, symbols);
            }
            RoutineStmt::OpenCursor { arguments, .. } => {
                for arg in arguments {
                    rewrite(&mut arg.expr.expr, names, symbols);
                }
            }
            RoutineStmt::OracleBuiltin { arguments, .. } => {
                for arg in arguments {
                    rewrite(&mut arg.expr, names, symbols);
                }
            }
            RoutineStmt::Sql(Statement::Call(function)) => rewrite(function, names, symbols),
            RoutineStmt::RaiseException {
                arguments, detail, ..
            } => {
                for arg in arguments {
                    rewrite(&mut arg.expr, names, symbols);
                }
                if let Some(expr) = detail {
                    rewrite(&mut expr.expr, names, symbols);
                }
            }
            RoutineStmt::Return(Some(expr)) | RoutineStmt::DynamicExecute(expr) => {
                rewrite(&mut expr.expr, names, symbols)
            }
            _ => {}
        }
    }
}

pub(crate) fn prepare_diagnostics(
    params: &mut [RoutineParam],
    declarations: &mut Vec<RoutineDecl>,
    body: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Option<Diagnostics> {
    let mut symbols = Diagnostics {
        use_code: false,
        use_message: false,
    };
    let protected = params
        .iter()
        .filter_map(|p| p.name.clone())
        .collect::<BTreeSet<_>>();
    for p in params {
        if let Some(expr) = &mut p.default_expr {
            rewrite(expr, &protected, &mut symbols);
        }
    }
    visit_scope(declarations, body, handlers, &protected, &mut symbols);
    (symbols.use_code || symbols.use_message).then_some(symbols)
}

fn oracle_code(error: &SqlError) -> Result<i32> {
    match error {
        SqlError::OracleError { code, .. } => Ok(*code),
        SqlError::Rethrown(inner) | SqlError::OracleContext { inner, .. } => oracle_code(inner),
        other => match other.sqlstate() {
            "P0002" => Ok(100),
            "P0003" => Ok(-1422),
            "22012" => Ok(-1476),
            "23505" => Ok(-1),
            _ => Err(unsupported(
                "SQLCODE mapping for this engine error is not implemented",
            )),
        },
    }
}

fn oracle_message(error: &SqlError) -> Result<String> {
    let text = match error {
        SqlError::OracleError {
            code,
            message,
            message_valid,
            ..
        } => {
            if message_valid.load(std::sync::atomic::Ordering::Relaxed) {
                message.clone()
            } else {
                match *code {
                    -21000=>"ORA-21000: error number argument to raise_application_error of  is out of range".into(),
                    -6502=>"ORA-06502: PL/SQL: value or conversion error".into(),
                    -1426 | -21002 =>message.clone(),
                    _=>format!("ORA-{:05}: ",code.unsigned_abs()),
                }
            }
        }
        SqlError::Rethrown(inner) | SqlError::OracleContext { inner, .. } => {
            return oracle_message(inner)
        }
        other => match other.sqlstate() {
            "P0002" => "ORA-01403: no data found".into(),
            "22012" => "ORA-01476: divisor is equal to zero".into(),
            _ => {
                return Err(unsupported(
                    "SQLERRM message for this engine error is not implemented",
                ))
            }
        },
    };
    Ok(truncate_bytes(&text, 512).to_string())
}

thread_local! { static CURRENT_ERROR: std::cell::RefCell<Option<Arc<SqlError>>> = const {std::cell::RefCell::new(None)}; }

pub(crate) struct DiagnosticScope(Option<Arc<SqlError>>);
impl DiagnosticScope {
    pub(crate) fn new(error: Option<Arc<SqlError>>) -> Self {
        Self(CURRENT_ERROR.with(|cell| cell.replace(error)))
    }
}
impl Drop for DiagnosticScope {
    fn drop(&mut self) {
        CURRENT_ERROR.with(|cell| {
            cell.replace(self.0.take());
        });
    }
}

pub(crate) fn current_diagnostic(name: &str) -> Result<SqlValue> {
    let error = CURRENT_ERROR.with(|cell| cell.borrow().clone());
    if name == "bicdb_oracle_current_sqlcode" {
        Ok(SqlValue::Int(i64::from(
            error.as_deref().map(oracle_code).transpose()?.unwrap_or(0),
        )))
    } else {
        Ok(SqlValue::String(
            error
                .as_deref()
                .map(oracle_message)
                .transpose()?
                .unwrap_or_else(|| "ORA-0000: normal, successful completion".into()),
        ))
    }
}

pub(crate) fn contains_diagnostics<T: sqlparser::ast::Visit>(node: &T) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(node, |expr| {
        if let Expr::Function(f) = expr {
            found |= object_name_lowercase(&f.name).is_ok_and(|name| {
                matches!(
                    name.as_ref(),
                    "bicdb_oracle_current_sqlcode" | "bicdb_oracle_current_sqlerrm"
                )
            });
        }
        ControlFlow::<()>::Continue(())
    });
    found
}
