use super::*;

impl RoutineIR {
    pub(crate) fn memo_identity(&self) -> usize {
        self.node_ownership.id
    }

    pub(crate) fn owned_ast_nodes(&self) -> Arc<FxHashSet<usize>> {
        Arc::clone(self.node_ownership.nodes.get_or_init(|| {
            let mut nodes = OwnedNodes::default();
            nodes.parameters(&self.params);
            nodes.declarations(&self.declarations);
            nodes.statements(&self.statements, &self.exception_handlers);
            Arc::new(nodes.0)
        }))
    }
}

#[derive(Default)]
struct OwnedNodes(FxHashSet<usize>);

impl OwnedNodes {
    fn ast(&mut self, ast: &impl sqlparser::ast::Visit) {
        let _ = sqlparser::ast::Visit::visit(ast, self);
    }

    fn expression(&mut self, expr: &RoutineExpr) {
        self.ast(&expr.expr);
    }

    fn parameters(&mut self, params: &[RoutineParam]) {
        for param in params {
            if let Some(expr) = &param.default_expr {
                self.ast(expr);
            }
        }
    }

    fn declarations(&mut self, declarations: &[RoutineDecl]) {
        for declaration in declarations {
            match declaration {
                RoutineDecl::Variable {
                    default_expr: Some(expr),
                    ..
                } => self.expression(expr),
                RoutineDecl::Cursor { params, query, .. } => {
                    self.parameters(params);
                    self.ast(query);
                }
                RoutineDecl::Alias { .. }
                | RoutineDecl::Variable {
                    default_expr: None, ..
                } => {}
            }
        }
    }

    fn arguments(&mut self, arguments: &[RoutineCursorArgument]) {
        for argument in arguments {
            self.expression(&argument.expr);
        }
    }

    fn statements(&mut self, statements: &[RoutineStmt], handlers: &[RoutineExceptionHandler]) {
        let mut pending = statements.iter().collect::<Vec<_>>();
        for handler in handlers {
            pending.extend(&handler.statements);
        }
        while let Some(statement) = pending.pop() {
            match statement {
                RoutineStmt::Block {
                    declarations,
                    body,
                    handlers,
                } => {
                    self.declarations(declarations);
                    pending.extend(body);
                    for handler in handlers {
                        pending.extend(&handler.statements);
                    }
                }
                RoutineStmt::Assignment { target, expr } => {
                    if let RoutineAssignmentTarget::ArrayElement { index, .. } = target {
                        self.expression(index);
                    }
                    self.expression(expr);
                }
                RoutineStmt::SelectInto { query, .. }
                | RoutineStmt::Perform { query }
                | RoutineStmt::ReturnQuery(query) => self.ast(query),
                RoutineStmt::Sql(statement) | RoutineStmt::SqlInto { statement, .. } => {
                    self.ast(statement)
                }
                RoutineStmt::DynamicExecute(expr) => self.expression(expr),
                RoutineStmt::If {
                    condition,
                    then_body,
                    else_body,
                } => {
                    self.expression(condition);
                    pending.extend(then_body);
                    pending.extend(else_body);
                }
                RoutineStmt::Case {
                    expression,
                    arms,
                    else_body,
                } => {
                    self.expression(expression);
                    for body in arms {
                        pending.extend(body);
                    }
                    if let Some(body) = else_body {
                        pending.extend(body);
                    }
                }
                RoutineStmt::ForLoop {
                    lower, upper, body, ..
                } => {
                    self.expression(lower);
                    self.expression(upper);
                    pending.extend(body);
                }
                RoutineStmt::ForeachLoop { array, body, .. } => {
                    self.expression(array);
                    pending.extend(body);
                }
                RoutineStmt::QueryForLoop { query, body, .. } => {
                    self.ast(query);
                    pending.extend(body);
                }
                RoutineStmt::CursorForLoop {
                    arguments, body, ..
                } => {
                    self.arguments(arguments);
                    pending.extend(body);
                }
                RoutineStmt::Loop { body } => pending.extend(body),
                RoutineStmt::WhileLoop { condition, body } => {
                    self.expression(condition);
                    pending.extend(body);
                }
                RoutineStmt::OpenCursor { arguments, .. } => self.arguments(arguments),
                RoutineStmt::RaiseException {
                    arguments, detail, ..
                } => {
                    for argument in arguments {
                        self.expression(argument);
                    }
                    if let Some(detail) = detail {
                        self.expression(detail);
                    }
                }
                RoutineStmt::OracleBuiltin { arguments, .. } => {
                    for argument in arguments {
                        self.expression(argument);
                    }
                }
                RoutineStmt::Return(Some(expr)) => self.expression(expr),
                // Opaque control/dynamic-SQL nodes may synthesize ASTs while
                // executing. Without an ownership proof their nodes stay out
                // of address-based memos.
                RoutineStmt::PostgresControl(_)
                | RoutineStmt::OracleDynamicExecute(_)
                | RoutineStmt::ContinueLoop
                | RoutineStmt::Null
                | RoutineStmt::ExitLoop
                | RoutineStmt::FetchCursor { .. }
                | RoutineStmt::CloseCursor { .. }
                | RoutineStmt::Rethrow
                | RoutineStmt::Return(None) => {}
            }
        }
    }
}

impl sqlparser::ast::Visitor for OwnedNodes {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        self.0.insert(expr as *const Expr as usize);
        match expr {
            Expr::Function(function) => {
                self.0.insert(function as *const Function as usize);
            }
            Expr::Case { conditions, .. } if !conditions.is_empty() => {
                self.0.insert(conditions.as_ptr() as usize);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.0.insert(query as *const Query as usize);
        // Values payloads have no separate visitor hook. Keep insert-plan
        // memoization available for stable VALUES owned by this query.
        let mut pending = vec![query.body.as_ref()];
        while let Some(body) = pending.pop() {
            match body {
                SetExpr::Values(values) => {
                    self.0
                        .insert(values as *const sqlparser::ast::Values as usize);
                }
                SetExpr::SetOperation { left, right, .. } => {
                    pending.push(left);
                    pending.push(right);
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        self.0.insert(select as *const Select as usize);
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        self.0.insert(statement as *const Statement as usize);
        match statement {
            // CALL contains a bare Function, rather than Expr::Function, so
            // the expression visitor cannot register its dispatch node.
            Statement::Call(function) => {
                self.0.insert(function as *const Function as usize);
            }
            Statement::Update(update) => {
                self.0
                    .insert(update as *const sqlparser::ast::Update as usize);
            }
            Statement::Delete(delete) => {
                self.0.insert(delete as *const Delete as usize);
            }
            Statement::Insert(insert) => {
                self.0.insert(insert as *const Insert as usize);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl CompiledNodeOwnership {
    pub(crate) fn statement_nodes(&self, statements: &[Statement]) -> Arc<FxHashSet<usize>> {
        Arc::clone(self.nodes.get_or_init(|| {
            let mut nodes = OwnedNodes::default();
            for statement in statements {
                nodes.ast(statement);
            }
            Arc::new(nodes.0)
        }))
    }
}
