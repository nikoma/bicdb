//! Resolve the datetime TRUNC overload without changing PostgreSQL trunc().
use super::*;

pub(super) fn lower(db: &BicDb, sql: &str, oids: &[i32]) -> Result<String> {
    if !sql.to_ascii_lowercase().contains("trunc") {
        return Ok(sql.to_string());
    }
    let mut statements = parse_statements(sql)?;
    if !oids.is_empty() {
        let _ = visit_expressions_mut(&mut statements, |expr| {
            if let Expr::Value(value) = expr {
                if let Value::Placeholder(name) = &value.value {
                    let oid = name
                        .strip_prefix('$')
                        .and_then(|n| n.parse::<usize>().ok())
                        .and_then(|n| n.checked_sub(1))
                        .and_then(|n| oids.get(n));
                    let kind = match oid {
                        Some(1082) => Some(DataType::Date),
                        Some(1114) => Some(DataType::Timestamp(
                            None,
                            sqlparser::ast::TimezoneInfo::None,
                        )),
                        _ => None,
                    };
                    if let Some(data_type) = kind {
                        *expr = Expr::Cast {
                            kind: sqlparser::ast::CastKind::Cast,
                            expr: Box::new(expr.clone()),
                            data_type,
                            format: None,
                            array: false,
                        };
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    rewrite(db, &mut statements)?;
    Ok(statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; "))
}

pub(super) fn rewrite(db: &BicDb, statements: &mut Vec<Statement>) -> Result<()> {
    struct Resolve<'a> {
        db: &'a BicDb,
        queries: Vec<Query>,
        statement_from: Vec<Option<String>>,
    }
    impl Resolve<'_> {
        fn input_type(&self, arg: &Expr) -> Option<String> {
            let direct = projected_expr_pg_type(arg, None);
            if matches!(direct.as_deref(), Some("date" | "timestamp")) {
                return direct;
            }
            let mut probe = if let Some(query) = self.queries.last() {
                let mut query = query.clone();
                let SetExpr::Select(select) = query.body.as_mut() else {
                    return direct;
                };
                select.projection = vec![SelectItem::UnnamedExpr(arg.clone())];
                query.locks.clear();
                query.to_string()
            } else if let Some(Some(from)) = self.statement_from.last() {
                format!("SELECT {arg} FROM {from}")
            } else {
                return direct;
            };
            // For overload selection TRUNC retains its input's type family.
            // This lets an outer query see a datetime produced by a CTE before
            // the inner rewrite has been reflected in the captured query AST.
            if let Ok(mut ast) = parse_statements(&probe) {
                let _ = visit_expressions_mut(&mut ast, |expr| {
                    if let Expr::Function(function) = expr {
                        if object_name_lowercase(&function.name).is_ok_and(|n| n == "trunc") {
                            if let Some(arg) = function_args(function).first() {
                                *expr = (*arg).clone();
                            }
                        }
                    }
                    ControlFlow::<()>::Continue(())
                });
                probe = ast
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ");
            }
            infer_query_result_types(self.db, &probe)
                .ok()
                .flatten()
                .and_then(|types| types.into_iter().next().flatten())
                .or(direct)
        }
    }
    impl sqlparser::ast::VisitorMut for Resolve<'_> {
        type Break = SqlError;
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<SqlError> {
            self.statement_from.push(match statement {
                Statement::Update(update) => Some(update.table.to_string()),
                Statement::CreateIndex(index) => Some(index.table_name.to_string()),
                _ => None,
            });
            ControlFlow::Continue(())
        }
        fn post_visit_statement(&mut self, _: &mut Statement) -> ControlFlow<SqlError> {
            self.statement_from.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            self.queries.push(query.clone());
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<SqlError> {
            self.queries.pop();
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<SqlError> {
            if let Expr::Function(function) = expr {
                if object_name_lowercase(&function.name).is_ok_and(|n| n == "trunc") {
                    let args = function_args(function);
                    if let Some(arg) = args.first() {
                        if matches!(self.input_type(arg).as_deref(), Some("date" | "timestamp")) {
                            function.name =
                                ObjectName::from(vec![Ident::new("bicdb_oracle_trunc_date")]);
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    match sqlparser::ast::VisitMut::visit(
        statements,
        &mut Resolve {
            db,
            queries: Vec::new(),
            statement_from: Vec::new(),
        },
    ) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}
