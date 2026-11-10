//! Evaluate safe, row-independent operands before selecting a join order.
use super::*;

impl SqlEngine<'_> {
    pub(crate) fn fold_join_constant_operands(
        &self,
        from: &TableWithJoins,
        selection: Option<&Expr>,
    ) -> Option<Expr> {
        let selection = selection?;
        if from.joins.is_empty() {
            return None;
        }
        let users = user_function_names(self.db_ref()).ok()?;
        let columns = self.from_relation_columns(std::slice::from_ref(from));
        let safe = |expr: &Expr| safe_operand(self, expr, columns.as_deref(), &users);
        fn walk(
            engine: &SqlEngine<'_>,
            expr: &mut Expr,
            safe: &impl Fn(&Expr) -> bool,
            changed: &mut bool,
        ) {
            // Preserve the expression's declared type: a string-valued NAME,
            // date, or numeric operand is not an unknown string literal.
            if matches!(expr, Expr::Identifier(_) | Expr::Function(_)) && safe(expr) {
                if let Some(pg_type) = engine.constant_expr_pg_type(expr) {
                    if pg_type != "unknown" {
                        if let (Ok(value), Ok(data_type)) = (
                            engine.eval_select_constant_expr(expr),
                            Parser::new(&PostgreSqlDialect {})
                                .try_with_sql(&pg_type)
                                .and_then(|mut parser| parser.parse_data_type()),
                        ) {
                            if !matches!(&value, SqlValue::Float(value) if !value.is_finite()) {
                                *expr = Expr::Cast {
                                    kind: CastKind::Cast,
                                    expr: Box::new(sql_value_to_literal_expr(&value)),
                                    data_type,
                                    array: false,
                                    format: None,
                                };
                                *changed = true;
                                return;
                            }
                        }
                    }
                }
            }
            // Do not visit nested queries: their columns and routine-variable
            // shadowing belong to another scope. Unknown calls and custom casts
            // also keep their authored arguments and execution frequency.
            match expr {
                Expr::BinaryOp { left, right, .. } => {
                    walk(engine, left, safe, changed);
                    walk(engine, right, safe, changed);
                }
                Expr::Nested(inner) | Expr::UnaryOp { expr: inner, .. } => {
                    walk(engine, inner, safe, changed);
                }
                Expr::Cast {
                    expr, data_type, ..
                } if !matches!(data_type, DataType::Custom(..)) => {
                    walk(engine, expr, safe, changed);
                }
                Expr::InList { expr, list, .. } => {
                    walk(engine, expr, safe, changed);
                    for item in list {
                        walk(engine, item, safe, changed);
                    }
                }
                Expr::Between {
                    expr, low, high, ..
                } => {
                    walk(engine, expr, safe, changed);
                    walk(engine, low, safe, changed);
                    walk(engine, high, safe, changed);
                }
                _ => {}
            }
        }
        let mut folded = selection.clone();
        let mut changed = false;
        walk(self, &mut folded, &safe, &mut changed);
        changed.then_some(folded)
    }
}

fn safe_operand(
    engine: &SqlEngine<'_>,
    expr: &Expr,
    columns: Option<&[RelationColumns]>,
    users: &rustc_hash::FxHashSet<String>,
) -> bool {
    match expr {
        Expr::Value(value) => !matches!(value.value, Value::Placeholder(_)),
        Expr::Nested(expr) => safe_operand(engine, expr, columns, users),
        Expr::Identifier(ident) => {
            let Some(columns) = columns else { return false };
            // A local column wins over a similarly named routine variable.
            // Unknown relation columns make variable folding unsafe.
            let name = ident_value(ident);
            !columns.iter().any(|relation| {
                relation
                    .columns
                    .iter()
                    .any(|(column, _)| column == &name || column.starts_with('\0'))
            }) && routine_var_from_ident(&engine.routine_vars, ident).is_some()
        }
        Expr::Cast {
            expr, data_type, ..
        } if !matches!(data_type, DataType::Custom(..)) => {
            safe_operand(engine, expr, columns, users)
        }
        Expr::Function(function) => {
            let Ok(name) = object_name(&function.name) else {
                return false;
            };
            let name = name.to_ascii_lowercase();
            let bare = name.strip_prefix("pg_catalog.").unwrap_or(&name);
            // A deliberately explicit pure builtin family, rather than an
            // incomplete blacklist of volatile/database-dependent functions.
            if !matches!(
                bare,
                "lower"
                    | "upper"
                    | "length"
                    | "char_length"
                    | "character_length"
                    | "octet_length"
                    | "bit_length"
                    | "ascii"
                    | "reverse"
                    | "initcap"
                    | "btrim"
                    | "ltrim"
                    | "rtrim"
                    | "replace"
                    | "translate"
                    | "left"
                    | "right"
                    | "lpad"
                    | "rpad"
                    | "substr"
                    | "substring"
                    | "strpos"
                    | "abs"
                    | "ceil"
                    | "ceiling"
                    | "floor"
                    | "round"
                    | "sign"
            ) || users.contains(bare)
                || (bare != name && !name.starts_with("pg_catalog."))
                || function.name.0.iter().any(|part| {
                    part.as_ident()
                        .is_none_or(|ident| ident.quote_style.is_some())
                })
                || function.over.is_some()
                || function.filter.is_some()
                || !function.within_group.is_empty()
            {
                return false;
            }
            let FunctionArguments::List(args) = &function.args else {
                return false;
            };
            args.duplicate_treatment.is_none() && args.clauses.is_empty() && args.args.iter().all(|arg| {
                matches!(arg, FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) if safe_operand(engine, expr, columns, users))
            })
        }
        _ => false,
    }
}
