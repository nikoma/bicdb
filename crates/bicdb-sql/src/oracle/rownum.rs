//! Prefix predicates on ROWNUM. Same-block ordering/grouping must not be
//! mistaken for an ordered LIMIT; unimplemented shapes are rejected explicitly.
use super::*;

fn is_rownum(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(id) if if id.quote_style.is_some() {
        id.value == "ROWNUM"
    } else {id.value.eq_ignore_ascii_case("rownum")})
}

fn contains<T: sqlparser::ast::Visit>(node: &T) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(node, |expr| {
        found |= is_rownum(expr);
        ControlFlow::<()>::Continue(())
    });
    found
}

fn integer(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Nested(expr) => integer(expr),
        Expr::Value(value) => match &value.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => integer(expr)?.checked_neg(),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => integer(expr),
        _ => None,
    }
}

fn predicate(expr: Expr) -> Result<(Option<Expr>, Option<i64>)> {
    if !contains(&expr) {
        return Ok((Some(expr), None));
    }
    match expr {
        Expr::Nested(inner) => predicate(*inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let (left, a) = predicate(*left)?;
            let (right, b) = predicate(*right)?;
            let remaining = match (left, right) {
                (Some(left), Some(right)) => Some(Expr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                }),
                (a, b) => a.or(b),
            };
            Ok((
                remaining,
                match (a, b) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                },
            ))
        }
        Expr::BinaryOp {
            left,
            mut op,
            right,
        } => {
            let bound = if is_rownum(&left) {
                integer(&right)
            } else if is_rownum(&right) {
                op = match op {
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    op => op,
                };
                integer(&left)
            } else {
                None
            }
            .ok_or_else(|| unsupported("ROWNUM requires a literal integer prefix comparison"))?;
            let limit = match op {
                BinaryOperator::Eq => Some(if bound == 1 { 1 } else { 0 }),
                BinaryOperator::NotEq => {
                    if bound > 1 {
                        Some(bound - 1)
                    } else if bound == 1 {
                        Some(0)
                    } else {
                        None
                    }
                }
                BinaryOperator::Lt => Some(bound.saturating_sub(1).max(0)),
                BinaryOperator::LtEq => Some(bound.max(0)),
                BinaryOperator::Gt => {
                    if bound < 1 {
                        None
                    } else {
                        Some(0)
                    }
                }
                BinaryOperator::GtEq => {
                    if bound <= 1 {
                        None
                    } else {
                        Some(0)
                    }
                }
                _ => return Err(unsupported("ROWNUM prefix operator is not implemented")),
            };
            Ok((None, limit))
        }
        _ => Err(unsupported(
            "ROWNUM requires conjunctive prefix comparisons",
        )),
    }
}

pub(super) fn lower<T: sqlparser::ast::VisitMut>(node: &mut T) -> Result<()> {
    struct Lower;
    impl sqlparser::ast::VisitorMut for Lower {
        type Break = SqlError;
        fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<SqlError> {
            if !contains(query) {
                return ControlFlow::Continue(());
            }
            let plain = query.order_by.is_none()
                && query.limit_clause.is_none()
                && query.fetch.is_none()
                && query.locks.is_empty()
                && query.for_clause.is_none();
            let SetExpr::Select(select) = query.body.as_mut() else {
                return ControlFlow::Break(unsupported(
                    "ROWNUM in a set operation requires an enclosing query",
                ));
            };
            let mut row_context = false;
            let _ = sqlparser::ast::visit_expressions(&select.projection, |expr| {
                row_context |= matches!(expr,Expr::Function(f) if f.over.is_some() || is_aggregate_function(f));
                ControlFlow::<()>::Continue(())
            });
            if !plain
                || row_context
                || contains(&select.projection)
                || select.distinct.is_some()
                || select.top.is_some()
                || !matches!(&select.group_by,GroupByExpr::Expressions(exprs,_) if exprs.is_empty())
                || select.having.is_some()
                || select.qualify.is_some()
                || select.prewhere.is_some()
                || !select.named_window.is_empty()
            {
                return ControlFlow::Break(unsupported(
                    "ROWNUM prefix requires a plain query block; put ordering in a subquery",
                ));
            }
            let Some(selection) = select.selection.take() else {
                return ControlFlow::Break(unsupported(
                    "ROWNUM is supported only in prefix predicates",
                ));
            };
            let (remaining, limit) = match predicate(selection) {
                Ok(result) => result,
                Err(error) => return ControlFlow::Break(error),
            };
            select.selection = remaining;
            if let Some(limit) = limit {
                match parse_single_query(&format!("SELECT 1 LIMIT {limit}")) {
                    Ok(parsed) => query.limit_clause = parsed.limit_clause,
                    Err(error) => return ControlFlow::Break(error),
                }
            }
            ControlFlow::Continue(())
        }
    }
    match node.visit(&mut Lower) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_rownum_prefix_preserves_inner_order_and_native_column_names() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for source in [
            "CREATE TABLE prefix_values(v NUMBER)",
            "INSERT INTO prefix_values VALUES(1),(2),(3)",
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        for (predicate, expected) in [
            ("ROWNUM=1", vec![3]),
            ("ROWNUM=2", vec![]),
            ("ROWNUM<=2", vec![3, 2]),
            ("ROWNUM<3", vec![3, 2]),
            ("ROWNUM>1", vec![]),
            ("ROWNUM>=2", vec![]),
            ("ROWNUM>=1", vec![3, 2, 1]),
            ("ROWNUM>0", vec![3, 2, 1]),
            ("ROWNUM<>2", vec![3]),
            ("ROWNUM<>1", vec![]),
            ("ROWNUM<=0", vec![]),
            ("ROWNUM<=-1", vec![]),
            ("2>=ROWNUM", vec![3, 2]),
            ("v<>3 AND ROWNUM<=1", vec![2]),
            ("ROWNUM>=1 AND ROWNUM<3", vec![3, 2]),
        ] {
            let source = format!(
                "SELECT v FROM (SELECT v FROM prefix_values ORDER BY v DESC) WHERE {predicate}"
            );
            let sql = lower_profile_sql(&db, &source, ORACLE_PREVIEW_PROFILE).unwrap();
            let rows = SqlSession::new(&mut db).execute(&sql).unwrap().rows;
            assert_eq!(
                rows.iter().map(|row| row[0].to_cell()).collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>(),
                "{source}"
            );
        }
        for source in [
            "SELECT v FROM prefix_values WHERE ROWNUM<=2 ORDER BY v",
            "SELECT COUNT(*) FROM prefix_values WHERE ROWNUM<=2",
            "SELECT v FROM prefix_values WHERE ROWNUM=1 OR v=3",
            "SELECT ROWNUM FROM prefix_values",
        ] {
            assert_eq!(
                lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE)
                    .unwrap_err()
                    .sqlstate(),
                "0A000",
                "{source}"
            );
        }
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE native_prefix(rownum INT); INSERT INTO native_prefix VALUES(8)")
            .unwrap();
        assert_eq!(
            session
                .execute("SELECT rownum FROM native_prefix WHERE rownum=8")
                .unwrap()
                .rows[0][0],
            SqlValue::Int(8)
        );
    }
}
