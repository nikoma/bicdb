//! Folding of statement-constant tests before planning.
//!
//! Application SQL often switches filters with parameters: `($2 = '' OR name ILIKE ..)`,
//! `CASE WHEN $3 = 'all' THEN status <> 'removed' ELSE status = $3 END`, `ORDER BY CASE WHEN
//! $5 = 'name' THEN .. END`. PostgreSQL folds these once per execution; evaluated per row they
//! cost as much as the real predicate. A comparison of two literals or bound `$n` parameters is
//! replaced by its boolean, and AND/OR/NOT/CASE over such booleans are simplified, with SQL's
//! three-valued rules (NULL operands are never folded).
use crate::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use std::ops::ControlFlow;

pub(crate) fn fold_statement_constants(
    query: &Query,
    params: &BTreeMap<String, SqlValue>,
) -> Option<Query> {
    if params.is_empty() && !query.to_string().contains('\'') {
        return None;
    }
    let mut folded = query.clone();
    let mut folder = Folder {
        params,
        changed: false,
    };
    fold_query(&mut folded, &mut folder);
    folder.changed.then_some(folded)
}

/// Filters and sort keys only: projected expressions keep their names and types.
fn fold_query(query: &mut Query, folder: &mut Folder<'_>) {
    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            fold_query(&mut cte.query, folder);
        }
    }
    fold_set_expr(query.body.as_mut(), folder);
    if let Some(OrderBy {
        kind: OrderByKind::Expressions(keys),
        ..
    }) = query.order_by.as_mut()
    {
        let mut constant_keys = Vec::new();
        for (index, key) in keys.iter_mut().enumerate() {
            let changed = folder.changed;
            folder.changed = false;
            let _ = key.expr.visit(folder);
            // A key that folded to a constant cannot order anything; dropping
            // it also keeps a bare literal from reading as an ordinal.
            if folder.changed && matches!(unwrap_nested_expr(&key.expr), Expr::Value(_)) {
                constant_keys.push(index);
            }
            folder.changed |= changed;
        }
        if !constant_keys.is_empty() && constant_keys.len() < keys.len() {
            for index in constant_keys.into_iter().rev() {
                keys.remove(index);
            }
        } else if !constant_keys.is_empty() {
            // Every key is constant: keep one written key (no ordering effect).
            keys.truncate(1);
            keys[0].expr = Expr::Nested(Box::new(Expr::value(Value::Null)));
        }
    }
}

fn fold_set_expr(body: &mut SetExpr, folder: &mut Folder<'_>) {
    match body {
        SetExpr::Select(select) => {
            for from in &mut select.from {
                fold_table_factor(&mut from.relation, folder);
                for join in &mut from.joins {
                    fold_table_factor(&mut join.relation, folder);
                    match &mut join.join_operator {
                        JoinOperator::Join(JoinConstraint::On(on))
                        | JoinOperator::Inner(JoinConstraint::On(on))
                        | JoinOperator::Left(JoinConstraint::On(on))
                        | JoinOperator::LeftOuter(JoinConstraint::On(on))
                        | JoinOperator::Right(JoinConstraint::On(on))
                        | JoinOperator::RightOuter(JoinConstraint::On(on))
                        | JoinOperator::FullOuter(JoinConstraint::On(on)) => {
                            let _ = on.visit(folder);
                        }
                        _ => {}
                    }
                }
            }
            if let Some(selection) = select.selection.as_mut() {
                let _ = selection.visit(folder);
            }
            if let Some(having) = select.having.as_mut() {
                let _ = having.visit(folder);
            }
        }
        SetExpr::Query(query) => fold_query(query, folder),
        SetExpr::SetOperation { left, right, .. } => {
            fold_set_expr(left, folder);
            fold_set_expr(right, folder);
        }
        _ => {}
    }
}

fn fold_table_factor(relation: &mut TableFactor, folder: &mut Folder<'_>) {
    if let TableFactor::Derived { subquery, .. } = relation {
        fold_query(subquery, folder);
    }
}

struct Folder<'a> {
    params: &'a BTreeMap<String, SqlValue>,
    changed: bool,
}

enum Constant {
    Text(String),
    Int(i64),
}

impl Folder<'_> {
    fn constant(&self, expr: &Expr) -> Option<Constant> {
        match expr {
            Expr::Nested(inner) => self.constant(inner),
            Expr::Value(value) => match &value.value {
                Value::SingleQuotedString(text) => Some(Constant::Text(text.clone())),
                Value::Number(number, _) => number.parse::<i64>().ok().map(Constant::Int),
                Value::Placeholder(name) => match self.params.get(name)? {
                    SqlValue::String(text) => Some(Constant::Text(text.clone())),
                    SqlValue::Int(value) => Some(Constant::Int(*value)),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    fn truth(expr: &Expr) -> Option<bool> {
        match expr {
            Expr::Nested(inner) => Self::truth(inner),
            Expr::Value(value) => match value.value {
                Value::Boolean(value) => Some(value),
                _ => None,
            },
            _ => None,
        }
    }
}

fn boolean(value: bool) -> Expr {
    Expr::value(Value::Boolean(value))
}

impl VisitorMut for Folder<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let replacement = match expr {
            Expr::BinaryOp { left, op, right }
                if matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) =>
            {
                let equal = match (self.constant(left), self.constant(right)) {
                    (Some(Constant::Text(left)), Some(Constant::Text(right))) => left == right,
                    (Some(Constant::Int(left)), Some(Constant::Int(right))) => left == right,
                    _ => return ControlFlow::Continue(()),
                };
                Some(boolean(equal == matches!(op, BinaryOperator::Eq)))
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Or,
                right,
            } => match (Self::truth(left), Self::truth(right)) {
                (Some(true), _) | (_, Some(true)) => Some(boolean(true)),
                (Some(false), _) => Some((**right).clone()),
                (_, Some(false)) => Some((**left).clone()),
                _ => None,
            },
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => match (Self::truth(left), Self::truth(right)) {
                (Some(false), _) | (_, Some(false)) => Some(boolean(false)),
                (Some(true), _) => Some((**right).clone()),
                (_, Some(true)) => Some((**left).clone()),
                _ => None,
            },
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: inner,
            } => Self::truth(inner).map(|value| boolean(!value)),
            Expr::Nested(inner) => Self::truth(inner).map(boolean),
            Expr::Case {
                operand: None,
                conditions,
                else_result,
                ..
            } if conditions
                .first()
                .is_some_and(|when| Self::truth(&when.condition).is_some()) =>
            {
                // Leading WHENs with a known outcome: skip false ones, stop at a true one.
                let mut result = None;
                let mut rest = Vec::new();
                for (index, when) in conditions.iter().enumerate() {
                    match Self::truth(&when.condition) {
                        Some(false) => continue,
                        Some(true) if rest.is_empty() => {
                            result = Some(when.result.clone());
                            break;
                        }
                        _ => {
                            rest.extend(conditions[index..].iter().cloned());
                            break;
                        }
                    }
                }
                match (result, rest.is_empty()) {
                    (Some(result), _) => Some(result),
                    (None, true) => Some(
                        else_result
                            .as_deref()
                            .cloned()
                            .unwrap_or_else(|| Expr::value(Value::Null)),
                    ),
                    (None, false) => {
                        *conditions = rest;
                        self.changed = true;
                        None
                    }
                }
            }
            _ => None,
        };
        if let Some(replacement) = replacement {
            *expr = replacement;
            self.changed = true;
        }
        ControlFlow::Continue(())
    }
}
