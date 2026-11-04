//! Finish aggregate inputs before evaluating conditional scalar projections.
use crate::*;
use sqlparser::ast::{Visit, Visitor};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupKey(pub usize, pub usize, pub usize, pub usize);

struct Context {
    key: GroupKey,
    nodes: FxHashSet<usize>,
    values: FxHashMap<usize, SqlValue>,
    value_bytes: usize,
}
thread_local! {
    static CONTEXTS: RefCell<Vec<Context>> = const { RefCell::new(Vec::new()) };
}
struct Scope(usize);
impl Drop for Scope {
    fn drop(&mut self) {
        CONTEXTS.with(|contexts| contexts.borrow_mut().truncate(self.0));
    }
}

#[derive(Default)]
struct Inventory {
    query_depth: usize,
    nodes: FxHashSet<usize>,
    aggregates: bool,
}
impl Visitor for Inventory {
    type Break = ();
    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.query_depth == 0 {
            self.nodes.insert(expr as *const Expr as usize);
            self.aggregates |=
                matches!(expr, Expr::Function(function) if is_aggregate_function(function));
        }
        ControlFlow::Continue(())
    }
}

fn observe_memory() -> Result<()> {
    let bytes = CONTEXTS.with(|contexts| {
        contexts.borrow().iter().fold(0usize, |sum, context| {
            sum.saturating_add(
                context
                    .nodes
                    .capacity()
                    .saturating_mul(2 * std::mem::size_of::<usize>()),
            )
            .saturating_add(
                context
                    .values
                    .capacity()
                    .saturating_mul(2 * std::mem::size_of::<(usize, SqlValue)>()),
            )
            .saturating_add(context.value_bytes)
        })
    });
    bicdb_core::query_budget::observe(bytes);
    bicdb_core::query_budget::check()?;
    Ok(())
}

/// AST addresses identify nodes only while this synchronous scope borrows the
/// expression. Group identity and node membership distinguish nested queries and
/// reentrant calls; the guard releases values on success and error alike.
pub(crate) fn evaluate(
    key: GroupKey,
    expr: &Expr,
    mut eval: impl FnMut(&Expr) -> Result<SqlValue>,
) -> Result<SqlValue> {
    let node = expr as *const Expr as usize;
    let active = CONTEXTS.with(|contexts| {
        contexts
            .borrow()
            .last()
            .filter(|context| context.key == key && context.nodes.contains(&node))
            .map(|context| context.values.get(&node).cloned())
    });
    if let Some(value) = active {
        return value.map(Ok).unwrap_or_else(|| eval(expr));
    }
    let mut inventory = Inventory::default();
    let _ = expr.visit(&mut inventory);
    if !inventory.aggregates {
        return eval(expr);
    }
    let depth = CONTEXTS.with(|contexts| {
        let mut contexts = contexts.borrow_mut();
        let depth = contexts.len();
        contexts.push(Context {
            key,
            nodes: inventory.nodes,
            values: FxHashMap::default(),
            value_bytes: 0,
        });
        depth
    });
    let _scope = Scope(depth);
    observe_memory()?;
    struct Prepare<'a, F> {
        query_depth: usize,
        eval: &'a mut F,
    }
    impl<F: FnMut(&Expr) -> Result<SqlValue>> Visitor for Prepare<'_, F> {
        type Break = SqlError;
        fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<SqlError> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &Query) -> ControlFlow<SqlError> {
            self.query_depth -= 1;
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<SqlError> {
            if self.query_depth == 0
                && matches!(expr, Expr::Function(f) if is_aggregate_function(f))
            {
                let value = match (self.eval)(expr) {
                    Ok(value) => value,
                    Err(error) => return ControlFlow::Break(error),
                };
                CONTEXTS.with(|contexts| {
                    let mut contexts = contexts.borrow_mut();
                    let context = contexts.last_mut().expect("aggregate scope");
                    context.value_bytes = context
                        .value_bytes
                        .saturating_add(sql_value_memory_estimate(&value));
                    context.values.insert(expr as *const Expr as usize, value);
                });
                if let Err(error) = observe_memory() {
                    return ControlFlow::Break(error);
                }
            }
            ControlFlow::Continue(())
        }
    }
    if let ControlFlow::Break(error) = expr.visit(&mut Prepare {
        query_depth: 0,
        eval: &mut eval,
    }) {
        return Err(error);
    }
    if let Some(value) = CONTEXTS.with(|contexts| {
        contexts
            .borrow()
            .last()
            .and_then(|context| context.values.get(&node).cloned())
    }) {
        return Ok(value);
    }
    eval(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn fake(key: GroupKey, expr: &Expr, calls: &Cell<usize>) -> Result<SqlValue> {
        evaluate(key, expr, |expr| match expr {
            Expr::Function(function) if is_aggregate_function(function) => {
                calls.set(calls.get() + 1);
                Ok(SqlValue::Int(7))
            }
            Expr::Function(function) => {
                for arg in borrowed_function_args(function) {
                    let value = fake(key, &arg, calls)?;
                    if !matches!(value, SqlValue::Null) {
                        return Ok(value);
                    }
                }
                Ok(SqlValue::Null)
            }
            _ => unreachable!("fixture expression"),
        })
    }

    #[test]
    fn aggregate_inputs_run_once_before_the_conditional_scalar() {
        let key = GroupKey(1, 2, 3, 4);
        let calls = Cell::new(0);
        let expr = parse_routine_expr("COALESCE(SUM(1),COUNT(*))").unwrap();
        assert_eq!(fake(key, &expr, &calls).unwrap(), SqlValue::Int(7));
        assert_eq!(
            calls.get(),
            2,
            "both aggregates execute, neither is replayed"
        );
        let root = parse_routine_expr("SUM(1)").unwrap();
        calls.set(0);
        assert_eq!(fake(key, &root, &calls).unwrap(), SqlValue::Int(7));
        assert_eq!(calls.get(), 1, "an aggregate root is not executed twice");
        calls.set(0);
        fake(key, &expr, &calls).unwrap();
        assert_eq!(calls.get(), 2);
        CONTEXTS.with(|contexts| assert!(contexts.borrow().is_empty()));
    }

    #[test]
    fn unselected_subqueries_own_their_aggregate_scope() {
        let expr = parse_routine_expr("COALESCE(SUM(1),(SELECT SUM(1/0)))").unwrap();
        let calls = Cell::new(0);
        assert_eq!(
            fake(GroupKey(1, 2, 3, 4), &expr, &calls).unwrap(),
            SqlValue::Int(7)
        );
        assert_eq!(calls.get(), 1);
        CONTEXTS.with(|contexts| assert!(contexts.borrow().is_empty()));
    }
}
