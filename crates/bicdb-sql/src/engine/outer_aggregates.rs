//! PostgreSQL owns a scalar subquery's aggregate at the nearest query level
//! supplying a reference in its inputs (including FILTER and aggregate
//! ordering). Columns bound inside an input subquery do not affect ownership.
use crate::planner::unwrap_nested_expr;
use crate::*;
use sqlparser::ast::{Query, Visit, Visitor};
use std::ops::ControlFlow;

impl SqlEngine<'_> {
    pub(crate) fn rewrite_outer_owned_scalar_aggregates(
        &self,
        query: &Query,
    ) -> Result<Option<Query>> {
        let SetExpr::Select(select) = query.body.as_ref() else {
            return Ok(None);
        };
        if select.from.is_empty() {
            return Ok(None);
        }

        let candidates = select
            .projection
            .iter()
            .enumerate()
            .filter_map(|(position, item)| {
                let (function, alias) = scalar_aggregate_projection(item)?;
                Some((position, function, alias))
            });
        let mut candidates = candidates.peekable();
        if candidates.peek().is_none() || oracle::is_oracle_database(self.db_ref())? {
            return Ok(None);
        }
        let Some(outer_columns) = self.from_relation_columns(&select.from) else {
            return Ok(None);
        };
        let mut replacement = None;
        for (position, function, alias) in candidates {
            let mut binding = AggregateInputs {
                engine: self,
                scopes: vec![outer_columns.clone()],
                references_outer: false,
            };
            if function.visit(&mut binding).is_break() || !binding.references_outer {
                continue;
            }
            let rewritten = replacement.get_or_insert_with(|| query.clone());
            let SetExpr::Select(rewritten) = rewritten.body.as_mut() else {
                unreachable!("rewriting a SELECT")
            };
            let expr = Expr::Function(function.clone());
            rewritten.projection[position] = match alias {
                Some(alias) => SelectItem::ExprWithAlias {
                    expr,
                    alias: alias.clone(),
                },
                None => SelectItem::UnnamedExpr(expr),
            };
        }
        Ok(replacement)
    }
}

fn projection_expr(item: &SelectItem) -> Option<(&Expr, Option<&Ident>)> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some((expr, None)),
        SelectItem::ExprWithAlias { expr, alias } => Some((expr, Some(alias))),
        _ => None,
    }
}

fn scalar_aggregate_projection(item: &SelectItem) -> Option<(&Function, Option<&Ident>)> {
    let (expr, outer_alias) = projection_expr(item)?;
    let Expr::Subquery(query) = unwrap_nested_expr(expr) else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.from.is_empty()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
        || !select.connect_by.is_empty()
        || !matches!(&select.group_by, GroupByExpr::Expressions(groups, modifiers)
                     if groups.is_empty() && modifiers.is_empty())
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return None;
    }
    let [output] = select.projection.as_slice() else {
        return None;
    };
    let (expr, inner_alias) = projection_expr(output)?;
    let Expr::Function(function) = unwrap_nested_expr(expr) else {
        return None;
    };
    (function.over.is_none() && function.within_group.is_empty() && is_aggregate_function(function))
        .then_some((function, outer_alias.or(inner_alias)))
}

struct AggregateInputs<'engine, 'db> {
    engine: &'engine SqlEngine<'db>,
    scopes: Vec<Vec<RelationColumns>>,
    references_outer: bool,
}

impl Visitor for AggregateInputs<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if query.with.is_some()
            || query.order_by.is_some()
            || !matches!(query.body.as_ref(), SetExpr::Select(_))
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        // GROUP BY can bind a SELECT output alias, which is separate from
        // the relation columns used by the ordinary expression scopes.
        if !matches!(&select.group_by, GroupByExpr::Expressions(groups, modifiers)
                     if groups.is_empty() && modifiers.is_empty())
        {
            return ControlFlow::Break(());
        }
        let Some(columns) = self.engine.from_relation_columns(&select.from) else {
            return ControlFlow::Break(());
        };
        self.scopes.push(columns);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        let (relation, column) = match expr {
            Expr::Identifier(column) => (None, column),
            Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [relation, column] => (Some(relation), column),
                _ => return ControlFlow::Break(()),
            },
            _ => return ControlFlow::Continue(()),
        };
        for (scope_index, scope) in self.scopes.iter().enumerate().rev() {
            let mut matches = scope
                .iter()
                .filter(|source| relation.is_none_or(|name| ident_matches(name, &source.alias)))
                .flat_map(|source| &source.columns)
                .filter(|(name, _)| ident_matches(column, name));
            if matches.next().is_none() {
                // A named relation at this level hides identically named
                // relations in an enclosing query, including on a missing column.
                if relation.is_some_and(|name| {
                    scope
                        .iter()
                        .any(|source| ident_matches(name, &source.alias))
                }) {
                    return ControlFlow::Break(());
                }
                continue;
            }
            if matches.next().is_some() {
                return ControlFlow::Break(());
            }
            self.references_outer |= scope_index == 0;
            return ControlFlow::Continue(());
        }
        ControlFlow::Break(())
    }
}

fn ident_matches(written: &Ident, bound: &str) -> bool {
    if written.quote_style.is_some() {
        written.value == bound
    } else {
        written.value.eq_ignore_ascii_case(bound)
    }
}
