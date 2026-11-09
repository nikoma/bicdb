//! SELECT privileges on every relation of a query's FROM tree, checked when
//! the statement starts, as PostgreSQL checks its range table at executor
//! start. The executor also checks each relation as it scans it, but a scan
//! that never runs (a join whose other side is empty, a view joined to an
//! empty relation) skipped that check: the statement returned rows instead
//! of `permission denied`, and how it answered depended on the data. Views
//! expand with their own semantics: a `security_invoker` (or ownerless) view
//! reads its sources as the caller, a definer view as its owner.
//!
//! Subqueries inside expressions are left to the executor: PostgreSQL drops
//! those its planner proves unreachable (`WHERE false AND EXISTS (...)`)
//! before checking, so requiring them here would refuse statements it runs.
use super::*;
use std::collections::BTreeSet;

struct Walk<'q> {
    // CTE scopes, innermost last: a FROM name matching one reads that CTE.
    ctes: Vec<Vec<(String, &'q Query)>>,
    // CTE bodies already walked, by address (recursive CTEs, repeated refs).
    walked: BTreeSet<usize>,
    // Expansion path, independent of CTE scopes. Valid deep view chains must
    // be checked completely; a cycle is refused instead of skipping checks.
    views: Vec<String>,
}

impl SqlEngine<'_> {
    pub(crate) fn require_from_relations_readable(&self, query: &Query) -> Result<()> {
        let role = rls_check_user_from_gucs(&self.session_gucs);
        let mut walk = Walk {
            ctes: Vec::new(),
            walked: BTreeSet::new(),
            views: Vec::new(),
        };
        self.eager_query(query, &role, &mut walk)
    }

    fn eager_query<'q>(&self, query: &'q Query, role: &str, walk: &mut Walk<'q>) -> Result<()> {
        let scoped = query.with.is_some();
        if let Some(with) = &query.with {
            walk.ctes.push(
                with.cte_tables
                    .iter()
                    .map(|cte| (ident_value(&cte.alias.name), cte.query.as_ref()))
                    .collect(),
            );
        }
        let result = self.eager_set_expr(&query.body, role, walk);
        if scoped {
            walk.ctes.pop();
        }
        result
    }

    fn eager_set_expr<'q>(&self, body: &'q SetExpr, role: &str, walk: &mut Walk<'q>) -> Result<()> {
        match body {
            SetExpr::Select(select) => {
                for from in &select.from {
                    self.eager_factor(&from.relation, role, walk)?;
                    for join in &from.joins {
                        self.eager_factor(&join.relation, role, walk)?;
                    }
                }
                Ok(())
            }
            SetExpr::Query(query) => self.eager_query(query, role, walk),
            SetExpr::SetOperation { left, right, .. } => {
                self.eager_set_expr(left, role, walk)?;
                self.eager_set_expr(right, role, walk)
            }
            _ => Ok(()),
        }
    }

    fn eager_factor<'q>(
        &self,
        factor: &'q TableFactor,
        role: &str,
        walk: &mut Walk<'q>,
    ) -> Result<()> {
        match factor {
            TableFactor::Table {
                name, args: None, ..
            } => {
                let Ok(name) = relation_name(name) else {
                    return Ok(());
                };
                let cte = walk.ctes.iter().rev().find_map(|scope| {
                    scope
                        .iter()
                        .find(|(cte, _)| cte.eq_ignore_ascii_case(&name))
                        .map(|(_, query)| *query)
                });
                if let Some(cte) = cte {
                    if walk.walked.insert(cte as *const Query as usize) {
                        self.eager_query(cte, role, walk)?;
                    }
                    return Ok(());
                }
                // A CTE bound by an enclosing statement (view or routine).
                if self.cte(&name).is_some() || is_virtual_table(&name) {
                    return Ok(());
                }
                if let Some(view) = load_view(self.db_ref(), &name)? {
                    self.require_select_as(role, &name)?;
                    if walk.views.iter().any(|view| view == &name) {
                        return Err(SqlError::Unsupported(format!(
                            "recursive view expansion for {name}"
                        )));
                    }
                    let Ok(query) = parse_query(&view.query_sql) else {
                        return Ok(());
                    };
                    let executor = match &view.owner {
                        Some(owner) if !view.security_invoker => owner.clone(),
                        _ => role.to_string(),
                    };
                    let mut inner = Walk {
                        ctes: Vec::new(),
                        walked: BTreeSet::new(),
                        views: walk.views.clone(),
                    };
                    inner.views.push(name);
                    return self.eager_query(&query, &executor, &mut inner);
                }
                if load_sequence(self.db_ref(), &name)?.is_some() {
                    return Ok(());
                }
                // Missing relations are the executor's error to report.
                if !relation_exists(self.db_ref(), &name) {
                    return Ok(());
                }
                let table = resolve_session_relation_name(self.db_ref(), &name)?;
                self.require_select_as(role, &table)
            }
            TableFactor::Derived { subquery, .. } => self.eager_query(subquery, role, walk),
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.eager_factor(&table_with_joins.relation, role, walk)?;
                for join in &table_with_joins.joins {
                    self.eager_factor(&join.relation, role, walk)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn require_select_as(&self, role: &str, table: &str) -> Result<()> {
        if role_has_table_privilege(self.db_ref(), role, table, "SELECT")? {
            return Ok(());
        }
        Err(SqlError::BicDb(BicDbError::Authorization(format!(
            "permission denied for table {}",
            relation_name_for_message(table)
        ))))
    }
}
