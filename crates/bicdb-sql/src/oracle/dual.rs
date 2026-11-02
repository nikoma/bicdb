//! Shared Oracle one-row DUAL lowering for sessions and stored routine SQL.
use super::*;
fn is_dual(name: &ObjectName) -> bool {
    name.0.len() == 1
        && name.0[0].as_ident().is_some_and(|id| {
            if id.quote_style.is_some() {
                id.value == "DUAL"
            } else {
                id.value.eq_ignore_ascii_case("DUAL")
            }
        })
}
pub(super) fn lower<T: sqlparser::ast::VisitMut>(
    node: &mut T,
    fold_identifiers: bool,
) -> Result<()> {
    struct Dual {
        fold_identifiers: bool,
        ctes: Vec<BTreeSet<String>>,
        scalar_queries: Vec<bool>,
    }
    impl sqlparser::ast::VisitorMut for Dual {
        type Break = SqlError;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            let mut names = self.ctes.last().cloned().unwrap_or_default();
            if let Some(with) = &query.with {
                for cte in &with.cte_tables {
                    let id = &cte.alias.name;
                    names.insert(if id.quote_style.is_some() {
                        id.value.clone()
                    } else {
                        id.value.to_uppercase()
                    });
                }
            }
            let plain = query.with.is_none()
                && query.order_by.is_none()
                && query.limit_clause.is_none()
                && query.fetch.is_none()
                && query.locks.is_empty()
                && query.for_clause.is_none()
                && matches!(query.body.as_ref(), SetExpr::Select(select)
                    if matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty())
                        && select.having.is_none() && select.qualify.is_none()
                        && select.distinct.is_none() && select.top.is_none()
                        && select.prewhere.is_none() && select.named_window.is_empty());
            self.scalar_queries.push(plain);
            self.ctes.push(names);
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            self.ctes.pop();
            self.scalar_queries.pop();
            if !self.fold_identifiers {
                return ControlFlow::Continue(());
            }
            for lock in &mut query.locks {
                if let Some(of) = &mut lock.of {
                    fold_name(of);
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
            let only_dual = !self.ctes.last().is_some_and(|names| names.contains("DUAL"))
                && select.from.len() == 1
                && select.from[0].joins.is_empty()
                && matches!(&select.from[0].relation,TableFactor::Table{name,..} if is_dual(name));
            if only_dual {
                let mut sequence = false;
                let mut references_row = false;
                let mut row_context = false;
                let _ = sqlparser::ast::visit_expressions(select, |expr| {
                    match expr {
                        Expr::Function(function)
                            if matches!(
                                function.name.to_string().as_str(),
                                "nextval" | "currval"
                            ) =>
                        {
                            sequence = true
                        }
                        Expr::Function(function)
                            if function.over.is_some() || is_aggregate_function(function) =>
                        {
                            row_context = true
                        }
                        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                            row_context = true
                        }
                        Expr::Identifier(_)
                        | Expr::CompoundIdentifier(_)
                        | Expr::Wildcard(_)
                        | Expr::QualifiedWildcard(..) => references_row = true,
                        _ => {}
                    }
                    ControlFlow::<()>::Continue(())
                });
                if sequence {
                    if references_row {
                        return ControlFlow::Break(unsupported(
                            "sequence projections from DUAL cannot also reference its row",
                        ));
                    }
                    // Stateful sequence evaluation belongs to SqlSession's
                    // scalar SELECT path, rather than a read-only derived row.
                    select.from.clear();
                } else if self.scalar_queries.last() == Some(&true)
                    && !references_row
                    && !row_context
                {
                    // DUAL contributes exactly one constant row. A row-independent
                    // scalar expression belongs to the caller's session, including
                    // stored functions that use session-local sequence state.
                    select.from.clear();
                }
            }
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            if !self.fold_identifiers {
                return ControlFlow::Continue(());
            }
            match expr {
                Expr::Identifier(ident) => fold(ident),
                Expr::CompoundIdentifier(idents) => idents.iter_mut().for_each(fold),
                _ => {}
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
            if !self.fold_identifiers {
                return ControlFlow::Continue(());
            }
            if let Statement::CreateTable(table) = statement {
                for column in &mut table.columns {
                    fold(&mut column.name);
                }
            }
            if let Statement::Copy {
                source: sqlparser::ast::CopySource::Table { columns, .. },
                ..
            } = statement
            {
                columns.iter_mut().for_each(fold);
            }
            if let Statement::Insert(insert) = statement {
                insert.columns.iter_mut().for_each(fold_name);
            }
            if let Statement::Update(update) = statement {
                for assignment in &mut update.assignments {
                    let names = match &mut assignment.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                            std::slice::from_mut(name)
                        }
                        sqlparser::ast::AssignmentTarget::Tuple(names) => names.as_mut_slice(),
                    };
                    names.iter_mut().for_each(fold_name);
                }
            }
            ControlFlow::Continue(())
        }
        fn post_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, alias, .. } = table {
                if is_dual(name) && !self.ctes.last().is_some_and(|names| names.contains("DUAL")) {
                    let mut parsed = Parser::parse_sql(
                        &PostgreSqlDialect {},
                        "SELECT * FROM (SELECT 'X' AS \"DUMMY\") AS \"DUAL\"",
                    )
                    .unwrap();
                    if let Statement::Query(query) = parsed.remove(0) {
                        if let SetExpr::Select(mut select) = *query.body {
                            let mut replacement = select.from.remove(0).relation;
                            if let TableFactor::Derived {
                                alias: new_alias, ..
                            } = &mut replacement
                            {
                                if alias.is_some() {
                                    *new_alias = alias.clone();
                                }
                            }
                            *table = replacement;
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    fn fold(ident: &mut Ident) {
        if ident.quote_style.is_none() {
            *ident = Ident::with_quote('"', ident.value.to_uppercase());
        }
    }
    fn fold_name(name: &mut ObjectName) {
        for part in &mut name.0 {
            if let sqlparser::ast::ObjectNamePart::Identifier(ident) = part {
                fold(ident);
            }
        }
    }
    if let ControlFlow::Break(error) = sqlparser::ast::VisitMut::visit(
        node,
        &mut Dual {
            fold_identifiers,
            ctes: Vec::new(),
            scalar_queries: Vec::new(),
        },
    ) {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod scalar_session_tests {
    use super::*;

    #[test]
    fn scalar_dual_elision_preserves_aggregates_limits_aliases_and_cte_shadows() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for (source, expected) in [
            ("SELECT 7 FROM DUAL", vec![vec!["7"]]),
            ("SELECT COUNT(1) FROM DUAL", vec![vec!["1"]]),
            ("SELECT SUM(1) FROM DUAL WHERE 1=0", vec![vec![""]]),
            ("SELECT DUMMY FROM DUAL", vec![vec!["X"]]),
            ("SELECT d.DUMMY FROM DUAL d", vec![vec!["X"]]),
            ("SELECT 7 FROM DUAL FETCH FIRST 0 ROWS ONLY", vec![]),
            (
                "WITH DUAL AS (SELECT 42 AS DUMMY) SELECT DUMMY FROM DUAL",
                vec![vec!["42"]],
            ),
        ] {
            let sql = lower_profile_sql(&db, source, ORACLE_PREVIEW_PROFILE).unwrap();
            let rows = SqlSession::new(&mut db)
                .execute(&sql)
                .unwrap_or_else(|error| panic!("{source}: {error}"));
            let cells = rows
                .rows
                .iter()
                .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            assert_eq!(cells, expected, "{source}");
        }
    }
}
