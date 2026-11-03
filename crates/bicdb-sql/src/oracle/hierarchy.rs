//! Depth-first hierarchy expansion with caller-supplied SQL cycle-key equality.
use super::*;

pub(super) fn bind_pseudocolumns<T: sqlparser::ast::VisitMut>(node: &mut T) -> Result<()> {
    struct Bind {
        scopes: Vec<bool>,
    }
    impl sqlparser::ast::VisitorMut for Bind {
        type Break = SqlError;
        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            self.scopes.push(matches!(query.body.as_ref(),SetExpr::Select(select) if !select.connect_by.is_empty()));
            ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<Self::Break> {
            self.scopes.pop();
            ControlFlow::Continue(())
        }
        fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
            if !select.connect_by.is_empty() {
                for item in &mut select.projection {
                    if let SelectItem::UnnamedExpr(Expr::Identifier(id)) = item {
                        if id.quote_style.is_none()
                            && (id.value.eq_ignore_ascii_case("LEVEL")
                                || id.value.eq_ignore_ascii_case("CONNECT_BY_ISCYCLE"))
                        {
                            *item = SelectItem::ExprWithAlias {
                                expr: Expr::Identifier(id.clone()),
                                alias: Ident::with_quote('"', id.value.to_ascii_uppercase()),
                            };
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            if self.scopes.last() == Some(&true) {
                if let Expr::Identifier(id) = expr {
                    if id.quote_style.is_none() {
                        let function = if id.value.eq_ignore_ascii_case("LEVEL") {
                            Some("bicdb_oracle_hierarchy_level()")
                        } else if id.value.eq_ignore_ascii_case("CONNECT_BY_ISCYCLE") {
                            Some("bicdb_oracle_hierarchy_cycle()")
                        } else {
                            None
                        };
                        if let Some(function) = function {
                            match expression(function) {
                                Ok(value) => *expr = value,
                                Err(error) => return ControlFlow::Break(error),
                            }
                        }
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    match sqlparser::ast::VisitMut::visit(node, &mut Bind { scopes: Vec::new() }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

pub(crate) struct Node<K> {
    pub row: usize,
    pub key: K,
}

pub(crate) struct Visit {
    pub row: usize,
    pub level: usize,
    pub cycle: bool,
}

/// Keys contain evaluated PRIOR expressions, rather than physical row IDs.
/// The SQL caller supplies ordering, expression evaluation, typed key equality
/// and cancellation. Disabling cycle checks implements non-PRIOR expansion.
pub(crate) fn traverse<K, Roots, Children, Expand, Equal, Cancel>(
    roots: Roots,
    mut expand: Expand,
    mut equal: Equal,
    mut cancel: Cancel,
    cycle_mode: Option<bool>,
    maximum_nodes: usize,
) -> Result<Vec<Visit>>
where
    Roots: IntoIterator<Item = Result<Node<K>>>,
    Children: IntoIterator<Item = Result<Node<K>>>,
    Expand: FnMut(&Node<K>, usize) -> Result<Children>,
    Equal: FnMut(&K, &K) -> Result<bool>,
    Cancel: FnMut() -> Result<()>,
{
    struct Frame<K> {
        node: Node<K>,
        children: std::vec::IntoIter<Node<K>>,
    }
    let mut output = Vec::new();
    let mut stack: Vec<Frame<K>> = Vec::new();
    let mut pending = 0usize;
    let mut roots = roots.into_iter();
    loop {
        cancel()?;
        let next = match stack.last_mut() {
            Some(frame) => match frame.children.next() {
                Some(node) => {
                    pending -= 1;
                    Some(Ok(node))
                }
                None => {
                    stack.pop();
                    continue;
                }
            },
            None => roots.next(),
        };
        let Some(node) = next else { break };
        let node = node?;
        let level = stack.len() + 1;
        if output.len().saturating_add(pending) >= maximum_nodes {
            return Err(SqlError::data_exception(
                "54000",
                "hierarchy node budget exceeded",
                None,
            ));
        }
        let mut cycle = false;
        let mut children = Vec::new();
        for child in expand(&node, level + 1)? {
            cancel()?;
            let child = child?;
            let mut repeats = false;
            if cycle_mode.is_some() {
                repeats = equal(&node.key, &child.key)?;
                if !repeats {
                    for frame in &stack {
                        if equal(&frame.node.key, &child.key)? {
                            repeats = true;
                            break;
                        }
                    }
                }
            }
            if repeats {
                if cycle_mode == Some(false) {
                    return Err(errors::database_error(
                        -1436,
                        "54001",
                        "CONNECT BY loop in user data",
                    ));
                }
                cycle = true;
                continue;
            }
            if output
                .len()
                .saturating_add(pending)
                .saturating_add(children.len())
                .saturating_add(1)
                >= maximum_nodes
            {
                return Err(SqlError::data_exception(
                    "54000",
                    "hierarchy node budget exceeded",
                    None,
                ));
            }
            children.push(child);
        }
        output.push(Visit {
            row: node.row,
            level,
            cycle,
        });
        pending += children.len();
        stack.push(Frame {
            node,
            children: children.into_iter(),
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_binds_level_and_sibling_order_before_native_validation() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_profile_sql(
            &db,
            "SELECT LEVEL FROM DUAL CONNECT BY LEVEL<=3",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(3)]
            ]
        );
        assert_eq!(result.columns, vec!["LEVEL"]);
        for sql in [
            "CREATE TABLE hierarchy_rows(id NUMBER,parent_id NUMBER)",
            "INSERT INTO hierarchy_rows VALUES(1,NULL),(2,1),(3,1),(4,2)",
        ] {
            let sql = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            SqlSession::new(&mut db).execute(&sql).unwrap();
        }
        let sql=lower_profile_sql(&db,"SELECT id,LEVEL FROM hierarchy_rows WHERE id<>2 START WITH parent_id IS NULL CONNECT BY PRIOR id=parent_id ORDER SIBLINGS BY id",ORACLE_PREVIEW_PROFILE).unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec!["1", "1"], vec!["4", "3"], vec!["3", "2"]]
        );
    }

    #[test]
    fn nested_hierarchies_and_wildcards_keep_query_local_state() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        for (sql, columns, expected) in [
            ("SELECT LEVEL,(SELECT MAX(LEVEL) FROM DUAL CONNECT BY LEVEL<=2) MX FROM DUAL CONNECT BY LEVEL<=3", vec!["LEVEL","MX"], vec![vec!["1","2"],vec!["2","2"],vec!["3","2"]]),
            ("SELECT T.*,LEVEL FROM (SELECT 7 ID FROM DUAL) T CONNECT BY LEVEL<=2", vec!["ID","LEVEL"], vec![vec!["7","1"],vec!["7","2"]]),
            ("SELECT * FROM (SELECT LEVEL L FROM DUAL CONNECT BY LEVEL<=2) CONNECT BY LEVEL<=2", vec!["L"], vec![vec!["1"],vec!["1"],vec!["2"],vec!["2"],vec!["1"],vec!["2"]]),
        ] {
            let lowered = lower_profile_sql(&db, sql, ORACLE_PREVIEW_PROFILE).unwrap();
            let result = SqlSession::new(&mut db).execute(&lowered).unwrap();
            assert_eq!(result.columns, columns, "{sql}");
            assert_eq!(result.rows.iter().map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>()).collect::<Vec<_>>(), expected, "{sql}");
        }
    }

    #[test]
    fn volatile_raw_keys_do_not_create_false_hierarchy_cycles() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let sql = lower_profile_sql(
            &db,
            "SELECT LEVEL FROM DUAL CONNECT BY PRIOR SYS_GUID() IS NOT NULL AND LEVEL<=3",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(2)],
                vec![SqlValue::Int(3)]
            ]
        );
        let sql = lower_profile_sql(
            &db,
            "SELECT SYS_GUID(),SYS_GUID() FROM DUAL",
            ORACLE_PREVIEW_PROFILE,
        )
        .unwrap();
        let result = SqlSession::new(&mut db).execute(&sql).unwrap();
        assert_ne!(result.rows[0][0], result.rows[0][1]);
        for value in &result.rows[0] {
            let text = value.to_cell();
            assert!(text.starts_with("\\x"));
            assert_eq!(text.len(), 34);
        }
        let native_root = tempfile::tempdir().unwrap();
        let mut native = BicDb::open(native_root.path()).unwrap();
        let mut session = SqlSession::new(&mut native);
        session
            .execute("CREATE FUNCTION sys_guid() RETURNS INT LANGUAGE sql AS $$SELECT 7$$")
            .unwrap();
        assert_eq!(
            session.execute("SELECT sys_guid()").unwrap().rows,
            vec![vec![SqlValue::Int(7)]]
        );
    }

    #[test]
    fn persisted_hierarchy_fence_keeps_temporal_and_character_capabilities() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        prepare_hierarchy_save(&mut db, "SELECT 'CONNECT BY' FROM DUAL").unwrap();
        assert!(!metadata_v3_enabled(&db).unwrap());
        prepare_hierarchy_save(
            &mut db,
            "SELECT LEVEL FROM DUAL CONNECT /* comment */ BY LEVEL<=3",
        )
        .unwrap();
        assert!(metadata_v2_enabled(&db).unwrap());
        assert!(metadata_v3_enabled(&db).unwrap());
        enable_metadata_v2(&mut db).unwrap();
        enable_metadata_v3(&mut db).unwrap();
        assert_eq!(
            db.get(PROFILE_COLLECTION, "profile")
                .unwrap()
                .unwrap()
                .metadata,
            metadata_v4()
        );
        assert!(!matches_profile(&metadata_v4(), POSTGRES_PROFILE));
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            db.get(PROFILE_COLLECTION, "profile")
                .unwrap()
                .unwrap()
                .metadata,
            metadata_v4()
        );
    }

    #[test]
    fn sibling_order_and_levels_are_depth_first() {
        let result = traverse(
            vec![Ok(Node { row: 1, key: 1 }), Ok(Node { row: 5, key: 5 })],
            |node, _| {
                Ok(match node.row {
                    1 => vec![2, 3],
                    2 => vec![4],
                    _ => vec![],
                }
                .into_iter()
                .map(|row| Ok(Node { row, key: row }))
                .collect::<Vec<_>>())
            },
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            100,
        )
        .unwrap();
        assert_eq!(
            result.iter().map(|v| (v.row, v.level)).collect::<Vec<_>>(),
            vec![(1, 1), (2, 2), (4, 3), (3, 2), (5, 1)]
        );
    }

    #[test]
    fn different_rows_with_equal_prior_keys_mark_the_parent() {
        let result = traverse(
            vec![Ok(Node { row: 1, key: 1 })],
            |_, _| Ok(vec![Ok(Node { row: 2, key: 1 })]),
            |a, b| Ok(a == b),
            || Ok(()),
            Some(true),
            10,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].cycle);
        assert!(traverse(
            vec![Ok(Node { row: 1, key: 1 })],
            |_, _| Ok(vec![Ok(Node { row: 2, key: 1 })]),
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            10
        )
        .is_err());
    }

    #[test]
    fn non_prior_generators_revisit_rows_and_keep_an_unexpanded_root() {
        for bound in [0, 1, 3] {
            let result = traverse(
                vec![Ok(Node { row: 0, key: () })],
                |_, level| {
                    Ok(if level <= bound {
                        vec![Ok(Node { row: 0, key: () })]
                    } else {
                        vec![]
                    })
                },
                |_, _| Ok(true),
                || Ok(()),
                None,
                10,
            )
            .unwrap();
            assert_eq!(result.len(), bound.max(1));
        }
    }

    #[test]
    fn changing_prior_keys_allow_repeated_physical_rows() {
        let result = traverse(
            vec![Ok(Node { row: 0, key: 1 })],
            |_, level| {
                Ok(if level <= 3 {
                    vec![Ok(Node { row: 0, key: level })]
                } else {
                    vec![]
                })
            },
            |a, b| Ok(a == b),
            || Ok(()),
            Some(false),
            10,
        )
        .unwrap();
        assert_eq!(
            result.iter().map(|v| v.level).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(result.iter().all(|v| v.row == 0 && !v.cycle));
    }

    #[test]
    fn unbounded_expansion_obeys_the_budget_and_cancellation() {
        let roots = || vec![Ok(Node { row: 0, key: () })];
        assert!(traverse(
            roots(),
            |_, _| Ok(roots()),
            |_, _| Ok(true),
            || Ok(()),
            None,
            3
        )
        .is_err());
        assert!(traverse(
            roots(),
            |_, _| Ok(roots()),
            |_, _| Ok(true),
            || Err(unsupported("cancelled")),
            None,
            3
        )
        .is_err());
    }
}
