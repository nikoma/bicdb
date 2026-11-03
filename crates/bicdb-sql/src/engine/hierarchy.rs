//! Hierarchy materialization, before output WHERE/projection/aggregation.
use super::*;
use sqlparser::ast::ConnectByKind;

impl<'db> SqlEngine<'db> {
    /// Internal integration entry; the profile compiler must establish grammar,
    /// durable-reader fencing and pseudocolumn bindings before public dispatch.
    pub(crate) fn execute_hierarchy_query(
        &self,
        select: &Select,
        query: &Query,
        siblings: bool,
        maximum_nodes: usize,
    ) -> Result<SqlResult> {
        let mut start = None;
        let mut relationships = None;
        let mut nocycle = false;
        for clause in &select.connect_by {
            match clause {
                ConnectByKind::StartWith { condition, .. } if start.is_none() => {
                    start = Some(condition.as_ref().clone())
                }
                ConnectByKind::ConnectBy {
                    relationships: conditions,
                    nocycle: mode,
                    ..
                } if relationships.is_none() => {
                    relationships = Some(conditions.clone());
                    nocycle = *mode;
                }
                _ => return Err(SqlError::Unsupported("duplicate hierarchy clauses".into())),
            }
        }
        let mut conditions = relationships
            .ok_or_else(|| SqlError::InvalidSql("START WITH requires CONNECT BY".into()))?;
        let mut normalized_from;
        let from = match select.from.as_slice() {
            [] => {
                return Err(SqlError::Unsupported(
                    "hierarchy without a materialized FROM requires the profile DUAL binding"
                        .into(),
                ))
            }
            [from] => from,
            items => {
                normalized_from = Self::comma_from_items_to_cross_join(items);
                &mut normalized_from
            }
        };
        // No output WHERE, projection pruning or top-k pushdown may discard a
        // parent before its descendants are expanded.
        let mut base = self.row_set_from_table_with_joins(from)?;
        if siblings {
            self.apply_row_order_by(
                &mut base.rows,
                query.order_by.as_ref(),
                &base.columns,
                &[],
                None,
            )?;
        }
        let mut output_select = select.clone();
        output_select.connect_by.clear();
        if output_select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        }) {
            return Err(SqlError::Unsupported(
                "hierarchy wildcard binding requires original-source projection metadata".into(),
            ));
        }
        let mut used: BTreeSet<String> = base
            .columns
            .iter()
            .flat_map(|name| {
                [
                    name.clone(),
                    name.rsplit('.').next().unwrap_or(name).to_owned(),
                ]
            })
            .collect();
        let _ = sqlparser::ast::visit_expressions(query, |expr| {
            if let Expr::Identifier(id) = expr {
                used.insert(id.value.clone());
            }
            ControlFlow::<()>::Continue(())
        });
        let mut fresh = |kind: &str| {
            let mut index = 0;
            loop {
                let name = format!("__bicdb_hierarchy_{kind}_{index}");
                index += 1;
                if used.insert(name.clone()) {
                    break name;
                }
            }
        };
        let level_column = fresh("level");
        let cycle_column = fresh("cycle");
        let mut priors = Vec::new();
        let mut prior_columns = Vec::new();
        let _ = visit_expressions_mut(&mut conditions, |expr| {
            if let Expr::Prior(inner) = expr {
                priors.push(inner.as_ref().clone());
                let name = fresh("prior");
                prior_columns.push(name.clone());
                *expr = Expr::Identifier(Ident::with_quote('"', name));
            }
            ControlFlow::<()>::Continue(())
        });
        let mut rewrite_pseudo = |expr: &mut Expr| {
            if let Expr::Identifier(id) = expr {
                if id.quote_style.is_none() {
                    if id.value.eq_ignore_ascii_case("LEVEL") {
                        *id = Ident::with_quote('"', &level_column);
                    } else if id.value.eq_ignore_ascii_case("CONNECT_BY_ISCYCLE") {
                        *id = Ident::with_quote('"', &cycle_column);
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        };
        let mut output_query = query.clone();
        let _ = visit_expressions_mut(&mut output_select, &mut rewrite_pseudo);
        let _ = visit_expressions_mut(&mut output_query, &mut rewrite_pseudo);
        let _ = visit_expressions_mut(&mut conditions, &mut rewrite_pseudo);
        let _ = visit_expressions_mut(&mut priors, &mut rewrite_pseudo);
        if let Some(start) = &mut start {
            let _ = visit_expressions_mut(start, &mut rewrite_pseudo);
        }
        if siblings {
            output_query.order_by = None;
        }
        output_query.body = Box::new(SetExpr::Select(Box::new(output_select.clone())));
        let row_at = |index: usize, level: usize| {
            let mut row = slot_row_to_sql_row(&base.columns, &base.rows[index]);
            row.insert(level_column.clone(), SqlValue::Int(level as i64));
            row.insert(cycle_column.clone(), SqlValue::Int(0));
            row
        };
        let key = |row: &SqlRow| {
            priors
                .iter()
                .map(|expr| self.eval_row_value(row, expr))
                .collect::<Result<Vec<_>>>()
        };
        let roots = (0..base.rows.len()).filter_map(|index| {
            let row = row_at(index, 1);
            match start
                .as_ref()
                .map(|expr| self.eval_row_predicate(&row, expr))
                .transpose()
            {
                Err(error) => Some(Err(error)),
                Ok(Some(false)) => None,
                _ => Some(key(&row).map(|key| oracle::hierarchy::Node { row: index, key })),
            }
        });
        let visits = oracle::hierarchy::traverse(
            roots,
            |parent, level| {
                let parent_key = parent.key.clone();
                let row_at = &row_at;
                let key = &key;
                let conditions = &conditions;
                let prior_columns = &prior_columns;
                Ok((0..base.rows.len()).filter_map(move |index| {
                    if let Err(error) = self.check_cancellation() {
                        return Some(Err(error));
                    }
                    let mut row = row_at(index, level);
                    for (name, value) in prior_columns.iter().zip(&parent_key) {
                        row.insert(name.clone(), value.clone());
                    }
                    for expr in conditions.iter() {
                        match self.eval_row_predicate(&row, expr) {
                            Ok(true) => {}
                            Ok(false) => return None,
                            Err(error) => return Some(Err(error)),
                        }
                    }
                    Some(key(&row).map(|key| oracle::hierarchy::Node { row: index, key }))
                }))
            },
            |a, b| tuple_values_not_distinct(a, b),
            || self.check_cancellation(),
            (!priors.is_empty()).then_some(nocycle),
            maximum_nodes,
        )?;
        let mut rows = Vec::with_capacity(visits.len());
        for visit in visits {
            let mut row = base.rows[visit.row].clone();
            row.push(SqlValue::Int(visit.level as i64));
            row.push(SqlValue::Int(i64::from(visit.cycle)));
            rows.push(row);
        }
        base.columns.extend([level_column, cycle_column]);
        self.execute_materialized_row_query(
            &output_select,
            &output_query,
            from,
            RowSet {
                rows,
                columns: base.columns,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query(sql: &str) -> Query {
        let Statement::Query(query) = Parser::parse_sql(&sqlparser::dialect::OracleDialect {}, sql)
            .unwrap()
            .remove(0)
        else {
            panic!("query expected")
        };
        *query
    }
    #[test]
    fn generator_aggregation_sees_all_root_and_child_rows() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE roots(id INTEGER PRIMARY KEY)")
                .unwrap();
            session.execute("INSERT INTO roots VALUES(1),(2)").unwrap();
        }
        let engine = SqlEngine::new(&db);
        let query=query("SELECT LEVEL AS depth,COUNT(*) FROM roots CONNECT BY LEVEL<=3 GROUP BY LEVEL ORDER BY LEVEL");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select expected")
        };
        let result = engine
            .execute_hierarchy_query(select, &query, false, 100)
            .unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Int(1), SqlValue::Int(2)],
                vec![SqlValue::Int(2), SqlValue::Int(4)],
                vec![SqlValue::Int(3), SqlValue::Int(8)]
            ]
        );
    }
    #[test]
    fn query_dispatch_requires_the_durable_preview_profile() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let q = query("SELECT id FROM (SELECT 1 AS id) roots CONNECT BY LEVEL<=3");
        assert_eq!(
            SqlEngine::new(&db)
                .execute_query(&q)
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let result = SqlEngine::new(&db).execute_query(&q).unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(1)],
                vec![SqlValue::Int(1)]
            ]
        );
    }
    #[test]
    fn sql_cycle_keys_and_null_generator_bounds_preserve_root_semantics() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE cycle_nodes(id INTEGER PRIMARY KEY,parent_id INTEGER)")
                .unwrap();
            session
                .execute("INSERT INTO cycle_nodes VALUES(10,11),(11,10)")
                .unwrap();
        }
        let engine = SqlEngine::new(&db);
        let q=query("SELECT id,LEVEL AS depth,CONNECT_BY_ISCYCLE AS cycle FROM cycle_nodes START WITH id=10 CONNECT BY NOCYCLE PRIOR id=parent_id");
        let SetExpr::Select(select) = q.body.as_ref() else {
            panic!("select expected")
        };
        let result = engine
            .execute_hierarchy_query(select, &q, false, 100)
            .unwrap();
        assert_eq!(
            result.rows,
            vec![
                vec![SqlValue::Int(10), SqlValue::Int(1), SqlValue::Int(0)],
                vec![SqlValue::Int(11), SqlValue::Int(2), SqlValue::Int(1)]
            ]
        );
        let q = query("SELECT id FROM cycle_nodes START WITH id=10 CONNECT BY PRIOR id=parent_id");
        let SetExpr::Select(select) = q.body.as_ref() else {
            panic!("select expected")
        };
        assert!(engine
            .execute_hierarchy_query(select, &q, false, 100)
            .is_err());
        let q = query("SELECT LEVEL AS depth FROM cycle_nodes CONNECT BY LEVEL<=NULL");
        let SetExpr::Select(select) = q.body.as_ref() else {
            panic!("select expected")
        };
        assert_eq!(
            engine
                .execute_hierarchy_query(select, &q, false, 100)
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(1)]]
        );
    }
    #[test]
    fn hierarchy_expansion_precedes_where_and_reverses_prior_direction() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE nodes(id INTEGER PRIMARY KEY,parent_id INTEGER)")
                .unwrap();
            session
                .execute("INSERT INTO nodes VALUES(1,NULL),(2,1),(3,1),(4,2),(5,NULL)")
                .unwrap();
        }
        let engine = SqlEngine::new(&db);
        for (sql,expected) in [
            ("SELECT id,LEVEL AS depth FROM nodes WHERE id<>2 START WITH parent_id IS NULL CONNECT BY PRIOR id=parent_id ORDER BY id",vec![vec![1,1],vec![4,3],vec![3,2],vec![5,1]]),
            ("SELECT id,LEVEL AS depth FROM nodes START WITH id=4 CONNECT BY PRIOR parent_id=id",vec![vec![4,1],vec![2,2],vec![1,3]]),
        ] {
            let query=query(sql);let SetExpr::Select(select)=query.body.as_ref() else { panic!("select expected") };
            let result=engine.execute_hierarchy_query(select,&query,true,100).unwrap();
            assert_eq!(result.rows,expected.into_iter().map(|row|row.into_iter().map(SqlValue::Int).collect::<Vec<_>>()).collect::<Vec<_>>());
        }
    }
}
