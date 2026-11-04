//! Matched-update MERGE uses the ordinary authorized UPDATE executor, with
//! statement-wide target cardinality rules instead of UPDATE FROM's first match.
use crate::*;
use sqlparser::ast::{Merge, MergeAction, MergeClauseKind, Update};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpdateMatchPolicy {
    First,
    PostgresMerge,
    OracleMerge,
}

impl UpdateMatchPolicy {
    pub(super) fn duplicate_error(self) -> SqlError {
        SqlError::data_exception(
            "21000",
            if self == Self::OracleMerge {
                "ORA-30926: MERGE cannot update the same target row twice"
            } else {
                "MERGE command cannot affect row a second time"
            },
            None,
        )
    }
}

impl SqlSession<'_> {
    pub(super) fn execute_merge(&mut self, merge: &Merge) -> Result<SqlResult> {
        // Validate the complete shape before evaluating the source or making
        // changes. Unsupported actions must never be silently discarded.
        let [clause] = merge.clauses.as_slice() else {
            return Err(SqlError::Unsupported(
                "MERGE currently requires one matched UPDATE clause".into(),
            ));
        };
        let MergeAction::Update(action) = &clause.action else {
            return Err(SqlError::Unsupported(
                "MERGE currently supports matched UPDATE actions".into(),
            ));
        };
        if clause.clause_kind != MergeClauseKind::Matched
            || action.delete_predicate.is_some()
            || merge.output.is_some()
        {
            return Err(SqlError::Unsupported(
                "MERGE action shape is not supported".into(),
            ));
        }
        let oracle = oracle::preview_routine_language_enabled(self.db_ref())?;
        if !oracle && action.update_predicate.is_some() {
            return Err(SqlError::InvalidSql(
                "PostgreSQL MERGE uses WHEN MATCHED AND, not UPDATE WHERE".into(),
            ));
        }
        let target = TableWithJoins {
            relation: merge.table.clone(),
            joins: Vec::new(),
        };
        let (table, _) = table_with_joins_name_and_alias(&target)?;
        let TableFactor::Table {
            name: target_name,
            alias: target_alias,
            ..
        } = &merge.table
        else {
            return Err(SqlError::Unsupported(
                "MERGE requires a table target".into(),
            ));
        };
        let target_parts = object_name_parts(target_name);
        let target_qualifiers = if let Some(alias) = target_alias {
            let name = ident_value(&alias.name);
            let mut qualifiers = vec![vec![name.clone()]];
            // Oracle namespace binding supplies the visible table name as
            // an alias for an otherwise unaliased relation. Its original
            // schema.table qualification still identifies the target.
            if target_parts.last() == Some(&name) {
                qualifiers.push(target_parts.clone());
            }
            qualifiers
        } else {
            vec![
                target_parts.clone(),
                target_parts.last().cloned().into_iter().collect(),
            ]
        };
        let resolved = resolve_session_relation_name_if_exists(self.db_ref(), &table);
        let schema = load_schema_shared(self.db_ref(), &resolved)?;
        // The current adapter materializes target/source rows. Require a
        // table read grant until per-column MERGE scope authorization exists.
        self.require_table_privilege(&resolved, "SELECT")?;
        let mut assignments = action.assignments.clone();
        for assignment in &mut assignments {
            let AssignmentTarget::ColumnName(name) = &mut assignment.target else {
                return Err(SqlError::Unsupported(
                    "MERGE supports single-column assignment targets".into(),
                ));
            };
            if name.0.len() > 1 {
                let parts = object_name_parts(name);
                if !oracle
                    || !target_qualifiers
                        .iter()
                        .any(|qualifier| qualifier.as_slice() == &parts[..parts.len() - 1])
                {
                    return Err(SqlError::InvalidSql(
                        "invalid qualified MERGE assignment target".into(),
                    ));
                }
                name.0 = vec![name.0.last().unwrap().clone()];
            }
        }
        if oracle {
            if expr_contains_subquery(&merge.on) {
                return Err(SqlError::Unsupported(
                    "MERGE ON subqueries require scoped target-column analysis".into(),
                ));
            }
            let mut references = BTreeSet::new();
            let _ = sqlparser::ast::visit_expressions(merge.on.as_ref(), |expr| {
                match expr {
                    Expr::Identifier(id)
                        if schema
                            .as_ref()
                            .is_some_and(|schema| schema.column(&ident_value(id)).is_some()) =>
                    {
                        references.insert(ident_value(id));
                    }
                    Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                        let qualifier = parts[..parts.len() - 1]
                            .iter()
                            .map(ident_value)
                            .collect::<Vec<_>>();
                        if target_qualifiers.contains(&qualifier) {
                            references.insert(ident_value(parts.last().unwrap()));
                        }
                    }
                    _ => {}
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            for assignment in &assignments {
                if let AssignmentTarget::ColumnName(name) = &assignment.target {
                    let name = relation_name(name)?;
                    if references.contains(&name) {
                        return Err(SqlError::data_exception(
                            "42000",
                            "ORA-38104: Columns referenced in the ON clause cannot be updated",
                            None,
                        ));
                    }
                }
            }
        }
        let mut selection = *merge.on.clone();
        for predicate in [clause.predicate.as_ref(), action.update_predicate.as_ref()]
            .into_iter()
            .flatten()
        {
            selection = and_expr(selection, predicate.clone());
        }
        let update = Update {
            update_token: action.update_token.clone(),
            optimizer_hints: merge.optimizer_hints.clone(),
            table: target,
            assignments,
            from: Some(UpdateTableFromKind::AfterSet(vec![TableWithJoins {
                relation: merge.source.clone(),
                joins: Vec::new(),
            }])),
            selection: Some(selection),
            returning: None,
            output: None,
            or: None,
            order_by: Vec::new(),
            limit: None,
        };
        // The UPDATE is a transient adapter, not an IR-owned AST. Its heap
        // addresses must never become persistent routine memo identities.
        let owned = std::mem::replace(&mut self.ir_owned_statement, false);
        let result = self.execute_update_with_match_policy(
            &update,
            BTreeMap::new(),
            if oracle {
                UpdateMatchPolicy::OracleMerge
            } else {
                UpdateMatchPolicy::PostgresMerge
            },
        );
        self.ir_owned_statement = owned;
        let mut result = result?;
        if let Some(tag) = result.command_tag.as_mut() {
            *tag = tag.replacen("UPDATE ", "MERGE ", 1);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execute(session: &mut SqlSession<'_>, oracle: bool, source: &str) -> Result<SqlResult> {
        let sql = if oracle {
            oracle::lower_session_sql(
                session.db_ref(),
                source,
                oracle::ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )?
        } else {
            source.to_string()
        };
        session.execute(&sql)
    }

    #[test]
    fn matched_merge_preserves_each_profiles_repeated_target_rules() {
        for oracle in [false, true] {
            for (values, oracle_value) in [
                (vec![0, 1], Some(1)),
                (vec![0, 0], Some(0)),
                (vec![1, 1], None),
                (vec![1, 0], None),
                (vec![0, 0, 1], Some(1)),
            ] {
                let root = tempfile::tempdir().unwrap();
                let mut db = BicDb::open(root.path()).unwrap();
                if oracle {
                    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE)
                        .unwrap();
                }
                let mut session = SqlSession::new(&mut db);
                for source in [
                    "CREATE TABLE merge_target(id INTEGER PRIMARY KEY, n INTEGER)",
                    "CREATE TABLE merge_source(seq INTEGER PRIMARY KEY, id INTEGER, n INTEGER)",
                    "INSERT INTO merge_target VALUES(1,0)",
                ] {
                    execute(&mut session, oracle, source).unwrap();
                }
                for (i, n) in values.iter().enumerate() {
                    execute(
                        &mut session,
                        oracle,
                        &format!("INSERT INTO merge_source VALUES({i},1,{n})"),
                    )
                    .unwrap();
                }
                let result = execute(&mut session, oracle, "MERGE INTO merge_target t USING (SELECT * FROM merge_source ORDER BY seq) s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n");
                let expected = if oracle { oracle_value } else { None };
                match expected {
                    Some(n) => {
                        assert_eq!(
                            result.unwrap().command_tag,
                            Some(format!("MERGE {}", values.len()))
                        );
                        assert_eq!(
                            execute(&mut session, oracle, "SELECT n FROM merge_target")
                                .unwrap()
                                .rows[0][0]
                                .to_cell(),
                            n.to_string()
                        );
                    }
                    None => {
                        assert_eq!(
                            result.unwrap_err().sqlstate(),
                            "21000",
                            "oracle={oracle} values={values:?}"
                        );
                        assert_eq!(
                            execute(&mut session, oracle, "SELECT n FROM merge_target")
                                .unwrap()
                                .rows[0][0]
                                .to_cell(),
                            "0"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn merge_filters_and_statement_rollback_keep_normal_update_from_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER CHECK(n>=0))")
            .unwrap();
        session
            .execute("CREATE TABLE merge_source(seq INTEGER PRIMARY KEY,id INTEGER,n INTEGER)")
            .unwrap();
        session
            .execute("INSERT INTO merge_target VALUES(1,0),(2,0)")
            .unwrap();
        session
            .execute("INSERT INTO merge_source VALUES(1,1,10),(2,2,-1)")
            .unwrap();
        let failed = session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n");
        assert_eq!(failed.unwrap_err().sqlstate(), "23514");
        assert_eq!(
            session
                .execute("SELECT SUM(n) FROM merge_target")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "0"
        );
        let result = session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED AND s.n>=0 THEN UPDATE SET n=s.n").unwrap();
        assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
        session.execute("DELETE FROM merge_source").unwrap();
        session
            .execute("INSERT INTO merge_source VALUES(1,1,11),(2,1,12)")
            .unwrap();
        session.execute("UPDATE merge_target AS t SET n=s.n FROM (SELECT * FROM merge_source ORDER BY seq) s WHERE t.id=s.id").unwrap();
        assert_eq!(
            session
                .execute("SELECT n FROM merge_target WHERE id=1")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "11"
        );
    }

    #[test]
    fn matched_merge_requires_target_update_and_source_read_authority() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE merge_writer LOGIN",
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
            "GRANT UPDATE(n) ON merge_target TO merge_writer",
            "SET SESSION AUTHORIZATION merge_writer",
        ] {
            session.execute(sql).unwrap();
        }
        let sql = "MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n";
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42501");
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
        session
            .execute("GRANT SELECT ON merge_target TO merge_writer")
            .unwrap();
        session
            .execute("SET SESSION AUTHORIZATION merge_writer")
            .unwrap();
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42501");
        session.execute("RESET SESSION AUTHORIZATION").unwrap();
        session
            .execute("GRANT SELECT ON merge_source TO merge_writer")
            .unwrap();
        session
            .execute("SET SESSION AUTHORIZATION merge_writer")
            .unwrap();
        assert_eq!(
            session.execute(sql).unwrap().command_tag.as_deref(),
            Some("MERGE 1")
        );
        assert_eq!(session.execute("MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET id=2").unwrap_err().sqlstate(),"42501");
    }

    #[test]
    fn transient_merge_adapters_do_not_reuse_routine_node_addresses() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
            "CREATE FUNCTION merge_twice(p INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n+p; MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET n=s.n+p*2; RETURN 1; END$$",
        ] { session.execute(sql).unwrap(); }
        for p in [1, 2, 3] {
            session
                .execute(&format!("SELECT merge_twice({p})"))
                .unwrap();
            assert_eq!(
                session.execute("SELECT n FROM merge_target").unwrap().rows[0][0].to_cell(),
                (10 + p * 2).to_string()
            );
        }
    }

    #[test]
    fn oracle_on_columns_keep_quoted_case_and_schema_qualification() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE SCHEMA \"Scope\"",
            "CREATE TABLE \"Scope\".\"Target\"(\"Key\" INTEGER,\"key\" INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO \"Scope\".\"Target\" VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,9)",
        ] {
            execute(&mut session, true, source).unwrap();
        }
        execute(&mut session,true,"MERGE INTO \"Scope\".\"Target\" t USING merge_source s ON(t.\"Key\"=s.id) WHEN MATCHED THEN UPDATE SET t.\"key\"=s.n").unwrap();
        let error=execute(&mut session,true,"MERGE INTO \"Scope\".\"Target\" USING merge_source s ON(\"Scope\".\"Target\".\"Key\"=s.id) WHEN MATCHED THEN UPDATE SET \"Key\"=s.n").unwrap_err();
        assert!(error.to_string().contains("ORA-38104"));
        assert_eq!(
            execute(
                &mut session,
                true,
                "SELECT \"Key\",\"key\" FROM \"Scope\".\"Target\""
            )
            .unwrap()
            .rows[0]
                .iter()
                .map(SqlValue::to_cell)
                .collect::<Vec<_>>(),
            vec!["1", "9"]
        );
    }

    #[test]
    fn oracle_update_where_and_on_column_restriction_are_explicit() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        for source in [
            "CREATE TABLE merge_target(id INTEGER PRIMARY KEY,n INTEGER)",
            "CREATE TABLE merge_source(id INTEGER,n INTEGER)",
            "INSERT INTO merge_target VALUES(1,0)",
            "INSERT INTO merge_source VALUES(1,10)",
        ] {
            execute(&mut session, true, source).unwrap();
        }
        let result = execute(&mut session,true,"MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET t.n=s.n WHERE t.n=0").unwrap();
        assert_eq!(result.command_tag.as_deref(), Some("MERGE 1"));
        let error = execute(&mut session,true,"MERGE INTO merge_target t USING merge_source s ON(t.id=s.id) WHEN MATCHED THEN UPDATE SET t.id=2").unwrap_err();
        assert_eq!(error.sqlstate(), "42000");
        assert!(error.to_string().contains("ORA-38104"));
        assert_eq!(
            execute(&mut session, true, "SELECT id FROM merge_target")
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "1"
        );
    }
}
