//! Scoped declarations and nested exception blocks share the routine frame.
//! Database undo follows the stored routine's language rather than its caller.
use super::*;

pub(crate) fn prepare_declared_types(
    statements: &mut [RoutineStmt],
    handlers: &mut [RoutineExceptionHandler],
) -> Result<()> {
    for statement in statements {
        match statement {
            RoutineStmt::Block {
                declarations,
                body,
                handlers,
            } => {
                oracle::routine_arithmetic::prepare_local_declarations(declarations)?;
                prepare_declared_types(body, handlers)?;
            }
            RoutineStmt::If {
                then_body,
                else_body,
                ..
            } => {
                prepare_declared_types(then_body, &mut [])?;
                prepare_declared_types(else_body, &mut [])?;
            }
            RoutineStmt::Case {
                arms, else_body, ..
            } => {
                for arm in arms {
                    prepare_declared_types(arm, &mut [])?;
                }
                if let Some(body) = else_body {
                    prepare_declared_types(body, &mut [])?;
                }
            }
            RoutineStmt::ForLoop { body, .. }
            | RoutineStmt::ForeachLoop { body, .. }
            | RoutineStmt::QueryForLoop { body, .. }
            | RoutineStmt::CursorForLoop { body, .. }
            | RoutineStmt::Loop { body }
            | RoutineStmt::WhileLoop { body, .. } => prepare_declared_types(body, &mut [])?,
            _ => {}
        }
    }
    for handler in handlers {
        prepare_declared_types(&mut handler.statements, &mut [])?;
    }
    Ok(())
}

impl SqlSession<'_> {
    pub(crate) fn execute_nested_routine_block(
        &mut self,
        frame: &mut RoutineFrame,
        declarations: &[RoutineDecl],
        body: &[RoutineStmt],
        handlers: &[RoutineExceptionHandler],
    ) -> Result<RoutineControl> {
        if declarations.is_empty() {
            return self.execute_routine_block(frame, body, handlers);
        }
        let types = Arc::clone(&frame.assignment_types);
        let registered_types = frame.registered_assignment_types.clone();
        let dates = frame.oracle_dates.clone();
        let mut saved = Vec::new();
        let local =
            RoutineFrameTemplate::build_with_declarations(&[], &frame.slot_names, declarations);
        for declaration in declarations {
            let name = match declaration {
                RoutineDecl::Variable { name, .. }
                | RoutineDecl::Alias { name, .. }
                | RoutineDecl::Cursor { name, .. } => name,
            };
            saved.push((name.clone(), frame.get(name), frame.cursors.remove(name)));
            frame.set(name, SqlValue::Null);
            Arc::make_mut(&mut frame.assignment_types).remove(name);
            if let Some(dates) = &mut frame.oracle_dates {
                Arc::make_mut(dates).remove(name);
            }
        }
        Arc::make_mut(&mut frame.assignment_types).extend(
            local
                .assignment_types
                .iter()
                .map(|(name, kind)| (name.clone(), kind.clone())),
        );
        for declaration in declarations {
            if let RoutineDecl::Alias { name, position } = declaration {
                if let Some(kind) = types.get(&format!("${position}")) {
                    Arc::make_mut(&mut frame.assignment_types).insert(name.clone(), kind.clone());
                }
                if dates
                    .as_ref()
                    .is_some_and(|dates| dates.contains(&format!("${position}")))
                {
                    let dates = frame
                        .oracle_dates
                        .get_or_insert_with(|| Arc::new(FxHashSet::default()));
                    Arc::make_mut(dates).insert(name.clone());
                }
            }
        }
        if let Some(local_dates) = local.oracle_dates {
            let dates = frame
                .oracle_dates
                .get_or_insert_with(|| Arc::new(FxHashSet::default()));
            Arc::make_mut(dates).extend(local_dates.iter().cloned());
        }
        // Initialization errors belong to the enclosing block, not this block's
        // handlers. Restore shadowed bindings for every error/control exit.
        let result = self
            .initialize_routine_declarations(frame, declarations)
            .and_then(|()| self.execute_routine_block(frame, body, handlers));
        frame.assignment_types = types;
        frame.registered_assignment_types = registered_types;
        frame.oracle_dates = dates;
        for (name, value, cursor) in saved {
            frame.set(&name, value);
            frame.cursors.remove(&name);
            if let Some(cursor) = cursor {
                frame.cursors.insert(name, cursor);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_declared_blocks_bind_constants_in_aggregate_queries() {
        for oracle in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            if oracle {
                oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            }
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE TABLE aggregate_input(k INT); INSERT INTO aggregate_input VALUES(1),(2); CREATE TABLE aggregate_result(n INT,c INT)").unwrap();
            let body="DECLARE local_n INTEGER:=TRUNC(7.9); BEGIN INSERT INTO \"aggregate_result\" SELECT local_n,COUNT(*) FROM \"aggregate_input\"; END;";
            let sql = if oracle {
                oracle::lower_session_sql(
                    session.db_ref(),
                    body,
                    oracle::ORACLE_PREVIEW_PROFILE,
                    None,
                    "bicdb",
                )
                .unwrap()
            } else {
                format!("DO $$ {body} $$")
            };
            session.execute(&sql).unwrap();
            assert_eq!(
                session
                    .execute("SELECT n,c FROM aggregate_result")
                    .unwrap()
                    .rows[0],
                vec![SqlValue::Int(7), SqlValue::Int(2)]
            );
            session
                .execute("DELETE FROM aggregate_input; DELETE FROM aggregate_result")
                .unwrap();
            session.execute(&sql).unwrap();
            assert_eq!(
                session
                    .execute("SELECT n,c FROM aggregate_result")
                    .unwrap()
                    .rows[0],
                vec![SqlValue::Int(7), SqlValue::Int(0)]
            );
            // A real column must not become an ungrouped constant simply
            // because a routine has a same-named variable.
            session.execute("DO $$DECLARE k INT:=7; BEGIN INSERT INTO aggregate_result SELECT k,COUNT(*) FROM aggregate_input; END$$").unwrap_err();
            assert_eq!(session.execute("DO $$DECLARE k INT:=7; n INT; BEGIN SELECT k+COUNT(*) INTO n FROM aggregate_input a CROSS JOIN aggregate_input b; END$$").unwrap_err().sqlstate(), "0A000");
        }
    }

    #[test]
    fn routine_cache_isolates_databases_with_equal_generations_and_negative_entries() {
        let roots = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let mut first = BicDb::open(roots[0].path()).unwrap();
        let mut second = BicDb::open(roots[1].path()).unwrap();
        for (db, expected, other) in [
            (&mut first, 11, "first_only"),
            (&mut second, 22, "second_only"),
        ] {
            let mut session = SqlSession::new(db);
            session.execute(&format!("CREATE FUNCTION cache_value() RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN {expected}; END$$")).unwrap();
            session.execute(&format!("CREATE FUNCTION {other}() RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN {expected}; END$$")).unwrap();
        }
        assert_eq!(
            first.collection_generation(ROUTINE_COLLECTION),
            second.collection_generation(ROUTINE_COLLECTION)
        );
        for _ in 0..2 {
            for (db, expected, present, missing) in [
                (&mut first, 11, "first_only", "second_only"),
                (&mut second, 22, "second_only", "first_only"),
            ] {
                assert!(resolve_routine_cached(db, RoutineKind::Function, missing)
                    .unwrap()
                    .is_none());
                assert!(resolve_routine_cached(db, RoutineKind::Function, present)
                    .unwrap()
                    .is_some());
                assert_eq!(
                    SqlSession::new(db)
                        .execute("SELECT cache_value()")
                        .unwrap()
                        .rows[0][0],
                    SqlValue::Int(expected)
                );
            }
        }
    }

    #[test]
    fn nested_blocks_preserve_independent_oracle_and_postgres_undo_contracts() {
        for oracle in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            if oracle {
                oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
            }
            let mut session = SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE block_data(id INT PRIMARY KEY,n INT)")
                .unwrap();
            let language = if oracle {
                oracle::PREVIEW_ROUTINE_LANGUAGE
            } else {
                "plpgsql"
            };
            for (name, body) in [
                ("assignment", "BEGIN UPDATE block_data SET n=20 WHERE id=1; local_n:=7; RAISE division_by_zero; EXCEPTION WHEN division_by_zero THEN SELECT n INTO result_n FROM block_data WHERE id=1; END;"),
                ("atomicity", "BEGIN UPDATE block_data SET n=20 WHERE id=1; local_n:=7; INSERT INTO block_data SELECT 2,30 UNION ALL SELECT 1,40; EXCEPTION WHEN unique_violation THEN SELECT n INTO result_n FROM block_data WHERE id=1; END;"),
                ("propagation", "BEGIN BEGIN UPDATE block_data SET n=20 WHERE id=1; RAISE division_by_zero; EXCEPTION WHEN division_by_zero THEN local_n:=7; RAISE no_data_found; WHEN no_data_found THEN local_n:=-999; END; EXCEPTION WHEN no_data_found THEN SELECT n INTO result_n FROM block_data WHERE id=1; END;"),
            ] {
                let ddl = format!("CREATE FUNCTION block_{name}() RETURNS INT LANGUAGE {language} AS $$DECLARE local_n INT:=0; result_n INT; BEGIN INSERT INTO block_data VALUES(1,10); {body} RETURN result_n*100+local_n; END$$");
                session.execute(&ddl).unwrap();
                session.execute("BEGIN").unwrap();
                let result = session.execute(&format!("SELECT block_{name}()"));
                assert_eq!(result.unwrap_or_else(|e| panic!("{language} {name}: {e}" )).rows[0][0], SqlValue::Int(if oracle {2007} else {1007}), "{language}: {name}");
                assert_eq!(session.execute("SELECT COUNT(*) FROM block_data").unwrap().rows[0][0], SqlValue::Int(1));
                session.execute("ROLLBACK").unwrap();
                assert_eq!(session.execute("SELECT COUNT(*) FROM block_data").unwrap().rows[0][0], SqlValue::Int(0));
            }
        }
    }

    #[test]
    fn nested_declarations_shadow_values_types_and_cursors_and_restore_on_control_exits() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION block_scopes() RETURNS INT LANGUAGE plpgsql AS $$DECLARE n INT:=8; result_n INT:=0; c CURSOR FOR SELECT 42 AS amount; BEGIN DECLARE n TEXT:='inside'; c CURSOR FOR SELECT 7 AS amount; BEGIN FOR r IN c LOOP result_n:=r.amount; END LOOP; IF n<>'inside' THEN RAISE EXCEPTION 'shadow failed'; END IF; END; FOR r IN c LOOP result_n:=result_n+r.amount; END LOOP; RETURN result_n+n; END$$",
            "CREATE FUNCTION block_controls() RETURNS INT LANGUAGE plpgsql AS $$DECLARE n INT:=8; result_n INT:=0; BEGIN FOR k IN 1..4 LOOP DECLARE n INT:=k; BEGIN IF n=2 THEN CONTINUE; END IF; IF n=4 THEN EXIT; END IF; result_n:=result_n+n; END; END LOOP; RETURN result_n+n; END$$",
            "CREATE FUNCTION block_init_error() RETURNS INT LANGUAGE plpgsql AS $$DECLARE n INT:=8; BEGIN BEGIN DECLARE n INT:=1/0; BEGIN NULL; EXCEPTION WHEN division_by_zero THEN RETURN -999; END; EXCEPTION WHEN division_by_zero THEN RETURN n; END; RETURN -998; END$$",
            "CREATE FUNCTION block_return() RETURNS TEXT LANGUAGE plpgsql AS $$DECLARE n TEXT:='outside'; BEGIN DECLARE n TEXT:='inside'; BEGIN RETURN n; END; END$$",
            "CREATE FUNCTION block_lexical() RETURNS INT LANGUAGE plpgsql AS $$DECLARE n INT:=0; BEGIN IF TRUE THEN BEGIN n:=CASE WHEN TRUE THEN 3 ELSE 4 END; RAISE division_by_zero; EXCEPTION WHEN division_by_zero THEN IF TRUE THEN n:=n+1; END IF; END; ELSE n:=-999; END IF; RETURN n; END$$",
        ] {session.execute(sql).unwrap();}
        for (name, expected) in [
            ("scopes", 57),
            ("controls", 12),
            ("init_error", 8),
            ("lexical", 4),
        ] {
            assert_eq!(
                session
                    .execute(&format!("SELECT block_{name}()"))
                    .unwrap_or_else(|e| panic!("{name}: {e}"))
                    .rows[0][0],
                SqlValue::Int(expected)
            );
        }
        assert_eq!(
            session.execute("SELECT block_return()").unwrap().rows[0][0],
            SqlValue::String("inside".into())
        );
    }

    #[test]
    fn native_blocks_in_preview_keep_native_policy_and_old_routines_keep_their_version() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE block_versions(n INT); INSERT INTO block_versions VALUES(10)")
            .unwrap();
        for (name, language, expected) in [
            ("native", "plpgsql", 10),
            ("previous", oracle::PREVIEW_ROUTINE_LANGUAGE_V5, 10),
            ("current", oracle::PREVIEW_ROUTINE_LANGUAGE, 20),
        ] {
            session.execute(&format!("CREATE FUNCTION block_{name}() RETURNS INT LANGUAGE {language} AS $$DECLARE result_n INT; BEGIN UPDATE block_versions SET n=20; RAISE division_by_zero; EXCEPTION WHEN division_by_zero THEN SELECT n INTO result_n FROM block_versions; RETURN result_n; END$$")).unwrap();
            session.execute("BEGIN").unwrap();
            assert_eq!(
                session
                    .execute(&format!("SELECT block_{name}()"))
                    .unwrap()
                    .rows[0][0],
                SqlValue::Int(expected)
            );
            session.execute("ROLLBACK").unwrap();
        }
        for (name, language, expected) in [
            ("native", "plpgsql", SqlValue::Null),
            (
                "current",
                oracle::PREVIEW_ROUTINE_LANGUAGE,
                SqlValue::Int(99),
            ),
        ] {
            session.execute(&format!("CREATE FUNCTION missing_{name}() RETURNS INT LANGUAGE {language} AS $$DECLARE result_n INT; BEGIN BEGIN SELECT n INTO result_n FROM block_versions WHERE FALSE; EXCEPTION WHEN no_data_found THEN RETURN 99; END; RETURN result_n; END$$")).unwrap();
            assert_eq!(
                session
                    .execute(&format!("SELECT missing_{name}()"))
                    .unwrap()
                    .rows[0][0],
                expected
            );
        }
    }
}
