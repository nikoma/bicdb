//! Compiler-signed autonomous execution with a distinct transaction owner.
use super::*;

pub(crate) fn prepare_body(body: &str, language: &str) -> Result<(String, bool)> {
    if !preview_routine_language_version(language).is_some_and(|v| v >= 15)
        || !body.to_ascii_uppercase().contains("PRAGMA")
    {
        return Ok((body.into(), false));
    }
    let source = tokens(body)?;
    if !source.iter().any(|t| word(t, "PRAGMA")) {
        return Ok((body.into(), false));
    }
    if !preview_routine_language_version(language).is_some_and(|v| v >= 15) {
        return Err(unsupported(
            "autonomous transaction declarations require compiler version 15",
        ));
    }
    let begin = source.iter().position(|t| word(t, "BEGIN")).unwrap_or(0);
    let mut result = Vec::new();
    let mut autonomous = false;
    let mut index = 0;
    while index < source.len() {
        if word(&source[index], "PRAGMA") {
            if index >= begin
                || autonomous
                || !source
                    .get(index + 1)
                    .is_some_and(|t| word(t, "AUTONOMOUS_TRANSACTION"))
                || source.get(index + 2) != Some(&Token::SemiColon)
            {
                return Err(unsupported(
                    "unsupported, duplicate or nested Oracle pragma",
                ));
            }
            autonomous = true;
            index += 3;
            continue;
        }
        result.push(source[index].clone());
        index += 1;
    }
    Ok((join(&result), autonomous))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(s: &mut SqlSession<'_>, sql: &str) -> Result<SqlResult> {
        let sql = lower_session_sql(s.db_ref(), sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
        s.execute(&sql)
    }
    fn setup() -> (tempfile::TempDir, BicDb) {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        (root, db)
    }
    fn ids(s: &mut SqlSession<'_>) -> Vec<String> {
        run(s, "SELECT id FROM auto_rows ORDER BY id")
            .unwrap()
            .rows
            .into_iter()
            .map(|r| r[0].to_cell())
            .collect()
    }
    #[test]
    fn autonomous_defaults_execute_in_caller_but_declarations_in_child() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)",
            "CREATE FUNCTION auto_default RETURN NUMBER IS BEGIN INSERT INTO auto_rows VALUES(3);RETURN 42;END;",
            "CREATE FUNCTION auto_argument(p NUMBER DEFAULT auto_default()) RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN RETURN p;END;",
            "CREATE PROCEDURE auto_default_caller IS v NUMBER;BEGIN v:=auto_argument();END;",
            "BEGIN", "CALL auto_default_caller()",
        ] { run(&mut s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
        assert!(!s.last_statement_had_independent_commits());
        assert_eq!(ids(&mut s), ["3"]);
        run(&mut s, "ROLLBACK").unwrap();
        assert!(ids(&mut s).is_empty());
        run(&mut s, "CREATE PROCEDURE auto_declared IS PRAGMA AUTONOMOUS_TRANSACTION;v NUMBER DEFAULT auto_default();BEGIN COMMIT;END;").unwrap();
        run(&mut s, "CALL auto_declared()").unwrap();
        assert_eq!(ids(&mut s), ["3"]);
    }

    #[test]
    fn autonomous_commit_preserves_serializable_parent_snapshot() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)", "INSERT INTO auto_rows VALUES(1)",
            "CREATE PROCEDURE auto_snapshot IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(2);COMMIT;END;",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "CALL auto_snapshot()",
        ] {run(&mut s, sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert_eq!(ids(&mut s), ["1"]);
        run(&mut s, "ROLLBACK").unwrap();
        assert_eq!(ids(&mut s), ["1", "2"]);
        for sql in ["BEGIN", "UPDATE auto_rows SET id=id WHERE id=1"] {
            run(&mut s, sql).unwrap();
        }
        let error = run(&mut s, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap_err();
        assert_eq!(
            error.to_string(),
            "ORA-01453: SET TRANSACTION must be first statement of transaction"
        );
        run(&mut s, "ROLLBACK").unwrap();
        assert!(parse_transaction_control("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").is_err());
    }

    #[test]
    fn autonomous_commit_and_error_do_not_commit_the_parent() {
        let (root, mut db) = setup();
        {
            let mut s = SqlSession::new(&mut db);
            for sql in [
                "CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)",
                "CREATE PROCEDURE auto_child IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(20);COMMIT;END;",
                "CREATE PROCEDURE auto_error IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(30);COMMIT;RAISE_APPLICATION_ERROR(-20100,'after commit');END;",
                "BEGIN",
                "INSERT INTO auto_rows VALUES(10)",
                "CALL auto_child()",
            ] { run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}")); }
            assert!(s.last_statement_had_independent_commits());
            assert_eq!(ids(&mut s), ["10", "20"]);
            let error = run(&mut s, "CALL auto_error()").unwrap_err();
            assert_eq!(error.to_string(), "ORA-20100: after commit");
            assert!(s.last_statement_had_independent_commits());
            run(&mut s, "ROLLBACK").unwrap();
            assert_eq!(ids(&mut s), ["20", "30"]);
        }
        drop(db);
        let mut db = BicDb::open(root.path()).unwrap();
        assert_eq!(ids(&mut SqlSession::new(&mut db)), ["20", "30"]);
    }

    #[test]
    fn autonomous_exit_activity_and_caught_failures_follow_oracle() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        run(&mut s, "CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)").unwrap();
        run(&mut s, "INSERT INTO auto_rows VALUES(1)").unwrap();
        for (name, body, expected) in [
            (
                "open",
                "INSERT INTO auto_rows VALUES(2);",
                Some("ORA-06519: active autonomous transaction detected and rolled back"),
            ),
            (
                "empty",
                "UPDATE auto_rows SET id=id WHERE id=999;",
                Some("ORA-06519: active autonomous transaction detected and rolled back"),
            ),
            (
                "point",
                "SAVEPOINT p;",
                Some("ORA-06519: active autonomous transaction detected and rolled back"),
            ),
            (
                "caught",
                "BEGIN INSERT INTO auto_rows VALUES(1);EXCEPTION WHEN OTHERS THEN NULL;END;",
                None,
            ),
            (
                "missing_lock",
                "DECLARE v NUMBER;BEGIN SELECT id INTO v FROM auto_rows WHERE id=999 FOR UPDATE;EXCEPTION WHEN NO_DATA_FOUND THEN NULL;END;",
                Some("ORA-06519: active autonomous transaction detected and rolled back"),
            ),
            (
                "read_only",
                "DECLARE v NUMBER;BEGIN SELECT COUNT(*) INTO v FROM auto_rows;END;",
                None,
            ),
        ] {
            run(&mut s,&format!("CREATE PROCEDURE auto_{name} IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN {body} END;")).unwrap();
            let result = run(&mut s, &format!("CALL auto_{name}()"));
            if let Some(message) = expected {
                assert_eq!(result.unwrap_err().to_string(), message, "{name}");
            } else {
                result.unwrap_or_else(|e| panic!("{name}: {e}"));
            }
            assert_eq!(ids(&mut s), ["1"]);
        }
    }

    #[test]
    fn autonomous_nested_calls_and_shared_error_context_are_distinct_from_data() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)",
            "CREATE PROCEDURE auto_leaf IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(20);COMMIT;END;",
            "CREATE PROCEDURE auto_parent IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(10);auto_leaf();ROLLBACK;END;",
            "CALL auto_parent()",
            "CREATE FUNCTION auto_status RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN RETURN SQLCODE;END;",
            "CREATE FUNCTION auto_inherit RETURN NUMBER IS BEGIN RAISE_APPLICATION_ERROR(-20100,'outer');EXCEPTION WHEN OTHERS THEN RETURN auto_status();END;",
        ] {run(&mut s,sql).unwrap_or_else(|e|panic!("{sql}: {e}"));}
        assert_eq!(ids(&mut s), ["20"]);
        assert_eq!(
            run(&mut s, "SELECT auto_inherit() FROM DUAL").unwrap().rows[0][0].to_cell(),
            "-20100"
        );
    }

    #[test]
    fn autonomous_sql_function_describe_has_no_independent_side_effects() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE auto_rows(id NUMBER PRIMARY KEY)",
            "CREATE FUNCTION auto_value RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_rows VALUES(7);COMMIT;RETURN 42;END;"] {run(&mut s,sql).unwrap();}
        {
            let _description = crate::SqlDescriptionScope::enter();
            run(&mut s, "SELECT auto_value() FROM DUAL").unwrap();
            assert!(!s.last_statement_had_independent_commits());
        }
        assert!(ids(&mut s).is_empty());
        assert_eq!(
            run(&mut s, "SELECT auto_value() FROM DUAL").unwrap().rows[0][0].to_cell(),
            "42"
        );
        assert!(s.last_statement_had_independent_commits());
        assert_eq!(ids(&mut s), ["7"]);
    }

    #[test]
    fn autonomous_parent_row_lock_is_a_deadlock_with_parent_state_preserved() {
        let (_root, mut db) = setup();
        let mut s = SqlSession::new(&mut db);
        for sql in ["CREATE TABLE auto_rows(id NUMBER PRIMARY KEY,v NUMBER)","INSERT INTO auto_rows VALUES(1,0)",
            "CREATE PROCEDURE auto_lock IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN UPDATE auto_rows SET v=2 WHERE id=1;COMMIT;END;",
            "BEGIN","UPDATE auto_rows SET v=1 WHERE id=1"] {run(&mut s,sql).unwrap();}
        let error = run(&mut s, "CALL auto_lock()").unwrap_err();
        assert_eq!(error.sqlstate(), "40P01");
        assert_eq!(
            error.to_string(),
            "ORA-00060: deadlock detected while waiting for resource"
        );
        assert_eq!(
            run(&mut s, "SELECT v FROM auto_rows").unwrap().rows[0][0].to_cell(),
            "1"
        );
        run(&mut s, "ROLLBACK").unwrap();
        assert_eq!(
            run(&mut s, "SELECT v FROM auto_rows").unwrap().rows[0][0].to_cell(),
            "0"
        );
    }
}

impl SqlSession<'_> {
    /// Formal defaults are evaluated before suspending the caller. Declaration
    /// initializers still run in the callee's independent transaction.
    pub(crate) fn autonomous_arguments(
        &mut self,
        ir: &RoutineIR,
        args: &[SqlValue],
    ) -> Result<Vec<SqlValue>> {
        let mut actuals = args.to_vec();
        for param in ir
            .params
            .iter()
            .filter(|p| p.mode != RoutineArgMode::Out)
            .skip(args.len())
        {
            let Some(expr) = &param.default_expr else {
                return Err(SqlError::data_exception(
                    "42883",
                    "stored routine argument is missing",
                    None,
                ));
            };
            actuals.push(self.eval_session_expr(expr)?);
        }
        Ok(actuals)
    }

    pub(crate) fn ensure_autonomous_transaction(&mut self) -> Result<()> {
        if self.oracle_autonomous && self.tx.is_none() {
            self.begin_guc_transaction();
            let tx = self.begin_session_transaction()?;
            if let Some(suspension) = &self.oracle_suspension {
                suspension.set_child(tx.id())?;
            }
            self.tx = Some(tx);
        }
        Ok(())
    }

    pub(crate) fn execute_autonomous<T>(
        &mut self,
        execute: impl FnOnce(&mut SqlSession<'_>) -> Result<T>,
    ) -> Result<T> {
        let effects = crate::statement_context::StatementScope::enter();
        // Once an independent routine starts, avoid speculative replay even
        // if a later commit reports a post-durability delivery error.
        crate::statement_context::note_independent_execution();
        let parent = self.read_tx().map(Transaction::id);
        // Only shared core APIs may run while an outer SQL evaluator borrows
        // the database. A child cannot obtain an exclusive alias to that DB.
        let mut child = SqlSession::from_parts(
            self.db,
            false,
            self.security_context.clone(),
            self.db_ref().collection_generation(SCHEMA_COLLECTION),
        );
        child.oracle_autonomous = true;
        child.settings = self.settings;
        child.session_gucs = self.session_gucs.clone();
        child.currvals = self.currvals.clone();
        child.oracle_output = self.oracle_output.clone();
        child.oracle_exception = self.oracle_exception.clone();
        child.oracle_error_tokens = self.oracle_error_tokens.clone();
        child.runtime = self.runtime.clone();
        child.cancellation = self.cancellation.clone();
        child.fts_limits = self.fts_limits;
        child.snapshot_floor = self
            .snapshot_floor
            .max(crate::statement_context::latest_independent_commit());
        child.ensure_autonomous_transaction()?;
        if let Some(parent) = parent {
            child.oracle_suspension = Some(Arc::new(
                self.db_ref()
                    .suspend_transaction_for(parent, child.tx.as_ref().unwrap().id())?,
            ));
        }
        let mut result = execute(&mut child);
        if result.is_ok()
            && (child.oracle_autonomous_active
                || child.tx.as_ref().is_some_and(|t| t.lock_len() > 0))
        {
            result = Err(errors::database_error(
                -6519,
                "2D000",
                "active autonomous transaction detected and rolled back",
            ));
        }
        // Errors roll back only the currently open child transaction. Earlier
        // child commits and the caller's pending transaction remain separate.
        let cleanup = child.rollback_current_transaction();
        self.currvals.extend(child.currvals);
        if let Some(last) = child.session_gucs.get(LASTVAL_SESSION_KEY) {
            Arc::make_mut(&mut self.session_gucs).insert(LASTVAL_SESSION_KEY.into(), last.clone());
        }
        let sequence = effects.latest_sequence();
        self.snapshot_floor = self.snapshot_floor.max(sequence);
        if let Some(tx) = self.tx.as_mut() {
            tx.refresh_read_committed_snapshot_after(sequence)?;
        }
        cleanup?;
        result
    }
}
