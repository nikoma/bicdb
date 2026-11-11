//! Connection defaults are distinct from the modes of an active transaction.
use super::*;

pub(super) fn isolation_label(level: &TransactionIsolationLevel) -> &'static str {
    match level {
        TransactionIsolationLevel::ReadCommitted => "read committed",
        TransactionIsolationLevel::ReadUncommitted => "read uncommitted",
        TransactionIsolationLevel::RepeatableRead => "repeatable read",
        TransactionIsolationLevel::Serializable => "serializable",
        TransactionIsolationLevel::Snapshot => "snapshot",
    }
}

pub(super) fn validate_default_isolation(value: &str, oracle: bool) -> Result<&'static str> {
    let level = transaction_isolation_level(value).ok_or_else(|| {
        SqlError::invalid_parameter_value(format!(
            "invalid value for parameter default_transaction_isolation: {value}"
        ))
    })?;
    validate_profile_transaction_modes(&[TransactionMode::IsolationLevel(level.clone())], oracle)?;
    Ok(isolation_label(&level))
}

pub(super) fn transaction_isolation_level(value: &str) -> Option<TransactionIsolationLevel> {
    Some(match value.to_ascii_lowercase().as_str() {
        "read committed" => TransactionIsolationLevel::ReadCommitted,
        "read uncommitted" => TransactionIsolationLevel::ReadUncommitted,
        "repeatable read" => TransactionIsolationLevel::RepeatableRead,
        "serializable" => TransactionIsolationLevel::Serializable,
        _ => return None,
    })
}

impl SqlSessionGucState {
    pub fn transaction_is_serializable(&self) -> bool {
        self.session_gucs
            .get("transaction_isolation")
            .is_some_and(|level| level == "serializable")
    }

    pub fn transaction_deferrable(&self) -> bool {
        self.session_gucs
            .get("transaction_deferrable")
            .is_some_and(|value| value == "on")
    }

    pub fn set_transaction_deferrable(&mut self, enabled: bool) {
        let key = "transaction_deferrable";
        if !enabled && !self.session_gucs.contains_key(key) {
            return;
        }
        self.begin_transaction();
        let previous = self.session_gucs.get(key).cloned();
        self.local_restore.entry(key.into()).or_insert(previous);
        Arc::make_mut(&mut self.session_gucs)
            .insert(key.into(), if enabled { "on" } else { "off" }.into());
    }

    /// Validate and return the connection's default under its fixed SQL profile.
    pub fn default_transaction_isolation(&self, profile: &str) -> Result<&'static str> {
        validate_default_isolation(
            self.session_gucs
                .get("default_transaction_isolation")
                .map(String::as_str)
                .unwrap_or("read committed"),
            profile == oracle::ORACLE_PREVIEW_PROFILE,
        )
    }

    /// Record the mode of an already-started host transaction. The host must
    /// enforce the corresponding isolation before publishing this label.
    pub fn set_transaction_isolation_label(&mut self, value: &str) -> Result<()> {
        if !matches!(
            value,
            "read committed" | "read uncommitted" | "repeatable read" | "serializable"
        ) {
            return Err(SqlError::invalid_parameter_value(
                "invalid active transaction isolation label",
            ));
        }
        self.begin_transaction();
        let key = "transaction_isolation".to_string();
        let previous = self.session_gucs.get(&key).cloned();
        self.local_restore.entry(key.clone()).or_insert(previous);
        Arc::make_mut(&mut self.session_gucs).insert(key, value.to_string());
        Ok(())
    }
}

impl SqlSession<'_> {
    pub(super) fn validate_transaction_deferrability(&self) -> Result<()> {
        if self
            .tx
            .as_ref()
            .is_some_and(Transaction::statement_snapshot_acquired)
            || !self.savepoints.is_empty()
        {
            return Err(SqlError::data_exception(
                "25001",
                "SET TRANSACTION DEFERRABLE must precede the first query",
                None,
            ));
        }
        Ok(())
    }

    pub(super) fn apply_transaction_deferrability(&mut self, deferrable: bool) -> Result<()> {
        self.validate_transaction_deferrability()?;
        if self.tx.is_none() {
            return Ok(());
        }
        let mut state = self.take_session_guc_state();
        state.set_transaction_deferrable(deferrable);
        self.install_session_guc_state(state);
        Ok(())
    }

    pub(crate) fn default_transaction_isolation(&self) -> Result<&'static str> {
        let Some(value) = self.session_gucs.get("default_transaction_isolation") else {
            return Ok("read committed");
        };
        validate_default_isolation(
            value,
            oracle::preview_routine_language_enabled(self.db_ref())?,
        )
    }

    pub(crate) fn set_transaction_isolation_label(&mut self, value: &str) -> Result<()> {
        let mut state = self.take_session_guc_state();
        let result = state.set_transaction_isolation_label(value);
        self.install_session_guc_state(state);
        result
    }

    pub(super) fn set_session_transaction_modes(
        &mut self,
        modes: &[TransactionMode],
    ) -> Result<()> {
        let [TransactionMode::IsolationLevel(level)] = modes else {
            return Err(SqlError::Unsupported(
                "session transaction characteristics currently require one isolation level".into(),
            ));
        };
        let value = validate_default_isolation(
            isolation_label(level),
            oracle::preview_routine_language_enabled(self.db_ref())?,
        )?;
        self.apply_scoped_guc_change(
            false,
            GucAssignmentTargets::setting("default_transaction_isolation"),
            |session| {
                Arc::make_mut(&mut session.session_gucs)
                    .insert("default_transaction_isolation".into(), value.into());
                Ok(())
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(session: &mut SqlSession<'_>, sql: &str, oracle: bool) -> Result<SqlResult> {
        let sql = if oracle {
            crate::oracle::lower_session_sql(
                session.db_ref(),
                sql,
                crate::oracle::ORACLE_PREVIEW_PROFILE,
                None,
                "bicdb",
            )?
        } else {
            sql.into()
        };
        session.execute(&sql)
    }

    fn shown(session: &mut SqlSession<'_>, oracle: bool, name: &str) -> String {
        run(session, &format!("SHOW {name}"), oracle).unwrap().rows[0][0].to_cell()
    }

    fn open(path: &std::path::Path, oracle: bool) -> BicDb {
        let mut db =
            BicDb::open_with_config(path, bicdb_core::DbConfig::default().with_fsync(false))
                .unwrap();
        if oracle {
            crate::oracle::ensure_database_profile(&mut db, crate::oracle::ORACLE_PREVIEW_PROFILE)
                .unwrap();
        }
        db
    }

    #[test]
    fn postgres_transaction_gucs_enforce_snapshot_and_reset_boundaries() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), false);
        let mut session = SqlSession::new(&mut db);
        session
            .execute("SET transaction_isolation='serializable'")
            .unwrap();
        assert_eq!(
            shown(&mut session, false, "transaction_isolation"),
            "read committed"
        );
        session.execute("BEGIN READ ONLY").unwrap();
        session
            .execute("SET transaction_isolation='serializable'")
            .unwrap();
        session.execute("SET TRANSACTION DEFERRABLE").unwrap();
        session.execute("RESET ALL").unwrap();
        assert_eq!(
            shown(&mut session, false, "transaction_isolation"),
            "serializable"
        );
        assert_eq!(shown(&mut session, false, "transaction_deferrable"), "on");
        session.execute("SELECT 1").unwrap();
        assert!(session.tx.as_ref().unwrap().is_postgres_serializable());
        for sql in [
            "SET transaction_isolation='read committed'",
            "SET transaction_deferrable=off",
            "SELECT set_config('transaction_isolation','read committed',false)",
        ] {
            assert_eq!(
                session.execute(sql).unwrap_err().sqlstate(),
                "25001",
                "{sql}"
            );
        }
        for sql in [
            "RESET transaction_isolation",
            "RESET transaction_deferrable",
        ] {
            assert_eq!(
                session.execute(sql).unwrap_err().sqlstate(),
                "0A000",
                "{sql}"
            );
        }
        session.execute("ROLLBACK").unwrap();
        assert_eq!(
            shown(&mut session, false, "transaction_isolation"),
            "read committed"
        );
        assert_eq!(shown(&mut session, false, "transaction_deferrable"), "off");
    }

    #[test]
    fn session_isolation_defaults_apply_to_future_transactions() {
        for oracle in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), oracle);
            let mut session = SqlSession::new(&mut db);
            let mode = if oracle {
                "serializable"
            } else {
                "read committed"
            };
            run(
                &mut session,
                &format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {mode}"),
                oracle,
            )
            .unwrap();
            assert!(session.tx.is_none());
            assert_eq!(shown(&mut session, oracle, "transaction_isolation"), mode);
            run(&mut session, "BEGIN", oracle).unwrap();
            assert_eq!(
                session.tx.as_ref().unwrap().isolation(),
                if oracle {
                    bicdb_core::TransactionIsolation::RepeatableRead
                } else {
                    bicdb_core::TransactionIsolation::ReadCommitted
                }
            );
            assert_eq!(
                shown(&mut session, oracle, "TRANSACTION ISOLATION LEVEL"),
                mode
            );
            run(&mut session, "COMMIT", oracle).unwrap();
            run(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", oracle).unwrap();
            assert_eq!(
                shown(&mut session, oracle, "transaction_isolation"),
                "read committed"
            );
            assert_eq!(
                session.tx.as_ref().unwrap().isolation(),
                bicdb_core::TransactionIsolation::ReadCommitted
            );
            run(&mut session, "COMMIT", oracle).unwrap();
            assert_eq!(shown(&mut session, oracle, "transaction_isolation"), mode);
            run(&mut session, "RESET default_transaction_isolation", oracle).unwrap();
            assert_eq!(
                shown(&mut session, oracle, "transaction_isolation"),
                "read committed"
            );
        }
    }

    #[test]
    fn postgres_read_uncommitted_defaults_keep_read_committed_execution() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), false);
        let mut session = SqlSession::new(&mut db);
        for setting in [
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            "SET default_transaction_isolation='read uncommitted'",
        ] {
            run(&mut session, setting, false).unwrap();
            assert_eq!(
                shown(&mut session, false, "default_transaction_isolation"),
                "read uncommitted"
            );
            run(&mut session, "BEGIN", false).unwrap();
            assert_eq!(
                shown(&mut session, false, "transaction_isolation"),
                "read uncommitted"
            );
            assert_eq!(
                session.tx.as_ref().unwrap().isolation(),
                bicdb_core::TransactionIsolation::ReadCommitted
            );
            run(&mut session, "ROLLBACK", false).unwrap();
            run(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", false).unwrap();
            assert_eq!(
                shown(&mut session, false, "transaction_isolation"),
                "read committed"
            );
            run(&mut session, "COMMIT", false).unwrap();
            assert_eq!(
                shown(&mut session, false, "transaction_isolation"),
                "read uncommitted"
            );
            run(&mut session, "RESET default_transaction_isolation", false).unwrap();
        }
    }

    #[test]
    fn session_default_changes_do_not_change_the_active_transaction() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), true);
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "BEGIN",
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            run(&mut session, sql, true).unwrap();
        }
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "read committed"
        );
        assert_eq!(
            shown(&mut session, true, "default_transaction_isolation"),
            "serializable"
        );
        assert_eq!(
            session.tx.as_ref().unwrap().isolation(),
            bicdb_core::TransactionIsolation::ReadCommitted
        );
        run(&mut session, "COMMIT", true).unwrap();
        run(&mut session, "BEGIN", true).unwrap();
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "serializable"
        );
        run(
            &mut session,
            "SET default_transaction_isolation='read committed'",
            true,
        )
        .unwrap();
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "serializable"
        );
        run(&mut session, "ROLLBACK", true).unwrap();
        assert_eq!(
            shown(&mut session, true, "default_transaction_isolation"),
            "serializable"
        );
        let result = run(&mut session, "SELECT current_setting('transaction_isolation'),current_setting('default_transaction_isolation') FROM DUAL", true).unwrap();
        assert_eq!(
            result.rows[0],
            vec![
                SqlValue::String("serializable".into()),
                SqlValue::String("serializable".into())
            ]
        );
    }

    #[test]
    fn unsupported_session_modes_never_change_the_connection_default() {
        for oracle in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut db = open(root.path(), oracle);
            let mut session = SqlSession::new(&mut db);
            for sql in [
                "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
                "SET default_transaction_isolation='invalid'",
            ] {
                assert!(run(&mut session, sql, oracle).is_err(), "{sql}");
                assert!(session.tx.is_none(), "{sql}");
                assert_eq!(shown(&mut session, oracle, "default_transaction_isolation"), "read committed");
            }
            if oracle {
                assert!(run(
                    &mut session,
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                    true
                )
                .is_err());
            }
            if !oracle {
                assert!(run(
                    &mut session,
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SNAPSHOT",
                    false
                )
                .is_err());
                assert_eq!(
                    shown(&mut session, false, "transaction_isolation"),
                    "read committed"
                );
            }
        }
    }

    #[test]
    fn reset_all_keeps_active_mode_but_resets_future_defaults() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), true);
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "BEGIN",
            "SELECT 1 FROM DUAL",
            "RESET ALL",
        ] {
            run(&mut session, sql, true).unwrap();
        }
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "serializable"
        );
        assert_eq!(
            shown(&mut session, true, "default_transaction_isolation"),
            "read committed"
        );
        assert_eq!(
            session.tx.as_ref().unwrap().isolation(),
            bicdb_core::TransactionIsolation::RepeatableRead
        );
        run(&mut session, "COMMIT", true).unwrap();
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "read committed"
        );
        run(&mut session, "BEGIN", true).unwrap();
        assert_eq!(
            session.tx.as_ref().unwrap().isolation(),
            bicdb_core::TransactionIsolation::ReadCommitted
        );
        run(&mut session, "ROLLBACK", true).unwrap();
    }

    #[test]
    fn independent_transaction_labels_use_defaults_instead_of_parent_overrides() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), true);
        let mut session = SqlSession::new(&mut db);
        run(&mut session, "CREATE FUNCTION independent_mode RETURN VARCHAR2 IS PRAGMA AUTONOMOUS_TRANSACTION; v_mode VARCHAR2(40); BEGIN SELECT current_setting('transaction_isolation') INTO v_mode FROM DUAL; COMMIT; RETURN v_mode; END;", true).unwrap();
        run(
            &mut session,
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
        )
        .unwrap();
        assert_eq!(
            run(&mut session, "SELECT independent_mode() FROM DUAL", true)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "read committed"
        );
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "serializable"
        );
        run(&mut session, "ROLLBACK", true).unwrap();
        run(
            &mut session,
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
        )
        .unwrap();
        run(&mut session, "BEGIN ISOLATION LEVEL READ COMMITTED", true).unwrap();
        assert_eq!(
            run(&mut session, "SELECT independent_mode() FROM DUAL", true)
                .unwrap()
                .rows[0][0]
                .to_cell(),
            "serializable"
        );
        assert_eq!(
            shown(&mut session, true, "transaction_isolation"),
            "read committed"
        );
        run(&mut session, "ROLLBACK", true).unwrap();
    }
}
