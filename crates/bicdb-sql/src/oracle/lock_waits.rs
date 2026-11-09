//! Query-local lock-wait policy carried through the ordinary query AST.
//!
//! sqlparser has no WAIT variant on LockClause. The preview front end lowers
//! positive WAIT seconds to a reserved query setting; WAIT 0 becomes NOWAIT.
//! The setting survives SQL serialization and prepared metadata passes without
//! mutating a session GUC. Native source WAIT syntax remains unsupported.
use super::*;
use std::time::Duration;

const POLICY: &str = "bicdb_oracle_lock_wait_seconds";

pub(crate) fn lower(sql: &str) -> Result<Option<String>> {
    if !sql.to_ascii_lowercase().contains("wait") {
        return Ok(None);
    }
    let mut source = tokens(sql)?;
    let mut changes = Vec::new();
    for start in 0..source.len().saturating_sub(1) {
        if !word(&source[start], "FOR") || !word(&source[start + 1], "UPDATE") {
            continue;
        }
        let mut end = start + 2;
        if source.get(end).is_some_and(|token| word(token, "OF")) {
            end += 1;
            loop {
                if !matches!(source.get(end), Some(Token::Word(_))) {
                    return Err(SqlError::InvalidSql(
                        "locking target expected after OF or comma".into(),
                    ));
                }
                end += 1;
                while source.get(end) == Some(&Token::Period)
                    && matches!(source.get(end + 1), Some(Token::Word(_)))
                {
                    end += 2;
                }
                if source.get(end) != Some(&Token::Comma) {
                    break;
                }
                end += 1;
            }
        }
        if source.get(end).is_some_and(|token| word(token, "WAIT")) {
            let Some(Token::Number(number, false)) = source.get(end + 1) else {
                return Err(SqlError::InvalidSql("WAIT requires integer seconds".into()));
            };
            if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(SqlError::InvalidSql("WAIT requires integer seconds".into()));
            }
            let seconds: u64 = number.parse().map_err(|_| {
                SqlError::InvalidSql("WAIT seconds exceed the supported range".into())
            })?;
            changes.push((start, end, seconds));
        }
    }
    if changes.is_empty() {
        return Ok(None);
    }
    for (start, end, seconds) in changes.into_iter().rev() {
        if seconds == 0 {
            source.splice(end..end + 2, tokens("NOWAIT")?);
        } else {
            source.drain(end..end + 2);
            source.splice(
                start..start,
                tokens(&format!("SETTINGS {POLICY} = {seconds}"))?,
            );
        }
    }
    Ok(Some(join(&source)))
}

/// Parse only the compiler's reserved query-local policy. Execution and static
/// description validate it against the durable profile before using the AST.
pub(crate) fn parse_lowered(sql: &str) -> Result<Option<Vec<Statement>>> {
    if !sql.to_ascii_lowercase().contains(POLICY) {
        return Ok(None);
    }
    let source = tokens(sql)?;
    if !source
        .windows(2)
        .any(|pair| word(&pair[0], "SETTINGS") && word(&pair[1], POLICY))
    {
        return Ok(None);
    }
    collections::multisets::parse_query_settings(sql)
        .map(Some)
        .map_err(|error| SqlError::InvalidSql(error.to_string()))
}

pub(crate) fn duration(db: &BicDb, query: &Query) -> Result<Option<Duration>> {
    let Some(settings) = &query.settings else {
        return Ok(None);
    };
    if !preview_routine_language_enabled(db)? {
        return Err(SqlError::InvalidSql(
            "query lock-wait policy requires the Oracle preview profile".into(),
        ));
    }
    if settings.len() != 1
        || settings[0].key.value != POLICY
        || query.locks.is_empty()
        || query.locks.iter().any(|lock| lock.nonblock.is_some())
    {
        return Err(SqlError::InvalidSql(
            "invalid query lock-wait policy".into(),
        ));
    }
    let Expr::Value(value) = &settings[0].value else {
        return Err(SqlError::InvalidSql(
            "lock-wait seconds must be a positive integer".into(),
        ));
    };
    let Value::Number(number, false) = &value.value else {
        return Err(SqlError::InvalidSql(
            "lock-wait seconds must be a positive integer".into(),
        ));
    };
    let seconds = number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
        .filter(|_| number.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| {
            SqlError::InvalidSql("lock-wait seconds must be a positive integer".into())
        })?;
    let duration = Duration::from_secs(seconds);
    std::time::Instant::now()
        .checked_add(duration)
        .ok_or_else(|| {
            SqlError::InvalidSql("lock-wait deadline exceeds the supported range".into())
        })?;
    Ok(Some(duration))
}

pub(crate) fn validate<T: sqlparser::ast::Visit>(db: &BicDb, node: &T) -> Result<()> {
    struct Policies<'a>(&'a BicDb);
    impl sqlparser::ast::Visitor for Policies<'_> {
        // A recursive policy scan must not copy the diagnostic payload
        // into each generated AST visitor frame.
        type Break = Box<SqlError>;
        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Box<SqlError>> {
            match duration(self.0, query) {
                Ok(_) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(Box::new(error)),
            }
        }
    }
    match sqlparser::ast::Visit::visit(node, &mut Policies(db)) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn lowered(db: &BicDb, sql: &str) -> String {
        lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb").unwrap()
    }

    fn setup(db: &mut BicDb) {
        ensure_database_profile(db, ORACLE_PREVIEW_PROFILE).unwrap();
        for sql in [
            "CREATE TABLE deadline_rows(id NUMBER PRIMARY KEY, value NUMBER)",
            "INSERT INTO deadline_rows VALUES(1,10)",
            "INSERT INTO deadline_rows VALUES(2,20)",
        ] {
            let sql = lowered(db, sql);
            SqlSession::new(&mut *db).execute(&sql).unwrap();
        }
    }

    #[test]
    fn deadline_roundtrip_describe_does_not_lock_and_wait_zero_remains_nowait() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        setup(&mut db);
        let sql = lowered(
            &db,
            "SELECT id FROM deadline_rows WHERE id=1 FOR UPDATE WAIT 2",
        );
        let statements = parse_statements(&sql).unwrap();
        validate(&db, &statements).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!()
        };
        assert_eq!(duration(&db, query).unwrap(), Some(Duration::from_secs(2)));
        let reparsed = parse_statements(&render_sql(&statements[0])).unwrap();
        assert_eq!(statements, reparsed);
        let mut owner = SqlSession::new_shared(&db);
        owner.execute("BEGIN").unwrap();
        owner.execute(&sql).unwrap();
        let at = Instant::now();
        for columns in [
            infer_query_result_columns(&db, &sql),
            infer_query_result_columns_for_describe(&db, &sql),
        ] {
            let columns = columns.unwrap().unwrap();
            // Oracle lowering quotes its normalized uppercase identifier.
            assert_eq!(columns[0].0, "ID");
            assert!(columns[0]
                .1
                .as_deref()
                .unwrap()
                .to_ascii_lowercase()
                .starts_with("numeric"));
        }
        assert!(
            at.elapsed() < Duration::from_secs(1),
            "Describe must not wait for the owner"
        );
        let zero = lowered(
            &db,
            "SELECT id FROM deadline_rows WHERE id=1 FOR UPDATE WAIT 0",
        );
        assert!(zero.contains("NOWAIT"));
        let mut waiter = SqlSession::new_shared(&db);
        waiter.execute("BEGIN").unwrap();
        assert_eq!(waiter.execute(&zero).unwrap_err().sqlstate(), "55P03");
        waiter.execute("ROLLBACK").unwrap();
        owner.execute("ROLLBACK").unwrap();
        assert_eq!(waiter.execute(&zero).unwrap().rows[0][0].to_cell(), "1");
    }

    #[test]
    fn deadline_expires_without_leaking_to_next_statement_and_statement_timeout_wins() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        setup(&mut db);
        let sql = lowered(
            &db,
            "SELECT id FROM deadline_rows WHERE id=1 FOR UPDATE WAIT 1",
        );
        let mut owner = SqlSession::new_shared(&db);
        owner.execute("BEGIN").unwrap();
        owner.execute(&sql).unwrap();
        let mut waiter = SqlSession::new_shared(&db);
        waiter.execute("BEGIN").unwrap();
        let at = Instant::now();
        assert_eq!(waiter.execute(&sql).unwrap_err().sqlstate(), "55P03");
        assert!(at.elapsed() >= Duration::from_millis(950));
        waiter.execute("ROLLBACK").unwrap();
        waiter = waiter.with_cancellation(bicdb_core::CancellationToken::new(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some(Instant::now() + Duration::from_millis(50)),
        ));
        waiter.execute("BEGIN").unwrap();
        let at = Instant::now();
        assert_eq!(waiter.execute(&sql).unwrap_err().sqlstate(), "57014");
        assert!(at.elapsed() < Duration::from_secs(1));
        waiter = waiter.with_cancellation(bicdb_core::CancellationToken::uncancelable());
        waiter.execute("ROLLBACK").unwrap();
        owner.execute("ROLLBACK").unwrap();
        assert_eq!(waiter.execute(&sql).unwrap().rows[0][0].to_cell(), "1");
    }

    #[test]
    fn deadline_wait_returns_latest_committed_row_after_owner_release() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        setup(&mut db);
        let sql = lowered(
            &db,
            "SELECT value FROM deadline_rows WHERE id=1 FOR UPDATE WAIT 2",
        );
        let update = lowered(&db, "UPDATE deadline_rows SET value=99 WHERE id=1");
        let mut owner = SqlSession::new_shared(&db);
        owner.execute("BEGIN").unwrap();
        owner.execute(&update).unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut waiter = SqlSession::new_shared(&db);
                waiter.execute("BEGIN").unwrap();
                let result = waiter.execute(&sql);
                waiter.execute("ROLLBACK").unwrap();
                send.send(result).unwrap();
            });
            let early = receive.recv_timeout(Duration::from_millis(100));
            owner.execute("COMMIT").unwrap();
            assert!(early.is_err());
            let result = receive
                .recv_timeout(Duration::from_secs(3))
                .unwrap()
                .unwrap();
            assert_eq!(result.rows[0][0].to_cell(), "99");
        });
    }

    #[test]
    fn deadline_grammar_is_bounded_and_native_profile_rejects_internal_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        setup(&mut db);
        for suffix in [
            "WAIT",
            "WAIT -1",
            "WAIT 1.5",
            "WAIT :1",
            "WAIT 1 NOWAIT",
            "WAIT 18446744073709551615",
        ] {
            assert!(
                lower_session_sql(
                    &db,
                    &format!("SELECT id FROM deadline_rows FOR UPDATE {suffix}"),
                    ORACLE_PREVIEW_PROFILE,
                    None,
                    "bicdb"
                )
                .and_then(|sql| {
                    let statements = parse_statements(&sql)?;
                    validate(&db, &statements)
                })
                .is_err(),
                "{suffix}"
            );
        }
        assert!(lower("SELECT 'FOR UPDATE WAIT 1' FROM dual")
            .unwrap()
            .is_none());
        assert!(lower("SELECT wait FROM t FOR UPDATE OF wait")
            .unwrap()
            .is_none());
        assert!(lower("SELECT t.wait FROM t FOR UPDATE OF t.wait")
            .unwrap()
            .is_none());
        let column_list = lowered(
            &db,
            "SELECT id FROM deadline_rows FOR UPDATE OF id,value WAIT 1",
        );
        validate(&db, &parse_statements(&column_list).unwrap()).unwrap();
        assert!(parse_statements("SELECT 1 FOR UPDATE WAIT 1").is_err());
        let internal = format!("SELECT 1 SETTINGS {POLICY}=1 FOR UPDATE");
        let native_dir = tempfile::tempdir().unwrap();
        let native = BicDb::open(native_dir.path()).unwrap();
        assert_eq!(
            infer_query_result_columns_for_describe(&native, &internal)
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
        assert_eq!(
            SqlSession::new_shared(&native)
                .execute(&internal)
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
        let hidden = format!("SELECT CASE WHEN false THEN ({internal}) ELSE 1 END");
        assert_eq!(
            SqlSession::new_shared(&native)
                .execute(&hidden)
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
        assert_eq!(
            SqlSession::new_shared(&native)
                .execute("SELECT 1 FOR UPDATE WAIT 1")
                .unwrap_err()
                .sqlstate(),
            "42601"
        );
    }
}
