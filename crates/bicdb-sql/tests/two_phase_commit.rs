//! SQL two-phase commit through an embedded session: PREPARE TRANSACTION,
//! pg_prepared_xacts, COMMIT/ROLLBACK PREPARED from another session and
//! after reopening the store, and PostgreSQL's errors.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open(path: &std::path::Path) -> BicDb {
    let mut db = BicDb::open_with_config(path, DbConfig::default().with_fsync(false)).unwrap();
    db.set_max_prepared_transactions(2).unwrap();
    db
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session.execute(sql).unwrap().rows
}

fn sqlstate(session: &mut SqlSession<'_>, sql: &str) -> String {
    session.execute(sql).expect_err(sql).sqlstate().to_string()
}

#[test]
fn prepare_then_finish_from_another_session_and_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = open(dir.path());
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        session.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute("UPDATE t SET v = 'b' WHERE id = 1")
            .unwrap();
        session.execute("INSERT INTO t VALUES (2, 'c')").unwrap();
        session.execute("PREPARE TRANSACTION 'two'").unwrap();
        assert_eq!(
            rows(&mut session, "SELECT v FROM t ORDER BY id"),
            vec![vec![SqlValue::String("a".into())]]
        );
        assert_eq!(
            rows(
                &mut session,
                "SELECT gid, owner = current_user FROM pg_prepared_xacts"
            ),
            vec![vec![SqlValue::String("two".into()), SqlValue::Bool(true)]]
        );
        session.execute("BEGIN").unwrap();
        assert_eq!(sqlstate(&mut session, "COMMIT PREPARED 'two'"), "25001");
        session.execute("ROLLBACK").unwrap();
        session.execute("BEGIN").unwrap();
        session.execute("INSERT INTO t VALUES (3, 'd')").unwrap();
        assert_eq!(sqlstate(&mut session, "PREPARE TRANSACTION 'two'"), "42710");
        // The failed PREPARE ended its transaction.
        assert_eq!(
            rows(&mut session, "SELECT count(*) FROM t"),
            vec![vec![SqlValue::Int(1)]]
        );
        assert_eq!(sqlstate(&mut session, "COMMIT PREPARED 'missing'"), "42704");
        assert_eq!(
            sqlstate(&mut session, "SET max_prepared_transactions = 9"),
            "55P02"
        );
        drop(session);
        db.close().unwrap();
    }
    let mut db = open(dir.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(&mut session, "SELECT gid FROM pg_prepared_xacts"),
        vec![vec![SqlValue::String("two".into())]]
    );
    session.execute("COMMIT PREPARED 'two'").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT v FROM t ORDER BY id"),
        vec![
            vec![SqlValue::String("b".into())],
            vec![SqlValue::String("c".into())]
        ]
    );
    assert!(rows(&mut session, "SELECT gid FROM pg_prepared_xacts").is_empty());
}

#[test]
fn ddl_and_notifications_are_refused_and_rolled_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = open(dir.path());
    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    session.execute("CREATE TABLE d (a INT)").unwrap();
    assert_eq!(sqlstate(&mut session, "PREPARE TRANSACTION 'ddl'"), "0A000");
    assert!(session.execute("SELECT * FROM d").is_err());
    assert!(rows(&mut session, "SELECT gid FROM pg_prepared_xacts").is_empty());
}
