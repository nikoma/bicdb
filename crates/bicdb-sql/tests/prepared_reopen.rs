//! A store must reopen after any SQL followed by a kill. Regression for the
//! PostgreSQL regression run whose server, restarted after `prepared_xacts`
//! hung, could not open its store again: a two-level partition tree with a
//! primary key (left by `triggers`) failed the startup constraint-index
//! check, and the hang itself was FOR SHARE waiting forever for a prepared
//! transaction's row lock.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;
use std::process::Command;

fn config() -> DbConfig {
    DbConfig {
        max_prepared_transactions: 2,
        ..DbConfig::default()
    }
}

fn run(db: &mut BicDb, sql: &str) {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// Child half: the statements, then an abort without closing the store.
#[test]
fn reopen_child() {
    let Ok(dir) = std::env::var("BICDB_PREPARED_REOPEN_DIR") else {
        return;
    };
    let mut db = BicDb::open_with_config(&dir, config()).unwrap();
    bicdb_sql::ensure_primary_key_indexes(&mut db).unwrap();
    // From `triggers`: a partition of a partition carrying its root's key.
    run(
        &mut db,
        "create table grandparent (id int, primary key (id)) partition by range (id)",
    );
    run(
        &mut db,
        "create table middle partition of grandparent for values from (1) to (10) partition by range (id)",
    );
    run(
        &mut db,
        "create table chi partition of middle for values from (1) to (5)",
    );
    run(
        &mut db,
        "create table cho partition of middle for values from (6) to (10)",
    );
    // From `prepared_xacts`, up to the statement that used to hang.
    run(
        &mut db,
        "CREATE TABLE pxtest_rowlock (id int PRIMARY KEY, data text); \
         INSERT INTO pxtest_rowlock VALUES (1, 'test data')",
    );
    let mut session = SqlSession::new(&mut db);
    session.execute("BEGIN").unwrap();
    session
        .execute("SELECT * FROM pxtest_rowlock WHERE id = 1 FOR SHARE")
        .unwrap();
    session.execute("PREPARE TRANSACTION 'regress_p1'").unwrap();
    let nowait = session
        .execute("SELECT * FROM pxtest_rowlock WHERE id = 1 FOR UPDATE NOWAIT")
        .unwrap_err();
    assert_eq!(nowait.sqlstate(), "55P03");
    session.execute("BEGIN").unwrap();
    // This waited forever for the prepared transaction; it now fails fast.
    let share = session
        .execute("SELECT * FROM pxtest_rowlock WHERE id = 1 FOR SHARE")
        .unwrap_err();
    assert_eq!(share.sqlstate(), "0A000", "{share}");
    session.execute("ROLLBACK").unwrap();
    drop(session);
    std::process::abort();
}

#[test]
fn a_store_reopens_after_the_prepared_xacts_sequence_and_a_kill() {
    let dir = tempfile::tempdir().unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "reopen_child", "--nocapture", "--test-threads=1"])
        .env("BICDB_PREPARED_REOPEN_DIR", dir.path())
        .status()
        .unwrap();
    assert!(!status.success(), "the child aborts after the statements");
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    // The server runs this at startup; it refused the two-level tree.
    bicdb_sql::ensure_primary_key_indexes(&mut db).unwrap();
    let mut session = SqlSession::new(&mut db);
    let listed = session
        .execute("SELECT gid FROM pg_prepared_xacts")
        .unwrap()
        .rows;
    assert_eq!(listed.len(), 1);
    // The restored prepared transaction still holds its row lock.
    assert_eq!(
        session
            .execute("SELECT * FROM pxtest_rowlock WHERE id = 1 FOR UPDATE NOWAIT")
            .unwrap_err()
            .sqlstate(),
        "55P03"
    );
    session.execute("ROLLBACK PREPARED 'regress_p1'").unwrap();
    session
        .execute("SELECT * FROM pxtest_rowlock WHERE id = 1 FOR UPDATE NOWAIT")
        .unwrap();
    session
        .execute("INSERT INTO grandparent VALUES (2)")
        .unwrap();
    assert_eq!(
        session
            .execute("INSERT INTO grandparent VALUES (2)")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}
