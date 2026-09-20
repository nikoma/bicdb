//! SQL over `storage_mode = server_paged`.
//!
//! The point of the paged engine is that nothing above it changes. These tests
//! run ordinary SQL against a database whose durable record storage is the page
//! engine, and assert the same results the in-memory engine gives — so the two
//! modes are one product with one SQL surface, which is the constraint
//! `docs/server-paged-storage-todo.md` opens with.
//!
//! Every test runs the same statements against both modes and compares, rather
//! than hardcoding expected output. A hardcoded expectation would let both modes
//! drift together; comparing them catches divergence, which is the failure that
//! actually matters here.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

fn open(dir: &TempDir, mode: StorageMode) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode.clone()),
    )
    .unwrap_or_else(|error| panic!("open in {mode} failed: {error}"))
}

/// Run `statements` in `mode`, returning the rows of the final one.
fn run(mode: StorageMode, statements: &[&str]) -> Vec<Vec<SqlValue>> {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir, mode.clone());
    let mut rows = Vec::new();
    let mut session = SqlSession::new(&mut db);
    for statement in statements {
        let result = session
            .execute(statement)
            .unwrap_or_else(|error| panic!("[{mode}] `{statement}` failed: {error}"));
        rows = result.rows;
    }
    rows
}

/// Assert both modes produce identical rows for the same statements.
fn assert_modes_agree(statements: &[&str]) -> Vec<Vec<SqlValue>> {
    let memory = run(StorageMode::EmbeddedMemory, statements);
    let paged = run(StorageMode::ServerPaged, statements);
    assert_eq!(
        format!("{memory:?}"),
        format!("{paged:?}"),
        "embedded_memory and server_paged disagreed on:\n  {}",
        statements.join("\n  ")
    );
    paged
}

#[test]
fn insert_and_select_agree_across_modes() {
    let rows = assert_modes_agree(&[
        "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, n INT)",
        "INSERT INTO notes VALUES ('a', 'alpha', 1)",
        "INSERT INTO notes VALUES ('b', 'beta', 2)",
        "INSERT INTO notes VALUES ('c', 'gamma', 3)",
        "SELECT id, body, n FROM notes ORDER BY id",
    ]);
    assert_eq!(rows.len(), 3);
}

#[test]
fn where_clauses_agree_across_modes() {
    assert_modes_agree(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, n INT)",
        "INSERT INTO t VALUES ('a', 1)",
        "INSERT INTO t VALUES ('b', 5)",
        "INSERT INTO t VALUES ('c', 9)",
        "SELECT id FROM t WHERE n > 3 ORDER BY id",
    ]);
}

#[test]
fn updates_agree_across_modes() {
    assert_modes_agree(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, body TEXT)",
        "INSERT INTO t VALUES ('a', 'before')",
        "INSERT INTO t VALUES ('b', 'keep')",
        "UPDATE t SET body = 'after' WHERE id = 'a'",
        "SELECT id, body FROM t ORDER BY id",
    ]);
}

#[test]
fn deletes_agree_across_modes() {
    assert_modes_agree(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, n INT)",
        "INSERT INTO t VALUES ('a', 1)",
        "INSERT INTO t VALUES ('b', 2)",
        "INSERT INTO t VALUES ('c', 3)",
        "DELETE FROM t WHERE n = 2",
        "SELECT id FROM t ORDER BY id",
    ]);
}

#[test]
fn aggregates_agree_across_modes() {
    assert_modes_agree(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, grp TEXT, n INT)",
        "INSERT INTO t VALUES ('a', 'x', 1)",
        "INSERT INTO t VALUES ('b', 'x', 2)",
        "INSERT INTO t VALUES ('c', 'y', 10)",
        "SELECT grp, COUNT(*), SUM(n) FROM t GROUP BY grp ORDER BY grp",
    ]);
}

#[test]
fn order_by_and_limit_agree_across_modes() {
    assert_modes_agree(&[
        "CREATE TABLE t (id TEXT PRIMARY KEY, n INT)",
        "INSERT INTO t VALUES ('a', 3)",
        "INSERT INTO t VALUES ('b', 1)",
        "INSERT INTO t VALUES ('c', 2)",
        "SELECT id, n FROM t ORDER BY n DESC LIMIT 2",
    ]);
}

#[test]
fn a_larger_table_agrees_across_modes() {
    // Enough rows to cross page boundaries in the paged engine, so this
    // exercises real page allocation rather than a single-page happy path.
    let mut statements = vec!["CREATE TABLE t (id TEXT PRIMARY KEY, n INT, pad TEXT)".to_string()];
    for index in 0..300 {
        statements.push(format!(
            "INSERT INTO t VALUES ('k{index:04}', {}, '{}')",
            index % 37,
            "p".repeat(60)
        ));
    }
    statements.push("SELECT COUNT(*) FROM t".to_string());
    let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
    let rows = assert_modes_agree(&borrowed);
    assert_eq!(rows.len(), 1);
}

#[test]
fn sql_data_survives_reopen_in_paged_mode() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = open(&dir, StorageMode::ServerPaged);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")
            .unwrap();
        for index in 0..120 {
            sql.execute(&format!(
                "INSERT INTO notes VALUES ('k{index:04}', 'durable-{index}')"
            ))
            .unwrap();
        }
        db.close().unwrap();
    }

    let mut db = open(&dir, StorageMode::ServerPaged);
    let result = SqlSession::new(&mut db)
        .execute("SELECT COUNT(*) FROM notes")
        .unwrap();
    assert_eq!(
        format!("{:?}", result.rows),
        format!("{:?}", vec![vec![SqlValue::Int(120)]]),
        "SQL rows did not survive reopen in server_paged mode"
    );

    let body = SqlSession::new(&mut db)
        .execute("SELECT body FROM notes WHERE id = 'k0042'")
        .unwrap();
    assert_eq!(body.rows.len(), 1, "point lookup lost after reopen");
}

#[test]
fn the_database_reports_its_paged_mode() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir, StorageMode::ServerPaged);
    db.close().unwrap();
    assert_eq!(
        bicdb_core::storage_mode(dir.path()).unwrap(),
        StorageMode::ServerPaged
    );
    // And the paged engine's files exist, so rows really did go through it.
    assert!(
        dir.path().join("paged").exists(),
        "no paged directory: records did not reach the page engine"
    );
}
