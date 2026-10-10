//! Catalog views for replication, synchronous commit and history storage.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

#[test]
fn replication_and_storage_views_describe_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    rows(
        &mut session,
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)",
    );
    rows(&mut session, "INSERT INTO accounts VALUES (1, 10), (2, 20)");
    rows(
        &mut session,
        "ALTER TABLE accounts FLASHBACK ARCHIVE RETENTION 7 DAYS",
    );
    rows(&mut session, "UPDATE accounts SET balance = balance + 1");
    rows(&mut session, "CREATE TABLE scratch (id INT PRIMARY KEY)");
    rows(&mut session, "INSERT INTO scratch VALUES (1), (2), (3)");
    rows(&mut session, "DROP TABLE scratch");

    let role = rows(
        &mut session,
        "SELECT replication, role, synchronous_standbys, write_lease FROM bicdb_replication_role",
    );
    assert_eq!(
        role,
        vec![vec![
            SqlValue::Bool(false),
            SqlValue::String("standalone".to_string()),
            SqlValue::Int(0),
            SqlValue::String("not configured".to_string()),
        ]]
    );
    assert!(rows(&mut session, "SELECT * FROM bicdb_replication_nodes").is_empty());
    let stats = rows(
        &mut session,
        "SELECT waits, failed_waits FROM bicdb_sync_commit_stats",
    );
    assert_eq!(stats, vec![vec![SqlValue::Int(0), SqlValue::Int(0)]]);

    let history = rows(
        &mut session,
        "SELECT table_name, retention_seconds, history_rows FROM bicdb_flashback_storage",
    );
    // Two baseline versions plus two updates.
    assert_eq!(
        history,
        vec![vec![
            SqlValue::String("accounts".to_string()),
            SqlValue::Int(7 * 86_400),
            SqlValue::Int(4),
        ]]
    );
    let bin = rows(
        &mut session,
        "SELECT original_name, row_count FROM bicdb_recyclebin_storage",
    );
    assert_eq!(
        bin,
        vec![vec![
            SqlValue::String("scratch".to_string()),
            SqlValue::Int(3)
        ]]
    );
}
