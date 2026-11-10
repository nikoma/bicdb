//! Direct SqlSession callers get statement snapshots without a pgwire wrapper.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn seeded(mode: StorageMode, path: &std::path::Path) -> BicDb {
    let mut db =
        BicDb::open_with_config(path, DbConfig::default().with_storage_mode(mode)).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE statement_rows(rowkey INT,payload INT); INSERT INTO statement_rows VALUES(1,10)")
        .unwrap();
    db
}

#[test]
fn read_committed_direct_session_observes_commits_between_client_statements() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = seeded(mode, root.path());
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("BEGIN").unwrap();
        assert_eq!(
            sql.execute("SELECT rowkey FROM statement_rows ORDER BY rowkey")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        let mut peer = db.begin_transaction().unwrap();
        peer.insert(
            "statement_rows",
            Record::new("other").with_metadata(serde_json::json!({"rowkey":2,"payload":20})),
        )
        .unwrap();
        peer.commit().unwrap();
        assert_eq!(
            sql.execute("SELECT rowkey FROM statement_rows ORDER BY rowkey")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
        );
        sql.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn fixed_read_only_snapshot_is_not_advanced_by_client_statement_refresh() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = seeded(mode, root.path());
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .unwrap();
        let expected = sql
            .execute("SELECT rowkey FROM statement_rows ORDER BY rowkey")
            .unwrap()
            .rows;
        let mut peer = db.begin_transaction().unwrap();
        peer.insert(
            "statement_rows",
            Record::new("other").with_metadata(serde_json::json!({"rowkey":2,"payload":20})),
        )
        .unwrap();
        peer.commit().unwrap();
        assert_eq!(
            sql.execute("SELECT rowkey FROM statement_rows ORDER BY rowkey")
                .unwrap()
                .rows,
            expected
        );
        sql.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn rename_copy_and_savepoint_restoration_are_visible_in_the_same_client_batch() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = seeded(mode, root.path());
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            sql.execute("BEGIN; SAVEPOINT before_rename; ALTER TABLE statement_rows RENAME TO copied_rows; SELECT rowkey,payload FROM copied_rows ORDER BY rowkey")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1),SqlValue::Int(10)]]
        );
        assert_eq!(
            sql.execute("ROLLBACK TO before_rename; SELECT rowkey,payload FROM statement_rows ORDER BY rowkey")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1),SqlValue::Int(10)]]
        );
        sql.execute("ROLLBACK").unwrap();
    }
}
