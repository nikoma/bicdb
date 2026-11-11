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
fn imported_native_fixed_snapshot_is_not_advanced_by_its_first_sql_statement() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = seeded(mode, root.path());
        let mut native = db.begin_transaction().unwrap();
        native.set_snapshot_isolation(true, 0).unwrap();
        let original_watermark = native.visibility_watermark();
        let mut peer = db.begin_transaction().unwrap();
        peer.insert("statement_rows", Record::new("other")
            .with_metadata(serde_json::json!({"rowkey":2,"payload":20}))).unwrap();
        peer.commit().unwrap();
        let mut sql = SqlSession::new_shared(&db).with_pending_transaction(native);
        assert_eq!(sql.execute("SELECT rowkey FROM statement_rows ORDER BY rowkey").unwrap().rows,
            vec![vec![SqlValue::Int(1)]]);
        let native = sql.take_pending_transaction().unwrap();
        assert_eq!(native.visibility_watermark(),original_watermark);
        native.rollback().unwrap();
    }
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
fn fixed_read_only_snapshot_starts_at_first_query_in_direct_sessions() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db = seeded(mode, root.path());
        let mut sql = SqlSession::new_shared(&db);
        sql.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .unwrap();
        sql.execute("SHOW transaction_isolation; SAVEPOINT before_query")
            .unwrap();
        for id in [2, 3] {
            let mut peer = db.begin_transaction().unwrap();
            peer.insert(
                "statement_rows",
                Record::new(id.to_string()).with_metadata(serde_json::json!({ "rowkey": id })),
            )
            .unwrap();
            peer.commit().unwrap();
            if id == 3 {
                sql.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .unwrap();
            }
            assert_eq!(
                sql.execute("SELECT rowkey FROM statement_rows ORDER BY rowkey")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
            );
        }
        let error = sql
            .execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .unwrap_err();
        assert_eq!(error.sqlstate(), "25001");
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

#[test]
fn chained_direct_session_inherits_access_mode_and_starts_a_new_snapshot() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for ending in ["COMMIT AND CHAIN", "ROLLBACK AND CHAIN"] {
            let root = tempfile::tempdir().unwrap();
            let db = seeded(mode.clone(), root.path());
            let mut sql = SqlSession::new_shared(&db);
            assert_eq!(sql.execute(ending).unwrap_err().sqlstate(), "25P01");
            sql.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .unwrap();
            let query = "SELECT rowkey FROM statement_rows ORDER BY rowkey";
            assert_eq!(
                sql.execute(query).unwrap().rows,
                vec![vec![SqlValue::Int(1)]]
            );
            let mut peer = db.begin_transaction().unwrap();
            peer.insert(
                "statement_rows",
                Record::new("peer").with_metadata(serde_json::json!({"rowkey": 2, "payload": 20})),
            )
            .unwrap();
            peer.commit().unwrap();
            assert_eq!(
                sql.execute(query).unwrap().rows,
                vec![vec![SqlValue::Int(1)]]
            );
            sql.execute(ending).unwrap();
            assert_eq!(
                sql.execute(query).unwrap().rows,
                vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
            );
            assert_eq!(
                sql.execute("INSERT INTO statement_rows VALUES (3,30)")
                    .unwrap_err()
                    .sqlstate(),
                "25006"
            );
            sql.execute("ROLLBACK").unwrap();
        }
    }
}
