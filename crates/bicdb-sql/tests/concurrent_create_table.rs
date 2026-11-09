use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{oracle::ensure_database_profile, SqlSession, SqlValue};

#[test]
fn fresh_constraint_indexes_allow_unrelated_pending_writes() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        ensure_database_profile(&mut db, profile).unwrap();
        SqlSession::new(&mut db)
            .execute("CREATE TABLE existing_rows(k INT PRIMARY KEY, value TEXT)")
            .unwrap();
        let mut pending = {
            let mut writer = SqlSession::new_shared(&db);
            writer.execute("BEGIN").unwrap();
            writer
                .execute("INSERT INTO existing_rows VALUES (1,'pending')")
                .unwrap();
            writer.take_pending_transaction().unwrap()
        };
        {
            let mut ddl = SqlSession::new(&mut db);
            ddl.execute("CREATE TABLE fresh_rows(k INT PRIMARY KEY, value TEXT UNIQUE)")
                .unwrap_or_else(|error| panic!("{profile}: {error}"));
            ddl.execute("INSERT INTO fresh_rows VALUES (1,'one')")
                .unwrap();
            assert_eq!(
                ddl.execute("INSERT INTO fresh_rows VALUES (1,'two')")
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            assert_eq!(
                ddl.execute("INSERT INTO fresh_rows VALUES (2,'one')")
                    .unwrap_err()
                    .sqlstate(),
                "23505"
            );
            ddl.execute("CREATE TABLE disposable_rows(k INT PRIMARY KEY)")
                .unwrap();
            ddl.execute("DROP TABLE disposable_rows").unwrap();
        }
        let sequence = db.commit_buffered_transaction(&mut pending).unwrap();
        db.tx_log_handle().write_durable(sequence).unwrap();
        pending.finalize_commit_admission().unwrap();
        pending.finalize_committed_broker_outbox();
        drop(pending);
        db.checkpoint_for_resume().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            sql.execute("SELECT value FROM existing_rows WHERE k=1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("pending".into())]]
        );
        assert_eq!(
            sql.execute("SELECT count(*) FROM fresh_rows").unwrap().rows,
            vec![vec![SqlValue::Int(1)]]
        );
    }
}
