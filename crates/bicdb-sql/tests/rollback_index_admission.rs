//! Refused index rebuilds retain the undo needed by a later full rollback.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn definitions(db: &BicDb) -> serde_json::Value {
    let mut definitions = db.index_definitions();
    definitions.sort_by(|left, right| left.name.cmp(&right.name));
    serde_json::to_value(definitions).unwrap()
}

#[test]
fn full_rollback_can_retry_a_refused_index_rebuild_after_the_foreign_writer_commits() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(item_key INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); CREATE INDEX source_payload_idx ON source(payload)").unwrap();
        db.create_collection("native_unrelated").unwrap();
        let expected = definitions(&db);
        let mut foreign = db.begin_transaction().unwrap();
        foreign
            .insert("native_unrelated", Record::new("independent_ack"))
            .unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("BEGIN; DROP INDEX source_payload_idx").unwrap();
            let rollback = sql.execute("ROLLBACK");
            if mode == StorageMode::ServerPaged {
                // The real full rebuild still refuses a pending owner. The
                // SQL transaction itself has ended before DDL undo runs.
                let error = rollback.unwrap_err();
                assert_eq!(error.sqlstate(), "XX000", "{mode}: {error}");
                assert!(error.to_string().contains("transactions are pending"));
            } else {
                rollback.unwrap();
            }
            foreign.commit().unwrap();
            // Retry the same full rollback after the actual blocker ends.
            sql.execute("ROLLBACK").unwrap();
        }
        for reopen in 0..3 {
            assert_eq!(definitions(&db), expected, "{mode} reopen={reopen}");
            assert!(db
                .get("native_unrelated", "independent_ack")
                .unwrap()
                .is_some());
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(sql.execute("SELECT count(*) FROM pg_indexes WHERE tablename='source' AND indexname='source_payload_idx'").unwrap().rows, vec![vec![SqlValue::Int(1)]]);
            assert_eq!(
                sql.execute("SELECT payload FROM source WHERE payload='acknowledged'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String("acknowledged".into())]]
            );
            drop(sql);
            db.close().unwrap();
            if reopen == 2 {
                break;
            }
            db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        }
    }
}
