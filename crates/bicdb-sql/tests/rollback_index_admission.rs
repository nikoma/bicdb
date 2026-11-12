//! A refused index undo must retain its retry after a savepoint rollback.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn index_restore_refusal_keeps_its_savepoint_undo_until_the_foreign_owner_finishes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(mode.clone());
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE source(item_key INT PRIMARY KEY,payload TEXT); INSERT INTO source VALUES(1,'acknowledged'); CREATE INDEX source_payload_idx ON source(payload)").unwrap();
        db.create_collection("native_unrelated").unwrap();
        bicdb_sql::initialize_rename_journal_identities(&mut db).unwrap();
        let mut foreign = db.begin_transaction().unwrap();
        foreign
            .insert("native_unrelated", Record::new("independent_ack"))
            .unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("BEGIN; ALTER TABLE source RENAME TO renamed; SAVEPOINT keep_rename; DROP INDEX source_payload_idx").unwrap();
            let error = sql
                .execute("ROLLBACK TO SAVEPOINT keep_rename")
                .unwrap_err();
            assert_eq!(error.sqlstate(), "XX000", "{mode}: {error}");
            assert!(
                error.to_string().contains("transactions are pending"),
                "{mode}: {error}"
            );
            // Do not weaken the Native full-index-build fence. Let its real
            // unrelated owner finish, then retry the exact savepoint command.
            foreign.commit().unwrap();
            sql.execute("ROLLBACK TO SAVEPOINT keep_rename; COMMIT")
                .unwrap();
        }
        for reopen in 0..3 {
            assert!(db.index_definitions().iter().any(|index| index.name=="source_payload_idx" && index.collection=="renamed"),"{mode} reopen={reopen}: Native executable index was lost despite restored SQL metadata");
            assert!(db
                .get("native_unrelated", "independent_ack")
                .unwrap()
                .is_some());
            assert_eq!(
                SqlSession::new(&mut db)
                    .execute("SELECT payload FROM renamed WHERE payload='acknowledged'")
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::String("acknowledged".into())]]
            );
            db.close().unwrap();
            if reopen < 2 {
                db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            }
        }
    }
}
