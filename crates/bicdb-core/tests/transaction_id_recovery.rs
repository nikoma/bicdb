//! Transaction identities returned for acknowledged writes survive WAL cuts.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode};

#[test]
fn acknowledged_transaction_ids_do_not_restart_after_checkpoint_and_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_storage_mode(mode.clone())
            .with_fsync(true)
            .with_sync_outbox(false)
            .with_auto_checkpoint_wal_bytes(0);
        let mut maximum = 0;
        for reopen in 0..3 {
            let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
            db.create_collection("rows").unwrap();
            for index in 0..40 {
                let mut tx = db.begin_transaction().unwrap();
                let id = tx.id().0;
                assert!(
                    id > maximum,
                    "{mode:?}, reopen={reopen}: acknowledged id {id} reused below {maximum}"
                );
                tx.insert("rows", Record::new(format!("{reopen}-{index}")))
                    .unwrap();
                tx.commit().unwrap();
                maximum = id;
            }
            db.checkpoint_for_resume().unwrap();
        }
    }
}
