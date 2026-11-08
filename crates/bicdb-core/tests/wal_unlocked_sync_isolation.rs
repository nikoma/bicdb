//! The unlocked page-log sync must stay off unless explicitly requested. See
//! `docs/wal-unlocked-sync.md`.
//!
//! The environment is process-global, so this file holds one test that walks
//! through every case in order.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn engaged(db: &BicDb) -> Option<bool> {
    db.paged_records_handle()
        .map(|paged| paged.store().wal().unlocked_sync())
}

fn open(config: DbConfig) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config.with_fsync(true)).unwrap();
    db.create_collection("rows").unwrap();
    for index in 0..10 {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "rows",
            Record::new(format!("r-{index}")).with_metadata(json!({"n": index})),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    (dir, db)
}

#[test]
fn unlocked_wal_sync_is_strictly_opt_in() {
    std::env::remove_var("BICDB_PAGED_WAL_SYNC_UNLOCKED");

    // Default configuration: off, and the page log runs the locked sync.
    assert!(!DbConfig::default().paged_wal_sync_unlocked);
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::ServerPaged));
    assert_eq!(engaged(&db), Some(false));
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory));
    assert_eq!(engaged(&db), None, "no page log outside server_paged");

    // Only the exact value enables it; near misses do not.
    for value in ["", "0", "true", "on", "yes", "TRUE", " 1", "1 ", "01", "2"] {
        std::env::set_var("BICDB_PAGED_WAL_SYNC_UNLOCKED", value);
        assert!(
            !DbConfig::default().paged_wal_sync_unlocked,
            "BICDB_PAGED_WAL_SYNC_UNLOCKED={value:?} must not enable the unlocked sync"
        );
    }
    std::env::set_var("BICDB_PAGED_WAL_SYNC_UNLOCKED", "1");
    assert!(DbConfig::default().paged_wal_sync_unlocked);
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::ServerPaged));
    assert_eq!(engaged(&db), Some(true));
    std::env::remove_var("BICDB_PAGED_WAL_SYNC_UNLOCKED");

    // The builder works without the environment, in both directions.
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_wal_sync_unlocked(true),
    );
    assert_eq!(engaged(&db), Some(true));
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_wal_sync_unlocked(false),
    );
    assert_eq!(engaged(&db), Some(false));
}
