//! The row-commit sync outside the page store's writer lock must stay off
//! unless explicitly requested. See `docs/commit-fsync-outside-locks.md`.
//!
//! The environment is process-global, so this file holds one test that walks
//! through every case in order.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

const ENV: &str = "BICDB_PAGED_COMMIT_SYNC_OUTSIDE_LOCK";

fn engaged(db: &BicDb) -> Option<(bool, u64)> {
    db.paged_records_handle().map(|paged| {
        let store = paged.store();
        (
            store.commit_sync_outside_lock(),
            store.committing_stats().enqueued,
        )
    })
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
fn commit_sync_outside_lock_is_strictly_opt_in() {
    std::env::remove_var(ENV);

    // Default: off, and no row commit ever enters the committing queue (the
    // original locked commit ran for all ten).
    assert!(!DbConfig::default().paged_commit_sync_outside_lock);
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::ServerPaged));
    assert_eq!(engaged(&db), Some((false, 0)));
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory));
    assert_eq!(engaged(&db), None, "no page store outside server_paged");

    // Only the exact value enables it; near misses do not.
    for value in ["", "0", "true", "on", "yes", "TRUE", " 1", "1 ", "01", "2", "1\n"] {
        std::env::set_var(ENV, value);
        assert!(
            !DbConfig::default().paged_commit_sync_outside_lock,
            "{ENV}={value:?} must not enable it"
        );
    }
    std::env::set_var(ENV, "1");
    assert!(DbConfig::default().paged_commit_sync_outside_lock);
    let (_dir, db) = open(DbConfig::default().with_storage_mode(StorageMode::ServerPaged));
    assert_eq!(engaged(&db), Some((true, 10)), "every row commit took the gated path");
    std::env::remove_var(ENV);

    // The builder works without the environment, in both directions.
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_commit_sync_outside_lock(true),
    );
    assert_eq!(engaged(&db), Some((true, 10)));
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_commit_sync_outside_lock(false),
    );
    assert_eq!(engaged(&db), Some((false, 0)));
}
