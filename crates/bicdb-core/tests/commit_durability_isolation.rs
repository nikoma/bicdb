//! The experimental transaction-log-only commit durability must stay off
//! unless explicitly requested, and must never engage outside `server_paged`.
//! See `docs/commit-durability-invariants.md`, section 8.
//!
//! The environment is process-global, so this file holds one test that walks
//! through every case in order.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn commit_rows(db: &BicDb, count: usize) {
    for index in 0..count {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "rows",
            Record::new(format!("r-{index}")).with_metadata(json!({"n": index})),
        )
        .unwrap();
        tx.commit().unwrap();
    }
}

fn open(config: DbConfig) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config.with_fsync(false)).unwrap();
    db.create_collection("rows").unwrap();
    (dir, db)
}

#[test]
fn transaction_log_only_durability_is_strictly_opt_in() {
    std::env::remove_var("BICDB_COMMIT_DURABILITY");

    // Default configuration: off, in both storage modes, and no materializer.
    assert!(!DbConfig::default().transaction_log_commit_durability);
    for mode in [StorageMode::ServerPaged, StorageMode::EmbeddedMemory] {
        let (_dir, db) = open(DbConfig::default().with_storage_mode(mode));
        commit_rows(&db, 20);
        assert!(
            db.deferred_materialize_stats().is_none(),
            "{mode:?}: the default commit path must not start the materializer"
        );
    }

    // Only the exact value enables it; near misses do not.
    for value in ["1", "on", "true", "TRANSACTION-LOG", "transaction_log", " transaction-log"] {
        std::env::set_var("BICDB_COMMIT_DURABILITY", value);
        assert!(
            !DbConfig::default().transaction_log_commit_durability,
            "BICDB_COMMIT_DURABILITY={value:?} must not enable the experimental mode"
        );
    }
    std::env::set_var("BICDB_COMMIT_DURABILITY", "transaction-log");
    assert!(DbConfig::default().transaction_log_commit_durability);
    std::env::remove_var("BICDB_COMMIT_DURABILITY");

    // Requested but not paged: no effect.
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_transaction_log_commit_durability(true),
    );
    commit_rows(&db, 5);
    assert!(db.deferred_materialize_stats().is_none());

    // Requested and paged: engaged, and every commit gets its marker.
    let (_dir, db) = open(
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_transaction_log_commit_durability(true),
    );
    commit_rows(&db, 5);
    db.checkpoint_for_resume().unwrap();
    let stats = db.deferred_materialize_stats().expect("materializer runs");
    assert_eq!(stats.queued, 0, "a checkpoint drains every queued marker");
    assert!(!stats.poisoned);
}
