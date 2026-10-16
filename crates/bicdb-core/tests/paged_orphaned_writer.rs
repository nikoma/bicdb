//! A row whose newest version was stamped by a transaction that died with its
//! process (written and flushed by a checkpoint, never committed or aborted)
//! must not read as an eternal concurrent writer after a restart — even when
//! the durable abort-exception region is full, which is what stopped the
//! open-time freeze from absorbing such ids on a long-lived production store
//! (walknorth-search, 2026-10-16: `write conflict on row \0\x04jobs…:
//! … last written by transaction 34879475974201 (in xmin), which is
//! concurrent with it`).

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_page::{PagedStore, PagedStoreOptions};
use serde_json::json;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn record_key(collection: &str, id: &str) -> Vec<u8> {
    let mut key = (collection.len() as u16).to_be_bytes().to_vec();
    key.extend_from_slice(collection.as_bytes());
    key.extend_from_slice(id.as_bytes());
    key
}

#[test]
fn a_crash_orphaned_row_version_heals_on_the_next_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("jobs").unwrap();
        db.insert(
            "jobs",
            Record::new("f72cdfac").with_metadata(json!({ "state": "running" })),
        )
        .unwrap();
        db.close().unwrap();
    }

    // Reproduce the production page-store state directly: exhaust the
    // exception region, then leave an orphaned head on the job row.
    let paged = dir.path().join("paged");
    {
        let (store, _) = PagedStore::open(&paged, PagedStoreOptions::default()).unwrap();
        let capacity = store.snapshot().unwrap().abort_exception_capacity;
        for _ in 0..capacity + 8 {
            let doomed = store.begin();
            store.abort(doomed).unwrap();
        }
        store.checkpoint().unwrap();
        assert_eq!(store.snapshot().unwrap().abort_exceptions, capacity);
        let orphan = store.begin();
        store
            .put(orphan, &record_key("jobs", "f72cdfac"), b"never committed")
            .unwrap();
        store.checkpoint().unwrap();
        // Process dies: no commit, no abort.
    }

    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let current = db.get("jobs", "f72cdfac").unwrap().unwrap();
    assert_eq!(current.metadata["state"], json!("running"));
    db.insert(
        "jobs",
        Record::new("f72cdfac").with_metadata(json!({ "state": "queued" })),
    )
    .expect("startup job recovery must be able to rewrite the orphaned row");
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let current = db.get("jobs", "f72cdfac").unwrap().unwrap();
    assert_eq!(current.metadata["state"], json!("queued"));
}
