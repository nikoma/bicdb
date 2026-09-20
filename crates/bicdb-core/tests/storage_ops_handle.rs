//! `PagedStorageOpsHandle`: the storage-only capability an embedding host
//! uses to run online backup and online extent migration WITHOUT holding its
//! own database-wide guard for the duration. The handle must produce the
//! same artifacts and reports as the `BicDb` entry points it mirrors.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bicdb_core::{
    restore_backup, verify_backup_chain, BackupCreateOptions, BackupRestoreOptions, BicDb,
    DbConfig, Record, StorageMode,
};
use serde_json::json;

const PASSPHRASE: &str = "storage-ops-handle-passphrase";

fn segmented_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_paged_extent_bytes(1024 * 1024)
}

fn monolithic_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_paged_extent_bytes(0)
}

fn record(index: usize) -> Record {
    Record::new(format!("row-{index:06}")).with_metadata(json!({ "body": "x".repeat(400) }))
}

#[test]
fn handle_online_backup_streams_under_load_and_restores() {
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let base_path = backup_dir.path().join("base.bicbackup");
    let tail_path = backup_dir.path().join("wal-tail.bicbackup");

    let mut db = BicDb::open_with_config(source_dir.path(), segmented_config()).unwrap();
    db.create_collection("rows").unwrap();
    let seeded = 2_000usize;
    db.bulk_load_insert("rows", (0..seeded).map(record))
        .unwrap();

    let handle = db.paged_storage_ops_handle().expect("paged store");

    // A writer commits through the engine for the whole backup window — the
    // handle must not need the writer paused, and the host is NOT holding
    // any database-wide guard while the handle streams.
    let paged = db.paged_records_handle().expect("paged store");
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let paged = Arc::clone(&paged);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let store = Arc::clone(paged.store());
            let mut index = 0u64;
            while !stop.load(Ordering::Acquire) {
                let xid = store.begin();
                let key = format!("zz-load/{index:012}");
                store.put(xid, key.as_bytes(), b"load").unwrap();
                store.commit(xid).unwrap();
                index += 1;
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            index
        })
    };

    std::thread::sleep(std::time::Duration::from_millis(50));
    let report = handle
        .create_online_backup(
            &base_path,
            &tail_path,
            BackupCreateOptions {
                passphrase: PASSPHRASE.to_string(),
                base_backup: None,
            },
        )
        .unwrap();
    assert!(report.base.full);
    assert!(!report.wal_tail.full);

    stop.store(true, Ordering::Release);
    assert!(writer.join().unwrap() > 0, "writer never committed");

    verify_backup_chain([&base_path, &tail_path], PASSPHRASE).unwrap();
    restore_backup(
        &base_path,
        restore_dir.path(),
        BackupRestoreOptions {
            passphrase: PASSPHRASE.to_string(),
            force: true,
        },
    )
    .unwrap();
    restore_backup(
        &tail_path,
        restore_dir.path(),
        BackupRestoreOptions {
            passphrase: PASSPHRASE.to_string(),
            force: false,
        },
    )
    .unwrap();

    let restored = BicDb::open_with_config(restore_dir.path(), segmented_config()).unwrap();
    for index in (0..seeded).step_by(97) {
        assert!(
            restored
                .get("rows", &format!("row-{index:06}"))
                .unwrap()
                .is_some(),
            "pre-backup row {index} missing after handle-driven restore"
        );
    }
}

#[test]
fn handle_online_backup_refuses_a_monolithic_store() {
    let dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), monolithic_config()).unwrap();
    db.create_collection("rows").unwrap();
    db.bulk_load_insert("rows", vec![record(0)]).unwrap();
    let handle = db.paged_storage_ops_handle().expect("paged store");
    let error = handle
        .create_online_backup(
            backup_dir.path().join("base.bicbackup"),
            backup_dir.path().join("tail.bicbackup"),
            BackupCreateOptions {
                passphrase: PASSPHRASE.to_string(),
                base_backup: None,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("extent-segmented"),
        "must name the prerequisite: {error}"
    );
}

#[test]
fn handle_drives_online_extent_migration_to_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), monolithic_config()).unwrap();
    db.create_collection("rows").unwrap();
    let seeded = 3_000usize;
    db.bulk_load_insert("rows", (0..seeded).map(record))
        .unwrap();

    let handle = db.paged_storage_ops_handle().expect("paged store");
    handle.begin_extent_migration(1024 * 1024).unwrap();
    let mut steps = 0usize;
    loop {
        let report = handle.advance_extent_migration(4_096).unwrap();
        steps += 1;
        assert!(steps < 10_000, "migration failed to converge");
        if report.complete {
            break;
        }
    }
    assert!(
        dir.path().join("paged/store.pages.1").is_file(),
        "migration must have produced segment files"
    );

    // Reads work on the migrated layout, live and after reopen.
    assert!(db.get("rows", "row-000000").unwrap().is_some());
    assert!(db.get("rows", "row-002999").unwrap().is_some());
    // The handle keeps the store (and its directory lock) alive: drop it
    // before close, or the reopen below sees the lock still held.
    drop(handle);
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), segmented_config()).unwrap();
    for index in (0..seeded).step_by(131) {
        assert!(db
            .get("rows", &format!("row-{index:06}"))
            .unwrap()
            .is_some());
    }
}
