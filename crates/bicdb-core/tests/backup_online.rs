//! Online checkpoint-consistent backup: base + WAL-tail chain streamed from
//! a LIVE paged store while another thread writes the whole time.
//!
//! Pins the online-backup snapshot contract:
//! - the backup does not fail under concurrent mutation (the old double-hash
//!   veto made any write during the copy abort the archive);
//! - every row committed before the backup began survives restore;
//! - the restored directory opens with ordinary recovery (the sealed WAL
//!   chain replays; the base's stale active-WAL copy truncates at its LSN
//!   discontinuity) — no repair step;
//! - the chain verifies with the standard chain verifier.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bicdb_core::{
    restore_backup, verify_backup_chain, BackupCreateOptions, BackupRestoreOptions, BicDb,
    DbConfig, Record, StorageMode,
};
use serde_json::json;

const PASSPHRASE: &str = "online-backup-test-passphrase";

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        // Small extents so the base spans several segment files.
        .with_paged_extent_bytes(1024 * 1024)
}

fn record(index: usize) -> Record {
    Record::new(format!("row-{index:06}"))
        .with_metadata(json!({ "body": "x".repeat(600), "n": index }))
}

#[test]
fn online_backup_chain_survives_concurrent_writes_and_restores_consistently() {
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let base_path = backup_dir.path().join("base.bicbackup");
    let tail_path = backup_dir.path().join("wal-tail.bicbackup");

    let mut db = BicDb::open_with_config(source_dir.path(), config()).unwrap();
    db.create_collection("rows").unwrap();
    let seeded = 4_000usize;
    db.bulk_load_insert("rows", (0..seeded).map(record))
        .unwrap();

    // A writer thread hammers the paged store for the WHOLE backup window,
    // through the same engine paths real commits use. It writes to a
    // keyspace no collection scans, so it mutates pages + WAL without
    // touching the resident catalogs.
    let paged = db.paged_records_handle().expect("paged store");
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let paged = Arc::clone(&paged);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let store = Arc::clone(paged.store());
            let mut index = 0u64;
            let mut committed = 0u64;
            while !stop.load(Ordering::Acquire) {
                let xid = store.begin();
                for _ in 0..8 {
                    let key = format!("zz-load/{index:012}");
                    let value = format!("load-{index}-{}", "y".repeat(256));
                    store.put(xid, key.as_bytes(), value.as_bytes()).unwrap();
                    index += 1;
                }
                store.commit(xid).unwrap();
                committed += 8;
                // Steady load, not a WAL flood: the point is mutation during
                // the copy, and an unthrottled loop just makes the restore's
                // recovery replay dominate the test's wall time.
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            committed
        })
    };

    // Let the writer get going, then stream the chain while it runs.
    std::thread::sleep(std::time::Duration::from_millis(100));
    let report = db
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

    std::thread::sleep(std::time::Duration::from_millis(50));
    stop.store(true, Ordering::Release);
    let committed_during = writer.join().unwrap();
    assert!(
        committed_during > 0,
        "the load thread never committed; the test proved nothing"
    );

    // The chain verifies like any full+incremental pair.
    verify_backup_chain([&base_path, &tail_path], PASSPHRASE).unwrap();

    // Restore base, then the WAL tail, into a fresh directory.
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

    // Ordinary open = ordinary recovery. Every pre-backup row must be there.
    let restored = BicDb::open_with_config(restore_dir.path(), config()).unwrap();
    for index in (0..seeded).step_by(211) {
        assert!(
            restored
                .get("rows", &format!("row-{index:06}"))
                .unwrap()
                .is_some(),
            "pre-backup row {index} missing after online chain restore"
        );
    }
    let stats = restored.stats().unwrap();
    assert!(stats.record_count as usize >= seeded);

    // The source keeps running: pin released, next checkpoint truncates.
    db.checkpoint_for_resume().unwrap();
    drop(paged); // release the raw engine handle before reopening
    drop(db);
    let reopened = BicDb::open_with_config(source_dir.path(), config()).unwrap();
    assert!(reopened.get("rows", "row-000000").unwrap().is_some());
}

#[test]
fn online_backup_refuses_monolithic_stores_with_guidance() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_sync_outbox(false)
            .with_paged_extent_bytes(0),
    )
    .unwrap();
    db.create_collection("rows").unwrap();
    let error = db
        .create_online_backup(
            dir.path().join("base.bicbackup"),
            dir.path().join("tail.bicbackup"),
            BackupCreateOptions {
                passphrase: PASSPHRASE.to_string(),
                base_backup: None,
            },
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("store segment"),
        "error should point at the migration tool: {error}"
    );
}

/// WAL archiving rolls a restore FORWARD past the backup: base + WAL-tail
/// chain, then more commits, then archive_wal_segments — restore the chain,
/// apply the archive, open: the post-backup commits are there. RPO = the
/// archive cadence, not the backup cadence.
#[test]
fn wal_archive_rolls_a_restore_forward_past_the_backup() {
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let side = tempfile::tempdir().unwrap();
    let base_path = side.path().join("base.bicbackup");
    let tail_path = side.path().join("wal-tail.bicbackup");
    let archive_dir = side.path().join("wal-archive");

    let mut db = BicDb::open_with_config(source_dir.path(), config()).unwrap();
    db.create_collection("rows").unwrap();
    db.bulk_load_insert("rows", (0..500).map(record)).unwrap();
    db.create_online_backup(
        &base_path,
        &tail_path,
        BackupCreateOptions {
            passphrase: PASSPHRASE.to_string(),
            base_backup: None,
        },
    )
    .unwrap();

    // Post-backup work: what only the archive can save. Interleave a
    // CHECKPOINT so local truncation runs mid-stream — retention must keep
    // unarchived sealed segments alive for the archiver.
    let paged = db.paged_records_handle().unwrap();
    let put_batch = |tag: &str, count: usize| {
        let store = paged.store();
        for chunk in 0..count / 10 {
            let xid = store.begin();
            for i in 0..10 {
                let key = format!("post/{tag}/{:06}", chunk * 10 + i);
                store
                    .put(
                        xid,
                        key.as_bytes(),
                        format!("v-{}", "z".repeat(200)).as_bytes(),
                    )
                    .unwrap();
            }
            store.commit(xid).unwrap();
        }
    };
    let first_archive = db.archive_wal_segments(&archive_dir).unwrap();
    put_batch("a", 200);
    db.archive_wal_segments(&archive_dir).unwrap();
    db.checkpoint_for_resume().unwrap(); // truncates archived segments locally
    put_batch("b", 200);
    let last_archive = db.archive_wal_segments(&archive_dir).unwrap();
    assert!(
        last_archive.archived > 0,
        "expected fresh segments archived"
    );
    assert!(
        last_archive.archived_through > first_archive.archived_through,
        "archive watermark must advance: {first_archive:?} → {last_archive:?}"
    );

    // Restore the chain, roll forward from the archive, open.
    for (artifact, force) in [(&base_path, true), (&tail_path, false)] {
        restore_backup(
            artifact,
            restore_dir.path(),
            BackupRestoreOptions {
                passphrase: PASSPHRASE.to_string(),
                force,
            },
        )
        .unwrap();
    }
    let applied = bicdb_core::apply_archived_wal(restore_dir.path(), &archive_dir).unwrap();
    assert!(applied > 0, "roll-forward applied no segments");
    let restored = BicDb::open_with_config(restore_dir.path(), config()).unwrap();
    assert!(restored.get("rows", "row-000499").unwrap().is_some());
    let restored_paged = restored.paged_records_handle().unwrap();
    assert!(
        restored_paged
            .store()
            .get(b"post/a/000000")
            .unwrap()
            .is_some(),
        "post-backup batch A lost — the archive did not roll forward"
    );
    assert!(
        restored_paged
            .store()
            .get(b"post/b/000199")
            .unwrap()
            .is_some(),
        "post-backup batch B (after local truncation!) lost"
    );
    drop(restored_paged);
    drop(restored);
    drop(paged);
    drop(db);
}
