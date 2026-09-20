//! Regression: an online backup taken while the active WAL is *rotating*
//! (segment seal/rename under append) must still succeed and restore
//! consistently.
//!
//! The prior online-backup test uses the default 64 MiB WAL segment and
//! writes only a few MB, so no rotation ever happens during the copy. In
//! production a multi-hour, hundreds-of-GB base stream naturally crosses
//! the 64 MiB boundary: the active `store.wal` sealed to `store.wal.<seq>`
//! and a fresh, smaller active file took its place. The base copied
//! `paged/store.wal` fuzzily by pinned-size-and-pathname, so the shorter
//! new active failed the "at least the pinned number of bytes" check and
//! aborted the whole backup (observed 48 MiB pinned -> 30 MiB found).
//!
//! Here we force that condition deterministically: a tiny WAL segment plus
//! a writer thread that commits continuously for the whole backup window,
//! producing many rotations while the base streams. The backup must
//! complete, and every row committed before the consistency cut must be
//! present after restoring base + WAL tail.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bicdb_core::{
    restore_backup, verify_backup_chain, BackupCreateOptions, BackupRestoreOptions, BicDb,
    DbConfig, Record, StorageMode,
};
use serde_json::json;

const PASSPHRASE: &str = "wal-rotation-online-backup-test";

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        // Small extents so the base spans several page-segment files, and a
        // tiny WAL segment so the active WAL rotates constantly under load.
        .with_paged_extent_bytes(1024 * 1024)
        .with_paged_wal_segment_bytes(64 * 1024)
}

fn record(index: usize) -> Record {
    Record::new(format!("seed-{index:06}"))
        .with_metadata(json!({ "body": "s".repeat(400), "n": index }))
}

#[test]
fn online_backup_survives_wal_rotation_during_the_base_stream() {
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let base_path = backup_dir.path().join("base.bicbackup");
    let tail_path = backup_dir.path().join("wal-tail.bicbackup");

    let mut db = BicDb::open_with_config(source_dir.path(), config()).unwrap();
    db.create_collection("rows").unwrap();
    // Enough base to make the stream span several rotations; throttled so
    // recovery replay stays quick.
    let seeded = 4_000usize;
    db.bulk_load_insert("rows", (0..seeded).map(record))
        .unwrap();

    // The writer hammers the paged store through the same engine paths real
    // commits use, in a keyspace no collection scans (mutates pages + WAL
    // without disturbing the resident catalogs). With a 64 KiB WAL segment
    // this rotates every few commits.
    let paged = db.paged_records_handle().expect("paged store");
    let stop = Arc::new(AtomicBool::new(false));
    let rotations_seen = Arc::new(AtomicU64::new(0));
    let writer = {
        let paged = Arc::clone(&paged);
        let stop = Arc::clone(&stop);
        let rotations_seen = Arc::clone(&rotations_seen);
        std::thread::spawn(move || {
            let store = Arc::clone(paged.store());
            let mut index = 0u64;
            let mut committed = 0u64;
            let mut last_segments = 0usize;
            while !stop.load(Ordering::Acquire) {
                let xid = store.begin();
                for _ in 0..16 {
                    let key = format!("churn/{index:012}");
                    let value = format!("v-{index}-{}", "y".repeat(300));
                    store.put(xid, key.as_bytes(), value.as_bytes()).unwrap();
                    index += 1;
                }
                store.commit(xid).unwrap();
                committed += 16;
                let segments = store.sealed_wal_segments().len();
                if segments != last_segments {
                    rotations_seen.fetch_add(1, Ordering::Relaxed);
                    last_segments = segments;
                }
                // Throttle: mutation during the copy is the point; an
                // unthrottled flood just makes the restore's replay dominate.
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            committed
        })
    };

    std::thread::sleep(std::time::Duration::from_millis(50));
    // The backup must NOT abort despite the WAL rotating under it.
    let report = db
        .create_online_backup(
            &base_path,
            &tail_path,
            BackupCreateOptions {
                passphrase: PASSPHRASE.to_string(),
                base_backup: None,
            },
        )
        .expect("online backup must survive WAL rotation");
    assert!(report.base.full);
    assert!(!report.wal_tail.full);

    stop.store(true, Ordering::Release);
    let committed_during = writer.join().unwrap();
    assert!(committed_during > 0, "writer never committed");
    assert!(
        rotations_seen.load(Ordering::Relaxed) >= 3,
        "the WAL must have rotated several times during the backup (saw {})",
        rotations_seen.load(Ordering::Relaxed)
    );

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

    // The deterministic regression guard: the online base must NOT carry a
    // copy of the active WAL at all — that copy is what rotation
    // invalidated. (Before the fix, `paged/store.wal` is always in the base;
    // this file exists right after restoring the base, before any open
    // recreates it.) The sealed chain in the WAL tail is authoritative.
    assert!(
        !restore_dir.path().join("paged/store.wal").exists(),
        "the online base must not include the rotation-fragile active WAL"
    );

    restore_backup(
        &tail_path,
        restore_dir.path(),
        BackupRestoreOptions {
            passphrase: PASSPHRASE.to_string(),
            force: false,
        },
    )
    .unwrap();

    // Ordinary open = ordinary recovery over the sealed WAL chain (the base
    // carries no active-WAL copy). Every pre-backup row must be present.
    let restored = BicDb::open_with_config(restore_dir.path(), config()).unwrap();
    for index in (0..seeded).step_by(97) {
        let id = format!("seed-{index:06}");
        assert!(
            restored.get("rows", &id).unwrap().is_some(),
            "row {id} lost across a rotation-during-backup restore"
        );
    }
    assert_eq!(restored.scan_collection("rows").unwrap().len(), seeded);
}
