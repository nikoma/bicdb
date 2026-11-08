//! Fail-closed archiving: the commit log archive and the page-log archive.
//! See `docs/durability-failure-handling.md`, section 7.5.
//!
//! Every archive I/O step (write, fsync, rename, directory fsync) is made to
//! fail once with `EIO`. After the failure:
//! - nothing is marked archived: the commit log archive's confirmed commit
//!   and the transaction-log hold stay where they were, and the page-log
//!   archive's confirmed segment does not advance;
//! - nothing local is deleted: checkpoints and transaction-log cuts keep
//!   everything after the confirmed point, also across a reopen;
//! - the failed handle is never retried (the fault fires once, so a retry
//!   would "succeed"; the tests prove no retry happens);
//! - the database stays healthy and writable;
//! - a new handle rewrites the unconfirmed part, and a point-in-time restore
//!   from base backup plus archive holds every acknowledged commit.
//!
//! Faults are scoped to each test's directory, so these tests run in
//! parallel.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use bicdb_core::log_archive::{read_log_archive, LogArchive, ReplayTarget};
use bicdb_core::{
    restore_backup, BackupCreateOptions, BackupRestoreOptions, BicDb, BicDbError, DbConfig, Record,
    ReplicationConfig, ReplicationCursor, ReplicationMode, ReplicationTlsConfig, StorageMode,
    LOG_ARCHIVE_HOLDER,
};
use bicdb_page::test_faults::arm_io_failure;
use serde_json::json;

const KEY: &str = "archive-fail-closed-test-passphrase-0123456789";

fn primary_config(storage: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(storage)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            cluster_id: "archive-fail-closed".to_string(),
            node_id: "n1".to_string(),
            listen_addr: Some("127.0.0.1:0".to_string()),
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..ReplicationConfig::default()
        })
}

/// Commits one row; returns its id when the commit was acknowledged.
fn put(db: &BicDb, id: &str) -> String {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new(id).with_metadata(json!({ "id": id })))
        .unwrap();
    tx.commit().unwrap();
    id.to_string()
}

fn hold(db: &BicDb) -> Option<u64> {
    db.sync_quorum().holds().get(LOG_ARCHIVE_HOLDER).copied()
}

fn archive_all(db: &BicDb, archive: &mut LogArchive) {
    let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
    while db.archive_commits(archive, &mut cursor, 3).unwrap() > 0 {}
    assert_eq!(archive.last_commit_seq(), db.current_commit_seq());
    assert_eq!(hold(db), Some(db.current_commit_seq()));
}

fn ids(path: &Path, storage: StorageMode) -> BTreeSet<String> {
    let db = BicDb::open_with_config(path, DbConfig::default().with_storage_mode(storage)).unwrap();
    db.scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect()
}

fn archive_failure_site(error: &BicDbError) -> &str {
    match error {
        BicDbError::ArchiveFailed { site, .. } => site,
        other => panic!("expected an archive failure, got {other}"),
    }
}

/// Every commit 1..=last is in the archive exactly once, in order.
fn archived_sequence(dir: &Path, after: u64) -> Vec<u64> {
    let mut seen = Vec::new();
    read_log_archive(dir, after, ReplayTarget::default(), |frame| {
        seen.push(frame.commit_seq);
        Ok(true)
    })
    .unwrap();
    seen
}

/// One commit log archive fault. `fresh`: the fault hits the archive's
/// first append (metadata and first segment); otherwise an established
/// archive with a base backup.
fn commit_log_case(storage: StorageMode, site: &str, nth: u64, fresh: bool) {
    let label = format!("{storage:?} {site}:{nth} fresh={fresh}");
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("archive");
    let mut db =
        BicDb::open_with_config(root.path().join("db"), primary_config(storage.clone())).unwrap();
    db.create_collection("items").unwrap();
    let mut acked = BTreeSet::new();
    for i in 0..3 {
        acked.insert(put(&db, &format!("a{i}")));
    }
    if !fresh {
        let mut archive = db.open_log_archive(&dir, true).unwrap();
        archive_all(&db, &mut archive);
        db.archive_base_backup(&dir, KEY).unwrap();
        for i in 0..3 {
            acked.insert(put(&db, &format!("b{i}")));
        }
    }
    let hold_before = hold(&db);
    let mut archive = db.open_log_archive(&dir, true).unwrap();
    let confirmed_before = archive.last_commit_seq();
    let guard = arm_io_failure(site, &dir, nth, false);
    let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
    let error = loop {
        match db.archive_commits(&mut archive, &mut cursor, 2) {
            Ok(0) => panic!("{label}: the fault was never reached"),
            Ok(_) => continue,
            Err(error) => break error,
        }
    };
    assert_eq!(guard.fired(), 1, "{label}");
    assert_eq!(archive_failure_site(&error), site, "{label}: {error}");
    assert_eq!(archive.failure().unwrap().site, site, "{label}");
    // Nothing published past the last confirmed commit.
    let confirmed = archive.last_commit_seq();
    assert!(confirmed >= confirmed_before, "{label}");
    assert!(confirmed < db.current_commit_seq(), "{label}");
    assert!(
        hold(&db).unwrap_or(0) <= confirmed,
        "{label}: hold {:?} advanced past the confirmed commit {confirmed} (before: {hold_before:?})",
        hold(&db)
    );
    let hold_after = hold(&db);
    // Never retried: the fault fired once, so a retry would succeed.
    let seen = guard.seen();
    for _ in 0..3 {
        let again = db
            .archive_commits(&mut archive, &mut cursor, 2)
            .unwrap_err();
        assert_eq!(archive_failure_site(&again), site, "{label}");
    }
    assert_eq!(
        guard.seen(),
        seen,
        "{label}: the failed handle did more I/O"
    );
    assert!(db.prepare_log_archive(&archive).is_err(), "{label}");
    assert_eq!(hold(&db), hold_after, "{label}");
    // The database itself is healthy and writable.
    assert!(db.durability_state().is_healthy(), "{label}");
    acked.insert(put(&db, "during-failure"));
    // Nothing local is deleted: a checkpoint (log cut) keeps every commit
    // after the hold.
    db.compact().unwrap();
    let keep_from = hold(&db).unwrap_or(0);
    let frames = db
        .replication_frames_after(&mut ReplicationCursor::new(keep_from), 10_000)
        .unwrap_or_else(|error| panic!("{label}: log cut past the hold: {error}"));
    assert_eq!(
        frames.first().map(|frame| frame.commit_seq),
        Some(keep_from + 1),
        "{label}"
    );
    assert_eq!(
        frames.last().unwrap().commit_seq,
        db.current_commit_seq(),
        "{label}"
    );
    drop(archive);
    drop(guard);

    // A new handle discards the unconfirmed suffix and rewrites it.
    let mut archive = db.open_log_archive(&dir, true).unwrap();
    assert!(archive.failure().is_none(), "{label}");
    assert!(
        archive.last_commit_seq() <= confirmed.max(hold_after.unwrap_or(0)),
        "{label}: unconfirmed commits survived the reopen"
    );
    archive_all(&db, &mut archive);
    if fresh {
        db.archive_base_backup(&dir, KEY).unwrap();
    }
    acked.insert(put(&db, "after-recovery"));
    archive_all(&db, &mut archive);
    let first = archive.meta().unwrap().first_commit_seq;
    assert_eq!(
        archived_sequence(&dir, first - 1),
        (first..=db.current_commit_seq()).collect::<Vec<_>>(),
        "{label}"
    );
    // Point-in-time restore reconciles against every acknowledged commit.
    let restored = root.path().join("restored");
    let report =
        bicdb_core::restore_point_in_time(&dir, &restored, KEY, true, ReplayTarget::default())
            .unwrap();
    assert_eq!(
        report.restored_commit_seq,
        db.current_commit_seq(),
        "{label}"
    );
    assert_eq!(ids(&restored, storage), acked, "{label}");
    db.release_log_archive_hold().unwrap();
}

#[test]
fn commit_log_archive_fails_closed_at_every_step() {
    for (site, nth, fresh) in [
        ("log_archive.write", 1, false),
        ("log_archive.write", 2, false),
        ("log_archive.sync", 1, false),
        ("log_archive.sync", 2, false),
        // First append of a new archive: archive.json, then the segment.
        ("log_archive.meta_write", 1, true),
        ("log_archive.meta_sync", 1, true),
        ("log_archive.rename", 1, true),
        ("log_archive.dir_sync", 1, true),
        ("log_archive.create", 1, true),
        ("log_archive.dir_sync", 2, true),
        ("log_archive.write", 1, true),
        ("log_archive.sync", 1, true),
    ] {
        commit_log_case(StorageMode::EmbeddedMemory, site, nth, fresh);
    }
}

#[test]
fn commit_log_archive_fails_closed_in_paged_storage() {
    for (site, nth, fresh) in [
        ("log_archive.write", 1, false),
        ("log_archive.sync", 1, false),
        ("log_archive.dir_sync", 2, true),
    ] {
        commit_log_case(StorageMode::ServerPaged, site, nth, fresh);
    }
}

/// A crash after the archive synced records but before the hold advanced
/// (or any archiver that wrote and never confirmed): the reopened server
/// discards that suffix and rewrites it from the log, across a restart of
/// the database, with a torn record at the tail too.
#[test]
fn unconfirmed_archive_suffix_is_rewritten_after_restart() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("archive");
        let db_path = root.path().join("db");
        let mut acked = BTreeSet::new();
        let (held, unconfirmed) = {
            let mut db =
                BicDb::open_with_config(&db_path, primary_config(storage.clone())).unwrap();
            db.create_collection("items").unwrap();
            acked.insert(put(&db, "a"));
            let mut archive = db.open_log_archive(&dir, true).unwrap();
            archive_all(&db, &mut archive);
            db.archive_base_backup(&dir, KEY).unwrap();
            for i in 0..5 {
                acked.insert(put(&db, &format!("b{i}")));
            }
            let held = hold(&db).unwrap();
            // Appended and synced, but the hold never advanced.
            let frames = db
                .replication_frames_after(
                    &mut ReplicationCursor::new(archive.last_commit_seq()),
                    100,
                )
                .unwrap();
            archive.append("archive-fail-closed", &frames).unwrap();
            assert_eq!(hold(&db), Some(held));
            let unconfirmed = frames.len() as u64;
            drop(archive);
            db.close().unwrap();
            (held, unconfirmed)
        };
        // A crash mid-append left half a record behind as well.
        let segment = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
            .max()
            .unwrap();
        let mut bytes = std::fs::read(&segment).unwrap();
        bytes.extend_from_slice(&[0, 0, 0, 40, 1, 2, 3]);
        std::fs::write(&segment, &bytes).unwrap();

        let db = BicDb::open_with_config(&db_path, primary_config(storage.clone())).unwrap();
        assert_eq!(hold(&db), Some(held), "the hold is persisted");
        // A fault while discarding stops that handle and changes nothing.
        {
            let guard = arm_io_failure("log_archive.truncate", &dir, 1, false);
            let error = db.open_log_archive(&dir, true).unwrap_err();
            assert_eq!(archive_failure_site(&error), "log_archive.truncate");
            assert_eq!(guard.fired(), 1);
            assert_eq!(hold(&db), Some(held));
        }
        let mut archive = LogArchive::open(&dir, true).unwrap();
        assert_eq!(archive.last_commit_seq(), held + unconfirmed);
        assert_eq!(db.reconcile_log_archive(&mut archive).unwrap(), unconfirmed);
        assert_eq!(archive.last_commit_seq(), held);
        acked.insert(put(&db, "after-restart"));
        archive_all(&db, &mut archive);
        let restored = root.path().join("restored");
        bicdb_core::restore_point_in_time(&dir, &restored, KEY, true, ReplayTarget::default())
            .unwrap();
        assert_eq!(ids(&restored, storage.clone()), acked, "{storage:?}");
        db.release_log_archive_hold().unwrap();
    }
}

/// A restore "to a time" from an archive that ends before that time may be
/// restoring to a gap (a failed or lagging archiver): refused, not silently
/// shortened. A commit target past the end is refused too.
#[test]
fn restore_refuses_a_target_the_archive_does_not_reach() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("archive");
    let mut db = BicDb::open_with_config(
        root.path().join("db"),
        primary_config(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    db.create_collection("items").unwrap();
    put(&db, "a");
    let mut archive = db.open_log_archive(&dir, true).unwrap();
    archive_all(&db, &mut archive);
    db.archive_base_backup(&dir, KEY).unwrap();
    put(&db, "b");
    archive_all(&db, &mut archive);
    // Committed but never archived: the archive ends before it.
    put(&db, "c");
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3600;
    for (index, target) in [
        ReplayTarget {
            commit_seq: None,
            timestamp: Some(future),
        },
        ReplayTarget {
            commit_seq: Some(db.current_commit_seq()),
            timestamp: None,
        },
        ReplayTarget {
            commit_seq: Some(db.current_commit_seq()),
            timestamp: Some(future),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let error = bicdb_core::restore_point_in_time(
            &dir,
            &root.path().join(format!("refused-{index}")),
            KEY,
            true,
            target,
        )
        .unwrap_err();
        let text = error.to_string();
        assert!(
            text.contains("without reaching") || text.contains("before requested target"),
            "{target:?}: {text}"
        );
    }
    // Restoring to the end of the archive, or to a reached target, works.
    let restored = root.path().join("to-end");
    bicdb_core::restore_point_in_time(&dir, &restored, KEY, true, ReplayTarget::default()).unwrap();
    assert_eq!(
        ids(&restored, StorageMode::EmbeddedMemory),
        ["a", "b"].map(String::from).into_iter().collect()
    );
    let restored = root.path().join("to-reached");
    bicdb_core::restore_point_in_time(
        &dir,
        &restored,
        KEY,
        true,
        ReplayTarget {
            commit_seq: Some(archive.last_commit_seq()),
            timestamp: Some(future),
        },
    )
    .unwrap();
    db.release_log_archive_hold().unwrap();
}

// ---- page-log archive ---------------------------------------------------

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        .with_paged_extent_bytes(1024 * 1024)
}

fn put_rows(db: &BicDb, tag: &str, count: usize, acked: &mut BTreeSet<String>) {
    for i in 0..count {
        let id = format!("{tag}-{i:04}");
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "items",
            Record::new(&id).with_metadata(json!({ "body": "x".repeat(300) })),
        )
        .unwrap();
        tx.commit().unwrap();
        acked.insert(id);
    }
}

fn sealed_on_disk(db_path: &Path) -> BTreeSet<u64> {
    std::fs::read_dir(db_path.join("paged"))
        .unwrap()
        .filter_map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_str()?
                .strip_prefix("store.wal.")?
                .parse()
                .ok()
        })
        .collect()
}

fn archived_through(db: &BicDb) -> u64 {
    db.paged_records_handle()
        .unwrap()
        .store()
        .wal()
        .archived_through()
}

fn online_backup(db: &BicDb, dir: &Path) -> (PathBuf, PathBuf) {
    let base = dir.join("base.bicbackup");
    let tail = dir.join("tail.bicbackup");
    db.create_online_backup(
        &base,
        &tail,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    (base, tail)
}

/// Restore base + tail, roll forward from the page-log archive, open.
fn roll_forward(
    base: &Path,
    tail: &Path,
    archive: &Path,
    target: &Path,
) -> bicdb_core::Result<BicDb> {
    for (artifact, force) in [(base, true), (tail, false)] {
        restore_backup(
            artifact,
            target,
            BackupRestoreOptions {
                passphrase: KEY.to_string(),
                force,
            },
        )?;
    }
    assert!(
        !target.join("paged/store.wal-archive-retention").exists(),
        "a restored copy must not inherit archive retention"
    );
    bicdb_core::apply_archived_wal(target, archive)?;
    BicDb::open_with_config(target, paged_config())
}

fn page_log_case(site: &str) {
    let label = format!("page log {site}");
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("db");
    let archive = root.path().join("wal-archive");
    let mut acked = BTreeSet::new();
    let (base, tail) = {
        let mut db = BicDb::open_with_config(&db_path, paged_config()).unwrap();
        db.create_collection("items").unwrap();
        put_rows(&db, "seed", 50, &mut acked);
        let (base, tail) = online_backup(&db, root.path());
        // The retention marker is the archiver's first step; every other
        // site fails on an established archive.
        let scope = if site == "wal_archive.retention" {
            db_path.clone()
        } else {
            db.archive_wal_segments(&archive).unwrap();
            put_rows(&db, "a", 40, &mut acked);
            archive.clone()
        };
        let through_before = archived_through(&db);
        let guard = arm_io_failure(site, &scope, 1, false);
        let error = db.archive_wal_segments(&archive).unwrap_err();
        assert_eq!(guard.fired(), 1, "{label}");
        assert_eq!(archive_failure_site(&error), site, "{label}: {error}");
        assert_eq!(db.wal_archive_failure().unwrap().site, site, "{label}");
        assert_eq!(
            archived_through(&db),
            through_before,
            "{label}: false archived mark"
        );
        assert!(db.wal_archive_retention_enabled(), "{label}");
        let unconfirmed_at_failure: BTreeSet<u64> = sealed_on_disk(&db_path)
            .into_iter()
            .filter(|sequence| *sequence > through_before)
            .collect();
        // The retention step fails before anything is sealed.
        assert!(
            !unconfirmed_at_failure.is_empty() || site == "wal_archive.retention",
            "{label}"
        );
        // Never retried, and nothing unconfirmed is deleted locally.
        let seen = guard.seen();
        let again = db.archive_wal_segments(&archive).unwrap_err();
        assert_eq!(archive_failure_site(&again), site, "{label}");
        assert_eq!(
            guard.seen(),
            seen,
            "{label}: the failed handle did more I/O"
        );
        put_rows(&db, "b", 40, &mut acked);
        db.checkpoint_for_resume().unwrap();
        assert!(db.durability_state().is_healthy(), "{label}");
        let on_disk = sealed_on_disk(&db_path);
        assert!(
            unconfirmed_at_failure.is_subset(&on_disk),
            "{label}: checkpoint deleted unarchived segments {:?}",
            unconfirmed_at_failure
                .difference(&on_disk)
                .collect::<Vec<_>>()
        );
        // The checkpoint sealed the rest of the log; that is retained too.
        let unconfirmed: BTreeSet<u64> = on_disk
            .into_iter()
            .filter(|sequence| *sequence > through_before)
            .collect();
        assert!(!unconfirmed.is_empty(), "{label}");
        // The disk cost of retention is visible.
        let status = db.wal_archive_status().unwrap();
        assert_eq!(status.failure.unwrap().site, site, "{label}");
        assert_eq!(status.archived_through, through_before, "{label}");
        assert_eq!(
            status.unarchived_segments,
            unconfirmed.len() as u64,
            "{label}"
        );
        let expected_bytes: u64 = unconfirmed
            .iter()
            .map(|sequence| {
                std::fs::metadata(db_path.join(format!("paged/store.wal.{sequence}")))
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(status.unarchived_bytes, expected_bytes, "{label}");
        assert!(expected_bytes > 0, "{label}");
        drop(guard);
        db.close().unwrap();
        let db = BicDb::open_with_config(&db_path, paged_config()).unwrap();
        assert!(db.wal_archive_failure().is_none(), "{label}");
        let chain = if site == "wal_archive.retention" {
            // The retention setting itself never became durable, so it does
            // not survive the reopen; no segment was ever confirmed either.
            // A roll-forward needs a base taken after archiving resumed.
            assert!(!db.wal_archive_retention_enabled(), "{label}");
            db.archive_wal_segments(&archive).unwrap();
            let fresh = root.path().join("after-resume");
            std::fs::create_dir_all(&fresh).unwrap();
            online_backup(&db, &fresh)
        } else {
            // Reopen (recovery runs; so does a checkpoint): still retained.
            assert!(
                db.wal_archive_retention_enabled(),
                "{label}: retention lost on reopen"
            );
            db.checkpoint_for_resume().unwrap();
            let on_disk = sealed_on_disk(&db_path);
            assert!(
                unconfirmed.is_subset(&on_disk),
                "{label}: reopen deleted unarchived segments"
            );
            // A new handle copies everything unconfirmed again.
            let report = db.archive_wal_segments(&archive).unwrap();
            assert!(report.archived >= unconfirmed.len(), "{label}: {report:?}");
            (base, tail)
        };
        put_rows(&db, "c", 40, &mut acked);
        db.archive_wal_segments(&archive).unwrap();
        let status = db.wal_archive_status().unwrap();
        assert!(
            status.failure.is_none() && status.retention_enabled,
            "{label}"
        );
        assert_eq!(
            (status.unarchived_segments, status.unarchived_bytes),
            (0, 0),
            "{label}: everything sealed is archived"
        );
        db.checkpoint_for_resume().unwrap();
        db.close().unwrap();
        chain
    };
    // Recovery evidence: the roll-forward holds every acknowledged commit.
    let restored = roll_forward(&base, &tail, &archive, &root.path().join("restored"))
        .unwrap_or_else(|error| panic!("{label}: roll-forward failed: {error}"));
    let rows: BTreeSet<String> = restored
        .scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect();
    assert_eq!(rows, acked, "{label}");
}

#[test]
fn page_log_archive_fails_closed_at_every_step() {
    for site in [
        "wal_archive.copy",
        "wal_archive.sync",
        "wal_archive.rename",
        "wal_archive.dir_sync",
        "wal_archive.retention",
    ] {
        page_log_case(site);
    }
}

/// A segment already in the archive that this handle did not confirm is
/// compared, not trusted: different bytes are refused and left alone.
#[test]
fn page_log_archive_refuses_a_differing_unconfirmed_copy() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("db");
    let archive = root.path().join("wal-archive");
    let unconfirmed = {
        let mut db = BicDb::open_with_config(&db_path, paged_config()).unwrap();
        db.create_collection("items").unwrap();
        put_rows(&db, "a", 30, &mut BTreeSet::new());
        db.archive_wal_segments(&archive).unwrap();
        put_rows(&db, "b", 30, &mut BTreeSet::new());
        // Renamed into the archive, but the directory fsync failed: present
        // there, never confirmed, so retained locally.
        let through = archived_through(&db);
        let guard = arm_io_failure("wal_archive.dir_sync", &archive, 1, false);
        db.archive_wal_segments(&archive).unwrap_err();
        drop(guard);
        db.close().unwrap();
        archived_names(&archive)
            .into_iter()
            .filter(|sequence| *sequence > through)
            .collect::<Vec<_>>()
    };
    let sequence = *unconfirmed
        .first()
        .expect("an unconfirmed archived segment");
    let copy = archive.join(format!("store.wal.{sequence}"));
    let local = db_path.join(format!("paged/store.wal.{sequence}"));
    assert!(
        local.exists(),
        "an unconfirmed segment was released locally"
    );
    let mut bytes = std::fs::read(&copy).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&copy, &bytes).unwrap();
    let db = BicDb::open_with_config(&db_path, paged_config()).unwrap();
    let error = db.archive_wal_segments(&archive).unwrap_err();
    assert_eq!(archive_failure_site(&error), "wal_archive.mismatch");
    assert_eq!(
        std::fs::read(&copy).unwrap(),
        bytes,
        "evidence must be kept"
    );
    db.checkpoint_for_resume().unwrap();
    assert!(
        local.exists(),
        "the local copy must outlive a refused archive"
    );
}

/// A page-log archive with a hole (a segment that never became durable)
/// must not roll a restore forward past the hole silently: the open
/// refuses.
#[test]
fn page_log_roll_forward_refuses_a_gapped_archive() {
    let root = tempfile::tempdir().unwrap();
    let db_path = root.path().join("db");
    let archive = root.path().join("wal-archive");
    let mut acked = BTreeSet::new();
    let (base, tail, middle) = {
        let mut db = BicDb::open_with_config(&db_path, paged_config()).unwrap();
        db.create_collection("items").unwrap();
        put_rows(&db, "seed", 20, &mut acked);
        let (base, tail) = online_backup(&db, root.path());
        let mut rounds = Vec::new();
        for round in 0..3 {
            let before: BTreeSet<u64> = archived_names(&archive);
            put_rows(&db, &format!("r{round}"), 20, &mut acked);
            db.archive_wal_segments(&archive).unwrap();
            let after = archived_names(&archive);
            rounds.push(after.difference(&before).copied().collect::<Vec<_>>());
        }
        db.close().unwrap();
        (base, tail, rounds[1].clone())
    };
    assert!(!middle.is_empty());
    // Control: the complete archive rolls forward.
    let complete = roll_forward(&base, &tail, &archive, &root.path().join("complete")).unwrap();
    assert_eq!(
        complete.scan_collection("items").unwrap().len(),
        acked.len()
    );
    drop(complete);
    for sequence in &middle {
        std::fs::remove_file(archive.join(format!("store.wal.{sequence}"))).unwrap();
    }
    let gapped = roll_forward(&base, &tail, &archive, &root.path().join("gapped"));
    assert!(
        gapped.is_err(),
        "a roll-forward over a gapped archive opened instead of refusing"
    );
}

fn archived_names(dir: &Path) -> BTreeSet<u64> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return BTreeSet::new();
    };
    entries
        .filter_map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_str()?
                .strip_prefix("store.wal.")?
                .parse()
                .ok()
        })
        .collect()
}

/// Found by the archive crash matrix: a transaction-log cut that renamed a
/// new log over the old one between the commit stream's anchor check and
/// its read made the read start at an old offset in the new file. Every
/// commit it then did not see was shipped as an empty frame, so the
/// commit-log archive (and a standby) silently lost those writes. The read
/// now uses the descriptor the anchor was checked on.
#[test]
fn a_log_cut_between_anchor_check_and_read_never_ships_empty_frames() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("db");
        let mut db = BicDb::open_with_config(&db_path, primary_config(storage.clone())).unwrap();
        db.create_collection("items").unwrap();
        put(&db, "a");
        let mut cursor = ReplicationCursor::new(0);
        let first = db.replication_frames_after(&mut cursor, 1000).unwrap();
        assert!(!first.is_empty());
        let shipped = cursor.sent_seq();
        put(&db, "b");
        put(&db, "c");
        // The cut: a new (here: empty) log renamed over the path while the
        // reader sits between its anchor check and its read.
        let log = db_path.join(bicdb_core::DEFAULT_TRANSACTION_LOG);
        let hook_log = log.clone();
        let guard = bicdb_page::test_faults::arm_io_hook(
            "replication.frames_read",
            &db_path,
            1,
            move || {
                let staging = hook_log.with_extension("cut-test");
                std::fs::write(&staging, b"").unwrap();
                std::fs::rename(&staging, &hook_log).unwrap();
            },
        );
        let frames = db.replication_frames_after(&mut cursor, 1000).unwrap();
        assert_eq!(guard.seen(), 1, "{storage:?}: the hook did not run");
        assert!(!frames.is_empty(), "{storage:?}");
        for frame in &frames {
            assert!(frame.commit_seq > shipped, "{storage:?}");
            assert!(
                !frame.writes.is_empty(),
                "{storage:?}: commit {} was shipped without its writes",
                frame.commit_seq
            );
        }
        drop(guard);
    }
}
