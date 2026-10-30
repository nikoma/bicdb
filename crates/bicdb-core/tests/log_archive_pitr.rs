//! Whole-database point-in-time recovery from the commit log archive.
use bicdb_core::log_archive::{read_log_archive, LogArchive, ReplayTarget};
use bicdb_core::{
    restore_from_log_archive, BicDb, DbConfig, Record, ReplicationConfig, ReplicationCursor,
    ReplicationMode, ReplicationTlsConfig, StorageMode,
};
use bicdb_core::{BackupCreateOptions, BackupRestoreOptions};
use serde_json::json;
use std::path::Path;
use std::time::Duration;

const KEY: &str = "log-archive-test-passphrase-0123456789";

fn primary_config(storage: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(storage)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            cluster_id: "archive-test".to_string(),
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

fn put(db: &BicDb, id: &str, value: i64) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "items",
        Record::new(id).with_metadata(json!({ "value": value })),
    )
    .unwrap();
    tx.commit().unwrap();
}

fn archive_all(db: &BicDb, dir: &Path) {
    let mut archive = LogArchive::open(dir, false).unwrap();
    let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
    while db.archive_commits(&mut archive, &mut cursor, 7).unwrap() > 0 {}
    assert_eq!(archive.last_commit_seq(), db.current_commit_seq());
}

fn ids(path: &Path, storage: StorageMode) -> Vec<String> {
    let db = BicDb::open_with_config(path, DbConfig::default().with_storage_mode(storage)).unwrap();
    let mut ids = db
        .scan_collection("items")
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[test]
fn restore_to_a_commit_or_a_time_from_base_backup_plus_archive() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let archive = root.path().join("archive");
        let backup = root.path().join("base.bak");
        let mut db = BicDb::open_with_config(&source, primary_config(storage.clone())).unwrap();
        db.create_collection("items").unwrap();
        put(&db, "a1", 1);
        archive_all(&db, &archive);
        let (_, base_seq) = db
            .create_consistent_backup(
                &backup,
                BackupCreateOptions {
                    passphrase: KEY.to_string(),
                    base_backup: None,
                },
            )
            .unwrap();

        put(&db, "b1", 1);
        put(&db, "b2", 2);
        let after_b2 = db.current_commit_seq();
        put(&db, "b3", 3);
        archive_all(&db, &archive);
        // The archive holds the log: a checkpoint cannot cut unarchived commits.
        put(&db, "c1", 1);
        db.compact().unwrap();
        std::thread::sleep(Duration::from_millis(1100));
        let midpoint = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        std::thread::sleep(Duration::from_millis(1100));
        put(&db, "d1", 1);
        db.delete("items", "a1").unwrap();
        archive_all(&db, &archive);
        let end_seq = db.current_commit_seq();
        drop(db);

        let restore = |name: &str, target: ReplayTarget| {
            let path = root.path().join(name);
            let report = restore_from_log_archive(
                &backup,
                &path,
                BackupRestoreOptions {
                    passphrase: KEY.to_string(),
                    force: true,
                },
                &archive,
                target,
            )
            .unwrap();
            (path, report)
        };

        let (to_commit, report) = restore(
            "to-commit",
            ReplayTarget {
                commit_seq: Some(after_b2),
                timestamp: None,
            },
        );
        assert_eq!(report.base_commit_seq, base_seq, "{storage:?}");
        assert_eq!(report.restored_commit_seq, after_b2, "{storage:?}");
        assert_eq!(
            ids(&to_commit, storage.clone()),
            ["a1", "b1", "b2"],
            "{storage:?}"
        );

        let (to_time, _) = restore(
            "to-time",
            ReplayTarget {
                commit_seq: None,
                timestamp: Some(midpoint),
            },
        );
        assert_eq!(
            ids(&to_time, storage.clone()),
            ["a1", "b1", "b2", "b3", "c1"],
            "{storage:?}"
        );

        let (to_end, report) = restore("to-end", ReplayTarget::default());
        assert_eq!(report.restored_commit_seq, end_seq, "{storage:?}");
        assert_eq!(
            ids(&to_end, storage.clone()),
            ["b1", "b2", "b3", "c1", "d1"],
            "{storage:?}"
        );
        // The restored database is an ordinary one that keeps going.
        let restored = BicDb::open_with_config(
            &to_end,
            DbConfig::default().with_storage_mode(storage.clone()),
        )
        .unwrap();
        put(&restored, "after-restore", 1);
        assert!(restored.current_commit_seq() > end_seq, "{storage:?}");
    }
}

#[test]
fn a_torn_archive_tail_is_repaired_and_gaps_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let archive = root.path().join("archive");
    let mut db =
        BicDb::open_with_config(&source, primary_config(StorageMode::EmbeddedMemory)).unwrap();
    db.create_collection("items").unwrap();
    put(&db, "a", 1);
    put(&db, "b", 2);
    archive_all(&db, &archive);
    let last = db.current_commit_seq();
    // A crash mid-append leaves half a record behind.
    let segment = std::fs::read_dir(&archive)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "log"))
        .unwrap();
    let mut bytes = std::fs::read(&segment).unwrap();
    bytes.extend_from_slice(&[0, 0, 1, 0, 9, 9]);
    std::fs::write(&segment, &bytes).unwrap();
    let reopened = LogArchive::open(&archive, false).unwrap();
    assert_eq!(reopened.last_commit_seq(), last);
    drop(reopened);
    put(&db, "c", 3);
    archive_all(&db, &archive);
    let mut seen = Vec::new();
    read_log_archive(&archive, 0, ReplayTarget::default(), |frame| {
        seen.push(frame.commit_seq);
        Ok(true)
    })
    .unwrap();
    assert_eq!(seen, (1..=db.current_commit_seq()).collect::<Vec<_>>());
    // A base older than the archive start is refused.
    let mut fresh = LogArchive::open(root.path().join("late"), false).unwrap();
    let mut cursor = ReplicationCursor::new(last);
    db.archive_commits(&mut fresh, &mut cursor, 100).unwrap();
    let error = read_log_archive(root.path().join("late"), 0, ReplayTarget::default(), |_| {
        Ok(true)
    })
    .unwrap_err();
    assert!(error.to_string().contains("missing"), "{error}");
    db.release_log_archive_hold().unwrap();
}

#[test]
fn archived_base_backups_drive_restores_and_pruning() {
    use bicdb_core::log_archive::{list_base_backups, prune_log_archive};
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let archive = root.path().join("archive");
    let mut db =
        BicDb::open_with_config(&source, primary_config(StorageMode::EmbeddedMemory)).unwrap();
    archive_all(&db, &archive);
    let first = db.archive_base_backup(&archive, KEY).unwrap();
    db.create_collection("items").unwrap();
    put(&db, "a", 1);
    archive_all(&db, &archive);
    let second = db.archive_base_backup(&archive, KEY).unwrap();
    put(&db, "b", 2);
    archive_all(&db, &archive);
    assert_eq!(list_base_backups(&archive).unwrap().len(), 2);

    // The newest base that precedes the target is used.
    let restored = root.path().join("restored");
    let report =
        bicdb_core::restore_point_in_time(&archive, &restored, KEY, true, ReplayTarget::default())
            .unwrap();
    assert_eq!(report.base_commit_seq, second.commit_seq);
    assert_eq!(ids(&restored, StorageMode::EmbeddedMemory), ["a", "b"]);
    let early = root.path().join("early");
    let report = bicdb_core::restore_point_in_time(
        &archive,
        &early,
        KEY,
        true,
        ReplayTarget {
            commit_seq: Some(second.commit_seq - 1),
            timestamp: None,
        },
    )
    .unwrap();
    assert_eq!(report.base_commit_seq, first.commit_seq);

    // Keeping one base drops the older one.
    let pruned = prune_log_archive(&archive, 1, false).unwrap();
    assert_eq!(pruned.base_backups, 1);
    assert_eq!(
        list_base_backups(&archive).unwrap()[0].commit_seq,
        second.commit_seq
    );
    db.release_log_archive_hold().unwrap();
}

#[test]
fn checksum_corruption_is_never_repaired_as_a_crash_tail() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("archive");
    let mut db = BicDb::open_with_config(
        root.path().join("db"),
        primary_config(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    db.create_collection("items").unwrap();
    put(&db, "a", 1);
    archive_all(&db, &archive);
    let segment = std::fs::read_dir(&archive)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "log"))
        .unwrap();
    let mut bytes = std::fs::read(&segment).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&segment, &bytes).unwrap();
    assert!(read_log_archive(&archive, 0, ReplayTarget::default(), |_| Ok(true)).is_err());
    assert!(LogArchive::open(&archive, false).is_err());
    assert_eq!(
        std::fs::read(&segment).unwrap(),
        bytes,
        "corrupt evidence must be preserved"
    );
}

#[test]
fn replay_refuses_a_missing_requested_suffix_and_invalid_append_is_atomic() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path().join("db"),
        primary_config(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    db.create_collection("items").unwrap();
    put(&db, "a", 1);
    let frames = db
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let dir = root.path().join("archive");
    let mut archive = LogArchive::open(&dir, false).unwrap();
    let mut invalid = frames.clone();
    invalid.last_mut().unwrap().cluster_id = "wrong".into();
    assert!(archive.append("archive-test", &invalid).is_err());
    assert_eq!(archive.last_commit_seq(), 0);
    archive.append("archive-test", &frames).unwrap();
    assert!(read_log_archive(
        &dir,
        0,
        ReplayTarget {
            commit_seq: Some(db.current_commit_seq() + 1),
            timestamp: None
        },
        |_| Ok(true)
    )
    .is_err());
}

#[test]
fn restore_drill_compares_source_and_proves_scratch_promotion_and_restart() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let archive = root.path().join("archive");
        let scratch = root.path().join("scratch");
        let mut db =
            BicDb::open_with_config(root.path().join("db"), primary_config(storage)).unwrap();
        db.create_collection("items").unwrap();
        put(&db, "a", 1);
        archive_all(&db, &archive);
        db.archive_base_backup(&archive, KEY).unwrap();
        put(&db, "b", 2);
        archive_all(&db, &archive);
        let manifest = db.verification_manifest().unwrap();
        let report =
            bicdb_core::verify_restore_drill(&archive, KEY, &scratch, Some(&manifest)).unwrap();
        assert!(report.promotion_and_restart);
        assert_eq!(report.restored_commit_seq, manifest.commit_seq);
        assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 0);
        let mut wrong = manifest.clone();
        wrong.tables.get_mut("items").unwrap().rows += 1;
        assert!(bicdb_core::verify_restore_drill(&archive, KEY, &scratch, Some(&wrong)).is_err());
        assert_eq!(std::fs::read_dir(&scratch).unwrap().count(), 0);
        assert_eq!(db.verification_manifest().unwrap(), manifest);
    }
}

#[test]
fn an_empty_database_also_has_a_verifiable_archive_identity() {
    let root = tempfile::tempdir().unwrap();
    let archive = root.path().join("archive");
    let db = BicDb::open_with_config(
        root.path().join("db"),
        primary_config(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    archive_all(&db, &archive);
    db.archive_base_backup(&archive, KEY).unwrap();
    bicdb_core::verify_restore_drill(
        &archive,
        KEY,
        &root.path().join("scratch"),
        Some(&db.verification_manifest().unwrap()),
    )
    .unwrap();
}

#[test]
fn a_new_archive_starts_at_its_published_base_and_retains_the_new_suffix() {
    for storage in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let dir = root.path().join("archive");
        {
            // Historical commits were made before commit-frame retention existed.
            let mut db = BicDb::open_with_config(
                &source,
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(storage.clone()),
            )
            .unwrap();
            db.create_collection("items").unwrap();
            put(&db, "old", 1);
            db.compact().unwrap();
        }
        let mut db = BicDb::open_with_config(&source, primary_config(storage.clone())).unwrap();
        let mut archive = LogArchive::open(&dir, false).unwrap();
        let base = db.archive_base_backup(&dir, KEY).unwrap();
        assert!(base.commit_seq > 0);
        // Checkpoint during the base-publication/metadata-installation window
        // must not retire a commit needed after that base.
        put(&db, "during_initialization", 2);
        db.compact().unwrap();
        db.initialize_log_archive_from_base(&mut archive, &base)
            .unwrap();
        assert_eq!(archive.last_commit_seq(), base.commit_seq);
        assert!(
            db.initialize_log_archive_from_base(&mut archive, &base)
                .is_err(),
            "an existing archive must never be silently rebased"
        );
        let mut cursor = ReplicationCursor::new(archive.last_commit_seq());
        while db.archive_commits(&mut archive, &mut cursor, 7).unwrap() > 0 {}
        put(&db, "after_initialization", 3);
        while db.archive_commits(&mut archive, &mut cursor, 7).unwrap() > 0 {}
        let final_seq = db.current_commit_seq();
        drop(archive);
        assert_eq!(
            LogArchive::open(&dir, false).unwrap().last_commit_seq(),
            final_seq
        );
        let restored = root.path().join("restored");
        let report = bicdb_core::restore_point_in_time(
            &dir,
            &restored,
            KEY,
            false,
            ReplayTarget {
                commit_seq: Some(final_seq),
                timestamp: None,
            },
        )
        .unwrap();
        assert_eq!(report.restored_commit_seq, final_seq);
        assert_eq!(
            ids(&restored, storage),
            vec!["after_initialization", "during_initialization", "old"]
        );
    }
}
