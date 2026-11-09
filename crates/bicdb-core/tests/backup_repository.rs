use std::sync::Arc;
use std::time::{Duration, Instant};

use bicdb_core::{
    restore_backup, verify_backup, BackupCreateOptions, BackupRestoreOptions, BicDb,
    Bm25Parameters, DbConfig, IndexDefinition, IndexField, IndexKind, OnlineBackupControl,
    OnlineBackupLimits, OnlineBackupPhase, Record, StorageMode,
};
use parking_lot::RwLock;
use serde_json::json;

const KEY: &str = "repository-backup-test-key";

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_sync_outbox(false)
        .with_storage_mode(mode)
        .with_paged_extent_bytes(1024 * 1024)
}

fn options() -> BackupCreateOptions {
    BackupCreateOptions {
        passphrase: KEY.into(),
        base_backup: None,
    }
}

fn seed(path: &std::path::Path, mode: StorageMode) -> BicDb {
    let mut db = BicDb::open_with_config(path, config(mode)).unwrap();
    db.create_collection("rows").unwrap();
    db.create_collection("catalog_at_cut").unwrap();
    for i in 0..80 {
        db.insert(
            "rows",
            Record::new(format!("r{i:04}")).with_metadata(json!({
                "n": i, "body": format!("snapshot original searchable {i}"),
                "padding": "p".repeat(2048),
            })),
        )
        .unwrap();
    }
    for (name, kind, field) in [
        ("rows_number", IndexKind::BTree, "n"),
        ("rows_fts", IndexKind::FullText, "body"),
    ] {
        db.create_index(IndexDefinition {
            nulls_not_distinct: false,
            name: name.into(),
            collection: "rows".into(),
            fields: vec![IndexField::MetadataPath(vec![field.into()])],
            kind,
            unique: false,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
    }
    db.checkpoint_for_resume().unwrap();
    // These dirty pages must be reconstructed by the captured WAL, rather than
    // an accidental full buffer flush at capture hiding missing recovery data.
    db.insert(
        "rows",
        Record::new("wal-only").with_metadata(json!({"n": 100, "body": "snapshot dirty"})),
    )
    .unwrap();
    db
}

#[test]
fn full_backup_restores_the_exact_cut_while_normal_commits_and_ddl_complete() {
    let source_dir = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let source = Arc::new(RwLock::new(seed(
        source_dir.path(),
        StorageMode::ServerPaged,
    )));
    let expected = source.read().verification_manifest().unwrap();
    let ranked = |db: &BicDb| {
        db.full_text_bm25_top_k(
            "rows_fts",
            &["snapshot"],
            Bm25Parameters::default(),
            200,
            true,
        )
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|hit| (hit.primary_key, hit.score.to_bits()))
        .collect::<Vec<_>>()
    };
    let expected_ranking = ranked(&source.read());
    let snapshot = source
        .read()
        .capture_online_backup(OnlineBackupLimits {
            bytes_per_second: 256 * 1024,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(snapshot.commit_seq(), expected.commit_seq);
    // Exercise preservation before the worker can copy any physical page.
    // Copying/WAL-release progress alone cannot guarantee that the later
    // foreground writes still touch an uncopied prefix: the worker may already
    // have copied all live low pages while it finishes the extent's tail.
    // Keep the independent commits, DDL and checkpoint during copying below.
    {
        let mut db = source.write();
        db.insert(
            "rows",
            Record::new("r0079").with_metadata(json!({
                "n": 79, "body": "changed after the cut before copying", "padding": "q".repeat(2048),
            })),
        )
        .unwrap();
        db.checkpoint_for_resume().unwrap();
    }
    let control = OnlineBackupControl::default();
    let worker_control = control.clone();
    let path = repo.path().join("full.bicbackup");
    let worker_path = path.clone();
    let worker =
        std::thread::spawn(move || snapshot.write_backup(worker_path, options(), worker_control));
    let deadline = Instant::now() + Duration::from_secs(15);
    while !control.progress().wal_retention_released {
        assert!(
            Instant::now() < deadline,
            "copy never started: {:?}",
            control.progress()
        );
        assert!(
            !worker.is_finished(),
            "backup ended before concurrent-write proof"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(control.progress().phase, OnlineBackupPhase::Copying);
    {
        let mut db = source.write();
        for i in 0..10 {
            assert!(db.delete("rows", &format!("r{i:04}")).unwrap());
            db.insert(
                "rows",
                Record::new(format!("after-{i}"))
                    .with_metadata(json!({"n": 200+i, "body": "changed later"})),
            )
            .unwrap();
        }
        db.drop_collection("catalog_at_cut").unwrap();
        db.create_collection("catalog_after_cut").unwrap();
        db.checkpoint_for_resume().unwrap();
    }
    assert_eq!(
        control.progress().phase,
        OnlineBackupPhase::Copying,
        "foreground work did not finish during the copy"
    );
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.commit_seq, expected.commit_seq);
    assert!(
        report.page_before_image_bytes > 0,
        "checkpoint did not exercise page preservation"
    );
    assert_eq!(control.progress().phase, OnlineBackupPhase::Complete);
    verify_backup(&path, KEY).unwrap();
    restore_backup(
        &path,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    let db = BicDb::open_with_config(target.path(), config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(db.verification_manifest().unwrap(), expected);
    assert_eq!(ranked(&db), expected_ranking);
    assert!(db
        .verify_indexes_consistently()
        .unwrap()
        .iter()
        .all(|i| i.valid));
    assert!(db.get("rows", "wal-only").unwrap().is_some());
    assert!(db.get("rows", "after-0").unwrap().is_none());
    assert!(db.collections().iter().any(|c| c.name == "catalog_at_cut"));
    assert!(!db
        .collections()
        .iter()
        .any(|c| c.name == "catalog_after_cut"));
}

#[test]
fn embedded_capture_keeps_append_prefixes_after_updates_and_compaction() {
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut db = seed(source.path(), StorageMode::EmbeddedMemory);
    let expected = db.verification_manifest().unwrap();
    let snapshot = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap();
    db.delete("rows", "r0000").unwrap();
    db.insert("rows", Record::new("new")).unwrap();
    db.compact().unwrap();
    let path = repo.path().join("full.bicbackup");
    snapshot
        .write_backup(&path, options(), OnlineBackupControl::default())
        .unwrap();
    restore_backup(
        &path,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    let restored =
        BicDb::open_with_config(target.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    assert_eq!(restored.verification_manifest().unwrap(), expected);
}

#[test]
fn cancellation_and_wrong_keys_do_not_publish_or_change_a_live_target() {
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    let cancelled = repo.path().join("cancelled.bicbackup");
    let snapshot = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap();
    let control = OnlineBackupControl::default();
    control.cancel();
    assert!(snapshot
        .write_backup(&cancelled, options(), control.clone())
        .is_err());
    assert!(!cancelled.exists());
    assert_eq!(control.progress().phase, OnlineBackupPhase::Cancelled);
    let path = repo.path().join("full.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&path, options(), OnlineBackupControl::default())
        .unwrap();
    assert!(verify_backup(&path, "wrong-key").is_err());
    let before = db.verification_manifest().unwrap();
    assert!(restore_backup(
        &path,
        source.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true
        }
    )
    .is_err());
    assert_eq!(db.verification_manifest().unwrap(), before);
    assert!(!source.path().join("backup-restore.pending").exists());
}

#[test]
fn pending_restore_is_refused_before_recovery_mutates_the_directory() {
    let source = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    drop(db);
    std::fs::write(source.path().join("backup-restore.pending"), b"interrupted").unwrap();
    let error =
        BicDb::open_with_config(source.path(), config(StorageMode::ServerPaged)).unwrap_err();
    assert!(error.to_string().contains("interrupted backup restore"));
    assert_eq!(
        std::fs::read(source.path().join("backup-restore.pending")).unwrap(),
        b"interrupted"
    );
}

#[test]
fn checkpoint_reopens_the_transaction_writer_before_the_next_commit() {
    for fsync in [false, true] {
        let source = tempfile::tempdir().unwrap();
        let cfg = config(StorageMode::ServerPaged).with_fsync(fsync);
        let mut db = BicDb::open_with_config(source.path(), cfg.clone()).unwrap();
        db.create_collection("rows").unwrap();
        db.insert("rows", Record::new("before")).unwrap();
        db.checkpoint_for_resume().unwrap();
        db.insert("rows", Record::new("after")).unwrap();
        let expected = db.verification_manifest().unwrap();
        assert!(
            std::fs::metadata(source.path().join("transactions.log"))
                .unwrap()
                .len()
                > 0,
            "the new commit was written to an unlinked old log inode"
        );
        drop(db); // Do not close/checkpoint: that would mask a lost seq marker.
        let reopened = BicDb::open_with_config(source.path(), cfg).unwrap();
        assert_eq!(reopened.verification_manifest().unwrap(), expected);
    }
}

#[test]
fn incrementals_skip_unchanged_extents_survive_restart_and_restore_without_parent_files() {
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut db = seed(source.path(), StorageMode::ServerPaged);
    for i in 0..700 {
        db.insert(
            "rows",
            Record::new(format!("large-{i:04}")).with_metadata(
                json!({"n": i+1000, "body": "snapshot incremental", "padding": "q".repeat(4096)}),
            ),
        )
        .unwrap();
    }
    db.checkpoint_for_resume().unwrap();
    let full = repo.path().join("full.bicbackup");
    let first = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&full, options(), OnlineBackupControl::default())
        .unwrap();
    let incremental = repo.path().join("incremental.bicbackup");
    let parent_options = |base: &std::path::Path| BackupCreateOptions {
        passphrase: KEY.into(),
        base_backup: Some(base.into()),
    };
    let second = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(
            &incremental,
            parent_options(&full),
            OnlineBackupControl::default(),
        )
        .unwrap();
    assert!(!second.artifact.full);
    assert_eq!(
        second.page_extents_copied, 0,
        "unchanged extent payloads must not be read"
    );
    assert_eq!(second.page_extents_reused, first.page_extents_copied);
    assert!(
        second.page_extents_reused > 1,
        "fixture must exercise multiple extents"
    );
    eprintln!("incremental evidence: full_source_bytes={}, incremental_source_bytes={}, reused_extent_bytes={}, extents_reused={}, capture_ms={}", first.source_bytes_read, second.source_bytes_read, second.page_extent_bytes_reused, second.page_extents_reused, second.coordination_millis);
    assert!(
        second.reused_bytes > 1024 * 1024,
        "no physical extent reuse: {second:?}"
    );
    assert!(
        second.source_bytes_read < first.source_bytes_read / 2,
        "unchanged source extents were reread"
    );
    db.insert(
        "rows",
        Record::new("after-parent")
            .with_metadata(json!({"n": 99999, "body": "incremental later WAL"})),
    )
    .unwrap();
    let expected = db.verification_manifest().unwrap();
    drop(db);
    let db = BicDb::open_with_config(source.path(), config(StorageMode::ServerPaged)).unwrap();
    let final_path = repo.path().join("final.bicbackup");
    let third = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(
            &final_path,
            parent_options(&incremental),
            OnlineBackupControl::default(),
        )
        .unwrap();
    assert!(
        third.page_extents_reused > 0,
        "restart disabled safe tracking reuse"
    );
    bicdb_core::verify_backup_chain([&full, &incremental, &final_path], KEY).unwrap();
    std::fs::remove_file(&full).unwrap();
    std::fs::remove_file(&incremental).unwrap();
    verify_backup(&final_path, KEY).unwrap();
    restore_backup(
        &final_path,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    let restored_tracker = target.path().join("paged/.backup-extents-store.pages");
    assert!(
        !restored_tracker.exists(),
        "runtime tracking must not be restored"
    );
    let original_tracker =
        std::fs::read(source.path().join("paged/.backup-extents-store.pages")).unwrap();
    let restored =
        BicDb::open_with_config(target.path(), config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(restored.verification_manifest().unwrap(), expected);
    assert!(restored
        .verify_indexes_consistently()
        .unwrap()
        .iter()
        .all(|index| index.valid));
    let fresh_tracker = std::fs::read(restored_tracker).unwrap();
    assert_ne!(
        &original_tracker[8..24],
        &fresh_tracker[8..24],
        "a restored store must durably start a new tracking lineage before recovery writes"
    );
}

#[test]
fn missing_objects_foreign_parents_and_gc_fail_before_publishing_or_installing() {
    let source = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    let full = repo.path().join("full.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&full, options(), OnlineBackupControl::default())
        .unwrap();
    let foreign_db = seed(other.path(), StorageMode::ServerPaged);
    let foreign = repo.path().join("foreign.bicbackup");
    let inc_options = || BackupCreateOptions {
        passphrase: KEY.into(),
        base_backup: Some(full.clone()),
    };
    let error = foreign_db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&foreign, inc_options(), OnlineBackupControl::default())
        .unwrap_err();
    assert!(error.to_string().contains("another repository or database"));
    assert!(!foreign.exists());
    let incremental = repo.path().join("incremental.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&incremental, inc_options(), OnlineBackupControl::default())
        .unwrap();
    std::fs::remove_file(&full).unwrap();
    let kept =
        bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, false).unwrap();
    assert_eq!(kept.manifests_retained, 1);
    assert_eq!(
        kept.objects_removed, 0,
        "shared objects from the removed parent must stay reachable"
    );
    // Remove one required object and prove a forced restore leaves the target
    // unchanged, and that collection refuses to sweep any further objects.
    let object = std::fs::read_dir(repo.path().join("objects"))
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().unwrap().is_dir())
        .unwrap()
        .path();
    let object = std::fs::read_dir(object)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let saved = std::fs::read(&object).unwrap();
    std::fs::remove_file(&object).unwrap();
    std::fs::write(target.path().join("sentinel"), b"preserve target").unwrap();
    assert!(verify_backup(&incremental, KEY).is_err());
    assert!(bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, false).is_err());
    assert!(restore_backup(
        &incremental,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true
        }
    )
    .is_err());
    assert_eq!(
        std::fs::read(target.path().join("sentinel")).unwrap(),
        b"preserve target"
    );
    let failed = repo.path().join("failed.bicbackup");
    assert!(db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(
            &failed,
            BackupCreateOptions {
                passphrase: KEY.into(),
                base_backup: Some(incremental.clone())
            },
            OnlineBackupControl::default()
        )
        .is_err());
    assert!(!failed.exists());
    std::fs::write(&object, saved).unwrap();
    let intact = std::fs::read(&incremental).unwrap();
    let mut corrupted = intact.clone();
    corrupted[0] ^= 1;
    std::fs::write(&incremental, &corrupted).unwrap();
    assert!(
        bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, false).is_err(),
        "damaged magic must not hide a retained manifest from collection"
    );
    std::fs::write(&incremental, intact).unwrap();
    std::fs::remove_file(&incremental).unwrap();
    let dry = bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, true).unwrap();
    assert!(dry.objects_unreferenced > 0);
    assert_eq!(dry.objects_removed, 0);
    let swept =
        bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, false).unwrap();
    assert_eq!(swept.objects_removed, dry.objects_unreferenced);
}

#[test]
fn attachment_inode_and_committed_reference_are_captured_together() {
    use std::io::Read;
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let mut db = seed(source.path(), StorageMode::ServerPaged);
    let original = vec![42; 2 * 1024 * 1024];
    db.write_attachment_from_reader(
        "rows",
        "r0000",
        "attachment",
        Some("application/octet-stream"),
        &original[..],
    )
    .unwrap();
    let expected = db.verification_manifest().unwrap();
    let snapshot = db
        .capture_online_backup(OnlineBackupLimits::default())
        .unwrap();
    db.write_attachment_from_reader("rows", "r0000", "attachment", None, &b"replacement"[..])
        .unwrap();
    let path = repo.path().join("blob.bicbackup");
    snapshot
        .write_backup(&path, options(), OnlineBackupControl::default())
        .unwrap();
    restore_backup(
        &path,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    let restored =
        BicDb::open_with_config(target.path(), config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(restored.verification_manifest().unwrap(), expected);
    let mut bytes = Vec::new();
    restored
        .open_attachment_reader("rows", "r0000", "attachment")
        .unwrap()
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, original);
}

// The test harness itself is the child executable; no optional helper build or
// graceful unwinding can mask a lost committed write in this regression.
#[test]
fn backup_crash_child() {
    if let Some(path) = std::env::var_os("BICDB_BACKUP_OWNERSHIP_PROBE") {
        assert!(
            BicDb::open_with_config(path, config(StorageMode::ServerPaged)).is_err(),
            "backup released a source ownership lock across processes"
        );
        return;
    }
    let Some(root) = std::env::var_os("BICDB_BACKUP_CRASH_CHILD") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut db = BicDb::open_with_config(
        root.join("db"),
        config(StorageMode::ServerPaged).with_fsync(true),
    )
    .unwrap();
    let snapshot = db
        .capture_online_backup(OnlineBackupLimits {
            bytes_per_second: 1024,
            ..Default::default()
        })
        .unwrap();
    let control = OnlineBackupControl::default();
    let copying = control.clone();
    let destination = root.join("repository/interrupted.bicbackup");
    let parent = root.join("repository/full.bicbackup");
    let _worker = std::thread::spawn(move || {
        snapshot.write_backup(
            destination,
            BackupCreateOptions {
                passphrase: KEY.into(),
                base_backup: Some(parent),
            },
            copying,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    while !control.progress().wal_retention_released {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(2));
    }
    db.insert("rows", Record::new("acknowledged-during-copy"))
        .unwrap();
    db.checkpoint_for_resume().unwrap();
    std::fs::write(
        root.join("acknowledged.json"),
        serde_json::to_vec(&db.verification_manifest().unwrap()).unwrap(),
    )
    .unwrap();
    loop {
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn sigkill_during_incremental_keeps_committed_source_and_prior_snapshot_recoverable() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("repository")).unwrap();
    let mut db = BicDb::open_with_config(
        root.path().join("db"),
        config(StorageMode::ServerPaged).with_fsync(true),
    )
    .unwrap();
    db.create_collection("rows").unwrap();
    db.insert("rows", Record::new("before")).unwrap();
    db.checkpoint_for_resume().unwrap();
    let full = root.path().join("repository/full.bicbackup");
    let original = db.verification_manifest().unwrap();
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&full, options(), OnlineBackupControl::default())
        .unwrap();
    drop(db);
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "backup_crash_child", "--nocapture"])
        .env("BICDB_BACKUP_CRASH_CHILD", root.path())
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let ready = root.path().join("acknowledged.json");
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "crash child exited before its acknowledged write"
        );
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("child never reached its copying checkpoint");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let expected: serde_json::Value = loop {
        if let Ok(value) = serde_json::from_slice(&std::fs::read(&ready).unwrap()) {
            break value;
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    child.kill().unwrap();
    child.wait().unwrap(); // SIGKILL: no destructors or close/checkpoint.
    assert!(!root
        .path()
        .join("repository/interrupted.bicbackup")
        .exists());
    verify_backup(&full, KEY).unwrap();
    let db = BicDb::open_with_config(
        root.path().join("db"),
        config(StorageMode::ServerPaged).with_fsync(true),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(db.verification_manifest().unwrap()).unwrap(),
        expected
    );
    let next = root.path().join("repository/restarted.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(
            &next,
            BackupCreateOptions {
                passphrase: KEY.into(),
                base_backup: Some(full.clone()),
            },
            OnlineBackupControl::default(),
        )
        .unwrap();
    let restored_path = root.path().join("restored");
    restore_backup(
        &next,
        &restored_path,
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: false,
        },
    )
    .unwrap();
    let restored =
        BicDb::open_with_config(&restored_path, config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(
        serde_json::to_value(restored.verification_manifest().unwrap()).unwrap(),
        expected
    );
    drop(restored);
    restore_backup(
        &full,
        &restored_path,
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    assert_eq!(
        BicDb::open_with_config(&restored_path, config(StorageMode::ServerPaged))
            .unwrap()
            .verification_manifest()
            .unwrap(),
        original
    );
}

#[test]
fn an_owned_capture_retains_database_ownership_after_its_handle_is_dropped() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let source = tempfile::tempdir().unwrap();
        let db = seed(source.path(), mode.clone());
        let snapshot = db
            .capture_online_backup(OnlineBackupLimits::default())
            .unwrap();
        drop(db);
        assert!(
            BicDb::open_with_config(source.path(), config(mode.clone())).is_err(),
            "capture lost its database directory ownership"
        );
        drop(snapshot);
        BicDb::open_with_config(source.path(), config(mode)).unwrap();
    }
}

#[test]
fn an_existing_v3_archive_can_adopt_v4_without_replacing_legacy_backups() {
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    let legacy = repo.path().join("legacy.bicbackup");
    db.create_consistent_backup(&legacy, options()).unwrap();
    let original = std::fs::read(&legacy).unwrap();
    let modern = repo.path().join("modern.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&modern, options(), OnlineBackupControl::default())
        .unwrap();
    verify_backup(&modern, KEY).unwrap();
    verify_backup(&legacy, KEY).unwrap();
    assert_eq!(std::fs::read(&legacy).unwrap(), original);
    assert_eq!(
        bicdb_core::prune_backup_repository(repo.path(), KEY, Duration::ZERO, false)
            .unwrap()
            .manifests_retained,
        1
    );
}

#[test]
fn tracking_feature_guard_survives_flush_close_and_reopen() {
    let source = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    drop(
        db.capture_online_backup(OnlineBackupLimits::default())
            .unwrap(),
    );
    let check = || {
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(source.path().join("format.json")).unwrap())
                .unwrap();
        assert!(metadata["feature_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "online_backup_extent_tracking_v1"));
    };
    check();
    db.flush().unwrap();
    check();
    db.close().unwrap();
    check();
    let db = BicDb::open_with_config(source.path(), config(StorageMode::ServerPaged)).unwrap();
    check();
    drop(db);
}

#[test]
fn legacy_backup_keeps_os_ownership_and_all_restores_refuse_live_child_handles() {
    let source = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let db = seed(source.path(), StorageMode::ServerPaged);
    let full = repo.path().join("full.bicbackup");
    db.capture_online_backup(OnlineBackupLimits::default())
        .unwrap()
        .write_backup(&full, options(), OnlineBackupControl::default())
        .unwrap();
    let legacy = repo.path().join("legacy.bicbackup");
    db.create_consistent_backup(&legacy, options()).unwrap();
    // Opening and closing a descriptor of store.lock releases fcntl locks on
    // Unix. A separate process must remain excluded after legacy copying.
    assert!(std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "backup_crash_child", "--nocapture"])
        .env("BICDB_BACKUP_OWNERSHIP_PROBE", source.path())
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap()
        .success());
    let expected = db.verification_manifest().unwrap();
    assert!(restore_backup(
        &legacy,
        source.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true
        }
    )
    .is_err());
    let storage = db.paged_records_handle().unwrap();
    drop(db);
    for path in [&legacy, &full] {
        assert!(
            restore_backup(
                path,
                source.path(),
                BackupRestoreOptions {
                    passphrase: KEY.into(),
                    force: true
                }
            )
            .is_err(),
            "restore removed a live child engine"
        );
        assert!(!source.path().join("backup-restore.pending").exists());
    }
    drop(storage);
    assert_eq!(
        BicDb::open_with_config(source.path(), config(StorageMode::ServerPaged))
            .unwrap()
            .verification_manifest()
            .unwrap(),
        expected
    );
}
