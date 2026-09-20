use bicdb_core::{
    create_backup, create_backup_to_writer, drill_backup_restore, load_distribution_config,
    restore_backup, restore_backup_to_point, restore_backup_to_point_with_limits, verify_backup,
    verify_backup_chain, BackupCreateOptions, BackupPitrReplayLimits,
    BackupPointInTimeRestoreOptions, BackupRestoreOptions, BicDb, ClusterBackupMetadata, ClusterId,
    ClusterNodeId, DbConfig, DistributionConfig, DistributionStore, Record, StorageMode,
    CURRENT_FORMAT_VERSION,
};
use serde_json::json;
use std::time::Duration;

const KEY: &str = "correct horse battery staple";

#[test]
fn full_backup_restores_an_identical_distributed_topology() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("cluster.bicbackup");

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("backup-cluster").unwrap(),
        node_id: ClusterNodeId::new("server-1").unwrap(),
        node_address: "10.0.0.1:9444".to_string(),
        node_capacity_bytes: 1_000_000,
        replication_factor: 1,
        initial_ranges: 4,
        ..DistributionConfig::default()
    };
    let store = DistributionStore::initialize_at(source.path(), config, false, 1_000).unwrap();
    let cluster_metadata = ClusterBackupMetadata::capture(store.topology()).unwrap();
    drop(store);

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let restored_config = load_distribution_config(restored.path()).unwrap();
    let restored_store = DistributionStore::open(restored.path(), restored_config, false).unwrap();
    cluster_metadata
        .validate_restored_topology(restored_store.topology())
        .unwrap();
    assert!(restored_store.topology().validate().is_ok());
}

#[test]
fn interrupted_backup_publish_keeps_previous_archive_until_atomic_replacement() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let first_restore = tempfile::tempdir().unwrap();
    let second_restore = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("atomic.bicbackup");
    let options = BackupCreateOptions {
        passphrase: KEY.to_string(),
        base_backup: None,
    };
    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(true)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    create_backup(source.path(), &backup_path, options.clone()).unwrap();
    let previous_archive = std::fs::read(&backup_path).unwrap();
    verify_backup(&backup_path, KEY).unwrap();

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(true)).unwrap();
        db.insert("patients", Record::new("p-2")).unwrap();
        db.close().unwrap();
    }
    let mut replacement_archive = Vec::new();
    create_backup_to_writer(source.path(), &mut replacement_archive, options.clone()).unwrap();
    assert!(replacement_archive.len() > 64);

    // Model process loss while write_atomic is filling `<archive>.tmp`.
    // The published path must remain byte-identical and restorable.
    let temporary_path = backup_path.with_extension("tmp");
    std::fs::write(
        &temporary_path,
        &replacement_archive[..replacement_archive.len() / 2],
    )
    .unwrap();
    assert_eq!(std::fs::read(&backup_path).unwrap(), previous_archive);
    verify_backup(&backup_path, KEY).unwrap();
    restore_backup(
        &backup_path,
        first_restore.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    let first = BicDb::open(first_restore.path()).unwrap();
    assert!(first.get("patients", "p-1").unwrap().is_some());
    assert!(first.get("patients", "p-2").unwrap().is_none());
    drop(first);

    // Retrying writes and atomically renames the complete replacement over
    // the old archive. No incomplete temp file survives publication.
    create_backup(source.path(), &backup_path, options).unwrap();
    assert!(!temporary_path.exists());
    assert_ne!(std::fs::read(&backup_path).unwrap(), previous_archive);
    verify_backup(&backup_path, KEY).unwrap();
    restore_backup(
        &backup_path,
        second_restore.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    let second = BicDb::open(second_restore.path()).unwrap();
    assert!(second.get("patients", "p-1").unwrap().is_some());
    assert!(second.get("patients", "p-2").unwrap().is_some());
}

#[test]
fn binary_backup_does_not_expand_payload_into_json_integer_arrays() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("compact.bicbackup");
    let payload_len = 4 * 1024 * 1024;
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let payload = (0..payload_len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect::<Vec<_>>();

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    std::fs::write(source.path().join("opaque-payload.bin"), &payload).unwrap();

    let report = create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    let backup = std::fs::read(&backup_path).unwrap();

    assert!(backup.starts_with(b"BICBAK03"));
    assert!(backup.len() < payload.len() * 2);
    assert_eq!(report.encrypted_bytes, backup.len() as u64);
    assert!(verify_backup(&backup_path, KEY).is_ok());
}

#[test]
fn encrypted_full_backup_verifies_and_restores_records() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("full.bicbackup");

    {
        let mut db = BicDb::open_with_config(
            source.path(),
            DbConfig::default()
                .with_storage_mode(StorageMode::EmbeddedMemory)
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.create_collection("vectors").unwrap();
        db.create_timeseries_collection("wearable").unwrap();
        db.insert(
            "patients",
            Record::new("p-1").with_metadata(json!({"name": "Asha"})),
        )
        .unwrap();
        db.insert("vectors", Record::new("v-1").with_vector(vec![1.0, 0.0]))
            .unwrap();
        db.insert(
            "wearable",
            Record::new("w-1")
                .with_timestamp(100)
                .with_metadata(json!({"metric": "hrv", "value": 52.0})),
        )
        .unwrap();
        db.close().unwrap();
    }

    let report = create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    assert!(report.full);
    assert!(report.files_included > 0);
    assert_eq!(report.source_format_version, CURRENT_FORMAT_VERSION);
    assert_eq!(report.source_feature_flags, vec!["format_metadata_v2"]);
    assert!(std::fs::read(&backup_path)
        .unwrap()
        .windows(b"Asha".len())
        .all(|window| window != b"Asha"));

    let verify = verify_backup(&backup_path, KEY).unwrap();
    assert_eq!(verify.backup_id, report.backup_id);
    assert_eq!(verify.source_format_version, CURRENT_FORMAT_VERSION);
    assert_eq!(verify.source_feature_flags, vec!["format_metadata_v2"]);
    assert!(verify_backup(&backup_path, "wrong key").is_err());

    let restore = restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    assert_eq!(restore.source_format_version, CURRENT_FORMAT_VERSION);
    let db = BicDb::open_with_config(
        restored.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap();
    assert_eq!(
        db.get("patients", "p-1").unwrap().unwrap().metadata["name"],
        "Asha"
    );
    assert!(db.get("vectors", "v-1").unwrap().unwrap().vector.is_some());
    assert_eq!(db.scan_time_range("wearable", 0, 200).unwrap().len(), 1);
}

#[test]
fn incremental_backup_contains_changed_files_and_applies_over_base_restore() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let full_path = backup_dir.path().join("full.bicbackup");
    let inc_path = backup_dir.path().join("inc.bicbackup");

    {
        let mut db = BicDb::open_with_config(
            source.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    let full = create_backup(
        source.path(),
        &full_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.insert(
            "patients",
            Record::new("p-2").with_metadata(json!({"incremental": true})),
        )
        .unwrap();
        db.close().unwrap();
    }
    let inc = create_backup(
        source.path(),
        &inc_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: Some(full_path.clone()),
        },
    )
    .unwrap();
    assert!(!inc.full);
    assert!(inc.files_included <= full.files_total);

    restore_backup(
        &full_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    restore_backup(
        &inc_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: false,
        },
    )
    .unwrap();

    let db = BicDb::open(restored.path()).unwrap();
    assert!(db.get("patients", "p-1").unwrap().is_some());
    assert_eq!(
        db.get("patients", "p-2").unwrap().unwrap().metadata["incremental"],
        true
    );

    let chain = verify_backup_chain([&full_path, &inc_path], KEY).unwrap();
    assert_eq!(chain.backups_verified, 2);
    assert_eq!(chain.base_backup_id, full.backup_id);
    assert_eq!(chain.final_backup_id, inc.backup_id);
}

#[test]
fn incremental_backup_removes_files_deleted_after_the_base() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let full_path = backup_dir.path().join("full-delete.bicbackup");
    let inc_path = backup_dir.path().join("inc-delete.bicbackup");
    let obsolete = source.path().join("obsolete-sidecar.bin");

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    std::fs::write(&obsolete, b"remove me from an incremental restore").unwrap();
    create_backup(
        source.path(),
        &full_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    std::fs::remove_file(&obsolete).unwrap();
    create_backup(
        source.path(),
        &inc_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: Some(full_path.clone()),
        },
    )
    .unwrap();

    restore_backup(
        &full_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    assert!(restored.path().join("obsolete-sidecar.bin").exists());
    restore_backup(
        &inc_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: false,
        },
    )
    .unwrap();
    assert!(!restored.path().join("obsolete-sidecar.bin").exists());
}

#[test]
fn partial_or_corrupt_backup_is_rejected() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("full.bicbackup");

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    assert!(verify_backup(&backup_path, KEY).is_ok());

    let partial_path = backup_dir.path().join("partial.bicbackup");
    let mut partial = std::fs::read(&backup_path).unwrap();
    partial.truncate(partial.len().saturating_sub(32));
    std::fs::write(&partial_path, partial).unwrap();
    assert!(verify_backup(&partial_path, KEY).is_err());

    let corrupt_path = backup_dir.path().join("corrupt.bicbackup");
    let mut corrupt = std::fs::read(&backup_path).unwrap();
    let middle = corrupt.len() / 2;
    corrupt[middle] ^= 0x40;
    std::fs::write(&corrupt_path, corrupt).unwrap();
    assert!(verify_backup(&corrupt_path, KEY).is_err());
}

#[test]
fn pitr_restore_replays_expected_timestamp_state_in_bounded_batches() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored_early = tempfile::tempdir().unwrap();
    let restored_late = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("full.bicbackup");

    let first_timestamp;
    let second_timestamp;
    {
        let mut db = BicDb::open_with_config(
            source.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert(
            "patients",
            Record::new("p-1").with_metadata(json!({"version": 1})),
        )
        .unwrap();
        first_timestamp = db
            .events()
            .read(bicdb_core::RECORD_AUDIT_STREAM)
            .last()
            .unwrap()
            .event
            .timestamp;
        std::thread::sleep(Duration::from_secs(1));
        db.insert(
            "patients",
            Record::new("p-2").with_metadata(json!({"version": 2})),
        )
        .unwrap();
        second_timestamp = db
            .events()
            .read(bicdb_core::RECORD_AUDIT_STREAM)
            .last()
            .unwrap()
            .event
            .timestamp;
        assert!(second_timestamp > first_timestamp);
        db.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();

    let early_report = restore_backup_to_point_with_limits(
        &backup_path,
        restored_early.path(),
        BackupPointInTimeRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
            target_timestamp: Some(first_timestamp),
        },
        BackupPitrReplayLimits {
            max_records_per_batch: 1,
            max_events_per_batch: 1,
            max_event_bytes_per_batch: 4 * 1024,
        },
    )
    .unwrap();
    assert_eq!(early_report.restored_records, Some(1));
    let db = BicDb::open(restored_early.path()).unwrap();
    assert!(db.get("patients", "p-1").unwrap().is_some());
    assert!(db.get("patients", "p-2").unwrap().is_none());

    restore_backup_to_point(
        &backup_path,
        restored_late.path(),
        BackupPointInTimeRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
            target_timestamp: Some(second_timestamp),
        },
    )
    .unwrap();
    let db = BicDb::open(restored_late.path()).unwrap();
    assert!(db.get("patients", "p-1").unwrap().is_some());
    assert!(db.get("patients", "p-2").unwrap().is_some());
}

#[test]
fn backup_drill_restores_verifies_and_reports_evidence() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("full.bicbackup");

    {
        let mut db = BicDb::open_with_config(
            source.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();
        db.close().unwrap();
    }
    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();

    let report = drill_backup_restore(
        &backup_path,
        restored.path(),
        BackupPointInTimeRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
            target_timestamp: None,
        },
    )
    .unwrap();
    assert!(report.integrity_checked);
    assert_eq!(report.smoke_collections, 1);
    assert_eq!(report.smoke_records, 1);
    assert!(report.archived_events >= 1);
}

#[test]
fn online_backup_during_concurrent_writes_restores_consistent_snapshot() {
    let source = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let backup_path = backup_dir.path().join("online.bicbackup");

    {
        let mut db =
            BicDb::open_with_config(source.path(), DbConfig::default().with_fsync(false)).unwrap();
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-seed")).unwrap();
        db.close().unwrap();
    }

    let writer_path = source.path().to_path_buf();
    let writer = std::thread::spawn(move || {
        let mut db =
            BicDb::open_with_config(&writer_path, DbConfig::default().with_fsync(false)).unwrap();
        for idx in 0..100 {
            db.insert("patients", Record::new(format!("p-{idx:03}")))
                .unwrap();
            if idx % 10 == 0 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        db.close().unwrap();
    });

    std::thread::sleep(Duration::from_millis(5));
    create_backup(
        source.path(),
        &backup_path,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    writer.join().unwrap();

    restore_backup(
        &backup_path,
        restored.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();
    let db = BicDb::open(restored.path()).unwrap();
    let records = db.scan_collection("patients").unwrap();
    assert!(records.iter().any(|record| record.id == "p-seed"));
    assert!(records.len() <= 101);
}
