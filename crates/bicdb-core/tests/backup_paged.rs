//! Backup and restore drill for `storage_mode = server_paged`.
//!
//! This is gate 7 of the PubMed import plan: "Complete a backup and restore
//! drill." Nothing else covers it in paged mode, and paged mode is where it is
//! most likely to go wrong — records live in `paged/`, a directory the backup
//! format learned about only by walking the tree, and the restored database
//! records `server_paged` in its own `format.json`.
//!
//! The failure this guards against is not a crash. It is a backup that appears
//! to succeed, verifies, restores without error, and comes back missing every
//! row — because the engine that owns the rows was not the engine consulted.

use bicdb_core::{
    create_backup, drill_backup_restore, restore_backup, restore_backup_to_point_with_limits,
    verify_backup, BackupCreateOptions, BackupPitrReplayLimits, BackupPointInTimeRestoreOptions,
    BackupRestoreOptions, BicDb, DbConfig, Record, StorageMode,
};
use serde_json::json;

const KEY: &str = "correct horse battery staple";

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn seed(path: &std::path::Path, count: usize) {
    let mut db = BicDb::open_with_config(path, paged_config()).unwrap();
    db.create_collection("articles").unwrap();
    for index in 0..count {
        db.insert(
            "articles",
            Record::new(format!("pmid-{index:05}")).with_metadata(json!({
                "title": format!("A study of subject {index}"),
                "year": 2000 + (index % 25),
            })),
        )
        .unwrap();
    }
    db.close().unwrap();
}

#[test]
fn a_paged_backup_restores_every_record() {
    let source = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let backup_file = backup.path().join("paged.bicbackup");
    let records = 500;

    seed(source.path(), records);

    let created = create_backup(
        source.path(),
        &backup_file,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    assert!(
        created.files_included > 0,
        "backup included no files at all"
    );

    verify_backup(&backup_file, KEY).unwrap();

    restore_backup(
        &backup_file,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    // The restored database must still be a paged database — a restore that
    // quietly downgraded the mode would "work" while abandoning the engine that
    // holds the rows.
    assert_eq!(
        bicdb_core::storage_mode(target.path()).unwrap(),
        StorageMode::ServerPaged,
        "restore did not preserve storage_mode"
    );

    let restored = BicDb::open_with_config(target.path(), paged_config()).unwrap();
    let scanned = restored.scan_collection("articles").unwrap();
    assert_eq!(
        scanned.len(),
        records,
        "restored database is missing rows: {} of {records} present",
        scanned.len()
    );
    for index in 0..records {
        let id = format!("pmid-{index:05}");
        let record = restored
            .get("articles", &id)
            .unwrap()
            .unwrap_or_else(|| panic!("{id} missing after restore"));
        assert_eq!(
            record.metadata["year"],
            json!(2000 + (index % 25)),
            "{id} restored with wrong metadata"
        );
    }
}

#[test]
fn paged_pitr_replays_and_clears_rows_in_bounded_batches() {
    let source = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let backup_file = backup.path().join("paged-pitr.bicbackup");
    let config = paged_config().with_audit_events(true);
    let first_timestamp;

    {
        let mut db = BicDb::open_with_config(source.path(), config).unwrap();
        db.create_collection("articles").unwrap();
        for index in 0..7 {
            db.insert("articles", Record::new(format!("early-{index}")))
                .unwrap();
        }
        first_timestamp = db
            .events()
            .read(bicdb_core::RECORD_AUDIT_STREAM)
            .last()
            .unwrap()
            .event
            .timestamp;
        std::thread::sleep(std::time::Duration::from_secs(1));
        for index in 0..9 {
            db.insert("articles", Record::new(format!("late-{index}")))
                .unwrap();
        }
        db.close().unwrap();
    }

    create_backup(
        source.path(),
        &backup_file,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    let report = restore_backup_to_point_with_limits(
        &backup_file,
        target.path(),
        BackupPointInTimeRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
            target_timestamp: Some(first_timestamp),
        },
        BackupPitrReplayLimits {
            max_records_per_batch: 3,
            max_events_per_batch: 2,
            max_event_bytes_per_batch: 1_024,
        },
    )
    .unwrap();

    assert_eq!(report.restored_records, Some(7));
    let restored = BicDb::open_with_config(target.path(), paged_config()).unwrap();
    assert_eq!(restored.collection_record_count("articles").unwrap(), 7);
    for index in 0..7 {
        assert!(restored
            .get("articles", &format!("early-{index}"))
            .unwrap()
            .is_some());
    }
    for index in 0..9 {
        assert!(restored
            .get("articles", &format!("late-{index}"))
            .unwrap()
            .is_none());
    }
}

#[test]
fn the_backup_drill_reports_evidence_for_a_paged_database() {
    // `drill_backup_restore` reopens what it restored. It must adopt the
    // restored database's own recorded mode rather than the ambient default,
    // or the drill is unusable for exactly the databases whose recovery most
    // needs rehearsing.
    let source = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let backup_file = backup.path().join("drill.bicbackup");
    let records = 200;

    seed(source.path(), records);
    create_backup(
        source.path(),
        &backup_file,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();

    let report = drill_backup_restore(
        &backup_file,
        target.path(),
        BackupPointInTimeRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
            target_timestamp: None,
        },
    )
    .unwrap();

    assert!(
        report.integrity_checked,
        "drill skipped its integrity check"
    );
    assert_eq!(
        report.smoke_records, records,
        "drill smoke-tested {} records, expected {records}",
        report.smoke_records
    );
    assert!(report.smoke_collections >= 1);
}

#[test]
fn a_backup_taken_while_writes_are_in_flight_restores_consistently() {
    // An online backup copies files while the database is being written. The
    // page engine is built to recover from exactly that kind of inconsistent
    // snapshot, but only if its WAL and page file are both captured — so this
    // asserts the restored copy opens, recovers, and holds a prefix of the
    // writes rather than a corrupt tree.
    let source = tempfile::tempdir().unwrap();
    let backup = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let backup_file = backup.path().join("online.bicbackup");

    seed(source.path(), 100);

    let mut db = BicDb::open_with_config(source.path(), paged_config()).unwrap();
    let source_path = source.path().to_path_buf();
    let writer = std::thread::spawn(move || {
        for index in 100..400 {
            db.insert(
                "articles",
                Record::new(format!("pmid-{index:05}")).with_metadata(json!({
                    "title": format!("Concurrent {index}"),
                    "year": 2020,
                })),
            )
            .unwrap();
        }
        db.close().unwrap();
    });

    // Take the backup while the writer is running.
    std::thread::sleep(std::time::Duration::from_millis(20));
    create_backup(
        &source_path,
        &backup_file,
        BackupCreateOptions {
            passphrase: KEY.to_string(),
            base_backup: None,
        },
    )
    .unwrap();
    writer.join().unwrap();

    verify_backup(&backup_file, KEY).unwrap();
    restore_backup(
        &backup_file,
        target.path(),
        BackupRestoreOptions {
            passphrase: KEY.to_string(),
            force: true,
        },
    )
    .unwrap();

    let restored = BicDb::open_with_config(target.path(), paged_config()).unwrap();
    let scanned = restored.scan_collection("articles").unwrap();
    // The rows written before the backup began must all be there; rows written
    // during it may or may not be, which is what "consistent snapshot" means.
    assert!(
        scanned.len() >= 100,
        "restored copy lost rows that predated the backup: {} present",
        scanned.len()
    );
    for index in 0..100 {
        let id = format!("pmid-{index:05}");
        assert!(
            restored.get("articles", &id).unwrap().is_some(),
            "{id} predated the backup but is missing after restore"
        );
    }
}
