use bicdb_core::{
    BTreeVerifyCursor, BTreeVerifyLimits, BTreeVerifyStopReason, BicDb, DbConfig, Record,
    StorageMode, VersionChainVerifyCursor, VersionChainVerifyLimits, VersionChainVerifyStopReason,
};

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

#[test]
fn public_bounded_mvcc_verifier_covers_the_store_without_restarting() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    db.create_collection("patients").unwrap();
    {
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .batch_insert(
                "patients",
                (0..7).map(|index| Record::new(format!("patient-{index}"))),
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    db.flush().unwrap();

    let limits = VersionChainVerifyLimits {
        max_keys: 2,
        max_duration_millis: 10_000,
        max_fault_samples: 0,
        max_fault_sample_bytes: 0,
        ..VersionChainVerifyLimits::default()
    };
    let mut cursor = VersionChainVerifyCursor::default();
    let mut keys = 0_u64;
    let mut steps = 0_u64;
    loop {
        let report = db.verify_paged_version_chains_step(cursor, limits).unwrap();
        steps += 1;
        keys += report.version_chains.keys_examined;
        assert!(report.version_chains.valid);
        assert!(report.version_chains.keys_examined <= limits.max_keys);
        assert!(report.version_chains.versions_examined <= limits.max_versions);
        assert!(report.bytes_examined <= limits.max_bytes);
        if report.complete {
            assert_eq!(report.stop_reason, VersionChainVerifyStopReason::Complete);
            break;
        }
        assert_eq!(report.stop_reason, VersionChainVerifyStopReason::KeyLimit);
        cursor = report.next_cursor;
    }
    assert_eq!(keys, 7);
    assert!(steps > 1);
}

#[test]
fn embedded_mode_rejects_the_paged_mvcc_verifier() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    let error = db
        .verify_paged_version_chains_step(
            VersionChainVerifyCursor::default(),
            VersionChainVerifyLimits::default(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("storage_mode = server_paged"));
}

#[test]
fn public_bounded_btree_verifier_resumes_after_database_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        db.create_collection("patients").unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .batch_insert(
                "patients",
                (0..40).map(|index| Record::new(format!("patient-{index:03}"))),
            )
            .unwrap();
        transaction.commit().unwrap();
        db.flush().unwrap();
    }

    let limits = BTreeVerifyLimits {
        max_entries: 3,
        max_duration_millis: 10_000,
        ..BTreeVerifyLimits::default()
    };
    let first = {
        let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
        db.verify_paged_btree_step(BTreeVerifyCursor::default(), limits)
            .unwrap()
    };
    assert!(first.valid);
    assert!(!first.complete);
    assert_eq!(first.stop_reason, BTreeVerifyStopReason::EntryLimit);
    assert_eq!(first.entries_examined, 3);

    let db = BicDb::open_with_config(root.path(), config(StorageMode::ServerPaged)).unwrap();
    let mut cursor = first.next_cursor;
    let mut entries = first.entries_examined;
    let mut steps = 1_u64;
    loop {
        let report = db.verify_paged_btree_step(cursor, limits).unwrap();
        assert!(report.valid, "{report:?}");
        entries += report.entries_examined;
        steps += 1;
        if report.complete {
            assert_eq!(report.stop_reason, BTreeVerifyStopReason::Complete);
            break;
        }
        cursor = report.next_cursor;
        assert!(steps < 100, "public B-tree verifier did not converge");
    }
    assert_eq!(entries, 40);
    assert!(steps > 1);
}

#[test]
fn embedded_mode_rejects_the_paged_btree_verifier() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), config(StorageMode::EmbeddedMemory)).unwrap();
    let error = db
        .verify_paged_btree_step(BTreeVerifyCursor::default(), BTreeVerifyLimits::default())
        .unwrap_err();
    assert!(error.to_string().contains("storage_mode = server_paged"));
}
