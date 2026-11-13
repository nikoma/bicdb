use super::*;
use crate::{ReplicationMode, ReplicationTlsConfig};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: ReplicationMode::Primary,
            listen_addr: Some("127.0.0.1:0".into()),
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        })
}

fn meta(db: &BicDb, name: &str) -> CollectionMeta {
    db.collections()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap()
}

fn enable(db: &mut BicDb) {
    db.activate_native_origin_root_v1().unwrap();
    db.activate_local_namespace_identity_v1().unwrap();
    db.activate_native_collection_birth_v1().unwrap();
    db.activate_native_wal_backing_capture_v1().unwrap();
    db.activate_native_catalog_allocation_facts_v1().unwrap();
}

fn exports(db: &BicDb, after: u64) -> [Vec<CommitFrame>; 2] {
    [
        db.export_replication_frames_since(after, usize::MAX)
            .unwrap(),
        db.replication_frames_after(&mut crate::ReplicationCursor::new(after), usize::MAX)
            .unwrap(),
    ]
}

fn facts(frames: &[CommitFrame], name: &str) -> Vec<CollectionMeta> {
    frames
        .iter()
        .filter_map(|f| {
            f.captured_native_catalog_allocations()
                .unwrap()
                .and_then(|m| m.get(name))
                .cloned()
        })
        .collect()
}

#[test]
fn genuine_create_captures_child_before_first_row_and_survives_two_opens() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("allocation-history", 0);
        enable(&mut db);
        let before = db.last_commit_seq();
        db.create_timeseries_collection("items").unwrap();
        let allocated = meta(&db, "items");
        assert!(!allocated.native_catalog_allocation_pending);
        assert!(allocated.native_backing_birth_id.is_some());
        assert_eq!(
            allocated.native_backing_origin_root,
            db.native_origin_root_id().unwrap()
        );
        let created = db.last_commit_seq();
        for frames in exports(&db, before) {
            assert_eq!(facts(&frames, "items"), vec![allocated.clone()]);
            assert!(frames
                .iter()
                .flat_map(|f| &f.writes)
                .all(|w| w.collection != "items"));
            for frame in frames {
                let decoded: CommitFrame =
                    serde_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
                assert!(decoded
                    .captured_native_catalog_allocations()
                    .unwrap()
                    .is_none());
                let mut changed = frame.clone();
                changed.commit_seq += 1;
                changed.checksum = changed.calculate_checksum();
                assert!(changed.captured_native_catalog_allocations().is_err());
            }
        }
        let row = Record::new("ACK").with_metadata(json!({"value":73}));
        db.insert("items", row.clone()).unwrap();
        assert!(exports(&db, created)
            .iter()
            .all(|f| facts(f, "items").is_empty()));
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(meta(&db, "items"), allocated);
            assert_eq!(
                db.get("items", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            for frames in exports(&db, before) {
                assert_eq!(facts(&frames, "items"), vec![allocated.clone()]);
            }
            db.close().unwrap();
        }
    }
}

#[test]
fn late_activation_and_forged_catalog_rows_never_mint_allocation_facts() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("allocation-history", 0);
        assert!(db.activate_native_catalog_allocation_facts_v1().is_err());
        db.activate_native_origin_root_v1().unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        db.activate_native_wal_backing_capture_v1().unwrap();
        db.create_collection("earlier").unwrap();
        let allocated = meta(&db, "earlier");
        let before = db.last_commit_seq();
        db.activate_native_catalog_allocation_facts_v1().unwrap();
        db.create_collection("earlier").unwrap();
        assert_eq!(meta(&db, "earlier"), allocated);
        assert_eq!(db.last_commit_seq(), before);
        let forged = Record::new("forged-create")
            .with_metadata(json!({"op":"create","name":"earlier","mode":"standard"}));
        // This is an ordinary Native row, even when its payload mimics DDL.
        db.insert(collection_rename::CATALOG_LOG_COLLECTION, forged)
            .unwrap();
        db.insert("earlier", Record::new("ACK")).unwrap();
        for frames in exports(&db, 0) {
            assert!(facts(&frames, "earlier").is_empty());
        }
        let mut donor = allocated.clone();
        donor.native_catalog_allocation_pending = true;
        assert!(!donor.portable_contract().native_catalog_allocation_pending);
        db.restore_migrated_collection_contract(&donor).unwrap();
        assert_eq!(meta(&db, "earlier"), allocated);
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            db.create_collection("earlier").unwrap();
            assert_eq!(meta(&db, "earlier"), allocated);
            for frames in exports(&db, 0) {
                assert!(facts(&frames, "earlier").is_empty());
            }
            assert!(db.get("earlier", "ACK").unwrap().is_some());
            db.close().unwrap();
        }
    }
}

#[test]
fn retained_creations_keep_actual_birth_and_contract_after_same_name_reuse() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("allocation-history", 0);
        enable(&mut db);
        db.create_collection("items").unwrap();
        let first = meta(&db, "items");
        let row = Record::new("first_ACK").with_metadata(json!({"value":91}));
        db.insert("items", row.clone()).unwrap();
        db.rename_collection("items", "kept").unwrap();
        db.create_timeseries_collection("items").unwrap();
        let second = meta(&db, "items");
        assert_ne!(
            first.native_backing_birth_id,
            second.native_backing_birth_id
        );
        db.drop_collection("items").unwrap();
        db.create_collection("items").unwrap();
        let third = meta(&db, "items");
        assert_ne!(
            second.native_backing_birth_id,
            third.native_backing_birth_id
        );
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            for frames in exports(&db, 0) {
                assert_eq!(
                    facts(&frames, "items"),
                    vec![first.clone(), second.clone(), third.clone()]
                );
            }
            assert_eq!(
                db.get("kept", "first_ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            assert!(db.get("items", "first_ACK").unwrap().is_none());
            db.close().unwrap();
        }
    }
}

#[test]
fn actual_wal_eio_leaves_pending_birth_and_retry_publishes_that_same_backing() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for site in ["transaction_log.write", "transaction_log.sync"] {
            let parent = tempfile::tempdir().unwrap();
            let path = parent.path().join("db");
            let cfg = config(mode);
            let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            db.sync_quorum().hold("allocation-history", 0);
            enable(&mut db);
            db.create_collection("independent").unwrap();
            let row = Record::new("ACK").with_metadata(json!({"value":79}));
            db.insert("independent", row.clone()).unwrap();
            let ack_seq = db.last_commit_seq();
            format::enable_range_image_install(&path).unwrap();
            let fault = bicdb_page::test_faults::arm_io_failure(site, &path, 1, false);
            assert!(db.create_collection("pending").is_err(), "{mode:?} {site}");
            drop(fault);
            let allocated = meta(&db, "pending");
            assert!(allocated.native_catalog_allocation_pending);
            assert!(db.replication_shippable_seq() <= ack_seq);
            drop(db);
            let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(meta(&db, "pending"), allocated);
            let error = db
                .insert("pending", Record::new("must_refuse"))
                .unwrap_err();
            assert!(error.to_string().contains("pending"), "{error}");
            assert!(db.rename_collection("pending", "must_refuse").is_err());
            if mode == StorageMode::ServerPaged {
                let error = db.capture_range_image_bindings().unwrap_err();
                assert!(error.to_string().contains("pending"), "{error}");
            }
            db.create_collection("pending").unwrap();
            let mut completed = allocated.clone();
            completed.native_catalog_allocation_pending = false;
            assert_eq!(meta(&db, "pending"), completed);
            for frames in exports(&db, ack_seq) {
                let captured = facts(&frames, "pending");
                assert!(!captured.is_empty());
                assert!(captured.iter().all(|fact| fact == &completed));
            }
            db.close().unwrap();
            for _ in 0..2 {
                let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
                assert_eq!(meta(&db, "pending"), completed);
                assert!(db.get("pending", "must_refuse").unwrap().is_none());
                assert_eq!(
                    db.get("independent", "ACK")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    row.content_hash().unwrap()
                );
                db.close().unwrap();
            }
        }
    }
}

#[test]
fn actual_marker_clear_eio_never_allocates_a_second_birth() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("allocation-history", 0);
        enable(&mut db);
        db.create_collection("independent").unwrap();
        db.insert("independent", Record::new("ACK")).unwrap();
        let before = db.last_commit_seq();
        // The catalog log already exists. First catalog sync installs the
        // pending allocation; the second clears it after the WAL decision.
        let fault = bicdb_page::test_faults::arm_io_failure("catalog.dir_sync", &path, 2, false);
        assert!(db.create_collection("items").is_err(), "{mode:?}");
        drop(fault);
        let birth = meta(&db, "items").native_backing_birth_id;
        assert!(birth.is_some());
        assert!(matches!(
            db.create_collection("refuse"),
            Err(BicDbError::WriteFailed { .. })
        ));
        drop(db);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.create_collection("items").unwrap();
        assert_eq!(meta(&db, "items").native_backing_birth_id, birth);
        assert!(!meta(&db, "items").native_catalog_allocation_pending);
        for frames in exports(&db, before) {
            let captured = facts(&frames, "items");
            assert!(!captured.is_empty());
            assert!(captured.iter().all(|m| m.native_backing_birth_id == birth));
        }
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(meta(&db, "items").native_backing_birth_id, birth);
            assert!(db.get("independent", "ACK").unwrap().is_some());
            db.close().unwrap();
        }
    }
}

#[test]
fn retained_creation_facts_require_their_fence_even_after_drop() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("allocation-history", 0);
        enable(&mut db);
        db.create_collection("items").unwrap();
        db.drop_collection("items").unwrap();
        assert!(db
            .collections()
            .iter()
            .all(|m| !m.native_catalog_allocation_pending));
        db.close().unwrap();
        let file = format::metadata_path(&path);
        let original = fs::read(&file).unwrap();
        let mut metadata: Value = serde_json::from_slice(&original).unwrap();
        metadata["feature_flags"]
            .as_array_mut()
            .unwrap()
            .retain(|f| f.as_str() != Some(format::NATIVE_CATALOG_ALLOCATION_FACTS_FEATURE_FLAG));
        fs::write(&file, serde_json::to_vec_pretty(&metadata).unwrap()).unwrap();
        let error = BicDb::open_with_config(&path, cfg.clone()).unwrap_err();
        assert!(error.to_string().contains("allocation facts"), "{error}");
        fs::write(file, original).unwrap();
        let db = BicDb::open_with_config(&path, cfg).unwrap();
        for frames in exports(&db, 0) {
            assert_eq!(facts(&frames, "items").len(), 1);
        }
    }
}
