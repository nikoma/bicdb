use super::*;
use crate::{DistributionConfig, DistributionStore};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}

fn enable(db: &mut BicDb) {
    db.activate_local_namespace_identity_v1().unwrap();
    db.activate_native_collection_birth_v1().unwrap();
}

fn meta(db: &BicDb, name: &str) -> CollectionMeta {
    db.collections()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap()
}

fn range() -> RangeDescriptor {
    let root = tempfile::tempdir().unwrap();
    DistributionStore::initialize_at(
        root.path(),
        DistributionConfig {
            enabled: true,
            initial_ranges: 1,
            replication_factor: 1,
            ..Default::default()
        },
        false,
        1,
    )
    .unwrap()
    .topology()
    .ranges
    .values()
    .next()
    .unwrap()
    .clone()
}

#[test]
fn native_birth_activation_and_retries_never_retrofit_legacy_backings() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.create_collection("legacy").unwrap();
        db.insert(
            "legacy",
            Record::new("ACK").with_metadata(json!({"value": 73})),
        )
        .unwrap();
        let ack = db
            .get("legacy", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        let seq = db.last_commit_seq();
        assert!(db.activate_native_collection_birth_v1().is_err());
        assert!(!format::read_or_legacy_metadata(root.path())
            .unwrap()
            .feature_flags
            .contains(format::NATIVE_COLLECTION_BIRTH_FEATURE_FLAG));
        db.activate_local_namespace_identity_v1().unwrap();
        let legacy_namespace = db.local_namespace_identity("legacy").unwrap();
        assert!(legacy_namespace.is_some());
        let schema = db.schema_compatibility_fingerprint().unwrap();
        for _ in 0..2 {
            db.activate_native_collection_birth_v1().unwrap();
            assert_eq!(db.native_collection_birth_id("legacy").unwrap(), None);
            assert_eq!(
                db.local_namespace_identity("legacy").unwrap(),
                legacy_namespace
            );
            assert_eq!(db.last_commit_seq(), seq);
            assert_eq!(db.schema_compatibility_fingerprint().unwrap(), schema);
            assert!(!format::required_reader_features(root.path())
                .unwrap()
                .iter()
                .any(|f| f == format::NATIVE_COLLECTION_BIRTH_FEATURE_FLAG));
        }
        db.create_collection("born").unwrap();
        let born = db.native_collection_birth_id("born").unwrap().unwrap();
        assert!(!born.is_nil());
        db.close().unwrap();
        for _ in 0..2 {
            let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            enable(&mut db);
            assert_eq!(db.native_collection_birth_id("legacy").unwrap(), None);
            assert_eq!(db.native_collection_birth_id("born").unwrap(), Some(born));
            assert_eq!(
                db.get("legacy", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                ack
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn native_birth_survives_rename_but_never_recreation_or_contract_restore() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        enable(&mut db);
        db.create_collection("items").unwrap();
        db.insert(
            "items",
            Record::new("ACK").with_metadata(json!({"owner": "first"})),
        )
        .unwrap();
        let ack = db
            .get("items", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        let original = meta(&db, "items");
        let birth = original.native_backing_birth_id.unwrap();
        db.rename_collection("items", "kept").unwrap();
        assert_eq!(db.native_collection_birth_id("kept").unwrap(), Some(birth));
        db.create_collection("items").unwrap();
        let replacement = meta(&db, "items");
        assert_ne!(replacement.storage(), original.storage());
        assert_ne!(replacement.native_backing_birth_id, Some(birth));
        db.drop_collection("items").unwrap();
        db.create_collection("items").unwrap();
        let recreated = db.native_collection_birth_id("items").unwrap().unwrap();
        assert_ne!(recreated, birth);
        assert_ne!(Some(recreated), replacement.native_backing_birth_id);
        db.restore_migrated_collection_contract(&original).unwrap();
        assert_eq!(
            db.native_collection_birth_id("items").unwrap(),
            Some(recreated)
        );
        assert!(db.get("items", "ACK").unwrap().is_none());
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
            assert_eq!(
                db.native_collection_birth_id("items").unwrap(),
                Some(recreated)
            );
            assert_eq!(db.native_collection_birth_id("kept").unwrap(), Some(birth));
            assert_eq!(
                db.get("kept", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                ack
            );
            db.close().unwrap();
        }
    }
}

#[test]
fn native_birth_untrusted_snapshot_and_commit_metadata_cannot_adopt_or_retrofit_birth() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let source_root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open_with_config(source_root.path(), config(mode.clone())).unwrap();
        enable(&mut source);
        source.create_collection("items").unwrap();
        let source_meta = meta(&source, "items");
        let source_birth = source_meta.native_backing_birth_id.unwrap();
        assert!(source_meta
            .portable_contract()
            .native_backing_birth_id
            .is_none());
        assert!(source_meta.portable_contract().local_namespace_id.is_none());
        let range = range();
        // Test a new born target, an existing unborn target under the new
        // fence, and a target that never enabled allocation identities.
        for target_kind in 0..3 {
            let target_root = tempfile::tempdir().unwrap();
            let cfg = config(mode.clone());
            let mut target = BicDb::open_with_config(target_root.path(), cfg.clone()).unwrap();
            if target_kind == 1 {
                target.create_collection("items").unwrap();
                target
                    .insert(
                        "items",
                        Record::new("local_ACK").with_metadata(json!({"local": true})),
                    )
                    .unwrap();
            }
            if target_kind != 2 {
                enable(&mut target);
            }
            let row = Record::new("snapshot_ACK")
                .with_metadata(json!({"source_birth_claim": source_birth}));
            let batch = RangeSnapshotBatch {
                collection: "items".into(),
                range_id: range.id,
                range_epoch: range.epoch,
                snapshot_commit_sequence: 1,
                required_reader_features: Vec::new(),
                resume_after_key: row.id.clone(),
                serialized_record_bytes: serde_json::to_vec(&row).unwrap().len(),
                records: vec![row.clone()],
            };
            target
                .apply_range_snapshot_batch(&range, &source_meta, &batch)
                .unwrap();
            let local_birth = target.native_collection_birth_id("items").unwrap();
            if target_kind == 0 {
                assert!(local_birth.is_some());
                assert_ne!(local_birth, Some(source_birth));
            } else {
                assert_eq!(local_birth, None);
            }
            let row2 =
                Record::new("frame_ACK").with_metadata(json!({"source_birth_claim": source_birth}));
            let frame = CommitFrame::new(
                "default",
                "local",
                "untrusted-test-stream",
                1,
                1,
                0,
                vec![ReplicationWrite {
                    collection: "items".into(),
                    record_id: row2.id.clone(),
                    operation: ReplicationOperationType::Upsert,
                    payload: serde_json::to_vec(&row2).unwrap(),
                    schema_version: 0,
                    collection_meta: Some(source_meta.clone()),
                }],
            );
            target
                .apply_range_commit_frame_as_local(&range, &frame)
                .unwrap();
            assert_eq!(
                target.native_collection_birth_id("items").unwrap(),
                local_birth
            );
            target
                .restore_migrated_collection_contract(&source_meta)
                .unwrap();
            assert_eq!(
                target.native_collection_birth_id("items").unwrap(),
                local_birth
            );
            target.close().unwrap();
            for _ in 0..2 {
                let target = BicDb::open_with_config(target_root.path(), cfg.clone()).unwrap();
                assert_eq!(
                    target.native_collection_birth_id("items").unwrap(),
                    local_birth
                );
                for record in [&row, &row2] {
                    assert_eq!(
                        target
                            .get("items", &record.id)
                            .unwrap()
                            .unwrap()
                            .content_hash()
                            .unwrap(),
                        record.content_hash().unwrap()
                    );
                }
                if target_kind == 1 {
                    assert!(target.get("items", "local_ACK").unwrap().is_some());
                }
                target.close().unwrap();
            }
        }
    }
}

#[test]
fn native_birth_flag_only_restart_is_future_only_and_damaged_catalogs_refuse() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        db.create_collection("legacy").unwrap();
        db.activate_local_namespace_identity_v1().unwrap();
        // The durable format fence can precede a successful API return. No
        // pre-existing collection may acquire a birth on the next open.
        format::enable_native_collection_birth(root.path()).unwrap();
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
        db.activate_native_collection_birth_v1().unwrap();
        assert_eq!(db.native_collection_birth_id("legacy").unwrap(), None);
        db.create_collection("born").unwrap();
        let actual = meta(&db, "born");
        assert!(actual.native_backing_birth_id.is_some());
        assert!(validate_catalog(&[actual.clone()], false, true).is_err());
        assert!(validate_catalog(&[actual.clone()], true, false).is_err());
        let mut damaged = actual.clone();
        damaged.native_backing_birth_id = Some(Uuid::nil());
        assert!(validate_catalog(&[damaged], true, true).is_err());
        let mut duplicate = actual.clone();
        duplicate.name = "different".into();
        duplicate.local_namespace_id = Some(Uuid::new_v4());
        assert!(validate_catalog(&[actual.clone(), duplicate], true, true).is_err());
        let mut unbound = actual;
        unbound.local_namespace_id = None;
        assert!(validate_catalog(&[unbound], true, true).is_err());
    }
}
