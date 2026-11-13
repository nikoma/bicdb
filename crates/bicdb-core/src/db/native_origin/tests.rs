use super::*;
use crate::{DistributionConfig, DistributionStore, ReplicationMode, ReplicationTlsConfig};
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
fn enable_birth(db: &mut BicDb) {
    db.activate_local_namespace_identity_v1().unwrap();
    db.activate_native_collection_birth_v1().unwrap();
    db.activate_native_wal_backing_capture_v1().unwrap();
}
fn meta(db: &BicDb, name: &str) -> CollectionMeta {
    db.collections()
        .into_iter()
        .find(|m| m.name == name)
        .unwrap()
}
fn hashes(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, path: &Path, out: &mut BTreeMap<String, String>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_string_lossy().into(),
                    hex::encode(sha2::Sha256::digest(fs::read(&path).unwrap())),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}
fn copy_tree(source: &Path, target: &Path) {
    fs::create_dir(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let dest = target.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).unwrap();
        }
    }
}

#[test]
fn native_origin_is_actual_future_allocation_scope_and_survives_two_opens() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("origin-control", 0);
        enable_birth(&mut db);
        db.create_collection("before").unwrap();
        db.insert("before", Record::new("old_ACK")).unwrap();
        let old_seq = db.last_commit_seq();
        let before = meta(&db, "before");
        db.activate_native_origin_root_v1().unwrap();
        let root = db.native_origin_root_id().unwrap().unwrap();
        db.activate_native_origin_root_v1().unwrap();
        assert_eq!(db.last_commit_seq(), old_seq);
        assert_eq!(meta(&db, "before"), before);
        assert_eq!(before.native_backing_origin_root, None);
        db.create_collection("after").unwrap();
        db.insert(
            "after",
            Record::new("ACK").with_metadata(json!({"value":67})),
        )
        .unwrap();
        let seq = db.last_commit_seq();
        let after = meta(&db, "after");
        assert_eq!(after.native_backing_origin_root, Some(root));
        assert_eq!(after.portable_contract().native_backing_origin_root, None);
        let ack = db
            .get("after", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        assert!(!format::required_reader_features(&path)
            .unwrap()
            .iter()
            .any(|f| f == format::NATIVE_ORIGIN_ROOT_FEATURE_FLAG));
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(db.native_origin_root_id().unwrap(), Some(root));
            assert_eq!(meta(&db, "before"), before);
            assert_eq!(meta(&db, "after"), after);
            for frame in [
                db.export_replication_frames_since(seq - 1, 1)
                    .unwrap()
                    .remove(0),
                db.replication_frames_after(&mut crate::ReplicationCursor::new(seq - 1), 1)
                    .unwrap()
                    .remove(0),
            ] {
                assert_eq!(
                    frame
                        .captured_native_origin_root()
                        .unwrap()
                        .unwrap()
                        .root_id(),
                    root
                );
                assert_eq!(
                    frame.captured_native_wal_backings().unwrap().unwrap()["after"]
                        .native_backing_origin_root,
                    Some(root)
                );
                let decoded: CommitFrame =
                    serde_json::from_slice(&serde_json::to_vec(&frame).unwrap()).unwrap();
                assert!(decoded.captured_native_origin_root().unwrap().is_none());
            }
            let old = db
                .export_replication_frames_since(old_seq - 1, 1)
                .unwrap()
                .remove(0);
            assert_eq!(
                old.captured_native_wal_backings().unwrap().unwrap()["before"]
                    .native_backing_origin_root,
                None
            );
            assert_eq!(
                db.get("after", "ACK")
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
fn native_origin_empty_existing_directory_is_not_a_fresh_factory() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let directory = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(directory.path(), config(mode)).unwrap();
        let before = hashes(directory.path());
        let error = db.activate_native_origin_root_v1().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("atomic directory creation proof"),
            "{error}"
        );
        assert_eq!(db.native_origin_root_id().unwrap(), None);
        assert_eq!(hashes(directory.path()), before);
    }
}

#[test]
fn native_origin_copied_store_is_refused_readonly_but_same_directory_move_preserves_root() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let source = parent.path().join("source");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&source, cfg.clone()).unwrap();
        db.activate_native_origin_root_v1().unwrap();
        enable_birth(&mut db);
        let root = db.native_origin_root_id().unwrap();
        db.create_collection("items").unwrap();
        db.insert(
            "items",
            Record::new("ACK").with_metadata(json!({"value":31})),
        )
        .unwrap();
        let ack = db
            .get("items", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        db.close().unwrap();
        let copied = parent.path().join("copied");
        copy_tree(&source, &copied);
        let before = hashes(&copied);
        let error = BicDb::open_with_config(&copied, cfg.clone()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("copied or replaced physical directory"),
            "{error}"
        );
        assert_eq!(hashes(&copied), before);
        let moved = parent.path().join("moved");
        fs::rename(&source, &moved).unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&moved, cfg.clone()).unwrap();
            assert_eq!(db.native_origin_root_id().unwrap(), root);
            assert_eq!(
                db.get("items", "ACK")
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
fn native_origin_untrusted_snapshot_metadata_never_adopts_or_retrofits_source_root() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let cfg = config(mode);
        let mut source =
            BicDb::open_with_config(parent.path().join("source"), cfg.clone()).unwrap();
        source.activate_native_origin_root_v1().unwrap();
        enable_birth(&mut source);
        source.create_collection("items").unwrap();
        let source_meta = meta(&source, "items");
        let topology = tempfile::tempdir().unwrap();
        let range = DistributionStore::initialize_at(
            topology.path(),
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
        .clone();
        for existing in [false, true] {
            let path = parent
                .path()
                .join(if existing { "existing" } else { "new" });
            let mut target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            enable_birth(&mut target);
            if existing {
                target.create_collection("items").unwrap();
            }
            target.activate_native_origin_root_v1().unwrap();
            let local = target.native_origin_root_id().unwrap();
            let row = Record::new("ACK").with_metadata(json!({"value":43}));
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
            let expected = if existing { None } else { local };
            assert_eq!(meta(&target, "items").native_backing_origin_root, expected);
            assert_ne!(expected, source_meta.native_backing_origin_root);
            target
                .restore_migrated_collection_contract(&source_meta)
                .unwrap();
            assert_eq!(meta(&target, "items").native_backing_origin_root, expected);
            target.close().unwrap();
            for _ in 0..2 {
                let target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
                assert_eq!(meta(&target, "items").native_backing_origin_root, expected);
                assert_eq!(
                    target
                        .get("items", "ACK")
                        .unwrap()
                        .unwrap()
                        .content_hash()
                        .unwrap(),
                    row.content_hash().unwrap()
                );
                target.close().unwrap();
            }
        }
    }
}

#[test]
fn native_origin_protected_image_binds_actual_root_and_rejects_live_directory_replacement() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("db");
    let mut db = BicDb::open_with_config(&path, config(StorageMode::ServerPaged)).unwrap();
    enable_birth(&mut db);
    let topology = tempfile::tempdir().unwrap();
    let range = DistributionStore::initialize_at(
        topology.path(),
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
    .clone();
    let row = Record::new("ACK").with_metadata(json!({"value":71}));
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
    db.apply_range_snapshot_batch(&range, &CollectionMeta::standard("items"), &batch)
        .unwrap();
    format::enable_range_image_install(&path).unwrap();
    db.schema_compatibility_fingerprint().unwrap();
    let before = db.capture_range_image_bindings().unwrap();
    assert!(before.native_origin().is_none());
    db.activate_native_origin_root_v1().unwrap();
    let root = db.native_origin_root_id().unwrap().unwrap();
    assert!(db.validate_range_image_bindings(&before).is_err());
    let current = db.capture_range_image_bindings().unwrap();
    assert_eq!(current.native_origin().unwrap().root_id(), root);
    assert_eq!(
        meta(&db, "items").native_backing_origin_root,
        None,
        "origin observation must not retroactively scope old backing"
    );
    let held = parent.path().join("held");
    fs::rename(&path, &held).unwrap();
    fs::create_dir(&path).unwrap();
    for name in [format::FORMAT_METADATA_FILE, ROOT_FILE] {
        fs::copy(held.join(name), path.join(name)).unwrap();
    }
    let foreign = hashes(&path);
    assert!(db.validate_range_image_bindings(&current).is_err());
    assert!(db.create_collection("must_refuse").is_err());
    assert_eq!(hashes(&path), foreign);
    fs::remove_dir_all(&path).unwrap();
    fs::rename(&held, &path).unwrap();
    db.validate_range_image_bindings(&current).unwrap();
    assert_eq!(
        db.get("items", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap(),
        row.content_hash().unwrap()
    );
}

#[test]
fn native_origin_retained_wal_requires_root_after_scoped_backing_was_dropped() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.sync_quorum().hold("origin-history-control", 0);
        db.activate_native_origin_root_v1().unwrap();
        enable_birth(&mut db);
        let root = db.native_origin_root_id().unwrap();
        db.create_collection("scoped").unwrap();
        db.insert("scoped", Record::new("ACK")).unwrap();
        db.drop_collection("scoped").unwrap();
        db.close().unwrap();
        let file = format::metadata_path(&path);
        let original = fs::read(&file).unwrap();
        let mut metadata: Value = serde_json::from_slice(&original).unwrap();
        metadata["feature_flags"]
            .as_array_mut()
            .unwrap()
            .retain(|f| f.as_str() != Some(format::NATIVE_ORIGIN_ROOT_FEATURE_FLAG));
        fs::write(&file, serde_json::to_vec_pretty(&metadata).unwrap()).unwrap();
        let error = BicDb::open_with_config(&path, cfg.clone()).unwrap_err();
        assert!(
            error.to_string().contains("recorded WAL allocation roots"),
            "{error}"
        );
        fs::write(file, original).unwrap();
        let db = BicDb::open_with_config(&path, cfg).unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), root);
    }
}

#[test]
fn native_origin_root_publication_eio_fences_then_recovers_actual_prepared_root() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        enable_birth(&mut db);
        db.create_collection("before").unwrap();
        db.insert(
            "before",
            Record::new("ACK").with_metadata(json!({"value":89})),
        )
        .unwrap();
        let ack = db
            .get("before", "ACK")
            .unwrap()
            .unwrap()
            .content_hash()
            .unwrap();
        let fault =
            bicdb_page::test_faults::arm_io_failure("native_origin.root.dir_sync", &path, 1, false);
        assert!(
            db.activate_native_origin_root_v1().is_err(),
            "actual publication EIO was not injected"
        );
        drop(fault);
        assert!(matches!(
            db.create_collection("must_refuse"),
            Err(BicDbError::WriteFailed { .. })
        ));
        let prepared = read_record(&path).unwrap().unwrap().root_id;
        drop(db);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), None);
        db.activate_native_origin_root_v1().unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), Some(prepared));
        assert_eq!(meta(&db, "before").native_backing_origin_root, None);
        db.close().unwrap();
        for _ in 0..2 {
            let db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(db.native_origin_root_id().unwrap(), Some(prepared));
            assert_eq!(
                db.get("before", "ACK")
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
fn native_origin_durable_prepared_factory_can_finish_flag_fence_without_rebinding() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("db");
        let cfg = config(mode);
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        db.activate_native_origin_root_v1().unwrap();
        let root = db.native_origin_root_id().unwrap();
        db.close().unwrap();
        let file = format::metadata_path(&path);
        let mut metadata: Value = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        metadata["feature_flags"]
            .as_array_mut()
            .unwrap()
            .retain(|f| f.as_str() != Some(format::NATIVE_ORIGIN_ROOT_FEATURE_FLAG));
        fs::write(file, serde_json::to_vec_pretty(&metadata).unwrap()).unwrap();
        let mut db = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        assert!(!db.fresh_directory_created);
        assert_eq!(db.native_origin_root_id().unwrap(), None);
        db.activate_native_origin_root_v1().unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), root);
        db.close().unwrap();
        let db = BicDb::open_with_config(&path, cfg).unwrap();
        assert_eq!(db.native_origin_root_id().unwrap(), root);
    }
}
