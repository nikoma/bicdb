use super::*;
use crate::{ReplicationMode, ReplicationTlsConfig};
use serde_json::json;

fn config(mode: StorageMode, role: ReplicationMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: role,
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
fn ship(source: &BicDb, target: &mut BicDb) {
    target
        .apply_replication_batch(
            &source
                .export_replication_frames_since(target.last_applied_commit_seq(), usize::MAX)
                .unwrap(),
        )
        .unwrap();
}

#[test]
fn actual_export_import_allocates_new_target_and_retains_origin_through_rename_and_two_opens() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("target");
        let cfg = config(mode.clone(), ReplicationMode::Standby);
        let mut source = BicDb::open_with_config(
            root.path().join("source"),
            config(mode, ReplicationMode::Primary),
        )
        .unwrap();
        source.sync_quorum().hold("origin-history", 0);
        source.persist_standby_slots().unwrap();
        assert!(source
            .initialize_fresh_primary_replication_origins()
            .unwrap());
        source.create_collection("items").unwrap();
        let first = meta(&source, "items");
        let row = Record::new("ACK").with_metadata(json!({"value":97}));
        source.insert("items", row.clone()).unwrap();
        let mut target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
        assert!(target.native_origin_root_id().unwrap().is_none());
        ship(&source, &mut target);
        let local = meta(&target, "items");
        let origin = local.native_imported_backing_origin.as_ref().unwrap();
        assert!(!origin.pending);
        assert!(origin
            .same_source(
                &source.observe_native_origin_root().unwrap().unwrap(),
                &first
            )
            .unwrap());
        assert_ne!(local.native_backing_birth_id, first.native_backing_birth_id);
        assert_ne!(local.local_namespace_id, first.local_namespace_id);
        assert_ne!(
            local.native_backing_origin_root,
            first.native_backing_origin_root
        );
        assert!(
            !target.native_catalog_allocation_logging_active(),
            "import must preserve standby role"
        );
        assert!(target.is_replication_standby());
        source.rename_collection("items", "kept").unwrap();
        source.create_timeseries_collection("items").unwrap();
        let second = meta(&source, "items");
        ship(&source, &mut target);
        let replacement = meta(&target, "items");
        let moved = meta(&target, "kept");
        assert_eq!(
            moved.native_imported_backing_origin,
            local.native_imported_backing_origin
        );
        assert_ne!(
            replacement
                .native_imported_backing_origin
                .as_ref()
                .unwrap()
                .generation,
            origin.generation
        );
        assert_eq!(
            replacement
                .native_imported_backing_origin
                .as_ref()
                .unwrap()
                .source_birth,
            second.native_backing_birth_id.unwrap()
        );
        assert_eq!(replacement.mode, CollectionMode::TimeSeries);
        target.close().unwrap();
        for _ in 0..2 {
            let target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            assert_eq!(meta(&target, "kept"), moved);
            assert_eq!(meta(&target, "items"), replacement);
            assert_eq!(
                target
                    .get("kept", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            assert!(target.get("items", "ACK").unwrap().is_none());
            target.close().unwrap();
        }
    }
}

#[test]
fn json_or_modified_frames_never_mint_target_provenance_and_duplicate_does_not_retrofit() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open_with_config(
            root.path().join("source"),
            config(mode.clone(), ReplicationMode::Primary),
        )
        .unwrap();
        source.sync_quorum().hold("origin-history", 0);
        assert!(source
            .initialize_fresh_primary_replication_origins()
            .unwrap());
        source.create_collection("items").unwrap();
        source.insert("items", Record::new("ACK")).unwrap();
        let frames = source
            .export_replication_frames_since(0, usize::MAX)
            .unwrap();
        let decoded: Vec<CommitFrame> =
            serde_json::from_slice(&serde_json::to_vec(&frames).unwrap()).unwrap();
        let mut target = BicDb::open_with_config(
            root.path().join("target"),
            config(mode.clone(), ReplicationMode::Standby),
        )
        .unwrap();
        target.apply_replication_batch(&decoded).unwrap();
        let local = meta(&target, "items");
        assert!(local.native_imported_backing_origin.is_none());
        target.apply_replication_batch(&frames).unwrap();
        assert_eq!(meta(&target, "items"), local);
        assert!(target.native_origin_root_id().unwrap().is_none());
        let mut changed = frames[0].clone();
        changed.timestamp += 1;
        changed.checksum = changed.calculate_checksum();
        let path = root.path().join("other");
        let mut other =
            BicDb::open_with_config(&path, config(mode, ReplicationMode::Standby)).unwrap();
        let catalog = other.collections();
        let format = fs::read(format::metadata_path(&path)).unwrap();
        assert!(other.apply_replication_frame(&changed).is_err());
        assert_eq!(other.collections(), catalog);
        assert_eq!(fs::read(format::metadata_path(&path)).unwrap(), format);
        assert_eq!(other.last_applied_commit_seq(), 0);
    }
}

#[test]
fn actual_import_eio_recovers_or_retries_only_the_same_actual_target_generation() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (site, hit) in [
            ("transaction_log.write", 1),
            ("transaction_log.sync", 1),
            ("catalog.dir_sync", 2),
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("target");
            let cfg = config(mode.clone(), ReplicationMode::Standby);
            let mut source = BicDb::open_with_config(
                root.path().join("source"),
                config(mode.clone(), ReplicationMode::Primary),
            )
            .unwrap();
            source.sync_quorum().hold("origin-history", 0);
            source
                .initialize_fresh_primary_replication_origins()
                .unwrap();
            source.create_collection("independent").unwrap();
            let row = Record::new("ACK").with_metadata(json!({"value":83}));
            source.insert("independent", row.clone()).unwrap();
            let mut target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            ship(&source, &mut target);
            target.sync_quorum().hold("origin-history", 0);
            target.persist_standby_slots().unwrap();
            source.create_collection("items").unwrap();
            let frames = source
                .export_replication_frames_since(target.last_applied_commit_seq(), usize::MAX)
                .unwrap();
            assert_eq!(frames.len(), 1);
            let fault = bicdb_page::test_faults::arm_io_failure(site, &path, hit, false);
            assert!(
                target.apply_replication_frame(&frames[0]).is_err(),
                "{mode:?} {site}"
            );
            drop(fault);
            let allocated = meta(&target, "items");
            let generation = allocated
                .native_imported_backing_origin
                .as_ref()
                .unwrap()
                .generation;
            assert!(target.apply_replication_frame(&frames[0]).is_err());
            drop(target);
            let mut target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
            target.apply_replication_batch(&frames).unwrap();
            let completed = meta(&target, "items");
            let origin = completed.native_imported_backing_origin.as_ref().unwrap();
            assert!(!origin.pending);
            assert_eq!(origin.generation, generation);
            assert_eq!(
                completed.native_backing_birth_id,
                allocated.native_backing_birth_id
            );
            assert_eq!(completed.local_namespace_id, allocated.local_namespace_id);
            assert_eq!(
                target
                    .get("independent", "ACK")
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                row.content_hash().unwrap()
            );
            target.close().unwrap();
            for _ in 0..2 {
                let target = BicDb::open_with_config(&path, cfg.clone()).unwrap();
                assert_eq!(meta(&target, "items"), completed);
                assert_eq!(
                    target
                        .get("independent", "ACK")
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
fn donated_metadata_and_existing_unowned_backing_are_never_adopted() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open_with_config(
            root.path().join("source"),
            config(mode.clone(), ReplicationMode::Primary),
        )
        .unwrap();
        source.sync_quorum().hold("origin-history", 0);
        source
            .initialize_fresh_primary_replication_origins()
            .unwrap();
        source.create_collection("placeholder").unwrap();
        source.create_collection("items").unwrap();
        let frames = source
            .export_replication_frames_since(0, usize::MAX)
            .unwrap();
        let mut target = BicDb::open_with_config(
            root.path().join("target"),
            config(mode.clone(), ReplicationMode::Standby),
        )
        .unwrap();
        ship(&source, &mut target);
        let owned = meta(&target, "items");
        assert!(owned
            .portable_contract()
            .native_imported_backing_origin
            .is_none());
        let mut fabricated = owned.clone();
        fabricated
            .native_imported_backing_origin
            .as_mut()
            .unwrap()
            .generation = Uuid::new_v4();
        target
            .promote_by_election(1, "origin-maintenance", None)
            .unwrap();
        drop(target);
        let mut target = BicDb::open_with_config(
            root.path().join("target"),
            config(mode.clone(), ReplicationMode::Primary),
        )
        .unwrap();
        target
            .restore_migrated_collection_contract(&fabricated)
            .unwrap();
        assert_eq!(meta(&target, "items"), owned);
        let mut other = BicDb::open_with_config(
            root.path().join("other"),
            config(mode, ReplicationMode::Standby),
        )
        .unwrap();
        assert_eq!(frames.len(), 2);
        let mut legacy: CommitFrame =
            serde_json::from_slice(&serde_json::to_vec(&frames[0]).unwrap()).unwrap();
        let mut record: Record = serde_json::from_slice(&legacy.writes[0].payload).unwrap();
        record.metadata["name"] = json!("items");
        legacy.writes[0].payload = serde_json::to_vec(&record).unwrap();
        legacy.checksum = legacy.calculate_checksum();
        other.apply_replication_frame(&legacy).unwrap();
        let before = meta(&other, "items");
        let before_format = fs::read(format::metadata_path(&other.path)).unwrap();
        let error = other.apply_replication_frame(&frames[1]).unwrap_err();
        assert!(error.to_string().contains("unowned"), "{error}");
        assert_eq!(meta(&other, "items"), before);
        assert_eq!(
            fs::read(format::metadata_path(&other.path)).unwrap(),
            before_format
        );
        assert_eq!(other.last_applied_commit_seq(), 1);
    }
}
