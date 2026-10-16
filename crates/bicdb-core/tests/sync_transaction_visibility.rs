//! Mesh projections must be visible to transactional readers and preserve
//! existing snapshots without manufacturing a new local author.
use bicdb_core::{BicDb, DbConfig, Record, StorageMode, SyncCheckpoint};
use serde_json::json;

#[test]
fn imported_updates_and_deletes_publish_mvcc_versions_and_survive_restart() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let directory = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(true)
            .with_audit_events(true)
            .with_storage_mode(mode);
        let mut source =
            BicDb::open_with_config(directory.path().join("source"), config.clone()).unwrap();
        let mut target =
            BicDb::open_with_config(directory.path().join("target"), config.clone()).unwrap();
        for db in [&mut source, &mut target] {
            db.create_collection("items").unwrap();
            db.set_collection_mesh_sync_enabled("items", true).unwrap();
        }
        target
            .pin_node_key(&source.node_id(), &source.mesh_verifying_key().unwrap())
            .unwrap();
        source
            .insert(
                "items",
                Record::new("one").with_metadata(json!({"value":1})),
            )
            .unwrap();
        target
            .import_sync_bundle(
                source
                    .export_sync_bundle_since(SyncCheckpoint::default())
                    .unwrap(),
            )
            .unwrap();
        let before = target.begin_transaction().unwrap();
        assert_eq!(
            before.get("items", "one").unwrap().unwrap().metadata,
            json!({"value":1})
        );
        source
            .insert(
                "items",
                Record::new("one").with_metadata(json!({"value":2})),
            )
            .unwrap();
        target
            .import_sync_bundle(
                source
                    .export_sync_bundle_since(SyncCheckpoint::default())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            before.get("items", "one").unwrap().unwrap().metadata,
            json!({"value":1})
        );
        let after = target.begin_transaction().unwrap();
        assert_eq!(
            after.get("items", "one").unwrap().unwrap().metadata,
            json!({"value":2})
        );
        source.delete("items", "one").unwrap();
        target
            .import_sync_bundle(
                source
                    .export_sync_bundle_since(SyncCheckpoint::default())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            after.get("items", "one").unwrap().unwrap().metadata,
            json!({"value":2})
        );
        assert!(target
            .begin_transaction()
            .unwrap()
            .get("items", "one")
            .unwrap()
            .is_none());
        assert_eq!(
            target.sync_vector().unwrap(),
            source.sync_vector().unwrap(),
            "projection must retain origin signatures without adding local authorship"
        );
        drop(before);
        drop(after);
        drop(target);
        let target = BicDb::open_with_config(directory.path().join("target"), config).unwrap();
        assert!(target
            .begin_transaction()
            .unwrap()
            .get("items", "one")
            .unwrap()
            .is_none());
    }
}

#[test]
fn changing_collection_sync_authorization_invalidates_cached_coverage() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("items").unwrap();
    db.insert("items", Record::new("one")).unwrap();
    assert!(db.sync_vector().unwrap().origins.is_empty());
    db.set_collection_mesh_sync_enabled("items", true).unwrap();
    assert!(!db.sync_vector().unwrap().origins.is_empty());
    db.set_collection_mesh_sync_enabled("items", false).unwrap();
    assert!(db.sync_vector().unwrap().origins.is_empty());
}

#[test]
fn bounded_deltas_resume_after_each_durable_prefix_without_skipping_a_tail() {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true);
    let mut source =
        BicDb::open_with_config(directory.path().join("source"), config.clone()).unwrap();
    let mut target = BicDb::open_with_config(directory.path().join("target"), config).unwrap();
    for db in [&mut source, &mut target] {
        db.create_collection("items").unwrap();
        db.set_collection_mesh_sync_enabled("items", true).unwrap();
    }
    target
        .pin_node_key(&source.node_id(), &source.mesh_verifying_key().unwrap())
        .unwrap();
    for id in 0..7 {
        source
            .insert(
                "items",
                Record::new(id.to_string()).with_metadata(json!({"value":id})),
            )
            .unwrap();
    }
    let mut received = 0;
    loop {
        let vector = target.sync_vector().unwrap();
        let bundle = source
            .export_sync_bundle_delta_bounded(&vector, 2, 100_000)
            .unwrap();
        assert_eq!(bundle.next_checkpoint, SyncCheckpoint::default());
        if bundle.event_count == 0 {
            break;
        }
        assert!(bundle.event_count <= 2);
        received += bundle.event_count;
        target.import_sync_bundle(bundle.clone()).unwrap();
        target.import_sync_bundle(bundle).unwrap();
        assert_eq!(target.scan_collection("items").unwrap().len(), received);
    }
    assert_eq!(received, 7);
    assert_eq!(source.sync_vector().unwrap(), target.sync_vector().unwrap());
    assert!(source
        .export_sync_bundle_delta_bounded(&Default::default(), 2, 1)
        .is_err());
}
