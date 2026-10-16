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
