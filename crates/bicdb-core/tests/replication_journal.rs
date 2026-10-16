use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, ReplicationConfig,
    ReplicationCursor, ReplicationMode, ReplicationTlsConfig, StorageMode, SyncCheckpoint,
    SyncVector,
};
use serde_json::json;

fn config(mode: StorageMode, role: ReplicationMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_audit_events(true)
        .with_storage_mode(mode)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: role,
            cluster_id: "journal-test".into(),
            listen_addr: Some("127.0.0.1:0".into()),
            record_journal: true,
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: false,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        })
}
fn ship(source: &BicDb, target: &mut BicDb, cursor: &mut ReplicationCursor) {
    let frames = source.replication_frames_after(cursor, 1000).unwrap();
    assert!(!frames.is_empty());
    for frame in &frames {
        assert_eq!(frame.protocol_version, 2);
        frame.verify_checksum().unwrap();
    }
    target.apply_replication_batch(&frames).unwrap();
}

#[test]
fn original_signatures_and_journal_only_commits_survive_native_replication_and_restart() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open_with_config(
            root.path().join("source"),
            config(mode.clone(), ReplicationMode::Primary),
        )
        .unwrap();
        let mut standby = BicDb::open_with_config(
            root.path().join("standby"),
            config(mode.clone(), ReplicationMode::Standby),
        )
        .unwrap();
        source.create_collection("items").unwrap();
        source
            .set_collection_mesh_sync_enabled("items", true)
            .unwrap();
        source
            .insert(
                "items",
                Record::new("one").with_metadata(json!({"value":1})),
            )
            .unwrap();
        let original = source
            .export_sync_bundle_delta(&SyncVector::default())
            .unwrap();
        assert!(original
            .events
            .iter()
            .all(|entry| entry.envelope.signature.is_some()));
        let mut cursor = ReplicationCursor::new(0);
        ship(&source, &mut standby, &mut cursor);
        let copied = standby
            .export_sync_bundle_delta(&SyncVector::default())
            .unwrap();
        assert_eq!(
            original
                .events
                .iter()
                .map(|entry| &entry.envelope)
                .collect::<Vec<_>>(),
            copied
                .events
                .iter()
                .map(|entry| &entry.envelope)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            source.sync_vector().unwrap(),
            standby.sync_vector().unwrap()
        );
        // An edge created and then deleted a row absent from this database.
        // Import must retain both original entries even with no row projection.
        let mut edge = BicDb::open_with_config(
            root.path().join("edge"),
            DbConfig::default().with_fsync(true).with_audit_events(true),
        )
        .unwrap();
        edge.create_collection("items").unwrap();
        edge.set_collection_mesh_sync_enabled("items", true)
            .unwrap();
        edge.insert("items", Record::new("gone")).unwrap();
        edge.delete("items", "gone").unwrap();
        source
            .pin_node_key(&edge.node_id(), &edge.mesh_verifying_key().unwrap())
            .unwrap();
        let bundle = edge.export_sync_bundle_delta(&Default::default()).unwrap();
        let before = source.current_commit_seq();
        source.import_sync_bundle_strict(bundle.clone()).unwrap();
        let suffix = source
            .export_replication_frames_since(before, 1000)
            .unwrap();
        assert_eq!(suffix.len(), 1);
        assert!(suffix[0].writes.is_empty());
        assert_eq!(suffix[0].journal.len(), 2);
        ship(&source, &mut standby, &mut cursor);
        assert!(standby.get("items", "gone").unwrap().is_none());
        let expected = source
            .export_sync_bundle_delta(&Default::default())
            .unwrap();
        drop(standby);
        let mut standby = BicDb::open_with_config(
            root.path().join("standby"),
            config(mode, ReplicationMode::Standby),
        )
        .unwrap();
        let received = standby
            .export_sync_bundle_delta(&Default::default())
            .unwrap();
        assert_eq!(
            expected
                .events
                .iter()
                .map(|entry| &entry.envelope)
                .collect::<Vec<_>>(),
            received
                .events
                .iter()
                .map(|entry| &entry.envelope)
                .collect::<Vec<_>>()
        );
        let mut receiver = BicDb::open_with_config(
            root.path().join("receiver"),
            DbConfig::default().with_fsync(true).with_audit_events(true),
        )
        .unwrap();
        receiver.create_collection("items").unwrap();
        receiver
            .set_collection_mesh_sync_enabled("items", true)
            .unwrap();
        receiver
            .pin_node_key(&source.node_id(), &source.mesh_verifying_key().unwrap())
            .unwrap();
        receiver
            .pin_node_key(&edge.node_id(), &edge.mesh_verifying_key().unwrap())
            .unwrap();
        receiver
            .import_sync_bundle_strict(received.clone())
            .unwrap();
        assert_eq!(
            receiver.get("items", "one").unwrap().unwrap().metadata,
            json!({"value":1})
        );
        assert!(receiver.get("items", "gone").unwrap().is_none());
        edge.insert("items", Record::new("later")).unwrap();
        edge.delete("items", "later").unwrap();
        standby
            .pin_node_key(&edge.node_id(), &edge.mesh_verifying_key().unwrap())
            .unwrap();
        let vector = standby.sync_vector().unwrap();
        let unwritten = edge.export_sync_bundle_delta(&vector).unwrap();
        assert_eq!(unwritten.event_count, 2);
        assert!(
            standby.import_sync_bundle_strict(unwritten).is_err(),
            "a journal-only import still needs writer authority"
        );
        assert_eq!(standby.sync_vector().unwrap(), vector);
        let mut corrupt = suffix[0].clone();
        corrupt.journal[0].payload = json!({"tampered":true});
        assert!(corrupt.verify_checksum().is_err());
        corrupt.checksum = corrupt.calculate_checksum();
        assert!(
            standby.apply_replication_frame(&corrupt).is_err(),
            "an invalid original envelope cannot hide in a transport-valid frame"
        );
        corrupt.protocol_version = 1;
        corrupt.checksum = corrupt.calculate_checksum();
        assert!(
            corrupt.verify_checksum().is_err(),
            "legacy checksums must not ignore journal fields"
        );
        let repeated = receiver.import_sync_bundle_strict(received).unwrap();
        assert_eq!(repeated.imported_events, 0);
    }
}

#[test]
fn an_invalid_projection_commits_neither_a_row_prefix_nor_its_origin_history() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let base = DbConfig::default()
            .with_fsync(true)
            .with_audit_events(true)
            .with_storage_mode(mode);
        let mut source = BicDb::open_with_config(root.path().join("source"), base.clone()).unwrap();
        let mut target = BicDb::open_with_config(root.path().join("target"), base.clone()).unwrap();
        for db in [&mut source, &mut target] {
            db.create_collection("items").unwrap();
            db.set_collection_mesh_sync_enabled("items", true).unwrap();
        }
        target
            .pin_node_key(&source.node_id(), &source.mesh_verifying_key().unwrap())
            .unwrap();
        target
            .create_index(IndexDefinition {
                name: "unique_value".into(),
                collection: "items".into(),
                fields: vec![IndexField::MetadataPath(vec!["value".into()])],
                unique: true,
                kind: IndexKind::BTree,
                predicate: None,
                exclusion: None,
            })
            .unwrap();
        for id in ["one", "two"] {
            source
                .insert("items", Record::new(id).with_metadata(json!({"value":1})))
                .unwrap();
        }
        let bundle = source
            .export_sync_bundle_since(SyncCheckpoint::default())
            .unwrap();
        let before = target.sync_vector().unwrap();
        assert!(target.import_sync_bundle_strict(bundle.clone()).is_err());
        assert!(target.scan_collection("items").unwrap().is_empty());
        assert_eq!(target.sync_vector().unwrap(), before);
        drop(target);
        let mut target = BicDb::open_with_config(root.path().join("target"), base).unwrap();
        assert!(target.scan_collection("items").unwrap().is_empty());
        assert_eq!(target.sync_vector().unwrap(), before);
        target.drop_index("unique_value").unwrap();
        let report = target.import_sync_bundle_strict(bundle).unwrap();
        assert_eq!(report.imported_events, 2);
        assert_eq!(target.scan_collection("items").unwrap().len(), 2);
    }
}
