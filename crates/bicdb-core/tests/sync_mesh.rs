use bicdb_core::{
    BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, IndexValue, NodeId, Record,
    SyncBundle, SyncCheckpoint, RECORD_AUDIT_STREAM,
};
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(false)
        .with_require_signed_imports(false)
        .with_unsafe_legacy_mesh_collections(true)
}

#[test]
fn node_id_is_persisted_and_status_tracks_pending_changes() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_node_id(temp.path(), sync_config(), node(1)).unwrap();
        assert_eq!(db.node_id(), node(1));
        db.create_collection("patients").unwrap();
        db.insert("patients", Record::new("p-1")).unwrap();

        let pending = db.pending_changes();
        assert_eq!(pending.node_id, node(1));
        assert_eq!(pending.pending_events, 1);
        assert_eq!(db.sync_status().total_events, 1);
        db.close().unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    assert_eq!(db.node_id(), node(1));
}

#[test]
fn sync_bundle_round_trips_and_verifies_checksums() {
    let temp = tempfile::tempdir().unwrap();
    let bundle_path = temp.path().join("left.syncbundle");
    let mut db = BicDb::open_with_node_id(temp.path().join("db"), sync_config(), node(1)).unwrap();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();

    let report = db
        .sync()
        .export_to_path(SyncCheckpoint::default(), &bundle_path)
        .unwrap();
    assert_eq!(report.event_count, 1);
    assert!(db.pending_changes().pending_events == 0);

    let bundle = SyncBundle::read(&bundle_path).unwrap();
    assert_eq!(bundle.event_count, 1);
    assert_eq!(bundle.source_node_id, node(1));
    assert!(bundle.verify().is_ok());
}

#[test]
fn sync_import_updates_secondary_indexes() {
    let source_temp = tempfile::tempdir().unwrap();
    let target_temp = tempfile::tempdir().unwrap();

    let mut source = BicDb::open_with_node_id(source_temp.path(), sync_config(), node(1)).unwrap();
    let mut target = BicDb::open_with_node_id(target_temp.path(), sync_config(), node(2)).unwrap();
    source.create_collection("patients").unwrap();
    target.create_collection("patients").unwrap();
    target
        .create_index(IndexDefinition {
            name: "idx_patients_clinic".to_string(),
            collection: "patients".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["clinic".to_string()])],
            unique: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();

    source
        .insert(
            "patients",
            Record::new("p1").with_metadata(json!({"clinic": "rural-7"})),
        )
        .unwrap();
    let bundle = source
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    target.import_sync_bundle(bundle).unwrap();

    assert_eq!(
        target
            .lookup_index("idx_patients_clinic", &[IndexValue::from("rural-7")])
            .unwrap(),
        vec!["p1".to_string()]
    );
    assert!(target.verify_index("idx_patients_clinic").unwrap().valid);
}

#[test]
fn diverged_databases_exchange_bundles_and_converge_without_data_loss() {
    let left_temp = tempfile::tempdir().unwrap();
    let right_temp = tempfile::tempdir().unwrap();
    let bundle_temp = tempfile::tempdir().unwrap();
    let left_bundle = bundle_temp.path().join("left.syncbundle");
    let right_bundle = bundle_temp.path().join("right.syncbundle");

    let mut left = BicDb::open_with_node_id(left_temp.path(), sync_config(), node(1)).unwrap();
    let mut right = BicDb::open_with_node_id(right_temp.path(), sync_config(), node(2)).unwrap();

    for db in [&mut left, &mut right] {
        db.create_collection("patients").unwrap();
        db.create_collection("vectors").unwrap();
        db.create_timeseries_collection("wearable").unwrap();
    }

    left.insert(
        "patients",
        Record::new("shared-patient")
            .with_timestamp(100)
            .with_metadata(json!({"owner": "left", "clinic": "rural-1"})),
    )
    .unwrap();
    left.insert(
        "vectors",
        Record::new("left-vector")
            .with_vector(vec![1.0, 0.0])
            .with_metadata(json!({"kind": "left-memory"})),
    )
    .unwrap();
    left.insert(
        "wearable",
        Record::new("left-wearable")
            .with_timestamp(1_710_000_000)
            .with_metadata(json!({"device_id": "band-left", "metric": "hrv", "value": 52.0})),
    )
    .unwrap();

    right
        .events_mut()
        .append(
            bicdb_core::Event::new(
                RECORD_AUDIT_STREAM,
                "RecordUpdated",
                json!({
                    "collection": "patients",
                    "collection_mode": "standard",
                    "record_id": "shared-patient",
                    "record": Record::new("shared-patient")
                        .with_timestamp(200)
                        .with_metadata(json!({"owner": "right", "clinic": "clinic-9"})),
                }),
            )
            .with_timestamp(9_999_999_999),
        )
        .unwrap();
    let right_self_bundle = right
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    right.import_sync_bundle(right_self_bundle).unwrap();
    right
        .insert(
            "vectors",
            Record::new("right-vector")
                .with_vector(vec![0.0, 1.0])
                .with_metadata(json!({"kind": "right-memory"})),
        )
        .unwrap();
    right
        .insert(
            "wearable",
            Record::new("right-wearable")
                .with_timestamp(1_710_000_100)
                .with_metadata(json!({"device_id": "band-right", "metric": "hrv", "value": 64.0})),
        )
        .unwrap();

    left.sync()
        .export_to_path(SyncCheckpoint::default(), &left_bundle)
        .unwrap();
    right.import_sync_bundle_file(&left_bundle).unwrap();
    right
        .sync()
        .export_to_path(SyncCheckpoint::default(), &right_bundle)
        .unwrap();
    let import_report = left.import_sync_bundle_file(&right_bundle).unwrap();

    assert!(import_report.imported_events >= 3);
    assert!(import_report.conflicts_resolved >= 1);

    let left_patient = left
        .get("patients", "shared-patient")
        .unwrap()
        .expect("left patient");
    let right_patient = right
        .get("patients", "shared-patient")
        .unwrap()
        .expect("right patient");
    assert_eq!(left_patient, right_patient);
    assert_eq!(left_patient.metadata["owner"], "right");
    assert_eq!(left_patient.metadata["clinic"], "clinic-9");

    assert!(left.get("vectors", "left-vector").unwrap().is_some());
    assert!(left.get("vectors", "right-vector").unwrap().is_some());
    assert!(right.get("vectors", "left-vector").unwrap().is_some());
    assert!(right.get("vectors", "right-vector").unwrap().is_some());
    assert_eq!(
        left.search_vector("vectors", &[1.0, 0.0], 2, None)
            .unwrap()
            .first()
            .unwrap()
            .record
            .id,
        "left-vector"
    );

    assert!(left.get("wearable", "left-wearable").unwrap().is_some());
    assert!(left.get("wearable", "right-wearable").unwrap().is_some());
    assert_eq!(
        left.scan_time_range("wearable", 1_710_000_000, 1_710_000_200)
            .unwrap()
            .len(),
        2
    );

    let left_audit_ids = left
        .events()
        .read(RECORD_AUDIT_STREAM)
        .into_iter()
        .map(|event| event.event.id)
        .collect::<std::collections::BTreeSet<_>>();
    let right_audit_ids = right
        .events()
        .read(RECORD_AUDIT_STREAM)
        .into_iter()
        .map(|event| event.event.id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(left_audit_ids, right_audit_ids);
}
