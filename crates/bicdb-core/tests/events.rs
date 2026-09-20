use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bicdb_core::{
    BicDb, DbConfig, DeviceView, Event, Geometry, IndexDefinition, IndexField, IndexKind, Record,
    SyncCheckpoint, RECORD_AUDIT_STREAM, SPATIAL_AUDIT_STREAM,
};
use serde_json::json;

fn open_temp() -> (tempfile::TempDir, BicDb) {
    let temp = tempfile::tempdir().expect("tempdir");
    let db = BicDb::open_with_config(temp.path(), DbConfig::default()).expect("open db");
    (temp, db)
}

#[test]
fn event_stream_appends_reads_replays_and_recovers() {
    let temp = tempfile::tempdir().unwrap();
    let first_offset;
    {
        let mut db = BicDb::open_with_config(temp.path(), DbConfig::default()).unwrap();
        first_offset = db
            .events_mut()
            .append(
                Event::new(
                    "patient-events",
                    "PatientCreated",
                    json!({"patient_id": "p-1", "name": "Asha"}),
                )
                .with_metadata(json!({"source": "test"}))
                .with_timestamp(10),
            )
            .unwrap();
        db.events_mut()
            .append(
                Event::new(
                    "patient-events",
                    "PatientUpdated",
                    json!({"patient_id": "p-1", "clinic": "rural-7"}),
                )
                .with_timestamp(20),
            )
            .unwrap();
        db.close().unwrap();
    }

    let db = BicDb::open(temp.path()).unwrap();
    let events = db.events().read("patient-events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].offset, first_offset);
    assert_eq!(events[0].event.event_type, "PatientCreated");
    assert_eq!(events[0].event.metadata["source"], "test");

    let since = db.events().read_since(first_offset);
    assert_eq!(since.len(), 2);

    let replay = db.events().replay_from("patient-events", events[1].offset);
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].event.event_type, "PatientUpdated");
}

#[test]
fn subscribers_receive_appended_events_and_can_replay_from_offset() {
    let (_temp, mut db) = open_temp();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_for_handler = Arc::clone(&seen);
    db.events_mut().subscribe("patient-events", move |event| {
        seen_for_handler
            .lock()
            .unwrap()
            .push(event.event.event_type.clone());
    });

    let (tx, rx) = mpsc::channel();
    db.events_mut()
        .subscribe_async("patient-events", move |event| {
            tx.send(event.event.event_type.clone()).unwrap();
        });

    let offset = db
        .events_mut()
        .append(Event::new(
            "patient-events",
            "PatientCreated",
            json!({"patient_id": "p-1"}),
        ))
        .unwrap();

    assert_eq!(seen.lock().unwrap().as_slice(), ["PatientCreated"]);
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "PatientCreated"
    );

    let replay = db.events().replay_from("patient-events", offset);
    assert_eq!(replay.len(), 1);
}

#[test]
fn projections_rebuild_queryable_state_from_events() {
    let (_temp, mut db) = open_temp();
    db.events_mut()
        .append(
            Event::new(
                "devices",
                "DeviceMeasurementReceived",
                json!({"device_id": "d-1", "metric": "hrv", "value": 52.5}),
            )
            .with_timestamp(40),
        )
        .unwrap();

    let devices: DeviceView = db.events().rebuild_projection("devices").unwrap();
    assert_eq!(devices.latest("d-1").unwrap()["metric"], "hrv");
}

#[test]
fn queue_publish_and_consume_are_backed_by_event_stream() {
    let (_temp, mut db) = open_temp();
    {
        let mut queue = db.queue("outbox");
        queue
            .publish(
                json!({"kind": "email", "to": "care@example.test"}),
                json!({}),
            )
            .unwrap();
        queue.publish(json!({"kind": "sms"}), json!({})).unwrap();
    }

    let messages = db.queue("outbox").consume(10).unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].event.stream, "queue:outbox");
    assert_eq!(messages[0].event.event_type, "QueueMessage");
    assert_eq!(messages[0].event.payload["kind"], "email");

    assert!(db.queue("outbox").consume(10).unwrap().is_empty());
    assert_eq!(db.events().read("queue:outbox").len(), 2);
}

#[test]
fn collection_audit_events_are_optional_and_capture_mutations() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("patients").unwrap();
    db.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha"})),
    )
    .unwrap();
    db.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"name": "Asha", "clinic": "rural-7"})),
    )
    .unwrap();
    assert!(db.delete("patients", "p-1").unwrap());

    let events = db.events().read(RECORD_AUDIT_STREAM);
    assert_eq!(
        events
            .iter()
            .map(|event| event.event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["RecordCreated", "RecordUpdated", "RecordDeleted"]
    );
    assert_eq!(events[0].event.payload["collection"], "patients");
    assert_eq!(events[0].event.payload["record_id"], "p-1");
}

#[test]
fn spatial_record_mutation_emits_spatial_event_when_enabled() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("places").unwrap();

    db.insert(
        "places",
        Record::new("clinic-1")
            .with_geometry(Geometry::point(-122.4194, 37.7749).unwrap())
            .with_metadata(json!({"name": "clinic"})),
    )
    .unwrap();

    let events = db.events().read(SPATIAL_AUDIT_STREAM);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.event_type, "SpatialRecordInserted");
    assert_eq!(events[0].event.payload["collection"], "places");
    assert_eq!(events[0].event.payload["record_id"], "clinic-1");
    assert_eq!(
        events[0].event.payload["spatial_fields"],
        json!(["geometry"])
    );
}

#[test]
fn spatial_index_create_and_rebuild_emit_spatial_index_events() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("places").unwrap();
    db.insert(
        "places",
        Record::new("clinic-1").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
    )
    .unwrap();

    db.create_index(IndexDefinition {
        name: "idx_places_geometry".to_string(),
        collection: "places".to_string(),
        fields: vec![IndexField::Geometry],
        unique: false,
        kind: IndexKind::Spatial,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.rebuild_index("idx_places_geometry").unwrap();

    let index_events = db
        .events()
        .read(SPATIAL_AUDIT_STREAM)
        .into_iter()
        .filter(|event| event.event.event_type == "SpatialIndexUpdated")
        .collect::<Vec<_>>();
    assert_eq!(index_events.len(), 2);
    assert_eq!(index_events[0].event.payload["operation"], "created");
    assert_eq!(index_events[1].event.payload["operation"], "rebuilt");
    assert_eq!(index_events[1].event.payload["indexed_records"], 1);
}

#[test]
fn route_computation_event_captures_metadata_without_full_payload() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        temp.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    insert_toy_roads(&mut db);

    let path = db
        .shortest_path_with_events(
            "roads",
            &Geometry::point(0.0, 0.0).unwrap(),
            &Geometry::point(0.002, 0.0).unwrap(),
        )
        .unwrap();
    assert_eq!(path.distance_m, 200.0);

    let event = db
        .events()
        .read(SPATIAL_AUDIT_STREAM)
        .into_iter()
        .find(|event| event.event.event_type == "RouteComputed")
        .expect("route event");
    assert_eq!(event.event.payload["operation"], "shortest_path");
    assert_eq!(event.event.payload["graph"], "roads");
    assert_eq!(event.event.payload["distance_m"], 200.0);
    assert_eq!(event.event.payload["node_count"], 3);
    assert!(event.event.payload.get("node_ids").is_none());
    assert!(event.event.payload.get("start").is_none());
    assert!(event.event.payload.get("end").is_none());
}

#[test]
fn sync_bundle_preserves_spatial_event_records() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let mut left = BicDb::open_with_config(
        left_dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    left.create_collection("places").unwrap();
    // Mesh replication is opt-in per collection: without this the export is
    // silently empty and the assertions below fail for a reason that has
    // nothing to do with spatial events.
    left.set_collection_mesh_sync_enabled("places", true)
        .unwrap();
    left.insert(
        "places",
        Record::new("clinic-1").with_geometry(Geometry::point(-122.4194, 37.7749).unwrap()),
    )
    .unwrap();
    left.create_spatial_index("places", "geometry").unwrap();

    let bundle = left
        .export_sync_bundle_since(SyncCheckpoint::default())
        .unwrap();
    assert!(bundle
        .events
        .iter()
        .any(|entry| entry.event.stream == SPATIAL_AUDIT_STREAM));

    let mut right =
        BicDb::open_with_config(right_dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    right.create_collection("places").unwrap();
    right
        .set_collection_mesh_sync_enabled("places", true)
        .unwrap();
    if let Some(key) = left.mesh_verifying_key() {
        right.pin_node_key(&left.node_id(), &key).unwrap();
    }
    right.import_sync_bundle(bundle).unwrap();
    let imported = right.events().read(SPATIAL_AUDIT_STREAM);
    assert!(imported
        .iter()
        .any(|event| event.event.event_type == "SpatialRecordInserted"));
    assert!(imported
        .iter()
        .any(|event| event.event.event_type == "SpatialIndexUpdated"));
}

#[test]
fn snapshot_at_rebuilds_record_state_from_audit_events() {
    let (_temp, mut db) = open_temp();
    db.events_mut()
        .append(
            Event::new(
                RECORD_AUDIT_STREAM,
                "RecordCreated",
                json!({
                    "collection": "patients",
                    "record_id": "p-1",
                    "record": Record::new("p-1").with_metadata(json!({"name": "Asha"})),
                }),
            )
            .with_timestamp(10),
        )
        .unwrap();
    db.events_mut()
        .append(
            Event::new(
                RECORD_AUDIT_STREAM,
                "RecordUpdated",
                json!({
                    "collection": "patients",
                    "record_id": "p-1",
                    "record": Record::new("p-1").with_metadata(json!({"name": "Asha", "clinic": "rural-7"})),
                }),
            )
            .with_timestamp(20),
        )
        .unwrap();
    db.events_mut()
        .append(
            Event::new(
                RECORD_AUDIT_STREAM,
                "RecordDeleted",
                json!({
                    "collection": "patients",
                    "record_id": "p-1",
                }),
            )
            .with_timestamp(30),
        )
        .unwrap();

    assert_eq!(
        db.snapshot_at(15)
            .unwrap()
            .get("patients", "p-1")
            .unwrap()
            .metadata["name"],
        "Asha"
    );
    assert_eq!(
        db.snapshot_at(25)
            .unwrap()
            .get("patients", "p-1")
            .unwrap()
            .metadata["clinic"],
        "rural-7"
    );
    assert!(db.snapshot_at(35).unwrap().get("patients", "p-1").is_none());
}

#[test]
fn event_transfer_imports_events_without_duplicates() {
    let (_left_temp, mut left) = open_temp();
    let (_right_temp, mut right) = open_temp();
    left.events_mut()
        .append(Event::new(
            "sync-source",
            "PatientCreated",
            json!({"patient_id": "p-1"}),
        ))
        .unwrap();

    let exported = left.export_events_since(0);
    assert_eq!(right.import_events(exported.clone()).unwrap(), 1);
    assert_eq!(right.events().read("sync-source").len(), 1);
    assert_eq!(right.import_events(exported).unwrap(), 0);
    assert_eq!(right.events().read("sync-source").len(), 1);
}

fn insert_toy_roads(db: &mut BicDb) {
    db.create_collection("roads_nodes").unwrap();
    db.create_collection("roads_edges").unwrap();
    for (id, lon) in [("a", 0.0), ("b", 0.001), ("c", 0.002)] {
        db.insert(
            "roads_nodes",
            Record::new(id).with_geometry(Geometry::point(lon, 0.0).unwrap()),
        )
        .unwrap();
    }
    for (id, from, to) in [("ab", "a", "b"), ("bc", "b", "c")] {
        db.insert(
            "roads_edges",
            Record::new(id).with_metadata(json!({
                "from": from,
                "to": to,
                "distance_m": 100.0,
                "duration_s": 10.0,
                "road_class": "residential"
            })),
        )
        .unwrap();
    }
}
