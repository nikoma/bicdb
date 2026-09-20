//! G9: the stateful geofence engine + offline replication. Fence layers,
//! presence, and transition events are ordinary records/events, so the
//! whole moving-assets story replicates over mesh sync with no extra
//! machinery.

use bicdb_core::{
    BicDb, DbConfig, GeofenceTransition, GeofenceTransitionKind, Geometry, NodeId, Record,
    SPATIAL_AUDIT_STREAM,
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

fn fence(id: &str, min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Record {
    Record::new(id).with_geometry(
        Geometry::from_wkt(&format!(
            "POLYGON (({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
        ))
        .unwrap(),
    )
}

fn kinds(transitions: &[GeofenceTransition]) -> Vec<(&str, GeofenceTransitionKind)> {
    transitions
        .iter()
        .map(|transition| (transition.geofence_id.as_str(), transition.kind))
        .collect()
}

#[test]
fn enter_dwell_exit_lifecycle_with_overlapping_fences() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_node_id(dir.path(), sync_config(), node(1)).unwrap();
    db.create_collection("camp_fences").unwrap();
    db.insert("camp_fences", fence("clinic", 0.0, 0.0, 1.0, 1.0))
        .unwrap();
    db.insert("camp_fences", fence("campus", 0.0, 0.0, 2.0, 2.0))
        .unwrap();

    // Outside everything: no transitions.
    assert!(db
        .process_device_location("camp_fences", "phone-1", 5.0, 5.0, 1_000)
        .unwrap()
        .is_empty());

    // Into the overlap: two enters.
    let transitions = db
        .process_device_location("camp_fences", "phone-1", 0.5, 0.5, 1_060)
        .unwrap();
    assert_eq!(
        kinds(&transitions),
        vec![
            ("campus", GeofenceTransitionKind::Enter),
            ("clinic", GeofenceTransitionKind::Enter),
        ]
    );

    // Still inside both 90s later: dwell for both.
    let transitions = db
        .process_device_location("camp_fences", "phone-1", 0.6, 0.6, 1_150)
        .unwrap();
    assert_eq!(
        kinds(&transitions),
        vec![
            ("campus", GeofenceTransitionKind::Dwell(90)),
            ("clinic", GeofenceTransitionKind::Dwell(90)),
        ]
    );

    // Out of the clinic, still on campus: one exit with total dwell, one
    // dwell.
    let transitions = db
        .process_device_location("camp_fences", "phone-1", 1.5, 1.5, 1_260)
        .unwrap();
    assert_eq!(
        kinds(&transitions),
        vec![
            ("campus", GeofenceTransitionKind::Dwell(200)),
            ("clinic", GeofenceTransitionKind::Exit(200)),
        ]
    );

    // Fully out: campus exit.
    let transitions = db
        .process_device_location("camp_fences", "phone-1", 9.0, 9.0, 1_400)
        .unwrap();
    assert_eq!(
        kinds(&transitions),
        vec![("campus", GeofenceTransitionKind::Exit(340))]
    );

    // The transition history landed on the spatial stream for broker
    // consumers: 2 enters + 3 dwells + 2 exits = 7 events.
    let spatial_events = db
        .export_events_since(0)
        .into_iter()
        .filter(|stored| {
            stored.event.stream == SPATIAL_AUDIT_STREAM
                && stored.event.event_type.starts_with("Geofence")
        })
        .count();
    assert_eq!(spatial_events, 7);
}

#[test]
fn geofence_state_replicates_over_mesh_sync() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut field_phone = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut hq = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    // HQ authors the fence layer; the field phone receives it by sync.
    hq.create_collection("zones").unwrap();
    hq.insert("zones", fence("ward", 10.0, 10.0, 11.0, 11.0))
        .unwrap();
    let vector = field_phone.sync_vector().unwrap();
    let bundle = hq.export_sync_bundle_delta(&vector).unwrap();
    field_phone.import_sync_bundle(bundle).unwrap();

    // Offline movement on the field phone.
    let transitions = field_phone
        .process_device_location("zones", "ambulance-7", 10.5, 10.5, 2_000)
        .unwrap();
    assert_eq!(
        kinds(&transitions),
        vec![("ward", GeofenceTransitionKind::Enter)]
    );
    field_phone
        .process_device_location("zones", "ambulance-7", 20.0, 20.0, 2_300)
        .unwrap();

    // Later sync: HQ sees the presence record and the transition events
    // without ever having run the engine itself.
    let vector = hq.sync_vector().unwrap();
    let bundle = field_phone.export_sync_bundle_delta(&vector).unwrap();
    hq.import_sync_bundle(bundle).unwrap();

    let presence = hq
        .get("zones_presence", "ambulance-7")
        .unwrap()
        .expect("presence replicated");
    assert_eq!(presence.metadata["updated_at"], json!(2_300));
    assert!(presence.metadata["fences"].as_object().unwrap().is_empty());

    let transition_events: Vec<String> = hq
        .export_events_since(0)
        .into_iter()
        .filter(|stored| {
            stored.event.stream == SPATIAL_AUDIT_STREAM
                && stored.event.event_type.starts_with("Geofence")
        })
        .map(|stored| stored.event.event_type)
        .collect();
    assert_eq!(transition_events, vec!["GeofenceEntered", "GeofenceExited"]);
}
