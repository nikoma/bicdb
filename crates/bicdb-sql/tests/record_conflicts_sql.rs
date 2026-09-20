//! SQL surface for mesh conflict review: `bicdb_record_conflicts` /
//! `bicdb_record_conflict` expose the causal resolver's surfaced
//! concurrency as JSON, so review screens can be built over plain SQL.

use bicdb_core::{BicDb, DbConfig, Event, NodeId, Record, SyncVector, RECORD_AUDIT_STREAM};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
}

fn sync_into(target: &mut BicDb, source: &mut BicDb) {
    // Signed imports come only from a pinned origin. Pin on first contact,
    // the way a real peering does, rather than disabling the check.
    if let Some(key) = source.mesh_verifying_key() {
        target.pin_node_key(&source.node_id(), &key).unwrap();
    }
    let vector = target.sync_vector().unwrap();
    let bundle = source.export_sync_bundle_delta(&vector).unwrap();
    target.import_sync_bundle(bundle).unwrap();
}

/// Declares a collection on both sides and opts it into mesh sync.
///
/// Record events only leave a node for collections explicitly opted in, and an
/// import never creates or opts in a collection implicitly.
fn create_mesh_collection(db: &mut BicDb, name: &str) {
    db.create_collection(name).unwrap();
    db.set_collection_mesh_sync_enabled(name, true).unwrap();
}

fn contexted_update(db: &mut BicDb, record_id: &str, metadata: serde_json::Value, wall: i64) {
    let context = db.sync_vector().unwrap();
    let event = Event::new(
        RECORD_AUDIT_STREAM,
        "RecordUpdated",
        json!({
            "collection": "patients",
            "collection_mode": "standard",
            "record_id": record_id,
            "record": Record::new(record_id).with_metadata(metadata),
            "write_context": context,
        }),
    )
    .with_timestamp(wall);
    db.events_mut().append(event).unwrap();
}

fn single_value(db: &mut BicDb, query: &str) -> SqlValue {
    let mut sql = SqlSession::new(db);
    let result = sql.execute(query).unwrap();
    result.rows[0][0].clone()
}

#[test]
fn sql_surfaces_conflicts_and_clears_after_causal_resolution() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let mut a = BicDb::open_with_node_id(dir_a.path(), sync_config(), node(1)).unwrap();
    let mut b = BicDb::open_with_node_id(dir_b.path(), sync_config(), node(2)).unwrap();

    create_mesh_collection(&mut a, "patients");
    create_mesh_collection(&mut b, "patients");
    a.insert(
        "patients",
        Record::new("p-1").with_metadata(json!({"phone": "111"})),
    )
    .unwrap();
    sync_into(&mut b, &mut a);

    // No conflicts yet: empty list, NULL single lookup.
    let listed = single_value(&mut a, "SELECT bicdb_record_conflicts('patients')");
    assert_eq!(listed, SqlValue::String("[]".to_string()));
    let single = single_value(&mut a, "SELECT bicdb_record_conflict('patients', 'p-1')");
    assert_eq!(single, SqlValue::Null);

    // Concurrent demographic edits on both devices.
    let base = 2_000_000_000_i64;
    contexted_update(&mut a, "p-1", json!({"phone": "222"}), base + 5);
    contexted_update(&mut b, "p-1", json!({"phone": "333"}), base + 9);
    sync_into(&mut b, &mut a);
    sync_into(&mut a, &mut b);

    let SqlValue::String(listed) =
        single_value(&mut a, "SELECT bicdb_record_conflicts('patients')")
    else {
        panic!("expected JSON text");
    };
    let parsed: serde_json::Value = serde_json::from_str(&listed).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 1);
    assert_eq!(parsed[0]["record_id"], "p-1");
    assert_eq!(parsed[0]["candidates"].as_array().unwrap().len(), 2);

    let SqlValue::String(single) =
        single_value(&mut a, "SELECT bicdb_record_conflict('patients', 'p-1')")
    else {
        panic!("expected JSON text");
    };
    let single: serde_json::Value = serde_json::from_str(&single).unwrap();
    assert_eq!(
        single["projected_event_id"],
        parsed[0]["projected_event_id"]
    );

    // A covering write (made after seeing both candidates) clears it.
    contexted_update(&mut a, "p-1", json!({"phone": "999"}), base + 30);
    sync_into(&mut b, &mut a);
    sync_into(&mut a, &mut b);
    assert_eq!(
        single_value(&mut a, "SELECT bicdb_record_conflict('patients', 'p-1')"),
        SqlValue::Null
    );
    assert_eq!(
        single_value(&mut b, "SELECT bicdb_record_conflicts('patients')"),
        SqlValue::String("[]".to_string())
    );
    let _ = SyncVector::default();
}
