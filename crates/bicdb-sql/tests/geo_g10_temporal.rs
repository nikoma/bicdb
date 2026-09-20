//! G10: temporal geo — "where was this boundary six months ago?" answered
//! from the audit stream with controlled timestamps.

use bicdb_core::{BicDb, DbConfig, Event, NodeId, Record, StorageMode, RECORD_AUDIT_STREAM};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
use uuid::Uuid;

fn boundary_event(kind: &str, wkt: Option<&str>, population: i64, ts: i64) -> Event {
    let mut record = json!({
        "id": "district-9",
        "metadata": {"population": population},
    });
    if let Some(wkt) = wkt {
        record["geometry"] =
            serde_json::to_value(bicdb_core::Geometry::from_wkt(wkt).unwrap()).unwrap();
    }
    Event::new(
        RECORD_AUDIT_STREAM,
        kind,
        json!({
            "collection": "districts",
            "collection_mode": "standard",
            "record_id": "district-9",
            "record": record,
        }),
    )
    .with_timestamp(ts)
}

#[test]
fn asof_returns_the_boundary_and_record_of_its_era() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_node_id(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_audit_events(true),
        NodeId(Uuid::from_u128(1)),
    )
    .unwrap();

    // v1 boundary at t=1000, redistricted (larger) at t=2000, dissolved at
    // t=3000.
    let v1 = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
    let v2 = "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))";
    db.events_mut()
        .append(boundary_event("RecordCreated", Some(v1), 1_000, 1_000))
        .unwrap();
    db.events_mut()
        .append(boundary_event("RecordUpdated", Some(v2), 1_500, 2_000))
        .unwrap();
    db.events_mut()
        .append(boundary_event("RecordDeleted", None, 1_500, 3_000))
        .unwrap();

    let mut sql = SqlSession::new(&mut db);
    let geometry_asof = |sql: &mut SqlSession, at: i64| -> SqlValue {
        sql.execute(&format!(
            "SELECT bicdb_geometry_asof('districts', 'district-9', {at})"
        ))
        .unwrap()
        .rows[0][0]
            .clone()
    };

    // Before creation: nothing.
    assert_eq!(geometry_asof(&mut sql, 500), SqlValue::Null);
    // The v1 era.
    let SqlValue::Geometry(geometry) = geometry_asof(&mut sql, 1_500) else {
        panic!("expected geometry");
    };
    assert_eq!(geometry.to_wkt(), "POLYGON((0 0,1 0,1 1,0 1,0 0))");
    // The redistricted era.
    let SqlValue::Geometry(geometry) = geometry_asof(&mut sql, 2_500) else {
        panic!("expected geometry");
    };
    assert_eq!(geometry.to_wkt(), "POLYGON((0 0,2 0,2 2,0 2,0 0))");
    // After dissolution: gone.
    assert_eq!(geometry_asof(&mut sql, 3_500), SqlValue::Null);

    // The full record travels with its era too.
    let SqlValue::String(record) = sql
        .execute("SELECT bicdb_record_asof('districts', 'district-9', 1500)")
        .unwrap()
        .rows[0][0]
        .clone()
    else {
        panic!("expected JSON");
    };
    let record: serde_json::Value = serde_json::from_str(&record).unwrap();
    assert_eq!(record["metadata"]["population"], 1_000);
    assert!(record["geometry"].as_str().unwrap().starts_with("POLYGON"));
}
