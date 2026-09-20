//! G7: travel_time, matrices, isochrones. A hand-built line graph makes
//! every expected time exactly computable.

use bicdb_core::{BicDb, DbConfig, Geometry, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (dir, db)
}

/// A west→east chain A—B—C—D at ~0.01° spacing (~717m at 52.5°N), each
/// edge stamped 1000m / 60s (both directions), plus an isolated node.
fn seed_roads(db: &mut BicDb) {
    db.create_collection("roads_nodes").unwrap();
    db.create_collection("roads_edges").unwrap();
    let nodes = [("a", 13.40), ("b", 13.41), ("c", 13.42), ("d", 13.43)];
    for (id, lon) in nodes {
        db.insert(
            "roads_nodes",
            Record::new(id).with_metadata(json!({"lon": lon, "lat": 52.52})),
        )
        .unwrap();
    }
    db.insert(
        "roads_nodes",
        Record::new("island").with_metadata(json!({"lon": 0.0, "lat": 0.0})),
    )
    .unwrap();
    let mut edge = |id: &str, from: &str, to: &str| {
        db.insert(
            "roads_edges",
            Record::new(id).with_metadata(json!({
                "from": from,
                "to": to,
                "distance_m": 1000.0,
                "duration_s": 60.0,
                "road_class": "residential",
            })),
        )
        .unwrap();
    };
    for (index, pair) in [("a", "b"), ("b", "c"), ("c", "d")].iter().enumerate() {
        edge(&format!("e{index}f"), pair.0, pair.1);
        edge(&format!("e{index}r"), pair.1, pair.0);
    }
}

#[test]
fn travel_time_respects_profiles_and_unreachability() {
    let (_dir, mut db) = session_db();
    seed_roads(&mut db);
    let mut sql = SqlSession::new(&mut db);

    // Driving A→C: two 60s edges.
    let SqlValue::Float(driving) = sql
        .execute("SELECT travel_time('roads', 13.40, 52.52, 13.42, 52.52, 'driving')")
        .unwrap()
        .rows[0][0]
        .clone()
    else {
        panic!("expected seconds");
    };
    assert!((driving - 120.0).abs() < 1e-6, "driving {driving}");

    // Walking costs by length at 1.4 m/s: 2000m ≈ 1428.6s.
    let SqlValue::Float(walking) = sql
        .execute("SELECT travel_time('roads', 13.40, 52.52, 13.42, 52.52, 'walking')")
        .unwrap()
        .rows[0][0]
        .clone()
    else {
        panic!("expected seconds");
    };
    assert!((walking - 2000.0 / 1.4).abs() < 1e-6, "walking {walking}");

    // The reachability WHERE-clause shape from the campaign doc works.
    sql.execute("CREATE TABLE dentists (id TEXT PRIMARY KEY, lon FLOAT, lat FLOAT)")
        .unwrap();
    sql.execute("INSERT INTO dentists VALUES ('near', 13.41, 52.52), ('far', 13.43, 52.52)")
        .unwrap();
    let rows = sql
        .execute(
            "SELECT id FROM dentists \
             WHERE travel_time('roads', 13.40, 52.52, lon, lat, 'driving') <= 90 ORDER BY id",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::String("near".to_string()));

    // The island snaps to itself; no route exists from the chain.
    assert!(sql
        .execute("SELECT travel_time('roads', 13.40, 52.52, 0.0, 0.0, 'driving')")
        .unwrap_err()
        .to_string()
        .contains("no route"));
}

#[test]
fn travel_matrix_and_isochrone() {
    let (_dir, mut db) = session_db();
    seed_roads(&mut db);

    // Core matrix: rows = origins (A, D), columns = destinations (B, C).
    let matrix = db
        .travel_time_matrix(
            "roads",
            &[
                Geometry::point(13.40, 52.52).unwrap(),
                Geometry::point(13.43, 52.52).unwrap(),
            ],
            &[
                Geometry::point(13.41, 52.52).unwrap(),
                Geometry::point(13.42, 52.52).unwrap(),
            ],
            "driving".parse().unwrap(),
        )
        .unwrap();
    assert_eq!(matrix.len(), 2);
    assert_eq!(matrix[0], vec![Some(60.0), Some(120.0)]);
    assert_eq!(matrix[1], vec![Some(120.0), Some(60.0)]);

    let mut sql = SqlSession::new(&mut db);
    let SqlValue::String(matrix_json) = sql
        .execute(
            "SELECT bicdb_travel_matrix('roads', '[[13.40,52.52]]', '[[13.42,52.52],[0.0,0.0]]', 'driving')",
        )
        .unwrap()
        .rows[0][0]
        .clone()
    else {
        panic!("expected JSON");
    };
    let parsed: Vec<Vec<Option<f64>>> = serde_json::from_str(&matrix_json).unwrap();
    assert_eq!(
        parsed,
        vec![vec![Some(120.0), None]],
        "island is unreachable"
    );

    // 90-second isochrone from A covers B (60s) but not C (120s) or D.
    let SqlValue::Geometry(catchment) = sql
        .execute("SELECT bicdb_isochrone('roads', 13.40, 52.52, 90, 'driving')")
        .unwrap()
        .rows[0][0]
        .clone()
    else {
        panic!("expected polygon");
    };
    let wkt = catchment.to_wkt();
    assert!(wkt.starts_with("POLYGON"), "{wkt}");
    let rows = sql
        .execute(&format!(
            "SELECT ST_Covers(ST_GeomFromText('{wkt}'), ST_Point(13.41, 52.52)), \
                    ST_Covers(ST_GeomFromText('{wkt}'), ST_Point(13.42, 52.52))"
        ))
        .unwrap();
    assert_eq!(
        rows.rows[0][0],
        SqlValue::Bool(true),
        "B inside 90s catchment"
    );
    assert_eq!(
        rows.rows[0][1],
        SqlValue::Bool(false),
        "C outside 90s catchment"
    );
}
