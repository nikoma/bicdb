//! G6: FTS-fused geocoding. Forward geocode rides a real FTS index
//! (stemming, ranking, multilingual/alias text all come for free), reverse
//! geocode rides the spatial kNN, and the admin hierarchy is containment
//! ordered by level.

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

fn json_rows(sql: &mut SqlSession, query: &str) -> Vec<serde_json::Value> {
    let SqlValue::String(text) = sql.execute(query).unwrap().rows[0][0].clone() else {
        panic!("expected JSON text");
    };
    serde_json::from_str(&text).unwrap()
}

#[test]
fn forward_geocode_ranks_fts_matches_with_aliases_and_languages() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE geo_names (id TEXT PRIMARY KEY, name TEXT, search_text TEXT, geom TEXT, kind TEXT)",
    )
    .unwrap();
    // search_text carries name + aliases + other languages — the FTS index
    // makes them all findable without schema ceremony.
    sql.execute(
        "INSERT INTO geo_names VALUES \
         ('q1', 'Munich', 'Munich München Muenchen Monaco di Baviera', 'POINT (11.575 48.137)', 'city'), \
         ('q2', 'Munich Airport', 'Munich Airport Flughafen München MUC', 'POINT (11.786 48.353)', 'airport'), \
         ('q3', 'Berlin', 'Berlin', 'POINT (13.405 52.52)', 'city')",
    )
    .unwrap();
    sql.execute(
        "CREATE INDEX idx_geo_names_fts ON geo_names USING GIN (to_tsvector('english', COALESCE(search_text, '')))",
    )
    .unwrap();

    // The German exonym finds Munich rows and never Berlin.
    let results = json_rows(
        &mut sql,
        "SELECT bicdb_geocode('idx_geo_names_fts', 'münchen', 10)",
    );
    let ids: Vec<&str> = results
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"q1") && ids.contains(&"q2"), "{ids:?}");
    assert!(!ids.contains(&"q3"));

    // A more specific query ranks the airport first.
    let results = json_rows(
        &mut sql,
        "SELECT bicdb_geocode('idx_geo_names_fts', 'munich airport', 10)",
    );
    assert_eq!(results[0]["id"], "q2");
    assert!(results[0]["score"].as_f64().unwrap() > 0.0);

    // Empty/stopword-only queries return an empty list, not an error.
    let results = json_rows(
        &mut sql,
        "SELECT bicdb_geocode('idx_geo_names_fts', 'the', 10)",
    );
    assert!(results.is_empty());
}

#[test]
fn reverse_geocode_returns_nearest_named_features() {
    let (_dir, mut db) = session_db();
    // Reverse geocoding rides the intrinsic Record.geometry field (what
    // spatial indexes target); rows arrive through the core API.
    db.create_collection("places").unwrap();
    for (id, name, lon, lat) in [
        ("alex", "Alexanderplatz", 13.4132, 52.5219),
        ("brandenburg", "Brandenburger Tor", 13.3777, 52.5163),
        ("eiffel", "Tour Eiffel", 2.2945, 48.8584),
    ] {
        db.insert(
            "places",
            Record::new(id)
                .with_metadata(json!({ "name": name }))
                .with_geometry(Geometry::point(lon, lat).unwrap()),
        )
        .unwrap();
    }
    let mut sql = SqlSession::new(&mut db);

    // Near Museum Island: Alexanderplatz first, Brandenburg Gate second,
    // Paris nowhere near the top-2.
    let results = json_rows(
        &mut sql,
        "SELECT bicdb_reverse_geocode('places', 'geometry', 13.4050, 52.5200, 2)",
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], "alex");
    assert_eq!(results[1]["id"], "brandenburg");
    let nearest = results[0]["distance_meters"].as_f64().unwrap();
    assert!((300.0..1_200.0).contains(&nearest), "distance {nearest}");
}

#[test]
fn admin_hierarchy_orders_containing_boundaries_by_level() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE admin (id TEXT PRIMARY KEY, name TEXT, geom TEXT, level INT)")
        .unwrap();
    // Nested squares: country ⊃ state ⊃ city, plus a disjoint sibling.
    sql.execute(
        "INSERT INTO admin VALUES \
         ('country', 'Absurdistan', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 2), \
         ('state', 'West Province', 'POLYGON ((1 1, 6 1, 6 6, 1 6, 1 1))', 4), \
         ('city', 'Port Vile', 'POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))', 8), \
         ('other_state', 'East Province', 'POLYGON ((7 7, 9 7, 9 9, 7 9, 7 7))', 4)",
    )
    .unwrap();

    let results = json_rows(
        &mut sql,
        "SELECT bicdb_admin_hierarchy('admin', 'geom', 'level', 3.0, 3.0)",
    );
    let path: Vec<(&str, i64)> = results
        .iter()
        .map(|row| (row["id"].as_str().unwrap(), row["level"].as_i64().unwrap()))
        .collect();
    assert_eq!(
        path,
        vec![("country", 2), ("state", 4), ("city", 8)],
        "containment path must run country → state → city"
    );

    // A point in the disjoint sibling gets country + that sibling only.
    let results = json_rows(
        &mut sql,
        "SELECT bicdb_admin_hierarchy('admin', 'geom', 'level', 8.0, 8.0)",
    );
    let ids: Vec<&str> = results
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["country", "other_state"]);
}
