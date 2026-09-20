//! `PACK SPATIAL INDEX <name> [USING HILBERT | STR]` — the SQL surface over
//! `BicDb::pack_spatial_index_with_strategy`.

use bicdb_core::{BicDb, DbConfig, Geometry, Record, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn place(id: &str, lon: f64, lat: f64) -> Record {
    Record::new(id.to_string())
        .with_metadata(json!({ "name": id }))
        .with_geometry(Geometry::point(lon, lat).unwrap())
}

fn seeded(dir: &std::path::Path) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    // The SQL schema must exist for CREATE SPATIAL INDEX; the rows can come
    // from the core API.
    SqlSession::new(&mut db)
        .execute("CREATE TABLE places (id TEXT PRIMARY KEY)")
        .unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.bulk_load_insert(
        "places",
        (0..120u32)
            .map(|index| place(&format!("p-{index:04}"), 0.001 * f64::from(index), 0.0))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db
}

fn cell(value: &SqlValue) -> String {
    match value {
        SqlValue::String(text) => text.clone(),
        SqlValue::Int(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

#[test]
fn pack_spatial_index_returns_the_report_row_and_queries_stay_exact() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_places_geometry ON places(geometry)")
        .unwrap();

    let before = session
        .execute(
            "SELECT id FROM places WHERE ST_DWithin(geometry, ST_Point(0.0, 0.0), 5000) ORDER BY id",
        )
        .unwrap();
    assert!(!before.rows.is_empty());

    let result = session
        .execute("PACK SPATIAL INDEX idx_places_geometry")
        .unwrap();
    assert_eq!(
        result.columns,
        vec![
            "index_name",
            "strategy",
            "generation",
            "entry_count",
            "node_count",
            "height"
        ]
    );
    assert_eq!(result.rows.len(), 1);
    let row: Vec<String> = result.rows[0].iter().map(cell).collect();
    assert_eq!(row[0], "idx_places_geometry");
    assert_eq!(row[1], "hilbert");
    assert_eq!(row[2], "1");
    assert_eq!(row[3], "120");

    let after = session
        .execute(
            "SELECT id FROM places WHERE ST_DWithin(geometry, ST_Point(0.0, 0.0), 5000) ORDER BY id",
        )
        .unwrap();
    assert_eq!(after.rows, before.rows, "packed answers diverged");

    // Re-pack with STR: generation bumps, strategy reported.
    let repacked = session
        .execute("PACK SPATIAL INDEX idx_places_geometry USING STR")
        .unwrap();
    let row: Vec<String> = repacked.rows[0].iter().map(cell).collect();
    assert_eq!(row[1], "str");
    assert_eq!(row[2], "2");
}

#[test]
fn pack_spatial_index_rejects_bad_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE SPATIAL INDEX idx_places_geometry ON places(geometry)")
        .unwrap();

    let error = session
        .execute("PACK SPATIAL INDEX idx_places_geometry USING QUADTREE")
        .unwrap_err();
    assert!(
        error.to_string().contains("HILBERT or USING STR"),
        "unexpected error: {error}"
    );

    let error = session
        .execute("PACK SPATIAL INDEX no_such_index")
        .unwrap_err();
    assert!(
        error.to_string().contains("not found"),
        "unexpected error: {error}"
    );

    session.execute("BEGIN").unwrap();
    let error = session
        .execute("PACK SPATIAL INDEX idx_places_geometry")
        .unwrap_err();
    assert!(
        error.to_string().contains("transaction"),
        "unexpected error: {error}"
    );
    session.execute("ROLLBACK").unwrap();
}
