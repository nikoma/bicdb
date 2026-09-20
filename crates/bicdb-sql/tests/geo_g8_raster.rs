//! G8: minimal rasters — bilinear sampling, slope, zonal mean.

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
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

fn float(sql: &mut SqlSession, query: &str) -> f64 {
    match sql.execute(query).unwrap().rows[0][0].clone() {
        SqlValue::Float(value) => value,
        other => panic!("expected float, got {other:?}"),
    }
}

#[test]
fn bilinear_sampling_slope_and_zonal_mean() {
    let (_dir, mut db) = session_db();
    db.create_collection("elevation").unwrap();
    // A 3×3 grid over [0,3]×[0,3] degrees: values rise by 100 per column
    // (west→east), constant across rows — a plane tilted purely eastward.
    db.insert(
        "elevation",
        Record::new("tile-0").with_metadata(json!({
            "min_lon": 0.0, "min_lat": 0.0, "max_lon": 3.0, "max_lat": 3.0,
            "width": 3, "height": 3,
            "values": [0.0, 100.0, 200.0,
                       0.0, 100.0, 200.0,
                       0.0, 100.0, 200.0],
        })),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);

    // Cell centers sample exactly.
    assert_eq!(
        float(
            &mut sql,
            "SELECT bicdb_raster_sample('elevation', 0.5, 1.5)"
        ),
        0.0
    );
    assert_eq!(
        float(
            &mut sql,
            "SELECT bicdb_raster_sample('elevation', 1.5, 1.5)"
        ),
        100.0
    );
    // Midway between column centers interpolates linearly.
    assert_eq!(
        float(
            &mut sql,
            "SELECT bicdb_raster_sample('elevation', 1.0, 1.5)"
        ),
        50.0
    );
    // Outside the tile: NULL, not an error.
    assert_eq!(
        sql.execute("SELECT bicdb_raster_sample('elevation', 10.0, 10.0)")
            .unwrap()
            .rows[0][0],
        SqlValue::Null
    );

    // Slope of the tilted plane: 100m over 1° of longitude at the equator
    // (~111.32 km) ≈ atan(100/111320) ≈ 0.0515°.
    let slope = float(&mut sql, "SELECT bicdb_raster_slope('elevation', 1.5, 1.5)");
    let expected = (100.0_f64 / 111_320.0).atan().to_degrees();
    assert!(
        (slope - expected).abs() < expected * 0.05,
        "slope {slope} vs {expected}"
    );

    // Zonal mean over the western two columns: mean(0, 100) = 50.
    let mean = float(
        &mut sql,
        "SELECT bicdb_raster_zonal_mean('elevation', 'POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))')",
    );
    assert_eq!(mean, 50.0);
}
