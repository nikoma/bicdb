//! G1: WKB/EWKB + Multi*/GeometryCollection + SRID plumbing.
//! Antimeridian coverage is a merge gate for every geo item (campaign rule).

use bicdb_core::{BicDb, DbConfig, Geometry, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn wkb_round_trip(wkt: &str) {
    let geometry = Geometry::from_wkt(wkt).unwrap();
    let wkb = geometry.to_wkb();
    let back = Geometry::from_wkb(&wkb).unwrap();
    assert_eq!(back, geometry, "WKB round trip failed for {wkt}");
    let ewkb = geometry.to_ewkb();
    let back = Geometry::from_wkb(&ewkb).unwrap();
    assert_eq!(back, geometry, "EWKB round trip failed for {wkt}");
}

#[test]
fn wkb_round_trips_every_geometry_kind() {
    wkb_round_trip("POINT (13.4 52.5)");
    wkb_round_trip("LINESTRING (0 0, 10 10, 20 5)");
    wkb_round_trip("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))");
    wkb_round_trip("MULTIPOINT ((1 2), (3 4))");
    wkb_round_trip("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))");
    wkb_round_trip("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))");
    wkb_round_trip("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))");
}

#[test]
fn big_endian_wkb_parses() {
    // POINT (1 2), big endian: 00 00000001 x=1.0 y=2.0
    let mut bytes = vec![0_u8, 0, 0, 0, 1];
    bytes.extend_from_slice(&1.0_f64.to_be_bytes());
    bytes.extend_from_slice(&2.0_f64.to_be_bytes());
    let geometry = Geometry::from_wkb(&bytes).unwrap();
    assert_eq!(geometry, Geometry::point(1.0, 2.0).unwrap());
}

#[test]
fn ewkb_srid_rules_are_enforced() {
    let point = Geometry::point(13.4, 52.5).unwrap();
    let ewkb = point.to_ewkb();
    // EWKB stamps 4326 little-endian at bytes 5..9.
    assert_eq!(&ewkb[5..9], &4326_u32.to_le_bytes());
    // A foreign SRID is refused, not misread.
    let mut projected = ewkb.clone();
    projected[5..9].copy_from_slice(&3857_u32.to_le_bytes());
    let error = Geometry::from_wkb(&projected).unwrap_err().to_string();
    assert!(error.contains("unsupported SRID 3857"), "{error}");
    // SRID 0 (unspecified) is fine.
    let mut unspecified = ewkb;
    unspecified[5..9].copy_from_slice(&0_u32.to_le_bytes());
    Geometry::from_wkb(&unspecified).unwrap();
}

#[test]
fn zm_dimensions_are_refused_not_misparsed() {
    // EWKB Z flag (0x80000000) on a point.
    let mut bytes = vec![1_u8];
    bytes.extend_from_slice(&(1_u32 | 0x8000_0000).to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&2.0_f64.to_le_bytes());
    bytes.extend_from_slice(&3.0_f64.to_le_bytes());
    assert!(Geometry::from_wkb(&bytes).is_err());
}

#[test]
fn multi_geometries_round_trip_geojson_storage_and_frames() {
    for wkt in [
        "MULTIPOINT ((1 2), (3 4))",
        "MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))",
        "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))",
        "GEOMETRYCOLLECTION (POINT (1 2), POLYGON ((0 0, 1 0, 1 1, 0 0)))",
    ] {
        let geometry = Geometry::from_wkt(wkt).unwrap();
        // GeoJSON round trip.
        let geojson = geometry.to_geojson_value();
        let back = Geometry::from_geojson_value(geojson).unwrap();
        assert_eq!(back, geometry, "GeoJSON round trip failed for {wkt}");
        // Binary frame round trip.
        let frame = geometry.to_bicdb_frame();
        let back = Geometry::from_bicdb_frame(&frame).unwrap();
        assert_eq!(back, geometry, "frame round trip failed for {wkt}");
        // Storage (serde) round trip.
        let stored = serde_json::to_value(&geometry).unwrap();
        let back: Geometry = serde_json::from_value(stored).unwrap();
        assert_eq!(back, geometry, "storage round trip failed for {wkt}");
        // WKT text round trip.
        let back = Geometry::from_wkt(&geometry.to_wkt()).unwrap();
        assert_eq!(back, geometry, "WKT round trip failed for {wkt}");
    }
}

/// Campaign rule: antimeridian coverage from day one. Geometries touching
/// or crossing lon=180 must round-trip every codec bit-exactly.
#[test]
fn antimeridian_geometries_round_trip_exactly() {
    for wkt in [
        "POINT (180 0)",
        "POINT (-180 -65.3)",
        "LINESTRING (179.9 10, -179.9 10)",
        "MULTIPOLYGON (((179 -1, 180 -1, 180 1, 179 1, 179 -1)), ((-180 -1, -179 -1, -179 1, -180 1, -180 -1)))",
        "GEOMETRYCOLLECTION (POINT (180 90), POINT (-180 -90))",
    ] {
        let geometry = Geometry::from_wkt(wkt).unwrap();
        assert_eq!(Geometry::from_wkb(&geometry.to_wkb()).unwrap(), geometry);
        assert_eq!(Geometry::from_wkb(&geometry.to_ewkb()).unwrap(), geometry);
        assert_eq!(
            Geometry::from_bicdb_frame(&geometry.to_bicdb_frame()).unwrap(),
            geometry
        );
        assert_eq!(
            Geometry::from_geojson_value(geometry.to_geojson_value()).unwrap(),
            geometry
        );
    }
    // The wide-box behavior of an antimeridian-crossing line is documented:
    // coordinate bounds span the numeric range, not the short way around.
    let crossing = Geometry::from_wkt("LINESTRING (179.9 10, -179.9 10)").unwrap();
    let (min, max) = crossing.coordinate_bounds().unwrap();
    assert_eq!((min[0], max[0]), (-179.9, 179.9));
}

#[test]
fn sql_surface_speaks_wkt_wkb_geojson_and_srid() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);

    let value = |sql: &mut SqlSession, query: &str| -> SqlValue {
        sql.execute(query).unwrap().rows[0][0].clone()
    };

    // Text → geometry → text.
    let wkt = value(
        &mut sql,
        "SELECT ST_AsText(ST_GeomFromText('MULTIPOINT ((1 2), (3 4))'))",
    );
    assert_eq!(wkt, SqlValue::String("MULTIPOINT((1 2),(3 4))".to_string()));

    // Geometry → WKB hex → geometry survives, including Multi*.
    let SqlValue::String(hex_wkb) = value(
        &mut sql,
        "SELECT ST_AsBinary(ST_GeomFromText('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))'))",
    ) else {
        panic!("expected hex text");
    };
    assert!(hex_wkb.starts_with("\\x"));
    let round = value(
        &mut sql,
        &format!("SELECT ST_AsText(ST_GeomFromWKB('{hex_wkb}'))"),
    );
    assert_eq!(
        round,
        SqlValue::String("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))".to_string())
    );

    // EWKB carries the SRID; ST_SRID reports it; foreign SRIDs are refused.
    let SqlValue::String(hex_ewkb) = value(&mut sql, "SELECT ST_AsEWKB(ST_Point(13.4, 52.5))")
    else {
        panic!("expected hex text");
    };
    assert!(hex_ewkb.starts_with("\\x"));
    let srid = value(&mut sql, "SELECT ST_SRID(ST_Point(1, 2))");
    assert_eq!(srid, SqlValue::Int(4326));
    assert!(sql
        .execute("SELECT ST_SetSRID(ST_Point(1, 2), 3857)")
        .is_err());
    sql.execute("SELECT ST_SetSRID(ST_Point(1, 2), 4326)")
        .unwrap();

    // GeoJSON in via SQL, including a collection.
    let kind = value(
        &mut sql,
        r#"SELECT ST_AsText(ST_GeomFromGeoJSON('{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}'))"#,
    );
    assert_eq!(
        kind,
        SqlValue::String("GEOMETRYCOLLECTION (POINT(1 2))".to_string())
    );
}

/// Multi* geometries index and query through the spatial index like any
/// other geometry (bbox semantics).
#[test]
fn multi_geometries_are_spatially_indexable() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE places (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute(
        "INSERT INTO places VALUES ('a', 'MULTIPOINT ((13.4 52.5), (13.5 52.6))'), ('b', 'POINT (2.35 48.85)')",
    )
    .unwrap();
    let rows = sql
        .execute(
            "SELECT id FROM places WHERE ST_DWithin(ST_GeomFromText(geom), ST_Point(13.41, 52.51), 5000) ORDER BY id",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::String("a".to_string()));
}
