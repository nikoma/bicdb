//! G5: Mapbox Vector Tiles from SQL. The equator point at z0 has exactly
//! computable command bytes, so encoding is pinned at the byte level; the
//! rest checks clipping, generalization, and tagging without a decoder.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

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

fn tile_bytes(sql: &mut SqlSession, query: &str) -> Vec<u8> {
    let SqlValue::String(hex_text) = sql.execute(query).unwrap().rows[0][0].clone() else {
        panic!("expected hex text");
    };
    assert!(hex_text.starts_with("\\x"));
    hex::decode(&hex_text[2..]).unwrap()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn point_encoding_is_byte_exact_and_layers_are_named() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE poi (id TEXT PRIMARY KEY, geom TEXT, kind TEXT)")
        .unwrap();
    sql.execute("INSERT INTO poi VALUES ('origin', 'POINT (0 0)', 'landmark')")
        .unwrap();

    let tile = tile_bytes(
        &mut sql,
        "SELECT bicdb_tile_mvt('poi', 'geom', 0, 0, 0, 'pois')",
    );
    // Tile message starts with layer field (3, wire 2) = 0x1a.
    assert_eq!(tile[0], 0x1a);
    // Layer name travels verbatim.
    assert!(contains(&tile, b"pois"));
    // (0,0) at z0 projects to tile-local (2048, 2048):
    // MoveTo count 1 = 0x09, zigzag(2048) = 4096 = varint [0x80, 0x20].
    assert!(
        contains(&tile, &[0x09, 0x80, 0x20, 0x80, 0x20]),
        "expected exact MoveTo(2048,2048) commands in {tile:02x?}"
    );
    // Tag key and value strings are in the layer dictionaries.
    assert!(contains(&tile, b"kind"));
    assert!(contains(&tile, b"landmark"));
}

#[test]
fn out_of_tile_features_are_dropped_and_polygons_are_clipped() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE shapes (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute(
        "INSERT INTO shapes VALUES \
         ('berlin', 'POINT (13.4 52.52)'), \
         ('sydney', 'POINT (151.2 -33.87)'), \
         ('spanning', 'POLYGON ((13.0 52.0, 14.0 52.0, 14.0 53.0, 13.0 53.0, 13.0 52.0))')",
    )
    .unwrap();

    // z10 tile containing Berlin: x=550, y=335.
    let berlin_tile = tile_bytes(
        &mut sql,
        "SELECT bicdb_tile_mvt('shapes', 'geom', 10, 550, 335, 'layer')",
    );
    assert!(contains(&berlin_tile, b"berlin"));
    assert!(
        !contains(&berlin_tile, b"sydney"),
        "a feature on the other side of the planet must be clipped away"
    );
    // The spanning polygon is clipped into this tile and closes its ring
    // (ClosePath command 0x0f).
    assert!(contains(&berlin_tile, b"spanning"));
    assert!(contains(&berlin_tile, &[0x0f]));

    // The same request one tile east still carries the polygon (it spans),
    // but not the Berlin point.
    let east_tile = tile_bytes(
        &mut sql,
        "SELECT bicdb_tile_mvt('shapes', 'geom', 10, 551, 335, 'layer')",
    );
    assert!(contains(&east_tile, b"spanning"));
    assert!(!contains(&east_tile, b"berlin"));
}

#[test]
fn coarse_zoom_generalizes_dense_lines() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE tracks (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    // A 1000-vertex wiggly track ~1km long.
    let points: Vec<String> = (0..1000)
        .map(|index| {
            format!(
                "{} {}",
                13.4 + index as f64 * 0.00001,
                52.52 + (index % 2) as f64 * 0.000001
            )
        })
        .collect();
    sql.execute(&format!(
        "INSERT INTO tracks VALUES ('t', 'LINESTRING ({})')",
        points.join(", ")
    ))
    .unwrap();

    // At z5 the whole track collapses to very few integer positions: the
    // tile must be tiny. At z14 it retains real detail.
    let coarse = tile_bytes(
        &mut sql,
        "SELECT bicdb_tile_mvt('tracks', 'geom', 5, 17, 10, 'trk')",
    );
    let fine = tile_bytes(
        &mut sql,
        "SELECT bicdb_tile_mvt('tracks', 'geom', 14, 8802, 5373, 'trk')",
    );
    assert!(
        coarse.len() < 200,
        "coarse tile should be generalized, got {} bytes",
        coarse.len()
    );
    assert!(
        fine.len() > coarse.len(),
        "fine zoom must retain more detail ({} vs {})",
        fine.len(),
        coarse.len()
    );
}
