//! G4: H3 grid analytics. Cell ids are checked against Uber's published
//! test vectors; the heatmap shape is plain GROUP BY over h3_cell.

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

fn value(sql: &mut SqlSession, query: &str) -> SqlValue {
    sql.execute(query).unwrap().rows[0][0].clone()
}

#[test]
fn h3_cells_match_reference_vectors() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // Uber H3 canonical test point (San Francisco) at resolution 9.
    assert_eq!(
        value(
            &mut sql,
            "SELECT h3_cell(-122.418307270836, 37.7752702151959, 9)"
        ),
        SqlValue::String("8928308280fffff".to_string())
    );
    // Geometry form agrees with lon/lat form.
    assert_eq!(
        value(
            &mut sql,
            "SELECT h3_cell(ST_Point(-122.418307270836, 37.7752702151959), 9)"
        ),
        SqlValue::String("8928308280fffff".to_string())
    );

    assert_eq!(
        value(&mut sql, "SELECT h3_resolution('8928308280fffff')"),
        SqlValue::Int(9)
    );

    // Parent at resolution 8 per the H3 reference.
    assert_eq!(
        value(&mut sql, "SELECT h3_cell_to_parent('8928308280fffff', 8)"),
        SqlValue::String("8828308281fffff".to_string())
    );

    // The cell center maps back into the same cell.
    assert_eq!(
        value(
            &mut sql,
            "SELECT h3_cell(h3_cell_to_center('8928308280fffff'), 9)"
        ),
        SqlValue::String("8928308280fffff".to_string())
    );

    // Boundary is a closed hexagon covering the center.
    assert_eq!(
        value(
            &mut sql,
            "SELECT ST_Covers(h3_cell_to_boundary('8928308280fffff'), h3_cell_to_center('8928308280fffff'))"
        ),
        SqlValue::Bool(true)
    );
}

#[test]
fn grid_disk_and_polyfill_laws() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // |disk(k)| = 1 + 6·(1+…+k) for hexagons: k=1 → 7, k=2 → 19.
    for (k, expected) in [(0, 1), (1, 7), (2, 19)] {
        let SqlValue::String(disk) = value(
            &mut sql,
            &format!("SELECT h3_grid_disk('8928308280fffff', {k})"),
        ) else {
            panic!("expected JSON");
        };
        let cells: Vec<String> = serde_json::from_str(&disk).unwrap();
        assert_eq!(cells.len(), expected, "disk k={k}");
    }

    // Polyfill of a cell's own boundary at its resolution contains the cell
    // itself, and every returned cell's center is inside the polygon.
    let SqlValue::String(filled) = value(
        &mut sql,
        "SELECT h3_polygon_to_cells(h3_cell_to_boundary('8928308280fffff'), 9)",
    ) else {
        panic!("expected JSON");
    };
    let cells: Vec<String> = serde_json::from_str(&filled).unwrap();
    assert!(
        cells.contains(&"8928308280fffff".to_string()),
        "polyfill must include the source cell: {cells:?}"
    );
    assert!(
        cells.len() <= 3,
        "a single hexagon fills to itself (+edge wobble): {cells:?}"
    );

    // A ~2km square in Berlin at res 9 (~0.1 km² cells) yields a sensible
    // count in the tens.
    let SqlValue::String(filled) = value(
        &mut sql,
        "SELECT h3_polygon_to_cells(ST_GeomFromText('POLYGON ((13.38 52.51, 13.41 52.51, 13.41 52.53, 13.38 52.53, 13.38 52.51))'), 9)",
    ) else {
        panic!("expected JSON");
    };
    let cells: Vec<String> = serde_json::from_str(&filled).unwrap();
    assert!(
        (20..80).contains(&cells.len()),
        "unexpected fill count {}",
        cells.len()
    );
}

#[test]
fn polyfill_rejects_excessive_sampling_work_before_iteration() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute(
            "SELECT h3_polygon_to_cells(ST_GeomFromText('POLYGON ((-170 -80, 170 -80, 170 80, -170 80, -170 -80))'), 15)",
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("sample work cap"),
        "unexpected error: {error}"
    );
}

#[test]
fn heatmap_is_group_by_h3_cell() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE businesses (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    // Two clusters ~20km apart; resolution 7 (~5 km²) separates them.
    let mut rows = Vec::new();
    for index in 0..10 {
        rows.push(format!(
            "('mitte{index}', 'POINT ({} 52.520)')",
            13.400 + index as f64 * 0.0004
        ));
    }
    for index in 0..4 {
        rows.push(format!(
            "('spandau{index}', 'POINT ({} 52.535)')",
            13.190 + index as f64 * 0.0004
        ));
    }
    sql.execute(&format!(
        "INSERT INTO businesses VALUES {}",
        rows.join(", ")
    ))
    .unwrap();

    let result = sql
        .execute(
            "SELECT h3_cell(geom, 7) AS cell, count(*) AS n FROM businesses \
             GROUP BY h3_cell(geom, 7) ORDER BY n DESC",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 2, "two density cells expected");
    assert_eq!(result.rows[0][1], SqlValue::Int(10));
    assert_eq!(result.rows[1][1], SqlValue::Int(4));

    // Antimeridian gate: cells at ±180 resolve and round-trip.
    let SqlValue::String(cell) = value(&mut sql, "SELECT h3_cell(179.9999, 0.0, 7)") else {
        panic!("expected cell");
    };
    assert_eq!(
        value(&mut sql, &format!("SELECT h3_resolution('{cell}')")),
        SqlValue::Int(7)
    );
    let SqlValue::String(cell_west) = value(&mut sql, "SELECT h3_cell(-179.9999, 0.0, 7)") else {
        panic!("expected cell");
    };
    // H3 has no antimeridian seam: the two near-180 points may share a cell
    // or sit in adjacent cells, but both must be valid and near each other.
    let SqlValue::String(disk) = value(&mut sql, &format!("SELECT h3_grid_disk('{cell}', 2)"))
    else {
        panic!("expected JSON");
    };
    let neighborhood: Vec<String> = serde_json::from_str(&disk).unwrap();
    assert!(
        neighborhood.contains(&cell_west),
        "±180 twins must be H3 neighbors: {cell} vs {cell_west}"
    );
}
