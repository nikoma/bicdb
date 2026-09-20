//! G2: DE-9IM predicates + constructive geometry, geodesic measures.
//! Ground truths cross-checked against PostGIS semantics.

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
fn de9im_predicates_match_postgis_semantics() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // Two squares sharing an edge: touches, not overlaps.
    let a = "ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')";
    let b = "ST_GeomFromText('POLYGON ((2 0, 4 0, 4 2, 2 2, 2 0))')";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Touches({a}, {b})")),
        SqlValue::Bool(true)
    );
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Overlaps({a}, {b})")),
        SqlValue::Bool(false)
    );

    // Overlapping squares: overlaps, not touches, not within.
    let c = "ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))')";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Overlaps({a}, {c})")),
        SqlValue::Bool(true)
    );

    // A line crossing a polygon: crosses.
    let line = "ST_GeomFromText('LINESTRING (-1 1, 3 1)')";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Crosses({line}, {a})")),
        SqlValue::Bool(true)
    );

    // Containment family: within/covers/coveredby/disjoint.
    let inner = "ST_GeomFromText('POLYGON ((0.5 0.5, 1 0.5, 1 1, 0.5 1, 0.5 0.5))')";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Within({inner}, {a})")),
        SqlValue::Bool(true)
    );
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Covers({a}, {inner})")),
        SqlValue::Bool(true)
    );
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_CoveredBy({inner}, {a})")),
        SqlValue::Bool(true)
    );
    let far = "ST_Point(50, 50)";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Disjoint({a}, {far})")),
        SqlValue::Bool(true)
    );

    // Equal rings written in different orders are topologically equal.
    let a_rev = "ST_GeomFromText('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))')";
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Equals({a}, {a_rev})")),
        SqlValue::Bool(true)
    );

    // The raw matrix: interior/interior of two edge-sharing squares is F,
    // boundary/boundary is 1 → "FF2F11212".
    let SqlValue::String(matrix) = value(&mut sql, &format!("SELECT ST_Relate({a}, {b})")) else {
        panic!("expected matrix text");
    };
    assert_eq!(matrix.len(), 9);
    assert_eq!(&matrix[0..1], "F", "interiors must not intersect: {matrix}");
    assert_eq!(&matrix[4..5], "1", "boundaries share a line: {matrix}");

    // ENVELOPE participates as its polygon.
    assert_eq!(
        value(
            &mut sql,
            &format!("SELECT ST_Within({inner}, ST_GeomFromText('BBOX (0 0, 2 2)'))")
        ),
        SqlValue::Bool(true)
    );
}

#[test]
fn boolean_ops_union_intersection_difference() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    let a = "ST_GeomFromText('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')";
    let c = "ST_GeomFromText('POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))')";

    // Planar sanity via degree-space areas is fragile; use geodesic area
    // ratios instead: |A∪C| + |A∩C| = |A| + |C|.
    let area = |sql: &mut SqlSession, expr: &str| -> f64 {
        match value(sql, &format!("SELECT ST_Area({expr})")) {
            SqlValue::Float(area) => area,
            other => panic!("expected area, got {other:?}"),
        }
    };
    let union = area(&mut sql, &format!("ST_Union({a}, {c})"));
    let intersection = area(&mut sql, &format!("ST_Intersection({a}, {c})"));
    let a_area = area(&mut sql, a);
    let c_area = area(&mut sql, c);
    let relative_error = ((union + intersection) - (a_area + c_area)).abs() / (a_area + c_area);
    // Boolean ops are planar (degree space) while ST_Area is geodesic:
    // edges split at planar intersection points measure minutely
    // differently than the unsplit geodesic edge (~1e-5 relative at this
    // scale). PostGIS's geometry type has the same planar-ops property.
    assert!(
        relative_error < 5e-5,
        "inclusion-exclusion violated: {relative_error}"
    );

    let difference = area(&mut sql, &format!("ST_Difference({a}, {c})"));
    let difference_error = (difference - (a_area - intersection)).abs() / a_area;
    assert!(
        difference_error < 5e-4,
        "difference identity off by {difference_error}"
    );

    // Disjoint squares union into a MULTIPOLYGON.
    let far = "ST_GeomFromText('POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))')";
    let SqlValue::String(kind) =
        value(&mut sql, &format!("SELECT ST_AsText(ST_Union({a}, {far}))"))
    else {
        panic!("expected text");
    };
    assert!(kind.starts_with("MULTIPOLYGON"), "{kind}");

    // Points are refused with a clear message, not silently dropped.
    assert!(sql
        .execute("SELECT ST_Union(ST_Point(1, 1), ST_Point(2, 2))")
        .unwrap_err()
        .to_string()
        .contains("POLYGON"));
}

#[test]
fn geodesic_measures_match_reference_values() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // Berlin→Paris ellipsoidal geodesic ≈ 879.70 km (the spherical
    // great-circle value is ~877.5 km; we measure on the WGS84 ellipsoid).
    let SqlValue::Float(length) = value(
        &mut sql,
        "SELECT ST_Length(ST_GeomFromText('LINESTRING (13.405 52.52, 2.3522 48.8566)'))",
    ) else {
        panic!("expected float");
    };
    assert!((length - 879_700.0).abs() < 2_000.0, "length {length}");

    // A 1°×1° square at the equator ≈ 12,308 km² (geodesic).
    let SqlValue::Float(area) = value(
        &mut sql,
        "SELECT ST_Area(ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))",
    ) else {
        panic!("expected float");
    };
    assert!((area - 12.308e9).abs() / 12.308e9 < 0.01, "area {area}");

    // Perimeter of that square ≈ 4 × ~111 km.
    let SqlValue::Float(perimeter) = value(
        &mut sql,
        "SELECT ST_Perimeter(ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))",
    ) else {
        panic!("expected float");
    };
    assert!(
        (perimeter - 443_000.0).abs() < 3_000.0,
        "perimeter {perimeter}"
    );

    // Centroid of the square.
    let SqlValue::Geometry(centroid) = value(
        &mut sql,
        "SELECT ST_Centroid(ST_GeomFromText('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'))",
    ) else {
        panic!("expected geometry");
    };
    assert_eq!(centroid.to_wkt(), "POINT(0.5 0.5)");
}

#[test]
fn buffer_simplify_convexhull_behave() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // A 1km point buffer has geodesic area ≈ πr² within 1%.
    let SqlValue::Float(area) = value(
        &mut sql,
        "SELECT ST_Area(ST_Buffer(ST_Point(13.4, 52.5), 1000))",
    ) else {
        panic!("expected float");
    };
    let circle = std::f64::consts::PI * 1000.0 * 1000.0;
    assert!((area - circle).abs() / circle < 0.01, "buffer area {area}");

    // Buffered MULTIPOINT with overlapping circles dissolves to one area.
    let SqlValue::String(dissolved) = value(
        &mut sql,
        "SELECT ST_AsText(ST_Buffer(ST_GeomFromText('MULTIPOINT ((13.4 52.5), (13.4005 52.5))'), 1000))",
    ) else {
        panic!("expected text");
    };
    assert!(dissolved.starts_with("POLYGON"), "{dissolved}");

    // Simplify collapses collinear-ish chains.
    let SqlValue::String(simplified) = value(
        &mut sql,
        "SELECT ST_AsText(ST_Simplify(ST_GeomFromText('LINESTRING (0 0, 1 0.0001, 2 0, 3 0.0001, 4 0)'), 0.01))",
    ) else {
        panic!("expected text");
    };
    assert_eq!(simplified, "LINESTRING(0 0,4 0)");

    // Convex hull of a multipoint is the enclosing polygon.
    let SqlValue::String(hull) = value(
        &mut sql,
        "SELECT ST_AsText(ST_ConvexHull(ST_GeomFromText('MULTIPOINT ((0 0), (2 0), (2 2), (0 2), (1 1))')))",
    ) else {
        panic!("expected text");
    };
    assert!(hull.starts_with("POLYGON"), "{hull}");
    assert!(
        !hull.contains("1 1"),
        "interior point must not be on hull: {hull}"
    );
}

/// Campaign merge gate: antimeridian coverage. Predicates and measures on
/// geometries at ±180 behave consistently in coordinate space.
#[test]
fn antimeridian_predicates_and_measures() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    // A polygon hugging +180 contains a point at 179.95.
    let east = "ST_GeomFromText('POLYGON ((179.9 -1, 180 -1, 180 1, 179.9 1, 179.9 -1))')";
    assert_eq!(
        value(
            &mut sql,
            &format!("SELECT ST_Covers({east}, ST_Point(179.95, 0))")
        ),
        SqlValue::Bool(true)
    );
    // Its mirror at -180 is coordinate-space disjoint (wide-box semantics
    // are documented; short-way-around topology arrives with split-box
    // query support later in the campaign).
    let west = "ST_GeomFromText('POLYGON ((-180 -1, -179.9 -1, -179.9 1, -180 1, -180 -1))')";
    // The shared 180/-180 edge is invisible in pure coordinate space:
    // numerically these polygons are disjoint. Documented wide-box
    // semantics; short-way-around topology arrives with split-box queries.
    assert_eq!(
        value(&mut sql, &format!("SELECT ST_Disjoint({east}, {west})")),
        SqlValue::Bool(true)
    );

    // Geodesic length across the antimeridian is the short way: ~22 km,
    // never the ~40,000 km wrap.
    let SqlValue::Float(length) = value(
        &mut sql,
        "SELECT ST_Length(ST_GeomFromText('LINESTRING (179.9 0, -179.9 0)'))",
    ) else {
        panic!("expected float");
    };
    assert!((length - 22_250.0).abs() < 500.0, "length {length}");
}
