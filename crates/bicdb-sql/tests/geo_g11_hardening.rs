//! G11: hardening. Deterministic fuzzing (seeded xorshift, no rand dep)
//! over every geometry codec, predicate-law invariants, a polar/
//! antimeridian sweep, and an optional PostGIS differential oracle
//! (POSTGIS_URL + psql; skipped cleanly when absent).

use bicdb_core::{BicDb, DbConfig, Geometry, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn f64(&mut self, low: f64, high: f64) -> f64 {
        low + (self.next() as f64 / u64::MAX as f64) * (high - low)
    }

    fn usize(&mut self, low: usize, high: usize) -> usize {
        low + (self.next() as usize) % (high - low + 1)
    }
}

/// Random valid-by-construction geometry. Coordinates cluster around the
/// antimeridian and poles a third of the time — the places bugs live.
fn random_geometry(rng: &mut Rng) -> Geometry {
    let (center_lon, center_lat) = match rng.usize(0, 2) {
        0 => (rng.f64(-179.0, 179.0), rng.f64(-85.0, 85.0)),
        1 => (
            rng.f64(178.0, 180.0) * if rng.next() % 2 == 0 { 1.0 } else { -1.0 },
            rng.f64(-30.0, 30.0),
        ),
        _ => (
            rng.f64(-30.0, 30.0),
            rng.f64(85.0, 89.9) * if rng.next() % 2 == 0 { 1.0 } else { -1.0 },
        ),
    };
    let clamp = |lon: f64, lat: f64| (lon.clamp(-180.0, 180.0), lat.clamp(-90.0, 90.0));
    let star_ring = |rng: &mut Rng, lon: f64, lat: f64| -> String {
        // Sorted-angle star polygon: simple (non-self-intersecting) always.
        let vertex_count = rng.usize(3, 8);
        let mut vertices: Vec<(f64, f64)> = (0..vertex_count)
            .map(|index| {
                let angle = index as f64 / vertex_count as f64 * std::f64::consts::TAU;
                let radius = rng.f64(0.05, 0.4);
                clamp(lon + radius * angle.cos(), lat + radius * angle.sin())
            })
            .collect();
        vertices.push(vertices[0]);
        vertices
            .iter()
            .map(|(lon, lat)| format!("{lon} {lat}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let wkt = match rng.usize(0, 4) {
        0 => format!("POINT ({center_lon} {center_lat})"),
        1 => {
            let points: Vec<String> = (0..rng.usize(2, 6))
                .map(|_| {
                    let (lon, lat) = clamp(
                        center_lon + rng.f64(-0.5, 0.5),
                        center_lat + rng.f64(-0.5, 0.5),
                    );
                    format!("{lon} {lat}")
                })
                .collect();
            format!("LINESTRING ({})", points.join(", "))
        }
        2 => format!("POLYGON (({}))", star_ring(rng, center_lon, center_lat)),
        3 => {
            let points: Vec<String> = (0..rng.usize(2, 5))
                .map(|_| {
                    let (lon, lat) = clamp(
                        center_lon + rng.f64(-0.5, 0.5),
                        center_lat + rng.f64(-0.5, 0.5),
                    );
                    format!("({lon} {lat})")
                })
                .collect();
            format!("MULTIPOINT ({})", points.join(", "))
        }
        _ => format!(
            "MULTIPOLYGON ((({})), (({})))",
            star_ring(rng, center_lon, center_lat),
            star_ring(
                rng,
                (center_lon - 1.0).max(-179.5),
                (center_lat - 1.0).clamp(-89.0, 89.0)
            )
        ),
    };
    Geometry::from_wkt(&wkt).unwrap()
}

#[test]
fn fuzz_every_codec_round_trips_500_geometries() {
    let mut rng = Rng(0x5eed_cafe_f00d_d00d);
    for iteration in 0..500 {
        let geometry = random_geometry(&mut rng);
        let context = || format!("iteration {iteration}: {}", geometry.to_wkt());

        let wkb = Geometry::from_wkb(&geometry.to_wkb()).unwrap();
        assert_eq!(wkb, geometry, "WKB {}", context());
        let ewkb = Geometry::from_wkb(&geometry.to_ewkb()).unwrap();
        assert_eq!(ewkb, geometry, "EWKB {}", context());
        let wkt = Geometry::from_wkt(&geometry.to_wkt()).unwrap();
        assert_eq!(wkt, geometry, "WKT {}", context());
        let geojson = Geometry::from_geojson_value(geometry.to_geojson_value()).unwrap();
        assert_eq!(geojson, geometry, "GeoJSON {}", context());
        let frame = Geometry::from_bicdb_frame(&geometry.to_bicdb_frame()).unwrap();
        assert_eq!(frame, geometry, "frame {}", context());
        let stored: Geometry =
            serde_json::from_value(serde_json::to_value(&geometry).unwrap()).unwrap();
        assert_eq!(stored, geometry, "storage {}", context());
        let hex =
            Geometry::from_wkb_hex(&format!("\\x{}", hex::encode(geometry.to_ewkb()))).unwrap();
        assert_eq!(hex, geometry, "hex {}", context());
    }
}

#[test]
fn fuzz_predicate_laws_hold() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    let flag = |sql: &mut SqlSession, expr: &str| -> bool {
        match sql.execute(&format!("SELECT {expr}")).unwrap().rows[0][0] {
            SqlValue::Bool(value) => value,
            ref other => panic!("expected bool from {expr}, got {other:?}"),
        }
    };

    let mut rng = Rng(0xdead_beef_1234_5678);
    for iteration in 0..80 {
        // Areal pairs near each other so intersections actually happen.
        let mut pair_rng = Rng(rng.next());
        let a = random_geometry(&mut pair_rng);
        let b = random_geometry(&mut pair_rng);
        if matches!(a, Geometry::GeometryCollection(_))
            || matches!(b, Geometry::GeometryCollection(_))
        {
            continue;
        }
        let (a, b) = (a.to_wkt(), b.to_wkt());
        let g = |wkt: &str| format!("ST_GeomFromText('{wkt}')");

        let intersects_ab = flag(&mut sql, &format!("ST_Intersects({}, {})", g(&a), g(&b)));
        let intersects_ba = flag(&mut sql, &format!("ST_Intersects({}, {})", g(&b), g(&a)));
        assert_eq!(
            intersects_ab, intersects_ba,
            "symmetry {iteration}: {a} | {b}"
        );

        let disjoint = flag(&mut sql, &format!("ST_Disjoint({}, {})", g(&a), g(&b)));
        assert_eq!(
            disjoint, !intersects_ab,
            "disjoint law {iteration}: {a} | {b}"
        );

        let within = flag(&mut sql, &format!("ST_Within({}, {})", g(&a), g(&b)));
        if within {
            assert!(intersects_ab, "within⇒intersects {iteration}: {a} | {b}");
            assert!(
                flag(&mut sql, &format!("ST_Covers({}, {})", g(&b), g(&a))),
                "within⇒covered {iteration}: {a} | {b}"
            );
        }
        // Self-laws.
        assert!(flag(&mut sql, &format!("ST_Equals({}, {})", g(&a), g(&a))));
        assert!(!flag(
            &mut sql,
            &format!("ST_Disjoint({}, {})", g(&a), g(&a))
        ));
    }
}

#[test]
fn polar_and_antimeridian_sweep() {
    // Codec exactness at the extreme corners of the coordinate space.
    for wkt in [
        "POINT (180 90)",
        "POINT (-180 -90)",
        "POINT (0 90)",
        "LINESTRING (-180 89.9, 180 89.9)",
        "POLYGON ((-180 89, 180 89, 180 90, -180 90, -180 89))",
        "MULTIPOINT ((180 0), (-180 0), (179.9999999 -89.9999999))",
    ] {
        let geometry = Geometry::from_wkt(wkt).unwrap();
        assert_eq!(
            Geometry::from_wkb(&geometry.to_wkb()).unwrap(),
            geometry,
            "{wkt}"
        );
        assert_eq!(
            Geometry::from_bicdb_frame(&geometry.to_bicdb_frame()).unwrap(),
            geometry,
            "{wkt}"
        );
        assert_eq!(
            Geometry::from_geojson_value(geometry.to_geojson_value()).unwrap(),
            geometry,
            "{wkt}"
        );
    }
}

/// Differential oracle: when POSTGIS_URL points at a PostGIS instance and
/// psql is on PATH, sampled predicates must agree with PostGIS exactly.
/// Absent that environment the test reports itself skipped and passes —
/// an environment property, not a code property.
#[test]
fn postgis_differential_oracle_when_available() {
    let Ok(url) = std::env::var("POSTGIS_URL") else {
        eprintln!("skipping: POSTGIS_URL not set");
        return;
    };
    let psql = |query: &str| -> Option<String> {
        let output = std::process::Command::new("psql")
            .args([&url, "-tA", "-c", query])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
    };
    if psql("SELECT PostGIS_Version()").is_none() {
        eprintln!("skipping: psql/PostGIS unreachable");
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    let mut rng = Rng(0x0000_1e07_ac1e_0001);
    let mut compared = 0;
    for _ in 0..60 {
        let mut pair_rng = Rng(rng.next());
        let a = random_geometry(&mut pair_rng).to_wkt();
        let b = random_geometry(&mut pair_rng).to_wkt();
        for predicate in ["ST_Intersects", "ST_Within", "ST_Touches"] {
            let query =
                format!("SELECT {predicate}(ST_GeomFromText('{a}'), ST_GeomFromText('{b}'))");
            let Some(oracle) = psql(&query) else { continue };
            let ours = match sql.execute(&query).unwrap().rows[0][0] {
                SqlValue::Bool(value) => if value { "t" } else { "f" }.to_string(),
                ref other => panic!("expected bool, got {other:?}"),
            };
            assert_eq!(
                ours, oracle,
                "{predicate} disagrees with PostGIS: {a} | {b}"
            );
            compared += 1;
        }
    }
    eprintln!("PostGIS differential: {compared} predicate evaluations agreed");
}
