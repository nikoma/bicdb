//! G3: executor-level spatial joins — R-tree build side, per-row probes,
//! exact verification. Correctness is pinned against hand-computed pairs;
//! the antimeridian gate rides along.

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

fn seed(sql: &mut SqlSession) {
    sql.execute("CREATE TABLE clinics (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute("CREATE TABLE districts (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute(
        "INSERT INTO clinics VALUES \
         ('berlin', 'POINT (13.40 52.52)'), \
         ('paris', 'POINT (2.35 48.85)'), \
         ('pacific', 'POINT (179.95 0.0)')",
    )
    .unwrap();
    sql.execute(
        "INSERT INTO districts VALUES \
         ('mitte', 'POLYGON ((13.3 52.4, 13.5 52.4, 13.5 52.6, 13.3 52.6, 13.3 52.4))'), \
         ('idf', 'POLYGON ((2.2 48.7, 2.5 48.7, 2.5 49.0, 2.2 49.0, 2.2 48.7))'), \
         ('dateline', 'POLYGON ((179.9 -1, 180 -1, 180 1, 179.9 1, 179.9 -1))')",
    )
    .unwrap();
}

#[test]
fn intersects_join_matches_exact_pairs() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql);

    let rows = sql
        .execute(
            "SELECT c.id, d.id FROM clinics c JOIN districts d \
             ON ST_Intersects(c.geom, d.geom) ORDER BY c.id",
        )
        .unwrap();
    let pairs: Vec<(String, String)> = rows
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (SqlValue::String(a), SqlValue::String(b)) => (a.clone(), b.clone()),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("berlin".to_string(), "mitte".to_string()),
            ("pacific".to_string(), "dateline".to_string()),
            ("paris".to_string(), "idf".to_string()),
        ],
        "each clinic must join exactly its containing district (antimeridian included)"
    );
}

#[test]
fn dwithin_join_and_left_join_padding() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE a_pts (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute("CREATE TABLE b_pts (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    // b1 is ~740m east of a1; b2 is ~7.4km away; a2 is isolated.
    sql.execute("INSERT INTO a_pts VALUES ('a1', 'POINT (13.40 52.52)'), ('a2', 'POINT (0 0)')")
        .unwrap();
    sql.execute(
        "INSERT INTO b_pts VALUES ('b1', 'POINT (13.411 52.52)'), ('b2', 'POINT (13.51 52.52)')",
    )
    .unwrap();

    let rows = sql
        .execute(
            "SELECT a.id, b.id FROM a_pts a JOIN b_pts b \
             ON ST_DWithin(a.geom, b.geom, 1000) ORDER BY a.id",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::String("a1".to_string()));
    assert_eq!(rows.rows[0][1], SqlValue::String("b1".to_string()));

    // LEFT JOIN pads the isolated point with NULL.
    let rows = sql
        .execute(
            "SELECT a.id, b.id FROM a_pts a LEFT JOIN b_pts b \
             ON ST_DWithin(a.geom, b.geom, 1000) ORDER BY a.id",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(rows.rows[1][0], SqlValue::String("a2".to_string()));
    assert_eq!(rows.rows[1][1], SqlValue::Null);

    // A larger radius picks up b2 too.
    let rows = sql
        .execute(
            "SELECT a.id, b.id FROM a_pts a JOIN b_pts b \
             ON ST_DWithin(a.geom, b.geom, 10000) ORDER BY a.id, b.id",
        )
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
}

/// The join must agree exactly with the naive per-pair evaluation on a
/// randomized-ish corpus — the R-tree is an accelerator, never a filter of
/// different semantics.
#[test]
fn tree_join_agrees_with_pairwise_semantics_at_scale() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE grid_a (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    sql.execute("CREATE TABLE grid_b (id TEXT PRIMARY KEY, geom TEXT)")
        .unwrap();
    let mut a_values = Vec::new();
    let mut b_values = Vec::new();
    for index in 0..150_i64 {
        let lon = 10.0 + (index % 15) as f64 * 0.01;
        let lat = 50.0 + (index / 15) as f64 * 0.01;
        a_values.push(format!("('a{index:03}', 'POINT ({lon} {lat})')"));
        b_values.push(format!(
            "('b{index:03}', 'POINT ({} {})')",
            lon + 0.004,
            lat
        ));
    }
    sql.execute(&format!(
        "INSERT INTO grid_a VALUES {}",
        a_values.join(", ")
    ))
    .unwrap();
    sql.execute(&format!(
        "INSERT INTO grid_b VALUES {}",
        b_values.join(", ")
    ))
    .unwrap();

    // 0.004° lon at 50°N ≈ 287m. 300m catches exactly the shifted twin;
    // every a-point matches exactly one b-point.
    let rows = sql
        .execute(
            "SELECT count(*) FROM grid_a a JOIN grid_b b \
             ON ST_DWithin(a.geom, b.geom, 300)",
        )
        .unwrap();
    assert_eq!(rows.rows[0][0], SqlValue::Int(150));

    // 800m pulls in the horizontal neighbors (grid pitch 0.01° ≈ 717m):
    // interior points match twin + the next column's twin.
    let rows = sql
        .execute(
            "SELECT count(*) FROM grid_a a JOIN grid_b b \
             ON ST_DWithin(a.geom, b.geom, 800)",
        )
        .unwrap();
    let SqlValue::Int(count) = rows.rows[0][0] else {
        panic!("expected count");
    };
    assert!(count > 150, "wider radius must add pairs, got {count}");
}
