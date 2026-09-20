//! G12 acceptance: the `CREATE CUBE` DDL the campaign deliberately withheld
//! until the engine underneath it was real.
//!
//! The syntax describes what the engine can actually maintain — additive
//! measures it can retract, sketches it can rebuild and merge — rather than
//! promising an aggregate vocabulary and discovering later that half of it
//! cannot be kept up to date.

use bicdb_core::{BicDb, DbConfig, Record};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;

fn seeded() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    db.create_collection("sales").unwrap();
    let regions = ["north", "south"];
    let categories = ["pizza", "sushi"];
    for index in 0..400 {
        db.insert(
            "sales",
            Record::new(format!("s{index}")).with_metadata(json!({
                "region": regions[index % 2],
                "category": categories[(index / 2) % 2],
                "host": format!("host{}", index % 15),
                "price": (index % 60) as f64,
                // A unique value per row — the grain that must be refused.
                "receipt": format!("r{index}"),
            })),
        )
        .unwrap();
    }
    (dir, db)
}

#[test]
fn create_cube_builds_a_queryable_cube() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let created = sql
        .execute(
            "CREATE CUBE sales_cube ON sales \
             DIMENSIONS (region, category) \
             MEASURES (SUM(price))",
        )
        .unwrap();
    assert_eq!(created.rows.len(), 1);
    assert_eq!(created.rows[0][0], SqlValue::String("sales_cube".into()));
    assert_eq!(
        created.rows[0][1],
        SqlValue::String("bicdb_projection.sales_cube".into())
    );
    assert_eq!(created.rows[0][2], SqlValue::Int(4));

    // And it is immediately queryable as a relation.
    let rows = sql
        .execute("SELECT region, category, count FROM bicdb_projection.sales_cube")
        .unwrap();
    assert_eq!(rows.rows.len(), 4);
    let total: i64 = rows
        .rows
        .iter()
        .map(|row| match row[2] {
            SqlValue::Int(count) => count,
            _ => panic!("count is not an integer"),
        })
        .sum();
    assert_eq!(total, 400);
}

/// Sketch measures declared through the DDL.
#[test]
fn a_cube_can_declare_sketch_measures() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE CUBE sales_rich ON sales \
         DIMENSIONS (region) \
         MEASURES (SUM(price), COUNT(DISTINCT host), PERCENTILES(price))",
    )
    .unwrap();

    let rows = sql
        .execute("SELECT region, count, distinct_host, p50_price FROM bicdb_projection.sales_rich")
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    for row in &rows.rows {
        // Regions take alternate rows and there are 15 hosts, so each region
        // sees all 15 of them.
        assert_eq!(
            row[2],
            SqlValue::Int(15),
            "distinct hosts per region should be 15"
        );
        assert!(matches!(row[3], SqlValue::Float(_)), "no p50 was produced");
    }
}

/// The estimator has to REFUSE an absurd grain, not build it and then report
/// how much memory it took.
#[test]
fn an_absurd_grain_is_refused_before_it_is_built() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("CREATE CUBE per_receipt ON sales DIMENSIONS (receipt) MEASURES (SUM(price))")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("refusing to build cube"),
        "an absurd grain was not refused: {error}"
    );
    assert!(
        error.contains("WITH FORCE"),
        "the refusal did not say how to override it: {error}"
    );
    // And nothing was left behind.
    assert!(sql
        .execute("SELECT * FROM bicdb_projection.per_receipt")
        .is_err());
}

/// The override exists, and it works — a refusal that cannot be overridden is
/// a policy, not an estimate.
#[test]
fn with_force_builds_the_refused_cube() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    let created = sql
        .execute(
            "CREATE CUBE per_receipt ON sales DIMENSIONS (receipt) \
             MEASURES (SUM(price)) WITH FORCE",
        )
        .unwrap();
    assert_eq!(created.rows[0][2], SqlValue::Int(400));
}

#[test]
fn a_cube_tracks_later_writes() {
    let (_dir, mut db) = seeded();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE CUBE live ON sales DIMENSIONS (region) MEASURES (SUM(price))")
            .unwrap();
    }
    db.insert(
        "sales",
        Record::new("extra").with_metadata(json!({
            "region": "north", "category": "pizza", "host": "hostX",
            "price": 1.0, "receipt": "rX",
        })),
    )
    .unwrap();

    let mut sql = SqlSession::new(&mut db);
    let rows = sql
        .execute("SELECT region, count FROM bicdb_projection.live WHERE region = 'north'")
        .unwrap();
    assert_eq!(
        rows.rows[0][1],
        SqlValue::Int(201),
        "the cube did not follow the write"
    );
}

#[test]
fn refresh_cube_reports_what_it_applied() {
    let (_dir, mut db) = seeded();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE CUBE refreshed ON sales DIMENSIONS (region) MEASURES (SUM(price))")
            .unwrap();
    }
    for index in 0..5 {
        db.insert(
            "sales",
            Record::new(format!("late{index}")).with_metadata(json!({
                "region": "south", "category": "sushi", "host": "hostY",
                "price": 2.0, "receipt": format!("rl{index}"),
            })),
        )
        .unwrap();
    }
    let mut sql = SqlSession::new(&mut db);
    let result = sql.execute("REFRESH CUBE refreshed").unwrap();
    assert_eq!(result.rows[0][0], SqlValue::Int(5));
    assert_eq!(result.rows[0][1], SqlValue::Int(2));
}

#[test]
fn a_cube_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_audit_events(true),
        )
        .unwrap();
        db.create_collection("sales").unwrap();
        for index in 0..50 {
            db.insert(
                "sales",
                Record::new(format!("s{index}")).with_metadata(json!({
                    "region": if index % 2 == 0 { "north" } else { "south" },
                    "price": index as f64,
                })),
            )
            .unwrap();
        }
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE CUBE durable ON sales DIMENSIONS (region) MEASURES (SUM(price))")
            .unwrap();
    }

    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_audit_events(true),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    let rows = sql
        .execute("SELECT region, count FROM bicdb_projection.durable")
        .unwrap();
    assert_eq!(rows.rows.len(), 2, "the cube did not survive the restart");

    // And it is listed in the catalog.
    let listed = sql.execute("SELECT name FROM bicdb_projections").unwrap();
    assert!(listed
        .rows
        .iter()
        .any(|row| row[0] == SqlValue::String("durable".into())));
}

#[test]
fn drop_cube_removes_it() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE CUBE doomed ON sales DIMENSIONS (region) MEASURES (SUM(price))")
        .unwrap();
    sql.execute("SELECT * FROM bicdb_projection.doomed")
        .unwrap();

    sql.execute("DROP CUBE doomed").unwrap();
    assert!(sql
        .execute("SELECT * FROM bicdb_projection.doomed")
        .is_err());

    // Dropping again is an error, unless IF EXISTS.
    assert!(sql.execute("DROP CUBE doomed").is_err());
    sql.execute("DROP CUBE IF EXISTS doomed").unwrap();
}

#[test]
fn creating_the_same_cube_twice_is_an_error() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE CUBE once ON sales DIMENSIONS (region) MEASURES (SUM(price))")
        .unwrap();
    let error = sql
        .execute("CREATE CUBE once ON sales DIMENSIONS (region) MEASURES (SUM(price))")
        .unwrap_err()
        .to_string();
    assert!(error.contains("already exists"), "unhelpful error: {error}");
}

#[test]
fn malformed_cube_ddl_is_a_clear_error() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    for (statement, expected) in [
        ("CREATE CUBE bad ON sales", "DIMENSIONS"),
        ("CREATE CUBE bad DIMENSIONS (region)", "ON <table>"),
        (
            "CREATE CUBE bad ON sales DIMENSIONS ()",
            "at least one dimension",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "`{statement}` gave an unhelpful error: {error}"
        );
    }
}

/// A cube with no measures at all is legitimate: `COUNT(*)` by grain.
#[test]
fn a_count_only_cube_is_allowed() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE CUBE counted ON sales DIMENSIONS (region, category)")
        .unwrap();
    let rows = sql
        .execute("SELECT region, category, count FROM bicdb_projection.counted")
        .unwrap();
    assert_eq!(rows.rows.len(), 4);
}

/// Cubes compose with the rest of the campaign: G8 rolls one up.
#[test]
fn a_cube_can_be_rolled_up() {
    let (_dir, mut db) = seeded();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE CUBE rollable ON sales DIMENSIONS (region, category) \
         MEASURES (SUM(price), COUNT(DISTINCT host))",
    )
    .unwrap();
    let rolled = sql
        .execute("ROLLUP MATERIALIZED AGGREGATE rollable TO (category WHOLE)")
        .unwrap();
    assert_eq!(rolled.rows.len(), 2, "expected one row per region");
    let distinct = rolled
        .columns
        .iter()
        .position(|column| column == "distinct_host")
        .unwrap();
    for row in &rolled.rows {
        // Each (region, category) cell already sees all 15 hosts, so ADDING
        // the two categories would give 30. Merging gives the truth.
        assert_eq!(
            row[distinct],
            SqlValue::Int(15),
            "rolled distinct hosts were added rather than merged"
        );
    }
}

/// A cube name becomes a filesystem path component, so an unvalidated one
/// was a cross-tenant destruction primitive: `DROP CUBE "../../<other>/…"`
/// escaped the data directory and `remove_dir_all`'d whatever it landed on,
/// and every tenant's cubes share the `.projection` suffix that made such a
/// landing likely. The DDL was also reachable by any SQL user with no
/// privilege check at all.
#[test]
fn cube_names_cannot_traverse_out_of_the_projections_directory() {
    let (directory, mut db) = seeded();

    // A victim directory beside the data dir, standing in for another
    // tenant's database.
    let victim = directory.path().join("victim.projection");
    std::fs::create_dir_all(victim.join("pages")).unwrap();
    std::fs::write(victim.join("manifest.json"), b"{}").unwrap();

    let mut sql = SqlSession::new(&mut db);
    for name in [
        "../victim",
        "../../victim",
        "/etc/bicdb-victim",
        "..",
        "sales/../../victim",
        "sales\\\\..\\\\victim",
    ] {
        for statement in [
            format!("DROP CUBE \"{name}\""),
            format!("REFRESH CUBE \"{name}\""),
            format!("CREATE CUBE \"{name}\" ON sales DIMENSIONS (region) MEASURES (SUM(price))"),
        ] {
            let error = sql
                .execute(&statement)
                .expect_err("a traversing cube name must be refused");
            let rendered = format!("{error}");
            assert!(
                rendered.contains("invalid projection name")
                    || rendered.contains("projection name"),
                "unexpected refusal for `{statement}`: {rendered}"
            );
        }
    }

    // The victim survived every attempt.
    assert!(
        victim.join("manifest.json").exists(),
        "cube DDL deleted a directory outside the projections tree"
    );
}
