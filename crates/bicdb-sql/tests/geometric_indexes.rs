use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn plan_lines(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect()
}

fn int_rows(values: &[i64]) -> Vec<Vec<SqlValue>> {
    values
        .iter()
        .copied()
        .map(|value| vec![SqlValue::Int(value)])
        .collect()
}

#[test]
fn planar_geometric_indexes_plan_recheck_and_survive_restart() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE geometric_index_values (
                    id int4 PRIMARY KEY,
                    location point,
                    region box,
                    boundary polygon,
                    radius circle
                 );
                 INSERT INTO geometric_index_values VALUES
                    (1, '(0,0)', '(0,0),(2,2)', '((0,0),(2,0),(2,2),(0,2))', '<(0,0),2>'),
                    (2, '(2,2)', '(3,3),(5,5)', '((3,3),(5,3),(5,5),(3,5))', '<(5,5),1>'),
                    (3, '(5,1)', '(-3,-1),(-1,1)', '((-3,-1),(-1,-1),(-1,1),(-3,1))', '<(-2,0),1>');
                 CREATE INDEX geometric_location_gist
                    ON geometric_index_values USING gist (location);
                 CREATE INDEX geometric_region_spgist
                    ON geometric_index_values USING spgist (region);
                 CREATE INDEX geometric_boundary_gist
                    ON geometric_index_values USING gist (boundary);
                 CREATE INDEX geometric_radius_gist
                    ON geometric_index_values USING gist (radius);",
            )
            .unwrap();

        assert!(plan_lines(
            &mut session,
            "SELECT id FROM geometric_index_values WHERE location << point(3,0)",
        )
        .iter()
        .any(|line| line == "GeometricIndexScan geometric_location_gist"));
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM geometric_index_values
             WHERE region && '(1,1),(4,4)'::box",
        )
        .iter()
        .any(|line| line == "GeometricIndexScan geometric_region_spgist"));
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM geometric_index_values
             ORDER BY location <-> point(0,0) LIMIT 2",
        )
        .iter()
        .any(|line| line == "GeometricKnnIndexScan geometric_location_gist"));

        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE location << point(3,0) ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2]),
        );
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM geometric_index_values WHERE point(3,0) >> location",
        )
        .iter()
        .any(|line| line == "GeometricIndexScan geometric_location_gist"));
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE point(3,0) >> location ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2]),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE region && '(1,1),(4,4)'::box ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2]),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE boundary @> '((1,1),(1.5,1),(1.5,1.5))'::polygon
                     ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1]),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE radius && '<(1,0),1>'::circle ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1]),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     ORDER BY location <-> point(0,0) LIMIT 2",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2]),
        );

        session
            .execute("UPDATE geometric_index_values SET location = point(1,1) WHERE id = 3")
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE location << point(3,0) ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2, 3]),
        );

        session
            .execute(
                "INSERT INTO geometric_index_values (id, location) VALUES (4, NULL);
                 UPDATE geometric_index_values SET location = point(1,1) WHERE id = 4;",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE location << point(3,0) ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 2, 3, 4]),
        );
        session
            .execute(
                "UPDATE geometric_index_values SET location = NULL WHERE id = 4;
                 DELETE FROM geometric_index_values WHERE id = 2;",
            )
            .unwrap();
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE location << point(3,0) ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1, 3]),
        );

        session
            .execute(
                "CREATE INDEX geometric_location_kd
                 ON geometric_index_values USING spgist (location kd_point_ops);
                 CREATE INDEX geometric_region_brin
                 ON geometric_index_values USING brin (region);
                 DROP INDEX geometric_region_spgist;",
            )
            .unwrap();
        assert!(plan_lines(
            &mut session,
            "SELECT id FROM geometric_index_values WHERE region @> point(1,1)",
        )
        .iter()
        .any(|line| line == "GeometricIndexScan geometric_region_brin"));
        assert_eq!(
            session
                .execute(
                    "SELECT id FROM geometric_index_values
                     WHERE region @> point(1,1) ORDER BY id",
                )
                .unwrap()
                .rows,
            int_rows(&[1]),
        );
        assert_eq!(
            session
                .execute(
                    "SELECT opcname FROM pg_opclass
                     WHERE opcmethod IN (783, 3580, 4000)
                       AND opcintype IN (600, 603, 604, 718)
                     ORDER BY oid",
                )
                .unwrap()
                .rows
                .len(),
            9,
        );
        assert_eq!(
            session
                .execute(
                    "SELECT indexname FROM pg_indexes
                     WHERE tablename = 'geometric_index_values'
                       AND indexname LIKE 'geometric_%'
                     ORDER BY indexname",
                )
                .unwrap()
                .rows
                .len(),
            6,
        );
    }

    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        session
            .execute(
                "SELECT id FROM geometric_index_values
                 WHERE location << point(3,0) ORDER BY id",
            )
            .unwrap()
            .rows,
        int_rows(&[1, 3]),
    );
    assert!(plan_lines(
        &mut session,
        "SELECT id FROM geometric_index_values
         WHERE region && '(1,1),(4,4)'::box",
    )
    .iter()
    .any(|line| line.starts_with("GeometricIndexScan geometric_region_")));
}

#[test]
fn planar_geometric_indexes_reject_missing_operator_classes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE invalid_geometric_indexes (
                id int4 PRIMARY KEY,
                p point,
                l line,
                b box,
                c circle
             )",
        )
        .unwrap();

    for (sql, sqlstate) in [
        (
            "CREATE INDEX invalid_point_btree ON invalid_geometric_indexes (p)",
            "42704",
        ),
        (
            "CREATE INDEX invalid_line_gist ON invalid_geometric_indexes USING gist (l)",
            "42704",
        ),
        (
            "CREATE INDEX invalid_circle_spgist ON invalid_geometric_indexes USING spgist (c)",
            "42704",
        ),
        (
            "CREATE INDEX invalid_box_opclass ON invalid_geometric_indexes USING gist (b point_ops)",
            "42804",
        ),
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), sqlstate);
    }
}

#[test]
fn multicolumn_gist_indexes_each_geometric_column_and_drop_as_one_object() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE multicolumn_geometric_index (
                id int4 PRIMARY KEY,
                location point,
                region box
             );
             INSERT INTO multicolumn_geometric_index VALUES
                (1, '(0,0)', '(0,0),(2,2)'),
                (2, '(5,5)', '(4,4),(6,6)');
             CREATE INDEX multicolumn_geometric_gist
                ON multicolumn_geometric_index USING gist (location, region);",
        )
        .unwrap();

    for sql in [
        "SELECT id FROM multicolumn_geometric_index WHERE location << point(3,0)",
        "SELECT id FROM multicolumn_geometric_index WHERE region && '(1,1),(3,3)'::box",
    ] {
        assert!(plan_lines(&mut session, sql)
            .iter()
            .any(|line| line == "GeometricIndexScan multicolumn_geometric_gist"));
    }

    session
        .execute("DROP INDEX multicolumn_geometric_gist")
        .unwrap();
    assert!(!plan_lines(
        &mut session,
        "SELECT id FROM multicolumn_geometric_index WHERE location << point(3,0)",
    )
    .iter()
    .any(|line| line.starts_with("GeometricIndexScan")));
}

/// B10 resolved: `ORDER BY <-> LIMIT k` pushes a static bound into the
/// k-NN index scan when nothing downstream can filter or extend the
/// candidate set — and declines back to exhaustive whenever something can.
#[test]
fn knn_limit_pushdown_is_bounded_correct_and_declines_when_unsound() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE spots (id int4 PRIMARY KEY, location point, kind text)")
        .unwrap();
    for index in 0..20 {
        session
            .execute(&format!(
                "INSERT INTO spots VALUES ({index}, point({index}, {index}), '{}')",
                if index % 2 == 0 { "cafe" } else { "bar" }
            ))
            .unwrap();
    }
    session
        .execute("CREATE INDEX spots_location_gist ON spots USING gist (location)")
        .unwrap();

    // Pushdown engaged: the scan is bounded by LIMIT, not the table size.
    let plan = plan_lines(
        &mut session,
        "SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT 4",
    );
    assert!(plan.iter().any(|line| line == "Limit 4"), "plan: {plan:?}");

    // True nearest, in distance order, from the bounded scan.
    assert_eq!(
        session
            .execute("SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT 4")
            .unwrap()
            .rows,
        int_rows(&[0, 1, 2, 3]),
    );
    // Query point away from the origin: order is by distance, not id.
    assert_eq!(
        session
            .execute("SELECT id FROM spots ORDER BY location <-> point(7.4, 7.4) LIMIT 3")
            .unwrap()
            .rows,
        int_rows(&[7, 8, 6]),
    );
    // OFFSET widens the pushed bound; the offset rows are the true nearest.
    let plan = plan_lines(
        &mut session,
        "SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT 2 OFFSET 2",
    );
    assert!(plan.iter().any(|line| line == "Limit 4"), "plan: {plan:?}");
    assert_eq!(
        session
            .execute("SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT 2 OFFSET 2")
            .unwrap()
            .rows,
        int_rows(&[2, 3]),
    );

    // `<->` ORDER BY keys are REAL values now: projecting the distance
    // returns euclidean distance, not Null (the silent-Null half of B10).
    let rows = session
        .execute("SELECT id, location <-> point(0,0) FROM spots ORDER BY 2 LIMIT 2")
        .unwrap()
        .rows;
    assert_eq!(rows[0][0], SqlValue::Int(0));
    assert_eq!(rows[0][1], SqlValue::Float(0.0));
    assert_eq!(rows[1][0], SqlValue::Int(1));
    assert_eq!(rows[1][1], SqlValue::Float(std::f64::consts::SQRT_2));

    // A WHERE clause filters AFTER the scan: the plan must stay exhaustive
    // and still produce k matching rows.
    let plan = plan_lines(
        &mut session,
        "SELECT id FROM spots WHERE kind = 'cafe' ORDER BY location <-> point(0,0) LIMIT 3",
    );
    assert!(plan.iter().any(|line| line == "Limit 20"), "plan: {plan:?}");
    assert_eq!(
        session
            .execute(
                "SELECT id FROM spots WHERE kind = 'cafe' \
                 ORDER BY location <-> point(0,0) LIMIT 3"
            )
            .unwrap()
            .rows,
        int_rows(&[0, 2, 4]),
    );

    // A dynamic LIMIT cannot be bounded statically.
    let plan = plan_lines(
        &mut session,
        "SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT (SELECT 4)",
    );
    assert!(plan.iter().any(|line| line == "Limit 20"), "plan: {plan:?}");

    // Pending transaction writes must stay visible: a nearer uncommitted row
    // wins even though it is not in the index snapshot the scan would use.
    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO spots VALUES (99, point(0.1, 0.1), 'popup')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM spots ORDER BY location <-> point(0,0) LIMIT 2")
            .unwrap()
            .rows,
        int_rows(&[0, 99]),
    );
    session.execute("ROLLBACK").unwrap();
}

/// Non-point geometric k-NN orders by true shape distance, which the MBR-
/// ordered index cannot guarantee for k < total: the plan must stay
/// exhaustive and the results must still be shape-distance ordered.
#[test]
fn knn_on_box_columns_stays_exhaustive_and_shape_ordered() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE zones (id int4 PRIMARY KEY, region box);
             INSERT INTO zones VALUES
                (1, '(1,1),(2,2)'),
                (2, '(4,4),(6,6)'),
                (3, '(-3,-3),(-2,-2)');
             CREATE INDEX zones_region_gist ON zones USING gist (region);",
        )
        .unwrap();
    let plan = plan_lines(
        &mut session,
        "SELECT id FROM zones ORDER BY region <-> point(0,0) LIMIT 2",
    );
    assert!(
        plan.iter()
            .any(|line| line == "GeometricKnnIndexScan zones_region_gist"),
        "plan: {plan:?}"
    );
    assert!(plan.iter().any(|line| line == "Limit 3"), "plan: {plan:?}");
    assert_eq!(
        session
            .execute("SELECT id FROM zones ORDER BY region <-> point(0,0) LIMIT 2")
            .unwrap()
            .rows,
        int_rows(&[1, 3]),
    );
}

/// CONCURRENTLY is refused, not silently stripped: a client asking for a
/// non-blocking build must not get a blocking one unknowingly. DROP's
/// variant still maps to plain DROP (a drop cannot stall for hours).
#[test]
fn create_index_concurrently_is_rejected_with_guidance() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE t (id int4 PRIMARY KEY, name text)")
        .unwrap();
    let error = session
        .execute("CREATE INDEX CONCURRENTLY t_name ON t (name)")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("create-index-concurrently-design"),
        "rejection should cite the design doc: {error}"
    );
    session.execute("CREATE INDEX t_name ON t (name)").unwrap();
    session.execute("DROP INDEX CONCURRENTLY t_name").unwrap();
}
