//! `col BETWEEN low AND high` on an indexed column is answered with an index
//! range probe (it is exactly `col >= low AND col <= high`) and returns the
//! same rows as an unindexed evaluation of the same predicate.

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn ids(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    let mut ids = session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn plan(session: &mut SqlSession<'_>, sql: &str) -> String {
    session
        .execute(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n")
}

fn setup(db: &mut BicDb) {
    SqlSession::new(db)
        .execute(
            "CREATE TABLE r (id int4 PRIMARY KEY, d int4, n numeric, t text, k int4);
             INSERT INTO r SELECT g, g % 500, g % 37, 'x' || (g % 11), g % 3
               FROM generate_series(1, 5000) g;
             INSERT INTO r VALUES (5001, NULL, NULL, NULL, NULL);
             CREATE INDEX r_d ON r (d);
             CREATE INDEX r_k_d ON r (k, d);
             CREATE INDEX r_n ON r (n);
             CREATE INDEX r_t ON r (t);",
        )
        .unwrap();
}

#[test]
fn between_matches_unindexed_evaluation() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    let mut session = SqlSession::new(&mut db);
    for (indexed, unindexed) in [
        ("d BETWEEN 10 AND 12", "d + 0 BETWEEN 10 AND 12"),
        ("d BETWEEN 12 AND 10", "d + 0 BETWEEN 12 AND 10"),
        ("d BETWEEN 499 AND 1000", "d + 0 BETWEEN 499 AND 1000"),
        ("d BETWEEN -5 AND 0", "d + 0 BETWEEN -5 AND 0"),
        ("d BETWEEN NULL AND 10", "d + 0 BETWEEN NULL AND 10"),
        ("d BETWEEN 10 AND NULL", "d + 0 BETWEEN 10 AND NULL"),
        ("d NOT BETWEEN 1 AND 498", "d + 0 NOT BETWEEN 1 AND 498"),
        (
            "d BETWEEN 10 AND 12 AND d <> 11",
            "d + 0 BETWEEN 10 AND 12 AND d + 0 <> 11",
        ),
        (
            "d BETWEEN 10 AND 20 AND d > 15",
            "d + 0 BETWEEN 10 AND 20 AND d + 0 > 15",
        ),
        (
            "(d BETWEEN 10 AND 12) OR id = 1",
            "(d + 0 BETWEEN 10 AND 12) OR id = 1",
        ),
        // Fractional bounds on an integer index.
        ("d BETWEEN 9.5 AND 11.5", "d + 0 BETWEEN 9.5 AND 11.5"),
        ("d BETWEEN 10.2 AND 10.8", "d + 0 BETWEEN 10.2 AND 10.8"),
        ("d > 10.6 AND d < 12.4", "d + 0 > 10.6 AND d + 0 < 12.4"),
        // Composite index: equality prefix, then BETWEEN on the next field.
        (
            "k = 1 AND d BETWEEN 100 AND 110",
            "k + 0 = 1 AND d + 0 BETWEEN 100 AND 110",
        ),
        // Numeric and text indexes.
        ("n BETWEEN 3.5 AND 6", "n + 0 BETWEEN 3.5 AND 6"),
        ("t BETWEEN 'x2' AND 'x4'", "t || '' BETWEEN 'x2' AND 'x4'"),
        // Bounds from an uncorrelated scalar subquery.
        (
            "d BETWEEN (SELECT min(k) FROM r) AND (SELECT max(k) FROM r)",
            "d + 0 BETWEEN (SELECT min(k) FROM r) AND (SELECT max(k) FROM r)",
        ),
    ] {
        let expected = ids(&mut session, &format!("SELECT id FROM r WHERE {unindexed}"));
        let actual = ids(&mut session, &format!("SELECT id FROM r WHERE {indexed}"));
        assert_eq!(actual, expected, "{indexed}");
        let counted = session
            .execute(&format!("SELECT count(*) FROM r WHERE {indexed}"))
            .unwrap();
        assert_eq!(
            counted.rows[0][0],
            SqlValue::Int(expected.len() as i64),
            "count {indexed}"
        );
    }
}

#[test]
fn between_on_an_indexed_column_plans_an_index_range() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    let mut session = SqlSession::new(&mut db);
    let explained = plan(&mut session, "SELECT id FROM r WHERE d BETWEEN 10 AND 12");
    assert!(explained.contains("IndexRangeScan r_d"), "{explained}");
    let explained = plan(
        &mut session,
        "SELECT id FROM r WHERE d NOT BETWEEN 10 AND 12",
    );
    assert!(!explained.contains("IndexRange"), "{explained}");
}

#[test]
fn between_correlated_to_another_table_still_joins_correctly() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    setup(&mut db);
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE w (id int4 PRIMARY KEY, lo int4, hi int4);
             INSERT INTO w VALUES (1, 10, 11), (2, 400, 402), (3, 7, 3);",
        )
        .unwrap();
    let joined = |session: &mut SqlSession<'_>, predicate: &str| {
        let mut rows = session
            .execute(&format!(
                "SELECT w.id, r.id FROM w JOIN r ON {predicate} ORDER BY w.id, r.id"
            ))
            .unwrap()
            .rows
            .into_iter()
            .map(|row| format!("{}:{}", row[0].to_cell(), row[1].to_cell()))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = joined(&mut session, "r.d + 0 BETWEEN w.lo AND w.hi");
    assert!(!expected.is_empty());
    assert_eq!(joined(&mut session, "r.d BETWEEN w.lo AND w.hi"), expected);
    let expected = session
        .execute(
            "SELECT count(*) FROM w WHERE EXISTS (SELECT 1 FROM r WHERE r.d + 0 BETWEEN w.lo AND w.hi)",
        )
        .unwrap()
        .rows;
    let actual = session
        .execute(
            "SELECT count(*) FROM w WHERE EXISTS (SELECT 1 FROM r WHERE r.d BETWEEN w.lo AND w.hi)",
        )
        .unwrap()
        .rows;
    assert_eq!(actual, expected);
}
