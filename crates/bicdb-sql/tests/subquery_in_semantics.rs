//! `x [NOT] IN (subquery)` keeps SQL's three-valued semantics whether the
//! subquery's result is probed through a hash table or scanned: NULLs on
//! either side, an empty result, duplicates, and mixed integer widths.

use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn values(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_cell())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn in_subquery_three_valued_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE o (k int4 PRIMARY KEY, v int4, t text);
             INSERT INTO o VALUES (1, 1, 'a'), (2, 2, 'b'), (3, NULL, NULL);
             CREATE TABLE s (k int4 PRIMARY KEY, w int4, u text);
             INSERT INTO s VALUES (1, 2, 'b'), (2, NULL, NULL), (3, 2, 'b');
             CREATE TABLE e (k int4 PRIMARY KEY, w int4);
             CREATE TABLE big (k int4 PRIMARY KEY, w int8);
             INSERT INTO big VALUES (1, 2), (2, 5000000000);",
        )
        .unwrap();
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT k FROM o WHERE v IN (SELECT w FROM s) ORDER BY k",
            &["2"],
        ),
        (
            "SELECT k FROM o WHERE v NOT IN (SELECT w FROM s) ORDER BY k",
            &[],
        ),
        (
            "SELECT k FROM o WHERE v NOT IN (SELECT w FROM s WHERE w IS NOT NULL) ORDER BY k",
            &["1"],
        ),
        (
            "SELECT k FROM o WHERE v IN (SELECT w FROM e) ORDER BY k",
            &[],
        ),
        (
            "SELECT k FROM o WHERE v NOT IN (SELECT w FROM e) ORDER BY k",
            &["1", "2", "3"],
        ),
        (
            "SELECT k FROM o WHERE t IN (SELECT u FROM s) ORDER BY k",
            &["2"],
        ),
        (
            "SELECT k FROM o WHERE t NOT IN (SELECT u FROM s WHERE k <> 2) ORDER BY k",
            &["1"],
        ),
        (
            "SELECT k FROM o WHERE v IN (SELECT w FROM big) ORDER BY k",
            &["2"],
        ),
        (
            "SELECT k FROM o WHERE NOT (v IN (SELECT w FROM s)) ORDER BY k",
            &[],
        ),
        (
            "SELECT k FROM o WHERE (v IN (SELECT w FROM s)) IS NULL ORDER BY k",
            &["1", "3"],
        ),
        (
            "SELECT count(*) FROM o WHERE v IN (SELECT w FROM s)",
            &["1"],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let got = values(&mut session, sql);
        if got != *expected {
            failures.push(format!("{sql}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn exists_subquery_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE o (k int4 PRIMARY KEY, v int4, t text);
             INSERT INTO o VALUES (1, 1, 'a'), (2, 2, 'b'), (3, NULL, NULL), (4, 4, 'd');
             CREATE TABLE s (k int4 PRIMARY KEY, w int4, u text, f int4);
             INSERT INTO s VALUES (1, 2, 'b', 0), (2, NULL, NULL, 0), (3, 2, 'b', 1), (4, 4, 'd', 1);
             CREATE TABLE e (k int4 PRIMARY KEY, w int4);
             CREATE TABLE big (k int4 PRIMARY KEY, w int8);
             INSERT INTO big VALUES (1, 1), (2, 5000000000);",
        )
        .unwrap();
    let cases: &[(&str, &[&str])] = &[
        ("SELECT k FROM o WHERE EXISTS (SELECT 1 FROM s WHERE s.w = o.v) ORDER BY k", &["2", "4"]),
        ("SELECT k FROM o WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.w = o.v) ORDER BY k", &["1", "3"]),
        (
            "SELECT k FROM o WHERE EXISTS (SELECT 1 FROM s WHERE s.w = o.v AND s.f = 1) ORDER BY k",
            &["2", "4"],
        ),
        (
            "SELECT k FROM o WHERE EXISTS (SELECT 1 FROM s WHERE o.v = s.w AND s.f = 0) ORDER BY k",
            &["2"],
        ),
        ("SELECT k FROM o WHERE EXISTS (SELECT 1 FROM e WHERE e.w = o.v) ORDER BY k", &[]),
        ("SELECT k FROM o WHERE NOT EXISTS (SELECT 1 FROM e WHERE e.w = o.v) ORDER BY k", &["1", "2", "3", "4"]),
        ("SELECT k FROM o WHERE EXISTS (SELECT 1 FROM s WHERE s.u = o.t) ORDER BY k", &["2", "4"]),
        ("SELECT k FROM o WHERE EXISTS (SELECT 1 FROM big WHERE big.w = o.v) ORDER BY k", &["1"]),
        (
            "SELECT k FROM o WHERE EXISTS (SELECT 1 FROM s WHERE s.w = o.v) OR o.k = 1 ORDER BY k",
            &["1", "2", "4"],
        ),
        ("SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM s WHERE s.w = o.v)", &["2"]),
        (
            "SELECT k FROM o x WHERE EXISTS (SELECT 1 FROM o WHERE o.v = x.k AND o.k > 1) ORDER BY k",
            &["2", "4"],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let got = values(&mut session, sql);
        if got != *expected {
            failures.push(format!("{sql}: got {got:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
