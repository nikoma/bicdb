//! A correlated EXISTS over a table without a primary key is answered from one scan of the inner
//! table instead of a rescan per outer row, and a recursive UNION over a type without hashing is
//! refused like PostgreSQL instead of iterating forever.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;
use std::time::Duration;

fn cells(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

#[test]
fn recursive_search_and_cycle_expose_rank_mark_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE edges(parent int, child int); INSERT INTO edges VALUES (1,2),(1,3),(2,4),(3,4),(4,1)")
        .unwrap();
    let rows = cells(
        &mut db,
        "WITH RECURSIVE walk(n) AS (
        SELECT 1 UNION ALL SELECT e.child FROM walk w JOIN edges e ON e.parent = w.n
    ) SEARCH DEPTH FIRST BY n SET ord CYCLE n SET hit USING path
    SELECT n, hit, ord::text, path::text FROM walk ORDER BY walk.ord",
    );
    let expected = [
        ["1", "false", "{(1)}", "{(1)}"],
        ["2", "false", "{(1),(2)}", "{(1),(2)}"],
        ["4", "false", "{(1),(2),(4)}", "{(1),(2),(4)}"],
        ["1", "true", "{(1),(2),(4),(1)}", "{(1),(2),(4),(1)}"],
        ["3", "false", "{(1),(3)}", "{(1),(3)}"],
        ["4", "false", "{(1),(3),(4)}", "{(1),(3),(4)}"],
        ["1", "true", "{(1),(3),(4),(1)}", "{(1),(3),(4),(1)}"],
    ];
    assert_eq!(rows, expected.map(|row| row.map(String::from).to_vec()));

    let rows = cells(&mut db, "WITH RECURSIVE walk(n, depth) AS (
        SELECT 1,0 UNION ALL SELECT e.child,w.depth+1 FROM walk w JOIN edges e ON e.parent = w.n WHERE w.depth < 2
    ) SEARCH BREADTH FIRST BY n SET ord
    SELECT n, ord::text FROM walk ORDER BY walk.ord");
    assert_eq!(
        rows,
        [
            ["1", "(0,1)"],
            ["2", "(1,2)"],
            ["3", "(1,3)"],
            ["4", "(2,4)"],
            ["4", "(2,4)"]
        ]
        .map(|row| row.map(String::from).to_vec())
    );
}

#[test]
fn correlated_exists_over_a_table_without_primary_key_is_not_quadratic() {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        cells(
            &mut db,
            "CREATE TABLE tk (unique1 int, unique2 int, thousand int)",
        );
        cells(
            &mut db,
            "INSERT INTO tk SELECT g, (g * 7) % 6000, g % 1000 FROM generate_series(0, 5999) g",
        );
        let exists = cells(
            &mut db,
            "select count(*), max(unique1) from tk a \
             where exists (select 1 from tk b where b.thousand = a.unique2)",
        );
        let not_exists = cells(
            &mut db,
            "select count(*) from tk a \
             where not exists (select 1 from tk b where b.thousand = a.unique2 and b.unique1 > 10)",
        );
        let with_second_correlation = cells(
            &mut db,
            "select count(*) from tk a \
             where exists (select 1 from tk b where b.thousand = a.unique2 and b.unique1 = a.unique1)",
        );
        sender
            .send((exists, not_exists, with_second_correlation))
            .unwrap();
    });
    let (exists, not_exists, with_second_correlation) = receiver
        .recv_timeout(Duration::from_secs(240))
        .expect("correlated EXISTS over 6000 x 6000 rows must not rescan the inner table per row");
    // Brute force: unique2 = (g*7) % 6000 is in 0..999 for exactly the g where that holds.
    let matching = (0..6000)
        .filter(|g| (g * 7) % 6000 < 1000)
        .collect::<Vec<i64>>();
    assert_eq!(
        exists,
        [[
            matching.len().to_string(),
            matching.iter().max().unwrap().to_string()
        ]]
    );
    // b.unique1 > 10 removes thousand values 0..=10 from the inner side.
    let not_matching = (0..6000)
        .filter(|g| {
            let u2 = (g * 7) % 6000;
            !(u2 < 1000 && (0..6000).any(|b: i64| b % 1000 == u2 && b > 10))
        })
        .count();
    assert_eq!(not_exists, [[not_matching.to_string()]]);
    let both = (0..6000)
        .filter(|g: &i64| (g * 7) % 6000 == g % 1000)
        .count();
    assert_eq!(with_second_correlation, [[both.to_string()]]);
}

#[test]
fn recursive_union_over_an_unhashable_type_is_refused_like_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let error = SqlSession::new(&mut db)
        .execute(
            "WITH RECURSIVE t(n) AS (VALUES ('01'::varbit) UNION \
             SELECT n || '10'::varbit FROM t WHERE n < '100'::varbit) SELECT n FROM t",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(error.to_string(), "could not implement recursive UNION");
    // Hashable types and UNION ALL are unaffected.
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (VALUES (1) UNION SELECT n + 1 FROM t WHERE n < 3) SELECT n FROM t"
        ),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (VALUES ('a'::text) UNION SELECT n || 'a' FROM t WHERE length(n) < 3) SELECT n FROM t"
        ),
        [["a"], ["aa"], ["aaa"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (VALUES ('01'::varbit) UNION ALL \
             SELECT n || '1'::varbit FROM t WHERE length(n) < 4) SELECT n FROM t"
        ),
        [["01"], ["011"], ["0111"]]
    );
}
