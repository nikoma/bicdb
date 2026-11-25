//! A recursive CTE that never ends no longer takes the server down: under `LIMIT` it stops once
//! the consumer has its rows, and otherwise its materialized rows are bounded (53200).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

/// One small bound for this whole test process (read once), so the unbounded cases fail fast.
fn small_bound() {
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("BICDB_MAX_CTE_MATERIALIZED_BYTES", "2000000");
    }
}

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
fn limit_stops_an_infinite_recursion_like_postgresql() {
    small_bound();
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT * FROM t LIMIT 5"
        ),
        [["1"], ["2"], ["3"], ["4"], ["5"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT * FROM t LIMIT 3 OFFSET 2"
        ),
        [["3"], ["4"], ["5"]]
    );
    // Growing fan-out: every row has two successors.
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t, generate_series(1, 2) g) \
             SELECT * FROM t LIMIT 4"
        )
        .len(),
        4
    );
    // A finite recursion under a larger LIMIT is complete.
    assert_eq!(
        cells(
            &mut db,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t LIMIT 10"
        ),
        [["1"], ["2"], ["3"]]
    );
}

#[test]
fn an_unbounded_recursion_fails_with_53200_instead_of_exhausting_memory() {
    small_bound();
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    for sql in [
        // The consumer needs every row: no early stop.
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t, generate_series(1, 2) g) \
         SELECT count(*) FROM t",
        // LIMIT over a filter is not a row demand on the CTE.
        "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t, generate_series(1, 2) g) \
         SELECT * FROM t WHERE n < 0 LIMIT 1",
    ] {
        let error = SqlSession::new(&mut db).execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "53200", "{sql}: {error}");
        assert_eq!(error.to_string(), "out of memory");
    }
    // The session keeps working.
    assert_eq!(cells(&mut db, "SELECT 1"), [["1"]]);
}

#[test]
fn limit_reaches_a_recursion_nested_in_the_recursive_term() {
    small_bound();
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    cells(
        &mut db,
        "CREATE TABLE department (id int PRIMARY KEY, parent_department int, name text)",
    );
    cells(
        &mut db,
        "INSERT INTO department VALUES (0, NULL, 'ROOT'), (1, 0, 'A'), (2, 1, 'B'), (3, 2, 'C'), \
         (4, 2, 'D'), (5, 0, 'E'), (6, 4, 'F'), (7, 5, 'G')",
    );
    // The regression suite's case: x never ends on its own inside q's recursive term.
    let rows = cells(
        &mut db,
        "with recursive q as (select * from department union all \
           (with recursive x as (select * from department union all \
              (select * from q union all select * from x)) select * from x)) \
         select * from q limit 32",
    );
    assert_eq!(rows.len(), 32);
    let ids = rows.iter().map(|row| row[0].as_str()).collect::<Vec<_>>();
    assert_eq!(
        &ids[..16],
        ["0", "1", "2", "3", "4", "5", "6", "7", "0", "1", "2", "3", "4", "5", "6", "7"]
    );
}

#[test]
fn limit_reaches_a_recursion_inside_a_union_all_branch() {
    small_bound();
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    cells(&mut db, "CREATE TABLE d (id int)");
    cells(&mut db, "INSERT INTO d VALUES (0), (1), (2)");
    let rows = cells(
        &mut db,
        "with recursive q as (select * from d union all \
           (with recursive x as (select * from d union all (select * from x)) select * from x)) \
         select * from q limit 7",
    );
    assert_eq!(rows, [["0"], ["1"], ["2"], ["0"], ["1"], ["2"], ["0"]]);
}
