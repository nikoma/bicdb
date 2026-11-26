//! Rows of a query whose window functions share one window leave in that window's sort order
//! (partition keys, then the window ORDER BY; ties in input order), as PostgreSQL's WindowAgg
//! emits them. An explicit ORDER BY still decides, with ties in that window order.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn cells(db: &mut BicDb, sql: &str) -> Vec<Vec<String>> {
    SqlSession::new(db)
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn setup() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    cells(
        &mut db,
        "CREATE TABLE emp (dep text, empno int, salary int)",
    );
    cells(
        &mut db,
        "INSERT INTO emp VALUES ('sales', 1, 5000), ('dev', 7, 4200), ('pers', 5, 3500), \
         ('sales', 4, 4800), ('dev', 9, 4500), ('dev', 8, 6000)",
    );
    (dir, db)
}

#[test]
fn rows_follow_the_shared_window_sort() {
    let (_dir, mut db) = setup();
    assert_eq!(
        cells(
            &mut db,
            "SELECT dep, empno, rank() OVER (PARTITION BY dep ORDER BY salary) FROM emp"
        ),
        [
            ["dev", "7", "1"],
            ["dev", "9", "2"],
            ["dev", "8", "3"],
            ["pers", "5", "1"],
            ["sales", "4", "1"],
            ["sales", "1", "2"]
        ]
    );
    // Partition only: ties keep input order within each partition.
    assert_eq!(
        cells(
            &mut db,
            "SELECT dep, empno, count(*) OVER (PARTITION BY dep) FROM emp"
        ),
        [
            ["dev", "7", "3"],
            ["dev", "9", "3"],
            ["dev", "8", "3"],
            ["pers", "5", "1"],
            ["sales", "1", "2"],
            ["sales", "4", "2"]
        ]
    );
    // An explicit ORDER BY decides; its ties come in window order.
    assert_eq!(
        cells(
            &mut db,
            "SELECT dep, empno, rank() OVER w FROM emp WINDOW w AS (PARTITION BY dep ORDER BY salary) \
             ORDER BY rank() OVER w"
        ),
        [
            ["dev", "7", "1"],
            ["pers", "5", "1"],
            ["sales", "4", "1"],
            ["dev", "9", "2"],
            ["sales", "1", "2"],
            ["dev", "8", "3"]
        ]
    );
}

#[test]
fn several_functions_over_the_same_window_share_its_order() {
    let (_dir, mut db) = setup();
    assert_eq!(
        cells(
            &mut db,
            "SELECT empno, sum(salary) OVER w1, count(*) OVER w2 FROM emp \
             WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary)"
        ),
        [
            ["5", "3500", "1"],
            ["7", "7700", "2"],
            ["9", "12200", "3"],
            ["4", "17000", "4"],
            ["1", "22000", "5"],
            ["8", "28000", "6"]
        ]
    );
}

#[test]
fn differently_sorted_windows_keep_the_input_order() {
    let (_dir, mut db) = setup();
    let rows = cells(
        &mut db,
        "SELECT empno, rank() OVER (ORDER BY salary), rank() OVER (ORDER BY empno) FROM emp",
    );
    assert_eq!(
        rows.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(),
        ["1", "7", "5", "4", "9", "8"]
    );
}
