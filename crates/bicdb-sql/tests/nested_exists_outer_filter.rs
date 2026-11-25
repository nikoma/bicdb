//! A condition inside EXISTS that refers only to outer rows is a one-time filter per outer row
//! (as in PostgreSQL), so a nested EXISTS that reaches two levels out no longer rescans the
//! inner tables for every inner row.
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
fn outer_only_conditions_gate_exists_with_postgresql_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    cells(&mut db, "CREATE TABLE o (id int, y int)");
    cells(&mut db, "CREATE TABLE i (x int)");
    cells(&mut db, "INSERT INTO o VALUES (1, 1), (2, 10), (3, NULL)");
    cells(&mut db, "INSERT INTO i VALUES (1), (2), (3)");
    assert_eq!(
        cells(
            &mut db,
            "SELECT id FROM o WHERE EXISTS (SELECT 1 FROM i WHERE i.x = o.id AND o.y > 5) ORDER BY id"
        ),
        [["2"]]
    );
    assert_eq!(
        cells(
            &mut db,
            "SELECT id FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.x = o.id AND o.y > 5) ORDER BY id"
        ),
        [["1"], ["3"]]
    );
    // An aggregate projection always yields a row: the outer condition must not gate it.
    assert_eq!(
        cells(
            &mut db,
            "SELECT id FROM o WHERE EXISTS (SELECT count(*) FROM i WHERE o.y > 1000) ORDER BY id"
        ),
        [["1"], ["2"], ["3"]]
    );
    // An empty inner table: EXISTS is false whatever the gate says.
    cells(&mut db, "CREATE TABLE e (x int)");
    assert_eq!(
        cells(
            &mut db,
            "SELECT count(*) FROM o WHERE EXISTS (SELECT 1 FROM e WHERE o.y > 0)"
        ),
        [["0"]]
    );
}

#[test]
fn nested_exists_referring_two_levels_out_is_not_quadratic() {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        cells(&mut db, "CREATE TABLE tk (thousand int, hundred int)");
        cells(
            &mut db,
            "INSERT INTO tk SELECT g % 1000, g % 100 FROM generate_series(0, 4999) g",
        );
        // The regression suite's "planner bug with nested EXISTS handling" shape.
        let rows = cells(
            &mut db,
            "select count(*) from tk a, tk b where a.thousand = b.thousand \
             and exists (select 1 from tk c where b.hundred = c.hundred \
                         and not exists (select 1 from tk d where a.thousand = d.thousand))",
        );
        let other = cells(
            &mut db,
            "select count(*) from tk a, tk b where a.thousand = b.thousand \
             and exists (select 1 from tk c where b.hundred = c.hundred \
                         and exists (select 1 from tk d where a.thousand = d.thousand))",
        );
        sender.send((rows, other)).unwrap();
    });
    let (rows, other) = receiver
        .recv_timeout(Duration::from_secs(240))
        .expect("nested EXISTS must not rescan the inner tables for every inner row");
    assert_eq!(rows, [["0"]]);
    // Every a has 5 b rows with the same thousand: 5000 * 5.
    assert_eq!(other, [["25000"]]);
}
