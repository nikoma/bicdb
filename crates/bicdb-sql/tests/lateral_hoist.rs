//! A LATERAL derived table whose outer references are only in its select list is joined by
//! reading the inner side once, instead of re-running the subquery for every outer row.
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
fn lateral_select_list_correlation_matches_postgresql_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    cells(&mut db, "CREATE TABLE l (lid int, ten int)");
    cells(&mut db, "CREATE TABLE r (k int, ten int, flag bool)");
    cells(
        &mut db,
        "INSERT INTO l VALUES (1, 10), (2, 20), (3, 30), (NULL, 40)",
    );
    cells(
        &mut db,
        "INSERT INTO r VALUES (1, 1, true), (1, 2, false), (2, 5, true), (NULL, 7, true)",
    );
    // LEFT: unmatched and NULL-keyed outer rows are null-extended.
    assert_eq!(
        cells(
            &mut db,
            "SELECT l.lid, ss.x FROM l LEFT JOIN LATERAL (SELECT l.ten + r.ten AS x, r.k FROM r) ss \
             ON l.lid = ss.k ORDER BY l.lid NULLS LAST, ss.x"
        ),
        [["1", "11"], ["1", "12"], ["2", "25"], ["3", ""], ["", ""]]
    );
    // The inner filter still applies, once.
    assert_eq!(
        cells(
            &mut db,
            "SELECT l.lid, ss.x FROM l JOIN LATERAL (SELECT l.ten * r.ten AS x, r.k FROM r WHERE r.flag) ss \
             ON ss.k = l.lid ORDER BY l.lid, ss.x"
        ),
        [["1", "10"], ["2", "100"]]
    );
    // An outer reference in the inner filter keeps the per-row execution, with the same result.
    assert_eq!(
        cells(
            &mut db,
            "SELECT l.lid, ss.x FROM l JOIN LATERAL (SELECT r.ten AS x, r.k FROM r WHERE r.ten < l.ten) ss \
             ON ss.k = l.lid ORDER BY l.lid, ss.x"
        ),
        [["1", "1"], ["1", "2"], ["2", "5"]]
    );
}

#[test]
fn lateral_join_over_large_tables_is_not_quadratic() {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        cells(
            &mut db,
            "CREATE TABLE tk (unique1 int, ten int, fivethous int)",
        );
        cells(
            &mut db,
            "INSERT INTO tk SELECT g, g % 10, g % 3000 FROM generate_series(0, 5999) g",
        );
        // The regression suite's "lateral with sum" shape.
        let rows = cells(
            &mut db,
            "select t1.ten, sum(x) from tk t1 left join lateral \
             (select t1.ten + t2.ten as x, t2.fivethous from tk t2) ss \
             on t1.unique1 = ss.fivethous group by t1.ten order by t1.ten",
        );
        sender.send(rows).unwrap();
    });
    let rows = receiver
        .recv_timeout(Duration::from_secs(240))
        .expect("LATERAL join must not re-run its subquery over the whole inner table per row");
    // Brute force.
    let mut sums = [None::<i64>; 10];
    for u1 in 0..6000i64 {
        let ten = u1 % 10;
        for g in 0..6000i64 {
            if g % 3000 == u1 {
                *sums[ten as usize].get_or_insert(0) += ten + g % 10;
            }
        }
    }
    let expected = (0..10)
        .map(|ten| {
            vec![
                ten.to_string(),
                sums[ten].map(|s| s.to_string()).unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}
