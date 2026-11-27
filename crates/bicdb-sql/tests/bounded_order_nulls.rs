//! `WHERE prefix = v ORDER BY next_col [DESC] LIMIT n` over a B-tree whose
//! ordering column holds NULLs: PostgreSQL sorts NULLs last ascending and
//! first descending by default, while index keys order NULL below every
//! value. The bounded ordered index read must not answer such statements
//! with the index's NULL placement.

use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn cells(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

#[test]
fn bounded_ordered_reads_keep_order_by_null_placement() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE n (id int4 PRIMARY KEY, k int4, j int4, s int4 NOT NULL);
             INSERT INTO n VALUES (1, 4, NULL, 1), (2, 4, 5, 2), (3, 4, 9, 3), (4, 4, NULL, 4),
               (5, 4, 1, 5), (6, 5, 2, 6);
             CREATE INDEX n_kj ON n (k, j);
             CREATE INDEX n_ks ON n (k, s);",
        )
        .unwrap();
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT id FROM n WHERE k = 4 ORDER BY j DESC LIMIT 3",
            &["1", "4", "3"],
        ),
        (
            "SELECT id FROM n WHERE k = 4 ORDER BY j LIMIT 3",
            &["5", "2", "3"],
        ),
        (
            "SELECT id FROM n WHERE k = 4 ORDER BY j NULLS FIRST LIMIT 2",
            &["1", "4"],
        ),
        (
            "SELECT id FROM n WHERE k = 4 ORDER BY j DESC NULLS LAST LIMIT 2",
            &["3", "2"],
        ),
        (
            "SELECT id FROM n WHERE k = 4 ORDER BY s DESC LIMIT 2",
            &["5", "4"],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let got = cells(&mut session, sql);
        let got_ids: Vec<&str> = got.iter().map(|row| row[0].as_str()).collect();
        // NULL ties (ids 1 and 4) may come in either order.
        let matches = if sql.contains("ORDER BY j DESC LIMIT 3") || sql.contains("NULLS FIRST") {
            let mut a = got_ids[..2].to_vec();
            a.sort();
            let mut b = expected[..2].to_vec();
            b.sort();
            a == b && got_ids[2..] == expected[2..]
        } else {
            got_ids == *expected
        };
        if !matches {
            failures.push(format!("{sql}: got {got_ids:?}, expected {expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
