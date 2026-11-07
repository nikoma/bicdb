//! An integer column compared with a decimal literal compares exactly (the
//! integer is promoted to numeric), as in PostgreSQL, with or without an index:
//! a fractional literal is never rounded to the column type, and an integral
//! one (`3.0`, `3e0`) matches the integer it equals.

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn ids(session: &mut SqlSession<'_>, sql: &str) -> Vec<i64> {
    let mut ids = session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| match &row[0] {
            SqlValue::Int(id) => *id,
            other => panic!("unexpected id {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn integer_columns_compare_exactly(reopen: bool, indexed: bool) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE m (id int4 PRIMARY KEY, i int4, s int2, b int8);
             INSERT INTO m VALUES (1, 2, 2, 2), (2, 3, 3, 3), (3, NULL, NULL, NULL), (4, -3, -3, -3);",
        )
        .unwrap();
    if indexed {
        SqlSession::new(&mut db)
            .execute(
                "CREATE INDEX m_i ON m (i); CREATE INDEX m_s ON m (s); CREATE INDEX m_b ON m (b);",
            )
            .unwrap();
    }
    if reopen {
        drop(db);
        db = BicDb::open(root.path()).unwrap();
    }
    let mut session = SqlSession::new(&mut db);
    // Primary-key lookups take their own path.
    for (predicate, expected) in [
        ("id = 2.0", &[2][..]),
        ("id = 2.5", &[][..]),
        ("id IN (1.0, 4)", &[1, 4][..]),
    ] {
        let sql = format!("SELECT id FROM m WHERE {predicate}");
        assert_eq!(ids(&mut session, &sql), expected, "{sql}");
    }
    for column in ["i", "s", "b"] {
        let cases: [(&str, &[i64]); 18] = [
            ("{c} > 2.6", &[2]),
            ("{c} < 2.4", &[1, 4]),
            ("{c} = 3e0", &[2]),
            ("{c} = 3.00", &[2]),
            ("{c} = -3.0", &[4]),
            ("{c} IN (2.0, 2.5)", &[1]),
            ("{c} = 2.5", &[]),
            ("{c} <> 2.5", &[1, 2, 4]),
            ("{c} > 2.5", &[2]),
            ("{c} >= 2.5", &[2]),
            ("{c} < 2.5", &[1, 4]),
            ("{c} <= 2.5", &[1, 4]),
            ("2.5 < {c}", &[2]),
            ("{c} = 3.0", &[2]),
            ("{c} > -2.5", &[1, 2]),
            ("{c} < -2.5", &[4]),
            ("{c} BETWEEN 2.5 AND 3.5", &[2]),
            ("{c} IN (2.5, 3)", &[2]),
        ];
        for (predicate, expected) in cases {
            let predicate = predicate.replace("{c}", column);
            for qualifier in ["", "m.", "x."] {
                let alias = if qualifier == "x." { " AS x" } else { "" };
                let qualified = predicate.replace(column, &format!("{qualifier}{column}"));
                let sql = format!("SELECT id FROM m{alias} WHERE {qualified}");
                assert_eq!(ids(&mut session, &sql), expected, "{sql}");
            }
        }
    }
}

#[test]
fn integer_columns_compare_exactly_with_fractional_literals() {
    integer_columns_compare_exactly(false, false);
}

#[test]
fn indexed_integer_columns_compare_exactly_with_fractional_literals() {
    integer_columns_compare_exactly(false, true);
}

#[test]
fn integer_columns_compare_exactly_with_fractional_literals_after_reopen() {
    integer_columns_compare_exactly(true, false);
}

#[test]
fn indexed_integer_columns_compare_exactly_after_reopen() {
    integer_columns_compare_exactly(true, true);
}
