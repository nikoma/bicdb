//! `^` and power() return NUMERIC for NUMERIC operands and float8 for
//! integer or float operands, as in PostgreSQL 16 (expected values are its
//! output).
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn row(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .remove(0)
        .into_iter()
        .map(|value| value.to_cell())
        .collect()
}

#[test]
fn power_is_numeric_for_numeric_operands() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        row(
            &mut session,
            "SELECT 10::numeric ^ 2, power(10::numeric, 2), 1 / 10::numeric ^ 2, \
             2::numeric ^ 0.5, 2 ^ 3, pg_typeof(10::numeric ^ 2), pg_typeof(2 ^ 3)"
        ),
        [
            "100.00000000000000",
            "100.00000000000000",
            "0.01000000000000000000",
            "1.4142135623730950",
            "8",
            "numeric",
            "double precision"
        ]
    );
    session
        .execute("CREATE TABLE amounts (precision NUMERIC); INSERT INTO amounts VALUES (2)")
        .unwrap();
    assert_eq!(
        row(&mut session, "SELECT 1 / 10 ^ precision FROM amounts"),
        ["0.01000000000000000000"]
    );
}
