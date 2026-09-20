//! Arithmetic with a typed NULL operand is NULL (SQL three-valued logic), not
//! an input-syntax error. Regression for HammerDB TPC-C NEWORD, whose 1%
//! invalid-item path sums an amount array holding a NULL element.

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn scalar(session: &mut SqlSession<'_>, sql: &str) -> SqlValue {
    let result = session
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    result.rows[0][0].clone()
}

#[test]
fn typed_null_operands_yield_null() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT 0 + CAST(NULL AS NUMERIC)",
        "SELECT CAST(NULL AS NUMERIC) * 2",
        "SELECT 1.5 + NULL::numeric",
        "SELECT NULL::numeric + 1.5",
        "SELECT 2 * CAST(NULL AS NUMERIC(5,2))",
        "SELECT 0 + CAST(NULL AS FLOAT8)",
        "SELECT CAST(NULL AS FLOAT4) / 2",
        "SELECT 10 - CAST(NULL AS NUMERIC) % 3",
        "SELECT CAST(NULL AS MONEY) + '1.00'::money",
    ] {
        assert_eq!(scalar(&mut session, sql), SqlValue::Null, "{sql}");
    }
    // Non-null typed arithmetic is untouched.
    assert_eq!(
        scalar(&mut session, "SELECT 0 + CAST(1.25 AS NUMERIC)"),
        SqlValue::String("1.25".to_string())
    );
}

#[test]
fn plpgsql_loop_sums_an_array_with_a_null_element() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE sums (id INT PRIMARY KEY, total NUMERIC(6,2))")
        .unwrap();
    session
        .execute(
            "DO $$ DECLARE amounts NUMERIC(5,2)[] := ARRAY[1.50, NULL, 2.25]; s NUMERIC; \
             BEGIN s := 0; FOR i IN 1..3 LOOP s := s + CAST(amounts[i] AS NUMERIC); END LOOP; \
             INSERT INTO sums VALUES (1, s); END $$",
        )
        .expect("NEWORD-shaped loop must not fail on a NULL element");
    assert_eq!(
        scalar(&mut session, "SELECT total FROM sums"),
        SqlValue::Null
    );
}
