//! The `^` operator raises to a power, in queries and in PL/pgSQL, as
//! PostgreSQL's power() does.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn exponent_operator_raises_to_a_power() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let row = session
        .execute("SELECT 2 ^ 3, 1 / 10 ^ 2, NULL::numeric ^ 2, 2 ^ 0.5 > 1.41, 2 ^ -1")
        .unwrap()
        .rows
        .remove(0);
    assert_eq!(
        row,
        vec![
            SqlValue::Float(8.0),
            SqlValue::Float(0.01),
            SqlValue::Null,
            SqlValue::Bool(true),
            SqlValue::Float(0.5),
        ]
    );
    assert!(session.execute("SELECT 0 ^ -1").is_err());
    session
        .execute(
            "CREATE FUNCTION smallest(p INT) RETURNS NUMERIC LANGUAGE plpgsql AS $$ \
             DECLARE v NUMERIC; BEGIN SELECT 1/10^p INTO v; RETURN v * 1000; END; $$",
        )
        .unwrap();
    let value = session.execute("SELECT smallest(2)").unwrap().rows[0][0].to_cell();
    assert_eq!(value.parse::<f64>().unwrap(), 10.0);
}
