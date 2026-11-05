use bicdb_core::BicDb;
use bicdb_sql::{infer_parameter_types, SqlSession};

#[test]
fn interval_scaling_parameters_are_numeric_in_both_orders_and_in_expiry_inserts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE expiry(id TEXT PRIMARY KEY, expires_at TIMESTAMPTZ)")
        .unwrap();
    for query in [
        "SELECT $1 * INTERVAL '1 second'",
        "SELECT INTERVAL '1 second' * $1",
        "SELECT INTERVAL '1 second' / $1",
        "INSERT INTO expiry VALUES('a',NOW()+($1*INTERVAL '1 second'))",
    ] {
        assert_eq!(
            infer_parameter_types(&db, query).unwrap(),
            vec![Some("float8".to_owned())],
            "{query}"
        );
    }
}

#[test]
fn explicitly_typed_interval_scalars_keep_their_parameter_types() {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        infer_parameter_types(&db, "SELECT $1::int4 * INTERVAL '1 second'").unwrap(),
        vec![Some("int4".to_owned())]
    );
    assert_eq!(
        infer_parameter_types(&db, "SELECT $1::interval / $2").unwrap(),
        vec![Some("interval".to_owned()), Some("float8".to_owned())]
    );
}

#[test]
fn two_intervals_cannot_be_multiplied_or_divided() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for query in [
        "SELECT INTERVAL '2 seconds' * INTERVAL '1 second'",
        "SELECT INTERVAL '2 seconds' / INTERVAL '1 second'",
    ] {
        assert_eq!(
            sql.execute(query).unwrap_err().sqlstate(),
            "42883",
            "{query}"
        );
    }
}
