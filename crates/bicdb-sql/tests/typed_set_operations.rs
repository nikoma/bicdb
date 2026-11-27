use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn typed_set_operations_handle_regression_scale_inputs() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (op, expected) in [
        ("INTERSECT", 5_000),
        ("INTERSECT ALL", 5_000),
        ("EXCEPT", 5_000),
        ("EXCEPT ALL", 5_000),
        ("UNION", 15_000),
    ] {
        let result = session
            .execute(&format!(
                "SELECT count(*) FROM (
                    SELECT i::int AS n FROM generate_series(1, 10000) AS left_series(i)
                    {op}
                    SELECT i::int AS n FROM generate_series(5001, 15000) AS right_series(i)
                 ) s"
            ))
            .unwrap();
        assert_eq!(result.rows, vec![vec![SqlValue::Int(expected)]], "{op}");
    }
}

#[test]
fn typed_sets_preserve_null_and_numeric_equality_with_duplicate_counts() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let left = "SELECT * FROM (VALUES (1.00::numeric), (1.0), (NULL), (NULL), (2.0)) l(n)";
    let right = "SELECT * FROM (VALUES (1::numeric), (NULL), (3.0)) r(n)";
    for (op, expected) in [
        ("INTERSECT", (2, 1)),
        ("INTERSECT ALL", (2, 1)),
        ("EXCEPT", (1, 1)),
        ("EXCEPT ALL", (3, 2)),
        ("UNION", (4, 3)),
    ] {
        let result = session
            .execute(&format!(
                "SELECT count(*), count(n) FROM ({left} {op} {right}) s"
            ))
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(expected.0), SqlValue::Int(expected.1)]],
            "{op}"
        );
    }
}
