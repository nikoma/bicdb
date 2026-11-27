use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn join_using_integer_columns_scales_and_checks_all_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE lhs (k int, j int)",
        "CREATE TABLE rhs (k int,n int,j int)",
        "INSERT INTO lhs SELECT i, i % 2 FROM generate_series(1,10000) s(i)",
        "INSERT INTO rhs SELECT i, i, 0 FROM generate_series(1,10000) s(i)",
        "INSERT INTO rhs VALUES (2, 2, 0), (NULL, 0, 0)",
        "INSERT INTO lhs VALUES (NULL, 0)",
    ] {
        session.execute(sql).unwrap();
    }
    for (query, expected) in [
        ("SELECT count(*) FROM lhs l JOIN rhs r USING (k)", 10001),
        ("SELECT count(*) FROM lhs l JOIN rhs r USING (k,j)", 5001),
        (
            "SELECT count(*) FROM lhs l LEFT JOIN rhs r USING (k,j) WHERE r.n IS NULL",
            5001,
        ),
    ] {
        assert_eq!(
            session.execute(query).unwrap().rows,
            vec![vec![SqlValue::Int(expected)]],
            "{query}"
        );
    }
}
