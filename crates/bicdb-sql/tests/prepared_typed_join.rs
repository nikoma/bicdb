use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn indexed_numeric_join_matches_the_scan_with_quoted_identifiers() {
    let directory = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE \"PARENTS\" (\"ID\" int PRIMARY KEY, \"KEY_VALUE\" numeric(6,2)); CREATE TABLE \"CHILDREN\" (\"ID\" int PRIMARY KEY, \"KEY_VALUE\" numeric(6,2)); INSERT INTO \"PARENTS\" VALUES (1,90),(2,91); INSERT INTO \"CHILDREN\" VALUES (10,90),(11,90),(12,91);").unwrap();
    let query="SELECT \"P\".\"ID\", \"C\".\"ID\" FROM \"PARENTS\" \"P\" JOIN \"CHILDREN\" \"C\" ON \"P\".\"KEY_VALUE\"=\"C\".\"KEY_VALUE\" ORDER BY 1,2";
    let expected = vec![
        vec![SqlValue::Int(1), SqlValue::Int(10)],
        vec![SqlValue::Int(1), SqlValue::Int(11)],
        vec![SqlValue::Int(2), SqlValue::Int(12)],
    ];
    assert_eq!(sql.execute(query).unwrap().rows, expected);
    sql.execute("CREATE INDEX child_key_idx ON \"CHILDREN\" (\"KEY_VALUE\")")
        .unwrap();
    assert_eq!(sql.execute(query).unwrap().rows, expected);
}
