//! UPDATE that changes a row's partition key moves the row (PostgreSQL 11+):
//! across sibling and nested partitions, only within the named subtree, with
//! the destination leaf's constraints (and its name in the error), RETURNING
//! the new tableoid, and no partial effect when a row has nowhere to go.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

fn rows(sql: &mut SqlSession<'_>, query: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(query).unwrap().rows
}

fn open() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    (dir, db)
}

#[test]
fn update_moves_rows_across_levels_and_reports_the_new_partition() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE mv (a text, b int, c int) PARTITION BY LIST (a)",
        "CREATE TABLE mv_a PARTITION OF mv FOR VALUES IN ('a') PARTITION BY RANGE (b)",
        "CREATE TABLE mv_a1 PARTITION OF mv_a FOR VALUES FROM (0) TO (10)",
        "CREATE TABLE mv_a2 PARTITION OF mv_a FOR VALUES FROM (10) TO (20)",
        "CREATE TABLE mv_b PARTITION OF mv FOR VALUES IN ('b')",
        "INSERT INTO mv VALUES ('a', 1, 1), ('b', 2, 2)",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        rows(
            &mut sql,
            "UPDATE mv SET b = 15 WHERE c = 1 RETURNING tableoid::regclass::text, b"
        ),
        vec![vec![s("mv_a2"), SqlValue::Int(15)]]
    );
    sql.execute("UPDATE mv SET a = 'a', b = 5 WHERE c = 2")
        .unwrap();
    sql.execute("UPDATE mv SET a = 'b' WHERE b = 15").unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT tableoid::regclass::text, a, b FROM mv ORDER BY c"
        ),
        vec![
            vec![s("mv_b"), s("b"), SqlValue::Int(15)],
            vec![s("mv_a1"), s("a"), SqlValue::Int(5)],
        ]
    );
    // Through a sub-partitioned table, rows move only inside its subtree.
    sql.execute("UPDATE mv_a SET b = 12").unwrap();
    let error = sql.execute("UPDATE mv_a SET a = 'b'").unwrap_err();
    assert_eq!(error.sqlstate(), "23514");
    assert_eq!(
        error.postgres_message(),
        "new row for relation \"mv_a\" violates partition constraint"
    );
}

#[test]
fn a_row_with_no_destination_fails_the_whole_statement() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE mn (a int, b text) PARTITION BY LIST (a)",
        "CREATE TABLE mn_1 PARTITION OF mn FOR VALUES IN (1)",
        "CREATE TABLE mn_2 PARTITION OF mn FOR VALUES IN (2)",
        "INSERT INTO mn VALUES (1, 'x'), (2, 'y')",
    ] {
        sql.execute(statement).unwrap();
    }
    let error = sql.execute("UPDATE mn SET a = a + 1").unwrap_err();
    assert_eq!(error.sqlstate(), "23514");
    assert_eq!(
        error.postgres_message(),
        "no partition of relation \"mn\" found for row"
    );
    assert_eq!(
        rows(
            &mut sql,
            "SELECT tableoid::regclass::text, a, b FROM mn ORDER BY a"
        ),
        vec![
            vec![s("mn_1"), SqlValue::Int(1), s("x")],
            vec![s("mn_2"), SqlValue::Int(2), s("y")],
        ]
    );
}

#[test]
fn constraint_violations_name_the_destination_partition() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE mk (a int, b int CHECK (b > 0), c int NOT NULL) PARTITION BY LIST (a)",
        "CREATE TABLE mk_1 PARTITION OF mk FOR VALUES IN (1)",
        "CREATE TABLE mk_2 PARTITION OF mk FOR VALUES IN (2)",
        "INSERT INTO mk VALUES (1, 1, 1)",
    ] {
        sql.execute(statement).unwrap();
    }
    for (statement, sqlstate, message) in [
        (
            "UPDATE mk SET a = 2, b = 0",
            "23514",
            "new row for relation \"mk_2\" violates check constraint \"mk_b_check\"",
        ),
        (
            "UPDATE mk SET b = 0",
            "23514",
            "new row for relation \"mk_1\" violates check constraint \"mk_b_check\"",
        ),
        (
            "INSERT INTO mk VALUES (2, 0, 1)",
            "23514",
            "new row for relation \"mk_2\" violates check constraint \"mk_b_check\"",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(
            (error.sqlstate(), error.postgres_message().as_str()),
            (sqlstate, message),
            "{statement}"
        );
    }
    let error = sql
        .execute("INSERT INTO mk VALUES (2, 1, NULL)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23502");
    assert!(error.postgres_message().contains("\"mk_2\""), "{error}");
    assert_eq!(
        rows(&mut sql, "SELECT tableoid::regclass::text, a FROM mk"),
        vec![vec![s("mk_1"), SqlValue::Int(1)]]
    );
}
