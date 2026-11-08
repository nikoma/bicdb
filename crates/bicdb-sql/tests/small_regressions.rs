//! Small behaviors clients depend on: wildcard result types through table
//! aliases, and missing relations reported as missing.
use bicdb_core::{BicDb, BicDbError, DbConfig};
use bicdb_sql::{SqlError, SqlSession};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

#[test]
fn alias_wildcards_carry_column_types_and_origins() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, amount NUMERIC(10,2), ok BOOLEAN)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'a', 1.5, true)")
        .unwrap();
    let expected = ["int4", "text", "numeric", "bool"].map(|ty| Some(ty.to_string()));
    for sql in [
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT a.* FROM t a",
    ] {
        let result = session.execute(sql).unwrap();
        assert_eq!(result.column_types, expected, "{sql}");
        assert_eq!(result.column_metadata.len(), 4, "{sql}");
        assert!(
            result
                .column_metadata
                .iter()
                .all(|meta| meta.table_oid != 0),
            "{sql}"
        );
    }
    let joined = session
        .execute("SELECT a.*, b.id AS other FROM t a JOIN t b ON a.id = b.id")
        .unwrap();
    assert_eq!(
        joined.column_types,
        ["int4", "text", "numeric", "bool", "int4"].map(|ty| Some(ty.to_string()))
    );
    drop(session);
    // Describe (before execution) agrees.
    for sql in [
        "SELECT a.* FROM t a",
        "SELECT a.*, b.id AS other FROM t a JOIN t b ON a.id = b.id",
    ] {
        let described = bicdb_sql::infer_query_result_columns(&db, sql)
            .unwrap()
            .unwrap_or_else(|| panic!("no columns described for {sql}"));
        assert_eq!(
            described[..4]
                .iter()
                .map(|(_, ty)| ty.clone())
                .collect::<Vec<_>>(),
            expected.to_vec(),
            "{sql}"
        );
        assert_eq!(
            described.len(),
            if sql.contains("JOIN") { 5 } else { 4 },
            "{sql}"
        );
    }
}

#[test]
fn queries_on_missing_tables_report_the_table_missing() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT count(*) FROM nope",
        "SELECT id FROM nope WHERE id = 1",
        "SELECT * FROM nope",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert!(
            matches!(
                &error,
                SqlError::BicDb(BicDbError::CollectionNotFound(_)) | SqlError::InvalidCollection(_)
            ),
            "{sql}: {error:?}"
        );
        assert_eq!(error.sqlstate(), "42P01", "{sql}");
    }
}

/// `top` is not a reserved word: a column, parameter or loop bound named
/// `top` must not be read as a `TOP n` row-limit clause.
#[test]
fn identifier_named_top_is_not_a_select_top_clause() {
    let (_dir, mut db) = session_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ranking (id INT PRIMARY KEY, top INT, score INT)")
        .unwrap();
    session
        .execute("INSERT INTO ranking VALUES (1, 5, 10), (2, 5, 20), (3, 7, 30)")
        .unwrap();
    let int = |value: i64| bicdb_sql::SqlValue::Int(value);
    for (sql, expected) in [
        (
            "SELECT top FROM ranking ORDER BY id",
            vec![vec![int(5)], vec![int(5)], vec![int(7)]],
        ),
        (
            "SELECT top, score FROM ranking WHERE id = 3",
            vec![vec![int(7), int(30)]],
        ),
        (
            "SELECT DISTINCT top FROM ranking ORDER BY top",
            vec![vec![int(5)], vec![int(7)]],
        ),
        (
            "SELECT ALL top FROM ranking WHERE id = 1",
            vec![vec![int(5)]],
        ),
        (
            "SELECT DISTINCT ON (top) top FROM ranking ORDER BY top",
            vec![vec![int(5)], vec![int(7)]],
        ),
        ("select Top from ranking where id = 2", vec![vec![int(5)]]),
        (
            "SELECT top + 1 FROM ranking WHERE id = 1",
            vec![vec![int(6)]],
        ),
        (
            "SELECT r.top FROM ranking r WHERE r.id = 1",
            vec![vec![int(5)]],
        ),
    ] {
        assert_eq!(session.execute(sql).unwrap().rows, expected, "{sql}");
    }
    // The result column keeps the identifier's folded name.
    assert_eq!(
        session
            .execute("SELECT top FROM ranking WHERE id = 1")
            .unwrap()
            .columns,
        vec!["top".to_string()]
    );
    // Literals and comments that merely contain the word are untouched.
    assert_eq!(
        session
            .execute("SELECT /* top */ 'select top' FROM ranking WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![bicdb_sql::SqlValue::String("select top".into())]]
    );

    session
        .execute(
            "CREATE FUNCTION top_sum(top int) RETURNS int LANGUAGE plpgsql AS $$
             DECLARE total int := 0;
             BEGIN
                 FOR j IN 1 .. top LOOP total := total + j; END LOOP;
                 RETURN total + top;
             END $$",
        )
        .unwrap();
    assert_eq!(
        session.execute("SELECT top_sum(4)").unwrap().rows,
        vec![vec![int(14)]]
    );
    // TOP is not row-limit syntax in this profile.
    assert!(session.execute("SELECT TOP 1 id FROM ranking").is_err());
}
