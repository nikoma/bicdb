use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn postgres_view_and_index_definitions_resolve_columns_before_catalog_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(r#"CREATE TABLE source ("Key" int, value int)"#)
        .unwrap();

    for (query, state) in [
        (
            "CREATE VIEW view_missing AS SELECT * FROM missing_relation",
            "42P01",
        ),
        (
            "CREATE VIEW view_missing AS SELECT s.no_such FROM source s",
            "42703",
        ),
        ("CREATE INDEX index_missing ON source (no_such)", "42703"),
        (
            r#"CREATE INDEX index_missing ON source ("Key") INCLUDE (no_such)"#,
            "42703",
        ),
        (
            r#"CREATE INDEX index_missing ON source ("Key") WHERE no_such > 1"#,
            "42703",
        ),
        ("CREATE INDEX index_missing ON source (key)", "42703"),
    ] {
        assert_eq!(
            session.execute(query).unwrap_err().sqlstate(),
            state,
            "{query}"
        );
    }
    session.execute(r#"CREATE VIEW view_missing AS WITH c AS (SELECT "Key" FROM source) SELECT "Key" FROM c"#).unwrap();
    session
        .execute(r#"CREATE INDEX index_missing ON source ("Key") INCLUDE (value) WHERE value > 0"#)
        .unwrap();
}
