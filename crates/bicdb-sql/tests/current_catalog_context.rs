//! Session catalog semantics through public SQL APIs, independent of application fixtures.
use bicdb_core::BicDb;
use bicdb_sql::oracle::{
    ensure_database_profile, lower_session_sql, ORACLE_HR_PROFILE, ORACLE_PREVIEW_PROFILE,
};
use bicdb_sql::{SqlEngine, SqlSession, SqlValue};
use std::{collections::HashMap, sync::Arc};

#[test]
fn current_catalog_index_filters_use_session_context_instead_of_constant_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE TABLE catalog_names(id INTEGER PRIMARY KEY, catalog TEXT); CREATE INDEX catalog_names_catalog_idx ON catalog_names(catalog); INSERT INTO catalog_names VALUES(1,'tenant_one'),(2,'OtherCatalog'),(3,'bicdb')").unwrap();
    for (database, expected) in [("tenant_one", 1), ("OtherCatalog", 2), ("bicdb", 3)] {
        let gucs = Arc::new(HashMap::from([("database".into(), database.into())]));
        let engine = SqlEngine::new(&db).with_session_gucs(gucs);
        let result = engine
            .execute("SELECT id FROM catalog_names WHERE catalog=current_catalog")
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::Int(expected)]],
            "{database}"
        );
        assert_eq!(
            engine
                .execute("SELECT count(*) FROM catalog_names WHERE catalog=current_catalog")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]],
            "{database}"
        );
    }
}

#[test]
fn current_catalog_reads_connected_database_and_retains_name_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE catalog_context_rows(id INTEGER, \"CURRENT_CATALOG\" TEXT); INSERT INTO catalog_context_rows VALUES(1,'column value'),(2,'second value')")
        .unwrap();
    for database in ["tenant_one", "OtherCatalog"] {
        let gucs = Arc::new(HashMap::from([(
            "database".to_string(),
            database.to_string(),
        )]));
        let engine = SqlEngine::new(&db).with_session_gucs(gucs);
        for sql in [
            "SELECT CURRENT_CATALOG",
            "SELECT (current_catalog) AS connected_catalog",
            "SELECT CURRENT_CATALOG FROM catalog_context_rows WHERE id=1 AND CURRENT_CATALOG=current_database()",
            "SELECT CASE WHEN id=1 THEN CURRENT_CATALOG ELSE current_database() END AS connected_catalog FROM catalog_context_rows WHERE id=1",
        ] {
            let result = engine.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(result.rows, vec![vec![SqlValue::String(database.into())]], "{sql}");
            if !sql.contains("CASE") {
                assert_eq!(result.column_types, vec![Some("name".into())], "{sql}");
            }
        }
        assert_eq!(
            engine
                .execute("SELECT pg_typeof(current_catalog)::text")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("name".into())]]
        );
        let bare = engine.execute("SELECT CURRENT_CATALOG").unwrap();
        assert_eq!(bare.columns, vec!["current_catalog"]);
        let quoted = engine
            .execute("SELECT \"CURRENT_CATALOG\" FROM catalog_context_rows WHERE id=1")
            .unwrap();
        assert_eq!(
            quoted.rows,
            vec![vec![SqlValue::String("column value".into())]]
        );
        assert_eq!(quoted.column_types, vec![Some("text".into())]);
        assert!(engine.execute("SELECT \"CURRENT_CATALOG\"").is_err());
    }
    SqlSession::new(&mut db).execute("CREATE TABLE native_catalog_column(current_catalog TEXT); INSERT INTO native_catalog_column VALUES('qualified column'); CREATE FUNCTION \"current_catalog\"() RETURNS TEXT LANGUAGE SQL AS $$ SELECT 'user function'::text $$").unwrap();
    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute("SELECT c.current_catalog FROM native_catalog_column c")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("qualified column".into())]]
    );
    let routine = engine.execute("SELECT \"current_catalog\"()").unwrap();
    assert_eq!(
        routine.rows,
        vec![vec![SqlValue::String("user function".into())]]
    );
    assert_eq!(routine.column_types, vec![Some("text".into())]);
    assert!(engine.execute("SELECT CURRENT_CATALOG()").is_err());
    assert_eq!(
        SqlEngine::new(&db)
            .execute("SELECT CURRENT_CATALOG")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("bicdb".into())]]
    );
}

#[test]
fn current_catalog_oracle_transport_keeps_native_context_and_quoted_application_columns() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    SqlSession::new(&mut db).execute("CREATE SCHEMA \"APP\"; CREATE TABLE \"APP\".\"CONTEXT_ROWS\"(\"CURRENT_CATALOG\" TEXT); INSERT INTO \"APP\".\"CONTEXT_ROWS\" VALUES('application column')").unwrap();
    let gucs = Arc::new(HashMap::from([(
        "database".into(),
        "application_catalog".into(),
    )]));
    for profile in [ORACLE_HR_PROFILE, ORACLE_PREVIEW_PROFILE] {
        for sql in [
            "SELECT CURRENT_CATALOG",
            "SELECT (current_catalog) AS connected_catalog",
        ] {
            let lowered = lower_session_sql(&db, sql, profile, Some("\"APP\""), "app").unwrap();
            assert_eq!(lowered, sql);
            let result = SqlEngine::new(&db)
                .with_session_gucs(gucs.clone())
                .execute(&lowered)
                .unwrap();
            assert_eq!(
                result.rows,
                vec![vec![SqlValue::String("application_catalog".into())]]
            );
            assert_eq!(result.column_types, vec![Some("name".into())]);
        }
    }
    for sql in [
        "SELECT current_catalog FROM context_rows",
        "SELECT \"CURRENT_CATALOG\" FROM context_rows",
    ] {
        let lowered =
            lower_session_sql(&db, sql, ORACLE_PREVIEW_PROFILE, Some("\"APP\""), "app").unwrap();
        let result = SqlEngine::new(&db)
            .with_session_gucs(gucs.clone())
            .execute(&lowered)
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::String("application column".into())]],
            "{lowered}"
        );
        assert_eq!(result.column_types, vec![Some("text".into())]);
    }
}

#[test]
fn current_catalog_without_from_resolves_through_sessions_inside_and_outside_transactions() {
    for profile in ["postgres", ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default().with_fsync(false),
        )
        .unwrap();
        ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        let expected = session.execute("SELECT current_database()").unwrap().rows;
        for (step, sql) in [
            ("autocommit", "select current_catalog"),
            ("begin", "BEGIN"),
            ("transaction", "select current_catalog"),
            ("transaction-upper", "SELECT CURRENT_CATALOG"),
            ("commit", "COMMIT"),
        ] {
            let result = session
                .execute(sql)
                .unwrap_or_else(|error| panic!("{profile} {step}: {error}"));
            if sql.to_ascii_lowercase().contains("current_catalog") {
                assert_eq!(result.rows, expected, "{profile} {step}");
            }
        }
    }
}
