//! The native reference accepts an unknown RETURN type with compilation errors;
//! PostgreSQL independently rejects the declaration with undefined_object.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{
    oracle::{ensure_database_profile, lower_session_sql, ORACLE_PREVIEW_PROFILE},
    SqlSession, SqlValue,
};
fn run(db: &mut BicDb, sql: &str) -> bicdb_sql::Result<bicdb_sql::SqlResult> {
    let sql = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
    SqlSession::new(db).execute(&sql)
}
#[test]
fn oracle_unknown_return_type_persists_invalid_object_diagnostics_and_call_error() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let config = DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        let result=run(&mut db,"CREATE OR REPLACE FUNCTION compat_bad_return RETURN compat_no_such_type IS BEGIN RETURN 1; END;").unwrap();
        assert!(result
            .notices
            .iter()
            .any(|notice| notice.message.to_lowercase().contains("compilation errors")));
        assert_eq!(
            run(
                &mut db,
                "SELECT status FROM user_objects WHERE object_name='COMPAT_BAD_RETURN'"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::String("INVALID".into())]]
        );
        assert_eq!(run(&mut db,"SELECT attribute,text FROM user_errors WHERE name='COMPAT_BAD_RETURN' ORDER BY sequence").unwrap().rows,vec![vec![SqlValue::String("ERROR".into()),SqlValue::String("PLS-00201: identifier 'COMPAT_NO_SUCH_TYPE' must be declared".into())],vec![SqlValue::String("ERROR".into()),SqlValue::String("PL/SQL: Compilation unit analysis terminated".into())]]);
        assert!(run(&mut db, "SELECT compat_bad_return() FROM dual")
            .unwrap_err()
            .to_string()
            .contains("ORA-06575"));
        db.flush().unwrap();
        drop(db);
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
        assert_eq!(
            run(
                &mut db,
                "SELECT status FROM user_objects WHERE object_name='COMPAT_BAD_RETURN'"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::String("INVALID".into())]]
        );
        run(&mut db, "DROP FUNCTION compat_bad_return").unwrap();
        assert_eq!(
            run(
                &mut db,
                "SELECT count(*) FROM user_errors WHERE name='COMPAT_BAD_RETURN'"
            )
            .unwrap()
            .rows,
            vec![vec![SqlValue::Int(0)]]
        );
    }
}
#[test]
fn replacing_valid_and_invalid_oracle_functions_updates_status_and_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(
        &mut db,
        "CREATE FUNCTION changing_return RETURN NUMBER IS BEGIN RETURN 1; END;",
    )
    .unwrap();
    run(&mut db,"CREATE OR REPLACE FUNCTION changing_return RETURN missing_return_type IS BEGIN RETURN 2; END;").unwrap();
    assert!(run(&mut db, "SELECT changing_return() FROM dual")
        .unwrap_err()
        .to_string()
        .contains("ORA-06575"));
    run(
        &mut db,
        "CREATE OR REPLACE FUNCTION changing_return RETURN NUMBER IS BEGIN RETURN 3; END;",
    )
    .unwrap();
    assert_eq!(
        run(&mut db, "SELECT changing_return() FROM dual")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "3"
    );
    assert_eq!(
        run(
            &mut db,
            "SELECT status FROM user_objects WHERE object_name='CHANGING_RETURN'"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::String("VALID".into())]]
    );
    assert_eq!(
        run(
            &mut db,
            "SELECT count(*) FROM user_errors WHERE name='CHANGING_RETURN'"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}
#[test]
fn postgres_unknown_return_type_still_rejects_create() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let error=SqlSession::new(&mut db).execute("CREATE FUNCTION pg_bad_return() RETURNS compat_no_such_type LANGUAGE sql AS $$SELECT 1$$").unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
}

#[test]
fn create_or_replace_cannot_overwrite_another_owners_function() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE routine_attacker; GRANT CREATE ON SCHEMA public TO routine_attacker; CREATE FUNCTION owned_function() RETURNS INT LANGUAGE sql AS $$SELECT 1$$").unwrap();
    sql.execute("SET ROLE routine_attacker").unwrap();
    let error = sql
        .execute(
            "CREATE OR REPLACE FUNCTION owned_function() RETURNS INT LANGUAGE sql AS $$SELECT 2$$",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    sql.execute("RESET ROLE").unwrap();
    assert_eq!(
        sql.execute("SELECT owned_function()").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn oracle_object_dictionary_includes_owned_relations_and_filters_other_roles() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    for statement in [
        "CREATE TABLE dictionary_table(id NUMBER PRIMARY KEY)",
        "CREATE VIEW dictionary_view AS SELECT id FROM dictionary_table",
        "CREATE SEQUENCE dictionary_sequence",
        "CREATE TYPE dictionary_labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION dictionary_function RETURN NUMBER IS BEGIN RETURN 1; END;",
    ] {
        run(&mut db, statement).unwrap();
    }
    let rows = run(&mut db, "SELECT object_name,object_type FROM user_objects")
        .unwrap()
        .rows;
    for (name, kind) in [
        ("DICTIONARY_TABLE", "TABLE"),
        ("DICTIONARY_VIEW", "VIEW"),
        ("DICTIONARY_SEQUENCE", "SEQUENCE"),
        ("DICTIONARY_LABELS", "TYPE"),
        ("DICTIONARY_FUNCTION", "FUNCTION"),
    ] {
        assert!(
            rows.contains(&vec![
                SqlValue::String(name.into()),
                SqlValue::String(kind.into())
            ]),
            "{name}/{kind}: {rows:?}"
        );
    }
    let objects = lower_session_sql(
        &db,
        "SELECT count(*) FROM user_objects",
        ORACLE_PREVIEW_PROFILE,
        None,
        "bicdb",
    )
    .unwrap();
    let errors = lower_session_sql(
        &db,
        "SELECT count(*) FROM user_errors",
        ORACLE_PREVIEW_PROFILE,
        None,
        "bicdb",
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE routine_catalog_guest;SET ROLE routine_catalog_guest")
        .unwrap();
    assert_eq!(
        sql.execute(&objects).unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        sql.execute(&errors).unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn oracle_dictionary_fallback_respects_local_tables_and_ctes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(
        &mut db,
        "CREATE TABLE user_errors(name VARCHAR2(20 CHAR),text VARCHAR2(20 CHAR))",
    )
    .unwrap();
    run(&mut db, "INSERT INTO user_errors VALUES('local','table')").unwrap();
    assert_eq!(
        run(&mut db, "SELECT text FROM user_errors").unwrap().rows,
        vec![vec![SqlValue::String("table".into())]]
    );
    assert_eq!(
        run(
            &mut db,
            "WITH user_errors AS (SELECT 'cte' AS text FROM dual) SELECT text FROM user_errors"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::String("cte".into())]]
    );
    assert_eq!(
        run(
            &mut db,
            "SELECT count(*) FROM SYS.USER_ERRORS WHERE name='ABSENT'"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}
