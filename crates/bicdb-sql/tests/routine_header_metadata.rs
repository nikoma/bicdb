//! Routine defaults use the current type catalog and retain caller permissions.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn default_calls(sql: &mut SqlSession<'_>) {
    assert_eq!(
        sql.execute("SELECT row_default()").unwrap().rows,
        vec![vec![SqlValue::String("kept".into())]]
    );
    sql.execute("CALL row_default_proc()").unwrap();
}

#[test]
fn routine_defaults_resolve_table_row_casts_before_and_after_reopen() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default().with_fsync(true).with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE typed_row(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION row_default(arg TEXT DEFAULT (ROW(1,'kept')::typed_row).payload) RETURNS TEXT LANGUAGE sql AS 'SELECT arg'; CREATE PROCEDURE row_default_proc(arg TEXT DEFAULT (ROW(1,'kept')::typed_row).payload) LANGUAGE sql AS 'SELECT 1'").unwrap();
            default_calls(&mut sql);
        }
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
        default_calls(&mut SqlSession::new(&mut db));
        db.close().unwrap();
    }
}

fn result_declarations(sql: &mut SqlSession<'_>) {
    for (name, expected) in [
        ("table_header", "TABLE(item typed_row)"),
        ("set_header", "SETOF integer"),
    ] {
        let result = sql
            .execute(&format!(
                "SELECT oid,pg_get_function_result(oid) FROM pg_proc WHERE proname='{name}'"
            ))
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], SqlValue::String(expected.into()));
        let oid = result.rows[0][0].to_cell();
        assert_eq!(
            sql.execute(&format!("SELECT pg_get_function_result({oid})"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::String(expected.into())]]
        );
    }
    assert!(sql
        .execute("SELECT * FROM table_header()")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn function_result_metadata_keeps_table_columns_and_setof_on_both_paths() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default().with_fsync(true).with_storage_mode(mode);
        let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE typed_row(id INT PRIMARY KEY,payload TEXT); CREATE FUNCTION table_header() RETURNS TABLE(item typed_row) LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$; CREATE FUNCTION set_header() RETURNS SETOF INTEGER LANGUAGE sql AS 'SELECT 1'").unwrap();
            result_declarations(&mut sql);
        }
        db.close().unwrap();
        let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
        result_declarations(&mut SqlSession::new(&mut db));
        db.close().unwrap();
    }
}

#[test]
fn routine_defaults_keep_the_callers_privileges_before_definer_entry() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            DbConfig::default().with_fsync(true).with_storage_mode(mode),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE ROLE header_guest; CREATE FUNCTION header_secret() RETURNS TEXT LANGUAGE sql AS 'SELECT ''secret'''; REVOKE ALL ON FUNCTION header_secret() FROM PUBLIC; CREATE FUNCTION header_definer(arg TEXT DEFAULT header_secret()) RETURNS TEXT LANGUAGE sql SECURITY DEFINER AS 'SELECT arg'; CREATE PROCEDURE header_definer_proc(arg TEXT DEFAULT header_secret()) LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'; CREATE FUNCTION header_definer_rows(arg TEXT DEFAULT header_secret()) RETURNS TABLE(item TEXT) LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN; END $$; GRANT EXECUTE ON FUNCTION header_definer(TEXT) TO header_guest; GRANT EXECUTE ON FUNCTION header_definer_proc(TEXT) TO header_guest; GRANT EXECUTE ON FUNCTION header_definer_rows(TEXT) TO header_guest; SET ROLE header_guest").unwrap();
        for statement in [
            "SELECT header_definer()",
            "CALL header_definer_proc()",
            "SELECT * FROM header_definer_rows()",
        ] {
            let error = sql.execute(statement).unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "{statement}: {error}");
            assert!(
                error.to_string().contains("header_secret"),
                "{statement}: {error}"
            );
        }
        assert_eq!(
            sql.execute("SELECT header_definer('explicit')")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("explicit".into())]]
        );
        sql.execute("CALL header_definer_proc('explicit')").unwrap();
        assert!(sql
            .execute("SELECT * FROM header_definer_rows('explicit')")
            .unwrap()
            .rows
            .is_empty());
        sql.execute("RESET ROLE; GRANT EXECUTE ON FUNCTION header_secret() TO header_guest; SET ROLE header_guest").unwrap();
        assert_eq!(
            sql.execute("SELECT header_definer()").unwrap().rows,
            vec![vec![SqlValue::String("secret".into())]]
        );
        sql.execute("CALL header_definer_proc()").unwrap();
        assert!(sql
            .execute("SELECT * FROM header_definer_rows()")
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn leakproof_function_metadata_survives_restart_and_enforces_role_privileges() {
    let root = tempfile::tempdir().unwrap();
    let cfg = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged);
    let mut db = BicDb::open_with_config(root.path(), cfg.clone()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE FUNCTION header_hash(v text) RETURNS text LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE LEAKPROOF RETURN substr(encode(sha256(v::bytea), 'hex'), 1, 10)").unwrap();
        sql.execute("CREATE FUNCTION header_hash(v bytea) RETURNS text LANGUAGE sql STRICT IMMUTABLE PARALLEL SAFE LEAKPROOF RETURN substr(encode(sha256(v), 'hex'), 1, 10)").unwrap();
        sql.execute("CREATE FUNCTION header_plain(v text) RETURNS text LANGUAGE sql NOT /* explicit */ LEAKPROOF AS 'SELECT v'").unwrap();
        assert_eq!(
            sql.execute("SELECT header_hash('x'::text), header_hash('x'::bytea)")
                .unwrap()
                .rows,
            vec![vec![
                SqlValue::String("2d711642b7".into()),
                SqlValue::String("2d711642b7".into()),
            ]]
        );
    }
    db.close().unwrap();

    let mut db = BicDb::open_with_config(root.path(), cfg).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("SELECT proleakproof, provolatile::text, proisstrict, proparallel::text, position('LEAKPROOF' IN pg_get_functiondef(oid)) > 0 FROM pg_proc WHERE proname = 'header_hash'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::String("i".into()),
            SqlValue::Bool(true),
            SqlValue::String("s".into()),
            SqlValue::Bool(true),
        ]; 2]
    );
    sql.execute("CREATE ROLE header_routine_owner; CREATE SCHEMA header_owned AUTHORIZATION header_routine_owner; SET ROLE header_routine_owner").unwrap();
    sql.execute(
        "CREATE FUNCTION header_owned.f(v int) RETURNS int LANGUAGE sql NOT LEAKPROOF RETURN v",
    )
    .unwrap();
    for statement in [
        "ALTER FUNCTION header_owned.f(int) LEAKPROOF",
        "CREATE FUNCTION header_owned.g(v int) RETURNS int LANGUAGE sql LEAKPROOF RETURN v",
    ] {
        let err = sql.execute(statement).unwrap_err();
        assert_eq!(err.sqlstate(), "42501");
        assert!(err
            .to_string()
            .contains("only superuser can define a leakproof function"));
    }
    sql.execute("RESET ROLE; ALTER FUNCTION header_owned.f(int) LEAKPROOF; SET ROLE header_routine_owner; ALTER FUNCTION header_owned.f(int) NOT LEAKPROOF; RESET ROLE").unwrap();
    assert_eq!(
        sql.execute("SELECT proleakproof FROM pg_proc WHERE proname = 'f' AND pronamespace = 'header_owned'::regnamespace")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );
}
