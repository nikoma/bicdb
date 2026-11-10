use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
#[test]
fn local_json_arguments_survive_cte_table_sources() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION routine_json_rows(p jsonb) RETURNS TABLE(v text) LANGUAGE plpgsql STABLE AS $$
#variable_conflict use_column
DECLARE m jsonb:=p;BEGIN RETURN QUERY WITH raw AS (SELECT x.value FROM jsonb_array_elements(COALESCE(m->'values','[]'::jsonb)) x) SELECT value->>'v' FROM raw;END $$").unwrap();
    assert_eq!(sql.execute("SELECT * FROM routine_json_rows('{\"values\":[{\"v\":\"first\"},{\"v\":\"second\"}]}'::jsonb)").unwrap().rows,vec![vec![SqlValue::String("first".into())],vec![SqlValue::String("second".into())]]);
    assert!(sql
        .execute("SELECT * FROM routine_json_rows('{\"values\":[]}'::jsonb)")
        .unwrap()
        .rows
        .is_empty());
}
#[test]
fn local_json_in_nested_case_and_subquery_keeps_row_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE readings(unit text,value numeric);INSERT INTO readings VALUES('kg',80),('lb',176);CREATE FUNCTION converted_rows(p jsonb) RETURNS TABLE(v numeric) LANGUAGE plpgsql STABLE AS $$
#variable_conflict use_column
DECLARE m jsonb:=p;BEGIN RETURN QUERY WITH raw AS (SELECT CASE WHEN r.unit=COALESCE(m->>'unit',r.unit) THEN r.value ELSE (SELECT r.value*(c->>'factor')::numeric FROM jsonb_array_elements(COALESCE(m->'conversions','[]'::jsonb)) c WHERE c->>'from_unit'=r.unit LIMIT 1) END AS result FROM readings r) SELECT result FROM raw;END $$").unwrap();
    let rows=sql.execute("SELECT v::int FROM converted_rows('{\"unit\":\"kg\",\"conversions\":[{\"from_unit\":\"lb\",\"factor\":0.5}]}'::jsonb)").unwrap().rows;
    assert_eq!(rows, vec![vec![SqlValue::Int(80)], vec![SqlValue::Int(88)]]);
}

#[test]
fn column_preference_preserves_declarations_and_prefers_query_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE measured_values(value int);INSERT INTO measured_values VALUES(3),(7);CREATE FUNCTION preferred_columns() RETURNS TABLE(value int) LANGUAGE plpgsql AS $$
-- Compiler option belongs to this function.
#variable_conflict use_column
/* declarations still follow */
DECLARE sentinel int:=42;BEGIN IF sentinel<>42 THEN RAISE EXCEPTION 'local declaration lost';END IF;RETURN QUERY SELECT value FROM measured_values ORDER BY value;END $$").unwrap();
    assert_eq!(
        sql.execute("SELECT * FROM preferred_columns()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)], vec![SqlValue::Int(7)]]
    );
}
#[test]
fn unsupported_compiler_options_are_explicit_instead_of_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for (name, header) in [
        ("unsupported_variable", "#variable_conflict use_variable"),
        ("unsupported_error", "#variable_conflict error"),
        ("unknown_directive", "#unknown_option on"),
    ] {
        sql.execute(&format!("CREATE FUNCTION {name}() RETURNS int LANGUAGE plpgsql AS $$\n{header}\nBEGIN RETURN 1;END $$")).unwrap();
        let error = sql.execute(&format!("SELECT {name}()")).unwrap_err();
        assert_eq!(error.sqlstate(), "0A000");
        assert!(error.to_string().contains("compiler directive"));
    }
}

#[test]
fn exception_handler_comments_preserve_boundaries_values_and_rollback() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE handler_comment_rows(id int PRIMARY KEY,v int);INSERT INTO handler_comment_rows VALUES(1,10)").unwrap();
        sql.execute(
            "CREATE FUNCTION commented_handlers(n int) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
 BEGIN
  UPDATE handler_comment_rows SET v=99 WHERE id=1;
  PERFORM 10/n;
 EXCEPTION -- An ordinary application annotation.
  /* outer /* nested comment with WHEN OTHERS THEN */ still a comment */
  WHEN /* condition annotation */ division_by_zero THEN
   RETURN '-- literal /* remains intact */';
  -- The second handler must remain separate.
  WHEN OTHERS THEN RETURN 'unexpected handler';
 END;
 RETURN 'success';
END $$",
        )
        .unwrap();
        assert_eq!(
            sql.execute("SELECT commented_handlers(0)").unwrap().rows,
            vec![vec![SqlValue::String(
                "-- literal /* remains intact */".into()
            )]]
        );
        assert_eq!(
            sql.execute("SELECT v FROM handler_comment_rows")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(10)]]
        );
        assert_eq!(
            sql.execute("SELECT commented_handlers(2)").unwrap().rows,
            vec![vec![SqlValue::String("success".into())]]
        );
        assert_eq!(
            sql.execute("SELECT v FROM handler_comment_rows")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(99)]]
        );
    }
}
