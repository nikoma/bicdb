use super::*;
fn run(db: &mut BicDb, sql: &str) -> Result<SqlResult> {
    let lowered = lower_session_sql(db, sql, ORACLE_PREVIEW_PROFILE, None, "bicdb")?;
    SqlSession::new(db).execute(&lowered)
}
fn database(path: &std::path::Path) -> BicDb {
    let mut db = BicDb::open(path).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(
        &mut db,
        "CREATE TABLE dated_rows(id NUMBER PRIMARY KEY,created DATE,precise TIMESTAMP(6))",
    )
    .unwrap();
    for sql in [
        "INSERT INTO dated_rows VALUES(1,DATE '2020-01-01',TIMESTAMP '2020-01-01 12:00:00.123456')",
        "INSERT INTO dated_rows VALUES(2,DATE '2020-01-10',TIMESTAMP '2020-01-10 12:00:00.654321')",
    ] {
        run(&mut db, sql).unwrap();
    }
    db
}
#[test]
fn delete_day_arithmetic_uses_target_column_provenance() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    run(
        &mut db,
        "DELETE FROM dated_rows WHERE created+7<DATE '2020-01-09'",
    )
    .unwrap();
    assert_eq!(
        run(&mut db, "SELECT id FROM dated_rows").unwrap().rows[0][0].to_cell(),
        "2"
    );
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        run(
            &mut db,
            "SELECT TO_CHAR(created,'YYYY-MM-DD') FROM dated_rows"
        )
        .unwrap()
        .rows[0][0]
            .to_cell(),
        "2020-01-10"
    );
}
#[test]
fn aliased_delete_and_update_predicates_bind_logical_dates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    run(
        &mut db,
        "UPDATE dated_rows SET id=3 WHERE created+7<DATE '2020-01-09'",
    )
    .unwrap();
    run(
        &mut db,
        "DELETE FROM dated_rows d WHERE d.created+7<DATE '2020-01-09' AND d.id=3",
    )
    .unwrap();
    let result = run(&mut db, "SELECT id,precise FROM dated_rows").unwrap();
    assert_eq!(result.rows[0][0].to_cell(), "2");
    assert_eq!(result.rows[0][1].to_cell(), "2020-01-10 12:00:00.654321");
}
#[test]
fn typed_builtins_in_delete_predicates_share_target_resolution() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    run(&mut db,"DELETE FROM dated_rows WHERE TO_CHAR(created+7,'YYYY-MM-DD')='2020-01-08' AND DECODE(created,DATE '2020-01-01','Y','N')='Y'").unwrap();
    assert_eq!(
        run(&mut db, "SELECT COUNT(*) FROM dated_rows")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
}
#[test]
fn procedural_delete_capability_is_versioned() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    let source="CREATE FUNCTION remove_early RETURN NUMBER IS total NUMBER;BEGIN DELETE FROM dated_rows WHERE created+7<DATE '2020-01-09';SELECT COUNT(*) INTO total FROM dated_rows;RETURN total;END;";
    let legacy = lower_session_sql(&db, source, ORACLE_PREVIEW_PROFILE, None, "bicdb")
        .unwrap()
        .replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V22);
    SqlSession::new(&mut db).execute(&legacy).unwrap();
    assert!(run(&mut db, "SELECT remove_early() FROM DUAL").is_err());
    run(
        &mut db,
        &source.replace("remove_early", "remove_early_current"),
    )
    .unwrap();
    assert_eq!(
        run(&mut db, "SELECT remove_early_current() FROM DUAL")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "1"
    );
}
#[test]
fn native_dml_and_timestamp_restrictions_remain_independent() {
    let root = tempfile::tempdir().unwrap();
    let mut db = database(root.path());
    assert!(run(
        &mut db,
        "DELETE FROM dated_rows WHERE precise+7<TIMESTAMP '2020-01-09 00:00:00'"
    )
    .is_err());
    let mut native = SqlSession::new(&mut db);
    native
        .execute("CREATE TABLE native_numbers(id int PRIMARY KEY,n int)")
        .unwrap();
    native
        .execute("INSERT INTO native_numbers VALUES(1,1),(2,10)")
        .unwrap();
    native
        .execute("DELETE FROM native_numbers WHERE n+7<10")
        .unwrap();
    assert_eq!(
        native
            .execute("SELECT id FROM native_numbers")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "2"
    );
}
