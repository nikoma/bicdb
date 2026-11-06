use super::*;

fn run(session: &mut SqlSession<'_>, source: &str) -> Result<SqlResult> {
    let sql = lower_session_sql(
        session.db_ref(),
        source,
        ORACLE_PREVIEW_PROFILE,
        None,
        "bicdb",
    )?;
    session.execute(&sql)
}

fn setup() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    let mut session = SqlSession::new(&mut db);
    run(
        &mut session,
        "CREATE TABLE payloads(id NUMBER PRIMARY KEY,b BLOB,r BYTEA,t VARCHAR2(20))",
    )
    .unwrap();
    run(&mut session, "INSERT INTO payloads VALUES(1,HEXTORAW('00FF'),HEXTORAW('00FF'),'text'),(2,EMPTY_BLOB(),NULL,NULL),(3,NULL,NULL,NULL)").unwrap();
    drop(session);
    (root, db)
}

#[test]
fn sql_blob_reads_distinguish_empty_null_and_persistent_values() {
    let (root, mut db) = setup();
    let mut session = SqlSession::new(&mut db);
    let result = run(&mut session, "SELECT SYS.DBMS_LOB.GETLENGTH(lob_loc=>p.b),DBMS_LOB.ISTEMPORARY(p.b) FROM payloads p ORDER BY id").unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(2), SqlValue::Int(0)],
            vec![SqlValue::Int(0), SqlValue::Int(0)],
            vec![SqlValue::Null, SqlValue::Null]
        ]
    );
    assert_eq!(run(&mut session,"SELECT NVL(DBMS_LOB.GETLENGTH(b),9)=CASE WHEN id=1 THEN 2 WHEN id=2 THEN 0 ELSE 9 END FROM payloads ORDER BY id").unwrap().rows,
        vec![vec![SqlValue::Bool(true)];3]);
    drop(session);
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        run(
            &mut session,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads ORDER BY id"
        )
        .unwrap()
        .rows,
        vec![
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(0)],
            vec![SqlValue::Null]
        ]
    );
}

#[test]
fn sql_blob_reads_follow_transaction_savepoint_and_observer_visibility() {
    let (_root, db) = setup();
    let mut writer = SqlSession::new_shared(&db);
    let mut observer = SqlSession::new_shared(&db);
    run(&mut writer, "BEGIN").unwrap();
    run(
        &mut writer,
        "UPDATE payloads SET b=HEXTORAW('AB') WHERE id=1",
    )
    .unwrap();
    assert_eq!(
        run(
            &mut writer,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads WHERE id=1"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        run(
            &mut observer,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads WHERE id=1"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    run(&mut writer, "SAVEPOINT before_blob").unwrap();
    run(
        &mut writer,
        "UPDATE payloads SET b=HEXTORAW('010203') WHERE id=1",
    )
    .unwrap();
    assert_eq!(
        run(
            &mut writer,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads WHERE id=1"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(3)]]
    );
    run(&mut writer, "ROLLBACK TO SAVEPOINT before_blob").unwrap();
    assert_eq!(
        run(
            &mut writer,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads WHERE id=1"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    run(&mut writer, "ROLLBACK").unwrap();
    assert_eq!(
        run(
            &mut writer,
            "SELECT DBMS_LOB.GETLENGTH(b) FROM payloads WHERE id=1"
        )
        .unwrap()
        .rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn sql_blob_reads_require_provenance_even_for_null_rows_and_describe_without_execution() {
    let (_root, mut db) = setup();
    for query in [
        "SELECT DBMS_LOB.GETLENGTH(r) FROM payloads WHERE id=2",
        "SELECT DBMS_LOB.GETLENGTH(t) FROM payloads WHERE id=2",
        "SELECT DBMS_LOB.GETLENGTH(NULL) FROM DUAL",
        "SELECT DBMS_LOB.GETLENGTH(b) OVER() FROM payloads",
    ] {
        assert!(
            lower_session_sql(&db, query, ORACLE_PREVIEW_PROFILE, None, "bicdb").is_err(),
            "{query}"
        );
    }
    let mut session = SqlSession::new(&mut db);
    run(&mut session, "CREATE TABLE observations(n NUMBER)").unwrap();
    run(&mut session,"CREATE FUNCTION observe_lob RETURN NUMBER IS BEGIN INSERT INTO observations VALUES(1); RETURN 1; END;").unwrap();
    let sql = lower_session_sql(
        session.db_ref(),
        "SELECT DBMS_LOB.GETLENGTH(p.b) FROM payloads p WHERE observe_lob()=1",
        ORACLE_PREVIEW_PROFILE,
        None,
        "bicdb",
    )
    .unwrap();
    assert_eq!(
        infer_query_result_types(session.db_ref(), &sql).unwrap(),
        Some(vec![Some("int8".into())])
    );
    assert_eq!(
        run(&mut session, "SELECT COUNT(*) FROM observations")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn postgres_qualified_user_functions_are_not_oracle_package_calls() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE SCHEMA \"DBMS_LOB\"").unwrap();
    session.execute("CREATE FUNCTION \"DBMS_LOB\".\"GETLENGTH\"(b BYTEA) RETURNS INTEGER LANGUAGE SQL AS $$SELECT octet_length(b)+5$$").unwrap();
    let sql = lower_session_sql(
        session.db_ref(),
        "SELECT \"DBMS_LOB\".\"GETLENGTH\"(decode('AB','hex'))",
        POSTGRES_PROFILE,
        None,
        "bicdb",
    )
    .unwrap();
    assert_eq!(
        session.execute(&sql).unwrap().rows,
        vec![vec![SqlValue::Int(6)]]
    );
}
