use super::*;
fn run(db: &mut BicDb, source: &str) -> Result<SqlResult> {
    let sql = lower_profile_sql(db, source, ORACLE_PREVIEW_PROFILE)?;
    SqlSession::new(db).execute(&sql)
}
fn code(error: &SqlError) -> i64 {
    let detail: JsonValue = serde_json::from_str(&errors::transport_detail(error)).unwrap();
    detail["oracle_error_stack"][0]["code"].as_i64().unwrap()
}
#[test]
fn nominal_types_numeric_precision_and_null_predicates_are_distinct() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE TYPE other_labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE TYPE numbers AS TABLE OF NUMBER",
    ] {
        run(&mut db, source).unwrap();
    }
    for (source,expected) in [
        ("CASE WHEN labels() IS EMPTY THEN 'E' ELSE 'N' END","E"),
        ("CASE WHEN CAST(NULL AS labels) IS EMPTY THEN 'E' WHEN CAST(NULL AS labels) IS NOT EMPTY THEN 'N' ELSE 'U' END","U"),
        ("CASE WHEN labels(NULL) MULTISET INTERSECT labels(NULL) IS NOT EMPTY THEN 'Y' ELSE 'N' END","Y"),
        ("CASE WHEN labels('01') MULTISET INTERSECT labels('1') IS EMPTY THEN 'E' ELSE 'N' END","E"),
        ("CASE WHEN numbers(0.12345678901234567890123456789012345678) MULTISET INTERSECT numbers(0.12345678901234567890123456789012345679) IS EMPTY THEN 'E' ELSE 'N' END","E"),
    ] {assert_eq!(run(&mut db,&format!("SELECT {source} FROM DUAL")).unwrap().rows[0][0],SqlValue::String(expected.into()),"{source}");}
    let error = run(
        &mut db,
        "SELECT labels('a') MULTISET INTERSECT other_labels('a') FROM DUAL",
    )
    .unwrap_err();
    assert_eq!(code(&error), -932);
    SqlSession::new(&mut db)
        .execute("CREATE DOMAIN native_labels AS text[]")
        .unwrap();
    assert_eq!(run(&mut db,"SELECT CAST(ARRAY['a'] AS native_labels) MULTISET INTERSECT CAST(ARRAY['a'] AS native_labels) FROM DUAL").unwrap_err().sqlstate(),"0A000");
}
#[test]
fn all_distinct_and_precedence_preserve_multiplicity() {
    let a = array_value(vec![
        SqlValue::String("a".into()),
        SqlValue::String("a".into()),
        SqlValue::Null,
    ]);
    let b = array_value(vec![SqlValue::String("a".into()), SqlValue::Null]);
    for (op, count) in [
        (grammar::Operation::IntersectAll, 2),
        (grammar::Operation::IntersectDistinct, 2),
        (grammar::Operation::UnionAll, 5),
        (grammar::Operation::UnionDistinct, 2),
        (grammar::Operation::ExceptAll, 1),
        (grammar::Operation::ExceptDistinct, 0),
    ] {
        let result = evaluate(&[
            a.clone(),
            b.clone(),
            SqlValue::String(op.marker().into()),
            SqlValue::String("varchar".into()),
        ])
        .unwrap();
        assert_eq!(count, dense_elements(&result).unwrap().len(), "{op:?}");
    }
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(&mut db, "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)").unwrap();
    let value=run(&mut db,"SELECT labels('a','b') MULTISET UNION labels('b') MULTISET INTERSECT labels('b') FROM DUAL").unwrap();
    assert_eq!(dense_elements(&value.rows[0][0]).unwrap().len(), 3);
}
#[test]
fn registered_return_types_and_scope_restore_nominal_identity() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    for source in ["CREATE SCHEMA red","CREATE TYPE \"public\".labels AS TABLE OF VARCHAR2(1 BYTE)","CREATE TYPE red.labels AS TABLE OF VARCHAR2(8 BYTE)","CREATE FUNCTION red.labels_value RETURN labels IS BEGIN RETURN labels('é');END;","CREATE FUNCTION red.intersects(a labels,b labels) RETURN NUMBER IS BEGIN DECLARE a NUMBER:=7;BEGIN NULL;END;IF a MULTISET INTERSECT b IS NOT EMPTY THEN RETURN 1;ELSE RETURN 0;END IF;END;"] {run(&mut db,source).unwrap();}
    let value = run(
        &mut db,
        "SELECT red.intersects(red.labels_value(),red.labels('é')) FROM DUAL",
    )
    .unwrap();
    assert_eq!(value.rows[0][0].to_cell(), "1");
}
#[test]
fn legacy_compilers_and_native_routines_do_not_gain_collection_grammar() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(&mut db, "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)").unwrap();
    let definition="CREATE FUNCTION legacy_set RETURN NUMBER IS a labels:=labels('a');BEGIN IF a MULTISET INTERSECT a IS NOT EMPTY THEN RETURN 1;ELSE RETURN 0;END IF;END;";
    let lowered = lower_profile_sql(&db, definition, ORACLE_PREVIEW_PROFILE)
        .unwrap()
        .replace(PREVIEW_ROUTINE_LANGUAGE, PREVIEW_ROUTINE_LANGUAGE_V20);
    SqlSession::new(&mut db).execute(&lowered).unwrap();
    assert!(run(&mut db, "SELECT legacy_set() FROM DUAL").is_err());
    assert_eq!(
        load_routine(&db, RoutineKind::Function, "legacy_set")
            .unwrap()
            .unwrap()
            .language,
        PREVIEW_ROUTINE_LANGUAGE_V20
    );
    SqlSession::new(&mut db).execute("CREATE FUNCTION native_invalid() RETURNS INTEGER LANGUAGE sql AS $$SELECT ARRAY[1] MULTISET INTERSECT ARRAY[1]$$").unwrap();
    run(
        &mut db,
        "CREATE FUNCTION native_caller RETURN NUMBER IS BEGIN RETURN native_invalid();END;",
    )
    .unwrap();
    assert!(run(&mut db, "SELECT native_caller() FROM DUAL").is_err());
    assert!(!enabled());
    assert!(SqlSession::new(&mut db)
        .execute("SELECT ARRAY[1] IS EMPTY")
        .is_err());
}
#[test]
fn native_set_operations_keep_their_own_semantics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let values = SqlSession::new(&mut db)
        .execute("SELECT 1 AS v UNION ALL SELECT 1 ORDER BY v")
        .unwrap();
    assert_eq!(
        values.rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(1)]]
    );
    let values = SqlSession::new(&mut db)
        .execute("SELECT 1 AS v UNION SELECT 2 INTERSECT SELECT 2 ORDER BY v")
        .unwrap();
    assert_eq!(
        values.rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );
    assert!(SqlSession::new(&mut db)
        .execute("SELECT ARRAY[1] MULTISET INTERSECT ARRAY[1]")
        .is_err());
}
#[test]
fn platform_limits_are_checked_before_multiset_allocation() {
    let small = array_value(vec![SqlValue::Null; 50_001]);
    let error = evaluate(&[
        small.clone(),
        small,
        SqlValue::String(grammar::Operation::UnionAll.marker().into()),
        SqlValue::String("varchar".into()),
    ])
    .unwrap_err();
    assert_eq!(error.sqlstate(), "54000");
    let wide = array_value(vec![SqlValue::String("x".repeat(4000)); 2100]);
    assert_eq!(
        evaluate(&[
            wide,
            array_value(vec![]),
            SqlValue::String(grammar::Operation::ExceptAll.marker().into()),
            SqlValue::String("varchar".into())
        ])
        .unwrap_err()
        .sqlstate(),
        "54000"
    );
    let invalid = SqlValue::Json(serde_json::json!([{"large":"payload"}]).into());
    assert_eq!(
        evaluate(&[
            invalid,
            array_value(vec![]),
            SqlValue::String(grammar::Operation::UnionAll.marker().into()),
            SqlValue::String("varchar".into())
        ])
        .unwrap_err()
        .sqlstate(),
        "0A000"
    );
}

#[test]
fn ordinary_invokers_can_use_pure_multisets_without_admin_authority() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    ensure_database_profile(&mut db, ORACLE_PREVIEW_PROFILE).unwrap();
    run(&mut db, "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)").unwrap();
    run(&mut db,"CREATE FUNCTION invoker_set RETURN NUMBER IS a labels:=labels('a');BEGIN IF a MULTISET INTERSECT labels('a') IS NOT EMPTY THEN RETURN 1;ELSE RETURN 0;END IF;END;").unwrap();
    let sql = lower_profile_sql(
        &db,
        "SELECT invoker_set() FROM DUAL",
        ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE multiset_invoker").unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION \"INVOKER_SET\"() TO multiset_invoker")
        .unwrap();
    session.execute("SET ROLE multiset_invoker").unwrap();
    assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(), "1");
    assert_eq!(
        session
            .execute("SELECT pg_catalog.bicdb_advance_transaction_floor(0)")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}
