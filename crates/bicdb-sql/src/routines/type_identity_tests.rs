use super::*;

#[test]
fn routine_return_names_preserve_identifier_case_and_separate_table_prefixes() {
    for (declaration, expected, returns_set) in [
        ("table_of_labels", "table_of_labels", false),
        ("\"MiXeD\"", "\"MiXeD\"", false),
        ("\"Owner\".\"MiXeD\"", "\"Owner\".\"MiXeD\"", false),
        ("SETOF \"Owner\".\"MiXeD\"", "\"Owner\".\"MiXeD\"", true),
        ("Owner.MyType", "owner.mytype", false),
        ("INTEGER", "integer", false),
        ("TABLE (value INTEGER)", "record", true),
    ] {
        let sql = format!(
            "CREATE FUNCTION type_identity() RETURNS {declaration} LANGUAGE sql AS $$SELECT NULL$$"
        );
        assert_eq!(
            raw_routine_return_type(&sql).unwrap(),
            (expected.into(), returns_set),
            "{declaration}"
        );
    }
}

#[test]
fn custom_routine_type_formatting_does_not_substitute_text() {
    let schema = raw_routine_type_schema("\"Owner\".\"MiXeD\"").unwrap();
    assert_eq!(schema.pg_type, "Owner.MiXeD");
    assert_eq!(schema.formatted(), "\"Owner\".\"MiXeD\"");
    let (declaration, _) = raw_routine_return_type(
        "CREATE FUNCTION f() RETURNS \"Owner\".\"MiXeD\" LANGUAGE sql AS $$SELECT NULL$$",
    )
    .unwrap();
    assert_eq!(raw_routine_type_schema(&declaration).unwrap(), schema);
    assert_eq!(
        raw_routine_type_schema("NUMERIC(9,2)").unwrap().formatted(),
        "numeric(9,2)"
    );
}

#[test]
fn raw_routine_return_declarations_preserve_native_type_modifiers() {
    for declaration in [
        "NUMERIC(9,2)",
        "VARCHAR(10)",
        "CHAR(3)",
        "TIMESTAMP(3)",
        "BIT(8)",
    ] {
        let sql = format!(
            "CREATE FUNCTION modified_type() RETURNS {declaration} LANGUAGE sql AS $$SELECT NULL$$"
        );
        let (returned, returns_set) = raw_routine_return_type(&sql).unwrap();
        assert!(!returns_set);
        let declared = raw_routine_type_schema(declaration).unwrap();
        assert!(declared.type_modifier.is_some());
        assert_eq!(
            raw_routine_type_schema(&returned).unwrap(),
            declared,
            "{declaration}"
        );
    }
}

#[test]
fn native_functions_return_registered_enum_and_array_domain_values() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE SCHEMA \"Owner\"",
        "CREATE TYPE \"Owner\".\"MiXeD\" AS ENUM ('ok','bad')",
        "CREATE DOMAIN table_of_labels AS text[]",
        "CREATE DOMAIN \"Owner\".table_of_labels AS integer",
        "CREATE FUNCTION \"Owner\".native_label_result() RETURNS table_of_labels LANGUAGE sql AS $$SELECT ARRAY['native',NULL]$$",
        "CREATE DOMAIN positive_result AS integer CHECK (VALUE>0)",
        "CREATE FUNCTION enum_result() RETURNS \"Owner\".\"MiXeD\" LANGUAGE sql AS $$SELECT 'ok'::\"Owner\".\"MiXeD\"$$",
        "CREATE FUNCTION label_result() RETURNS table_of_labels LANGUAGE sql AS $$SELECT ARRAY['a',NULL]$$",
        "CREATE FUNCTION constrained_result(p integer) RETURNS positive_result LANGUAGE sql AS $$SELECT p$$",
    ] { SqlSession::new(&mut db).execute(sql).unwrap(); }
    let result = SqlSession::new(&mut db)
        .execute("SELECT enum_result(),label_result(),constrained_result(3)")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::String("ok".into()));
    assert_eq!(
        nullable_array_values(&result.rows[0][1], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("a".into()), SqlValue::Null]
    );
    assert_eq!(result.rows[0][2], SqlValue::Int(3));
    assert!(SqlSession::new(&mut db)
        .execute("SELECT constrained_result(-1)")
        .is_err());
    let result = SqlSession::new(&mut db)
        .execute("SELECT \"Owner\".native_label_result()")
        .unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("native".into()), SqlValue::Null]
    );
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut db)
            .execute("SELECT enum_result()")
            .unwrap()
            .rows[0][0],
        SqlValue::String("ok".into())
    );
}

#[test]
fn oracle_functions_return_registered_collections_without_flattening_to_text() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION collection_value RETURN labels IS BEGIN RETURN labels('é',NULL);END;",
        "CREATE FUNCTION empty_collection RETURN labels IS BEGIN RETURN labels();END;",
    ] {
        let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    let sql = oracle::lower_profile_sql(
        &db,
        "SELECT collection_value(),empty_collection() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    let result = SqlSession::new(&mut db).execute(&sql).unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("é".into()), SqlValue::Null]
    );
    assert_eq!(
        nullable_array_values(&result.rows[0][1], "test")
            .unwrap()
            .unwrap(),
        vec![]
    );
}

#[test]
fn oracle_collection_return_types_resolve_in_the_declaring_schema() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE SCHEMA red",
        "CREATE TYPE red.labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION red.collection_value RETURN labels IS BEGIN RETURN labels('é',NULL);END;",
    ] {
        let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    let sql = oracle::lower_profile_sql(
        &db,
        "SELECT red.collection_value() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    let result = SqlSession::new(&mut db).execute(&sql).unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("é".into()), SqlValue::Null]
    );
    let routine = load_routine(&db, RoutineKind::Function, "red.collection_value")
        .unwrap()
        .unwrap();
    assert_eq!(routine.return_type, "RED.LABELS");
}

#[test]
fn saved_custom_return_declarations_repair_legacy_case_and_set_flags() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE SCHEMA \"Owner\"",
        "CREATE TYPE \"Owner\".\"MiXeD\" AS ENUM ('ok')",
        "CREATE DOMAIN table_of_labels AS text[]",
        "CREATE FUNCTION enum_result() RETURNS \"Owner\".\"MiXeD\" LANGUAGE sql AS $$SELECT 'ok'::\"Owner\".\"MiXeD\"$$",
        "CREATE FUNCTION label_result() RETURNS table_of_labels LANGUAGE sql AS $$SELECT ARRAY['a']$$",
    ] {
        SqlSession::new(&mut db).execute(sql).unwrap();
    }
    for (name, broken_type, broken_set) in [
        ("enum_result", "owner\".\"mixed", false),
        ("label_result", "record", true),
    ] {
        let mut routine = load_routine(&db, RoutineKind::Function, name)
            .unwrap()
            .unwrap();
        routine.return_type = broken_type.into();
        routine.return_type_declaration = Some("text".into());
        routine.returns_set = broken_set;
        save_routine(&mut db, &routine).unwrap();
    }
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let result = SqlSession::new(&mut db)
        .execute("SELECT enum_result(),label_result()")
        .unwrap();
    assert_eq!(result.rows[0][0], SqlValue::String("ok".into()));
    assert_eq!(
        nullable_array_values(&result.rows[0][1], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("a".into())]
    );
    let routine = load_routine(&db, RoutineKind::Function, "enum_result")
        .unwrap()
        .unwrap();
    assert_eq!(routine.return_type, "Owner.MiXeD");
    assert!(!routine.security_definer);
}

#[test]
fn collection_constructor_compilation_preserves_version_18_routines() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    let source = "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)";
    let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    SqlSession::new(&mut db).execute(&sql).unwrap();
    let source = "CREATE FUNCTION legacy_collection RETURN labels IS BEGIN RETURN labels('a');END;";
    let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE)
        .unwrap()
        .replace(
            oracle::PREVIEW_ROUTINE_LANGUAGE,
            oracle::PREVIEW_ROUTINE_LANGUAGE_V18,
        );
    SqlSession::new(&mut db).execute(&sql).unwrap();
    let query = oracle::lower_profile_sql(
        &db,
        "SELECT legacy_collection() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    assert!(SqlSession::new(&mut db).execute(&query).is_err());
    assert_eq!(
        load_routine(&db, RoutineKind::Function, "legacy_collection")
            .unwrap()
            .unwrap()
            .language,
        oracle::PREVIEW_ROUTINE_LANGUAGE_V18
    );
    assert!(!oracle::collections::routine_constructors_enabled());
    assert_eq!(
        oracle::preview_routine_language_version(oracle::PREVIEW_ROUTINE_LANGUAGE),
        Some(28)
    );
}

#[test]
fn registered_local_types_enforce_domains_and_restore_nested_shadowing() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE DOMAIN local_labels AS text[]",
        "CREATE DOMAIN local_positive AS integer CHECK (VALUE>0)",
        "CREATE FUNCTION local_array() RETURNS local_labels LANGUAGE plpgsql AS $$DECLARE v local_labels:=ARRAY['outer'];BEGIN DECLARE v integer:=7;BEGIN NULL;END;RETURN v;END;$$",
        "CREATE FUNCTION local_check() RETURNS integer LANGUAGE plpgsql AS $$DECLARE v local_positive:=3;BEGIN v:=-1;RETURN v;END;$$",
    ] {
        SqlSession::new(&mut db).execute(sql).unwrap();
    }
    let result = SqlSession::new(&mut db)
        .execute("SELECT local_array()")
        .unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("outer".into())]
    );
    assert!(SqlSession::new(&mut db)
        .execute("SELECT local_check()")
        .is_err());
}

#[test]
fn oracle_collection_locals_use_registered_declaring_schema_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE SCHEMA red",
        "CREATE TYPE red.labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION red.local_value RETURN labels IS v labels:=labels('é',NULL);BEGIN RETURN v;END;",
    ] {
        let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    let sql = oracle::lower_profile_sql(
        &db,
        "SELECT red.local_value() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    let result = SqlSession::new(&mut db).execute(&sql).unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("é".into()), SqlValue::Null]
    );
}

#[test]
fn oracle_dense_collection_methods_count_extend_copy_and_assign() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION dense_count RETURN NUMBER IS v labels:=labels();BEGIN v.EXTEND;v(1):='a';v.EXTEND(2,1);IF v(2)='a' AND v(3)='a' THEN RETURN v.COUNT;ELSE RETURN -1;END IF;END;",
    ] {
        let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    let sql = oracle::lower_profile_sql(
        &db,
        "SELECT dense_count() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    assert_eq!(
        SqlSession::new(&mut db).execute(&sql).unwrap().rows[0][0].to_cell(),
        "3"
    );
}

#[test]
fn collection_helpers_run_for_invokers_without_exposing_admin_functions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    oracle::ensure_database_profile(&mut db, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
    for source in [
        "CREATE TYPE labels AS TABLE OF VARCHAR2(8 BYTE)",
        "CREATE FUNCTION invoker_collection RETURN NUMBER IS v labels:=labels('a');BEGIN v.EXTEND;RETURN v.COUNT+1;END;",
    ] {
        let sql = oracle::lower_profile_sql(&db, source, oracle::ORACLE_PREVIEW_PROFILE).unwrap();
        SqlSession::new(&mut db).execute(&sql).unwrap();
    }
    let sql = oracle::lower_profile_sql(
        &db,
        "SELECT invoker_collection() FROM DUAL",
        oracle::ORACLE_PREVIEW_PROFILE,
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE collection_invoker").unwrap();
    session
        .execute("GRANT EXECUTE ON FUNCTION \"INVOKER_COLLECTION\"() TO collection_invoker")
        .unwrap();
    session.execute("SET ROLE collection_invoker").unwrap();
    assert_eq!(session.execute(&sql).unwrap().rows[0][0].to_cell(), "3");
    for query in [
        "SELECT pg_catalog.bicdb_advance_transaction_floor(0)",
        "SELECT pg_catalog.bicdb_oracle_unregistered_admin(0)",
    ] {
        assert_eq!(session.execute(query).unwrap_err().sqlstate(), "42501");
    }
}

#[test]
fn native_named_types_ignore_callers_national_character_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    for sql in [
        "CREATE DOMAIN \"NVARCHAR2\" AS text[]",
        "CREATE FUNCTION native_national_name() RETURNS \"NVARCHAR2\" LANGUAGE sql AS $$SELECT ARRAY['native']::\"NVARCHAR2\"$$",
    ] { SqlSession::new(&mut db).execute(sql).unwrap(); }
    let mut legacy = load_routine(&db, RoutineKind::Function, "native_national_name")
        .unwrap()
        .unwrap();
    legacy.return_type = "nvarchar2".into();
    legacy.return_type_declaration = Some("text".into());
    save_routine(&mut db, &legacy).unwrap();
    let _caller = oracle::national::Scope::new(true);
    let loaded = load_routine(&db, RoutineKind::Function, "native_national_name")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.return_type, "NVARCHAR2");
    let result = SqlSession::new(&mut db)
        .execute("SELECT native_national_name()")
        .unwrap();
    assert_eq!(
        nullable_array_values(&result.rows[0][0], "test")
            .unwrap()
            .unwrap(),
        vec![SqlValue::String("native".into())]
    );
}
