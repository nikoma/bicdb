use super::*;

fn scalar(source: &str, query: &str) -> Result<SqlValue> {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(source)?;
    Ok(session.execute(query)?.rows[0][0].clone())
}

#[test]
fn routine_quantified_json_parameter_and_local_array_use_live_values() {
    let source = r#"CREATE FUNCTION measurement_known(item jsonb, ids text[]) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RETURN (item->>'measurement') = ANY(ids); END $$"#;
    for (json, values, expected) in [
        (
            r#"{"measurement":"weight"}"#,
            "ARRAY['weight','pulse']",
            SqlValue::Bool(true),
        ),
        (
            r#"{"measurement":"missing"}"#,
            "ARRAY['weight','pulse']",
            SqlValue::Bool(false),
        ),
        (
            r#"{"measurement":"missing"}"#,
            "ARRAY['weight',NULL]",
            SqlValue::Null,
        ),
    ] {
        assert_eq!(
            scalar(
                source,
                &format!("SELECT measurement_known('{json}'::jsonb,{values})")
            )
            .unwrap(),
            expected
        );
    }
}

#[test]
fn routine_quantified_null_empty_and_all_preserve_three_valued_logic() {
    let source = r#"CREATE FUNCTION quantified_values(x integer, items integer[], use_all boolean) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN IF use_all THEN RETURN x = ALL(items); ELSE RETURN x = ANY(items); END IF; END $$"#;
    for (args, expected) in [
        ("1,ARRAY[1,NULL],false", SqlValue::Bool(true)),
        ("2,ARRAY[1,NULL],false", SqlValue::Null),
        ("2,ARRAY[1,NULL],true", SqlValue::Bool(false)),
        ("1,ARRAY[1,NULL],true", SqlValue::Null),
        ("NULL,ARRAY[]::integer[],false", SqlValue::Bool(false)),
        ("NULL,ARRAY[]::integer[],true", SqlValue::Bool(true)),
        ("1,NULL::integer[],false", SqlValue::Null),
        ("1,NULL::integer[],true", SqlValue::Null),
    ] {
        assert_eq!(
            scalar(source, &format!("SELECT quantified_values({args})")).unwrap(),
            expected,
            "{args}"
        );
    }
}

#[test]
fn routine_quantified_loop_rebinds_scalars_and_arrays_without_stale_results() {
    let result = scalar(
        r#"CREATE FUNCTION changing_arrays() RETURNS integer LANGUAGE plpgsql AS $$
        DECLARE item integer; ids integer[]; score integer := 0;
        BEGIN
            FOR item IN 1..3 LOOP
                ids := ARRAY[item];
                IF item = ANY(ids) THEN score := score + item; END IF;
                IF item + 1 = ANY(ids) THEN score := score - 100; END IF;
                IF item < ALL(ARRAY[item + 1,item + 2]) THEN score := score + 10; END IF;
            END LOOP;
            RETURN score;
        END $$"#,
        "SELECT changing_arrays()",
    )
    .unwrap();
    assert_eq!(result, SqlValue::Int(36));
}

#[test]
fn routine_quantified_subquery_uses_routine_parameters_and_checks_column_count() {
    let source = r#"CREATE FUNCTION from_query(x integer) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RETURN x = ANY(SELECT n FROM generate_series(1,x) n); END $$"#;
    assert_eq!(
        scalar(source, "SELECT from_query(3)").unwrap(),
        SqlValue::Bool(true)
    );
    let source = r#"CREATE FUNCTION too_many(x integer) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RETURN x = ANY(SELECT x,x); END $$"#;
    assert!(scalar(source, "SELECT too_many(3)").is_err());
}

#[test]
fn routine_quantified_ordinary_text_is_not_a_catalog_vector() {
    let source = r#"CREATE FUNCTION vector_match(x oid, items oidvector) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RETURN x = ANY(items); END $$"#;
    assert_eq!(
        scalar(source, "SELECT vector_match(23,'23 25'::oidvector)").unwrap(),
        SqlValue::Bool(true)
    );
    let source = r#"CREATE FUNCTION invalid_array(items text) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RETURN 'a' = ANY(items); END $$"#;
    for query in [
        "SELECT invalid_array('a b')",
        "SELECT invalid_array('{a,b}')",
        "SELECT invalid_array(NULL)",
    ] {
        assert_eq!(scalar(source, query).unwrap_err().sqlstate(), "42809");
    }
}
