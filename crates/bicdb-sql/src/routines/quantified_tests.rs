use super::*;

fn scalar(source: &str, query: &str) -> Result<SqlValue> {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(source)?;
    Ok(session.execute(query)?.rows[0][0].clone())
}

#[test]
fn routine_type_parser_preserves_type_details_and_requires_a_complete_type() {
    for declaration in [
        "jsonb",
        "text[]",
        "numeric(12, 3)",
        "character varying(40)",
        "timestamp(3) with time zone",
        "date",
        "pg_catalog.int4",
        "\"care schema\".\"Goal type\"",
    ] {
        let statement = format!("CREATE TABLE type_reference(value {declaration})");
        let Statement::CreateTable(table) = Parser::parse_sql(&PostgreSqlDialect {}, &statement)
            .unwrap()
            .remove(0)
        else {
            unreachable!()
        };
        let data_type = &table.columns[0].data_type;
        let (actual, date) = raw_routine_type_details(declaration).unwrap();
        assert_eq!(
            actual,
            routine_type_schema(data_type).unwrap(),
            "{declaration}"
        );
        assert_eq!(date, matches!(data_type, DataType::Date), "{declaration}");
    }
    for declaration in [
        "text); SELECT 1; --",
        "integer, extra text",
        "text DEFAULT 'x'",
    ] {
        assert!(
            raw_routine_type_details(declaration).is_err(),
            "{declaration}"
        );
    }
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
fn routine_series_arguments_resolve_from_the_frame_and_preserve_read_only_row_calls() {
    let source = r#"CREATE TABLE series_probe_calls(id integer PRIMARY KEY, n integer);
        INSERT INTO series_probe_calls VALUES(1,0);
        CREATE FUNCTION series_probe_tick() RETURNS integer LANGUAGE plpgsql AS $$
            BEGIN UPDATE series_probe_calls SET n=n+1 WHERE id=1; RETURN 1; END $$;
        CREATE FUNCTION series_probe(stop integer) RETURNS integer LANGUAGE plpgsql AS $$
            DECLARE total integer; calls integer;
            BEGIN
                SELECT count(*)::integer INTO total FROM generate_series(series_probe_tick(),stop) n;
                SELECT n INTO calls FROM series_probe_calls WHERE id=1;
                RETURN total * 10 + calls;
            END $$"#;
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(source).unwrap();
    let error = session.execute("SELECT series_probe(3)").unwrap_err();
    assert!(matches!(error,SqlError::Unsupported(ref text) if text.contains("can only read data")));
    assert_eq!(
        session
            .execute("SELECT n FROM series_probe_calls WHERE id=1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    let source = r#"CREATE FUNCTION series_dates(lo timestamp, hi timestamp, step interval) RETURNS integer LANGUAGE plpgsql AS $$
        BEGIN RETURN (SELECT count(*)::integer FROM generate_series(lo,hi,step) n); END $$"#;
    assert_eq!(
        scalar(
            source,
            "SELECT series_dates('2026-11-01'::timestamp,'2026-11-03'::timestamp,interval '1 day')"
        )
        .unwrap(),
        SqlValue::Int(3)
    );
}

#[test]
fn routine_jsonb_numeric_cast_preserves_exact_numbers_and_rejects_other_json_kinds() {
    let source = r#"CREATE FUNCTION json_numeric(value jsonb) RETURNS numeric LANGUAGE plpgsql AS $$
        BEGIN RETURN value::numeric; END $$"#;
    for value in ["0", "2.50", "-1.25", "9007199254740993.125"] {
        assert_eq!(
            scalar(
                source,
                &format!("SELECT json_numeric('{value}'::jsonb)::text")
            )
            .unwrap(),
            SqlValue::String(value.into())
        );
    }
    for value in ["null", "\"0\"", "true", "[]", "{}"] {
        assert_eq!(
            scalar(source, &format!("SELECT json_numeric('{value}'::jsonb)"))
                .unwrap_err()
                .sqlstate(),
            "22023"
        );
    }
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

#[test]
fn routine_monitoring_validator_accepts_actual_application_config_and_rejects_unknown_measurement()
{
    // Exercise the complete real validator on an ordinary protocol worker stack,
    // independent of a developer's RUST_MIN_STACK test configuration.
    std::thread::Builder::new()
        .name("ehr-monitoring-validator-stack".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let ddl = include_str!("../../tests/fixtures/ehr-monitoring-validator.sql");
            let config: JsonValue = serde_json::from_str(include_str!(
                "../../tests/fixtures/ehr-monitoring-config.json"
            ))
            .unwrap();
            let validate = |config: &JsonValue| {
                let text = serde_json::to_string(config).unwrap().replace('\'', "''");
                scalar(
                    ddl,
                    &format!("SELECT monitoring_validate_config('{text}'::jsonb)"),
                )
            };
            let SqlValue::Json(result) = validate(&config).unwrap() else {
                panic!("JSON validator response required")
            };
            assert_eq!(
                result["valid"],
                JsonValue::Bool(true),
                "{}",
                result["errors"]
            );
            let mut invalid = config.clone();
            invalid["forms"][0]["mappings"][0]["measurement"] =
                JsonValue::String("unknown_measurement".into());
            let SqlValue::Json(result) = validate(&invalid).unwrap() else {
                panic!("JSON validator response required")
            };
            assert_eq!(result["valid"], JsonValue::Bool(false));
            assert!(result["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value.as_str().unwrap().contains("unknown measurement")));
            for (description, expected) in [
                ("a".repeat(135000), true),
                ("é".repeat(135000), false),
                ("a".repeat(270000), false),
            ] {
                let mut sized = config.clone();
                sized["description"] = JsonValue::String(description);
                let SqlValue::Json(result) = validate(&sized).unwrap() else {
                    panic!("JSON validator response required")
                };
                assert_eq!(
                    result["valid"],
                    JsonValue::Bool(expected),
                    "{}",
                    result["errors"]
                );
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
