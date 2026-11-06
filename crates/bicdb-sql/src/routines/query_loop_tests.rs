use super::*;

fn call(source: &str, query: &str) -> Result<SqlResult> {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute(source)?;
    session.execute(query)
}

#[test]
fn routine_query_loop_binds_declared_text_as_scalar_for_json_keys() {
    let result = call(
        r#"CREATE FUNCTION validate_keys(cfg jsonb) RETURNS boolean LANGUAGE plpgsql AS $$
            DECLARE path text;
            BEGIN
                FOR path IN SELECT unnest(ARRAY['measurements','forms']) LOOP
                    IF cfg ? path AND jsonb_typeof(cfg->path) <> 'array' THEN
                        RETURN false;
                    END IF;
                END LOOP;
                RETURN true;
            END $$"#,
        r#"SELECT validate_keys('{"measurements":[],"forms":[]}'::jsonb),
                  validate_keys('{"measurements":[],"forms":4}'::jsonb),
                  validate_keys('{}'::jsonb)"#,
    )
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true)
        ]]
    );
}

#[test]
fn routine_query_loop_preserves_scalar_null_and_zero_row_values() {
    let result = call(
        r#"CREATE FUNCTION loop_values() RETURNS integer LANGUAGE plpgsql AS $$
            DECLARE item integer := 9; total integer := 0;
            BEGIN
                FOR item IN SELECT unnest(ARRAY[2,NULL,5]) LOOP
                    IF item IS NULL THEN total := total + 10;
                    ELSE total := total + item; END IF;
                END LOOP;
                FOR item IN SELECT 100 WHERE false LOOP total := -1000; END LOOP;
                RETURN total + item;
            END $$"#,
        "SELECT loop_values()",
    )
    .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(22)]]);
}

#[test]
fn routine_query_loop_distinguishes_json_scalar_and_single_column_record() {
    let result = call(
        r#"CREATE FUNCTION loop_json() RETURNS integer LANGUAGE plpgsql AS $$
            DECLARE scalar_value jsonb; record_value record; total integer := 0;
            BEGIN
                FOR scalar_value IN SELECT '{"score":3}'::jsonb AS payload LOOP
                    total := total + (scalar_value->>'score')::integer;
                END LOOP;
                FOR record_value IN SELECT 7 AS score LOOP
                    total := total + record_value.score;
                END LOOP;
                RETURN total;
            END $$"#,
        "SELECT loop_json()",
    )
    .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(10)]]);
}

#[test]
fn routine_query_loop_keeps_multicolumn_records_and_exit_value() {
    let result = call(
        r#"CREATE FUNCTION loop_record() RETURNS integer LANGUAGE plpgsql AS $$
            DECLARE item record;
            BEGIN
                FOR item IN SELECT 3 AS first_value, 4 AS second_value LOOP
                    EXIT;
                END LOOP;
                RETURN item.first_value + item.second_value;
            END $$"#,
        "SELECT loop_record()",
    )
    .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Int(7)]]);
}

#[test]
fn routine_query_loop_enforces_declared_scalar_type_modifier() {
    let error = call(
        r#"CREATE FUNCTION loop_short_text() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE item varchar(2);
            BEGIN
                FOR item IN SELECT 'too long' LOOP RETURN item; END LOOP;
                RETURN NULL;
            END $$"#,
        "SELECT loop_short_text()",
    )
    .unwrap_err();
    assert_eq!(error.sqlstate(), "22001");
}

#[test]
fn routine_query_loop_assigns_and_checks_registered_scalar_domain() {
    let source = r#"CREATE DOMAIN positive_loop_value AS integer CHECK (VALUE > 0);
        CREATE FUNCTION loop_domain(p integer) RETURNS integer LANGUAGE plpgsql AS $$
            DECLARE item positive_loop_value;
            BEGIN
                FOR item IN SELECT p LOOP RETURN item + 1; END LOOP;
                RETURN NULL;
            END $$"#;
    assert_eq!(
        call(source, "SELECT loop_domain(3)").unwrap().rows,
        vec![vec![SqlValue::Int(4)]]
    );
    assert_eq!(
        call(source, "SELECT loop_domain(-1)")
            .unwrap_err()
            .sqlstate(),
        "23514"
    );
}
