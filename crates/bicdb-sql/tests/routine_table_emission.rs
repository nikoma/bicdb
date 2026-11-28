use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn statement_keyword_prefixes_remain_ordinary_variable_names() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION keyword_variables() RETURNS int LANGUAGE plpgsql AS $$ DECLARE open_h int;fetch_count int;close_total int;raise_total int;execute_total int;perform_total int;BEGIN open_h:=2;fetch_count:=3;close_total:=5;raise_total:=7;execute_total:=11;perform_total:=13;RETURN open_h+fetch_count+close_total+raise_total+execute_total+perform_total;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT keyword_variables()").unwrap().rows[0][0],
        SqlValue::Int(41)
    );
}

#[test]
fn table_return_next_snapshots_outputs_and_continues_the_loop() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION table_rows() RETURNS TABLE(n int,label text) LANGUAGE plpgsql AS $$ DECLARE i int;BEGIN RETURN NEXT;FOR i IN 1..3 LOOP n:=i;label:='row-'||i;RETURN NEXT;END LOOP;END $$").unwrap();
    let result = session.execute("SELECT * FROM table_rows()").unwrap();
    assert_eq!(result.columns, vec!["n", "label"]);
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::Null],
            vec![SqlValue::Int(1), SqlValue::String("row-1".into())],
            vec![SqlValue::Int(2), SqlValue::String("row-2".into())],
            vec![SqlValue::Int(3), SqlValue::String("row-3".into())]
        ]
    );
}

#[test]
fn reverse_for_loops_count_down_from_the_first_bound() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION countdown(high int) RETURNS text LANGUAGE plpgsql AS $$ DECLARE acc text := '';j int;BEGIN for j in reverse high .. 1 loop acc := acc || j;END LOOP;FOR j IN REVERSE 1 .. 3 LOOP acc := acc || 'x';END LOOP;RETURN acc;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT countdown(3)").unwrap().rows[0][0],
        SqlValue::String("321".into())
    );
    assert_eq!(
        session.execute("SELECT countdown(0)").unwrap().rows[0][0],
        SqlValue::String("".into())
    );
}

#[test]
fn table_return_next_and_return_query_preserve_emission_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION mixed_rows() RETURNS TABLE(n int) LANGUAGE plpgsql AS $$ BEGIN n:=2;RETURN NEXT;RETURN QUERY SELECT 7;n:=9;RETURN NEXT;RETURN;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT * FROM mixed_rows()").unwrap().rows,
        vec![
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(7)],
            vec![SqlValue::Int(9)]
        ]
    );
}

#[test]
fn return_next_expressions_are_refused_with_out_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION unsupported_rows() RETURNS TABLE(n int) LANGUAGE plpgsql AS $$ BEGIN RETURN NEXT 7;END $$").unwrap();
    // PostgreSQL: RETURN NEXT cannot have a parameter in function with OUT
    // parameters (42804).
    let error = session
        .execute("SELECT * FROM unsupported_rows()")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42804");
    session.execute("CREATE FUNCTION scalar_rows() RETURNS SETOF int LANGUAGE plpgsql AS $$ BEGIN RETURN NEXT 7; RETURN NEXT 8; END $$").unwrap();
    assert_eq!(
        session.execute("SELECT * FROM scalar_rows()").unwrap().rows,
        vec![vec![SqlValue::Int(7)], vec![SqlValue::Int(8)]]
    );
}

#[test]
fn sql_return_bodies_use_arguments_and_bind_tables_at_creation() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE SCHEMA shadow",
        "CREATE TABLE public.sql_return_data(n int)",
        "CREATE TABLE shadow.sql_return_data(n int)",
        "INSERT INTO public.sql_return_data VALUES (7)",
        "INSERT INTO shadow.sql_return_data VALUES (9)",
        "CREATE FUNCTION sql_return_scalar(a int, b int DEFAULT 2) RETURNS int LANGUAGE SQL RETURN a + b",
        "CREATE FUNCTION sql_return_table() RETURNS int RETURN (SELECT n FROM sql_return_data)",
        "CREATE FUNCTION sql_return_rows() RETURNS SETOF int RETURN generate_series(2, 4)",
        "SET search_path = shadow, public",
    ] {
        session.execute(sql).unwrap();
    }
    assert_eq!(
        session
            .execute("SELECT sql_return_scalar(3), sql_return_scalar(b => 2, a => 6), sql_return_table()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(5), SqlValue::Int(8), SqlValue::Int(7)]]
    );
    assert_eq!(
        session
            .execute("SELECT * FROM sql_return_rows()")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(3)],
            vec![SqlValue::Int(4)]
        ]
    );
    for sql in [
        "CREATE FUNCTION sql_return_error(a anyarray) RETURNS anyelement RETURN a[1]",
        "CREATE FUNCTION sql_return_error() RETURNS int LANGUAGE plpgsql RETURN 1",
        "CREATE FUNCTION sql_return_error() RETURNS int LANGUAGE sql AS 'SELECT 1' RETURN 1",
    ] {
        assert_eq!(
            session.execute(sql).unwrap_err().sqlstate(),
            "42P13",
            "{sql}"
        );
    }
}

#[test]
fn sql_atomic_body_keeps_statement_boundaries_and_parameter_scope() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let definition = r#"CREATE FUNCTION sql_atomic(x int) RETURNS int LANGUAGE SQL
        BEGIN /* body */ ATOMIC
            INSERT INTO atomic_output VALUES ($1, 'END; BEGIN ATOMIC');
            RETURN (SELECT CASE WHEN n > 0 THEN CASE WHEN n > 1 THEN n ELSE 1 END ELSE 0 END
                    FROM atomic_output WHERE n = x);
        END"#;
    let batch = format!("{definition}; SELECT 3;");
    assert_eq!(bicdb_sql::split_sql_statements(&batch).len(), 2);
    assert!(bicdb_sql::infer_parameter_types(&db, definition)
        .unwrap()
        .is_empty());
    assert!(bicdb_sql::infer_parameter_types(
        &db,
        "CREATE FUNCTION direct_arg(x int) RETURNS int RETURN $1"
    )
    .unwrap()
    .is_empty());

    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE atomic_output(n int, note text)")
        .unwrap();
    session.execute(&batch).unwrap();
    assert_eq!(
        session.execute("SELECT sql_atomic(5)").unwrap().rows,
        vec![vec![SqlValue::Int(5)]]
    );
    assert_eq!(
        session
            .execute("SELECT note FROM atomic_output")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("END; BEGIN ATOMIC".into())]]
    );
}
