use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn integer_array_assignment_preserves_integer_arithmetic_and_decimal_amounts() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE results (quotient integer, amount numeric(6,2))")
        .unwrap();
    session
        .execute(
            r#"
        CREATE PROCEDURE amount_probe() LANGUAGE plpgsql AS $$
        DECLARE quantities SMALLINT[];
        BEGIN
            quantities[1] := round(2.0::double precision);
            INSERT INTO results
            SELECT qty / 4, qty * 44.90 * 1.30 * 0.75
            FROM unnest(quantities) AS inputs(qty);
        END;
        $$;
    "#,
        )
        .unwrap();
    for _ in 0..2 {
        session.execute("CALL amount_probe()").unwrap();
    }
    let result = session
        .execute("SELECT quotient, amount FROM results")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::Int(0), SqlValue::String("87.56".into())]; 2]
    );
}

#[test]
fn routine_assignments_enforce_integer_range_before_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE effects (id integer)")
        .unwrap();
    for (name, declaration, assignment) in [
        ("scalar_overflow", "value SMALLINT", "value := 32768"),
        ("array_overflow", "value SMALLINT[]", "value[1] := 32768"),
        ("into_overflow", "value SMALLINT", "SELECT 32768 INTO value"),
    ] {
        session.execute(&format!("CREATE PROCEDURE {name}() LANGUAGE plpgsql AS $$ DECLARE {declaration}; BEGIN {assignment}; INSERT INTO effects VALUES(1); END; $$;")).unwrap();
        let error = session.execute(&format!("CALL {name}()")).expect_err(name);
        assert_eq!(error.sqlstate(), "22003", "{name}: {error}");
    }
    assert_eq!(
        session
            .execute("SELECT count(*) FROM effects")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn routine_numeric_assignments_apply_declared_scale() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for (name, declaration, assignment, expression) in [
        (
            "scalar_scale",
            "value NUMERIC(5,2)",
            "value := 1.235",
            "value",
        ),
        (
            "array_scale",
            "value NUMERIC(5,2)[]",
            "value[1] := 1.235",
            "value[1]",
        ),
        (
            "into_scale",
            "value NUMERIC(5,2)",
            "SELECT 1.235 INTO value",
            "value",
        ),
    ] {
        session.execute(&format!("CREATE FUNCTION {name}() RETURNS numeric LANGUAGE plpgsql AS $$ DECLARE {declaration}; BEGIN {assignment}; RETURN {expression}; END; $$;")).unwrap();
        let result = session.execute(&format!("SELECT {name}()")).unwrap();
        assert_eq!(
            result.rows,
            vec![vec![SqlValue::String("1.24".into())]],
            "{name}"
        );
    }
}

#[test]
fn array_casts_apply_element_modifiers_and_preserve_nulls_and_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute("SELECT amount FROM unnest(CAST('[0:1]={1.235,NULL}' AS numeric(5,2)[])) AS inputs(amount)").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("1.24".into())], vec![SqlValue::Null]]
    );
    let bounds = session
        .execute("SELECT array_dims(CAST('[0:1]={1.235,NULL}' AS numeric(5,2)[]))")
        .unwrap();
    assert_eq!(bounds.rows, vec![vec![SqlValue::String("[0:1]".into())]]);
    let text = session
        .execute("SELECT value FROM unnest(CAST(ARRAY['abcd'] AS varchar(2)[])) AS inputs(value)")
        .unwrap();
    assert_eq!(text.rows, vec![vec![SqlValue::String("ab".into())]]);
}

#[test]
fn failed_array_assignment_preserves_the_previous_element() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            r#"
        CREATE FUNCTION assignment_error_probe() RETURNS integer LANGUAGE plpgsql AS $$
        DECLARE values_array SMALLINT[] := ARRAY[7];
        BEGIN
            values_array[1] := 32768;
            RETURN 0;
        EXCEPTION WHEN SQLSTATE '22003' THEN
            RETURN values_array[1];
        END;
        $$;
    "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT assignment_error_probe()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(7)]]
    );
}
