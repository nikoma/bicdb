//! PostgreSQL parity, second batch: float output inside rows and JSON,
//! untyped-literal float overflow, lexer warnings, set-returning calls in
//! lockstep, quantified non-comparison operators, inherited NOT NULL names.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open(root: &std::path::Path) -> BicDb {
    BicDb::open_with_config(root, DbConfig::default().with_fsync(false)).unwrap()
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    SqlValue::Null => "NULL".to_string(),
                    SqlValue::Bool(value) => if value { "t" } else { "f" }.to_string(),
                    other => other.to_cell(),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn float4_values_print_as_float4_inside_rows_and_json() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT ROW(1.1::float4, 'NaN'::float8, 1e20::float8)::text, \
             to_json(1.1::float4)::text, row_to_json(ROW(1.1::float4))::text"
        ),
        vec![r#"(1.1,NaN,1e+20)|1.1|{"f1":1.1}"#]
    );
    assert_eq!(
        rows(&mut session, "SELECT to_char(1.1::float4, '9.99999999')"),
        vec![" 1.10000"]
    );
    session
        .execute("CREATE TABLE f(v float8); INSERT INTO f VALUES (1e300), (1e-300)")
        .unwrap();
    let error = session
        .execute("SELECT v * '1e200' FROM f WHERE v > 1")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22003");
    let error = session.execute("SELECT ln(0::float8)").unwrap_err();
    assert_eq!(error.to_string(), "cannot take logarithm of zero");
}

#[test]
fn nonstandard_escapes_warn_while_conforming_strings_are_off() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("SET standard_conforming_strings = off")
        .unwrap();
    let result = session.execute(r"SELECT 'a\\b', 'q\'r', 'plain'").unwrap();
    let warnings = result
        .notices
        .iter()
        .map(|notice| (notice.severity(), notice.message.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        warnings,
        vec![
            ("WARNING", r"nonstandard use of \\ in a string literal"),
            ("WARNING", r"nonstandard use of \' in a string literal"),
        ]
    );
    session.execute("SET escape_string_warning = off").unwrap();
    assert!(session
        .execute(r"SELECT 'a\\b'")
        .unwrap()
        .notices
        .is_empty());
}

#[test]
fn several_set_returning_calls_advance_in_lockstep() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION a() RETURNS SETOF int LANGUAGE sql AS 'SELECT 1 UNION ALL SELECT 2';
             CREATE FUNCTION b() RETURNS SETOF int LANGUAGE sql
               AS 'SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30';
             CREATE FUNCTION t(n int) RETURNS TABLE(x int, y text) LANGUAGE sql
               AS $$ SELECT g, 'v' || g FROM generate_series(1, n) g $$;
             CREATE FUNCTION p(n int) RETURNS SETOF int LANGUAGE plpgsql
               AS $$ BEGIN FOR i IN 1..n LOOP RETURN NEXT i * 2; END LOOP; END $$",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a(), b()"),
        vec!["1|10", "2|20", "NULL|30"]
    );
    assert_eq!(
        rows(&mut session, "SELECT t(2)::text"),
        vec!["(1,v1)", "(2,v2)"]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM p(3)"),
        vec!["2", "4", "6"]
    );
}

#[test]
fn quantified_operators_and_hstore_row_forms() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE v(d tsvector); INSERT INTO v VALUES ('a b'), ('c'), ('d');
             CREATE EXTENSION hstore; CREATE TABLE r(a int, b text)",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM v WHERE d @@ ANY('{a,c}')"
        ),
        vec!["2"]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT 'a'::tsvector @@ ANY(ARRAY[NULL, 'b']::tsquery[]), \
             'a'::tsvector @@ ALL(ARRAY[]::tsquery[])"
        ),
        vec!["NULL|t"]
    );
    assert_eq!(
        rows(&mut session, "SELECT (ROW(1, 'q')::r #= 'b=>z'::hstore).*"),
        vec!["1|z"]
    );
    session
        .execute(
            "CREATE TABLE h(x hstore); INSERT INTO h VALUES ('a=>1'), ('b=>2');
             CREATE INDEX h_gin ON h USING gin (x); CREATE INDEX h_gist ON h USING gist (x)",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM h WHERE x ? 'a'"),
        vec!["1"]
    );
}

#[test]
fn inherited_not_null_constraints_carry_the_parent_name() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path());
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE p(a int NOT NULL); CREATE TABLE c() INHERITS (p)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT conrelid::regclass::text, conname, coninhcount, conislocal \
             FROM pg_constraint WHERE contype = 'n' ORDER BY 1"
        ),
        vec!["c|p_a_not_null|1|f", "p|p_a_not_null|0|t"]
    );
}
