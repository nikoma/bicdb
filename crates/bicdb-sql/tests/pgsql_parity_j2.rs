//! PostgreSQL parity: error CONTEXT of PL/pgSQL and SQL functions, the
//! `max_stack_depth` setting, and further scalar functions. Expected values
//! are PostgreSQL 18's.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlErrorField, SqlSession, SqlValue};
use tempfile::TempDir;

fn open() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn cells(sql: &mut SqlSession<'_>, statement: &str) -> Vec<Vec<String>> {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("{statement}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

/// The error's message and its CONTEXT lines.
fn context(sql: &mut SqlSession<'_>, statement: &str) -> (String, Vec<String>) {
    let error = match sql.execute(statement) {
        Ok(result) => panic!("{statement} succeeded: {:?}", result.rows),
        Err(error) => error,
    };
    let lines = error
        .fields()
        .into_iter()
        .filter_map(|field| match field {
            SqlErrorField::Context(context) => Some(context),
            _ => None,
        })
        .flat_map(|context| context.lines().map(str::to_string).collect::<Vec<_>>())
        .collect();
    (error.to_string(), lines)
}

#[test]
fn routine_errors_carry_postgres_context() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE FUNCTION cx_raise(i integer) RETURNS int LANGUAGE plpgsql AS $$\nbegin\n  if i > 0 then\n    raise exception 'bad %', i;\n  end if;\n  return i;\nend$$",
        "CREATE FUNCTION cx_inner(x int) RETURNS int LANGUAGE plpgsql AS $$\nbegin\n  raise exception 'inner %', x;\nend$$",
        "CREATE FUNCTION cx_outer() RETURNS int LANGUAGE plpgsql AS $$\ndeclare r int;\nbegin\n  r := cx_inner(5);\n  return r;\nend$$",
        "CREATE FUNCTION cx_pout() RETURNS void LANGUAGE plpgsql AS $$\nbegin\n  perform cx_inner(1);\nend$$",
        "CREATE TABLE cx_t(a int primary key)",
        "CREATE FUNCTION cx_ins(a int) RETURNS void LANGUAGE plpgsql AS $$\nbegin\n  insert into cx_t values (a);\nend$$",
        "CREATE FUNCTION cx_two(a character, b character) RETURNS void LANGUAGE plpgsql AS $$\nbegin\n  raise exception 'two';\nend$$",
        "CREATE FUNCTION cx_handler() RETURNS text LANGUAGE plpgsql AS $$\nbegin\n  begin\n    raise exception 'first';\n  exception when others then\n    return 'caught ' || sqlerrm;\n  end;\nend$$",
    ] {
        sql.execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    assert_eq!(
        context(&mut sql, "SELECT cx_raise(3)"),
        (
            "bad 3".to_string(),
            vec!["PL/pgSQL function cx_raise(integer) line 4 at RAISE".to_string()]
        )
    );
    assert_eq!(
        context(&mut sql, "SELECT cx_outer()").1,
        [
            "PL/pgSQL function cx_inner(integer) line 3 at RAISE",
            "PL/pgSQL function cx_outer() line 4 at assignment",
        ]
    );
    assert_eq!(
        context(&mut sql, "SELECT cx_pout()").1,
        [
            "PL/pgSQL function cx_inner(integer) line 3 at RAISE",
            "SQL statement \"SELECT cx_inner(1)\"",
            "PL/pgSQL function cx_pout() line 3 at PERFORM",
        ]
    );
    sql.execute("SELECT cx_ins(1)").unwrap();
    assert_eq!(
        context(&mut sql, "SELECT cx_ins(1)").1,
        [
            "SQL statement \"insert into cx_t values (a)\"",
            "PL/pgSQL function cx_ins(integer) line 3 at SQL statement",
        ]
    );
    assert_eq!(
        context(&mut sql, "SELECT cx_two('a', 'b')").1,
        ["PL/pgSQL function cx_two(character,character) line 3 at RAISE"]
    );
    // A caught error leaves no context behind.
    assert_eq!(cells(&mut sql, "SELECT cx_handler()"), [["caught first"]]);
}
