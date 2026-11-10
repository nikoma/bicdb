//! User-defined functions that share a built-in function's name resolve by
//! PostgreSQL's rules: a user function whose input types match the call's
//! argument types exactly is chosen over a built-in that needs a conversion,
//! wherever its schema is on the search_path; when a built-in has the very
//! same signature, the search_path order decides, with `pg_catalog` searched
//! first unless the path names it. Expected values were taken from
//! PostgreSQL 16.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn cells(session: &mut SqlSession<'_>, sql: &str) -> Vec<String> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .flatten()
        .map(|value| value.to_cell())
        .collect()
}

fn setup(session: &mut SqlSession<'_>) {
    for sql in [
        "CREATE SCHEMA s",
        "CREATE FUNCTION s.trunc(timestamptz) RETURNS text LANGUAGE sql AS $$ SELECT 'user-trunc' $$",
        "CREATE FUNCTION s.lower(text) RETURNS text LANGUAGE sql AS $$ SELECT 'user-lower' $$",
        "CREATE FUNCTION s.round(numeric, numeric) RETURNS text LANGUAGE sql AS $$ SELECT 'user-round' $$",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

const PROBE: &str =
    "SELECT trunc(now()), lower('A'), round(1.5, 1), round(1.5, 1::numeric), trunc(12.7)";

#[test]
fn user_functions_of_builtin_names_follow_type_and_path_rules() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    for (path, expected) in [
        ("s", ["user-trunc", "a", "1.5", "user-round", "12"]),
        (
            "s, pg_catalog",
            ["user-trunc", "user-lower", "1.5", "user-round", "12"],
        ),
        (
            "pg_catalog, s",
            ["user-trunc", "a", "1.5", "user-round", "12"],
        ),
    ] {
        session
            .execute(&format!("SET search_path = {path}"))
            .unwrap();
        assert_eq!(cells(&mut session, PROBE), expected, "search_path = {path}");
    }
}

/// The application's pattern: date truncation through a user `trunc`
/// overload inside PL/pgSQL, next to the numeric built-in.
#[test]
fn routines_call_user_overloads_of_builtin_names_by_argument_type() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE SCHEMA app",
        "SET search_path = app, pg_catalog",
        "CREATE FUNCTION trunc(datetime timestamptz) RETURNS timestamptz LANGUAGE plpgsql AS $$ \
         BEGIN RETURN date_trunc('day', datetime); END; $$",
        "CREATE FUNCTION due(doc timestamptz, days numeric) RETURNS text LANGUAGE plpgsql AS $$ \
         DECLARE d timestamptz; BEGIN d := TRUNC(doc); \
         RETURN to_char(d, 'YYYY-MM-DD') || ' ' || trunc(days + 0.7)::text; END; $$",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert_eq!(
        cells(
            &mut session,
            "SELECT due(timestamptz '2026-03-15 13:45:00+00', 2)"
        ),
        ["2026-03-15 2"]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT trunc(timestamptz '2026-03-15 13:45:00+00')"
        ),
        ["2026-03-15 00:00:00+00"]
    );
    let _ = SqlValue::Null;
}

/// Resolution holds after the database is reopened (a server restart), when
/// the routine catalog has not been read yet in this process, in both
/// storage modes, for overloads of built-in names and ordinary overloads.
#[test]
fn overloads_of_builtin_names_resolve_after_reopen() {
    use bicdb_core::{DbConfig, StorageMode};
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                root.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut session = SqlSession::new(&mut db);
            setup(&mut session);
            for sql in [
                "CREATE FUNCTION s.trunc(interval) RETURNS text LANGUAGE sql AS $$ SELECT 'user-interval' $$",
                // Ordinary overloads (no built-in of the name): the first one
                // created must not take the others' calls after a restart.
                "CREATE FUNCTION s.adddays(i interval, d numeric) RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$",
                "CREATE FUNCTION s.adddays(t timestamptz, d numeric) RETURNS timestamptz LANGUAGE sql AS $$ SELECT t + interval '1 day' * d $$",
                "CREATE FUNCTION s.kind(n integer) RETURNS text LANGUAGE sql AS $$ SELECT 'integer' $$",
                "CREATE FUNCTION s.kind(t text) RETURNS text LANGUAGE sql AS $$ SELECT 'text' $$",
            ] {
                session.execute(sql).unwrap();
            }
        }
        let mut db = open();
        let mut session = SqlSession::new(&mut db);
        session.execute("SET search_path = s, pg_catalog").unwrap();
        assert_eq!(
            cells(
                &mut session,
                "SELECT trunc(now()), trunc(interval '1 day'), trunc(12.7), lower('A')"
            ),
            ["user-trunc", "user-interval", "12", "user-lower"],
            "{mode}"
        );
        session.execute("SET TIME ZONE 'UTC'").unwrap();
        assert_eq!(
            cells(
                &mut session,
                "SELECT adddays(timestamptz '2024-01-31 00:00:00+00', 10::numeric), \
                 kind(1), kind('a'::text)"
            ),
            ["2024-02-10 00:00:00+00", "integer", "text"],
            "{mode}"
        );
    }
}

/// The result type of a user function that takes a built-in name's call is
/// known before it runs, so an operator over it resolves to the user-defined
/// operator for that type (the application's `trunc(date) + days`).
#[test]
fn user_overload_results_type_operator_resolution() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE SCHEMA app",
        "SET search_path = app, pg_catalog",
        "CREATE FUNCTION trunc(datetime timestamptz) RETURNS timestamptz LANGUAGE sql AS \
         $$ SELECT date_trunc('day', datetime) $$",
        "CREATE FUNCTION adddays(datetime timestamptz, days numeric) RETURNS timestamptz \
         LANGUAGE sql AS $$ SELECT datetime + (interval '1' second * (86400 * days)) $$",
        "CREATE OPERATOR + (LEFTARG = timestamptz, RIGHTARG = numeric, PROCEDURE = adddays)",
        "SET TIME ZONE 'UTC'",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert_eq!(
        cells(
            &mut session,
            "SELECT TRUNC(timestamptz '2024-01-31 13:45:00+00') + 30::numeric"
        ),
        ["2024-03-01 00:00:00+00"]
    );
}

/// Inside PL/pgSQL, a cast of a user overload's result checks the type of
/// the overload the arguments select, not the name's first routine's.
#[test]
fn routine_casts_of_user_overload_results() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE SCHEMA app",
        "SET search_path = app, pg_catalog",
        "CREATE FUNCTION trunc(i interval) RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$",
        "CREATE FUNCTION trunc(datetime timestamptz) RETURNS timestamptz LANGUAGE sql AS \
         $$ SELECT date_trunc('day', datetime) $$",
        "CREATE FUNCTION adddays(datetime timestamptz, days numeric) RETURNS timestamptz \
         LANGUAGE sql AS $$ SELECT datetime + (interval '1' second * (86400 * days)) $$",
        "CREATE OPERATOR + (LEFTARG = timestamptz, RIGHTARG = numeric, PROCEDURE = adddays)",
        "SET TIME ZONE 'UTC'",
        "CREATE FUNCTION day_of(d timestamptz) RETURNS text LANGUAGE plpgsql AS \
         $$ BEGIN RETURN CAST(TRUNC(d) AS timestamptz)::text; END $$",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert_eq!(
        cells(
            &mut session,
            "SELECT day_of(timestamptz '2024-01-31 13:45:00+00')"
        ),
        ["2024-01-31 00:00:00+00"]
    );
}

#[test]
fn typed_date_call_uses_implicit_user_overload_when_builtin_cannot_accept_it() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_storage_mode(mode)
                .with_fsync(false),
        )
        .unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE SCHEMA implicit_calls",
            "SET search_path=implicit_calls,pg_catalog",
            "CREATE FUNCTION trunc(timestamp) RETURNS text LANGUAGE sql AS $$ SELECT 'without-zone' $$",
            "CREATE FUNCTION trunc(timestamptz) RETURNS text LANGUAGE sql AS $$ SELECT 'with-zone' $$",
            "CREATE FUNCTION wrap_date(d date) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN trunc(d); END $$",
            "CREATE TABLE selected_calls(v text)",
            "INSERT INTO selected_calls VALUES(trunc(CAST('2026-11-10' AS date)))",
        ] { session.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
        assert_eq!(cells(&mut session,"SELECT trunc(date '2026-11-10'),trunc(timestamp '2026-11-10 13:00:00'),trunc(timestamptz '2026-11-10 13:00:00+00'),trunc(12.7),wrap_date(date '2026-11-10')"),["with-zone","without-zone","with-zone","12","with-zone"]);
        assert_eq!(
            cells(&mut session, "SELECT v FROM selected_calls"),
            ["with-zone"]
        );
    }
}
