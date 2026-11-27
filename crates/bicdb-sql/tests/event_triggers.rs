//! Event triggers: catalog, DDL errors and ddl_command_start/end firing.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn count(session: &mut SqlSession<'_>, sql: &str) -> i64 {
    match &session.execute(sql).unwrap().rows[0][0] {
        SqlValue::Int(value) => *value,
        other => panic!("{other:?}"),
    }
}

fn setup(session: &mut SqlSession<'_>) {
    session
        .execute("CREATE TABLE fired (event text, tag text)")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION log_event() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO fired VALUES (tg_event, tg_tag); END $$",
        )
        .unwrap();
}

#[test]
fn ddl_command_triggers_fire_with_event_and_tag() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    session
        .execute("CREATE EVENT TRIGGER s ON ddl_command_start EXECUTE FUNCTION log_event()")
        .unwrap();
    session
        .execute(
            "CREATE EVENT TRIGGER e ON ddl_command_end WHEN TAG IN ('create table') EXECUTE PROCEDURE log_event()",
        )
        .unwrap();
    session.execute("CREATE TABLE t (a int)").unwrap();
    session.execute("CREATE INDEX t_a ON t (a)").unwrap();
    // DML and global objects do not fire.
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    session.execute("CREATE ROLE r_evt").unwrap();
    let rows = session
        .execute("SELECT event, tag FROM fired ORDER BY event, tag")
        .unwrap()
        .rows;
    let rows = rows
        .iter()
        .map(|row| format!("{} {}", row[0].to_cell(), row[1].to_cell()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            "ddl_command_end CREATE TABLE",
            "ddl_command_start CREATE INDEX",
            "ddl_command_start CREATE TABLE",
        ]
    );
    session.execute("ALTER EVENT TRIGGER s DISABLE").unwrap();
    session.execute("DELETE FROM fired").unwrap();
    session.execute("CREATE TABLE t2 (a int)").unwrap();
    assert_eq!(count(&mut session, "SELECT count(*) FROM fired"), 1);
}

#[test]
fn an_error_in_an_end_trigger_undoes_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION deny() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'denied %', tg_tag; END $$",
        )
        .unwrap();
    session
        .execute("CREATE EVENT TRIGGER d ON ddl_command_end EXECUTE FUNCTION deny()")
        .unwrap();
    let error = session.execute("CREATE TABLE gone (a int)").unwrap_err();
    assert!(error.to_string().contains("denied CREATE TABLE"), "{error}");
    session.execute("DROP EVENT TRIGGER d").unwrap();
    assert_eq!(
        count(
            &mut session,
            "SELECT count(*) FROM pg_class WHERE relname = 'gone'"
        ),
        0
    );
}

#[test]
fn events_that_cannot_fire_faithfully_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    for event in ["sql_drop", "table_rewrite", "login"] {
        let error = session
            .execute(&format!(
                "CREATE EVENT TRIGGER x ON {event} EXECUTE FUNCTION log_event()"
            ))
            .unwrap_err();
        assert_eq!(error.sqlstate(), "0A000", "{event}: {error}");
    }
    let error = session
        .execute("SELECT * FROM pg_event_trigger_ddl_commands()")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "39P03");
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM pg_event_trigger"),
        0
    );
}

#[test]
fn event_triggers_require_a_superuser() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    setup(&mut session);
    session.execute("CREATE ROLE plain_evt").unwrap();
    session.execute("SET ROLE plain_evt").unwrap();
    let error = session
        .execute("CREATE EVENT TRIGGER n ON ddl_command_start EXECUTE FUNCTION log_event()")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
}

#[test]
fn event_trigger_failure_rolls_back_foreign_data_catalog_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE FUNCTION reject_wrapper() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'rejected %', tg_tag; END $$",
        )
        .unwrap();
    session
        .execute(
            "CREATE EVENT TRIGGER reject_wrapper ON ddl_command_end
             WHEN TAG IN ('CREATE FOREIGN DATA WRAPPER') EXECUTE FUNCTION reject_wrapper()",
        )
        .unwrap();
    let error = session
        .execute("CREATE FOREIGN DATA WRAPPER new_wrapper")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("rejected CREATE FOREIGN DATA WRAPPER"),
        "{error}"
    );
    assert_eq!(
        count(
            &mut session,
            "SELECT count(*) FROM pg_foreign_data_wrapper WHERE fdwname = 'new_wrapper'"
        ),
        0
    );
}
