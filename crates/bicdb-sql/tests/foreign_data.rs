//! Foreign-data wrappers, servers and user mappings: DDL, options and their
//! validator, USAGE privileges, dependencies, comments and the catalogs.
//! Expected results were checked against PostgreSQL 18.4.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::SqlSession;

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn run(session: &mut SqlSession<'_>, sql: &str) {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

fn error(session: &mut SqlSession<'_>, sql: &str) -> String {
    match session.execute(sql) {
        Ok(_) => panic!("{sql}: unexpectedly succeeded"),
        Err(error) => error.to_string(),
    }
}

fn with_session(test: impl FnOnce(&mut SqlSession<'_>)) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    test(&mut session);
}

#[test]
fn wrappers_servers_and_mappings_round_trip_through_the_catalogs() {
    with_session(|session| {
        run(
            session,
            "CREATE FOREIGN DATA WRAPPER w1 OPTIONS (testing '1', \"two words\" 'x')",
        );
        run(
            session,
            "CREATE FOREIGN DATA WRAPPER pgw VALIDATOR postgresql_fdw_validator",
        );
        run(session, "COMMENT ON FOREIGN DATA WRAPPER w1 IS 'first'");
        assert_eq!(
            rows(
                session,
                "SELECT fdwname, fdwvalidator::regproc::text, fdwoptions::text FROM pg_foreign_data_wrapper ORDER BY 1"
            ),
            vec![
                vec!["pgw".to_string(), "postgresql_fdw_validator".to_string(), "".to_string()],
                vec![
                    "w1".to_string(),
                    "-".to_string(),
                    "{testing=1,\"two words=x\"}".to_string()
                ],
            ]
        );
        run(session, "CREATE SERVER s1 TYPE 'remote' VERSION '1.0' FOREIGN DATA WRAPPER pgw OPTIONS (host 'h', dbname 'd')");
        run(
            session,
            "CREATE USER MAPPING FOR current_user SERVER s1 OPTIONS (user 'u', password 'p')",
        );
        run(session, "CREATE USER MAPPING FOR public SERVER s1");
        assert_eq!(
            rows(
                session,
                "SELECT s.srvname, f.fdwname, s.srvtype, s.srvversion, \
                 array_to_string(ARRAY(SELECT option_name || '=' || option_value \
                 FROM pg_options_to_table(s.srvoptions)), ',') \
                 FROM pg_foreign_server s JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw"
            ),
            vec![vec![
                "s1".to_string(),
                "pgw".to_string(),
                "remote".to_string(),
                "1.0".to_string(),
                "host=h,dbname=d".to_string()
            ]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT srvname, usename, umoptions::text FROM pg_user_mappings ORDER BY usename"
            ),
            vec![
                vec![
                    "s1".to_string(),
                    "bicdb".to_string(),
                    "{user=u,password=p}".to_string()
                ],
                vec!["s1".to_string(), "public".to_string(), "".to_string()],
            ]
        );
        assert_eq!(
            rows(
                session,
                "SELECT d.description FROM pg_foreign_data_wrapper w \
                 JOIN pg_description d ON d.classoid = w.tableoid AND d.objoid = w.oid"
            ),
            vec![vec!["first".to_string()]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT foreign_server_name, option_name, option_value \
                 FROM information_schema.foreign_server_options ORDER BY 2"
            ),
            vec![
                vec!["s1".to_string(), "dbname".to_string(), "d".to_string()],
                vec!["s1".to_string(), "host".to_string(), "h".to_string()],
            ]
        );
    });
}

#[test]
fn options_are_checked_by_the_validator_and_the_alter_actions() {
    with_session(|session| {
        run(
            session,
            "CREATE FOREIGN DATA WRAPPER pgw VALIDATOR postgresql_fdw_validator",
        );
        assert_eq!(
            error(
                session,
                "CREATE SERVER bad FOREIGN DATA WRAPPER pgw OPTIONS (foo '1')"
            ),
            "invalid option \"foo\""
        );
        run(
            session,
            "CREATE SERVER s FOREIGN DATA WRAPPER pgw OPTIONS (host 'a')",
        );
        assert_eq!(
            error(
                session,
                "CREATE USER MAPPING FOR current_user SERVER s OPTIONS (username 'x')"
            ),
            "invalid option \"username\""
        );
        assert_eq!(
            error(session, "ALTER SERVER s OPTIONS (host 'b')"),
            "option \"host\" provided more than once"
        );
        assert_eq!(
            error(session, "ALTER SERVER s OPTIONS (SET port '1')"),
            "option \"port\" not found"
        );
        run(
            session,
            "ALTER SERVER s OPTIONS (SET host 'b', ADD port '5432')",
        );
        run(session, "ALTER SERVER s OPTIONS (DROP host)");
        assert_eq!(
            rows(session, "SELECT srvoptions::text FROM pg_foreign_server"),
            vec![vec!["{port=5432}".to_string()]]
        );
        assert_eq!(
            error(
                session,
                "CREATE FOREIGN DATA WRAPPER w OPTIONS (a '1', a '2')"
            ),
            "option \"a\" provided more than once"
        );
        assert_eq!(
            error(session, "CREATE FOREIGN DATA WRAPPER w VALIDATOR nope"),
            "function nope(text[], oid) does not exist"
        );
    });
}

#[test]
fn usage_privileges_gate_servers_and_mappings() {
    with_session(|session| {
        run(session, "CREATE ROLE fd_alice");
        run(session, "CREATE ROLE fd_bob");
        run(session, "CREATE FOREIGN DATA WRAPPER w");
        run(session, "SET ROLE fd_alice");
        assert_eq!(
            error(session, "CREATE SERVER s FOREIGN DATA WRAPPER w"),
            "permission denied for foreign-data wrapper w"
        );
        assert!(error(session, "CREATE FOREIGN DATA WRAPPER w2")
            .contains("permission denied to create foreign-data wrapper \"w2\""));
        run(session, "RESET ROLE");
        run(
            session,
            "GRANT USAGE ON FOREIGN DATA WRAPPER w TO fd_alice WITH GRANT OPTION",
        );
        run(session, "SET ROLE fd_alice");
        run(session, "CREATE SERVER s FOREIGN DATA WRAPPER w");
        run(session, "GRANT USAGE ON FOREIGN DATA WRAPPER w TO fd_bob");
        run(session, "CREATE USER MAPPING FOR fd_alice SERVER s");
        run(session, "RESET ROLE");
        assert_eq!(
            rows(session, "SELECT fdwacl::text FROM pg_foreign_data_wrapper"),
            vec![vec![
                "{bicdb=U/bicdb,fd_alice=U*/bicdb,fd_bob=U/fd_alice}".to_string()
            ]]
        );
        assert_eq!(
            rows(
                session,
                "SELECT has_foreign_data_wrapper_privilege('fd_bob', 'w', 'USAGE'), \
                 has_server_privilege('fd_bob', 's', 'USAGE')"
            ),
            vec![vec!["true".to_string(), "false".to_string()]]
        );
        assert_eq!(
            error(
                session,
                "REVOKE USAGE ON FOREIGN DATA WRAPPER w FROM fd_alice"
            ),
            "dependent privileges exist"
        );
        run(
            session,
            "REVOKE USAGE ON FOREIGN DATA WRAPPER w FROM fd_alice CASCADE",
        );
        assert_eq!(
            rows(session, "SELECT fdwacl::text FROM pg_foreign_data_wrapper"),
            vec![vec!["{bicdb=U/bicdb}".to_string()]]
        );
        // A mapping or a server keeps its role from being dropped.
        assert!(error(session, "DROP ROLE fd_alice").contains("cannot be dropped"));
        run(session, "SET ROLE fd_bob");
        assert_eq!(
            error(session, "DROP SERVER s"),
            "must be owner of foreign server s"
        );
        assert_eq!(
            error(session, "SELECT * FROM pg_user_mapping"),
            "permission denied for table pg_user_mapping"
        );
        run(session, "RESET ROLE");
        assert!(error(session, "DROP SERVER s").contains("because other objects depend on it"));
        run(session, "DROP FOREIGN DATA WRAPPER w CASCADE");
        assert_eq!(
            rows(session, "SELECT count(*) FROM pg_foreign_server"),
            vec![vec!["0".to_string()]]
        );
        run(session, "DROP ROLE fd_alice");
    });
}

#[test]
fn foreign_data_ddl_rolls_back_with_its_transaction() {
    with_session(|session| {
        run(session, "CREATE FOREIGN DATA WRAPPER kept");
        run(session, "BEGIN");
        run(session, "CREATE FOREIGN DATA WRAPPER gone");
        run(session, "DROP FOREIGN DATA WRAPPER kept");
        run(session, "ROLLBACK");
        assert_eq!(
            rows(session, "SELECT fdwname FROM pg_foreign_data_wrapper"),
            vec![vec!["kept".to_string()]]
        );
    });
}

#[test]
fn foreign_tables_import_and_handlers_refuse_clearly() {
    with_session(|session| {
        run(session, "CREATE FOREIGN DATA WRAPPER w");
        run(session, "CREATE SERVER s FOREIGN DATA WRAPPER w");
        assert_eq!(
            error(session, "IMPORT FOREIGN SCHEMA r FROM SERVER s INTO public"),
            "foreign-data wrapper \"w\" has no handler"
        );
        run(
            session,
            "CREATE FUNCTION not_a_handler() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        );
        assert_eq!(
            error(
                session,
                "CREATE FOREIGN DATA WRAPPER h HANDLER not_a_handler"
            ),
            "function not_a_handler must return type fdw_handler"
        );
    });
}
