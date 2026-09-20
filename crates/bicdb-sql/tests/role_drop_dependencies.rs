use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

#[test]
fn role_drop_requires_removal_of_every_persisted_object_dependency() {
    for (create, remove) in [
        ("CREATE SCHEMA AUTHORIZATION retiring", "ALTER SCHEMA retiring OWNER TO keeper"),
        ("CREATE TABLE held_rows (id INT PRIMARY KEY); ALTER TABLE held_rows OWNER TO retiring", "DROP TABLE held_rows"),
        ("SET ROLE retiring; CREATE TYPE held_type AS ENUM ('one'); RESET ROLE", "DROP TYPE held_type"),
        ("CREATE SEQUENCE granted_seq; GRANT USAGE ON SEQUENCE granted_seq TO retiring", "REVOKE USAGE ON SEQUENCE granted_seq FROM retiring"),
        ("CREATE SCHEMA granted_schema; GRANT USAGE ON SCHEMA granted_schema TO retiring", "REVOKE USAGE ON SCHEMA granted_schema FROM retiring"),
        ("CREATE DATABASE granted_db; GRANT CONNECT ON DATABASE granted_db TO retiring", "REVOKE CONNECT ON DATABASE granted_db FROM retiring"),
        ("CREATE FUNCTION granted_fn() RETURNS INT LANGUAGE SQL AS 'SELECT 1'; GRANT EXECUTE ON FUNCTION granted_fn() TO retiring", "REVOKE EXECUTE ON FUNCTION granted_fn() FROM retiring"),
        ("CREATE TYPE granted_type AS ENUM ('one'); GRANT USAGE ON TYPE granted_type TO retiring", "REVOKE USAGE ON TYPE granted_type FROM retiring"),
        ("CREATE TABLE granted_columns (id INT PRIMARY KEY); GRANT UPDATE (id) ON granted_columns TO retiring", "REVOKE UPDATE (id) ON granted_columns FROM retiring"),
        ("CREATE SEQUENCE held_seq; ALTER SEQUENCE held_seq OWNER TO retiring", "DROP SEQUENCE held_seq"),
        ("CREATE VIEW held_view AS SELECT 1 AS n; ALTER VIEW held_view OWNER TO retiring", "DROP VIEW held_view"),
        ("CREATE FUNCTION held_fn() RETURNS INT LANGUAGE SQL AS 'SELECT 1'; ALTER FUNCTION held_fn() OWNER TO retiring", "DROP FUNCTION held_fn()"),
        ("CREATE SCHEMA held_schema AUTHORIZATION retiring", "ALTER SCHEMA held_schema OWNER TO keeper"),
        ("CREATE DATABASE held_db OWNER retiring", "ALTER DATABASE held_db OWNER TO keeper"),
        ("CREATE TABLE held_rows (id INT PRIMARY KEY); CREATE POLICY access_rows ON held_rows TO retiring USING (true)", "DROP POLICY access_rows ON held_rows"),
        ("ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO retiring", "ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM retiring"),
        ("SET ROLE retiring; ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO reader; RESET ROLE", "SET ROLE retiring; ALTER DEFAULT PRIVILEGES REVOKE SELECT ON TABLES FROM reader; RESET ROLE"),
        ("CREATE TABLE held_rows (id INT PRIMARY KEY); GRANT SELECT ON held_rows TO retiring", "REVOKE SELECT ON held_rows FROM retiring"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE ROLE retiring; CREATE ROLE reader; CREATE ROLE keeper").unwrap();
        execute_steps(&mut session, create);
        let error = session.execute("DROP ROLE retiring").expect_err(create);
        assert_eq!(error.sqlstate(), "2BP01", "{create}: {error}");
        assert!(session.execute("CREATE ROLE retiring").is_err(), "role must still exist");
        execute_steps(&mut session, remove);
        session.execute("DROP ROLE retiring; CREATE ROLE retiring").unwrap();
    }
}

#[test]
fn membership_grantors_and_bulk_role_drop_preserve_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE ROLE retiring CREATEROLE; CREATE ROLE grouping; CREATE ROLE recipient; CREATE ROLE unrelated; SET ROLE retiring; GRANT grouping TO recipient; RESET ROLE").unwrap();
    assert_eq!(
        session
            .execute("DROP ROLE retiring")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    assert_eq!(
        session
            .execute("DROP ROLE unrelated, retiring")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    assert!(
        session.execute("CREATE ROLE unrelated").is_err(),
        "a failed bulk drop must not remove earlier roles"
    );
    execute_steps(
        &mut session,
        "REVOKE grouping FROM recipient; DROP ROLE unrelated, retiring; CREATE ROLE retiring",
    );
}

#[test]
fn revoked_privileges_do_not_return_after_restart_and_role_name_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE ROLE retiring; CREATE TABLE private_rows (id INT PRIMARY KEY); INSERT INTO private_rows VALUES (1); GRANT SELECT ON private_rows TO retiring").unwrap();
        assert_eq!(
            session
                .execute("DROP ROLE retiring")
                .unwrap_err()
                .sqlstate(),
            "2BP01"
        );
        session.execute("REVOKE SELECT ON private_rows FROM retiring; DROP ROLE retiring; CREATE ROLE retiring").unwrap();
    }
    drop(db);
    let mut reopened = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new_unprivileged(&mut reopened, "retiring");
    assert!(session.execute("SELECT * FROM private_rows").is_err());
}

fn execute_steps(session: &mut SqlSession<'_>, sql: &str) {
    for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
        session
            .execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

#[test]
fn schema_authorization_requires_an_existing_settable_owner() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE ROLE creator; CREATE ROLE owner_role")
        .unwrap();
    let mut session = SqlSession::new_unprivileged(&mut db, "creator");
    assert!(session
        .execute("CREATE SCHEMA foreign_owned AUTHORIZATION owner_role")
        .is_err());
    assert!(session
        .execute("CREATE SCHEMA unknown_owned AUTHORIZATION missing_role")
        .is_err());
    session
        .execute("CREATE SCHEMA self_owned AUTHORIZATION creator")
        .unwrap();
}
