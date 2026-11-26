//! PostgreSQL predefined roles and type USAGE privileges. Every path a
//! predefined role newly allows has a canary for what it must NOT allow.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn open() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    (dir, db)
}

fn denied(sql: &mut SqlSession<'_>, statement: &str) -> String {
    let error = sql.execute(statement).unwrap_err();
    assert_eq!(error.sqlstate(), "42501", "{statement}: {error}");
    error.postgres_message()
}

#[test]
fn pg_read_all_data_reads_but_never_writes_or_bypasses_row_security() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE reader",
        "CREATE TABLE rd (a int)",
        "INSERT INTO rd VALUES (1), (2)",
        "CREATE TABLE rd_rls (a int)",
        "INSERT INTO rd_rls VALUES (1), (2)",
        "ALTER TABLE rd_rls ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY rd_one ON rd_rls USING (a = 1)",
        "GRANT pg_read_all_data TO reader",
        "SET ROLE reader",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        sql.execute("SELECT count(*) FROM rd").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
    assert_eq!(
        sql.execute("SELECT a FROM rd_rls").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
    for statement in [
        "INSERT INTO rd VALUES (3)",
        "UPDATE rd SET a = 0",
        "DELETE FROM rd",
        "TRUNCATE rd",
    ] {
        denied(&mut sql, statement);
    }
}

#[test]
fn pg_write_all_data_writes_but_never_reads() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE writer",
        "CREATE TABLE wr (a int)",
        "GRANT pg_write_all_data TO writer",
        "SET ROLE writer",
        "INSERT INTO wr VALUES (1)",
        "UPDATE wr SET a = 2",
        "DELETE FROM wr",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        denied(&mut sql, "SELECT * FROM wr"),
        "permission denied for table wr"
    );
    // A WHERE clause reads the table: SELECT is needed as in PostgreSQL.
    denied(&mut sql, "UPDATE wr SET a = 3 WHERE a = 1");
    denied(&mut sql, "TRUNCATE wr");
}

#[test]
fn predefined_roles_exist_are_reserved_and_only_admins_grant_them() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute(
            "SELECT rolname, rolcanlogin, rolsuper FROM pg_roles WHERE rolname IN ('pg_read_all_data', 'pg_maintain') ORDER BY 1"
        )
        .unwrap()
        .rows,
        vec![
            vec![
                SqlValue::String("pg_maintain".into()),
                SqlValue::Bool(false),
                SqlValue::Bool(false)
            ],
            vec![
                SqlValue::String("pg_read_all_data".into()),
                SqlValue::Bool(false),
                SqlValue::Bool(false)
            ],
        ]
    );
    let error = sql.execute("CREATE ROLE pg_mine").unwrap_err();
    assert_eq!(error.sqlstate(), "42939");
    assert_eq!(
        error.postgres_message(),
        "role name \"pg_mine\" is reserved"
    );
    assert_eq!(
        sql.execute("DROP ROLE pg_read_all_data")
            .unwrap_err()
            .sqlstate(),
        "2BP01"
    );
    // CREATEROLE without ADMIN OPTION must not hand out data access.
    sql.execute("CREATE ROLE manager CREATEROLE").unwrap();
    sql.execute("CREATE ROLE victim").unwrap();
    sql.execute("SET ROLE manager").unwrap();
    for statement in [
        "GRANT pg_read_all_data TO victim",
        "GRANT pg_write_all_data TO manager",
    ] {
        denied(&mut sql, statement);
    }
}

#[test]
fn type_usage_is_enforced_with_postgresql_texts() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE ROLE tu",
        "GRANT CREATE ON SCHEMA public TO tu",
        "CREATE DOMAIN tu_d AS int",
        "REVOKE USAGE ON DOMAIN tu_d FROM PUBLIC",
        "CREATE TYPE tu_t AS (a int)",
        "REVOKE ALL ON TYPE tu_t FROM PUBLIC",
        "CREATE TABLE tu_owned (a int)",
        "ALTER TABLE tu_owned OWNER TO tu",
        "SET ROLE tu",
    ] {
        sql.execute(statement).unwrap();
    }
    for (statement, message) in [
        (
            "CREATE TABLE tu1 (a tu_d)",
            "permission denied for type tu_d",
        ),
        (
            "CREATE TABLE tu2 (a tu_t[])",
            "permission denied for type tu_t",
        ),
        (
            "ALTER TABLE tu_owned ADD COLUMN b tu_d",
            "permission denied for type tu_d",
        ),
        (
            "CREATE FUNCTION tu_f(a tu_d) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
            "permission denied for type tu_d",
        ),
        (
            "CREATE FUNCTION tu_g(a int) RETURNS tu_d LANGUAGE sql AS $$ SELECT 1 $$",
            "permission denied for type tu_d",
        ),
    ] {
        assert_eq!(denied(&mut sql, statement), message, "{statement}");
    }
    sql.execute("RESET ROLE").unwrap();
    assert_eq!(
        sql.execute("GRANT USAGE ON DOMAIN tu_t TO tu")
            .unwrap_err()
            .sqlstate(),
        "42809"
    );
    assert_eq!(
        sql.execute("GRANT USAGE ON TYPE _tu_t TO tu")
            .unwrap_err()
            .sqlstate(),
        "0LP01"
    );
    sql.execute("GRANT USAGE ON DOMAIN tu_d TO tu").unwrap();
    sql.execute("SET ROLE tu").unwrap();
    sql.execute("CREATE TABLE tu3 (a tu_d)").unwrap();
}
