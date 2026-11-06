use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn refused(sql: &mut SqlSession<'_>, statement: &str) {
    let error = sql.execute(statement).expect_err(statement);
    assert_eq!(error.sqlstate(), "25006", "{statement}: {error}");
}

#[test]
fn read_only_refuses_direct_and_cte_writes_before_any_row_changes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int PRIMARY KEY,n int);INSERT INTO guarded VALUES(1,10)")
        .unwrap();
    for statement in [
        "INSERT INTO guarded VALUES(2,20)",
        "UPDATE guarded SET n=99",
        "DELETE FROM guarded",
        "TRUNCATE guarded",
        "WITH changed AS (DELETE FROM guarded RETURNING id) SELECT count(*) FROM changed",
    ] {
        sql.execute("BEGIN READ ONLY").unwrap();
        refused(&mut sql, statement);
        sql.execute("ROLLBACK").unwrap();
        assert_eq!(
            sql.execute("SELECT * FROM guarded").unwrap().rows,
            vec![vec![SqlValue::Int(1), SqlValue::Int(10)]]
        );
    }
    sql.execute("BEGIN READ ONLY;SELECT n FROM guarded;COMMIT;INSERT INTO guarded VALUES(2,20)")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM guarded").unwrap().rows,
        vec![vec![SqlValue::Int(2)]]
    );
}

#[test]
fn read_only_reaches_procedural_writes_and_nontransactional_sequences() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int PRIMARY KEY);CREATE SEQUENCE guarded_seq START 10;CREATE FUNCTION guarded_write() RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO guarded VALUES(1); RETURN 1; END $$").unwrap();
    for statement in [
        "SELECT guarded_write()",
        "SELECT nextval('guarded_seq')",
        "SELECT setval('guarded_seq',99)",
    ] {
        sql.execute("BEGIN READ ONLY").unwrap();
        refused(&mut sql, statement);
        sql.execute("ROLLBACK").unwrap();
    }
    assert_eq!(
        sql.execute("SELECT nextval('guarded_seq')").unwrap().rows,
        vec![vec![SqlValue::Int(10)]]
    );
    assert_eq!(
        sql.execute("SELECT count(*) FROM guarded").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn read_only_survives_reset_all_savepoints_and_reports_its_actual_mode() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int)").unwrap();
    sql.execute("BEGIN READ ONLY;SAVEPOINT preserved;SET app.fixture='value';RESET ALL;ROLLBACK TO preserved").unwrap();
    assert_eq!(
        sql.execute("SHOW transaction_read_only").unwrap().rows,
        vec![vec![SqlValue::String("on".into())]]
    );
    refused(&mut sql, "INSERT INTO guarded VALUES(1)");
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SHOW transaction_read_only").unwrap().rows,
        vec![vec![SqlValue::String("off".into())]]
    );
    sql.execute("BEGIN;SET TRANSACTION READ ONLY").unwrap();
    refused(&mut sql, "INSERT INTO guarded VALUES(2)");
    sql.execute("ROLLBACK").unwrap();
}

#[test]
fn read_only_cannot_be_unset_with_generic_settings() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int)").unwrap();
    for escape in [
        "SET transaction_read_only=off",
        "SELECT set_config('transaction_read_only','off',true)",
        "RESET transaction_read_only",
        "SET TRANSACTION READ WRITE",
    ] {
        sql.execute("BEGIN READ ONLY").unwrap();
        assert!(sql.execute(escape).is_err(), "{escape}");
        refused(&mut sql, "INSERT INTO guarded VALUES(1)");
        sql.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn read_only_refuses_raw_ddl_batches_and_privilege_changes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int);CREATE ROLE guarded_guest")
        .unwrap();
    for statement in [
        "CREATE TABLE forbidden(id int)",
        "ALTER TABLE guarded ADD COLUMN forbidden int",
        "GRANT SELECT ON guarded TO guarded_guest",
        "DO $$ BEGIN INSERT INTO guarded VALUES(1); END $$",
    ] {
        sql.execute("BEGIN READ ONLY").unwrap();
        refused(&mut sql, statement);
        sql.execute("ROLLBACK").unwrap();
    }
}

#[test]
fn read_only_function_cannot_end_its_callers_transaction_then_write() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE guarded(id int);CREATE FUNCTION escape_readonly() RETURNS int LANGUAGE plpgsql AS $$ BEGIN COMMIT; INSERT INTO guarded VALUES(1); RETURN 1; END $$").unwrap();
    sql.execute("BEGIN READ ONLY").unwrap();
    refused(&mut sql, "SELECT escape_readonly()");
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM guarded").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
}
