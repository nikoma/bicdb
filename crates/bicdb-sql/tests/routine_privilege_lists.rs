use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn fixture() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db).execute("CREATE ROLE list_reader LOGIN; CREATE FUNCTION list_first(a INT,b TEXT) RETURNS INT LANGUAGE SQL AS $$ SELECT 11 $$; CREATE FUNCTION list_second() RETURNS INT LANGUAGE SQL AS $$ SELECT 22 $$; REVOKE ALL ON FUNCTION list_first(INT,TEXT) FROM PUBLIC; REVOKE ALL ON FUNCTION list_second() FROM PUBLIC").unwrap();
    (root, db)
}

#[test]
fn multi_function_grants_and_revokes_cover_every_target_in_a_batch() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "SELECT 1; GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),list_second() TO list_reader",
    )
    .unwrap();
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a'),list_second()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(22)]]
    );
    sql.execute("RESET SESSION AUTHORIZATION; REVOKE ALL ON FUNCTION list_first(INT,TEXT),list_second() FROM list_reader; SET SESSION AUTHORIZATION list_reader").unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a')")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    assert_eq!(
        sql.execute("SELECT list_second()").unwrap_err().sqlstate(),
        "42501"
    );
}

#[test]
fn missing_later_function_does_not_grant_an_earlier_function() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute(
            "GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),missing_function() TO list_reader"
        )
        .unwrap_err()
        .sqlstate(),
        "42883"
    );
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a')")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}

#[test]
fn multi_function_grants_cannot_bypass_ownership() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),list_second() TO CURRENT_USER")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    assert_eq!(
        sql.execute("SELECT list_second()").unwrap_err().sqlstate(),
        "42501"
    );
}

#[test]
fn a_function_grant_at_the_start_of_a_batch_does_not_swallow_later_statements() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(sql.execute("GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),list_second() TO list_reader; SELECT 42 AS answer").unwrap().rows,
        vec![vec![SqlValue::Int(42)]]);
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a'),list_second()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(22)]]
    );
}

#[test]
fn anonymous_block_function_list_grants_apply_every_target() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("DO $$ BEGIN IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='list_reader') THEN GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),list_second() TO list_reader; END IF; END $$").unwrap();
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a'),list_second()")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(11), SqlValue::Int(22)]]
    );
}

#[test]
fn anonymous_block_invalid_later_target_cannot_partially_grant() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(sql.execute("DO $$ BEGIN GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),missing_function() TO list_reader; END $$").unwrap_err().sqlstate(),"42883");
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT list_first(1,'a')")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
}

#[test]
fn anonymous_block_cannot_self_grant_someone_elses_functions() {
    let (_root, mut db) = fixture();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("SET SESSION AUTHORIZATION list_reader")
        .unwrap();
    assert_eq!(sql.execute("DO $$ BEGIN GRANT EXECUTE ON FUNCTION list_first(INT,TEXT),list_second() TO CURRENT_USER; END $$").unwrap_err().sqlstate(),"42501");
    assert_eq!(
        sql.execute("SELECT list_second()").unwrap_err().sqlstate(),
        "42501"
    );
}
