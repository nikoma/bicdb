use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn row_routine_reads_the_callers_transaction_identity() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE tx_rows(id int PRIMARY KEY);INSERT INTO tx_rows VALUES(1),(2);CREATE FUNCTION row_transaction(n int) RETURNS bigint LANGUAGE plpgsql STABLE AS $$ BEGIN IF n<0 THEN RAISE EXCEPTION 'negative test input';END IF;RETURN txid_current();END $$").unwrap();
    session.execute("BEGIN").unwrap();
    let id = session.execute("SELECT txid_current()").unwrap().rows[0][0].clone();
    let result = session
        .execute("SELECT row_transaction(id) FROM tx_rows ORDER BY id")
        .unwrap();
    assert_eq!(result.rows, vec![vec![id.clone()], vec![id]]);
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn per_row_routines_still_refuse_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE tx_rows(id int PRIMARY KEY);INSERT INTO tx_rows VALUES(1),(2);CREATE FUNCTION row_write(n int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO tx_rows VALUES(n+10);RETURN n;END $$").unwrap();
    session.execute("BEGIN").unwrap();
    assert!(session
        .execute("SELECT row_write(id) FROM tx_rows")
        .is_err());
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT count(*) FROM tx_rows")
            .unwrap()
            .rows[0][0],
        SqlValue::Int(2)
    );
}
