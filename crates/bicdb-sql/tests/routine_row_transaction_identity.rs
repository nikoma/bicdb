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

#[test]
fn row_routines_can_read_nested_table_functions() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE tx_rows(id int PRIMARY KEY);INSERT INTO tx_rows VALUES(1),(2);CREATE FUNCTION row_values(n int) RETURNS TABLE(v int) LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN QUERY SELECT id FROM tx_rows WHERE id=n;END $$;CREATE FUNCTION row_total(n int) RETURNS int LANGUAGE plpgsql STABLE AS $$ DECLARE answer int;BEGIN SELECT sum(v)::int INTO answer FROM row_values(n);RETURN answer;END $$").unwrap();
    session
        .execute("BEGIN;INSERT INTO tx_rows VALUES(3)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT row_total(id) FROM tx_rows ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(3)]
        ]
    );
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn row_routines_still_refuse_row_locks() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE tx_rows(id int PRIMARY KEY);INSERT INTO tx_rows VALUES(1),(2);CREATE FUNCTION row_lock(n int) RETURNS int LANGUAGE plpgsql AS $$ DECLARE answer int;BEGIN SELECT id INTO answer FROM tx_rows WHERE id=n FOR UPDATE;RETURN answer;END $$").unwrap();
    session.execute("BEGIN").unwrap();
    assert!(session.execute("SELECT row_lock(id) FROM tx_rows").is_err());
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn nested_table_functions_still_refuse_writes() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE tx_rows(id int PRIMARY KEY);INSERT INTO tx_rows VALUES(1),(2);CREATE FUNCTION row_values_write(n int) RETURNS TABLE(v int) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO tx_rows VALUES(n+10);RETURN QUERY SELECT n;END $$;CREATE FUNCTION row_total_write(n int) RETURNS int LANGUAGE plpgsql AS $$ DECLARE answer int;BEGIN SELECT sum(v)::int INTO answer FROM row_values_write(n);RETURN answer;END $$").unwrap();
    session.execute("BEGIN").unwrap();
    assert!(session
        .execute("SELECT row_total_write(id) FROM tx_rows")
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
