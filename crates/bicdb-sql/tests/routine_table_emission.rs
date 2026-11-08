use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn statement_keyword_prefixes_remain_ordinary_variable_names() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION keyword_variables() RETURNS int LANGUAGE plpgsql AS $$ DECLARE open_h int;fetch_count int;close_total int;raise_total int;execute_total int;perform_total int;BEGIN open_h:=2;fetch_count:=3;close_total:=5;raise_total:=7;execute_total:=11;perform_total:=13;RETURN open_h+fetch_count+close_total+raise_total+execute_total+perform_total;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT keyword_variables()").unwrap().rows[0][0],
        SqlValue::Int(41)
    );
}

#[test]
fn table_return_next_snapshots_outputs_and_continues_the_loop() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION table_rows() RETURNS TABLE(n int,label text) LANGUAGE plpgsql AS $$ DECLARE i int;BEGIN RETURN NEXT;FOR i IN 1..3 LOOP n:=i;label:='row-'||i;RETURN NEXT;END LOOP;END $$").unwrap();
    let result = session.execute("SELECT * FROM table_rows()").unwrap();
    assert_eq!(result.columns, vec!["n", "label"]);
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Null, SqlValue::Null],
            vec![SqlValue::Int(1), SqlValue::String("row-1".into())],
            vec![SqlValue::Int(2), SqlValue::String("row-2".into())],
            vec![SqlValue::Int(3), SqlValue::String("row-3".into())]
        ]
    );
}

#[test]
fn reverse_for_loops_count_down_from_the_first_bound() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION countdown(high int) RETURNS text LANGUAGE plpgsql AS $$ DECLARE acc text := '';j int;BEGIN for j in reverse high .. 1 loop acc := acc || j;END LOOP;FOR j IN REVERSE 1 .. 3 LOOP acc := acc || 'x';END LOOP;RETURN acc;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT countdown(3)").unwrap().rows[0][0],
        SqlValue::String("321".into())
    );
    assert_eq!(
        session.execute("SELECT countdown(0)").unwrap().rows[0][0],
        SqlValue::String("".into())
    );
}

#[test]
fn table_return_next_and_return_query_preserve_emission_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION mixed_rows() RETURNS TABLE(n int) LANGUAGE plpgsql AS $$ BEGIN n:=2;RETURN NEXT;RETURN QUERY SELECT 7;n:=9;RETURN NEXT;RETURN;END $$").unwrap();
    assert_eq!(
        session.execute("SELECT * FROM mixed_rows()").unwrap().rows,
        vec![
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(7)],
            vec![SqlValue::Int(9)]
        ]
    );
}

#[test]
fn scalar_return_next_expressions_remain_explicitly_unsupported() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION unsupported_rows() RETURNS TABLE(n int) LANGUAGE plpgsql AS $$ BEGIN RETURN NEXT 7;END $$").unwrap();
    assert!(session.execute("SELECT * FROM unsupported_rows()").is_err());
}
