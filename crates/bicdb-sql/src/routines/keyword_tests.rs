//! Clause keywords inside data and longer identifiers remain ordinary text.
use super::*;

#[test]
fn native_routines_accept_clause_words_in_data_and_called_names() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE keyword_values(v text)",
        "CREATE PROCEDURE exceptional_writer(p text) LANGUAGE plpgsql AS $$BEGIN INSERT INTO keyword_values VALUES(p);END$$",
        "CREATE PROCEDURE dispatch_words() LANGUAGE plpgsql AS $$BEGIN CALL exceptional_writer('an exception');CALL exceptional_writer('EXCEPTION');END$$",
        "CALL dispatch_words()",
        "CREATE FUNCTION keyword_handler() RETURNS text LANGUAGE plpgsql AS $$BEGIN PERFORM 1/0;EXCEPTION WHEN division_by_zero THEN RETURN 'handled exception';END$$",
    ] { session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}")); }
    assert_eq!(
        session
            .execute("SELECT v FROM keyword_values ORDER BY v")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("EXCEPTION".into())],
            vec![SqlValue::String("an exception".into())]
        ]
    );
    assert_eq!(
        session.execute("SELECT keyword_handler()").unwrap().rows,
        vec![vec![SqlValue::String("handled exception".into())]]
    );
    assert!(parse_routine_statement("EXCEPTION").is_err());
}
