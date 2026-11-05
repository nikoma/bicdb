use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn notice_does_not_abort_or_skip_remaining_writes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE notice_receipts(id integer PRIMARY KEY)")
        .unwrap();
    session.execute("DO $$ BEGIN INSERT INTO notice_receipts VALUES (1); RAISE NOTICE 'count=%', 1; INSERT INTO notice_receipts VALUES (2); END $$").unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM notice_receipts ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]]
    );
}

#[test]
fn notice_preserves_bound_values_nulls_percent_and_detail() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute("DO $$ DECLARE n integer := 7; BEGIN RAISE NOTICE '測定=% / %% / %', n, NULL USING DETAIL = 'synthetic detail', ERRCODE = '01000'; END $$").unwrap();
    assert_eq!(result.notices.len(), 1);
    assert_eq!(result.notices[0].message, "測定=7 / % / <NULL>");
    assert_eq!(result.notices[0].sqlstate, "01000");
    let debug = format!("{:?}", result.notices[0]);
    assert!(!debug.contains("測定") && !debug.contains("synthetic detail"));
    assert_eq!(
        result.notices[0].fields,
        vec![bicdb_sql::SqlErrorField::Detail("synthetic detail".into())]
    );
    let mut stream = result.into_stream();
    assert!(stream.empty_result().notices.is_empty());
    assert_eq!(stream.take_notices().len(), 1);
    assert!(stream.take_notices().is_empty());
}

#[test]
fn notice_before_error_keeps_the_real_error_and_does_not_leak_to_next_statement() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let error = session.execute("DO $$ BEGIN RAISE NOTICE 'before'; RAISE EXCEPTION 'blocked' USING ERRCODE = '23514', CONSTRAINT = 'guard'; END $$").unwrap_err();
    assert_eq!(error.sqlstate(), "23514");
    assert!(error
        .fields()
        .contains(&bicdb_sql::SqlErrorField::Constraint("guard".into())));
    assert_eq!(error.notices()[0].message, "before");
    assert_eq!(error.to_string(), "blocked");
    assert!(session.execute("SELECT 1").unwrap().notices.is_empty());
}

#[test]
fn client_min_messages_suppresses_notice_without_skipping_the_statement() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("SET client_min_messages = warning")
        .unwrap();
    assert!(session
        .execute("DO $$ BEGIN RAISE NOTICE 'suppressed'; END $$")
        .unwrap()
        .notices
        .is_empty());
    session.execute("SET client_min_messages = notice").unwrap();
    assert_eq!(
        session
            .execute("DO $$ BEGIN RAISE NOTICE 'visible'; END $$")
            .unwrap()
            .notices
            .len(),
        1
    );
}

#[test]
fn notice_requires_matching_format_arguments() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "DO $$ BEGIN RAISE NOTICE 'missing %'; END $$",
        "DO $$ BEGIN RAISE NOTICE 'extra', 1; END $$",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42601");
    }
}

#[test]
fn notice_buffer_is_bounded_and_cleared_after_failure() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let error = session
        .execute("DO $$ BEGIN FOR n IN 1..1025 LOOP RAISE NOTICE 'item %', n; END LOOP; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "54000");
    assert_eq!(error.notices().len(), 1024);
    assert!(session.execute("SELECT 1").unwrap().notices.is_empty());
}

#[test]
fn handled_exception_keeps_notices_and_notice_preserves_found() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let result = session.execute("DO $$ DECLARE n integer; BEGIN SELECT 7 INTO n; RAISE NOTICE 'found %', n; IF NOT FOUND THEN RAISE EXCEPTION 'NOTICE changed FOUND'; END IF; BEGIN RAISE NOTICE 'before rollback'; RAISE EXCEPTION 'handled'; EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'after rollback'; END; END $$").unwrap();
    assert_eq!(
        result
            .notices
            .iter()
            .map(|notice| notice.message.as_str())
            .collect::<Vec<_>>(),
        vec!["found 7", "before rollback", "after rollback"]
    );
}
