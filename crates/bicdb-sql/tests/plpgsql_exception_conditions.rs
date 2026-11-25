//! PL/pgSQL EXCEPTION handling as PostgreSQL does it: every errcodes.txt
//! condition name, category conditions, SQLSTATE literals, OR lists, OTHERS
//! (which never traps query_canceled or assert_failure), the protected
//! block's writes rolled back while outer ones stay, re-RAISE, RAISE USING
//! fields and GET STACKED DIAGNOSTICS.
use bicdb_core::BicDb;
use bicdb_sql::{SqlErrorField, SqlSession, SqlValue};

fn notices(sql: &mut SqlSession<'_>, statement: &str) -> Vec<String> {
    sql.execute(statement)
        .unwrap()
        .notices
        .into_iter()
        .map(|notice| notice.message)
        .collect()
}

#[test]
fn condition_names_categories_and_sqlstates_trap_as_in_postgresql() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE ec (a int CHECK (a > 0), b int NOT NULL DEFAULT 0, u int UNIQUE)")
        .unwrap();
    for (statement, expected) in [
        (
            "DO $$ BEGIN INSERT INTO ec (a) VALUES (-1); EXCEPTION WHEN check_violation THEN RAISE NOTICE 'check %', SQLSTATE; END $$",
            "check 23514",
        ),
        (
            "DO $$ BEGIN INSERT INTO ec (a, b) VALUES (1, NULL); EXCEPTION WHEN integrity_constraint_violation THEN RAISE NOTICE 'class %', SQLSTATE; END $$",
            "class 23502",
        ),
        (
            "DO $$ BEGIN PERFORM 1/0; EXCEPTION WHEN SQLSTATE '22000' THEN RAISE NOTICE 'data %', SQLSTATE; END $$",
            "data 22012",
        ),
        (
            "DO $$ BEGIN PERFORM 1/0; EXCEPTION WHEN unique_violation OR division_by_zero THEN RAISE NOTICE 'or %', SQLSTATE; END $$",
            "or 22012",
        ),
    ] {
        assert_eq!(notices(&mut sql, statement), vec![expected.to_string()], "{statement}");
    }
    // An unmatched condition propagates the original error.
    let error = sql
        .execute("DO $$ BEGIN PERFORM 1/0; EXCEPTION WHEN unique_violation THEN NULL; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22012");
    // Unknown names are PostgreSQL's 42704.
    let error = sql
        .execute("DO $$ BEGIN PERFORM 1; EXCEPTION WHEN no_such_condition THEN NULL; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    assert_eq!(
        error.postgres_message(),
        "unrecognized exception condition \"no_such_condition\""
    );
    // OTHERS never traps an assertion failure.
    let error = sql
        .execute("DO $$ BEGIN ASSERT 1 = 2, 'nope'; EXCEPTION WHEN others THEN RAISE NOTICE 'caught'; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "P0004");
}

#[test]
fn protected_block_writes_roll_back_and_outer_writes_stay() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE rb (a int)").unwrap();
    sql.execute(
        "DO $$ BEGIN INSERT INTO rb VALUES (1); BEGIN INSERT INTO rb VALUES (2); PERFORM 1/0; \
         EXCEPTION WHEN division_by_zero THEN NULL; END; INSERT INTO rb VALUES (3); END $$",
    )
    .unwrap();
    assert_eq!(
        sql.execute("SELECT a FROM rb ORDER BY a").unwrap().rows,
        vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]]
    );
    // Re-RAISE hands the same error to the outer handler.
    assert_eq!(
        notices(
            &mut sql,
            "DO $$ BEGIN BEGIN PERFORM 1/0; EXCEPTION WHEN division_by_zero THEN RAISE; END; \
             EXCEPTION WHEN division_by_zero THEN RAISE NOTICE 'outer %', SQLERRM; END $$"
        ),
        vec!["outer division by zero".to_string()]
    );
}

#[test]
fn raise_using_fields_and_stacked_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    let error = sql
        .execute("DO $$ BEGIN RAISE EXCEPTION 'boom %', 1 USING ERRCODE = 'division_by_zero', DETAIL = 'd1', HINT = 'h1'; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22012");
    assert_eq!(error.postgres_message(), "boom 1");
    assert!(error.fields().contains(&SqlErrorField::Detail("d1".into())));
    assert!(error.fields().contains(&SqlErrorField::Hint("h1".into())));
    let error = sql
        .execute("DO $$ BEGIN RAISE division_by_zero USING DETAIL = 'some detail'; END $$")
        .unwrap_err();
    assert_eq!(
        (error.sqlstate(), error.postgres_message().as_str()),
        ("22012", "division_by_zero")
    );
    assert_eq!(
        notices(
            &mut sql,
            "DO $$ DECLARE m text; d text; h text; s text; BEGIN \
             RAISE EXCEPTION 'msg' USING DETAIL = 'det', HINT = 'hin', ERRCODE = '22012'; \
             EXCEPTION WHEN others THEN GET STACKED DIAGNOSTICS m = MESSAGE_TEXT, \
             d = PG_EXCEPTION_DETAIL, h = PG_EXCEPTION_HINT, s = RETURNED_SQLSTATE; \
             RAISE NOTICE '% / % / % / %', m, d, h, s; END $$"
        ),
        vec!["msg / det / hin / 22012".to_string()]
    );
    sql.execute("CREATE TABLE sd (a int CONSTRAINT sd_pos CHECK (a > 0))")
        .unwrap();
    assert_eq!(
        notices(
            &mut sql,
            "DO $$ DECLARE t text; c text; BEGIN INSERT INTO sd VALUES (0); \
             EXCEPTION WHEN check_violation THEN GET STACKED DIAGNOSTICS t = TABLE_NAME, \
             c = CONSTRAINT_NAME; RAISE NOTICE '% %', t, c; END $$"
        ),
        vec!["sd sd_pos".to_string()]
    );
    let error = sql
        .execute("DO $$ DECLARE m text; BEGIN GET STACKED DIAGNOSTICS m = MESSAGE_TEXT; END $$")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0Z002");
}

#[test]
fn conditions_may_be_queries_and_use_postfix_null_tests() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE cq (a int)").unwrap();
    sql.execute("INSERT INTO cq VALUES (1)").unwrap();
    assert_eq!(
        notices(
            &mut sql,
            "DO $$ DECLARE v int; BEGIN IF count(*) = 0 FROM cq WHERE a = 2 THEN RAISE NOTICE 'none'; END IF; \
             IF v isnull THEN RAISE NOTICE 'null'; END IF; IF 1 notnull THEN RAISE NOTICE 'set'; END IF; END $$"
        ),
        vec!["none".to_string(), "null".to_string(), "set".to_string()]
    );
}
