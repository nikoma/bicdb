use bicdb_core::{BicDb, SecurityContext};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn trusted_callers_execute_guard_bodies_regardless_of_function_name() {
    for name in [
        "enforce_operation_context",
        "finalize_operation_context_after_insert",
        "validate_row",
    ] {
        for timing in ["BEFORE", "AFTER"] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            {
                let mut setup = SqlSession::new(&mut db);
                setup.execute("CREATE SCHEMA application_security; CREATE TABLE guarded_rows (id TEXT PRIMARY KEY); CREATE TABLE observed_rows (id TEXT PRIMARY KEY)").unwrap();
                setup.execute(&format!("CREATE FUNCTION application_security.{name}() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN INSERT INTO observed_rows VALUES (NEW.id); IF NEW.id <> 'allowed' THEN RAISE EXCEPTION 'row rejected by authored guard' USING ERRCODE = '42501'; END IF; RETURN NEW; END $$")).unwrap();
                setup.execute(&format!("CREATE TRIGGER row_guard {timing} INSERT ON guarded_rows FOR EACH ROW EXECUTE FUNCTION application_security.{name}()")).unwrap();
            }
            let mut session =
                SqlSession::new_secure(&mut db, SecurityContext::new("user-a", "tenant-a"));
            let error = session
                .execute("INSERT INTO guarded_rows VALUES ('denied')")
                .unwrap_err();
            assert_eq!(error.sqlstate(), "42501", "{name} {timing}");
            assert!(error.to_string().contains("row rejected by authored guard"));
            session
                .execute("INSERT INTO guarded_rows VALUES ('allowed')")
                .unwrap();
            for table in ["guarded_rows", "observed_rows"] {
                assert_eq!(
                    session
                        .execute(&format!("SELECT id FROM {table}"))
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::String("allowed".into())]],
                    "{name} {timing} {table}",
                );
            }
        }
    }
}

#[test]
fn trigger_rejections_rollback_update_delete_copy_and_writing_ctes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE guarded (id INT PRIMARY KEY, value INT); INSERT INTO guarded VALUES (1, 10)").unwrap();
    session.execute("CREATE FUNCTION deny_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected change' USING ERRCODE = '42501'; END $$").unwrap();
    session.execute("CREATE TRIGGER change_guard AFTER UPDATE OR DELETE ON guarded FOR EACH ROW EXECUTE FUNCTION deny_change()").unwrap();
    for sql in ["UPDATE guarded SET value = 20", "DELETE FROM guarded"] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42501");
        assert_eq!(
            session.execute("SELECT value FROM guarded").unwrap().rows,
            vec![vec![SqlValue::Int(10)]]
        );
    }
    session.execute("CREATE FUNCTION filter_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = 3 THEN RAISE EXCEPTION 'rejected insert' USING ERRCODE = '42501'; END IF; RETURN NEW; END $$").unwrap();
    session.execute("CREATE TRIGGER insert_guard AFTER INSERT ON guarded FOR EACH ROW EXECUTE FUNCTION filter_insert()").unwrap();
    assert_eq!(
        session
            .copy_insert_rows(
                "guarded",
                &["id".into(), "value".into()],
                vec![
                    vec![Some("2".into()), Some("20".into())],
                    vec![Some("3".into()), Some("30".into())]
                ]
            )
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    let cte_error = session.execute("WITH added AS (INSERT INTO guarded VALUES (2, 20) RETURNING id) INSERT INTO guarded SELECT 3, 30 FROM added").unwrap_err();
    assert_eq!(cte_error.sqlstate(), "42501", "{cte_error}");
    session
        .execute("CREATE TABLE earlier_effect (id INT PRIMARY KEY)")
        .unwrap();
    let read_cte_error = session.execute("WITH earlier AS (INSERT INTO earlier_effect VALUES (1) RETURNING id), rejected AS (INSERT INTO guarded SELECT 3, 30 FROM earlier RETURNING id) SELECT id FROM rejected").unwrap_err();
    assert_eq!(read_cte_error.sqlstate(), "42501", "{read_cte_error}");
    assert!(session
        .execute("SELECT id FROM earlier_effect")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        session.execute("SELECT id FROM guarded").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn trigger_cannot_commit_around_its_guard_and_failed_statements_leave_no_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE guarded (id INT PRIMARY KEY)")
        .unwrap();
    session.execute("CREATE FUNCTION guard_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('app.guard_seen', 'yes', true); IF NEW.id = 2 THEN RAISE EXCEPTION 'guard rejection'; END IF; IF NEW.id = 3 THEN COMMIT; END IF; RETURN NEW; END $$").unwrap();
    session.execute("CREATE TRIGGER row_guard AFTER INSERT ON guarded FOR EACH ROW EXECUTE FUNCTION guard_row()").unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO guarded VALUES (1)").unwrap();
    session
        .execute("SELECT set_config('app.guard_seen', 'before', true)")
        .unwrap();
    assert!(session.execute("INSERT INTO guarded VALUES (2)").is_err());
    assert_eq!(
        session
            .execute("SELECT current_setting('app.guard_seen')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("before".into())]]
    );
    assert_eq!(
        session
            .execute("INSERT INTO guarded VALUES (3)")
            .unwrap_err()
            .sqlstate(),
        "2D000"
    );
    session.execute("COMMIT").unwrap();
    assert_eq!(
        session.execute("SELECT id FROM guarded").unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );
}
