use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn deferred_triggers_follow_savepoint_and_exception_rollback() {
    for exception in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE source_rows (id INT PRIMARY KEY)",
            "CREATE TABLE delivered_rows (id INT PRIMARY KEY)",
            "CREATE FUNCTION deliver_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO delivered_rows VALUES (NEW.id); RETURN NEW; END $$",
            "CREATE CONSTRAINT TRIGGER deliver AFTER INSERT ON source_rows DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION deliver_row()",
            "CREATE FUNCTION failing_block() RETURNS void LANGUAGE plpgsql AS $$ BEGIN INSERT INTO source_rows VALUES (2); RAISE EXCEPTION 'discard source'; EXCEPTION WHEN OTHERS THEN INSERT INTO source_rows VALUES (3); END $$",
        ] {
            session.execute(sql).unwrap();
        }
        session.execute("BEGIN").unwrap();
        session
            .execute("INSERT INTO source_rows VALUES (1)")
            .unwrap();
        if exception {
            session.execute("SELECT failing_block()").unwrap();
        } else {
            session.execute("SAVEPOINT keep_first").unwrap();
            session
                .execute("INSERT INTO source_rows VALUES (2)")
                .unwrap();
            session.execute("ROLLBACK TO keep_first").unwrap();
            // Reusing the same mark must discard the new suffix again.
            session
                .execute("INSERT INTO source_rows VALUES (4)")
                .unwrap();
            session.execute("ROLLBACK TO keep_first").unwrap();
            session
                .execute("INSERT INTO source_rows VALUES (3)")
                .unwrap();
        }
        assert!(session
            .execute("SELECT id FROM delivered_rows")
            .unwrap()
            .rows
            .is_empty());
        session.execute("COMMIT").unwrap();
        let expected = vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]];
        assert_eq!(
            session
                .execute("SELECT id FROM source_rows ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
        assert_eq!(
            session
                .execute("SELECT id FROM delivered_rows ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
    }
}

#[test]
fn drop_table_drops_its_triggers() {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(directory.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE TABLE audited (id int)").unwrap();
    session.execute("CREATE TABLE audit_log (id int)").unwrap();
    session
        .execute("CREATE FUNCTION log_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit_log VALUES (NEW.id); RETURN NEW; END $$")
        .unwrap();
    session
        .execute("CREATE TRIGGER log_insert AFTER INSERT ON audited FOR EACH ROW EXECUTE FUNCTION log_row()")
        .unwrap();
    let count =
        |session: &mut SqlSession, sql: &str| session.execute(sql).unwrap().rows[0][0].clone();
    assert_eq!(
        count(
            &mut session,
            "SELECT count(*) FROM pg_trigger WHERE tgname = 'log_insert'"
        ),
        SqlValue::Int(1)
    );
    // Rolled back with the transaction that dropped the table.
    session.execute("BEGIN").unwrap();
    session.execute("DROP TABLE audited").unwrap();
    session.execute("ROLLBACK").unwrap();
    session.execute("INSERT INTO audited VALUES (1)").unwrap();
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM audit_log"),
        SqlValue::Int(1)
    );
    session.execute("DROP TABLE audited").unwrap();
    assert_eq!(
        count(
            &mut session,
            "SELECT count(*) FROM pg_trigger WHERE tgname = 'log_insert'"
        ),
        SqlValue::Int(0)
    );
    // A new table of the same name does not inherit the old trigger.
    session.execute("CREATE TABLE audited (id int)").unwrap();
    session.execute("INSERT INTO audited VALUES (2)").unwrap();
    assert_eq!(
        count(&mut session, "SELECT count(*) FROM audit_log"),
        SqlValue::Int(1)
    );
}
