use super::*;

#[test]
fn primary_key_parents_use_point_reads_in_both_profiles() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id)); INSERT INTO p VALUES(1)").unwrap();
        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let result = sql.execute("INSERT INTO c VALUES(1,1)");
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
        result.unwrap();
        assert_eq!(stats.foreign_key_parent_scans, 0, "{profile}");
        assert_eq!(stats.foreign_key_parent_lookups, 1, "{profile}");
    }
}

#[test]
fn foreign_key_point_reads_preserve_pending_parent_visibility_and_savepoints() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id)); BEGIN; INSERT INTO p VALUES(1); INSERT INTO c VALUES(1,1); SAVEPOINT kept; DELETE FROM c; DELETE FROM p").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO c VALUES(2,1)")
                .unwrap_err()
                .sqlstate(),
            "23503"
        );
        sql.execute("ROLLBACK TO SAVEPOINT kept; INSERT INTO c VALUES(2,1); COMMIT")
            .unwrap();
    }
}

#[test]
fn foreign_key_point_reads_observe_committed_parent_changes_after_transaction_start() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, profile).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE p(id INTEGER PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(id)); INSERT INTO p VALUES(1)").unwrap();
        let mut first = SqlSession::new_shared(&db);
        let mut second = SqlSession::new_shared(&db);
        first.execute("BEGIN; SELECT COUNT(*) FROM p").unwrap();
        second
            .execute("BEGIN; DELETE FROM p WHERE id=1; COMMIT")
            .unwrap();
        assert_eq!(
            first
                .execute("INSERT INTO c VALUES(1,1)")
                .unwrap_err()
                .sqlstate(),
            "23503"
        );
        first
            .execute("ROLLBACK; BEGIN; SELECT COUNT(*) FROM p")
            .unwrap();
        second
            .execute("BEGIN; INSERT INTO p VALUES(2); COMMIT")
            .unwrap();
        first.execute("INSERT INTO c VALUES(2,2); COMMIT").unwrap();
    }
}

#[test]
fn nonprimary_unique_parents_keep_the_authoritative_scan() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE p(id INTEGER PRIMARY KEY,code INTEGER UNIQUE); CREATE TABLE c(id INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(code)); INSERT INTO p VALUES(1,42)").unwrap();
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let result = sql.execute("INSERT INTO c VALUES(1,42)");
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    result.unwrap();
    assert_eq!(stats.foreign_key_parent_scans, 1);
    assert_eq!(stats.foreign_key_parent_lookups, 0);
}

#[test]
fn deferred_parent_checks_use_pending_primary_key_point_reads() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE p(a INTEGER,b INTEGER,PRIMARY KEY(a,b)); CREATE TABLE c(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,FOREIGN KEY(a,b) REFERENCES p(a,b) DEFERRABLE INITIALLY DEFERRED); BEGIN; INSERT INTO c VALUES(1,2,3); INSERT INTO p VALUES(2,3)").unwrap();
        SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
        let result = sql.execute("COMMIT");
        let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
        result.unwrap();
        assert_eq!(stats.foreign_key_parent_scans, 0, "{profile}");
        assert!(stats.foreign_key_parent_lookups >= 1, "{profile}");
    }
}

#[test]
fn primary_key_lookup_keeps_cross_type_equality_and_failure_diagnostics() {
    for profile in [oracle::POSTGRES_PROFILE, oracle::ORACLE_PREVIEW_PROFILE] {
        for (parent_type, child_type, parent, child, valid) in [
            ("BIGINT", "SMALLINT", "1", "1", true),
            ("NUMERIC", "INTEGER", "1.00", "1", true),
            ("INTEGER", "NUMERIC", "1", "1.5", false),
            ("SMALLINT", "BIGINT", "1", "2147483648", false),
            ("CHAR(4)", "VARCHAR(8)", "'a'", "'a   '", true),
            (
                "UUID",
                "UUID",
                "'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'",
                "'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'",
                true,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(root.path()).unwrap();
            oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut sql = SqlSession::new(&mut db);
            sql.execute(&format!("CREATE TABLE p(id {parent_type} PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid {child_type} REFERENCES p(id)); INSERT INTO p VALUES({parent})")).unwrap();
            let result = sql.execute(&format!("INSERT INTO c VALUES(1,{child})"));
            if valid {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().sqlstate(),
                    "23503",
                    "{profile}: {parent_type}/{child_type}"
                );
            }
        }
    }
}

#[test]
fn out_of_range_foreign_keys_keep_deferred_validation_timing() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE p(id SMALLINT PRIMARY KEY); CREATE TABLE c(id INTEGER PRIMARY KEY,pid BIGINT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED); BEGIN; INSERT INTO c VALUES(1,2147483648)").unwrap();
    assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23503");
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT COUNT(*) FROM c").unwrap().rows,
        vec![vec![SqlValue::Int(0)]]
    );
    sql.execute("BEGIN; INSERT INTO c VALUES(2,2147483648); DELETE FROM c WHERE id=2; COMMIT")
        .unwrap();
    sql.execute("CREATE TABLE legacy(id INTEGER PRIMARY KEY,pid BIGINT); INSERT INTO legacy VALUES(1,2147483648); INSERT INTO p VALUES(1)").unwrap();
    assert_eq!(
        sql.execute(
            "ALTER TABLE legacy ADD CONSTRAINT legacy_fk FOREIGN KEY(pid) REFERENCES p(id)"
        )
        .unwrap_err()
        .sqlstate(),
        "23503"
    );
}
