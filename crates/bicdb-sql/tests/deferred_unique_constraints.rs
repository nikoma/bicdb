//! DEFERRABLE UNIQUE constraints: statement-end checks in IMMEDIATE mode,
//! commit-time checks while deferred, SET CONSTRAINTS, savepoints, catalogs.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn modes() -> [StorageMode; 2] {
    [StorageMode::EmbeddedMemory, StorageMode::ServerPaged]
}

fn open(root: &std::path::Path, mode: StorageMode) -> BicDb {
    BicDb::open_with_config(
        root,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap()
}

fn ints(sql: &mut SqlSession<'_>, query: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(query).unwrap().rows
}

fn int_rows(values: &[i64]) -> Vec<Vec<SqlValue>> {
    values
        .iter()
        .map(|value| vec![SqlValue::Int(*value)])
        .collect()
}

#[test]
fn initially_deferred_shift_commits_and_survives_reopen() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        {
            let mut db = open(root.path(), mode.clone());
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE d(a INT UNIQUE DEFERRABLE INITIALLY DEFERRED); INSERT INTO d VALUES (1),(2)").unwrap();
            sql.execute("BEGIN; UPDATE d SET a=a+1; COMMIT").unwrap();
            assert_eq!(
                ints(&mut sql, "SELECT a FROM d ORDER BY a"),
                int_rows(&[2, 3])
            );
        }
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            ints(&mut sql, "SELECT a FROM d ORDER BY a"),
            int_rows(&[2, 3])
        );
        // The constraint still holds after reopen.
        assert_eq!(
            sql.execute("INSERT INTO d VALUES (3)")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
    }
}

#[test]
fn deferrable_immediate_checks_at_statement_end_but_not_deferrable_checks_per_row() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE di(a INT UNIQUE DEFERRABLE); INSERT INTO di VALUES (1),(2); CREATE TABLE nd(a INT UNIQUE); INSERT INTO nd VALUES (1),(2)").unwrap();
        sql.execute("UPDATE di SET a=a+1").unwrap();
        assert_eq!(
            ints(&mut sql, "SELECT a FROM di ORDER BY a"),
            int_rows(&[2, 3])
        );
        assert_eq!(
            sql.execute("INSERT INTO di VALUES (2)")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        assert_eq!(
            sql.execute("UPDATE nd SET a=a+1").unwrap_err().sqlstate(),
            "23505"
        );
    }
}

#[test]
fn duplicate_left_at_commit_fails_with_23505_and_rolls_back() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE dc(id INT PRIMARY KEY, a INT CONSTRAINT dc_a UNIQUE DEFERRABLE INITIALLY DEFERRED); INSERT INTO dc VALUES (1,1)").unwrap();
        sql.execute("BEGIN; INSERT INTO dc VALUES (2,2); INSERT INTO dc VALUES (3,1)")
            .unwrap();
        let error = sql.execute("COMMIT").unwrap_err();
        assert_eq!(error.sqlstate(), "23505", "{error}");
        assert!(error.to_string().contains("dc_a"), "{error}");
        let _ = sql.execute("ROLLBACK");
        assert_eq!(
            ints(&mut sql, "SELECT id FROM dc ORDER BY id"),
            int_rows(&[1])
        );
    }
}

#[test]
fn set_constraints_switches_timing_and_immediate_checks_pending_rows() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE ds(id INT PRIMARY KEY, a INT CONSTRAINT ds_a UNIQUE DEFERRABLE INITIALLY IMMEDIATE); INSERT INTO ds VALUES (1,1)").unwrap();
        // IMMEDIATE by default: a duplicate fails at once.
        sql.execute("BEGIN").unwrap();
        assert_eq!(
            sql.execute("INSERT INTO ds VALUES (2,1)")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        sql.execute("ROLLBACK").unwrap();
        // Deferred, a duplicate resolved before commit is accepted.
        sql.execute("BEGIN; SET CONSTRAINTS ds_a DEFERRED; INSERT INTO ds VALUES (2,1); UPDATE ds SET a=5 WHERE id=2; COMMIT").unwrap();
        assert_eq!(
            ints(&mut sql, "SELECT a FROM ds ORDER BY id"),
            int_rows(&[1, 5])
        );
        // Switching back to IMMEDIATE checks the pending rows now; a savepoint
        // rollback restores both the queue and the timing.
        sql.execute(
            "BEGIN; SET CONSTRAINTS ds_a DEFERRED; INSERT INTO ds VALUES (3,1); SAVEPOINT s",
        )
        .unwrap();
        assert_eq!(
            sql.execute("SET CONSTRAINTS ds_a IMMEDIATE")
                .unwrap_err()
                .sqlstate(),
            "23505"
        );
        sql.execute("ROLLBACK TO SAVEPOINT s; UPDATE ds SET a=6 WHERE id=3; SET CONSTRAINTS ALL IMMEDIATE; COMMIT").unwrap();
        assert_eq!(
            ints(&mut sql, "SELECT a FROM ds ORDER BY id"),
            int_rows(&[1, 5, 6])
        );
        // A savepoint rollback discards a queued duplicate.
        sql.execute("BEGIN; SET CONSTRAINTS ALL DEFERRED; SAVEPOINT s; INSERT INTO ds VALUES (4,1); ROLLBACK TO SAVEPOINT s; INSERT INTO ds VALUES (4,7); COMMIT").unwrap();
        assert_eq!(
            ints(&mut sql, "SELECT a FROM ds ORDER BY id"),
            int_rows(&[1, 5, 6, 7])
        );
    }
}

#[test]
fn deferred_composite_swap_and_nulls() {
    for mode in modes() {
        let root = tempfile::tempdir().unwrap();
        let mut db = open(root.path(), mode);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE dw(id INT PRIMARY KEY, x INT, y INT, UNIQUE (x, y) DEFERRABLE INITIALLY DEFERRED); INSERT INTO dw VALUES (1,1,1),(2,1,2)").unwrap();
        sql.execute("BEGIN; UPDATE dw SET y=3-y; COMMIT").unwrap();
        assert_eq!(
            ints(&mut sql, "SELECT y FROM dw ORDER BY id"),
            int_rows(&[2, 1])
        );
        sql.execute("BEGIN; INSERT INTO dw VALUES (3,NULL,1),(4,NULL,1); COMMIT")
            .unwrap();
        sql.execute("BEGIN; INSERT INTO dw VALUES (5,1,1)").unwrap();
        assert_eq!(sql.execute("COMMIT").unwrap_err().sqlstate(), "23505");
    }
}

#[test]
fn deferrable_primary_key_is_refused_and_on_conflict_rejects_deferrable_arbiters() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE pk1(a INT PRIMARY KEY DEFERRABLE)",
        "CREATE TABLE pk2(a INT, PRIMARY KEY (a) DEFERRABLE INITIALLY DEFERRED)",
    ] {
        assert_eq!(sql.execute(statement).unwrap_err().sqlstate(), "0A000");
    }
    sql.execute("CREATE TABLE oc(a INT UNIQUE DEFERRABLE INITIALLY DEFERRED, b INT UNIQUE)")
        .unwrap();
    for statement in [
        "INSERT INTO oc VALUES (1,1) ON CONFLICT (a) DO NOTHING",
        "INSERT INTO oc VALUES (1,1) ON CONFLICT DO NOTHING",
    ] {
        assert_eq!(sql.execute(statement).unwrap_err().sqlstate(), "55000");
    }
    sql.execute("INSERT INTO oc VALUES (1,1) ON CONFLICT (b) DO NOTHING")
        .unwrap();
    assert_eq!(ints(&mut sql, "SELECT a FROM oc"), int_rows(&[1]));
}

#[test]
fn catalogs_report_deferrable_unique_timing() {
    let root = tempfile::tempdir().unwrap();
    let mut db = open(root.path(), StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE dk(a INT CONSTRAINT dk_a UNIQUE DEFERRABLE INITIALLY DEFERRED, b INT CONSTRAINT dk_b UNIQUE DEFERRABLE, c INT CONSTRAINT dk_c UNIQUE)").unwrap();
    let rows = sql
        .execute("SELECT conname, condeferrable, condeferred, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='dk'::regclass AND contype='u' ORDER BY conname")
        .unwrap()
        .rows;
    let text = |value: &str| SqlValue::String(value.into());
    assert_eq!(
        rows,
        vec![
            vec![
                text("dk_a"),
                SqlValue::Bool(true),
                SqlValue::Bool(true),
                text("UNIQUE (a) DEFERRABLE INITIALLY DEFERRED")
            ],
            vec![
                text("dk_b"),
                SqlValue::Bool(true),
                SqlValue::Bool(false),
                text("UNIQUE (b) DEFERRABLE")
            ],
            vec![
                text("dk_c"),
                SqlValue::Bool(false),
                SqlValue::Bool(false),
                text("UNIQUE (c)")
            ],
        ]
    );
    let rows = sql
        .execute("SELECT c.relname, i.indisunique, i.indimmediate FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='dk'::regclass ORDER BY 1")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![text("dk_a"), SqlValue::Bool(true), SqlValue::Bool(false)],
            vec![text("dk_b"), SqlValue::Bool(true), SqlValue::Bool(false)],
            vec![text("dk_c"), SqlValue::Bool(true), SqlValue::Bool(true)],
        ]
    );
}
