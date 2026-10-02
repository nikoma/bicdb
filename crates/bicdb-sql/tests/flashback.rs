//! Flashback (time travel) through SQL: Oracle `AS OF` / `VERSIONS BETWEEN`,
//! SQL:2011 `FOR SYSTEM_TIME`, `FLASHBACK TABLE`, and their authorization.
//! Every test runs against both storage engines.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn modes() -> Vec<(&'static str, DbConfig)> {
    vec![
        ("embedded", DbConfig::default().with_fsync(false)),
        (
            "paged",
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        ),
    ]
}

fn open(config: DbConfig) -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(directory.path(), config).unwrap();
    (directory, db)
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

fn int(value: &SqlValue) -> i64 {
    match value {
        SqlValue::Int(value) => *value,
        other => panic!("expected int, got {other:?}"),
    }
}

fn text(value: &SqlValue) -> String {
    match value {
        SqlValue::String(value) => value.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn scn(session: &mut SqlSession<'_>) -> i64 {
    int(&rows(session, "SELECT current_scn()")[0][0])
}

fn balances(session: &mut SqlSession<'_>, sql: &str) -> Vec<(i64, i64)> {
    rows(session, sql)
        .iter()
        .map(|row| (int(&row[0]), int(&row[1])))
        .collect()
}

/// accounts: (1,100) (2,200) at s0; (1,150) at s1; (2 deleted) at s2;
/// (3,300) at s3.
fn seed(session: &mut SqlSession<'_>) -> [i64; 4] {
    rows(
        session,
        "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)",
    );
    rows(session, "INSERT INTO accounts VALUES (1, 100), (2, 200)");
    rows(
        session,
        "ALTER TABLE accounts FLASHBACK ARCHIVE RETENTION 30 DAYS",
    );
    let s0 = scn(session);
    rows(session, "UPDATE accounts SET balance = 150 WHERE id = 1");
    let s1 = scn(session);
    rows(session, "DELETE FROM accounts WHERE id = 2");
    let s2 = scn(session);
    rows(session, "INSERT INTO accounts VALUES (3, 300)");
    let s3 = scn(session);
    [s0, s1, s2, s3]
}

#[test]
fn as_of_queries_return_past_states_through_every_path() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        let [s0, s1, s2, s3] = seed(&mut s);

        let q = |at: i64| format!("SELECT id, balance FROM accounts AS OF SCN {at} ORDER BY id");
        assert_eq!(balances(&mut s, &q(s0)), vec![(1, 100), (2, 200)], "{mode}");
        assert_eq!(balances(&mut s, &q(s1)), vec![(1, 150), (2, 200)], "{mode}");
        assert_eq!(balances(&mut s, &q(s2)), vec![(1, 150)], "{mode}");
        assert_eq!(balances(&mut s, &q(s3)), vec![(1, 150), (3, 300)], "{mode}");

        // Shapes that have current-data fast paths must still read history.
        assert_eq!(
            balances(
                &mut s,
                &format!("SELECT id, balance FROM accounts AS OF SCN {s0} WHERE id = 2")
            ),
            vec![(2, 200)],
            "{mode}: primary-key lookup"
        );
        assert_eq!(
            int(&rows(
                &mut s,
                &format!("SELECT count(*) FROM accounts AS OF SCN {s0}")
            )[0][0]),
            2,
            "{mode}: count(*)"
        );
        assert_eq!(
            int(&rows(
                &mut s,
                &format!("SELECT max(balance) FROM accounts AS OF SCN {s0}")
            )[0][0]),
            200,
            "{mode}: aggregate"
        );
        // Alias, qualified columns, and a join against current data.
        assert_eq!(
            balances(
                &mut s,
                &format!(
                    "SELECT old.id, cur.balance - old.balance FROM accounts AS OF SCN {s0} old \
                     JOIN accounts cur ON cur.id = old.id ORDER BY old.id"
                )
            ),
            vec![(1, 50)],
            "{mode}: join history with current"
        );
        // SQL:2011 / SQL Server syntax with an SCN-valued bound.
        assert_eq!(
            balances(
                &mut s,
                &format!("SELECT id, balance FROM accounts FOR SYSTEM_TIME AS OF {s1} ORDER BY id")
            ),
            vec![(1, 150), (2, 200)],
            "{mode}"
        );
        // AS OF TIMESTAMP via scn_to_timestamp, and the round trip back.
        let at = text(&rows(&mut s, &format!("SELECT scn_to_timestamp({s0})"))[0][0]);
        assert_eq!(
            balances(
                &mut s,
                &format!("SELECT id, balance FROM accounts AS OF TIMESTAMP '{at}' ORDER BY id")
            ),
            vec![(1, 100), (2, 200)],
            "{mode}: AS OF TIMESTAMP"
        );
        let back = int(&rows(&mut s, &format!("SELECT timestamp_to_scn('{at}')"))[0][0]);
        assert_eq!(back, s0, "{mode}: timestamp_to_scn(scn_to_timestamp(s0))");
        // A literal that merely contains the words is untouched.
        assert_eq!(
            text(&rows(&mut s, "SELECT 'x AS OF SCN 5'")[0][0]),
            "x AS OF SCN 5",
            "{mode}"
        );
        // Current reads are unaffected.
        assert_eq!(
            balances(&mut s, "SELECT id, balance FROM accounts ORDER BY id"),
            vec![(1, 150), (3, 300)],
            "{mode}"
        );
    }
}

#[test]
fn versions_between_exposes_oracle_pseudocolumns() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        let [_s0, _s1, _s2, s3] = seed(&mut s);

        let result = s
            .execute(
                "SELECT id, balance, versions_operation, versions_startscn, versions_endscn \
                 FROM accounts VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE \
                 ORDER BY id, versions_startscn NULLS FIRST",
            )
            .unwrap();
        let summary = result
            .rows
            .iter()
            .map(|row| {
                (
                    int(&row[0]),
                    int(&row[1]),
                    match &row[2] {
                        SqlValue::Null => "-".to_string(),
                        value => text(value),
                    },
                    !matches!(row[3], SqlValue::Null),
                    !matches!(row[4], SqlValue::Null),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            vec![
                (1, 100, "-".to_string(), false, true),
                (1, 150, "U".to_string(), true, false),
                (2, 200, "-".to_string(), false, true),
                (2, 200, "D".to_string(), true, false),
                (3, 300, "I".to_string(), true, false),
            ],
            "{mode}"
        );
        // `*` does not expand to the pseudocolumns, as in Oracle.
        let star = s
            .execute(&format!(
                "SELECT * FROM accounts VERSIONS BETWEEN SCN MINVALUE AND {s3}"
            ))
            .unwrap();
        assert_eq!(
            star.columns,
            vec!["id".to_string(), "balance".to_string()],
            "{mode}"
        );
        // SQL:2011 FOR SYSTEM_TIME ALL is the same version set.
        let all = rows(&mut s, "SELECT count(*) FROM accounts FOR SYSTEM_TIME ALL");
        assert_eq!(int(&all[0][0]), 5, "{mode}");
    }
}

#[test]
fn history_reads_are_authorized_like_current_reads() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let s0 = {
            let mut owner = SqlSession::new(&mut db);
            let [s0, ..] = seed(&mut owner);
            rows(&mut owner, "CREATE ROLE mallory LOGIN NOSUPERUSER");
            rows(&mut owner, "CREATE ROLE alice LOGIN NOSUPERUSER");
            rows(
                &mut owner,
                "CREATE TABLE notes (id INT PRIMARY KEY, owner TEXT, body TEXT)",
            );
            rows(
                &mut owner,
                "INSERT INTO notes VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1')",
            );
            rows(&mut owner, "ALTER TABLE notes ENABLE ROW LEVEL SECURITY");
            rows(
                &mut owner,
                "CREATE POLICY own ON notes FOR SELECT USING (owner = current_user)",
            );
            rows(&mut owner, "GRANT SELECT ON notes TO alice");
            rows(&mut owner, "ALTER TABLE notes FLASHBACK ARCHIVE");
            let s0 = scn(&mut owner);
            rows(&mut owner, "UPDATE notes SET body = 'b2' WHERE id = 2");
            s0
        };
        let notes_at = scn(&mut SqlSession::new(&mut db));

        let mut mallory = SqlSession::new_unprivileged(&mut db, "mallory");
        assert!(
            mallory
                .execute(&format!("SELECT * FROM accounts AS OF SCN {s0}"))
                .is_err(),
            "{mode}: AS OF requires SELECT on the table"
        );
        assert!(
            mallory
                .execute("SELECT * FROM accounts VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE")
                .is_err(),
            "{mode}: VERSIONS requires SELECT on the table"
        );
        assert!(
            mallory
                .execute("ALTER TABLE accounts NO FLASHBACK ARCHIVE")
                .is_err(),
            "{mode}: only the owner may change flashback archiving"
        );
        assert!(
            mallory
                .execute(&format!("FLASHBACK TABLE accounts TO SCN {s0}"))
                .is_err(),
            "{mode}: FLASHBACK TABLE requires write privileges"
        );

        let mut alice = SqlSession::new_unprivileged(&mut db, "alice");
        let visible = rows(
            &mut alice,
            &format!("SELECT id FROM notes AS OF SCN {notes_at} ORDER BY id"),
        );
        assert_eq!(
            visible.len(),
            1,
            "{mode}: RLS applies to history: {visible:?}"
        );
        assert_eq!(int(&visible[0][0]), 1, "{mode}");
        let versions = rows(
            &mut alice,
            "SELECT id FROM notes VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE",
        );
        assert!(
            versions.iter().all(|row| int(&row[0]) == 1),
            "{mode}: RLS applies to versions: {versions:?}"
        );
    }
}

#[test]
fn flashback_table_restores_a_past_state_and_can_be_undone() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        let [s0, ..] = seed(&mut s);
        let before_restore = scn(&mut s);

        rows(&mut s, &format!("FLASHBACK TABLE accounts TO SCN {s0}"));
        assert_eq!(
            balances(&mut s, "SELECT id, balance FROM accounts ORDER BY id"),
            vec![(1, 100), (2, 200)],
            "{mode}"
        );
        // The restore is itself history: flash back over it.
        rows(
            &mut s,
            &format!("FLASHBACK TABLE accounts TO SCN {before_restore}"),
        );
        assert_eq!(
            balances(&mut s, "SELECT id, balance FROM accounts ORDER BY id"),
            vec![(1, 150), (3, 300)],
            "{mode}"
        );
        // Unarchived tables cannot be flashed back.
        rows(&mut s, "CREATE TABLE plain (id INT PRIMARY KEY)");
        assert!(
            s.execute(&format!("FLASHBACK TABLE plain TO SCN {s0}"))
                .is_err(),
            "{mode}"
        );
        // Tables in foreign-key relationships are refused, not half-restored.
        rows(
            &mut s,
            "CREATE TABLE transfers (id INT PRIMARY KEY, account INT REFERENCES accounts(id))",
        );
        assert!(
            s.execute(&format!("FLASHBACK TABLE accounts TO SCN {s0}"))
                .is_err(),
            "{mode}"
        );
    }
}

#[test]
fn archive_ddl_and_schema_qualified_tables() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        rows(&mut s, "CREATE SCHEMA ledger");
        rows(
            &mut s,
            "CREATE TABLE ledger.entries (id INT PRIMARY KEY, amount INT)",
        );
        rows(&mut s, "INSERT INTO ledger.entries VALUES (1, 10)");
        assert!(
            s.execute("SELECT * FROM ledger.entries AS OF SCN 1")
                .is_err(),
            "{mode}: not archived yet"
        );
        rows(&mut s, "ALTER TABLE ledger.entries FLASHBACK ARCHIVE");
        let at = scn(&mut s);
        rows(&mut s, "UPDATE ledger.entries SET amount = 20 WHERE id = 1");
        assert_eq!(
            balances(
                &mut s,
                &format!("SELECT id, amount FROM ledger.entries AS OF SCN {at}")
            ),
            vec![(1, 10)],
            "{mode}"
        );
        assert!(
            s.execute(&format!(
                "SELECT * FROM ledger.entries AS OF SCN {}",
                at - 60_000_000
            ))
            .is_err(),
            "{mode}: before the archive began"
        );
        rows(&mut s, "ALTER TABLE ledger.entries NO FLASHBACK ARCHIVE");
        assert!(
            s.execute(&format!("SELECT * FROM ledger.entries AS OF SCN {at}"))
                .is_err(),
            "{mode}: history discarded"
        );
        assert!(
            s.execute("ALTER TABLE ledger.entries FLASHBACK ARCHIVE my_archive")
                .is_err(),
            "{mode}: named archives are rejected, not ignored"
        );
    }
}
