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

#[test]
fn errors_carry_postgres_sqlstates() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        rows(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
        let not_archived = s.execute("SELECT * FROM t AS OF SCN 1").unwrap_err();
        assert_eq!(not_archived.sqlstate(), "55000", "{mode}: {not_archived}");
        rows(&mut s, "ALTER TABLE t FLASHBACK ARCHIVE");
        let too_old = s.execute("SELECT * FROM t AS OF SCN 1").unwrap_err();
        assert_eq!(too_old.sqlstate(), "72000", "{mode}: {too_old}");
        assert!(too_old.to_string().contains("UTC"), "{mode}: {too_old}");
        let future = s
            .execute("SELECT * FROM t AS OF SCN 9000000000000000")
            .unwrap_err();
        assert_eq!(future.sqlstate(), "22023", "{mode}: {future}");
    }
}

#[test]
fn describe_types_and_parameters_follow_the_base_table() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        {
            let mut s = SqlSession::new(&mut db);
            seed(&mut s);
        }
        let params =
            bicdb_sql::infer_parameter_types(&db, "SELECT * FROM accounts AS OF SCN $1").unwrap();
        assert_eq!(params, vec![Some("int8".to_string())], "{mode}");
        let params = bicdb_sql::infer_parameter_types(
            &db,
            "SELECT * FROM accounts VERSIONS BETWEEN TIMESTAMP $1 AND $2",
        )
        .unwrap();
        assert_eq!(
            params,
            vec![
                Some("timestamptz".to_string()),
                Some("timestamptz".to_string())
            ],
            "{mode}"
        );
        let typed = |sql: &str| {
            bicdb_sql::infer_query_result_columns(&db, sql)
                .unwrap()
                .unwrap_or_else(|| panic!("{mode}: no inferred columns for {sql}"))
        };
        assert_eq!(
            typed("SELECT * FROM accounts AS OF SCN 1"),
            vec![
                ("id".to_string(), Some("int4".to_string())),
                ("balance".to_string(), Some("int4".to_string()))
            ],
            "{mode}"
        );
        assert_eq!(
            typed(
                "SELECT *, versions_starttime, versions_operation \
                 FROM accounts VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE"
            ),
            vec![
                ("id".to_string(), Some("int4".to_string())),
                ("balance".to_string(), Some("int4".to_string())),
                (
                    "versions_starttime".to_string(),
                    Some("timestamptz".to_string())
                ),
                ("versions_operation".to_string(), Some("text".to_string())),
            ],
            "{mode}"
        );
    }
}

#[test]
fn clause_rewrite_handles_real_world_sql_shapes() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        let [s0, s1, ..] = seed(&mut s);
        // Multi-line SQL with comments, a quoted table name, a subquery, and a
        // BETWEEN bound that itself contains AND inside parentheses.
        let sql = format!(
            "-- what did account 1 look like?\n\
             SELECT a.id,\n       a.balance /* then */\n\
             FROM \"accounts\"\n  AS OF SCN ({s0})\n  a\n\
             WHERE a.id IN (SELECT id FROM accounts AS OF SCN {s0} WHERE balance > 0 AND id < 2)"
        );
        assert_eq!(balances(&mut s, &sql), vec![(1, 100)], "{mode}");
        let between = format!(
            "SELECT id, balance FROM accounts VERSIONS BETWEEN SCN (CASE WHEN 1 = 1 AND 2 = 2 THEN {s0} END) \
             AND {s1} WHERE id = 1 ORDER BY balance"
        );
        assert_eq!(
            balances(&mut s, &between),
            vec![(1, 100), (1, 150)],
            "{mode}"
        );
        let from_to = format!(
            "SELECT id, balance FROM accounts FOR SYSTEM_TIME FROM {s0} TO {s1} WHERE id = 2"
        );
        assert_eq!(balances(&mut s, &from_to), vec![(2, 200)], "{mode}");
        // `v.*` hides the pseudocolumns just like `*`.
        let qualified = s
            .execute("SELECT v.* FROM accounts VERSIONS BETWEEN SCN MINVALUE AND MAXVALUE v")
            .unwrap();
        assert_eq!(
            qualified.columns,
            vec!["id".to_string(), "balance".to_string()],
            "{mode}"
        );
        // A table or column literally named like the keywords still works.
        rows(
            &mut s,
            "CREATE TABLE versions (id INT PRIMARY KEY, scn INT)",
        );
        rows(&mut s, "INSERT INTO versions VALUES (1, 2)");
        assert_eq!(
            balances(&mut s, "SELECT id, scn FROM versions"),
            vec![(1, 2)],
            "{mode}"
        );
    }
}

#[test]
fn keywords_in_ordinary_predicates_are_not_flashback_clauses() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        rows(
            &mut s,
            "CREATE TABLE releases (id INT PRIMARY KEY, versions TIMESTAMP, of INT)",
        );
        rows(
            &mut s,
            "INSERT INTO releases VALUES (1, '2026-01-05 00:00:00', 7), (2, '2027-01-05 00:00:00', 8)",
        );
        let found = rows(
            &mut s,
            "SELECT id FROM releases WHERE versions BETWEEN TIMESTAMP '2026-01-01 00:00:00' \
             AND TIMESTAMP '2026-12-31 00:00:00'",
        );
        assert_eq!(found.len(), 1, "{mode}: {found:?}");
        assert_eq!(int(&found[0][0]), 1, "{mode}");
        let aliased = rows(&mut s, "SELECT of AS of FROM releases ORDER BY of");
        assert_eq!(aliased.len(), 2, "{mode}");
    }
}

#[test]
fn history_feeds_views_and_insert_select() {
    for (mode, config) in modes() {
        let (_dir, mut db) = open(config);
        let mut s = SqlSession::new(&mut db);
        let [s0, ..] = seed(&mut s);
        // INSERT is statement-cached: its literals become $n placeholders, so
        // the SCN reaches the flashback clause as a bound parameter.
        rows(
            &mut s,
            "CREATE TABLE recovered (id INT PRIMARY KEY, balance INT)",
        );
        rows(
            &mut s,
            &format!("INSERT INTO recovered SELECT id, balance FROM accounts AS OF SCN {s0}"),
        );
        assert_eq!(
            balances(&mut s, "SELECT id, balance FROM recovered ORDER BY id"),
            vec![(1, 100), (2, 200)],
            "{mode}"
        );
        rows(
            &mut s,
            &format!(
                "CREATE VIEW opening_balances AS SELECT id, balance FROM accounts AS OF SCN {s0}"
            ),
        );
        assert_eq!(
            balances(
                &mut s,
                "SELECT id, balance FROM opening_balances ORDER BY id"
            ),
            vec![(1, 100), (2, 200)],
            "{mode}"
        );
        // FLASHBACK TABLE inside an explicit transaction joins it.
        rows(&mut s, "BEGIN");
        rows(&mut s, &format!("FLASHBACK TABLE accounts TO SCN {s0}"));
        rows(&mut s, "ROLLBACK");
        assert_eq!(
            balances(&mut s, "SELECT id, balance FROM accounts ORDER BY id"),
            vec![(1, 150), (3, 300)],
            "{mode}: rolled back with the caller's transaction"
        );
    }
}
