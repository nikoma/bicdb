//! Row-level security on the write path, pinned to PostgreSQL 16 behavior:
//! `INSERT ... ON CONFLICT` under row policies, `RETURNING` under SELECT
//! policies, and the order of the policy check relative to unique keys.
//! Each expectation was observed on a stock PostgreSQL 16 server; both
//! storage modes are covered.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

const MODES: [StorageMode; 2] = [StorageMode::EmbeddedMemory, StorageMode::ServerPaged];

fn open(mode: StorageMode) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    (dir, db)
}

fn exec(sql: &mut SqlSession<'_>, statement: &str) {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
}

/// Rows of a successful statement, as sorted debug strings.
fn rows(sql: &mut SqlSession<'_>, statement: &str) -> Vec<String> {
    let result = sql
        .execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| value.to_cell())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn affected(sql: &mut SqlSession<'_>, statement: &str) -> usize {
    let result = sql
        .execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
    let tag = result.command_tag.unwrap_or_default();
    tag.rsplit(' ')
        .next()
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("`{statement}`: no row count in tag `{tag}`"))
}

/// The SQLSTATE and message of a statement that must fail.
fn error(sql: &mut SqlSession<'_>, statement: &str) -> (String, String) {
    match sql.execute(statement) {
        Ok(result) => panic!("`{statement}` succeeded: {:?}", result.rows),
        Err(error) => (error.sqlstate().to_string(), error.to_string()),
    }
}

fn assert_denied(sql: &mut SqlSession<'_>, statement: &str, message: &str) {
    let (state, text) = error(sql, statement);
    assert_eq!(state, "42501", "`{statement}`: {text}");
    assert!(text.contains(message), "`{statement}`: {text}");
}

const NEW_ROW: &str = "new row violates row-level security policy for table \"t\"";
const USING: &str = "new row violates row-level security policy (USING expression) for table \"t\"";

/// Tenant 1 (`app.org = 1`) as `alice`. SELECT shows only the session's
/// tenant; UPDATE, INSERT and DELETE policies are wide open, so only the
/// SELECT policy stands between alice and tenant 2's rows.
fn tenant_fixture(sql: &mut SqlSession<'_>) {
    for statement in [
        "CREATE ROLE alice LOGIN",
        "CREATE TABLE t (id INT PRIMARY KEY, org_id INT, number INT, label TEXT, owner_name TEXT, code INT UNIQUE)",
        "INSERT INTO t VALUES (1,1,1,'a','alice',101),(2,1,2,'b','alice',102),(15,2,12,'x','bob',115),(16,2,3,'y','bob',116)",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY ps ON t FOR SELECT USING (org_id = (SELECT current_setting('app.org')::int))",
        "CREATE POLICY pu ON t FOR UPDATE USING (true) WITH CHECK (true)",
        "CREATE POLICY pi ON t FOR INSERT WITH CHECK (true)",
        "CREATE POLICY pd ON t FOR DELETE USING (true)",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON t TO alice",
    ] {
        exec(sql, statement);
    }
}

fn as_tenant_one(sql: &mut SqlSession<'_>) {
    exec(sql, "SET app.org = '1'");
    exec(sql, "SET ROLE alice");
}

fn table_state(sql: &mut SqlSession<'_>) -> Vec<String> {
    exec(sql, "RESET ROLE");
    let state = rows(sql, "SELECT id, org_id, number, label FROM t");
    exec(sql, "SET ROLE alice");
    state
}

#[test]
fn on_conflict_do_update_never_modifies_or_returns_a_hidden_row() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        tenant_fixture(&mut sql);
        as_tenant_one(&mut sql);
        let before = table_state(&mut sql);
        // Row 15 belongs to tenant 2: the existing row fails the SELECT
        // policy, which PostgreSQL enforces as an error on the conflict path.
        for statement in [
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label='conf' RETURNING id, label",
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label='conf'",
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label=excluded.label",
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label=t.label || 'z'",
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT ON CONSTRAINT t_pkey DO UPDATE SET label='conf'",
            // The conflict found through a secondary unique key.
            "INSERT INTO t VALUES (60,1,1,'ins','alice',115) ON CONFLICT (code) DO UPDATE SET label='conf'",
        ] {
            assert_denied(&mut sql, statement, USING);
            assert_eq!(table_state(&mut sql), before, "{mode:?} `{statement}` changed rows");
        }
        // A multi-row statement stops at the hidden conflict.
        exec(&mut sql, "BEGIN");
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (2,1,2,'b2','alice',102),(15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label='conf'",
            USING,
        );
        exec(&mut sql, "ROLLBACK");
        assert_eq!(table_state(&mut sql), before, "{mode:?}");
        // The visible row may be upserted, but not moved out of sight.
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (1,1,1,'ins','alice',901) ON CONFLICT (id) DO UPDATE SET org_id = 2",
            NEW_ROW,
        );
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (1,2,1,'ins','alice',901) ON CONFLICT (id) DO UPDATE SET org_id = excluded.org_id RETURNING id",
            NEW_ROW,
        );
        assert_eq!(table_state(&mut sql), before, "{mode:?}");
        assert_eq!(
            rows(
                &mut sql,
                "INSERT INTO t VALUES (1,1,1,'ins','alice',901) ON CONFLICT (id) DO UPDATE SET label='ok' RETURNING id, label"
            ),
            vec!["1|ok"],
            "{mode:?}"
        );
        // The DO UPDATE WHERE is evaluated before the conflict check.
        assert_eq!(
            affected(
                &mut sql,
                "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO UPDATE SET label='c' WHERE false"
            ),
            0,
            "{mode:?}"
        );
    }
}

#[test]
fn on_conflict_do_update_applies_update_using_and_names_restrictive_policies() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        for statement in [
            "CREATE ROLE alice LOGIN",
            "CREATE TABLE t (id INT PRIMARY KEY, org_id INT, number INT)",
            "INSERT INTO t VALUES (1,1,50),(2,1,1),(5,2,1)",
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY ps ON t FOR SELECT USING (true)",
            "CREATE POLICY pi ON t FOR INSERT WITH CHECK (true)",
            "CREATE POLICY pu ON t FOR UPDATE USING (number < 10)",
            "CREATE POLICY r ON t AS RESTRICTIVE FOR UPDATE USING (org_id = 1)",
            "GRANT SELECT, INSERT, UPDATE ON t TO alice",
            "SET ROLE alice",
        ] {
            exec(&mut sql, statement);
        }
        // Visible, but the permissive UPDATE USING rejects it.
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (1,1,1) ON CONFLICT (id) DO UPDATE SET number = 2",
            USING,
        );
        // A restrictive UPDATE policy is reported by name.
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (5,1,1) ON CONFLICT (id) DO UPDATE SET number = 2",
            "new row violates row-level security policy \"r\" (USING expression) for table \"t\"",
        );
        assert_eq!(
            rows(&mut sql, "INSERT INTO t VALUES (2,1,1) ON CONFLICT (id) DO UPDATE SET number = 3 RETURNING id, number"),
            vec!["2|3"],
            "{mode:?}"
        );
        // The updated row must pass the UPDATE check (here the restrictive
        // policy, through USING since there is no WITH CHECK).
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (2,1,1) ON CONFLICT (id) DO UPDATE SET org_id = 2",
            "new row violates row-level security policy \"r\" for table \"t\"",
        );
    }
}

#[test]
fn on_conflict_do_nothing_checks_every_proposed_row_first() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        tenant_fixture(&mut sql);
        as_tenant_one(&mut sql);
        let before = table_state(&mut sql);
        // A visible proposed row conflicting with a hidden one is a no-op
        // (PostgreSQL's behavior), with or without RETURNING.
        assert_eq!(
            affected(
                &mut sql,
                "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO NOTHING"
            ),
            0
        );
        assert!(rows(
            &mut sql,
            "INSERT INTO t VALUES (15,1,12,'ins','alice',915) ON CONFLICT (id) DO NOTHING RETURNING id"
        )
        .is_empty());
        // With an arbiter the SELECT policy joins the INSERT checks, before
        // arbitration: a row alice could not see is refused even when it
        // conflicts.
        for statement in [
            "INSERT INTO t VALUES (15,2,12,'ins','alice',915) ON CONFLICT (id) DO NOTHING",
            "INSERT INTO t VALUES (15,2,12,'ins','alice',915) ON CONFLICT (id) DO NOTHING RETURNING id",
            "INSERT INTO t VALUES (70,2,12,'ins','alice',970) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
            "INSERT INTO t VALUES (3,1,1,'ok','alice',103),(71,2,12,'ins','alice',971) ON CONFLICT (id) DO NOTHING",
        ] {
            assert_denied(&mut sql, statement, NEW_ROW);
            assert_eq!(table_state(&mut sql), before, "{mode:?} `{statement}` changed rows");
        }
        // Without an arbiter or RETURNING nothing is read: no SELECT check.
        assert_eq!(
            affected(
                &mut sql,
                "INSERT INTO t VALUES (15,2,12,'ins','alice',915) ON CONFLICT DO NOTHING"
            ),
            0
        );
        assert_eq!(table_state(&mut sql), before, "{mode:?}");
    }
}

#[test]
fn insert_policy_failure_wins_over_the_unique_key_it_would_reveal() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        tenant_fixture(&mut sql);
        exec(&mut sql, "DROP POLICY pi ON t");
        exec(
            &mut sql,
            "CREATE POLICY pi ON t FOR INSERT WITH CHECK (org_id = (SELECT current_setting('app.org')::int))",
        );
        exec(&mut sql, "DROP POLICY pu ON t");
        exec(
            &mut sql,
            "CREATE POLICY pu ON t FOR UPDATE USING (true) WITH CHECK (org_id = (SELECT current_setting('app.org')::int))",
        );
        as_tenant_one(&mut sql);
        // Key 15 exists for tenant 2; the row is refused by policy (42501),
        // so its existence is not confirmed by a duplicate-key error.
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (15,2,1,'ins','alice',915)",
            NEW_ROW,
        );
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (60,2,1,'ins','alice',115)",
            NEW_ROW,
        );
        // A row that passes the policy still meets the key (PostgreSQL too).
        assert_eq!(
            error(&mut sql, "INSERT INTO t VALUES (15,1,1,'ins','alice',915)").0,
            "23505"
        );
        // Row-at-a-time order: an earlier row's duplicate key is reported
        // before a later row's policy failure, and vice versa.
        assert_eq!(
            error(
                &mut sql,
                "INSERT INTO t VALUES (1,1,1,'d','alice',901),(40,2,1,'p','alice',940)"
            )
            .0,
            "23505"
        );
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (40,2,1,'p','alice',940),(1,1,1,'d','alice',901)",
            NEW_ROW,
        );
        // UPDATE: the new row's policy check precedes its unique keys.
        assert_denied(
            &mut sql,
            "UPDATE t SET code = 115, org_id = 2 WHERE id = 1",
            NEW_ROW,
        );
        assert_eq!(
            error(&mut sql, "UPDATE t SET code = 115 WHERE id = 1").0,
            "23505"
        );
    }
}

#[test]
fn returning_never_exposes_a_row_the_select_policy_hides() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        tenant_fixture(&mut sql);
        as_tenant_one(&mut sql);
        let before = table_state(&mut sql);
        // INSERT ... RETURNING checks the new row against SELECT.
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (30,2,12,'ins','alice',930) RETURNING id",
            NEW_ROW,
        );
        assert_denied(
            &mut sql,
            "INSERT INTO t VALUES (31,2,12,'ins','alice',931) RETURNING id + 0",
            NEW_ROW,
        );
        assert_denied(
            &mut sql,
            "INSERT INTO t SELECT 32,2,1,'s','alice',932 RETURNING *",
            NEW_ROW,
        );
        assert_eq!(table_state(&mut sql), before, "{mode:?}");
        // RETURNING that reads no column, or none at all, is not a read.
        assert_eq!(
            rows(
                &mut sql,
                "INSERT INTO t VALUES (33,2,12,'ins','alice',933) RETURNING 1"
            ),
            vec!["1"]
        );
        assert_eq!(
            affected(&mut sql, "INSERT INTO t VALUES (34,2,12,'ins','alice',934)"),
            1
        );
        // UPDATE ... RETURNING: rows are filtered by SELECT, and an updated
        // row that leaves the writer's view is refused.
        assert_eq!(
            rows(&mut sql, "UPDATE t SET label = 'u' RETURNING id"),
            vec!["1", "2"]
        );
        assert_denied(
            &mut sql,
            "UPDATE t SET org_id = 2 WHERE number = 1 RETURNING id",
            NEW_ROW,
        );
        assert_denied(&mut sql, "UPDATE t SET org_id = 2 WHERE id = 2", NEW_ROW);
        // DELETE ... RETURNING only touches and returns visible rows.
        exec(&mut sql, "BEGIN");
        assert_eq!(
            rows(&mut sql, "DELETE FROM t WHERE id IN (2, 15) RETURNING id"),
            vec!["2"]
        );
        exec(&mut sql, "ROLLBACK");
        // Statements that read no column of the target apply no SELECT
        // policy (PostgreSQL): every row the UPDATE/DELETE policy admits.
        exec(&mut sql, "BEGIN");
        assert_eq!(
            affected(&mut sql, "UPDATE t SET label = 'q' WHERE (SELECT true)"),
            6
        );
        assert_eq!(
            rows(&mut sql, "UPDATE t SET label = (SELECT 'z') RETURNING 1").len(),
            6
        );
        assert_eq!(affected(&mut sql, "DELETE FROM t"), 6);
        exec(&mut sql, "ROLLBACK");
    }
}
