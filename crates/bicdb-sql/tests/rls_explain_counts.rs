//! EXPLAIN ANALYZE must not report row counts taken before the row policies
//! filter: for a reader under a policy they reveal how many rows of another
//! tenant match. Readers that bypass the policies (the owner, BYPASSRLS)
//! keep the diagnostic. Both storage modes are covered.
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

fn plan(sql: &mut SqlSession<'_>, statement: &str) -> String {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"))
        .rows
        .iter()
        .map(|row| row[0].to_cell())
        .collect::<Vec<_>>()
        .join("\n")
}

fn fixture(sql: &mut SqlSession<'_>) {
    for statement in [
        "CREATE ROLE ua LOGIN",
        "CREATE ROLE ubypass LOGIN BYPASSRLS",
        "CREATE TABLE m1 (id INT PRIMARY KEY, org_id INT, number INT, label TEXT)",
        "INSERT INTO m1 SELECT g, g % 4, g % 10, 'l' || g FROM generate_series(1, 2000) g",
        "CREATE INDEX m1_ix ON m1 (org_id, number)",
        "ALTER TABLE m1 ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY p ON m1 USING (org_id = (SELECT current_setting('app.org')::int))",
        "CREATE TABLE open_t (id INT PRIMARY KEY, number INT)",
        "INSERT INTO open_t SELECT g, g % 10 FROM generate_series(1, 200) g",
        "CREATE INDEX open_t_ix ON open_t (number)",
        "GRANT SELECT ON m1, open_t TO ua, ubypass",
        "SET app.org = '1'",
    ] {
        exec(sql, statement);
    }
}

const QUERIES: [&str; 4] = [
    // Another tenant's rows only: 100 match before the policy, none after.
    "EXPLAIN ANALYZE SELECT id FROM m1 WHERE number = 2 AND org_id = 2",
    "EXPLAIN ANALYZE SELECT id FROM m1 WHERE number = 3",
    "EXPLAIN ANALYZE SELECT id FROM m1 WHERE label = 'l6'",
    "EXPLAIN ANALYZE SELECT id FROM m1",
];

#[test]
fn policy_filtered_readers_see_no_pre_policy_fetch_counts() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        fixture(&mut sql);
        exec(&mut sql, "SET ROLE ua");
        for query in QUERIES {
            let output = plan(&mut sql, query);
            assert!(
                !output.contains("ActualRowsFetched"),
                "{mode:?} `{query}` reports a pre-policy count:\n{output}"
            );
        }
        // The post-policy result count stays, and matches the query.
        let output = plan(&mut sql, QUERIES[0]);
        assert!(output.contains("ActualRows 0"), "{mode:?}\n{output}");
        let output = plan(&mut sql, QUERIES[1]);
        assert!(output.contains("ActualRows 100"), "{mode:?}\n{output}");
        exec(&mut sql, "SET app.org = '2'");
        let output = plan(&mut sql, QUERIES[1]);
        assert!(output.contains("ActualRows 0"), "{mode:?}\n{output}");
        // A table without policies keeps the diagnostic for the same reader.
        let output = plan(
            &mut sql,
            "EXPLAIN ANALYZE SELECT id FROM open_t WHERE number = 3",
        );
        assert!(
            output.contains("ActualRowsFetched 20"),
            "{mode:?}\n{output}"
        );
        exec(&mut sql, "RESET ROLE");
    }
}

#[test]
fn readers_bypassing_the_policies_keep_fetch_counts() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        fixture(&mut sql);
        // The owner and a BYPASSRLS role read every row: the count is theirs.
        for role in [None, Some("ubypass")] {
            if let Some(role) = role {
                exec(&mut sql, &format!("SET ROLE {role}"));
            }
            let output = plan(&mut sql, QUERIES[0]);
            assert!(
                output.contains("ActualRowsFetched 100"),
                "{mode:?} {role:?}\n{output}"
            );
            assert!(
                output.contains("ActualRows 100"),
                "{mode:?} {role:?}\n{output}"
            );
            exec(&mut sql, "RESET ROLE");
        }
    }
}
