//! SELECT privilege on every relation a query's FROM tree reads is checked
//! before any row is read, as PostgreSQL checks its range table at executor
//! start: whether the statement is refused must not depend on whether the
//! executor happened to scan the relation (an empty join side used to skip
//! it). Views expand with their own semantics: a `security_invoker` view as
//! the caller, a definer view as its owner. Expectations were observed on
//! PostgreSQL 16; both storage modes are covered.
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

fn denied(sql: &mut SqlSession<'_>, statement: &str, table: &str) {
    match sql.execute(statement) {
        Ok(result) => panic!("`{statement}` returned {:?}, expected 42501", result.rows),
        Err(error) => {
            assert_eq!(error.sqlstate(), "42501", "`{statement}`: {error}");
            assert!(
                error
                    .to_string()
                    .contains(&format!("permission denied for table {table}")),
                "`{statement}`: {error}"
            );
        }
    }
}

fn rows(sql: &mut SqlSession<'_>, statement: &str) -> usize {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"))
        .rows
        .len()
}

fn fixture(sql: &mut SqlSession<'_>) {
    for statement in [
        "CREATE ROLE app_owner LOGIN",
        "CREATE ROLE alice LOGIN",
        "CREATE ROLE bob LOGIN",
        "CREATE TABLE vw_t (id INT PRIMARY KEY, org_id INT, number INT)",
        "INSERT INTO vw_t VALUES (1,1,1),(2,2,1),(3,1,2)",
        "CREATE INDEX vw_t_ix ON vw_t (org_id, number)",
        "ALTER TABLE vw_t ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY p ON vw_t USING (org_id = (SELECT current_setting('app.org')::int))",
        "ALTER TABLE vw_t OWNER TO app_owner",
        "GRANT SELECT ON vw_t TO alice",
        "CREATE VIEW vw_d AS SELECT id, org_id, number FROM vw_t",
        "ALTER VIEW vw_d OWNER TO app_owner",
        "GRANT SELECT ON vw_d TO alice, bob",
        "CREATE VIEW vw_i WITH (security_invoker = true) AS SELECT id, org_id, number FROM vw_t",
        "ALTER VIEW vw_i OWNER TO app_owner",
        "GRANT SELECT ON vw_i TO alice, bob",
        "CREATE TABLE lz_open (id INT PRIMARY KEY, n INT)",
        "INSERT INTO lz_open VALUES (1,1)",
        "CREATE TABLE lz_secret (id INT PRIMARY KEY, n INT)",
        "INSERT INTO lz_secret VALUES (1,1)",
        "GRANT SELECT ON lz_open TO bob",
        "SET app.org = '1'",
    ] {
        exec(sql, statement);
    }
}

#[test]
fn invoker_view_without_base_grant_is_refused_even_when_nothing_is_scanned() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        fixture(&mut sql);
        exec(&mut sql, "SET ROLE bob");
        for statement in [
            "SELECT id FROM vw_i WHERE number = 1",
            "SELECT v.id FROM vw_d v JOIN vw_i w ON v.id = w.id WHERE v.number = 1",
            // The definer side matches nothing: the invoker view is never
            // scanned, which used to answer an empty result.
            "SELECT v.id FROM vw_d v JOIN vw_i w ON v.id = w.id WHERE v.number = 99",
            "SELECT v.id FROM vw_d v JOIN vw_i w ON v.id = w.id WHERE v.org_id = 2",
            "SELECT count(*) FROM (SELECT * FROM vw_i) s",
            "WITH c AS (SELECT * FROM vw_i) SELECT count(*) FROM c",
            "SELECT id FROM vw_d WHERE false UNION ALL SELECT id FROM vw_i WHERE false",
        ] {
            denied(&mut sql, statement, "vw_t");
        }
        // The definer view reads as its owner: allowed, and so is the
        // invoker view for a caller holding the base grant.
        assert_eq!(rows(&mut sql, "SELECT id FROM vw_d"), 3, "{mode:?}");
        exec(&mut sql, "SET ROLE alice");
        assert_eq!(
            rows(
                &mut sql,
                "SELECT v.id FROM vw_d v JOIN vw_i w ON v.id = w.id WHERE v.number = 99"
            ),
            0,
            "{mode:?}"
        );
        assert_eq!(rows(&mut sql, "SELECT id FROM vw_i"), 2, "{mode:?}");
        exec(&mut sql, "RESET ROLE");
    }
}

#[test]
fn ungranted_tables_in_the_from_tree_are_refused_regardless_of_data() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        fixture(&mut sql);
        exec(&mut sql, "SET ROLE bob");
        for statement in [
            "SELECT o.id FROM lz_open o JOIN lz_secret s ON o.id = s.id WHERE o.n = 99",
            "SELECT o.id FROM lz_open o LEFT JOIN lz_secret s ON false",
            "SELECT o.id FROM lz_open o, (SELECT id FROM lz_secret) s WHERE o.n = 99",
            "SELECT o.id FROM lz_open o JOIN (lz_open p JOIN lz_secret s ON p.id = s.id) ON o.n = 99",
        ] {
            denied(&mut sql, statement, "lz_secret");
        }
        // Subqueries PostgreSQL's planner removes as unreachable are not
        // checked by it either; an unused CTE neither.
        assert_eq!(
            rows(
                &mut sql,
                "SELECT count(*) FROM lz_open o WHERE false AND EXISTS (SELECT 1 FROM lz_secret)"
            ),
            1
        );
        assert_eq!(
            rows(
                &mut sql,
                "SELECT CASE WHEN false THEN (SELECT count(*) FROM lz_secret) END"
            ),
            1
        );
        assert_eq!(rows(&mut sql, "SELECT id FROM lz_open"), 1);
        exec(&mut sql, "RESET ROLE");
    }
}

#[test]
fn deep_invoker_view_chains_do_not_skip_base_privileges() {
    for mode in MODES {
        let (_dir, mut db) = open(mode.clone());
        let mut sql = SqlSession::new(&mut db);
        fixture(&mut sql);
        let mut source = "vw_i".to_string();
        for level in 0..40 {
            let view = format!("deep_{level}");
            exec(
                &mut sql,
                &format!(
                    "CREATE VIEW {view} WITH (security_invoker = true) AS SELECT id FROM {source}"
                ),
            );
            exec(&mut sql, &format!("ALTER VIEW {view} OWNER TO app_owner"));
            exec(&mut sql, &format!("GRANT SELECT ON {view} TO bob"));
            source = view;
        }
        exec(&mut sql, "SET ROLE bob");
        denied(
            &mut sql,
            "SELECT o.id FROM lz_open o JOIN deep_39 v ON o.id = v.id WHERE o.n = 99",
            "vw_t",
        );
        exec(&mut sql, "RESET ROLE");
    }
}
