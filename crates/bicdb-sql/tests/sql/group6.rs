//! Test group split from the former monolithic tests/sql.rs.
use super::*;

#[test]
fn rollback_removes_alter_table_add_column_schema_change() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ci_partitions (id bigint PRIMARY KEY)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE ci_partitions ADD builds_id_range int8range")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT column_name FROM information_schema.columns
                 WHERE table_name = 'ci_partitions' AND column_name = 'builds_id_range'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("builds_id_range".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();

    assert!(session
        .execute(
            "SELECT column_name FROM information_schema.columns
             WHERE table_name = 'ci_partitions' AND column_name = 'builds_id_range'"
        )
        .unwrap()
        .rows
        .is_empty());
    session
        .execute("ALTER TABLE ci_partitions ADD builds_id_range int8range")
        .unwrap();
}

#[test]
fn rollback_removes_exclusion_constraint_added_with_native_range_column() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ci_partitions (id bigint PRIMARY KEY, builds_id_range int8range)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute(
            "ALTER TABLE ci_partitions
             ADD CONSTRAINT check_ci_partitions_builds_id_range_no_overlap
             EXCLUDE USING gist (builds_id_range WITH &&)
             WHERE (builds_id_range IS NOT NULL)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT conname FROM pg_catalog.pg_constraint
                 WHERE conname = 'check_ci_partitions_builds_id_range_no_overlap'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "check_ci_partitions_builds_id_range_no_overlap".to_string()
        )]]
    );
    session.execute("ROLLBACK").unwrap();

    assert!(session
        .execute(
            "SELECT conname FROM pg_catalog.pg_constraint
             WHERE conname = 'check_ci_partitions_builds_id_range_no_overlap'"
        )
        .unwrap()
        .rows
        .is_empty());
    session
        .execute(
            "ALTER TABLE ci_partitions
             ADD CONSTRAINT check_ci_partitions_builds_id_range_no_overlap
             EXCLUDE USING gist (builds_id_range WITH &&)
             WHERE (builds_id_range IS NOT NULL)",
        )
        .unwrap();
}

#[test]
fn rollback_restores_schema_after_gitlab_style_migration_bookkeeping() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ci_partitions (id bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("CREATE TABLE schema_migrations (version varchar NOT NULL)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE ci_partitions ADD builds_id_range int8range")
        .unwrap();
    session
        .execute(
            "ALTER TABLE ci_partitions
             ADD CONSTRAINT check_ci_partitions_builds_id_range_no_overlap
             EXCLUDE USING gist (builds_id_range WITH &&)
             WHERE (builds_id_range IS NOT NULL)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "INSERT INTO schema_migrations (version)
                 VALUES ('20260413132957')
                 RETURNING version"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("20260413132957".to_string())]]
    );
    session.execute("ROLLBACK").unwrap();

    assert!(session
        .execute(
            "SELECT column_name FROM information_schema.columns
             WHERE table_name = 'ci_partitions' AND column_name = 'builds_id_range'"
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute(
            "SELECT conname FROM pg_catalog.pg_constraint
             WHERE conname = 'check_ci_partitions_builds_id_range_no_overlap'"
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute("SELECT version FROM schema_migrations")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn rollback_restores_row_rewrites_from_alter_table_operations() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE tasks (id text PRIMARY KEY, label text)")
        .unwrap();
    session
        .execute("INSERT INTO tasks (id, label) VALUES ('t1', 'first')")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE tasks ADD status text DEFAULT 'queued'")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    session
        .execute("ALTER TABLE tasks ADD status text")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT label, status FROM tasks WHERE id = 't1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".to_string()), SqlValue::Null,]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE tasks DROP COLUMN label")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM tasks WHERE id = 't1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".to_string())]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE tasks RENAME COLUMN label TO title")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM tasks WHERE id = 't1'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("first".to_string())]]
    );
}

#[test]
fn postgres_truncate_table_deletes_rows_transactionally_and_preserves_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE upload_chunks (
                id BIGSERIAL PRIMARY KEY,
                path TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute("CREATE INDEX index_upload_chunks_on_path ON upload_chunks (path)")
        .unwrap();
    session
        .execute("INSERT INTO upload_chunks (path) VALUES ('a'), ('b')")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session.execute("TRUNCATE TABLE upload_chunks").unwrap();
    assert_eq!(
        session.execute("COMMIT").unwrap().command_complete_tag(),
        "COMMIT"
    );
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM upload_chunks")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT relname FROM pg_catalog.pg_class
                 WHERE relkind = 'i' AND relname = 'index_upload_chunks_on_path'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "index_upload_chunks_on_path".to_string()
        )]]
    );

    session
        .execute("INSERT INTO upload_chunks (path) VALUES ('c')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM upload_chunks WHERE path = 'c'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(3)]]
    );

    session
        .execute("TRUNCATE TABLE upload_chunks RESTART IDENTITY")
        .unwrap();
    session
        .execute("INSERT INTO upload_chunks (path) VALUES ('d')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM upload_chunks WHERE path = 'd'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );

    session.execute("BEGIN").unwrap();
    session.execute("TRUNCATE TABLE upload_chunks").unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM upload_chunks")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute("TRUNCATE TABLE IF EXISTS missing_upload_chunks")
            .unwrap()
            .command_complete_tag(),
        "TRUNCATE TABLE"
    );
}

#[test]
fn postgres_truncate_cascade_expands_foreign_key_dependents() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE truncate_parents (id TEXT PRIMARY KEY);
             CREATE TABLE truncate_children (
                 id TEXT PRIMARY KEY,
                 parent_id TEXT NOT NULL REFERENCES truncate_parents(id)
             );
             CREATE TABLE truncate_grandchildren (
                 id TEXT PRIMARY KEY,
                 child_id TEXT NOT NULL REFERENCES truncate_children(id)
             );
             INSERT INTO truncate_parents VALUES ('p1');
             INSERT INTO truncate_children VALUES ('c1', 'p1');
             INSERT INTO truncate_grandchildren VALUES ('g1', 'c1');",
        )
        .unwrap();

    let restricted = session.execute("TRUNCATE truncate_parents").unwrap_err();
    assert_eq!(restricted.sqlstate(), "2BP01");

    session
        .execute("TRUNCATE truncate_parents CASCADE")
        .unwrap();
    for table in [
        "truncate_parents",
        "truncate_children",
        "truncate_grandchildren",
    ] {
        assert_eq!(
            session
                .execute(&format!("SELECT COUNT(*) FROM {table}"))
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(0)]],
            "{table} was not truncated"
        );
    }
}

#[test]
fn postgres_truncate_cascade_accepts_explicit_multi_table_targets() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE graph_node (id TEXT PRIMARY KEY);
             CREATE TABLE graph_edge (
                 id TEXT PRIMARY KEY,
                 source_id TEXT NOT NULL REFERENCES graph_node(id)
             );
             INSERT INTO graph_node VALUES ('n1');
             INSERT INTO graph_edge VALUES ('e1', 'n1');",
        )
        .unwrap();
    session
        .execute("TRUNCATE graph_edge, graph_node CASCADE")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM graph_node")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        session
            .execute("SELECT COUNT(*) FROM graph_edge")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
}

#[test]
fn correlated_locator_cache_does_not_reuse_null_outer_values() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE graph_edge (\
                source_id VARCHAR NOT NULL, \
                target_id VARCHAR NOT NULL, \
                relationship_name VARCHAR NOT NULL, \
                PRIMARY KEY (source_id, target_id, relationship_name)\
            )",
        )
        .unwrap();
    session
        .execute("INSERT INTO graph_edge VALUES ('e1', 'e2', 'KNOWS')")
        .unwrap();

    let null_probe = session
        .execute(
            "SELECT q.src, q.tgt, q.rel \
             FROM (VALUES (NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) AS q(src, tgt, rel) \
             WHERE EXISTS (\
                 SELECT 1 FROM graph_edge \
                 WHERE graph_edge.source_id = q.src \
                   AND graph_edge.target_id = q.tgt \
                   AND graph_edge.relationship_name = q.rel\
             )",
        )
        .unwrap();
    assert!(null_probe.rows.is_empty());

    let real_values = session
        .execute(
            "SELECT q.src, q.tgt, q.rel \
             FROM (VALUES ('e1'::VARCHAR, 'e2'::VARCHAR, 'KNOWS'::VARCHAR)) AS q(src, tgt, rel) \
             WHERE EXISTS (\
                 SELECT 1 FROM graph_edge \
                 WHERE graph_edge.source_id = q.src \
                   AND graph_edge.target_id = q.tgt \
                   AND graph_edge.relationship_name = q.rel\
             )",
        )
        .unwrap();
    assert_eq!(
        real_values.rows,
        vec![vec![
            SqlValue::String("e1".to_string()),
            SqlValue::String("e2".to_string()),
            SqlValue::String("KNOWS".to_string()),
        ]]
    );
}

#[test]
fn transaction_isolation_commands_accept_read_only_repeatable_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .unwrap();
    session
        .execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .unwrap();
    session.execute("COMMIT").unwrap();
    session
        .execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    session
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .unwrap();
    session.execute("ROLLBACK").unwrap();

    for sql in [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE",
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE",
    ] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "0A000");
        assert_eq!(
            error.to_string(),
            "unsupported SQL: transaction isolation level REPEATABLE READ requires READ ONLY"
        );
    }

    let error = session
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(
        error.to_string(),
        "unsupported SQL: transaction isolation level SERIALIZABLE is not supported"
    );
}

#[test]
fn update_from_rechecks_concurrently_modified_rows_read_committed() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute("CREATE TABLE stock (s_i_id INT PRIMARY KEY, s_quantity INT)")
            .unwrap();
        setup
            .execute("INSERT INTO stock VALUES (1, 50), (2, 70)")
            .unwrap();
    }

    let mut tx_session = SqlSession::new_shared(&db);
    tx_session.execute("BEGIN").unwrap();
    // Pin the transaction snapshot before the competing writer commits.
    tx_session
        .execute("SELECT s_quantity FROM stock WHERE s_i_id = 1")
        .unwrap();

    let mut competing = SqlSession::new_shared(&db);
    competing.execute("BEGIN").unwrap();
    competing
        .execute("UPDATE stock SET s_quantity = 40 WHERE s_i_id = 1")
        .unwrap();
    competing.execute("COMMIT").unwrap();

    // UPDATE ... FROM deliberately does NOT do a per-row READ COMMITTED
    // recheck (the per-row lock+refresh is contention-priced at high
    // concurrency; see execute_update_with_ctes). The stale write must
    // instead surface as a serialization failure at COMMIT so the caller
    // (pgwire's bounded retry, or the client) replays the statement against
    // a fresh snapshot — never a silent lost update.
    tx_session
        .execute(
            "UPDATE stock SET s_quantity = stock.s_quantity - v.qty \
             FROM (SELECT 1 AS id, 5 AS qty UNION ALL SELECT 2, 7) v \
             WHERE stock.s_i_id = v.id",
        )
        .unwrap();
    let error = tx_session.execute("COMMIT").unwrap_err();
    assert!(matches!(
        error,
        SqlError::BicDb(BicDbError::TransactionConflict(_))
    ));

    let mut check = SqlSession::new_shared(&db);
    let result = check
        .execute("SELECT s_i_id, s_quantity FROM stock ORDER BY s_i_id")
        .unwrap();
    // The concurrent committer's value survives untouched; nothing was lost.
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1), SqlValue::Int(40)],
            vec![SqlValue::Int(2), SqlValue::Int(70)],
        ]
    );
}

#[test]
fn payment_shaped_updates_repair_at_commit_instead_of_conflicting() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut setup = SqlSession::new(&mut db);
        setup
            .execute("CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_ytd NUMERIC, w_name TEXT)")
            .unwrap();
        setup
            .execute("INSERT INTO warehouse VALUES (1, 100, 'alpha')")
            .unwrap();
    }

    let mut tx_session = SqlSession::new_shared(&db);
    tx_session.execute("SET bicdb.update_repair = on").unwrap();
    tx_session.execute("BEGIN").unwrap();
    // Pin the snapshot before the competing committer.
    tx_session
        .execute("SELECT w_name FROM warehouse WHERE w_id = 1")
        .unwrap();

    let mut competing = SqlSession::new_shared(&db);
    competing.execute("BEGIN").unwrap();
    competing
        .execute("UPDATE warehouse SET w_ytd = w_ytd + 10 WHERE w_id = 1")
        .unwrap();
    competing.execute("COMMIT").unwrap();

    // Without repair this transaction serialization-fails at COMMIT (the row
    // moved past its snapshot). With repair the record is rebuilt at commit as
    // latest (110) + delta (5).
    let updated = tx_session
        .execute("UPDATE warehouse SET w_ytd = w_ytd + 5 WHERE w_id = 1 RETURNING w_name")
        .unwrap();
    assert_eq!(
        updated.rows,
        vec![vec![SqlValue::String("alpha".to_string())]]
    );
    tx_session.execute("COMMIT").unwrap();

    let mut check = SqlSession::new_shared(&db);
    let result = check
        .execute("SELECT w_ytd, w_name FROM warehouse WHERE w_id = 1")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("115".to_string()),
            SqlValue::String("alpha".to_string())
        ]]
    );
    let record = db.get("warehouse", "1").unwrap().unwrap();
    let expected =
        bicdb_sql::pg_typed_index_key("numeric", &SqlValue::String("115".into())).unwrap();
    let encoded = expected
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(
        record
            .metadata
            .pointer("/w_ytd/$bicdb_typed/index_key")
            .and_then(|value| value.as_str()),
        Some(encoded.as_str()),
        "repair must update the stored numeric ordering key as well as its text"
    );
    drop((tx_session, competing, check));
    let mut indexed = SqlSession::new(&mut db);
    indexed
        .execute("CREATE INDEX warehouse_ytd_idx ON warehouse(w_ytd)")
        .unwrap();
    assert_eq!(
        indexed
            .execute("SELECT w_id FROM warehouse WHERE w_ytd = 115")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]],
        "an index built after repair must find the repaired value"
    );
}

#[test]
fn update_repair_rechecks_mutable_predicates() {
    for predicate in ["balance >= 60", "enabled = TRUE"] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        {
            let mut setup = SqlSession::new(&mut db);
            setup
                .execute(
                    "CREATE TABLE accounts (id INT PRIMARY KEY, balance NUMERIC, enabled BOOLEAN)",
                )
                .unwrap();
            setup
                .execute("INSERT INTO accounts VALUES (1, 100, TRUE)")
                .unwrap();
        }
        let mut pending = SqlSession::new_shared(&db);
        pending.execute("SET bicdb.update_repair = on").unwrap();
        pending.execute("BEGIN").unwrap();
        pending
            .execute("SELECT balance FROM accounts WHERE id = 1")
            .unwrap();
        let mut competing = SqlSession::new_shared(&db);
        competing.execute("BEGIN").unwrap();
        competing
            .execute("UPDATE accounts SET balance = 40, enabled = FALSE WHERE id = 1")
            .unwrap();
        competing.execute("COMMIT").unwrap();
        let updated = pending
            .execute(&format!(
            "UPDATE accounts SET balance = balance - 60 WHERE id = 1 AND {predicate} RETURNING id"
        ))
            .unwrap();
        pending.execute("COMMIT").unwrap();
        let mut check = SqlSession::new_shared(&db);
        assert_eq!(
            check
                .execute("SELECT balance FROM accounts WHERE id = 1")
                .unwrap()
                .rows,
            vec![vec![SqlValue::String("40".into())]],
            "stale predicate {predicate} must not authorize a debit"
        );
        assert!(
            updated.rows.is_empty(),
            "a predicate invalidated before the UPDATE must be rechecked"
        );
    }
}

#[test]
fn discard_session_commands_are_accepted_for_client_pool_resets() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("DISCARD ALL")
            .unwrap()
            .command_complete_tag(),
        "DISCARD ALL"
    );
    assert_eq!(
        session
            .execute("DISCARD SEQUENCES")
            .unwrap()
            .command_complete_tag(),
        "DISCARD SEQUENCES"
    );
    assert_eq!(
        session
            .execute("DISCARD PLANS")
            .unwrap()
            .command_complete_tag(),
        "DISCARD PLANS"
    );
    assert_eq!(
        session
            .execute("DISCARD TEMP")
            .unwrap()
            .command_complete_tag(),
        "DISCARD TEMP"
    );
}

#[test]
fn local_transactions_support_nested_savepoint_rollback_and_release() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p1', 'Keep')")
        .unwrap();
    session.execute("SAVEPOINT outer_sp").unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p2', 'Drop outer')")
        .unwrap();
    session.execute("SAVEPOINT inner_sp").unwrap();
    session
        .execute("INSERT INTO patients (id, name) VALUES ('p3', 'Drop inner')")
        .unwrap();
    session.execute("ROLLBACK TO SAVEPOINT inner_sp").unwrap();
    session.execute("RELEASE SAVEPOINT inner_sp").unwrap();
    session.execute("ROLLBACK TO SAVEPOINT outer_sp").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(
        session
            .execute("SELECT id FROM patients ORDER BY id")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("p1".to_string())]]
    );
}

#[test]
fn local_transactions_report_released_savepoint_as_missing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("BEGIN").unwrap();
    session.execute("SAVEPOINT gone").unwrap();
    session.execute("RELEASE SAVEPOINT gone").unwrap();
    let error = session.execute("ROLLBACK TO SAVEPOINT gone").unwrap_err();
    assert_eq!(error.sqlstate(), "3B001");
    assert_eq!(error.to_string(), "savepoint \"gone\" does not exist");
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn non_recursive_ctes_select_from_tables_and_constants() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO patients (id, name, age) VALUES ('p1', 'Ada', 45), ('p2', 'Grace', 36)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "WITH adults AS (SELECT id, name FROM patients WHERE age > 40) SELECT id, name FROM adults"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("p1".to_string()),
            SqlValue::String("Ada".to_string())
        ]]
    );
    assert_eq!(
        session
            .execute("WITH one AS (SELECT 1 AS value) SELECT value FROM one")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn values_ctes_feed_scalar_lookups_and_downstream_joins() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE app_manifests (
                app_name TEXT PRIMARY KEY,
                org_id TEXT,
                hub_id TEXT,
                is_enabled BOOLEAN
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE hub_app_grants (
                app_name TEXT PRIMARY KEY,
                org_id TEXT,
                hub_id TEXT,
                user_id TEXT,
                can_use BOOLEAN,
                status TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO app_manifests (app_name, org_id, hub_id, is_enabled)
             VALUES ('calendar', 'org-1', 'hub-1', true),
                    ('disabled', 'org-1', 'hub-1', false)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO hub_app_grants
                (app_name, org_id, hub_id, user_id, can_use, status)
             VALUES ('calendar', 'org-1', 'hub-1', 'user-1', true, 'active')",
        )
        .unwrap();

    let result = session
        .execute(
            "WITH role_ranks(role, role_rank) AS (
                VALUES ('guest', 10), ('member', 20), ('admin', 40), ('owner', 50)
             ),
             actor_context AS (
                SELECT
                    'admin'::text AS role,
                    COALESCE((
                        SELECT role_rank
                        FROM role_ranks
                        WHERE role = 'admin'::text
                    ), 0) AS role_rank
             ),
             viewer_grants AS (
                SELECT g.app_name, g.can_use
                FROM hub_app_grants g
                WHERE g.org_id = 'org-1'
                  AND g.hub_id = 'hub-1'
                  AND g.user_id = 'user-1'
                  AND g.status = 'active'
             )
             SELECT app.app_name, actor_context.role, actor_context.role_rank, vg.can_use
             FROM app_manifests app
             CROSS JOIN actor_context
             LEFT JOIN viewer_grants vg ON vg.app_name = app.app_name
             WHERE app.org_id = 'org-1'
               AND app.hub_id = 'hub-1'
               AND app.is_enabled = true",
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::String("calendar".to_string()),
            SqlValue::String("admin".to_string()),
            SqlValue::Int(40),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn lateral_no_from_projection_names_and_boolean_values_match_postgres_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE lateral_flags (id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    session
        .execute("INSERT INTO lateral_flags VALUES (1, true), (2, NULL)")
        .unwrap();

    let result = session
        .execute(
            r#"SELECT
                   f.id,
                   arithmetic."?column?",
                   truth.is_true,
                   truth.is_not_false,
                   truth.is_unknown
               FROM lateral_flags f
               CROSS JOIN LATERAL (SELECT f.id + 1) arithmetic
               CROSS JOIN LATERAL (
                   SELECT
                       f.flag IS TRUE AS is_true,
                       f.flag IS NOT FALSE AS is_not_false,
                       f.flag IS UNKNOWN AS is_unknown
               ) truth
               ORDER BY f.id"#,
        )
        .unwrap();

    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::Int(1),
                SqlValue::Int(2),
                SqlValue::Bool(true),
                SqlValue::Bool(true),
                SqlValue::Bool(false),
            ],
            vec![
                SqlValue::Int(2),
                SqlValue::Int(3),
                SqlValue::Bool(false),
                SqlValue::Bool(true),
                SqlValue::Bool(true),
            ],
        ]
    );
}

#[test]
fn cte_column_aliases_replace_output_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, name TEXT, age INT)")
        .unwrap();
    session
        .execute("INSERT INTO patients (id, name, age) VALUES ('p1', 'Ada', 45)")
        .unwrap();

    let result = session
        .execute(
            "WITH renamed(patient_id, years) AS (SELECT id, age FROM patients) SELECT patient_id, years FROM renamed",
        )
        .unwrap();
    assert_eq!(result.columns, vec!["patient_id", "years"]);
    assert_eq!(
        result.rows,
        vec![vec![SqlValue::String("p1".to_string()), SqlValue::Int(45)]]
    );
    assert!(matches!(
        session.execute(
            "WITH renamed(only_one) AS (SELECT id, age FROM patients) SELECT only_one FROM renamed"
        ),
        Err(SqlError::InvalidSql(message)) if message.contains("columns available")
    ));
}

#[test]
fn recursive_ctes_expand_union_terms_until_fixpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    let result = session
        .execute(
            "WITH RECURSIVE nums(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM nums WHERE n < 3
             )
             SELECT n FROM nums ORDER BY n",
        )
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(3)]
        ]
    );
}

#[test]
fn recursive_ctes_can_walk_postgres_role_memberships() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE ROLE owner_role").unwrap();
        session
            .execute("CREATE USER worker_user PASSWORD 'secret'")
            .unwrap();
        session.execute("GRANT owner_role TO worker_user").unwrap();
    }

    let result = SqlEngine::new(&db)
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT oid, 0 AS steps, true AS inherit_option
                FROM pg_roles
                WHERE rolname = 'worker_user'
                UNION ALL
                SELECT m.roleid, c.steps + 1, c.inherit_option AND c.inherit_option
                FROM cte c
                JOIN pg_auth_members m ON m.member = c.oid
             )
             SELECT count(*) > 0 AS is_owner
             FROM cte, pg_roles r
             WHERE cte.oid = r.oid AND r.rolname = 'owner_role'",
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::Bool(true)]]);
}

#[test]
fn data_modifying_insert_ctes_feed_later_ctes_and_outer_select() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE assignment_sources (id TEXT PRIMARY KEY, title TEXT);
             CREATE TABLE adherence_expectations (id TEXT PRIMARY KEY, source_id TEXT, title TEXT);
             CREATE TABLE adherence_instances (id TEXT PRIMARY KEY, expectation_id TEXT);
             CREATE TABLE adherence_events (id TEXT PRIMARY KEY, instance_id TEXT);
             INSERT INTO assignment_sources VALUES ('activity-1', 'Walk')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "WITH normalized AS (
                     SELECT id, title FROM assignment_sources
                 ), inserted_expectations AS (
                     INSERT INTO adherence_expectations (id, source_id, title)
                     SELECT id || '-expectation', id, title FROM normalized
                     RETURNING id, source_id
                 ), inserted_instances AS (
                     INSERT INTO adherence_instances (id, expectation_id)
                     SELECT source_id || '-instance', id FROM inserted_expectations
                     RETURNING id, expectation_id
                 ), events AS (
                     INSERT INTO adherence_events (id, instance_id)
                     SELECT expectation_id || '-event', id FROM inserted_instances
                     RETURNING id
                 )
                 SELECT COUNT(*) FROM inserted_expectations",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT
                     (SELECT COUNT(*) FROM adherence_expectations),
                     (SELECT COUNT(*) FROM adherence_instances),
                     (SELECT COUNT(*) FROM adherence_events)"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1), SqlValue::Int(1), SqlValue::Int(1)]]
    );
}

#[test]
fn ctes_feed_update_from_statements() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE batched_background_migrations (
                id TEXT PRIMARY KEY,
                status INT,
                queued_migration_version TEXT,
                min_value INT,
                max_value INT,
                finished_at TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE batched_background_migration_jobs (
                id TEXT PRIMARY KEY,
                batched_background_migration_id TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migrations
             (id, status, queued_migration_version, min_value, max_value, finished_at)
             VALUES
             ('affected', 3, '20250905091200', 7, 7, 'done'),
             ('has_job', 3, '20250905091200', 7, 7, 'done'),
             ('outside_range', 3, '20240101000000', 7, 7, 'done'),
             ('not_single', 3, '20250905091200', 7, 8, 'done')",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migration_jobs
             (id, batched_background_migration_id)
             VALUES ('job-1', 'has_job')",
        )
        .unwrap();

    let result = session
        .execute(
            "WITH affected_migrations AS (
                SELECT m.id
                FROM batched_background_migrations m
                LEFT JOIN batched_background_migration_jobs j
                  ON m.id = j.batched_background_migration_id
                WHERE j.id IS NULL
                  AND m.status IN (3, 6)
                  AND m.queued_migration_version BETWEEN '20250905091200' AND '20260216140430'
                  AND m.min_value IS NOT NULL
                  AND m.min_value = m.max_value
             )
             UPDATE batched_background_migrations
             SET status = 0, finished_at = NULL
             FROM affected_migrations
             WHERE batched_background_migrations.id = affected_migrations.id",
        )
        .unwrap();
    assert_eq!(result.rows, Vec::<Vec<SqlValue>>::new());

    assert_eq!(
        session
            .execute(
                "SELECT id, status, finished_at
                 FROM batched_background_migrations
                 ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("affected".to_string()),
                SqlValue::Int(0),
                SqlValue::Null,
            ],
            vec![
                SqlValue::String("has_job".to_string()),
                SqlValue::Int(3),
                SqlValue::String("done".to_string()),
            ],
            vec![
                SqlValue::String("not_single".to_string()),
                SqlValue::Int(3),
                SqlValue::String("done".to_string()),
            ],
            vec![
                SqlValue::String("outside_range".to_string()),
                SqlValue::Int(3),
                SqlValue::String("done".to_string()),
            ],
        ]
    );
}

#[test]
fn cte_qualified_wildcard_can_be_realiased_and_joined() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE batched_background_migrations (
                id TEXT PRIMARY KEY,
                status INT,
                queued_migration_version TEXT,
                gitlab_schema TEXT,
                min_value INT,
                max_value INT,
                min_cursor TEXT,
                max_cursor TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE batched_background_migration_jobs (
                id TEXT PRIMARY KEY,
                batched_background_migration_id TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migrations
             (id, status, queued_migration_version, gitlab_schema, min_value, max_value, min_cursor, max_cursor)
             VALUES
             ('affected', 0, '20250905091200', 'gitlab_main', 7, 7, NULL, NULL),
             ('has_job', 0, '20250905091200', 'gitlab_main', 7, 7, NULL, NULL),
             ('active', 1, '20250905091200', 'gitlab_main', 7, 7, NULL, NULL)",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO batched_background_migration_jobs
             (id, batched_background_migration_id)
             VALUES ('job-1', 'has_job')",
        )
        .unwrap();

    let rows = session
        .execute(
            "WITH migrations AS (
                SELECT batched_background_migrations.*
                FROM batched_background_migrations
                WHERE batched_background_migrations.gitlab_schema IN ('gitlab_main')
                  AND batched_background_migrations.queued_migration_version BETWEEN '20250905091200' AND '20260216140430'
                  AND batched_background_migrations.status = 0
             )
             SELECT m.*
             FROM migrations m
             LEFT JOIN batched_background_migration_jobs j
               ON m.id = j.batched_background_migration_id
             WHERE j.id IS NULL
               AND (
                 (m.min_value IS NOT NULL AND m.min_value = m.max_value)
                 OR
                 (m.min_cursor IS NOT NULL AND m.min_cursor = m.max_cursor)
               )",
        )
        .unwrap()
        .rows;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::String("affected".to_string()));
}

#[test]
fn window_functions_execute_ranking_aggregates_values_and_rows_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE patients (id TEXT PRIMARY KEY, team TEXT, score INT, value INT)")
        .unwrap();
    session
        .execute(
            "INSERT INTO patients (id, team, score, value) VALUES
             ('p1', 'a', 10, 10), ('p2', 'a', 20, 20), ('p3', 'a', 20, NULL),
             ('p4', 'b', 5, 5), ('p5', 'b', 15, 15)",
        )
        .unwrap();

    let ranking = session
        .execute(
            "SELECT id,
                    ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC, id),
                    RANK() OVER (PARTITION BY team ORDER BY score DESC),
                    DENSE_RANK() OVER (PARTITION BY team ORDER BY score DESC),
                    NTILE(3) OVER (PARTITION BY team ORDER BY score, id)
             FROM patients ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        ranking.rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Int(3),
                SqlValue::Int(3),
                SqlValue::Int(2),
                SqlValue::Int(1)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(1),
                SqlValue::Int(1),
                SqlValue::Int(1),
                SqlValue::Int(2)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(2),
                SqlValue::Int(1),
                SqlValue::Int(1),
                SqlValue::Int(3)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Int(2),
                SqlValue::Int(2),
                SqlValue::Int(2),
                SqlValue::Int(1)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(1),
                SqlValue::Int(1),
                SqlValue::Int(1),
                SqlValue::Int(2)
            ],
        ]
    );
    assert_eq!(
        ranking.column_types,
        vec![
            Some("text".to_string()),
            Some("int8".to_string()),
            Some("int8".to_string()),
            Some("int8".to_string()),
            Some("int8".to_string()),
        ]
    );
    let wildcard = session
        .execute("SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM patients ORDER BY id")
        .unwrap();
    assert_eq!(wildcard.columns.len(), 5);
    assert!(wildcard
        .columns
        .iter()
        .all(|column| !column.starts_with("__bicdb_window__")));
    assert_eq!(
        session
            .execute(
                "SELECT id FROM patients
                 ORDER BY ROW_NUMBER() OVER (ORDER BY score DESC, id)"
            )
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            SqlValue::String("p2".into()),
            SqlValue::String("p3".into()),
            SqlValue::String("p5".into()),
            SqlValue::String("p1".into()),
            SqlValue::String("p4".into()),
        ]
    );

    let aggregates = session
        .execute(
            "SELECT id,
                    SUM(score) OVER (PARTITION BY team),
                    SUM(score) OVER (PARTITION BY team ORDER BY score),
                    SUM(score) OVER (PARTITION BY team ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                    COUNT(value) OVER (PARTITION BY team)
             FROM patients ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        aggregates.rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Int(50),
                SqlValue::Int(10),
                SqlValue::Int(10),
                SqlValue::Int(2)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(50),
                SqlValue::Int(50),
                SqlValue::Int(30),
                SqlValue::Int(2)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(50),
                SqlValue::Int(50),
                SqlValue::Int(50),
                SqlValue::Int(2)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Int(20),
                SqlValue::Int(5),
                SqlValue::Int(5),
                SqlValue::Int(2)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(2)
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id, SUM(score) OVER (
                    PARTITION BY team ORDER BY score, id
                    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
                 ) FROM patients ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("p1".into()), SqlValue::Int(30)],
            vec![SqlValue::String("p2".into()), SqlValue::Int(50)],
            vec![SqlValue::String("p3".into()), SqlValue::Int(40)],
            vec![SqlValue::String("p4".into()), SqlValue::Int(20)],
            vec![SqlValue::String("p5".into()), SqlValue::Int(20)],
        ]
    );

    let values = session
        .execute(
            "SELECT id,
                    LAG(value) OVER (PARTITION BY team ORDER BY score, id),
                    LEAD(value, 1, -1) OVER (PARTITION BY team ORDER BY score, id),
                    FIRST_VALUE(value) OVER (PARTITION BY team ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    LAST_VALUE(value) OVER (PARTITION BY team ORDER BY score, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                    CASE WHEN ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC, id) = 1 THEN 'top' ELSE 'rest' END
             FROM patients ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        values.rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Null,
                SqlValue::Int(20),
                SqlValue::Int(10),
                SqlValue::Null,
                SqlValue::String("rest".into())
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(10),
                SqlValue::Null,
                SqlValue::Int(10),
                SqlValue::Null,
                SqlValue::String("top".into())
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(20),
                SqlValue::Int(-1),
                SqlValue::Int(10),
                SqlValue::Null,
                SqlValue::String("rest".into())
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Null,
                SqlValue::Int(15),
                SqlValue::Int(5),
                SqlValue::Int(15),
                SqlValue::String("rest".into())
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(5),
                SqlValue::Int(-1),
                SqlValue::Int(5),
                SqlValue::Int(15),
                SqlValue::String("top".into())
            ],
        ]
    );

    assert_eq!(
        session
            .execute("SELECT ROW_NUMBER() OVER (ORDER BY 1)")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT COALESCE(ROW_NUMBER() OVER (ORDER BY 1), 0),
                        ROW_NUMBER() OVER (ORDER BY 1) BETWEEN 1 AND 1"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1), SqlValue::Bool(true)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT id, ROW_NUMBER() OVER ranked
                 FROM patients
                 WINDOW ranked AS (PARTITION BY team ORDER BY score DESC, id)
                 ORDER BY id"
            )
            .unwrap()
            .rows[0][1],
        SqlValue::Int(3)
    );
}

#[test]
fn window_functions_cover_range_groups_grouped_inheritance_nulls_and_distribution() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE window_rows (
               id TEXT PRIMARY KEY, team TEXT, score INT, value INT, happened_at TIMESTAMP
             )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO window_rows VALUES
             ('p1', 'a', 10, 10, '2025-01-01'),
             ('p2', 'a', 20, 20, '2025-01-03'),
             ('p3', 'a', 20, NULL, '2025-01-03'),
             ('p4', 'b', 5, 5, '2025-01-01'),
             ('p5', 'b', 15, 15, '2025-01-10')",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT id,
                        SUM(score) OVER (
                          PARTITION BY team ORDER BY score
                          RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING
                        ),
                        SUM(score) OVER (
                          PARTITION BY team ORDER BY score
                          GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW
                        ),
                        SUM(score) OVER (
                          PARTITION BY team ORDER BY score DESC
                          RANGE BETWEEN 5 PRECEDING AND CURRENT ROW
                        ),
                        SUM(score) OVER (
                          PARTITION BY team ORDER BY happened_at
                          RANGE BETWEEN INTERVAL '2 days' PRECEDING
                                    AND INTERVAL '2 days' FOLLOWING
                        )
                 FROM window_rows ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Int(10),
                SqlValue::Int(10),
                SqlValue::Int(10),
                SqlValue::Int(50)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(40),
                SqlValue::Int(50),
                SqlValue::Int(40),
                SqlValue::Int(50)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(40),
                SqlValue::Int(50),
                SqlValue::Int(40),
                SqlValue::Int(50)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Int(5),
                SqlValue::Int(5),
                SqlValue::Int(5),
                SqlValue::Int(5)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(15),
                SqlValue::Int(20),
                SqlValue::Int(15),
                SqlValue::Int(15)
            ],
        ]
    );

    let distribution = session
        .execute(
            "SELECT id,
                    PERCENT_RANK() OVER (PARTITION BY team ORDER BY score),
                    CUME_DIST() OVER (PARTITION BY team ORDER BY score)
             FROM window_rows ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        distribution.rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Float(0.0),
                SqlValue::Float(1.0 / 3.0)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Float(0.5),
                SqlValue::Float(1.0)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Float(0.5),
                SqlValue::Float(1.0)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Float(0.0),
                SqlValue::Float(0.5)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Float(1.0),
                SqlValue::Float(1.0)
            ],
        ]
    );
    assert_eq!(
        distribution.column_types,
        vec![
            Some("text".to_string()),
            Some("float8".to_string()),
            Some("float8".to_string()),
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT id,
                        LEAD(value, 1, -1) IGNORE NULLS OVER (
                          PARTITION BY team ORDER BY score, id
                        ),
                        LAST_VALUE(value) IGNORE NULLS OVER (
                          PARTITION BY team ORDER BY score, id
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ),
                        NTH_VALUE(value, 2) RESPECT NULLS OVER (
                          PARTITION BY team ORDER BY score, id
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ),
                        NTH_VALUE(value, 2) IGNORE NULLS OVER (
                          PARTITION BY team ORDER BY score DESC, id
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        )
                 FROM window_rows ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(10)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(-1),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(10)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(-1),
                SqlValue::Int(20),
                SqlValue::Int(20),
                SqlValue::Int(10)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Int(15),
                SqlValue::Int(15),
                SqlValue::Int(15),
                SqlValue::Int(5)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(-1),
                SqlValue::Int(15),
                SqlValue::Int(15),
                SqlValue::Int(5)
            ],
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT id,
                        ROW_NUMBER() OVER ordered,
                        SUM(score) OVER framed
                 FROM window_rows
                 WINDOW base AS (PARTITION BY team),
                        ordered AS (base ORDER BY score, id),
                        framed AS (ordered ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                 ORDER BY id"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("p1".into()),
                SqlValue::Int(1),
                SqlValue::Int(10)
            ],
            vec![
                SqlValue::String("p2".into()),
                SqlValue::Int(2),
                SqlValue::Int(30)
            ],
            vec![
                SqlValue::String("p3".into()),
                SqlValue::Int(3),
                SqlValue::Int(50)
            ],
            vec![
                SqlValue::String("p4".into()),
                SqlValue::Int(1),
                SqlValue::Int(5)
            ],
            vec![
                SqlValue::String("p5".into()),
                SqlValue::Int(2),
                SqlValue::Int(20)
            ],
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT team,
                        SUM(score),
                        RANK() OVER (ORDER BY SUM(score)),
                        SUM(SUM(score)) OVER (
                          ORDER BY SUM(score) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )
                 FROM window_rows
                 GROUP BY team
                 ORDER BY team"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("a".into()),
                SqlValue::Int(50),
                SqlValue::Int(2),
                SqlValue::Int(70)
            ],
            vec![
                SqlValue::String("b".into()),
                SqlValue::Int(20),
                SqlValue::Int(1),
                SqlValue::Int(20)
            ],
        ]
    );

    assert_eq!(
        session
            .execute(
                "SELECT SUM(score),
                        ROW_NUMBER() OVER (),
                        SUM(SUM(score)) OVER ()
                 FROM window_rows"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(70), SqlValue::Int(1), SqlValue::Int(70),]]
    );

    assert_eq!(
        session
            .execute(
                "SELECT team,
                        SUM(score),
                        RANK() OVER (ORDER BY SUM(score))
                 FROM window_rows
                 GROUP BY team
                 HAVING SUM(score) > 20"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("a".into()),
            SqlValue::Int(50),
            SqlValue::Int(1),
        ]]
    );
}

#[test]
fn order_by_honors_secondary_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE ordered_rows (id TEXT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    session
        .execute("INSERT INTO ordered_rows VALUES ('x', 1, 2), ('y', 1, 1), ('z', 2, 0)")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT id FROM ordered_rows ORDER BY a, b DESC")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("x".into())],
            vec![SqlValue::String("y".into())],
            vec![SqlValue::String("z".into())],
        ]
    );
    assert_eq!(
        session
            .execute("SELECT id, a + 0 FROM ordered_rows ORDER BY a, b DESC")
            .unwrap()
            .rows
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![
            SqlValue::String("x".into()),
            SqlValue::String("y".into()),
            SqlValue::String("z".into()),
        ]
    );
    assert_eq!(
        session
            .execute("SELECT a, b, COUNT(*) FROM ordered_rows GROUP BY a, b ORDER BY a, b DESC")
            .unwrap()
            .rows
            .into_iter()
            .map(|row| (row[0].clone(), row[1].clone()))
            .collect::<Vec<_>>(),
        vec![
            (SqlValue::Int(1), SqlValue::Int(2)),
            (SqlValue::Int(1), SqlValue::Int(1)),
            (SqlValue::Int(2), SqlValue::Int(0)),
        ]
    );
}

#[test]
fn procedural_ddl_is_metadata_only_and_catalog_visible() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE audit_patients (id TEXT PRIMARY KEY, name TEXT)")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "CREATE FUNCTION audit_touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'",
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE FUNCTION"
    );
    assert_eq!(
        session
            .execute("CREATE PROCEDURE audit_proc() LANGUAGE sql AS 'SELECT 1'")
            .unwrap()
            .command_complete_tag(),
        "CREATE PROCEDURE"
    );
    assert_eq!(
        session
            .execute(
                "CREATE TRIGGER audit_patients_touch AFTER INSERT ON audit_patients FOR EACH ROW EXECUTE FUNCTION audit_touch()"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE TRIGGER"
    );
    assert_eq!(
        session
            .execute(
                "CREATE FUNCTION audit_delete_override() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE FUNCTION"
    );
    assert_eq!(
        session
            .execute(
                "CREATE TRIGGER audit_patients_loose_fk AFTER DELETE ON audit_patients REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION audit_delete_override('audit_patients')"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE TRIGGER"
    );

    assert_eq!(
        session
            .execute(
                "SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'audit_proc' OR proname = 'audit_touch' ORDER BY proname"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("audit_proc".to_string()),
                SqlValue::String("p".to_string())
            ],
            vec![
                SqlValue::String("audit_touch".to_string()),
                SqlValue::String("f".to_string())
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT tgname FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("audit_patients_touch".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT tgname FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_loose_fk'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "audit_patients_loose_fk".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT relhastriggers FROM pg_catalog.pg_class WHERE relname = 'audit_patients'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("O".to_string())]]
    );
    assert_eq!(
        session
            .execute("ALTER TABLE audit_patients DISABLE TRIGGER audit_patients_touch")
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );
    assert_eq!(
        session
            .execute(
                "SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("D".to_string())]]
    );
    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE audit_patients ENABLE REPLICA TRIGGER audit_patients_touch")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("D".to_string())]]
    );
    session
        .execute("ALTER TABLE audit_patients ENABLE ALWAYS TRIGGER audit_patients_touch")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("A".to_string())]]
    );
    session
        .execute("ALTER TABLE audit_patients ENABLE TRIGGER audit_patients_touch")
        .unwrap();
    session
        .execute("ALTER TABLE audit_patients DISABLE TRIGGER ALL")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname LIKE 'audit_patients_%' ORDER BY tgname")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("D".to_string())],
            vec![SqlValue::String("D".to_string())],
        ]
    );
    session
        .execute("ALTER TABLE audit_patients ENABLE TRIGGER USER")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT tgenabled FROM pg_catalog.pg_trigger WHERE tgname LIKE 'audit_patients_%' ORDER BY tgname")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("O".to_string())],
            vec![SqlValue::String("O".to_string())],
        ]
    );

    assert_eq!(
        session
            .execute("CALL audit_proc()")
            .unwrap()
            .command_complete_tag(),
        "CALL"
    );
    let function_error = session.execute("SELECT audit_touch()").unwrap_err();
    assert_eq!(function_error.sqlstate(), "0A000");

    session
        .execute("DROP TRIGGER audit_patients_touch ON audit_patients")
        .unwrap();
    session.execute("DROP FUNCTION audit_touch()").unwrap();
    session.execute("DROP PROCEDURE audit_proc()").unwrap();
    assert!(session
        .execute("SELECT tgname FROM pg_catalog.pg_trigger WHERE tgname = 'audit_patients_touch'")
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute(
            "SELECT proname FROM pg_catalog.pg_proc WHERE proname = 'audit_proc' OR proname = 'audit_touch'"
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn row_triggers_bind_postgresql_metadata_and_arguments() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE trigger_metadata_target (id TEXT PRIMARY KEY, version INT)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE trigger_metadata_observations (
                trigger_name TEXT,
                trigger_when TEXT,
                trigger_level TEXT,
                trigger_op TEXT,
                table_schema TEXT,
                table_name TEXT,
                relation_name TEXT,
                relation_oid BIGINT,
                argument_count INT,
                first_argument TEXT,
                second_argument TEXT,
                qualified_table TEXT,
                declared_argument TEXT
            )",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION observe_trigger_metadata()
             RETURNS trigger
             LANGUAGE plpgsql
             AS $$
             DECLARE
               qualified_table TEXT := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
               declared_argument TEXT := TG_ARGV[0];
             BEGIN
               INSERT INTO trigger_metadata_observations (
                 trigger_name, trigger_when, trigger_level, trigger_op,
                 table_schema, table_name, relation_name, relation_oid,
                 argument_count, first_argument, second_argument,
                 qualified_table, declared_argument
               ) VALUES (
                 TG_NAME, TG_WHEN, TG_LEVEL, TG_OP,
                 TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELNAME, TG_RELID,
                 TG_NARGS, TG_ARGV[0], TG_ARGV[1],
                 qualified_table, declared_argument
               );
               RETURN NEW;
             END
             $$",
        )
        .unwrap();
    session
        .execute(
            "CREATE TRIGGER trigger_metadata_before_insert
             BEFORE INSERT ON trigger_metadata_target
             FOR EACH ROW
             EXECUTE FUNCTION observe_trigger_metadata('id', 'version')",
        )
        .unwrap();

    session
        .execute("INSERT INTO trigger_metadata_target (id, version) VALUES ('row-1', 1)")
        .unwrap();
    let row = session
        .execute(
            "SELECT trigger_name, trigger_when, trigger_level, trigger_op,
                    table_schema, table_name, relation_name,
                    relation_oid > 0, argument_count,
                    first_argument, second_argument,
                    qualified_table, declared_argument
             FROM trigger_metadata_observations",
        )
        .unwrap();
    assert_eq!(
        row.rows,
        vec![vec![
            SqlValue::String("trigger_metadata_before_insert".to_string()),
            SqlValue::String("BEFORE".to_string()),
            SqlValue::String("ROW".to_string()),
            SqlValue::String("INSERT".to_string()),
            SqlValue::String("public".to_string()),
            SqlValue::String("trigger_metadata_target".to_string()),
            SqlValue::String("trigger_metadata_target".to_string()),
            SqlValue::Bool(true),
            SqlValue::Int(2),
            SqlValue::String("id".to_string()),
            SqlValue::String("version".to_string()),
            SqlValue::String("public.trigger_metadata_target".to_string()),
            SqlValue::String("id".to_string()),
        ]]
    );
}

#[test]
fn alter_function_reset_all_validates_function_and_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE FUNCTION trigger_7840c345e48f() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'",
        )
        .unwrap();
    assert_eq!(
        session
            .execute("ALTER FUNCTION \"trigger_7840c345e48f\" RESET ALL")
            .unwrap()
            .command_complete_tag(),
        "ALTER FUNCTION"
    );

    let error = session
        .execute("ALTER FUNCTION missing_trigger_fn RESET ALL")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42601");
    assert_eq!(
        error.to_string(),
        "invalid SQL: function \"missing_trigger_fn\" does not exist"
    );
}

#[test]
fn plpgsql_functions_are_metadata_only_and_other_languages_return_stable_sqlstate() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "CREATE FUNCTION pl_fn() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END'"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE FUNCTION"
    );
    assert_eq!(
        session
            .execute("SELECT proname, prokind FROM pg_catalog.pg_proc WHERE proname = 'pl_fn'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("pl_fn".to_string()),
            SqlValue::String("f".to_string())
        ]]
    );

    let error = session
        .execute("CREATE FUNCTION py_fn() RETURNS int LANGUAGE plpythonu AS 'return 1'")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(
        error.to_string(),
        "unsupported SQL: procedural language plpythonu is not supported"
    );
}

#[test]
fn postgres_create_function_returns_table_is_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                r#"
                CREATE FUNCTION postgres_pg_stat_activity_autovacuum()
                RETURNS TABLE(query text, query_start timestamp with time zone)
                    LANGUAGE sql SECURITY DEFINER
                    SET search_path TO 'pg_catalog', 'pg_temp'
                    AS $$
                      SELECT query, query_start
                      FROM pg_stat_activity
                    $$;
                "#,
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE FUNCTION"
    );
    assert_eq!(
        session
            .execute(
                "SELECT proname, prorettype FROM pg_catalog.pg_proc WHERE proname = 'postgres_pg_stat_activity_autovacuum'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("postgres_pg_stat_activity_autovacuum".to_string()),
            SqlValue::Int(2249)
        ]]
    );
}

#[test]
fn sql_returns_table_function_executes_as_a_typed_table_factor() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE routine_doctors (
                id TEXT PRIMARY KEY,
                clinic TEXT NOT NULL,
                city TEXT NOT NULL
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO routine_doctors (id, clinic, city) VALUES
                ('1', 'alpha', 'San Francisco'),
                ('2', 'alpha', 'San Francisco'),
                ('3', 'alpha', 'Los Angeles')",
        )
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION routine_doctor_report(text)
             RETURNS TABLE(city text, total bigint)
             LANGUAGE SQL STABLE AS
             'SELECT city, count(*)::bigint AS total
              FROM routine_doctors
              WHERE clinic = $1
              GROUP BY city
              ORDER BY city'",
        )
        .unwrap();

    let result = session
        .execute("SELECT city, total FROM routine_doctor_report('alpha')")
        .unwrap();
    assert_eq!(result.columns, vec!["city", "total"]);
    assert_eq!(
        result.rows,
        vec![
            vec![
                SqlValue::String("Los Angeles".to_string()),
                SqlValue::Int(1)
            ],
            vec![
                SqlValue::String("San Francisco".to_string()),
                SqlValue::Int(2)
            ],
        ]
    );
}

#[test]
fn select_exists_catalog_subquery_without_from_matches_mastodon_function_probe() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute("SELECT EXISTS (SELECT * FROM pg_proc WHERE proname = 'timestamp_id')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]]
    );

    session
        .execute(
            "CREATE FUNCTION timestamp_id(table_name text)
             RETURNS bigint
             LANGUAGE plpgsql
             AS 'BEGIN RETURN 1; END'",
        )
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT EXISTS (SELECT * FROM pg_proc WHERE proname = 'timestamp_id')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}

#[test]
fn postgres_create_function_returns_setof_is_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                r#"
                CREATE OR REPLACE FUNCTION hammerdb_ostat(INTEGER, INTEGER)
                RETURNS SETOF record AS '
                BEGIN
                    RETURN;
                END;
                ' LANGUAGE 'plpgsql';
                "#,
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE FUNCTION"
    );
    assert_eq!(
        session
            .execute(
                "SELECT proname, proretset, prorettype, pg_catalog.pg_get_function_result(oid) \
                 FROM pg_catalog.pg_proc WHERE proname = 'hammerdb_ostat'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("hammerdb_ostat".to_string()),
            SqlValue::Bool(true),
            SqlValue::Int(2249),
            SqlValue::String("SETOF record".to_string())
        ]]
    );
}

#[test]
fn sql_setof_function_keeps_columns_when_outer_relation_is_empty() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE SCHEMA carrier_private;
            CREATE TABLE setof_search_rows (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL
            );
            CREATE FUNCTION carrier_private.find_setof_search_ids(search_query TEXT)
            RETURNS SETOF TEXT
            LANGUAGE SQL
            AS $$
                SELECT id::TEXT
                FROM setof_search_rows
                WHERE label = search_query
            $$;",
        )
        .unwrap();

    assert!(session
        .execute(
            "SELECT *
             FROM setof_search_rows
             WHERE id IN (SELECT * FROM carrier_private.find_setof_search_ids('match'))"
        )
        .unwrap()
        .rows
        .is_empty());

    session
        .execute("INSERT INTO setof_search_rows VALUES ('one', 'match'), ('two', 'other')")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT id
                 FROM setof_search_rows
                 WHERE id IN (SELECT * FROM carrier_private.find_setof_search_ids('match'))"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("one".to_string())]]
    );
}

#[test]
fn postgres_comment_on_function_signature_is_metadata_only() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "COMMENT ON FUNCTION table_sync_function_3f39f64fc3() IS 'Partitioning migration: table sync'"
            )
            .unwrap()
            .command_complete_tag(),
        "COMMENT"
    );
}

#[test]
fn postgres_network_address_types_are_string_compatible() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE audit_events (id bigint PRIMARY KEY, ip_address inet)")
        .unwrap();
    session
        .execute("INSERT INTO audit_events (id, ip_address) VALUES (1, '192.168.0.1/24')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT ip_address FROM audit_events WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("192.168.0.1/24".to_string())]]
    );
    assert_eq!(
        session
            .execute("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'inet'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(869),
            SqlValue::String("inet".to_string())
        ]]
    );
}

#[test]
fn postgres_discrete_ranges_canonicalize_values_and_errors() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE vulnerability_archive_exports (id bigint PRIMARY KEY, date_range daterange NOT NULL)")
        .unwrap();
    session
        .execute("INSERT INTO vulnerability_archive_exports (id, date_range) VALUES (1, '[2026-01-01,2026-01-31]')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT date_range FROM vulnerability_archive_exports WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "[2026-01-01,2026-02-01)".to_string()
        )]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '[1,5]'::int4range, '(1,5)'::int4range, \
                 '(1,1]'::int4range, \
                 '[9007199254740993,9007199254741000]'::int8range",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("[1,6)".to_string()),
            SqlValue::String("[2,5)".to_string()),
            SqlValue::String("empty".to_string()),
            SqlValue::String("[9007199254740993,9007199254741001)".to_string()),
        ]]
    );
    let reversed = session.execute("SELECT '[2,1)'::int4range").unwrap_err();
    assert_eq!(reversed.sqlstate(), "22000");
    let overflow = session
        .execute("SELECT '(2147483647,)'::int4range")
        .unwrap_err();
    assert_eq!(overflow.sqlstate(), "22003");
    assert_eq!(
        session
            .execute("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'daterange'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3912),
            SqlValue::String("daterange".to_string())
        ]]
    );
}

#[test]
fn postgres_range_functions_operators_and_ordering_are_typed() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT int4range(1, 5), int4range(1, 5, '[]'), \
                 int4range(NULL, 5), int4range(1, NULL), int4range(NULL, NULL)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("[1,5)".to_string()),
            SqlValue::String("[1,6)".to_string()),
            SqlValue::String("(,5)".to_string()),
            SqlValue::String("[1,)".to_string()),
            SqlValue::String("(,)".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT isempty('empty'::int4range), lower('[1,5)'::int4range), \
                 upper('[1,5)'::int4range), lower_inc('[1,5)'::int4range), \
                 upper_inc('[1,5)'::int4range), lower_inf('(,5)'::int4range), \
                 upper_inf('[1,)'::int4range), lower('empty'::int4range)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Int(1),
            SqlValue::Int(5),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '[1,5)'::int4range @> '[2,4)'::int4range, \
                 '[1,5)'::int4range @> 2, 2 <@ '[1,5)'::int4range, \
                 '[1,5)'::int4range && '[4,8)'::int4range, \
                 '[1,5)'::int4range -|- '[5,8)'::int4range, \
                 '[1,5)'::int4range << '[5,8)'::int4range, \
                 '[5,8)'::int4range >> '[1,5)'::int4range, \
                 '[1,8)'::int4range &< '[5,10)'::int4range, \
                 '[5,10)'::int4range &> '[1,8)'::int4range",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true); 9]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT '[1,5)'::int4range + '[5,8)'::int4range, \
                 '[1,5)'::int4range * '[4,8)'::int4range, \
                 '[1,8)'::int4range - '[1,3)'::int4range, \
                 range_merge('[1,5)'::int4range, '[8,10)'::int4range)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("[1,8)".to_string()),
            SqlValue::String("[4,5)".to_string()),
            SqlValue::String("[3,8)".to_string()),
            SqlValue::String("[1,10)".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT x FROM (VALUES ('empty'::int4range), ('(,)'::int4range), \
                 ('[1,2)'::int4range), ('[1,3)'::int4range), \
                 ('[1,3]'::int4range)) AS ranges(x) ORDER BY x",
            )
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::String("empty".to_string())],
            vec![SqlValue::String("(,)".to_string())],
            vec![SqlValue::String("[1,2)".to_string())],
            vec![SqlValue::String("[1,3)".to_string())],
            vec![SqlValue::String("[1,4)".to_string())],
        ]
    );

    session
        .execute("CREATE TABLE range_rows (id integer PRIMARY KEY, span int4range NOT NULL)")
        .unwrap();
    session
        .execute("INSERT INTO range_rows VALUES (1, '[2,7)')")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT lower(span), upper(span), span @> 4 FROM range_rows")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::Int(7),
            SqlValue::Bool(true)
        ]]
    );

    let union_error = session
        .execute("SELECT '[1,5)'::int4range + '[8,10)'::int4range")
        .unwrap_err();
    assert_eq!(union_error.sqlstate(), "22000");
    let difference_error = session
        .execute("SELECT '[1,8)'::int4range - '[3,5)'::int4range")
        .unwrap_err();
    assert_eq!(difference_error.sqlstate(), "22000");
}

#[test]
fn postgres_multirange_constructors_canonicalize_all_builtin_families() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("SET TIME ZONE 'UTC'").unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT int4multirange(),
                        int4multirange('[8,10)'::int4range, '[1,5)'::int4range,
                                       '[4,9)'::int4range, 'empty'::int4range),
                        int8multirange('[9007199254740993,9007199254741000]'::int8range),
                        nummultirange('[1.00,2.00)'::numrange, '(2.00,3.00)'::numrange)",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("{}".to_string()),
            SqlValue::String("{[1,10)}".to_string()),
            SqlValue::String("{[9007199254740993,9007199254741001)}".to_string()),
            SqlValue::String("{[1.00,2.00),(2.00,3.00)}".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT datemultirange('[2024-01-01,2024-01-03]'::daterange,
                                        '[2024-01-04,2024-01-05)'::daterange),
                        tsmultirange('[2024-01-01 00:00:00,2024-01-02)'::tsrange),
                        tstzmultirange('[2024-01-01 00:00:00+00,2024-01-02 00:00:00+00)'::tstzrange),
                        '{[8,10),[1,5),[4,9),empty}'::int4multirange",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("{[2024-01-01,2024-01-05)}".to_string()),
            SqlValue::String(
                "{[\"2024-01-01 00:00:00\",\"2024-01-02 00:00:00\")}".to_string(),
            ),
            SqlValue::String(
                "{[\"2024-01-01 00:00:00+00\",\"2024-01-02 00:00:00+00\")}".to_string(),
            ),
            SqlValue::String("{[1,10)}".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT int4multirange(NULL::int4range),
                        pg_typeof(int4multirange()), pg_typeof(nummultirange())",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Null,
            SqlValue::String("int4multirange".to_string()),
            SqlValue::String("nummultirange".to_string()),
        ]]
    );
    let null_member = session
        .execute("SELECT int4multirange('[1,2)'::int4range, NULL::int4range)")
        .unwrap_err();
    assert_eq!(null_member.sqlstate(), "22004");

    session
        .execute(
            "CREATE TABLE multirange_rows (
                id TEXT PRIMARY KEY,
                spans INT4MULTIRANGE NOT NULL UNIQUE
            )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO multirange_rows VALUES
             ('canonical', '{[8,10),[1,5),[4,9),empty}')",
        )
        .unwrap();
    let duplicate = session
        .execute("INSERT INTO multirange_rows VALUES ('duplicate', '{[1,10)}')")
        .unwrap_err();
    assert_eq!(duplicate.sqlstate(), "23505");
    drop(session);
    db.close().unwrap();
    let mut reopened = BicDb::open(dir.path()).unwrap();
    assert_eq!(
        SqlSession::new(&mut reopened)
            .execute("SELECT spans FROM multirange_rows")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("{[1,10)}".to_string())]]
    );
}

#[test]
fn postgres_range_multirange_cross_operators_and_aggregates_are_typed() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    assert_eq!(
        session
            .execute(
                "SELECT
                   '{[1,3),[8,10)}'::int4multirange + '{[2,5),[9,12)}'::int4multirange,
                   '{[1,3),[8,10)}'::int4multirange * '{[2,5),[9,12)}'::int4multirange,
                   '{[1,3),[8,10)}'::int4multirange - '{[2,5),[9,12)}'::int4multirange,
                   '{[1,10)}'::int4multirange - '{[3,5),[7,8)}'::int4multirange",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("{[1,5),[8,12)}".to_string()),
            SqlValue::String("{[2,3),[9,10)}".to_string()),
            SqlValue::String("{[1,2),[8,9)}".to_string()),
            SqlValue::String("{[1,3),[5,7),[8,10)}".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT
                   '{[1,3),[8,10)}'::int4multirange @> '[1,2)'::int4range,
                   '[1,10)'::int4range @> '{[1,3),[8,10)}'::int4multirange,
                   '{[1,3),[8,10)}'::int4multirange <@ '[1,10)'::int4range,
                   '[3,8)'::int4range && '{[1,3),[8,10)}'::int4multirange,
                   '[3,8)'::int4range -|- '{[1,3),[8,10)}'::int4multirange,
                   '{[1,3),[8,10)}'::int4multirange @> 9,
                   9 <@ '{[1,3),[8,10)}'::int4multirange",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT
                   '{[1,3),[8,10)}'::int4multirange << '[10,12)'::int4range,
                   '[10,12)'::int4range >> '{[1,3),[8,10)}'::int4multirange,
                   '{[1,3),[8,10)}'::int4multirange &< '{[2,5),[9,12)}'::int4multirange,
                   '{[2,5),[9,12)}'::int4multirange &> '[2,4)'::int4range,
                   '{}'::int4multirange << '[1,2)'::int4range,
                   '{}'::int4multirange @> 'empty'::int4range,
                   '{[1,3)}'::int4multirange < '{[1,4)}'::int4multirange",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(true),
        ]]
    );
    for sql in [
        "SELECT '[1,3)'::int4range + '{[3,5)}'::int4multirange",
        "SELECT '{[1,3)}'::int4multirange @> '[1,2)'::int8range",
        "SELECT '{[1,3)}'::int4multirange = '[1,3)'::int4range",
        "SELECT '{[1,3)}'::int4multirange && '{[1,3)}'::int8multirange",
    ] {
        assert_eq!(session.execute(sql).unwrap_err().sqlstate(), "42883");
    }

    session
        .execute(
            "CREATE TABLE range_set_rows (
               id INT PRIMARY KEY,
               bucket TEXT NOT NULL,
               span INT4RANGE,
               spans INT4MULTIRANGE
             )",
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO range_set_rows VALUES
             (1, 'a', '[1,3)', '{[1,3),[8,10)}'),
             (2, 'a', '[2,5)', '{[2,5),[9,12)}'),
             (3, 'b', NULL, NULL)",
        )
        .unwrap();
    let aggregates = session
        .execute(
            "SELECT range_agg(span), range_intersect_agg(span),
                    range_agg(spans), range_intersect_agg(spans),
                    pg_typeof(range_agg(span)),
                    pg_typeof(range_intersect_agg(span))
             FROM range_set_rows",
        )
        .unwrap();
    assert_eq!(
        aggregates.rows,
        vec![vec![
            SqlValue::String("{[1,5)}".to_string()),
            SqlValue::String("[2,3)".to_string()),
            SqlValue::String("{[1,5),[8,12)}".to_string()),
            SqlValue::String("{[2,3),[9,10)}".to_string()),
            SqlValue::String("int4multirange".to_string()),
            SqlValue::String("int4range".to_string()),
        ]]
    );
    assert_eq!(
        aggregates.column_types,
        vec![
            Some("int4multirange".to_string()),
            Some("int4range".to_string()),
            Some("int4multirange".to_string()),
            Some("int4multirange".to_string()),
            Some("regtype".to_string()),
            Some("regtype".to_string()),
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT range_agg(span), range_intersect_agg(span),
                        range_agg(spans), range_intersect_agg(spans)
                 FROM range_set_rows WHERE id < 0",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT bucket, range_agg(span), range_intersect_agg(span)
                 FROM range_set_rows GROUP BY bucket ORDER BY bucket",
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("a".to_string()),
                SqlValue::String("{[1,5)}".to_string()),
                SqlValue::String("[2,3)".to_string()),
            ],
            vec![
                SqlValue::String("b".to_string()),
                SqlValue::Null,
                SqlValue::Null,
            ],
        ]
    );
}

#[test]
fn postgres_full_text_search_types_are_string_compatible() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE TABLE p_ci_build_names (
                build_id bigint NOT NULL,
                partition_id bigint NOT NULL,
                project_id bigint NOT NULL,
                name text NOT NULL,
                search_vector tsvector GENERATED ALWAYS AS (
                    to_tsvector('english'::regconfig, COALESCE(name, ''::text))
                ) STORED,
                CONSTRAINT check_1722c96346 CHECK ((char_length(name) <= 255))
            ) PARTITION BY LIST (partition_id)
            "#,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO p_ci_build_names (build_id, partition_id, project_id, name) \
             VALUES (1, 101, 5, 'build')",
        )
        .unwrap();

    assert_eq!(
        session.execute("SELECT 'english'::regconfig").unwrap().rows,
        vec![vec![SqlValue::Int(810_102)]]
    );
    assert_eq!(
        session
            .execute("SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'tsvector'")
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::Int(3614),
            SqlValue::String("tsvector".to_string())
        ]]
    );
}

#[test]
fn postgres_trim_both_from_checks_are_parse_compatible() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE TABLE approval_policy_merge_request_bypass_events (
                id bigint PRIMARY KEY,
                reason text NOT NULL,
                CONSTRAINT check_3169f0d109 CHECK (
                    ((length(TRIM(BOTH FROM reason)) >= 1)
                    AND (length(TRIM(BOTH FROM reason)) <= 1024))
                )
            )
            "#,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO approval_policy_merge_request_bypass_events (id, reason) \
             VALUES (1, ' approved ')",
        )
        .unwrap();

    let error = session
        .execute(
            "INSERT INTO approval_policy_merge_request_bypass_events (id, reason) \
             VALUES (2, '   ')",
        )
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23514");
}

#[test]
fn postgres_keyless_partitioned_tables_use_hidden_internal_row_id() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            r#"
            CREATE TABLE backup_finding_evidences (
                created_at timestamp with time zone NOT NULL,
                updated_at timestamp with time zone NOT NULL,
                original_record_identifier bigint NOT NULL,
                finding_id bigint NOT NULL,
                project_id bigint NOT NULL,
                date date NOT NULL,
                data jsonb NOT NULL
            ) PARTITION BY RANGE (date)
            "#,
        )
        .unwrap();
    session
        .execute(
            r#"
            INSERT INTO backup_finding_evidences (
                created_at,
                updated_at,
                original_record_identifier,
                finding_id,
                project_id,
                date,
                data
            ) VALUES (
                '2026-06-21 00:00:00+00',
                '2026-06-21 00:00:00+00',
                42,
                7,
                3,
                '2026-06-21',
                '{"state":"ok"}'
            )
            "#,
        )
        .unwrap();

    let result = session
        .execute("SELECT * FROM backup_finding_evidences")
        .unwrap();
    assert_eq!(
        result.columns,
        [
            "created_at",
            "updated_at",
            "original_record_identifier",
            "finding_id",
            "project_id",
            "date",
            "data"
        ]
    );
    assert_eq!(result.rows.len(), 1);

    assert!(session
        .execute(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'backup_finding_evidences' \
             AND column_name = '__bicdb_rowid'"
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute(
            "SELECT attname FROM pg_catalog.pg_attribute \
             WHERE attname = '__bicdb_rowid'"
        )
        .unwrap()
        .rows
        .is_empty());
    assert!(session
        .execute(
            "SELECT constraint_name FROM information_schema.table_constraints \
             WHERE table_name = 'backup_finding_evidences' \
             AND constraint_type = 'PRIMARY KEY'"
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn postgres_attach_partition_is_metadata_only() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_static")
        .unwrap();
    session
        .execute(
            "CREATE TABLE analytics_cycle_analytics_issue_stage_events \
             (id bigint PRIMARY KEY, partition_id bigint NOT NULL) \
             PARTITION BY HASH (partition_id)",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE gitlab_partitions_static.analytics_cycle_analytics_issue_stage_events_00 \
             (id bigint PRIMARY KEY, partition_id bigint NOT NULL)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "ALTER TABLE ONLY analytics_cycle_analytics_issue_stage_events \
                 ATTACH PARTITION gitlab_partitions_static.analytics_cycle_analytics_issue_stage_events_00 \
                 FOR VALUES WITH (modulus 32, remainder 0)"
            )
            .unwrap()
            .command_complete_tag(),
        "ALTER TABLE"
    );
    assert_eq!(
        session
            .execute(
                "SELECT c.relname, c.relkind, c.relispartition, p.partstrat, p.partattrs
                 FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_partitioned_table p ON p.partrelid = c.oid
                 WHERE c.relname = 'analytics_cycle_analytics_issue_stage_events'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("analytics_cycle_analytics_issue_stage_events".to_string()),
            SqlValue::String("p".to_string()),
            SqlValue::Bool(false),
            SqlValue::String("h".to_string()),
            SqlValue::String("2".to_string()),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT child.relname, child.relispartition, child_ns.nspname, parent.relname, pg_get_expr(child.relpartbound, child.oid)
                 FROM pg_catalog.pg_inherits i
                 JOIN pg_catalog.pg_class child ON child.oid = i.inhrelid
                 JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
                 JOIN pg_catalog.pg_class parent ON parent.oid = i.inhparent
                 WHERE parent.relname = 'analytics_cycle_analytics_issue_stage_events'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("analytics_cycle_analytics_issue_stage_events_00".to_string()),
            SqlValue::Bool(true),
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String("analytics_cycle_analytics_issue_stage_events".to_string()),
            SqlValue::String("FOR VALUES WITH (modulus 32, remainder 0)".to_string()),
        ]]
    );

    assert_eq!(
        session
            .execute(
                "CREATE INDEX index_issue_stage_events_project_duration
                 ON ONLY analytics_cycle_analytics_issue_stage_events
                 USING btree (
                   stage_event_hash_id,
                   project_id,
                   end_event_timestamp,
                   issue_id,
                   start_event_timestamp
                 )
                 WHERE (end_event_timestamp IS NOT NULL)"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE INDEX"
    );
    assert_eq!(
        session
            .execute(
                "SELECT relname
                 FROM pg_catalog.pg_class
                 WHERE relname = 'index_issue_stage_events_project_duration'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "index_issue_stage_events_project_duration".to_string()
        )]]
    );
    let index_metadata = session
        .execute(
            "SELECT indexprs
             FROM pg_catalog.pg_index
             WHERE indexprs IS NOT NULL",
        )
        .unwrap();
    assert!(index_metadata.rows.iter().any(|row| {
        row[0].to_cell().contains("stage_event_hash_id")
            && row[0].to_cell().contains("end_event_timestamp IS NOT NULL")
    }));

    assert_eq!(
        session
            .execute(
                "ALTER INDEX analytics_cycle_analytics_issue_stage_events_pkey
                 ATTACH PARTITION gitlab_partitions_static.analytics_cycle_analytics_issue_stage_events_00_pkey"
            )
            .unwrap()
            .command_complete_tag(),
        "ALTER INDEX"
    );
}

#[test]
fn postgres_pg_get_partkeydef_renders_partition_key_definition() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE ai_audit_events (
                id bigint PRIMARY KEY,
                created_at timestamp with time zone NOT NULL
             ) PARTITION BY RANGE (created_at)",
        )
        .unwrap();

    let oid = match session
        .execute("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'ai_audit_events'")
        .unwrap()
        .rows[0][0]
    {
        SqlValue::Int(oid) => oid,
        ref other => panic!("expected relation oid, got {other:?}"),
    };

    assert_eq!(
        session
            .execute(&format!("SELECT pg_catalog.pg_get_partkeydef('{oid}')"))
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("RANGE (created_at)".to_string())]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT pg_catalog.pg_get_partkeydef(c.oid)
                 FROM pg_catalog.pg_class c
                 WHERE c.relname = 'ai_audit_events'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("RANGE (created_at)".to_string())]]
    );
}

#[test]
fn postgres_catalogs_report_schema_qualified_partition_indexes() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_static")
        .unwrap();
    session
        .execute(
            "CREATE TABLE gitlab_partitions_static.virtual_registries_container_cache_remote_entries_00 (
                id bigint PRIMARY KEY,
                digest text
            )",
        )
        .unwrap();
    // The original GitLab trace uses CREATE INDEX CONCURRENTLY; BicDB now
    // REJECTS that keyword instead of silently running a blocking build
    // (docs/create-index-concurrently-design.md), so the catalog fixture
    // uses the plain form.
    session
        .execute(
            "CREATE INDEX index_6d81e95e7a
             ON gitlab_partitions_static.virtual_registries_container_cache_remote_entries_00 (digest)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT schemaname, tablename, indexname, indexdef
                 FROM pg_indexes
                 WHERE schemaname = 'gitlab_partitions_static'
                   AND tablename = 'virtual_registries_container_cache_remote_entries_00'
                   AND indexname = 'index_6d81e95e7a'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String(
                "virtual_registries_container_cache_remote_entries_00".to_string()
            ),
            SqlValue::String("index_6d81e95e7a".to_string()),
            SqlValue::String(
                "CREATE INDEX index_6d81e95e7a ON gitlab_partitions_static.virtual_registries_container_cache_remote_entries_00 USING btree (digest)"
                    .to_string()
            ),
        ]]
    );
    assert_eq!(
        session
            .execute(
                "SELECT n.nspname, c.relname
                 FROM pg_catalog.pg_class c
                 INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'gitlab_partitions_static'
                   AND c.relname IN (
                     'virtual_registries_container_cache_remote_entries_00_pkey',
                     'index_6d81e95e7a'
                   )
                 ORDER BY c.relname"
            )
            .unwrap()
            .rows,
        vec![
            vec![
                SqlValue::String("gitlab_partitions_static".to_string()),
                SqlValue::String("index_6d81e95e7a".to_string()),
            ],
            vec![
                SqlValue::String("gitlab_partitions_static".to_string()),
                SqlValue::String(
                    "virtual_registries_container_cache_remote_entries_00_pkey".to_string()
                ),
            ],
        ]
    );
    assert_eq!(
        session
            .execute(
                "SELECT (((pg_namespace.nspname)::text || '.'::text) || (i.relname)::text) AS identifier,
                        pg_namespace.nspname AS schema,
                        i.relname AS name,
                        pg_indexes.tablename,
                        pg_index.indisvalid AS valid_index,
                        pg_indexes.indexdef AS definition
                 FROM pg_index
                 JOIN pg_class i ON i.oid = pg_index.indexrelid
                 JOIN pg_namespace ON i.relnamespace = pg_namespace.oid
                 JOIN pg_indexes ON i.relname = pg_indexes.indexname
                                AND pg_namespace.nspname = pg_indexes.schemaname
                 WHERE pg_namespace.nspname = 'gitlab_partitions_static'
                   AND i.relname = 'index_6d81e95e7a'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("gitlab_partitions_static.index_6d81e95e7a".to_string()),
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String("index_6d81e95e7a".to_string()),
            SqlValue::String(
                "virtual_registries_container_cache_remote_entries_00".to_string()
            ),
            SqlValue::Bool(true),
            SqlValue::String(
                "CREATE INDEX index_6d81e95e7a ON gitlab_partitions_static.virtual_registries_container_cache_remote_entries_00 USING btree (digest)"
                    .to_string()
            ),
        ]]
    );

    session
        .execute(
            r#"
            CREATE VIEW postgres_indexes AS
             SELECT (((pg_namespace.nspname)::text || '.'::text) || (i.relname)::text) AS identifier,
                pg_index.indexrelid,
                pg_namespace.nspname AS schema,
                i.relname AS name,
                pg_indexes.tablename,
                a.amname AS type,
                pg_index.indisunique AS "unique",
                pg_index.indisvalid AS valid_index,
                i.relispartition AS partitioned,
                pg_index.indisexclusion AS exclusion,
                (pg_index.indexprs IS NOT NULL) AS expression,
                (pg_index.indpred IS NOT NULL) AS partial,
                pg_indexes.indexdef AS definition,
                pg_relation_size((i.oid)::regclass) AS ondisk_size_bytes
               FROM ((((pg_index
                 JOIN pg_class i ON ((i.oid = pg_index.indexrelid)))
                 JOIN pg_namespace ON ((i.relnamespace = pg_namespace.oid)))
                 JOIN pg_indexes ON (((i.relname = pg_indexes.indexname) AND (pg_namespace.nspname = pg_indexes.schemaname))))
                 JOIN pg_am a ON ((i.relam = a.oid)))
              WHERE ((pg_namespace.nspname <> 'pg_catalog'::name) AND (pg_namespace.nspname = ANY (ARRAY["current_schema"(), 'gitlab_partitions_dynamic'::name, 'gitlab_partitions_static'::name])))
            "#,
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT identifier, schema, name, tablename, valid_index
                 FROM postgres_indexes
                 WHERE schema = 'gitlab_partitions_static'
                   AND name = 'index_6d81e95e7a'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("gitlab_partitions_static.index_6d81e95e7a".to_string()),
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String("index_6d81e95e7a".to_string()),
            SqlValue::String("virtual_registries_container_cache_remote_entries_00".to_string()),
            SqlValue::Bool(true),
        ]]
    );
}

#[test]
fn drop_metadata_only_index_updates_catalog_and_rolls_back() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute(
            "CREATE TABLE virtual_registries_container_cache_entries (
                id bigint PRIMARY KEY,
                relative_path text,
                object_storage_key text
            ) PARTITION BY HASH (id)",
        )
        .unwrap();
    session
        .execute(
            "CREATE INDEX idx_vregs_container_cache_entries_on_uniq_object_storage_key
             ON ONLY virtual_registries_container_cache_entries
             USING btree (relative_path, object_storage_key)",
        )
        .unwrap();

    let exists_sql = "SELECT 1 AS one FROM pg_indexes \
         WHERE tablename = 'virtual_registries_container_cache_entries' \
           AND indexname = 'idx_vregs_container_cache_entries_on_uniq_object_storage_key'";
    assert_eq!(
        session.execute(exists_sql).unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("DROP INDEX idx_vregs_container_cache_entries_on_uniq_object_storage_key")
        .unwrap();
    assert!(session.execute(exists_sql).unwrap().rows.is_empty());
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session.execute(exists_sql).unwrap().rows,
        vec![vec![SqlValue::Int(1)]]
    );

    session.execute("BEGIN").unwrap();
    session
        .execute("DROP INDEX idx_vregs_container_cache_entries_on_uniq_object_storage_key")
        .unwrap();
    session.execute("COMMIT").unwrap();
    assert!(session.execute(exists_sql).unwrap().rows.is_empty());
    session
        .execute(
            "DROP INDEX IF EXISTS idx_vregs_container_cache_entries_on_uniq_object_storage_key",
        )
        .unwrap();
}

#[test]
fn postgres_create_table_partition_of_records_partition_metadata() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_static")
        .unwrap();
    session
        .execute(
            "CREATE TABLE virtual_registry_entries \
             (group_id bigint NOT NULL, iid bigint NOT NULL, relative_path text NOT NULL, \
              PRIMARY KEY (group_id, iid)) \
             PARTITION BY HASH (group_id)",
        )
        .unwrap();

    assert_eq!(
        session
            .execute(
                "CREATE TABLE gitlab_partitions_static.virtual_registry_entries_00 \
                 PARTITION OF virtual_registry_entries \
                 FOR VALUES WITH (MODULUS 16, REMAINDER 0)"
            )
            .unwrap()
            .command_complete_tag(),
        "CREATE TABLE"
    );

    assert_eq!(
        session
            .execute(
                "SELECT child.relname, child.relispartition, child_ns.nspname, parent.relname, pg_get_expr(child.relpartbound, child.oid)
                 FROM pg_catalog.pg_inherits i
                 JOIN pg_catalog.pg_class child ON child.oid = i.inhrelid
                 JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
                 JOIN pg_catalog.pg_class parent ON parent.oid = i.inhparent
                 WHERE parent.relname = 'virtual_registry_entries'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("virtual_registry_entries_00".to_string()),
            SqlValue::Bool(true),
            SqlValue::String("gitlab_partitions_static".to_string()),
            SqlValue::String("virtual_registry_entries".to_string()),
            SqlValue::String("FOR VALUES WITH (MODULUS 16, REMAINDER 0)".to_string()),
        ]]
    );

    let duplicate = session
        .execute(
            "CREATE TABLE gitlab_partitions_static.virtual_registry_entries_00 \
             PARTITION OF virtual_registry_entries \
             FOR VALUES WITH (MODULUS 16, REMAINDER 0)",
        )
        .unwrap_err()
        .to_string();
    assert!(duplicate.contains("relation \"virtual_registry_entries_00\" already exists"));
}

#[test]
fn alter_table_parent_column_is_visible_on_existing_partitions_and_indexes() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE SCHEMA gitlab_partitions_static")
        .unwrap();
    session
        .execute(
            "CREATE TABLE virtual_registry_entries (
                id bigint NOT NULL,
                partition_id bigint NOT NULL
            ) PARTITION BY HASH (partition_id)",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE gitlab_partitions_static.virtual_registry_entries_00
             PARTITION OF virtual_registry_entries
             FOR VALUES WITH (MODULUS 16, REMAINDER 0)",
        )
        .unwrap();

    session
        .execute("ALTER TABLE virtual_registry_entries ADD COLUMN digest text")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT attname, attnum
                 FROM pg_catalog.pg_attribute a
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'gitlab_partitions_static'
                   AND c.relname = 'virtual_registry_entries_00'
                   AND a.attname = 'digest'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("digest".to_string()),
            SqlValue::Int(3)
        ]]
    );

    session
        .execute(
            "CREATE INDEX index_virtual_registry_entries_00_on_digest
             ON gitlab_partitions_static.virtual_registry_entries_00 (digest)",
        )
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT d.indkey, pg_get_indexdef(d.indexrelid)
                 FROM pg_catalog.pg_class t
                 INNER JOIN pg_catalog.pg_index d ON t.oid = d.indrelid
                 INNER JOIN pg_catalog.pg_class i ON d.indexrelid = i.oid
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
                 WHERE i.relkind IN ('i', 'I')
                   AND d.indisprimary = false
                   AND i.relname = 'index_virtual_registry_entries_00_on_digest'
                   AND t.relname = 'virtual_registry_entries_00'
                   AND n.nspname = 'gitlab_partitions_static'"
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("3".to_string()),
            SqlValue::String(
                "CREATE INDEX index_virtual_registry_entries_00_on_digest ON gitlab_partitions_static.virtual_registry_entries_00 USING btree (digest)"
                    .to_string()
            ),
        ]]
    );
}

#[test]
fn partition_child_local_foreign_key_survives_parent_constraint_inheritance() {
    let (_dir, mut db) = empty_test_db();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TABLE referenced_partition_rows (id int, partition_id int)")
        .unwrap();
    session
        .execute(
            "CREATE TABLE partition_parent (
                id int,
                partition_id int
            ) PARTITION BY LIST (partition_id)",
        )
        .unwrap();
    session
        .execute(
            "CREATE TABLE public.partition_child
             PARTITION OF partition_parent
             FOR VALUES IN (1)",
        )
        .unwrap();
    session
        .execute(
            "ALTER TABLE public.partition_child
             ADD CONSTRAINT fk_partition_child_ref
             FOREIGN KEY (id, partition_id)
             REFERENCES referenced_partition_rows (id, partition_id)
             NOT VALID",
        )
        .unwrap();
    session
        .execute("ALTER TABLE public.partition_child VALIDATE CONSTRAINT fk_partition_child_ref")
        .unwrap();

    assert_eq!(
        session
            .execute(
                "SELECT convalidated
                 FROM pg_catalog.pg_constraint con
                 JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
                 WHERE rel.relname = 'partition_child'
                   AND con.conname = 'fk_partition_child_ref'"
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]]
    );
}
