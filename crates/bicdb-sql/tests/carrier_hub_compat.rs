//! PostgreSQL behaviours the WalkNorth Hub's Carrier-generated schema and
//! runtime depend on: partial unique indexes on jsonb text fields, grants on
//! system catalog views, `DROP DATABASE ... WITH (FORCE)` parsing, and queue
//! claims that write the locking clause before `LIMIT`.
use bicdb_core::BicDb;
use bicdb_sql::{parse_database_ddl, DatabaseDdl, SqlSession, SqlValue};

fn session_rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
}

#[test]
fn partial_unique_index_on_jsonb_text_field_constrains_only_matching_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE outbox (id INT PRIMARY KEY, document_id INT NOT NULL, event_kind TEXT NOT NULL, payload JSONB NOT NULL)",
        // The Hub's document projection index (migration 0031): one rebuild
        // request per document among projection-rebuild events only.
        "CREATE UNIQUE INDEX outbox_rebuild_request ON outbox (document_id) \
         WHERE event_kind = 'metadata_changed' AND payload ->> 'change_kind' = 'projection_rebuild_requested'",
        "INSERT INTO outbox VALUES (1, 7, 'metadata_changed', '{\"change_kind\": \"projection_rebuild_requested\"}')",
        // Same document, different change kind: outside the index.
        "INSERT INTO outbox VALUES (2, 7, 'metadata_changed', '{\"change_kind\": \"renamed\"}')",
        // Same document, key missing (reads as NULL): outside the index.
        "INSERT INTO outbox VALUES (3, 7, 'metadata_changed', '{}')",
        // Same document, different event kind: outside the index.
        "INSERT INTO outbox VALUES (4, 7, 'created', '{\"change_kind\": \"projection_rebuild_requested\"}')",
        // Another document: its own uniqueness slot.
        "INSERT INTO outbox VALUES (5, 8, 'metadata_changed', '{\"change_kind\": \"projection_rebuild_requested\"}')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let duplicate = session
        .execute(
            "INSERT INTO outbox VALUES (6, 7, 'metadata_changed', '{\"change_kind\": \"projection_rebuild_requested\"}')",
        )
        .expect_err("a second rebuild request for document 7 must violate the partial unique index");
    assert!(
        duplicate.to_string().to_ascii_lowercase().contains("unique")
            || duplicate.to_string().to_ascii_lowercase().contains("duplicate"),
        "{duplicate}"
    );
    // Moving a row out of the predicate frees its slot.
    session
        .execute("UPDATE outbox SET payload = '{\"change_kind\": \"done\"}' WHERE id = 1")
        .unwrap();
    session
        .execute(
            "INSERT INTO outbox VALUES (6, 7, 'metadata_changed', '{\"change_kind\": \"projection_rebuild_requested\"}')",
        )
        .unwrap();
}

#[test]
fn grants_on_system_catalog_views_are_accepted() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE ROLE catalog_reader LOGIN",
        "CREATE TABLE notes (id INT PRIMARY KEY)",
        // Carrier's role SQL grants every relation its actions read.
        "GRANT SELECT ON TABLE pg_timezone_names TO catalog_reader",
        "GRANT SELECT ON TABLE pg_catalog.pg_roles TO catalog_reader",
        "GRANT SELECT ON TABLE notes, pg_timezone_names TO catalog_reader",
        "REVOKE SELECT ON TABLE pg_timezone_names FROM catalog_reader",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    // A missing user table is still an error.
    assert!(session
        .execute("GRANT SELECT ON TABLE no_such_table TO catalog_reader")
        .is_err());
}

#[test]
fn drop_database_parses_the_force_option() {
    for (sql, expected_force) in [
        ("DROP DATABASE example", false),
        ("DROP DATABASE IF EXISTS example WITH (FORCE)", true),
        ("DROP DATABASE \"carrier_test_x\" WITH (FORCE);", true),
        ("DROP DATABASE example (FORCE)", true),
        ("drop database example with ( force )", true),
    ] {
        let Some(DatabaseDdl::Drop { force, .. }) = parse_database_ddl(sql).unwrap() else {
            panic!("drop not recognized: {sql}");
        };
        assert_eq!(force, expected_force, "{sql}");
    }
    assert!(parse_database_ddl("DROP DATABASE example WITH (SOMETHING)").is_err());
    assert!(parse_database_ddl("DROP DATABASE example CASCADE").is_err());
}

#[test]
fn locking_clause_may_precede_limit_in_queue_claims() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE jobs (id INT PRIMARY KEY, available_at INT NOT NULL, attempts INT NOT NULL DEFAULT 0)",
        "INSERT INTO jobs (id, available_at) VALUES (1, 20), (2, 10), (3, 30)",
        "CREATE FUNCTION claim_job() RETURNS TABLE (job_id INT) LANGUAGE plpgsql AS $claim$
         BEGIN
           RETURN QUERY
           WITH due AS (
             SELECT jobs.id FROM jobs
             WHERE jobs.attempts = 0
             ORDER BY jobs.available_at
             FOR UPDATE SKIP LOCKED
             LIMIT 1
           )
           UPDATE jobs SET attempts = jobs.attempts + 1 FROM due
           WHERE jobs.id = due.id
           RETURNING jobs.id;
         END
         $claim$",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    // Top-level statement in PostgreSQL's alternative clause order.
    assert_eq!(
        session_rows(
            &mut session,
            "SELECT id FROM jobs ORDER BY available_at FOR UPDATE SKIP LOCKED LIMIT 1 OFFSET 1"
        ),
        vec![vec![SqlValue::Int(1)]]
    );
    // Inside a PL/pgSQL set-returning function: oldest first, each job once.
    let mut claimed = Vec::new();
    for _ in 0..4 {
        session.execute("BEGIN").unwrap();
        claimed.extend(session_rows(&mut session, "SELECT job_id FROM claim_job()"));
        session.execute("COMMIT").unwrap();
    }
    assert_eq!(
        claimed,
        vec![
            vec![SqlValue::Int(2)],
            vec![SqlValue::Int(1)],
            vec![SqlValue::Int(3)],
        ]
    );
    // Quoted text that merely looks like the clause is left alone.
    assert_eq!(
        session_rows(&mut session, "SELECT 'FOR UPDATE SKIP LOCKED LIMIT 1' AS text LIMIT 1"),
        vec![vec![SqlValue::String("FOR UPDATE SKIP LOCKED LIMIT 1".to_string())]]
    );
}

#[test]
fn unique_index_on_multi_column_coalesce_keys_on_the_first_non_null_column() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE grants (id INT PRIMARY KEY, document_id INT NOT NULL, principal_kind TEXT NOT NULL, \
         user_id TEXT, team_id TEXT, hub_id TEXT, origin TEXT NOT NULL, revoked_at TEXT)",
        // The Hub's document grant identity (migration 0015).
        "CREATE UNIQUE INDEX grant_identity ON grants (document_id, principal_kind, COALESCE(user_id, team_id, hub_id)) \
         WHERE origin = 'direct' AND revoked_at IS NULL",
        "INSERT INTO grants VALUES (1, 1, 'user', 'u1', NULL, NULL, 'direct', NULL)",
        "INSERT INTO grants VALUES (2, 1, 'team', NULL, 't1', NULL, 'direct', NULL)",
        // Same principal value through a different column of another kind.
        "INSERT INTO grants VALUES (3, 1, 'hub', NULL, NULL, 'u1', 'direct', NULL)",
        // All principals NULL: NULLS DISTINCT, so never a conflict.
        "INSERT INTO grants VALUES (4, 1, 'user', NULL, NULL, NULL, 'direct', NULL)",
        "INSERT INTO grants VALUES (5, 1, 'user', NULL, NULL, NULL, 'direct', NULL)",
        // Revoked or inherited grants are outside the partial index.
        "INSERT INTO grants VALUES (6, 1, 'user', 'u1', NULL, NULL, 'direct', 'yesterday')",
        "INSERT INTO grants VALUES (7, 1, 'user', 'u1', NULL, NULL, 'inherited', NULL)",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert!(session
        .execute("INSERT INTO grants VALUES (8, 1, 'user', 'u1', NULL, NULL, 'direct', NULL)")
        .is_err());
    // The second column is used when the first is NULL.
    assert!(session
        .execute("INSERT INTO grants VALUES (9, 1, 'team', NULL, 't1', 'other', 'direct', NULL)")
        .is_err());
    session
        .execute("UPDATE grants SET revoked_at = 'now' WHERE id = 1")
        .unwrap();
    session
        .execute("INSERT INTO grants VALUES (8, 1, 'user', 'u1', NULL, NULL, 'direct', NULL)")
        .unwrap();
    // Mixed column types are not folded into one key.
    session
        .execute("CREATE TABLE mixed (id INT PRIMARY KEY, a TEXT, b INT)")
        .unwrap();
    assert!(session
        .execute("CREATE UNIQUE INDEX mixed_key ON mixed (COALESCE(a, b))")
        .is_err());
}
