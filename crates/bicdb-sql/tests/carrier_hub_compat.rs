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
        duplicate
            .to_string()
            .to_ascii_lowercase()
            .contains("unique")
            || duplicate
                .to_string()
                .to_ascii_lowercase()
                .contains("duplicate"),
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
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
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
        session_rows(
            &mut session,
            "SELECT 'FOR UPDATE SKIP LOCKED LIMIT 1' AS text LIMIT 1"
        ),
        vec![vec![SqlValue::String(
            "FOR UPDATE SKIP LOCKED LIMIT 1".to_string()
        )]]
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

#[test]
fn disable_trigger_all_suspends_foreign_keys_for_a_data_load() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE definitions (id INT PRIMARY KEY, current_version_id INT)",
        "CREATE TABLE versions (id INT PRIMARY KEY, definition_id INT NOT NULL REFERENCES definitions(id))",
        "ALTER TABLE definitions ADD CONSTRAINT definitions_current_version FOREIGN KEY (current_version_id) REFERENCES versions(id)",
        // pg_restore --disable-triggers: a circular pair loads in any order.
        "ALTER TABLE definitions DISABLE TRIGGER ALL",
        "ALTER TABLE versions DISABLE TRIGGER ALL",
        "INSERT INTO definitions VALUES (1, 10)",
        "INSERT INTO versions VALUES (10, 1)",
        "ALTER TABLE definitions ENABLE TRIGGER ALL",
        "ALTER TABLE versions ENABLE TRIGGER ALL",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    // Checks are back on once the triggers are enabled again.
    assert!(session
        .execute("INSERT INTO versions VALUES (11, 99)")
        .is_err());
    assert!(session
        .execute("INSERT INTO definitions VALUES (2, 99)")
        .is_err());
    // DISABLE TRIGGER USER leaves foreign keys enforced.
    session
        .execute("ALTER TABLE versions DISABLE TRIGGER USER")
        .unwrap();
    assert!(session
        .execute("INSERT INTO versions VALUES (12, 98)")
        .is_err());
}

/// Board fields carry CHECK (NOT (options_json ? 'options') OR ...): the jsonb
/// key-existence operators must work in CHECK constraints and WHERE clauses,
/// not only in SELECT lists.
#[test]
fn jsonb_existence_operators_work_in_check_constraints_and_where() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE fields (id INT PRIMARY KEY, options_json JSONB NOT NULL, \
         CHECK (NOT (options_json ? 'options') OR jsonb_typeof(options_json->'options') = 'array'))",
        "INSERT INTO fields VALUES (1, '{\"options\": []}')",
        "INSERT INTO fields VALUES (2, '{\"schemaVersion\": \"1\"}')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert!(
        session
            .execute("INSERT INTO fields VALUES (3, '{\"options\": 5}')")
            .is_err(),
        "the CHECK constraint must reject a non-array options value"
    );
    let ids = |session: &mut SqlSession<'_>, sql: &str| -> Vec<SqlValue> {
        session_rows(session, sql)
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    };
    assert_eq!(
        ids(
            &mut session,
            "SELECT id FROM fields WHERE options_json ? 'options' ORDER BY id"
        ),
        vec![SqlValue::Int(1)]
    );
    assert_eq!(
        ids(&mut session, "SELECT id FROM fields WHERE options_json ?| array['options', 'schemaVersion'] ORDER BY id"),
        vec![SqlValue::Int(1), SqlValue::Int(2)]
    );
    assert_eq!(
        ids(&mut session, "SELECT id FROM fields WHERE NOT (options_json ?& array['options', 'schemaVersion']) ORDER BY id"),
        vec![SqlValue::Int(1), SqlValue::Int(2)]
    );
}

/// The Hub's role SQL compares trigger functions with constant
/// 'schema.f(args)'::regprocedure casts inside correlated subqueries. pg_proc is
/// cached for those lookups; the cache must follow routines created, replaced
/// and dropped, also inside an open transaction.
#[test]
fn regprocedure_casts_follow_routine_changes() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    let resolves = |session: &mut SqlSession<'_>, signature: &str| -> bool {
        let sql = format!("SELECT to_regprocedure('{signature}') IS NOT NULL");
        session_rows(session, &sql)[0][0] == SqlValue::Bool(true)
    };
    session
        .execute("CREATE FUNCTION guard_one() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
        .unwrap();
    assert!(resolves(&mut session, "guard_one()"));
    assert!(!resolves(&mut session, "guard_two(integer)"));
    session.execute("BEGIN").unwrap();
    session
        .execute("CREATE FUNCTION guard_two(value integer) RETURNS integer LANGUAGE sql AS 'SELECT value'")
        .unwrap();
    assert!(
        resolves(&mut session, "guard_two(integer)"),
        "a routine created in the open transaction resolves"
    );
    session.execute("DROP FUNCTION guard_one()").unwrap();
    assert!(
        !resolves(&mut session, "guard_one()"),
        "a routine dropped in the open transaction no longer resolves"
    );
    session.execute("ROLLBACK").unwrap();
    assert!(
        resolves(&mut session, "guard_one()"),
        "rollback restores the dropped routine"
    );
    assert!(
        !resolves(&mut session, "guard_two(integer)"),
        "rollback removes the created routine"
    );
    // A constant cast evaluated once per outer row of a correlated subquery.
    for sql in [
        "CREATE TABLE things (id INT PRIMARY KEY)",
        "INSERT INTO things SELECT generate_series(1, 200)",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let rows = session_rows(
        &mut session,
        "SELECT count(*) FROM things AS outer_thing WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_proc AS p \
         WHERE p.oid = 'guard_one()'::regprocedure AND outer_thing.id > 0)",
    );
    assert_eq!(rows[0][0], SqlValue::Int(200));
}

/// Board rows carry CHECK (carrier_board_payload_within_limits(payload_json)), a
/// SQL function with a subquery; document sync tickets compare timestamps with
/// timestamp + interval. CHECK constraints are evaluated on the typed row by
/// the SQL engine, on every write path.
#[test]
fn check_constraints_call_user_functions_and_use_typed_arithmetic() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE FUNCTION payload_within_limits(payload JSONB) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $f$
           SELECT payload IS NULL OR (
             jsonb_typeof(payload) = 'object'
             AND NOT EXISTS (
               SELECT 1 FROM jsonb_each(payload) item(key, value)
               WHERE jsonb_typeof(item.value) = 'string' AND octet_length(item.value #>> '{}') > 8
             )
           )
         $f$",
        "CREATE TABLE rows_with_payload (id INT PRIMARY KEY, payload_json JSONB, \
         CONSTRAINT payload_capacity CHECK (payload_within_limits(payload_json)))",
        "CREATE TABLE tickets (id INT PRIMARY KEY, issued_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, \
         CONSTRAINT ticket_window CHECK (expires_at >= issued_at + INTERVAL '30 seconds' \
           AND expires_at <= issued_at + INTERVAL '5 minutes'))",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    // User function: single-row insert, multi-row insert, NULL, update.
    session
        .execute("INSERT INTO rows_with_payload VALUES (1, '{\"a\": \"short\"}')")
        .unwrap();
    session
        .execute("INSERT INTO rows_with_payload VALUES (2, '{\"b\": 1}'), (3, NULL)")
        .unwrap();
    assert!(session
        .execute("INSERT INTO rows_with_payload VALUES (4, '{\"a\": \"much too long\"}')")
        .is_err());
    assert!(session
        .execute("INSERT INTO rows_with_payload VALUES (5, '{\"a\": \"ok\"}'), (6, '[1]')")
        .is_err());
    assert!(session
        .execute(
            "UPDATE rows_with_payload SET payload_json = '{\"a\": \"much too long\"}' WHERE id = 1"
        )
        .is_err());
    session
        .execute("UPDATE rows_with_payload SET payload_json = '{\"a\": \"fine\"}' WHERE id = 1")
        .unwrap();
    // Timestamp + interval, including both boundaries and sub-second values.
    for sql in [
        "INSERT INTO tickets VALUES (1, '2026-07-12 09:00:21.483573+00', '2026-07-12 09:05:21.483573+00')",
        "INSERT INTO tickets VALUES (2, '2026-07-12 09:00:21+00', '2026-07-12 09:00:51+00')",
        "INSERT INTO tickets VALUES (3, '2026-07-12 09:00:21.483573+00', '2026-07-12 09:05:21+00')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    for sql in [
        "INSERT INTO tickets VALUES (4, '2026-07-12 09:00:21.483573+00', '2026-07-12 09:05:21.483574+00')",
        "INSERT INTO tickets VALUES (5, '2026-07-12 09:00:21+00', '2026-07-12 09:00:50+00')",
    ] {
        assert!(session.execute(sql).is_err(), "{sql} must violate the ticket window");
    }
    let count = session_rows(&mut session, "SELECT count(*) FROM tickets");
    assert_eq!(count[0][0], SqlValue::Int(3));
}

/// `column + interval` keeps microseconds and the time zone and supports day
/// and month intervals, like the same expression on a typed literal.
#[test]
fn timestamp_column_interval_arithmetic_is_exact() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SET TIME ZONE 'UTC'",
        "CREATE TABLE stamps (id INT PRIMARY KEY, at TIMESTAMPTZ, local_at TIMESTAMP)",
        "INSERT INTO stamps VALUES (1, '2026-07-12 09:00:21.483573+00', '2026-07-12 09:00:21.483573')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let cell = |session: &mut SqlSession<'_>, expr: &str| -> String {
        session_rows(session, &format!("SELECT ({expr})::text FROM stamps"))[0][0].to_cell()
    };
    assert_eq!(
        cell(&mut session, "at + INTERVAL '5 minutes'"),
        "2026-07-12 09:05:21.483573+00"
    );
    assert_eq!(
        cell(&mut session, "INTERVAL '5 minutes' + at"),
        "2026-07-12 09:05:21.483573+00"
    );
    assert_eq!(
        cell(&mut session, "at - INTERVAL '30 seconds'"),
        "2026-07-12 08:59:51.483573+00"
    );
    assert_eq!(
        cell(&mut session, "at + INTERVAL '1 day 2 hours'"),
        "2026-07-13 11:00:21.483573+00"
    );
    assert_eq!(
        cell(&mut session, "at + INTERVAL '1 month'"),
        "2026-08-12 09:00:21.483573+00"
    );
    assert_eq!(
        cell(&mut session, "local_at + INTERVAL '5 minutes'"),
        "2026-07-12 09:05:21.483573"
    );
    let rows = session_rows(
        &mut session,
        "SELECT id FROM stamps WHERE at + INTERVAL '5 minutes' >= '2026-07-12 09:05:21.483573+00'",
    );
    assert_eq!(rows.len(), 1);
}

/// The Hub's board capacity trigger nests a PL/pgSQL CASE statement (closed by
/// END CASE) inside IF ... ELSE ... END IF.
#[test]
fn plpgsql_case_statement_inside_if_block() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE items (id INT PRIMARY KEY, kind TEXT, score INT)",
        "CREATE FUNCTION score_item() RETURNS trigger LANGUAGE plpgsql AS $f$
         BEGIN
           IF NEW.kind = 'fixed' THEN
             NEW.score := 1;
           ELSE
             CASE NEW.kind
               WHEN 'a' THEN NEW.score := 2;
               WHEN 'b' THEN NEW.score := 3;
               ELSE NEW.score := 4;
             END CASE;
           END IF;
           RETURN NEW;
         END
         $f$",
        "CREATE TRIGGER score_item BEFORE INSERT ON items FOR EACH ROW EXECUTE FUNCTION score_item()",
        "INSERT INTO items (id, kind) VALUES (1, 'fixed'), (2, 'a'), (3, 'b'), (4, 'z')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let rows = session_rows(&mut session, "SELECT score FROM items ORDER BY id");
    let scores = rows
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        scores,
        vec![
            SqlValue::Int(1),
            SqlValue::Int(2),
            SqlValue::Int(3),
            SqlValue::Int(4)
        ]
    );
}

/// The Hub's document capability query is WITH RECURSIVE target, ancestors
/// (recursive), ..., candidates AS (... UNION ALL ...). Only a CTE that
/// references itself is recursive; the UNION ALL sibling is evaluated once.
#[test]
fn with_recursive_evaluates_non_self_referencing_union_ctes_once() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE docs (id INT PRIMARY KEY, parent_id INT)",
        "INSERT INTO docs VALUES (1, NULL), (2, 1), (3, 2)",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let rows = session_rows(
        &mut session,
        "WITH RECURSIVE target AS (SELECT id, parent_id FROM docs WHERE id = 3),
         ancestors AS (
           SELECT parent_id AS id, 1 AS depth FROM target WHERE parent_id IS NOT NULL
           UNION ALL
           SELECT docs.parent_id, ancestors.depth + 1 FROM ancestors JOIN docs ON docs.id = ancestors.id
           WHERE docs.parent_id IS NOT NULL
         ),
         candidates AS (
           SELECT 'owner' AS source, 0 AS depth FROM target
           UNION ALL
           SELECT 'ancestor', depth FROM ancestors
           UNION ALL
           SELECT 'none', 999
         )
         SELECT source, depth FROM candidates ORDER BY depth",
    );
    let got = rows
        .into_iter()
        .map(|row| format!("{}:{}", row[0].to_cell(), row[1].to_cell()))
        .collect::<Vec<_>>();
    assert_eq!(got, vec!["owner:0", "ancestor:1", "ancestor:2", "none:999"]);
}

#[test]
fn plpgsql_case_statement_forms() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE things (id INT PRIMARY KEY, kind TEXT)",
        "INSERT INTO things VALUES (1, 'a'), (2, 'a'), (3, 'b')",
        "CREATE FUNCTION classify(kind TEXT, n INT) RETURNS TEXT LANGUAGE plpgsql AS $f$
         DECLARE result TEXT; total BIGINT; wanted TEXT := kind;
         BEGIN
           CASE
             WHEN n < 0 THEN result := 'negative';
             WHEN n = 0 THEN
               IF kind IS NULL THEN result := 'zero-null'; ELSE result := 'zero'; END IF;
             ELSE
               CASE kind
                 WHEN 'a', 'b' THEN
                   SELECT count(*) INTO total FROM things WHERE things.kind = wanted;
                   result := kind || ':' || total::text;
                 WHEN 'c' THEN result := (CASE WHEN n > 10 THEN 'big' ELSE 'small' END);
               END CASE;
           END CASE;
           RETURN result;
         END
         $f$",
    ] {
        session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let one = |session: &mut SqlSession<'_>, sql: &str| session_rows(session, sql)[0][0].to_cell();
    assert_eq!(one(&mut session, "SELECT classify('x', -1)"), "negative");
    assert_eq!(one(&mut session, "SELECT classify(NULL, 0)"), "zero-null");
    assert_eq!(one(&mut session, "SELECT classify('q', 0)"), "zero");
    assert_eq!(one(&mut session, "SELECT classify('a', 5)"), "a:2");
    assert_eq!(one(&mut session, "SELECT classify('b', 5)"), "b:1");
    assert_eq!(one(&mut session, "SELECT classify('c', 50)"), "big");
    let error = session
        .execute("SELECT classify('zzz', 5)")
        .unwrap_err()
        .to_string();
    assert!(error.contains("case not found"), "{error}");
}

/// The Hub's board row hierarchy guard raises with USING ERRCODE and
/// CONSTRAINT; the routine must compile, and the raised error is reported as a
/// constraint violation naming that constraint.
#[test]
fn raise_using_constraint_reports_a_constraint_violation() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE nodes (id INT PRIMARY KEY, parent_id INT)",
        "CREATE FUNCTION nodes_guard() RETURNS trigger LANGUAGE plpgsql AS $f$
         BEGIN
           IF NEW.parent_id IS NULL THEN
             RETURN NEW;
           END IF;
           IF NEW.parent_id = NEW.id THEN
             RAISE EXCEPTION 'node cannot be its own parent'
               USING ERRCODE = '23514',
                     CONSTRAINT = 'nodes_parent_not_self';
           END IF;
           RETURN NEW;
         END
         $f$",
        "CREATE TRIGGER nodes_guard BEFORE INSERT OR UPDATE ON nodes FOR EACH ROW EXECUTE FUNCTION nodes_guard()",
        "INSERT INTO nodes VALUES (1, NULL), (2, 1)",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    let error = session.execute("INSERT INTO nodes VALUES (3, 3)").unwrap_err();
    assert_eq!(error.sqlstate(), "23514", "{error}");
    assert!(error.to_string().contains("node cannot be its own parent"), "{error}");
    match &error {
        bicdb_sql::SqlError::ConstraintViolation { constraint, .. } => {
            assert_eq!(constraint.as_deref(), Some("nodes_parent_not_self"))
        }
        other => panic!("expected a constraint violation, got {other:?}"),
    }
}

/// The Hub's AI capability discovery selects `command_item AS source` from
/// `CROSS JOIN LATERAL jsonb_array_elements(...) AS command_item`; the column
/// is jsonb (also when no row matches, which is what Describe sees).
#[test]
fn set_returning_function_alias_column_is_typed() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE apps (id INT PRIMARY KEY, commands JSONB)",
        "INSERT INTO apps VALUES (1, '{\"items\": [{\"id\": \"a\"}]}')",
    ] {
        session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    for sql in [
        "SELECT item AS source FROM jsonb_array_elements('[1,2]'::jsonb) AS item",
        "SELECT item AS source FROM apps app CROSS JOIN LATERAL \
         jsonb_array_elements(COALESCE(app.commands->'items', '[]'::jsonb)) AS item",
        "SELECT item AS source FROM apps app CROSS JOIN LATERAL \
         jsonb_array_elements(COALESCE(app.commands->'items', '[]'::jsonb)) AS item WHERE app.id = NULL",
    ] {
        let result = session.execute(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(result.column_types, vec![Some("jsonb".to_string())], "{sql}");
    }
}

/// The Hub's app manifest listing filters each navigation item with correlated
/// EXISTS subqueries; the outer row carries megabytes of manifest JSON. Reusing
/// the outer row between subquery runs must give exactly the per-row answers.
#[test]
fn correlated_exists_per_array_element_on_wide_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE manifests (id INT PRIMARY KEY, min_rank INT, navigation JSONB, padding JSONB)")
        .unwrap();
    session.execute("CREATE TABLE ranks (role TEXT PRIMARY KEY, rank INT)").unwrap();
    session
        .execute("INSERT INTO ranks VALUES ('guest', 10), ('member', 20), ('admin', 40)")
        .unwrap();
    let padding = format!("{{\"blob\": \"{}\"}}", "x".repeat(200_000));
    for (id, min_rank) in [(1, 0), (2, 15), (3, 30)] {
        let items = (0..300)
            .map(|i| {
                let role = ["guest", "member", "admin"][i % 3];
                format!("{{\"n\": {i}, \"required_role\": \"{role}\"}}")
            })
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!(
                "INSERT INTO manifests VALUES ({id}, {min_rank}, '{{\"items\": [{items}]}}', '{padding}')"
            ))
            .unwrap();
    }
    let started = std::time::Instant::now();
    let rows = session_rows(
        &mut session,
        "SELECT m.id, (
           SELECT count(*) FROM jsonb_array_elements(m.navigation->'items') AS entry(item)
           WHERE EXISTS (
             SELECT 1 FROM ranks r
             WHERE r.role = entry.item->>'required_role' AND r.rank >= m.min_rank
           )
         )::bigint AS visible
         FROM manifests m ORDER BY m.id",
    );
    let got = rows
        .into_iter()
        .map(|row| (row[0].to_cell(), row[1].to_cell()))
        .collect::<Vec<_>>();
    // min_rank 0: all 300; 15: member + admin (200); 30: admin only (100).
    assert_eq!(
        got,
        vec![("1".into(), "300".into()), ("2".into(), "200".into()), ("3".into(), "100".into())]
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(30), "took {:?}", started.elapsed());
}
