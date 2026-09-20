//! SQL surface tests for the stream broker (Slice 3).

use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::Value as JsonValue;

fn empty_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open(dir.path()).unwrap();
    (dir, db)
}

fn scalar(db: &mut BicDb, sql: &str) -> SqlValue {
    let mut session = SqlSession::new(db);
    let result = session.execute(sql).unwrap();
    assert_eq!(result.rows.len(), 1, "one row from {sql}");
    result.rows[0][0].clone()
}

fn scalar_json(db: &mut BicDb, sql: &str) -> JsonValue {
    match scalar(db, sql) {
        SqlValue::Json(value) => value,
        SqlValue::String(text) => serde_json::from_str(&text).expect("json result"),
        other => panic!("expected json from {sql}, got {other:?}"),
    }
}

#[test]
fn broker_publish_consume_ack_roundtrip_via_sql() {
    let (_dir, mut db) = empty_db();

    let receipt = scalar_json(
        &mut db,
        r#"SELECT broker_publish(
            'commerce.orders',
            '{"order": 1}'::json,
            '{"tenant": "t1"}'::json
        )"#,
    );
    assert_eq!(receipt["sequence"], 0);
    assert_eq!(receipt["deduplicated"], false);
    let message_id = receipt["message_id"].as_str().unwrap().to_string();

    let batch = scalar_json(
        &mut db,
        "SELECT broker_consume('commerce.orders', 'sis-enrollment', 'worker-1', 10, 30000)",
    );
    let batch = batch.as_array().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0]["message_id"].as_str().unwrap(), message_id);
    assert_eq!(batch[0]["payload"], serde_json::json!({"order": 1}));
    assert_eq!(batch[0]["headers"], serde_json::json!({"tenant": "t1"}));
    assert_eq!(batch[0]["attempts"], 1);

    let acked = scalar(
        &mut db,
        &format!(
            "SELECT broker_ack('commerce.orders', 'sis-enrollment', 'worker-1', '{message_id}')"
        ),
    );
    assert_eq!(acked, SqlValue::Bool(true));

    // Nothing left for the group; a second group still gets the message.
    let empty = scalar_json(
        &mut db,
        "SELECT broker_consume('commerce.orders', 'sis-enrollment', 'worker-1', 10, 0)",
    );
    assert!(empty.as_array().unwrap().is_empty());
    let other = scalar_json(
        &mut db,
        "SELECT broker_consume('commerce.orders', 'billing', 'worker-1', 10, 30000)",
    );
    assert_eq!(other.as_array().unwrap().len(), 1);
}

#[test]
fn broker_nack_and_dead_letters_via_sql() {
    let (_dir, mut db) = empty_db();

    let receipt = scalar_json(&mut db, r#"SELECT broker_publish('q', '{"n": 1}', NULL)"#);
    let message_id = receipt["message_id"].as_str().unwrap().to_string();

    scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 1, 30000)");
    // Nack without requeue dead-letters immediately.
    let nacked = scalar(
        &mut db,
        &format!("SELECT broker_nack('q', 'g', 'w', '{message_id}', false, NULL, 'poison')"),
    );
    assert_eq!(nacked, SqlValue::Bool(true));

    let dead = scalar_json(&mut db, "SELECT broker_dead_letters('q', 'g')");
    let dead = dead.as_array().unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0]["broker"]["reason"], "nacked without requeue");
    assert_eq!(dead[0]["broker"]["last_error"], "poison");

    // Redrive brings it back as a fresh message; purge then empties the DLQ.
    let receipts = scalar_json(&mut db, "SELECT broker_redrive_dead_letters('q', 'g', 10)");
    assert_eq!(receipts.as_array().unwrap().len(), 1);
    let purged = scalar(&mut db, "SELECT broker_purge_dead_letters('q', 'g')");
    assert_eq!(purged, SqlValue::Int(1));
    let dead = scalar_json(&mut db, "SELECT broker_dead_letters('q', 'g')");
    assert!(dead.as_array().unwrap().is_empty());
}

#[test]
fn broker_publish_with_options_and_group_info_via_sql() {
    let (_dir, mut db) = empty_db();

    let first = scalar_json(
        &mut db,
        r#"SELECT broker_publish_with('q', '{"n": 1}',
            '{"idempotency_key": "evt-1", "max_attempts": 3, "headers": {"a": 1}}')"#,
    );
    assert_eq!(first["deduplicated"], false);
    let duplicate = scalar_json(
        &mut db,
        r#"SELECT broker_publish_with('q', '{"n": 1}', '{"idempotency_key": "evt-1"}')"#,
    );
    assert_eq!(duplicate["deduplicated"], true);
    assert_eq!(duplicate["message_id"], first["message_id"]);

    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 30000)");
    let batch = batch.as_array().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0]["max_attempts"], 3);
    assert_eq!(batch[0]["headers"], serde_json::json!({"a": 1}));
    assert_eq!(batch[0]["idempotency_key"], "evt-1");

    let info = scalar_json(&mut db, "SELECT broker_group_info('q', 'g')");
    assert_eq!(info["in_flight"], 1);
    assert_eq!(info["last_delivered_sequence"], 0);

    // Unknown group reports NULL.
    let missing = scalar(&mut db, "SELECT broker_group_info('q', 'nope')");
    assert_eq!(missing, SqlValue::Null);
}

#[test]
fn broker_configure_and_trim_via_sql() {
    let (_dir, mut db) = empty_db();

    scalar(
        &mut db,
        r#"SELECT broker_configure_queue('q', '{"retention_max_messages": 1}')"#,
    );
    let config = scalar_json(&mut db, "SELECT broker_queue_config('q')");
    assert_eq!(config["retention_max_messages"], 1);

    for n in 0..3 {
        scalar_json(
            &mut db,
            &format!(r#"SELECT broker_publish('q', '{{"n": {n}}}', NULL)"#),
        );
    }
    let report = scalar_json(&mut db, "SELECT broker_trim('q')");
    assert_eq!(report["removed_messages"], 2);
    assert_eq!(report["through_sequence"], 1);

    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 30000)");
    let batch = batch.as_array().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0]["sequence"], 2);
}

#[test]
fn broker_sql_functions_validate_arguments() {
    let (_dir, mut db) = empty_db();
    let mut session = SqlSession::new(&mut db);

    assert!(session.execute("SELECT broker_publish('q')").is_err());
    assert!(session
        .execute("SELECT broker_publish('q', 'not json', NULL)")
        .is_err());
    assert!(session
        .execute("SELECT broker_ack('q', 'g', 'w', 'not-a-uuid')")
        .is_err());
    assert!(session
        .execute("SELECT broker_consume('bad/queue', 'g', 'w', 1, 0)")
        .is_err());
}

#[test]
fn broker_stats_snapshot_via_sql() {
    let (_dir, mut db) = empty_db();

    scalar_json(&mut db, r#"SELECT broker_publish('q', '{"n": 1}', NULL)"#);
    scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 1, 30000)");

    let stats = scalar_json(&mut db, "SELECT broker_stats()");
    let queues = stats["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["queue"], "q");
    assert_eq!(queues[0]["retained_messages"], 1);
    assert_eq!(queues[0]["groups"][0]["group"], "g");
    assert_eq!(queues[0]["groups"][0]["in_flight"], 1);
}

// ---------- Slice 6: per-queue ACLs ----------

fn secure_scalar(
    db: &mut BicDb,
    roles: &[&str],
    sql: &str,
) -> Result<SqlValue, bicdb_sql::SqlError> {
    let ctx =
        bicdb_core::SecurityContext::new("app-user", "tenant-1").with_roles(roles.iter().copied());
    let mut session = SqlSession::new_secure(db, ctx);
    session.execute(sql).map(|result| result.rows[0][0].clone())
}

#[test]
fn restricted_sessions_use_unrestricted_queues_but_not_admin_ops() {
    let (_dir, mut db) = empty_db();

    // Publish + consume work for a restricted session on an unconfigured queue.
    let receipt = secure_scalar(
        &mut db,
        &["app"],
        r#"SELECT broker_publish('q', '{"n": 1}', NULL)"#,
    )
    .unwrap();
    assert!(matches!(receipt, SqlValue::Json(_)));
    secure_scalar(
        &mut db,
        &["app"],
        "SELECT broker_consume('q', 'g', 'w', 1, 30000)",
    )
    .unwrap();

    // Admin operations are denied to restricted sessions by default.
    assert!(secure_scalar(&mut db, &["app"], "SELECT broker_trim('q')").is_err());
    assert!(secure_scalar(
        &mut db,
        &["app"],
        r#"SELECT broker_configure_queue('q', '{"retention_max_messages": 1}')"#
    )
    .is_err());
    assert!(secure_scalar(
        &mut db,
        &["app"],
        "SELECT broker_purge_dead_letters('q', 'g')"
    )
    .is_err());
}

#[test]
fn queue_acls_gate_publish_consume_and_admin_by_role() {
    let (_dir, mut db) = empty_db();

    // Trusted session sets the ACL.
    scalar(
        &mut db,
        r#"SELECT broker_configure_queue('orders',
            '{"publish_roles": ["producer"], "consume_roles": ["consumer"], "admin_roles": ["ops"]}')"#,
    );
    let config = scalar_json(&mut db, "SELECT broker_queue_config('orders')");
    assert_eq!(config["publish_roles"], serde_json::json!(["producer"]));

    // Producer can publish but not consume.
    secure_scalar(
        &mut db,
        &["producer"],
        r#"SELECT broker_publish('orders', '{"n": 1}', NULL)"#,
    )
    .unwrap();
    assert!(secure_scalar(
        &mut db,
        &["producer"],
        "SELECT broker_consume('orders', 'g', 'w', 1, 30000)"
    )
    .is_err());

    // Consumer can consume but not publish.
    secure_scalar(
        &mut db,
        &["consumer"],
        "SELECT broker_consume('orders', 'g', 'w', 1, 30000)",
    )
    .unwrap();
    assert!(secure_scalar(
        &mut db,
        &["consumer"],
        r#"SELECT broker_publish('orders', '{"n": 2}', NULL)"#
    )
    .is_err());

    // Ops role can run admin operations; unrelated roles cannot.
    secure_scalar(&mut db, &["ops"], "SELECT broker_trim('orders')").unwrap();
    assert!(secure_scalar(&mut db, &["consumer"], "SELECT broker_trim('orders')").is_err());

    // Trusted (no security context) sessions always pass.
    scalar_json(
        &mut db,
        r#"SELECT broker_publish('orders', '{"n": 3}', NULL)"#,
    );
}

#[test]
fn broker_stats_filters_queues_for_restricted_sessions() {
    let (_dir, mut db) = empty_db();

    scalar_json(
        &mut db,
        r#"SELECT broker_publish('open', '{"n": 1}', NULL)"#,
    );
    scalar(
        &mut db,
        r#"SELECT broker_configure_queue('locked', '{"consume_roles": ["insider"]}')"#,
    );
    scalar_json(
        &mut db,
        r#"SELECT broker_publish('locked', '{"n": 1}', NULL)"#,
    );

    let stats = secure_scalar(&mut db, &["app"], "SELECT broker_stats()").unwrap();
    let SqlValue::Json(stats) = stats else {
        panic!("json stats")
    };
    let names: Vec<&str> = stats["queues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|queue| queue["queue"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"open"));
    assert!(!names.contains(&"locked"));

    // The trusted session sees everything.
    let stats = scalar_json(&mut db, "SELECT broker_stats()");
    assert_eq!(stats["queues"].as_array().unwrap().len(), 2);
}

// ---------- Slice 7: transactional publish ----------

#[test]
fn publish_on_commit_defers_until_commit_and_drops_on_rollback() {
    let (_dir, mut db) = empty_db();

    // Deferred inside a transaction: invisible until COMMIT.
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        let result = session
            .execute(r#"SELECT broker_publish_on_commit('q', '{"n": 1}', NULL)"#)
            .unwrap();
        let SqlValue::Json(receipt) = &result.rows[0][0] else {
            panic!("json receipt")
        };
        assert_eq!(receipt["deferred"], true);
        assert!(receipt["sequence"].is_null());
        let visible = session
            .execute("SELECT broker_consume('q', 'g', 'w', 10, 0)")
            .unwrap();
        let SqlValue::Json(batch) = &visible.rows[0][0] else {
            panic!("json batch")
        };
        assert!(batch.as_array().unwrap().is_empty(), "invisible pre-commit");
        session.execute("COMMIT").unwrap();
    }
    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 30000)");
    assert_eq!(batch.as_array().unwrap().len(), 1);
    assert_eq!(
        batch.as_array().unwrap()[0]["payload"],
        serde_json::json!({"n": 1})
    );

    // Dropped on ROLLBACK.
    {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session
            .execute(r#"SELECT broker_publish_on_commit('q', '{"n": 2}', NULL)"#)
            .unwrap();
        session.execute("ROLLBACK").unwrap();
    }
    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 0)");
    assert!(
        batch.as_array().unwrap().is_empty(),
        "rolled-back publish never appears"
    );
}

#[test]
fn publish_on_commit_respects_savepoints_and_autocommit() {
    let (_dir, mut db) = empty_db();

    {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        session
            .execute(r#"SELECT broker_publish_on_commit('q', '{"keep": true}', NULL)"#)
            .unwrap();
        session.execute("SAVEPOINT sp").unwrap();
        session
            .execute(r#"SELECT broker_publish_on_commit('q', '{"drop": true}', NULL)"#)
            .unwrap();
        session.execute("ROLLBACK TO SAVEPOINT sp").unwrap();
        session.execute("COMMIT").unwrap();
    }
    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 30000)");
    let batch = batch.as_array().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0]["payload"], serde_json::json!({"keep": true}));

    // Autocommit: publishes immediately with a full receipt.
    let receipt = scalar_json(
        &mut db,
        r#"SELECT broker_publish_on_commit('q2', '{"n": 1}', NULL)"#,
    );
    assert_eq!(receipt["deferred"], false);
    assert_eq!(receipt["sequence"], 0);
    let batch = scalar_json(&mut db, "SELECT broker_consume('q2', 'g', 'w', 10, 0)");
    assert_eq!(batch.as_array().unwrap().len(), 1);
}

#[test]
fn publish_on_commit_with_options_carries_idempotency_to_commit_time() {
    let (_dir, mut db) = empty_db();

    {
        let mut session = SqlSession::new(&mut db);
        session.execute("BEGIN").unwrap();
        // Same idempotency key buffered twice in one transaction: the second
        // deduplicates at commit time.
        for _ in 0..2 {
            session
                .execute(
                    r#"SELECT broker_publish_with_on_commit('q', '{"n": 1}',
                        '{"idempotency_key": "evt-9", "headers": {"a": 1}}')"#,
                )
                .unwrap();
        }
        session.execute("COMMIT").unwrap();
    }
    let batch = scalar_json(&mut db, "SELECT broker_consume('q', 'g', 'w', 10, 30000)");
    let batch = batch.as_array().unwrap();
    assert_eq!(
        batch.len(),
        1,
        "second buffered publish deduplicated at commit"
    );
    assert_eq!(batch[0]["headers"], serde_json::json!({"a": 1}));
}

#[test]
fn exception_rollback_discards_only_protected_block_publishes() {
    let (_dir, mut db) = empty_db();
    {
        let mut session = SqlSession::new(&mut db);
        session
            .execute("CREATE TABLE events_source (id INT PRIMARY KEY)")
            .unwrap();
        session
            .execute(
                r#"
            CREATE FUNCTION emit_with_handler() RETURNS void LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO events_source VALUES (2);
                PERFORM broker_publish_on_commit('events', '{"id":2}', NULL);
                RAISE EXCEPTION 'rollback inner work';
            EXCEPTION WHEN OTHERS THEN
                INSERT INTO events_source VALUES (3);
                PERFORM broker_publish_on_commit('events', '{"id":3}', NULL);
            END $$
        "#,
            )
            .unwrap();
        session.execute("BEGIN").unwrap();
        session
            .execute("INSERT INTO events_source VALUES (1)")
            .unwrap();
        session
            .execute(r#"SELECT broker_publish_on_commit('events', '{"id":1}', NULL)"#)
            .unwrap();
        session.execute("SELECT emit_with_handler()").unwrap();
        assert_eq!(
            session
                .execute("SELECT id FROM events_source ORDER BY id")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(3)]]
        );
        session.execute("COMMIT").unwrap();
    }
    let batch = scalar_json(
        &mut db,
        "SELECT broker_consume('events', 'g', 'w', 10, 30000)",
    );
    let payloads: Vec<_> = batch
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["payload"].clone())
        .collect();
    assert_eq!(
        payloads,
        vec![serde_json::json!({"id":1}), serde_json::json!({"id":3})]
    );
}
