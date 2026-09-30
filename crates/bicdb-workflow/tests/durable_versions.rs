use bicdb_core::{BicDb, ConsumeOptions, DbConfig, NackOptions, Record};
use bicdb_script::Limits;
use bicdb_workflow::*;
use serde_json::json;

fn context() -> JobContext {
    JobContext {
        tenant: "clinic-a".into(),
        principal: "eligibility-worker".into(),
        roles: vec!["billing".into()],
        trace_id: "trace-1".into(),
    }
}

#[test]
fn stored_javascript_and_commit_only_jobs_pin_versions_across_activation_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = DbConfig::default().with_fsync(true);
    let mut db = BicDb::open_with_config(dir.path(), config.clone()).unwrap();
    initialize(&mut db).unwrap();
    initialize(&mut db).unwrap();
    db.create_collection("appointments").unwrap();
    let v1 = publish_version(
        &mut db,
        "clinic-a",
        "eligibility",
        "v1",
        Language::TypeScript,
        "function workflow(event: {id:string}) { return {version:1, id:event.id}; }",
        &Limits::default(),
    )
    .unwrap();
    assert!(!v1.executable.contains("id:string"));
    publish_version(
        &mut db,
        "clinic-a",
        "eligibility",
        "v2",
        Language::TypeScript,
        "function workflow(event: {id:string}) { return {version:2, id:event.id}; }",
        &Limits::default(),
    )
    .unwrap();
    assert!(publish_version(
        &mut db,
        "clinic-a",
        "eligibility",
        "v1",
        Language::TypeScript,
        "function workflow() { return 9; }",
        &Limits::default()
    )
    .is_err());
    let mut tx = db.begin_transaction().unwrap();
    activate_version(&mut tx, "clinic-a", "eligibility", "v1").unwrap();
    tx.commit().unwrap();
    let mut cancelled = db.begin_transaction().unwrap();
    enqueue_on_commit(
        &mut cancelled,
        "eligibility",
        "cancelled",
        json!({"id":"cancelled"}),
        context(),
    )
    .unwrap();
    cancelled.rollback().unwrap();
    assert!(db
        .broker()
        .consume(QUEUE, "workers", "worker-a", ConsumeOptions::default())
        .unwrap()
        .is_empty());
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "appointments",
        Record::new("a1").with_metadata(json!({"status":"booked"})),
    )
    .unwrap();
    enqueue_on_commit(
        &mut tx,
        "eligibility",
        "booked-1",
        json!({"id":"a1","tenant":"attacker"}),
        context(),
    )
    .unwrap();
    assert!(db
        .broker()
        .consume(QUEUE, "workers", "worker-a", ConsumeOptions::default())
        .unwrap()
        .is_empty());
    tx.commit().unwrap();
    let mut tx = db.begin_transaction().unwrap();
    activate_version(&mut tx, "clinic-a", "eligibility", "v2").unwrap();
    enqueue_on_commit(
        &mut tx,
        "eligibility",
        "booked-2",
        json!({"id":"a2"}),
        context(),
    )
    .unwrap();
    tx.commit().unwrap();
    let mut duplicate = db.begin_transaction().unwrap();
    enqueue_on_commit(
        &mut duplicate,
        "eligibility",
        "booked-1",
        json!({"id":"a1","tenant":"attacker"}),
        context(),
    )
    .unwrap();
    duplicate.commit().unwrap();
    let mut changed = db.begin_transaction().unwrap();
    assert!(enqueue_on_commit(
        &mut changed,
        "eligibility",
        "booked-1",
        json!({"id":"different"}),
        context()
    )
    .is_err());
    changed.rollback().unwrap();
    drop(db);
    let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
    assert!(db.get("appointments", "a1").unwrap().is_some());
    let deliveries = db
        .broker()
        .consume(
            QUEUE,
            "workers",
            "worker-a",
            ConsumeOptions {
                max_messages: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    for (index, delivery) in deliveries.iter().enumerate() {
        let job: WorkflowJob = serde_json::from_value(delivery.payload.clone()).unwrap();
        let script = load_job_script(&db, &job).unwrap();
        let result = bicdb_script::execute(
            &script.executable,
            &job.event,
            &Limits::default(),
            |_, _| Err("unexpected host call".into()),
        )
        .unwrap();
        assert_eq!(result["version"], json!(index + 1));
        assert_eq!(job.context.tenant, "clinic-a");
    }
    // Durable retry preserves the original pinned version after activation.
    db.broker()
        .nack(
            QUEUE,
            "workers",
            "worker-a",
            deliveries[0].message_id,
            NackOptions {
                requeue: true,
                delay_ms: Some(0),
                error: Some("HTTP 429".into()),
            },
        )
        .unwrap();
    db.broker()
        .ack(QUEUE, "workers", "worker-a", deliveries[1].message_id)
        .unwrap();
    let retry = db
        .broker()
        .consume(QUEUE, "workers", "worker-b", ConsumeOptions::default())
        .unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].attempts, 2);
    assert_eq!(retry[0].payload["version"], "v1");
    let mut forged: WorkflowJob = serde_json::from_value(retry[0].payload.clone()).unwrap();
    forged.context.tenant = "clinic-b".into();
    assert!(load_job_script(&db, &forged).is_err());
}
