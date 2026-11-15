//! A known competing listener distinguishes a bind failure from migration or
//! replication loss. It does not identify the unretained owner in main519 CI.
use super::*;
use bicdb_app_host::database::JOB_QUEUE;

#[test]
fn migrated_jobs_bind_failure_retains_original_error_and_allows_fresh_same_identity_open() {
    let root = tempfile::tempdir().unwrap();
    let initial_ports = ports(1);
    {
        let mut legacy = BicDb::open(root.path().join("jobs")).unwrap();
        legacy
            .broker()
            .publish(JOB_QUEUE, json!({"work": 1}), json!({}))
            .unwrap();
    }
    assert!(Database::open_replicated(root.path(), config(0, &initial_ports)).is_err());
    let no_listener = TcpListener::bind(("127.0.0.1", initial_ports[0])).unwrap();
    drop(no_listener);
    assert!(!root.path().join("state-replica.json").exists());
    {
        let mut standalone = Database::open(root.path()).unwrap();
        assert_eq!(standalone.job_status().unwrap()["queued"], 1);
    }
    // This actual listener is acquired AFTER the standalone migration. Retain
    // it across the actual failed open, so no released-port race mints the cut.
    let competitor = TcpListener::bind("127.0.0.1:0").unwrap();
    let actual_address = competitor.local_addr().unwrap();
    let replica = config(0, &[actual_address.port()]);
    let original_error = match Database::open_replicated(root.path(), replica.clone()) {
        Err(error) => error,
        Ok(database) => {
            database.request_shutdown();
            panic!("the actual retained competing listener did not cause a bind refusal");
        }
    };
    let original_error_text = format!("{original_error:#}");
    assert!(matches!(
        original_error.downcast_ref::<bicdb_pgwire::PgWireError>(),
        Some(bicdb_pgwire::PgWireError::Io(error))
            if error.kind() == std::io::ErrorKind::AddrInUse
    ));
    assert_eq!(competitor.local_addr().unwrap(), actual_address);
    // Initialization precedes HA listener binding: a failed open can leave a
    // durable replica binding. Do not claim this failure had no durable effects.
    let binding: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("state-replica.json")).unwrap())
            .unwrap();
    assert_eq!(binding["format"], 1);
    assert_eq!(binding["cluster_id"], "application-state-test");
    assert_eq!(binding["node_id"], "n0");
    eprintln!(
        "ACTUAL_MIGRATED_STATE_PORT_OWNER {}",
        json!({"address":actual_address.to_string(), "original_error":original_error_text,
            "actual_error_kind":"AddrInUse", "competing_listener_still_owned":true,
            "binding_after_failed_open":binding, "historical_CI_owner_identified":false})
    );
    // Release only this test's listener, then issue a DISTINCT normal open with
    // the exact same configuration. This is not a retry of an unknown mutation.
    drop(competitor);
    let mut database = Database::open_replicated(root.path(), replica).unwrap();
    assert_eq!(format!("{original_error:#}"), original_error_text);
    database.prepare(&package()).unwrap();
    assert_eq!(
        database.consume("worker", 1000).unwrap().unwrap().payload,
        json!({"work": 1})
    );
    let mut sync_package = package();
    sync_package.manifest.sync.insert(
        "effects".into(),
        bicdb_app_host::manifest::TableSync::AppendOnly,
    );
    assert!(database.prepare(&sync_package).is_err());
    assert!(database.migrate(&sync_package).is_err());
    assert!(database
        .configure_sync(&sync_package.manifest.sync)
        .is_err());
    database.request_shutdown();
}
