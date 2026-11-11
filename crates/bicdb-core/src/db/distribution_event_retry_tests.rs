//! Row-version regression only; the authority here is a test double.
use super::*;
use crate::distribution_events::{
    execute_cluster_event, ClusterEventOperation, ClusterEventResponse, CLUSTER_EVENTS_COLLECTION,
};

#[derive(Debug)]
struct FinishMissingPrefix {
    database: std::sync::Weak<BicDb>,
    admissions: std::sync::atomic::AtomicUsize,
}
impl CommitAdmission for FinishMissingPrefix {
    fn admit(&self, intent: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
        assert_eq!(intent.mutations.len(), 1);
        self.admissions.fetch_add(1, AtomicOrdering::SeqCst);
        self.database.upgrade().unwrap().mark_committed_seq(2);
        Ok(CommitAdmissionTicket {
            authority: "row-version-fixture".into(),
            command_id: "retry".into(),
        })
    }
    fn local_applied(&self, _: &CommitAdmissionTicket, _: u64) -> Result<()> {
        Ok(())
    }
}

#[test]
fn identical_event_retry_uses_exact_current_row_above_contiguous_watermark() {
    let root = tempfile::tempdir().unwrap();
    let mut database = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    database
        .create_collection(CLUSTER_EVENTS_COLLECTION)
        .unwrap();
    let expected = serde_json::json!({"current":true});
    let mut seed = database.begin_transaction().unwrap();
    seed.insert(
        CLUSTER_EVENTS_COLLECTION,
        Record::new("fixture/retry").with_metadata(serde_json::json!({"old":true})),
    )
    .unwrap();
    assert_eq!(database.commit_buffered_transaction(&mut seed).unwrap(), 1);
    drop(seed);
    // Model a real reserved and WAL-enqueued lower commit whose application is
    // pending. The retry must not lift a global floor merely to overwrite a row.
    assert_eq!(database.commit_seq.fetch_add(1, AtomicOrdering::SeqCst), 1);
    database.tx_log.enqueue(2, Vec::new());
    let mut later = database.begin_transaction_after(3).unwrap();
    later
        .insert(
            CLUSTER_EVENTS_COLLECTION,
            Record::new("fixture/retry").with_metadata(expected.clone()),
        )
        .unwrap();
    assert_eq!(database.commit_buffered_transaction(&mut later).unwrap(), 3);
    drop(later);
    assert_eq!(database.current_visibility_watermark(), 1);
    let store_root = tempfile::tempdir().unwrap();
    let store = crate::DistributionStore::initialize_at(
        store_root.path(),
        crate::DistributionConfig {
            enabled: true,
            initial_ranges: 1,
            replication_factor: 1,
            ..Default::default()
        },
        false,
        1,
    )
    .unwrap();
    let range = store.topology().ranges.values().next().unwrap().clone();
    let database = Arc::new(database);
    let authority = Arc::new(FinishMissingPrefix {
        database: Arc::downgrade(&database),
        admissions: std::sync::atomic::AtomicUsize::new(0),
    });
    database.install_commit_admission(authority.clone());
    let worker = Arc::clone(&database);
    // A different thread has no read-your-own-commit floor for sequence3.
    let result = std::thread::spawn(move || {
        execute_cluster_event(
            &worker,
            &range,
            ClusterEventOperation::Append {
                namespace: "fixture".into(),
                key: "retry".into(),
                value: expected,
            },
        )
    })
    .join()
    .unwrap();
    assert_eq!(
        result.unwrap(),
        ClusterEventResponse::Appended { inserted: false }
    );
    assert_eq!(authority.admissions.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(database.current_visibility_watermark(), 4);
    database.clear_commit_admission();
}
