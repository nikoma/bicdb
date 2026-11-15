//! Real WAL/apply scheduling controls for issue 205. These exercise the
//! unchanged five-second visibility wait, not a synthetic watermark or failure.
use super::*;
use crate::memory_index::{MemoryIndexMode, MemoryJobStatus, ModelRegistryEntry};
use serde_json::json;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

const A_COLLECTION: &str = "visibility_lower";
const B_COLLECTION: &str = "visibility_higher";
const WATCHDOG: Duration = Duration::from_secs(15);

/// Declared inside the scoped-thread body, so unwinding releases A before the
/// scope joins either writer. This never changes a database deadline or state.
struct ReleaseLower(Option<Sender<()>>);

impl ReleaseLower {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for ReleaseLower {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug)]
struct HigherReturn {
    transaction: TransactionId,
    observer: TransactionOutcomeObserver,
    outcome_at_return: TransactionOutcome,
    result: Result<u64>,
    elapsed: Duration,
    thread_floor_before: u64,
    thread_floor_after: u64,
    retained_owner_state_before_drop: Option<TxState>,
}

fn receive<T>(receiver: &Receiver<T>, stage: &str) -> T {
    receiver
        .recv_timeout(WATCHDOG)
        .unwrap_or_else(|error| panic!("actual {stage} not reached: {error}"))
}

fn full_record(id: &str, ordinal: i64) -> Record {
    Record::new(id)
        .with_metadata(json!({
            "ordinal": ordinal,
            "body": format!("exact original payload for {id}"),
            "nested": {"present_null": null, "values": [ordinal, 17]}
        }))
        .with_payload(vec![0, ordinal as u8, 255, 13, 10])
        .with_timestamp(1_700_000_000 + ordinal)
}

fn setup(memory_job: bool) -> (tempfile::TempDir, BicDb, DbConfig) {
    let directory = tempfile::tempdir().unwrap();
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(true)
        .with_secondary_indexes(true);
    let mut db = BicDb::open_with_config(directory.path(), config.clone()).unwrap();
    for collection in [A_COLLECTION, B_COLLECTION] {
        db.create_collection(collection).unwrap();
        db.create_index(IndexDefinition {
            name: format!("{collection}_ordinal"),
            collection: collection.into(),
            fields: vec![IndexField::MetadataPath(vec!["ordinal".into()])],
            unique: false,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
    }
    if memory_job {
        db.register_embedding_model(ModelRegistryEntry::local_test("visibility_model", 16))
            .unwrap();
        db.create_memory_index(
            "visibility_body",
            B_COLLECTION,
            "body",
            "visibility_model",
            MemoryIndexMode::Sync,
        )
        .unwrap();
        assert!(db.memory_index_jobs().is_empty());
    }
    (directory, db, config)
}

fn read_full_pair_on_fresh_thread(db: &BicDb, expected_a: &Record, expected_b: &Record) {
    // A distinct reader thread cannot inherit either writer's thread-local
    // success floor and accidentally hide a noncontiguous global watermark.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let transaction = db.begin_transaction().unwrap();
                assert_eq!(
                    transaction
                        .get(A_COLLECTION, &expected_a.id)
                        .unwrap()
                        .as_deref(),
                    Some(expected_a)
                );
                assert_eq!(
                    transaction
                        .get(B_COLLECTION, &expected_b.id)
                        .unwrap()
                        .as_deref(),
                    Some(expected_b)
                );
            })
            .join()
            .unwrap();
    });
}

fn verify_pair_indexes(db: &BicDb) {
    for collection in [A_COLLECTION, B_COLLECTION] {
        let report = db.verify_index(&format!("{collection}_ordinal")).unwrap();
        assert!(report.valid, "actual secondary index invalid: {report:?}");
        assert_eq!(report.expected_records, 1);
        assert_eq!(report.indexed_records, 1);
    }
}

fn require_reader_without_unknown_job_capability_refuses(db: &BicDb) {
    let result = crate::format::test_reader_without_memory_job_outcome_unknown(db.data_path());
    eprintln!(
        "ISSUE205_UNKNOWN_JOB_READER_FENCE {}",
        json!({
            "result": format!("{result:?}"),
            "scope": "current FORMAT validator with only new capability removed; NOT old-binary execution"
        })
    );
    match result {
        Err(BicDbError::FormatCompatibility(detail)) => assert_eq!(
            detail,
            "database requires unknown format feature flag `memory_job_outcome_unknown_v1`"
        ),
        other => panic!("missing unknown-job reader capability must refuse: {other:?}"),
    }
}

fn exercise_original_visibility_wait(exhaust_wait: bool, memory_job: bool) {
    exercise_visibility_wait_with_actual_owner(exhaust_wait, memory_job, false);
}

fn exercise_visibility_wait_with_actual_owner(
    exhaust_wait: bool,
    memory_job: bool,
    retain_actual_owner: bool,
) {
    let (directory, db, config) = setup(memory_job);
    let original_a = full_record("original-lower", 1);
    let original_b = full_record("original-higher", 2);
    let mut expected_b = original_b.clone();
    std::thread::scope(|scope| {
        let (lower_durable_sender, lower_durable) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        // Drop before the enclosing scoped threads' implicit joins on panic.
        let mut release_lower = ReleaseLower(Some(release_sender));
        let (higher_applied_sender, higher_applied) = mpsc::channel();
        let (higher_return_sender, higher_return) = mpsc::channel();
        let lower = scope.spawn(|| {
            let mut transaction = db.begin_transaction().unwrap();
            assert_eq!(transaction.isolation(), TransactionIsolation::ReadCommitted);
            transaction
                .insert(A_COLLECTION, original_a.clone())
                .unwrap();
            let observer = transaction.observe_outcome();
            let hook_observer = observer.clone();
            let _hook = arm_after_real_wal(Box::new(move |sequence| {
                lower_durable_sender
                    .send((sequence, hook_observer))
                    .unwrap();
                // A safety watchdog only; the parent normally releases this
                // exact actual writer after observing B's original wait.
                release_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .expect("parent did not release actual lower WAL writer");
            }));
            (observer, transaction.commit_with_sequence())
        });
        let (lower_sequence, lower_observer) = receive(&lower_durable, "lower durable WAL");
        assert_eq!(
            lower_observer.outcome(),
            TransactionOutcome::Committing {
                sequence: lower_sequence
            }
        );
        let higher = scope.spawn(|| {
            let thread_floor_before = db.last_commit_seq_on_this_thread();
            let mut transaction = db.begin_transaction().unwrap();
            assert_eq!(transaction.isolation(), TransactionIsolation::ReadCommitted);
            transaction
                .insert(B_COLLECTION, original_b.clone())
                .unwrap();
            let transaction_id = transaction.id();
            let observer = transaction.observe_outcome();
            let _hook = arm_after_actual_apply(Box::new(move |sequence| {
                higher_applied_sender.send(sequence).unwrap();
            }));
            let started = Instant::now();
            let (result, retained_owner_state_before_drop) = if retain_actual_owner {
                let result = transaction.commit_with_sequence_in_place();
                // The owning transaction itself decides this distinction;
                // neither Unknown nor a receipt is a substitute for state().
                let state = transaction.state();
                drop(transaction);
                (result, Some(state))
            } else {
                (transaction.commit_with_sequence(), None)
            };
            let elapsed = started.elapsed();
            let outcome_at_return = observer.outcome();
            let thread_floor_after = db.last_commit_seq_on_this_thread();
            higher_return_sender
                .send(HigherReturn {
                    transaction: transaction_id,
                    observer,
                    outcome_at_return,
                    result,
                    elapsed,
                    thread_floor_before,
                    thread_floor_after,
                    retained_owner_state_before_drop,
                })
                .unwrap();
        });
        let higher_sequence = receive(&higher_applied, "higher actual applied position");
        assert_eq!(higher_sequence, lower_sequence + 1);
        assert!(db.current_visibility_watermark() < lower_sequence);
        let returned = if exhaust_wait {
            // B must reach the real production bound while A remains held.
            receive(
                &higher_return,
                "higher API return after original visibility wait",
            )
        } else {
            assert!(matches!(
                higher_return.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release_lower.release();
            receive(
                &higher_return,
                "higher successful return after closing actual gap",
            )
        };
        eprintln!(
            "ISSUE205_VISIBILITY_RETURN {}",
            json!({
                "exhaust_original_wait": exhaust_wait,
                "sync_memory_index": memory_job,
                "lower_actual_durable_sequence": lower_sequence,
                "higher_actual_applied_sequence": higher_sequence,
                "higher_original_transaction_id": returned.transaction.0,
                "actual_visible_prefix_at_return_observation": db.current_visibility_watermark(),
                "higher_result": format!("{:?}", returned.result),
                "higher_receipt_at_return": format!("{:?}", returned.outcome_at_return),
                "writer_thread_floor_before": returned.thread_floor_before,
                "writer_thread_floor_after": returned.thread_floor_after,
                "retained_owner_state_before_drop": format!("{:?}", returned.retained_owner_state_before_drop),
                "retained_owner_scope": "Core owner observation only; SQL committed-DDL cleanup integration NOT TESTED",
                "elapsed_millis": returned.elapsed.as_millis(),
                "durability_state": format!("{:?}", db.durability_state())
            })
        );
        db.check_recovery_state().unwrap();
        if retain_actual_owner {
            assert_eq!(
                returned.retained_owner_state_before_drop,
                Some(TxState::Committed)
            );
        } else {
            assert_eq!(returned.retained_owner_state_before_drop, None);
        }
        if exhaust_wait {
            assert!(db.current_visibility_watermark() < lower_sequence);
            assert!(returned.elapsed >= Duration::from_secs(5));
            match &returned.result {
                Err(BicDbError::CommitOutcomeUnknown {
                    sequence,
                    transaction_id,
                    incarnation,
                    ..
                }) => {
                    assert_eq!(*sequence, Some(higher_sequence));
                    assert_eq!(*transaction_id, Some(returned.transaction.0));
                    assert_eq!(incarnation, &db.incarnation);
                }
                other => panic!("unclosed actual prefix must not ACK: {other:?}"),
            }
            assert_eq!(
                returned.outcome_at_return,
                TransactionOutcome::RecoveryRequired {
                    sequence: higher_sequence
                }
            );
            assert_eq!(returned.thread_floor_after, returned.thread_floor_before);
            std::thread::scope(|reader_scope| {
                reader_scope
                    .spawn(|| {
                        let reader = db.begin_transaction().unwrap();
                        assert!(reader.visibility_watermark() < lower_sequence);
                        assert!(reader.get(A_COLLECTION, &original_a.id).unwrap().is_none());
                        assert!(reader.get(B_COLLECTION, &original_b.id).unwrap().is_none());
                    })
                    .join()
                    .unwrap();
            });
            if memory_job {
                let before = db.memory_index_jobs();
                assert_eq!(before.len(), 1);
                assert_eq!(before[0].record_id, original_b.id);
                assert_eq!(before[0].source_commit_seq, Some(higher_sequence));
                assert_eq!(before[0].status, MemoryJobStatus::Pending);
                assert!(before[0].error.is_none());
                let report = db.process_memory_index_jobs(8).unwrap();
                eprintln!(
                    "ISSUE205_PENDING_JOB {}",
                    json!({
                        "actual_source_floor": before[0].source_commit_seq,
                        "actual_job_id": before[0].job_id,
                        "processed": report.processed, "failed": report.failed,
                        "pending": report.pending, "failures": report.failures
                    })
                );
                assert_eq!(report.processed, 0);
                assert_eq!(report.failed, 0);
                assert_eq!(report.pending, 1);
                assert_eq!(db.memory_index_jobs(), before);
            }
            release_lower.release();
        } else {
            assert_eq!(returned.result.as_ref().unwrap(), &higher_sequence);
            assert!(returned.thread_floor_after >= higher_sequence);
            assert_eq!(
                returned.outcome_at_return,
                TransactionOutcome::Committed {
                    sequence: higher_sequence
                }
            );
            read_full_pair_on_fresh_thread(&db, &original_a, &original_b);
        }
        higher.join().unwrap();
        let (lower_receipt, lower_result) = lower.join().unwrap();
        assert_eq!(lower_result.unwrap(), lower_sequence);
        assert_eq!(
            lower_receipt.outcome(),
            TransactionOutcome::Committed {
                sequence: lower_sequence
            }
        );
        assert!(db.current_visibility_watermark() >= higher_sequence);
        if exhaust_wait {
            // Later convergence must not relabel an already returned unknown
            // as an application ACK or replace its original receipt identity.
            assert_eq!(returned.observer.outcome(), returned.outcome_at_return);
        }
    });
    read_full_pair_on_fresh_thread(&db, &original_a, &original_b);
    verify_pair_indexes(&db);
    if memory_job {
        let before = db.memory_index_jobs();
        let report = db.process_memory_index_jobs(8).unwrap();
        assert_eq!(report.processed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.pending, 0);
        let after = db.memory_index_jobs();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].job_id, before[0].job_id);
        assert_eq!(after[0].source_hash, before[0].source_hash);
        assert_eq!(after[0].source_commit_seq, before[0].source_commit_seq);
        assert_eq!(after[0].status, MemoryJobStatus::Indexed);
        assert!(after[0].error.is_none());
        let model = db.embedding_models().into_iter().next().unwrap();
        expected_b.vector = Some(
            model
                .embed(original_b.metadata["body"].as_str().unwrap())
                .unwrap(),
        );
        read_full_pair_on_fresh_thread(&db, &original_a, &expected_b);
        verify_pair_indexes(&db);
    }
    db.check_recovery_state().unwrap();
    // Ordinary same-format close/reopen checks retained rows and indexes;
    // this is not a SIGKILL or distributed/cold recovery qualification.
    db.close().unwrap();
    let reopened = BicDb::open_with_config(directory.path(), config).unwrap();
    read_full_pair_on_fresh_thread(&reopened, &original_a, &expected_b);
    verify_pair_indexes(&reopened);
    reopened.check_recovery_state().unwrap();
    reopened.close().unwrap();
}

#[test]
fn actual_fsynced_higher_commit_wait_expiry_is_exact_unknown_not_success() {
    exercise_original_visibility_wait(true, false);
}

#[test]
fn actual_gap_closes_within_original_wait_success_is_visible_on_fresh_thread() {
    exercise_original_visibility_wait(false, false);
}

#[test]
fn actual_pending_sync_memory_job_defers_until_its_original_commit_prefix_is_visible() {
    exercise_original_visibility_wait(true, true);
}

#[test]
fn actual_retained_owner_is_committed_before_drop_after_original_visibility_unknown() {
    exercise_visibility_wait_with_actual_owner(true, false, true);
}

#[test]
fn actual_derived_memory_write_unknown_retains_original_identity_without_worker_replay() {
    let (directory, mut db, config) = setup(false);
    let model = ModelRegistryEntry::local_test("derived_visibility_model", 16);
    db.register_embedding_model(model.clone()).unwrap();
    db.create_memory_index(
        "derived_visibility_body",
        B_COLLECTION,
        "body",
        "derived_visibility_model",
        MemoryIndexMode::Async,
    )
    .unwrap();
    let original_b = full_record("already-visible-source", 7);
    db.insert(B_COLLECTION, original_b.clone()).unwrap();
    let original_job = db.memory_index_jobs();
    assert_eq!(original_job.len(), 1);
    assert_eq!(original_job[0].status, MemoryJobStatus::Pending);
    let source_floor = original_job[0].source_commit_seq.unwrap();
    assert!(source_floor <= db.current_visibility_watermark());
    let original_a = full_record("lower-during-real-worker", 8);
    let retained_unknown = std::thread::scope(|scope| {
        let (lower_durable_sender, lower_durable) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let mut release_lower = ReleaseLower(Some(release_sender));
        let lower = scope.spawn(|| {
            let mut transaction = db.begin_transaction().unwrap();
            assert_eq!(transaction.isolation(), TransactionIsolation::ReadCommitted);
            transaction
                .insert(A_COLLECTION, original_a.clone())
                .unwrap();
            let observer = transaction.observe_outcome();
            let _hook = arm_after_real_wal(Box::new(move |sequence| {
                lower_durable_sender.send(sequence).unwrap();
                release_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .expect("parent did not release lower writer during actual memory work");
            }));
            (observer, transaction.commit_with_sequence())
        });
        let lower_sequence = receive(&lower_durable, "lower WAL before actual worker");
        let (derived_applied_sender, derived_applied) = mpsc::channel();
        let (worker_return_sender, worker_return) = mpsc::channel();
        let worker = scope.spawn(|| {
            let thread_floor_before = db.last_commit_seq_on_this_thread();
            let _hook = arm_after_actual_apply(Box::new(move |sequence| {
                derived_applied_sender.send(sequence).unwrap();
            }));
            let started = Instant::now();
            let result = db.process_memory_index_jobs(8);
            worker_return_sender
                .send((
                    result,
                    started.elapsed(),
                    thread_floor_before,
                    db.last_commit_seq_on_this_thread(),
                ))
                .unwrap();
        });
        let derived_sequence = receive(&derived_applied, "actual derived write applied position");
        assert_eq!(derived_sequence, lower_sequence + 1);
        let (result, elapsed, floor_before, floor_after) = receive(
            &worker_return,
            "original derived write visibility wait outcome",
        );
        let retained = db.memory_index_jobs();
        eprintln!(
            "ISSUE205_DERIVED_WRITE_RETURN {}",
            json!({
                "lower_actual_durable_sequence": lower_sequence,
                "derived_actual_applied_sequence": derived_sequence,
                "original_source_floor": source_floor,
                "original_job_id": original_job[0].job_id,
                "actual_worker_result": format!("{result:?}"),
                "retained_jobs": retained,
                "elapsed_millis": elapsed.as_millis(),
                "writer_thread_floor_before": floor_before,
                "writer_thread_floor_after": floor_after,
                "actual_visible_prefix": db.current_visibility_watermark(),
                "durability_state": format!("{:?}", db.durability_state())
            })
        );
        let report = result.unwrap();
        assert_eq!(report.processed, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(report.unresolved, 1);
        assert!(elapsed >= Duration::from_secs(5));
        assert_eq!(floor_before, floor_after);
        assert!(db.current_visibility_watermark() < lower_sequence);
        db.check_recovery_state().unwrap();
        assert_eq!(retained.len(), 1);
        let job = &retained[0];
        assert_eq!(job.job_id, original_job[0].job_id);
        assert_eq!(job.source_hash, original_job[0].source_hash);
        assert_eq!(job.source_commit_seq, Some(source_floor));
        assert_eq!(job.status, MemoryJobStatus::OutcomeUnknown);
        assert_eq!(job.unresolved_derived_writes.len(), 1);
        let uncertainty = &job.unresolved_derived_writes[0];
        assert_eq!(uncertainty.sequence, Some(derived_sequence));
        assert!(uncertainty.transaction_id.is_some());
        assert_eq!(uncertainty.incarnation, db.incarnation);
        assert!(!uncertainty.error.is_empty());
        require_reader_without_unknown_job_capability_refuses(&db);
        release_lower.release();
        worker.join().unwrap();
        let (lower_observer, lower_result) = lower.join().unwrap();
        assert_eq!(lower_result.unwrap(), lower_sequence);
        assert_eq!(
            lower_observer.outcome(),
            TransactionOutcome::Committed {
                sequence: lower_sequence
            }
        );
        assert!(db.current_visibility_watermark() >= derived_sequence);
        retained
    });
    let mut expected_b = original_b.clone();
    expected_b.vector = Some(
        model
            .embed(original_b.metadata["body"].as_str().unwrap())
            .unwrap(),
    );
    read_full_pair_on_fresh_thread(&db, &original_a, &expected_b);
    verify_pair_indexes(&db);
    let sequence_before_retry = db.last_commit_seq();
    let report = db.process_memory_index_jobs(8).unwrap();
    assert_eq!(report.processed, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(report.unresolved, 1);
    assert_eq!(db.last_commit_seq(), sequence_before_retry);
    assert_eq!(db.memory_index_jobs(), retained_unknown);
    db.close().unwrap();
    let reopened = BicDb::open_with_config(directory.path(), config).unwrap();
    assert_eq!(reopened.memory_index_jobs(), retained_unknown);
    require_reader_without_unknown_job_capability_refuses(&reopened);
    let sequence_before_retry = reopened.last_commit_seq();
    let report = reopened.process_memory_index_jobs(8).unwrap();
    assert_eq!(report.processed, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(report.unresolved, 1);
    assert_eq!(reopened.last_commit_seq(), sequence_before_retry);
    assert_eq!(reopened.memory_index_jobs(), retained_unknown);
    read_full_pair_on_fresh_thread(&reopened, &original_a, &expected_b);
    verify_pair_indexes(&reopened);
    reopened.check_recovery_state().unwrap();
    reopened.close().unwrap();
}

fn setup_actual_async_source(
    id: &str,
) -> (
    tempfile::TempDir,
    BicDb,
    DbConfig,
    Record,
    ModelRegistryEntry,
) {
    let (directory, mut db, config) = setup(false);
    let model = ModelRegistryEntry::local_test("fault_visibility_model", 16);
    db.register_embedding_model(model.clone()).unwrap();
    db.create_memory_index(
        "fault_visibility_body",
        B_COLLECTION,
        "body",
        "fault_visibility_model",
        MemoryIndexMode::Async,
    )
    .unwrap();
    let source = full_record(id, 11);
    db.insert(B_COLLECTION, source.clone()).unwrap();
    let jobs = db.memory_index_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, MemoryJobStatus::Pending);
    assert!(jobs[0].source_commit_seq.unwrap() <= db.current_visibility_watermark());
    (directory, db, config, source, model)
}

#[test]
fn actual_unknown_job_format_before_rename_failure_keeps_status_unpublished_and_open_fenced() {
    let (_directory, db, _config, source, _model) =
        setup_actual_async_source("real-format-failure-source");
    let original_job = db.memory_index_jobs();
    let jobs_path = db.data_path().join(DEFAULT_MEMORY_INDEX_JOBS);
    let before_jobs_bytes = std::fs::read(&jobs_path).unwrap();
    assert!(!db
        .format_metadata()
        .unwrap()
        .feature_flags
        .contains(crate::format::MEMORY_JOB_OUTCOME_UNKNOWN_FEATURE_FLAG));
    let a = full_record("lower-during-real-format-failure", 12);
    std::thread::scope(|scope| {
        let (a_sender, a_wal) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let mut release_a = ReleaseLower(Some(release_sender));
        let lower = scope.spawn(|| {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert(A_COLLECTION, a.clone()).unwrap();
            let observer = tx.observe_outcome();
            let hook_observer = observer.clone();
            let _hook = arm_after_real_wal(Box::new(move |sequence| {
                a_sender.send((sequence, hook_observer)).unwrap();
                release_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
            }));
            (observer, tx.commit_with_sequence())
        });
        let (a_sequence, a_observer) = receive(&a_wal, "A actual WAL before format failure");
        let (c_sender, c_apply) = mpsc::channel();
        let (worker_sender, worker_result) = mpsc::channel();
        let format_path = db.data_path().join(crate::format::FORMAT_METADATA_FILE);
        let worker = scope.spawn(|| {
            let fault = Rc::new(RefCell::new(None));
            let hook_fault = fault.clone();
            let _hook = arm_after_actual_apply(Box::new(move |sequence| {
                let actual = bicdb_page::test_faults::arm_io_failure(
                    "memory_jobs.outcome_unknown.format.rename",
                    &format_path,
                    1,
                    false,
                );
                *hook_fault.borrow_mut() = Some(actual);
                c_sender.send(sequence).unwrap();
            }));
            let started = Instant::now();
            let result = db.process_memory_index_jobs(8);
            let fault = fault.borrow_mut().take().expect("actual derived apply cut");
            let fired = fault.fired();
            drop(fault);
            worker_sender
                .send((result, fired, started.elapsed()))
                .unwrap();
        });
        let c_sequence = receive(&c_apply, "actual C apply before format failure");
        assert_eq!(c_sequence, a_sequence + 1);
        let (result, fired, elapsed) = receive(&worker_result, "actual failed format publication");
        let retained = db.memory_index_jobs();
        let after_jobs_bytes = std::fs::read(&jobs_path).unwrap();
        let format_has_bit = db
            .format_metadata()
            .unwrap()
            .feature_flags
            .contains(crate::format::MEMORY_JOB_OUTCOME_UNKNOWN_FEATURE_FLAG);
        eprintln!(
            "ISSUE205_REAL_FORMAT_RENAME_FAILURE {}",
            json!({
                "actual_original_job": original_job[0].job_id,
                "actual_A_sequence": a_sequence, "actual_C_sequence": c_sequence,
                "actual_result": format!("{result:?}"), "fault_fired": fired,
                "elapsed_millis": elapsed.as_millis(), "RAM_jobs": retained,
                "old_job_bytes_unchanged": before_jobs_bytes == after_jobs_bytes,
                "format_has_new_bit": format_has_bit,
                "actual_health": format!("{:?}", db.durability_state()),
                "scope": "same-open preservation/fence only; disk old-Pending crash/reopen recovery remains OPEN/NOT TESTED"
            })
        );
        assert_eq!(fired, 1);
        assert!(elapsed >= Duration::from_secs(5));
        assert_eq!(before_jobs_bytes, after_jobs_bytes);
        assert!(!format_has_bit);
        assert!(db.check_recovery_state().is_err());
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].job_id, original_job[0].job_id);
        assert_eq!(retained[0].status, MemoryJobStatus::OutcomeUnknown);
        assert_eq!(retained[0].unresolved_derived_writes.len(), 1);
        let actual = &retained[0].unresolved_derived_writes[0];
        assert_eq!(actual.sequence, Some(c_sequence));
        match result {
            Err(BicDbError::CommitOutcomeUnknown {
                sequence,
                transaction_id,
                incarnation,
                ..
            }) => {
                assert_eq!(sequence, Some(c_sequence));
                assert_eq!(transaction_id, actual.transaction_id);
                assert!(transaction_id.is_some());
                assert_eq!(incarnation, actual.incarnation);
                assert_eq!(incarnation, db.incarnation);
            }
            other => panic!("real semantic-barrier failure must preserve C outcome: {other:?}"),
        }
        // This is a genuine storage failure, not the healthy timeout case.
        // Never clear health or require the paused lower transaction to ACK.
        release_a.release();
        worker.join().unwrap();
        let (_, a_result) = lower.join().unwrap();
        assert!(matches!(a_result, Err(BicDbError::CommitOutcomeUnknown {
            sequence:Some(sequence), transaction_id:Some(transaction), ..
        }) if sequence == a_sequence && transaction == a_observer.transaction_id().0));
        assert_eq!(
            a_observer.outcome(),
            TransactionOutcome::RecoveryRequired {
                sequence: a_sequence
            }
        );
        assert_eq!(db.memory_index_jobs(), retained);
        assert_eq!(std::fs::read(&jobs_path).unwrap(), before_jobs_bytes);
        assert!(db.check_recovery_state().is_err());
        assert_eq!(source.id, original_job[0].record_id);
    });
}

#[test]
fn actual_older_pending_worker_snapshot_cannot_replay_after_other_worker_retains_unknown() {
    let (directory, db, config, original_b, model) =
        setup_actual_async_source("real-stale-cohort-source");
    let original_job = db.memory_index_jobs();
    let a = full_record("lower-during-real-stale-cohort", 13);
    let retained = std::thread::scope(|scope| {
        let (selected_sender, selected) = mpsc::channel();
        let (release_old_sender, release_old_receiver) = mpsc::channel();
        let mut release_old = ReleaseLower(Some(release_old_sender));
        let (old_sender, old_result) = mpsc::channel();
        let older = scope.spawn(|| {
            let _hook = arm_after_memory_job_selection(Box::new(move |ids| {
                selected_sender.send(ids).unwrap();
                release_old_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
            }));
            old_sender.send(db.process_memory_index_jobs(8)).unwrap();
        });
        assert_eq!(
            receive(&selected, "older actual Pending selection"),
            vec![original_job[0].job_id]
        );
        let (a_sender, a_wal) = mpsc::channel();
        let (release_a_sender, release_a_receiver) = mpsc::channel();
        let mut release_a = ReleaseLower(Some(release_a_sender));
        let lower = scope.spawn(|| {
            let mut tx = db.begin_transaction().unwrap();
            tx.insert(A_COLLECTION, a.clone()).unwrap();
            let _hook = arm_after_real_wal(Box::new(move |sequence| {
                a_sender.send(sequence).unwrap();
                release_a_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
            }));
            tx.commit_with_sequence()
        });
        let a_sequence = receive(&a_wal, "A WAL for selected-cohort race");
        let (c_sender, c_apply) = mpsc::channel();
        let (new_sender, new_result) = mpsc::channel();
        let newer = scope.spawn(|| {
            let _hook = arm_after_actual_apply(Box::new(move |sequence| {
                c_sender.send(sequence).unwrap();
            }));
            new_sender.send(db.process_memory_index_jobs(8)).unwrap();
        });
        let c_sequence = receive(&c_apply, "newer actual derived apply");
        assert_eq!(c_sequence, a_sequence + 1);
        let actual_new_result = receive(&new_result, "newer actual uncertainty publication");
        let retained = db.memory_index_jobs();
        eprintln!(
            "ISSUE205_REAL_STALE_COHORT_NEW_OUTCOME {}",
            json!({
                "original_job_id": original_job[0].job_id,
                "actual_A_sequence": a_sequence, "actual_C_sequence": c_sequence,
                "actual_newer_result": format!("{actual_new_result:?}"),
                "retained_jobs": retained
            })
        );
        let report = actual_new_result.unwrap();
        assert_eq!(report.processed, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(report.unresolved, 1);
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].status, MemoryJobStatus::OutcomeUnknown);
        assert_eq!(
            retained[0].unresolved_derived_writes[0].sequence,
            Some(c_sequence)
        );
        release_a.release();
        newer.join().unwrap();
        assert_eq!(lower.join().unwrap().unwrap(), a_sequence);
        let before_sequence = db.last_commit_seq();
        let paged = db.paged_records.as_ref().unwrap();
        let before_xid = paged.store().transactions().next_xid();
        let before_lsn = paged.store().wal().next_lsn();
        release_old.release();
        let result = receive(&old_result, "older selected cohort fresh-current refusal");
        eprintln!(
            "ISSUE205_REAL_STALE_COHORT_RETURN {}",
            json!({
                "actual_result": format!("{result:?}"),
                "core_sequence_before": before_sequence, "core_sequence_after": db.last_commit_seq(),
                "native_xid_before": before_xid, "native_xid_after": paged.store().transactions().next_xid(),
                "native_lsn_before": before_lsn, "native_lsn_after": paged.store().wal().next_lsn(),
                "scope": "actual old Pending snapshot resumed after current same-job Unknown; not a fake status update"
            })
        );
        let report = result.unwrap();
        assert_eq!(report.processed, 0);
        assert_eq!(report.failed, 0);
        assert_eq!(report.deferred, 1);
        assert_eq!(report.claim_deferred, 0);
        assert_eq!(report.unresolved, 1);
        assert_eq!(db.last_commit_seq(), before_sequence);
        assert_eq!(paged.store().transactions().next_xid(), before_xid);
        assert_eq!(paged.store().wal().next_lsn(), before_lsn);
        assert_eq!(db.memory_index_jobs(), retained);
        assert!(db.memory_job_in_flight.lock().is_empty());
        older.join().unwrap();
        retained
    });
    let expected_b = original_b.clone().with_vector(
        model
            .embed(original_b.metadata["body"].as_str().unwrap())
            .unwrap(),
    );
    read_full_pair_on_fresh_thread(&db, &a, &expected_b);
    verify_pair_indexes(&db);
    db.close().unwrap();
    let reopened = BicDb::open_with_config(directory.path(), config).unwrap();
    assert_eq!(reopened.memory_index_jobs(), retained);
    read_full_pair_on_fresh_thread(&reopened, &a, &expected_b);
    verify_pair_indexes(&reopened);
    reopened.close().unwrap();
}
