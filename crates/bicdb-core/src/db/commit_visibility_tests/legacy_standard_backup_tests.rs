//! The ORIGINAL Standard path-based producer at real Core-WAL/page cuts.
//! This deliberately expects success/verify/restore. An observed changed-file
//! refusal remains a failing regression, never an expected-error pass.
use super::*;
use crate::backup::{BackupCreateOptions, BackupCreateReport, BackupRestoreOptions};
use serde_json::json;
use sha2::Digest;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const COLLECTION: &str = "legacy_standard_cut";
const INDEX: &str = "legacy_standard_cut_ordinal";
const KEY: &str = "synthetic-test-passphrase-standard-cut";
const WATCHDOG: Duration = Duration::from_secs(15);

#[derive(Debug)]
enum Event {
    DurableWal {
        transaction: u64,
        sequence: u64,
        actual_thread: String,
        live_shared_guards: Vec<crate::storage::backup_gate_observation::GuardSnapshot>,
    },
    WriterContinued {
        sequence: u64,
        release_timed_out: bool,
    },
    Gate(crate::storage::backup_gate_observation::Observation),
    Manifest {
        point: crate::backup::legacy_standard_observation::Observation,
        actual_thread: String,
        live_exclusive_guards: Vec<crate::storage::backup_gate_observation::GuardSnapshot>,
        callback_interval: CallbackIntervalObservation,
    },
    BackupCallbackReturned {
        timed_out: bool,
    },
}
// This observes only the actual callback interval. It cannot establish an EX
// lock; it can only refuse a cut once the callback starts returning. The SAME
// descriptor-bound actual guard snapshot must independently be live.
#[derive(Clone, Debug)]
struct CallbackIntervalObservation(Arc<std::sync::atomic::AtomicBool>);
impl CallbackIntervalObservation {
    fn is_waiting(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
struct CallbackExit {
    interval: CallbackIntervalObservation,
}
impl CallbackExit {
    fn entered() -> Self {
        Self {
            interval: CallbackIntervalObservation(Arc::new(std::sync::atomic::AtomicBool::new(
                true,
            ))),
        }
    }
    fn observation(&self) -> CallbackIntervalObservation {
        self.interval.clone()
    }
}
impl Drop for CallbackExit {
    fn drop(&mut self) {
        // Clear BEFORE the callback returns and the real EX can be destroyed.
        self.interval
            .0
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct WriterOutcome {
    transaction: u64,
    state_before_drop: TxState,
    receipt: String,
    result: Result<u64>,
}
struct Cleanup {
    release_a: Option<Sender<()>>,
    release_backup: Option<Sender<()>>,
    writer: Option<JoinHandle<Result<WriterOutcome>>>,
    backup: Option<JoinHandle<Result<BackupCreateReport>>>,
}
impl Cleanup {
    fn release_a(&mut self) {
        if let Some(send) = self.release_a.take() {
            let _ = send.send(());
        }
    }
    fn release_backup(&mut self) {
        if let Some(send) = self.release_backup.take() {
            let _ = send.send(());
        }
    }
    fn join(&mut self) -> (Result<WriterOutcome>, Result<BackupCreateReport>) {
        self.release_a();
        self.release_backup();
        // Both callbacks are released BEFORE either join, including unwind.
        // The callback watchdogs do NOT bound these joins or kernel I/O: an
        // engine deadlock still requires the outer test-job process watchdog.
        let writer = self
            .writer
            .take()
            .ok_or_else(|| failure("actual writer not spawned"))
            .and_then(|thread| thread.join().map_err(|_| failure("actual writer panicked")))
            .and_then(|result| result);
        let backup = self
            .backup
            .take()
            .ok_or_else(|| failure("actual backup not spawned"))
            .and_then(|thread| thread.join().map_err(|_| failure("actual backup panicked")))
            .and_then(|result| result);
        (writer, backup)
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.release_a();
        self.release_backup();
        if let Some(thread) = self.writer.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.backup.take() {
            let _ = thread.join();
        }
    }
}
fn failure(detail: &str) -> BicDbError {
    BicDbError::Backup(format!("actual Standard backup cut: {detail}"))
}
fn append_trace(event: &Event, trace: &mut Vec<serde_json::Value>) {
    let gate = match event {
        Event::Gate(point) => json!({
            "guard_id":point.guard_id,"gate":point.gate,"descriptor":point.descriptor,
            "file_identity":point.file_identity,"exclusive":point.exclusive,
            "acquired":point.acquired,"released_after_actual_File_drop":point.released,
            "actual_thread":point.actual_thread,
        }),
        _ => serde_json::Value::Null,
    };
    let writer_watchdog_timed_out = match event {
        Event::WriterContinued {
            release_timed_out, ..
        } => Some(*release_timed_out),
        _ => None,
    };
    let backup_callback_timed_out = match event {
        Event::BackupCallbackReturned { timed_out } => Some(*timed_out),
        _ => None,
    };
    trace.push(
        json!({"actual_event":format!("{event:?}"),"gate_lifetime":gate,
        "writer_watchdog_timed_out":writer_watchdog_timed_out,
        "backup_callback_timed_out":backup_callback_timed_out}),
    );
}
fn receive(events: &Receiver<Event>, trace: &mut Vec<serde_json::Value>) -> Result<Event> {
    let event = events
        .recv_timeout(WATCHDOG)
        .map_err(|_| failure("handshake prerequisite not reached; INCONCLUSIVE"))?;
    append_trace(&event, trace);
    Ok(event)
}
fn record(id: &str, ordinal: i64) -> Record {
    Record::new(id)
        .with_metadata(
            json!({"ordinal":ordinal,"full_body":format!("original {id}"),
        "nested":{"present_null":null,"values":[ordinal,17]}}),
        )
        .with_payload(vec![0, ordinal as u8, 255, 13, 10])
        .with_timestamp(1_700_000_000 + ordinal)
}
fn gate_identity(path: &Path) -> Result<Option<(u64, u64)>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let actual = std::fs::metadata(path)?;
        Ok(Some((actual.dev(), actual.ino())))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}
fn file_point(path: &Path) -> Result<(u64, String)> {
    let mut input = std::fs::File::open(path)?;
    let mut bytes = [0u8; 16 * 1024];
    let mut hash = sha2::Sha256::new();
    let mut count = 0u64;
    loop {
        let n = input.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
        count += n as u64;
    }
    Ok((count, hex::encode(hash.finalize())))
}

fn require_live_manifest_ex(
    snapshots: &[crate::storage::backup_gate_observation::GuardSnapshot],
    acquired: &crate::storage::backup_gate_observation::Observation,
    callback: &CallbackIntervalObservation,
    backup_thread: &str,
    root: &Path,
    stage: &str,
    trace: &mut Vec<serde_json::Value>,
) -> Result<()> {
    let exact_live_ex = snapshots.len() == 1
        && snapshots.iter().all(|guard| {
            guard.exclusive
                && guard.gate == root.join(crate::storage::BACKUP_WRITE_GATE)
                && acquired.exclusive
                && acquired.acquired
                && !acquired.released
                && acquired.gate == guard.gate
                && acquired.actual_thread == backup_thread
                && Some(guard.guard_id) == acquired.guard_id
                && guard.file_identity.is_some()
                && guard.file_identity == acquired.file_identity
                && guard.descriptor.is_some()
                && guard.descriptor == acquired.descriptor
                && guard.acquiring_thread == backup_thread
                && guard.is_live()
        });
    // Read the monotonic actual callback interval LAST. It clears before the
    // callback returns, covering the File-close -> marker-update interval;
    // the interval observation alone never substitutes for the real EX checks.
    let callback_waiting = callback.is_waiting();
    // Do not emit thousands of redundant polling records. Failed checks and
    // both at-manifest endpoints are retained; a changed read is logged below
    // only AFTER both corresponding poll endpoints have actually passed.
    if !exact_live_ex || !callback_waiting || stage.contains("at_manifest") {
        trace.push(json!({"actual_EX_liveness_check":stage,
            "exact_same_backup_EX_live":exact_live_ex,"actual_callback_still_waiting":callback_waiting,
            "guard_id":acquired.guard_id,"descriptor":acquired.descriptor,
            "file_identity":acquired.file_identity,"actual_backup_thread":backup_thread}));
    }
    if !exact_live_ex || !callback_waiting {
        return Err(failure(&format!(
            "actual same EX/callback lifetime lost at {stage}; INCONCLUSIVE"
        )));
    }
    Ok(())
}

#[test]
fn legacy_standard_backup_at_actual_core_wal_before_page_apply_restores_full_indexed_rows() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<crate::storage::BackupGateGuard>();
    let source = tempfile::tempdir().unwrap();
    let archive_dir = tempfile::tempdir().unwrap();
    let restored_dir = tempfile::tempdir().unwrap();
    let root = source.path().canonicalize().unwrap();
    let archive = archive_dir.path().join("same-standard.bicbackup");
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::ServerPaged)
        .with_fsync(true)
        .with_secondary_indexes(true);
    let baseline = (0..100)
        .map(|i| record(&format!("baseline-{i:03}"), i))
        .collect::<Vec<_>>();
    let a = record("actual-A-after-Core-WAL", 100);
    {
        let mut seed = BicDb::open_with_config(&root, config.clone()).unwrap();
        seed.create_collection(COLLECTION).unwrap();
        seed.create_index(IndexDefinition {
            name: INDEX.into(),
            collection: COLLECTION.into(),
            fields: vec![IndexField::MetadataPath(vec!["ordinal".into()])],
            unique: false,
            nulls_not_distinct: false,
            kind: IndexKind::BTree,
            predicate: None,
            exclusion: None,
        })
        .unwrap();
        for record in &baseline {
            seed.insert(COLLECTION, record.clone()).unwrap();
        }
        seed.close().unwrap();
    }
    // All 100 actual indexed rows are durable BEFORE installing either hook.
    let db = Arc::new(BicDb::open_with_config(&root, config.clone()).unwrap());
    let before = db.verify_index(INDEX).unwrap();
    assert!(before.valid);
    assert_eq!(before.indexed_records, 100);
    let actual_gate_identity =
        gate_identity(&root.join(crate::storage::BACKUP_WRITE_GATE)).unwrap();
    let (send, events) = mpsc::channel();
    let (release_a, resume_a) = mpsc::channel();
    let (release_backup, resume_backup) = mpsc::channel();
    let mut cleanup = Cleanup {
        release_a: Some(release_a),
        release_backup: Some(release_backup),
        writer: None,
        backup: None,
    };
    let writer_db = db.clone();
    let writer_root = root.clone();
    let writer_a = a.clone();
    let writer_events = send.clone();
    cleanup.writer = Some(std::thread::spawn(move || -> Result<WriterOutcome> {
        let observed = writer_events.clone();
        let _gate = crate::storage::backup_gate_observation::arm(
            &writer_root,
            false,
            Box::new(move |point| {
                let _ = observed.send(Event::Gate(point));
            }),
        )?;
        let mut tx = writer_db.begin_transaction()?;
        tx.insert(COLLECTION, writer_a)?;
        let transaction = tx.id().0;
        let receipt = tx.observe_outcome();
        let _wal = arm_after_real_wal(Box::new(move |sequence| {
            let _ = writer_events.send(Event::DurableWal {
                transaction,
                sequence,
                actual_thread: format!("{:?}", std::thread::current().id()),
                live_shared_guards: crate::storage::backup_gate_observation::live_shared_guards(
                    &writer_root,
                ),
            });
            let release_timed_out = resume_a.recv_timeout(WATCHDOG).is_err();
            let _ = writer_events.send(Event::WriterContinued {
                sequence,
                release_timed_out,
            });
        }));
        let result = tx.commit_with_sequence_in_place();
        let state_before_drop = tx.state();
        drop(tx);
        Ok(WriterOutcome {
            transaction,
            state_before_drop,
            receipt: format!("{:?}", receipt.outcome()),
            result,
        })
    }));
    let mut trace = Vec::new();
    let mut reached = json!({"classification":"INCONCLUSIVE until actual prerequisites complete"});
    let schedule = (|| -> Result<()> {
        let (transaction, sequence, actual_a_thread, actual_a_shared_guards) = loop {
            if let Event::DurableWal {
                transaction,
                sequence,
                actual_thread,
                live_shared_guards,
            } = receive(&events, &mut trace)?
            {
                break (transaction, sequence, actual_thread, live_shared_guards);
            }
        };
        reached["actual_A_thread"] = json!(actual_a_thread);
        reached["actual_A_live_shared_guards_at_WAL"] = json!(actual_a_shared_guards
            .iter()
            .map(|guard| json!({
                "guard_id":guard.guard_id,"gate":guard.gate,"descriptor":guard.descriptor,
                "file_identity":guard.file_identity,"acquiring_thread":guard.acquiring_thread,
                "observed_live":guard.is_live(),
            }))
            .collect::<Vec<_>>());
        reached["actual_A_transaction"] = json!(transaction);
        reached["actual_A_durable_Core_sequence"] = json!(sequence);
        let backup_root = root.clone();
        let backup_path = archive.clone();
        let backup_events = send.clone();
        cleanup.backup = Some(std::thread::spawn(move || -> Result<BackupCreateReport> {
            let observed = backup_events.clone();
            let _gate = crate::storage::backup_gate_observation::arm(
                &backup_root,
                true,
                Box::new(move |point| {
                    let _ = observed.send(Event::Gate(point));
                }),
            )?;
            let manifest_root = backup_root.clone();
            let _manifest = crate::backup::legacy_standard_observation::arm(
                &backup_root,
                Box::new(move |point| {
                    let callback_exit = CallbackExit::entered();
                    let _ = backup_events.send(Event::Manifest {
                        point,
                        actual_thread: format!("{:?}", std::thread::current().id()),
                        live_exclusive_guards:
                            crate::storage::backup_gate_observation::live_exclusive_guards(
                                &manifest_root,
                            ),
                        callback_interval: callback_exit.observation(),
                    });
                    let result = resume_backup.recv_timeout(WATCHDOG);
                    drop(callback_exit);
                    let _ = backup_events.send(Event::BackupCallbackReturned {
                        timed_out: result.is_err(),
                    });
                    result.map_err(|_| failure("post-manifest release timed out; INCONCLUSIVE"))
                }),
            )?;
            // EXACT original producer/mode, not owned capture or fuzzy backup.
            crate::backup::create_backup(
                &backup_root,
                &backup_path,
                BackupCreateOptions {
                    passphrase: KEY.into(),
                    base_backup: None,
                },
            )
        }));
        let mut exclusive_acquired = false;
        let mut exclusive_contended_before_release = false;
        let mut actual_ex_acquisition = None;
        let (manifest, actual_backup_thread, actual_ex_guards, callback_interval) = loop {
            match receive(&events, &mut trace)? {
                Event::Gate(point) if point.exclusive => {
                    if point.gate != root.join(crate::storage::BACKUP_WRITE_GATE) {
                        return Err(failure("exclusive observer selected another actual root"));
                    }
                    if point.file_identity != actual_gate_identity {
                        return Err(failure(
                            "observed backup gate inode differs from actual source gate",
                        ));
                    }
                    if point.acquired {
                        reached["actual_EX_gate_file_identity"] = json!(point.file_identity);
                        exclusive_acquired = true;
                        actual_ex_acquisition = Some(point.clone());
                    } else if !point.released {
                        reached["actual_EX_contention_observed"] = json!(true);
                        // EX refusal alone can come from unrelated maintenance.
                        // Early release additionally requires an actual SAME A
                        // thread/root/descriptor guard STILL live at this cut.
                        let held_by_a = actual_a_shared_guards
                            .iter()
                            .filter(|guard| {
                                !guard.exclusive
                                    && guard.gate == point.gate
                                    && guard.file_identity.is_some()
                                    && guard.file_identity == actual_gate_identity
                                    && guard.descriptor.is_some()
                                    && guard.acquiring_thread == actual_a_thread
                                    && guard.is_live()
                            })
                            .map(|guard| guard.guard_id)
                            .collect::<Vec<_>>();
                        if !held_by_a.is_empty() {
                            reached["actual_A_held_shared_guard_ids_before_early_release"] =
                                json!(held_by_a);
                            exclusive_contended_before_release = true;
                            cleanup.release_a();
                        }
                    }
                }
                Event::Manifest {
                    point,
                    actual_thread,
                    live_exclusive_guards,
                    callback_interval,
                } => {
                    break (
                        point,
                        actual_thread,
                        live_exclusive_guards,
                        callback_interval,
                    )
                }
                Event::WriterContinued {
                    release_timed_out: true,
                    ..
                } => {
                    return Err(failure(
                        "writer watchdog released without parent; INCONCLUSIVE",
                    ))
                }
                _ => {}
            }
        };
        if !exclusive_acquired || manifest.root != root {
            return Err(failure(
                "actual EX/post-manifest cut not proven; INCONCLUSIVE",
            ));
        }
        let actual_ex_acquisition = actual_ex_acquisition
            .ok_or_else(|| failure("actual EX acquisition DATA missing; INCONCLUSIVE"))?;
        reached["actual_backup_thread"] = json!(actual_backup_thread);
        reached["actual_manifest_EX_guard_id"] = json!(actual_ex_acquisition.guard_id);
        reached["actual_manifest_EX_descriptor"] = json!(actual_ex_acquisition.descriptor);
        let (manifest_bytes, manifest_sha, fuzzy) = manifest
            .page_wal
            .ok_or_else(|| failure("actual Standard page-WAL manifest entry absent"))?;
        if fuzzy {
            return Err(failure("Standard unexpectedly marked actual WAL fuzzy"));
        }
        reached["actual_EX_acquired"] = json!(exclusive_acquired);
        reached["actual_EX_contended_before_A_release"] = json!(exclusive_contended_before_release);
        reached["actual_manifest_sha256"] = json!(manifest.manifest_sha256);
        reached["actual_manifest_page_WAL"] =
            json!({"bytes":manifest_bytes,"sha256":manifest_sha,"fuzzy":fuzzy});
        require_live_manifest_ex(
            &actual_ex_guards,
            &actual_ex_acquisition,
            &callback_interval,
            &actual_backup_thread,
            &root,
            "before_at_manifest_read",
            &mut trace,
        )?;
        let at_manifest = file_point(&root.join("paged/store.wal"))?;
        require_live_manifest_ex(
            &actual_ex_guards,
            &actual_ex_acquisition,
            &callback_interval,
            &actual_backup_thread,
            &root,
            "after_at_manifest_read",
            &mut trace,
        )?;
        reached["actual_page_WAL_at_manifest"] = json!(at_manifest);
        if at_manifest != (manifest_bytes, manifest_sha.clone()) {
            return Err(failure(
                "actual page WAL already differs before releasing A; INCONCLUSIVE",
            ));
        }
        cleanup.release_a();
        if !exclusive_contended_before_release {
            // The actual EX is held and the real manifest is hashed. Wait for
            // physical bytes, not elapsed sleep or a generic SH refusal as
            // proof of a page effect. A root SH can be the later Core marker;
            // its observation alone cannot establish a pre-page block.
            // Preserve a changed-file error as failure.
            let until = Instant::now() + WATCHDOG;
            let mut physical_change = false;
            let mut shared_contended = false;
            while Instant::now() < until && !physical_change {
                require_live_manifest_ex(
                    &actual_ex_guards,
                    &actual_ex_acquisition,
                    &callback_interval,
                    &actual_backup_thread,
                    &root,
                    "before_poll_read",
                    &mut trace,
                )?;
                let now = file_point(&root.join("paged/store.wal"))?;
                require_live_manifest_ex(
                    &actual_ex_guards,
                    &actual_ex_acquisition,
                    &callback_interval,
                    &actual_backup_thread,
                    &root,
                    "after_poll_read",
                    &mut trace,
                )?;
                if now != (manifest_bytes, manifest_sha.clone()) {
                    physical_change = true;
                    trace.push(
                        json!({"actual_changed_page_WAL_read_checked_before_and_after":true,
                        "same_actual_EX_guard_id":actual_ex_acquisition.guard_id,
                        "same_actual_EX_descriptor":actual_ex_acquisition.descriptor,
                        "same_actual_EX_file_identity":actual_ex_acquisition.file_identity,
                        "actual_backup_thread":actual_backup_thread,"actual_page_WAL":now}),
                    );
                    reached["actual_page_WAL_changed_while_EX_held"] = json!(now);
                    break;
                }
                match events.recv_timeout(Duration::from_millis(5)) {
                    Ok(event) => {
                        if let Event::Gate(point) = &event {
                            shared_contended |= !point.exclusive
                                && !point.acquired
                                && !point.released
                                && point.gate == root.join(crate::storage::BACKUP_WRITE_GATE);
                        }
                        if matches!(
                            &event,
                            Event::WriterContinued {
                                release_timed_out: true,
                                ..
                            }
                        ) {
                            return Err(failure("writer watchdog fired; INCONCLUSIVE"));
                        }
                        append_trace(&event, &mut trace);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(failure("actual writer evidence disconnected; INCONCLUSIVE"))
                    }
                }
            }
            reached["actual_physical_page_WAL_change"] = json!(physical_change);
            reached["actual_shared_gate_contention"] = json!(shared_contended);
            reached["shared_contention_is_pre_page_proof"] = json!(false);
            if !physical_change {
                return Err(failure(
                    "no actual page effect reached (generic SH refusal is insufficient); INCONCLUSIVE",
                ));
            }
        }
        require_live_manifest_ex(
            &actual_ex_guards,
            &actual_ex_acquisition,
            &callback_interval,
            &actual_backup_thread,
            &root,
            "before_parent_backup_release",
            &mut trace,
        )?;
        reached["classification"] = json!(
            "actual original Standard cuts reached with same live EX; archive/restore assertions still required"
        );
        cleanup.release_backup();
        Ok(())
    })();
    // Never assert while either callback could remain paused.
    let (writer, backup) = cleanup.join();
    while let Ok(event) = events.try_recv() {
        append_trace(&event, &mut trace);
    }
    eprintln!(
        "LEGACY_STANDARD_ACTUAL_WAL_PAGE_CUT {}",
        json!({
            "source":"fbb source-only regression definition; actual runtime uses compiled test source",
            "reached":&reached,"trace":&trace,"schedule_result":format!("{schedule:?}"),
            "actual_writer_result":format!("{writer:?}"),"actual_Standard_backup_result":format!("{backup:?}"),
            "source_healthy":db.check_recovery_state().is_ok(),"owned_API_used":false,"changed_file_error_is_success":false,
        })
    );
    schedule.unwrap();
    assert!(
        trace
            .iter()
            .all(|event| event["writer_watchdog_timed_out"] != true),
        "writer watchdog fired: cut is INCONCLUSIVE even if later operations returned successfully"
    );
    assert!(trace.iter().all(|event| event["backup_callback_timed_out"] != true),
        "backup callback watchdog fired: cut is INCONCLUSIVE even if a later read observed changed bytes");
    if let Some(ids) = reached["actual_A_held_shared_guard_ids_before_early_release"].as_array() {
        for id in ids {
            let original_guard = reached["actual_A_live_shared_guards_at_WAL"]
                .as_array()
                .unwrap()
                .iter()
                .find(|guard| guard["guard_id"] == *id)
                .unwrap();
            assert!(
                trace.iter().any(|event| {
                    let gate = &event["gate_lifetime"];
                    gate["guard_id"] == *id
                        && gate["exclusive"] == false
                        && gate["released_after_actual_File_drop"] == true
                        && gate["descriptor"] == original_guard["descriptor"]
                        && gate["file_identity"] == original_guard["file_identity"]
                        && gate["actual_thread"] == reached["actual_A_thread"]
                }),
                "same A-thread descriptor release not observed after real File drop: {id}"
            );
        }
    }
    let writer = writer.unwrap();
    assert_eq!(writer.state_before_drop, TxState::Committed);
    let sequence = writer.result.unwrap();
    assert_eq!(
        reached["actual_A_durable_Core_sequence"].as_u64(),
        Some(sequence)
    );
    assert_eq!(
        reached["actual_A_transaction"].as_u64(),
        Some(writer.transaction)
    );
    backup.unwrap(); // A changed-file refusal is the ORIGINAL failing outcome.
    db.check_recovery_state().unwrap();
    crate::backup::verify_backup(&archive, KEY).unwrap();
    crate::backup::restore_backup(
        &archive,
        restored_dir.path(),
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: true,
        },
    )
    .unwrap();
    let restored = BicDb::open_with_config(restored_dir.path(), config).unwrap();
    assert_eq!(restored.scan_collection(COLLECTION).unwrap().len(), 101);
    for record in &baseline {
        assert_eq!(
            restored.get(COLLECTION, &record.id).unwrap().as_deref(),
            Some(record)
        );
    }
    assert_eq!(
        restored.get(COLLECTION, &a.id).unwrap().as_deref(),
        Some(&a)
    );
    let index = restored.verify_index(INDEX).unwrap();
    assert!(index.valid, "actual restored index invalid: {index:?}");
    assert_eq!(index.expected_records, 101);
    assert_eq!(index.indexed_records, 101);
    assert_eq!(
        restored
            .lookup_index(INDEX, &[IndexValue::Int(100)])
            .unwrap(),
        vec![a.id.clone()]
    );
}
