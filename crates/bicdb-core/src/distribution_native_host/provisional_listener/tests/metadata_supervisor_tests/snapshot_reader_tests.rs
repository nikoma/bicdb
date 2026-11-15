//! Actual competing native snapshot readers. No votes, status, log, quorum or
//! anchors are fabricated. The original 12s wait and ordinary timing remain.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

#[test]
fn native_owned_metadata_supervisor_short_snapshot_readers_preserve_anchor_progress() {
    let mut f = Network::new();
    f.start(2);
    assert_eq!(f.wait_leader(&[2]), 2);
    f.start(0);
    f.start(1);
    f.fresh_mq(0, 2);
    let old_term = f.launch(2).metadata.lock().status().current_term;
    let stop = Arc::new(AtomicBool::new(false));
    let readers = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let omitted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Separate active controls retain the TWO original round tokens only so
    // the test can prove they stay dead. These are not passive fact records.
    let denied_rounds = Arc::new(Mutex::new(Vec::<CancellationToken>::new()));
    let observations = [0, 1].map(|i| f.workers[i].as_ref().unwrap().test_observation());
    let execution = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        std::thread::scope(|scope| {
            // Drop BEFORE the scoped join on panic, releasing reader waits and
            // removing only this test's hooks. Nothing cancels a protocol token.
            struct Cleanup<'a> {
                stop: &'a AtomicBool,
                clear: Vec<Box<dyn Fn() + 'a>>,
            }
            impl Drop for Cleanup<'_> {
                fn drop(&mut self) {
                    self.stop.store(true, Ordering::Release);
                    for clear in &self.clear {
                        clear();
                    }
                }
            }
            let mut cleanup = Cleanup {
                stop: &stop,
                clear: Vec::new(),
            };
            for i in [0, 1] {
                let metadata = f.launch(i).metadata.clone();
                let actual = f.launch(i).service.lock().listener_open_guard();
                let observed = observations[i].clone();
                let observer_for_reader = observed.clone();
                let rows = readers.clone();
                let dropped = omitted.clone();
                let stopping = stop.clone();
                let denied = denied_rounds.clone();
                let (tx, rx) =
                    mpsc::sync_channel::<(CancellationToken, u64, mpsc::SyncSender<()>)>(1);
                scope.spawn(move || {
                let mut denied_once = false;
                while !stopping.load(Ordering::Acquire) {
                    let (request, initial_busy, granted) = match rx.recv_timeout(Duration::from_millis(5)) {
                        Ok(request) => request,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    let entered = Instant::now();
                    let result = (|| -> Result<serde_json::Value> {
                        let held = loop {
                            request.check()?;
                            if stopping.load(Ordering::Acquire) {
                                return Err(BicDbError::QueryCanceled);
                            }
                            if let Some(held) = metadata.try_lock_for(Duration::from_millis(5)) {
                                break held;
                            }
                        };
                        request.check()?;
                        // Genuine native owner/open/Store/voting validation,
                        // exactly like ordinary status inspection.
                        let snapshot = held.native_supervisor_snapshot(&actual, None)?.1;
                        request.check()?;
                        granted.try_send(()).map_err(|_| invalid("snapshot reader grant receiver ended"))?;
                        // Hold only until the SAME worker's actual initial
                        // try_lock reports busy, or the ORIGINAL round ends.
                        // This observes the old source's exact failure edge;
                        // it never manufactures a mutex failure.
                        while observer_for_reader.initial_busy_count() == initial_busy
                            && request.check().is_ok()
                            && !stopping.load(Ordering::Acquire)
                        {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        let actual_busy = observer_for_reader.initial_busy_count() > initial_busy;
                        let denial = if actual_busy && !denied_once {
                            if i == 0 {
                                request.cancel(); // exact original WORKER ROUND, not its process
                            } else {
                                // No new deadline: hold this real reader until
                                // the original 400ms round expires naturally.
                                while request.check().is_ok() && !stopping.load(Ordering::Acquire) {
                                    std::thread::sleep(Duration::from_millis(1));
                                }
                            }
                            denied_once = true;
                            denied.lock().push(request.clone());
                            if i == 0 { "actual_round_canceled" } else { "original_round_expired" }
                        } else { "none" };
                        let remaining = request.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros());
                        drop(held); // before diagnostics/formatting or any join
                        Ok(serde_json::json!({"node_index":i,"native_snapshot_completed":true,
                            "actual_worker_probe_busy_while_reader_held":actual_busy,
                            "denial_control":denial,"original_round_still_dead_at_release":request.check().is_err(),
                            "actual_term":snapshot.status.current_term,"actual_role":format!("{:?}",snapshot.status.role),
                            "actual_voting_actor":snapshot.voting_actor,"actual_anchor":snapshot.anchored,
                            "reader_elapsed_us":entered.elapsed().as_micros(),"original_round_remaining_us_at_release":remaining}))
                    })();
                    let row = match result {
                        Ok(value) => value,
                        Err(error) => serde_json::json!({"node_index":i,"reader_error":error.to_string(),
                            "original_round_expired_or_canceled":request.check().is_err()}),
                    };
                    let mut output = rows.lock();
                    if output.len()<256 { output.push(row); }
                    else { dropped.fetch_add(1,Ordering::Relaxed); }
                }
            });
                let stopping = stop.clone();
                let counter = Arc::downgrade(&observed);
                observed
                    .set_snapshot_hook(Some(Arc::new(move |parent: &CancellationToken| {
                        parent.check()?;
                        if stopping.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let (grant, received) = mpsc::sync_channel(1);
                        let counter = counter
                            .upgrade()
                            .ok_or_else(|| invalid("actual worker observation ended"))?;
                        tx.try_send((parent.clone(), counter.initial_busy_count(), grant))
                            .map_err(|_| invalid("snapshot reader request queue busy or ended"))?;
                        loop {
                            parent.check()?;
                            if stopping.load(Ordering::Acquire) {
                                return Ok(());
                            }
                            match received.recv_timeout(Duration::from_millis(1)) {
                                Ok(()) => return Ok(()),
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(mpsc::RecvTimeoutError::Disconnected) => {
                                    return Err(invalid("actual snapshot reader did not complete"));
                                }
                            }
                        }
                    })))
                    .unwrap();
                let observer = observed.clone();
                cleanup.clear.push(Box::new(move || {
                    let _ = observer.set_snapshot_hook(None);
                }));
            }
            f.listeners[2]
                .as_mut()
                .unwrap()
                .stop_and_drain(&budget())
                .unwrap();
            f.listeners[2].take();
            // SAME existing proof and SAME 12s assertion, with all competing
            // readers active. Do not replace anchored/voting with cached status.
            let healthy = f.wait_leader(&[0, 1]);
            assert!(f.launch(healthy).metadata.lock().status().current_term > old_term);
            assert!(observations[healthy].initial_busy_count() > 0);
            drop(cleanup);
            f.fresh_mq(0, healthy);
            f.fresh_mq(1, healthy);
        })
    }));
    let rows = readers.lock().clone();
    eprintln!(
        "NATIVE_METADATA_SHORT_READER_CONTROL {}",
        serde_json::json!({
            "actual_readers":rows,"omitted":omitted.load(Ordering::Relaxed),
            "workers":[observations[0].snapshot(),observations[1].snapshot()],
            "round_ms":400,"original_wait_leader_seconds":12,
            "old_source_comparison":"initial actual try_lock None would abort the old tick; old binary not run",
            "original203_cause_proven":false,"physical_fault_or_RTO_claim":false,
        })
    );
    if let Err(original) = execution {
        std::panic::resume_unwind(original);
    }
    let denied = denied_rounds.lock();
    assert_eq!(
        denied.len(),
        2,
        "both actual original denial cuts must be reached"
    );
    assert!(
        denied.iter().all(|round| round.check().is_err()),
        "old round was revived"
    );
    assert!(observations[0].snapshot()["canceled_admission_refusals"]
        .as_u64()
        .is_some_and(|n| n > 0));
    assert!(observations[1].snapshot()["expired_admission_refusals"]
        .as_u64()
        .is_some_and(|n| n > 0));
    assert!(rows
        .iter()
        .any(|row| row["native_snapshot_completed"] == true
            && row["actual_worker_probe_busy_while_reader_held"] == true));
}
