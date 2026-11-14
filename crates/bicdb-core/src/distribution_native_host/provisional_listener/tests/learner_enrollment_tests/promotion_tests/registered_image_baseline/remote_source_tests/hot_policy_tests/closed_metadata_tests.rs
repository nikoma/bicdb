//! Actual MStore lock contention after real OWN acquisition. The waiter never
//! renews authority or replaces the retained selected transition.
use super::*;
use std::sync::mpsc;

struct HeldMetadata {
    release: Option<mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Drop for HeldMetadata {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
#[test]
fn native_registered_fifth_remote_hot_closed_actual_mstore_contention_same_own_progress() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotClosedMetadataContention),
    );
}
#[test]
fn native_registered_fifth_remote_hot_closed_actual_mstore_wait_expired_own_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotClosedMetadataOwnExpired),
    );
}
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    selected: crate::RangeId,
    process: &CancellationToken,
    output: &std::path::Path,
) {
    let expired = matches!(case, SourceCase::TargetHotClosedMetadataOwnExpired);
    let actual_metadata = metadata.clone();
    let held = Arc::new(Mutex::new(None::<HeldMetadata>));
    let held_at_cut = held.clone();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let captured = observations.clone();
    let mut actual_deadline = None;
    let mut busy_reached = false;
    let cut = crate::BicDb::test_writer_policy_metadata_cut(move |stage, deadline| {
        match stage {
            "before_acquire" => {
                assert!(
                    actual_deadline.is_none(),
                    "no replacement OWN acquisition in same call"
                );
                actual_deadline = deadline;
                let actual = actual_metadata.clone();
                let (ready_tx, ready_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let _actual_store = actual.lock();
                    ready_tx.send(()).unwrap();
                    let _ = release_rx.recv();
                });
                *held_at_cut.lock() = Some(HeldMetadata {
                    release: Some(release_tx),
                    worker: Some(worker),
                });
                ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                captured.lock().push(serde_json::json!({
                    "stage":stage,"actual_store_lock_held":true,
                    "original_own_live":std::time::Instant::now() < actual_deadline.unwrap(),
                }));
            }
            "busy_slice" if !busy_reached => {
                busy_reached = true;
                if expired {
                    // Keep the REAL MStore mutex held while the already issued
                    // OWN expires. The caller remains its original 5s request.
                    std::thread::sleep(
                        actual_deadline
                            .unwrap()
                            .saturating_duration_since(std::time::Instant::now())
                            + Duration::from_millis(1),
                    );
                    captured.lock().push(serde_json::json!({"stage":stage,"actual_own_expired":std::time::Instant::now() >= actual_deadline.unwrap(),"actual_store_lock_still_held":true}));
                } else {
                    if let Some(release) = held_at_cut.lock().as_mut().unwrap().release.take() {
                        release.send(()).unwrap();
                    }
                    captured.lock().push(serde_json::json!({"stage":stage,"actual_busy_slice_reached":true,"original_own_still_live":std::time::Instant::now() < actual_deadline.unwrap(),"actual_store_release_sent":true}));
                }
            }
            _ => {}
        }
    });
    let request = budget(5000);
    let result = control.advance_native_writer_closed_domain(selected, &request);
    drop(cut);
    // Never join a thread holding the actual MStore until the original call
    // has returned and its own guards have ended.
    drop(held.lock().take());
    let recorded = observations.lock().clone();
    let closed_before_retry = control
        .native_closed_writer_domain(selected, process)
        .is_ok();
    evidence(
        &output.join("registered-survivor-metadata-contention.json"),
        &serde_json::json!({
            "original_result":result.as_ref().err().map(ToString::to_string),
            "original_request_still_live":request.check().is_ok(),
            "original_process_still_live":process.check().is_ok(),
            "actual_stages":recorded,
            "closed_before_retry":closed_before_retry,
            "expired_own_control":expired,
        }),
    )
    .unwrap();
    assert_eq!(recorded[0]["stage"], "before_acquire");
    assert_eq!(recorded[1]["stage"], "busy_slice");
    assert_eq!(recorded[0]["original_own_live"], true);
    assert_eq!(
        recorded.len(),
        2,
        "both actual held-store and busy-slice controls must run"
    );
    process.check().unwrap();
    request.check().unwrap();
    if expired {
        match result.as_ref().err().expect("expired original OWN must refuse") {
            crate::BicDbError::QueryTimedOut => {},
            crate::BicDbError::Cluster(detail)
                if detail == "metadata consensus: host metadata observation request changed or expired" => {},
            other => panic!("expected actual original OWN expiry, got {other:?}"),
        }
        assert_eq!(recorded[1]["actual_own_expired"], true);
        assert!(!closed_before_retry);
        let fresh = budget(5000);
        let retry = control.advance_native_writer_closed_domain(selected, &fresh);
        evidence(
            &output.join("registered-survivor-metadata-contention-retry.json"),
            &serde_json::json!({
                "new_explicit_request_after_original_refusal":true,
                "fresh_result":retry.as_ref().err().map(ToString::to_string),
                "closed_after_retry":control.native_closed_writer_domain(selected, process).is_ok(),
            }),
        )
        .unwrap();
        retry.unwrap();
    } else {
        result.unwrap();
        assert_eq!(recorded[1]["original_own_still_live"], true);
        assert!(closed_before_retry);
    }
}
