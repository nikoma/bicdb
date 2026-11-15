//! Real promoted fifth owner and SAME original status/OWN windows. This is a
//! component cut at the exact CLI refresh sink; the public CLI test is unchanged.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
enum Cut {
    Release,
    CallerCancel,
    OriginalScopeExpiry,
}
struct Finished<'a>(&'a AtomicBool);
impl Drop for Finished<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
fn exercise(cut: Cut) {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("actual-owned-status-wait");
    let id = start_owned(&f, &mut host);
    let setup = CancellationToken::uncancelable();
    let actual = ready(&f, &setup);
    let (post, promotion_business) = f.current();
    let configuration = config(&f);
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(
            &actual,
            &post,
            promotion_business,
            &configuration,
            41,
            &setup,
        )
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&setup).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success && reply.native_provenance.is_some());
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
    let metadata = host.test_owned_metadata().unwrap();
    assert!(metadata.lock().status().voters.contains(&node(4)));
    let own_parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let business = Uuid::now_v7();
    let observed = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &own_parent)
        .unwrap();
    // SAME actual OWN deadline, independently cancelable caller. No new window
    // is minted, and canceling the caller can be distinguished from OWN expiry.
    let original = CancellationToken::uncancelable().child_with_deadline(observed.deadline());
    let native_before = {
        let db = factory.database.read();
        serde_json::json!({"incarnation":db.incarnation_id(),"commit_sequence":db.current_commit_seq(),
            "uids":db.capture_range_image_bindings().unwrap().namespace_ids(),"rows":db.scan_collection("items").unwrap()})
    };
    let before = {
        let store = metadata.lock();
        serde_json::json!({"status":store.status(),"C":store.committed_topology(),
            "store_id":store.registered_native_metadata_member_store_id().unwrap()})
    };
    let barrier = NativeUnassignedLearnerHost::test_owned_metadata_status_barrier();
    let finished = AtomicBool::new(false);
    let clocks = std::sync::Mutex::new(Vec::new());
    let own_deadline = observed.deadline();
    let result = std::thread::scope(|threads| {
        let holder = threads.spawn(|| {
            let setup_limit = Instant::now() + Duration::from_secs(2);
            while !barrier.entered() {
                if finished.load(Ordering::Acquire) {
                    return;
                }
                assert!(Instant::now() < setup_limit, "actual status sink not reached");
                std::thread::sleep(Duration::from_millis(1));
            }
            let store = metadata.lock();
            barrier.release();
            while !barrier.busy() {
                if finished.load(Ordering::Acquire) {
                    return;
                }
                assert!(Instant::now() < setup_limit, "real status lock contention not reached");
                std::thread::sleep(Duration::from_millis(1));
            }
            original.check().unwrap();
            own_parent.check().unwrap();
            let now = Instant::now();
            assert!(now < own_deadline);
            clocks.lock().unwrap().push(serde_json::json!({"stage":"actual_failed_lock_while_held",
                "caller_remaining_us":original.deadline().unwrap().saturating_duration_since(now).as_micros(),
                "own_remaining_us":own_deadline.saturating_duration_since(now).as_micros(),
                "caller_canceled":original.is_cancel_requested()}));
            assert_eq!(
                serde_json::json!({"status":store.status(),"C":store.committed_topology(),
                "store_id":store.registered_native_metadata_member_store_id().unwrap()}),
                before
            );
            if matches!(cut, Cut::CallerCancel) {
                original.cancel();
            }
            if !matches!(cut, Cut::Release) {
                // This is only a test cleanup watchdog, not a caller/OWN window.
                let cleanup_limit = Instant::now() + Duration::from_secs(7);
                while !finished.load(Ordering::Acquire) {
                    assert!(Instant::now() < cleanup_limit, "status did not refuse while store held");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            drop(store);
        });
        let completed = Finished(&finished);
        let result =
            host.test_owned_metadata_snapshot_wait(&observed, business, &original, barrier.clone());
        // Copy original memory clocks BEFORE diagnostic native/storage reads.
        let now = Instant::now();
        clocks.lock().unwrap().push(serde_json::json!({"stage":"actual_status_return",
            "caller_remaining_us":original.deadline().unwrap().saturating_duration_since(now).as_micros(),
            "own_remaining_us":own_deadline.saturating_duration_since(now).as_micros(),
            "caller_canceled":original.is_cancel_requested(),"returned_ok":result.is_ok(),
            "exact_error":result.as_ref().err().map(ToString::to_string)}));
        drop(completed);
        holder.join().unwrap();
        result
    });
    eprintln!(
        "OWNED_METADATA_STATUS_WAIT {}",
        serde_json::json!({"cut":format!("{cut:?}"),
        "actual_sink_entered":barrier.entered(),"actual_lock_busy":barrier.busy(),"clocks":*clocks.lock().unwrap(),
        "native_before":native_before,"metadata_before":before,"component_only":true,"CLI_election_replayed":false})
    );
    assert!(barrier.entered() && barrier.busy());
    match cut {
        Cut::Release => {
            let (actual_id, status, listener) = result.unwrap().unwrap();
            assert_eq!(actual_id, id);
            assert_eq!(serde_json::to_value(status).unwrap(), before["status"]);
            assert!(listener);
            original.check().unwrap();
            observed.require_live_request(business).unwrap();
        }
        Cut::CallerCancel => {
            assert!(
                matches!(result, Err(crate::BicDbError::QueryCanceled)),
                "{result:?}"
            );
            // Genuine OWN remains live: the helper refused this caller, not a
            // synthesized or reissued metadata observation.
            observed.require_live_request(business).unwrap();
        }
        Cut::OriginalScopeExpiry => {
            assert!(
                matches!(result, Err(crate::BicDbError::QueryTimedOut)),
                "{result:?}"
            );
            assert!(Instant::now() >= original.deadline().unwrap().min(own_deadline));
        }
    }
    {
        let store = metadata.lock();
        assert_eq!(
            serde_json::json!({"status":store.status(),"C":store.committed_topology(),
            "store_id":store.registered_native_metadata_member_store_id().unwrap()}),
            before
        );
    }
    {
        let db = factory.database.read();
        assert_eq!(
            serde_json::json!({"incarnation":db.incarnation_id(),"commit_sequence":db.current_commit_seq(),
            "uids":db.capture_range_image_bindings().unwrap().namespace_ids(),"rows":db.scan_collection("items").unwrap()}),
            native_before
        );
    }
    // A separately fresh real read proves the SAME owner remains usable. It
    // does not renew the canceled/expired observation or re-run an election.
    let fresh_parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let fresh_business = Uuid::now_v7();
    let fresh = f
        .learner_transport
        .acquire_host_metadata_observation(
            &node(0),
            fresh_business,
            Duration::from_secs(5),
            &fresh_parent,
        )
        .unwrap();
    let (same_id, _, listener) = host
        .owned_metadata_snapshot(&fresh, fresh_business, &fresh_parent)
        .unwrap()
        .unwrap();
    assert_eq!(same_id, id);
    assert!(listener);
}

#[test]
fn native_registered_metadata_owned_status_waits_for_real_store_with_same_original_read() {
    exercise(Cut::Release);
}
#[test]
fn native_registered_metadata_owned_status_canceled_caller_refuses_while_real_store_held() {
    exercise(Cut::CallerCancel);
}
#[test]
fn native_registered_metadata_owned_status_original_scope_expires_while_real_store_held() {
    exercise(Cut::OriginalScopeExpiry);
}
