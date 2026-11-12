//! Actual fifth-factory promotion, real TLS OWN MQ, and final MStore contention.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy)]
enum Validation {
    OwnedMember,
    RegisteredCatchup,
}
fn exercise(cancel: bool, validation: Validation) {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("actual-owned-member-validation-wait");
    let id = start_owned(&f, &mut host);
    let setup = CancellationToken::uncancelable();
    let actual = ready(&f, &setup);
    let (post, business) = f.current();
    // config() reads this SAME metadata mutex; capture it before the method
    // receiver takes the promotion guard. The real proof still validates later.
    let configuration = config(&f);
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&actual, &post, business, &configuration, 41, &setup)
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
    assert_eq!(
        metadata
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        id
    );
    let original = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let business = Uuid::now_v7();
    let observed = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &original)
        .unwrap();
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
    let barrier = NativeUnassignedLearnerHost::test_owned_metadata_validation_barrier();
    let observed_deadline = observed.deadline();
    let finished = AtomicBool::new(false);
    let result = std::thread::scope(|threads| {
        // Only the actual MStore/atomic rendezvous cross threads. The real host,
        // actual factory/native proof and OWN MQ stay on their producing thread.
        let holder = threads.spawn(|| {
            let until = Instant::now() + Duration::from_secs(2);
            while !barrier.entered() {
                assert!(
                    Instant::now() < until,
                    "actual factory validation not reached"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let store = metadata.lock();
            barrier.release();
            while !barrier.busy() {
                assert!(
                    Instant::now() < until,
                    "actual final MStore contention not reached"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            original.check().unwrap();
            assert!(
                Instant::now() < observed_deadline,
                "original OWN MQ expired before the contention cut"
            );
            assert_eq!(
                serde_json::json!({"status":store.status(),"C":store.committed_topology(),
                "store_id":store.registered_native_metadata_member_store_id().unwrap()}),
                before
            );
            if cancel {
                original.cancel();
                let cleanup = Instant::now() + Duration::from_secs(2);
                while !finished.load(Ordering::Acquire) {
                    assert!(
                        Instant::now() < cleanup,
                        "canceled validator did not exit while MStore remained held"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            drop(store);
        });
        let result = match validation {
            Validation::OwnedMember => host.test_validate_owned_metadata_member_after_factory(
                &observed,
                business,
                &original,
                barrier.clone(),
            ),
            Validation::RegisteredCatchup => host
                .test_validate_registered_metadata_catchup_after_factory(
                    &observed,
                    business,
                    &original,
                    barrier.clone(),
                )
                .map(|local_installed_current| assert!(local_installed_current)),
        };
        finished.store(true, Ordering::Release);
        holder.join().unwrap();
        result
    });
    assert!(barrier.entered() && barrier.busy());
    if cancel {
        assert!(
            matches!(result, Err(crate::BicDbError::QueryCanceled)),
            "{result:?}"
        );
    } else {
        result.unwrap();
        original.check().unwrap();
        observed.require_live_request(business).unwrap();
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
    // Actual NEW request only after the canceled attempt ended, to confirm the
    // same owned identity survives. It cannot renew or bless the canceled proof.
    let fresh = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &fresh)
        .unwrap();
    host.validate_owned_metadata_member(&own, business, &fresh)
        .unwrap();
    assert!(Arc::ptr_eq(&metadata, &host.test_owned_metadata().unwrap()));
    host.shutdown().unwrap();
    f.shutdown();
}
#[test]
fn native_owned_metadata_validation_waits_for_actual_final_store_then_keeps_same_proof() {
    exercise(false, Validation::OwnedMember);
}
#[test]
fn native_owned_metadata_validation_original_cancel_exits_while_actual_final_store_held() {
    exercise(true, Validation::OwnedMember);
}

#[test]
fn native_registered_metadata_catchup_waits_for_actual_final_store_with_same_proof() {
    exercise(false, Validation::RegisteredCatchup);
}
#[test]
fn native_registered_metadata_catchup_original_cancel_exits_while_actual_final_store_held() {
    exercise(true, Validation::RegisteredCatchup);
}
