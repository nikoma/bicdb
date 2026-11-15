//! Real OpenInitialData -> first genesis MetadataReadAcquire -> old listener
//! backlog -> actual close/rebind. READ continuation is not DATA readiness.
use super::*;
use crate::distribution_transport::InitialTransitionControl;
use crate::BicDbError;

#[derive(Clone, Copy, Debug)]
enum Cut {
    Resume,
    Cancel,
    Expire,
}
struct Release<'a> {
    server: &'a InitialTransitionControl,
    client: &'a InitialTransitionControl,
    requests: [&'a CancellationToken; 2],
}
impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.server.release_all();
        self.client.release_all();
        for request in self.requests {
            request.cancel();
        }
    }
}
fn panic_data(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_else(|| "non-string panic payload".to_owned())
}
fn run(cut: Cut) {
    let (fixture, mut controls, _) = actual_initial_metadata_controls();
    let mut follower = controls.remove(1);
    let mut leader = controls.remove(0);
    let (server, _) = leader.test_initial_listener_transition_controls().unwrap();
    let (_, client) = follower
        .test_initial_listener_transition_controls()
        .unwrap();
    let leader_original = budget(5000);
    let follower_original = budget(5000);
    let coordination = budget(5000);
    server.pause_before_drain();
    client.pause_before_genesis_read();
    if !matches!(cut, Cut::Resume) {
        client.pause_after_genesis_socket_failure();
    }
    let mut events = Vec::new();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        std::thread::scope(|scope| {
            // On unwind this releases/cancels before scoped automatic joins.
            // Normally hooks release before explicit joins and cancellation
            // happens only when this guard drops after those joins.
            let release = Release {
                server: &server,
                client: &client,
                requests: [&leader_original, &follower_original],
            };
            let leader_request = &leader_original;
            let opening = scope.spawn(move || {
                let returned = leader.open_initial_data(leader_request);
                let state = leader.test_initial_transition_snapshot();
                (leader, returned, state)
            });
            let mut reading = None;
            let action = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                server.wait("drain", &coordination).unwrap();
                server.wait("idle", &coordination).unwrap();
                let follower_request = &follower_original;
                reading = Some(scope.spawn(move || {
                    let returned = follower.open_initial_data(follower_request);
                    let state = follower.test_initial_transition_snapshot();
                    (follower, returned, state)
                }));
                // The follower has already actually enrolled, drained its own
                // old socket and started DATA before reaching this real call.
                client.wait("genesis", &coordination).unwrap();
                assert_eq!(client.snapshot()["actual_retained_enrollment_installs"], 1);
                server.pause_accept();
                server.wait("accept", &coordination).unwrap();
                server.wait("idle", &coordination).unwrap();
                client.pause_connected_genesis_read(node(0));
                client.release_genesis_read();
                client.wait("connect", &coordination).unwrap();
                let connected = client.snapshot();
                let original_request = &connected["actual_connected_request"];
                assert_eq!(
                    original_request["actual_request_kind"],
                    "MetadataReadAcquire"
                );
                assert_eq!(original_request["actual_requester"], "n1");
                assert_eq!(original_request["peer_TLS_not_yet_authenticated"], true);
                assert_eq!(server.snapshot()["actual_active_connections"], 0);
                assert!(original_request["original_exchange_remaining_us"]
                    .as_u64()
                    .is_some_and(|n| n > 0));
                events.push(
                    serde_json::json!({"stage":"actual_genesis_MQ_connected_to_old_backlog",
                    "server":server.snapshot(),"client":connected,
                    "same_original_parent_deadline":format!("{:?}",follower_original.deadline())}),
                );
                server.release_drain();
                server.wait("shutdown", &coordination).unwrap();
                server.wait("accept_ended", &coordination).unwrap();
                client.release_connect();
                match cut {
                    Cut::Resume => client.wait("genesis_success", &follower_original).unwrap(),
                    Cut::Cancel | Cut::Expire => {
                        client.wait("genesis_failure", &coordination).unwrap();
                        let paused = client.snapshot();
                        assert!(paused["actual_genesis_read_returns"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|r| r["typed_socket_failure"] == true));
                        follower_original.check().unwrap();
                        events.push(serde_json::json!({"stage":"actual_original_socket_failure_before_retry",
                            "client":paused,"parent_cancelled":follower_original.is_cancel_requested()}));
                        if matches!(cut, Cut::Cancel) {
                            follower_original.cancel();
                        } else {
                            // The actual reached cut is above. Waiting for the
                            // ORIGINAL five-second deadline does not set it.
                            while follower_original.check().is_ok() {
                                std::thread::sleep(Duration::from_millis(2));
                            }
                            assert!(!follower_original.is_cancel_requested());
                        }
                    }
                }
            }));
            server.release_all();
            client.release_all();
            if action.is_err() {
                leader_original.cancel();
                follower_original.cancel();
            }
            // Both worker results are collected before selecting the primary
            // action failure. Joins/kernel operations are not hard preemptible.
            let leader_result = opening.join();
            let follower_result = reading.map(|worker| worker.join());
            let mut worker_panics = Vec::new();
            let mut cleanup_errors = Vec::new();
            if let Err(panic) = &leader_result {
                worker_panics.push(panic_data(panic.as_ref()));
            }
            if let Some(Err(panic)) = &follower_result {
                worker_panics.push(panic_data(panic.as_ref()));
            }
            if let Ok((_, returned, state)) = &leader_result {
                events.push(serde_json::json!({"stage":"actual_leader_OpenInitialData_return",
                    "returned_ok":returned.is_ok(),"original_error":returned.as_ref().err().map(ToString::to_string),"state":state}));
            }
            if let Some(Ok((_, returned, state))) = &follower_result {
                events.push(serde_json::json!({"stage":"actual_follower_OpenInitialData_return",
                    "returned_ok":returned.is_ok(),"original_error":returned.as_ref().err().map(ToString::to_string),
                    "state":state,"client":client.snapshot()}));
            }
            let follower_assertions =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if action.is_err() || !worker_panics.is_empty() {
                        return;
                    }
                    let (_, returned, state) = follower_result.as_ref().unwrap().as_ref().unwrap();
                    let trace = client.snapshot();
                    assert_eq!(trace["genesis_omitted"], 0);
                    assert_eq!(trace["actual_retained_enrollment_installs"], 1);
                    assert_eq!(state["actual_enrollment_retained"], true);
                    let request = &trace["actual_connected_request"];
                    let reads = trace["actual_genesis_read_returns"].as_array().unwrap();
                    assert!(reads.iter().all(|read| read["original_parent_deadline"]
                        == format!("{:?}", follower_original.deadline())));
                    let failed = reads
                        .iter()
                        .find(|r| r["typed_socket_failure"] == true)
                        .expect("the actual old-backlog socket failure must be reached");
                    assert_eq!(failed["actual_request_kind"], "MetadataReadAcquire");
                    assert_eq!(failed["actual_business"], request["actual_business"]);
                    assert!(failed["original_parent_remaining_us"]
                        .as_u64()
                        .is_some_and(|n| n > 0));
                    match cut {
                        Cut::Resume => {
                            let succeeded = reads
                                .iter()
                                .find(|r| r["returned_ok"] == true)
                                .expect("actual new authenticated genesis MQ must return");
                            assert_eq!(succeeded["actual_range"], failed["actual_range"]);
                            assert_eq!(succeeded["actual_epoch"], failed["actual_epoch"]);
                            assert_ne!(succeeded["actual_business"], failed["actual_business"]);
                            assert_ne!(
                                succeeded["actual_success_acquisition"],
                                request["actual_acquisition"]
                            );
                            assert!(succeeded["actual_success_context"]
                                .as_str()
                                .is_some_and(|s| s != Uuid::nil().to_string()));
                            assert!(succeeded["original_parent_remaining_us"]
                                .as_u64()
                                .is_some_and(|n| n > 0));
                            assert!(succeeded["actual_success_deadline_remaining_us"]
                                .as_u64()
                                .is_some_and(|n| n > 0));
                            // Later native QQ/genesis/final-MQ/dispatcher errors are
                            // retained above, never called full DATA/APP success.
                        }
                        Cut::Cancel | Cut::Expire => {
                            if matches!(cut, Cut::Cancel) {
                                assert!(matches!(returned, Err(BicDbError::QueryCanceled)));
                            } else {
                                assert!(matches!(returned, Err(BicDbError::QueryTimedOut)));
                            }
                            assert!(reads.iter().all(|r| r["returned_ok"] == false));
                            assert_eq!(state["actual_APP_published"], false);
                            assert!(!state["actual_genesis_ready_ranges"]
                                .as_array()
                                .unwrap()
                                .contains(&request["actual_range"]));
                        }
                    }
                }));
            if let Ok((mut owner, _, _)) = leader_result {
                if let Err(error) = owner.stop(&budget(5000)) {
                    cleanup_errors.push(error.to_string());
                }
            }
            if let Some(Ok((mut owner, _, _))) = follower_result {
                if let Err(error) = owner.stop(&budget(5000)) {
                    cleanup_errors.push(error.to_string());
                }
            }
            events.push(
                serde_json::json!({"stage":"all_worker_and_cleanup_outcomes",
                "worker_panics":worker_panics,"cleanup_errors":cleanup_errors}),
            );
            if let Err(panic) = action {
                std::panic::resume_unwind(panic);
            }
            if let Err(panic) = follower_assertions {
                std::panic::resume_unwind(panic);
            }
            assert!(
                worker_panics.is_empty(),
                "actual workers panicked: {worker_panics:?}"
            );
            assert!(
                cleanup_errors.is_empty(),
                "actual owner stop failed: {cleanup_errors:?}"
            );
            drop(release);
        })
    }));
    let mut cleanup_errors = Vec::new();
    for control in &mut controls {
        if let Err(error) = control.stop(&budget(5000)) {
            cleanup_errors.push(error.to_string());
        }
    }
    eprintln!(
        "ACTUAL_INITIAL_GENESIS_READ_TRANSITION {}",
        serde_json::json!({
        "cut":format!("{cut:?}"),"events":events,"remaining_cleanup_errors":cleanup_errors,
        "full_DATA_Serving_physical_qualification":false,"source_definition_only_until_executed":true})
    );
    drop(fixture);
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    assert!(
        cleanup_errors.is_empty(),
        "other owner cleanup failed: {cleanup_errors:?}"
    );
}
#[test]
fn native_initial_genesis_actual_old_backlog_read_recovers_under_original_action() {
    run(Cut::Resume);
}
#[test]
fn native_initial_genesis_actual_socket_failure_original_cancel_blocks_readiness() {
    run(Cut::Cancel);
}
#[test]
fn native_initial_genesis_actual_socket_failure_original_expiry_blocks_readiness() {
    run(Cut::Expire);
}
