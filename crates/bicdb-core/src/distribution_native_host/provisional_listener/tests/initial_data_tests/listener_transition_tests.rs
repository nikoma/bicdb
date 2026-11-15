//! Actual current native control owners, mutual TLS, real close/rebind and
//! original bounded OpenInitialData producer calls. No native authority is
//! synthesized, and this is not a full DATA/Serving/physical qualification.
use super::*;
use crate::distribution_transport::InitialTransitionControl;

#[derive(Clone, Copy, Debug)]
enum Cut {
    AcceptedAuthenticated,
    PendingBeforeAccept,
}
struct Release<'a> {
    server: &'a InitialTransitionControl,
    client: &'a InitialTransitionControl,
    requests: Vec<CancellationToken>,
}
impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.server.release_all();
        self.client.release_all();
        // These are the actual original caller tokens, not replacement process or
        // native permissions. Cancellation cannot erase an already durable effect.
        for request in &self.requests {
            request.cancel();
        }
    }
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
    let mut events = Vec::new();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        std::thread::scope(|scope| {
            // Created after the scope so panic cleanup releases hooks/cancels the
            // actual requests BEFORE Rust waits for scoped native owner threads.
            let release = Release {
                server: &server,
                client: &client,
                requests: vec![leader_original.clone(), follower_original.clone()],
            };
            let leader_request = &leader_original;
            let opening = scope.spawn(move || {
                let returned = leader.open_initial_data(leader_request);
                let state = leader.test_initial_transition_snapshot();
                (leader, returned, state)
            });
            server.wait("drain", &coordination).unwrap();
            server.wait("idle", &coordination).unwrap();
            match cut {
                Cut::AcceptedAuthenticated => server.pause_authenticated_host_read(node(1)),
                Cut::PendingBeforeAccept => {
                    server.pause_accept();
                    server.wait("accept", &coordination).unwrap();
                    server.wait("idle", &coordination).unwrap();
                    client.pause_connected_host_read(node(0));
                }
            }
            let follower_request = &follower_original;
            let reading = scope.spawn(move || {
                let returned = follower.open_initial_data(follower_request);
                let state = follower.test_initial_transition_snapshot();
                (follower, returned, state)
            });
            match cut {
                Cut::AcceptedAuthenticated => server.wait("auth", &coordination).unwrap(),
                Cut::PendingBeforeAccept => client.wait("connect", &coordination).unwrap(),
            }
            let before = server.snapshot();
            let client_before = client.snapshot();
            events.push(serde_json::json!({"stage":"actual_original_READ_paused_before_leader_transition","cut":format!("{cut:?}"),
            "server":before,"client":client_before,"original_deadline_not_recreated":true}));
            match cut {
                Cut::AcceptedAuthenticated => assert!(before["actual_active_connections"]
                    .as_u64()
                    .is_some_and(|n| n > 0)),
                Cut::PendingBeforeAccept => assert_eq!(before["actual_active_connections"], 0),
            }
            server.release_drain();
            server.wait("shutdown", &coordination).unwrap();
            if matches!(cut, Cut::AcceptedAuthenticated) {
                // An already authenticated worker is counted and drained. Do not
                // manufacture/assert a reset from semantic ingress revocation.
                let stopped = server.snapshot();
                events.push(serde_json::json!({"stage":"actual_drain_waiting_for_original_authenticated_worker","server":stopped}));
                assert!(stopped["actual_active_connections"]
                    .as_u64()
                    .is_some_and(|n| n > 0));
                server.release_auth();
            } else {
                server.wait("accept_ended", &coordination).unwrap();
                // The client socket was genuinely established to the old listen
                // backlog. Peer TLS is not yet authenticated on THIS failed try.
                client.release_connect();
            }
            // Observe the actual readonly return while its ORIGINAL action is
            // still live. Do not wait for unrelated genesis readiness and then
            // invent a replacement request budget to obtain this evidence.
            client
                .wait(
                    if matches!(cut, Cut::PendingBeforeAccept) {
                        "read_success"
                    } else {
                        "read_return"
                    },
                    &follower_original,
                )
                .unwrap();
            events.push(serde_json::json!({"stage":"actual_READ_outcome_under_original_action",
            "client":client.snapshot(),"original_parent_remaining_us":follower_original.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_micros())}));
            let (mut leader, leader_return, leader_state) = opening
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            let (mut follower, follower_return, follower_state) = reading
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            let trace = client.snapshot();
            events.push(serde_json::json!({"stage":"actual_original_OpenInitialData_returns","leader_error":leader_return.as_ref().err().map(ToString::to_string),
            "follower_error":follower_return.as_ref().err().map(ToString::to_string),"leader_returned_ok":leader_return.is_ok(),
            "follower_returned_ok":follower_return.is_ok(),"leader_retained_state":leader_state,"follower_retained_state":follower_state,
            "server":server.snapshot(),"actual_READ_trace":trace,"full_DATA_ready_not_asserted":true}));
            assert_eq!(leader_state["actual_old_listener_drained"], true);
            assert_eq!(leader_state["actual_DATA_listener_retained"], true);
            assert_eq!(follower_state["actual_initial_owner_retained"], true);
            let reads = trace["actual_read_returns"].as_array().unwrap();
            assert!(!reads.is_empty());
            assert_eq!(trace["omitted"], 0);
            match cut {
                Cut::AcceptedAuthenticated => {
                    assert_eq!(
                        before["actual_authenticated_request"]["TLS_authenticated"],
                        true
                    );
                    assert_eq!(
                        reads[0]["typed_socket_failure"], false,
                        "accepted semantic failures are framed, not fabricated socket resets"
                    );
                }
                Cut::PendingBeforeAccept => {
                    assert_eq!(
                        client_before["actual_connected_request"]["peer_TLS_not_yet_authenticated"],
                        true
                    );
                    assert!(
                        reads.iter().any(|r| r["typed_socket_failure"] == true),
                        "actual old-backlog failure must be reached"
                    );
                    assert!(
                        reads.iter().any(|r| r["returned_ok"] == true),
                        "genuine new authenticated READ must succeed under original action"
                    );
                    assert_eq!(
                        trace["actual_retained_enrollment_installs"], 1,
                        "READ retries cannot replay policy/hook/native enrollment"
                    );
                    assert!(reads.iter().all(|r| r["original_parent_remaining_us"]
                        .as_u64()
                        .is_some_and(|n| n > 0)));
                }
            }
            drop(release);
            // Original stop semantics are used for the actual retained owners.
            // A failed DATA open is kept in events; these tests assert only READ
            // handoff/continuation, not a hidden full-Serving success.
            let leader_stop = leader.stop(&budget(5000));
            let follower_stop = follower.stop(&budget(5000));
            events.push(serde_json::json!({"stage":"actual_owner_stop_returns","leader_error":leader_stop.as_ref().err().map(ToString::to_string),
            "follower_error":follower_stop.as_ref().err().map(ToString::to_string)}));
            leader_stop.unwrap();
            follower_stop.unwrap();
        })
    }));
    let mut other_cleanup_errors = Vec::new();
    for control in &mut controls {
        let result = control.stop(&budget(5000));
        if let Err(error) = &result {
            other_cleanup_errors.push(error.to_string());
        }
        events.push(serde_json::json!({"stage":"other_actual_owner_stop","error":result.as_ref().err().map(ToString::to_string)}));
    }
    eprintln!(
        "ACTUAL_INITIAL_LISTENER_TRANSITION {}",
        serde_json::json!({"cut":format!("{cut:?}"),"events":events,"physical_qualification":false,"runtime_source_definition_only_until_executed":true})
    );
    drop(fixture);
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
    assert!(
        other_cleanup_errors.is_empty(),
        "actual other owner stop errors: {other_cleanup_errors:?}"
    );
}
#[test]
fn native_initial_actual_authenticated_read_is_drained_across_control_to_DATA_transition() {
    run(Cut::AcceptedAuthenticated);
}
#[test]
fn native_initial_actual_old_accept_backlog_socket_failure_reacquires_READ_with_original_action() {
    run(Cut::PendingBeforeAccept);
}
