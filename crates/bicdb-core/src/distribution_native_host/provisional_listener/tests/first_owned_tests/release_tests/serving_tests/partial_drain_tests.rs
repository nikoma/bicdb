//! Actual socket/handler continuation component; no SDK/Serving success claim.
use super::*;
use crate::distribution_transport::InitialTransitionControl;
struct Release<'a> {
    listener: &'a InitialTransitionControl,
    peer: &'a CancellationToken,
}
impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.listener.release_all();
        self.peer.cancel();
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    coordinator: &mut NativeMetadataHost,
    metadata: usize,
    member: &ClusterNodeId,
    range: crate::RangeId,
    phase: Uuid,
    controls: &BTreeMap<ClusterNodeId, mpsc::Sender<Control>>,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let business = Uuid::now_v7();
    let peer_request = parent.child_with_deadline(Instant::now() + Duration::from_secs(4));
    let peer_call =
        coordinator.test_actual_partial_drain_peer_call(member, business, &peer_request)?;
    let (ready, started) = mpsc::sync_channel(1);
    let (send, returned) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::LocalServingPartialDrain(
            range,
            phase,
            node(metadata),
            business,
            ready,
            send,
        ))
        .map_err(|_| invalid("actual partial-drain target actor ended"))?;
    let listener = started
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual Installed Assigned listener cut unavailable"))?;
    let (cancelled, retained, held, peer) = std::thread::scope(|scope| -> Result<_> {
        let reader = scope.spawn(move || peer_call());
        // Normal path: release_auth precedes the explicit join; cancellation
        // occurs only on guard drop AFTER that join, preserving the exact
        // original peer error. Early unwind drops this guard (declared after
        // the worker) before automatic scope join, releasing AND cancelling.
        let cleanup = Release {
            listener: &listener,
            peer: &peer_request,
        };
        let action = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let actual = report.data_call(
                "actual_shutdown_and_counted_peer_drain_cancel_API_return",
                || {
                    returned
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(|_| invalid("actual partial drain return unavailable"))
                },
            );
            let retained = report.data_call(
                "actual_native_Installed_and_error_history_after_partial_drain",
                || target_control(target, range, phase, false, None),
            );
            let held = listener.snapshot();
            (actual, retained, held)
        }));
        listener.release_auth();
        let read = reader.join();
        report.event(serde_json::json!({"actual_partial_drain_primary":match &action {
            Ok((returned,retained,held))=>serde_json::json!({"actual_return":returned.as_ref().ok(),
                "return_error":returned.as_ref().err().map(ToString::to_string),
                "actual_retained":retained.as_ref().ok(),"retained_error":retained.as_ref().err().map(ToString::to_string),
                "actual_still_held_handler":held}),
            Err(panic)=>serde_json::json!({"primary_panic":panic_data(panic.as_ref())}),
        },"actual_authenticated_peer_return":read.as_ref().ok().and_then(|r|r.as_ref().ok()),
            "actual_peer_error":read.as_ref().ok().and_then(|r|r.as_ref().err()).map(ToString::to_string),
            "peer_worker_panic":read.as_ref().err().map(|p|panic_data(p.as_ref())),
            "cleanup_released_original_handler_before_join":true}));
        drop(cleanup);
        let (actual, retained, held) = match action {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        // Actual action failure remains primary; every peer panic/error is
        // already retained. A successful action cannot suppress failed cleanup.
        let actual = require_actual_ok(actual?)?;
        let retained = retained?;
        let peer = match read {
            Ok(value) => value?,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        Ok((actual, retained, held, peer))
    })?;
    let cut = &cancelled["actual_shutdown_active_drain_cut"];
    assert_eq!(cut["same_original_listener"], true);
    assert_eq!(cut["actual_shutdown_requested"], true);
    assert_eq!(cut["actual_accept_handle_joined"], true);
    assert!(cut["actual_active_handlers"]
        .as_u64()
        .is_some_and(|n| n > 0));
    assert_eq!(cut["original_parent_live_before_cancel"], true);
    assert!(cut["original_parent_remaining_us_before_cancel"]
        .as_u64()
        .is_some_and(|n| n > 0));
    let handler = &cut["actual_authenticated_handler"];
    assert_eq!(handler["actual_authenticated_handler_paused"], true);
    assert!(
        handler["actual_authenticated_handler_remaining_us"]
            .as_u64()
            .is_some_and(|n| n > 0),
        "expired original server handler is an unreached intended cut"
    );
    assert_eq!(
        handler["actual_authenticated_request"]["TLS_authenticated"],
        true
    );
    assert_eq!(
        handler["actual_authenticated_request"]["actual_requester"],
        serde_json::to_value(node(metadata))?
    );
    assert_eq!(
        handler["actual_authenticated_request"]["actual_business"],
        peer["actual_business"]
    );
    assert_eq!(held["actual_shutdown"], true);
    assert!(held["actual_active_connections"]
        .as_u64()
        .is_some_and(|n| n > 0));
    assert_eq!(
        cancelled["actual_original_stop_and_drain_return"]["actual_returned_ok"],
        false
    );
    assert_eq!(
        cancelled["actual_original_stop_and_drain_return"]["actual_typed_QueryCanceled"],
        true
    );
    assert!(!cancelled["actual_original_stop_and_drain_return"]["original_error"].is_null());
    assert_eq!(cancelled["original_policy_API_returned_ok"], false);
    assert_eq!(cancelled["original_policy_typed_QueryCanceled"], true);
    assert!(!cancelled["original_policy_error"].is_null());
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_original_Closed_life"],
        false
    );
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_same_listener_retained"],
        true
    );
    assert_eq!(
        cancelled["actual_retained_Assigned"]["actual_stopping"],
        false
    );
    assert_eq!(cancelled["actual_Joined_owner_present"], false);
    assert_eq!(
        cancelled["actual_original_install_evidence"]["stage"],
        "Installed"
    );
    assert!(cancelled["install_evidence_error"].is_null());
    assert_eq!(peer["actual_API_returned_ok"], false);
    assert_eq!(peer["actual_cluster_body"],format!(
        "cluster node {} rejected RPC cluster_data_operation_failed: cluster error: native host bootstrap: assigned CLOSED owner stopped or ended",member),
        "held real peer must retain the original ended-Closed refusal after actual release, not another error");
    // The real accepted peer has returned and joined. A distinct finite actor
    // command now resumes the same shutdown handle through genuine Drained.
    let resumed = report.data_call(
        "distinct_actor_resumes_actual_partially_drained_same_Installed_handle",
        || {
            actor_call(
                coordinator,
                metadata,
                member,
                range,
                phase,
                controls,
                target,
                parent,
                false,
            )
        },
    );
    let (send, receive) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::PolicySocketProfile(range, phase, send))
        .map_err(|_| invalid("actual resumed partial-drain owner ended"))?;
    let profile = report.data_call("actual_Joined_after_partial_socket_drain_resume", || {
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| invalid("actual partial drain resume profile unavailable"))
    });
    let after = report.data_call(
        "actual_same_native_after_partial_socket_drain_resume",
        || target_control(target, range, phase, false, None),
    );
    report.event(serde_json::json!({"actual_resume":resumed.as_ref().ok(),"resume_error":resumed.as_ref().err().map(ToString::to_string),
        "actual_profile":profile.as_ref().ok(),"profile_error":profile.as_ref().err().map(ToString::to_string),
        "actual_native_after":after.as_ref().ok(),"snapshot_error":after.as_ref().err().map(ToString::to_string),
        "snapshots_separate_nonatomic_DATA":true,"component_not_SDK_Strong_or_automatic_recovery_acceptance":true}));
    let resumed = require_actual_ok(resumed?)?;
    let profile = require_actual_ok(profile?)?;
    let after = after?;
    assert_eq!(
        resumed["transition"],
        "actual_policy_installed_readiness_still_request_bound"
    );
    assert_eq!(resumed["incomplete"], true);
    assert_eq!(resumed["Serving"], false);
    assert_eq!(profile["actual_Registered_Joined_owner"], true);
    assert_eq!(profile["actual_replacement_listener_bound"], true);
    assert_eq!(profile["actual_original_drained_owner_verified"], true);
    assert_eq!(profile["actual_APP_dispatcher_published"], false);
    assert_eq!(
        profile["original_listeners"]["replica_listener_present"],
        false
    );
    assert_eq!(
        profile["original_listeners"]["old_replica_listener_present"],
        false
    );
    assert_eq!(
        profile["actual_original_install_evidence"],
        cancelled["actual_original_install_evidence"]
    );
    same_native(&retained, &after);
    assert_eq!(retained["queue"], after["queue"]);
    for history in ["policy_API_returns", "policy_outer_API_returns"] {
        assert_eq!(
            retained["actual_successor"][history]["last_actual_return_ok"],
            false
        );
        assert!(!retained["actual_successor"][history]["first_original_error"].is_null());
        assert_eq!(
            after["actual_successor"][history]["first_original_error"],
            retained["actual_successor"][history]["first_original_error"]
        );
        assert_eq!(
            after["actual_successor"][history]["last_actual_return_ok"],
            true
        );
        let count = retained["actual_successor"][history]["returns"]
            .as_u64()
            .expect("actual original policy history required");
        assert_eq!(after["actual_successor"][history]["returns"], count + 1);
    }
    assert_eq!(
        report.omitted, 0,
        "all actual partial-drain/peer/resume evidence must be retained"
    );
    Ok(())
}
