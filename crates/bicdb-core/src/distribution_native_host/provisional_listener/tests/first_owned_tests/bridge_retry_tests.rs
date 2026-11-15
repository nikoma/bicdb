//! Actual native bridge cuts and distinct same-original readonly resolution.
//! No simulated IO outcome, token, owner or frame may satisfy these controls.
use super::*;
fn control(
    target: &mpsc::Sender<OwnerCommand>,
    range: crate::RangeId,
    arm: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, recv) = mpsc::sync_channel(1);
    target
        .send(OwnerCommand::BridgeControl(range, arm, send))
        .map_err(|_| invalid("actual bridge control owner ended"))?;
    let actual = recv
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual bridge control return unavailable"))?;
    if actual["ok"] != true {
        return Err(invalid(&format!("actual bridge control failed: {actual}")));
    }
    Ok(actual["data"].clone())
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    coordinator: &mut NativeMetadataHost,
    relocation: crate::distribution::RelocationId,
    phase: Uuid,
    range: crate::RangeId,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    case: Case,
    lifetime: &CancellationToken,
) -> Result<()> {
    let mode = if case == Case::BridgeCancelledAfterNative {
        1
    } else {
        2
    };
    let before = report.data_call("actual_bridge_original_owner_before_native_fault", || {
        control(target, range, Some(mode))
    })?;
    let first = report.data_call(
        "actual_bridge_original_native_call_with_genuine_cut",
        || coordinator.drive_finite_remote_bridge(relocation, phase, &node(4), lifetime),
    );
    let after = report.data_call("actual_bridge_retained_original_outcome_after_call", || {
        control(target, range, None)
    });
    report.event(serde_json::json!({"actual_original_return":first.as_ref().ok(),"actual_original_error":first.as_ref().err().map(ToString::to_string),
        "native_before":before,"native_after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string)}));
    let after = after?;
    assert!(
        first.is_err(),
        "the genuine cut must refuse the original request"
    );
    let attempt = &after["memory_before_storage"]["bridge"]["actual_native_attempt"];
    assert!(
        attempt["actual_issued_xid"].as_u64().is_some_and(|x| x > 0),
        "actual original native bridge must be entered"
    );
    assert!(attempt["original_candidate"].is_object());
    let replay = report.data_call("actual_unknown_bridge_mutation_replay_refused", || {
        coordinator.drive_finite_remote_bridge(relocation, phase, &node(4), lifetime)
    });
    let denied = report.data_call("actual_bridge_state_after_mutation_replay_denial", || {
        control(target, range, None)
    })?;
    report.event(serde_json::json!({"replay_error":replay.as_ref().err().map(ToString::to_string),"native_after_denial":denied}));
    assert!(replay.as_ref().err().is_some_and(|e| e
        .to_string()
        .contains("original pending return requires readonly resolution")));
    assert_eq!(
        after, denied,
        "denied mutation may not dispatch or replace original native attempt"
    );
    let resolved = report.data_call("fresh_actual_TLS_same_original_ResolveBridge", || {
        coordinator.resolve_finite_remote_bridge(relocation, phase, &node(4), lifetime)
    });
    let final_state = report.data_call("actual_bridge_after_distinct_known_only_reader", || {
        control(target, range, None)
    });
    report.event(serde_json::json!({"fresh_resolution_return":resolved.as_ref().ok(),"fresh_resolution_error":resolved.as_ref().err().map(ToString::to_string),
        "actual_final_native":final_state.as_ref().ok(),"diagnostic_error":final_state.as_ref().err().map(ToString::to_string),
        "old_call_not_relabelled_success":true,"no_postfault_APP_or_Serving_claim":true}));
    let final_state = final_state?;
    let current = &final_state["memory_before_storage"]["bridge"]["actual_native_attempt"];
    assert_eq!(attempt["actual_issued_xid"], current["actual_issued_xid"]);
    assert_eq!(attempt["original_candidate"], current["original_candidate"]);
    assert_eq!(attempt["actual_return"], current["actual_return"]);
    assert_eq!(
        after["storage"]["native_next_xid"],
        final_state["storage"]["native_next_xid"]
    );
    assert_eq!(
        after["storage"]["native_next_lsn"],
        final_state["storage"]["native_next_lsn"]
    );
    if case == Case::BridgeCancelledAfterNative {
        assert_eq!(attempt["actual_return"], "actual_native_returned_known");
        assert_eq!(attempt["actual_cancel_after_native_return_fired"], 1);
        let event = &attempt["actual_cancel_event"];
        assert_eq!(event["before_cancel"]["server_cancelled"], false);
        assert_eq!(event["before_cancel"]["producer_process_live"], true);
        for key in [
            "server_remaining_us",
            "role_remaining_us",
            "own_remaining_us",
        ] {
            assert!(
                event["before_cancel"][key].as_u64().is_some_and(|v| v > 0),
                "original {key} must be live at deliberate native-return cut"
            );
        }
        assert_eq!(event["original_server_cancelled"], true);
        assert_eq!(event["producer_process_live_after_cancel"], true);
        assert_eq!(
            after["memory_before_storage"]["bridge"]["completed_native_owner_retained"],
            false
        );
        let actual = resolved?;
        assert_eq!(actual["readonly_resolution"], true);
        assert_eq!(actual["original_call_relabelled_success"], false);
        assert_eq!(
            actual["point"]["native_bridge_sha256"],
            attempt["original_candidate"]["checksum_sha256"]
        );
        assert_eq!(
            final_state["memory_before_storage"]["bridge"]["completed_native_owner_retained"],
            true
        );
        assert_eq!(
            after["storage"]["bridge_current"],
            final_state["storage"]["bridge_current"]
        );
        assert_eq!(
            final_state["storage"]["bridge_current"]["decoded"],
            final_state["storage"]["bridge_archive"]["decoded"]
        );
    } else {
        assert_eq!(attempt["actual_return"], "actual_native_returned_unknown");
        assert_eq!(attempt["actual_commit_sync_fault_fired"], 1);
        assert!(
            resolved.is_err(),
            "actual sync Unknown must never become known from visible bytes"
        );
        assert_eq!(
            final_state["memory_before_storage"]["bridge"]["completed_native_owner_retained"],
            false
        );
        assert_eq!(attempt["actual_error"], current["actual_error"]);
    }
    Ok(())
}
