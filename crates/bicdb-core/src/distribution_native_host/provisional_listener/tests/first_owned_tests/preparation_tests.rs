//! Actual normal-host/TLS preparation controls, after the real FIRST receive,
//! native/catalogue decision and both real committed frozen membership steps.
//! These are five objects in one process, not five process/VM qualification.
use super::*;
pub(super) fn control(
    sender: &mpsc::Sender<OwnerCommand>,
    range: crate::RangeId,
    arm: Option<u8>,
) -> Result<serde_json::Value> {
    let (send, recv) = mpsc::sync_channel(1);
    sender
        .send(OwnerCommand::PreparationControl(range, arm, send))
        .map_err(|_| invalid("actual preparation owner control ended"))?;
    let actual = recv
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| invalid("actual preparation owner diagnostic unavailable"))?;
    if actual["ok"] != true {
        return Err(invalid(&format!(
            "actual preparation diagnostic failed: {actual}"
        )));
    }
    Ok(actual["data"].clone())
}
pub(super) fn exercise(
    five: &mut Five,
    metadata: usize,
    relocation: crate::distribution::RelocationId,
    phase: Uuid,
    range: crate::RangeId,
    final_members: &[ClusterNodeId],
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
    case: Case,
) -> Result<()> {
    for expected in ["PromotedFrozen", "AwaitingActivation"] {
        let actual = report.data_call(
            "real_metadata_owner_frozen_membership_for_remote_preparation",
            || {
                five.sources[metadata].advance_finite_remote_membership(
                    relocation,
                    phase,
                    &five.lifetime,
                )
            },
        )?;
        assert_eq!(actual["actual_committed_record"]["stage"], expected);
    }
    if release_tests::is_case(case) {
        return release_tests::exercise(
            five,
            metadata,
            relocation,
            phase,
            range,
            final_members,
            target,
            report,
            case,
        );
    }
    if open_tests::is_case(case) {
        return open_tests::exercise(
            five,
            metadata,
            relocation,
            phase,
            range,
            final_members,
            target,
            report,
            case,
        );
    }
    if matches!(
        case,
        Case::Bridge
            | Case::BridgeMissingPrepared
            | Case::BridgeCancelledAfterNative
            | Case::BridgeCommitUnknown
    ) {
        return bridge_tests::exercise(
            five,
            metadata,
            relocation,
            phase,
            range,
            final_members,
            target,
            report,
            case,
        );
    }
    let mode = match case {
        Case::PreparationCancelledAfterNative => Some(1),
        Case::PreparationCommitUnknown => Some(2),
        Case::PreparationHostPollCancelled => Some(3),
        Case::PreparationHostPollExpired => Some(4),
        _ => None,
    };
    let before = report.data_call("actual_target_preparation_original_before_step", || {
        control(target, range, mode)
    })?;
    let first = report.data_call(
        "actual_coordinator_real_TLS_Prepare_original_request",
        || {
            five.sources[metadata].drive_finite_remote_preparation(
                relocation,
                phase,
                &node(4),
                false,
                &five.lifetime,
            )
        },
    );
    let after = report.data_call(
        "actual_target_preparation_retained_after_original_return",
        || control(target, range, None),
    );
    report.event(serde_json::json!({"original_prepare_return":first.as_ref().ok(),"original_prepare_error":first.as_ref().err().map(ToString::to_string),
        "before":before,"after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string),
        "original_request_budget_ms":5000,"Serving":false,"physical_qualification_claim":false}));
    let after = after?;
    if matches!(
        case,
        Case::PreparationHostPollCancelled | Case::PreparationHostPollExpired
    ) {
        let expected = if case == Case::PreparationHostPollCancelled {
            BicDbError::QueryCanceled.to_string()
        } else {
            BicDbError::QueryTimedOut.to_string()
        };
        let actual_error = first
            .as_ref()
            .err()
            .expect("original actual Prepare must refuse its ended actor");
        // The actual server maps the handler error to this RPC envelope;
        // the pinned client returns Cluster(body) without another wrapper.
        // A caller timeout, setup refusal or unrelated substring cannot pass.
        let expected_body = format!(
            "cluster node {} rejected RPC cluster_data_operation_failed: {}",
            node(4),
            expected
        );
        assert!(
            matches!(actual_error, BicDbError::Cluster(body) if body == &expected_body),
            "required actual actor denial was not delivered: {actual_error:?}"
        );
        let cut = &after["memory_before_storage"]["actual_host_poll_cut"];
        assert_eq!(cut["fired"], 1);
        assert_eq!(cut["selected_original_tls_ingress"], true);
        assert_eq!(cut["original_server_still_live"], true);
        assert_eq!(cut["original_Role_still_live"], true);
        assert_eq!(cut["original_OWN_still_live"], true);
        assert_eq!(cut["original_server_deadline_unchanged"], true);
        assert_eq!(cut["original_server_cancelled"], false);
        assert_eq!(
            cut["real_poll_parent_cancelled"],
            case == Case::PreparationHostPollCancelled
        );
        assert_eq!(cut["actual_poll_parent_error"], expected);
        assert_eq!(cut["bound_effect_error"], expected);
        for field in [
            "original_server_remaining_us_before",
            "original_Role_remaining_us_before",
            "original_OWN_remaining_us_before",
        ] {
            assert!(
                cut[field].as_u64().is_some_and(|n| n > 0),
                "actual original allowance must remain positive: {field}"
            );
        }
        assert_eq!(
            after["memory_before_storage"]["continuation_present"],
            false
        );
        for observed in [&before, &after] {
            let storage = &observed["storage"];
            assert!(
                storage["health_error"].is_null(),
                "actual native owner must be healthy"
            );
            for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
                assert!(
                    storage[field].as_u64().is_some(),
                    "actual native counter required: {field}"
                );
            }
            for field in ["current", "archive", "bridge_current", "bridge_archive"] {
                let value = &storage[field];
                assert!(value.is_null() || (value["raw_read_error"].is_null()
                    && value["decode_error"].is_null() && value["omitted"].is_null()),
                    "actual absence/decoded bytes required, not a matching forensic failure: {field}");
            }
        }
        assert_eq!(
            before["storage"], after["storage"],
            "actual native/WAL/catalogue must remain unchanged before Prepare effects"
        );
        assert_eq!(
            before["memory_before_storage"]["actual_participant"],
            after["memory_before_storage"]["actual_participant"]
        );
        return Ok(());
    }
    if case == Case::Preparation {
        let actual = first?;
        assert!(actual["point"]["preparation"].is_object());
        assert_eq!(actual["local_quorum_constructed"], false);
        assert_eq!(
            after["memory_before_storage"]["native_attempt"]["completed_original_owner_retained"],
            true
        );
        assert_eq!(
            after["storage"]["current"]["decoded"],
            after["storage"]["archive"]["decoded"]
        );
        return Ok(());
    }
    assert!(first.is_err());
    let attempt = &after["memory_before_storage"]["native_attempt"]["actual_native_attempt"];
    assert!(
        attempt["actual_issued_xid"].as_u64().is_some_and(|x| x > 0),
        "genuine native attempt must be reached"
    );
    let replay = report.data_call(
        "same_original_unknown_Prepare_mutation_not_reissued",
        || {
            five.sources[metadata].drive_finite_remote_preparation(
                relocation,
                phase,
                &node(4),
                false,
                &five.lifetime,
            )
        },
    );
    let still = report.data_call(
        "actual_target_preparation_after_refused_mutation_replay",
        || control(target, range, None),
    )?;
    report.event(serde_json::json!({"mutation_replay_error":replay.as_ref().err().map(ToString::to_string),"before_resolution":after,"after_refused_replay":still}));
    assert!(replay.as_ref().err().is_some_and(|e| e
        .to_string()
        .contains("original pending return requires readonly resolution")));
    assert_eq!(after, still, "denial must not dispatch a new preparation");
    let resolved = report.data_call(
        "fresh_real_TLS_SAME_original_ResolvePrepared_readonly",
        || {
            five.sources[metadata].drive_finite_remote_preparation(
                relocation,
                phase,
                &node(4),
                true,
                &five.lifetime,
            )
        },
    );
    let final_state = report.data_call(
        "actual_native_preparation_after_distinct_readonly_resolution",
        || control(target, range, None),
    );
    report.event(serde_json::json!({"fresh_resolution_return":resolved.as_ref().ok(),"fresh_resolution_error":resolved.as_ref().err().map(ToString::to_string),
        "actual_final_native":final_state.as_ref().ok(),"diagnostic_error":final_state.as_ref().err().map(ToString::to_string),
        "old_IO_return_not_relabelled":true,"no_post_fault_APP_claim":true}));
    let final_state = final_state?;
    assert_eq!(
        attempt["actual_issued_xid"],
        final_state["memory_before_storage"]["native_attempt"]["actual_native_attempt"]
            ["actual_issued_xid"]
    );
    assert_eq!(
        attempt["issued_candidate"],
        final_state["memory_before_storage"]["native_attempt"]["actual_native_attempt"]
            ["issued_candidate"]
    );
    assert_eq!(
        after["storage"]["native_next_xid"],
        final_state["storage"]["native_next_xid"]
    );
    assert_eq!(
        after["storage"]["native_next_lsn"],
        final_state["storage"]["native_next_lsn"]
    );
    if case == Case::PreparationCancelledAfterNative {
        assert_eq!(
            attempt["actual_cancel_after_native_return_fired"], 1,
            "actual postcommit original-server cut required"
        );
        assert_eq!(
            attempt["actual_return"]["state"],
            "actual_native_returned_ok"
        );
        let before_cancel = &attempt["actual_cancel_event"]["before_cancel"];
        assert_eq!(before_cancel["server_cancelled"], false);
        assert_eq!(before_cancel["producer_process_live"], true);
        for window in [
            "server_remaining_us",
            "role_remaining_us",
            "own_remaining_us",
        ] {
            assert!(
                before_cancel[window].as_u64().is_some_and(|us| us > 0),
                "actual original {window} must still be live at the deliberate postcommit cut"
            );
        }
        assert_eq!(
            attempt["actual_cancel_event"]["original_server_cancelled"],
            true
        );
        assert_eq!(
            attempt["actual_cancel_event"]["producer_process_live_after_cancel"],
            true
        );
        assert_eq!(
            after["memory_before_storage"]["native_attempt"]["completed_original_owner_retained"],
            false
        );
        let actual = resolved?;
        assert_eq!(actual["readonly_resolution"], true);
        assert_eq!(actual["original_call_relabelled_success"], false);
        assert!(actual["point"]["preparation"].is_object());
        assert_eq!(
            final_state["memory_before_storage"]["native_attempt"]
                ["completed_original_owner_retained"],
            true
        );
        assert_eq!(
            after["storage"]["current"],
            final_state["storage"]["current"]
        );
        assert_eq!(
            final_state["storage"]["current"]["decoded"],
            final_state["storage"]["archive"]["decoded"]
        );
    } else {
        assert_eq!(
            attempt["actual_commit_sync_fault_fired"], 1,
            "real commit-sync IO fault required"
        );
        assert_eq!(
            attempt["actual_return"]["state"],
            "actual_native_returned_error_unknown_commit"
        );
        assert!(resolved.is_err());
        assert_eq!(
            final_state["memory_before_storage"]["native_attempt"]
                ["completed_original_owner_retained"],
            false
        );
        assert_eq!(
            attempt["actual_return"],
            final_state["memory_before_storage"]["native_attempt"]["actual_native_attempt"]
                ["actual_return"]
        );
    }
    Ok(())
}
