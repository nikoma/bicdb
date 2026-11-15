//! Genuine pinned-TLS publication after actual S2 + all participant pin handoffs.
//! Native DECISION only: these tests do not count a vote or activate a writer.
use super::*;
mod failure_evidence;
pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetRemoteDecision
            | SourceCase::TargetRemoteDecisionPostCommitCancel
            | SourceCase::TargetRemoteDecisionBeforePutCancel
            | SourceCase::TargetRemoteDecisionCommitUnknown
    )
}
#[test]
fn native_registered_fifth_remote_decision_actual_donor_provenance_and_native_target_record() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecision),
    );
}
#[test]
fn native_registered_fifth_remote_decision_actual_postcommit_cancel_reconciles_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionPostCommitCancel),
    );
}
#[test]
fn native_registered_fifth_remote_decision_actual_preput_abort_retains_original_attempt() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionBeforePutCancel),
    );
}
#[test]
fn native_registered_fifth_remote_decision_actual_commit_sync_unknown_never_replaces_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionCommitUnknown),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    target: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    mf: &NativeRemoteImageManifestV1,
    phase: Uuid,
    facts: &NativeRemoteNewFactSetV1,
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    journal: &[serde_json::Value],
    ledger: &mut Vec<serde_json::Value>,
) {
    // Preserve the existing exact per-call diagnostic bytes on a failing test.
    // This DATA-only guard never retains a native owner or changes its lifetime.
    let mut _failure_evidence = failure_evidence::OnPanic::new(output);
    let before = state(&factory.database, topology, &node(4), ranges).unwrap();
    let gate_before = gates(services, factory, mf.capture_range.id, mf);
    let operation = NewOp::PublishDecision {
        facts: facts.clone(),
    };
    let code = match case {
        SourceCase::TargetRemoteDecisionBeforePutCancel => Some(1),
        SourceCase::TargetRemoteDecisionPostCommitCancel => Some(2),
        SourceCase::TargetRemoteDecisionCommitUnknown => Some(3),
        _ => None,
    };
    if let Some(code) = code {
        target
            .test_arm_remote_native_decision_cut(mf.capture_range.id, code)
            .unwrap();
    }
    let first = exchange(
        &clients[0],
        &mf.target_id,
        &request(mf, phase, operation.clone()),
        ledger,
        output,
    );
    // Actual API result precedes ALL diagnostics; diagnostic errors are DATA.
    let primary = first.as_ref().err().map(|e| format!("{e:?}"));
    _failure_evidence.record_api("first", primary.as_deref());
    let native_first = target.test_remote_native_decision_evidence(mf.capture_range.id);
    let after_first = state(&factory.database, topology, &node(4), ranges);
    evidence(&output.join("remote-decision-first-before-assertions.json"),&serde_json::json!({
        "case":format!("{case:?}"),"original_MF":mf,"phase":phase,"fact_set":facts,
        "actual_error":primary,"actual_reply":first.as_ref().ok().map(|f|f.reply()),
        "native":native_first.as_ref().ok(),"native_diagnostic_error":native_first.as_ref().err().map(ToString::to_string),
        "before":before,"after":after_first.as_ref().ok(),"after_error":after_first.as_ref().err().map(ToString::to_string),
        "source_APP_acknowledgments":acknowledged,"every_actual_operation":ledger,"source_frame_journal":journal,
        "no_catalogue_vote_Ready_Serving_cold_or_physical_qualification":true})).unwrap();
    if code.is_none() {
        // Report a failed real publication at its actual API boundary. An
        // absent format flag after an earlier refusal is not a reader bug.
        first
            .as_ref()
            .expect("actual native publication must succeed before reader-fence controls");
    }
    let native_first = native_first.unwrap();
    if let Some(code) = code {
        assert_eq!(native_first["actual_test_cut"]["code"], code);
        assert_eq!(
            native_first["actual_test_cut"]["fired"], true,
            "the actual intended native cut must be reached; an earlier refusal is not its proof"
        );
    }
    let old_reader_error = crate::format::test_old_reader_without_remote_joint_decision(
        factory.database.read().data_path(),
    )
    .err()
    .map(|e| e.to_string());
    evidence(&output.join("remote-decision-reader-fence.json"),&serde_json::json!({"actual_old_reader_error":old_reader_error,"new_semantic_flag":crate::format::NATIVE_REMOTE_JOINT_DECISION_FEATURE_FLAG})).unwrap();
    assert!(
        old_reader_error.is_some(),
        "older reader must refuse actual remote semantic flag"
    );
    let actual = &native_first["native"]["actual_native_attempt"];
    let original_xid = actual["native_xid"]
        .as_u64()
        .expect("actual issued native xid retained");
    assert!(original_xid > 0);
    let outcome = actual["outcome"]["state"].as_str().unwrap();
    if code.is_none() {
        let controls =
            target.test_remote_native_decision_current_reader_refusals(mf.capture_range.id);
        evidence(&output.join("remote-decision-actual-current-reader-refusals.json"),&serde_json::json!({
            "actual_reader_observations":controls.as_ref().ok(),"diagnostic_error":controls.as_ref().err().map(ToString::to_string),
            "original_native_xid":original_xid,"injected_read_observations_not_disk_corruption":true,
        })).unwrap();
        let controls = controls.unwrap();
        assert_eq!(controls["actual_native_xid"], original_xid);
        assert_eq!(controls["actual_native_status"], "Committed");
        assert_eq!(controls["original_native_bytes_unchanged"], true);
        let observations = controls["observations"].as_array().unwrap();
        assert_eq!(observations.len(), 6);
        for observed in observations {
            assert_eq!(observed["receipt_returned"], false);
            assert!(!observed["actual_current_reader_error"].is_null());
            assert!(
                observed["actual_current_reader_error"]
                    .as_str()
                    .unwrap()
                    .contains(observed["expected_refusal"].as_str().unwrap()),
                "intended current-reader branch actually refused: {observed}"
            );
        }
    }
    if code.is_none() {
        audit_source_data_rejections(
            &actual["provenance"]["correlation"]["source_proof"],
            mf,
            output,
        );
    }
    if code == Some(1) {
        assert!(first.is_err());
        assert_eq!(outcome, "actual_abort_returned");
    } else if code == Some(3) {
        assert!(first.is_err());
        assert_eq!(outcome, "unknown_effect");
        assert_eq!(native_first["native"]["actual_commit_sync_fault_count"], 1);
    } else {
        assert_eq!(outcome, "actual_commit_returned");
        if code == Some(2) {
            assert!(first.is_err());
        } else {
            let reply = first
                .as_ref()
                .unwrap()
                .reply()
                .native_decision
                .as_ref()
                .unwrap();
            assert_eq!(reply.native_page_xid, original_xid);
        }
    }
    let retry = exchange(
        &clients[0],
        &mf.target_id,
        &request(mf, phase, operation),
        ledger,
        output,
    );
    let retry_primary = retry.as_ref().err().map(|e| format!("{e:?}"));
    _failure_evidence.record_api("retry", retry_primary.as_deref());
    let native_after = target.test_remote_native_decision_evidence(mf.capture_range.id);
    let after = state(&factory.database, topology, &node(4), ranges);
    let gate_after = gates(services, factory, mf.capture_range.id, mf);
    evidence(&output.join("remote-decision-retry-before-assertions.json"),&serde_json::json!({
        "case":format!("{case:?}"),"original_native_xid":original_xid,"actual_error":retry_primary,
        "actual_reply":retry.as_ref().ok().map(|f|f.reply()),"native_first":native_first,
        "native_after":native_after.as_ref().ok(),"native_diagnostic_error":native_after.as_ref().err().map(ToString::to_string),
        "before":before,"after":after.as_ref().ok(),"after_error":after.as_ref().err().map(ToString::to_string),
        "gate_before":gate_before,"gate_after":gate_after,"source_APP_acknowledgments":acknowledged,
        "every_actual_operation":ledger,"source_frame_journal":journal,"no_catalogue_vote_or_Serving":true})).unwrap();
    let native_after = native_after.unwrap();
    assert_eq!(
        native_after["native"], native_first["native"],
        "same retained native outcome/bytes/xid; no replacement"
    );
    assert_eq!(gate_before, gate_after);
    if code == Some(1) || code == Some(3) {
        assert!(retry.is_err());
    } else {
        let reply = retry.unwrap().reply().native_decision.clone().unwrap();
        assert_eq!(reply.native_page_xid, original_xid);
        assert_eq!(
            reply.native_record_sha256,
            actual["record"]["checksum_sha256"]
        );
        assert_eq!(
            before,
            after.unwrap(),
            "native metadata does not advance rows/floor/catalogue/writer"
        );
    }
    assert!(!factory.database.read().commit_admission_ready());
}

fn audit_source_data_rejections(
    actual: &serde_json::Value,
    mf: &NativeRemoteImageManifestV1,
    output: &std::path::Path,
) {
    use crate::distribution_range_handoff::NativeRemoteDecisionSourceData;
    let original: NativeRemoteDecisionSourceData = serde_json::from_value(actual.clone()).unwrap();
    original.require_manifest(mf).unwrap();
    let mut cases = Vec::new();
    let mut omitted = original.clone();
    omitted.constraints.pop();
    cases.push(("omitted_original_constraints", omitted));
    let mut duplicate = original.clone();
    duplicate.constraints[1] = duplicate.constraints[0].clone();
    cases.push(("duplicated_original_constraint", duplicate));
    let mut close = original.clone();
    close.original_close_records.remove(&mf.source_id);
    cases.push(("omitted_selected_native_close", close));
    let mut binding = original.clone();
    binding.binding.source_session = Uuid::now_v7();
    cases.push(("different_source_session", binding));
    let mut pending = original.clone();
    pending.constraints[0]
        .unresolved_commands
        .insert(mf.range_index + 1, "a".repeat(64));
    cases.push(("unresolved_future_obligation", pending));
    let mut native = original.clone();
    native.constraints[0].native_materialized_pending_index = Some(mf.range_index + 1);
    cases.push(("future_native_materialized_pending", native));
    let mut observations = Vec::new();
    for (name, mut data) in cases {
        data.checksum_sha256 = data.checksum().unwrap();
        let error = data.require_manifest(mf).err().map(|e| e.to_string());
        observations.push(
            serde_json::json!({"case":name,"actual_error":error,"DATA_only_not_RPC_or_grant":true}),
        );
    }
    evidence(
        &output.join("remote-decision-source-data-refusals.json"),
        &observations,
    )
    .unwrap();
    assert_eq!(observations.len(), 6);
    for value in observations {
        assert!(!value["actual_error"].is_null());
    }
}
