//! Actual callback-token cuts; all other original scopes must stay LIVE.
use super::*;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetSourceExportCallbackCancelAtDetach
            | SourceCase::TargetSourceExportCallbackExpiryAtDetach
    )
}
#[test]
fn native_registered_fifth_source_export_retirement_actual_callback_cancel_at_final_detach() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetSourceExportCallbackCancelAtDetach),
    );
}
#[test]
fn native_registered_fifth_source_export_retirement_actual_callback_expiry_at_final_detach() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetSourceExportCallbackExpiryAtDetach),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn finish(
    case: SourceCase,
    donor: &NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    before: &serde_json::Value,
    after: &serde_json::Value,
    old_before: &serde_json::Value,
    old_after: &serde_json::Value,
    target_before: &serde_json::Value,
    result: &Result<crate::distribution_range_image::NativeSourceExportReplyV1>,
    manifest: &NativeRemoteImageManifestV1,
    source_journal: &[serde_json::Value],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
) {
    let trace = donor.test_source_export_callback_request_trace();
    let target_after = state(&factory.database, pending, &node(4), ranges);
    // Capture BOTH prerequisite and fault outcomes before any assertions. If
    // native/MQ work consumed the callback first, that is a prerequisite
    // failure, not evidence that the final-use check was actually reached.
    evidence(&output.join("source-export-callback-final-liveness.json"),&serde_json::json!({
        "case":format!("{case:?}"),"actual_callback_cut":trace.as_ref().ok(),
        "trace_error":trace.as_ref().err().map(|e|format!("{e:?}")),
        "target_before":target_before,"target_after":target_after.as_ref().ok(),
        "target_after_error":target_after.as_ref().err().map(|e|format!("{e:?}")),
        "source_before":before,"source_after":after,"old_native_before":old_before,"old_native_after":old_after,
        "actual_retire_error":result.as_ref().err().map(|e|format!("{e:?}")),
        "actual_retire_reply":result.as_ref().ok(),"actual_manifest":manifest,
        "source_frame_journal":source_journal,"original_app_acknowledgments":acknowledged,
        "configured_shorter_real_callback_budget_ms":if matches!(case,SourceCase::TargetSourceExportCallbackExpiryAtDetach){Some(600)}else{None},
        "outer_own_parent_sdk_budgets_extended":false,"serving_or_old_release_proof":false})).unwrap();
    let trace = trace.unwrap();
    assert_eq!(
        trace["seam_reached"], true,
        "actual callback final-cut prerequisite"
    );
    assert_eq!(trace["phase"], "after_actual_callback_fault");
    assert!(
        trace["action_error"].is_null(),
        "actual callback must complete live before injecting: {trace}"
    );
    assert!(trace["before"]["callback"]["exact_error"].is_null());
    assert!(
        trace["before"]["callback"]["remaining_us"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        trace["actual_callback_request"]["binding"]["manifest_sha256"],
        manifest.checksum_sha256
    );
    assert_eq!(
        trace["actual_callback_request"]["binding"]["target_attempt"],
        serde_json::to_value(manifest.target_attempt).unwrap()
    );
    for stage in ["before", "after"] {
        for field in [
            "own_live",
            "outer_parent_live",
            "original_source_process_live",
            "original_source_session_live",
        ] {
            assert_eq!(
                trace[stage][field], true,
                "distinct scope must remain live: {stage}/{field}: {trace}"
            );
        }
        assert_eq!(
            trace[stage]["original_source_session_cancel_requested"],
            false
        );
        assert!(trace[stage]["own_remaining_us"].as_u64().unwrap() > 0);
    }
    let error = trace["after"]["callback"]["exact_error"].as_str().unwrap();
    let final_point = &trace["actual_final_point"];
    for field in [
        "guard_check_reached",
        "outer_parent_live",
        "original_source_session_live",
        "original_source_process_live",
        "own_deadline_live",
    ] {
        assert_eq!(
            final_point[field], true,
            "actual final guarded callback check prerequisite: {field}: {trace}"
        );
    }
    assert_eq!(final_point["callback_live"], false);
    assert!(final_point["own_remaining_us"].as_u64().unwrap() > 0);
    if matches!(case, SourceCase::TargetSourceExportCallbackExpiryAtDetach) {
        assert_eq!(
            trace["actual_callback_request"]["budget_ms"], 600,
            "shorter real wire-budget prerequisite must be reached unchanged"
        );
        assert_eq!(trace["after"]["callback"]["remaining_us"], 0);
        assert_eq!(trace["after"]["callback"]["cancel_requested"], false);
        assert!(error.contains("QueryTimedOut"), "{error}");
        assert_eq!(final_point["callback_expired"], true);
        assert_eq!(final_point["callback_canceled"], false);
    } else {
        assert_eq!(trace["after"]["callback"]["cancel_requested"], true);
        assert!(trace["after"]["callback"]["remaining_us"].as_u64().unwrap() > 0);
        assert!(error.contains("QueryCanceled"), "{error}");
        assert_eq!(final_point["callback_canceled"], true);
        assert_eq!(final_point["callback_expired"], false);
    }
    assert!(
        result.is_err(),
        "expired/canceled actual callback cannot retire its export"
    );
    assert_eq!(
        old_before, old_after,
        "original native OLD receipts unchanged"
    );
    assert_eq!(
        *target_before,
        target_after.unwrap(),
        "readonly callback cannot change actual native image/base/rows"
    );
    assert_eq!(after["source"]["capture_retained"], true);
    assert_eq!(
        after["source"]["original_export_session_cancel_requested"],
        false
    );
    assert!(after["source"]["export_retirement"]["completed"].is_null());
    assert_eq!(
        after["source"]["actual_gate"],
        before["source"]["actual_gate"]
    );
    let observations = after["source"]["export_retirement"]["observations"]
        .as_array()
        .unwrap();
    assert_eq!(
        observations.len(),
        1,
        "exact real callback/intended outcome retained before refusal"
    );
    assert_eq!(
        observations[0]["actual_reply"]["request"],
        trace["actual_callback_request"]
    );
    assert_eq!(
        observations[0]["intended"]["binding"]["manifest_sha256"],
        manifest.checksum_sha256
    );
}
