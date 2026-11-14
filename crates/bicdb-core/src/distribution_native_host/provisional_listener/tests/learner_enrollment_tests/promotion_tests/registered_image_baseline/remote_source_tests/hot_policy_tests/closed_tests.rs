//! Real survivor native CLOSED audit followed by canceled reply and a fresh
//! SAME-owner reconciliation. No source/target readiness or Serving is fabricated.
use super::*;

#[test]
fn native_registered_fifth_remote_hot_closed_audit_native_reply_lost_same_attempt_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotClosedAuditReplyLost),
    );
}
pub(super) fn exercise(
    control: &mut NativeMetadataControlHost,
    selected: crate::RangeId,
    process: &CancellationToken,
    output: &std::path::Path,
) {
    let request = budget(5000);
    let cancel = request.clone();
    let observed = Arc::new(Mutex::new(None));
    let capture = observed.clone();
    let cut = crate::BicDb::test_writer_closed_native_after_commit(move |actual| {
        *capture.lock() = Some(actual);
        cancel.cancel();
    });
    let result = control.advance_native_writer_closed_domain(selected, &request);
    drop(cut);
    let before = observed.lock().clone();
    evidence(&output.join("registered-survivor-closed-audit-retry.json"), &serde_json::json!({
        "stage":"after_original_result_before_assertions",
        "original_result":result.as_ref().err().map(ToString::to_string),
        "actual_cut":before,
        "original_parent_still_live":process.check().is_ok(),
        "closed_fact_unavailable_before_fresh_reconciliation":control.native_closed_writer_domain(selected, process).is_err(),
    })).unwrap();
    assert!(result.is_err());
    process.check().unwrap();
    let before = before.expect("actual native CLOSED post-readback cut must run");
    assert_eq!(
        before["actual_original_survivor_historical_scope_verified"],
        true
    );
    assert!(
        before["native_closed_record"]["native_page_xid"]
            .as_u64()
            .unwrap()
            > 0
    );
    for key in ["ordinary_range_error", "ordinary_no_image_error"] {
        assert_eq!(
            before[key],
            "cluster error: native range image: selected writer DATA policy publication is CLOSED or unresolved"
        );
    }
    assert_eq!(before["other_original_range_still_serving"], true);
    assert!(before["wrong_range_error"]
        .as_str()
        .unwrap()
        .contains("changed selected range"));
    assert!(control
        .native_closed_writer_domain(selected, process)
        .is_err());
    let fresh = budget(5000);
    let retry = control.advance_native_writer_closed_domain(selected, &fresh);
    let after = control
        .native_closed_writer_domain(selected, process)
        .and_then(|actual| actual.test_closed_record_evidence());
    evidence(
        &output.join("registered-survivor-closed-audit-retry.json"),
        &serde_json::json!({
            "stage":"fresh_same_attempt_original_audit_reconciled",
            "original_result":result.as_ref().err().map(ToString::to_string),
            "actual_cut":before,
            "fresh_retry_result":retry.as_ref().err().map(ToString::to_string),
            "actual_after_record":after.as_ref().ok(),
            "after_error":after.as_ref().err().map(ToString::to_string),
            "original_parent_still_live":process.check().is_ok(),
            "external_application_qualification":false,
        }),
    )
    .unwrap();
    retry.unwrap();
    assert_eq!(after.unwrap(), before["native_closed_record"]);
}
