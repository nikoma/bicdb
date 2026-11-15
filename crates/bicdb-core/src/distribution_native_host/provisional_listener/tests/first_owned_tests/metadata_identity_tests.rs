//! SAME original coordinator/OWN and real held service. These component
//! controls stop before ALL-OLD effects; they are not relocation acceptance.
use super::*;

#[test]
fn native_first_actual_same_service_identity_wait_releases_to_original_own() {
    run(Case::MetadataIdentityWait);
}
#[test]
fn native_first_actual_same_service_identity_wait_original_cancel_refuses() {
    run(Case::MetadataIdentityCancel);
}
#[test]
fn native_first_actual_same_service_identity_wait_original_expiry_refuses() {
    run(Case::MetadataIdentityExpiry);
}

pub(super) fn exercise(
    original: &NativeMetadataHost,
    range: crate::RangeId,
    case: Case,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let mode = match case {
        Case::MetadataIdentityWait => 0,
        Case::MetadataIdentityCancel => 1,
        Case::MetadataIdentityExpiry => 2,
        _ => return Err(invalid("wrong actual metadata identity control case")),
    };
    let before = report.data_call("actual_FIRST_owner_before_identity_contention", || {
        original.test_first_coordinator_snapshot(range)
    })?;
    let actual = report.data_call("actual_same_FIRST_OWN_identity_wait_return", || {
        original.test_first_metadata_identity_wait(range, mode, parent)
    });
    let after = report.data_call("actual_FIRST_owner_after_identity_contention", || {
        original.test_first_coordinator_snapshot(range)
    });
    report.event(serde_json::json!({"stage":"actual_metadata_identity_all_original_results_before_assertions",
        "actual_capture_control":actual.as_ref().ok(),"original_control_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "actual_owner_after":after.as_ref().ok(),"owner_error":after.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
        "control_does_not_call_ALL_OLD_native_mutations":true}));
    let actual = actual?;
    let after = after?;
    assert_eq!(
        before, after,
        "identity acquisition must preserve actual original FIRST progress/unknown/intent"
    );
    assert_eq!(actual["held"]["actual_service_guard_held"], true);
    assert_eq!(actual["held"]["actual_database_guard_live"], true);
    assert_eq!(
        actual["original_one_shot_exact_busy"], true,
        "one-shot callers that may hold a service must retain immediate strict refusal"
    );
    assert_eq!(
        actual["contention"]["actual_service_try_lock_slice_failed"], true,
        "real original service contention is mandatory, not a scheduled sleep"
    );
    assert_eq!(
        actual["contention"]["actual_capture_parent_live_before_action"],
        true
    );
    assert_eq!(
        actual["actual_worker_joined_ok"], true,
        "actual guard holder cleanup failed: {}",
        actual["cleanup_error"]
    );
    assert_eq!(
        actual["original_OWN_still_live"], true,
        "the intended capture-parent boundary must precede the genuine OWN expiry"
    );
    assert_eq!(actual["fresh_OWN_acquired_during_wait"], false);
    assert_eq!(actual["native_mutation_or_Close_RPC_called"], false);
    match mode {
        0 => {
            assert_eq!(actual["actual_capture_returned_ok"], true);
            assert_eq!(actual["actual_point"]["original_request_binding"], true);
            assert_eq!(
                actual["actual_point"]["context"],
                actual["original_OWN_context"]
            );
            assert_eq!(actual["actual_point"]["term"], actual["original_OWN_term"]);
            assert_eq!(
                actual["actual_point"]["index"],
                actual["original_OWN_index"]
            );
            assert_eq!(
                actual["actual_point"]["actual_store"],
                actual["held"]["actual_store"]
            );
            assert!(actual["original_capture_error"].is_null());
        }
        1 => {
            assert_eq!(actual["actual_capture_returned_ok"], false);
            assert_eq!(actual["original_capture_error_class"], "QueryCanceled");
            assert_eq!(actual["capture_parent_cancelled"], true);
            assert!(actual["actual_point"].is_null());
        }
        2 => {
            assert_eq!(actual["actual_capture_returned_ok"], false);
            assert_eq!(actual["original_capture_error_class"], "QueryTimedOut");
            assert_eq!(actual["capture_parent_expired"], true);
            assert_eq!(actual["capture_parent_cancelled"], false);
            assert!(actual["actual_point"].is_null());
        }
        _ => unreachable!(),
    }
    Ok(())
}
