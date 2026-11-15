//! Actual normal-host finite observations. No failed opening is retried and
//! no failed SourceStart/Describe may create missing capture custody.
use super::*;
#[test]
fn native_first_actual_remote_new_identity_own_cancel_refuses_while_caller_live() {
    run(Case::RemoteNewOwnCancel);
}
pub(super) fn own_cancel(
    host: &NativeMetadataHost,
    range: crate::RangeId,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let data = report.data_call(
        "actual_RemoteNEW_identity_OWN_cancel_same_original_caller",
        || host.test_first_metadata_identity_own_cancel(range, parent),
    )?;
    assert!(data["original_control_error"].is_null());
    assert!(data["before_diagnostic_error"].is_null());
    assert!(data["after_diagnostic_error"].is_null());
    let identity = &data["identity_control"];
    assert!(identity["original_control_error"].is_null());
    assert_eq!(identity["outer_caller_still_live"], true);
    assert_eq!(identity["actual_OWN_original_cancelled"], true);
    assert_eq!(identity["actual_OWN_live_after"], false);
    let actual = &identity["actual_control"];
    assert_eq!(actual["cut"]["actual_failed_service_slice"], true);
    assert_eq!(actual["cut"]["parent_live_before_action"], true);
    assert_eq!(actual["actual_returned_ok"], false);
    assert_eq!(actual["original_error_class"], "QueryCanceled");
    assert_eq!(actual["holder_still_held_at_original_return"], true);
    assert!(actual["cleanup_error"].is_null());
    for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
        assert!(data["before_native"]["actual_native_storage"][field].is_number());
        assert_eq!(
            data["before_native"]["actual_native_storage"][field],
            data["after_native"]["actual_native_storage"][field]
        );
    }
    assert!(data["before_native"]["actual_source_lineage"].is_object());
    assert_eq!(
        data["before_native"]["actual_source_lineage"],
        data["after_native"]["actual_source_lineage"]
    );
    Ok(())
}
#[test]
fn native_first_actual_retained_maintenance_state_release_same_request() {
    run(Case::HotRequestWait);
}
#[test]
fn native_first_actual_retained_maintenance_state_cancel_same_request() {
    run(Case::HotRequestCancel);
}
#[test]
fn native_first_actual_retained_maintenance_state_original_expiry() {
    run(Case::HotRequestExpiry);
}
#[test]
fn native_first_actual_post_open_status_service_release_same_request() {
    run(Case::StatusRequestWait);
}
#[test]
fn native_first_actual_post_open_status_service_cancel_same_request() {
    run(Case::StatusRequestCancel);
}
#[test]
fn native_first_actual_post_open_status_service_original_expiry() {
    run(Case::StatusRequestExpiry);
}
#[test]
fn native_first_actual_closed_old_service_release_full_observation() {
    run(Case::OldRequestWait);
}
#[test]
fn native_first_actual_closed_old_service_cancel_preserves_native() {
    run(Case::OldRequestCancel);
}
#[test]
fn native_first_actual_closed_old_service_earliest_original_expiry() {
    run(Case::OldRequestExpiry);
}

fn require_actual(data: &serde_json::Value, mode: u8) {
    assert!(data["original_control_error"].is_null());
    assert!(data["before_diagnostic_error"].is_null());
    assert!(data["after_diagnostic_error"].is_null());
    assert!(data["before_native"].is_object());
    for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
        assert!(
            data["before_native"][field].is_number(),
            "actual native counter required: {field}"
        );
        assert_eq!(data["before_native"][field], data["after_native"][field]);
    }
    assert_eq!(data["original_process_still_live"], true);
    let actual = &data["actual_control"];
    assert_eq!(actual["actual_original_instant_busy"], true);
    assert_eq!(actual["cut"]["actual_failed_service_slice"], true);
    assert_eq!(actual["cut"]["parent_live_before_action"], true);
    assert!(actual["cleanup_error"].is_null());
    assert!(actual["serialization_error"].is_null());
    assert_eq!(actual["same_original_request_no_new_deadline"], true);
    if mode == 0 {
        assert_eq!(actual["actual_returned_ok"], true);
        assert!(actual["original_error"].is_null());
    } else {
        assert_eq!(actual["actual_returned_ok"], false);
        assert_eq!(actual["holder_still_held_at_original_return"], true);
        assert_eq!(
            actual["original_error_class"],
            if mode == 1 {
                "QueryCanceled"
            } else {
                "QueryTimedOut"
            }
        );
    }
}
pub(super) fn hot(
    host: &NativeMetadataHost,
    range: crate::RangeId,
    case: Case,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let mode = match case {
        Case::HotRequestWait => 0,
        Case::HotRequestCancel => 1,
        Case::HotRequestExpiry => 2,
        _ => return Err(invalid("wrong State wait control")),
    };
    let data = report.data_call(
        "actual_same_maintenance_State_original_request_wait",
        || host.test_first_hot_request_wait(range, mode, parent),
    )?;
    assert!(data["original_control_error"].is_null());
    assert!(data["before_diagnostic_error"].is_null());
    assert!(data["after_diagnostic_error"].is_null());
    assert_eq!(data["original_process_still_live"], true);
    for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
        assert!(data["before_native"][field].is_number());
        assert_eq!(data["before_native"][field], data["after_native"][field]);
    }
    let actual = &data["actual_control"];
    assert_eq!(
        actual["original_instant_error"],
        "cluster error: native host bootstrap: hot DATA maintenance attempt already active"
    );
    assert_eq!(actual["cut"]["actual_failed_State_slice"], true);
    assert_eq!(actual["cut"]["parent_live_before_action"], true);
    assert_eq!(
        actual["actual_State_address"],
        actual["held"]["actual_State_address"]
    );
    assert_eq!(actual["held"], actual["same_holder_State_before_drop"]);
    assert!(actual["cleanup_error"].is_null());
    if mode == 0 {
        assert_eq!(actual["actual_returned_ok"], true);
    } else {
        assert_eq!(actual["actual_returned_ok"], false);
        assert_eq!(actual["holder_still_held_at_original_return"], true);
        assert_eq!(
            actual["original_error_class"],
            if mode == 1 {
                "QueryCanceled"
            } else {
                "QueryTimedOut"
            }
        );
    }
    Ok(())
}
pub(super) fn status(
    host: &NativeMetadataHost,
    range: crate::RangeId,
    case: Case,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let mode = match case {
        Case::StatusRequestWait => 0,
        Case::StatusRequestCancel => 1,
        Case::StatusRequestExpiry => 2,
        _ => return Err(invalid("wrong status wait control")),
    };
    let data = report.data_call(
        "actual_post_open_public_status_original_request_wait",
        || host.test_first_status_request_wait(range, mode, parent),
    )?;
    // Original API/control result and all diagnostic errors are reported first.
    require_actual(&data, mode);
    assert_eq!(data["actual_OpenInitialData_replayed"], false);
    Ok(())
}
pub(super) fn old(
    host: &NativeMetadataHost,
    range: crate::RangeId,
    case: Case,
    report: &mut Report,
    parent: &CancellationToken,
) -> Result<()> {
    let mode = match case {
        Case::OldRequestWait => 0,
        Case::OldRequestCancel => 1,
        Case::OldRequestExpiry => 2,
        _ => return Err(invalid("wrong OLD wait control")),
    };
    let data = report.data_call(
        "actual_closed_OLD_original_request_service_observation",
        || host.test_first_old_request_wait(range, mode, parent),
    )?;
    require_actual(&data, mode);
    assert_eq!(data["original_OWN_live_before"], true);
    assert!(data["before_OLD_owner_error"].is_null());
    assert!(data["after_OLD_owner_error"].is_null());
    assert_eq!(
        data["before_actual_OLD_owner"]["same_actual_Frozen_current"],
        true
    );
    assert!(data["before_actual_OLD_owner"]["actual_frozen_owner"].is_string());
    assert_eq!(
        data["before_actual_OLD_owner"],
        data["after_actual_OLD_owner"]
    );
    assert_eq!(
        data["original_OLD_instant_error"],
        "cluster error: native joint data handoff: old closure service busy during revalidation"
    );
    if mode != 2 {
        assert_eq!(data["original_OWN_live_after"], true);
    }
    assert_eq!(data["SourceStart_replayed"], false);
    assert_eq!(data["capture_permission_manufactured"], false);
    Ok(())
}
