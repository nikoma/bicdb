//! Real original CLOSED survivor publication after actual scope acquisition.
//! The test-owned faults retain the same journal and genuine Controlled grant.
//! No passing result, timing or WAL stage is inferred from a source definition.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointActivationAdmission, NativeJointServingPublicationRound,
};
pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetPublicationClosedArchiveAfterScope
            | SourceCase::TargetPublicationMiddleHistoryAfterScope
    )
}
#[test]
fn native_registered_fifth_publication_closed_archive_after_scope_refuses_before_bridge_wal() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetPublicationClosedArchiveAfterScope),
    );
}
#[test]
fn native_registered_fifth_publication_retained_middle_history_after_scope_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetPublicationMiddleHistoryAfterScope),
    );
}
fn snapshot(
    service: &Arc<Mutex<ClusterDataNodeService>>,
    range: crate::distribution::RangeId,
) -> Result<serde_json::Value> {
    let service = service
        .try_lock()
        .ok_or_else(|| invalid("scope control diagnostic service busy"))?;
    let database = service.database();
    let db = database
        .try_read()
        .ok_or_else(|| invalid("scope control diagnostic database busy"))?;
    Ok(
        serde_json::json!({"storage":db.test_joint_prepublication_storage(range)?,
        "gate":db.range_mutation_gate().test_source_capture_pin_state(range)?}),
    )
}
pub(super) fn exercise(
    case: SourceCase,
    admission: &NativeJointActivationAdmission,
    publication: &mut NativeJointServingPublicationRound,
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    process: &CancellationToken,
    output: &std::path::Path,
) {
    let local = admission.local_preparation().local_node_id();
    let i = (0..services.len()).find(|i| node(*i) == *local).unwrap();
    let range = admission.data().old_range().id;
    let before = snapshot(&services[i], range);
    let code = if matches!(case, SourceCase::TargetPublicationClosedArchiveAfterScope) {
        1
    } else {
        2
    };
    let component = if code == 1 {
        let foreign = (0..services.len()).find(|other| *other != i).unwrap();
        Some(admission.test_fresh_publication_identity_substitutions(&services[foreign]))
    } else {
        None
    };
    let after_component = if code == 1 {
        Some(snapshot(&services[i], range))
    } else {
        None
    };
    BicDb::test_arm_closed_publication_scope_fault(code).unwrap();
    let primary = publication.publish_actual_local_member(admission, process);
    // Actual API outcome and clocks captured before any diagnostic storage I/O.
    let own_at_return = admission.committed().test_request_memory();
    let phase = admission.test_phase_cancellation();
    let at = Instant::now();
    let phase_remaining = phase
        .deadline()
        .map(|d| d.saturating_duration_since(at).as_micros());
    let phase_canceled = phase.is_cancel_requested();
    let fault = BicDb::test_take_closed_publication_scope_fault();
    let postwal = BicDb::test_take_native_joint_post_wal_failure();
    let after = snapshot(&services[i], range);
    let value = serde_json::json!({"case":format!("{case:?}"),
        "primary_success":primary.is_ok(),"primary_error":primary.as_ref().err().map(ToString::to_string),
        "original_own_at_return":own_at_return,"phase_remaining_us_at_return":phase_remaining,
        "phase_cancel_requested_at_return":phase_canceled,
        "fault":fault,"post_wal_failure":postwal,
        "identity_component":component.as_ref().and_then(|r|r.as_ref().ok()),
        "identity_component_error":component.as_ref().and_then(|r|r.as_ref().err()).map(ToString::to_string),
        "after_identity_component":after_component.as_ref().and_then(|r|r.as_ref().ok()),
        "after_identity_component_diagnostic_error":after_component.as_ref().and_then(|r|r.as_ref().err()).map(ToString::to_string),
        "before":before.as_ref().ok(),"before_diagnostic_error":before.as_ref().err().map(ToString::to_string),
        "after":after.as_ref().ok(),"after_diagnostic_error":after.as_ref().err().map(ToString::to_string),
        "scope":"test-owned after-scope corruption; API error never proves rollback; no Serving/physical qualification"});
    eprintln!("NATIVE_PUBLICATION_SCOPE_CONTROL {value}");
    if let Err(diagnostic) = evidence(
        &output.join("publication-scope-before-assertions.json"),
        &value,
    ) {
        if let Err(primary) = &primary {
            eprintln!("PUBLICATION_SCOPE_DIAGNOSTIC_FAILED {diagnostic}; original={primary}");
        } else {
            panic!("publication scope evidence failed: {diagnostic}");
        }
    }
    let before = before.unwrap();
    let after = after.unwrap();
    let fault = fault.expect("actual after-scope fault must be reached");
    assert_eq!(fault["actual_scope_completed"], true);
    assert_eq!(fault["fault_completed"], true);
    assert_eq!(fault["local_node_id"], serde_json::to_value(local).unwrap());
    assert!(fault["fault_xid"].as_u64().is_some_and(|n| n > 0));
    let error = primary.expect_err("the unchanged final publisher must reject actual corruption");
    if code == 1 {
        let component = component.unwrap().unwrap();
        let after_component = after_component.unwrap().unwrap();
        for key in ["foreign_service_error", "other_actual_preparation_error"] {
            assert_eq!(
                component[key],
                "cluster error: native joint data handoff: fresh publication substituted original service/preparation"
            );
        }
        for key in ["native_next_xid", "native_next_lsn"] {
            assert_eq!(before["storage"][key], after_component["storage"][key]);
        }
        assert_eq!(before["gate"], after_component["gate"]);
        assert!(
            matches!(error,BicDbError::Cluster(ref d) if d=="native range image: writer CLOSED audit CURRENT/archive missing")
        );
        assert!(after["storage"]["bridge_current"].is_null());
        for name in ["native_next_xid", "native_next_lsn"] {
            assert_eq!(fault["after_fault_storage"][name], after["storage"][name]);
        }
    } else {
        let index = fault["history_index"]
            .as_u64()
            .expect("actual middle history index");
        assert!(index > 0 && index < admission.data().range_index());
        assert!(
            matches!(error,BicDbError::Cluster(ref d) if d==&format!("native range image: native logical history is incomplete at index {index}"))
        );
        // Full postread may discover this AFTER bridge WAL. Never require or
        // infer no native effect merely because the actual API returned Err.
        assert!(
            after["storage"]["native_next_xid"].as_u64().unwrap()
                >= fault["after_fault_storage"]["native_next_xid"]
                    .as_u64()
                    .unwrap()
        );
    }
    assert_eq!(phase_canceled, false);
    assert!(phase_remaining.is_some_and(|n| n > 0));
    assert_eq!(own_at_return["cancel_requested"], false);
    assert!(own_at_return["remaining_us"]
        .as_u64()
        .is_some_and(|n| n > 0));
    assert_eq!(before["gate"]["frozen"], after["gate"]["frozen"]);
    assert!(!after["gate"]["frozen"].is_null());
    assert_eq!(after["gate"]["frozen_owner_dropped"], false);
    assert_eq!(after["gate"]["poisoned"], false);
    assert!(after["gate"]["capture_pin"].is_null());
    assert_eq!(after["gate"]["controlled"], 0);
    process.check().unwrap();
}
