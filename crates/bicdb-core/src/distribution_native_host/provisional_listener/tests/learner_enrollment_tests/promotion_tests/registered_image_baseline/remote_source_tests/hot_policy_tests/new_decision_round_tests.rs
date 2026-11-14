//! Real original NEW issuances must still refuse direct round consumption.
//! Copied deadline/identity DATA below never renews a role or an observation.
use super::*;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetNewDecisionOriginalIssuanceExpired
            | SourceCase::TargetNewDecisionOriginalCallerCanceled
    )
}

#[test]
fn native_registered_fifth_new_decision_direct_actual_original_role_and_own_expiry_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNewDecisionOriginalIssuanceExpired),
    );
}

#[test]
fn native_registered_fifth_new_decision_direct_actual_original_caller_cancel_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNewDecisionOriginalCallerCanceled),
    );
}

/// Scalar diagnostics from the actual original producer, copied before moving
/// its authority into the evidence. There is no role/OWN/permit in this type.
pub(super) struct OriginalIssuance {
    role_deadline: Instant,
    own_deadline: Instant,
    identity: serde_json::Value,
}
impl OriginalIssuance {
    pub(super) fn copy_from_actual(
        role: &crate::distribution_range_recovery::HandoffRoleAdmission,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
    ) -> Self {
        Self {
            role_deadline: role.role().deadline(),
            own_deadline: own.deadline(),
            identity: serde_json::json!({"role_context":role.role().context_id(),
                "role_recipient":role.role().local_node_id(),
                "role_term":role.role().metadata_term(),
                "role_read_index":role.role().metadata_read_index(),
                "own_context":own.context_id(),"own_business":business,
                "own_term":own.term(),"own_read_index":own.read_index(),
                "copied_data_only":true}),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    round: NativeJointNewRound,
    owner: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    issuance: OriginalIssuance,
    original_caller: &CancellationToken,
    process: &CancellationToken,
    output: &std::path::Path,
    acknowledged: &[serde_json::Value],
) {
    original_caller.check().unwrap();
    process.check().unwrap();
    let before_cut = Instant::now();
    assert!(before_cut < issuance.role_deadline);
    assert!(before_cut < issuance.own_deadline);
    let native_before = owner.test_decision_attempt_evidence().unwrap();
    let original_a = owner
        .retained_freeze()
        .unwrap()
        .try_as_frozen()
        .unwrap()
        .owner();
    let cut_fired = match case {
        SourceCase::TargetNewDecisionOriginalIssuanceExpired => {
            // Cross BOTH actual finite issuance deadlines while keeping their
            // original five-second caller and the process alive. No new RPC.
            let deadline = issuance.role_deadline.max(issuance.own_deadline);
            while Instant::now() <= deadline {
                original_caller.check().unwrap();
                process.check().unwrap();
                std::thread::sleep(Duration::from_millis(2));
            }
            true
        }
        SourceCase::TargetNewDecisionOriginalCallerCanceled => {
            original_caller.cancel();
            original_caller.is_cancel_requested()
        }
        _ => unreachable!(),
    };
    let consumption_entered = Instant::now();
    let caller_canceled_at_entry = original_caller.is_cancel_requested();
    let caller_live_at_entry = original_caller.check().is_ok();
    let result = round.prepare_decision();
    let returned = Instant::now();
    // Preserve the real primary Result and exact original host custody before
    // any assertion. These readouts cannot arm a cut or publish a decision.
    let native_after = owner.test_decision_attempt_evidence();
    let retained_a = owner
        .retained_freeze()
        .and_then(|frozen| Ok(frozen.try_as_frozen()?.owner()));
    let exact_error = result.as_ref().err().map(ToString::to_string);
    evidence(
        &output.join("new-decision-direct-original-issuance-refusal.json"),
        &serde_json::json!({"case":format!("{case:?}"),"actual_cut_fired":cut_fired,
            "identity":issuance.identity,"original_target_attempt":original_a,
            "retained_target_attempt":retained_a.as_ref().ok(),
            "retained_target_attempt_error":retained_a.as_ref().err().map(ToString::to_string),
            "role_expired_before_consumption":consumption_entered>=issuance.role_deadline,
            "own_expired_before_consumption":consumption_entered>=issuance.own_deadline,
            "role_past_deadline_at_entry_us":consumption_entered.saturating_duration_since(issuance.role_deadline).as_micros(),
            "own_past_deadline_at_entry_us":consumption_entered.saturating_duration_since(issuance.own_deadline).as_micros(),
            "caller_canceled_at_entry":caller_canceled_at_entry,
            "caller_live_at_entry":caller_live_at_entry,
            "process_live_after_return":process.check().is_ok(),
            "consumption_elapsed_us":returned.duration_since(consumption_entered).as_micros(),
            "exact_error":exact_error,"error_debug":result.as_ref().err().map(|e|format!("{e:?}")),
            "native_before":native_before,"native_after":native_after.as_ref().ok(),
            "native_after_error":native_after.as_ref().err().map(ToString::to_string),
            "acknowledged_before_control":acknowledged,
            "decision_publication_not_invoked":true,"serving_or_application_tested":false}),
    )
    .unwrap();
    assert!(cut_fired);
    assert_eq!(retained_a.unwrap(), original_a);
    assert_eq!(native_after.unwrap(), native_before);
    assert_eq!(native_before["not_started"], true);
    process.check().unwrap();
    match case {
        SourceCase::TargetNewDecisionOriginalIssuanceExpired => {
            assert!(!caller_canceled_at_entry);
            assert!(caller_live_at_entry);
            assert!(consumption_entered >= issuance.role_deadline);
            assert!(consumption_entered >= issuance.own_deadline);
            assert!(exact_error
                .unwrap()
                .contains("recovery-role witness scope changed or expired"));
        }
        SourceCase::TargetNewDecisionOriginalCallerCanceled => {
            assert!(caller_canceled_at_entry);
            assert!(matches!(result, Err(BicDbError::QueryCanceled)));
        }
        _ => unreachable!(),
    }
}
