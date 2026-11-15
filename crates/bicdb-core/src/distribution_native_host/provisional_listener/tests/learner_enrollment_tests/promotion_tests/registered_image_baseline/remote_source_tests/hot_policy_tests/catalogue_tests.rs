//! Genuine fifth Registered metadata catalogue publication cuts. These stop
//! before later serving phases. They are not physical crash qualification.
use super::*;
use crate::distribution_range_consensus::{test_registered_decision_catalogue_cut, CatalogueCut};
use crate::distribution_range_handoff::NativeJointDecisionPublicationRound;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn is_catalogue_control(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetHotDecisionCatalogueBeforeMemory
            | SourceCase::TargetHotDecisionCatalogueRenameFailed
            | SourceCase::TargetHotDecisionCatalogueReadbackFailed
            | SourceCase::TargetHotDecisionCataloguePostCanceled
            | SourceCase::TargetHotDecisionCataloguePostTerm
    )
}
#[test]
fn native_registered_fifth_remote_hot_decision_catalogue_pre_memory_cancel_same_attempt_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionCatalogueBeforeMemory),
    );
}
#[test]
fn native_registered_fifth_remote_hot_decision_catalogue_actual_rename_error_same_attempt_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionCatalogueRenameFailed),
    );
}
#[test]
fn native_registered_fifth_remote_hot_decision_catalogue_actual_readback_error_same_attempt_retry()
{
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionCatalogueReadbackFailed),
    );
}
#[test]
fn native_registered_fifth_remote_hot_decision_catalogue_post_fsync_cancel_same_attempt_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionCataloguePostCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_hot_decision_catalogue_post_fsync_actual_term_refuses_vote() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionCataloguePostTerm),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    decision: &mut NativeJointDecisionPublicationRound,
    role: RecoveryRoleAdmission,
    mq: crate::distribution_consensus::HostMetadataReadObservation,
    business: Uuid,
    request: &CancellationToken,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    plan: &NativeJointHandoffPlan,
    client: &Arc<TcpClusterRelocationTransport>,
    metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    original: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
) {
    let before = state(&factory.database, topology, &node(4), ranges).unwrap();
    let fired = Arc::new(AtomicBool::new(false));
    let path = factory
        .database
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let aside = path.with_extension("registered-decision-readback-cut");
    let term_trace;
    let mut fault = None;
    let callback: Box<dyn FnOnce()>;
    let cut;
    if matches!(case, SourceCase::TargetHotDecisionCataloguePostTerm) {
        let (mutate, trace) = local_term_tests::mutation(metadata.clone(), request.clone(), output);
        term_trace = Some(trace);
        let observed = fired.clone();
        callback = Box::new(move || {
            observed.store(true, Ordering::SeqCst);
            mutate();
        });
        cut = CatalogueCut::AfterFsync;
    } else {
        term_trace = None;
        let observed = fired.clone();
        let token = request.clone();
        let original_path = path.clone();
        let held_path = aside.clone();
        callback = Box::new(move || {
            observed.store(true, Ordering::SeqCst);
            match case {
                SourceCase::TargetHotDecisionCatalogueBeforeMemory
                | SourceCase::TargetHotDecisionCataloguePostCanceled => token.cancel(),
                SourceCase::TargetHotDecisionCatalogueReadbackFailed => {
                    std::fs::rename(original_path, held_path).unwrap()
                }
                SourceCase::TargetHotDecisionCatalogueRenameFailed => {}
                _ => unreachable!(),
            }
        });
        cut = if matches!(
            case,
            SourceCase::TargetHotDecisionCatalogueBeforeMemory
                | SourceCase::TargetHotDecisionCatalogueRenameFailed
        ) {
            CatalogueCut::BeforeMemory
        } else {
            CatalogueCut::AfterFsync
        };
        if matches!(case, SourceCase::TargetHotDecisionCatalogueRenameFailed) {
            fault = Some(bicdb_page::test_faults::arm_io_failure(
                "registered_joint_decision.catalogue.rename",
                path.parent().unwrap(),
                1,
                false,
            ));
        }
    }
    let guard = test_registered_decision_catalogue_cut(cut, callback);
    let result = operation(
        output,
        ledger,
        "actual_registered_decision_catalogue_cut",
        || decision.publish_actual_owned_member(role, mq, business, request),
    );
    drop(guard);
    let fault_count = fault.as_ref().map(|f| f.fired());
    drop(fault);
    // Preserve missing actual file at failed open, then restore the SAME bytes
    // solely to permit subsequent forensic snapshots. No engine is reopened.
    let missing_after_call = !path.exists();
    if matches!(case, SourceCase::TargetHotDecisionCatalogueReadbackFailed) && aside.exists() {
        std::fs::rename(&aside, &path).unwrap();
    }
    let native_before_retry = original.test_decision_attempt_evidence();
    let catalogue_before_retry = original.test_decision_catalogue_evidence();
    let after_error = state(&factory.database, topology, &node(4), ranges);
    let counted_after_error = decision.test_actual_durable_member_ids();
    let mutation = term_trace.as_ref().map(|t| t.lock().clone());
    evidence(&output.join("registered-hot-decision-catalogue-cut.json"),&serde_json::json!({
        "case":format!("{case:?}"),"cut_fired":fired.load(Ordering::SeqCst),"fault_count":fault_count,
        "exact_error":result.as_ref().err().map(ToString::to_string),"api_returned_success":result.is_ok(),
        "actual_path_missing_after_call":missing_after_call,"native_before_retry":native_before_retry.as_ref().ok(),
        "catalogue_before_retry":catalogue_before_retry.as_ref().ok(),"after_error":after_error.as_ref().ok(),
        "evidence_errors":[native_before_retry.as_ref().err().map(ToString::to_string),
            catalogue_before_retry.as_ref().err().map(ToString::to_string),after_error.as_ref().err().map(ToString::to_string)],
        "mutation":mutation,"counted_after_error":counted_after_error,
        "source_app_ack_ledger":acknowledged,"before":before,"new_sdk_operations":[],
        "physical_power_loss_tested":false})).unwrap();
    assert!(
        fired.load(Ordering::SeqCst),
        "intended catalogue cut actually reached"
    );
    let error = result.expect_err("catalogue cut must preserve original error");
    assert!(!counted_after_error.contains(&node(4)));
    let native = native_before_retry.unwrap();
    let attempts = native["native_attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["outcome"]["state"], "actual_commit_returned");
    let catalogue = catalogue_before_retry.unwrap().unwrap();
    assert_eq!(catalogue["verified"], false);
    let writes = catalogue["attempts"].as_array().unwrap();
    if matches!(case, SourceCase::TargetHotDecisionCatalogueBeforeMemory) {
        assert!(writes.is_empty());
    } else {
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0]["actual_replace_returned_ok"],
            !matches!(case, SourceCase::TargetHotDecisionCatalogueRenameFailed)
        );
    }
    if matches!(case, SourceCase::TargetHotDecisionCatalogueRenameFailed) {
        assert_eq!(fault_count, Some(1));
    }
    if matches!(case, SourceCase::TargetHotDecisionCatalogueReadbackFailed) {
        assert!(missing_after_call);
    }
    let after_error = after_error.unwrap();
    assert_unchanged(&before, &after_error, ranges[1].id);
    if matches!(case, SourceCase::TargetHotDecisionCataloguePostTerm) {
        assert!(error
            .to_string()
            .contains("actual metadata authority version changed/dirty"));
        let mutation = mutation.unwrap();
        assert_eq!(mutation["mutation_proven"], true);
        assert_eq!(mutation["original_parent_live_after_mutation"], true);
        // This narrow control proves refusal/retention. A further fresh MQ
        // after a real term change is a separate liveness acceptance gate.
        return;
    }
    let request = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &request);
    let (mq, business) = own(client, &request);
    let retry = operation(
        output,
        ledger,
        "registered_catalogue_fresh_same_native_attempt_retry",
        || decision.publish_actual_owned_member(role, mq, business, &request),
    );
    let native_after = original.test_decision_attempt_evidence();
    let catalogue_after = original.test_decision_catalogue_evidence();
    let after = state(&factory.database, topology, &node(4), ranges);
    evidence(&output.join("registered-hot-decision-catalogue-retry.json"),&serde_json::json!({
        "case":format!("{case:?}"),"retry_returned_success":retry.is_ok(),
        "retry_error":retry.as_ref().err().map(ToString::to_string),"native_after":native_after.as_ref().ok(),
        "catalogue_after":catalogue_after.as_ref().ok(),"after":after.as_ref().ok(),
        "counted_after":decision.test_actual_durable_member_ids(),"same_native_before":native,
        "same_catalogue_before":catalogue,"source_app_ack_ledger":acknowledged,"no_serving_tested":true})).unwrap();
    retry.unwrap();
    assert_eq!(
        native_after.unwrap(),
        native,
        "same exact native xid/bytes/outcome"
    );
    let publication = catalogue_after.unwrap().unwrap();
    assert_eq!(
        publication["candidate_sha256"],
        catalogue["candidate_sha256"]
    );
    assert_eq!(publication["verified"], true);
    assert_eq!(
        publication["attempts"].as_array().unwrap().len(),
        writes.len() + 1
    );
    assert!(decision.test_actual_durable_member_ids().contains(&node(4)));
    assert_unchanged(&before, &after.unwrap(), ranges[1].id);
}
fn assert_unchanged(before: &serde_json::Value, after: &serde_json::Value, other: crate::RangeId) {
    for key in [
        "open",
        "birth",
        "namespace_ids",
        "protected_catalogue_uuid",
        "points",
        "rows",
    ] {
        assert_eq!(
            before[key], after[key],
            "physical data/original identity {key}"
        );
    }
    assert_eq!(after["writer_installed"], false);
    let previous: Vec<u8> = serde_json::from_value(before["raw"].clone()).unwrap();
    let current: Vec<u8> = serde_json::from_value(after["raw"].clone()).unwrap();
    let previous: serde_json::Value = serde_json::from_slice(&previous).unwrap();
    let current: serde_json::Value = serde_json::from_slice(&current).unwrap();
    let other = other.get().to_string();
    for map in [
        "ranges",
        "native_image_bases",
        "native_joint_handoffs",
        "image_install_intents",
    ] {
        assert_eq!(
            previous["catalog"][map][&other], current["catalog"][map][&other],
            "untouched other range {map}"
        );
    }
}
