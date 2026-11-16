//! Genuine actual first-policy entry lifetime/metadata actors. These are
//! negative component controls, not a new deadline or a successful move.
use super::*;
use crate::distribution_range_handoff::NativeJointServingPolicyAdmission;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetHotEntryCanceled
            | SourceCase::TargetHotEntryExpired
            | SourceCase::TargetHotEntryMetadataChanged
    )
}
#[test]
fn native_registered_first_policy_actual_entry_caller_cancel_refuses_before_native() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotEntryCanceled),
    );
}
#[test]
fn native_registered_first_policy_actual_entry_earliest_window_expiry_refuses_before_native() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotEntryExpired),
    );
}
#[test]
fn native_registered_first_policy_actual_entry_metadata_term_change_refuses_before_native() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotEntryMetadataChanged),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    range: crate::RangeId,
    admission: Arc<NativeJointServingPolicyAdmission>,
    metadata: Arc<Mutex<crate::MetadataConsensusStore>>,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    topology: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
) {
    let request = budget(5000);
    let before = local_term_tests::snapshot(control, factory, topology, ranges);
    let observed = Arc::new(Mutex::new(serde_json::Value::Null));
    let facts = observed.clone();
    let caller = request.clone();
    let (mutation, mutation_facts) = local_term_tests::mutation(metadata, request.clone(), output);
    let cut = NativeMetadataControlHost::test_registered_policy_entry_cut(
        matches!(case, SourceCase::TargetHotEntryMetadataChanged),
        Box::new(move |actual| {
            let caller_live = caller.check().is_ok();
            let admission_live = actual.check().is_ok();
            *facts.lock() = serde_json::json!({"cut_reached":true,
            "caller_live_at_cut":caller_live,"admission_live_at_cut":admission_live,
            "caller_deadline":caller.deadline().map(|d|format!("{d:?}")),
            "actual_earliest_deadline":actual.deadline().map(|d|format!("{d:?}")),
            "original_windows_preserved":true});
            if caller_live && admission_live {
                match case {
                    SourceCase::TargetHotEntryCanceled => caller.cancel(),
                    SourceCase::TargetHotEntryExpired => {
                        while actual.check().is_ok() {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                    SourceCase::TargetHotEntryMetadataChanged => mutation(),
                    _ => unreachable!(),
                }
            }
        }),
    );
    let started = Instant::now();
    let result = operation(output, ledger, "actual_registered_entry_negative", || {
        control.install_registered_serving_data_policy(range, admission, &request)
    });
    drop(cut);
    let facts = observed.lock().clone();
    let mutation = mutation_facts.lock().clone();
    let after = local_term_tests::snapshot(control, factory, topology, ranges);
    let report = serde_json::json!({"case":format!("{case:?}"),"entry":facts,
        "original_result_error":result.as_ref().err().map(ToString::to_string),
        "elapsed_us":started.elapsed().as_micros(),"original_parent_ms":5000,
        "original_parent_live_after_return":request.check().is_ok(),
        "original_parent_cancelled":request.is_cancel_requested(),
        "mutation":mutation,"before":before,"after":after,
        "scope":"actual original Registered entry negative, no physical fault or successful-move claim"});
    eprintln!("NATIVE_REGISTERED_ENTRY_CONTROL {report}");
    evidence(&output.join("registered-entry-negative.json"), &report).unwrap();
    assert_eq!(facts["cut_reached"], true);
    assert_eq!(facts["caller_live_at_cut"], true);
    assert_eq!(facts["admission_live_at_cut"], true);
    match case {
        SourceCase::TargetHotEntryCanceled => assert!(
            matches!(&result, Err(BicDbError::QueryCanceled)),
            "{result:?}"
        ),
        SourceCase::TargetHotEntryExpired => {
            assert!(!request.is_cancel_requested());
            assert!(
                matches!(&result, Err(BicDbError::QueryTimedOut)),
                "{result:?}"
            );
        }
        SourceCase::TargetHotEntryMetadataChanged => {
            assert_eq!(mutation["mutation_proven"], true);
            assert_eq!(mutation["actor_joined"], true);
            let error = result
                .as_ref()
                .expect_err("actual changed metadata must refuse original policy");
            assert!(
                error
                    .to_string()
                    .contains("actual metadata authority version changed/dirty"),
                "{error:?}"
            );
        }
        _ => unreachable!(),
    }
    assert!(before["database_error"].is_null(), "{before}");
    assert!(after["database_error"].is_null(), "{after}");
    assert_eq!(
        before["database"], after["database"],
        "same actual rows/commit sequence/native prefix/WAL bytes"
    );
    assert!(before["hook_evidence_error"].is_null(), "{before}");
    assert!(after["hook_evidence_error"].is_null(), "{after}");
    assert_eq!(
        before["actual_hook"], after["actual_hook"],
        "no native writer publication"
    );
    assert!(
        control.test_registered_first_policy_evidence().is_err(),
        "no native attempt before fresh factory admission"
    );
}
