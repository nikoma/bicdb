//! Real protected preparation rejected at an uncounted candidate boundary.
//! No DTO/fake service/longer OWN is used to make a candidate count.
use super::*;
use crate::distribution_data::ActualServiceNativeJointPreparedEvidence;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetPreparedCandidateForeignService
            | SourceCase::TargetPreparedCandidateExpiredOwn
            | SourceCase::TargetPreparedCandidateMissingArchive
            | SourceCase::TargetPreparedCandidateCanceledCaller
    )
}
macro_rules! case {
    ($name:ident,$case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
case!(
    native_registered_fifth_prepared_candidate_actual_foreign_service_refuses_count,
    TargetPreparedCandidateForeignService
);
case!(
    native_registered_fifth_prepared_candidate_actual_expired_own_refuses_count,
    TargetPreparedCandidateExpiredOwn
);
case!(
    native_registered_fifth_prepared_candidate_actual_missing_archive_refuses_count,
    TargetPreparedCandidateMissingArchive
);
case!(
    native_registered_fifth_prepared_candidate_actual_caller_cancel_refuses_count,
    TargetPreparedCandidateCanceledCaller
);

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    actual: ActualServiceNativeJointPreparedEvidence,
    service: Arc<Mutex<ClusterDataNodeService>>,
    services: &[Arc<Mutex<ClusterDataNodeService>>],
    request: &CancellationToken,
    process: &CancellationToken,
    output: &std::path::Path,
    acknowledged: &[serde_json::Value],
) {
    request.check().unwrap();
    process.check().unwrap();
    let original_deadline = actual.committed().deadline();
    let original_node = actual.prepared().local_node_id().clone();
    let original_checksum = actual.prepared().checksum_sha256().to_string();
    let count_parent = request.child();
    let mut damage = serde_json::Value::Null;
    assert!(
        Instant::now() < original_deadline,
        "fault must arm while real original OWN is live"
    );
    let chosen = if matches!(case, SourceCase::TargetPreparedCandidateForeignService) {
        services
            .iter()
            .find(|other| !Arc::ptr_eq(other, &service))
            .unwrap()
            .clone()
    } else {
        service.clone()
    };
    match case {
        SourceCase::TargetPreparedCandidateExpiredOwn => {
            while Instant::now() <= original_deadline {
                request.check().unwrap();
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        SourceCase::TargetPreparedCandidateMissingArchive => {
            let database = service.lock().database();
            damage = database
                .read()
                .test_remove_joint_preparation_archive(actual.prepared(), request)
                .unwrap();
        }
        SourceCase::TargetPreparedCandidateCanceledCaller => count_parent.cancel(),
        SourceCase::TargetPreparedCandidateForeignService => {}
        _ => unreachable!(),
    }
    let own_live_before_count = Instant::now() < original_deadline;
    let candidate =
        NativeJointPreparedServingCandidate::from_actual_service(actual, chosen.clone());
    let result = NativeJointPreparedServingRound::begin_candidate(candidate, &count_parent);
    let after = Instant::now();
    let error = result.as_ref().err().map(ToString::to_string);
    let chosen_node = chosen.lock().node_id().clone();
    evidence(
        &output.join("prepared-candidate-count-negative.json"),
        &serde_json::json!({
            "case":format!("{case:?}"),"original_node":original_node,
            "chosen_actual_service_node":chosen_node,
            "original_preparation_checksum":original_checksum,"damage":damage,
            "actual_error":error,"count_succeeded":result.is_ok(),
            "original_own_live_before_count":own_live_before_count,
            "original_own_live_after_count":after<original_deadline,
            "original_request_live":request.check().is_ok(),
            "actual_count_parent_canceled":count_parent.is_cancel_requested(),
            "acknowledged_full_payloads":acknowledged,"serving_granted":false,
        }),
    )
    .unwrap();
    let error = result
        .err()
        .expect("uncounted candidate must not become a quorum");
    request.check().unwrap();
    process.check().unwrap();
    match case {
        SourceCase::TargetPreparedCandidateExpiredOwn => {
            assert!(!own_live_before_count);
            assert!(matches!(error, crate::BicDbError::QueryTimedOut));
            assert!(!count_parent.is_cancel_requested());
        }
        SourceCase::TargetPreparedCandidateCanceledCaller => {
            assert!(count_parent.is_cancel_requested());
            assert!(matches!(error, crate::BicDbError::QueryCanceled));
            assert!(after < original_deadline);
        }
        SourceCase::TargetPreparedCandidateForeignService => {
            assert!(own_live_before_count && after < original_deadline);
            assert!(error
                .to_string()
                .contains("cluster service belongs to another actual database/open"));
            assert!(!count_parent.is_cancel_requested());
        }
        SourceCase::TargetPreparedCandidateMissingArchive => {
            assert!(own_live_before_count && after < original_deadline);
            assert!(error
                .to_string()
                .contains("NEW joint preparation immutable archive missing"));
            assert_eq!(damage["original_current"], damage["removed_archive"]);
            assert_eq!(damage["actual_archive_absent"], true);
            assert!(!count_parent.is_cancel_requested());
        }
        _ => unreachable!(),
    }
}
