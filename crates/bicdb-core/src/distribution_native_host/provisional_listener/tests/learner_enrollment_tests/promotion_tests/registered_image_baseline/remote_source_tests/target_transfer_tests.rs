//! Actual original fifth ownership transfer only; no native Installing/WAL.
use super::*;

#[test]
fn native_registered_fifth_remote_transfer_unique_runtime_owner_preserves_data() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetTransfer));
}
#[test]
fn native_registered_fifth_remote_transfer_actual_callback_arc_blocks_then_fresh_retry() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetTransferBusy));
}
#[test]
fn native_registered_fifth_remote_transfer_actual_cancellation_before_rotation_preserves_n() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetTransferCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_transfer_actual_hook_aba_before_rotation_preserves_n() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetTransferHookRevoked),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    runtime: &Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    _service: &Arc<Mutex<crate::ClusterDataNodeService>>,
    _metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    client: &TcpClusterRelocationTransport,
    source: Arc<NativeRemoteImageFact>,
    frame: &crate::distribution_range_image::NativeRemoteImageStep<'_>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
) {
    let range = plan.old_range().id;
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent)
        .into_handoff()
        .unwrap();
    let scope = RecoveryFenceScope::from_role(role.role()).unwrap();
    let callback = if matches!(case, SourceCase::TargetTransferBusy) {
        Some(runtime.source_capture_freeze(&scope).unwrap())
    } else {
        None
    };
    let (admission, _) = control.test_registered_receiver_parts().unwrap();
    let original_registration = admission.test_keep_original_registration().unwrap();
    let hook = if matches!(case, SourceCase::TargetTransferCanceled) {
        let original = parent.clone();
        Some(
            crate::distribution_range_recovery::test_before_registered_remote_rotation(Box::new(
                move || original.cancel(),
            )),
        )
    } else if matches!(case, SourceCase::TargetTransferHookRevoked) {
        let actual_db = factory.database.clone();
        Some(
            crate::distribution_range_recovery::test_before_registered_remote_rotation(Box::new(
                move || actual_db.read().clear_commit_admission(),
            )),
        )
    } else {
        None
    };
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let result = control.prepare_registered_remote_install_attempt(
        source.clone(),
        frame,
        role,
        own,
        business,
        &parent,
    );
    drop(hook);
    let mut failure = None;
    if matches!(case, SourceCase::TargetTransfer) {
        result.unwrap();
    } else {
        let error = result.unwrap_err();
        failure = Some(error.to_string());
        if matches!(case, SourceCase::TargetTransferBusy) {
            assert!(
                error
                    .to_string()
                    .contains("actual Frozen callbacks to drain"),
                "{error}"
            );
        } else if matches!(case, SourceCase::TargetTransferCanceled) {
            assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        } else {
            assert!(
                error
                    .to_string()
                    .contains("original registered receiver hook changed"),
                "{error}"
            );
        }
        let actual = runtime.source_capture_freeze(&scope).unwrap();
        assert_eq!(actual.owner(), scope.freeze_owner().unwrap());
        drop(actual);
        let retained = control.test_registered_remote_attempt(range).unwrap();
        assert_eq!(retained["lease_present"], false);
        assert_eq!(retained["actual_owner"], serde_json::Value::Null);
    }
    drop(callback);
    if matches!(case, SourceCase::TargetTransferBusy) {
        // A distinct original request, SAME retained MF Arc and attempted data.
        let retry = budget(5000);
        let role = actual_role(provider, actor, plan, &node(4), &retry)
            .into_handoff()
            .unwrap();
        let business = Uuid::now_v7();
        let own = client
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &retry)
            .unwrap();
        control
            .prepare_registered_remote_install_attempt(
                source.clone(),
                frame,
                role,
                own,
                business,
                &retry,
            )
            .unwrap();
    }
    let retained = control.test_registered_remote_attempt(range).unwrap();
    if matches!(
        case,
        SourceCase::TargetTransfer | SourceCase::TargetTransferBusy
    ) {
        assert_eq!(retained["lease_present"], true);
        assert_eq!(
            retained["actual_owner"],
            serde_json::to_value(source.manifest().target_attempt).unwrap()
        );
        assert_ne!(
            source.manifest().target_attempt,
            scope.freeze_owner().unwrap()
        );
        let error = runtime.source_capture_freeze(&scope).unwrap_err();
        assert!(error.to_string().contains("freeze is leased"), "{error}");
        parent.cancel();
        assert_eq!(
            control.test_registered_remote_attempt(range).unwrap(),
            retained,
            "ended 5-second caller must not release physical custody or become authority"
        );
    }
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(
        after, before,
        "actual ownership transfer must not write native/user/catalogue bytes"
    );
    std::hint::black_box(&original_registration);
    evidence(
        &output.join(format!("registered-transfer-{}.json", range.get())),
        &serde_json::json!({"actual_original_fifth":true,"same_actual_MF_arc":true,
            "failure":failure,"retained":retained,"before":before,"after":after,
            "ownership_only_no_installing_wal":true,"no_rows_or_serving":true}),
    )
    .unwrap();
}
