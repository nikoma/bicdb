//! Genuine A lease/native proposal only. No catalogue Installing or row work.
use super::*;
#[test]
fn native_registered_fifth_remote_proposal_actual_wal_current_archive_preserve_data() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetProposal));
}
#[test]
fn native_registered_fifth_remote_proposal_actual_post_wal_cancel_fresh_retry_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetProposalPostCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_proposal_actual_pre_wal_caller_revocation_preserves_data() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetProposalBeforeCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_proposal_actual_pre_wal_hook_aba_preserves_data() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetProposalBeforeHookRevoked),
    );
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: Arc<NativeRemoteImageFact>,
    actual_frame: &crate::distribution_range_image::NativeRemoteImageStep<'_>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
) {
    let range = plan.old_range().id;
    let transfer = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &transfer)
        .into_handoff()
        .unwrap();
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &transfer)
        .unwrap();
    control
        .prepare_registered_remote_install_attempt(
            source.clone(),
            actual_frame,
            role,
            own,
            business,
            &transfer,
        )
        .unwrap();
    transfer.cancel();
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let native_before = factory
        .database
        .read()
        .test_registered_remote_native_inventory()
        .unwrap();
    let readers_before =
        crate::format::required_reader_features(factory.database.read().data_path()).unwrap();
    assert_eq!(native_before.len(), 4, "real original Registered inventory");
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent)
        .into_handoff()
        .unwrap();
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let (admission, _) = control.test_registered_receiver_parts().unwrap();
    let original_registration = admission.test_keep_original_registration().unwrap();
    let cut = if matches!(
        case,
        SourceCase::TargetProposalPostCanceled | SourceCase::TargetProposalBeforeCanceled
    ) {
        let caller = parent.clone();
        Some(crate::db::test_registered_remote_proposal_cut(
            matches!(case, SourceCase::TargetProposalPostCanceled),
            Box::new(move || caller.cancel()),
        ))
    } else if matches!(case, SourceCase::TargetProposalBeforeHookRevoked) {
        let db = factory.database.clone();
        Some(crate::db::test_registered_remote_proposal_cut(
            false,
            Box::new(move || db.read().clear_commit_admission()),
        ))
    } else {
        None
    };
    let result = control.publish_registered_remote_proposal(
        &source,
        actual_frame,
        role,
        own,
        business,
        &parent,
    );
    drop(cut);
    let exact_error = result.as_ref().err().map(ToString::to_string);
    let first_outcome = control.test_registered_remote_attempt(range).unwrap();
    let native_after_first = factory
        .database
        .read()
        .test_registered_remote_native_inventory()
        .unwrap();
    let record_after_first = factory
        .database
        .read()
        .test_registered_remote_proposal_record(source.manifest().target_attempt)
        .unwrap();
    let mut final_pointer = None;
    if matches!(case, SourceCase::TargetProposal) {
        final_pointer = Some(result.unwrap());
    } else {
        let error = result.unwrap_err();
        if matches!(case, SourceCase::TargetProposalBeforeHookRevoked) {
            assert!(
                error
                    .to_string()
                    .contains("original registered receiver hook changed"),
                "{error}"
            );
            parent.check().unwrap();
        } else {
            assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        }
    }
    if matches!(
        case,
        SourceCase::TargetProposalBeforeCanceled | SourceCase::TargetProposalBeforeHookRevoked
    ) {
        assert!(record_after_first.is_none());
        assert_eq!(native_after_first, native_before);
        assert_eq!(
            first_outcome["proposal"]["outcome"],
            "prepared_no_native_xid"
        );
        assert_eq!(
            first_outcome["proposal"]["attempted_pointer"],
            serde_json::Value::Null
        );
    } else {
        let first = record_after_first
            .as_ref()
            .expect("native WAL current must exist even if reply canceled");
        assert_eq!(native_after_first.len(), native_before.len() + 2);
        assert_eq!(
            first["source"]["checksum_sha256"],
            source.manifest().checksum_sha256
        );
        assert_eq!(
            first["source"]["range_index"],
            source.manifest().range_index
        );
        assert_eq!(first["source"]["h0"], source.manifest().h0);
        assert_eq!(first["source"]["h"], source.manifest().h);
        assert_eq!(
            first["preimage"]["applied"],
            serde_json::Value::Null,
            "absence never means R0"
        );
        if matches!(case, SourceCase::TargetProposalPostCanceled) {
            assert_eq!(
                first_outcome["proposal"]["outcome"],
                "native_commit_returned_before_postchecks"
            );
            let retry = budget(5000);
            let business = Uuid::now_v7();
            let role = actual_role(provider, actor, plan, &node(4), &retry)
                .into_handoff()
                .unwrap();
            let own = client
                .acquire_host_metadata_observation(
                    &node(0),
                    business,
                    Duration::from_secs(5),
                    &retry,
                )
                .unwrap();
            let mut frames = Vec::new();
            let fresh = source
                .bind_step(
                    super::frame(client, source.manifest(), Op::Finish, output, &mut frames)
                        .unwrap(),
                )
                .unwrap();
            let pointer = control
                .publish_registered_remote_proposal(&source, &fresh, role, own, business, &retry)
                .unwrap();
            assert_eq!(
                serde_json::to_value(&pointer).unwrap(),
                first_outcome["proposal"]["attempted_pointer"]
            );
            final_pointer = Some(pointer);
            assert_eq!(
                factory
                    .database
                    .read()
                    .test_registered_remote_proposal_record(source.manifest().target_attempt)
                    .unwrap(),
                record_after_first,
                "fresh same-attempt reconciliation must retain original exact xid/bytes"
            );
            assert_eq!(
                factory
                    .database
                    .read()
                    .test_registered_remote_native_inventory()
                    .unwrap(),
                native_after_first
            );
        }
        let pointer = final_pointer.as_ref().unwrap();
        let mut original_native = native_after_first.clone();
        assert!(original_native
            .remove(&format!(
                "registered-remote-image-proposal/{}/current",
                pointer.attempt_id
            ))
            .is_some());
        assert!(original_native
            .remove(&format!(
                "registered-remote-image-proposal/{}/archive/{:020}",
                pointer.attempt_id, pointer.native_page_xid
            ))
            .is_some());
        assert_eq!(
            original_native, native_before,
            "only exact owned proposal CURRENT/archive may change"
        );
    }
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(
        crate::format::required_reader_features(factory.database.read().data_path()).unwrap(),
        readers_before,
        "sticky local Registered proposal semantics cannot alter portable schema readers"
    );
    assert_eq!(
        after, before,
        "proposal must preserve user rows, raw catalogue, UID, native range points and other range"
    );
    std::hint::black_box(&original_registration);
    evidence(&output.join(format!("registered-proposal-{}.json",range.get())),&serde_json::json!({
        "exact_error":exact_error,"first_outcome":first_outcome,"record_after_first":record_after_first,
        "native_before":native_before,"native_after_first":native_after_first,"pointer":final_pointer,
        "before":before,"after":after,"actual_A_lease":true,"no_catalogue_installing":true,"no_rows_or_serving":true})).unwrap();
}
