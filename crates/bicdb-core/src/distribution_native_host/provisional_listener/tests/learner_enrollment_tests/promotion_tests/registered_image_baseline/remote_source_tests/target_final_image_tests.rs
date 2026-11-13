//! Genuine fifth, independent target-primary scan and protected final receipt.
//! No adopted floor, NEW, Ready, Serving, cold or physical qualification.
use super::*;
use crate::db::{
    NativeRegisteredRemoteImageOperation as Operation,
    NativeRegisteredRemoteImageProgress as Progress,
};
#[test]
fn native_registered_fifth_remote_final_image_actual_independent_scan_and_native_receipt() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetFinalImage));
}
#[test]
fn native_registered_fifth_remote_final_image_actual_refuses_without_independent_target_scan() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetFinalImageWithoutScan),
    );
}
#[test]
fn native_registered_fifth_remote_final_image_actual_missing_physical_row_is_not_hidden_by_import_hash(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetFinalImageCorrupt),
    );
}
#[test]
fn native_registered_fifth_remote_final_image_actual_post_wal_cancel_reconciles_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetFinalImageCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_final_image_actual_historical_integrity_uses_immutable_header_archive(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetFinalImageHistorical),
    );
}
#[allow(clippy::too_many_arguments)]
fn advance(
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
    operation: Operation,
    cancel: bool,
) -> Result<Progress> {
    let parent = budget(5000);
    let role = actual_role(provider, actor, plan, &node(4), &parent).into_handoff()?;
    let business = Uuid::now_v7();
    let own = client.acquire_host_metadata_observation(
        &node(0),
        business,
        Duration::from_secs(5),
        &parent,
    )?;
    let frame = source.bind_step(super::frame(
        client,
        source.manifest(),
        Op::Finish,
        output,
        journal,
    )?)?;
    let guard = cancel.then(|| {
        let token = parent.clone();
        crate::db::test_registered_remote_final_image_after_wal(Box::new(move || token.cancel()))
    });
    let result = control
        .advance_registered_remote_image(source, &frame, role, own, business, &parent, operation);
    drop(guard);
    parent.cancel();
    result
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &serde_json::Value,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) {
    // Actual same receiver/attempt, completed and separately audited history,
    // row WAL and imported ACK verification. Preserve the original journal.
    target_image_tests::exercise(
        SourceCase::TargetImageRows,
        control,
        client,
        source.clone(),
        plan,
        provider,
        actor,
        factory,
        pending,
        ranges,
        acknowledged,
        output,
        journal,
    );
    let range = plan.old_range().id;
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let owner_before = control.test_registered_remote_image_state(range).unwrap();
    let original_b = factory.database.read().current_commit_seq();
    if matches!(case, SourceCase::TargetFinalImageWithoutScan) {
        let error = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::PublishFinalImage,
            false,
        )
        .unwrap_err();
        evidence(&output.join("registered-final-image-missing-scan.json"),&serde_json::json!({"exact_error":error.to_string(),"before":before,
            "owner_before":owner_before,"owner_after":control.test_registered_remote_image_state(range).unwrap(),"no_native_floor_or_serving":true})).unwrap();
        assert!(
            error
                .to_string()
                .contains("requires independent target scan"),
            "{error}"
        );
        assert_eq!(
            control.test_registered_remote_image_state(range).unwrap(),
            owner_before
        );
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            before
        );
        return;
    }
    if matches!(case, SourceCase::TargetFinalImageCorrupt) {
        factory
            .database
            .read()
            .test_registered_remote_remove_physical_row(
                crate::CLUSTER_EVENTS_COLLECTION,
                acknowledged["record_id"].as_str().unwrap(),
            )
            .unwrap();
    }
    advance(
        control,
        client,
        &source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::BeginVerification,
        false,
    )
    .unwrap();
    let mut progress = Vec::new();
    let mut completed = false;
    for _ in 0..4096 {
        let result = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::VerifyTarget {
                max_scanned_records: 1,
            },
            false,
        );
        if matches!(case, SourceCase::TargetFinalImageCorrupt) {
            if let Err(error) = result {
                let owner = control.test_registered_remote_image_state(range).unwrap();
                evidence(&output.join("registered-final-image-independent-corruption.json"),&serde_json::json!({"exact_error":error.to_string(),"owner":owner,
                    "removed_actual_record_id":acknowledged["record_id"],"progress":progress,"actual_page_wal_corruption_control":true,"no_floor_or_serving":true})).unwrap();
                assert!(
                    error
                        .to_string()
                        .contains("independent actual target scan differs"),
                    "{error}"
                );
                assert_eq!(owner["verification"]["invalid"], true);
                assert!(owner["final_image"].is_null());
                assert!(control
                    .registered_remote_final_image_receipt(range)
                    .is_err());
                return;
            }
        }
        let Progress::Verification(actual) = result.unwrap() else {
            panic!("actual independent scan progress expected")
        };
        completed = serde_json::to_value(&actual).unwrap()["status"] == "Complete";
        progress.push(serde_json::to_value(actual).unwrap());
        if completed {
            break;
        }
    }
    assert!(
        !matches!(case, SourceCase::TargetFinalImageCorrupt),
        "missing actual row must fail independent scan"
    );
    assert!(completed, "bounded actual target verification must finish");
    let mut canceled = None;
    if matches!(case, SourceCase::TargetFinalImageCanceled) {
        let error = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::PublishFinalImage,
            true,
        )
        .unwrap_err();
        let retained = control.test_registered_remote_image_state(range).unwrap();
        evidence(
            &output.join("registered-final-image-after-wal-cancel.json"),
            &serde_json::json!({"exact_error":error.to_string(),"retained":retained}),
        )
        .unwrap();
        assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        assert_eq!(
            retained["final_image"]["state"],
            "native_commit_returned_before_postchecks"
        );
        assert!(
            retained["final_image"]["actual_commit_returned"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(control
            .registered_remote_final_image_receipt(range)
            .is_err());
        canceled = Some(retained);
    }
    let Progress::FinalImage(pointer) = advance(
        control,
        client,
        &source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::PublishFinalImage,
        false,
    )
    .unwrap() else {
        panic!("actual protected final image expected")
    };
    let repeated = advance(
        control,
        client,
        &source,
        plan,
        provider,
        actor,
        output,
        journal,
        Operation::PublishFinalImage,
        false,
    )
    .unwrap();
    let Progress::FinalImage(repeated) = repeated else {
        panic!("same final pointer expected")
    };
    assert_eq!(pointer, repeated);
    let receipt = control
        .registered_remote_final_image_receipt(range)
        .unwrap();
    assert_eq!(receipt.pointer(), pointer);
    assert_eq!(
        receipt.manifest().checksum_sha256,
        source.manifest().checksum_sha256
    );
    assert_eq!(receipt.target_reserved_b(), original_b);
    let owner_after = control.test_registered_remote_image_state(range).unwrap();
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    evidence(&output.join("registered-final-image-before-assertions.json"),&serde_json::json!({"before":before,"after":after,"owner_before":owner_before,"owner_after":owner_after,
        "pointer":pointer,"canceled":canceled,"progress":progress,"original_target_b":original_b,"independent_actual_primary_scan":true,"independent_index_parity":false,
        "no_native_floor_catalogue_adoption_ready_or_serving":true,"cold_or_physical_qualification":false})).unwrap();
    assert_eq!(after,before,"only protected final native records may change; rows/index/schema/raw floors/other range remain unchanged");
    assert_eq!(owner_after["final_image"]["state"], "verified");
    assert_eq!(owner_after["history"], owner_before["history"]);
    assert_eq!(owner_after["rows"], owner_before["rows"]);
    assert_eq!(factory.database.read().current_commit_seq(), original_b);
    {
        let mf = source.manifest();
        let db = factory.database.read();
        let native = db
            .native_recovery_admission(
                &mf.cluster_id,
                mf.bootstrap_id,
                &mf.target_id,
                mf.target_birth.node_incarnation(),
            )
            .unwrap();
        let historical = db
            .read_registered_final_image_integrity(&native, &pointer)
            .unwrap();
        assert_eq!(historical.pointer(), pointer);
        assert_eq!(historical.range_index(), mf.range_index);
        assert_eq!(historical.logical_genesis_sha256(), mf.h0);
        assert_eq!(historical.logical_history_sha256(), mf.h);
        historical.require_current(&db, &native).unwrap();
        let mut wrong = pointer.clone();
        wrong.checksum_sha256.replace_range(
            0..1,
            if pointer.checksum_sha256.starts_with('0') {
                "1"
            } else {
                "0"
            },
        );
        assert!(db
            .read_registered_final_image_integrity(&native, &wrong)
            .is_err());
    }
    if let Some(canceled) = canceled {
        assert_eq!(
            canceled["final_image"]["record"],
            owner_after["final_image"]["record"]
        );
        assert_eq!(
            canceled["final_image"]["actual_commit_returned"],
            owner_after["final_image"]["actual_commit_returned"]
        );
    }
    if matches!(case, SourceCase::TargetFinalImageHistorical) {
        factory
            .database
            .read()
            .test_registered_remote_remove_mutable_headers(pointer.attempt_id)
            .unwrap();
        let mf = source.manifest();
        let historical_pointer = {
            let db = factory.database.read();
            let native = db
                .native_recovery_admission(
                    &mf.cluster_id,
                    mf.bootstrap_id,
                    &mf.target_id,
                    mf.target_birth.node_incarnation(),
                )
                .unwrap();
            let historical = db
                .read_registered_final_image_integrity(&native, &pointer)
                .unwrap();
            historical.require_current(&db, &native).unwrap();
            assert_eq!(historical.range_index(), mf.range_index);
            assert_eq!(
                serde_json::to_value(historical.target_namespace_ids()).unwrap(),
                owner_after["final_image"]["record"]["namespaces"]
            );
            historical.pointer()
        };
        let error = advance(
            control,
            client,
            &source,
            plan,
            provider,
            actor,
            output,
            journal,
            Operation::PublishFinalImage,
            false,
        )
        .unwrap_err();
        evidence(&output.join("registered-final-image-historical-integrity.json"),&serde_json::json!({"pointer":pointer,"historical_pointer":historical_pointer,
            "current_resume_error":error.to_string(),"actual_mutable_header_removal":true,"immutable_full_header_archive_retained":true,
            "no_original_A_or_cold_resume_reconstruction":true,"no_native_floor_or_serving":true})).unwrap();
        assert_eq!(historical_pointer, pointer);
        assert!(
            error.to_string().contains("history header absent"),
            "{error}"
        );
    }
}
