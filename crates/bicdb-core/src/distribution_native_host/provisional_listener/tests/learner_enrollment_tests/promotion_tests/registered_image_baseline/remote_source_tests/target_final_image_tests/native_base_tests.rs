//! Actual owned native-base publication, separately from catalogue adoption.
use super::*;

#[test]
fn native_registered_fifth_remote_native_base_actual_refuses_without_live_final_image() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseWithoutFinal),
    );
}

#[test]
fn native_registered_fifth_remote_native_base_actual_preserves_verified_history_floor() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetNativeBase));
}
#[test]
fn native_registered_fifth_remote_native_base_actual_post_wal_cancel_reconciles_same_xid() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseCanceled),
    );
}
#[test]
fn native_registered_fifth_remote_native_base_actual_integrity_survives_image_invalidation() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetNativeBaseInvalidated),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
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
    let range = plan.old_range().id;
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let owner_before = control.test_registered_remote_image_state(range).unwrap();
    let final_pointer = control
        .registered_remote_final_image_receipt(range)
        .unwrap()
        .pointer();
    let before_base = factory
        .database
        .read()
        .test_registered_native_base_json(range)
        .unwrap();
    assert!(before_base["base"].is_null());
    let canceled = if matches!(case, SourceCase::TargetNativeBaseCanceled) {
        let error = critical_diagnostics::advance_publication(
            control,
            client,
            source,
            plan,
            provider,
            actor,
            factory,
            pending,
            ranges,
            acknowledged,
            output,
            journal,
            Operation::PublishNativeBase,
            true,
        )
        .unwrap_err();
        let retained = control.test_registered_remote_image_state(range).unwrap();
        let actual_base = factory
            .database
            .read()
            .test_registered_native_base_json(range)
            .unwrap();
        evidence(
            &output.join("registered-native-base-canceled.json"),
            &serde_json::json!({
                "exact_error":error.to_string(),"retained":retained,"actual_native":actual_base,
                "expected_final_pointer":final_pointer,"catalogue_adoption_or_serving":false,
            }),
        )
        .unwrap();
        assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
        assert!(retained["native_base"]["attempted"].as_bool().unwrap());
        assert!(
            retained["native_base"]["actual_commit_returned"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(retained["native_base"]["verified"], false);
        assert_eq!(actual_base["base"], actual_base["archive"]);
        Some(retained)
    } else {
        None
    };
    let first = critical_diagnostics::advance_publication(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        factory,
        pending,
        ranges,
        acknowledged,
        output,
        journal,
        Operation::PublishNativeBase,
        false,
    )
    .unwrap();
    let second = critical_diagnostics::advance_publication(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        factory,
        pending,
        ranges,
        acknowledged,
        output,
        journal,
        Operation::PublishNativeBase,
        false,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::to_value(&second).unwrap()
    );
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    let owner_after = control.test_registered_remote_image_state(range).unwrap();
    let actual = factory
        .database
        .read()
        .test_registered_native_base_json(range)
        .unwrap();
    evidence(
        &output.join("registered-native-base-before-assertions.json"),
        &serde_json::json!({
            "before":before,"after":after,"owner_before":owner_before,"owner_after":owner_after,
            "actual_native":actual,"first_progress":first,"second_progress":second,
            "canceled":canceled,"expected_final_pointer":final_pointer,
            "native_floor_published":true,"catalogue_floor_adopted":false,"serving":false,
            "cold_recovery_or_physical_qualification":false,
        }),
    )
    .unwrap();
    let Progress::NativeBase {
        range_index,
        native_page_xid,
        checksum_sha256,
    } = first
    else {
        panic!("actual native base publication expected")
    };
    assert_eq!(range_index, source.manifest().range_index);
    assert_eq!(actual["base"], actual["archive"]);
    assert_eq!(actual["base"]["format_version"], 2);
    assert_eq!(
        actual["base"]["registered_final_image"],
        serde_json::to_value(&final_pointer).unwrap()
    );
    assert_eq!(actual["base"]["native_page_xid"], native_page_xid);
    assert_eq!(actual["base"]["checksum_sha256"], checksum_sha256);
    assert_eq!(owner_after["native_base"]["verified"], true);
    for field in [
        "rows",
        "schema",
        "namespace_ids",
        "open",
        "birth",
        "B",
        "raw",
        "slots",
        "protected_catalogue_uuid",
        "writer_installed",
    ] {
        assert_eq!(before[field], after[field], "native base changed {field}");
    }
    assert_eq!(after["writer_installed"], false);
    for other in ranges.iter().filter(|r| r.id != range) {
        let key = other.id.get().to_string();
        assert!(before["points"].get(&key).is_some());
        assert_eq!(before["points"][&key], after["points"][&key]);
    }
    assert_eq!(owner_before["history"], owner_after["history"]);
    assert_eq!(owner_before["rows"], owner_after["rows"]);
    assert_eq!(owner_before["final_image"], owner_after["final_image"]);
    if let Some(canceled) = canceled {
        assert_eq!(
            canceled["native_base"]["receipt"],
            owner_after["native_base"]["receipt"]
        );
        assert_eq!(
            canceled["native_base"]["actual_commit_returned"],
            native_page_xid
        );
    }
    let mf = source.manifest();
    {
        let db = factory.database.read();
        let native = db
            .native_recovery_admission(
                &mf.cluster_id,
                mf.bootstrap_id,
                &mf.target_id,
                mf.target_birth.node_incarnation(),
            )
            .unwrap();
        let integrity = db
            .native_installed_range_image_integrity(&native, range)
            .unwrap()
            .unwrap();
        assert_eq!(integrity.range_index(), mf.range_index);
        assert_eq!(integrity.logical_genesis_sha256(), mf.h0);
        assert_eq!(integrity.logical_history_sha256(), mf.h);
        integrity.require_current(&db, &native).unwrap();
    }
    if matches!(case, SourceCase::TargetNativeBaseInvalidated) {
        let generations = factory
            .database
            .read()
            .test_invalidate_registered_installed_base(range)
            .unwrap();
        let db = factory.database.read();
        let native = db
            .native_recovery_admission(
                &mf.cluster_id,
                mf.bootstrap_id,
                &mf.target_id,
                mf.target_birth.node_incarnation(),
            )
            .unwrap();
        let integrity = db
            .native_installed_range_image_integrity(&native, range)
            .unwrap()
            .unwrap();
        integrity.require_current(&db, &native).unwrap();
        assert_eq!(integrity.range_index(), mf.range_index);
        let current_error = db
            .native_installed_range_image_base(&native, range)
            .unwrap_err();
        let actual_after = db.test_registered_native_base_json(range).unwrap();
        evidence(&output.join("registered-native-base-invalidated-integrity.json"), &serde_json::json!({
            "actual_native":actual_after,"original_native":actual,"integrity_range_index":integrity.range_index(),"actual_image_generations":generations,
            "current_image_error":current_error.to_string(),"durable_floor_survives_image_invalidation":true,
            "serving":false,"cold_owner_reconstruction":false,
        })).unwrap();
        assert_eq!(actual_after, actual);
    }
}
