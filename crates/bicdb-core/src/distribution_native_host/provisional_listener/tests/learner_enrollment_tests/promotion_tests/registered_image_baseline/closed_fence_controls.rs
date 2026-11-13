//! Every control first requires BOTH real TLS fences/full original source ACKs.
use super::*;
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Case {
    Required,
    Ingress,
    Foreign,
    Canceled,
    HookAba,
    SlotAba,
    Stopped,
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    case: Case,
    host: &mut crate::distribution_native_host::NativeUnassignedLearnerHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    target: &Arc<Mutex<crate::ClusterDataNodeService>>,
    foreign: &Arc<Mutex<crate::ClusterDataNodeService>>,
    provider: &MetadataReadProvider,
    clients: &[Arc<TcpClusterRelocationTransport>],
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
    reports: &mut Vec<serde_json::Value>,
    network: &ClusterNetworkTransportConfig,
) {
    let range = pending.range_by_id(ranges[0].id).unwrap();
    let plan = pending
        .relocations
        .values()
        .find(|r| r.range_id == range.id)
        .unwrap();
    let session = plan.handoff_session.as_ref().unwrap();
    let source = (0..4).find(|i| node(*i) == range.leader).unwrap();
    let actor = clients[source].local_metadata_read_participant();
    let (admission, runtime) = host.test_registered_receiver_parts().unwrap();
    let before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(
        &before["raw"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap() as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let actual_format = raw["catalog"]["format_version"].clone();
    // Each control is a distinct original operation; no canceled/expired proof
    // is refreshed or reinterpreted as the original operation.
    evidence(
        &output.join("closed-fence-control-intent.json"),
        &serde_json::json!({
        "case":format!("{case:?}"),"before":before,"configuration":pending,
        "logical_local_api_not_raw_wire":true,"outcome":"not_started"}),
    )
    .unwrap();
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
        relocation_id: plan.id,
        plan_id: plan.plan_id.clone(),
        session_id: session.session_id,
        nonce: session.nonce,
    };
    let witness = provider
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            session.nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    let role = RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &node(4),
        business,
        range.id,
        range.epoch,
        session.nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    let capability = admission
        .observe_assigned_closed_fence(target, &runtime, &role, &scope, &parent)
        .unwrap();
    capability.require_live().unwrap();
    capability.require_metadata_current().unwrap();
    target
        .try_lock()
        .expect("actual target service busy in control")
        .preflight_registered_closed_fence(&capability, &parent)
        .unwrap();
    let reached_valid_capability = true;
    evidence(&output.join("closed-fence-control-intent.json"), &serde_json::json!({
        "case":format!("{case:?}"),"before":before,"configuration":pending,"scope":scope,
        "original_parent_remaining_ms":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()),
        "original_role_remaining_ms":role.deadline().saturating_duration_since(Instant::now()).as_millis(),
        "reached_genuine_valid_capability":true,"outcome":"before_actual_control"})).unwrap();
    parent.check().unwrap();
    capability.require_live().unwrap();
    let error = match case {
        Case::Ingress => {
            // Exact existing generic/older code path remains closed despite
            // persisted Active fences. This is not an old-binary execution.
            let error = target
                .try_lock()
                .expect("actual target service busy in control")
                .preflight_native_recovery(&role, &scope, &parent)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("enforced native writer host policy is not installed"),
                "{error}"
            );
            let cold = capability
                .test_old_cold_context_refusal(
                    factory.database.read().data_path(),
                    db_config(),
                    network,
                    &parent,
                )
                .unwrap_err();
            assert!(cold.to_string().contains("Registered catch-up requires exact current bound own identity, TLS and no data assignment"),"{cold}");
            evidence(&output.join("old-generic-cold-context-refusals.json"), &serde_json::json!({
                "generic_error":error.to_string(),"cold_context_error":cold.to_string(),
                "actual_catalogue_format":actual_format,"old_binary_executed":false,
                "cold_context_rejected_before_engine_open":true,"all_owner_cold_recovery_claimed":false})).unwrap();
            error
        }
        Case::Foreign => {
            let error = foreign
                .try_lock()
                .expect("actual foreign service busy in control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            assert!(
                error.to_string().contains("another actual")
                    || error.to_string().contains("another database")
                    || error.to_string().contains("actual DB"),
                "{error}"
            );
            error
        }
        Case::Canceled => {
            let frozen = runtime.test_retained_registered_freeze(&scope).unwrap();
            parent.cancel();
            let error = target
                .try_lock()
                .expect("actual target service busy in control")
                .fence_registered_closed_receiver(&capability, &frozen, &parent)
                .unwrap_err();
            assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
            assert!(capability.require_live().is_err());
            error
        }
        Case::HookAba => {
            let original = capability.test_keep_original_registration().unwrap();
            factory.database.read().clear_commit_admission();
            assert!(!factory.database.read().commit_admission_ready());
            let error = target
                .try_lock()
                .expect("actual target service busy in control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("original registered receiver hook changed"),
                "{error}"
            );
            std::hint::black_box(&original);
            error
        }
        Case::SlotAba => {
            let temporary = tempfile::tempdir().unwrap();
            let replacement = crate::RangeWriteStore::open(
                temporary.path(),
                pending.cluster_id.clone(),
                node(4),
                false,
            )
            .unwrap();
            let original = target
                .try_lock()
                .expect("actual target service busy in control")
                .test_replace_native_range_write_store(replacement);
            let removed = target
                .try_lock()
                .expect("actual target service busy in control")
                .test_replace_native_range_write_store(original);
            // SAME original Store survives/restores, but its actual slot has
            // been irreversibly revoked. No fake proof/UUID is installed.
            let error = target
                .try_lock()
                .expect("actual target service busy in control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            assert!(
                error.to_string().contains("Store slot ended or changed"),
                "{error}"
            );
            std::hint::black_box(&removed);
            error
        }
        Case::Stopped => {
            host.stop_owned_metadata(&parent).unwrap();
            let error = target
                .try_lock()
                .expect("actual target service busy in control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            assert!(
                error.to_string().contains("owner ended or is stopping"),
                "{error}"
            );
            error
        }
        Case::Required => unreachable!(),
    };
    if case != Case::Canceled {
        parent.check().unwrap();
        role.require_live_request().unwrap();
    }
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(
        before, after,
        "authority control changed actual native/raw/rows/slots/UID/birth"
    );
    reports.push(serde_json::json!({"control":format!("{case:?}"),"reached_genuine_valid_capability":reached_valid_capability,
        "actual_catalogue_format":actual_format,"wire_version":crate::distribution::CLUSTER_DATA_PROTOCOL_VERSION,
        "scope":scope,"error":error.to_string(),"before":before,"after":after,
        "original_parent_canceled":case==Case::Canceled,"not_old_binary_execution":true,
        "all_earlier_required_real_tls_fences_and_original_source_acks_passed":true}));
    evidence(&output.join("closed-fence-controls.json"), reports).unwrap();
}
macro_rules! control {
    ($name:ident,$case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline(Case::$case);
        }
    };
}
control!(
    native_registered_fifth_closed_fence_generic_ingress_stays_strict,
    Ingress
);
control!(
    native_registered_fifth_closed_fence_actual_foreign_database_refused,
    Foreign
);
control!(
    native_registered_fifth_closed_fence_captured_original_caller_cancel_refused,
    Canceled
);
control!(
    native_registered_fifth_closed_fence_original_empty_hook_aba_refused,
    HookAba
);
control!(
    native_registered_fifth_closed_fence_actual_store_slot_restore_refused,
    SlotAba
);
control!(
    native_registered_fifth_closed_fence_actual_receiver_stop_refused,
    Stopped
);
