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
    SinkCanceled,
    SinkIoUnknown,
    ActualInvalidation,
    UnprivilegedInitiator,
    AssignedWithoutImage,
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
    let mut sink_report = None;
    let mut foreign_report = None;
    let error = match case {
        Case::UnprivilegedInitiator => {
            // This member is genuinely TLS-authenticated, but is neither
            // metadata leader nor this range's leader. Reusing correlation
            // DATA from the valid operation cannot authorize its new RPC.
            let caller = clients[3].local_metadata_read_participant();
            assert_eq!(caller.node_id(), &node(3));
            assert_ne!(caller.node_id(), &range.leader);
            assert_ne!(caller.node_id(), &node(0));
            let error = clients[3]
                .recovery_epoch_fence_observation(
                    &node(4),
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 4000,
                    },
                    &parent,
                )
                .unwrap_err();
            assert!(
                error.to_string().contains(
                    "metadata recovery-role identity, purpose or committed configuration changed"
                ),
                "unprivileged real RPC did not reach the expected role refusal: {error}"
            );
            error
        }
        Case::AssignedWithoutImage => {
            // This actual read also refuses any CURRENT/PREVIOUS protected
            // native policy; absence is not inferred from the hook flag.
            let original_hook = factory
                .database
                .read()
                .capture_registered_receiver_hook()
                .unwrap();
            let frozen = runtime.test_retained_registered_freeze(&scope).unwrap();
            let error = host
                .install_assigned_closed_data_policy(
                    crate::distribution_data::NativeJointServingLease::Survivor(frozen),
                    &parent,
                )
                .unwrap_err();
            assert!(
                matches!(&error,BicDbError::Cluster(detail)
                if detail=="native host bootstrap: Assigned DATA owner requires a real target Image lease"),
                "{error}"
            );
            let database = factory.database.read();
            original_hook.require_current(&database).unwrap();
            database.capture_registered_receiver_hook().unwrap();
            assert!(!database.commit_admission_ready());
            error
        }
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
            let foreign_database = foreign
                .try_lock()
                .expect("actual foreign service busy before control")
                .database();
            assert!(!Arc::ptr_eq(&foreign_database, &factory.database));
            let foreign_before = state(&foreign_database, pending, &node(0), ranges).unwrap();
            assert_ne!(foreign_before["birth"], before["birth"]);
            assert_eq!(foreign_before["writer_installed"], true);
            let error = foreign
                .try_lock()
                .expect("actual foreign service busy in control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            // The actual receiver validates its birth/local-member binding
            // before looking up the Registered capability. A foreign INITIAL
            // DB therefore refuses at this earlier, stricter identity gate.
            assert!(
                matches!(&error, BicDbError::Cluster(detail)
                if detail == "native range image: native recovery birth/bootstrap/local member binding mismatch"),
                "unexpected foreign-database refusal: {error}"
            );
            let foreign_after = state(&foreign_database, pending, &node(0), ranges).unwrap();
            assert_eq!(
                foreign_before, foreign_after,
                "foreign refusal changed actual native/raw/rows/slots/UID/birth"
            );
            foreign_report = Some(serde_json::json!({
                "before":foreign_before,"after":foreign_after,
                "actual_initial_foreign_database_unchanged":true,
                "refusal_boundary":"actual native birth/bootstrap/local-member admission before capability lookup"}));
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
            let frozen = runtime.test_retained_registered_freeze(&scope).unwrap();
            let mut wrote = false;
            let error = target
                .try_lock()
                .expect("actual target service busy in slot control")
                .test_registered_closed_sink(&capability, &frozen, &parent, |path, bytes, fsync| {
                    wrote = true;
                    crate::storage::write_atomic(path, bytes, fsync)
                })
                .unwrap_err();
            assert!(
                !wrote,
                "revoked original Store slot reached actual refusal write"
            );
            assert!(
                error.to_string().contains("Store slot ended or changed"),
                "{error}"
            );
            std::hint::black_box(&removed);
            error
        }
        Case::SinkCanceled | Case::SinkIoUnknown => {
            let frozen = runtime.test_retained_registered_freeze(&scope).unwrap();
            let mut actual_write_completed = false;
            let mut write_deadlines = None;
            let error=target.try_lock().expect("target busy at actual sink control")
                .test_registered_closed_sink(&capability,&frozen,&parent,|path,bytes,fsync| {
                    crate::storage::write_atomic(path,bytes,fsync)?;
                    parent.check()?; capability.require_live()?;
                    actual_write_completed=true;
                    write_deadlines=Some(serde_json::json!({
                        "original_parent_remaining_ms":parent.deadline().map(|d|d.saturating_duration_since(Instant::now()).as_millis()),
                        "original_role_remaining_ms":role.deadline().saturating_duration_since(Instant::now()).as_millis(),
                        "actual_file_and_directory_fsync_returned":true}));
                    if case==Case::SinkCanceled {parent.cancel();Ok(())}
                    else {Err(std::io::Error::other("actual registered CLOSED durable sink reply lost").into())}
                }).unwrap_err();
            assert!(actual_write_completed, "control never reached actual fsync");
            if case == Case::SinkCanceled {
                assert!(matches!(error, BicDbError::QueryCanceled), "{error}");
                assert!(capability.require_live().is_err());
            } else {
                assert!(
                    matches!(&error,BicDbError::Io(io) if io.to_string().contains("actual registered CLOSED durable sink reply lost")),
                    "{error}"
                );
                parent.check().unwrap();
                capability.require_live().unwrap();
            }
            let persisted = target
                .try_lock()
                .expect("target busy after sink cut")
                .test_registered_closed_persistence_state()
                .unwrap();
            assert_eq!(persisted["uncertain"], true);
            assert_eq!(persisted["actual_checksum_verified"], true);
            assert_eq!(persisted["actual_equals_memory"], true);
            assert_eq!(
                persisted["actual_catalogue"]["recovery_fences"][range.id.get().to_string()]
                    ["scope"],
                serde_json::to_value(&scope).unwrap()
            );
            let after_cut = state(&factory.database, pending, &node(4), ranges).unwrap();
            let mut expected_catalogue = raw["catalog"].clone();
            expected_catalogue["recovery_fences"][range.id.get().to_string()]["scope"] =
                serde_json::to_value(&scope).unwrap();
            assert_eq!(
                persisted["actual_catalogue"], expected_catalogue,
                "actual sink changed more than exact restrictive scope provenance"
            );
            let mut before_native = before.clone();
            before_native.as_object_mut().unwrap().remove("raw");
            let mut after_native = after_cut.clone();
            after_native.as_object_mut().unwrap().remove("raw");
            assert_eq!(
                before_native, after_native,
                "actual sink changed native/row/slot/UID/B obligations"
            );
            let mut retried = None;
            if case == Case::SinkIoUnknown {
                // SAME original live operation, no fresh role/token/TTL. Only
                // exact re-fsync can clear the uncertainty and return a reply.
                let reply = target
                    .try_lock()
                    .expect("target busy at exact re-fsync")
                    .fence_registered_closed_receiver(&capability, &frozen, &parent)
                    .unwrap();
                assert!(reply.native.native_applied_index.is_none());
                let current = target
                    .try_lock()
                    .unwrap()
                    .test_registered_closed_persistence_state()
                    .unwrap();
                assert_eq!(current["uncertain"], false);
                assert_eq!(current["actual_equals_memory"], true);
                assert_eq!(current["actual_catalogue"], persisted["actual_catalogue"]);
                retried = Some(current);
            }
            sink_report = Some(
                serde_json::json!({"reached_actual_write":actual_write_completed,"at_write":write_deadlines,
                "durable_refusal_after_error":persisted,"native_after_cut":after_cut,"same_original_scope_refsync":retried,
                "canceled_scope_not_refreshed":case==Case::SinkCanceled}),
            );
            error
        }
        Case::ActualInvalidation => {
            let actual = factory
                .database
                .read()
                .test_invalidate_original_registered_receiver(range.id)
                .unwrap();
            capability.require_metadata_current().unwrap();
            let error = target
                .try_lock()
                .expect("target busy at durable invalidation control")
                .preflight_registered_closed_fence(&capability, &parent)
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("existing native domain/invalidation/install/history record"),
                "{error}"
            );
            // Diagnostic UNKNOWN/invalidated was true both before AND after;
            // it is never converted into an actual no-domain proof or R0.
            assert_eq!(
                before["points"][range.id.get().to_string()]["invalidated"],
                true
            );
            sink_report = Some(serde_json::json!({"actual_durable_invalidation":actual,
                "existing_valid_capability_before_real_invalidation":true,"no_genesis_or_floor_created":true}));
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
    if case != Case::Canceled && case != Case::SinkCanceled {
        parent.check().unwrap();
        role.require_live_request().unwrap();
    }
    let after = state(&factory.database, pending, &node(4), ranges).unwrap();
    if !matches!(case, Case::SinkCanceled | Case::SinkIoUnknown) {
        assert_eq!(
            before, after,
            "authority control changed actual native/raw/rows/slots/UID/birth"
        );
    } else {
        let mut a = before.clone();
        a.as_object_mut().unwrap().remove("raw");
        let mut b = after.clone();
        b.as_object_mut().unwrap().remove("raw");
        assert_eq!(a, b);
    }
    reports.push(serde_json::json!({"control":format!("{case:?}"),"reached_genuine_valid_capability":reached_valid_capability,
        "actual_catalogue_format":actual_format,"wire_version":crate::distribution::CLUSTER_DATA_PROTOCOL_VERSION,
        "scope":scope,"error":error.to_string(),"before":before,"after":after,
        "original_parent_canceled":matches!(case,Case::Canceled|Case::SinkCanceled),"actual_sink_cut":sink_report,"actual_foreign_database":foreign_report,"not_old_binary_execution":true,
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

control!(
    native_registered_fifth_closed_fence_actual_sink_fsync_then_original_cancel_retains_refusal,
    SinkCanceled
);
control!(
    native_registered_fifth_closed_fence_actual_sink_io_unknown_exact_live_refsync,
    SinkIoUnknown
);

control!(
    native_registered_fifth_closed_fence_actual_durable_invalidation_is_not_unknown_absence,
    ActualInvalidation
);

control!(
    native_registered_fifth_closed_fence_actual_nonleader_rpc_initiator_refused,
    UnprivilegedInitiator
);
control!(
    native_registered_fifth_closed_fence_actual_assigned_policy_requires_image,
    AssignedWithoutImage
);
