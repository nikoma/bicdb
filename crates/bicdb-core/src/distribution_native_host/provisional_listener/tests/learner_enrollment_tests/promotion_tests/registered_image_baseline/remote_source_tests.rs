//! Source-wire prerequisites with separate actual local-OLD and remote-OLD
//! source cases. Target is the real fifth; none grants image rows or Serving.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeJointHandoffRuntime, NativeJointOldRound,
};
use crate::distribution_range_image::{
    NativeRemoteImageFact, NativeRemoteImageManifestV1, NativeRemoteImageSourceOperationV1 as Op,
    NativeRemoteImageSourcePayloadV1 as Payload, NativeRemoteImageSourceRequestV1,
    RangeImageCursorV1, RangeImageHistoryCursorV1,
};
use crate::distribution_transport::AuthenticatedNativeRemoteImageSourceFrame;

#[derive(Clone, Copy, Debug)]
pub(super) enum SourceCase {
    Frames,
    CancelAfterIssuedFrame,
    RevokeBeforeNativeWal,
    RemoteOldFrames,
    RemoteOldRetainedNative,
    TargetNoDomain,
    TargetNoDomainHookRevoked,
    TargetNoDomainMemberChanged,
    TargetNoDomainSchemaRevoked,
    TargetTransfer,
    TargetTransferBusy,
    TargetTransferCanceled,
    TargetTransferHookRevoked,
    TargetProposal,
    TargetProposalPostCanceled,
    TargetProposalBeforeCanceled,
    TargetProposalBeforeHookRevoked,
    TargetProposalReaderFenceRetry,
    TargetInstalling,
    TargetInstallingPostCanceled,
    TargetInstallingDirSyncRetry,
    TargetImageRows,
    TargetImageRowsWithoutAudit,
    TargetImageRowsPreWalCanceled,
    TargetImageRowsPostWalCanceled,
    TargetImageRowsApplyFailed,
    TargetFinalImage,
    TargetFinalImageCanceled,
    TargetFinalImageCorrupt,
    TargetFinalImageWithoutScan,
    TargetFinalImageHistorical,
    TargetNativeBase,
    TargetNativeBaseCanceled,
    TargetNativeBaseInvalidated,
    TargetNativeBaseWithoutFinal,
    TargetNativeBaseRoleExpiryBeforePut,
    TargetNativeBaseRoleExpiryAfterCommit,
    TargetCatalogueFloor,
    TargetCatalogueFloorMissingBase,
    TargetCatalogueFloorDirSyncRetry,
    TargetCatalogueFloorCanceled,
    RemoteConstraintCapture,
    RemoteOldCollectorExpiredObservation,
    RemoteOldCollectorUnrelatedAdvance,
    RemoteOldCollectorPlanChanged,
    RemoteOldCollectorDonorRevoked,
    RemoteOldCollectorCallerCanceled,
}
#[test]
fn native_registered_fifth_remote_source_actual_tls_history_rows_seal_local_old_seed() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::Frames));
}
#[test]
fn native_registered_fifth_remote_source_actual_session_cancel_preserves_issued_facts_target_closed(
) {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::CancelAfterIssuedFrame),
    );
}
#[test]
fn native_registered_fifth_remote_old_source_actual_hook_revoked_after_promise_before_native_wal() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RevokeBeforeNativeWal),
    );
}
#[test]
fn native_registered_fifth_remote_old_actual_close_fetch_cut_and_source_tls_without_local_quorum() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::RemoteOldFrames));
}
#[test]
fn native_registered_fifth_remote_old_post_native_cancel_retains_outcome_and_exact_fresh_retry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldRetainedNative),
    );
}
#[test]
fn native_registered_fifth_remote_old_constraint_only_guarded_capture_is_not_closed_vote() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteConstraintCapture),
    );
}
#[test]
fn native_registered_fifth_remote_old_collector_actual_fresh_final_mq_after_observation_expiry() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldCollectorExpiredObservation),
    );
}
#[test]
fn native_registered_fifth_remote_old_collector_actual_unrelated_metadata_advance_is_allowed() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldCollectorUnrelatedAdvance),
    );
}
#[test]
fn native_registered_fifth_remote_old_collector_actual_selected_plan_phase_change_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldCollectorPlanChanged),
    );
}
#[test]
fn native_registered_fifth_remote_old_collector_actual_original_donor_revocation_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldCollectorDonorRevoked),
    );
}
#[test]
fn native_registered_fifth_remote_old_collector_actual_original_caller_cancellation_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteOldCollectorCallerCanceled),
    );
}
// The later target-history/row cases exercise the actual production remote
// collector. Local OLD API cases retain their original finite issuance
// contract and are deliberately not switched to this lane.
pub(super) fn uses_actual_remote_old_seed(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::RemoteOldFrames
            | SourceCase::RemoteOldRetainedNative
            | SourceCase::TargetImageRows
            | SourceCase::TargetImageRowsWithoutAudit
            | SourceCase::TargetImageRowsPreWalCanceled
            | SourceCase::TargetImageRowsPostWalCanceled
            | SourceCase::TargetImageRowsApplyFailed
            | SourceCase::TargetFinalImage
            | SourceCase::TargetFinalImageCanceled
            | SourceCase::TargetFinalImageCorrupt
            | SourceCase::TargetFinalImageWithoutScan
            | SourceCase::TargetFinalImageHistorical
            | SourceCase::TargetNativeBase
            | SourceCase::TargetNativeBaseCanceled
            | SourceCase::TargetNativeBaseInvalidated
            | SourceCase::TargetNativeBaseWithoutFinal
            | SourceCase::TargetNativeBaseRoleExpiryBeforePut
            | SourceCase::TargetNativeBaseRoleExpiryAfterCommit
            | SourceCase::TargetCatalogueFloor
            | SourceCase::TargetCatalogueFloorMissingBase
            | SourceCase::TargetCatalogueFloorDirSyncRetry
            | SourceCase::TargetCatalogueFloorCanceled
            | SourceCase::RemoteOldCollectorExpiredObservation
            | SourceCase::RemoteOldCollectorUnrelatedAdvance
            | SourceCase::RemoteOldCollectorPlanChanged
            | SourceCase::RemoteOldCollectorDonorRevoked
            | SourceCase::RemoteOldCollectorCallerCanceled
    )
}
fn actual_role(
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    plan: &NativeJointHandoffPlan,
    recipient: &ClusterNodeId,
    parent: &CancellationToken,
) -> RecoveryRoleAdmission {
    let promise = plan.promise();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
        relocation_id: promise.relocation_id,
        plan_id: promise.plan_id.clone(),
        session_id: promise.session_id,
        nonce: promise.nonce,
    };
    let observed = provider
        .witness_recovery_role(
            actor,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            purpose.clone(),
            parent,
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        observed,
        actor,
        recipient,
        business,
        plan.old_range().id,
        plan.old_range().epoch,
        promise.nonce,
        challenge,
        &purpose,
    )
    .unwrap()
}
#[allow(clippy::too_many_arguments)]
fn sink_control(
    revoke: bool,
    source: usize,
    controls: &[NativeMetadataControlHost],
    runtime: &Arc<NativeJointHandoffRuntime>,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    plan: &Arc<NativeJointHandoffPlan>,
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
) {
    let database = services[source].lock().database();
    let before = state(&database, pending, &node(source), ranges).unwrap();
    assert!(!database
        .read()
        .test_native_old_close_present(plan.old_range().id)
        .unwrap());
    evidence(
        &output.join("old-sink-control-intent.json"),
        &serde_json::json!({
        "operation":"actual OLD source sink fault","revoke_hook":revoke,
        "before":before,"outcome":"not_started","logical_local_api_not_wire":true}),
    )
    .unwrap();
    let caller = budget(5000);
    let original_role = Arc::new(actual_role(provider, actor, plan, &node(source), &caller));
    let own_parent = caller.child();
    let own_business = Uuid::now_v7();
    let own = clients[source]
        .acquire_host_metadata_observation(
            &node(0),
            own_business,
            Duration::from_secs(5),
            &own_parent,
        )
        .unwrap();
    let own_deadline = own.deadline();
    let hook = if revoke {
        crate::distribution_native_host::CancelOldOwnBeforeNativeWal::revoke_hook(
            &database,
            &original_role,
        )
    } else {
        crate::distribution_native_host::CancelOldOwnBeforeNativeWal::install(
            &database,
            &original_role,
            &own_parent,
        )
    };
    let result = controls[source].close_initial_old_step(
        runtime.clone(),
        original_role.clone(),
        own,
        own_business,
        &caller,
    );
    assert!(
        hook.fired(),
        "actual durable OLD raw promise returned before native-WAL fault"
    );
    caller.check().unwrap();
    original_role.require_live_request().unwrap();
    assert!(
        Instant::now() < own_deadline,
        "original OWN deadline must remain live; only deliberate cancel/revoke explains refusal"
    );
    let error = result
        .err()
        .expect("actual original step must refuse at pre-native-WAL boundary");
    if revoke {
        own_parent.check().unwrap();
        assert!(error.to_string().contains("native writer hook"), "{error}");
    } else {
        assert!(matches!(&error, BicDbError::QueryCanceled), "{error}");
        assert!(matches!(own_parent.check(), Err(BicDbError::QueryCanceled)));
    }
    assert!(
        !database
            .read()
            .test_native_old_close_present(plan.old_range().id)
            .unwrap(),
        "native OLD close must not be minted after original input revocation"
    );
    let after = state(&database, pending, &node(source), ranges).unwrap();
    let raw_bytes = after["raw"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b.as_u64().unwrap() as u8)
        .collect::<Vec<_>>();
    let raw: serde_json::Value = serde_json::from_slice(&raw_bytes).unwrap();
    let promise: crate::distribution_range_handoff::RangeHandoffPromise = serde_json::from_value(
        raw["catalog"]["handoff_promises"][plan.old_range().id.get().to_string()].clone(),
    )
    .unwrap();
    assert!(promise.same_request(plan.promise()));
    assert_eq!(
        promise.resolved_through, 1,
        "real fsynced raw promise remains owned, not erased on error"
    );
    let mut normalized_before = before.clone();
    let mut normalized_after = after.clone();
    normalized_before.as_object_mut().unwrap().remove("raw");
    normalized_after.as_object_mut().unwrap().remove("raw");
    if revoke {
        normalized_before
            .as_object_mut()
            .unwrap()
            .remove("writer_installed");
        normalized_after
            .as_object_mut()
            .unwrap()
            .remove("writer_installed");
    }
    assert_eq!(
        normalized_before, normalized_after,
        "fault changed acknowledged rows/native R/H/B/UID/birth/slots"
    );
    evidence(&output.join("old-sink-control-result.json"),&serde_json::json!({
        "revoke_hook":revoke,"reached_after_durable_raw_before_native_wal":true,
        "original_role_and_caller_live":true,"original_own_deadline_live":true,"error":error.to_string(),
        "before":before,"after":after,"actual_promise":promise,"native_old_close_present":false,
        "rollback_claimed":false,"logical_local_api_not_wire":true})).unwrap();
}
fn frame(
    target: &TcpClusterRelocationTransport,
    manifest: &NativeRemoteImageManifestV1,
    operation: Op,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) -> Result<AuthenticatedNativeRemoteImageSourceFrame> {
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let own = target.acquire_host_metadata_observation(
        &node(0),
        business,
        Duration::from_secs(5),
        &parent,
    )?;
    let request = NativeRemoteImageSourceRequestV1 {
        source_session: manifest.source_session,
        target_attempt: manifest.target_attempt,
        manifest_sha256: manifest.checksum_sha256.clone(),
        business,
        challenge: Uuid::now_v7(),
        budget_ms: 4000,
        operation,
    };
    let index = journal.len();
    journal.push(
        serde_json::json!({"operation":"source_image_frame","intent":request,
        "source":manifest.source_id,"target":manifest.target_id,
        "outcome":"not_started","actual_tls_observation_not_target_permission":true}),
    );
    evidence(&output.join("source-frame-ledger.json"), journal)?;
    let result =
        target.request_remote_image_source_frame(manifest, &request, &own, business, &parent);
    let original_parent_live_at_reply = parent.check();
    let original_own_live_at_reply = own.require_live_request(business);
    journal[index]["outcome"] = if result.is_ok() {
        "returned"
    } else {
        "error_reply_unknown_effect"
    }
    .into();
    journal[index]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
    journal[index]["original_parent_live_at_reply"] = original_parent_live_at_reply.is_ok().into();
    journal[index]["original_own_live_at_reply"] = original_own_live_at_reply.is_ok().into();
    if let Ok(actual) = &result {
        actual.require_request_live()?;
        journal[index]["actual_reply"] = serde_json::to_value(actual.reply()).unwrap();
        assert_eq!(actual.manifest().checksum_sha256, manifest.checksum_sha256);
    }
    // This observation has returned; ledger IO failure does not relabel its
    // actual effect. Preserve the primary API error if both failed.
    let written = evidence(&output.join("source-frame-ledger.json"), journal);
    match result {
        Ok(actual) => {
            original_parent_live_at_reply?;
            original_own_live_at_reply?;
            written?;
            Ok(actual)
        }
        Err(error) => {
            if let Err(evidence_error) = written {
                eprintln!("source frame evidence error: {evidence_error}");
            }
            Err(error)
        }
    }
}
#[allow(clippy::too_many_arguments)]
fn remote_old_seed(
    case: SourceCase,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    plan: &Arc<NativeJointHandoffPlan>,
    process: &CancellationToken,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) -> Option<(
    usize,
    crate::distribution_range_image::NativeRetainedRemoteImageSource,
    Instant,
)> {
    use crate::distribution_range_handoff::{NativeRemoteOldOperationV1, NativeRemoteOldRequestV1};
    let fault_after_native = matches!(case, SourceCase::RemoteOldRetainedNative);
    let mut last_closed = Instant::now();
    for (ordinal, recipient) in plan.old_members().keys().enumerate() {
        let native_fault = fault_after_native && ordinal == 0;
        let recipient_index = (0..4).find(|i| node(*i) == *recipient).unwrap();
        let database = services[recipient_index].lock().database();
        let hook = native_fault.then(|| {
            crate::distribution_native_host::CancelRemoteOldAfterNativeWal::install(
                &database,
                plan.old_range().id,
                plan.promise().nonce,
            )
        });
        let parent = budget(5000);
        let business = Uuid::now_v7();
        // The real metadata leader is a valid mutation initiator even when
        // it is not an OLD data voter. Its response is NOT forwarded into the
        // donor certificate; the donor independently fetches every node later.
        let own = clients[0]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
            .unwrap();
        let request = NativeRemoteOldRequestV1::from_plan(
            plan,
            NativeRemoteOldOperationV1::Close,
            business,
            4000,
        )
        .unwrap();
        journal.push(
            serde_json::json!({"operation":"remote_old_close", "request":request,
            "actual_initiator":node(0), "recipient":recipient,"outcome":"not_started"}),
        );
        let index = journal.len() - 1;
        evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
        let result = clients[0].request_remote_native_old(recipient, plan, &request, &own, &parent);
        journal[index]["outcome"] = if result.is_ok() {
            "returned_actual_closure"
        } else {
            "error_unknown_effect"
        }
        .into();
        journal[index]["exact_error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        if let Ok(actual) = &result {
            journal[index]["actual_reply"] = serde_json::to_value(actual.reply()).unwrap();
        }
        evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
        let actual = if let Some(hook) = hook {
            assert!(
                hook.fired(),
                "actual selected receiver reached after-native boundary"
            );
            drop(hook);
            parent.check().unwrap();
            own.require_live_request(business).unwrap();
            let error = result
                .err()
                .expect("actual receiver's original call was canceled");
            assert!(
                error
                    .to_string()
                    .contains(&BicDbError::QueryCanceled.to_string()),
                "{error}"
            );
            let native_after_error = database
                .read()
                .test_native_old_close_record(plan.old_range().id)
                .unwrap()
                .expect("actual protected OLD WAL record remains after postcheck cancellation");
            // A completed native receipt is retained, but raw confirmation/pin
            // may still be incomplete. Readonly Observe must refuse explicitly
            // rather than manufacture a completed closure from that record.
            let readonly_parent = budget(5000);
            let readonly_business = Uuid::now_v7();
            let readonly_own = clients[recipient_index]
                .acquire_host_metadata_observation(
                    &node(0),
                    readonly_business,
                    Duration::from_secs(5),
                    &readonly_parent,
                )
                .unwrap();
            let readonly_request = NativeRemoteOldRequestV1::from_plan(
                plan,
                NativeRemoteOldOperationV1::Observe,
                readonly_business,
                4000,
            )
            .unwrap();
            let readonly = clients[recipient_index].request_remote_native_old(
                recipient,
                plan,
                &readonly_request,
                &readonly_own,
                &readonly_parent,
            );
            let readonly_error = readonly
                .err()
                .expect("partial exact OLD result is not a closure vote");
            assert!(
                readonly_error
                    .to_string()
                    .contains("OLD source native closure is not known complete"),
                "{readonly_error}"
            );
            let retry_parent = budget(5000);
            let retry_business = Uuid::now_v7();
            let retry_own = clients[0]
                .acquire_host_metadata_observation(
                    &node(0),
                    retry_business,
                    Duration::from_secs(5),
                    &retry_parent,
                )
                .unwrap();
            let retry_request = NativeRemoteOldRequestV1::from_plan(
                plan,
                NativeRemoteOldOperationV1::Close,
                retry_business,
                4000,
            )
            .unwrap();
            let retry_index = journal.len();
            journal.push(serde_json::json!({
                "operation":"remote_old_close_retry", "request":retry_request,
                "recipient":recipient,"outcome":"not_started",
                "original_error":error.to_string(),"native_after_original_error":native_after_error,
                "readonly_request":readonly_request,"readonly_exact_error":readonly_error.to_string(),
                "native_receipt_is_not_completed_closure":true,
            }));
            evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
            let retried = clients[0].request_remote_native_old(
                recipient,
                plan,
                &retry_request,
                &retry_own,
                &retry_parent,
            );
            journal[retry_index]["outcome"] = if retried.is_ok() {
                "returned_actual_closure"
            } else {
                "error_unknown_effect"
            }
            .into();
            journal[retry_index]["exact_error"] =
                serde_json::to_value(retried.as_ref().err().map(ToString::to_string)).unwrap();
            if let Ok(actual) = &retried {
                journal[retry_index]["actual_reply"] =
                    serde_json::to_value(actual.reply()).unwrap();
            }
            let native_after_retry = database
                .read()
                .test_native_old_close_record(plan.old_range().id)
                .unwrap()
                .unwrap();
            journal[retry_index]["native_after_retry"] = native_after_retry.clone();
            evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
            assert_eq!(
                native_after_retry, native_after_error,
                "same durable native receipt/xid; fresh role does not rewrite WAL"
            );
            let actual = retried.unwrap();
            actual.require_request_live().unwrap();
            assert_eq!(
                actual
                    .reply()
                    .closed
                    .as_ref()
                    .unwrap()
                    .native_receipt_sha256,
                native_after_error["checksum_sha256"].as_str().unwrap()
            );
            retry_parent.check().unwrap();
            retry_own.require_live_request(retry_business).unwrap();
            actual
        } else {
            result.unwrap()
        };
        actual.require_request_live().unwrap();
        if !native_fault {
            parent.check().unwrap();
            own.require_live_request(business).unwrap();
        }
        assert!(actual.reply().closed.is_some());
        last_closed = Instant::now();
    }
    // Every OLD mutation request was <=4s at its real receiver and completed
    // before this causal local receipt. Waiting >5s ends those original finite
    // acquisitions; the next readonly facts must not revive any of them.
    let expiry_bound = last_closed + Duration::from_millis(5001);
    while Instant::now() <= expiry_bound {
        process.check().unwrap();
        std::thread::sleep(Duration::from_millis(10));
    }
    let donor = plan
        .old_members()
        .keys()
        .find(|id| **id != plan.old_range().leader && *id != plan.retiring_source())
        .expect("required genuine nonleader donor")
        .clone();
    let source = (0..4).find(|i| node(*i) == donor).unwrap();
    assert_ne!(donor, plan.old_range().leader);
    assert_ne!(&donor, plan.retiring_source());
    // Independently retain every real protected native receipt before the
    // fresh readonly collector. Its result must not re-close any OLD node or
    // allocate another native xid after the mutation requests have expired.
    let native_before = plan
        .old_members()
        .keys()
        .map(|recipient| {
            let index = (0..4).find(|i| node(*i) == *recipient).unwrap();
            let database = services[index].lock().database();
            let record = database
                .read()
                .test_native_old_close_record(plan.old_range().id)
                .unwrap()
                .expect("each actual OLD Close retained its native receipt");
            (recipient.to_string(), record)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(native_before.len(), plan.old_members().len());
    let collect_index = journal.len();
    journal.push(serde_json::json!({
        "operation":"remote_old_fresh_all_closed_collection",
        "actual_donor":donor, "plan_sha256":plan.checksum_sha256().unwrap(),
        "range_id":plan.old_range().id, "old_members":plan.old_members().keys().collect::<Vec<_>>(),
        "original_mutation_requests_expired":true,
        "native_before":native_before, "outcome":"not_started",
        "all_closed_and_irreversible_constraints_required":true,
    }));
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    let parent = budget(5000);
    let mut boundary_observation = serde_json::Value::Null;
    let collector_control = matches!(
        case,
        SourceCase::RemoteOldCollectorExpiredObservation
            | SourceCase::RemoteOldCollectorUnrelatedAdvance
            | SourceCase::RemoteOldCollectorPlanChanged
            | SourceCase::RemoteOldCollectorDonorRevoked
            | SourceCase::RemoteOldCollectorCallerCanceled
    );
    let result = if collector_control {
        controls[source].test_collect_initial_remote_old_at_actual_donor(
            plan.old_range().id,
            &parent,
            |original_own, original_business, frames| {
                parent.check()?;
                for frame in frames {
                    frame.require_request_live()?;
                }
                boundary_observation = serde_json::json!({
                    "original_context_id":original_own.context_id(),
                    "original_term":original_own.term(),
                    "original_read_index":original_own.read_index(),
                    "case":format!("{case:?}"),
                    "actual_observe_fetch_completed":true,
                    "actual_observe_frames":frames.iter().map(|f|f.reply()).collect::<Vec<_>>(),
                    "original_frame_requests_live_at_fetch":true,
                });
                match case {
                    SourceCase::RemoteOldCollectorExpiredObservation => {
                        while Instant::now() <= original_own.deadline() {
                            parent.check()?;
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        assert!(original_own
                            .require_live_request(original_business)
                            .is_err());
                        boundary_observation["original_metadata_authority_expired"] = true.into();
                    }
                    SourceCase::RemoteOldCollectorUnrelatedAdvance
                    | SourceCase::RemoteOldCollectorPlanChanged => {
                        let mut next = metadata[0].lock().committed_topology().clone();
                        if matches!(case, SourceCase::RemoteOldCollectorPlanChanged) {
                            next.relocations
                                .get_mut(&plan.promise().relocation_id)
                                .unwrap()
                                .phase = crate::distribution::RelocationPhase::SnapshotCopying;
                        } else {
                            next.ranges
                                .values_mut()
                                .find(|r| r.id != plan.old_range().id)
                                .unwrap()
                                .approximate_qps += 1;
                        }
                        next.generation += 1;
                        next.validate()?;
                        let entry = metadata[0].lock().propose_topology(next.clone())?;
                        controls[0].heartbeat(&parent)?;
                        controls[0].heartbeat(&parent)?;
                        for actual in metadata {
                            let store = actual.lock();
                            assert!(store.status().commit_index >= entry.index);
                            assert_eq!(store.committed_topology(), &next);
                        }
                        boundary_observation["actual_committed_metadata_index"] =
                            entry.index.into();
                        boundary_observation["actual_committed_topology"] =
                            serde_json::to_value(&next).unwrap();
                    }
                    SourceCase::RemoteOldCollectorDonorRevoked => {
                        services[source]
                            .lock()
                            .database()
                            .read()
                            .clear_commit_admission();
                        boundary_observation["actual_original_writer_hook_revoked"] = true.into();
                    }
                    SourceCase::RemoteOldCollectorCallerCanceled => {
                        parent.cancel();
                        boundary_observation["actual_original_caller_canceled"] = true.into();
                    }
                    _ => unreachable!(),
                }
                Ok(())
            },
        )
    } else {
        controls[source].collect_initial_remote_old_at_actual_donor(plan.old_range().id, &parent)
    };
    let native_after = plan
        .old_members()
        .keys()
        .map(|recipient| {
            let index = (0..4).find(|i| node(*i) == *recipient).unwrap();
            let database = services[index].lock().database();
            let record = database
                .read()
                .test_native_old_close_record(plan.old_range().id)
                .unwrap()
                .expect("readonly collection cannot remove an actual OLD receipt");
            (recipient.to_string(), record)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    journal[collect_index]["outcome"] = if result.is_ok() {
        "returned_actual_all_old_certificate"
    } else {
        "error_no_certificate"
    }
    .into();
    journal[collect_index]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
    journal[collect_index]["native_after"] = serde_json::to_value(&native_after).unwrap();
    journal[collect_index]["actual_boundary_observation"] = boundary_observation.clone();
    if let Ok(actual) = &result {
        journal[collect_index]["actual_collector"] =
            actual.test_actual_collector_evidence().clone();
    }
    journal[collect_index]["native_receipts_unchanged"] =
        serde_json::json!(native_after == native_before);
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
    assert_eq!(
        native_after, native_before,
        "readonly collection preserves all native record fields, xids and checksums"
    );
    if matches!(
        case,
        SourceCase::RemoteOldCollectorPlanChanged
            | SourceCase::RemoteOldCollectorDonorRevoked
            | SourceCase::RemoteOldCollectorCallerCanceled
    ) {
        let error = result
            .err()
            .expect("actual boundary invalidation must refuse OLD certificate");
        match case {
            SourceCase::RemoteOldCollectorPlanChanged => assert!(
                error.to_string().contains("selected phase changed"),
                "{error}"
            ),
            SourceCase::RemoteOldCollectorDonorRevoked => assert!(
                error.to_string().contains("registration") || error.to_string().contains("writer"),
                "{error}"
            ),
            SourceCase::RemoteOldCollectorCallerCanceled => {
                assert!(matches!(error, BicDbError::QueryCanceled), "{error}")
            }
            _ => unreachable!(),
        }
        return None;
    }
    let actual = result.unwrap();
    assert_eq!(actual.selected_source_id(), &donor);
    if collector_control {
        let final_evidence = actual.test_actual_collector_evidence();
        assert_ne!(
            final_evidence["final_context_id"],
            boundary_observation["original_context_id"]
        );
        assert!(
            final_evidence["final_term"].as_u64().unwrap()
                >= boundary_observation["original_term"].as_u64().unwrap()
        );
        assert!(
            final_evidence["final_read_index"].as_u64().unwrap()
                >= boundary_observation["original_read_index"]
                    .as_u64()
                    .unwrap()
        );
        assert_eq!(
            final_evidence["actual_observe_frames"]
                .as_array()
                .unwrap()
                .len(),
            plan.old_members().len()
        );
        if matches!(case, SourceCase::RemoteOldCollectorUnrelatedAdvance) {
            let committed = boundary_observation["actual_committed_metadata_index"]
                .as_u64()
                .unwrap();
            assert!(
                committed
                    > boundary_observation["original_read_index"]
                        .as_u64()
                        .unwrap()
            );
            assert!(final_evidence["final_read_index"].as_u64().unwrap() >= committed);
        }
        // Collector controls stop at the actual certificate; they do not
        // publish a source image or start target installation.
        return None;
    }
    let mut actual = Some(actual);
    let retained = controls[source]
        .retain_initial_image_from_remote_old(&mut actual, Uuid::now_v7(), &parent)
        .unwrap();
    assert!(actual.is_none());
    parent.check().unwrap();
    Some((source, retained, expiry_bound))
}
#[allow(clippy::too_many_arguments)]
fn constraint_capture_control(
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    plan: &Arc<NativeJointHandoffPlan>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) {
    use crate::distribution_range_handoff::{
        NativeRemoteOldOperationV1 as OldOp, NativeRemoteOldRequestV1,
    };
    let recipient = plan.old_members().keys().next().unwrap();
    let index = (0..4).find(|i| node(*i) == *recipient).unwrap();
    let donor = plan
        .old_members()
        .keys()
        .find(|id| *id != recipient)
        .unwrap();
    let donor_index = (0..4).find(|i| node(*i) == *donor).unwrap();
    let db = services[index].lock().database();
    let before = state(&db, pending, recipient, ranges).unwrap();
    assert!(db
        .read()
        .test_native_old_close_record(plan.old_range().id)
        .unwrap()
        .is_none());
    let mut request = |actor: usize, target: &ClusterNodeId, operation: OldOp| {
        let parent = budget(5000);
        let business = Uuid::now_v7();
        let own = clients[actor]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
            .unwrap();
        let request = NativeRemoteOldRequestV1::from_plan(plan, operation, business, 4000).unwrap();
        let journal_index = journal.len();
        journal.push(
            serde_json::json!({"operation":"remote_old_constraint_control", "request":request,
            "actual_initiator":node(actor),"recipient":target,"outcome":"not_started"}),
        );
        evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
        let result =
            clients[actor].request_remote_native_old(target, plan, &request, &own, &parent);
        journal[journal_index]["outcome"] = if result.is_ok() {
            "actual_returned"
        } else {
            "error_unknown_effect"
        }
        .into();
        journal[journal_index]["exact_error"] =
            serde_json::to_value(result.as_ref().err().map(ToString::to_string)).unwrap();
        if let Ok(actual) = &result {
            journal[journal_index]["actual_reply"] = serde_json::to_value(actual.reply()).unwrap();
        }
        evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
        let actual = result.unwrap();
        actual.require_request_live().unwrap();
        parent.check().unwrap();
        own.require_live_request(business).unwrap();
        actual
    };
    let capture = request(0, recipient, OldOp::Constrain);
    assert!(capture.reply().closed.is_none());
    assert_eq!(capture.reply().constraints.native_applied_index, Some(1));
    assert_eq!(
        capture.reply().constraints.native_pending_index,
        None,
        "this control uses a healthy replica; genuine KnownPending qualification remains OPEN"
    );
    assert_eq!(capture.reply().constraints.actual_last, 1);
    assert_eq!(capture.reply().constraints.actual_resolved, 1);
    let readonly = request(donor_index, recipient, OldOp::Observe);
    assert!(readonly.reply().closed.is_none());
    assert_eq!(readonly.reply().constraints, capture.reply().constraints);
    assert_eq!(
        readonly.reply().database_open,
        capture.reply().database_open
    );
    assert_eq!(readonly.reply().store_open, capture.reply().store_open);
    assert_eq!(
        state(&db, pending, recipient, ranges).unwrap(),
        before,
        "constraint capture cannot write rows/native/RAW/B/UID or replace writer policy"
    );
    assert!(db
        .read()
        .test_native_old_close_record(plan.old_range().id)
        .unwrap()
        .is_none());
    for old in plan.old_members().keys().filter(|id| *id != recipient) {
        let closed = request(0, old, OldOp::Close);
        assert!(closed.reply().closed.is_some());
    }
    drop(request);
    // Actual donor has its own closed prefix, but the first ALL-old collector
    // must refuse the real independently fetched constraint-only peer.
    let parent = budget(5000);
    let result = controls[donor_index]
        .collect_initial_remote_old_at_actual_donor(plan.old_range().id, &parent);
    let error = result
        .err()
        .expect("all-OLD certificate cannot reinterpret constraint-only peer as a vote");
    assert!(
        error
            .to_string()
            .contains("remote OLD peer closure unknown; not a zero/absence vote"),
        "{error}"
    );
    journal.push(
        serde_json::json!({"operation":"remote_old_all_closed_collector_control",
        "actual_donor":donor,"constraint_recipient":recipient,"exact_error":error.to_string(),
        "healthy_constraint_peer_not_known_pending":true,"all_closed_required":true,
        "source_image_captured":false,"target_permission":false}),
    );
    evidence(&output.join("remote-old-ledger.json"), journal).unwrap();
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &mut NativeMetadataControlHost,
    target_runtime: &Arc<crate::distribution_range_recovery::RangeRecoveryRuntime>,
    target_service: &Arc<Mutex<crate::ClusterDataNodeService>>,
    target_metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    ledger: &[serde_json::Value],
    output: &std::path::Path,
    original_writers: &[Arc<crate::db::InstalledNativeWriterHostGuard>],
    process: &CancellationToken,
) {
    for source in controls {
        source.bind_initial_image_source_process(process).unwrap();
    }
    let target_before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let mut journal = Vec::new();
    let mut retained_sources = Vec::new();
    let mut finished_ranges = std::collections::BTreeSet::new();
    let mut last_manifest = None;
    let mut retained_runtimes = Vec::new();
    let mut retained_remote_facts: Vec<Arc<NativeRemoteImageFact>> = Vec::new();
    for (original, acknowledged) in ranges.iter().zip(ledger) {
        let range = pending.range_by_id(original.id).unwrap();
        let relocation = pending
            .relocations
            .values()
            .find(|p| p.range_id == range.id)
            .unwrap();
        let leader_index = (0..4).find(|i| node(*i) == range.leader).unwrap();
        let actor = clients[leader_index].local_metadata_read_participant();
        let business = Uuid::now_v7();
        let parent = budget(5000);
        let observation = clients[leader_index]
            .acquire_metadata_read_observation(
                &node(0),
                business,
                range.id,
                range.epoch,
                Duration::from_secs(5),
                &parent,
            )
            .unwrap();
        let mut round = NativeJointOldRound::begin(observation, business, relocation.id).unwrap();
        let plan = round.plan();
        if matches!(case, SourceCase::RemoteConstraintCapture) {
            drop(round);
            constraint_capture_control(
                controls,
                clients,
                services,
                &plan,
                pending,
                ranges,
                output,
                &mut journal,
            );
            for (i, original_guard) in original_writers.iter().enumerate() {
                assert!(Arc::ptr_eq(
                    original_guard,
                    &controls[i]
                        .test_retained_initial_data_writer_guard()
                        .unwrap()
                ));
                original_guard
                    .require_current(&services[i].lock().database().read(), original)
                    .unwrap();
            }
            assert_eq!(
                state(&factory.database, pending, &node(4), ranges).unwrap(),
                target_before
            );
            continue;
        }
        let mut runtimes = Vec::new();
        let (source, retained, original_old_expiry) = if uses_actual_remote_old_seed(case) {
            // The metadata round supplied purpose DATA only. It never records
            // local service observations and never finishes a local OLD QQ.
            drop(round);
            journal.push(serde_json::json!({
                "operation":"source_seed_selection", "case":format!("{case:?}"),
                "changed_seed":"actual_production_remote_old", "range_id":range.id,
                "plan_sha256":plan.checksum_sha256().unwrap(),
                "local_old_issuance_contract_unchanged":true,
                "target_row_fault_not_yet_exercised":true,
            }));
            evidence(&output.join("remote-old-ledger.json"), &journal).unwrap();
            let seed = remote_old_seed(
                case,
                controls,
                clients,
                services,
                metadata,
                &plan,
                process,
                output,
                &mut journal,
            );
            let Some(seed) = seed else {
                assert_eq!(
                    state(&factory.database, pending, &node(4), ranges).unwrap(),
                    target_before
                );
                return;
            };
            seed
        } else {
            let old_indices = plan
                .old_members()
                .keys()
                .map(|id| (0..4).find(|i| node(*i) == *id).unwrap())
                .collect::<Vec<_>>();
            let mut providers = Vec::new();
            for &i in &old_indices {
                runtimes.push(Arc::new(
                    NativeJointHandoffRuntime::new(services[i].clone()).unwrap(),
                ));
                providers.push(
                    MetadataReadProvider::new(metadata[i].clone(), clients[i].clone()).unwrap(),
                );
            }
            for ((&i, runtime), provider) in old_indices.iter().zip(&runtimes).zip(&providers) {
                let operation = budget(5000);
                let constraints = runtime
                    .observe_constraints(
                        plan.clone(),
                        actual_role(provider, &actor, &plan, &node(i), &operation),
                        &operation,
                    )
                    .unwrap();
                round.record_constraints(constraints, &services[i]).unwrap();
            }
            if original.id == ranges[0].id && !matches!(case, SourceCase::CancelAfterIssuedFrame) {
                let i = old_indices[0];
                sink_control(
                    matches!(case, SourceCase::RevokeBeforeNativeWal),
                    i,
                    controls,
                    &runtimes[0],
                    &providers[0],
                    &actor,
                    &plan,
                    clients,
                    services,
                    pending,
                    ranges,
                    output,
                );
                if matches!(case, SourceCase::RevokeBeforeNativeWal) {
                    assert_eq!(
                        state(&factory.database, pending, &node(4), ranges).unwrap(),
                        target_before
                    );
                    return;
                }
                // A recorded failed operation has produced only an actual raw
                // refusal. Begin a DISTINCT fresh collector, not a renewal of any
                // counted/issued OLD quorum (the former round counted none).
                drop(round);
                let next_parent = budget(5000);
                let next_business = Uuid::now_v7();
                let next = clients[leader_index]
                    .acquire_metadata_read_observation(
                        &node(0),
                        next_business,
                        range.id,
                        range.epoch,
                        Duration::from_secs(5),
                        &next_parent,
                    )
                    .unwrap();
                round = NativeJointOldRound::begin(next, next_business, relocation.id).unwrap();
                assert_eq!(
                    round.plan().checksum_sha256().unwrap(),
                    plan.checksum_sha256().unwrap()
                );
                for ((&i, runtime), provider) in old_indices.iter().zip(&runtimes).zip(&providers) {
                    let operation = budget(5000);
                    let constraints = runtime
                        .observe_constraints(
                            plan.clone(),
                            actual_role(provider, &actor, &plan, &node(i), &operation),
                            &operation,
                        )
                        .unwrap();
                    round.record_constraints(constraints, &services[i]).unwrap();
                }
            }
            for ((&i, runtime), provider) in old_indices.iter().zip(&runtimes).zip(&providers) {
                let operation = budget(5000);
                let actual_role = actual_role(provider, &actor, &plan, &node(i), &operation);
                let own_business = Uuid::now_v7();
                let own = clients[i]
                    .acquire_host_metadata_observation(
                        &node(0),
                        own_business,
                        Duration::from_secs(5),
                        &operation,
                    )
                    .unwrap();
                let closed = controls[i]
                    .close_initial_old_step(
                        runtime.clone(),
                        Arc::new(actual_role),
                        own,
                        own_business,
                        &operation,
                    )
                    .unwrap();
                round.record_retained_closed_actual(closed).unwrap();
            }
            let closed = round.finish().unwrap();
            let original_old_expiry = closed.original_issuance_deadline();
            let source = (0..4)
                .find(|i| node(*i) == *closed.selected_source_id())
                .unwrap();
            {
                let original_request = budget(5000);
                let provider =
                    MetadataReadProvider::new(metadata[source].clone(), clients[source].clone())
                        .unwrap();
                let original_role =
                    actual_role(&provider, &actor, &plan, &node(source), &original_request);
                let foreign_business = Uuid::now_v7();
                let foreign_own = target
                    .acquire_host_metadata_observation(
                        &node(0),
                        foreign_business,
                        Duration::from_secs(5),
                        &original_request,
                    )
                    .unwrap();
                let error = NativeJointHandoffPlan::from_actual_role_and_own(
                    &original_role,
                    &foreign_own,
                    foreign_business,
                )
                .unwrap_err();
                original_request.check().unwrap();
                original_role.require_live_request().unwrap();
                foreign_own.require_live_request(foreign_business).unwrap();
                assert!(
                    error
                        .to_string()
                        .contains("OWN metadata differs from original recovery role"),
                    "{error}"
                );
            }
            let retain_request = budget(5000);
            let mut old = Some(closed);
            let retained = controls[source]
                .retain_initial_remote_image_source(&mut old, Uuid::now_v7(), &retain_request)
                .unwrap();
            assert!(old.is_none());
            (source, retained, original_old_expiry)
        };
        let publish_parent = budget(5000);
        let publish_business = Uuid::now_v7();
        let own = clients[source]
            .acquire_host_metadata_observation(
                &node(0),
                publish_business,
                Duration::from_secs(5),
                &publish_parent,
            )
            .unwrap();
        let manifest = retained
            .publish(&own, publish_business, &publish_parent)
            .unwrap();
        assert_eq!(manifest.source_id, node(source));
        assert_eq!(manifest.target_id, node(4));
        assert_eq!(manifest.target_birth, *factory.actual_birth());
        // The original finite acquisitions REALLY end. Keep only actual
        // hosted-process/session ownership; each following TLS step acquires
        // its own fresh OWN context under a distinct original bounded caller.
        parent.cancel();
        publish_parent.cancel();
        while Instant::now() <= original_old_expiry {
            process.check().unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(plan.require_live().is_err());
        assert!(matches!(parent.check(), Err(BicDbError::QueryCanceled)));
        assert!(matches!(
            publish_parent.check(),
            Err(BicDbError::QueryCanceled)
        ));
        process.check().unwrap();
        assert_eq!(manifest.range_index, 1);
        assert_ne!(manifest.source_reserved_b, 0);
        let actual = frame(target, &manifest, Op::Manifest, output, &mut journal).unwrap();
        assert!(matches!(&actual.reply().payload, Payload::Manifest(_)));
        if matches!(case, SourceCase::CancelAfterIssuedFrame) {
            let issued = serde_json::to_vec(actual.reply()).unwrap();
            retained.request_cancel();
            actual.require_request_live().unwrap();
            assert_eq!(serde_json::to_vec(actual.reply()).unwrap(), issued);
            let refused = frame(target, &manifest, Op::Manifest, output, &mut journal)
                .err()
                .expect("canceled source session must not issue another frame");
            assert!(
                refused
                    .to_string()
                    .contains(&BicDbError::QueryCanceled.to_string()),
                "{refused}"
            );
        } else {
            let remote_fact =
                Arc::new(NativeRemoteImageFact::from_actual_manifest(actual).unwrap());
            assert_eq!(
                remote_fact.manifest().checksum_sha256,
                manifest.checksum_sha256
            );
            if matches!(
                case,
                SourceCase::TargetFinalImage
                    | SourceCase::TargetFinalImageCanceled
                    | SourceCase::TargetFinalImageCorrupt
                    | SourceCase::TargetFinalImageWithoutScan
                    | SourceCase::TargetFinalImageHistorical
                    | SourceCase::TargetNativeBase
                    | SourceCase::TargetNativeBaseCanceled
                    | SourceCase::TargetNativeBaseInvalidated
                    | SourceCase::TargetNativeBaseWithoutFinal
                    | SourceCase::TargetNativeBaseRoleExpiryBeforePut
                    | SourceCase::TargetNativeBaseRoleExpiryAfterCommit
                    | SourceCase::TargetCatalogueFloor
                    | SourceCase::TargetCatalogueFloorMissingBase
                    | SourceCase::TargetCatalogueFloorDirSyncRetry
                    | SourceCase::TargetCatalogueFloorCanceled
            ) {
                target_final_image_tests::exercise(
                    case,
                    target_control,
                    target,
                    remote_fact.clone(),
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    acknowledged,
                    output,
                    &mut journal,
                );
                return;
            }
            if matches!(
                case,
                SourceCase::TargetImageRows
                    | SourceCase::TargetImageRowsWithoutAudit
                    | SourceCase::TargetImageRowsPreWalCanceled
                    | SourceCase::TargetImageRowsPostWalCanceled
                    | SourceCase::TargetImageRowsApplyFailed
            ) {
                target_image_tests::exercise(
                    case,
                    target_control,
                    target,
                    remote_fact.clone(),
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    acknowledged,
                    output,
                    &mut journal,
                );
                return;
            }
            let mut history = RangeImageHistoryCursorV1::default();
            loop {
                let actual = frame(
                    target,
                    &manifest,
                    Op::History {
                        cursor: history.clone(),
                        max_entries: 256,
                    },
                    output,
                    &mut journal,
                )
                .unwrap();
                let actual = remote_fact.bind_step(actual).unwrap();
                actual.require_request_live().unwrap();
                assert_eq!(
                    actual.source().manifest().checksum_sha256,
                    manifest.checksum_sha256
                );
                let Payload::History(step) = actual.payload().unwrap() else {
                    panic!("actual history reply expected")
                };
                history = step.next_cursor.clone();
                if history.complete() {
                    break;
                }
            }
            let mut cursor = RangeImageCursorV1::default();
            let mut records = Vec::new();
            loop {
                let actual = frame(
                    target,
                    &manifest,
                    Op::Rows {
                        cursor: cursor.clone(),
                        options: Default::default(),
                    },
                    output,
                    &mut journal,
                )
                .unwrap();
                let actual = remote_fact.bind_step(actual).unwrap();
                let Payload::Rows(step) = actual.payload().unwrap() else {
                    panic!("actual rows reply expected")
                };
                // Genuine retry with its own bounded request must return the
                // same immutable inner step, never a different snapshot cut.
                let duplicate = frame(
                    target,
                    &manifest,
                    Op::Rows {
                        cursor: cursor.clone(),
                        options: Default::default(),
                    },
                    output,
                    &mut journal,
                )
                .unwrap();
                let Payload::Rows(replayed) = &duplicate.reply().payload else {
                    panic!("actual duplicate rows expected")
                };
                assert_eq!(step, replayed);
                if let Some(batch) = &step.batch {
                    records.extend(
                        batch
                            .records
                            .iter()
                            .map(|r| serde_json::to_value(r).unwrap()),
                    );
                }
                cursor = step.next_cursor.clone();
                // This is another real authenticated reply, never a forged
                // DTO. It cannot start a Manifest fact; for range two it also
                // cannot be rebound to range one's retained issued fact.
                if let Some(previous) = retained_remote_facts.last() {
                    let error = previous.bind_step(duplicate).err().expect(
                        "actual other-range frame must not bind to first issued source cut",
                    );
                    assert!(
                        error
                            .to_string()
                            .contains("differs from actual issued Manifest/session"),
                        "{error}"
                    );
                } else {
                    let error = NativeRemoteImageFact::from_actual_manifest(duplicate)
                        .err()
                        .expect("real Rows response cannot construct a Manifest fact");
                    assert!(
                        error
                            .to_string()
                            .contains("requires an actual Manifest response"),
                        "{error}"
                    );
                }
                if step.complete {
                    break;
                }
            }
            assert!(
                records.contains(&acknowledged["actual_full_record"]),
                "full acknowledged value must actually cross TLS"
            );
            let actual = frame(target, &manifest, Op::Finish, output, &mut journal).unwrap();
            let actual = remote_fact.bind_step(actual).unwrap();
            let Payload::Finished(seal) = actual.payload().unwrap() else {
                panic!("real complete source seal expected")
            };
            assert_eq!(seal.record_count, records.len() as u64);
            assert_eq!(seal.range_index, 1);
            assert_eq!(seal.history_entries, 2);
            finished_ranges.insert(original.id);
            if matches!(
                case,
                SourceCase::TargetInstalling
                    | SourceCase::TargetInstallingPostCanceled
                    | SourceCase::TargetInstallingDirSyncRetry
            ) {
                target_installing_tests::exercise(
                    case,
                    target_control,
                    target,
                    remote_fact.clone(),
                    &actual,
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    output,
                );
                return;
            }
            if matches!(
                case,
                SourceCase::TargetProposal
                    | SourceCase::TargetProposalPostCanceled
                    | SourceCase::TargetProposalBeforeCanceled
                    | SourceCase::TargetProposalBeforeHookRevoked
                    | SourceCase::TargetProposalReaderFenceRetry
            ) {
                target_proposal_tests::exercise(
                    case,
                    target_control,
                    target,
                    remote_fact.clone(),
                    &actual,
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    output,
                );
                return;
            }
            if matches!(
                case,
                SourceCase::TargetTransfer
                    | SourceCase::TargetTransferBusy
                    | SourceCase::TargetTransferCanceled
                    | SourceCase::TargetTransferHookRevoked
            ) {
                target_transfer_tests::exercise(
                    case,
                    target_control,
                    target_runtime,
                    target_service,
                    target_metadata,
                    target,
                    remote_fact.clone(),
                    &actual,
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    output,
                );
                // These source tests establish actual ownership only. A range
                // now owns A, so the next source-only loop cannot pretend N.
                return;
            }
            if matches!(
                case,
                SourceCase::TargetNoDomain
                    | SourceCase::TargetNoDomainHookRevoked
                    | SourceCase::TargetNoDomainMemberChanged
                    | SourceCase::TargetNoDomainSchemaRevoked
            ) {
                target_no_domain_tests::exercise(
                    case,
                    target_control,
                    target_runtime,
                    target_service,
                    target_metadata,
                    target,
                    &actual,
                    &plan,
                    &MetadataReadProvider::new(target_metadata.clone(), target.clone()).unwrap(),
                    &actor,
                    factory,
                    pending,
                    ranges,
                    output,
                );
                if !matches!(case, SourceCase::TargetNoDomain) {
                    return;
                }
            }
            drop(actual);
            retained_remote_facts.push(remote_fact);
        }
        for (i, original_guard) in original_writers.iter().enumerate() {
            assert!(Arc::ptr_eq(
                original_guard,
                &controls[i]
                    .test_retained_initial_data_writer_guard()
                    .unwrap()
            ));
            original_guard
                .require_current(&services[i].lock().database().read(), original)
                .unwrap();
        }
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            target_before,
            "source historical frame MUST NOT install rows or grant a target policy"
        );
        last_manifest = Some(manifest);
        retained_sources.push(retained);
        retained_runtimes.extend(runtimes);
    }
    if matches!(case, SourceCase::RemoteConstraintCapture) {
        evidence(&output.join("source-constraints-result.json"), &serde_json::json!({
            "actual_two_range_constraint_captures":true,"actual_pinned_independent_observe":true,
            "actual_closed_majority_not_all_closed":true,"all_old_collector_refused":true,
            "healthy_constraint_peer_not_known_pending":true,"known_pending_qualified":false,
            "source_image_captured":false,"serving_granted":false,"target_before":target_before,
            "target_after":state(&factory.database,pending,&node(4),ranges).unwrap(),
        })).unwrap();
        return;
    }
    if matches!(
        case,
        SourceCase::Frames | SourceCase::RemoteOldFrames | SourceCase::RemoteOldRetainedNative
    ) {
        let manifest = last_manifest.as_ref().unwrap();
        // This session remains healthy immediately before ACTUAL hosted
        // process cancellation; a per-session cancel cannot masquerade here.
        frame(target, manifest, Op::Manifest, output, &mut journal).unwrap();
        process.check().unwrap();
        process.cancel();
        let refused = frame(target, manifest, Op::Manifest, output, &mut journal)
            .err()
            .expect("actual original hosted process cancellation must close fresh source frames");
        assert!(
            refused
                .to_string()
                .contains(&BicDbError::QueryCanceled.to_string()),
            "{refused}"
        );
        assert_eq!(
            state(&factory.database, pending, &node(4), ranges).unwrap(),
            target_before
        );
    }
    assert_eq!(retained_sources.len(), 2);
    assert!(!factory.database.read().commit_admission_ready());
    let target_after = state(&factory.database, pending, &node(4), ranges).unwrap();
    assert_eq!(
        target_after, target_before,
        "source streaming changed actual target state"
    );
    evidence(&output.join("source-frames-result.json"), &serde_json::json!({
        "actual_two_range_manifest_observations":true,
        "actual_two_range_history_rows_seals":finished_ranges.len()==2,
        "actual_finished_range_ids":finished_ranges,
        "actual_session_cancel_after_issued_manifest":matches!(case,SourceCase::CancelAfterIssuedFrame),
        "actual_hosted_process_cancel_after_healthy_frames":matches!(case,SourceCase::Frames | SourceCase::RemoteOldFrames | SourceCase::RemoteOldRetainedNative),
        "original_finite_old_and_publish_acquisitions_ended":true,
        "source_seed":if uses_actual_remote_old_seed(case){"actual node-local CLOSE RPC then donor-fetched pinned TLS historical OLD; ALL-old component"}else{"genuine local native OLD QQ, NOT remote collector"},
        "actual_fifth_target_unchanged":true,"target_rows_installed":false,"serving_granted":false,
        "target_before":target_before,"target_after":target_after,"actual_frame_count":journal.iter().filter(|r|r["operation"]=="source_image_frame").count(),
        "actual_remote_old_close_logical_calls":journal.iter().filter(|r|r["operation"]=="remote_old_close").count(),
        "actual_remote_old_close_retry_logical_calls":journal.iter().filter(|r|r["operation"]=="remote_old_close_retry").count(),
        "actual_post_native_cancel_and_exact_fresh_retry":matches!(case,SourceCase::RemoteOldRetainedNative),
        "original_four_writer_objects_retained":true,
        "real_target_namespaces":factory.database.read().capture_range_image_bindings().unwrap().namespace_ids(),
    })).unwrap();
}
mod target_final_image_tests;
mod target_image_tests;
mod target_installing_tests;
mod target_no_domain_tests;
mod target_proposal_tests;
mod target_transfer_tests;
