//! Replacement source under a higher data epoch follows the distinct existing-decision quorum. Every authority
//! is produced from actual protected native data, fresh metadata witnesses,
//! a DISTINCT mixed quorum and full actual source history; no intent is cast.
use super::copied_full_image_case::recipient_role;
use super::*;
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeCurrentImageProposalPointer, NativeImageInstallRole,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    materialize_actual_current_repair_image, NativeImageInstallFreeze,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn finish_actual_current_image_replacement(
    fixture: &ActualNativeRf3Fixture,
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: &BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    initiator: &MetadataReadParticipant,
    range: &RangeDescriptor,
    scope: &RecoveryFenceScope,
    proposal: NativeCurrentImageProposalPointer,
    previously_unknown: &crate::distribution_range_consensus::RangeWriteCommand,
    acknowledged: &str,
    acknowledged_sha: &str,
    target_uid: Uuid,
    expected_h: &str,
    parent: &CancellationToken,
    handover_cut: Option<bool>,
) -> serde_json::Value {
    let source_id = node("n2");
    let target_id = node("n1");
    let mut quorum = InstallingCommittedQuorumRound::begin(
        fresh_role(providers, initiator, range, scope.recovery_nonce),
        target_id.clone(),
        proposal.clone(),
        parent,
    )
    .unwrap();
    quorum
        .record_ordinary(
            runtimes[&source_id]
                .fence_epoch(
                    initiator,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 5000,
                    },
                    parent,
                )
                .unwrap(),
        )
        .unwrap();
    let installing_request = InstallingCommittedRequest {
        scope: scope.clone(),
        proposal: proposal.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    };
    let installing = runtimes[&target_id]
        .acquire_installing_committed_fence(initiator, &installing_request, parent)
        .unwrap();
    assert_eq!(installing.evidence().progress.last_index, 0);
    assert_eq!(
        installing
            .evidence()
            .preimage
            .native_point
            .as_ref()
            .unwrap()
            .range_index,
        0
    );
    assert_eq!(
        installing.evidence().irreversible_commands.get(&2),
        Some(&previously_unknown.checksum_sha256)
    );
    let observed_native = installing.evidence().native.clone();
    quorum.record_installing(installing).unwrap();
    // A second actual reply from the same receiver is still ONE data voter.
    let duplicate = runtimes[&target_id]
        .acquire_installing_committed_fence(initiator, &installing_request, parent)
        .unwrap();
    assert!(quorum
        .record_installing(duplicate)
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
    let authority = Arc::new(quorum.finish().unwrap());
    assert_eq!(authority.votes().len(), 2);
    let frozen = runtimes[&source_id].source_capture_freeze(scope).unwrap();
    let source = fixture.services[&source_id]
        .lock()
        .attest_native_recovery_ready(
            fresh_role(providers, initiator, range, scope.recovery_nonce),
            Arc::clone(&frozen),
            parent,
        )
        .unwrap();
    assert_eq!(source.range_index(), 2);
    assert_eq!(source.logical_history_sha256(), expected_h);
    let source_service = &fixture.services[&source_id];
    let mut history = source_service
        .lock()
        .begin_current_image_replacement_history(authority, &source)
        .unwrap();
    let mut complete = false;
    for _ in 0..16 {
        if source_service
            .lock()
            .advance_current_image_replacement_history(&mut history, &source, parent)
            .unwrap()
        {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "fresh mixed round must verify all history0..2 within ORIGINAL deadline"
    );
    let cut = source_service
        .lock()
        .finish_current_image_replacement_history(history, &source)
        .unwrap();
    let source_db = Arc::clone(&fixture.databases[&source_id]);
    let lineage = source_db
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    drop(source);
    let mut capture = Some(
        RangeImageCapture::begin(
            source_db,
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            parent,
        )
        .unwrap(),
    );
    let source = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    assert_eq!(source.cut().resume_proposal(), Some(&proposal));
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let attempt = Uuid::now_v7();
    if let Some(before_catalogue) = handover_cut {
        let target_db = &fixture.databases[&target_id];
        let bytes = std::fs::read(
            target_db
                .read()
                .data_path()
                .join("cluster-range-write-log.json"),
        )
        .unwrap();
        let result = runtimes[&target_id].test_replacement_handover_cut(
            scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            attempt,
            before_catalogue,
            parent,
        );
        if before_catalogue {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("BEFORE catalogue handover"));
            assert_eq!(
                std::fs::read(
                    target_db
                        .read()
                        .data_path()
                        .join("cluster-range-write-log.json")
                )
                .unwrap(),
                bytes
            );
        } else {
            drop(result.unwrap());
        }
        let db = target_db.read();
        let native = db
            .native_recovery_admission(
                role.cluster_id(),
                role.bootstrap_id(),
                role.local_node_id(),
                role.local_member_incarnation(),
            )
            .unwrap();
        let p2 = db
            .read_native_current_image_proposal_by_id(&native, attempt)
            .unwrap()
            .unwrap();
        let p1 = db
            .read_native_current_image_proposal(&native, &proposal)
            .unwrap();
        assert_eq!(p1.pointer(), proposal);
        assert_eq!(p1.historical_source().source_id, node("n0"));
        assert_eq!(p2.historical_source().source_id, source_id);
        assert_eq!(p2.historical_source().range.epoch, range.epoch);
        assert_eq!(
            p1.historical_source().logical_history_sha256,
            p2.historical_source().logical_history_sha256
        );
        assert_eq!(
            db.native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            None
        );
        assert!(db.range_mutation_gate().try_enter(range.id).is_err());
        return serde_json::json!({"handover_cut":if before_catalogue{"NATIVE_P2_ORPHAN_BEFORE_CATALOGUE"}else{"DURABLE_N2_INTENT_P2_BEFORE_TRANSFER"},"attempt":attempt,"p1":proposal,"p2":p2.pointer(),"ordinary_mutation_refused":true,"native_unknown":true});
    }
    let lease = runtimes[&target_id]
        .transfer_current_image_replacement(
            scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            attempt,
            parent,
        )
        .unwrap();
    let target_service = &fixture.services[&target_id];
    let previous_stage_sha = fixture.databases[&target_id]
        .read()
        .native_history_stage_state_commitment(range.id)
        .unwrap();
    let install = target_service
        .lock()
        .begin_native_range_image_install(
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            lease,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            parent,
        )
        .unwrap();
    if let Some(previous_sha) = previous_stage_sha {
        let archive = fixture.databases[&target_id]
            .read()
            .test_native_history_substitution_archive(range.id)
            .unwrap();
        assert_eq!(
            archive["previous_header_sha256"], previous_sha,
            "substitution must retain EXACT previous received/audited stage"
        );
        if archive["previous_verified_entries"] == 0 {
            assert_eq!(archive["previous_target_point"][0], 0,
                "the actual fresh target's native Genesis commitment must survive InstallingUnknown");
            assert_eq!(
                archive["previous_target_point_matched"], false,
                "no history entry was received to match the old native point before source death"
            );
        }
        assert_eq!(archive["previous_history_sha256"], expected_h);
        assert_eq!(archive["current_history_sha256"], expected_h);
        assert!(
            archive["previous_source_range_index"].as_u64().unwrap()
                <= archive["current_source_range_index"].as_u64().unwrap()
        );
        eprintln!("ACTUAL_CURRENT_QUORUM_HISTORY_SUBSTITUTION {archive}");
    }
    drop(source);
    drop(role);
    let verified = materialize_actual_current_repair_image(
        &mut capture,
        target_service,
        || recipient_role(providers, initiator, range, scope.recovery_nonce),
        || {
            let obligations = target_service
                .lock()
                .test_image_history_obligations(range.id)
                .unwrap();
            assert_eq!(
                obligations.irrevocable_commands().get(&2),
                Some(&previously_unknown.checksum_sha256)
            );
        },
        install,
    );
    assert_eq!(verified.base().range_index(), 2);
    assert_eq!(verified.base().logical_history_sha256(), expected_h);
    let target_b = verified.base().target_snapshot_bound().get();
    assert_ne!(
        target_b, 2,
        "actual local MVCC bound must not be replaced with rangeR2"
    );
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let adopted = target_service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let (freeze, finalization) = target_service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual resumed Runtime must retain owned lease")
    };
    let role = recipient_role(providers, initiator, range, scope.recovery_nonce);
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            parent,
        )
        .unwrap();
    let frozen = runtimes[&target_id].source_capture_freeze(scope).unwrap();
    let prepared = target_service
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            recipient_role(providers, initiator, range, scope.recovery_nonce),
            Arc::clone(&frozen),
            parent,
        )
        .unwrap();
    assert_eq!(prepared.range_index(), 2);
    assert_eq!(prepared.logical_history_sha256(), expected_h);
    assert_eq!(prepared.local_visibility_sequence(), target_b);
    let db = &fixture.databases[&target_id];
    assert_eq!(
        db.read().local_namespace_identity("items").unwrap(),
        Some(target_uid)
    );
    let actual = db.read().get("items", acknowledged).unwrap().unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(serde_json::to_vec(&actual).unwrap())),
        acknowledged_sha
    );
    for mutation in &previously_unknown.mutations {
        assert_eq!(
            db.read()
                .get(&mutation.collection, &mutation.record_id)
                .unwrap()
                .as_deref(),
            mutation.record.as_ref()
        );
    }
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("controller-acknowledged.jsonl"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    drop(prepared);
    drop(frozen);
    let ready = runtimes[&target_id]
        .prepare_native_ready(
            initiator,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            parent,
        )
        .unwrap();
    assert_eq!(ready.reply().range_index, 2);
    assert_eq!(ready.reply().local_visibility_sequence, target_b);
    assert!(db.read().range_mutation_gate().try_enter(range.id).is_err());
    let final_catalogue: serde_json::Value = serde_json::from_slice(
        &std::fs::read(db.read().data_path().join("cluster-range-write-log.json")).unwrap(),
    )
    .unwrap();
    let range_key = range.id.get().to_string();
    assert!(final_catalogue["catalog"]["image_install_intents"]
        .get(&range_key)
        .is_none());
    assert_eq!(
        final_catalogue["catalog"]["native_image_bases"][&range_key]["range_index"],
        2
    );
    assert_eq!(
        final_catalogue["catalog"]["recovery_fences"][&range_key]["phase"]["NativePrepared"]
            ["range_index"],
        2
    );
    serde_json::json!({"result":"ACTUAL_NATIVE_PREPARED_READY_RECOVERED","target_range_index":2,
        "target_actual_local_visibility":target_b,"logical_history_sha256":expected_h,"target_uid":target_uid,
        "installing_native_observation_honest":observed_native,"protected_parent":proposal,
        "new_attempt":attempt,"distinct_current_voters":2,"duplicate_actual_reply_rejected":true,
        "acknowledged_values_reconciled_on_target":1,"unknown2_resolved_but_not_new_ack":true,
        "fresh_mixed_qq_and_full_history":true,"ordinary_no_image_relaxed":false,
        "final_snapshot_phase":"NATIVE_PREPARED_READY_STILL_FROZEN",
        "final_exact_image_intent_removed":true,"final_native_base_published":true,
        "activation_proven":false,"new_application_ack_tested":false,"physical_qualification":false,"strong_get_tested":false})
}
