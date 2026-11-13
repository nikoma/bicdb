//! Source-wire prerequisite only. Genuine LOCAL native OLD QQ seeds the real
//! source owner; target is the real fifth, but NO image row/Tx/install grant.
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

#[derive(Clone, Copy)]
pub(super) enum SourceCase {
    Frames,
    CancelAfterIssuedFrame,
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
pub(super) fn exercise(
    case: SourceCase,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    ledger: &[serde_json::Value],
    output: &std::path::Path,
    original_writers: &[Arc<crate::db::InstalledNativeWriterHostGuard>],
) {
    let target_before = state(&factory.database, pending, &node(4), ranges).unwrap();
    let mut journal = Vec::new();
    let mut retained_sources = Vec::new();
    let mut retained_runtimes = Vec::new();
    let mut retained_remote_facts: Vec<NativeRemoteImageFact> = Vec::new();
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
        let old_indices = plan
            .old_members()
            .keys()
            .map(|id| (0..4).find(|i| node(*i) == *id).unwrap())
            .collect::<Vec<_>>();
        let mut runtimes = Vec::new();
        let mut providers = Vec::new();
        for &i in &old_indices {
            runtimes.push(NativeJointHandoffRuntime::new(services[i].clone()).unwrap());
            providers
                .push(MetadataReadProvider::new(metadata[i].clone(), clients[i].clone()).unwrap());
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
            let closed = controls[i].with_initial_old_step(Arc::new(actual_role), own, own_business,
                &operation, |step| {
                    assert_eq!(step.plan().checksum_sha256().unwrap(), plan.checksum_sha256().unwrap(),
                        "paired actual recipient role/OWN plan must match actual controller plan");
                    runtime.close_old_for_initial_step(step, &operation)
                }).unwrap();
            round.record_closed(closed).unwrap();
        }
        let closed = round.finish().unwrap();
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
        let source_session = CancellationToken::uncancelable();
        let mut old = Some(closed);
        let retained = controls[source]
            .retain_initial_remote_image_source(&mut old, Uuid::now_v7(), &source_session)
            .unwrap();
        assert!(old.is_none());
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
        assert_eq!(manifest.range_index, 1);
        assert_ne!(manifest.source_reserved_b, 0);
        let actual = frame(target, &manifest, Op::Manifest, output, &mut journal).unwrap();
        assert!(matches!(actual.reply().payload, Payload::Manifest(_)));
        if matches!(case, SourceCase::CancelAfterIssuedFrame) {
            let issued = serde_json::to_vec(actual.reply()).unwrap();
            source_session.cancel();
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
            let remote_fact = NativeRemoteImageFact::from_actual_manifest(actual).unwrap();
            assert_eq!(
                remote_fact.manifest().checksum_sha256,
                manifest.checksum_sha256
            );
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
        retained_sources.push(retained);
        retained_runtimes.extend(runtimes);
    }
    assert_eq!(retained_sources.len(), 2);
    assert!(!factory.database.read().commit_admission_ready());
    evidence(&output.join("source-frames-result.json"), &serde_json::json!({
        "actual_two_range_source_streams":true,"source_seed":"genuine local native OLD QQ, NOT remote collector",
        "actual_fifth_target_unchanged":true,"target_rows_installed":false,"serving_granted":false,
        "target_before":target_before,"actual_frame_count":journal.len(),"original_four_writer_objects_retained":true,
        "real_target_namespaces":factory.database.read().capture_range_image_bindings().unwrap().namespace_ids(),
    })).unwrap();
}
