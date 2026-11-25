//! Coordinator-side remote decision certificate: the complete genuine pinned-TLS
//! round, the read-only durable re-collection, and the metadata stages that
//! accept ONLY the authenticated dual-majority proof.
use super::*;
use crate::distribution::{NativeJointHandoffStageV1, RangeReplicaRole, RelocationPhase};
use crate::distribution_range_handoff::{
    NativeJointDecisionProof, NativeRemoteJointDecisionCertificate,
    NativeRemoteJointDecisionCertificateRound,
};

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetRemoteDecisionCertificateStages
            | SourceCase::TargetRemoteDecisionCertificateRecollect
            | SourceCase::TargetRemoteDecisionCertificateRefusals
    )
}
#[test]
fn native_remote_decision_certificate_full_round_is_accepted_by_all_metadata_stages() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionCertificateStages),
    );
}
#[test]
fn native_remote_decision_certificate_readonly_observe_recollects_equal_certificate() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionCertificateRecollect),
    );
}
#[test]
fn native_remote_decision_certificate_refuses_wrong_peer_duplicate_checksum_and_missing_quorums() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetRemoteDecisionCertificateRefusals),
    );
}

fn observe(
    client: &TcpClusterRelocationTransport,
    recipient: &ClusterNodeId,
    mf: &NativeRemoteImageManifestV1,
    phase: Uuid,
) -> Result<AuthenticatedNativeRemoteNewFrame> {
    client.remote_joint_decision_exchange(
        &node(0),
        recipient,
        mf,
        phase,
        NewOp::ObserveDecision,
        &budget(5000),
    )
}
fn collect(
    client: &TcpClusterRelocationTransport,
    plan: &Arc<NativeJointHandoffPlan>,
    mf: &NativeRemoteImageManifestV1,
    phase: Uuid,
    members: &[ClusterNodeId],
) -> Result<NativeRemoteJointDecisionCertificate> {
    let parent = budget(20_000);
    let mut round =
        NativeRemoteJointDecisionCertificateRound::begin(plan.clone(), mf.clone(), phase, &parent)?;
    for id in members {
        round.record(observe(client, id, mf, phase)?)?;
    }
    round.finish()
}
fn commit_candidate(
    controls: &[NativeMetadataControlHost],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    clients: &[Arc<TcpClusterRelocationTransport>],
    candidate: ClusterTopology,
    parent: &CancellationToken,
) {
    let entry = metadata[0]
        .lock()
        .propose_topology(candidate.clone())
        .unwrap();
    controls[0].heartbeat(parent).unwrap();
    controls[0].heartbeat(parent).unwrap();
    for actual in metadata {
        let actual = actual.lock();
        assert!(actual.status().commit_index >= entry.index);
        assert_eq!(actual.committed_topology(), &candidate);
    }
    for client in clients {
        client.install_topology(candidate.clone()).unwrap();
    }
}
/// Build the leader's planning view of the fresh committed topology and apply
/// one membership stage from the PROOF (trait object, as production does).
fn stage_candidate(
    clients: &[Arc<TcpClusterRelocationTransport>],
    template: &crate::DistributionConfig,
    proof: &dyn NativeJointDecisionProof,
    stage: u8,
    parent: &CancellationToken,
) -> ClusterTopology {
    let (fresh, business) = own(&clients[0], parent);
    let current = fresh.committed_topology(business).unwrap().clone();
    let snapshot = crate::ClusterBootstrapSnapshot {
        topology: current,
        config_template: template.clone(),
        metadata_leader_id: Some(node(0)),
    };
    // Every candidate is built on its own planning view of the same fresh
    // committed configuration.
    let open_view = || {
        let root = tempfile::tempdir().unwrap();
        let config = crate::provision_cluster_member_directory(
            root.path(),
            &snapshot,
            &node(0),
            template.transport.clone(),
            true,
        )
        .unwrap();
        crate::DistributionStore::open(root.path(), config, true)
            .unwrap()
            .fork_ephemeral()
    };
    let relocation = proof.data().promise().relocation_id;
    let mut view = open_view();
    // The same stage from the committed record alone, with no proof.
    let mut committed = open_view();
    match stage {
        0 => {
            // Nothing is certified yet: the committed record cannot stand in
            // for the proof of the certification.
            for error in [
                committed
                    .promote_certified_native_joint_handoff_frozen(
                        relocation,
                        &fresh,
                        business,
                        &node(0),
                        301,
                    )
                    .unwrap_err(),
                committed
                    .finalize_certified_native_joint_membership_frozen(
                        relocation,
                        &fresh,
                        business,
                        &node(0),
                        301,
                    )
                    .unwrap_err(),
            ] {
                assert!(
                    error.to_string().contains("joint decision not committed"),
                    "{error}"
                );
            }
            assert_eq!(committed.topology(), &snapshot.topology);
            assert!(view
                .certify_native_joint_handoff(proof, &fresh, business, &node(0), 301)
                .unwrap());
            certified_scope_validators(view.topology(), relocation, proof);
        }
        1 => {
            // The final membership is not built on a merely certified record.
            let error = committed
                .finalize_certified_native_joint_membership_frozen(
                    relocation,
                    &fresh,
                    business,
                    &node(0),
                    302,
                )
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("requires its exact frozen union decision"),
                "{error}"
            );
            assert_eq!(committed.topology(), &snapshot.topology);
            let epoch = view
                .promote_native_joint_handoff_frozen(proof, &fresh, business, &node(0), 302)
                .unwrap();
            assert_eq!(
                committed
                    .promote_certified_native_joint_handoff_frozen(
                        relocation,
                        &fresh,
                        business,
                        &node(0),
                        302,
                    )
                    .unwrap(),
                epoch
            );
        }
        2 => {
            let error = committed
                .promote_certified_native_joint_handoff_frozen(
                    relocation,
                    &fresh,
                    business,
                    &node(0),
                    303,
                )
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("requires its exact certified decision"),
                "{error}"
            );
            assert_eq!(committed.topology(), &snapshot.topology);
            let epoch = view
                .finalize_native_joint_membership_frozen(proof, &fresh, business, &node(0), 303)
                .unwrap();
            assert_eq!(
                committed
                    .finalize_certified_native_joint_membership_frozen(
                        relocation,
                        &fresh,
                        business,
                        &node(0),
                        303,
                    )
                    .unwrap(),
                epoch
            );
        }
        _ => unreachable!(),
    }
    if stage != 0 {
        // The proof path and the committed-record path build the same
        // candidate, byte for byte.
        assert_eq!(committed.topology(), view.topology());
        assert_eq!(
            serde_json::to_vec(committed.topology()).unwrap(),
            serde_json::to_vec(view.topology()).unwrap()
        );
    }
    view.topology().clone()
}

/// The certified record carries exactly the proof's scope, and its
/// validators refuse a record whose scope is missing or was altered.
fn certified_scope_validators(
    certified: &ClusterTopology,
    relocation: crate::RelocationId,
    proof: &dyn NativeJointDecisionProof,
) {
    use crate::distribution_range_handoff::{
        NativeJointBridgeDecision, NativeJointCommittedDecision,
    };
    let plan = &certified.relocations[&relocation];
    let record = plan.native_joint_handoff_v1.as_ref().unwrap();
    let data = proof.data();
    let scope = record.certified_scope().expect("certified with its scope");
    assert_eq!(*scope, data.certified_scope());
    assert_eq!(scope.activation_range(), data.activation_range());
    assert_eq!(scope.old_range(), data.old_range());
    record.validate_for(plan).unwrap();
    certified.validate().unwrap();
    // The committed decision identity equals the decision body's.
    let committed = NativeJointCommittedDecision::from_committed(plan)
        .unwrap()
        .unwrap();
    assert_eq!(committed.checksum_sha256(), data.checksum_sha256());
    assert_eq!(
        committed.plan_sha256().unwrap(),
        data.plan_sha256().unwrap()
    );
    assert_eq!(
        NativeJointBridgeDecision::promise(&committed),
        data.promise()
    );
    assert_eq!(
        NativeJointBridgeDecision::new_members(&committed),
        data.new_members()
    );
    let json = serde_json::to_value(plan).unwrap();
    let refused = |name: &str, change: &dyn Fn(&mut serde_json::Value)| {
        let mut altered = json.clone();
        change(&mut altered["native_joint_handoff_v1"]);
        let Ok(altered) = serde_json::from_value::<crate::distribution::RangeRelocation>(altered)
        else {
            return;
        };
        let record = altered.native_joint_handoff_v1.as_ref().unwrap();
        assert!(
            record.validate_for(&altered).is_err(),
            "{name}: the record validator must refuse"
        );
        let mut next = certified.clone();
        next.relocations.insert(relocation, altered);
        assert!(next.validate().is_err(), "{name}: the topology must refuse");
        assert!(
            crate::distribution::test_validate_native_joint_handoff_preservation(certified, &next)
                .is_err(),
            "{name}: a later configuration may not change the certified record"
        );
    };
    assert!(json["native_joint_handoff_v1"]["certified_scope"]["activation_range"].is_object());
    refused("missing scope", &|record| {
        record.as_object_mut().unwrap().remove("certified_scope");
    });
    refused("activation epoch", &|record| {
        let epoch = &mut record["certified_scope"]["activation_range"]["epoch"];
        *epoch = serde_json::json!(epoch.as_u64().unwrap() + 1);
    });
    refused("activation leader", &|record| {
        let old = record["certified_scope"]["old_range"]["leader"].clone();
        let leader = &mut record["certified_scope"]["activation_range"]["leader"];
        assert_ne!(*leader, old, "the fixture retires the range leader");
        *leader = old;
    });
    refused("activation replica dropped", &|record| {
        record["certified_scope"]["activation_range"]["replicas"]
            .as_array_mut()
            .unwrap()
            .pop();
    });
    refused("another session", &|record| {
        record["certified_scope"]["promise"]["nonce"] =
            serde_json::json!(Uuid::now_v7().to_string());
    });
    refused("another new configuration", &|record| {
        record["certified_scope"]["promise"]["new_config_sha256"] =
            serde_json::json!("0".repeat(64));
    });
    refused("another target", &|record| {
        let source = record["certified_scope"]["old_source"].clone();
        record["certified_scope"]["target"] = source;
    });
    // Removing the scope together with the version it requires yields a
    // well-formed record of the earlier version; it is still not the record
    // that was certified.
    let mut legacy = json.clone();
    legacy["native_joint_handoff_v1"]
        .as_object_mut()
        .unwrap()
        .remove("certified_scope");
    legacy["native_joint_handoff_v1"]["version"] = serde_json::json!(1);
    let legacy: crate::distribution::RangeRelocation = serde_json::from_value(legacy).unwrap();
    legacy
        .native_joint_handoff_v1
        .as_ref()
        .unwrap()
        .validate_for(&legacy)
        .unwrap();
    assert!(NativeJointCommittedDecision::from_committed(&legacy)
        .unwrap()
        .is_none());
    let mut next = certified.clone();
    next.relocations.insert(relocation, legacy);
    assert!(
        crate::distribution::test_validate_native_joint_handoff_preservation(certified, &next)
            .is_err()
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    controls: &[NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    template: &crate::DistributionConfig,
    target: &NativeMetadataControlHost,
    mf: &NativeRemoteImageManifestV1,
    phase: Uuid,
    plan: &Arc<NativeJointHandoffPlan>,
    output: &std::path::Path,
) {
    let coordinator = &clients[0];
    let range = mf.capture_range.id;
    let mut members = mf.old_members.keys().cloned().collect::<Vec<_>>();
    members.push(mf.target_id.clone());
    let leader = mf.activation_range.leader.clone();
    let retiree = mf
        .old_members
        .keys()
        .find(|id| !mf.new_members.contains_key(*id))
        .unwrap()
        .clone();
    let mut survivors = mf
        .old_members
        .keys()
        .filter(|id| mf.new_members.contains_key(*id))
        .cloned()
        .collect::<Vec<_>>();
    // The new leader first, so every subset below keeps it.
    survivors.sort_by_key(|id| *id != leader);
    // RF3: OLD = retiree + two survivors, NEW = two survivors + target.
    assert_eq!(mf.old_members.len(), 3);
    assert_eq!(mf.new_members.len(), 3);
    assert_eq!(survivors.len(), 2);
    assert_eq!(
        leader, mf.target_id,
        "this fixture's new leader is the joining target"
    );

    // Read-only observation cannot create a participant or a decision.
    let premature = coordinator.recollect_remote_joint_decision_certificate(
        plan.clone(),
        mf,
        phase,
        &members,
        &node(0),
        &budget(20_000),
    );
    let premature_error = premature.as_ref().err().map(ToString::to_string);
    assert!(premature.is_err(), "no durable decision exists yet");

    let round_parent = budget(60_000);
    let started = Instant::now();
    let actual = coordinator.run_remote_joint_decision_round(
        plan.clone(),
        mf,
        phase,
        &members,
        &node(0),
        &round_parent,
    );
    let round_error = actual.as_ref().err().map(ToString::to_string);
    evidence(
        &output.join("remote-decision-certificate-round.json"),
        &serde_json::json!({
            "case":format!("{case:?}"),"members":members,"phase":phase,
            "premature_readonly_observation_error":premature_error,
            "actual_round_error":round_error,"elapsed_ms":started.elapsed().as_millis(),
            "publishers":actual.as_ref().ok().map(|c|c.publishers()),
            "decision":actual.as_ref().ok().map(|c|c.data().checksum_sha256().to_string()),
            "certificate":actual.as_ref().ok().and_then(|c|c.checksum_sha256().ok()),
        }),
    )
    .unwrap();
    let certificate = actual.unwrap();
    certificate.require_current().unwrap();
    assert_eq!(certificate.publishers().len(), members.len());
    for id in &members {
        assert!(certificate.publishers().contains(id));
    }
    let data = certificate.data();
    assert_eq!(data.plan_sha256().unwrap(), plan.checksum_sha256().unwrap());
    assert_eq!(data.old_range().id, range);
    assert_eq!(data.old_range().epoch, mf.capture_range.epoch);
    assert_eq!(data.range_index(), mf.range_index);
    assert_eq!(data.logical_history_sha256(), mf.h);
    assert_eq!(data.activation_range().leader, leader);
    let native = target.test_remote_native_decision_evidence(range).unwrap();
    let native_xid = native["native"]["actual_native_attempt"]["native_xid"]
        .as_u64()
        .unwrap();
    assert!(native_xid > 0);
    assert_eq!(
        native["native"]["actual_native_attempt"]["outcome"]["state"],
        "actual_commit_returned"
    );

    match case {
        SourceCase::TargetRemoteDecisionCertificateRecollect => {
            let again = coordinator
                .recollect_remote_joint_decision_certificate(
                    plan.clone(),
                    mf,
                    phase,
                    &members,
                    &node(0),
                    &budget(20_000),
                )
                .unwrap();
            assert_eq!(again.publishers(), certificate.publishers());
            assert_eq!(again.data(), certificate.data());
            assert_eq!(
                again.checksum_sha256().unwrap(),
                certificate.checksum_sha256().unwrap()
            );
            // The observation is read-only: same single native attempt/xid.
            let frame = observe(coordinator, &mf.target_id, mf, phase).unwrap();
            let receipt = frame.reply().native_decision.clone().unwrap();
            assert_eq!(receipt.node_id, mf.target_id);
            assert_eq!(receipt.native_page_xid, native_xid);
            assert_eq!(receipt.decision_sha256, data.checksum_sha256());
            assert_eq!(frame.actual_peer(), &mf.target_id);
            assert_eq!(
                frame.reply().native_decision_body.as_ref(),
                Some(certificate.data())
            );
            let after = target.test_remote_native_decision_evidence(range).unwrap();
            assert_eq!(after["native"], native["native"]);
            // The proof needs majorities, not every member: the retiree may
            // be unreachable for a restarted coordinator.
            let mut majority = survivors.clone();
            majority.push(mf.target_id.clone());
            let partial = coordinator
                .recollect_remote_joint_decision_certificate(
                    plan.clone(),
                    mf,
                    phase,
                    &majority,
                    &node(0),
                    &budget(20_000),
                )
                .unwrap();
            assert_eq!(partial.publishers(), {
                let mut expected = majority.clone();
                expected.sort();
                expected
            });
            assert_eq!(partial.data(), certificate.data());
            // Plain participant callbacks stay exactly as before: no receipt.
            let participant = (0..4).find(|i| node(*i) == survivors[0]).unwrap();
            let plain = clients[participant]
                .remote_joint_decision_exchange(
                    &node(0),
                    &mf.target_id,
                    mf,
                    phase,
                    NewOp::Observe,
                    &budget(5000),
                )
                .unwrap();
            assert!(plain.reply().native_decision.is_none());
            assert!(plain.reply().native_decision_body.is_none());
            let retained = |host: &NativeMetadataControlHost| {
                host.test_remote_new_phase_evidence(range).unwrap()["retained_requests"]
                    .as_u64()
                    .unwrap()
            };
            let before = retained(target);
            // This fixture's coordinator is outside the plan: every read
            // above was admitted only by the initiator rule (metadata
            // leader), evaluated by each recipient on its own observation.
            assert!(!mf.old_members.contains_key(&node(0)));
            assert!(!mf.new_members.contains_key(&node(0)));
            // Legitimate decision reads never consume the lifetime request
            // table: more reads than its capacity leave it unchanged ...
            for _ in 0..70 {
                let frame = observe(coordinator, &mf.target_id, mf, phase).unwrap();
                assert_eq!(
                    frame.reply().native_decision_body.as_ref(),
                    Some(certificate.data())
                );
            }
            assert_eq!(retained(target), before);
            assert!(before < 64);
            // ... and a following slot-retaining exchange is still admitted.
            clients[participant]
                .remote_joint_decision_exchange(
                    &node(0),
                    &mf.target_id,
                    mf,
                    phase,
                    NewOp::Observe,
                    &budget(5000),
                )
                .unwrap();
            // An admitted request takes one slot while its own budget runs;
            // slots of earlier requests whose budget ended are free again.
            assert!((1..=before + 1).contains(&retained(target)));
            let after = target.test_remote_native_decision_evidence(range).unwrap();
            assert_eq!(after["native"], native["native"]);
        }
        SourceCase::TargetRemoteDecisionCertificateRefusals => {
            let mut observations = Vec::new();
            let mut refuse = |name: &str, result: Result<()>, expected: &str| {
                let error = result.as_ref().err().map(ToString::to_string);
                observations.push(serde_json::json!({"case":name,"actual_error":error,
                    "expected_refusal":expected}));
                let error = error.unwrap_or_else(|| panic!("{name} must be refused"));
                assert!(error.contains(expected), "{name}: {error}");
            };
            let begin = || {
                NativeRemoteJointDecisionCertificateRound::begin(
                    plan.clone(),
                    mf.clone(),
                    phase,
                    &budget(20_000),
                )
                .unwrap()
            };
            // Authenticated peer A; decoded reply DATA claims member B.
            let a = &survivors[0];
            let b = &survivors[1];
            let other = observe(coordinator, b, mf, phase).unwrap();
            let forged = observe(coordinator, a, mf, phase)
                .unwrap()
                .test_replace_decoded_reply(other.reply().clone());
            assert_eq!(forged.actual_peer(), a);
            assert_eq!(&forged.reply().native_decision.as_ref().unwrap().node_id, b);
            refuse(
                "authenticated_peer_differs_from_receipt_member",
                begin().record(forged),
                "differs from authenticated peer",
            );
            // The same genuine member twice.
            let mut round = begin();
            round
                .record(observe(coordinator, a, mf, phase).unwrap())
                .unwrap();
            refuse(
                "duplicate_member",
                round.record(observe(coordinator, a, mf, phase).unwrap()),
                "already counted",
            );
            // A receipt for a different decision checksum.
            let genuine = observe(coordinator, b, mf, phase).unwrap();
            let mut altered = genuine.reply().clone();
            altered.native_decision.as_mut().unwrap().decision_sha256 = "0".repeat(64);
            altered.checksum_sha256 = altered.checksum().unwrap();
            refuse(
                "different_decision_checksum",
                round.record(genuine.test_replace_decoded_reply(altered)),
                "differs",
            );
            // NEW majority with the leader, but only one OLD member.
            let new_only = vec![survivors[0].clone(), mf.target_id.clone()];
            refuse(
                "missing_old_quorum",
                collect(coordinator, plan, mf, phase, &new_only).map(|_| ()),
                "both OLD and NEW data majorities",
            );
            // OLD majority, but only one NEW member (the new leader is the
            // target here, so a NEW minority necessarily lacks it as well).
            let old_only = vec![survivors[0].clone(), retiree.clone()];
            refuse(
                "missing_new_quorum",
                collect(coordinator, plan, mf, phase, &old_only).map(|_| ()),
                "both OLD and NEW data majorities",
            );
            // Both majorities but without the new leader.
            let leaderless = vec![survivors[0].clone(), survivors[1].clone(), retiree.clone()];
            refuse(
                "missing_new_leader",
                collect(coordinator, plan, mf, phase, &leaderless).map(|_| ()),
                "both OLD and NEW data majorities",
            );
            // Control: exact majorities including the leader are sufficient.
            let mut majority = survivors.clone();
            majority.push(mf.target_id.clone());
            let control = collect(coordinator, plan, mf, phase, &majority).unwrap();
            assert_eq!(control.data(), certificate.data());
            evidence(
                &output.join("remote-decision-certificate-refusals.json"),
                &observations,
            )
            .unwrap();
            assert_eq!(observations.len(), 6);
        }
        SourceCase::TargetRemoteDecisionCertificateStages => {
            let parent = budget(30_000);
            let relocation = mf.relocation_id;
            let target_replica = |topology: &ClusterTopology| {
                topology
                    .range_by_id(range)
                    .unwrap()
                    .replicas
                    .iter()
                    .find(|r| r.node_id == mf.target_id)
                    .unwrap()
                    .role
            };
            // Certified: exact decision correlation, membership untouched.
            let certified = stage_candidate(clients, template, &certificate, 0, &parent);
            let plan_row = &certified.relocations[&relocation];
            let record = plan_row.native_joint_handoff_v1.as_ref().unwrap();
            assert_eq!(record.decision_sha256(), data.checksum_sha256());
            assert_eq!(record.stage(), NativeJointHandoffStageV1::Certified);
            assert_eq!(record.range_index(), mf.range_index);
            assert_eq!(record.logical_history_sha256(), mf.h);
            assert_eq!(
                record.old_configuration_sha256(),
                data.promise().old_config_sha256
            );
            assert_eq!(
                record.new_configuration_sha256(),
                data.promise().new_config_sha256
            );
            assert_eq!(plan_row.phase, RelocationPhase::ReadyToPromote);
            let certified_range = certified.range_by_id(range).unwrap();
            assert_eq!(certified_range.epoch, mf.capture_range.epoch);
            assert_eq!(target_replica(&certified), RangeReplicaRole::Learner);
            commit_candidate(controls, metadata, clients, certified, &parent);
            // Union membership: target becomes a voter, epoch + 1.
            certificate.require_current().unwrap();
            let promoted = stage_candidate(clients, template, &certificate, 1, &parent);
            let promoted_range = promoted.range_by_id(range).unwrap();
            assert_eq!(promoted_range.epoch, mf.capture_range.epoch + 1);
            assert_eq!(promoted_range.leader, leader);
            assert_eq!(target_replica(&promoted), RangeReplicaRole::Voter);
            assert!(promoted_range.replicas.iter().any(|r| r.node_id == retiree));
            assert_eq!(
                promoted.relocations[&relocation]
                    .native_joint_handoff_v1
                    .as_ref()
                    .unwrap()
                    .stage(),
                NativeJointHandoffStageV1::PromotedFrozen
            );
            commit_candidate(controls, metadata, clients, promoted, &parent);
            // Final membership: exactly the decision's activation range.
            certificate.require_current().unwrap();
            let finalized = stage_candidate(clients, template, &certificate, 2, &parent);
            assert_eq!(
                finalized.range_by_id(range).unwrap(),
                data.activation_range()
            );
            assert!(!finalized
                .range_by_id(range)
                .unwrap()
                .replicas
                .iter()
                .any(|r| r.node_id == retiree));
            let final_row = &finalized.relocations[&relocation];
            assert_eq!(final_row.phase, RelocationPhase::CleaningUp);
            assert_eq!(
                final_row.native_joint_handoff_v1.as_ref().unwrap().stage(),
                NativeJointHandoffStageV1::AwaitingActivation
            );
            assert_eq!(
                final_row
                    .native_joint_handoff_v1
                    .as_ref()
                    .unwrap()
                    .decision_sha256(),
                data.checksum_sha256()
            );
            commit_candidate(controls, metadata, clients, finalized, &parent);
        }
        _ => unreachable!(),
    }
}
