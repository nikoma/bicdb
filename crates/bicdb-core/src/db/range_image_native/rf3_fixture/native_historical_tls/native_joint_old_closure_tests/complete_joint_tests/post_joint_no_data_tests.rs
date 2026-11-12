//! EXPECTED-RED acceptance baseline: real completed handoff, then a NEW-domain
//! Prepare-only slot. Refusal is audited but never counted as recovery success.
use super::*;
use crate::distribution_range_recovery::{
    NativeAbortRequest, NativeCompletionRequest, NativeReadyRequest, RecoveryFenceQuorumRound,
};
use std::collections::BTreeSet;

#[test]
fn native_post_joint_actual_new_domain_prepare_only_recovers_then_ack4_and_strong() {
    assert_eq!(
        run_complete_joint_with_continuation(0, JointFullContinuation::PostJointNoDataRecovery),
        JointTestOutcome::FullHandoffThenPostJointNoDataAck4Strong,
        "an earlier handoff/adversarial result cannot qualify post-joint recovery"
    );
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
fn members(range: &crate::RangeDescriptor) -> BTreeSet<ClusterNodeId> {
    range
        .replicas
        .iter()
        .filter(|r| r.role == crate::RangeReplicaRole::Voter)
        .map(|r| r.node_id.clone())
        .collect()
}
fn snapshot(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    id: &ClusterNodeId,
) -> serde_json::Value {
    let mut result = {
        let db = fixture.native.databases[id].read();
        let native = db.current_native_range_source_lineage(range.id).unwrap();
        let raw: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                db.data_path()
                    .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
            )
            .unwrap(),
        )
        .unwrap();
        let key = range.id.get().to_string();
        serde_json::json!({
            "node":id,"r":native.range_index(),"h0":native.logical_genesis_sha256(),"h":native.logical_history_sha256(),
            "history_generation":native.history_generation(),"local_data_b":native.actual_local_visibility_sequence(),
            "uid":db.local_namespace_identity("items").unwrap(),"physical_range":native.base_range(),
            "effective_range":native.native_write_range().unwrap(),"effective_members":native.native_write_members().unwrap(),
            "slots":db.native_abort_slot_inventory().unwrap(),
            "handoff_promise":raw["catalog"]["handoff_promises"][&key],
            "joint":raw["catalog"]["native_joint_handoffs"][&key],
        })
    };
    // Release the actual DB borrow BEFORE taking this same service's latch.
    result["progress"] = serde_json::to_value(
        fixture.native.services[id]
            .lock()
            .range_write_progress(range.id),
    )
    .unwrap();
    result
}
fn report(fixture: &EnforcedNetwork, name: &str, value: &serde_json::Value) {
    let envelope = serde_json::json!({"source_revision":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"test":std::thread::current().name(),"actual_data":value});
    crate::storage::write_atomic(
        &fixture.native.root.path().join(name),
        &serde_json::to_vec_pretty(&envelope).unwrap(),
        true,
    )
    .unwrap();
}
fn require_inherited(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    before: &BTreeMap<ClusterNodeId, serde_json::Value>,
    ack1: &crate::Record,
    ack2: &crate::Record,
) {
    for (id, old) in before {
        let now = snapshot(fixture, range, id);
        assert_eq!(
            now["uid"], old["uid"],
            "{id}: target namespace identity must survive"
        );
        assert_eq!(
            now["history_generation"], old["history_generation"],
            "{id}: no history replacement"
        );
        assert_eq!(
            now["handoff_promise"], old["handoff_promise"],
            "{id}: permanent handoff refusal must survive"
        );
        for (field, original) in old["joint"]
            .as_object()
            .expect("actual protected joint state")
            .iter()
        {
            assert_eq!(
                &now["joint"][field], original,
                "{id}: original joint decision/activation field {field} must survive"
            );
        }
        let current_slots = now["slots"].as_array().unwrap();
        for original in old["slots"].as_array().unwrap() {
            assert!(
                current_slots.contains(original),
                "{id}: inherited protected Commit/slot audit changed: {original}"
            );
        }
        let db = fixture.native.databases[id].read();
        let generation: Uuid = serde_json::from_value(old["history_generation"].clone()).unwrap();
        let entry = db
            .read_native_history_entry(range.id, generation, old["r"].as_u64().unwrap())
            .unwrap();
        assert_eq!(
            entry.commitment_sha256,
            old["h"].as_str().unwrap(),
            "{id}: inherited native prefix commitment"
        );
        assert_eq!(
            db.get("items", &ack1.id).unwrap().as_deref(),
            Some(ack1),
            "{id}: original ACK1 payload"
        );
        if members(range).contains(id) {
            assert_eq!(
                db.get("items", &ack2.id).unwrap().as_deref(),
                Some(ack2),
                "{id}: real target-leader ACK2 payload"
            );
        }
    }
}
fn fresh_new_scope(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
) -> RecoveryFenceScope {
    // The existing same-voter helper hardcodes old0/1/2. This scope instead
    // derives EVERY actual NEW voter from the committed descriptor.
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let nonce = Uuid::now_v7();
    let purpose = crate::distribution_consensus::MetadataRecoveryRolePurpose::NormalRecovery;
    let parent = token();
    let witness = fixture.providers[&range.leader]
        .witness_recovery_role(
            &actor,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    let role = crate::distribution_range_recovery::RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
        &range.leader,
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap();
    assert_eq!(
        role.member_credentials()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        members(range)
    );
    report(
        fixture,
        "actual-post-joint-new-metadata-authority.json",
        &serde_json::json!({"context_id":role.context_id(),"term":role.metadata_term(),"read_index":role.metadata_read_index(),"range":role.current_range(),"members":role.member_credentials(),"nonce":nonce,"purpose":purpose}),
    );
    let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
    let scope = round.scope().clone();
    for destination in members(range) {
        let actual = fixture.clients[&range.leader]
            .recovery_epoch_fence_observation(
                &destination,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 4000,
                },
                &parent,
            )
            .unwrap();
        round.record(Arc::new(actual)).unwrap();
    }
    round.finish().unwrap().require_live().unwrap();
    fixture.idle();
    scope
}
pub(super) fn exercise_actual_new_domain_prepare_only_recovery(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
) {
    let new_members = members(range);
    assert_eq!(
        new_members,
        BTreeSet::from([node("n1"), node("n2"), node("n3")])
    );
    assert_eq!(range.leader, node("n3"));
    let before = fixture
        .native
        .databases
        .keys()
        .map(|id| (id.clone(), snapshot(fixture, range, id)))
        .collect::<BTreeMap<_, _>>();
    for id in &new_members {
        assert_eq!(before[id]["r"], 2);
    }
    require_inherited(fixture, range, &before, ack1, ack2);
    report(
        fixture,
        "actual-post-joint-no-data-before.json",
        &serde_json::json!({"base_source":"61eb4d92b5938349cf18a283e936eee45fa97e9d","prerequisite":"actual all3 release + ACK1/ACK2 + StrongR2","snapshots":before}),
    );
    for id in new_members.iter().filter(|id| *id != &range.leader) {
        fixture.native.services[id]
            .lock()
            .test_refuse_native_prepare_after_start(true);
    }
    let failed_key = fixture.key(range.id, "post-joint-new-domain-prepare-only-no-ACK3");
    let db = fixture.native.databases[&range.leader].write();
    let wal = db.tx_log_handle().stats();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", crate::Record::new(&failed_key)).unwrap();
    let outcome = tx.observe_outcome();
    let error = tx.commit().unwrap_err();
    assert_eq!(
        outcome.outcome(),
        crate::TransactionOutcome::Aborted,
        "real prepare-only cleanup: {error}"
    );
    assert!(!matches!(
        &error,
        crate::BicDbError::CommitOutcomeUnknown { .. }
    ));
    assert_eq!(db.tx_log_handle().stats().written_seq, wal.written_seq);
    assert_eq!(
        db.tx_log_handle().stats().commits_written,
        wal.commits_written
    );
    drop(db);
    fixture.idle();
    for id in new_members.iter().filter(|id| *id != &range.leader) {
        fixture.native.services[id]
            .lock()
            .test_refuse_native_prepare_after_start(false);
    }
    let command = fixture.native.services[&range.leader]
        .lock()
        .prepared_range_writes(range.id)
        .into_iter()
        .find(|c| c.index == 3)
        .expect("actual new leader RawPrepare3");
    assert_eq!(command.range_epoch, range.epoch);
    assert_eq!(command.leader_node_id, range.leader);
    for id in &new_members {
        let actual = snapshot(fixture, range, id);
        assert_eq!(actual["r"], before[id]["r"]);
        assert_eq!(actual["h"], before[id]["h"]);
        assert_eq!(actual["local_data_b"], before[id]["local_data_b"]);
        let db = fixture.native.databases[id].read();
        assert!(db.get("items", &failed_key).unwrap().is_none());
        let slot = db
            .native_abort_slot_inventory()
            .unwrap()
            .into_iter()
            .find(|s| s.command.range_id == range.id && s.command.index == 3)
            .unwrap();
        assert_eq!(slot.command, command);
        assert!(!slot.ever_commit);
        assert!(slot.abort_promise_sha256.is_none());
        assert_eq!(
            slot.domain.member_ids(),
            new_members,
            "actual protected NEW domain"
        );
        assert_eq!(slot.predecessor_index, 2);
        assert_eq!(
            slot.predecessor_history_sha256,
            before[id]["h"].as_str().unwrap()
        );
        assert_eq!(
            slot.no_effect_sha256.is_some(),
            id == &range.leader,
            "only real Core origin cleanup mints no-effect"
        );
    }
    require_inherited(fixture, range, &before, ack1, ack2);
    report(
        fixture,
        "actual-post-joint-prepare-only.json",
        &serde_json::json!({"actual_command":command,"outcome":format!("{:?}",outcome.outcome()),"actual_prepare_failure":error.to_string(),"failed_key":failed_key,"business_ACK_count":2,"no_ACK_for_index3":true,"snapshots":fixture.native.databases.keys().map(|id|(id.clone(),snapshot(fixture,range,id))).collect::<BTreeMap<_,_>>()}),
    );
    let scope = fresh_new_scope(fixture, range);
    let request = NativeAbortRequest {
        scope: scope.clone(),
        range_index: 3,
        origin_node_id: range.leader.clone(),
        candidate_sha256: None,
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    let candidate = fixture.clients[&range.leader]
        .observe_native_abort_candidate(&range.leader, &request, &token())
        .unwrap();
    assert_eq!(candidate.record().command, command);
    assert_eq!(candidate.record().predecessor_index(), 2);
    assert_eq!(
        candidate.record().predecessor_history_sha256(),
        before[&range.leader]["h"].as_str().unwrap()
    );
    assert_eq!(candidate.record().domain.member_ids(), new_members);
    let actual_no_effect = serde_json::to_value(candidate.record()).unwrap();
    assert_eq!(
        actual_no_effect["local_data_visibility"],
        before[&range.leader]["local_data_b"]
    );
    assert!(actual_no_effect["actual_native_page_xid"].as_u64().unwrap() > 0);

    let request = NativeAbortRequest {
        candidate_sha256: Some(candidate.sha256().into()),
        ..request
    };
    report(
        fixture,
        "actual-post-joint-no-data-candidate.json",
        &serde_json::json!({"scope":scope,"command":command,"actual_no_effect_candidate":candidate.record(),"snapshots":new_members.iter().map(|id|(id.clone(),snapshot(fixture,range,id))).collect::<BTreeMap<_,_>>()}),
    );
    for id in &new_members {
        let promised = fixture.clients[&range.leader].promise_native_abort(id, &request, &token());
        eprintln!(
            "ACTUAL_POST_JOINT_NEW_PROMISE recipient={id} error={:?}",
            promised.as_ref().err()
        );
        promised
            .expect("actual present NEW-domain prepare-only slot must produce a permanent promise");
        fixture.idle();
    }
    let promises = new_members
        .iter()
        .map(|id| {
            let db = fixture.native.databases[id].read();
            let actual = db
                .observe_actual_native_abort_promise(range.id, 3)
                .unwrap()
                .unwrap();
            assert_eq!(actual.command_sha256, command.checksum_sha256);
            assert_eq!(actual.candidate_sha256, candidate.sha256());
            assert_eq!(actual.domain.member_ids(), new_members);
            (id.clone(), actual)
        })
        .collect::<BTreeMap<_, _>>();
    report(
        fixture,
        "actual-post-joint-no-data-before-resolve.json",
        &serde_json::json!({"scope":scope,"command":command,"actual_no_effect_candidate":candidate.record(),"actual_permanent_NEW_promises":promises,"snapshots":new_members.iter().map(|id|(id.clone(),snapshot(fixture,range,id))).collect::<BTreeMap<_,_>>()}),
    );
    for id in &new_members {
        let resolved =
            fixture.clients[&range.leader].resolve_native_abort_at(id, &request, &token());
        fixture.idle();
        if let Err(error) = &resolved {
            let stopped = new_members
                .iter()
                .map(|n| (n.clone(), snapshot(fixture, range, n)))
                .collect::<BTreeMap<_, _>>();
            report(
                fixture,
                "actual-post-joint-no-data-refusal.json",
                &serde_json::json!({"scenario":"FAILED_UNSUPPORTED_POST_JOINT_NEW_DOMAIN_NO_DATA","failing_recipient":id,"error":error.to_string(),"scope":scope,"command":command,"candidate":candidate.record(),"promises":promises,"snapshots":stopped}),
            );
            require_inherited(fixture, range, &before, ack1, ack2);
            for member in &new_members {
                let db = fixture.native.databases[member].read();
                let source = db.current_native_range_source_lineage(range.id).unwrap();
                assert_eq!(source.range_index(), 2);
                assert_eq!(
                    source.actual_local_visibility_sequence(),
                    before[member]["local_data_b"].as_u64().unwrap()
                );
                assert_eq!(
                    source.logical_history_sha256(),
                    before[member]["h"].as_str().unwrap()
                );
                assert_eq!(
                    db.observe_actual_native_abort_promise(range.id, 3)
                        .unwrap()
                        .as_ref(),
                    Some(&promises[member])
                );
                assert!(db.get("items", &failed_key).unwrap().is_none());
            }
        }
        let (r,_actual_decision_sha,b)=resolved.expect("post-joint NEW-domain Abort3 recovery MUST succeed; refusal alone is not recovery qualification");
        assert_eq!(r, 3);
        assert_eq!(b, before[id]["local_data_b"].as_u64().unwrap());
        let db = fixture.native.databases[id].read();
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), 3);
        assert_eq!(
            db.read_native_history_entry(range.id, source.history_generation(), 3)
                .unwrap()
                .outcome,
            crate::db::range_image_native::history::NativeLogicalHistoryOutcomeV1::Aborted
        );
    }
    require_inherited(fixture, range, &before, ack1, ack2);
    for id in &new_members {
        fixture.clients[&range.leader]
            .prepare_native_ready_observation(
                id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    for id in &new_members {
        fixture.clients[&range.leader]
            .complete_native_recovery_observation(
                id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    let actor = fixture.clients[&range.leader].local_metadata_read_participant();
    for id in new_members
        .iter()
        .filter(|id| *id != &range.leader)
        .chain(std::iter::once(&range.leader))
    {
        fixture.runtimes[id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &token(),
            )
            .unwrap();
        fixture.idle();
    }
    fixture.coordinators[&range.leader]
        .recover_local_leadership(8)
        .unwrap();
    assert_strong(fixture, range, &ack1.id, Some(ack1), 3);
    assert_strong(fixture, range, &ack2.id, Some(ack2), 3);
    assert_strong(fixture, range, &failed_key, None, 3);

    let ack4 =
        crate::Record::new(fixture.key(range.id, "post-joint-real-ACK4-after-canonical-Abort3"));
    {
        let db = fixture.native.databases[&range.leader].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", ack4.clone()).unwrap();
        tx.commit().unwrap();
    }
    crate::storage::write_atomic(
        &fixture
            .native
            .root
            .path()
            .join("actual-joint-selected-range-ack-ledger.json"),
        &serde_json::to_vec(&vec![ack1, ack2, &ack4]).unwrap(),
        true,
    )
    .unwrap();
    require_inherited(fixture, range, &before, ack1, ack2);
    for id in &new_members {
        let db = fixture.native.databases[id].read();
        let native = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(native.range_index(), 4);
        assert!(db.get("items", &failed_key).unwrap().is_none());
        assert_eq!(db.get("items", &ack4.id).unwrap().as_deref(), Some(&ack4));
        assert_eq!(
            db.read_native_history_entry(range.id, native.history_generation(), 3)
                .unwrap()
                .outcome,
            crate::db::range_image_native::history::NativeLogicalHistoryOutcomeV1::Aborted
        );
    }
    assert_strong(fixture, range, &ack4.id, Some(&ack4), 4);
    assert_strong(fixture, range, &ack1.id, Some(ack1), 4);
    assert_strong(fixture, range, &ack2.id, Some(ack2), 4);
    eprintln!("ACTUAL_POST_JOINT_NO_DATA_TERMINAL inherited-ACK1-ACK2=true PrepareOnly3-noACK=true actual-NoData3=true real-businessACKs=3 rangeR=4 StrongR4=true old-promises-and-Commit-obligations-preserved=true");
}

fn assert_strong(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    key: &str,
    expected: Option<&crate::Record>,
    r: u64,
) {
    let request = fixture.request(key, range);
    let parent = token();
    let first = fixture.clients[&range.leader]
        .acquire_metadata_read_observation(
            &node("n1"),
            request.business_request_id,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let read = observed(fixture.coordinators[&range.leader].strong_point_read(
        &fixture.runtimes[&range.leader],
        Some(fixture.hosts[&range.leader].clone()),
        fixture.clients[&range.leader].clone(),
        first,
        request,
        &parent,
    ));
    assert_eq!(read.record().map(|record| record.as_ref()), expected);
    assert_eq!(read.audit().range_index, r);
    fixture.idle();
}
