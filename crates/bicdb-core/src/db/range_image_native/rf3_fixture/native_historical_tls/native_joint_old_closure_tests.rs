//! Component control only: FOUR actual fresh born-v2 members, real mTLS host
//! policies and RF3 application ACK, then an actual committed learner plan and
//! old-data closure. The relocation target is the existing metadata-only n3;
//! this is NOT the fresh fifth-node/activation/cleanup acceptance case.
use super::*;
use crate::distribution::{
    RangeHandoffSession, RangeRelocation, RelocationId, RelocationPhase, ReplicaMoveReason,
};
use crate::distribution_range_handoff::{NativeJointHandoffRuntime, NativeJointOldRound};

#[path = "native_joint_old_closure_tests/complete_joint_tests.rs"]
mod complete_joint_tests;

fn commit_pending(fixture: &mut EnforcedNetwork, original: &RangeDescriptor) -> RelocationId {
    commit_pending_with_policy(fixture, original, PendingPolicy::ReinstallFromActualQuorum)
}

fn commit_pending_retaining_original_writer(
    fixture: &mut EnforcedNetwork,
    original: &RangeDescriptor,
) -> RelocationId {
    commit_pending_with_policy(fixture, original, PendingPolicy::RetainOriginal)
}

enum PendingPolicy {
    ReinstallFromActualQuorum,
    RetainOriginal,
}

fn commit_pending_with_policy(
    fixture: &mut EnforcedNetwork,
    original: &RangeDescriptor,
    policy: PendingPolicy,
) -> RelocationId {
    let mut candidate = fixture.native.topology.read().clone();
    // Explicit operator plan DATA, proposed and committed by real fsynced
    // metadata stores below. No role/quorum/native grant is constructed here.
    let id = RelocationId::new(candidate.next_relocation_id).unwrap();
    candidate.next_relocation_id += 1;
    let replica_id = ReplicaId::new(candidate.next_replica_id).unwrap();
    candidate.next_replica_id += 1;
    let range = candidate
        .ranges
        .values_mut()
        .find(|r| r.id == original.id)
        .unwrap();
    assert_eq!(range.epoch, 1);
    range.epoch += 1;
    range.replicas.push(RangeReplica {
        id: replica_id,
        node_id: node("n3"),
        role: RangeReplicaRole::Learner,
    });
    let relocation = RangeRelocation {
        id,
        plan_id: "actual-born2-old-closure-only".into(),
        handoff_session: Some(RangeHandoffSession {
            session_id: Uuid::now_v7(),
            nonce: Uuid::now_v7(),
        }),
        native_joint_handoff_v1: None,
        range_id: original.id,
        expected_epoch: original.epoch,
        learner_epoch: original.epoch + 1,
        promoted_epoch: None,
        source: Some(node("n0")),
        target: node("n3"),
        target_replica_id: replica_id,
        reason: ReplicaMoveReason::LoadBalance,
        phase: RelocationPhase::LearnerAllocated,
        resume_phase: None,
        snapshot_id: None,
        snapshot_sha256: None,
        snapshot_bytes_copied: 0,
        snapshot_resume_after_key: None,
        snapshot_commit_sequence: None,
        destination_durable_commit_sequence: None,
        source_commit_sequence: None,
        cleanup_resume_after_key: None,
        cleanup_records_deleted: 0,
        created_at_ms: 2,
        updated_at_ms: 2,
        last_error: None,
    };
    candidate.relocations.insert(id, relocation);
    candidate.generation += 1;
    candidate.validate().unwrap();
    let mut leader = fixture.metadata[&node("n1")].lock();
    let entry = leader.propose_topology(candidate).unwrap();
    for follower in [node("n0"), node("n2"), node("n3")] {
        let response = fixture.metadata[&follower]
            .lock()
            .handle_append_request(leader.append_request_from(entry.index, 16).unwrap())
            .unwrap();
        leader.record_append_response(&follower, &response).unwrap();
    }
    for follower in [node("n0"), node("n2"), node("n3")] {
        let response = fixture.metadata[&follower]
            .lock()
            .handle_append_request(leader.append_request_from(entry.index + 1, 16).unwrap())
            .unwrap();
        leader.record_append_response(&follower, &response).unwrap();
    }
    let committed = leader.committed_topology().clone();
    drop(leader);
    assert_eq!(committed.range_by_id(original.id).unwrap().epoch, 2);
    for store in fixture.metadata.values() {
        assert_eq!(store.lock().committed_topology(), &committed);
    }
    *fixture.native.topology.write() = committed;
    if matches!(policy, PendingPolicy::RetainOriginal) {
        return id;
    }
    // Fresh actual HostMQ policy on EVERY process after the committed route
    // change, before any further data decision. No historical R1 retrofit.
    for local in [node("n0"), node("n1"), node("n2"), node("n3")] {
        let business = Uuid::now_v7();
        let mq = fixture.clients[&local]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let policy = {
            let service = fixture.native.services[&local].lock();
            let db = fixture.native.databases[&local].read();
            service
                .prepare_native_host_writer_enrollment(&db, mq, business)
                .unwrap()
        };
        let guard = fixture.native.databases[&local]
            .read()
            .install_enrolled_native_coordinator(fixture.coordinators[&local].clone(), policy)
            .unwrap();
        fixture.hosts.insert(local, Arc::new(guard));
    }
    id
}
fn role(
    fixture: &EnforcedNetwork,
    plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    recipient: &ClusterNodeId,
) -> RecoveryRoleAdmission {
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let promise = plan.promise();
    let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
        relocation_id: promise.relocation_id,
        plan_id: promise.plan_id.clone(),
        session_id: promise.session_id,
        nonce: promise.nonce,
    };
    let token = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    // Actual provider read-index quorum with real mTLS peer probes. This
    // component invokes its trusted local caller seam, not a new handoff RPC.
    let witness = fixture.providers[recipient]
        .witness_recovery_role(
            &actor,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            purpose.clone(),
            &token,
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
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

#[test]
fn native_joint_actual_old_closure_four_metadata_rf3_nonempty_complete_prefix_and_shared_pin() {
    let mut fixture = EnforcedNetwork::new_with_metadata_members(4);
    let (key, old) = fixture.seed();
    let original = fixture.native.databases[&node("n0")]
        .read()
        .get("items", &key)
        .unwrap()
        .unwrap();
    let relocation = commit_pending(&mut fixture, &old);
    let current = fixture
        .native
        .topology
        .read()
        .range_by_id(old.id)
        .unwrap()
        .clone();
    let business = Uuid::now_v7();
    let mq = fixture.clients[&node("n0")]
        .acquire_metadata_read_observation(
            &node("n1"),
            business,
            current.id,
            current.epoch,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let mut round = NativeJointOldRound::begin(mq, business, relocation).unwrap();
    let plan = round.plan();
    let runtimes = [node("n0"), node("n1"), node("n2")]
        .into_iter()
        .map(|id| {
            (
                id.clone(),
                NativeJointHandoffRuntime::new(fixture.native.services[&id].clone()).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for local in runtimes.keys() {
        let constraints = runtimes[local]
            .observe_constraints(
                plan.clone(),
                role(&fixture, &plan, local),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        round
            .record_constraints(constraints, &fixture.native.services[local])
            .unwrap();
    }
    for local in [node("n0"), node("n1")] {
        let actual = runtimes[&local]
            .close_old(
                plan.clone(),
                role(&fixture, &plan, &local),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(actual.range_index(), 1);
        round.record_closed(actual).unwrap();
    }
    let mut closed = round.finish().unwrap();
    assert_eq!(closed.range_index(), 1);
    assert_eq!(closed.selected_source_id(), &node("n0"));
    closed.require_current().unwrap();
    let mut capture = closed
        .begin_capture(Duration::from_secs(2), &CancellationToken::uncancelable())
        .unwrap();
    assert!(capture.reserved_upper_bound().get() >= 3);
    assert_ne!(capture.reserved_upper_bound().get(), closed.range_index());
    let cursor = capture.history_cursor().clone();
    let history = capture
        .export_history_step(&cursor, 2, &CancellationToken::uncancelable())
        .unwrap();
    assert_eq!(history.history.entries.len(), 2);
    assert_eq!(history.history.entries[1].index, 1);
    assert_eq!(
        history.history.entries[1].commitment_sha256,
        closed.logical_history_sha256()
    );
    assert!(fixture
        .native
        .append(&fixture.key(old.id, "must-refuse-after-actual-old-quorum-close"))
        .is_err());
    assert_eq!(
        fixture.native.databases[&node("n0")]
            .read()
            .get("items", &key)
            .unwrap()
            .unwrap(),
        original
    );
    for local in [node("n0"), node("n1")] {
        assert_eq!(
            fixture.native.databases[&local]
                .read()
                .current_native_range_source_lineage(old.id)
                .unwrap()
                .range_index(),
            1
        );
    }
    drop(capture);
    closed.require_current().unwrap();
}
