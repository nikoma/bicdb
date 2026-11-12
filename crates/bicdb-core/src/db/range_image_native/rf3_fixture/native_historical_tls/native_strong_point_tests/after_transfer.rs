//! Same genuinely born-v2 cluster, with the initial registered policies before
//! ACK1. Only the metadata leadership decision is a declared fixture change;
//! data recovery/physical preparation/completion/activation are real producers.
use super::*;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeReadyRequest};

impl EnforcedNetwork {
    pub(super) fn refresh_actual_writer_policies(&mut self) {
        for id in [node("n0"), node("n1"), node("n2")] {
            let business = Uuid::now_v7();
            // Fresh HostMQ before either local latch, never a topology DTO.
            let observed = self.clients[&id]
                .acquire_host_metadata_observation(
                    &node("n1"),
                    business,
                    Duration::from_secs(5),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            let policy = {
                let service = self.native.services[&id].lock();
                let database = self.native.databases[&id].read();
                service
                    .prepare_native_host_writer_enrollment(&database, observed, business)
                    .unwrap()
            };
            let host = self.native.databases[&id]
                .read()
                .install_enrolled_native_coordinator(self.coordinators[&id].clone(), policy)
                .unwrap();
            self.hosts.insert(id, Arc::new(host));
        }
    }
    fn read_as_current_leader(
        &self,
        key: &str,
        range: &RangeDescriptor,
    ) -> NativeStrongPointReadOutcome {
        let id = &range.leader;
        let request = self.request(key, range);
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let first = self.clients[id]
            .acquire_metadata_read_observation(
                &node("n1"),
                request.business_request_id,
                range.id,
                range.epoch,
                Duration::from_secs(5),
                &parent,
            )
            .unwrap();
        self.coordinators[id].strong_point_read(
            &self.runtimes[id],
            Some(Arc::clone(&self.hosts[id])),
            Arc::clone(&self.clients[id]),
            first,
            request,
            &parent,
        )
    }
    pub(super) fn actual_current_recovery_scope(
        &self,
        range: &RangeDescriptor,
    ) -> RecoveryFenceScope {
        let actor = self.clients[&range.leader].local_metadata_read_participant();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let nonce = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let witness = self.providers[&range.leader]
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
        let role = RecoveryRoleAdmission::from_witness(
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
        let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
        let scope = round.scope().clone();
        for destination in [node("n0"), node("n1"), node("n2")] {
            let observed = self.clients[&range.leader]
                .recovery_epoch_fence_observation(
                    &destination,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 4000,
                    },
                    &parent,
                )
                .unwrap();
            round.record(Arc::new(observed)).unwrap();
        }
        let authority = round.finish().unwrap();
        authority.require_live().unwrap();
        self.idle();
        scope
    }
}

#[test]
fn native_strong_point_after_actual_same_voter_leader_transfer_and_recovery_reads_acked_value() {
    let mut fixture = EnforcedNetwork::new();
    let (original, old_range) = fixture.seed();
    let old_value = observed(fixture.read_as_current_leader(&original, &old_range));
    assert_eq!(old_value.audit().range_index, 1);
    fixture.idle();
    // Actual fsynced metadata proposal+quorum commit, same voters/schema/
    // incarnation/pins. This fixture does not claim automatic election.
    fixture.change_committed_leader(old_range.id);
    let range = fixture
        .native
        .topology
        .read()
        .range_by_id(old_range.id)
        .unwrap()
        .clone();
    assert_eq!(range.epoch, old_range.epoch + 1);
    assert_eq!(range.leader, node("n2"));
    assert_ne!(range.leader, node("n1"));
    fixture.refresh_actual_writer_policies();
    // Fresh writer enrollment is not native recovered-serving permission.
    let premature = unavailable(fixture.read_as_current_leader(&original, &range));
    assert!(matches!(
        premature.stage,
        NativeStrongReadStage::DataQuorum
            | NativeStrongReadStage::Snapshot
            | NativeStrongReadStage::WriterAdmission
    ));
    fixture.idle();
    let scope = fixture.actual_current_recovery_scope(&range);
    // All original ordinary Apply instructions were delivered over real TLS;
    // the real raw/native histories already match R1. No history is fabricated.
    for id in [node("n0"), node("n1"), node("n2")] {
        let observed = fixture.clients[&node("n2")]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(observed.reply().range_index, 1);
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        let completed = fixture.clients[&node("n2")]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(completed.reply().range_index, 1);
        fixture.idle();
    }
    let actor = fixture.clients[&node("n2")].local_metadata_read_participant();
    // Activation is the actual trusted LOCAL Runtime API here; unlike the
    // preceding Fence/Prepare/Complete calls it is NOT an incoming TLS endpoint.
    // Followers first, new leader last. Completion anchors are not read proofs.
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.idle();
    }
    // Actual ordinary recovery verifies the installed new coordinator. Never
    // write recovered_epochs or clear flags to make a read/write pass.
    fixture.coordinators[&node("n2")]
        .recover_local_leadership(8)
        .unwrap();
    let value = observed(fixture.read_as_current_leader(&original, &range));
    assert_eq!(value.record().unwrap().id, original);
    assert_eq!(value.audit().range_epoch, range.epoch);
    assert_eq!(value.audit().range_index, 1);
    fixture.idle();
    // A real new-leader ACK then reads its actual current native prefix, while
    // the immutable activation decision still describes its original R1.
    let next = fixture.key(range.id, "after-transfer-ack");
    fixture.native.append(&next).unwrap();
    let current = observed(fixture.read_as_current_leader(&next, &range));
    assert_eq!(current.record().unwrap().id, next);
    assert_eq!(current.audit().range_index, 2);
    assert!(observed(fixture.read_as_current_leader(&original, &range))
        .record()
        .is_some());
    fixture.idle();
}
