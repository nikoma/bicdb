//! Actual two-range RF1 component fixtures, not network or HA qualification.
use super::*;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_range_recovery::{
    test_role_admission, ClosedNativeReadyQuorumRound, NativeCompletionObservation,
    NativeCompletionQuorum, NativeCompletionQuorumRound, NativeCompletionRequest,
    NativeReadyObservation, NativeReadyRequest, RecoveryRoleAdmission,
};
use crate::{BicDb, CancellationToken};
use std::time::Duration;

struct Fixture {
    root: tempfile::TempDir,
    db: Arc<RwLock<BicDb>>,
    topology: ClusterTopology,
    spec: crate::db::RangeImageBirthSpec,
}
impl Fixture {
    fn new() -> Self {
        let (root, db, mut topology, spec) = native_origin_fixture();
        let ranges = topology.ranges.values().map(|r| r.id).collect::<Vec<_>>();
        assert_eq!(ranges.len(), 2);
        for range in ranges {
            seed_actual_origin(&db, topology.clone(), &spec, range, 1);
        }
        for range in topology.ranges.values_mut() {
            range.epoch += 1;
        }
        Self {
            root,
            db,
            topology,
            spec,
        }
    }
    fn role(&self, range: RangeId, nonce: Uuid) -> RecoveryRoleAdmission {
        let current = self.topology.range_by_id(range).unwrap();
        test_role_admission(
            &self
                .root
                .path()
                .join(format!("two-range-meta-{range}-{nonce}")),
            &self.topology,
            &self.spec.node_id,
            &self.spec.node_id,
            &self.spec.node_id,
            range,
            current.epoch,
            nonce,
            MetadataRecoveryRolePurpose::NormalRecovery,
            Duration::from_secs(5),
        )
        .unwrap()
    }
    fn peer(&self) -> MetadataReadParticipant {
        MetadataReadParticipant::from_transport(
            self.spec.node_id.clone(),
            self.spec.member_incarnation,
            None,
            true,
        )
    }
    fn prepare(
        &self,
        store: &mut RangeWriteStore,
        range: RangeId,
        nonce: Uuid,
    ) -> PreparedNativeRecoveryReady {
        let role = self.role(range, nonce);
        let frozen = Arc::new(
            self.db
                .read()
                .range_mutation_gate()
                .try_freeze(range, nonce)
                .unwrap(),
        );
        let native = self
            .db
            .read()
            .native_recovery_admission(
                &self.topology.cluster_id,
                self.spec.bootstrap_id,
                &self.spec.node_id,
                self.spec.member_incarnation,
            )
            .unwrap();
        let scope = RecoveryFenceScope::from_role(&role).unwrap();
        store.fence_recovery_epoch(&role, &native, &scope).unwrap();
        let observed = store
            .observe_recovery_resolved_prefix(&role, &native, &scope)
            .unwrap();
        let physical = self
            .db
            .read()
            .attest_local_native_recovery_ready(
                native,
                role,
                observed,
                store,
                frozen,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        store
            .prepare_native_recovery_ready(&self.db.read(), physical)
            .unwrap()
    }
    fn complete(&self, store: &mut RangeWriteStore, proof: &PreparedNativeRecoveryReady) {
        let role = self.role(proof.scope().range_id, proof.scope().recovery_nonce);
        let request = NativeReadyRequest {
            scope: RecoveryFenceScope::from_role(&role).unwrap(),
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let actual = store
            .observe_prepared_native_for_role(&self.db.read(), proof, &role, &request)
            .unwrap();
        let observation =
            NativeReadyObservation::from_local(actual, self.peer(), &request, role.deadline())
                .unwrap();
        let mut round =
            ClosedNativeReadyQuorumRound::begin(role, &CancellationToken::uncancelable()).unwrap();
        round.record(observation).unwrap();
        store
            .complete_native_recovery_frozen(
                &self.db.read(),
                proof,
                round.finish().unwrap(),
                &self.role(proof.scope().range_id, proof.scope().recovery_nonce),
            )
            .unwrap();
    }
    fn completion_quorum(
        &self,
        store: &RangeWriteStore,
        proof: &PreparedNativeRecoveryReady,
    ) -> NativeCompletionQuorum {
        let role = self.role(proof.scope().range_id, proof.scope().recovery_nonce);
        let request = NativeCompletionRequest {
            scope: RecoveryFenceScope::from_role(&role).unwrap(),
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let actual = store
            .observe_native_completion_for_role(&self.db.read(), proof, &role, &request)
            .unwrap();
        let observation =
            NativeCompletionObservation::from_local(actual, self.peer(), &request, role.deadline())
                .unwrap();
        let mut round =
            NativeCompletionQuorumRound::begin(role, &CancellationToken::uncancelable()).unwrap();
        round.record(observation).unwrap();
        round.finish().unwrap()
    }
}

fn check_other_range_domain(completed_b: bool) {
    let fixture = Fixture::new();
    let ranges = fixture
        .topology
        .ranges
        .values()
        .map(|r| r.id)
        .collect::<Vec<_>>();
    let mut store = RangeWriteStore::open(
        fixture.db.read().data_path(),
        fixture.topology.cluster_id.clone(),
        fixture.spec.node_id.clone(),
        true,
    )
    .unwrap();
    let a = fixture.prepare(&mut store, ranges[0], Uuid::now_v7());
    fixture.complete(&mut store, &a);
    let b = fixture.prepare(&mut store, ranges[1], Uuid::now_v7());
    if completed_b {
        fixture.complete(&mut store, &b);
    }
    let original_b_commitment = store.recovery_range_catalog_sha256(ranges[1]).unwrap();
    let original_b_phase = store.recovery_observation_phase(b.scope()).unwrap();
    let role_b = fixture.role(ranges[1], b.scope().recovery_nonce);
    b.require_current_under_role(&fixture.db.read(), &store, &role_b)
        .unwrap();
    let quorum = fixture.completion_quorum(&store, &a);
    let receipt = store
        .activate_native_recovery(
            &fixture.db.read(),
            &a,
            quorum,
            &fixture.role(ranges[0], a.scope().recovery_nonce),
        )
        .unwrap();
    receipt
        .require_current(
            &fixture.db.read(),
            &store,
            &a,
            &fixture.role(ranges[0], a.scope().recovery_nonce),
        )
        .unwrap();
    assert_eq!(store.catalog.format_version, 9);
    assert_eq!(
        store.recovery_range_catalog_sha256(ranges[1]).unwrap(),
        original_b_commitment
    );
    assert_eq!(
        store.recovery_observation_phase(b.scope()).unwrap(),
        original_b_phase
    );
    b.require_current_under_role(
        &fixture.db.read(),
        &store,
        &fixture.role(ranges[1], b.scope().recovery_nonce),
    )
    .unwrap();
    // Deliberate test-only catalogue corruption, not a legal phase producer.
    // The per-range hash excludes this global field, but minimum/known format
    // validation is independently mandatory on the actual retained proof.
    let known = store.catalog.format_version;
    for corrupt in [1, u32::MAX] {
        store.catalog.format_version = corrupt;
        assert!(b
            .require_current_under_role(
                &fixture.db.read(),
                &store,
                &fixture.role(ranges[1], b.scope().recovery_nonce)
            )
            .is_err());
        assert!(store.require_native_phase_format(ranges[1]).is_err());
    }
    store.catalog.format_version = known;
    b.require_current_under_role(
        &fixture.db.read(),
        &store,
        &fixture.role(ranges[1], b.scope().recovery_nonce),
    )
    .unwrap();
    for proof in [&a, &b] {
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_enter(proof.scope().range_id)
            .is_err());
    }
    // Store activation only: Runtime's release/arm is exercised elsewhere.
    assert!(store
        .require_actual_native_activated_open(ranges[0])
        .is_err());
}

#[test]
fn native_activation_other_range_prepared_domain_survives_global_format_upgrade() {
    check_other_range_domain(false);
}
#[test]
fn native_activation_other_range_completed_domain_survives_global_format_upgrade() {
    check_other_range_domain(true);
}
