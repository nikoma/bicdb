//! Actual RF1 native-WAL component controls, not transport or HA qualification.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_range_consensus::{native_origin_fixture, seed_actual_origin};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    test_role_admission, ClosedNativeReadyQuorumRound, NativeCompletionObservation,
    NativeCompletionQuorumRound, NativeCompletionRequest, NativeReadyObservation,
    NativeReadyRequest,
};

struct Fixture {
    root: tempfile::TempDir,
    db: Arc<RwLock<BicDb>>,
    topology: crate::ClusterTopology,
    spec: RangeImageBirthSpec,
}
impl Fixture {
    fn new() -> Self {
        let (root, db, mut topology, spec) = native_origin_fixture();
        let range = topology.ranges.values().next().unwrap().id;
        seed_actual_origin(&db, topology.clone(), &spec, range, 1);
        // Declared new metadata epoch, not an election/power-loss experiment.
        topology.ranges.values_mut().next().unwrap().epoch += 1;
        Self {
            root,
            db,
            topology,
            spec,
        }
    }
    fn role(&self, nonce: Uuid) -> RecoveryRoleAdmission {
        let range = self.topology.ranges.values().next().unwrap();
        test_role_admission(
            &self.root.path().join(format!("decision-role-{nonce}")),
            &self.topology,
            &self.spec.node_id,
            &self.spec.node_id,
            &self.spec.node_id,
            range.id,
            range.epoch,
            nonce,
            MetadataRecoveryRolePurpose::NormalRecovery,
            Duration::from_secs(5),
        )
        .unwrap()
    }
    fn participant(&self) -> MetadataReadParticipant {
        MetadataReadParticipant::from_transport(
            self.spec.node_id.clone(),
            self.spec.member_incarnation,
            None,
            true,
        )
    }
    fn completed(
        &self,
        nonce: Uuid,
    ) -> (
        RangeWriteStore,
        PreparedNativeRecoveryReady,
        Arc<FrozenRangeMutationPermit>,
    ) {
        let role = self.role(nonce);
        let frozen = Arc::new(
            self.db
                .read()
                .range_mutation_gate()
                .try_freeze(role.current_range().id, nonce)
                .unwrap(),
        );
        let mut store = RangeWriteStore::open(
            self.db.read().data_path(),
            self.topology.cluster_id.clone(),
            self.spec.node_id.clone(),
            true,
        )
        .unwrap();
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
                &store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&self.db.read(), physical)
            .unwrap();
        let fresh = self.role(nonce);
        let request = NativeReadyRequest {
            scope: RecoveryFenceScope::from_role(&fresh).unwrap(),
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let actual = store
            .observe_prepared_native_for_role(&self.db.read(), &prepared, &fresh, &request)
            .unwrap();
        let observed = NativeReadyObservation::from_local(
            actual,
            self.participant(),
            &request,
            fresh.deadline(),
        )
        .unwrap();
        let mut round =
            ClosedNativeReadyQuorumRound::begin(fresh, &CancellationToken::uncancelable()).unwrap();
        round.record(observed).unwrap();
        store
            .complete_native_recovery_frozen(
                &self.db.read(),
                &prepared,
                round.finish().unwrap(),
                &self.role(nonce),
            )
            .unwrap();
        (store, prepared, frozen)
    }
    fn quorum(
        &self,
        nonce: Uuid,
        store: &RangeWriteStore,
        prepared: &PreparedNativeRecoveryReady,
    ) -> NativeCompletionQuorum {
        let fresh = self.role(nonce);
        let request = NativeCompletionRequest {
            scope: RecoveryFenceScope::from_role(&fresh).unwrap(),
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let actual = store
            .observe_native_completion_for_role(&self.db.read(), prepared, &fresh, &request)
            .unwrap();
        let observed = NativeCompletionObservation::from_local(
            actual,
            self.participant(),
            &request,
            fresh.deadline(),
        )
        .unwrap();
        let mut round =
            NativeCompletionQuorumRound::begin(fresh, &CancellationToken::uncancelable()).unwrap();
        round.record(observed).unwrap();
        round.finish().unwrap()
    }
}

#[test]
fn native_activation_decision_actual_quorum_publishes_before_catalogue_and_reuses_original_audit() {
    let fixture = Fixture::new();
    let nonce = Uuid::now_v7();
    let (store, prepared, _frozen) = fixture.completed(nonce);
    let quorum = fixture.quorum(nonce, &store, &prepared);
    let original_qq = quorum.decision_sha256().unwrap();
    let fresh = fixture.role(nonce);
    let receipt = fixture
        .db
        .read()
        .persist_native_activation_decision(&prepared, &store, &quorum, &fresh)
        .unwrap();
    receipt.require_current(&fixture.db.read()).unwrap();
    // Binding-rule negative only: no numeric values mint a fresh role. The
    // actual role/observer readers feed this check their opaque MQ evidence.
    assert!(receipt
        .record
        .require_fresh_anchor(
            receipt.record.post_quorum_metadata_term - 1,
            receipt.record.post_quorum_metadata_read_index,
        )
        .is_err());
    assert!(receipt
        .record
        .require_fresh_anchor(
            receipt.record.post_quorum_metadata_term,
            receipt.record.post_quorum_metadata_read_index - 1,
        )
        .is_err());
    assert_eq!(receipt.completion_quorum_sha256(), original_qq);
    assert_eq!(receipt.range_index(), 1);
    assert!(receipt.record.native_page_xid > 0);
    assert_eq!(
        store.recovery_observation_phase(prepared.scope()).unwrap(),
        crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen
    );
    assert!(fixture
        .db
        .read()
        .range_mutation_gate()
        .try_enter_database()
        .is_err());
    let second = fixture.quorum(nonce, &store, &prepared);
    assert_ne!(second.decision_sha256().unwrap(), original_qq);
    let retried = fixture
        .db
        .read()
        .persist_native_activation_decision(&prepared, &store, &second, &fixture.role(nonce))
        .unwrap();
    assert_eq!(retried.checksum_sha256(), receipt.checksum_sha256());
    assert_eq!(retried.completion_quorum_sha256(), original_qq);
    let read = fixture
        .db
        .read()
        .observe_native_activation_decision_for_role(
            &fixture.role(nonce),
            receipt.scope(),
            receipt.checksum_sha256(),
            receipt.native_serving_sha256(),
            receipt.range_index(),
            receipt.logical_genesis_sha256(),
            receipt.logical_history_sha256(),
        )
        .unwrap();
    assert_eq!(read.checksum_sha256(), receipt.checksum_sha256());
    assert!(
        fixture
            .db
            .read()
            .observe_native_activation_decision_for_role(
                &fixture.role(nonce),
                receipt.scope(),
                receipt.completion_quorum_sha256(),
                receipt.native_serving_sha256(),
                receipt.range_index(),
                receipt.logical_genesis_sha256(),
                receipt.logical_history_sha256()
            )
            .is_err(),
        "a catalogue/QQ hash cannot replace actual protected-record identity"
    );
}

#[test]
fn native_activation_decision_missing_record_and_reused_pre_quorum_context_refuse() {
    let fixture = Fixture::new();
    let nonce = Uuid::now_v7();
    let (store, prepared, _frozen) = fixture.completed(nonce);
    let quorum = fixture.quorum(nonce, &store, &prepared);
    let fresh = fixture.role(nonce);
    let serving = prepared
        .require_completed_for_activation(&fixture.db.read(), &store, &fresh)
        .unwrap();
    assert!(fixture
        .db
        .read()
        .observe_native_activation_decision_for_role(
            &fresh,
            prepared.scope(),
            &quorum.decision_sha256().unwrap(),
            serving.checksum_sha256(),
            prepared.range_index(),
            prepared.logical_genesis_sha256(),
            prepared.logical_history_sha256()
        )
        .is_err());
    let receipt = fixture
        .db
        .read()
        .persist_native_activation_decision(&prepared, &store, &quorum, &fixture.role(nonce))
        .unwrap();
    let mut forged = receipt.record.clone();
    forged.post_quorum_metadata_context_id = forged.provenance.metadata_context_id;
    forged.checksum_sha256 = forged.checksum().unwrap();
    assert!(
        forged.validate().is_err(),
        "a new role must not be inferred from old metadata counters"
    );
    forged = receipt.record.clone();
    forged
        .provenance
        .voters
        .values_mut()
        .next()
        .unwrap()
        .observed_metadata_read_index = forged.post_quorum_metadata_read_index + 1;
    forged.completion_quorum_sha256 = forged.provenance.checksum_sha256().unwrap();
    forged.checksum_sha256 = forged.checksum().unwrap();
    assert!(
        forged.validate().is_err(),
        "post-quorum metadata cannot predate a recorded voter's anchor"
    );
    forged = receipt.record.clone();
    forged.provenance.voters.clear();
    forged.completion_quorum_sha256 = forged.provenance.checksum_sha256().unwrap();
    forged.checksum_sha256 = forged.checksum().unwrap();
    assert!(
        forged.validate().is_err(),
        "rehashing an audit DTO cannot create the missing distinct majority"
    );
}

#[test]
fn native_activation_decision_immutable_history_corruption_cannot_mint_a_receipt() {
    let fixture = Fixture::new();
    let nonce = Uuid::now_v7();
    let (store, prepared, _frozen) = fixture.completed(nonce);
    let quorum = fixture.quorum(nonce, &store, &prepared);
    let receipt = fixture
        .db
        .read()
        .persist_native_activation_decision(&prepared, &store, &quorum, &fixture.role(nonce))
        .unwrap();
    // Deliberate test-only protected-page corruption, not a legal producer.
    let mut wrong = receipt.record.clone();
    wrong.native_page_xid += 1;
    wrong.checksum_sha256 = wrong.checksum().unwrap();
    fixture
        .db
        .read()
        .write_native_range_image_record(
            &decision_history_key(receipt.scope().range_id, receipt.checksum_sha256()),
            &wrong,
        )
        .unwrap();
    assert!(receipt.require_current(&fixture.db.read()).is_err());
    assert!(fixture
        .db
        .read()
        .observe_native_activation_decision_for_role(
            &fixture.role(nonce),
            receipt.scope(),
            receipt.checksum_sha256(),
            receipt.native_serving_sha256(),
            receipt.range_index(),
            receipt.logical_genesis_sha256(),
            receipt.logical_history_sha256()
        )
        .is_err());
    assert_eq!(
        store.recovery_observation_phase(prepared.scope()).unwrap(),
        crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen
    );
}

#[test]
fn native_activation_decision_strict_cold_open_reissues_only_actual_native_record() {
    let fixture = Fixture::new();
    let nonce = Uuid::now_v7();
    let (store, prepared, frozen) = fixture.completed(nonce);
    let quorum = fixture.quorum(nonce, &store, &prepared);
    let receipt = fixture
        .db
        .read()
        .persist_native_activation_decision(&prepared, &store, &quorum, &fixture.role(nonce))
        .unwrap();
    let path = fixture.db.read().data_path().to_path_buf();
    drop(quorum);
    drop(prepared);
    drop(frozen);
    drop(store);
    let Fixture {
        root,
        db,
        topology,
        spec,
    } = fixture;
    assert_eq!(Arc::strong_count(&db), 1);
    drop(db);
    let (reopened, _) = BicDb::open_range_image_database(
        &path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        &spec,
    )
    .unwrap();
    assert!(
        receipt.require_current(&reopened).is_err(),
        "old-open token must not survive reopening"
    );
    let range = topology.ranges.values().next().unwrap();
    let fresh = test_role_admission(
        // Reopen the SAME durable metadata stores; a fresh empty term-1
        // metadata cluster would correctly fail the post-decision anchor.
        &root.path().join(format!("decision-role-{nonce}")),
        &topology,
        &spec.node_id,
        &spec.node_id,
        &spec.node_id,
        range.id,
        range.epoch,
        nonce,
        MetadataRecoveryRolePurpose::NormalRecovery,
        Duration::from_secs(5),
    )
    .unwrap();
    let reissued = reopened
        .observe_native_activation_decision_for_role(
            &fresh,
            receipt.scope(),
            receipt.checksum_sha256(),
            receipt.native_serving_sha256(),
            receipt.range_index(),
            receipt.logical_genesis_sha256(),
            receipt.logical_history_sha256(),
        )
        .unwrap();
    assert_eq!(reissued.checksum_sha256(), receipt.checksum_sha256());
    assert_eq!(
        reissued.completion_quorum_sha256(),
        receipt.completion_quorum_sha256()
    );
    assert!(reopened
        .get("items", "stage-actual-origin-0")
        .unwrap()
        .is_some());
}
