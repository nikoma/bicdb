//! Actual local physical readiness. This is deliberately neither a data-quorum
//! certificate, leadership authority, nor permission to serve a strong read.
use super::*;
use crate::distribution::{ClusterNodeId, RangeDescriptor, RangeId, RangeReplicaRole};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{
    RangeWriteState, RangeWriteStore, RecoveryResolvedCatalogObservation,
};
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, SourceCaptureReadPin};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

fn invalid(message: &str) -> BicDbError {
    BicDbError::Cluster(format!("local native recovery readiness: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution_range_consensus::{
        native_origin_fixture, native_ready_test_command, seed_actual_origin, RangeWriteCommand,
        RecoveryFenceScope, DEFAULT_RANGE_WRITE_LOG,
    };
    use crate::distribution_range_recovery::test_role_admission;
    use crate::ClusterTopology;

    struct Fixture {
        root: tempfile::TempDir,
        db: Arc<RwLock<BicDb>>,
        topology: ClusterTopology,
        spec: RangeImageBirthSpec,
    }
    impl Fixture {
        fn new() -> Self {
            let (root, db, topology, spec) = native_origin_fixture();
            Self {
                root,
                db,
                topology,
                spec,
            }
        }
        fn range(&self) -> &RangeDescriptor {
            self.topology.ranges.values().next().unwrap()
        }
        fn seed(&self, range: RangeId, count: usize) {
            seed_actual_origin(&self.db, self.topology.clone(), &self.spec, range, count);
        }
        fn role(&self, nonce: Uuid) -> RecoveryRoleAdmission {
            test_role_admission(
                &self.root.path().join(format!("fresh-role-{nonce}")),
                &self.topology,
                &self.spec.node_id,
                &self.spec.node_id,
                &self.spec.node_id,
                self.range().id,
                self.range().epoch,
                nonce,
                MetadataRecoveryRolePurpose::NormalRecovery,
                Duration::from_secs(5),
            )
            .unwrap()
        }
        fn store(&self) -> RangeWriteStore {
            RangeWriteStore::open(
                self.db.read().data_path(),
                self.topology.cluster_id.clone(),
                self.spec.node_id.clone(),
                true,
            )
            .unwrap()
        }
        fn native(&self) -> NativeRecoveryAdmission {
            self.db
                .read()
                .native_recovery_admission(
                    &self.topology.cluster_id,
                    self.spec.bootstrap_id,
                    &self.spec.node_id,
                    self.spec.member_incarnation,
                )
                .unwrap()
        }
        fn freeze(&self, role: &RecoveryRoleAdmission) -> Arc<FrozenRangeMutationPermit> {
            Arc::new(
                self.db
                    .read()
                    .range_mutation_gate()
                    .try_freeze(self.range().id, role.recovery_nonce())
                    .unwrap(),
            )
        }
        fn attest(
            &self,
            role: RecoveryRoleAdmission,
            store: &mut RangeWriteStore,
            frozen: Arc<FrozenRangeMutationPermit>,
            token: &CancellationToken,
        ) -> Result<LocalNativeRecoveryReady> {
            let native = self.native();
            let scope = RecoveryFenceScope::from_role(&role)?;
            store.fence_recovery_epoch(&role, &native, &scope)?;
            let observed = store.observe_recovery_resolved_prefix(&role, &native, &scope)?;
            self.db
                .read()
                .attest_local_native_recovery_ready(native, role, observed, store, frozen, token)
        }
        fn conflicting_command(&self) -> RangeWriteCommand {
            let key = (0..)
                .map(|i| format!("different-native-command-{i}"))
                .find(|key| {
                    self.topology.range_for_key("items", key).unwrap().id == self.range().id
                })
                .unwrap();
            native_ready_test_command(
                self.topology.cluster_id.clone(),
                self.range(),
                1,
                self.spec.node_id.clone(),
                &CommitAdmissionIntent {
                    transaction_id: 999,
                    mutations: vec![CommitAdmissionMutation {
                        collection: "items".into(),
                        record_id: key.clone(),
                        record: Some(Record::new(key)),
                    }],
                },
            )
            .unwrap()
        }
        fn mark_catalogue_applied(store: &mut RangeWriteStore, command: &RangeWriteCommand) {
            // Deliberately only the actual durable logical store. This models
            // a counter/label claiming Apply without corresponding native rows.
            store.prepare(command.clone()).unwrap();
            store.commit(command).unwrap();
            store.certify(command).unwrap();
            store.mark_applied(command).unwrap();
        }
    }

    #[test]
    fn actual_native_preparation_stops_mutation_and_survives_store_reopen() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 2);
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let gate = fixture.db.read().range_mutation_gate();
        let mut store = fixture.store();
        let local = fixture
            .attest(
                role,
                &mut store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .unwrap();
        assert_eq!(prepared.range_index(), 2);
        assert_eq!(prepared.logical_history_sha256().len(), 64);
        assert_eq!(prepared.native_receipt_sha256().len(), 64);
        assert!(prepared.local_visibility_sequence() >= prepared.range_index());
        prepared
            .require_current(&fixture.db.read(), &store)
            .unwrap();
        let scope = RecoveryFenceScope::from_role(prepared.role()).unwrap();
        let native = fixture.native();
        assert!(store
            .require_recovery_scope(prepared.role(), &native, &scope)
            .is_err());
        assert!(store
            .fence_recovery_epoch(prepared.role(), &native, &scope)
            .is_err());
        assert!(store
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert!(gate.try_controlled_mutation(&frozen).is_err());
        assert_eq!(
            store.recovery_observation_phase(&scope).unwrap(),
            crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared
        );
        drop(prepared);
        drop(frozen);
        // Dropping the in-process freeze cannot clear the durable refusal.
        assert!(gate.try_enter(fixture.range().id).is_ok());
        drop(store);
        let reopened = fixture.store();
        assert!(reopened
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert_eq!(
            reopened.recovery_observation_phase(&scope).unwrap(),
            crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared
        );
    }

    #[test]
    fn prepared_replica_can_be_reattested_by_a_fresh_same_session_role() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let local = fixture
            .attest(
                role,
                &mut store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .unwrap();
        let point = (
            prepared.range_index(),
            prepared.logical_history_sha256().to_owned(),
            prepared.native_receipt_sha256().to_owned(),
            prepared.local_visibility_sequence(),
        );
        drop(prepared);
        let role = fixture.role(nonce);
        let native = fixture.native();
        let scope = RecoveryFenceScope::from_role(&role).unwrap();
        // Historical replay still cannot use the prepared state.
        assert!(store
            .observe_recovery_resolved_prefix(&role, &native, &scope)
            .is_err());
        let observed = store
            .observe_native_ready_resolved_prefix(&role, &native, &scope)
            .unwrap();
        let local = fixture
            .db
            .read()
            .attest_local_native_recovery_ready(
                native,
                role,
                observed,
                &store,
                frozen,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let reissued = store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .unwrap();
        assert_eq!(
            (
                reissued.range_index(),
                reissued.logical_history_sha256().to_owned(),
                reissued.native_receipt_sha256().to_owned(),
                reissued.local_visibility_sequence()
            ),
            point
        );
        reissued
            .require_current(&fixture.db.read(), &store)
            .unwrap();
    }

    #[test]
    fn durable_preparation_requires_new_authority_after_original_request_cancellation() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let token =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let local = fixture.attest(role, &mut store, frozen, &token).unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .unwrap();
        token.cancel();
        assert!(prepared
            .require_current(&fixture.db.read(), &store)
            .is_err());
        // The saved materialized point may outlive its original request, but
        // only an actual newly challenged role for the SAME session can use it.
        let fresh = fixture.role(nonce);
        prepared
            .require_current_under_role(&fixture.db.read(), &store, &fresh)
            .unwrap();
        let wrong = fixture.role(Uuid::now_v7());
        assert!(prepared
            .require_current_under_role(&fixture.db.read(), &store, &wrong)
            .is_err());
        assert!(store
            .require_recovery_scope(&fresh, &fixture.native(), prepared.scope())
            .is_err());
    }

    #[test]
    fn cancelled_local_ready_cannot_persist_a_native_preparation() {
        let fixture = Fixture::new();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let token =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let local = fixture.attest(role, &mut store, frozen, &token).unwrap();
        let scope = RecoveryFenceScope::from_role(local.role()).unwrap();
        token.cancel();
        assert!(store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .is_err());
        assert_eq!(
            store.recovery_observation_phase(&scope).unwrap(),
            crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery
        );
    }

    #[test]
    fn actual_empty_genesis_ready_owns_the_read_pin_until_proof_drop() {
        let fixture = Fixture::new();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let proof = fixture
            .attest(
                role,
                &mut store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        assert_eq!(proof.range_index(), 0);
        proof.require_current(&fixture.db.read(), &store).unwrap();
        let gate = fixture.db.read().range_mutation_gate();
        assert!(gate.try_enter(fixture.range().id).is_err());
        assert!(gate.try_controlled_mutation(&frozen).is_err());
        drop(proof);
        drop(gate.try_controlled_mutation(&frozen).unwrap());
        assert!(gate.try_enter(fixture.range().id).is_err());
        drop(frozen);
        assert!(gate.try_enter(fixture.range().id).is_ok());
    }

    #[test]
    fn actual_nonempty_native_prefix_uses_local_mvcc_sequence_not_range_index() {
        let fixture = Fixture::new();
        let selected = fixture.range().id;
        let other = fixture
            .topology
            .ranges
            .values()
            .find(|range| range.id != selected)
            .unwrap()
            .id;
        fixture.seed(other, 1);
        fixture.seed(selected, 2);
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let proof = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap();
        assert_eq!(proof.range_index(), 2);
        assert!(proof.visibility().get() > proof.range_index());
        let database = fixture.db.read();
        let mut tx = database.begin_transaction().unwrap();
        tx.set_snapshot_isolation(true, proof.visibility().get())
            .unwrap();
        let snapshot = tx
            .capture_record_prefix_snapshot_unchecked("items", "stage-actual-origin-", 1024 * 1024)
            .unwrap();
        let keys = (0..)
            .map(|i| format!("stage-actual-origin-{i}"))
            .filter(|key| fixture.topology.range_for_key("items", key).unwrap().id == selected)
            .take(2)
            .collect::<Vec<_>>();
        for key in keys {
            assert!(
                snapshot.get(&database, &key).unwrap().is_some(),
                "acknowledged row {key}"
            );
        }
        proof.require_current(&database, &store).unwrap();
    }

    #[test]
    fn catalogue_applied_without_native_materialization_cannot_become_ready() {
        let fixture = Fixture::new();
        let mut store = fixture.store();
        Fixture::mark_catalogue_applied(&mut store, &fixture.conflicting_command());
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let error = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap_err();
        assert!(error.to_string().contains("native anchor"), "{error}");
        assert_eq!(
            fixture
                .db
                .read()
                .native_range_prefix_observation(fixture.range().id)
                .unwrap()
                .native_applied_index,
            Some(0)
        );
    }

    #[test]
    fn equal_index_conflicting_actual_catalogue_and_native_history_is_rejected() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        // Characterize a rebuilt/different logical catalogue at the same R;
        // create it through actual store transitions, not private field edits.
        fs::remove_file(fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
        let mut store = fixture.store();
        Fixture::mark_catalogue_applied(&mut store, &fixture.conflicting_command());
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let error = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("conflicts with actual native history"),
            "{error}"
        );
    }

    #[test]
    fn actual_native_prefix_ahead_of_rebuilt_catalogue_requires_adoption_not_numeric_floor() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        fs::remove_file(fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
        let mut store = fixture.store();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        assert!(fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .is_err());
        assert_eq!(store.progress(fixture.range().id).resolved_through, 0);
    }

    #[test]
    fn original_parent_cancellation_invalidates_an_actual_local_ready_proof() {
        let fixture = Fixture::new();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let cancelled = Arc::new(AtomicBool::new(false));
        let parent = CancellationToken::new(Arc::clone(&cancelled), None);
        let proof = fixture.attest(role, &mut store, frozen, &parent).unwrap();
        cancelled.store(true, AtomicOrdering::SeqCst);
        assert!(proof.require_current(&fixture.db.read(), &store).is_err());
    }

    #[test]
    fn same_identity_catalogue_from_another_actual_native_database_is_rejected() {
        let fixture = Fixture::new();
        let other_path = fixture.root.path().join("different-physical-database");
        let mut other_spec = fixture.spec.clone();
        other_spec.initial_ranges.clear();
        let (_other_database, _) = BicDb::create_new_range_image_database(
            &other_path,
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            other_spec,
        )
        .unwrap();
        let mut other_store = RangeWriteStore::open(
            &other_path,
            fixture.topology.cluster_id.clone(),
            fixture.spec.node_id.clone(),
            true,
        )
        .unwrap();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let error = fixture
            .attest(
                role,
                &mut other_store,
                frozen,
                &CancellationToken::uncancelable(),
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("another physical database"),
            "{error}"
        );
    }

    #[test]
    fn actual_reopen_invalidates_the_prior_open_local_readiness_proof() {
        let fixture = Fixture::new();
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let proof = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap();
        let path = fixture.db.read().data_path().to_path_buf();
        let Fixture {
            root: _root,
            db,
            topology: _topology,
            spec,
        } = fixture;
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
        assert!(proof.require_current(&reopened, &store).is_err());
    }
}

/// Actual target-local native materialization, never range R, source B, or a
/// page-WAL transaction ID. Only the producer below constructs this type.
#[derive(Debug)]
pub(crate) struct NativeLocalVisibilitySequence(u64);
impl NativeLocalVisibilitySequence {
    pub(crate) fn get(&self) -> u64 {
        self.0
    }
}

/// Owns the local read pin and its freeze for the entire proof lifetime. No
/// serde/Clone/numeric constructor: callers cannot turn a reported watermark
/// into a physical proof, or release the pin while retaining this value.
#[derive(Debug)]
pub(crate) struct LocalNativeRecoveryReady {
    role: RecoveryRoleAdmission,
    native: NativeRecoveryAdmission,
    catalogue: RecoveryResolvedCatalogObservation,
    lineage: SourceRangeImageLineage,
    visibility: NativeLocalVisibilitySequence,
    cancellation: CancellationToken,
    pin: SourceCaptureReadPin,
    frozen: Arc<FrozenRangeMutationPermit>,
}

fn voters(range: &RangeDescriptor) -> BTreeMap<crate::ReplicaId, ClusterNodeId> {
    range
        .replicas
        .iter()
        .filter(|replica| replica.role == RangeReplicaRole::Voter)
        .map(|replica| (replica.id, replica.node_id.clone()))
        .collect()
}

impl LocalNativeRecoveryReady {
    pub(crate) fn physical_base_range(&self) -> &RangeDescriptor {
        self.lineage.base_range()
    }
    /// Reuses this actual owned source pin; a reported history frame cannot
    /// be substituted by the caller. Every bounded step revalidates the local
    /// native receipt, DB open, catalogue and original request cancellation.
    pub(crate) fn export_actual_history_step(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        first_index: u64,
        cancellation: &CancellationToken,
    ) -> Result<NativeLogicalHistoryStepV1> {
        self.require_current(db, store)?;
        let token = cancellation.child_with_deadline(self.role.deadline());
        let step = db.export_native_logical_history_step_with_source_pin(
            &self.lineage,
            &self.frozen,
            &self.pin,
            first_index,
            256,
            &token,
        )?;
        self.require_current(db, store)?;
        Ok(step)
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.role
    }
    pub(crate) fn range_index(&self) -> u64 {
        self.lineage.range_index()
    }
    pub(crate) fn logical_history_sha256(&self) -> &str {
        self.lineage.logical_history_sha256()
    }
    pub(crate) fn logical_genesis_sha256(&self) -> &str {
        self.lineage.logical_genesis_sha256()
    }
    pub(crate) fn native_receipt_sha256(&self) -> &str {
        self.lineage.native_receipt_sha256()
    }
    pub(crate) fn visibility(&self) -> &NativeLocalVisibilitySequence {
        &self.visibility
    }
    pub(crate) fn native_admission(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    /// Physical half of this owned proof. Catalogue preparation deliberately
    /// changes its catalogue commitment, so the prepared producer must validate
    /// the new durable record separately while retaining this SAME read pin.
    pub(crate) fn require_physical_current(&self, db: &BicDb) -> Result<()> {
        self.cancellation.check()?;
        self.role.require_live_request()?;
        self.require_owned_materialization_current(db)
    }
    /// Pure materialization validation under the SAME owned pin. A durable
    /// preparation may outlive the request which created it; its consumer must
    /// independently validate a fresh role and the exact saved preparation.
    pub(crate) fn require_owned_materialization_current(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let range_id = self.role.current_range().id;
        db.range_mutations.validate_frozen(range_id, &self.frozen)?;
        db.range_mutations
            .validate_source_capture_read_pin(range_id, &self.pin)?;
        db.validate_source_range_image_lineage(&self.lineage, range_id, self.range_index())?;
        if self.visibility.0 != self.lineage.actual_local_visibility_sequence()
            || self.catalogue.resolved_index() != self.range_index()
        {
            return Err(invalid(
                "native materialization and resolved catalogue differ",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_physical_current(db)?;
        store.revalidate_recovery_resolved_observation(
            &self.catalogue,
            &self.role,
            &self.native,
        )?;
        Ok(())
    }
}

impl BicDb {
    /// Read-only local producer. Caller has already drained the actual range
    /// outside DB/service latches and persisted the exact recovery session. The
    /// local catalogue can still be behind the cluster; a separate actual data
    /// quorum and history decision protocol MUST establish leadership readiness.
    pub(crate) fn attest_local_native_recovery_ready(
        &self,
        native: NativeRecoveryAdmission,
        role: RecoveryRoleAdmission,
        catalogue: RecoveryResolvedCatalogObservation,
        store: &RangeWriteStore,
        frozen: Arc<FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<LocalNativeRecoveryReady> {
        let cancellation = parent.child_with_deadline(role.deadline());
        cancellation.check()?;
        role.require_live_request()?;
        native.require_current(self)?;
        let range = role.current_range();
        let owner = match role.purpose() {
            MetadataRecoveryRolePurpose::NormalRecovery => role.recovery_nonce(),
            MetadataRecoveryRolePurpose::CommittedHandoff { session_id, .. } => *session_id,
            MetadataRecoveryRolePurpose::ObserveFencedRecovery { .. } => {
                return Err(invalid(
                    "read-only metadata observer cannot certify readiness",
                ));
            }
        };
        self.range_mutations.validate_frozen(range.id, &frozen)?;
        if frozen.owner() != owner {
            return Err(invalid(
                "actual freeze owner differs from the recovery session",
            ));
        }
        let pin = self
            .range_mutations
            .try_source_capture_read_pin(range.id, &frozen)?;
        let lineage = self.current_native_range_source_lineage(range.id)?;
        let base = lineage.base_range();
        let members = role
            .member_credentials()
            .iter()
            .map(|(node, identity)| {
                (
                    node.clone(),
                    RangeImageBirthMember {
                        incarnation: identity.incarnation,
                        tls_certificate_sha256: identity.active_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: identity.pending_certificate_sha256.clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        // A physical anchor remains valid through leadership/epoch changes
        // only with the SAME voter identities, span, schema and restore domain.
        // This grants no new-epoch write/read authority. Membership changes
        // require an actual installed/rebound anchor, not intended placement.
        if lineage.cluster_id() != role.cluster_id()
            || lineage.bootstrap_id() != role.bootstrap_id()
            || lineage.local_node_id() != role.local_node_id()
            || lineage.local_member_incarnation() != role.local_member_incarnation()
            || lineage.restore_activation() != role.restore_activation()
            || Some(lineage.schema_sha256()) != role.expected_schema_sha256()
            || base.id != range.id
            || base.start_token != range.start_token
            || base.end_token != range.end_token
            || base.placement != range.placement
            || base.epoch > range.epoch
            || voters(base) != voters(range)
            || lineage.base_members() != &members
            || !members.contains_key(role.local_node_id())
            || catalogue.resolved_index() != lineage.range_index()
        {
            return Err(invalid(
                "native anchor does not match the current physical recovery scope",
            ));
        }
        store.verify_native_ready_catalog(
            &catalogue,
            &role,
            &native,
            self.data_path(),
            |entry| {
                cancellation.check()?;
                let step = self.export_native_logical_history_step_with_source_pin(
                    &lineage,
                    &frozen,
                    &pin,
                    entry.command.index,
                    1,
                    &cancellation,
                )?;
                let actual = step
                    .entries
                    .first()
                    .ok_or_else(|| invalid("missing native decision"))?;
                let matching = match (&entry.state, &actual.outcome) {
                    (
                        RangeWriteState::Applied,
                        NativeLogicalHistoryOutcomeV1::Applied {
                            command_checksum_sha256,
                        },
                    ) => command_checksum_sha256 == &entry.command.checksum_sha256,
                    (RangeWriteState::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => true,
                    _ => false,
                };
                if !matching {
                    return Err(invalid(
                        "retained catalogue decision conflicts with actual native history",
                    ));
                }
                Ok(())
            },
        )?;
        let proof = LocalNativeRecoveryReady {
            visibility: NativeLocalVisibilitySequence(lineage.actual_local_visibility_sequence()),
            role,
            native,
            catalogue,
            lineage,
            cancellation,
            pin,
            frozen,
        };
        proof.require_current(self, store)?;
        store.require_prepared_native_point_matches(&proof)?;
        Ok(proof)
    }
}
