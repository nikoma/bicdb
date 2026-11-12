//! Actual local physical readiness. This is deliberately neither a data-quorum
//! certificate, leadership authority, nor permission to serve a strong read.
use super::*;
use crate::distribution::{ClusterNodeId, RangeDescriptor, RangeId, RangeReplicaRole};
use crate::distribution_consensus::MetadataRecoveryRolePurpose;
use crate::distribution_range_consensus::{
    RangeWriteState, RangeWriteStore, RecoveryResolvedCatalogObservation,
};
use crate::distribution_range_image_install::FinalizedNativeRecoveryImage;
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
    fn native_produced_ready_projects_actual_point_under_same_owned_preparation() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let role = fixture.role(Uuid::now_v7());
        let frozen = fixture.freeze(&role);
        let scope = RecoveryFenceScope::from_role(&role).unwrap();
        let parent =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let mut store = fixture.store();
        let physical = fixture
            .attest(role, &mut store, Arc::clone(&frozen), &parent)
            .unwrap();
        let actual = (
            physical.range_index(),
            physical.logical_history_sha256().to_owned(),
            physical.native_receipt_sha256().to_owned(),
            physical.visibility().get(),
        );
        let request = crate::distribution_range_recovery::NativeReadyRequest {
            scope,
            challenge: Uuid::now_v7(),
            budget_ms: 4500,
        };
        let (prepared, evidence) = store
            .prepare_and_observe_native_recovery_ready(
                &fixture.db.read(),
                physical,
                &request,
                &parent,
            )
            .unwrap();
        let reply = evidence.into_reply();
        assert_eq!(
            (
                reply.range_index,
                reply.logical_history_sha256.clone(),
                reply.native_receipt_sha256.clone(),
                reply.local_visibility_sequence
            ),
            actual
        );
        assert_eq!(reply.challenge, request.challenge);
        assert!(reply.scope.same_session(&request.scope));
        assert_eq!(
            reply.observed_metadata_context_id,
            prepared.role().context_id()
        );
        prepared
            .require_current(&fixture.db.read(), &store)
            .unwrap();
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_enter(fixture.range().id)
            .is_err());
        assert!(store
            .require_recovery_scope(prepared.role(), &fixture.native(), prepared.scope())
            .is_err());
    }

    #[test]
    fn native_produced_ready_canceled_caller_and_foreign_scope_do_not_publish_preparation() {
        for canceled in [false, true] {
            let fixture = Fixture::new();
            fixture.seed(fixture.range().id, 1);
            let role = fixture.role(Uuid::now_v7());
            let frozen = fixture.freeze(&role);
            let scope = RecoveryFenceScope::from_role(&role).unwrap();
            let parent =
                CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
            let mut store = fixture.store();
            let physical = fixture
                .attest(role, &mut store, Arc::clone(&frozen), &parent)
                .unwrap();
            let request = crate::distribution_range_recovery::NativeReadyRequest {
                scope: if canceled {
                    scope.clone()
                } else {
                    RecoveryFenceScope::from_role(&fixture.role(Uuid::now_v7())).unwrap()
                },
                challenge: Uuid::now_v7(),
                budget_ms: 4500,
            };
            let path = fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG);
            let before = std::fs::read(&path).unwrap();
            if canceled {
                parent.cancel();
            } else {
                parent.check().unwrap();
                physical.role().require_live_request().unwrap();
            }
            let original_deadline = physical.role().deadline();
            let error = store
                .prepare_and_observe_native_recovery_ready(
                    &fixture.db.read(),
                    physical,
                    &request,
                    &parent,
                )
                .unwrap_err();
            if canceled {
                assert_eq!(error.to_string(), BicDbError::QueryCanceled.to_string());
            } else {
                parent.check().unwrap();
                assert!(std::time::Instant::now() < original_deadline);
                assert!(
                    error
                        .to_string()
                        .contains("recovery fence does not bind the independently fresh role"),
                    "{error}"
                );
            }
            assert_eq!(std::fs::read(&path).unwrap(), before);
            assert_eq!(
                store.recovery_observation_phase(&scope).unwrap(),
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery
            );
            assert_eq!(
                fixture
                    .db
                    .read()
                    .current_native_range_source_lineage(fixture.range().id)
                    .unwrap()
                    .range_index(),
                1
            );
        }
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
    fn ambiguous_native_prepare_persistence_refsyncs_exact_refusal_before_new_physical_proof() {
        for after_actual_write in [false, true] {
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
            assert!(store
                .test_prepare_native_recovery_ready_with(
                    &fixture.db.read(),
                    local,
                    |path, bytes, fsync| {
                        if after_actual_write {
                            crate::storage::write_atomic(path, bytes, fsync)?;
                        }
                        Err(invalid("injected preparation persistence ambiguity"))
                    }
                )
                .is_err());
            let fresh = fixture.role(nonce);
            let scope = RecoveryFenceScope::from_role(&fresh).unwrap();
            let native = fixture.native();
            assert!(store
                .observe_native_ready_resolved_prefix(&fresh, &native, &scope)
                .is_err());
            let wrong = fixture.role(Uuid::now_v7());
            assert!(store
                .refsync_native_prepared_refusal(
                    &fixture.db.read(),
                    &native,
                    &wrong,
                    &frozen,
                    &CancellationToken::uncancelable()
                )
                .is_err());
            store
                .refsync_native_prepared_refusal(
                    &fixture.db.read(),
                    &native,
                    &fresh,
                    &frozen,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            assert_eq!(
                store.recovery_observation_phase(&scope).unwrap(),
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared
            );
            assert!(store
                .require_recovery_scope(&fresh, &native, &scope)
                .is_err());
            let observed = store
                .observe_native_ready_resolved_prefix(&fresh, &native, &scope)
                .unwrap();
            let local = fixture
                .db
                .read()
                .attest_local_native_recovery_ready(
                    native,
                    fresh,
                    observed,
                    &store,
                    frozen,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            let reissued = store
                .prepare_native_recovery_ready(&fixture.db.read(), local)
                .unwrap();
            assert_eq!(reissued.range_index(), 1);
            reissued
                .require_current(&fixture.db.read(), &store)
                .unwrap();
        }
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
    fn corrupt_compacted_certification_marker_cannot_become_native_ready() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let role = fixture.role(Uuid::now_v7());
        let native = fixture.native();
        let scope = RecoveryFenceScope::from_role(&role).unwrap();
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        store.fence_recovery_epoch(&role, &native, &scope).unwrap();
        // This deliberately damaged persisted catalogue is a negative fixture,
        // not evidence that valid protocol operations can produce corruption.
        store
            .test_corrupt_compacted_marker(&scope, 1, "f".repeat(64))
            .unwrap();
        let observed = store
            .observe_recovery_resolved_prefix(&role, &native, &scope)
            .unwrap();
        let result = fixture.db.read().attest_local_native_recovery_ready(
            native,
            role,
            observed,
            &store,
            frozen,
            &CancellationToken::uncancelable(),
        );
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("certification marker conflicts"),
            "{error}"
        );
        assert_eq!(
            fixture
                .db
                .read()
                .native_range_prefix_observation(fixture.range().id)
                .unwrap()
                .native_applied_index,
            Some(1)
        );
    }

    fn actual_local_closed_ready_quorum(
        fixture: &Fixture,
        store: &RangeWriteStore,
        prepared: &crate::distribution_range_consensus::PreparedNativeRecoveryReady,
        nonce: Uuid,
        parent: &CancellationToken,
    ) -> crate::distribution_range_recovery::ClosedNativeReadyQuorum {
        use crate::distribution_range_recovery::{
            ClosedNativeReadyQuorumRound, NativeReadyObservation, NativeReadyRequest,
        };
        let fresh = fixture.role(nonce);
        let request = NativeReadyRequest {
            scope: RecoveryFenceScope::from_role(&fresh).unwrap(),
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let actual = store
            .observe_prepared_native_for_role(&fixture.db.read(), prepared, &fresh, &request)
            .unwrap();
        let participant = crate::distribution_consensus::MetadataReadParticipant::from_transport(
            fixture.spec.node_id.clone(),
            fixture.spec.member_incarnation,
            None,
            true,
        );
        let reply =
            NativeReadyObservation::from_local(actual, participant, &request, fresh.deadline())
                .unwrap();
        let mut round = ClosedNativeReadyQuorumRound::begin(fresh, parent).unwrap();
        round.record(reply).unwrap();
        round.finish().unwrap()
    }

    #[test]
    fn actual_closed_rf1_quorum_persists_new_serving_epoch_but_keeps_prepared_refusal() {
        let mut fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let physical_epoch = fixture.range().epoch;
        // A declared metadata epoch change, NOT an automatic election/HA test.
        fixture.topology.ranges.values_mut().next().unwrap().epoch += 1;
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
        let closed = actual_local_closed_ready_quorum(
            &fixture,
            &store,
            &prepared,
            nonce,
            &CancellationToken::uncancelable(),
        );
        let fresh = fixture.role(nonce);
        let receipt = fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(&prepared, &store, &closed, &fresh)
            .unwrap();
        receipt.require_current(&fixture.db.read()).unwrap();
        assert_eq!(receipt.range_index(), 1);
        let actual = fixture
            .db
            .read()
            .current_native_range_source_lineage(fixture.range().id)
            .unwrap();
        assert_eq!(actual.base_range().epoch, physical_epoch);
        assert_eq!(
            actual.native_write_range().unwrap().epoch,
            physical_epoch + 1
        );
        assert_eq!(actual.range_index(), 1);
        assert_eq!(
            actual.logical_history_sha256(),
            prepared.logical_history_sha256()
        );
        // Native persistence alone does not finish recovery or admit writes.
        assert!(store
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_controlled_mutation(&frozen)
            .is_err());
        prepared
            .require_current_under_role(&fixture.db.read(), &store, &fresh)
            .unwrap();
        // Same opaque quorum + actual state is an idempotent native-write retry.
        let retried = fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(&prepared, &store, &closed, &fresh)
            .unwrap();
        assert_eq!(receipt.checksum_sha256(), retried.checksum_sha256());
    }

    #[test]
    fn recovered_native_serving_epoch_survives_strict_cold_open_without_relabelling_capture() {
        let mut fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        fixture.topology.ranges.values_mut().next().unwrap().epoch += 1;
        let range_id = fixture.range().id;
        let epoch = fixture.range().epoch;
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
        let closed = actual_local_closed_ready_quorum(
            &fixture,
            &store,
            &prepared,
            nonce,
            &CancellationToken::uncancelable(),
        );
        let receipt = fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(
                &prepared,
                &store,
                &closed,
                &fixture.role(nonce),
            )
            .unwrap();
        let history = prepared.logical_history_sha256().to_owned();
        let path = fixture.db.read().data_path().to_path_buf();
        let blocked_command = fixture.conflicting_command();
        drop(closed);
        drop(receipt);
        drop(prepared);
        drop(frozen);
        drop(store);
        let Fixture {
            root: _root,
            db,
            topology,
            spec,
        } = fixture;
        assert_eq!(
            Arc::strong_count(&db),
            1,
            "no DB service owner may survive the cold-open cut"
        );
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
        let fingerprint = reopened.schema_compatibility_fingerprint().unwrap();
        assert_eq!(
            fingerprint.sha256,
            topology.nodes[&spec.node_id].labels
                [crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL]
        );
        let lineage = reopened
            .current_native_range_source_lineage(range_id)
            .unwrap();
        assert_eq!(lineage.base_range().epoch + 1, epoch);
        assert_eq!(lineage.native_write_range().unwrap().epoch, epoch);
        assert_eq!(lineage.range_index(), 1);
        assert_eq!(lineage.logical_history_sha256(), history);
        let reopened_store = RangeWriteStore::open(
            &path,
            topology.cluster_id.clone(),
            spec.node_id.clone(),
            true,
        )
        .unwrap();
        assert!(reopened_store
            .normal_native_write_allowed(&blocked_command)
            .is_err());
    }

    #[test]
    fn cancelled_closed_quorum_cannot_persist_a_serving_epoch() {
        let fixture = Fixture::new();
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let local = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), local)
            .unwrap();
        let parent =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let closed = actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &parent);
        parent.cancel();
        assert!(fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(
                &prepared,
                &store,
                &closed,
                &fixture.role(nonce)
            )
            .is_err());
        assert!(!format::native_recovery_serving_active(fixture.db.read().data_path()).unwrap());
        assert_eq!(
            fixture
                .db
                .read()
                .current_native_range_source_lineage(fixture.range().id)
                .unwrap()
                .native_write_range()
                .unwrap()
                .epoch,
            fixture.range().epoch
        );
    }

    #[test]
    fn actual_native_completion_stays_frozen_and_can_close_a_later_ready_round() {
        use crate::distribution_range_recovery::FrozenNativeReadyStage;
        let mut fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        fixture.topology.ranges.values_mut().next().unwrap().epoch += 1;
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
        let closed = actual_local_closed_ready_quorum(
            &fixture,
            &store,
            &prepared,
            nonce,
            &CancellationToken::uncancelable(),
        );
        let fresh = fixture.role(nonce);
        let receipt = store
            .complete_native_recovery_frozen(&fixture.db.read(), &prepared, closed, &fresh)
            .unwrap();
        receipt
            .require_current(&fixture.db.read(), &store, &prepared, &fresh)
            .unwrap();
        assert_eq!(
            prepared.frozen_stage().unwrap(),
            FrozenNativeReadyStage::NativeCompletedFrozen
        );
        assert_eq!(store.recovery_observation_phase(prepared.scope()).unwrap(),
            crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen);
        assert!(store
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_controlled_mutation(&frozen)
            .is_err());
        assert!(store
            .require_recovery_scope(&fresh, &fixture.native(), prepared.scope())
            .is_err());
        // Completion retains the ACTUAL same pin; later participants can still
        // collect its closed readiness without downgrading it to Prepared.
        let next_closed = actual_local_closed_ready_quorum(
            &fixture,
            &store,
            &prepared,
            nonce,
            &CancellationToken::uncancelable(),
        );
        let retried = store
            .complete_native_recovery_frozen(
                &fixture.db.read(),
                &prepared,
                next_closed,
                &fixture.role(nonce),
            )
            .unwrap();
        assert_eq!(
            retried.native_serving_sha256(),
            receipt.native_serving_sha256()
        );
        assert_eq!(
            prepared.frozen_stage().unwrap(),
            FrozenNativeReadyStage::NativeCompletedFrozen
        );
        // Even a dropped in-process owner never clears the durable refusal.
        drop(prepared);
        drop(frozen);
        drop(store);
        assert!(fixture
            .store()
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
    }

    #[test]
    fn native_produced_completion_direct_native_mutator_rejects_foreign_actual_store_open() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let physical = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), physical)
            .unwrap();
        let closed = actual_local_closed_ready_quorum(
            &fixture,
            &store,
            &prepared,
            nonce,
            &CancellationToken::uncancelable(),
        );
        // A real second Store open is a FOREIGN consumer, never a producer
        // substitute. Its durable bytes match; its actual ephemeral owner does not.
        let foreign = fixture.store();
        let fresh = fixture.role(nonce);
        let path = fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG);
        let before = std::fs::read(&path).unwrap();
        let serving = || {
            let db = fixture.db.read();
            let _journal = db.try_native_image_journal().unwrap();
            db.read_native_range_image_record::<serde_json::Value>(&format!(
                "serving-configuration/{}",
                fixture.range().id
            ))
            .unwrap()
        };
        let native_before = serving();
        let error = fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(&prepared, &foreign, &closed, &fresh)
            .unwrap_err();
        fresh.require_live_request().unwrap();
        closed.require_live().unwrap();
        assert!(
            error
                .to_string()
                .contains("prepared proof has stale actual store/physical state"),
            "{error}"
        );
        assert_eq!(serving(), native_before);
        assert_eq!(std::fs::read(path).unwrap(), before);
        prepared
            .require_current_under_role(&fixture.db.read(), &store, &fresh)
            .unwrap();
    }

    #[test]
    fn native_produced_completion_direct_native_mutator_keeps_original_quorum_cancellation() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let physical = fixture
            .attest(role, &mut store, frozen, &CancellationToken::uncancelable())
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), physical)
            .unwrap();
        let original =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let closed =
            actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &original);
        let fresh = fixture.role(nonce);
        let path = fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG);
        let before = std::fs::read(&path).unwrap();
        let serving = || {
            let db = fixture.db.read();
            let _journal = db.try_native_image_journal().unwrap();
            db.read_native_range_image_record::<serde_json::Value>(&format!(
                "serving-configuration/{}",
                fixture.range().id
            ))
            .unwrap()
        };
        let native_before = serving();
        original.cancel();
        fresh.require_live_request().unwrap();
        let error = fixture
            .db
            .read()
            .persist_native_recovered_serving_configuration(&prepared, &store, &closed, &fresh)
            .unwrap_err();
        assert_eq!(error.to_string(), BicDbError::QueryCanceled.to_string());
        fresh.require_live_request().unwrap();
        assert_eq!(serving(), native_before);
        assert_eq!(std::fs::read(path).unwrap(), before);
        prepared
            .require_current_under_role(&fixture.db.read(), &store, &fresh)
            .unwrap();
    }

    #[test]
    fn native_produced_completion_projects_actual_binding_and_stays_frozen() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let physical = fixture
            .attest(
                role,
                &mut store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), physical)
            .unwrap();
        let parent =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let closed = actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &parent);
        let fresh = fixture.role(nonce);
        let request = crate::distribution_range_recovery::NativeCompletionRequest {
            scope: prepared.scope().clone(),
            challenge: Uuid::now_v7(),
            budget_ms: 4500,
        };
        let path = fixture.db.read().data_path().join(DEFAULT_RANGE_WRITE_LOG);
        let before = std::fs::read(&path).unwrap();
        let uncompleted = store
            .observe_native_completion_for_role(&fixture.db.read(), &prepared, &fresh, &request)
            .unwrap_err();
        fresh.require_live_request().unwrap();
        parent.check().unwrap();
        assert!(
            uncompleted
                .to_string()
                .contains("native completion has not actually persisted"),
            "{uncompleted}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let reply = store
            .complete_and_observe_native_recovery_frozen(
                &fixture.db.read(),
                &prepared,
                closed,
                &fresh,
                &request,
                &parent,
            )
            .unwrap()
            .into_reply();
        assert_eq!(
            reply.stage,
            crate::distribution_range_recovery::NativeCompletionStage::CompletedFrozen
        );
        assert_eq!(reply.challenge, request.challenge);
        assert!(reply.scope.same_session(&request.scope));
        assert_eq!(reply.range_index, 1);
        assert_eq!(
            reply.logical_history_sha256,
            prepared.logical_history_sha256()
        );
        assert_eq!(
            reply.native_receipt_sha256,
            prepared.native_receipt_sha256()
        );
        let actual = prepared
            .require_completed_for_activation(&fixture.db.read(), &store, &fresh)
            .unwrap();
        assert_eq!(reply.native_serving_sha256, actual.checksum_sha256());
        let observed = store
            .observe_native_completion_for_role(&fixture.db.read(), &prepared, &fresh, &request)
            .unwrap()
            .into_reply();
        assert_eq!(observed.native_serving_sha256, reply.native_serving_sha256);
        prepared
            .require_current_under_role(&fixture.db.read(), &store, &fresh)
            .unwrap();
        assert!(store
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_enter(fixture.range().id)
            .is_err());
    }

    #[test]
    fn native_produced_completion_preserves_ambiguity_until_actual_refsync() {
        for after_actual_write in [false, true] {
            let fixture = Fixture::new();
            fixture.seed(fixture.range().id, 1);
            let nonce = Uuid::now_v7();
            let role = fixture.role(nonce);
            let frozen = fixture.freeze(&role);
            let mut store = fixture.store();
            let physical = fixture
                .attest(
                    role,
                    &mut store,
                    Arc::clone(&frozen),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            let prepared = store
                .prepare_native_recovery_ready(&fixture.db.read(), physical)
                .unwrap();
            let parent =
                CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
            let closed =
                actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &parent);
            let fresh = fixture.role(nonce);
            let request = crate::distribution_range_recovery::NativeCompletionRequest {
                scope: prepared.scope().clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 4500,
            };
            let error = store
                .test_complete_and_observe_native_recovery_frozen_with(
                    &fixture.db.read(),
                    &prepared,
                    closed,
                    &fresh,
                    &request,
                    &parent,
                    |path, bytes, fsync| {
                        if after_actual_write {
                            crate::storage::write_atomic(path, bytes, fsync)?;
                        }
                        Err(invalid("actual produced completion persistence ambiguity"))
                    },
                )
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("actual produced completion persistence ambiguity"));
            parent.check().unwrap();
            fresh.require_live_request().unwrap();
            assert!(store
                .observe_native_completion_for_role(&fixture.db.read(), &prepared, &fresh, &request)
                .is_err());
            assert!(prepared
                .refresh_after_durable_native_completion(&fixture.db.read(), &store, &fresh)
                .is_err());
            assert!(store
                .normal_native_write_allowed(&fixture.conflicting_command())
                .is_err());
            let renewed = fixture.role(nonce);
            store
                .refsync_native_prepared_refusal(
                    &fixture.db.read(),
                    &fixture.native(),
                    &renewed,
                    &frozen,
                    &parent,
                )
                .unwrap();
            prepared
                .refresh_after_durable_native_completion(&fixture.db.read(), &store, &renewed)
                .unwrap();
            let closed =
                actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &parent);
            let reply = store
                .complete_and_observe_native_recovery_frozen(
                    &fixture.db.read(),
                    &prepared,
                    closed,
                    &fixture.role(nonce),
                    &request,
                    &parent,
                )
                .unwrap()
                .into_reply();
            assert_eq!(reply.range_index, 1);
            assert_eq!(
                reply.logical_history_sha256,
                prepared.logical_history_sha256()
            );
            assert!(fixture
                .db
                .read()
                .range_mutation_gate()
                .try_enter(fixture.range().id)
                .is_err());
        }
    }

    #[test]
    fn native_produced_completion_caller_cancel_after_fsync_emits_no_observation() {
        let fixture = Fixture::new();
        fixture.seed(fixture.range().id, 1);
        let nonce = Uuid::now_v7();
        let role = fixture.role(nonce);
        let frozen = fixture.freeze(&role);
        let mut store = fixture.store();
        let physical = fixture
            .attest(
                role,
                &mut store,
                Arc::clone(&frozen),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let prepared = store
            .prepare_native_recovery_ready(&fixture.db.read(), physical)
            .unwrap();
        let parent =
            CancellationToken::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let closed = actual_local_closed_ready_quorum(&fixture, &store, &prepared, nonce, &parent);
        let fresh = fixture.role(nonce);
        let request = crate::distribution_range_recovery::NativeCompletionRequest {
            scope: prepared.scope().clone(),
            challenge: Uuid::now_v7(),
            budget_ms: 4500,
        };
        let error = store
            .test_complete_and_observe_native_recovery_frozen_with(
                &fixture.db.read(),
                &prepared,
                closed,
                &fresh,
                &request,
                &parent,
                |path, bytes, fsync| {
                    crate::storage::write_atomic(path, bytes, fsync)?;
                    parent.cancel();
                    Ok(())
                },
            )
            .unwrap_err();
        assert_eq!(error.to_string(), BicDbError::QueryCanceled.to_string());
        assert_eq!(store.recovery_observation_phase(prepared.scope()).unwrap(), crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen);
        assert!(store
            .normal_native_write_allowed(&fixture.conflicting_command())
            .is_err());
        assert!(fixture
            .db
            .read()
            .range_mutation_gate()
            .try_enter(fixture.range().id)
            .is_err());
    }

    #[test]
    fn ambiguous_native_completion_requires_exact_refsync_before_refreshing_held_readiness() {
        use crate::distribution_range_recovery::FrozenNativeReadyStage;
        for after_actual_write in [false, true] {
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
            let closed = actual_local_closed_ready_quorum(
                &fixture,
                &store,
                &prepared,
                nonce,
                &CancellationToken::uncancelable(),
            );
            let fresh = fixture.role(nonce);
            assert!(store
                .test_complete_native_recovery_frozen_with(
                    &fixture.db.read(),
                    &prepared,
                    closed,
                    &fresh,
                    |path, bytes, fsync| {
                        if after_actual_write {
                            crate::storage::write_atomic(path, bytes, fsync)?;
                        }
                        Err(invalid("injected completion persistence ambiguity"))
                    }
                )
                .is_err());
            assert!(prepared
                .require_current_under_role(&fixture.db.read(), &store, &fresh)
                .is_err());
            assert!(prepared
                .refresh_after_durable_native_completion(&fixture.db.read(), &store, &fresh)
                .is_err());
            assert!(store
                .normal_native_write_allowed(&fixture.conflicting_command())
                .is_err());
            let renewed = fixture.role(nonce);
            store
                .refsync_native_prepared_refusal(
                    &fixture.db.read(),
                    &fixture.native(),
                    &renewed,
                    &frozen,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            prepared
                .refresh_after_durable_native_completion(&fixture.db.read(), &store, &renewed)
                .unwrap();
            assert_eq!(
                prepared.frozen_stage().unwrap(),
                FrozenNativeReadyStage::NativeCompletedFrozen
            );
            let closed = actual_local_closed_ready_quorum(
                &fixture,
                &store,
                &prepared,
                nonce,
                &CancellationToken::uncancelable(),
            );
            store
                .complete_native_recovery_frozen(
                    &fixture.db.read(),
                    &prepared,
                    closed,
                    &fixture.role(nonce),
                )
                .unwrap();
            assert!(store
                .normal_native_write_allowed(&fixture.conflicting_command())
                .is_err());
        }
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
enum LocalNativeRecoveryCatalogue {
    Resolved(RecoveryResolvedCatalogObservation),
    FinalizedImage(Box<FinalizedNativeRecoveryImage>),
}
impl LocalNativeRecoveryCatalogue {
    fn resolved_index(&self) -> u64 {
        match self {
            Self::Resolved(observed) => observed.resolved_index(),
            Self::FinalizedImage(actual) => actual.base().range_index(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalNativeRecoveryReady {
    role: RecoveryRoleAdmission,
    native: NativeRecoveryAdmission,
    catalogue: LocalNativeRecoveryCatalogue,
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
    /// An exact physical image/adoption observation only. The catalogue owns
    /// the atomic intent→Prepared transition; callers cannot clear a refusal.
    pub(crate) fn finalized_image(&self) -> Option<&FinalizedNativeRecoveryImage> {
        match &self.catalogue {
            LocalNativeRecoveryCatalogue::FinalizedImage(image) => Some(image.as_ref()),
            LocalNativeRecoveryCatalogue::Resolved(_) => None,
        }
    }
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
        #[cfg(test)]
        let pin_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
            "refresh-owned-pin-admission-and-gate",
        );
        self.require_owned_pin_live(db)?;
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(pin_trace);
        #[cfg(test)]
        let lineage_trace =
            crate::distribution_range_handoff::NativeJointPhaseTrace::refresh_detail(
                "refresh-owned-full-physical-lineage",
            );

        let range_id = self.role.current_range().id;
        db.validate_source_range_image_lineage(&self.lineage, range_id, self.range_index())?;
        if self.visibility.0 != self.lineage.actual_local_visibility_sequence()
            || self.catalogue.resolved_index() != self.range_index()
        {
            return Err(invalid(
                "native materialization and resolved catalogue differ",
            ));
        }
        #[cfg(test)]
        crate::distribution_range_handoff::NativeJointPhaseTrace::finish_detail(lineage_trace);
        Ok(())
    }
    /// Necessary refusal only: actual DB open/health, owned Frozen and pin.
    /// This does not validate history/materialization or mint readiness. A
    /// scoped producer may call it at return after its full physical check.
    pub(crate) fn require_owned_pin_live(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let range_id = self.role.current_range().id;
        db.range_mutations.validate_frozen(range_id, &self.frozen)?;
        db.range_mutations
            .validate_source_capture_read_pin(range_id, &self.pin)?;
        Ok(())
    }
    pub(crate) fn require_current(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_physical_current(db)?;
        if matches!(
            self.role.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            store.require_current_joint_recovery_domain(db, &self.role)?;
        }
        store.require_actual_abort_controls_covered(
            db,
            self.role.current_range().id,
            self.range_index(),
        )?;
        match &self.catalogue {
            LocalNativeRecoveryCatalogue::Resolved(observed) => {
                store.revalidate_recovery_resolved_observation(
                    observed,
                    &self.role,
                    &self.native,
                )?;
            }
            LocalNativeRecoveryCatalogue::FinalizedImage(image) => {
                image.require_current(db, store, &self.role, &self.frozen)?;
            }
        }
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
        self.attest_native_recovery_materialization(
            native,
            role,
            LocalNativeRecoveryCatalogue::Resolved(catalogue),
            store,
            frozen,
            parent,
        )
    }

    /// Consumes the real Runtime image-return proof. It never opens a temporary
    /// NoImage window: the exact image intent/adoption MUST still be present.
    pub(crate) fn attest_finalized_native_recovery_image(
        &self,
        image: FinalizedNativeRecoveryImage,
        role: RecoveryRoleAdmission,
        store: &RangeWriteStore,
        frozen: Arc<FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<LocalNativeRecoveryReady> {
        parent.check()?;
        image.require_current(self, store, &role, &frozen)?;
        let native = self.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.attest_native_recovery_materialization(
            native,
            role,
            LocalNativeRecoveryCatalogue::FinalizedImage(Box::new(image)),
            store,
            frozen,
            parent,
        )
    }

    fn attest_native_recovery_materialization(
        &self,
        native: NativeRecoveryAdmission,
        role: RecoveryRoleAdmission,
        catalogue: LocalNativeRecoveryCatalogue,
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
            MetadataRecoveryRolePurpose::NormalRecovery
            | MetadataRecoveryRolePurpose::CurrentJointRecovery { .. } => role.recovery_nonce(),
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
        lineage.require_native_serving_role_floor(&role)?;
        let joint_fact = if matches!(
            role.purpose(),
            MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            Some(store.require_current_joint_recovery_domain(self, &role)?)
        } else {
            None
        };
        let base = if joint_fact.is_some() {
            lineage.native_write_range()?
        } else {
            lineage.base_range()
        };
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
            || (if joint_fact.is_some() {
                lineage.native_write_members()?
            } else {
                lineage.physical_range_members()?
            }) != members
            || !members.contains_key(role.local_node_id())
            || catalogue.resolved_index() != lineage.range_index()
        {
            return Err(invalid(
                "native anchor does not match the current physical recovery scope",
            ));
        }
        store.require_actual_abort_controls_covered(self, range.id, lineage.range_index())?;
        match &catalogue {
            LocalNativeRecoveryCatalogue::Resolved(observed) => {
                store.verify_native_ready_catalog(
                    observed,
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
                            (RangeWriteState::Aborted, NativeLogicalHistoryOutcomeV1::Aborted) => {
                                true
                            }
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
                store.verify_native_ready_historical_markers(observed, &role, &native, |index, checksum| {
            cancellation.check()?;
            let step = self.export_native_logical_history_step_with_source_pin(
                &lineage, &frozen, &pin, index, 1, &cancellation)?;
            if !matches!(step.entries.first().map(|entry| &entry.outcome),
                Some(NativeLogicalHistoryOutcomeV1::Applied { command_checksum_sha256 }) if command_checksum_sha256 == checksum)
            {
                return Err(invalid("durable historical certification marker conflicts with actual native history"));
            }
            Ok(())
        })?;
            }
            LocalNativeRecoveryCatalogue::FinalizedImage(image) => {
                image.require_current(self, store, &role, &frozen)?;
                store.verify_finalized_native_image_historical_markers(
                    image, self, &role, &frozen, |index, checksum| {
                        cancellation.check()?;
                        let step = self.export_native_logical_history_step_with_source_pin(
                            &lineage, &frozen, &pin, index, 1, &cancellation,
                        )?;
                        if !matches!(step.entries.first().map(|entry| &entry.outcome),
                            Some(NativeLogicalHistoryOutcomeV1::Applied { command_checksum_sha256 })
                                if command_checksum_sha256 == checksum)
                        {
                            return Err(invalid("finalized image historical certification marker conflicts with actual native history"));
                        }
                        Ok(())
                    },
                )?;
            }
        }
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
