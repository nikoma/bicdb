//! Receiver-owned recovery sessions. Metadata/network waits and actual range
//! drain happen outside service/DB latches. A durable fence retains its freeze
//! even when its response is lost or the caller's one-shot budget expires.
use super::*;
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_mutation::{FrozenRangeMutationPermit, RangeMutationGate};
use crate::CancellationToken;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::Arc;

mod participants;

const MAX_CONTEXTS: usize = 4;
const MAX_SESSIONS: usize = 64;
const LATCH_POLL: Duration = Duration::from_millis(5);

#[derive(Debug)]
struct Session {
    scope: RecoveryFenceScope,
    frozen: Option<Arc<FrozenRangeMutationPermit>>,
}
#[derive(Debug, Default)]
struct Sessions {
    current: BTreeMap<RangeId, Session>,
    active: BTreeSet<RangeId>,
    contexts: usize,
}

#[derive(Debug)]
pub(crate) struct RangeRecoveryRuntime {
    service: Arc<Mutex<ClusterDataNodeService>>,
    provider: Arc<MetadataReadProvider>,
    local_node_id: ClusterNodeId,
    gate: RangeMutationGate,
    sessions: Arc<Mutex<Sessions>>,
    participant_transport:
        Option<Arc<dyn crate::distribution_range_consensus::RangeWriteTransport>>,
}
struct Operation {
    sessions: Arc<Mutex<Sessions>>,
    range: RangeId,
    exclusive: bool,
}
impl Drop for Operation {
    fn drop(&mut self) {
        let mut sessions = self.sessions.lock();
        sessions.contexts -= 1;
        if self.exclusive {
            sessions.active.remove(&self.range);
        }
    }
}
/// Exclusive owned installer lease. Dropping it preserves an A-owned freeze;
/// only actual catalogue adoption may later authorize returning to recovery N.
/// There is deliberately no extraction or Clone operation.
pub(crate) struct RecoveryImageInstallFreeze {
    runtime: Arc<RangeRecoveryRuntime>,
    scope: RecoveryFenceScope,
    frozen: Option<FrozenRangeMutationPermit>,
    preflight: crate::distribution_range_consensus::NativeImageInstallTransferPreflight,
}
impl std::fmt::Debug for RecoveryImageInstallFreeze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryImageInstallFreeze")
            .field("range", &self.scope.range_id)
            .field("frozen", &self.frozen)
            .finish_non_exhaustive()
    }
}
impl RecoveryImageInstallFreeze {
    pub(crate) fn as_frozen(&self) -> &FrozenRangeMutationPermit {
        self.frozen
            .as_ref()
            .expect("owned image freeze consumed only during Drop")
    }
    pub(crate) fn preflight(
        &self,
    ) -> &crate::distribution_range_consensus::NativeImageInstallTransferPreflight {
        &self.preflight
    }
}
impl Drop for RecoveryImageInstallFreeze {
    fn drop(&mut self) {
        let Some(frozen) = self.frozen.take() else {
            return;
        };
        let mut sessions = self.runtime.sessions.lock();
        let session = sessions
            .current
            .get_mut(&self.scope.range_id)
            .expect("leased recovery session remains retained");
        // No runtime operation can replace a session whose freeze is leased.
        assert!(session.frozen.is_none() && session.scope.same_session(&self.scope));
        session.frozen = Some(Arc::new(frozen));
    }
}
impl RangeRecoveryRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn transfer_image_install(
        self: &Arc<Self>,
        scope: &RecoveryFenceScope,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &crate::distribution_range_consensus::NativeImageInstallRole<'_>,
        purpose: crate::distribution_range_consensus::ImageInstallPurpose,
        attempt_id: Uuid,
        parent: &CancellationToken,
    ) -> Result<RecoveryImageInstallFreeze> {
        let _operation = self.reserve(scope.range_id)?;
        scope.require_role(role.role())?;
        let token = parent.child_with_deadline(role.role().deadline());
        let original = self.source_capture_freeze(scope)?;
        let preflight = {
            let mut service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.preflight_recovery_image_transfer(
                source, role, &original, purpose, attempt_id, &token,
            )?
        };
        if preflight.range_id() != scope.range_id
            || preflight.nonce() != scope.recovery_nonce
            || preflight.session_id() != scope.freeze_owner()?
            || preflight.original_freeze_owner() != scope.freeze_owner()?
        {
            return Err(invalid(
                "actual image transfer preflight has a different recovery session",
            ));
        }
        // Discard our temporary read reference. No source/read callback may
        // retain another Arc when the only owner changes N -> attempt A.
        drop(original);
        token.check()?;
        let mut sessions = self.sessions.lock();
        let session = sessions
            .current
            .get_mut(&scope.range_id)
            .ok_or_else(|| invalid("recovery session disappeared before image transfer"))?;
        if !session.scope.same_session(scope) {
            return Err(invalid("recovery session changed before image transfer"));
        }
        let arc = session
            .frozen
            .as_mut()
            .ok_or_else(|| invalid("recovery freeze is already leased"))?;
        let frozen = Arc::get_mut(arc)
            .ok_or_else(|| invalid("source/callback still owns recovery freeze"))?;
        if frozen.owner() != preflight.original_freeze_owner() {
            return Err(invalid(
                "recovery freeze owner changed before image transfer",
            ));
        }
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("service is busy before image freeze transfer"))?;
        service.rotate_recovery_image_transfer(&preflight, source, role, frozen, &token)?;
        drop(service);
        let arc = session.frozen.take().unwrap();
        let frozen =
            Arc::try_unwrap(arc).expect("unique recovery freeze checked under session lock");
        Ok(RecoveryImageInstallFreeze {
            runtime: Arc::clone(self),
            scope: scope.clone(),
            frozen: Some(frozen),
            preflight,
        })
    }
    /// Read-only freeze sharing. SourceCapture must atomically acquire its
    /// own exclusive read pin; this Arc cannot grant a frozen write bypass.
    pub(crate) fn source_capture_freeze(
        &self,
        scope: &RecoveryFenceScope,
    ) -> Result<Arc<FrozenRangeMutationPermit>> {
        let sessions = self.sessions.lock();
        let session = sessions
            .current
            .get(&scope.range_id)
            .ok_or_else(|| invalid("no retained recovery source freeze"))?;
        if !session.scope.same_session(scope)
            || session
                .frozen
                .as_ref()
                .ok_or_else(|| invalid("recovery freeze is leased to an image installer"))?
                .owner()
                != scope.freeze_owner()?
        {
            return Err(invalid(
                "source freeze is from another actual recovery session",
            ));
        }
        let frozen = session
            .frozen
            .as_ref()
            .ok_or_else(|| invalid("recovery freeze is leased to an image installer"))?;
        self.gate
            .validate_quiescent_frozen(scope.range_id, frozen)?;
        Ok(Arc::clone(frozen))
    }
    pub(crate) fn observe_history(
        &self,
        observer: &MetadataReadParticipant,
        request: &RecoveryFencedProbeRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve_observer(request.scope.range_id)?;
        self.observe_history_inner(observer, request, &token)
    }
    fn observe_history_inner(
        &self,
        observer: &MetadataReadParticipant,
        request: &RecoveryFencedProbeRequest,
        token: &CancellationToken,
    ) -> Result<RecoveryHistoryObservation> {
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let scope = &request.scope;
        let purpose = RecoveryObservationRoleAdmission::purpose(scope)?;
        let witness = self.provider.witness_recovery_role(
            observer,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            purpose,
            &token,
        )?;
        let role = RecoveryObservationRoleAdmission::from_witness(
            witness,
            observer,
            &self.local_node_id,
            business,
            challenge,
            scope,
        )?;
        let token = token.child_with_deadline(role.deadline());
        // This cannot create/refresh a durable fence; the service below must
        // find the exact already-persisted session and native DB incarnation.
        {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.require_persisted_recovery_scope(scope)?;
        }
        let frozen = self.frozen(scope, &token, false)?;
        let service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let reply = service.observe_native_recovery_history(&role, request, &frozen, &token)?;
        token.check()?;
        RecoveryHistoryObservation::from_local(
            reply,
            self.provider.local_participant(),
            request,
            token.deadline().unwrap(),
        )
    }
    pub(crate) fn new(
        service: Arc<Mutex<ClusterDataNodeService>>,
        provider: Arc<MetadataReadProvider>,
    ) -> Self {
        let (local_node_id, gate) = {
            let service = service.lock();
            (service.node_id().clone(), service.range_mutation_gate())
        };
        Self {
            service,
            provider,
            local_node_id,
            gate,
            sessions: Arc::new(Mutex::new(Sessions::default())),
            participant_transport: None,
        }
    }
    pub(crate) fn with_participant_transport(
        mut self,
        transport: Arc<dyn crate::distribution_range_consensus::RangeWriteTransport>,
    ) -> Self {
        self.participant_transport = Some(transport);
        self
    }
    fn reserve(&self, range: RangeId) -> Result<Arc<Operation>> {
        self.reserve_context(range, true)
    }
    fn reserve_observer(&self, range: RangeId) -> Result<Arc<Operation>> {
        self.reserve_context(range, false)
    }
    fn reserve_context(&self, range: RangeId, exclusive: bool) -> Result<Arc<Operation>> {
        let mut sessions = self.sessions.lock();
        if sessions
            .current
            .get(&range)
            .is_some_and(|s| s.frozen.is_none())
            || sessions.contexts >= MAX_CONTEXTS
            || (exclusive && !sessions.active.insert(range))
        {
            return Err(invalid("receiver recovery context/range is busy"));
        }
        sessions.contexts += 1;
        Ok(Arc::new(Operation {
            sessions: Arc::clone(&self.sessions),
            range,
            exclusive,
        }))
    }
    fn frozen(
        &self,
        scope: &RecoveryFenceScope,
        token: &CancellationToken,
        replace: bool,
    ) -> Result<Arc<FrozenRangeMutationPermit>> {
        token.check()?;
        {
            let mut sessions = self.sessions.lock();
            if let Some(session) = sessions.current.get_mut(&scope.range_id) {
                let actual = session
                    .frozen
                    .as_ref()
                    .ok_or_else(|| invalid("recovery freeze is leased to an image installer"))?;
                if actual.owner() != session.scope.freeze_owner()? {
                    return Err(invalid("retained image freeze requires actual catalogue adoption before any recovery rotation"));
                }
                if session.scope.same_session(scope) {
                    let frozen = session.frozen.as_ref().ok_or_else(|| {
                        invalid("recovery freeze is leased to an image installer")
                    })?;
                    if frozen.owner() != scope.freeze_owner()? {
                        return Err(invalid("image installation has not returned its freeze through catalogue adoption"));
                    }
                    self.gate
                        .validate_quiescent_frozen(scope.range_id, frozen)?;
                    return Ok(Arc::clone(frozen));
                }
                if !replace {
                    return Err(invalid(
                        "read-only observation cannot rotate a recovery freeze",
                    ));
                }
                let frozen =
                    Arc::get_mut(session.frozen.as_mut().ok_or_else(|| {
                        invalid("recovery freeze is leased to an image installer")
                    })?)
                    .ok_or_else(|| {
                        invalid("old recovery freeze still has a source/callback owner")
                    })?;
                frozen.try_rotate_owner(scope.freeze_owner()?)?;
                session.scope = scope.clone();
                return Ok(Arc::clone(session.frozen.as_ref().unwrap()));
            }
            if sessions.current.len() >= MAX_SESSIONS {
                return Err(invalid("receiver recovery session bound reached"));
            }
        }
        let remaining = token
            .deadline()
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .ok_or_else(|| invalid("recovery drain deadline expired"))?;
        // No service/database/session-map lock while waiting for WAL callbacks.
        let frozen =
            Arc::new(
                self.gate
                    .freeze(scope.range_id, scope.freeze_owner()?, remaining, token)?,
            );
        token.check()?;
        let mut sessions = self.sessions.lock();
        sessions.current.insert(
            scope.range_id,
            Session {
                scope: scope.clone(),
                frozen: Some(Arc::clone(&frozen)),
            },
        );
        Ok(frozen)
    }
    pub(crate) fn fence_epoch(
        &self,
        initiator: &MetadataReadParticipant,
        request: &RecoveryEpochFenceRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryFenceObservation> {
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let scope = &request.scope;
        // Actual initiating peer and fresh recipient challenge enter a new
        // metadata leader begin/probe/record/finish; no cached role fallback.
        let observation = self.provider.witness_recovery_role(
            initiator,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            scope.purpose.clone(),
            &token,
        )?;
        let role = RecoveryRoleAdmission::from_witness(
            observation,
            initiator,
            &self.local_node_id,
            business,
            scope.range_id,
            scope.range_epoch,
            scope.recovery_nonce,
            challenge,
            &scope.purpose,
        )?;
        scope.require_role(&role)?;
        let token = token.child_with_deadline(role.deadline());
        {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.preflight_native_recovery(&role, scope, &token)?;
        }
        let frozen = self.frozen(scope, &token, true)?;
        token.check()?;
        let mut service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let reply = service.fence_native_recovery(&role, scope, &frozen, &token)?;
        token.check()?;
        RecoveryFenceObservation::from_local(
            reply,
            self.provider.local_participant(),
            scope,
            token.deadline().unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{RangeImageBirthMember, RangeImageBirthSpec};
    use crate::{
        BicDb, ClusterDataNodeService, ClusterNode, DbConfig, DistributionConfig,
        DistributionStore, PlannedReplicaMove, RebalancePlan, ReplicaMoveReason, StorageMode,
    };
    use parking_lot::RwLock;

    #[test]
    fn actual_runtime_handoff_uses_committed_session_owner_after_normal_preflight() {
        let root = tempfile::tempdir().unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let target = ClusterNodeId::new("n2").unwrap();
        let bootstrap = Uuid::now_v7();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: crate::ClusterId::new("actual-runtime-handoff").unwrap(),
            node_id: node.clone(),
            node_address: "127.0.0.1:10101".into(),
            initial_ranges: 1,
            replication_factor: 1,
            range_image_bootstrap_id: Some(bootstrap),
            ..Default::default()
        };
        let mut control =
            DistributionStore::initialize_at(root.path().join("control"), config, true, 1).unwrap();
        control
            .join_node(
                ClusterNode::new(target.clone(), "127.0.0.1:10102", 1, 1_000_000, 2).unwrap(),
                &node,
                2,
            )
            .unwrap();
        let initial = control.topology().ranges.values().next().unwrap().clone();
        control
            .apply_rebalance_plan(
                &RebalancePlan {
                    id: "runtime-owner-plan".into(),
                    topology_generation: control.topology().generation,
                    created_at_ms: 3,
                    replica_moves: vec![PlannedReplicaMove {
                        range_id: initial.id,
                        expected_epoch: initial.epoch,
                        source: Some(node.clone()),
                        target: target.clone(),
                        estimated_bytes: 0,
                        reason: ReplicaMoveReason::LoadBalance,
                    }],
                    leader_transfers: vec![],
                    unplaced_ranges: vec![],
                    estimated_bytes_in_flight: 0,
                },
                &node,
                3,
            )
            .unwrap();
        let topology = control.topology().clone();
        let range = topology.ranges.values().next().unwrap().clone();
        let plan = topology.relocations.values().next().unwrap();
        let session = plan.handoff_session.as_ref().unwrap();
        assert_ne!(session.session_id, session.nonce);
        let providers = super::super::test_recovery_metadata_cluster(
            &root.path().join("metadata"),
            &topology,
            &node,
        )
        .unwrap();
        let provider = Arc::clone(&providers[&node]);
        let spec = RangeImageBirthSpec {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: bootstrap,
            node_id: node.clone(),
            member_incarnation: topology.nodes[&node].incarnation,
            initial_members: topology
                .nodes
                .iter()
                .map(|(id, m)| {
                    (
                        id.clone(),
                        RangeImageBirthMember {
                            incarnation: m.incarnation,
                            tls_certificate_sha256: m.tls_certificate_sha256.clone(),
                            pending_tls_certificate_sha256: m
                                .pending_tls_certificate_sha256
                                .clone(),
                        },
                    )
                })
                .collect(),
            // Actual completed learner birth, not fabricated empty range R0.
            initial_ranges: vec![],
            restore_activation: None,
        };
        let dbconfig = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let dbpath = root.path().join("db");
        let (db, _) = BicDb::create_new_range_image_database(&dbpath, dbconfig, spec).unwrap();
        let db = Arc::new(RwLock::new(db));
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                topology.cluster_id.clone(),
                node.clone(),
                &dbpath,
                Arc::clone(&db),
                true,
            )
            .unwrap(),
        ));
        let runtime = RangeRecoveryRuntime::new(Arc::clone(&service), Arc::clone(&provider));
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let role_for = |purpose: MetadataRecoveryRolePurpose, nonce: Uuid| {
            let caller = provider.local_participant();
            let business = Uuid::now_v7();
            let challenge = Uuid::now_v7();
            let witness = provider
                .witness_recovery_role(
                    &caller,
                    business,
                    range.id,
                    range.epoch,
                    nonce,
                    challenge,
                    purpose.clone(),
                    &parent,
                )
                .unwrap();
            RecoveryRoleAdmission::from_witness(
                witness,
                &caller,
                &node,
                business,
                range.id,
                range.epoch,
                nonce,
                challenge,
                &purpose,
            )
            .unwrap()
        };
        let normal = role_for(MetadataRecoveryRolePurpose::NormalRecovery, Uuid::now_v7());
        let normal_scope = RecoveryFenceScope::from_role(&normal).unwrap();
        let first = runtime
            .fence_epoch(
                &provider.local_participant(),
                &RecoveryEpochFenceRequest {
                    scope: normal_scope.clone(),
                    budget_ms: 3000,
                },
                &parent,
            )
            .unwrap();
        assert!(first.is_active_recovery());
        assert_eq!(
            runtime
                .source_capture_freeze(&normal_scope)
                .unwrap()
                .owner(),
            normal_scope.recovery_nonce
        );
        let mut changed = topology.clone();
        changed.generation += 1;
        changed.nodes.get_mut(&node).unwrap().tls_certificate_sha256 = Some("ab".repeat(32));
        changed.validate().unwrap();
        let changed_role = super::super::test_handoff_role_admission(
            &root.path().join("changed-credentials-metadata"),
            &changed,
            &node,
            &node,
            &node,
            plan.id,
            Duration::from_secs(3),
        )
        .unwrap();
        let changed_scope = RecoveryFenceScope::from_role(changed_role.role()).unwrap();
        assert!(service.lock().preflight_native_recovery(changed_role.role(),&changed_scope,&parent).is_err(),
            "Normal-to-Handoff bridge cannot rebind old voter credentials without an actual activation/ancestry proof");
        assert_eq!(
            runtime
                .source_capture_freeze(&normal_scope)
                .unwrap()
                .owner(),
            normal_scope.recovery_nonce
        );
        let handoff = role_for(
            MetadataRecoveryRolePurpose::CommittedHandoff {
                relocation_id: plan.id,
                plan_id: plan.plan_id.clone(),
                session_id: session.session_id,
                nonce: session.nonce,
            },
            session.nonce,
        );
        let scope = RecoveryFenceScope::from_role(&handoff).unwrap();
        assert_eq!(
            normal_scope.range_configuration_sha256,
            scope.range_configuration_sha256
        );
        assert_ne!(
            normal_scope.configuration_sha256,
            scope.configuration_sha256
        );
        let observed = runtime
            .fence_epoch(
                &provider.local_participant(),
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 3000,
                },
                &parent,
            )
            .unwrap();
        assert!(observed.is_active_recovery());
        assert_eq!(observed.native().native_applied_index, None);
        let frozen = runtime.source_capture_freeze(&scope).unwrap();
        assert_eq!(frozen.owner(), session.session_id);
        let gate = db.read().range_mutation_gate();
        let pin = gate.try_source_capture_read_pin(range.id, &frozen).unwrap();
        assert!(gate.try_controlled_mutation(&frozen).is_err());
        drop(pin);
        drop(frozen);
        let refused = role_for(MetadataRecoveryRolePurpose::NormalRecovery, Uuid::now_v7());
        let refused_scope = RecoveryFenceScope::from_role(&refused).unwrap();
        assert!(runtime
            .fence_epoch(
                &provider.local_participant(),
                &RecoveryEpochFenceRequest {
                    scope: refused_scope,
                    budget_ms: 3000,
                },
                &parent
            )
            .is_err());
        assert_eq!(
            runtime.source_capture_freeze(&scope).unwrap().owner(),
            session.session_id,
            "rejected normal takeover cannot rotate the actual committed-session freeze"
        );
    }
}
