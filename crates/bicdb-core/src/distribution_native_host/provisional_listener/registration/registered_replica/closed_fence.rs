//! Invocation-borrowed refusal capability. No rows, votes, serving or TTL renewal.
use super::*;
use crate::distribution_consensus::{
    HostMetadataReadObservation, NativeOwnedMetadataMemberObservation,
};
use crate::distribution_range_consensus::{RangeWriteStore, RecoveryFenceScope};
use crate::distribution_range_recovery::RecoveryRoleAdmission;

pub(crate) struct NativeAssignedRegisteredClosedFence<'a> {
    admission: &'a NativeRegisteredReplicaAdmission,
    role: &'a RecoveryRoleAdmission,
    scope: &'a RecoveryFenceScope,
    original_caller: &'a CancellationToken,
    own_request: CancellationToken,
    own: HostMetadataReadObservation,
    business: Uuid,
    proof: NativeOwnedMetadataMemberObservation,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl NativeRegisteredReplicaAdmission {
    pub(crate) fn observe_assigned_closed_fence<'a>(
        &'a self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        runtime: &Arc<RangeRecoveryRuntime>,
        role: &'a RecoveryRoleAdmission,
        scope: &'a RecoveryFenceScope,
        parent: &'a CancellationToken,
    ) -> Result<NativeAssignedRegisteredClosedFence<'a>> {
        self.require_listener(service, runtime)?;
        parent.check()?;
        scope.require_role(role)?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("original receiver backend ended"))?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("original receiver factory ended"))?;
        if !Arc::ptr_eq(service, &backend.service) || !factory.spec.initial_ranges.is_empty() {
            return Err(invalid(
                "assigned CLOSED fence requires original registered receiver",
            ));
        }
        // This is a routing hint only. The following real OWN MQ must agree
        // with the separate sealed role and the actually installed MStore.
        let route = backend
            .lock_metadata(parent)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("assigned receiver current metadata leader unknown"))?;
        let own_request = parent.child_with_deadline(role.deadline());
        let business = Uuid::now_v7();
        let remaining = role.remaining_budget().ok_or(BicDbError::QueryTimedOut)?;
        let own = backend.transport.acquire_host_metadata_observation(
            &route,
            business,
            remaining,
            &own_request,
        )?;
        role.require_own_host_metadata(&own, business)?;
        let proof = {
            let metadata = backend.lock_metadata(&own_request)?;
            metadata.require_listener_database(&self.database)?;
            metadata.observe_owned_native_metadata_member(&own, business, &own_request)?
        };
        if proof.store_id() != self.metadata_store_id {
            return Err(invalid(
                "assigned CLOSED fence protected MStore identity changed",
            ));
        }
        let result = NativeAssignedRegisteredClosedFence {
            admission: self,
            role,
            scope,
            original_caller: parent,
            own_request,
            own,
            business,
            proof,
            _local: std::marker::PhantomData,
        };
        result.require_live()?;
        result.require_metadata_current()?;
        Ok(result)
    }
}
impl NativeAssignedRegisteredClosedFence<'_> {
    #[cfg(test)]
    pub(crate) fn test_old_cold_context_refusal(
        &self,
        root: &Path,
        config: crate::DbConfig,
        network: &ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.require_live()?;
        NativeUnassignedLearnerFactory::reopen_registered_metadata(
            root,
            config,
            &self.own,
            self.business,
            network,
            parent,
        )
        .map(|_| ())
    }
    #[cfg(test)]
    pub(crate) fn test_keep_original_registration(
        &self,
    ) -> Result<impl std::any::Any + Send + Sync> {
        self.admission
            .original_hook
            .test_keep_original_registration()
    }

    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        self.role
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        self.scope
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.original_caller.check()?;
        self.own_request.check()?;
        self.admission.require_live()?;
        if !self
            .admission
            .closed_fence_ready
            .upgrade()
            .is_some_and(|live| live.load(Ordering::Acquire))
        {
            return Err(invalid(
                "assigned CLOSED receiver listener was not published or was revoked",
            ));
        }
        self.role
            .require_own_host_metadata(&self.own, self.business)?;
        self.scope.require_role(self.role)?;
        let topology = self.own.committed_topology(self.business)?;
        let local = topology
            .nodes
            .get(self.role.local_node_id())
            .ok_or_else(|| invalid("assigned receiver absent from actual OWN MQ"))?;
        let plan = self.role.committed_relocation().ok_or_else(|| {
            invalid("assigned CLOSED fence needs committed handoff, not normal recovery")
        })?;
        if !matches!(
            self.role.purpose(),
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff { .. }
        ) || local.lifecycle != ClusterNodeLifecycle::Active
            || local.metadata_role != MetadataMemberRole::Voter
            || local.incarnation != self.role.local_member_incarnation()
            || local.native_store_birth_v1.as_ref().is_none_or(|b| {
                b.birth_id() != self.admission.birth_id
                    || b.birth_sha256() != self.admission.birth_sha256
            })
            || plan.target != *self.role.local_node_id()
            || plan.phase != crate::distribution::RelocationPhase::LearnerAllocated
            || plan.promoted_epoch.is_some()
            || plan.learner_epoch != self.scope.range_epoch
            || plan.expected_epoch.checked_add(1) != Some(plan.learner_epoch)
            || plan.source.as_ref() != Some(self.role.initiator_node_id())
            || self.role.initiator_node_id() != &self.role.current_range().leader
            || !self.role.current_range().replicas.iter().any(|r| {
                r.id == plan.target_replica_id
                    && r.node_id == plan.target
                    && r.role == RangeReplicaRole::Learner
            })
            || topology
                .nodes
                .values()
                .filter(|m| {
                    m.lifecycle == ClusterNodeLifecycle::Active
                        && m.metadata_role == MetadataMemberRole::Voter
                })
                .any(|m| {
                    m.native_store_birth_v1.as_ref().is_none_or(|b| {
                        b.writer_enforcement_version() != 1 || b.abort_protocol_version() != 1
                    })
                })
            || topology.ranges.values().any(|r| {
                r.replicas
                    .iter()
                    .any(|p| p.node_id == plan.target && p.role != RangeReplicaRole::Learner)
            })
        {
            return Err(invalid(
                "assigned CLOSED fence differs from actual Pending target Learner/plan/birth",
            ));
        }
        let backend = self
            .admission
            .backend
            .upgrade()
            .ok_or_else(|| invalid("original receiver backend ended"))?;
        let actor = backend.transport.local_metadata_read_participant();
        if actor.node_id() != self.role.local_node_id()
            || actor.member_incarnation() != local.incarnation
            || actor.plaintext_loopback()
            || actor
                .certificate_sha256()
                .is_none_or(|p| !local.accepts_tls_certificate_sha256(p))
        {
            return Err(invalid(
                "assigned CLOSED receiver actual pinned identity changed",
            ));
        }
        self.original_caller.check()
    }
    /// No service/DB guard may be held by the caller of this method.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let backend = self
            .admission
            .backend
            .upgrade()
            .ok_or_else(|| invalid("receiver backend ended"))?;
        let metadata = backend.lock_metadata(&self.own_request)?;
        metadata.require_listener_database(&self.admission.database)?;
        self.proof.require_current(&metadata)?;
        self.require_live()
    }
    pub(crate) fn with_original_hook_persist<R>(
        &self,
        db: &BicDb,
        operation: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        self.admission
            .original_hook
            .with_persist(db, self, operation)
    }
    /// Full original factory/protected identity checks; no metadata lock or RPC.
    pub(crate) fn require_database_store(&self, db: &BicDb, store: &RangeWriteStore) -> Result<()> {
        self.require_database(db)?;
        if store.native_writer_store_open_id() != self.admission.store.id() {
            return Err(invalid("assigned CLOSED fence actual Store changed"));
        }
        self.require_live()
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.require_live()?;
        self.admission.database.require_database(db)?;
        self.admission.original_hook.require_current(db)?;
        self.admission.store.require_live()?;
        if db.incarnation_id() != self.admission.database_incarnation {
            return Err(invalid(
                "assigned CLOSED fence actual DB/Store slot changed",
            ));
        }
        let factory = self
            .admission
            .factory
            .upgrade()
            .ok_or_else(|| invalid("original receiver factory ended"))?;
        factory.require_assigned_closed_database(db, &self.proof, &self.own, self.business)?;
        self.require_live()
    }
}
