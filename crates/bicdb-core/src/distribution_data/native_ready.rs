//! Same-store native preparation/status. Metadata authority is acquired by the
//! Runtime before this latch; DB acquisition here never waits or performs RPC.
use super::*;
use crate::distribution_range_consensus::{LocalNativeReadyEvidence, PreparedNativeRecoveryReady};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::{
    NativeReadyRequest, RecoveryObservationRoleAdmission, RecoveryRoleAdmission,
};

impl ClusterDataNodeService {
    pub(crate) fn preflight_native_ready(
        &self,
        role: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
    ) -> Result<()> {
        request.scope.require_role(role)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .preflight_native_ready_freeze(role, &native, &request.scope)
    }
    pub(crate) fn prepare_local_native_ready(
        &mut self,
        role: RecoveryRoleAdmission,
        request: &NativeReadyRequest,
        frozen: Arc<FrozenRangeMutationPermit>,
        parent: &CancellationToken,
    ) -> Result<(PreparedNativeRecoveryReady, LocalNativeReadyEvidence)> {
        parent.check()?;
        request.scope.require_role(&role)?;
        #[cfg(test)]
        let mut local_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-prepare-local-database-admission-preflight",
            role.local_node_id(),
            parent,
        );
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .preflight_native_ready_freeze(&role, &native, &request.scope)?;
        #[cfg(test)]
        {
            local_trace.complete();
            drop(local_trace);
        }
        #[cfg(test)]
        let mut catalogue_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-prepare-existing-refsync-and-resolved-catalogue",
            role.local_node_id(),
            parent,
        );
        match self.range_writes.recovery_observation_phase(&request.scope) {
            Ok(
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery,
            ) => {}
            Ok(
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared,
            )
            | Ok(crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen)
            | Err(_) => {
                // This exact re-fsync path validates the existing Prepared
                // phase, native birth/open and fresh scope. Errors are never
                // ignored or converted into an Active/counter fallback.
                self.range_writes
                    .current_mut()
                    .refsync_native_prepared_refusal(&db, &native, &role, &frozen, parent)?;
            }
            _ => {
                return Err(data_error(
                    "activated recovery cannot be prepared through an old session",
                ))
            }
        }
        let catalogue = self.range_writes.observe_native_ready_resolved_prefix(
            &role,
            &native,
            &request.scope,
        )?;
        #[cfg(test)]
        {
            catalogue_trace.complete();
            drop(catalogue_trace);
        }
        #[cfg(test)]
        let mut physical_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-prepare-actual-physical-producer",
            role.local_node_id(),
            parent,
        );
        let physical = db.attest_local_native_recovery_ready(
            native,
            role,
            catalogue,
            &self.range_writes,
            frozen,
            parent,
        )?;
        #[cfg(test)]
        {
            physical_trace.complete();
            drop(physical_trace);
        }
        #[cfg(test)]
        let mut prepared_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-prepare-catalogue-producer-and-projection",
            physical.role().local_node_id(),
            parent,
        );
        let (prepared, evidence) = self
            .range_writes
            .current_mut()
            .prepare_and_observe_native_recovery_ready(&db, physical, request, parent)?;
        #[cfg(test)]
        {
            prepared_trace.complete();
            drop(prepared_trace);
        }
        parent.check()?;
        Ok((prepared, evidence))
    }
    pub(crate) fn reattest_local_native_ready(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        request: &NativeReadyRequest,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<LocalNativeReadyEvidence> {
        parent.check()?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .current_mut()
            .refsync_native_prepared_refusal(&db, &native, role, frozen, parent)?;
        if self.range_writes.recovery_observation_phase(&request.scope)?
            == crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen {
            prepared.refresh_after_durable_native_completion(&db,&self.range_writes,role)?;
        }
        let evidence = self
            .range_writes
            .observe_prepared_native_for_role(&db, prepared, role, request)?;
        parent.check()?;
        Ok(evidence)
    }
    /// A Complete of this session that persisted the catalogue completion and
    /// then failed before refreshing the retained binding (a busy native
    /// journal, an expired budget) leaves the binding at the prepared
    /// catalogue, and every later Complete would refuse it as stale. The
    /// same repair as a re-sent Prepare (`reattest_local_native_ready`) runs
    /// first: re-fsync of the exact saved refusal, then the full native
    /// readback refresh. It is a no-op unless the saved phase is completed
    /// (or its persistence is uncertain); nothing else changes.
    /// Scheduling only: whether `refresh_retained_completed_native_binding`
    /// can have work (the saved phase is completed or not durably known).
    pub(crate) fn native_completed_binding_repair_wanted(
        &self,
        scope: &crate::distribution_range_consensus::RecoveryFenceScope,
    ) -> bool {
        matches!(
            self.range_writes.recovery_observation_phase(scope),
            Ok(crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeCompletedFrozen)
                | Err(_)
        )
    }
    pub(crate) fn refresh_retained_completed_native_binding(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        role: &RecoveryRoleAdmission,
        frozen: &FrozenRangeMutationPermit,
        parent: &CancellationToken,
    ) -> Result<()> {
        use crate::distribution_range_recovery::RecoveryFenceObservationPhase as Phase;
        parent.check()?;
        prepared.scope().require_role(role)?;
        if !matches!(
            self.range_writes
                .recovery_observation_phase(prepared.scope()),
            Ok(Phase::NativeCompletedFrozen) | Err(_)
        ) {
            return Ok(());
        }
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .current_mut()
            .refsync_native_prepared_refusal(&db, &native, role, frozen, parent)?;
        if self
            .range_writes
            .recovery_observation_phase(prepared.scope())?
            == Phase::NativeCompletedFrozen
        {
            prepared.refresh_after_durable_native_completion(&db, &self.range_writes, role)?;
        }
        parent.check()
    }
    pub(crate) fn observe_local_native_ready(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeReadyEvidence> {
        parent.check()?;
        #[cfg(test)]
        let _probe_context = crate::distribution_range_recovery::NativeProbeTraceContext::enter(
            "ready",
            observer.local_node_id(),
            &request.scope,
            request.challenge,
            Some(observer.deadline()),
        );
        #[cfg(test)]
        let db_trace = crate::distribution_range_recovery::NativeProbeTraceSpan::current(
            "receiver-db-acquisition",
        );
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        #[cfg(test)]
        crate::distribution_range_recovery::NativeProbeTraceSpan::finish(db_trace);
        let evidence = if matches!(
            request.scope.purpose,
            crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
        ) {
            let slot = self.native_replica_listener_store_open_guard();
            self.range_writes
                .observe_prepared_native_for_observer_scoped(
                    &db, prepared, observer, request, &slot, parent,
                )?
        } else {
            self.range_writes
                .observe_prepared_native_for_observer(&db, prepared, observer, request)?
        };
        parent.check()?;
        Ok(evidence)
    }
}
