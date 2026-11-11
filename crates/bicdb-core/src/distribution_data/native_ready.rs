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
        let db = try_database_read(&self.db)?;
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
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .preflight_native_ready_freeze(&role, &native, &request.scope)?;
        match self.range_writes.recovery_observation_phase(&request.scope) {
            Ok(
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::ActiveRecovery,
            ) => {}
            Ok(
                crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared,
            )
            | Err(_) => {
                // This exact re-fsync path validates the existing Prepared
                // phase, native birth/open and fresh scope. Errors are never
                // ignored or converted into an Active/counter fallback.
                self.range_writes
                    .refsync_native_prepared_refusal(&db, &native, &role, &frozen, parent)?;
            }
            _ => {
                return Err(data_error(
                    "completed recovery cannot be prepared through an old session",
                ))
            }
        }
        let catalogue = self.range_writes.observe_native_ready_resolved_prefix(
            &role,
            &native,
            &request.scope,
        )?;
        let physical = db.attest_local_native_recovery_ready(
            native,
            role,
            catalogue,
            &self.range_writes,
            frozen,
            parent,
        )?;
        let prepared = self
            .range_writes
            .prepare_native_recovery_ready(&db, physical)?;
        let evidence = self.range_writes.observe_prepared_native_for_role(
            &db,
            &prepared,
            prepared.role(),
            request,
        )?;
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
        let db = try_database_read(&self.db)?;
        let native = db.native_recovery_admission(
            role.cluster_id(),
            role.bootstrap_id(),
            role.local_node_id(),
            role.local_member_incarnation(),
        )?;
        self.range_writes
            .refsync_native_prepared_refusal(&db, &native, role, frozen, parent)?;
        let evidence = self
            .range_writes
            .observe_prepared_native_for_role(&db, prepared, role, request)?;
        parent.check()?;
        Ok(evidence)
    }
    pub(crate) fn observe_local_native_ready(
        &self,
        prepared: &PreparedNativeRecoveryReady,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeReadyEvidence> {
        parent.check()?;
        let db = try_database_read(&self.db)?;
        let evidence = self
            .range_writes
            .observe_prepared_native_for_observer(&db, prepared, observer, request)?;
        parent.check()?;
        Ok(evidence)
    }
}
