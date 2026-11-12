//! Same actual service-owned store. All authority/network acquisition precedes
//! this latch; DB acquisition is nonwaiting and the owned preparation survives.
use super::*;
use crate::distribution_range_consensus::{
    LocalNativeCompletionEvidence, PreparedNativeRecoveryReady,
};
use crate::distribution_range_recovery::{
    ClosedNativeReadyQuorum, NativeCompletionRequest, RecoveryObservationRoleAdmission,
    RecoveryRoleAdmission,
};
impl ClusterDataNodeService {
    pub(crate) fn complete_local_native_recovery(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        quorum: ClosedNativeReadyQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        #[cfg(test)]
        let mut db_trace = crate::distribution_range_handoff::NativeJointPhaseTrace::new(
            "recovery-complete-database-read-acquisition",
            fresh.local_node_id(),
            parent,
        );
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        #[cfg(test)]
        {
            db_trace.complete();
            drop(db_trace);
        }
        let evidence = self
            .range_writes
            .current_mut()
            .complete_and_observe_native_recovery_frozen(
                &db, prepared, quorum, fresh, request, parent,
            )?;
        // A canceled/lost response after durability never erases the completed
        // refusal. A fresh readonly request can attest the retained proof.
        parent.check()?;
        Ok(evidence)
    }
    pub(crate) fn adopt_local_native_completion(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        quorum: crate::distribution_range_recovery::NativeCompletionQuorum,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        let evidence = self
            .range_writes
            .current_mut()
            .adopt_and_observe_native_completion_decision(
                &db, prepared, quorum, fresh, request, parent,
            )?;
        parent.check()?;
        Ok(evidence)
    }
    pub(crate) fn adopt_local_native_activated_decision(
        &mut self,
        prepared: &PreparedNativeRecoveryReady,
        decision: crate::distribution_range_recovery::ActivatedNativeCompletionDecision,
        fresh: &RecoveryRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        request.scope.require_role(fresh)?;
        let db = checked_database_read(&self.db, &self.storage_policy)?;
        self.range_writes
            .current_mut()
            .adopt_native_activated_decision(&db, prepared, decision, fresh)?;
        parent.check()?;
        self.range_writes
            .observe_native_completion_for_role(&db, prepared, fresh, request)
    }
    pub(crate) fn native_completion_is_activated(
        &self,
        request: &NativeCompletionRequest,
    ) -> Result<bool> {
        Ok(self.range_writes.recovery_observation_phase(&request.scope)?==crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeActivatedAnchor)
    }
    pub(crate) fn observe_local_native_completion(
        &self,
        prepared: Option<&PreparedNativeRecoveryReady>,
        observer: &RecoveryObservationRoleAdmission,
        request: &NativeCompletionRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeCompletionEvidence> {
        parent.check()?;
        #[cfg(test)]
        let _probe_context = crate::distribution_range_recovery::NativeProbeTraceContext::enter(
            "completion-status",
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
        let evidence = if self.range_writes.recovery_observation_phase(&request.scope)?==crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativeActivatedAnchor {
            self.range_writes.observe_activated_completion_for_observer(&db,observer,request)?
        } else {
            self.range_writes.observe_native_completion_for_observer(&db,prepared.ok_or_else(||data_error("completion has no actual retained frozen pin"))?,observer,request)?
        };
        parent.check()?;
        Ok(evidence)
    }
}
