//! Original registered receiver only: independent role + OWN MQ, then refusal.
use super::*;
use crate::distribution_native_host::NativeRegisteredReplicaAdmission;
impl RangeRecoveryRuntime {
    pub(crate) fn fence_registered_replica(
        self: &Arc<Self>,
        admission: &NativeRegisteredReplicaAdmission,
        initiator: &MetadataReadParticipant,
        request: &RecoveryEpochFenceRequest,
        parent: &CancellationToken,
    ) -> Result<RecoveryFenceObservation> {
        admission.require_listener(&self.service, self)?;
        let token = request.cancellation(parent)?;
        let _operation = self.reserve(request.scope.range_id)?;
        let scope = &request.scope;
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let witness = self.provider.witness_recovery_role(
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
            witness,
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
        let capability =
            admission.observe_assigned_closed_fence(&self.service, self, &role, scope, &token)?;
        {
            let service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.preflight_registered_closed_fence(&capability, &token)?;
        }
        capability.require_metadata_current()?;
        let frozen = self.frozen(scope, &token, true)?;
        capability.require_metadata_current()?;
        let reply = {
            let mut service = loop {
                token.check()?;
                if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                    break service;
                }
            };
            service.fence_registered_closed_receiver(&capability, &frozen, &token)?
        };
        // No service/DB/journal/hook guard spans this actual MStore check.
        // A failure here retains the persisted refusal and actual Frozen;
        // it is an uncertain/failed reply, never rollback or floor creation.
        capability.require_metadata_current()?;
        capability.require_live()?;
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
impl RangeRecoveryRuntime {
    pub(crate) fn test_retained_registered_freeze(
        &self,
        scope: &RecoveryFenceScope,
    ) -> Result<Arc<FrozenRangeMutationPermit>> {
        let sessions = self
            .sessions
            .try_lock()
            .ok_or_else(|| invalid("actual session busy"))?;
        let saved = sessions
            .current
            .get(&scope.range_id)
            .ok_or_else(|| invalid("actual completed fence session absent"))?;
        if !saved.scope.same_session(scope) {
            return Err(invalid("actual retained session changed"));
        }
        let frozen = saved
            .frozen
            .as_ref()
            .ok_or_else(|| invalid("actual retained freeze absent"))?
            .clone();
        self.gate
            .validate_quiescent_frozen(scope.range_id, &frozen)?;
        Ok(frozen)
    }
}
