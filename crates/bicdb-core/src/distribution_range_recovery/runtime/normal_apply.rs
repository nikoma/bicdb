//! Ordinary replication uses a fresh leader/recipient identity witness only.
//! It does NOT create a recovery session, epoch fence, readiness or QQ.
use super::*;
use crate::distribution_native_apply::{
    NativeLeaderInstructionAdmission, NativeOriginApplyRequest, NativeOriginStatusRequest,
    NativeReplicaApplyObservation,
};
impl RangeRecoveryRuntime {
    pub(super) fn normal_role(
        &self,
        initiator: &MetadataReadParticipant,
        range_id: RangeId,
        epoch: u64,
        correlation: Uuid,
        parent: &CancellationToken,
    ) -> Result<RecoveryRoleAdmission> {
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let witness = self.provider.witness_recovery_role(
            initiator,
            business,
            range_id,
            epoch,
            correlation,
            challenge,
            purpose.clone(),
            parent,
        )?;
        RecoveryRoleAdmission::from_witness(
            witness,
            initiator,
            &self.local_node_id,
            business,
            range_id,
            epoch,
            correlation,
            challenge,
            &purpose,
        )
    }
    pub(crate) fn observe_native_ordinary(
        &self,
        initiator: &MetadataReadParticipant,
        request: &NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        let token = request.token(parent)?;
        let _operation = self.reserve_observer(request.range_id)?;
        let role = self.normal_role(
            initiator,
            request.range_id,
            request.range_epoch,
            request.challenge,
            &token,
        )?;
        if role.bootstrap_id() != request.bootstrap_id || role.cluster_id() != &request.cluster_id {
            return Err(invalid(
                "ordinary native status has another cluster/bootstrap",
            ));
        }
        // No metadata/network work is performed after entering these latches.
        let service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        service.observe_native_ordinary_for_role(&role, request.challenge)
    }
    pub(crate) fn apply_native_ordinary(
        &self,
        initiator: &MetadataReadParticipant,
        request: NativeOriginApplyRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        let token = request.token(parent)?;
        let _operation = self.reserve(request.command.range_id)?;
        let role = self.normal_role(
            initiator,
            request.command.range_id,
            request.command.range_epoch,
            request.challenge,
            &token,
        )?;
        let admission =
            NativeLeaderInstructionAdmission::from_actual_peer(role, initiator, request)?;
        #[cfg(test)]
        if self.test_refuse_actual_native_apply_before_effect(&admission) {
            return Err(invalid(
                "injected actual authenticated NativeApply refusal before native effect",
            ));
        }
        let mut service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        service.apply_native_ordinary_instruction(&admission, &token)
    }
}
