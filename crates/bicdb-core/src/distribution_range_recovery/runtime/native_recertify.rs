//! Fresh actual current-leader receiver path; no network under local latches.
use super::*;
use crate::distribution_native_apply::{NativeLeaderInstructionAdmission, NativeRecertifyRequest};
impl RangeRecoveryRuntime {
    pub(crate) fn recertify_native_ordinary(
        &self,
        initiator: &MetadataReadParticipant,
        request: NativeRecertifyRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_data::LocalNativeRecertifyEvidence> {
        let token = request.token(parent)?;
        let _operation = self.reserve(request.instruction.command.range_id)?;
        let role = self.normal_role(
            initiator,
            request.instruction.command.range_id,
            request.instruction.command.range_epoch,
            request.instruction.challenge,
            &token,
        )?;
        let token = token.child_with_deadline(role.deadline());
        let admission = NativeLeaderInstructionAdmission::from_actual_peer(
            role,
            initiator,
            request.instruction,
        )?;
        let mut service = loop {
            token.check()?;
            if let Some(service) = self.service.try_lock_for(LATCH_POLL) {
                break service;
            }
        };
        let actual = service.recertify_native_ordinary(&admission, &token)?;
        token.check()?;
        admission.require_live()?;
        Ok(actual)
    }
}
