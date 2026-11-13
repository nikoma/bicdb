//! Bounded actual-origin suffix recertification. This is neither an ACK to a
//! user nor a certificate/quorum reconstructed from decoded voter data.
use super::*;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRecertifyRequest {
    pub instruction: NativeOriginApplyRequest,
}
impl NativeRecertifyRequest {
    pub(crate) fn from_source_plan(
        command: &RangeWriteCommand,
        plan: &NativeRangeCommandIntent,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        command.validate()?;
        validate_native_recovery_command_size(command)?;
        if plan.command_id() != command.command_id
            || plan.command_checksum_sha256() != command.checksum_sha256
            || plan.range_index() != command.index
            || plan.origin_scope_version() != NATIVE_ORIGIN_SCOPE_VERSION
        {
            return Err(invalid("recertification requires exact actual source plan"));
        }
        Ok(Self {
            instruction: NativeOriginApplyRequest {
                protocol_version: NATIVE_ORIGIN_SCOPE_VERSION,
                command: command.clone(),
                bootstrap_id: plan.bootstrap_id(),
                restore_activation: plan.restore_activation().cloned(),
                origin_scope_sha256: plan.scope_sha256().to_owned(),
                challenge: Uuid::now_v7(),
                budget_ms: remaining_budget(parent)?,
            },
        })
    }
    pub(crate) fn token(&self, parent: &CancellationToken) -> Result<CancellationToken> {
        self.instruction.token(parent)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRecertifyReply {
    pub challenge: Uuid,
    pub command_sha256: String,
    pub origin_scope_sha256: String,
    pub member_incarnation: u64,
    pub store_open_id: Uuid,
    pub database_open_incarnation: String,
    pub ack: crate::distribution_range_consensus::RangeWriteAck,
}
#[derive(Debug)]
pub(crate) struct NativeRecertifyObservation {
    reply: NativeRecertifyReply,
    cancellation: CancellationToken,
    deadline: Instant,
}
impl NativeRecertifyObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        actual: crate::distribution_transport::AuthenticatedNativeRecertifyReply,
        request: &NativeRecertifyRequest,
        parent: &CancellationToken,
        deadline: Instant,
    ) -> Result<Self> {
        let (reply, peer) = actual.into_parts();
        let command = &request.instruction.command;
        if reply.challenge != request.instruction.challenge
            || reply.command_sha256 != command.checksum_sha256
            || reply.origin_scope_sha256 != request.instruction.origin_scope_sha256
            || peer.node_id() != &reply.ack.node_id
            || peer.member_incarnation() != reply.member_incarnation
            || reply.store_open_id.is_nil()
            || reply.database_open_incarnation.is_empty()
            || reply.ack.range_id != command.range_id
            || reply.ack.range_epoch != command.range_epoch
            || reply.ack.index != command.index
            || !matches!(
                reply.ack.state,
                crate::distribution_range_consensus::RangeWriteState::QuorumCommitted
                    | crate::distribution_range_consensus::RangeWriteState::Applied
            )
        {
            return Err(invalid(
                "actual recertification reply differs from request/member",
            ));
        }
        let result = Self {
            reply,
            cancellation: parent.clone(),
            deadline,
        };
        result.require_live()?;
        Ok(result)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if Instant::now() >= self.deadline {
            return Err(crate::BicDbError::QueryTimedOut);
        }
        Ok(())
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_original_open(
        &self,
        original: &crate::distribution_transport::HotRecoveryPeerOpen,
    ) -> Result<()> {
        self.require_live()?;
        original.require_actual_fields(
            &self.reply.ack.node_id,
            self.reply.store_open_id,
            &self.reply.database_open_incarnation,
        )
    }
    pub(crate) fn ack(&self) -> &crate::distribution_range_consensus::RangeWriteAck {
        &self.reply.ack
    }
}
