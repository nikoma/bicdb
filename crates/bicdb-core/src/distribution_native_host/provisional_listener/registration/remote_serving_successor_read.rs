//! Readonly continuation through a real Registered→DATA listener replacement.
//! The actual host owns this registration; runtime holds only a Weak route.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_native_host::{
    NativeRemoteServingOperationV1 as Op, NativeRemoteServingReplyV1, NativeRemoteServingRequestV1,
};
use crate::distribution_range_recovery::RangeRecoveryRuntime;
use std::sync::Weak;

pub(crate) struct NativeRemoteSuccessorReadRegistration {
    backend: Weak<NativeMetadataControlBackend>,
    runtime: Weak<RangeRecoveryRuntime>,
    participant: Weak<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
    successor: Weak<crate::distribution_data::NativeRemoteReleaseOwner>,
    range: crate::RangeId,
    phase: Uuid,
}
impl std::fmt::Debug for NativeRemoteSuccessorReadRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRemoteSuccessorReadRegistration")
            .field("range", &self.range)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}
impl NativeRemoteSuccessorReadRegistration {
    pub(super) fn capture(
        backend: &Arc<NativeMetadataControlBackend>,
        runtime: &Arc<RangeRecoveryRuntime>,
        participant: &Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
        successor: &Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    ) -> Result<Arc<Self>> {
        successor.require_participant(participant)?;
        if !Arc::ptr_eq(successor.service(), &backend.service)
            || successor.native().local_node_id() != &backend.node_id
        {
            return Err(invalid(
                "successor reader changed actual host/service/recipient",
            ));
        }
        runtime.require_point_read_service(&backend.service, &backend.node_id)?;
        Ok(Arc::new(Self {
            backend: Arc::downgrade(backend),
            runtime: Arc::downgrade(runtime),
            participant: Arc::downgrade(participant),
            successor: Arc::downgrade(successor),
            range: participant.snapshot().binding.range_id,
            phase: participant.snapshot().phase_id,
        }))
    }
    pub(crate) fn attach(self: &Arc<Self>) -> Result<()> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("successor actual runtime ended before attach"))?;
        runtime.attach_remote_successor_read(self)
    }
    pub(crate) fn key(&self) -> (crate::RangeId, Uuid) {
        (self.range, self.phase)
    }
    pub(crate) fn require_runtime(&self, actual: &RangeRecoveryRuntime) -> Result<()> {
        let original = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("successor original runtime ended"))?;
        if !std::ptr::eq(original.as_ref(), actual) {
            return Err(invalid("successor readonly route changed actual runtime"));
        }
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("successor original backend ended"))?;
        actual.require_point_read_service(&backend.service, &backend.node_id)
    }
    pub(crate) fn read(
        &self,
        runtime: &RangeRecoveryRuntime,
        peer: MetadataReadParticipant,
        request: &NativeRemoteServingRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        self.require_runtime(runtime)?;
        parent.check()?;
        request.require_shape()?;
        if !matches!(
            request.operation,
            Op::ObserveOpen | Op::ObserveReleased | Op::ObserveReleasedForPolicy
        ) || request.manifest.capture_range.id != self.range
            || request.phase_id != self.phase
        {
            return Err(invalid(
                "successor listener route accepts exact readonly released phase only",
            ));
        }
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("successor original backend ended"))?;
        let participant = self
            .participant
            .upgrade()
            .ok_or_else(|| invalid("successor original participant ended"))?;
        let successor = self
            .successor
            .upgrade()
            .ok_or_else(|| invalid("successor actual released owner ended"))?;
        successor.require_participant(&participant)?;
        let token = request.cancellation(parent)?;
        let fresh = remote_serving_metadata::RemoteServingRecipientMetadata::acquire(
            &backend.provider,
            &backend.transport,
            &backend.metadata,
            &backend.service,
            &peer,
            request,
            &token,
        )?;
        let leader = backend
            .provider
            .current_metadata_leader_hint(&token)?
            .ok_or_else(|| invalid("successor actual metadata leader absent"))?;
        let observed = successor.observe_current(&backend.transport, &leader, &token)?;
        participant.observe_actual_release_for_serving(
            request,
            &fresh.role,
            &fresh.own,
            fresh.business,
            &fresh.point,
            &token,
            &observed,
        )
    }
}
