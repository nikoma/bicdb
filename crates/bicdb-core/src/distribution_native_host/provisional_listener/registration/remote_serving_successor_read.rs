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
    /// Explicit same-owner Joined selection, before ORIGINAL INITIAL dispatch.
    /// A real INITIAL survivor is a different domain and stays on its original
    /// handler. Missing/invalid owner on a selected Registered route is an
    /// error, never a handler-error fallback or a new policy grant.
    pub(crate) fn select_registered_replacement(
        self: &Arc<Self>,
        runtime: &RangeRecoveryRuntime,
        listener: &crate::distribution_native_host::NativeInitialDataAdmission,
    ) -> Result<Option<Arc<Self>>> {
        self.require_runtime(runtime)?;
        let successor = self
            .successor
            .upgrade()
            .ok_or_else(|| invalid("Registered successor route actual owner ended"))?;
        let crate::db::NativeJointWriterDomain::Registered(original) = successor.domain() else {
            return Ok(None);
        };
        let owner = original
            .upgrade()
            .ok_or_else(|| invalid("Registered successor route original receiver ended"))?;
        owner.require_first_policy_attempt_lifetime()?;
        let actual_runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("Registered successor route original runtime ended"))?;
        listener.require_listener(successor.service(), &actual_runtime)?;
        // Exact installed writer pointer, actual DB/open/slot and registration
        // lifetime bind this listener to the original native install. A boolean
        // profile or an absent INITIAL source registry supplies no authority.
        listener.require_registered_successor_listener_memory(&owner)?;
        successor.require_memory_lifetime()?;
        Ok(Some(self.clone()))
    }
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
