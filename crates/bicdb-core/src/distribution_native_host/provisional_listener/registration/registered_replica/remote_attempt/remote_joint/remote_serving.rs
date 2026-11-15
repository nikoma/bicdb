//! The SAME original Registered phase. No cold/default/replacement owner is
//! created by a serving request or by an expired weak registration.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_native_host::provisional_listener::registration::remote_serving_metadata::RemoteServingRecipientMetadata;
use crate::distribution_native_host::{NativeRemoteServingReplyV1, NativeRemoteServingRequestV1};
use crate::distribution_range_handoff::NativeRemoteNewParticipant;

impl NativeRegisteredReplicaAdmission {
    pub(crate) fn receive_remote_serving_from_authenticated_dispatch(
        &self,
        peer: MetadataReadParticipant,
        request: NativeRemoteServingRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("serving original Registered backend ended"))?;
        let table = self
            .joint_targets
            .upgrade()
            .ok_or_else(|| invalid("serving original Registered table ended"))?;
        let owner = table
            .try_lock()
            .ok_or_else(|| invalid("serving actual Registered table busy"))?
            .get(&request.manifest.capture_range.id)
            .cloned()
            .ok_or_else(|| invalid("serving original Registered A absent"))?;
        if !Arc::ptr_eq(owner.service(), &backend.service) {
            return Err(invalid("serving Registered substituted original service"));
        }
        let participant = owner
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("serving actual Registered phase busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("serving Registered original native participant absent"))?;
        let fresh = RemoteServingRecipientMetadata::acquire(
            &backend.provider,
            &backend.transport,
            &backend.metadata,
            &backend.service,
            &peer,
            &request,
            &token,
        )?;
        if request.operation
            == crate::distribution_native_host::NativeRemoteServingOperationV1::ObserveDecision
        {
            return participant.observe_original_decision_for_serving(
                &request,
                &fresh.role,
                &fresh.own,
                fresh.business,
                &fresh.point,
                &token,
            );
        }
        if request.operation
            == crate::distribution_native_host::NativeRemoteServingOperationV1::ObservePrepared
        {
            let leader = backend
                .provider
                .current_metadata_leader_hint(&token)?
                .ok_or_else(|| invalid("prepared observation current metadata leader absent"))?;
            return participant.observe_retained_preparation_for_serving(
                &request,
                &fresh.role,
                &fresh.own,
                fresh.business,
                &fresh.point,
                &backend.transport,
                &leader,
                &token,
            );
        }
        let queue = participant.serving_queue_for_original(
            &request,
            &fresh.role,
            &fresh.own,
            fresh.business,
            &fresh.point,
        )?;
        token.check()?;
        queue.receive(
            peer,
            request,
            fresh.role,
            fresh.own,
            fresh.business,
            fresh.point,
            token,
        )
    }
}
impl NativeRegisteredReplicaReceiver {
    pub(in crate::distribution_native_host) fn retained_remote_serving_participants(
        &self,
    ) -> Result<Vec<Arc<NativeRemoteNewParticipant>>> {
        if self.stopping {
            return Err(invalid(
                "stopping Registered owner cannot consume serving continuation",
            ));
        }
        self.admission.require_live()?;
        let owners = self
            .remote_joint_targets
            .try_lock()
            .ok_or_else(|| invalid("serving actual Registered owner table busy"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        for owner in owners {
            if let Some(phase) = &*owner
                .remote_new
                .try_lock()
                .ok_or_else(|| invalid("serving Registered original phase busy"))?
            {
                actual.push(phase.clone());
            }
        }
        Ok(actual)
    }
}
