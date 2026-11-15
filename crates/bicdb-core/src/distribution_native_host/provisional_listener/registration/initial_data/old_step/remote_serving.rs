//! Existing original INITIAL phase only. No incoming request can create an
//! OLD observation, source registry, host or replacement native attempt.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_native_host::provisional_listener::registration::remote_serving_metadata::RemoteServingRecipientMetadata;
use crate::distribution_native_host::{NativeRemoteServingReplyV1, NativeRemoteServingRequestV1};
use crate::distribution_range_handoff::NativeRemoteNewParticipant;

impl NativeInitialDataAdmission {
    pub(crate) fn receive_remote_serving_from_authenticated_dispatch(
        &self,
        peer: MetadataReadParticipant,
        request: NativeRemoteServingRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_initial_source_hosting()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let metadata = self
            .old_metadata
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL MStore ended"))?;
        let provider = self
            .old_provider
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL provider ended"))?;
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL transport ended"))?;
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("serving original INITIAL OLD table ended"))?;
        let original = {
            let table = table
                .try_lock()
                .ok_or_else(|| invalid("serving original OLD table busy"))?;
            table
                .get(&request.manifest.capture_range.id)
                .cloned()
                .ok_or_else(|| invalid("serving cannot create missing original OLD phase"))?
        };
        let participant = original
            .try_lock()
            .ok_or_else(|| invalid("serving original OLD phase busy"))?
            .remote_new
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("serving original remote native participant absent"))?;
        let fresh = RemoteServingRecipientMetadata::acquire(
            &provider, &transport, &metadata, &service, &peer, &request, &token,
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
            let leader = provider
                .current_metadata_leader_hint(&token)?
                .ok_or_else(|| invalid("prepared observation current metadata leader absent"))?;
            return participant.observe_retained_preparation_for_serving(
                &request,
                &fresh.role,
                &fresh.own,
                fresh.business,
                &fresh.point,
                &transport,
                &leader,
                &token,
            );
        }
        if matches!(
            request.operation,
            crate::distribution_native_host::NativeRemoteServingOperationV1::ObserveBridge
                | crate::distribution_native_host::NativeRemoteServingOperationV1::ObserveOpen
        ) {
            let leader = provider
                .current_metadata_leader_hint(&token)?
                .ok_or_else(|| invalid("Open observation current metadata leader absent"))?;
            return participant.observe_retained_open_for_serving(
                &request,
                &fresh.role,
                &fresh.own,
                fresh.business,
                &fresh.point,
                &transport,
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
impl NativeInitialDataHost {
    pub(in crate::distribution_native_host) fn retained_remote_serving_participants(
        &self,
    ) -> Result<Vec<Arc<NativeRemoteNewParticipant>>> {
        self.require_writer_data_owner_for_serving()?;
        let entries = self
            .old_closures
            .try_lock()
            .ok_or_else(|| invalid("serving actual INITIAL table busy"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut actual = Vec::new();
        for entry in entries {
            if let Some(phase) = &entry
                .try_lock()
                .ok_or_else(|| invalid("serving original INITIAL participant busy"))?
                .remote_new
            {
                actual.push(phase.clone());
            }
        }
        Ok(actual)
    }
    fn require_writer_data_owner_for_serving(&self) -> Result<()> {
        if self.stopping {
            return Err(invalid(
                "stopping INITIAL owner cannot consume serving continuation",
            ));
        }
        self.admission
            .as_ref()
            .ok_or_else(|| invalid("serving actual INITIAL admission absent"))?
            .require_live()
    }
}
