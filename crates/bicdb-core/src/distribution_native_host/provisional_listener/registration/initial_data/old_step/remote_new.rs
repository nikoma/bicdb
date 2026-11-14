//! Actual INITIAL OLD/survivor recipient for remote NEW. No network request
//! can construct a missing OLD closure or a replacement source process.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_native_host::provisional_listener::registration::remote_new_metadata::RemoteNewRecipientMetadata;
use crate::distribution_range_handoff::{
    NativeRemoteNewOperationV1, NativeRemoteNewParticipant, NativeRemoteNewReplyV1,
    NativeRemoteNewRequestV1,
};

impl NativeInitialDataAdmission {
    pub(crate) fn observe_remote_new(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteNewReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_initial_source_hosting()?;
        let process = self
            .image_sources
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL process ended"))?;
        process.require_original_process()?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL runtime ended"))?;
        self.require_listener(&service, &runtime)?;
        let metadata = self
            .old_metadata
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL MStore ended"))?;
        let provider = self
            .old_provider
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL provider ended"))?;
        let transport = self
            .old_transport
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original INITIAL transport ended"))?;
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original OLD table ended"))?;
        let first = RemoteNewRecipientMetadata::acquire(
            &provider,
            &transport,
            metadata.clone(),
            &service,
            peer,
            request,
            matches!(
                &request.operation,
                NativeRemoteNewOperationV1::Materialize
                    | NativeRemoteNewOperationV1::PublishDecision { .. }
            ),
            &token,
        )?;
        let retained = {
            let table = table
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original OLD table busy"))?;
            table
                .get(&request.manifest.capture_range.id)
                .cloned()
                .ok_or_else(|| invalid("remote NEW cannot create a missing original OLD closure"))?
        };
        let state = {
            let mut held = retained
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original OLD operation busy"))?;
            if !Arc::ptr_eq(&held.writer, &self.writer()) {
                return Err(invalid("remote NEW original INITIAL writer changed"));
            }
            request.require_plan(&held.plan)?;
            if held.remote_new.is_none() {
                if !matches!(&request.operation, NativeRemoteNewOperationV1::Materialize) {
                    return Err(invalid(
                        "remote NEW observation/preparation cannot recreate a missing participant",
                    ));
                }
                let actual = held
                    .actual
                    .clone()
                    .ok_or_else(|| invalid("remote NEW original native OLD outcome unresolved"))?;
                let role = first.role.as_ref().ok_or_else(|| {
                    invalid("remote NEW materialization requires genuine current recipient Role")
                })?;
                let state = NativeRemoteNewParticipant::from_actual_old(
                    request,
                    actual,
                    self.clone(),
                    held.writer.clone(),
                    process.clone(),
                    role,
                    &first.own,
                    first.business,
                    &first.point,
                    first.metadata_store_id(),
                )?;
                // Original actual owner/pins precede any following postcheck.
                held.remote_new = Some(Arc::new(state));
            }
            held.remote_new.as_ref().unwrap().clone()
        };
        state.retain_request(request)?;
        state.require_current(&token)?;
        first.require_current()?;
        let mut native_decision = None;
        let current = if matches!(
            &request.operation,
            NativeRemoteNewOperationV1::PublishDecision { .. }
        ) {
            let role = first
                .role
                .as_ref()
                .ok_or_else(|| invalid("remote OLD decision initial fresh Role absent"))?;
            let plan=crate::distribution_range_handoff::NativeJointHandoffPlan::from_actual_role_and_own(role,&first.own,first.business)?;
            let source = transport.fetch_remote_decision_source(
                &plan,
                &state,
                &request.manifest,
                &first.own,
                first.business,
                &token,
            )?;
            let fetched = transport.fetch_remote_native_new_fact_set(
                request,
                &first.own,
                first.business,
                &token,
            )?;
            drop(first);
            let fresh = RemoteNewRecipientMetadata::acquire(
                &provider, &transport, metadata, &service, peer, request, true, &token,
            )?;
            if fresh.metadata_store_id() != state.snapshot().metadata_store_id {
                return Err(invalid("remote OLD decision original MStore UUID changed"));
            }
            native_decision = Some(
                state.publish_remote_decision(
                    request,
                    fetched,
                    source,
                    fresh
                        .role
                        .as_ref()
                        .ok_or_else(|| invalid("remote OLD decision final actual Role absent"))?
                        .clone(),
                    fresh.own.clone(),
                    fresh.business,
                    fresh.point.clone(),
                    &token,
                )?,
            );
            fresh
        } else if matches!(
            &request.operation,
            NativeRemoteNewOperationV1::PrepareDecision { .. }
        ) {
            // Independent callbacks, with NO source/table/MStore/DB guards.
            let fetched = transport.fetch_remote_native_new_fact_set(
                request,
                &first.own,
                first.business,
                &token,
            )?;
            drop(first);
            let fresh = RemoteNewRecipientMetadata::acquire(
                &provider, &transport, metadata, &service, peer, request, true, &token,
            )?;
            if fresh.metadata_store_id() != state.snapshot().metadata_store_id {
                return Err(invalid(
                    "remote NEW preparation changed actual original MStore UUID",
                ));
            }
            state.prepare_decision_pin_handoff(
                request,
                fetched,
                None,
                fresh
                    .role
                    .as_ref()
                    .ok_or_else(|| invalid("remote NEW preparation lacks actual Role"))?
                    .clone(),
                fresh.own.clone(),
                fresh.business,
                fresh.point.clone(),
                &token,
            )?;
            fresh
        } else {
            first
        };
        state.require_current(&token)?;
        current.require_current()?;
        self.require_listener(&service, &runtime)?;
        process.require_original_process()?;
        token.check()?;
        let mut reply = NativeRemoteNewReplyV1 {
            request: request.clone(),
            snapshot: state.snapshot().clone(),
            handoff: state.handoff_outcome()?,
            native_decision,
            metadata_term: current.own.term(),
            metadata_read_index: current.own.read_index(),
            metadata_context_id: current.own.context_id(),
            checksum_sha256: String::new(),
        };
        reply.checksum_sha256 = reply.checksum()?;
        reply.require_request(request)?;
        current.require_current()?;
        token.check()?;
        Ok(reply)
    }
}

#[cfg(test)]
fn original_old_remote_phase(
    host: &NativeMetadataControlHost,
    range: crate::RangeId,
) -> Result<Arc<NativeRemoteNewParticipant>> {
    let initial = host
        .data
        .as_ref()
        .ok_or_else(|| invalid("original OLD host absent"))?;
    let held = initial
        .old_closures
        .try_lock()
        .ok_or_else(|| invalid("original OLD table busy"))?
        .get(&range)
        .cloned()
        .ok_or_else(|| invalid("original OLD closure absent"))?;
    let phase = held
        .try_lock()
        .ok_or_else(|| invalid("original OLD phase custody busy"))?
        .remote_new
        .as_ref()
        .cloned()
        .ok_or_else(|| invalid("original OLD remote NEW phase absent"))?;
    Ok(phase)
}
#[cfg(test)]
impl NativeMetadataControlHost {
    pub(crate) fn test_arm_old_remote_native_decision_cut(
        &self,
        range: crate::RangeId,
        code: u8,
    ) -> Result<()> {
        original_old_remote_phase(self, range)?.test_arm_remote_decision_cut(code)
    }
    pub(crate) fn test_old_remote_native_decision_evidence(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        original_old_remote_phase(self, range)?.test_remote_decision_evidence()
    }
}
