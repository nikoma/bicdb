//! Genuine Registered NEW participant behind the original immutable receiver.
//! Fresh Role/OWN authorizes each mutation; retained DATA is never that Role.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_native_host::provisional_listener::registration::remote_new_metadata::RemoteNewRecipientMetadata;
use crate::distribution_range_handoff::{
    NativeJointHandoffPlan, NativeRemoteNewOperationV1, NativeRemoteNewParticipant,
    NativeRemoteNewReplyV1, NativeRemoteNewRequestV1,
};

pub(crate) struct NativeRemoteNewTargetTransition<'a> {
    _held: parking_lot::MutexGuard<'a, Option<Arc<NativeRemoteNewParticipant>>>,
}
impl NativeRegisteredRemoteJointOwner {
    /// Close future actual target pin acquisition under its ORIGINAL owner
    /// transition lock before the final unique-Arc/drop check. A count alone
    /// is not sufficient; the retained exact phase must already be closing.
    pub(crate) fn borrow_remote_new_transition(
        &self,
        phase: &NativeRemoteNewParticipant,
    ) -> Result<NativeRemoteNewTargetTransition<'_>> {
        let held = self
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("remote NEW original target acquisition transition busy"))?;
        if held
            .as_ref()
            .is_none_or(|actual| !std::ptr::eq(actual.as_ref(), phase))
            || !phase.handoff_started()
        {
            return Err(invalid(
                "remote NEW target transition changed original phase or is not closed",
            ));
        }
        Ok(NativeRemoteNewTargetTransition { _held: held })
    }
}

impl NativeRegisteredReplicaAdmission {
    pub(crate) fn observe_remote_new(
        &self,
        peer: &MetadataReadParticipant,
        request: &NativeRemoteNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteNewReplyV1> {
        let token = request.cancellation(parent)?;
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original Registered backend ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original Registered runtime ended"))?;
        self.require_listener(&backend.service, &runtime)?;
        let first = RemoteNewRecipientMetadata::acquire(
            &backend.provider,
            &backend.transport,
            backend.metadata.clone(),
            &backend.service,
            peer,
            request,
            matches!(
                &request.operation,
                NativeRemoteNewOperationV1::Materialize
                    | NativeRemoteNewOperationV1::PublishDecision { .. }
            ),
            &token,
        )?;
        if first.metadata_store_id() != self.metadata_store_id {
            return Err(invalid(
                "remote NEW Registered original MStore UUID changed",
            ));
        }
        let table = self
            .joint_targets
            .upgrade()
            .ok_or_else(|| invalid("remote NEW original Registered owner table ended"))?;
        let owner = {
            let table = table
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original Registered table busy"))?;
            table
                .get(&request.manifest.capture_range.id)
                .cloned()
                .ok_or_else(|| {
                    invalid("remote NEW requires original actual native base/catalogue/A owner")
                })?
        };
        if !Arc::ptr_eq(owner.service(), &backend.service) {
            return Err(invalid(
                "remote NEW original Registered actual service differs",
            ));
        }
        let state = {
            let mut held = owner
                .remote_new
                .try_lock()
                .ok_or_else(|| invalid("remote NEW original Registered participant busy"))?;
            if held.is_none() {
                if !matches!(&request.operation, NativeRemoteNewOperationV1::Materialize) {
                    return Err(invalid("remote NEW observation/preparation cannot create original Registered participant"));
                }
                let role = first.role.as_ref().ok_or_else(|| {
                    invalid("remote NEW target creation needs genuine recipient Role")
                })?;
                let plan = NativeJointHandoffPlan::from_actual_role_and_own(
                    role,
                    &first.own,
                    first.business,
                )?;
                let state = NativeRemoteNewParticipant::from_actual_registered(
                    request,
                    &owner,
                    &plan,
                    role,
                    &first.own,
                    first.business,
                    &first.point,
                    first.metadata_store_id(),
                    &token,
                )?;
                // Original actual owner/A/freeze/materialization pin is kept
                // before metadata/postcheck/reply failure can lose the result.
                *held = Some(Arc::new(state));
            }
            held.as_ref().unwrap().clone()
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
                .ok_or_else(|| invalid("remote DECISION initial actual Role absent"))?;
            let plan =
                NativeJointHandoffPlan::from_actual_role_and_own(role, &first.own, first.business)?;
            let source = backend.transport.fetch_remote_decision_source(
                &plan,
                &request.manifest,
                &first.own,
                first.business,
                &token,
            )?;
            let fetched = backend.transport.fetch_remote_native_new_fact_set(
                request,
                &first.own,
                first.business,
                &token,
            )?;
            drop(first);
            let fresh = RemoteNewRecipientMetadata::acquire(
                &backend.provider,
                &backend.transport,
                backend.metadata.clone(),
                &backend.service,
                peer,
                request,
                true,
                &token,
            )?;
            if fresh.metadata_store_id() != self.metadata_store_id {
                return Err(invalid(
                    "remote DECISION changed original target MStore UUID",
                ));
            }
            native_decision = Some(
                state.publish_remote_decision(
                    request,
                    fetched,
                    source,
                    fresh
                        .role
                        .as_ref()
                        .ok_or_else(|| invalid("remote DECISION final actual Role absent"))?
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
            // The ORIGINAL source must already have completed S2 retirement.
            // Fetch its real historical terminal before preventing export-pin
            // reacquisition; otherwise a still-live capture could deadlock.
            let terminal = backend.transport.observe_remote_source_export_terminal(
                &request.manifest,
                &first.own,
                first.business,
                &token,
            )?;
            let fetched = backend.transport.fetch_remote_native_new_fact_set(
                request,
                &first.own,
                first.business,
                &token,
            )?;
            drop(first);
            let fresh = RemoteNewRecipientMetadata::acquire(
                &backend.provider,
                &backend.transport,
                backend.metadata.clone(),
                &backend.service,
                peer,
                request,
                true,
                &token,
            )?;
            if fresh.metadata_store_id() != self.metadata_store_id {
                return Err(invalid(
                    "remote NEW target preparation changed original MStore UUID",
                ));
            }
            state.prepare_decision_pin_handoff(
                request,
                fetched,
                Some(terminal),
                fresh
                    .role
                    .as_ref()
                    .ok_or_else(|| invalid("remote NEW target preparation lacks actual Role"))?
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
        self.require_listener(&backend.service, &runtime)?;
        owner.require_first_policy_attempt_lifetime()?;
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
impl NativeMetadataControlHost {
    pub(crate) fn test_arm_remote_native_decision_cut(
        &self,
        range: crate::RangeId,
        code: u8,
    ) -> Result<()> {
        let owner = self.registered_remote_joint_owner(range)?;
        let phase = owner
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION original phase busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("remote DECISION original phase absent"))?;
        phase.test_arm_remote_decision_cut(code)
    }
    pub(crate) fn test_remote_native_decision_current_reader_refusals(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let owner = self.registered_remote_joint_owner(range)?;
        let service = owner
            .service()
            .try_lock()
            .ok_or_else(|| invalid("reader control original service busy"))?;
        let database = service.database();
        let db = database
            .try_read()
            .ok_or_else(|| invalid("reader control original DB busy"))?;
        db.test_remote_native_decision_current_reader_refusals(owner.target().native(), range)
    }
    pub(crate) fn test_remote_native_decision_evidence(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        // MEMORY-ONLY custody observation after WriteFailed. Do not run the
        // ordinary actual-native identity getter on an intentionally fenced DB.
        // This cannot issue an owner handle or recover any native authority.
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("remote DECISION original receiver absent"))?;
        let owner = receiver
            .remote_joint_targets
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION retained owner table busy"))?
            .get(&range)
            .cloned()
            .ok_or_else(|| invalid("remote DECISION original owner absent"))?;
        let phase = owner
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION original phase busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("remote DECISION original phase absent"))?;
        phase.test_remote_decision_evidence()
    }
    pub(crate) fn test_arm_remote_new_cancel_at_handoff(
        &self,
        range: crate::RangeId,
        after: bool,
    ) -> Result<()> {
        let owner = self.registered_remote_joint_owner(range)?;
        let phase = owner
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("remote NEW diagnostic owner busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("remote NEW actual participant not created"))?;
        phase.test_arm_original_request_cancel(after)
    }
    pub(crate) fn test_remote_new_phase_evidence(
        &self,
        range: crate::RangeId,
    ) -> Result<serde_json::Value> {
        let owner = self.registered_remote_joint_owner(range)?;
        let phase = owner
            .remote_new
            .try_lock()
            .ok_or_else(|| invalid("remote NEW diagnostic owner busy"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| invalid("remote NEW actual participant not created"))?;
        phase.test_evidence()
    }
}
