//! Direct protected reads of the actual host-retained release successor. Old
//! preparation/Frozen slots are never revived after one-way retirement.
use super::*;
impl NativeRemoteNewParticipant {
    pub(crate) fn retain_remote_release_route(
        &self,
        actual: &Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
    ) -> Result<()> {
        self.require_lifetime_memory()?;
        actual.require_participant(self)?;
        let mut slot = self
            .remote_release
            .try_lock()
            .ok_or_else(|| invalid("remote release original observation route busy"))?;
        if let Some(previous) = slot.as_ref() {
            if !Weak::ptr_eq(previous, &Arc::downgrade(actual)) {
                return Err(invalid(
                    "remote release route cannot replace original successor",
                ));
            }
        } else {
            *slot = Some(Arc::downgrade(actual));
        }
        self.require_lifetime_memory()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_actual_release_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
        observed: &crate::distribution_data::NativeRemoteReleaseObservation,
    ) -> Result<NativeRemoteServingReplyV1> {
        use crate::distribution_native_host::NativeRemoteServingOperationV1 as Op;
        if !matches!(
            request.operation,
            Op::ObserveOpen | Op::ObserveReleased | Op::Release | Op::ReconcileRelease
        ) {
            return Err(invalid(
                "release response requires actual Release or distinct protected read",
            ));
        }
        self.serving_queue_for_original(request, role, own, business, metadata)?;
        observed.owner().require_participant(self)?;
        metadata.require_external_current()?;
        let (term, index, _) = observed.metadata_anchor();
        if term < own.term() || index < own.read_index() {
            return Err(invalid(
                "released observation regressed original ingress metadata anchors",
            ));
        }
        if matches!(
            request.operation,
            Op::ObserveReleased | Op::Release | Op::ReconcileRelease
        ) {
            observed.require_released_open()?;
        }
        let actual = observed.inspect(
            |physical, store, head, activation, frozen_owner, released| {
                let held = self
                    .decision
                    .try_lock()
                    .ok_or_else(|| invalid("released observation original native custody busy"))?;
                let receipt = held.completed.as_ref().ok_or_else(|| {
                    invalid("released observation original known native return absent")
                })?;
                let native = held.native.as_ref().ok_or_else(|| {
                    invalid("released observation original native attempt absent")
                })?;
                let catalogue = held.catalogue.as_ref().ok_or_else(|| {
                    invalid("released observation original decision catalogue absent")
                })?;
                let admission = NativeRemoteServingReadAdmission {
                    participant: self,
                    request,
                    role,
                    own,
                    business,
                    metadata,
                    caller: parent,
                    receipt,
                    native,
                };
                admission.require_current()?;
                admission.require_native(physical.database(), store)?;
                if observed
                    .actual()
                    .committed()
                    .native_receipt()
                    .checksum_sha256()
                    != receipt.checksum_sha256()
                    || physical.native().checksum_sha256()
                        != observed.owner().native().checksum_sha256()
                {
                    return Err(invalid(
                        "released observation substituted actual original native decision/bridge",
                    ));
                }
                catalogue.require_original_successful_publication()?;
                if request.operation != Op::ObserveOpen && !released {
                    return Err(invalid(
                        "released response still has Frozen or unarmed Open",
                    ));
                }
                Ok(NativeRemoteServingPointV1 {
                    phase_id: self.snapshot.phase_id,
                    original_participant_sha256: self.snapshot.checksum_sha256.clone(),
                    decision_sha256: receipt.decision_sha256().into(),
                    native_decision_record_sha256: receipt.checksum_sha256().into(),
                    remote_provenance_sha256: native.actual_provenance_sha256()?.into(),
                    catalogue_decision_sha256: catalogue.candidate_sha256().into(),
                    current_catalogue_sha256: head,
                    native_decision_xid: native.actual_committed_xid()?,
                    native_database_open: physical.database().incarnation_id().into(),
                    store_open: store.native_writer_store_open_id(),
                    preparation: Some(observed.vote().clone()),
                    native_bridge_sha256: Some(physical.native().checksum_sha256().into()),
                    catalogue_activation_sha256: Some(activation),
                    original_frozen_owner: frozen_owner,
                    actual_dropped_open: released,
                    actual_writer_installed: false,
                })
            },
            || {
                parent.check()?;
                self.require_lifetime_memory()?;
                role.require_live_request()?;
                own.require_live_request(business)?;
                metadata.require_current_memory()
            },
        )?;
        let (term, index, context) = observed.metadata_anchor();
        let reply = NativeRemoteServingReplyV1 {
            request: request.clone(),
            node_id: self.snapshot.node_id.clone(),
            actual,
            metadata_term: term,
            metadata_read_index: index,
            metadata_context: context,
        };
        reply.require_request(request)?;
        observed.require_memory_current()?;
        parent.check()?;
        Ok(reply)
    }
}
