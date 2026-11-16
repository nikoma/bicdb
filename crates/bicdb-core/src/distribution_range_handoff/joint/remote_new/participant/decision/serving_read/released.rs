//! Direct protected reads of the actual host-retained release successor. Old
//! preparation/Frozen slots are never revived after one-way retirement.
use super::*;
/// Distinct released-owner read; generic prepublication/empty-hook read
/// admission stays strict. No Stored record or failed constructor selects it.
struct ReleasedRead<'a> {
    original: NativeRemoteServingReadAdmission<'a>,
    successor: &'a crate::distribution_data::NativeRemoteReleaseOwner,
}
impl ReleasedRead<'_> {
    fn require_memory(&self) -> Result<()> {
        let p = &self.original;
        p.caller.check()?;
        p.participant
            .require_released_successor_memory(self.successor)?;
        p.role.require_live_request()?;
        p.own.require_live_request(p.business)?;
        p.metadata.require_current_memory()?;
        if p.request.phase_id != p.participant.snapshot.phase_id
            || p.request.manifest.checksum_sha256 != p.participant.snapshot.binding.manifest_sha256
            || p.request.decision_sha256 != p.receipt.decision_sha256()
            || p.role.local_node_id() != &p.participant.snapshot.node_id
        {
            return Err(invalid(
                "released protected read changed original participant/phase",
            ));
        }
        p.native.actual_committed_xid()?;
        Ok(())
    }
    fn require_current(&self) -> Result<()> {
        self.require_memory()?;
        let p = &self.original;
        p.role.require_own_host_metadata(p.own, p.business)?;
        p.metadata.require_request(p.own, p.business)?;
        if p.role.purpose() != &p.request.purpose()
            || p.role.context_id() == p.receipt.metadata_context_id()
            || p.role.metadata_term() < p.receipt.metadata_anchor().0
            || p.role.metadata_read_index() < p.receipt.metadata_anchor().1
        {
            return Err(invalid(
                "released protected read changed fresh purpose/metadata anchors",
            ));
        }
        p.receipt.data().require_metadata_domain(
            p.own.committed_topology(p.business)?,
            p.own.restore_activation(p.business)?,
        )?;
        self.require_memory()
    }
    fn require_native(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_current()?;
        let p = &self.original;
        p.metadata.require_native_store_point(db, store)?;
        if db.incarnation_id() != p.participant.snapshot.database_open
            || store.native_writer_store_open_id() != p.participant.snapshot.store_open
        {
            return Err(invalid(
                "released protected read replaced original DB/Store open",
            ));
        }
        db.observe_known_remote_native_decision(p.receipt, p.native)?;
        self.require_memory()
    }
}

impl NativeRemoteNewParticipant {
    /// Only the genuine retained release successor selects this lifetime.
    /// The ordinary original empty-hook/current getters remain unchanged.
    pub(crate) fn require_released_successor_memory(
        &self,
        actual: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> Result<()> {
        actual.require_original_participant_identity(self)?;
        match &self.original {
            OriginalParticipant::Old {
                admission,
                writer,
                process,
                ..
            } => {
                process.require_original_process()?;
                admission.require_remote_new_original_memory(writer)
            }
            OriginalParticipant::Registered { owner, target, .. } => {
                let owner = owner
                    .upgrade()
                    .ok_or_else(|| invalid("released original Registered owner ended"))?;
                owner.require_first_policy_attempt_lifetime()?;
                if let Some(writer) = owner.terminal_writer() {
                    // This field is minted only by the actual FIRST native
                    // installer after retaining its Installed guard. A live
                    // registration is necessary, not serving/Ready authority.
                    writer.require_remote_new_registration_memory()
                } else {
                    target
                        .original_hook()
                        .require_remote_new_registration_memory()
                }
            }
        }
    }

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
            Op::ObserveOpen
                | Op::ObserveReleased
                | Op::ObserveReleasedForPolicy
                | Op::Release
                | Op::ReconcileRelease
        ) {
            return Err(invalid(
                "release response requires actual Release or distinct protected read",
            ));
        }
        self.serving_queue_for_released(request, role, own, business, metadata, observed.owner())?;
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
            Op::ObserveReleased | Op::ObserveReleasedForPolicy | Op::Release | Op::ReconcileRelease
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
                let admission = ReleasedRead { successor: observed.owner(), original: NativeRemoteServingReadAdmission {
                    participant: self,
                    request,
                    role,
                    own,
                    business,
                    metadata,
                    caller: parent,
                    receipt,
                    native,
                }};
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
                    policy_released_audit: if request.operation == Op::ObserveReleasedForPolicy {
                        Some(crate::distribution_native_host::NativeRemoteReleasedPolicyAuditV1::from_protected_release(observed.actual())?)
                    } else { None },
                })
            },
            || {
                parent.check()?;
                self.require_released_successor_memory(observed.owner())?;
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
