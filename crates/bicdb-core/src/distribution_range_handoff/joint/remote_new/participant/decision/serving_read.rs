//! Post-membership read of this SAME private known native outcome and its
//! successful owned catalogue publication. No Pending callback is renewed.
use super::*;
use crate::distribution_native_host::{
    NativeRemoteServingPointV1, NativeRemoteServingReplyV1, NativeRemoteServingRequestV1,
};
mod released;

pub(crate) struct NativeRemoteServingReadAdmission<'a> {
    participant: &'a NativeRemoteNewParticipant,
    request: &'a NativeRemoteServingRequestV1,
    role: &'a RecoveryRoleAdmission,
    own: &'a HostMetadataReadObservation,
    business: Uuid,
    metadata: &'a ActualNativeJointMetadataPoint,
    caller: &'a CancellationToken,
    receipt: &'a crate::db::NativeJointDecisionReceipt,
    native: &'a crate::db::NativeRemoteJointDecisionAttempt,
}
impl NativeRemoteServingReadAdmission<'_> {
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.caller.check()?;
        self.participant.require_lifetime_memory()?;
        self.role.require_live_request()?;
        self.own.require_live_request(self.business)?;
        self.metadata.require_current_memory()?;
        if self.request.phase_id != self.participant.snapshot.phase_id
            || self.request.manifest.checksum_sha256
                != self.participant.snapshot.binding.manifest_sha256
            || self.request.decision_sha256 != self.receipt.decision_sha256()
            || self.role.local_node_id() != &self.participant.snapshot.node_id
        {
            return Err(invalid(
                "serving protected read changed original actual participant/phase",
            ));
        }
        self.native.actual_committed_xid()?;
        Ok(())
    }
    /// Full immutable-domain/topology validation is outside short guards.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_memory_current()?;
        self.role
            .require_own_host_metadata(self.own, self.business)?;
        self.metadata.require_request(self.own, self.business)?;
        if self.role.purpose() != &self.request.purpose()
            || self.role.context_id() == self.receipt.metadata_context_id()
            || self.role.metadata_term() < self.receipt.metadata_anchor().0
            || self.role.metadata_read_index() < self.receipt.metadata_anchor().1
        {
            return Err(invalid(
                "serving protected read changed fresh purpose/metadata anchors",
            ));
        }
        self.receipt.data().require_metadata_domain(
            self.own.committed_topology(self.business)?,
            self.own.restore_activation(self.business)?,
        )?;
        self.require_memory_current()
    }
    pub(crate) fn require_native(
        &self,
        db: &crate::BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_current()?;
        self.metadata.require_native_store_point(db, store)?;
        if db.incarnation_id() != self.participant.snapshot.database_open
            || store.native_writer_store_open_id() != self.participant.snapshot.store_open
        {
            return Err(invalid(
                "serving protected read replaced original actual DB/Store open",
            ));
        }
        db.observe_known_remote_native_decision(self.receipt, self.native)?;
        self.require_memory_current()
    }
    pub(crate) fn require_old_parts(
        &self,
        db: &crate::BicDb,
        writer: &crate::db::NativeWriterHostState,
        native: &crate::db::NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.require_memory_current()?;
        match &self.participant.original {
            OriginalParticipant::Old {
                writer: original, ..
            } if std::ptr::eq(original.as_ref(), writer) => {
                let old = self.participant.original_old()?;
                old.require_remote_decision_parts(native, promise, frozen)?;
                old.require_remote_decision_predecessor(db, writer)
            }
            _ => Err(invalid(
                "serving protected read changed original OLD State/closure/promise/lease",
            )),
        }
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.receipt.data()
    }
    pub(crate) fn parent(&self) -> &CancellationToken {
        self.caller
    }
    pub(crate) fn original_participant_sha256(&self) -> &str {
        &self.participant.snapshot.checksum_sha256
    }
    pub(crate) fn receipt(&self) -> &crate::db::NativeJointDecisionReceipt {
        self.receipt
    }
}
impl NativeRemoteNewParticipant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_original_decision_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        if request.operation
            != crate::distribution_native_host::NativeRemoteServingOperationV1::ObserveDecision
        {
            return Err(invalid(
                "serving native read requires distinct readonly decision operation",
            ));
        }
        self.observe_original_serving_point(request, role, own, business, metadata, parent)
    }
    #[allow(clippy::too_many_arguments)]
    fn observe_original_serving_point(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        self.serving_queue_for_original(request, role, own, business, metadata)?;
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("serving original decision custody busy"))?;
        let receipt = held
            .completed
            .as_ref()
            .ok_or_else(|| invalid("serving actual successful native helper return absent"))?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("serving private original native attempt absent"))?;
        if held.expected.as_ref() != Some(receipt.data()) {
            return Err(invalid("serving exact original decision body changed"));
        }
        let catalogue = held
            .catalogue
            .as_ref()
            .ok_or_else(|| invalid("serving successful owned catalogue absent"))?;
        let scope = NativeRemoteServingReadAdmission {
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
        scope.require_current()?;
        metadata.require_external_current()?;
        let inspect = |db: &crate::BicDb,
                       store: &crate::distribution_range_consensus::RangeWriteStore,
                       frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit|
         -> Result<NativeRemoteServingPointV1> {
            scope.require_native(db, store)?;
            let actual_current_catalogue_sha = catalogue.observe_for_serving(db, store, &scope)?;
            let point = NativeRemoteServingPointV1 {
                phase_id: self.snapshot.phase_id,
                original_participant_sha256: self.snapshot.checksum_sha256.clone(),
                decision_sha256: receipt.decision_sha256().into(),
                native_decision_record_sha256: receipt.checksum_sha256().into(),
                remote_provenance_sha256: native.actual_provenance_sha256()?.into(),
                catalogue_decision_sha256: catalogue.candidate_sha256().into(),
                current_catalogue_sha256: actual_current_catalogue_sha,
                native_decision_xid: native.actual_committed_xid()?,
                native_database_open: db.incarnation_id().into(),
                store_open: store.native_writer_store_open_id(),
                preparation: None,
                native_bridge_sha256: None,
                catalogue_activation_sha256: None,
                original_frozen_owner: frozen.owner(),
                actual_dropped_open: false,
                actual_writer_installed: false,
            };
            scope.require_native(db, store)?;
            let _version = metadata.try_publication_guard()?;
            scope.require_memory_current()?;
            Ok(point)
        };
        let actual = match &self.original {
            OriginalParticipant::Old { writer, .. } => self
                .original_old()?
                .observe_remote_serving_decision(writer, &scope, inspect)?,
            OriginalParticipant::Registered {
                target,
                service,
                owner,
                ..
            } => {
                owner
                    .upgrade()
                    .ok_or_else(|| invalid("serving original target owner ended"))?
                    .require_first_policy_attempt_lifetime()?;
                let frozen = self.original_registered_freeze()?;
                let service = service
                    .try_lock()
                    .ok_or_else(|| invalid("serving original target service busy"))?;
                let database = service.database();
                let db = database
                    .try_read()
                    .ok_or_else(|| invalid("serving original target DB busy"))?;
                let physical =
                    db.registered_joint_exclusion_scope(target, frozen.try_as_frozen()?, parent)?;
                service.with_writer_data_domain_store(&db, |store| {
                    physical.require_current(&db, store)?;
                    let result = inspect(&db, store, frozen.try_as_frozen()?);
                    match result {
                        Err(primary) => Err(primary),
                        Ok(value) => {
                            physical.require_current(&db, store)?;
                            Ok(value)
                        }
                    }
                })?
            }
        };
        parent.check()?;
        let reply = NativeRemoteServingReplyV1 {
            request: request.clone(),
            node_id: self.snapshot.node_id.clone(),
            actual,
            metadata_term: own.term(),
            metadata_read_index: own.read_index(),
            metadata_context: own.context_id(),
        };
        reply.require_request(request)?;
        scope.require_current()?;
        Ok(reply)
    }
}

impl NativeRemoteNewParticipant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_actual_preparation_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
        preparation: &crate::distribution_range_handoff::NativeRemotePreparedObservation,
    ) -> Result<NativeRemoteServingReplyV1> {
        if !matches!(
            request.operation,
            crate::distribution_native_host::NativeRemoteServingOperationV1::Prepare
                | crate::distribution_native_host::NativeRemoteServingOperationV1::PublishBridge
                | crate::distribution_native_host::NativeRemoteServingOperationV1::ObserveBridge
                | crate::distribution_native_host::NativeRemoteServingOperationV1::ResolveBridge
                | crate::distribution_native_host::NativeRemoteServingOperationV1::ObservePrepared
                | crate::distribution_native_host::NativeRemoteServingOperationV1::ResolvePrepared
        ) {
            return Err(invalid(
                "protected preparation reply needs actual Prepare/ObservePrepared step",
            ));
        }
        preparation.require_original_remote_participant(&self.snapshot.checksum_sha256)?;
        preparation.require_current()?;
        let mut actual =
            self.observe_original_serving_point(request, role, own, business, metadata, parent)?;
        actual.actual.preparation = Some(preparation.vote().clone());
        let (term, index, context) = preparation.metadata_anchor();
        if term < own.term() || index < own.read_index() {
            return Err(invalid(
                "protected preparation observation regressed ingress metadata anchors",
            ));
        }
        // Attribute the response to the actual later physical observation,
        // rather than claiming its vote used the earlier ingress MQ context.
        actual.metadata_term = term;
        actual.metadata_read_index = index;
        actual.metadata_context = context;
        preparation.require_current()?;
        parent.check()?;
        role.require_live_request()?;
        own.require_live_request(business)?;
        preparation.require_request_memory()?;
        let _guard = metadata.try_publication_guard()?;
        parent.check()?;
        role.require_live_request()?;
        own.require_live_request(business)?;
        preparation.require_request_memory()?;
        drop(_guard);
        preparation.require_protected_wire_boundary(role, own, business, metadata, parent)?;
        actual.require_request(request)?;
        Ok(actual)
    }
}

impl NativeRemoteNewParticipant {
    /// Necessary equality to this original private known native attempt, not
    /// a new current grant and never an upgrade of a status/visible record.
    pub(crate) fn require_original_committed_for_serving(
        &self,
        db: &crate::BicDb,
        receipt: &crate::db::NativeJointDecisionReceipt,
    ) -> Result<()> {
        self.require_lifetime_memory()?;
        let actual = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("serving original native decision busy at physical boundary"))?;
        let completed = actual
            .completed
            .as_ref()
            .ok_or_else(|| invalid("serving original successful native read absent"))?;
        if actual.expected.as_ref() != Some(receipt.data())
            || completed.checksum_sha256() != receipt.checksum_sha256()
        {
            return Err(invalid(
                "serving physical boundary substituted original native receipt/body",
            ));
        }
        let native = actual.native.as_ref().ok_or_else(|| {
            invalid("serving original native attempt absent at physical boundary")
        })?;
        db.observe_known_remote_native_decision(receipt, native)?;
        actual
            .catalogue
            .as_ref()
            .ok_or_else(|| invalid("serving original decision catalogue absent"))?
            .require_original_successful_publication()?;
        self.require_lifetime_memory()
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retain_original_serving_lease(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        point: &ActualNativeJointMetadataPoint,
    ) -> Result<(
        crate::distribution_data::NativeJointServingLease,
        Option<crate::db::NativeJointWriterDomain>,
    )> {
        self.serving_queue_for_original(request, role, own, business, point)?;
        self.require_original_active()?;
        match &self.original {
            OriginalParticipant::Old { .. } => {
                let (service, lease) = self.original_old()?.retained_serving_lease()?;
                if service
                    .try_lock()
                    .ok_or_else(|| invalid("serving original OLD service busy at lease capture"))?
                    .node_id()
                    != &self.snapshot.node_id
                {
                    return Err(invalid(
                        "serving original OLD lease substituted actual service",
                    ));
                }
                Ok((lease, None))
            }
            OriginalParticipant::Registered { owner, pin, .. } => {
                if pin
                    .try_lock()
                    .ok_or_else(|| invalid("serving original target pin busy at lease capture"))?
                    .is_some()
                    || !self.progress.target_materialization.load(Ordering::Acquire)
                    || !self.progress.target_export.load(Ordering::Acquire)
                {
                    return Err(invalid(
                        "serving target lease requires actual completed read-pin handoff",
                    ));
                }
                let owner = owner.upgrade().ok_or_else(|| {
                    invalid("serving original target owner ended at lease capture")
                })?;
                owner.require_first_policy_attempt_lifetime()?;
                let frozen = self.original_registered_freeze()?;
                let domain = crate::db::NativeJointWriterDomain::Registered(Arc::downgrade(&owner));
                Ok((
                    crate::distribution_data::NativeJointServingLease::Registered { owner, frozen },
                    Some(domain),
                ))
            }
        }
    }
}

impl NativeRemoteNewParticipant {
    /// Install only a weak route to the SAME genuine host-retained owner.
    /// No preparation, pin or process lifetime is reconstructed here.
    pub(crate) fn retain_remote_preparation_route(
        &self,
        actual: &Arc<crate::distribution_range_handoff::NativeJointPreparationCustody>,
    ) -> Result<()> {
        self.require_lifetime_memory()?;
        actual.require_remote_participant(&self.snapshot.checksum_sha256)?;
        let mut slot = self
            .remote_prepared
            .try_lock()
            .ok_or_else(|| invalid("remote preparation observation route busy"))?;
        if let Some(old) = slot.as_ref() {
            if old
                .upgrade()
                .is_none_or(|owner| !Arc::ptr_eq(&owner, actual))
            {
                return Err(invalid(
                    "remote preparation cannot replace ended/original route owner",
                ));
            }
        } else {
            *slot = Some(Arc::downgrade(actual));
        }
        self.require_lifetime_memory()
    }
    /// Direct readonly physical observation avoids queuing an RPC behind its
    /// own serialized host while that host collects actual ALL preparations.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_retained_preparation_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        transport: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        if request.operation
            != crate::distribution_native_host::NativeRemoteServingOperationV1::ObservePrepared
        {
            return Err(invalid(
                "direct preparation route is readonly ObservePrepared only",
            ));
        }
        self.serving_queue_for_original(request, role, own, business, metadata)?;
        let owner = self
            .remote_prepared
            .try_lock()
            .ok_or_else(|| invalid("remote preparation observation route busy"))?
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| invalid("actual original host preparation owner unavailable"))?;
        owner.require_remote_participant(&self.snapshot.checksum_sha256)?;
        let actual = owner.observe_remote_preparation(transport, metadata_leader, parent)?;
        self.observe_actual_preparation_for_serving(
            request, role, own, business, metadata, parent, &actual,
        )
    }
}

impl NativeRemoteNewParticipant {
    /// Weak route only to the SAME actual host-owned completed native bridge.
    /// It neither prolongs its custody nor manufactures a missing owner.
    pub(crate) fn retain_remote_bridge_route(
        &self,
        custody: &Arc<crate::distribution_range_handoff::NativeJointPreparationCustody>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
    ) -> Result<()> {
        self.require_lifetime_memory()?;
        custody.require_remote_participant(&self.snapshot.checksum_sha256)?;
        {
            let actual = self
                .decision
                .try_lock()
                .ok_or_else(|| invalid("remote bridge route original decision busy"))?;
            let original = actual
                .completed
                .as_ref()
                .ok_or_else(|| invalid("remote bridge route original native read absent"))?;
            if original.data() != bridge.data() {
                return Err(invalid("remote bridge route changed original decision"));
            }
        }
        let mut route = self
            .remote_bridge
            .try_lock()
            .ok_or_else(|| invalid("remote bridge readonly route busy"))?;
        if let Some((c, n)) = route.as_ref() {
            if c.upgrade().is_none_or(|a| !Arc::ptr_eq(&a, custody))
                || n.upgrade().is_none_or(|a| !Arc::ptr_eq(&a, bridge))
            {
                return Err(invalid(
                    "remote bridge readonly route replaced original ended owner",
                ));
            }
        } else {
            *route = Some((Arc::downgrade(custody), Arc::downgrade(bridge)));
        }
        self.require_lifetime_memory()
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_retained_open_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        transport: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteServingReplyV1> {
        use crate::distribution_native_host::NativeRemoteServingOperationV1 as Op;
        if matches!(request.operation, Op::ObserveOpen | Op::ObserveReleased) {
            let route = self
                .remote_release
                .try_lock()
                .ok_or_else(|| invalid("remote release readonly route busy"))?
                .clone();
            if let Some(route) = route {
                let original = route.upgrade().ok_or_else(|| {
                    invalid("remote release original successor ended; no reconstruction")
                })?;
                let observed = original.observe_current(transport, metadata_leader, parent)?;
                return self.observe_actual_release_for_serving(
                    request, role, own, business, metadata, parent, &observed,
                );
            }
            if request.operation == Op::ObserveReleased {
                return Err(invalid(
                    "released observation requires actual original successor",
                ));
            }
        }
        if !matches!(request.operation, Op::ObserveBridge | Op::ObserveOpen) {
            return Err(invalid(
                "direct Open route accepts distinct readonly bridge/Open observations only",
            ));
        }
        self.serving_queue_for_original(request, role, own, business, metadata)?;
        let (custody, native) = {
            let route = self
                .remote_bridge
                .try_lock()
                .ok_or_else(|| invalid("remote bridge readonly route busy"))?;
            let (c, n) = route
                .as_ref()
                .ok_or_else(|| invalid("actual original host bridge route absent"))?;
            (
                c.upgrade()
                    .ok_or_else(|| invalid("actual original host preparation owner ended"))?,
                n.upgrade()
                    .ok_or_else(|| invalid("actual original host native bridge owner ended"))?,
            )
        };
        let observed = custody.observe_remote_open(native, transport, metadata_leader, parent)?;
        self.observe_actual_open_for_serving(
            request, role, own, business, metadata, parent, &observed,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_actual_open_for_serving(
        &self,
        request: &NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
        observed: &crate::distribution_range_handoff::NativeRemoteOpenObservation,
    ) -> Result<NativeRemoteServingReplyV1> {
        use crate::distribution_native_host::NativeRemoteServingOperationV1 as Op;
        if !matches!(
            request.operation,
            Op::Open | Op::ReconcileOpen | Op::ObserveOpen | Op::ObserveBridge
        ) {
            return Err(invalid(
                "actual Open reply requires original Open or distinct readonly operation",
            ));
        }
        self.serving_queue_for_original(request, role, own, business, metadata)?;
        observed.require_participant(&self.snapshot.checksum_sha256)?;
        metadata.require_external_current()?;
        let (term, index, _) = observed.metadata_anchor();
        if term < own.term() || index < own.read_index() {
            return Err(invalid(
                "remote Open protected observation regressed ingress metadata anchors",
            ));
        }
        let actual =
            observed.inspect(
                |physical, store, head, activation, frozen_owner| {
                    let held = self
                        .decision
                        .try_lock()
                        .ok_or_else(|| invalid("remote Open original native custody busy"))?;
                    let receipt = held.completed.as_ref().ok_or_else(|| {
                        invalid("remote Open original successful native read absent")
                    })?;
                    let native = held.native.as_ref().ok_or_else(|| {
                        invalid("remote Open original private native attempt absent")
                    })?;
                    let catalogue = held
                        .catalogue
                        .as_ref()
                        .ok_or_else(|| invalid("remote Open original catalogue owner absent"))?;
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
                    observed.require_native_receipt(receipt)?;
                    catalogue.require_original_successful_publication()?;
                    if request.operation == Op::ObserveOpen && activation.is_none() {
                        return Err(invalid("actual remote Open catalogue activation absent"));
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
                        catalogue_activation_sha256: activation,
                        original_frozen_owner: frozen_owner,
                        actual_dropped_open: false,
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
        parent.check()?;
        observed.require_memory_current()?;
        Ok(reply)
    }
}
