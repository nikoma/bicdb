//! Genuine target-owned remote DECISION. No local quorum capability is made.
use super::*;
use crate::distribution_range_handoff::{NativeJointDecisionData, NativeRemoteDecisionReceiptV1};
use crate::distribution_transport::AuthenticatedNativeRemoteDecisionSource;
mod catalogue;
mod serving_read;
pub(crate) use serving_read::NativeRemoteServingReadAdmission;

struct DecisionContext {
    frames: AuthenticatedNativeRemoteNewFactSet,
    source: AuthenticatedNativeRemoteDecisionSource,
    role: Arc<RecoveryRoleAdmission>,
    own: Arc<HostMetadataReadObservation>,
    business: Uuid,
    metadata: Arc<ActualNativeJointMetadataPoint>,
    caller: CancellationToken,
}
#[derive(Default)]
pub(super) struct DecisionCustody {
    expected: Option<NativeJointDecisionData>,
    /// Actual native or fresh KnownRead helper return, retained BEFORE outer
    /// metadata/reply checks. Visible bytes alone never fill this slot.
    completed: Option<crate::db::NativeJointDecisionReceipt>,
    catalogue:
        Option<crate::distribution_range_consensus::NativeRemoteDecisionCataloguePublication>,
    contexts: Vec<Arc<DecisionContext>>,
    encoded_bytes: usize,
    native: Option<crate::db::NativeRemoteJointDecisionAttempt>,
    #[cfg(test)]
    cut: u8,
    #[cfg(test)]
    observed_cut: Option<(u8, Arc<AtomicBool>)>,
    #[cfg(test)]
    lose_success_reply: bool,
    #[cfg(test)]
    lost_success_reply: Option<NativeRemoteDecisionReceiptV1>,
    #[cfg(test)]
    lose_catalogue_reply: bool,
    #[cfg(test)]
    lost_catalogue_reply: Option<NativeRemoteCatalogueDecisionV1>,
    #[cfg(test)]
    fail_catalogue_dirsync: bool,
    #[cfg(test)]
    fail_catalogue_rename: bool,
    #[cfg(test)]
    cancel_catalogue_before_io: bool,
    #[cfg(test)]
    catalogue_dirsync_fault_count: u64,
    #[cfg(test)]
    catalogue_rename_fault_count: u64,
}
/// Constructor is private to the original participant after genuine callbacks.
/// This does NOT implement or convert to a local publication admission.
pub(crate) struct NativeRemoteDecisionAdmission<'a> {
    participant: &'a NativeRemoteNewParticipant,
    context: DecisionRequest<'a>,
    data: &'a NativeJointDecisionData,
    plan: &'a NativeJointHandoffPlan,
}
enum DecisionRequest<'a> {
    Native(&'a DecisionContext),
    KnownRead {
        role: &'a RecoveryRoleAdmission,
        own: &'a HostMetadataReadObservation,
        business: Uuid,
        metadata: &'a ActualNativeJointMetadataPoint,
        caller: &'a CancellationToken,
        native: &'a crate::db::NativeRemoteJointDecisionAttempt,
    },
    Catalogue {
        role: &'a RecoveryRoleAdmission,
        own: &'a HostMetadataReadObservation,
        business: Uuid,
        metadata: &'a ActualNativeJointMetadataPoint,
        caller: &'a CancellationToken,
        completed: &'a crate::db::NativeJointDecisionReceipt,
        #[cfg(test)]
        cancel_before_io: bool,
        native: &'a crate::db::NativeRemoteJointDecisionAttempt,
    },
}
impl NativeRemoteDecisionAdmission<'_> {
    #[cfg(test)]
    pub(crate) fn test_cancel_catalogue_before_io(&self) -> Result<Option<serde_json::Value>> {
        if let DecisionRequest::Catalogue {
            caller,
            native,
            cancel_before_io: true,
            ..
        } = &self.context
        {
            self.require_memory_current()?;
            let (own, _) = self.own_request();
            let now = std::time::Instant::now();
            let remaining = |d: std::time::Instant| d.saturating_duration_since(now).as_micros();
            let evidence = serde_json::json!({"original_server_request_cancelled":true,
                "all_actual_contexts_live_before_cut":true,"role_remaining_us":remaining(self.role().deadline()),
                "own_remaining_us":remaining(own.deadline()),"caller_remaining_us":caller.deadline().map(remaining),
                "actual_native_xid":native.actual_committed_xid()?,"original_participant_sha256":self.original_participant_sha256()});
            self.require_memory_current()?;
            caller.cancel();
            Ok(Some(evidence))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn require_native_mutation(&self) -> Result<()> {
        if !matches!(&self.context, DecisionRequest::Native(_)) {
            return Err(invalid(
                "catalogue continuation cannot re-enter native DECISION WAL",
            ));
        }
        Ok(())
    }
    pub(crate) fn catalogue_receipt(&self) -> Result<&crate::db::NativeJointDecisionReceipt> {
        match &self.context {
            DecisionRequest::Catalogue {
                completed, native, ..
            } => {
                native.actual_committed_xid()?;
                if completed.data() != self.data {
                    return Err(invalid(
                        "catalogue continuation changed original known native body",
                    ));
                }
                Ok(completed)
            }
            _ => Err(invalid(
                "catalogue requires its distinct original known-outcome admission",
            )),
        }
    }
    pub(crate) fn known_read_attempt(
        &self,
    ) -> Result<&crate::db::NativeRemoteJointDecisionAttempt> {
        match &self.context {
            DecisionRequest::KnownRead { native, .. } => {
                native.actual_committed_xid()?;
                Ok(native)
            }
            _ => Err(invalid(
                "known native readback needs distinct fresh KnownRead admission",
            )),
        }
    }
    pub(crate) fn original_participant_sha256(&self) -> &str {
        &self.participant.snapshot.checksum_sha256
    }
    pub(crate) fn require_known_catalogue_native(&self, db: &crate::BicDb) -> Result<()> {
        match &self.context {
            DecisionRequest::Catalogue {
                completed, native, ..
            } => {
                self.catalogue_receipt()?;
                db.observe_known_remote_native_decision(completed, native)
            }
            _ => Err(invalid(
                "remote catalogue lacks actual original known native return",
            )),
        }
    }
    fn own_request(&self) -> (&HostMetadataReadObservation, Uuid) {
        match &self.context {
            DecisionRequest::Native(c) => (&c.own, c.business),
            DecisionRequest::Catalogue { own, business, .. }
            | DecisionRequest::KnownRead { own, business, .. } => (own, *business),
        }
    }
    pub(crate) fn durable_binding(
        &self,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteDecisionBindingV1> {
        let DecisionRequest::Native(context) = &self.context else {
            return Err(invalid(
                "catalogue observation cannot frame another native decision",
            ));
        };
        let value = crate::distribution_range_handoff::NativeRemoteDecisionBindingV1 {
            binding: self.participant.snapshot.binding.clone(),
            phase_id: self.participant.snapshot.phase_id,
            participant: self.participant.snapshot.clone(),
            fact_set_sha256: context.frames.facts().checksum_sha256.clone(),
            source_evidence_sha256: context.source.provenance_sha256().to_string(),
            source_proof: context.source.historical_provenance().clone(),
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn plan(&self) -> &NativeJointHandoffPlan {
        self.plan
    }
    pub(crate) fn require_old_predecessor(
        &self,
        db: &crate::BicDb,
        writer: &crate::db::NativeWriterHostState,
    ) -> Result<()> {
        match &self.participant.original {
            OriginalParticipant::Old {
                writer: original, ..
            } if std::ptr::eq(original.as_ref(), writer) => self
                .participant
                .original_old()?
                .require_remote_decision_predecessor(db, writer),
            _ => Err(invalid(
                "remote OLD predecessor changed original participant/State",
            )),
        }
    }
    pub(crate) fn require_old_parts(
        &self,
        native: &crate::db::NativeJointOldCloseReceipt,
        promise: &crate::distribution_range_consensus::NativeJointOldPromiseReceipt,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
        writer: &crate::db::NativeWriterHostState,
    ) -> Result<()> {
        match &self.participant.original {
            OriginalParticipant::Old {
                writer: original, ..
            } if std::ptr::eq(original.as_ref(), writer) => self
                .participant
                .original_old()?
                .require_remote_decision_parts(native, promise, frozen),
            _ => Err(invalid(
                "remote OLD decision changed genuine original participant/writer",
            )),
        }
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.data
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        match &self.context {
            DecisionRequest::Native(c) => &c.role,
            DecisionRequest::Catalogue { role, .. } | DecisionRequest::KnownRead { role, .. } => {
                role
            }
        }
    }
    pub(crate) fn metadata(&self) -> &ActualNativeJointMetadataPoint {
        match &self.context {
            DecisionRequest::Native(c) => &c.metadata,
            DecisionRequest::Catalogue { metadata, .. }
            | DecisionRequest::KnownRead { metadata, .. } => metadata,
        }
    }
    /// Memory/request checks ONLY. Full immutable-body/physical validation is
    /// performed outside the final publication gate under lexical owners.
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        match &self.context {
            DecisionRequest::Native(c) => {
                c.caller.check()?;
                c.frames.require_request_live()?;
                c.source.require_request_live()?;
            }
            DecisionRequest::Catalogue { caller, native, .. }
            | DecisionRequest::KnownRead { caller, native, .. } => {
                caller.check()?;
                native.actual_committed_xid()?;
            }
        }
        self.role().require_live_request()?;
        let (own, business) = self.own_request();
        own.require_live_request(business)?;
        self.metadata().require_current_memory()?;
        self.participant.require_lifetime_memory()
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_memory_current()?;
        self.plan.require_role(self.role())?;
        let (own, business) = self.own_request();
        self.role().require_own_host_metadata(own, business)?;
        self.metadata().require_request(own, business)?;
        self.data.validate()
    }
    pub(crate) fn require_target(&self, target: &NativeRegisteredRemoteJointTarget) -> Result<()> {
        match &self.participant.original {
            OriginalParticipant::Registered { target: actual, .. }
                if std::ptr::eq(actual.as_ref(), target) =>
            {
                Ok(())
            }
            _ => Err(invalid("remote DECISION changed original actual target/A")),
        }
    }
    pub(crate) fn publication_guard(
        &self,
    ) -> Result<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>> {
        self.metadata().try_publication_guard()
    }
}
impl NativeRemoteNewParticipant {
    pub(crate) fn require_remote_release_proof(
        &self,
        proof: &NativeRemoteAllOpenQuorum,
    ) -> Result<()> {
        self.require_lifetime_memory()?;
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote Release original native outcome busy"))?;
        let completed = held
            .completed
            .as_ref()
            .ok_or_else(|| invalid("remote Release original known native return absent"))?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("remote Release original private native attempt absent"))?;
        if held.expected.as_ref() != Some(completed.data()) {
            return Err(invalid("remote Release original native body changed"));
        }
        held.catalogue
            .as_ref()
            .ok_or_else(|| invalid("remote Release original decision catalogue absent"))?
            .require_original_successful_publication()?;
        proof.require_original_native(self, completed, native)
    }
    pub(crate) fn require_remote_policy_proof(
        &self,
        proof: &NativeRemoteAllOpenQuorum,
        successor: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> Result<()> {
        self.require_released_successor_memory(successor)?;
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote Release original native outcome busy"))?;
        let completed = held
            .completed
            .as_ref()
            .ok_or_else(|| invalid("remote Release original known native return absent"))?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("remote Release original private native attempt absent"))?;
        if held.expected.as_ref() != Some(completed.data()) {
            return Err(invalid("remote Release original native body changed"));
        }
        held.catalogue
            .as_ref()
            .ok_or_else(|| invalid("remote Release original decision catalogue absent"))?
            .require_original_successful_publication()?;
        proof.require_original_native_for_policy(self, completed, native, successor)
    }
    /// Intent DATA selected only from this actual retained participant. It
    /// cannot construct an owner or authorize retirement without a newly
    /// collected opaque ALL-Open proof and fresh local metadata point.
    pub(crate) fn retained_serving_intent(
        &self,
    ) -> Result<(Arc<NativeRemoteImageManifestV1>, Uuid, String)> {
        self.require_lifetime_memory()?;
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("retirement original native decision busy"))?;
        let completed = held
            .completed
            .as_ref()
            .ok_or_else(|| invalid("retirement requires actual known native decision return"))?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("retirement original native decision attempt absent"))?;
        native.actual_committed_xid()?;
        if held.expected.as_ref() != Some(completed.data()) || held.catalogue.is_none() {
            return Err(invalid(
                "retirement original decision/catalogue custody changed",
            ));
        }
        let manifest: NativeRemoteImageManifestV1 = serde_json::from_slice(&self.manifest_bytes)?;
        manifest.require_shape()?;
        if manifest.checksum_sha256 != self.snapshot.binding.manifest_sha256 {
            return Err(invalid("retirement retained manifest identity changed"));
        }
        Ok((
            Arc::new(manifest),
            self.snapshot.phase_id,
            completed.decision_sha256().to_string(),
        ))
    }

    pub(super) fn require_all_open_known_decision(
        &self,
        phase: &NativeRemoteAllOpenRetirementAdmission<'_>,
    ) -> Result<()> {
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("ALL Open original native outcome busy"))?;
        let completed = held
            .completed
            .as_ref()
            .ok_or_else(|| invalid("ALL Open original actual native receipt absent"))?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("ALL Open original actual native attempt absent"))?;
        if held.expected.as_ref() != Some(completed.data()) {
            return Err(invalid(
                "ALL Open changed exact original retained native decision",
            ));
        }
        phase.require_native_decision(self, completed, native)
    }

    /// Necessary SAME private native/body/process binding, never permission to
    /// prepare/activate or bypass a physical/current catalogue validation.
    pub(crate) fn serving_queue_for_original(
        &self,
        request: &crate::distribution_native_host::NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
    ) -> Result<Arc<crate::distribution_native_host::NativeRemoteServingQueue>> {
        request.require_shape()?;
        self.require_lifetime_memory()?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        if role.purpose() != &request.purpose()
            || request.phase_id != self.snapshot.phase_id
            || request.manifest.checksum_sha256 != self.snapshot.binding.manifest_sha256
            || crate::distribution_range_image::bounded_json(&request.manifest, 2 * 1024 * 1024)?
                != self.manifest_bytes
            || role.local_node_id() != &self.snapshot.node_id
        {
            return Err(invalid(
                "serving continuation substituted original participant/MF/phase",
            ));
        }
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("serving original native decision busy"))?;
        let actual = held.completed.as_ref().ok_or_else(|| {
            invalid("serving continuation lacks actual known native helper return")
        })?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("serving continuation original native attempt absent"))?;
        native.actual_committed_xid()?;
        if actual.decision_sha256() != request.decision_sha256
            || held.expected.as_ref() != Some(actual.data())
            || held.catalogue.is_none()
        {
            return Err(invalid(
                "serving continuation changed original known native/catalogue custody",
            ));
        }
        actual.data().require_metadata_domain(
            own.committed_topology(business)?,
            own.restore_activation(business)?,
        )?;
        Ok(self.serving_queue.clone())
    }
    // Distinct SAME-successor binding after original references/hook retire.
    pub(crate) fn serving_queue_for_released(
        &self,
        request: &crate::distribution_native_host::NativeRemoteServingRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        successor: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> Result<Arc<crate::distribution_native_host::NativeRemoteServingQueue>> {
        request.require_shape()?;
        self.require_released_successor_memory(successor)?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        if role.purpose() != &request.purpose()
            || request.phase_id != self.snapshot.phase_id
            || request.manifest.checksum_sha256 != self.snapshot.binding.manifest_sha256
            || crate::distribution_range_image::bounded_json(&request.manifest, 2 * 1024 * 1024)?
                != self.manifest_bytes
            || role.local_node_id() != &self.snapshot.node_id
        {
            return Err(invalid(
                "serving continuation substituted original participant/MF/phase",
            ));
        }
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("serving original native decision busy"))?;
        let actual = held.completed.as_ref().ok_or_else(|| {
            invalid("serving continuation lacks actual known native helper return")
        })?;
        let native = held
            .native
            .as_ref()
            .ok_or_else(|| invalid("serving continuation original native attempt absent"))?;
        native.actual_committed_xid()?;
        if actual.decision_sha256() != request.decision_sha256
            || held.expected.as_ref() != Some(actual.data())
            || held.catalogue.is_none()
        {
            return Err(invalid(
                "serving continuation changed original known native/catalogue custody",
            ));
        }
        actual.data().require_metadata_domain(
            own.committed_topology(business)?,
            own.restore_activation(business)?,
        )?;
        Ok(self.serving_queue.clone())
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_known_remote_decision(
        &self,
        request: &NativeRemoteNewRequestV1,
        role: &RecoveryRoleAdmission,
        own: &HostMetadataReadObservation,
        business: Uuid,
        metadata: &ActualNativeJointMetadataPoint,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteDecisionReceiptV1> {
        self.require_request(request)?;
        let NativeRemoteNewOperationV1::ObserveKnownDecision { facts } = &request.operation else {
            return Err(invalid(
                "known native read requires explicit readonly operation",
            ));
        };
        self.require_current(parent)?;
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(role, own, business)?;
        request.require_plan(&plan)?;
        role.require_own_host_metadata(own, business)?;
        metadata.require_request(own, business)?;
        metadata.require_external_current()?;
        if role.local_node_id() != &self.snapshot.node_id
            || self
                .handoff_outcome()?
                .is_none_or(|h| h.fact_set_sha256 != facts.checksum_sha256)
        {
            return Err(invalid(
                "known native read changed original recipient/phase intent",
            ));
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("known native read original attempt active"))?;
        let DecisionCustody {
            expected,
            native,
            completed,
            ..
        } = &mut *held;
        let data = expected
            .as_ref()
            .ok_or_else(|| invalid("known native read original D absent"))?;
        let native = native
            .as_ref()
            .ok_or_else(|| invalid("known native read original native attempt absent"))?;
        // Private outcome is set ONLY after real paged.commit returned Ok.
        // Native Unknown/abort/unframed cannot enter this helper, regardless
        // of visible bytes, native tx status or an old wire response.
        native.actual_committed_xid()?;
        let admission = NativeRemoteDecisionAdmission {
            participant: self,
            data,
            plan: &plan,
            context: DecisionRequest::KnownRead {
                role,
                own,
                business,
                metadata,
                caller: parent,
                native,
            },
        };
        admission.require_current()?;
        let receipt=match &self.original {
            OriginalParticipant::Registered {target,service,..}=>crate::distribution_data::ClusterDataNodeService::recover_known_remote_registered_decision(
                service,target,self.original_registered_freeze()?.try_as_frozen()?,&admission,parent)?,
            OriginalParticipant::Old {writer,..}=>self.original_old()?.recover_known_remote_decision(writer,&admission,parent)?,
        };
        // NEW actual successful fresh helper return, retained before outer
        // checks. Original failed call/outcome/XID are never rewritten.
        *completed = Some(receipt);
        admission.require_current()?;
        metadata.require_external_current()?;
        let actual = completed.as_ref().unwrap();
        let reply = NativeRemoteDecisionReceiptV1 {
            binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
            phase_id: self.snapshot.phase_id,
            fact_set_sha256: facts.checksum_sha256.clone(),
            node_id: self.snapshot.node_id.clone(),
            decision_sha256: actual.decision_sha256().into(),
            native_record_sha256: actual.checksum_sha256().into(),
            native_page_xid: native.actual_committed_xid()?,
            remote_provenance_sha256: native.actual_provenance_sha256()?.into(),
        };
        reply.require_request(request, facts)?;
        parent.check()?;
        Ok(reply)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_remote_decision(
        &self,
        request: &NativeRemoteNewRequestV1,
        fetched: AuthenticatedNativeRemoteNewFactSet,
        source: AuthenticatedNativeRemoteDecisionSource,
        role: Arc<RecoveryRoleAdmission>,
        own: Arc<HostMetadataReadObservation>,
        business: Uuid,
        metadata: Arc<ActualNativeJointMetadataPoint>,
        parent: &CancellationToken,
    ) -> Result<NativeRemoteDecisionReceiptV1> {
        self.require_request(request)?;
        let NativeRemoteNewOperationV1::PublishDecision { facts } = &request.operation else {
            return Err(invalid(
                "remote DECISION needs its explicit production operation",
            ));
        };
        let original_registered_owner = match &self.original {
            OriginalParticipant::Registered { owner, .. } => Some(
                owner
                    .upgrade()
                    .ok_or_else(|| invalid("remote DECISION original Registered owner ended"))?,
            ),
            OriginalParticipant::Old { .. } => None,
        };
        let handoff = self
            .handoff_outcome()?
            .ok_or_else(|| invalid("remote DECISION original pin handoff incomplete"))?;
        if source.actual_observer() != &self.snapshot.node_id
            || source.actual_observer_birth() != &self.snapshot.birth
            || fetched.actual_observer() != &self.snapshot.node_id
            || fetched.facts() != facts
            || handoff.fact_set_sha256 != facts.checksum_sha256
            || role.local_node_id() != &self.snapshot.node_id
        {
            return Err(invalid(
                "remote DECISION changed actual observer/original pin intent",
            ));
        }
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(&role, &own, business)?;
        request.require_plan(&plan)?;
        role.require_own_host_metadata(&own, business)?;
        metadata.require_request(&own, business)?;
        metadata.require_external_current()?;
        self.require_current(parent)?;
        let provenance = source.data(&request.manifest)?;
        let new = fetched.decision_materialized_facts()?;
        let data = NativeJointDecisionData::from_remote_historical_data(
            &plan,
            &request.manifest,
            provenance,
            new,
        )?;
        // Fresh phase authority cannot reuse an issuance context or regress
        // behind any counted original physical observation.
        for fact in request
            .manifest
            .actual_old_closures
            .values()
            .chain(data.remote_new_facts().values())
        {
            if role.context_id() == fact.metadata_context_id
                || role.metadata_term() < fact.metadata_term
                || role.metadata_read_index() < fact.metadata_read_index
            {
                return Err(invalid(
                    "remote DECISION phase authority not independently fresh",
                ));
            }
        }
        let additional = fetched
            .retained_encoded_bytes()?
            .checked_add(source.retained_encoded_bytes()?)
            .ok_or_else(|| invalid("remote DECISION retained byte count overflow"))?;
        let mut retained = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION original attempt active"))?;
        if retained.expected.as_ref().is_some_and(|old| old != &data) {
            return Err(invalid(
                "remote DECISION cannot replace its original native body",
            ));
        }
        if retained.contexts.len() >= 8
            || retained
                .encoded_bytes
                .checked_add(additional)
                .is_none_or(|n| n > 8 * 1024 * 1024)
        {
            return Err(invalid(
                "remote DECISION custody exhausted (8 contexts/8MiB); no eviction or new attempt",
            ));
        }
        let context = Arc::new(DecisionContext {
            frames: fetched,
            source,
            role,
            own,
            business,
            metadata,
            caller: parent.child(),
        });
        retained.expected.get_or_insert_with(|| data.clone());
        retained.contexts.push(context.clone());
        retained.encoded_bytes += additional;
        if retained.native.is_none() {
            retained.native =
                Some(crate::db::NativeRemoteJointDecisionAttempt::retain_expected(&data)?);
        }
        #[cfg(test)]
        let _test_cut = {
            let code = std::mem::take(&mut retained.cut);
            let caller = context.caller.clone();
            let fired = Arc::new(AtomicBool::new(false));
            if code != 0 {
                retained.observed_cut = Some((code, fired.clone()));
            }
            if code == 3 {
                retained
                    .native
                    .as_mut()
                    .unwrap()
                    .test_arm_commit_sync_failure();
            }
            crate::db::test_remote_joint_decision_cut(
                code == 2,
                Box::new(move || {
                    if code != 0 {
                        fired.store(true, Ordering::Release);
                    }
                    if code == 1 || code == 2 {
                        caller.cancel();
                    }
                }),
            )
        };
        // Every original owner, exact body, callback and request is retained
        // BEFORE native begin, first put, postcheck or reply can fail.
        let admission = NativeRemoteDecisionAdmission {
            participant: self,
            context: DecisionRequest::Native(&context),
            data: &data,
            plan: &plan,
        };
        admission.require_current()?;
        if let Some(owner) = &original_registered_owner {
            owner.require_first_policy_attempt_lifetime()?;
        }
        let receipt=match &self.original {
            OriginalParticipant::Registered {target,service,..}=>crate::distribution_data::ClusterDataNodeService::publish_remote_registered_decision(
                service,target,self.original_registered_freeze()?.try_as_frozen()?,&admission,retained.native.as_mut().unwrap(),parent)?,
            OriginalParticipant::Old {writer,..}=>self.original_old()?.publish_remote_decision(writer,&admission,retained.native.as_mut().unwrap(),parent)?,
        };
        // The actual sink return is retained before all outer fallible checks.
        // It never replaces the native attempt/outcome or its original XID.
        retained.completed = Some(receipt);
        admission.require_current()?;
        context.metadata.require_external_current()?;
        parent.check()?;
        let receipt = retained.completed.as_ref().unwrap();
        let reply = NativeRemoteDecisionReceiptV1 {
            binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
            phase_id: request.phase_id,
            fact_set_sha256: facts.checksum_sha256.clone(),
            node_id: self.snapshot.node_id.clone(),
            decision_sha256: receipt.decision_sha256().to_string(),
            native_record_sha256: receipt.checksum_sha256().to_string(),
            native_page_xid: retained.native.as_ref().unwrap().actual_committed_xid()?,
            remote_provenance_sha256: retained
                .native
                .as_ref()
                .unwrap()
                .actual_provenance_sha256()?
                .to_string(),
        };
        reply.require_request(request, facts)?;
        Ok(reply)
    }
}

impl NativeRemoteNewParticipant {
    /// READ-ONLY: the original participant re-proves that the native DECISION
    /// it already durably committed still exists with its exact bytes/xid and
    /// protected provenance, then returns the same receipt DATA plus the exact
    /// body. No Role, pin, write, vote or replacement attempt is created here.
    pub(crate) fn observe_remote_decision(
        &self,
        request: &NativeRemoteNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<(NativeRemoteDecisionReceiptV1, NativeJointDecisionData)> {
        self.require_request(request)?;
        if !matches!(
            &request.operation,
            NativeRemoteNewOperationV1::ObserveDecision
        ) {
            return Err(invalid(
                "durable DECISION observation needs its explicit read-only operation",
            ));
        }
        let handoff = self
            .handoff_outcome()?
            .ok_or_else(|| invalid("observed DECISION original pin handoff incomplete"))?;
        self.require_lifetime_memory()?;
        parent.check()?;
        let retained = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION original attempt active"))?;
        let expected = retained
            .expected
            .as_ref()
            .ok_or_else(|| invalid("no original remote DECISION body was retained"))?;
        let attempt = retained
            .native
            .as_ref()
            .ok_or_else(|| invalid("no original remote native DECISION attempt exists"))?;
        let receipt = match &self.original {
            OriginalParticipant::Registered {
                target, service, ..
            } => {
                let service = service
                    .try_lock()
                    .ok_or_else(|| invalid("remote DECISION observation target service busy"))?;
                let database = service.database();
                let db = database
                    .try_read()
                    .ok_or_else(|| invalid("remote DECISION observation target DB busy"))?;
                db.observe_remote_native_decision(target.native(), attempt)?
            }
            OriginalParticipant::Old { .. } => {
                self.original_old()?.observe_remote_decision(attempt)?
            }
        };
        if receipt.data() != expected
            || receipt.decision_sha256() != expected.checksum_sha256()
            || receipt.local_node_id() != &self.snapshot.node_id
        {
            return Err(invalid(
                "observed durable DECISION differs from original body/recipient",
            ));
        }
        let reply = NativeRemoteDecisionReceiptV1 {
            binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
            phase_id: request.phase_id,
            fact_set_sha256: handoff.fact_set_sha256.clone(),
            node_id: self.snapshot.node_id.clone(),
            decision_sha256: receipt.decision_sha256().to_string(),
            native_record_sha256: receipt.checksum_sha256().to_string(),
            native_page_xid: attempt.actual_committed_xid()?,
            remote_provenance_sha256: attempt.actual_provenance_sha256()?.to_string(),
        };
        reply.require_observation(request, &handoff.fact_set_sha256)?;
        self.require_lifetime_memory()?;
        parent.check()?;
        Ok((reply, expected.clone()))
    }
}

#[cfg(test)]
impl NativeRemoteNewParticipant {
    pub(crate) fn test_arm_remote_decision_cut(&self, code: u8) -> Result<()> {
        if !(1..=10).contains(&code) {
            return Err(invalid("unknown remote DECISION fault code"));
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION diagnostic custody busy"))?;
        if code == 10 {
            if held.cancel_catalogue_before_io || held.catalogue.is_some() {
                return Err(invalid(
                    "actual catalogue pre-IO cancellation needs original unstarted candidate",
                ));
            }
            held.cancel_catalogue_before_io = true;
            return Ok(());
        }
        if code == 8 {
            return held
                .catalogue
                .as_mut()
                .ok_or_else(|| invalid("actual pending catalogue candidate absent"))?
                .test_arm_foreign_disk();
        }
        if code == 9 {
            if held.fail_catalogue_rename || held.catalogue_rename_fault_count != 0 {
                return Err(invalid("actual catalogue rename fault already armed/fired"));
            }
            held.fail_catalogue_rename = true;
            return Ok(());
        }
        if code == 7 {
            return held
                .catalogue
                .as_mut()
                .ok_or_else(|| invalid("actual pending catalogue candidate absent"))?
                .test_arm_intervening_write();
        }
        if code == 5 {
            if held.lose_catalogue_reply || held.lost_catalogue_reply.is_some() {
                return Err(invalid(
                    "actual catalogue success reply loss already armed/fired",
                ));
            }
            held.lose_catalogue_reply = true;
            return Ok(());
        }
        if code == 6 {
            if held.fail_catalogue_dirsync || held.catalogue_dirsync_fault_count != 0 {
                return Err(invalid(
                    "actual catalogue dirsync fault already armed/fired",
                ));
            }
            held.fail_catalogue_dirsync = true;
            return Ok(());
        }
        if code == 4 {
            if held.cut != 0 || held.lose_success_reply || held.lost_success_reply.is_some() {
                return Err(invalid(
                    "actual native DECISION reply loss already armed/fired",
                ));
            }
            held.lose_success_reply = true;
            return Ok(());
        }
        if held.cut != 0 {
            return Err(invalid("remote DECISION cut already armed"));
        }
        held.cut = code;
        Ok(())
    }
    pub(crate) fn test_after_success_reply(&self, reply: &NativeRemoteNewReplyV1) -> Result<()> {
        if matches!(
            reply.request.operation,
            NativeRemoteNewOperationV1::PublishCatalogue { .. }
        ) {
            let mut held = self
                .decision
                .try_lock()
                .ok_or_else(|| invalid("actual catalogue reply-loss observation busy"))?;
            if held.lose_catalogue_reply {
                let actual = reply.catalogue_decision.clone().ok_or_else(|| {
                    invalid("catalogue loss requires actual successful returned receipt")
                })?;
                if held.catalogue.is_none() {
                    return Err(invalid(
                        "catalogue loss requires retained original physical candidate",
                    ));
                }
                held.lose_catalogue_reply = false;
                held.lost_catalogue_reply = Some(actual);
                return Err(invalid(
                    "injected loss of actual successful catalogue wire reply",
                ));
            }
            return Ok(());
        }
        if !matches!(
            reply.request.operation,
            NativeRemoteNewOperationV1::PublishDecision { .. }
        ) {
            return Ok(());
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("actual decision reply-loss observation busy"))?;
        if held.lose_success_reply {
            if held.completed.is_none() {
                return Err(invalid(
                    "reply-loss hook requires actual successful native sink",
                ));
            }
            let receipt = reply
                .native_decision
                .clone()
                .ok_or_else(|| invalid("reply-loss hook needs actual returned native receipt"))?;
            held.lose_success_reply = false;
            held.lost_success_reply = Some(receipt);
            return Err(invalid(
                "injected loss of actual successful native DECISION wire reply",
            ));
        }
        Ok(())
    }
    pub(crate) fn test_remote_decision_evidence(&self) -> Result<serde_json::Value> {
        let predecessor = match &self.original {
            OriginalParticipant::Old { .. } => {
                Some(self.original_old()?.test_remote_decision_predecessor()?)
            }
            _ => None,
        };
        let held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION diagnostic custody busy"))?;
        Ok(
            serde_json::json!({"expected":held.expected,"retained_request_contexts":held.contexts.len(),
            "successful_sink_return_retained":held.completed.is_some(),"actual_success_reply_lost":held.lost_success_reply,
            "catalogue":held.catalogue.as_ref().map(|c|c.test_evidence()),
            "actual_catalogue_success_reply_lost":held.lost_catalogue_reply,
            "actual_catalogue_dirsync_fault_count":held.catalogue_dirsync_fault_count,"actual_catalogue_rename_fault_count":held.catalogue_rename_fault_count,
            "actual_test_cut":held.observed_cut.as_ref().map(|(code,fired)|serde_json::json!({"code":code,"fired":fired.load(Ordering::Acquire)})),
            "retained_encoded_bytes":held.encoded_bytes,"native":held.native.as_ref().map(|n|n.test_evidence()),
            "source_callback_actual_observers":held.contexts.iter().map(|c|serde_json::json!({"node":c.source.actual_observer(),"birth":c.source.actual_observer_birth()})).collect::<Vec<_>>(),
            "original_phase":self.snapshot.phase_id,"original_MF_binding":self.snapshot.binding,
            "retained_pre_freeze_predecessor":predecessor,
            "no_catalogue_vote_or_serving":held.catalogue.is_none(),"no_Serving_grant":true}),
        )
    }
}
