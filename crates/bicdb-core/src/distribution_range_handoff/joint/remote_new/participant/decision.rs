//! Genuine target-owned remote DECISION. No local quorum capability is made.
use super::*;
use crate::distribution_range_handoff::{NativeJointDecisionData, NativeRemoteDecisionReceiptV1};
use crate::distribution_transport::AuthenticatedNativeRemoteDecisionSource;

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
    /// Actual full sink return retained BEFORE outer metadata/reply checks.
    /// Absence stays unknown even when bytes later happen to be visible.
    completed: Option<crate::db::NativeJointDecisionReceipt>,
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
}
/// Constructor is private to the original participant after genuine callbacks.
/// This does NOT implement or convert to a local publication admission.
pub(crate) struct NativeRemoteDecisionAdmission<'a> {
    participant: &'a NativeRemoteNewParticipant,
    context: &'a DecisionContext,
    data: &'a NativeJointDecisionData,
    plan: &'a NativeJointHandoffPlan,
}
impl NativeRemoteDecisionAdmission<'_> {
    pub(crate) fn durable_binding(
        &self,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteDecisionBindingV1> {
        let value = crate::distribution_range_handoff::NativeRemoteDecisionBindingV1 {
            binding: self.participant.snapshot.binding.clone(),
            phase_id: self.participant.snapshot.phase_id,
            participant: self.participant.snapshot.clone(),
            fact_set_sha256: self.context.frames.facts().checksum_sha256.clone(),
            source_evidence_sha256: self.context.source.provenance_sha256().to_string(),
            source_proof: self.context.source.historical_provenance().clone(),
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
                actual,
                writer: original,
                ..
            } if std::ptr::eq(original.as_ref(), writer) => {
                actual.require_remote_decision_predecessor(db, writer)
            }
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
                actual,
                writer: original,
                ..
            } if std::ptr::eq(original.as_ref(), writer) => {
                actual.require_remote_decision_parts(native, promise, frozen)
            }
            _ => Err(invalid(
                "remote OLD decision changed genuine original participant/writer",
            )),
        }
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.data
    }
    pub(crate) fn role(&self) -> &RecoveryRoleAdmission {
        &self.context.role
    }
    pub(crate) fn metadata(&self) -> &ActualNativeJointMetadataPoint {
        &self.context.metadata
    }
    pub(crate) fn require_memory_current(&self) -> Result<()> {
        self.context.caller.check()?;
        self.context.role.require_live_request()?;
        self.context
            .own
            .require_live_request(self.context.business)?;
        self.context.frames.require_request_live()?;
        self.context.source.require_request_live()?;
        self.context.metadata.require_current_memory()?;
        self.participant.require_lifetime_memory()
    }
    pub(crate) fn require_current(&self) -> Result<()> {
        self.require_memory_current()?;
        self.plan.require_role(&self.context.role)?;
        self.context
            .role
            .require_own_host_metadata(&self.context.own, self.context.business)?;
        self.context
            .metadata
            .require_request(&self.context.own, self.context.business)?;
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
        self.context.metadata.try_publication_guard()
    }
}
impl NativeRemoteNewParticipant {
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
        let NativeRemoteNewOperationV1::ObserveDecision { facts } = &request.operation else {
            return Err(invalid(
                "remote DECISION observation needs explicit readonly operation",
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
                "remote DECISION observation changed original participant/intent",
            ));
        }
        let retained = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION original attempt active"))?;
        let completed=retained.completed.as_ref().ok_or_else(||invalid("remote DECISION original successful sink return absent; unknown remains unresolved"))?;
        let expected = retained
            .expected
            .as_ref()
            .ok_or_else(|| invalid("remote DECISION original body absent"))?;
        let native = retained
            .native
            .as_ref()
            .ok_or_else(|| invalid("remote DECISION original native attempt absent"))?;
        if completed.data() != expected
            || expected.remote_new_facts().len() != facts.members.len()
            || role.metadata_term() < completed.metadata_anchor().0
            || role.metadata_read_index() < completed.metadata_anchor().1
        {
            return Err(invalid(
                "remote DECISION observation changed original body/current metadata floor",
            ));
        }
        let service = match &self.original {
            OriginalParticipant::Old { actual, .. } => actual.actual_service(),
            OriginalParticipant::Registered { service, .. } => service,
        };
        crate::distribution_data::ClusterDataNodeService::observe_known_remote_decision(
            service, completed, native, metadata, parent,
        )?;
        let reply = NativeRemoteDecisionReceiptV1 {
            binding: NativeSourceExportBindingV1::from_manifest(&request.manifest)?,
            phase_id: self.snapshot.phase_id,
            fact_set_sha256: facts.checksum_sha256.clone(),
            node_id: self.snapshot.node_id.clone(),
            decision_sha256: completed.decision_sha256().into(),
            native_record_sha256: completed.checksum_sha256().into(),
            native_page_xid: native.actual_committed_xid()?,
            remote_provenance_sha256: native.actual_provenance_sha256()?.into(),
        };
        drop(retained);
        self.require_current(parent)?;
        metadata.require_external_current()?;
        role.require_own_host_metadata(own, business)?;
        parent.check()?;
        reply.require_request(request, facts)?;
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
            context: &context,
            data: &data,
            plan: &plan,
        };
        admission.require_current()?;
        if let Some(owner) = &original_registered_owner {
            owner.require_first_policy_attempt_lifetime()?;
        }
        let receipt=match &self.original {
            OriginalParticipant::Registered {target,service,frozen,..}=>crate::distribution_data::ClusterDataNodeService::publish_remote_registered_decision(
                service,target,frozen.try_as_frozen()?,&admission,retained.native.as_mut().unwrap(),parent)?,
            OriginalParticipant::Old {actual,writer,..}=>actual.publish_remote_decision(writer,&admission,retained.native.as_mut().unwrap(),parent)?,
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

#[cfg(test)]
impl NativeRemoteNewParticipant {
    pub(crate) fn test_arm_remote_decision_cut(&self, code: u8) -> Result<()> {
        if !(1..=4).contains(&code) {
            return Err(invalid("unknown remote DECISION fault code"));
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION diagnostic custody busy"))?;
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
            OriginalParticipant::Old { actual, .. } => {
                Some(actual.test_remote_decision_predecessor()?)
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
            "actual_test_cut":held.observed_cut.as_ref().map(|(code,fired)|serde_json::json!({"code":code,"fired":fired.load(Ordering::Acquire)})),
            "retained_encoded_bytes":held.encoded_bytes,"native":held.native.as_ref().map(|n|n.test_evidence()),
            "source_callback_actual_observers":held.contexts.iter().map(|c|serde_json::json!({"node":c.source.actual_observer(),"birth":c.source.actual_observer_birth()})).collect::<Vec<_>>(),
            "original_phase":self.snapshot.phase_id,"original_MF_binding":self.snapshot.binding,
            "retained_pre_freeze_predecessor":predecessor,
            "no_catalogue_vote_or_serving":true}),
        )
    }
}
