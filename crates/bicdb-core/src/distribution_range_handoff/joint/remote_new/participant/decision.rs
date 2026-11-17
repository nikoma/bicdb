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
    // The exact request of this context and what its retention costs.
    challenge: Uuid,
    encoded_bytes: usize,
    // Set when this context fixed the expected body or the native attempt
    // began under it. A context that did neither holds no obligation.
    effect: AtomicBool,
}
/// Publication contexts kept at once. Only a context that fixed the body or
/// began the single native attempt stays; see `release_spent_contexts`.
const MAX_DECISION_CONTEXTS: usize = 8;
#[derive(Default)]
pub(super) struct DecisionCustody {
    expected: Option<NativeJointDecisionData>,
    contexts: Vec<Arc<DecisionContext>>,
    encoded_bytes: usize,
    native: Option<crate::db::NativeRemoteJointDecisionAttempt>,
    #[cfg(test)]
    cut: u8,
    #[cfg(test)]
    observed_cut: Option<(u8, Arc<AtomicBool>)>,
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
        // A request that is already dead is refused before anything is
        // retained for it: it is not started rather than started and lost.
        role.require_live_request()?;
        own.require_live_request(business)?;
        fetched.require_request_live()?;
        source.require_request_live()?;
        metadata.require_current_memory()?;
        parent.check()?;
        self.release_spent_contexts(&mut retained)?;
        if retained.contexts.len() >= MAX_DECISION_CONTEXTS
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
            challenge: request.challenge,
            encoded_bytes: additional,
            effect: AtomicBool::new(retained.expected.is_none()),
        });
        self.hold_request(request.challenge, true)?;
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
        let begun = retained.native.as_ref().unwrap().native_begun();
        let published = match &self.original {
            OriginalParticipant::Registered {
                target,
                service,
                frozen,
                ..
            } => frozen.try_as_frozen().and_then(|frozen| {
                crate::distribution_data::ClusterDataNodeService::publish_remote_registered_decision(
                    service,
                    target,
                    frozen,
                    &admission,
                    retained.native.as_mut().unwrap(),
                    parent,
                )
            }),
            OriginalParticipant::Old { actual, writer, .. } => actual.publish_remote_decision(
                writer,
                &admission,
                retained.native.as_mut().unwrap(),
                parent,
            ),
        };
        // Recorded on every exit: the context under which the single native
        // attempt began is the one its outcome rests on.
        if !begun && retained.native.as_ref().unwrap().native_begun() {
            context.effect.store(true, Ordering::Release);
        }
        let receipt = published?;
        admission.require_current()?;
        context.metadata.require_external_current()?;
        parent.check()?;
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
    /// A context that neither fixed the expected body nor saw the native
    /// attempt begin authorized nothing: the durable outcome does not rest
    /// on its Role, callbacks or request. It is released when the next
    /// publication enters, so a publication that was refused before the
    /// native begin (a busy service, an expired request) does not use up the
    /// participant. A context that had either effect is never released.
    fn release_spent_contexts(&self, retained: &mut DecisionCustody) -> Result<()> {
        let mut released = Vec::new();
        retained.contexts.retain(|context| {
            let keep = context.effect.load(Ordering::Acquire);
            if !keep {
                released.push((context.challenge, context.encoded_bytes));
            }
            keep
        });
        for (challenge, bytes) in released {
            retained.encoded_bytes = retained.encoded_bytes.saturating_sub(bytes);
            self.hold_request(challenge, false)?;
        }
        Ok(())
    }
    /// A repeated publication of the decision this participant ALREADY
    /// committed for exactly this fact set (by a request whose caller may
    /// have stopped waiting). The answer is the read-only re-proof of the
    /// durable record, the same one a decision observation returns. It takes
    /// no context, acquires no pin and writes nothing. `None` when no
    /// committed decision exists (the ordinary path decides).
    pub(crate) fn repeated_publish_receipt(
        &self,
        request: &NativeRemoteNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<Option<NativeRemoteDecisionReceiptV1>> {
        self.require_request(request)?;
        let NativeRemoteNewOperationV1::PublishDecision { facts } = &request.operation else {
            return Err(invalid(
                "remote DECISION needs its explicit production operation",
            ));
        };
        {
            let Some(retained) = self.decision.try_lock() else {
                return Ok(None);
            };
            if retained
                .native
                .as_ref()
                .is_none_or(|native| native.actual_committed_xid().is_err())
            {
                return Ok(None);
            }
        }
        let (handoff, receipt, _) = self.committed_decision(request, parent)?;
        if handoff.fact_set_sha256 != facts.checksum_sha256 {
            return Err(invalid(
                "remote DECISION changed actual observer/original pin intent",
            ));
        }
        receipt.require_request(request, facts)?;
        Ok(Some(receipt))
    }
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
        let (handoff, reply, expected) = self.committed_decision(request, parent)?;
        reply.require_observation(request, &handoff.fact_set_sha256)?;
        Ok((reply, expected))
    }
    /// The read-only re-proof itself, shared by the decision observation and
    /// by a repeated publication. The caller binds the receipt to its request.
    fn committed_decision(
        &self,
        request: &NativeRemoteNewRequestV1,
        parent: &CancellationToken,
    ) -> Result<(
        NativeRemoteNewHandoffV1,
        NativeRemoteDecisionReceiptV1,
        NativeJointDecisionData,
    )> {
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
            OriginalParticipant::Old { actual, .. } => actual.observe_remote_decision(attempt)?,
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
        self.require_lifetime_memory()?;
        parent.check()?;
        let expected = expected.clone();
        drop(retained);
        Ok((handoff, reply, expected))
    }
}

#[cfg(test)]
impl NativeRemoteNewParticipant {
    pub(crate) fn test_arm_remote_decision_cut(&self, code: u8) -> Result<()> {
        if !(1..=3).contains(&code) {
            return Err(invalid("unknown remote DECISION fault code"));
        }
        let mut held = self
            .decision
            .try_lock()
            .ok_or_else(|| invalid("remote DECISION diagnostic custody busy"))?;
        if held.cut != 0 {
            return Err(invalid("remote DECISION cut already armed"));
        }
        held.cut = code;
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
            "actual_test_cut":held.observed_cut.as_ref().map(|(code,fired)|serde_json::json!({"code":code,"fired":fired.load(Ordering::Acquire)})),
            "retained_encoded_bytes":held.encoded_bytes,"native":held.native.as_ref().map(|n|n.test_evidence()),
            "source_callback_actual_observers":held.contexts.iter().map(|c|serde_json::json!({"node":c.source.actual_observer(),"birth":c.source.actual_observer_birth()})).collect::<Vec<_>>(),
            "original_phase":self.snapshot.phase_id,"original_MF_binding":self.snapshot.binding,
            "retained_pre_freeze_predecessor":predecessor,
            "no_catalogue_vote_or_serving":true}),
        )
    }
}
