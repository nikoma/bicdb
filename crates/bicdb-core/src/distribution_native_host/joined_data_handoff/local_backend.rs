//! The in-process backend of the joined data handoff: every step that locks a
//! member's service or runtime, and the custody of the live joint owners those
//! steps issue. The stage logic in the parent module decides when a step runs;
//! nothing here derives a stage, commits metadata or decides an abort.
//!
//! The statement order inside these bodies is load-bearing: owners, locks and
//! phases are released by drop order.
use super::*;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, NativeJointCatalogueActivationReceipt,
    RecoveryFenceScope,
};
use crate::distribution_range_handoff::{
    NativeJointDecisionPublicationRound, NativeJointDurableDecision, NativeJointHandoffPlan,
    NativeJointNewObservation, NativeJointNewRound, NativeJointOldClosedQuorum,
    NativeJointOldRound, NativeJointPreparationCustody, NativeJointPreparationPhaseQuorum,
    NativeJointPreparedServingQuorum, NativeJointPreparedServingRound,
    NativeJointPublishedServingQuorum, NativeJointReleaseContinuation,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze, NativeRangeImageInstaller,
};
use crate::distribution_range_recovery::RecoveryEpochFenceRequest;
use crate::RangeSnapshotOptions;
use std::collections::BTreeMap;

/// The member handles of one call, resolved from the committed configuration.
pub(crate) struct Members {
    actor: MetadataReadParticipant,
    metadata_leader: ClusterNodeId,
    by_node: BTreeMap<ClusterNodeId, NativeJoinedDataMember>,
}
impl Members {
    fn get(&self, node: &ClusterNodeId) -> Result<&NativeJoinedDataMember> {
        self.by_node
            .get(node)
            .ok_or_else(|| handoff_error(format!("member {node} is not a participant")))
    }
    /// One fresh committed-handoff role of `recipient`, witnessed by the
    /// recipient's own metadata provider.
    fn role(
        &self,
        plan: &NativeJointHandoffPlan,
        recipient: &ClusterNodeId,
        parent: &CancellationToken,
    ) -> Result<RecoveryRoleAdmission> {
        let promise = plan.promise();
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: promise.relocation_id,
            plan_id: promise.plan_id.clone(),
            session_id: promise.session_id,
            nonce: promise.nonce,
        };
        let token = bounded(parent, REQUEST);
        let observed = self.get(recipient)?.provider.witness_recovery_role(
            &self.actor,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            purpose.clone(),
            &token,
        )?;
        RecoveryRoleAdmission::from_witness(
            observed,
            &self.actor,
            recipient,
            business,
            plan.old_range().id,
            plan.old_range().epoch,
            promise.nonce,
            challenge,
            &purpose,
        )
    }
}

/// Member custody of one handoff onto `target`, as described in the parent
/// module documentation. It is created empty and filled by the stages.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TestHandoffCustodyCut {
    AfterTargetNew,
    BeforePreparationOwn,
    AfterFirstNativePreparation,
}

pub(crate) struct LocalJoinedHandoffBackend {
    target: ClusterNodeId,
    /// Lifetime of the retained joint owners; never a request budget.
    session: CancellationToken,
    /// Freeze owners of the old members; survivors keep theirs until release.
    runtimes: BTreeMap<ClusterNodeId, Arc<NativeJointHandoffRuntime>>,
    /// A source round was started: a permanent promise may exist.
    cut_attempted: bool,
    target_fenced: bool,
    /// An image lease was taken from the target runtime at least once.
    target_leased: bool,
    plan: Option<Arc<NativeJointHandoffPlan>>,
    closed: Option<Arc<NativeJointOldClosedQuorum>>,
    image: Option<(
        Arc<NativeImageInstallFreeze>,
        Arc<NativeImageCatalogueFinalization>,
    )>,
    publishing: Option<(NativeJointDecisionPublicationRound, BTreeSet<ClusterNodeId>)>,
    decision: Option<NativeJointDurableDecision>,
    policies_installed: bool,
    preparation_custody: BTreeMap<ClusterNodeId, Arc<NativeJointPreparationCustody>>,
    prepared: Option<Arc<NativeJointPreparedServingQuorum>>,
    published: Option<Arc<NativeJointPublishedServingQuorum>>,
    activations: BTreeMap<ClusterNodeId, Arc<NativeJointCatalogueActivationReceipt>>,
    image_owner: Option<crate::distribution_data::NativeJointImageReleaseOwner>,
    continuation: Option<Arc<NativeJointReleaseContinuation>>,
    released: BTreeSet<ClusterNodeId>,
    /// Ranges the target recovered when it opened ingress; set on completion.
    recovered: Option<usize>,
    /// Records per copied image step.
    image_batch_records: usize,
    /// The attempt of the last image lease taken from the target runtime.
    last_attempt: Option<Uuid>,
    /// Refusals of `restart_dropped_image_transfer` observed in the states
    /// the install passes through, when a test asked for them.
    #[cfg(test)]
    restart_probes: Option<Vec<(&'static str, String)>>,
    #[cfg(test)]
    custody_cut: Option<TestHandoffCustodyCut>,
    #[cfg(test)]
    custody_trace: Vec<serde_json::Value>,
    #[cfg(test)]
    custody_trace_enabled: bool,
    #[cfg(test)]
    cut_round_starts: usize,
    #[cfg(test)]
    publication_starts: usize,
}

impl LocalJoinedHandoffBackend {
    pub(super) fn new(target: ClusterNodeId) -> Self {
        Self {
            target,
            session: CancellationToken::uncancelable(),
            runtimes: BTreeMap::new(),
            cut_attempted: false,
            target_fenced: false,
            target_leased: false,
            plan: None,
            closed: None,
            image: None,
            publishing: None,
            decision: None,
            policies_installed: false,
            preparation_custody: BTreeMap::new(),
            prepared: None,
            published: None,
            activations: BTreeMap::new(),
            image_owner: None,
            continuation: None,
            released: BTreeSet::new(),
            recovered: None,
            image_batch_records: 256,
            last_attempt: None,
            #[cfg(test)]
            restart_probes: None,
            #[cfg(test)]
            custody_cut: None,
            #[cfg(test)]
            custody_trace: Vec::new(),
            #[cfg(test)]
            custody_trace_enabled: false,
            #[cfg(test)]
            cut_round_starts: 0,
            #[cfg(test)]
            publication_starts: 0,
        }
    }
    #[cfg(test)]
    pub(super) fn test_cancel_custody_once(&mut self, cut: TestHandoffCustodyCut) {
        self.custody_cut = Some(cut);
        self.custody_trace_enabled = true;
    }
    #[cfg(test)]
    pub(super) fn test_custody_snapshot(&self) -> serde_json::Value {
        let image = self.image.as_ref().map(|(frozen, image)| {
            ClusterDataNodeService::test_joint_image_custody_identity(image, frozen)
        });
        serde_json::json!({"image":image,"decision":self.decision.as_ref().map(|d|d.test_preparation_custody()),
            "prepared_retained":self.prepared.is_some(),"publishing_retained":self.publishing.is_some(),
            "native_preparation_custody_nodes":self.preparation_custody.keys().collect::<Vec<_>>(),
            "native_preparation_custody":self.preparation_custody.iter().map(|(id,c)|(id.clone(),c.test_evidence())).collect::<BTreeMap<_,_>>(),
            "session_canceled":self.session.is_cancel_requested(),"last_attempt":self.last_attempt,
            "source_cut_rounds":self.cut_round_starts,"publication_rounds":self.publication_starts,
            "trace":self.custody_trace})
    }
    #[cfg(test)]
    fn test_custody_cut(
        &mut self,
        point: TestHandoffCustodyCut,
        caller: &CancellationToken,
        detail: serde_json::Value,
    ) {
        if self.custody_cut == Some(point) {
            self.custody_trace
                .push(serde_json::json!({"point":format!("{point:?}"),
                "detail":detail,"caller_was_live":caller.check().is_ok(),
                "session_was_live":self.session.check().is_ok()}));
            self.custody_cut = None;
            caller.cancel();
        }
    }
    /// Ask the target runtime to restart a dropped transfer in every state in
    /// which it must refuse, and record what it answered.
    #[cfg(test)]
    pub(super) fn test_probe_restart_refusals(&mut self) {
        self.restart_probes = Some(Vec::new());
    }
    #[cfg(test)]
    pub(super) fn test_restart_probes(&self) -> &[(&'static str, String)] {
        self.restart_probes.as_deref().unwrap_or(&[])
    }
    #[cfg(test)]
    fn probe_restart(
        &mut self,
        label: &'static str,
        runtime: &Arc<RangeRecoveryRuntime>,
        scope: &RecoveryFenceScope,
        source: &crate::distribution_range_image::TentativeNativeRangeImageSourceAuthorization<'_>,
        role: &NativeImageInstallRole<'_>,
        attempt: Uuid,
    ) {
        let Some(log) = self.restart_probes.as_mut() else {
            return;
        };
        let outcome = runtime.restart_dropped_image_transfer(
            scope,
            source,
            role,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &self.session,
        );
        log.push((
            label,
            match outcome {
                Ok(_) => "ACCEPTED".to_string(),
                Err(error) => error.to_string(),
            },
        ));
    }
    /// Bound the records of one copied image step (1..=256).
    pub(super) fn set_image_batch_records(&mut self, records: usize) {
        self.image_batch_records = records.clamp(1, 256);
    }
    /// True once a source round was started. Abort is refused from then on.
    pub(super) fn cut_attempted(&self) -> bool {
        self.cut_attempted
    }

    /// Ship the frozen source image and its complete history into the
    /// target's Installing state, verify it there and finalize its catalogue.
    /// After an interrupted transfer the target's recovery session still owns
    /// the dropped attempt: the same session is restarted under a new attempt
    /// and the bounded clear and copy start again.
    fn install_on_target(
        &mut self,
        hosts: &mut dyn NativeJoinedDataHandoffHosts,
        members: &Members,
        plan: &NativeJointHandoffPlan,
        mut capture: RangeImageCapture,
        parent: &CancellationToken,
    ) -> Result<(NativeImageInstallFreeze, NativeImageCatalogueFinalization)> {
        let target = self.target.clone();
        let service = members.get(&target)?.service.clone();
        let runtime = hosts.target_recovery_runtime()?;
        let session = self.session.clone();
        let handoff_role = |parent: &CancellationToken| {
            members
                .role(plan, &target, parent)
                .and_then(|role| role.into_handoff())
        };
        let initial = members.role(plan, &target, parent)?;
        let scope = RecoveryFenceScope::from_role(&initial)?;
        #[cfg(test)]
        {
            if !self.target_fenced {
                let handoff = handoff_role(parent)?;
                let source = capture.authorize_target_install()?;
                self.probe_restart(
                    "no recovery session",
                    &runtime,
                    &scope,
                    &source,
                    &NativeImageInstallRole::CommittedHandoff(&handoff),
                    Uuid::now_v7(),
                );
            }
        }
        if !self.target_fenced {
            runtime.fence_epoch(
                &members.actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &session,
            )?;
            self.target_fenced = true;
        }
        let handoff = handoff_role(parent)?;
        let image_role = NativeImageInstallRole::CommittedHandoff(&handoff);
        let attempt = Uuid::now_v7();
        let source = capture.authorize_target_install()?;
        #[cfg(test)]
        {
            if self.target_leased {
                if let Some(dropped) = self.last_attempt {
                    self.probe_restart(
                        "dropped attempt reused",
                        &runtime,
                        &scope,
                        &source,
                        &image_role,
                        dropped,
                    );
                }
                let mut foreign = scope.clone();
                foreign.recovery_nonce = Uuid::now_v7();
                self.probe_restart(
                    "foreign session",
                    &runtime,
                    &foreign,
                    &source,
                    &image_role,
                    Uuid::now_v7(),
                );
            } else {
                self.probe_restart(
                    "session never transferred",
                    &runtime,
                    &scope,
                    &source,
                    &image_role,
                    Uuid::now_v7(),
                );
            }
        }
        let lease = if self.target_leased {
            runtime.restart_dropped_image_transfer(
                &scope,
                &source,
                &image_role,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?
        } else {
            runtime.transfer_image_install(
                &scope,
                &source,
                &image_role,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?
        };
        self.target_leased = true;
        self.last_attempt = Some(attempt);
        let mut install: NativeRangeImageInstaller =
            service.lock().begin_native_range_image_install(
                &source,
                &image_role,
                lease,
                ImageInstallPurpose::RelocationLearner,
                attempt,
                &session,
            )?;
        #[cfg(test)]
        self.probe_restart(
            "lease still held",
            &runtime,
            &scope,
            &source,
            &image_role,
            Uuid::now_v7(),
        );
        drop(source);
        loop {
            let cursor = capture.history_cursor().clone();
            let step = capture.export_history_step(&cursor, 256, &session)?;
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            let stage_id = install.history_stage_id();
            let progress = service.lock().stage_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                stage_id,
                &step,
                &session,
            )?;
            if progress.cursor.complete() {
                break;
            }
        }
        loop {
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            if service
                .lock()
                .audit_native_range_image_history(
                    &mut install,
                    &NativeImageInstallRole::CommittedHandoff(&handoff),
                    &source,
                    256,
                    &session,
                )?
                .audit_complete
            {
                break;
            }
        }
        loop {
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            if service.lock().clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                &session,
            )? {
                break;
            }
        }
        let options = RangeSnapshotOptions {
            max_records_per_batch: self.image_batch_records,
            max_bytes_per_batch: 1024 * 1024,
            max_record_bytes: 1024 * 1024,
        };
        let mut copied = 0usize;
        loop {
            let cursor = capture.cursor().clone();
            let step = capture.export_step(&cursor, &options, &session)?;
            let handoff = handoff_role(parent)?;
            let source = capture.authorize_target_install()?;
            service.lock().copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                attempt,
                &step,
                &session,
            )?;
            drop(source);
            copied += 1;
            hosts.transfer_progress(copied)?;
            if step.complete {
                break;
            }
        }
        let completed = capture.complete()?;
        let handoff = handoff_role(parent)?;
        let mut verification = service.lock().begin_native_range_image_verification(
            install,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            completed,
            &session,
        )?;
        let mut complete = false;
        for _ in 0..4096 {
            parent.check()?;
            let handoff = handoff_role(parent)?;
            if service.lock().advance_native_range_image_verification(
                &mut verification,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &session,
            )? == crate::db::NativeTargetImageVerificationProgress::Complete
            {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(handoff_error("bounded target verification did not finish"));
        }
        let handoff = handoff_role(parent)?;
        let verified = service.lock().finish_native_range_image_verification(
            verification,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        )?;
        let handoff = handoff_role(parent)?;
        let adopted = service.lock().adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        )?;
        let handoff = handoff_role(parent)?;
        let finalized = service.lock().finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &session,
        );
        finalized
    }
}

impl JoinedHandoffBackend for LocalJoinedHandoffBackend {
    type Members = Members;
    type Hosts = dyn NativeJoinedDataHandoffHosts;

    fn cut_started(&self) -> bool {
        self.cut_attempted || !self.runtimes.is_empty()
    }

    fn old_epoch(
        &self,
        hosts: &Self::Hosts,
        node: &ClusterNodeId,
        range: &RangeDescriptor,
    ) -> Result<u64> {
        let progress = hosts
            .member(node)?
            .service
            .try_lock_for(Duration::from_secs(1))
            .ok_or_else(|| handoff_error("old voter service busy during abort"))?
            .range_write_progress(range.id);
        Ok(progress.current_epoch)
    }

    fn forget_aborted_allocation(&mut self) {
        self.plan = None;
        self.closed = None;
    }

    fn admit_target(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<()> {
        hosts.open_target(&bounded(parent, HOST_OPERATION))
    }

    fn members(
        &self,
        hosts: &Self::Hosts,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
    ) -> Result<Members> {
        let mut nodes = BTreeSet::new();
        nodes.insert(self.target.clone());
        if let Some(plan) = &self.plan {
            nodes.extend(plan.old_members().keys().cloned());
            nodes.extend(plan.new_members().keys().cloned());
        } else {
            nodes.extend(range.replicas.iter().map(|replica| replica.node_id.clone()));
        }
        let source = relocation
            .source
            .clone()
            .ok_or_else(|| handoff_error("committed relocation names no source"))?;
        nodes.insert(source.clone());
        let mut by_node = BTreeMap::new();
        for node in nodes {
            let member = hosts.member(&node)?;
            by_node.insert(node, member);
        }
        let actor = by_node
            .get(&source)
            .ok_or_else(|| handoff_error("source member handle absent"))?
            .client
            .local_metadata_read_participant();
        Ok(Members {
            actor,
            metadata_leader: hosts.metadata_leader()?,
            by_node,
        })
    }

    fn cut_required(&self) -> bool {
        self.decision.is_none() && self.publishing.is_none() && self.image.is_none()
    }

    /// Close the old voters, capture the selected source and install the
    /// verified image on the target. A previous round's retained closures are
    /// bound to their original issuance window and cannot enter a new round,
    /// so every call closes again: the durable promise and native close record
    /// of the same session are confirmed, never replaced.
    fn cut_and_ship(
        &mut self,
        hosts: &mut Self::Hosts,
        members: &Members,
        range: &RangeDescriptor,
        relocation: &RangeRelocation,
        parent: &CancellationToken,
    ) -> Result<()> {
        #[cfg(test)]
        {
            self.cut_round_starts += 1;
        }
        self.cut_attempted = true;
        self.closed = None;
        self.plan = None;
        let token = bounded(parent, REQUEST);
        let business = Uuid::now_v7();
        let observation = members
            .get(&range.leader)?
            .client
            .acquire_metadata_read_observation(
                &members.metadata_leader,
                business,
                range.id,
                range.epoch,
                REQUEST,
                &token,
            )
            .map_err(|error| stage_error("source round metadata", error))?;
        let mut round = NativeJointOldRound::begin(observation, business, relocation.id)
            .map_err(|error| stage_error("source round", error))?;
        let plan = round.plan();
        let old = plan.old_members().keys().cloned().collect::<Vec<_>>();
        for node in &old {
            hosts.forget_old_closure(node, range.id)?;
            if !self.runtimes.contains_key(node) {
                let runtime = NativeJointHandoffRuntime::new(members.get(node)?.service.clone())?;
                self.runtimes.insert(node.clone(), Arc::new(runtime));
            }
        }
        for node in &old {
            let operation = bounded(parent, REQUEST);
            let role = members.role(&plan, node, parent)?;
            let constraints = self.runtimes[node]
                .observe_constraints(plan.clone(), role, &operation)
                .map_err(|error| stage_error("source constraints", error))?;
            round
                .record_constraints(constraints, &members.get(node)?.service)
                .map_err(|error| stage_error("source constraints", error))?;
        }
        for node in &old {
            let operation = bounded(parent, REQUEST);
            let role = members.role(&plan, node, parent)?;
            let own_business = Uuid::now_v7();
            let own = members
                .get(node)?
                .client
                .acquire_host_metadata_observation(
                    &members.metadata_leader,
                    own_business,
                    REQUEST,
                    &operation,
                )
                .map_err(|error| stage_error("source close metadata", error))?;
            let closed = hosts
                .close_old(
                    node,
                    self.runtimes[node].clone(),
                    Arc::new(role),
                    own,
                    own_business,
                    &operation,
                )
                .map_err(|error| stage_error("source close", error))?;
            round
                .record_retained_closed_actual(closed)
                .map_err(|error| stage_error("source close", error))?;
        }
        let mut closed = round
            .finish()
            .map_err(|error| stage_error("source cut", error))?;
        let capture = closed
            .begin_capture(Duration::from_secs(2), &self.session)
            .map_err(|error| stage_error("source capture", error))?;
        self.plan = Some(plan.clone());
        self.closed = Some(Arc::new(closed));
        let image = self
            .install_on_target(hosts, members, &plan, capture, parent)
            .map_err(|error| stage_error("image transfer", error))?;
        self.image = Some((Arc::new(image.0), Arc::new(image.1)));
        Ok(())
    }

    fn decision_durable(&self) -> bool {
        self.decision.is_some()
    }

    /// Materialize the new set on the same cut and publish the complete
    /// decision on every old member and the target. Once the publication
    /// round exists it is retained, so a later call publishes only the
    /// members that are still missing.
    fn decide(&mut self, members: &Members, parent: &CancellationToken) -> Result<()> {
        if self.publishing.is_none() {
            let closed = self
                .closed
                .clone()
                .ok_or_else(|| handoff_error("closed source custody absent before decision"))?;
            let plan = closed.plan();
            let survivors = plan
                .new_members()
                .keys()
                .filter(|node| **node != self.target)
                .cloned()
                .collect::<Vec<_>>();
            // Every fallible acquisition precedes the consumption of the
            // finalized image, which exists exactly once.
            let handoff = members
                .role(&plan, &self.target, parent)
                .and_then(|role| role.into_handoff())
                .map_err(|error| stage_error("target materialization role", error))?;
            let mut round = NativeJointNewRound::begin(closed.clone())
                .map_err(|error| stage_error("new round", error))?;
            for node in &survivors {
                let role = members.role(&plan, node, parent)?;
                round
                    .record_actual_survivor(node, role)
                    .map_err(|error| stage_error("survivor materialization", error))?;
            }
            let (frozen, image) =
                self.image.as_ref().cloned().ok_or_else(|| {
                    handoff_error("finalized image custody absent before decision")
                })?;
            let service = members.get(&self.target)?.service.clone();
            let target = service
                .lock()
                .observe_native_joint_new_target_retained(
                    &closed,
                    handoff,
                    image,
                    frozen,
                    &self.session,
                )
                .map_err(|error| stage_error("target materialization", error))?;
            #[cfg(test)]
            if self.custody_cut == Some(TestHandoffCustodyCut::AfterTargetNew) {
                self.test_custody_cut(
                    TestHandoffCustodyCut::AfterTargetNew,
                    parent,
                    target.test_image_custody_identity(),
                );
            }
            round
                .record(
                    NativeJointNewObservation::from_finalized_actual_target(target, service)
                        .map_err(|error| stage_error("target materialization", error))?,
                )
                .map_err(|error| stage_error("target materialization", error))?;
            let publication = round
                .prepare_decision()
                .map_err(|error| stage_error("decision preparation", error))?
                .publication_round();
            self.publishing = Some((publication, BTreeSet::new()));
            #[cfg(test)]
            {
                self.publication_starts += 1;
            }
            // The actual publication now owns the same finalized image and
            // freeze. Drain the driver's extra references before publication.
            self.image = None;
        }
        let plan = self
            .plan
            .clone()
            .ok_or_else(|| handoff_error("plan custody absent before decision publication"))?;
        let mut publishers = plan.old_members().keys().cloned().collect::<Vec<_>>();
        publishers.push(self.target.clone());
        let session = self.session.clone();
        let (publication, done) = self.publishing.as_mut().unwrap();
        for node in publishers {
            if done.contains(&node) {
                continue;
            }
            retry_timed_out(|| {
                let role = members.role(&plan, &node, parent)?;
                let business = role.business_request_id();
                // This request cannot renew the original Role. Use the actual
                // recipient transport and keep its OWN/publication under the
                // same Role deadline, original session and drive caller.
                let token = parent
                    .child_linked_to(&session)
                    .child_with_deadline(role.deadline());
                let own = members
                    .get(&node)?
                    .client
                    .acquire_host_metadata_observation(
                        &members.metadata_leader,
                        business,
                        REQUEST,
                        &token,
                    )?;
                publication.publish_actual_owned_member(role, own, business, &token)
            })
            .map_err(|error| stage_error("decision publication", error))?;
            done.insert(node);
        }
        let (publication, done) = self.publishing.take().unwrap();
        match publication.try_finish() {
            Ok(decision) => self.decision = Some(decision),
            Err((error, publication)) => {
                self.publishing = Some((publication, done));
                return Err(stage_error("decision quorum", error));
            }
        }
        Ok(())
    }

    fn require_decision(&self) -> Result<()> {
        self.decision.as_ref().map(|_| ()).ok_or_else(|| {
            handoff_error(
                "durable decision custody absent; the membership stages need the live decision",
            )
        })
    }

    /// One staged metadata membership step, applied from the retained durable
    /// decision to the caller's planning view of the fresh committed
    /// configuration. Nothing is committed here.
    fn stage_candidate(
        &mut self,
        committed: Option<NativeJointHandoffStageV1>,
        view: &mut DistributionStore,
        fresh: &HostMetadataReadObservation,
        business: Uuid,
        actor: &ClusterNodeId,
        now: u64,
    ) -> Result<()> {
        let decision = self.decision.as_ref().ok_or_else(|| {
            handoff_error(
                "durable decision custody absent; the membership stages need the live decision",
            )
        })?;
        match committed {
            None => {
                view.certify_native_joint_handoff(decision, fresh, business, actor, now)
                    .map_err(|error| stage_error("decision certification", error))?;
            }
            Some(NativeJointHandoffStageV1::Certified) => {
                view.promote_native_joint_handoff_frozen(decision, fresh, business, actor, now)
                    .map_err(|error| stage_error("union membership", error))?;
            }
            Some(NativeJointHandoffStageV1::PromotedFrozen) => {
                view.finalize_native_joint_membership_frozen(decision, fresh, business, actor, now)
                    .map_err(|error| stage_error("final membership", error))?;
            }
            Some(_) => return Err(handoff_error("membership stage is already final")),
        }
        Ok(())
    }

    /// Policy continuation, serving preparation, bridge publication, Open and
    /// release. Every finished part is retained, so a later call continues
    /// after the last finished part.
    fn activate(
        &mut self,
        hosts: &mut Self::Hosts,
        members: &Members,
        parent: &CancellationToken,
    ) -> Result<()> {
        let plan = self
            .plan
            .clone()
            .ok_or_else(|| handoff_error("plan custody absent before activation"))?;
        let range = plan.old_range().id;
        let leader = members.metadata_leader.clone();
        let new = plan.new_members().keys().cloned().collect::<Vec<_>>();
        let survivors = new
            .iter()
            .filter(|node| **node != self.target)
            .cloned()
            .collect::<Vec<_>>();
        let mut order = vec![self.target.clone()];
        order.extend(survivors.iter().cloned());
        let session = self.session.clone();
        if !self.policies_installed {
            for node in plan.old_members().keys() {
                hosts.forget_old_closure(node, range)?;
            }
            for node in hosts.data_owners()? {
                hosts
                    .refresh_owner_policy(&node, &bounded(parent, HOST_OPERATION))
                    .map_err(|error| stage_error("owner policy continuation", error))?;
            }
            hosts
                .refresh_target_policy(&bounded(parent, HOST_OPERATION))
                .map_err(|error| stage_error("target policy continuation", error))?;
            self.policies_installed = true;
        }
        if self.prepared.is_none() && self.published.is_none() && self.continuation.is_none() {
            let decision = self.decision.as_ref().ok_or_else(|| {
                handoff_error("durable decision custody absent before serving preparation")
            })?;
            let owners = decision
                .retain_new_serving_owners()
                .map_err(|error| stage_error("serving owners", error))?;
            #[cfg(test)]
            if self.custody_cut == Some(TestHandoffCustodyCut::BeforePreparationOwn) {
                self.test_custody_cut(
                    TestHandoffCustodyCut::BeforePreparationOwn,
                    parent,
                    serde_json::json!({"actual_lease_count":owners.len()}),
                );
            }
            for (local, (service, lease)) in owners {
                if self.preparation_custody.contains_key(&local) {
                    continue;
                }
                let business = Uuid::now_v7();
                let token = bounded(parent, REQUEST);
                let fresh = members
                    .get(&local)?
                    .client
                    .acquire_host_metadata_observation(&leader, business, REQUEST, &token)
                    .map_err(|error| stage_error("serving preparation metadata", error))?;
                // Acquire the actual attached local MStore point BEFORE any
                // service/DB/native guard. A live OWN alone is insufficient at
                // protected native publication sinks.
                let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
                    &service, &fresh, business, &token,
                )
                .map_err(|error| stage_error("serving preparation metadata point", error))?;
                let actual = {
                    let actual = service.lock();
                    let committed = actual
                        .observe_committed_native_joint_decision_with_point(
                            fresh, business, range, point,
                        )
                        .map_err(|error| stage_error("committed decision", error))?;
                    actual
                        .prepare_local_native_joint_serving(committed, lease, &session)
                        .map_err(|error| stage_error("serving preparation", error))?
                };
                // Native facts/State/A/pin enter retained custody BEFORE any
                // later callback, fresh metadata request or count can fail.
                self.preparation_custody.insert(
                    local.clone(),
                    NativeJointPreparationCustody::retain(actual, service),
                );
                #[cfg(test)]
                if self.custody_cut == Some(TestHandoffCustodyCut::AfterFirstNativePreparation) {
                    let retained = self.preparation_custody[&local].test_evidence();
                    let detail = serde_json::json!({"actual_node":local,
                        "actual_preparation_checksum":retained["native_preparation_sha256"],
                        "retained_custody":retained});
                    self.test_custody_cut(
                        TestHandoffCustodyCut::AfterFirstNativePreparation,
                        parent,
                        detail,
                    );
                }
            }
            // Physical preparation never spends an earlier member's OWN
            // lifetime. Only this DISTINCT fresh collection counts anything.
            if self
                .preparation_custody
                .keys()
                .ne(plan.new_members().keys())
            {
                return Err(handoff_error(
                    "retained preparations changed complete NEW participant set",
                ));
            }
            let collection = bounded(parent, PHASE).child_linked_to(&session);
            let mut round: Option<NativeJointPreparedServingRound> = None;
            for (local, custody) in &self.preparation_custody {
                let candidate = custody
                    .fresh_candidate(&members.get(local)?.client, &leader, &collection)
                    .map_err(|error| stage_error("fresh preparation observation", error))?;
                if let Some(round) = &mut round {
                    round
                        .record_candidate(candidate)
                        .map_err(|error| stage_error("serving preparation", error))?;
                } else {
                    round = Some(
                        NativeJointPreparedServingRound::begin_candidate(candidate, &session)
                            .map_err(|error| stage_error("serving preparation", error))?,
                    );
                }
            }
            let prepared = round
                .ok_or_else(|| handoff_error("no new serving owner"))?
                .finish()
                .map_err(|error| stage_error("serving preparation quorum", error))?;
            self.prepared = Some(Arc::new(prepared));
            // The complete successor owns the SAME actual objects; these
            // extra RAM references must not survive into unique release.
            self.preparation_custody.clear();
            #[cfg(test)]
            if self.custody_trace_enabled {
                self.custody_trace.push(serde_json::json!({"point":"complete_prepared_quorum_retained",
                    "original_decision":self.decision.as_ref().map(|d|d.test_preparation_custody())}));
            }
            // Retain the complete successor BEFORE dropping decided custody.
            // Extra original-owner references must drain before joint release.
            self.decision = None;
        }
        if self.published.is_none() && self.continuation.is_none() {
            let prepared = self
                .prepared
                .clone()
                .ok_or_else(|| handoff_error("prepared serving custody absent"))?;
            let fresh_phase = || -> Result<Arc<NativeJointPreparationPhaseQuorum>> {
                let mut fresh = prepared.begin_fresh_preparation_phase(PHASE)?;
                for node in &new {
                    fresh.acquire_actual_member(&members.get(node)?.client, &leader)?;
                }
                Ok(Arc::new(fresh.finish()?))
            };
            let mut publication = prepared
                .resume_native_publication()
                .map_err(|error| stage_error("serving publication", error))?;
            for local in &new {
                retry_timed_out(|| {
                    let phase = fresh_phase()?;
                    let admission =
                        phase.acquire_local_activation(&members.get(local)?.client, &leader)?;
                    publication.publish_actual_local_member(&admission, &session)
                })
                .map_err(|error| stage_error("serving publication", error))?;
            }
            let phase = retry_timed_out(fresh_phase)
                .map_err(|error| stage_error("serving publication aggregate", error))?;
            self.published = Some(Arc::new(
                publication
                    .finish_with_fresh_phase(phase)
                    .map_err(|error| stage_error("serving publication quorum", error))?,
            ));
        }
        if self.continuation.is_none() {
            let published = self
                .published
                .clone()
                .ok_or_else(|| handoff_error("published serving custody absent"))?;
            for local in &order {
                if self.activations.contains_key(local) {
                    continue;
                }
                let image_local = *local == self.target;
                let (receipt, owner) = retry_timed_out(|| {
                    let mut round = published.begin_fresh_open_phase(PHASE)?;
                    for node in &new {
                        round.acquire_actual_member(&members.get(node)?.client, &leader)?;
                    }
                    let round = Arc::new(round.finish()?);
                    let open = round.acquire_local_open(&members.get(local)?.client, &leader)?;
                    let receipt = Arc::new(open.persist_local_catalogue_activation(&session)?);
                    let owner = if image_local {
                        Some(open.retain_image_release_owner(receipt.clone())?)
                    } else {
                        None
                    };
                    Ok((receipt, owner))
                })
                .map_err(|error| stage_error("joint Open", error))?;
                if let Some(owner) = owner {
                    self.image_owner = Some(owner);
                }
                self.activations.insert(local.clone(), receipt);
            }
            let image = self
                .image_owner
                .as_ref()
                .ok_or_else(|| handoff_error("target image release owner absent"))?;
            let mut runtimes = BTreeMap::new();
            for node in &survivors {
                let runtime = self
                    .runtimes
                    .get(node)
                    .cloned()
                    .ok_or_else(|| handoff_error("survivor freeze owner absent"))?;
                runtimes.insert(node.clone(), runtime);
            }
            let continuation = published
                .retain_release_continuation(self.activations.clone(), runtimes, image.clone())
                .map_err(|error| stage_error("release continuation", error))?;
            self.continuation = Some(Arc::new(continuation));
            // Only the retained successor now owns the release view. Drop the
            // extra original reference before the unique-owner release gate.
            self.image_owner = None;
            // The real freeze owners must drain before release.
            drop(published);
            self.published = None;
            self.prepared = None;
            self.closed = None;
        }
        let continuation = self
            .continuation
            .clone()
            .ok_or_else(|| handoff_error("release continuation custody absent"))?;
        for local in &order {
            if self.released.contains(local) {
                continue;
            }
            retry_timed_out(|| {
                let mut round = continuation.begin_fresh_release_phase(PHASE)?;
                for node in &new {
                    round.acquire_actual_member(&members.get(node)?.client, &leader)?;
                }
                Arc::new(round.finish()?)
                    .acquire_local_release(&members.get(local)?.client, &leader)?
                    .release_actual_local()
            })
            .map_err(|error| stage_error("joint release", error))?;
            self.released.insert(local.clone());
        }
        Ok(())
    }

    /// Target leadership and ingress; returns the ranges the target recovered.
    fn serve(&mut self, hosts: &mut Self::Hosts, parent: &CancellationToken) -> Result<usize> {
        hosts
            .publish_target(&bounded(parent, LEADERSHIP))
            .map_err(|error| stage_error("target leadership and ingress", error))
    }
}
