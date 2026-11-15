//! The original Registered target's own bridge steps after the decision is
//! committed. Its joint target owner, real freeze and native preparation stay
//! in this receiver; a peer supplies identity DATA and receives checksums.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_native_host::provisional_listener::registration::bridge_step::{
    accept_member_host_command, acquire_bridge_step_own, bridge_reply, observe_bridge_phase,
    open_member_bridge, publish_member_bridge, release_member_bridge, retained_prepare_step,
    retire_member_old_custody, BridgeStepOwn, NativeBridgeHostCommand, NativeBridgeMemberSteps,
};
use crate::distribution_range_handoff::{
    NativeJointHandoffStepOperationV1 as StepOperation, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepReplyV1, NativeJointHandoffStepRequestV1,
    NativeJointHandoffStepStageV1 as Stage, NativeJointPreparationCustody,
};

impl NativeRegisteredReplicaAdmission {
    /// One bounded bridge step of this original receiver. `token` is the
    /// request's own bounded cancellation.
    pub(super) fn advance_joint_bridge_step(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        token: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original Registered backend ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original Registered runtime ended"))?;
        self.require_listener(&backend.service, &runtime)?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original target factory ended"))?;
        let steps = self
            .bridge_steps
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original member custody ended"))?;
        // Authenticate and bind to OWN committed metadata before any retained
        // state is created, read or disclosed.
        let fresh =
            acquire_bridge_step_own(&backend.provider, &backend.transport, peer, request, token)?;
        let local = backend.transport.local_metadata_read_participant();
        let birth = factory.actual_birth().clone();
        if fresh.birth != birth
            || birth.birth_id() != self.birth_id
            || birth.birth_sha256() != self.birth_sha256
        {
            return Err(invalid(
                "joint bridge step committed birth differs from this original receiver",
            ));
        }
        let mut writer_audit = None;
        let (outcome, anchor) = match &request.operation {
            StepOperation::BridgePhaseObserve { kind, .. } => observe_bridge_phase(
                &steps,
                peer,
                request,
                *kind,
                fresh,
                &mut writer_audit,
                token,
            )?,
            // Both transitions drain and rebind the listener serving this
            // very request and replace this host's DATA owner: they belong
            // to the host thread. The step records the command only.
            StepOperation::BridgeWriterPolicy { .. } | StepOperation::TargetServe { .. } => {
                let commands = self
                    .host_commands
                    .upgrade()
                    .ok_or_else(|| invalid("joint bridge step original host command slot ended"))?;
                accept_member_host_command(&steps, &commands, request, fresh, token)?
            }
            StepOperation::BridgePrepare { .. } => {
                self.prepare_registered_bridge(&backend, &steps, request, fresh, token)?
            }
            StepOperation::BridgePublish { .. } => publish_member_bridge(
                &steps,
                &backend.provider,
                &backend.transport,
                request,
                fresh,
                token,
            )?,
            // The original Registered receiver also retains its release
            // owner under the same live admission that activated.
            StepOperation::BridgeOpen { .. } => open_member_bridge(
                &steps,
                &backend.provider,
                &backend.transport,
                request,
                fresh,
                None,
                token,
            )?,
            // The added member never held OLD custody of this range. What
            // it retires under the same proof is its own obsolete
            // participation custody of a remote decision round, when it
            // took part in one; otherwise nothing is removed.
            StepOperation::RetireOldCustody { .. } => retire_member_old_custody(
                &steps,
                &backend.provider,
                &backend.transport,
                request,
                fresh,
                &|proof| {
                    let owner = self
                        .joint_targets
                        .upgrade()
                        .ok_or_else(|| invalid("joint bridge step original owner table ended"))?
                        .try_lock()
                        .ok_or_else(|| invalid("joint bridge step owner table busy"))?
                        .get(&request.range_id)
                        .cloned();
                    match owner {
                        Some(owner) => {
                            owner.retire_remote_decision_custody_after_remote_joint_open(proof)
                        }
                        None => Ok(()),
                    }
                },
                token,
            )?,
            StepOperation::BridgeRelease { .. } => release_member_bridge(
                &steps,
                &backend.provider,
                &backend.transport,
                request,
                fresh,
                &|| Ok(()),
                token,
            )?,
            StepOperation::SourceRetire { .. } => {
                return Err(invalid(
                    "joint source retirement requires the retired original member",
                ))
            }
            StepOperation::TargetInstall { .. } | StepOperation::SourceExportRetire { .. } => {
                return Err(invalid("joint bridge step received an install operation"))
            }
        };
        self.require_listener(&backend.service, &runtime)?;
        bridge_reply(
            request,
            local.node_id().clone(),
            birth,
            outcome,
            anchor,
            writer_audit,
            token,
        )
    }

    /// The same receiver after its host became a DATA owner and rebound its
    /// socket. Only what cannot change this member's install or bridge state
    /// is served there: read-only phase observations and the two host
    /// commands (whose retained outcome the coordinator reads back).
    pub(crate) fn advance_joint_bridge_step_as_data_owner(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        if !matches!(
            &request.operation,
            StepOperation::BridgePhaseObserve { .. }
                | StepOperation::BridgeWriterPolicy { .. }
                | StepOperation::TargetServe { .. }
        ) {
            return Err(invalid(
                "joint handoff step refused: this original receiver is a DATA owner and serves only bridge observations and its host commands",
            ));
        }
        self.advance_joint_handoff_step(peer, request, parent)
    }

    /// The same target-local preparation the owning driver runs in-process:
    /// own metadata point, own committed decision under the retained
    /// Registered owner, native preparation, custody retained before any
    /// later check can fail. A retained preparation is returned unchanged.
    fn prepare_registered_bridge(
        &self,
        backend: &Arc<NativeMetadataControlBackend>,
        steps: &NativeBridgeMemberSteps,
        request: &NativeJointHandoffStepRequestV1,
        fresh: BridgeStepOwn,
        token: &CancellationToken,
    ) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
        let range = request.range_id;
        let anchor = fresh.anchor;
        let mut held = steps.hold()?;
        let step = retained_prepare_step(&mut held, request)?;
        if let Some(custody) = step.custody() {
            request.require_decision(custody.data())?;
            return Ok((step.prepare_outcome(Stage::Complete)?, anchor));
        }
        // A new preparation consumes what the target install retained (the
        // joint target owner and its real freeze). It therefore holds the
        // SAME install custody a receiver-driven install step and the host's
        // DATA owner transition hold, until the preparation is retained: no
        // install step advances and no distinct DATA owner appears meanwhile.
        // Nonwaiting; a refusal consumed nothing and is retryable.
        let install = self
            .remote_install
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original install custody ended"))?;
        let install_held = install.hold()?;
        // Read under that custody: the host sets it only while holding it.
        if install_held.data_owner_present() {
            return Err(invalid(
                "joint bridge step cannot prepare beside an existing DATA owner",
            ));
        }
        let owner = self
            .joint_targets
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original owner table ended"))?
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step owner table busy"))?
            .get(&range)
            .cloned()
            .ok_or_else(|| invalid("joint bridge step retains no original joint target"))?;
        if !Arc::ptr_eq(owner.service(), &backend.service) {
            return Err(invalid(
                "joint bridge step retained owner has another actual service",
            ));
        }
        // First part: this member confirms the decision into its OWN
        // catalogue, from its own committed metadata and its own protected
        // native decision record, under its live retained Registered owner.
        // An already confirmed decision writes nothing; another one refuses.
        ClusterDataNodeService::confirm_committed_native_joint_decision(
            &backend.service,
            &fresh.own,
            fresh.business,
            request.relocation_id,
            range,
            token,
        )?;
        // The real freeze the retained owner itself still holds; never one
        // rebuilt from a plan, a number or request DATA.
        let lease = NativeJointServingLease::Registered {
            owner: owner.clone(),
            frozen: owner.retained_freeze()?,
        };
        let BridgeStepOwn { own, business, .. } = fresh;
        // The actual attached local MStore point BEFORE any service guard.
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &backend.service,
            &own,
            business,
            token,
        )?;
        let session = step.session().clone();
        let actual = {
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint bridge step actual service busy"))?;
            let domain = crate::db::NativeJointWriterDomain::Registered(Arc::downgrade(&owner));
            let committed = service.observe_joint_decision_with_writer_domain_and_point(
                &domain, own, business, range, point,
            )?;
            // The decision this member itself protected and sees committed.
            request.require_decision(committed.data())?;
            service.prepare_local_registered_joint_serving(committed, lease, &session)?
        };
        // Native facts/State/A/pin enter retained custody BEFORE any later
        // check or reply can fail.
        step.retain_custody(NativeJointPreparationCustody::retain(
            actual,
            backend.service.clone(),
        ));
        drop(install_held);
        token.check()?;
        Ok((step.prepare_outcome(Stage::Complete)?, anchor))
    }
}

impl NativeMetadataControlHost {
    /// The host thread's half of the two host commands of the added member.
    /// Called from the owning process loop at a point where no request of
    /// this thread is in flight. Performs at most one pending command and
    /// retains its outcome in the slot the step receiver reads; returns
    /// whether a command ran. `process` is the process lifetime; each
    /// command runs under its own bounded budget.
    pub(in crate::distribution_native_host) fn advance_joint_bridge_host_commands(
        &mut self,
        process: &CancellationToken,
    ) -> Result<bool> {
        process.check()?;
        let Some(commands) = self
            .replica
            .as_ref()
            .map(|receiver| receiver.host_commands.clone())
        else {
            return Ok(false);
        };
        let Some(command) = commands.pending() else {
            return Ok(false);
        };
        let token = process.child_with_deadline(Instant::now() + Duration::from_secs(5));
        let result = if command.serves() {
            self.serve_registered_joint_target(&command, &token)
                .map(Some)
        } else {
            self.install_registered_joint_writer_policy(&command, &token)
                .map(|()| None)
        }
        .map_err(|error| error.to_string());
        // Retained BEFORE anything else can fail.
        commands.finish(&command, result.clone());
        result.map(|_| true).map_err(|text| invalid(&text))
    }

    /// `BridgeWriterPolicy` on the added member: this member's own remote
    /// all-released admission (own observations plus one validated WRITER
    /// AUDIT frame of every other NEW member), the unchanged first DATA
    /// policy installation, then a second window that must report every
    /// other member unchanged. A repeated run reconciles the installed
    /// policy under a fresh admission instead of installing another.
    fn install_registered_joint_writer_policy(
        &mut self,
        command: &NativeBridgeHostCommand,
        token: &CancellationToken,
    ) -> Result<()> {
        let range = command.request.range_id;
        let release = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?
            .bridge_steps
            .released_member(range)?;
        // The decision this member itself protected, never request DATA.
        command.request.require_decision(release.data())?;
        let transport = self.backend.transport.clone();
        let leader = self
            .backend
            .lock_metadata(token)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("joint writer policy metadata leader unknown"))?;
        let business = Uuid::now_v7();
        let own = transport.acquire_host_metadata_observation(
            &leader,
            business,
            Duration::from_secs(5),
            token,
        )?;
        let (admission, observed) =
            release.acquire_remote_writer_policy(&transport, &leader, &own, business, token)?;
        self.install_registered_serving_data_policy(range, Arc::new(admission), token)?;
        release.confirm_remote_release_second_window(&observed, &transport, &leader, token)?;
        token.check()
    }

    /// `TargetServe` on the added member: the unchanged transfer of its
    /// retained first policy to its ordinary DATA socket, then the unchanged
    /// guarded ingress publication. Both continue an earlier partial run.
    fn serve_registered_joint_target(
        &mut self,
        command: &NativeBridgeHostCommand,
        token: &CancellationToken,
    ) -> Result<u64> {
        let release = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?
            .bridge_steps
            .released_member(command.request.range_id)?;
        command.request.require_decision(release.data())?;
        self.open_registered_joined_data(token)?;
        let recovered = self.publish_joined_data(token)?;
        token.check()?;
        Ok(recovered as u64)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// What the host command slot of this original receiver retains.
    pub(crate) fn test_registered_bridge_host_commands(&self) -> Result<serde_json::Value> {
        Ok(self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?
            .host_commands
            .test_evidence())
    }
    /// Historical identity of the preparation this original Registered
    /// receiver retained; grants nothing.
    pub(crate) fn test_registered_bridge_preparation(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<serde_json::Value>> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let held = receiver.bridge_steps.hold()?;
        Ok(held
            .get(&range)
            .and_then(|step| step.custody())
            .map(|custody| custody.test_evidence()))
    }
    /// What this original Registered receiver retained and durably holds of
    /// its own bridge publication; grants nothing.
    pub(crate) fn test_registered_bridge_publication(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<serde_json::Value>> {
        let receiver = self
            .replica
            .as_ref()
            .ok_or_else(|| invalid("actual Registered receiver absent"))?;
        let held = receiver.bridge_steps.hold()?;
        held.get(&range)
            .map(|step| step.test_publication_evidence())
            .transpose()
    }
}
