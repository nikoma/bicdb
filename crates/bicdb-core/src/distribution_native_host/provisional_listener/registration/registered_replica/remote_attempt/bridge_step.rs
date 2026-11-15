//! The original Registered target's own bridge steps after the decision is
//! committed. Its joint target owner, real freeze and native preparation stay
//! in this receiver; a peer supplies identity DATA and receives checksums.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_native_host::provisional_listener::registration::bridge_step::{
    acquire_bridge_step_own, bridge_reply, observe_bridge_phase, open_member_bridge,
    publish_member_bridge, release_member_bridge, retained_prepare_step, retire_member_old_custody,
    BridgeStepOwn, NativeBridgeMemberSteps,
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
        let (outcome, anchor) = match &request.operation {
            StepOperation::BridgePhaseObserve { kind, .. } => {
                observe_bridge_phase(&steps, peer, request, *kind, fresh, token)?
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
            // The added member never held OLD custody of this range: the
            // proof is still required, nothing is removed.
            StepOperation::RetireOldCustody { .. } => retire_member_old_custody(
                &steps,
                &backend.provider,
                &backend.transport,
                request,
                fresh,
                &|_| Ok(()),
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
            StepOperation::TargetInstall { .. } => {
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
            token,
        )
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
        token.check()?;
        Ok((step.prepare_outcome(Stage::Complete)?, anchor))
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
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
