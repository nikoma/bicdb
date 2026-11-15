//! A surviving original member's own bridge steps after the decision is
//! committed. Its retained OLD closure, CLOSED writer transition, real freeze
//! and native preparation stay in this host; a peer supplies identity DATA.
use super::*;
use crate::db::{NativeWriterDataDomainPolicy, NativeWriterDataDomainTransition};
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_native_host::provisional_listener::registration::bridge_step::{
    acquire_bridge_step_own, bridge_remaining, bridge_reply, observe_bridge_phase,
    open_member_bridge, publish_member_bridge, release_member_bridge, retained_prepare_step,
    retire_joint_source, retire_member_old_custody, BridgeStepOwn, NativeBridgeMemberSteps,
    MIN_BRIDGE_SUBSTEP_BUDGET,
};
use crate::distribution_native_host::provisional_listener::registration::NativeDataOwnerAdmission;
use crate::distribution_range_handoff::{
    NativeJointHandoffStepOperationV1 as StepOperation, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepReplyV1, NativeJointHandoffStepRequestV1,
    NativeJointHandoffStepStageV1 as Stage, NativeJointPreparationCustody,
};

/// The original owners one bounded survivor step runs over.
struct SurvivorBridgeOwners {
    service: Arc<Mutex<ClusterDataNodeService>>,
    runtime: Arc<RangeRecoveryRuntime>,
    metadata: Arc<Mutex<MetadataConsensusStore>>,
    provider: Arc<MetadataReadProvider>,
    transport: Arc<TcpClusterRelocationTransport>,
}

impl NativeInitialDataAdmission {
    /// One bounded bridge step of this original INITIAL member.
    pub(crate) fn advance_joint_bridge_step(
        self: &Arc<Self>,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        parent: &CancellationToken,
    ) -> Result<NativeJointHandoffStepReplyV1> {
        let token = request.cancellation(parent)?;
        if request.operation.bridge_decision_sha256().is_none() {
            return Err(invalid(
                "joint handoff install step requires the original Registered target",
            ));
        }
        self.require_initial_source_hosting()?;
        let owners = SurvivorBridgeOwners {
            service: self
                .service
                .upgrade()
                .ok_or_else(|| invalid("joint bridge step original INITIAL service ended"))?,
            runtime: self
                .runtime
                .upgrade()
                .ok_or_else(|| invalid("joint bridge step original INITIAL runtime ended"))?,
            metadata: self
                .old_metadata
                .upgrade()
                .ok_or_else(|| invalid("joint bridge step original INITIAL MStore ended"))?,
            provider: self
                .old_provider
                .upgrade()
                .ok_or_else(|| invalid("joint bridge step original INITIAL provider ended"))?,
            transport: self
                .old_transport
                .upgrade()
                .ok_or_else(|| invalid("joint bridge step original INITIAL transport ended"))?,
        };
        self.require_listener(&owners.service, &owners.runtime)?;
        let steps = self
            .bridge_steps
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original member custody ended"))?;
        // Authenticate and bind to OWN committed metadata before any retained
        // state is created, read or disclosed.
        let fresh =
            acquire_bridge_step_own(&owners.provider, &owners.transport, peer, request, &token)?;
        let local = owners.transport.local_metadata_read_participant();
        let birth = fresh.birth.clone();
        let (outcome, anchor) = match &request.operation {
            StepOperation::BridgePhaseObserve { kind, .. } => {
                observe_bridge_phase(&steps, peer, request, *kind, fresh, &token)?
            }
            StepOperation::BridgePrepare { .. } => {
                self.prepare_survivor_bridge(&owners, &steps, peer, request, fresh, &token)?
            }
            StepOperation::BridgePublish { .. } => publish_member_bridge(
                &steps,
                &owners.provider,
                &owners.transport,
                request,
                fresh,
                &token,
            )?,
            // A survivor's release owner is the runtime that owns the real
            // freeze of its own retained OLD closure.
            StepOperation::BridgeOpen { .. } => open_member_bridge(
                &steps,
                &owners.provider,
                &owners.transport,
                request,
                fresh,
                Some(&|| self.retained_old_runtime(request.range_id, &request.scope_sha256)),
                &token,
            )?,
            // The donor holds BOTH the remote export certificate and its own
            // OLD observation; every other survivor only the latter.
            StepOperation::RetireOldCustody { .. } => retire_member_old_custody(
                &steps,
                &owners.provider,
                &owners.transport,
                request,
                fresh,
                &|proof| {
                    if let Some(source) = self.retained_export_source(request.range_id)? {
                        source.retire_old_export_custody_after_remote_joint_open(proof)?;
                    }
                    self.retire_old_observation_after_remote_joint_open(request.range_id, proof)
                },
                &token,
            )?,
            StepOperation::BridgeRelease { .. } => release_member_bridge(
                &steps,
                &owners.provider,
                &owners.transport,
                request,
                fresh,
                &|| {
                    if self.retains_old_observation(request.range_id)?
                        || self
                            .retained_export_source(request.range_id)?
                            .map(|source| source.old_custody_retired_after_joint_open())
                            .transpose()?
                            == Some(false)
                    {
                        return Err(invalid(
                            "joint bridge release refused: this member still retains OLD custody",
                        ));
                    }
                    Ok(())
                },
                &token,
            )?,
            // Only the retired source passes the authorisation above for
            // this operation; a member of the final membership never does.
            StepOperation::SourceRetire { .. } => {
                retire_joint_source(&owners.service, local.node_id(), request, fresh, &token)?
            }
            StepOperation::TargetInstall { .. } => unreachable!("refused above"),
        };
        self.require_listener(&owners.service, &owners.runtime)?;
        bridge_reply(
            request,
            local.node_id().clone(),
            birth,
            outcome,
            anchor,
            &token,
        )
    }

    /// The same survivor-local sequence the owning host runs in-process:
    /// begin and verify the CLOSED writer DATA domain over the retained OLD
    /// closure's real freeze, take the lease back, then prepare under an own
    /// fresh committed decision. Every finished part is retained, so a later
    /// call with the same identity continues after the last finished part.
    #[allow(clippy::too_many_arguments)]
    fn prepare_survivor_bridge(
        self: &Arc<Self>,
        owners: &SurvivorBridgeOwners,
        steps: &NativeBridgeMemberSteps,
        peer: &MetadataReadParticipant,
        request: &NativeJointHandoffStepRequestV1,
        fresh: BridgeStepOwn,
        token: &CancellationToken,
    ) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
        let range = request.range_id;
        let mut anchor = fresh.anchor;
        let mut held = steps.hold()?;
        let step = retained_prepare_step(&mut held, request)?;
        if let Some(custody) = step.custody() {
            request.require_decision(custody.data())?;
            return Ok((step.prepare_outcome(Stage::Complete)?, anchor));
        }
        let domains = self
            .writer_data_domains
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original DATA continuation ended"))?;
        let mut domains = domains
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step DATA continuation table busy"))?;
        let coordinator = self
            .source_coordinator
            .read()
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original coordinator ended"))?;
        let guard = self
            .source_writer
            .read()
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original writer guard ended"))?;
        let database = owners
            .service
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step actual service busy"))?
            .database();
        let mut prepared = Some(fresh);
        let mut metadata_substeps = 0u32;
        // Fresh OWN for one sub-step: the request's first observation while it
        // is still live, otherwise a new one under the same full peer check.
        let mut own = |anchor: &mut (u64, u64, Uuid)| -> Result<Option<BridgeStepOwn>> {
            if metadata_substeps != 0 && bridge_remaining(token)? < MIN_BRIDGE_SUBSTEP_BUDGET {
                return Ok(None);
            }
            metadata_substeps += 1;
            let fresh = match prepared.take() {
                Some(fresh) if fresh.own.require_live_request(fresh.business).is_ok() => fresh,
                _ => acquire_bridge_step_own(
                    &owners.provider,
                    &owners.transport,
                    peer,
                    request,
                    token,
                )?,
            };
            *anchor = fresh.anchor;
            Ok(Some(fresh))
        };
        if step.lease.is_none() {
            if !domains.retains_closed(range) {
                let lease = self.retained_survivor_lease(owners, request)?;
                let service = owners
                    .service
                    .try_lock()
                    .ok_or_else(|| invalid("joint bridge step actual service busy"))?;
                let db = database
                    .try_read()
                    .ok_or_else(|| invalid("joint bridge step actual DB busy"))?;
                domains.require_new(range)?;
                let attempt = NativeWriterDataDomainTransition::begin(
                    &db,
                    &owners.service,
                    &service,
                    &owners.runtime,
                    NativeDataOwnerAdmission::Initial(self.clone()),
                    &coordinator,
                    &guard,
                    range,
                    lease,
                    step.session(),
                )?;
                // Retained by the original host BEFORE the first native write.
                domains.retain(range, attempt)?;
            }
            if domains.closed(range).is_err() {
                let Some(BridgeStepOwn { own, business, .. }) = own(&mut anchor)? else {
                    return Ok((step.prepare_outcome(Stage::BridgeWriterDomain)?, anchor));
                };
                let policy = NativeWriterDataDomainPolicy::from_actual(
                    &owners.service,
                    &owners.metadata,
                    own,
                    business,
                    token,
                )?;
                domains.advance_closed(range, &coordinator, &policy)?;
            }
            let closed = domains.closed(range)?;
            closed.require_lifetime()?;
            let db = database
                .try_read()
                .ok_or_else(|| invalid("joint bridge step actual DB busy"))?;
            // The real lease leaves the transition exactly once and is kept
            // here before the fallible native preparation.
            step.lease = Some(domains.take_joint_lease(range, &db)?);
        }
        let closed = domains.closed(range)?;
        closed.require_lifetime()?;
        let Some(BridgeStepOwn { own, business, .. }) = own(&mut anchor)? else {
            return Ok((step.prepare_outcome(Stage::BridgePreparation)?, anchor));
        };
        // The actual attached local MStore point BEFORE any service guard.
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &owners.service,
            &own,
            business,
            token,
        )?;
        let session = step.session().clone();
        let lease = step
            .lease
            .clone()
            .ok_or_else(|| invalid("joint bridge step lost its retained real lease"))?;
        let actual = {
            let service = owners
                .service
                .try_lock()
                .ok_or_else(|| invalid("joint bridge step actual service busy"))?;
            let domain = crate::db::NativeJointWriterDomain::Closed(closed.clone());
            let committed = service.observe_joint_decision_with_writer_domain_and_point(
                &domain, own, business, range, point,
            )?;
            // The decision this member itself protected and sees committed.
            request.require_decision(committed.data())?;
            service.prepare_local_joint_serving_with_closed_writer(
                closed, committed, lease, &session,
            )?
        };
        // Native facts/State/A/pin enter retained custody BEFORE any later
        // check or reply can fail.
        step.retain_custody(NativeJointPreparationCustody::retain(
            actual,
            owners.service.clone(),
        ));
        token.check()?;
        Ok((step.prepare_outcome(Stage::Complete)?, anchor))
    }

    /// The export source this original donor process retained for `range`.
    fn retained_export_source(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<Arc<crate::distribution_range_image::NativeRetainedRemoteImageSource>>> {
        match self.image_sources.upgrade() {
            Some(registry) => registry.retained_range_source(range),
            None => Ok(None),
        }
    }

    /// The real freeze of this member's own retained OLD closure. Nothing is
    /// rebuilt from the request: a missing or foreign closure refuses.
    fn retained_survivor_lease(
        &self,
        owners: &SurvivorBridgeOwners,
        request: &NativeJointHandoffStepRequestV1,
    ) -> Result<crate::distribution_data::NativeJointServingLease> {
        let table = self
            .old_closures
            .upgrade()
            .ok_or_else(|| invalid("joint bridge step original OLD table ended"))?;
        let retained = table
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step original OLD table busy"))?
            .get(&request.range_id)
            .cloned()
            .ok_or_else(|| invalid("joint bridge step retains no original OLD closure"))?;
        let held = retained
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step original OLD operation busy"))?;
        if !Arc::ptr_eq(&held.writer, &self.writer()) || held.plan_sha256 != request.scope_sha256 {
            return Err(invalid(
                "joint bridge step original OLD closure has another writer/scope",
            ));
        }
        let actual = held
            .actual
            .clone()
            .ok_or_else(|| invalid("joint bridge step original native OLD outcome unresolved"))?;
        let (service, lease) = actual.retained_serving_lease()?;
        if !Arc::ptr_eq(&service, &owners.service) {
            return Err(invalid(
                "joint bridge step original OLD closure has another actual service",
            ));
        }
        Ok(lease)
    }
}

impl NativeMetadataControlHost {
    /// Coordinator side, in the metadata leader's own process: observe that
    /// EVERY member of the new set of `data` (the decision this coordinator
    /// observed committed) is durably activated, released and open.
    ///
    /// A coordinator that is itself a member of the new set re-observes its
    /// own released member under its own lock and collects one authenticated
    /// release observation from every other member; a coordinator outside
    /// the new set (it holds no replica of the moved range, or its replica
    /// is the retired source) collects one from EVERY member. Both run over this host's own pinned transport. The
    /// result backs the metadata activation record and nothing else.
    pub(crate) fn certify_remote_joint_activation(
        &self,
        data: &crate::distribution_range_handoff::NativeJointDecisionData,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_handoff::NativeRemoteJointActivationCertificate> {
        parent.check()?;
        let leader = self
            .backend
            .lock_metadata(parent)?
            .status()
            .leader_id
            .ok_or_else(|| invalid("joint activation metadata leader unknown"))?;
        let transport = &self.backend.transport;
        let local = transport.local_metadata_read_participant();
        if !data.new_members().contains_key(local.node_id()) {
            return crate::distribution_range_handoff::NativeRemoteJointActivationCertificate::observe_as_outside_coordinator(
                data, transport, &leader, parent,
            );
        }
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("joint activation observer is not an original member host"))?;
        let release = initial.bridge_steps.released_member(data.old_range().id)?;
        if release.data() != data {
            return Err(invalid(
                "joint activation observer member holds another decision",
            ));
        }
        release.certify_remote_activated_quorum(transport, &leader, parent)
    }
}

#[cfg(test)]
impl NativeMetadataControlHost {
    /// Historical identity of the preparation this original INITIAL member
    /// retained; grants nothing.
    pub(crate) fn test_initial_bridge_preparation(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<serde_json::Value>> {
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("original INITIAL host absent"))?;
        let held = initial.bridge_steps.hold()?;
        Ok(held
            .get(&range)
            .and_then(|step| step.custody())
            .map(|custody| custody.test_evidence()))
    }
    /// Fixture custody transfer: the in-process donor seed's export source
    /// moves into this donor's own original registry.
    pub(crate) fn test_adopt_remote_export_custody(
        &self,
        range: crate::RangeId,
        source: crate::distribution_range_image::NativeRetainedRemoteImageSource,
    ) {
        self.data
            .as_ref()
            .expect("original INITIAL host absent")
            .image_sources
            .as_ref()
            .expect("original INITIAL image registry absent")
            .test_adopt_range_source(range, source);
    }
    /// Whether the export source this donor retains for `range` retired its
    /// OLD certificate custody; `None` when it retains no source.
    pub(crate) fn test_remote_export_custody_retired(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<bool>> {
        self.data
            .as_ref()
            .ok_or_else(|| invalid("original INITIAL host absent"))?
            .image_sources
            .as_ref()
            .ok_or_else(|| invalid("original INITIAL image registry absent"))?
            .retained_range_source(range)?
            .map(|source| source.old_custody_retired_after_joint_open())
            .transpose()
    }
    /// What this original INITIAL member retained and durably holds of its
    /// own bridge publication; grants nothing.
    pub(crate) fn test_initial_bridge_publication(
        &self,
        range: crate::RangeId,
    ) -> Result<Option<serde_json::Value>> {
        let initial = self
            .data
            .as_ref()
            .ok_or_else(|| invalid("original INITIAL host absent"))?;
        let held = initial.bridge_steps.hold()?;
        held.get(&range)
            .map(|step| step.test_publication_evidence())
            .transpose()
    }
}
