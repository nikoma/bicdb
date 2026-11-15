//! Member-local bridge steps after the joint decision is durable and the
//! membership is final. The authenticated peer supplies identity DATA only.
//! THIS member acquires its own fresh host metadata, compares the request
//! with the decision IT observes committed, and performs or re-proves only
//! its own local state. A reply is historical checksummed DATA: never a
//! quorum, phase, lease, publication or serving grant.
use super::*;
use crate::distribution_consensus::MetadataReadParticipant;
use crate::distribution_data::NativeJointServingLease;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_handoff::{
    NativeJointBridgePhaseKindV1, NativeJointHandoffRuntime, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepReplyV1, NativeJointHandoffStepRequestV1,
    NativeJointHandoffStepStageV1 as Stage, NativeJointMemberRelease,
    NativeJointMemberReleaseOwner, NativeJointPreparationCustody, NativeJointPreparationPhaseFacts,
    NativeJointPreparationRetainedFacts, NativeJointRemoteOldRetirementProof,
};
use std::time::Duration;

/// A further sub-step starts only with this much of the request left. The
/// first sub-step of a call always runs so every call can make progress.
pub(super) const MIN_BRIDGE_SUBSTEP_BUDGET: Duration = Duration::from_millis(1500);

/// One member's retained bridge custody for one range. Created by the first
/// authorised `BridgePrepare` and kept by the member's own host: the later
/// publish/open/release steps of the same process continue from here.
pub(super) struct NativeBridgeMemberStep {
    binding_sha256: String,
    decision_sha256: String,
    /// Member-owned preparation session. It outlives every finite request and
    /// ends with this retained entry; no request can renew or replace it.
    session: CancellationToken,
    /// A survivor's real lease after it left the CLOSED writer transition,
    /// retained here BEFORE the fallible native preparation can consume it.
    pub(super) lease: Option<NativeJointServingLease>,
    custody: Option<Arc<NativeJointPreparationCustody>>,
    /// Set BEFORE this member's first native publication attempt and cleared
    /// only when a second observation window confirmed it. While set, the
    /// durable effect of an earlier attempt is unknown or unconfirmed.
    publication_pending: bool,
    /// One publication attempt of this member at a time.
    publishing: Arc<AtomicBool>,
    /// This member's own durable bridge receipt, retained as soon as the
    /// native publisher returned it and before any later check.
    bridge: Option<Arc<crate::db::NativeJointServingBridgeReceipt>>,
    bridge_confirmed: bool,
    /// Same marker for this member's own catalogue activation.
    activation_pending: bool,
    /// This member's own catalogue activation receipt, retained as soon as
    /// the local activation returned it; historical once its phase ended.
    activation:
        Option<Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>>,
    activation_sha256: Option<String>,
    activation_confirmed: bool,
    /// The original Registered receiver's release owner, retained under the
    /// same live admission that activated, until it moves into `release`.
    /// Always `None` on a surviving original member.
    release_owner: Option<crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner>,
    /// This member's own release continuation entry, built from its retained
    /// preparation, confirmed bridge and confirmed activation together with
    /// the real freeze owner. Set together with `activation_confirmed`.
    release: Option<Arc<NativeJointMemberRelease>>,
    /// This member proved every NEW member durably open and holds no OLD
    /// custody of this decision any more.
    old_custody_retired: bool,
    /// Same uncertain marker for this member's own release: set before the
    /// first attempt, cleared only when a second window confirmed it.
    release_pending: bool,
    release_confirmed: bool,
    released_generation: Option<Uuid>,
}
impl Drop for NativeBridgeMemberStep {
    fn drop(&mut self) {
        // Custody cannot outlive the member host that retained it.
        self.session.cancel();
    }
}
impl NativeBridgeMemberStep {
    pub(super) fn session(&self) -> &CancellationToken {
        &self.session
    }
    pub(super) fn custody(&self) -> Option<&Arc<NativeJointPreparationCustody>> {
        self.custody.as_ref()
    }
    /// Memory and durable evidence of this member's own publication; DATA.
    #[cfg(test)]
    pub(super) fn test_publication_evidence(&self) -> Result<serde_json::Value> {
        let custody = self.custody.as_ref();
        Ok(serde_json::json!({
            "publication_pending":self.publication_pending,
            "bridge_confirmed":self.bridge_confirmed,
            "retained_bridge_sha256":self.bridge.as_ref().map(|receipt|receipt.checksum_sha256().to_owned()),
            "read_pin_released":custody.map(|c|c.test_read_pin_released()).transpose()?,
            "durable_bridge":custody.map(|c|c.test_durable_bridge_record()).transpose()?.flatten(),
            "activation_pending":self.activation_pending,
            "activation_confirmed":self.activation_confirmed,
            "retained_activation_sha256":self.activation_sha256,
            "retained_activation_bridge_sha256":self.activation.as_ref().map(|a|a.native_bridge_sha256().to_owned()),
            "release_owner_retained":self.release_owner.is_some()
                || self.release.as_ref().is_some_and(|r|!r.data().old_members().contains_key(r.local_node_id())),
            "preparation_custody_retained":custody.is_some(),
            "release_continuation_retained":self.release.is_some(),
            "old_custody_retired":self.old_custody_retired,
            "release_pending":self.release_pending,
            "release_confirmed":self.release_confirmed,
            "released_generation":self.released_generation,
            "durable_activation_sha256":match (custody,&self.bridge) {
                (Some(custody),Some(bridge)) => custody.durable_activation_sha256(bridge)?,
                _ => None,
            },
        }))
    }
    /// Infallible custody transfer immediately after the genuine producer.
    pub(super) fn retain_custody(&mut self, custody: Arc<NativeJointPreparationCustody>) {
        self.custody = Some(custody);
        // The native preparation now owns the same real lease.
        self.lease = None;
    }
    pub(super) fn prepare_outcome(&self, stage: Stage) -> Result<NativeJointHandoffStepOutcomeV1> {
        Ok(match &self.custody {
            Some(custody) => {
                prepared_outcome(self.binding_sha256.clone(), custody.retained_facts()?, None)
            }
            None => NativeJointHandoffStepOutcomeV1::bridge_progress(
                self.binding_sha256.clone(),
                stage,
                self.decision_sha256.clone(),
            ),
        })
    }
}
#[derive(Default)]
pub(super) struct NativeBridgeMemberSteps {
    steps: Mutex<BTreeMap<crate::RangeId, NativeBridgeMemberStep>>,
}
impl NativeBridgeMemberSteps {
    pub(super) fn hold(
        &self,
    ) -> Result<parking_lot::MutexGuard<'_, BTreeMap<crate::RangeId, NativeBridgeMemberStep>>> {
        self.steps
            .try_lock()
            .ok_or_else(|| invalid("joint bridge step member custody busy"))
    }
    /// This member's own continuation entry once its release is confirmed.
    /// Memory custody only; every use re-observes under the service lock.
    pub(super) fn released_member(
        &self,
        range: crate::RangeId,
    ) -> Result<Arc<NativeJointMemberRelease>> {
        self.hold()?
            .get(&range)
            .filter(|step| step.release_confirmed && !step.release_pending)
            .and_then(|step| step.release.clone())
            .ok_or_else(|| invalid("joint bridge member holds no confirmed release"))
    }
    /// Retention of a produced outcome may not be skipped because another
    /// bounded request of this member is in the table; holders never wait on
    /// this member's own publication.
    fn hold_for_retention(
        &self,
    ) -> parking_lot::MutexGuard<'_, BTreeMap<crate::RangeId, NativeBridgeMemberStep>> {
        self.steps.lock()
    }
}

/// The retained entry for a `BridgePrepare`, created only after the request
/// was authorised against this member's own committed metadata.
pub(super) fn retained_prepare_step<'a>(
    steps: &'a mut BTreeMap<crate::RangeId, NativeBridgeMemberStep>,
    request: &NativeJointHandoffStepRequestV1,
) -> Result<&'a mut NativeBridgeMemberStep> {
    let binding_sha256 = request.binding()?;
    let decision_sha256 = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    if let Some(retained) = steps.get(&request.range_id) {
        if retained.binding_sha256 != binding_sha256 || retained.decision_sha256 != decision_sha256
        {
            return Err(invalid(
                "joint bridge step retains another exact decision preparation",
            ));
        }
    } else {
        if steps.len() >= 8 {
            return Err(invalid(
                "joint bridge step member custody capacity; nothing is evicted",
            ));
        }
        steps.insert(
            request.range_id,
            NativeBridgeMemberStep {
                binding_sha256,
                decision_sha256,
                session: CancellationToken::new(Arc::new(AtomicBool::new(false)), None),
                lease: None,
                custody: None,
                publication_pending: false,
                publishing: Arc::new(AtomicBool::new(false)),
                bridge: None,
                bridge_confirmed: false,
                activation_pending: false,
                activation: None,
                activation_sha256: None,
                activation_confirmed: false,
                release_owner: None,
                release: None,
                old_custody_retired: false,
                release_pending: false,
                release_confirmed: false,
                released_generation: None,
            },
        );
    }
    Ok(steps.get_mut(&request.range_id).unwrap())
}

fn prepared_outcome(
    binding_sha256: String,
    facts: NativeJointPreparationRetainedFacts,
    phase: Option<(NativeJointBridgePhaseKindV1, u64)>,
) -> NativeJointHandoffStepOutcomeV1 {
    let mut outcome = NativeJointHandoffStepOutcomeV1::bridge_progress(
        binding_sha256,
        Stage::Complete,
        facts.decision_sha256,
    );
    outcome.preparation_sha256 = Some(facts.preparation_sha256);
    outcome.native_database_incarnation = Some(facts.native_database_incarnation);
    outcome.local_visibility_sequence = Some(facts.local_visibility_sequence);
    outcome.decision_receipt_sha256 = Some(facts.decision_receipt_sha256);
    outcome.store_open_id = Some(facts.store_open_id);
    outcome.writer_identity_sha256 = Some(facts.writer_identity_sha256);
    if let Some((kind, remaining_ms)) = phase {
        outcome.phase_kind = Some(kind);
        outcome.window_remaining_ms = Some(remaining_ms);
    }
    outcome
}

pub(super) fn bridge_remaining(token: &CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .filter(|left| !left.is_zero())
        .ok_or(BicDbError::QueryTimedOut)
}

/// This member's own fresh committed metadata for one bounded sub-step.
/// Never retained across sub-steps and never derived from request DATA.
pub(super) struct BridgeStepOwn {
    pub(super) own: HostMetadataReadObservation,
    pub(super) business: Uuid,
    pub(super) anchor: (u64, u64, Uuid),
    pub(super) birth: crate::distribution::NativeStoreBirthV1,
    /// The authenticated peer is the range leader or the metadata leader.
    pub(super) coordinator: bool,
}

/// Authenticate the peer and bind the request to the decision committed in
/// THIS member's own fresh metadata, before any retained state is created,
/// read or disclosed.
///
/// A mutation is accepted only from the coordinator: the same initiator
/// predicate every other handoff mutation uses (the range leader or the
/// metadata leader), evaluated against this member's own observation. A
/// read-only observation is also accepted from another data voter of the
/// final membership; the caller re-checks that peer against the NEW member
/// set of the committed decision it observes natively.
pub(super) fn acquire_bridge_step_own(
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    peer: &MetadataReadParticipant,
    request: &NativeJointHandoffStepRequestV1,
    token: &CancellationToken,
) -> Result<BridgeStepOwn> {
    token.check()?;
    request.require_shape()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?;
    let local = transport.local_metadata_read_participant();
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let business = Uuid::now_v7();
    let own = transport.acquire_host_metadata_observation(
        &leader,
        business,
        bridge_remaining(token)?.min(Duration::from_secs(5)),
        token,
    )?;
    let topology = own.committed_topology(business)?;
    let actor = topology
        .nodes
        .get(peer.node_id())
        .ok_or_else(|| invalid("joint bridge step authenticated actor absent"))?;
    if peer.plaintext_loopback()
        || peer.node_id() == local.node_id()
        || actor.lifecycle != ClusterNodeLifecycle::Active
        || actor.incarnation != peer.member_incarnation()
        || peer
            .certificate_sha256()
            .is_none_or(|pin| !actor.accepts_tls_certificate_sha256(pin))
        || own.requester_node_id() != local.node_id()
        || own.requester_incarnation() != local.member_incarnation()
    {
        return Err(invalid(
            "joint bridge step actual peer/credential differs from committed member",
        ));
    }
    let retiring = matches!(
        &request.operation,
        crate::distribution_range_handoff::NativeJointHandoffStepOperationV1::SourceRetire { .. }
    );
    let relocation = topology
        .relocations
        .get(&request.relocation_id)
        .ok_or_else(|| invalid("joint bridge step relocation absent from own metadata"))?;
    let record = relocation
        .native_joint_handoff_v1
        .as_ref()
        .ok_or_else(|| invalid("joint bridge step decision is not committed in own metadata"))?;
    let range = topology
        .range_by_id(request.range_id)
        .ok_or_else(|| invalid("joint bridge step range absent from own metadata"))?;
    let voter = |node: &ClusterNodeId| {
        range.replicas.iter().any(|replica| {
            &replica.node_id == node && replica.role == crate::distribution::RangeReplicaRole::Voter
        })
    };
    if relocation.range_id != request.range_id
        || relocation.plan_id != request.plan_id
        || relocation.learner_epoch != request.range_epoch
        || relocation.handoff_session.as_ref().is_none_or(|session| {
            session.session_id != request.session_id || session.nonce != request.nonce
        })
        || record.stage() < crate::distribution::NativeJointHandoffStageV1::AwaitingActivation
        || record.decision_sha256() != decision
        || record.session_id() != request.session_id
        || record.nonce() != request.nonce
        || if retiring {
            // The retired source: named by the committed relocation, absent
            // from the committed final membership, after the recorded
            // activation of every member of the new set.
            relocation.source.as_ref() != Some(local.node_id())
                || range
                    .replicas
                    .iter()
                    .any(|replica| &replica.node_id == local.node_id())
                || record.stage() < crate::distribution::NativeJointHandoffStageV1::AwaitingCleanup
        } else {
            !voter(local.node_id())
        }
    {
        return Err(invalid(
            "joint bridge step differs from the decision committed in own metadata",
        ));
    }
    let coordinator = peer.node_id() == &leader || &range.leader == peer.node_id();
    if !(coordinator || (request.operation.read_only() && voter(peer.node_id()))) {
        return Err(invalid(
            "joint bridge step authenticated peer is not an admitted initiator",
        ));
    }
    let birth = topology
        .nodes
        .get(local.node_id())
        .and_then(|node| node.native_store_birth_v1.clone())
        .ok_or_else(|| invalid("joint bridge step local native birth absent"))?;
    let anchor = (own.term(), own.read_index(), own.context_id());
    token.check()?;
    Ok(BridgeStepOwn {
        own,
        business,
        anchor,
        birth,
        coordinator,
    })
}

/// Read-only PREPARATION phase observation of this member's retained custody.
/// A member that retained nothing has nothing to attest and refuses.
pub(super) fn observe_bridge_phase(
    steps: &NativeBridgeMemberSteps,
    peer: &MetadataReadParticipant,
    request: &NativeJointHandoffStepRequestV1,
    kind: NativeJointBridgePhaseKindV1,
    fresh: BridgeStepOwn,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    enum Retained {
        Preparation(
            Arc<NativeJointPreparationCustody>,
            Option<Arc<crate::db::NativeJointServingBridgeReceipt>>,
        ),
        Release(Arc<NativeJointMemberRelease>),
    }
    let nothing =
        || invalid("joint bridge phase observation of this kind has no retained member state yet");
    let retained = {
        let held = steps.hold()?;
        let step = held.get(&request.range_id);
        match kind {
            NativeJointBridgePhaseKindV1::Preparation | NativeJointBridgePhaseKindV1::Open => {
                let custody = step
                    .and_then(|step| step.custody().cloned())
                    .ok_or_else(|| invalid("joint bridge phase member retains no preparation"))?;
                let bridge = match kind {
                    // Only a bridge this member itself published AND
                    // confirmed is attested; an unconfirmed or absent one
                    // has nothing to report.
                    NativeJointBridgePhaseKindV1::Open => Some(
                        step.filter(|step| step.bridge_confirmed && !step.publication_pending)
                            .and_then(|step| step.bridge.clone())
                            .ok_or_else(nothing)?,
                    ),
                    _ => None,
                };
                Retained::Preparation(custody, bridge)
            }
            // Only a catalogue activation this member itself persisted AND
            // confirmed has a release continuation entry to attest.
            NativeJointBridgePhaseKindV1::Release => Retained::Release(
                step.filter(|step| step.activation_confirmed && !step.activation_pending)
                    .and_then(|step| step.release.clone())
                    .ok_or_else(nothing)?,
            ),
        }
    };
    // The NEW member set comes from the decision this member itself holds
    // natively, never from the request or from the peer.
    let data = match &retained {
        Retained::Preparation(custody, _) => custody.data(),
        Retained::Release(release) => release.data(),
    };
    request.require_decision(data)?;
    if data
        .new_members()
        .get(peer.node_id())
        .is_none_or(|member| member.incarnation != peer.member_incarnation())
        && !fresh.coordinator
    {
        return Err(invalid(
            "joint bridge phase observer is outside the committed NEW member set",
        ));
    }
    let BridgeStepOwn { own, business, .. } = fresh;
    let window = |deadline: Instant| -> Result<u64> {
        Ok(deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or(BicDbError::QueryTimedOut)?
            .as_millis()
            .clamp(1, 5000) as u64)
    };
    let (custody, bridge) = match retained {
        Retained::Preparation(custody, bridge) => (custody, bridge),
        Retained::Release(release) => {
            let facts = release.observe_fresh_release_phase(own, business, token)?;
            let mut outcome = prepared_outcome(
                request.binding()?,
                facts.retained,
                Some((kind, window(facts.deadline)?)),
            );
            outcome.bridge_receipt_sha256 = Some(facts.bridge.0);
            outcome.bridge_anchor = Some(facts.bridge.1);
            outcome.activation_sha256 = Some(facts.activation_sha256);
            outcome.released = Some(facts.released_generation.is_some());
            outcome.released_generation = facts.released_generation;
            return Ok((outcome, facts.metadata_anchor));
        }
    };
    let facts: NativeJointPreparationPhaseFacts = match &bridge {
        None => custody.observe_fresh_preparation_phase(own, business, token)?,
        Some(bridge) => custody.observe_fresh_open_phase(bridge, own, business, token)?,
    };
    let anchor = (
        facts.metadata_term,
        facts.metadata_read_index,
        facts.metadata_context_id,
    );
    let mut outcome = prepared_outcome(
        request.binding()?,
        facts.retained,
        Some((kind, window(facts.deadline)?)),
    );
    if let Some((bridge_sha256, bridge_anchor)) = facts.bridge {
        outcome.bridge_receipt_sha256 = Some(bridge_sha256);
        outcome.bridge_anchor = Some(bridge_anchor);
    }
    Ok((outcome, anchor))
}

/// Clears the single-attempt claim when one publication attempt ends.
struct PublishingClaim(Arc<AtomicBool>);
impl Drop for PublishingClaim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// This member publishes its OWN native serving bridge.
///
/// The member itself observes every other NEW member over its own pinned
/// transport before the durable write and again after it. The reply reports
/// the durable receipt checksum only after that second window confirmed an
/// unchanged member set; otherwise the uncertain marker stays and the same
/// request identity resumes under a distinct fresh phase, where the native
/// publisher reconciles the exact existing bridge instead of rewriting it.
pub(super) fn publish_member_bridge(
    steps: &NativeBridgeMemberSteps,
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let binding = request.binding()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let published_outcome = |custody: &NativeJointPreparationCustody,
                             receipt: &crate::db::NativeJointServingBridgeReceipt|
     -> Result<NativeJointHandoffStepOutcomeV1> {
        let mut outcome = prepared_outcome(binding.clone(), custody.retained_facts()?, None);
        outcome.bridge_receipt_sha256 = Some(receipt.checksum_sha256().into());
        Ok(outcome)
    };
    let anchor = fresh.anchor;
    let (custody, confirmed, claim) = {
        let mut held = steps.hold()?;
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member retains no preparation"))?;
        let custody = step
            .custody()
            .cloned()
            .ok_or_else(|| invalid("joint bridge publish member retains no preparation"))?;
        // The decision this member itself protected, never request DATA.
        request.require_decision(custody.data())?;
        if step.bridge_confirmed && !step.publication_pending {
            (custody, step.bridge.clone(), None)
        } else {
            if step.publishing.swap(true, Ordering::AcqRel) {
                return Err(invalid(
                    "joint bridge publish member attempt already running",
                ));
            }
            // Uncertain from here until a second window confirms.
            step.publication_pending = true;
            (
                custody,
                None,
                Some(PublishingClaim(step.publishing.clone())),
            )
        }
    };
    if let Some(receipt) = confirmed {
        // An identical retry: the SAME retained receipt, re-read as this
        // member's actual protected bridge under its own lock.
        custody.require_published_bridge_current(&receipt)?;
        token.check()?;
        return Ok((published_outcome(&custody, &receipt)?, anchor));
    }
    let _claim = claim;
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let BridgeStepOwn { own, business, .. } = fresh;
    let published = custody.publish_remote_bridge(transport, &leader, &own, business, token)?;
    let receipt = published.receipt().clone();
    // Retained BEFORE the second window or any later check can fail.
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member custody ended"))?;
        if step
            .bridge
            .as_ref()
            .is_some_and(|old| old.checksum_sha256() != receipt.checksum_sha256())
        {
            return Err(invalid(
                "joint bridge publish changed this member's retained native bridge",
            ));
        }
        step.bridge = Some(receipt.clone());
    }
    let incomplete = || {
        NativeJointHandoffStepOutcomeV1::bridge_progress(
            binding.clone(),
            Stage::BridgePublication,
            decision.clone(),
        )
    };
    if bridge_remaining(token).map_or(true, |left| left < MIN_BRIDGE_SUBSTEP_BUDGET) {
        return Ok((incomplete(), anchor));
    }
    if let Err(_error) =
        custody.confirm_remote_bridge_second_window(&published, transport, &leader, token)
    {
        // Fail closed: the durable bridge exists but is not reported. The
        // marker stays; the next call re-proves everything under a new phase.
        #[cfg(test)]
        eprintln!(
            "NATIVE_BRIDGE_PUBLISH_UNCONFIRMED node={} error={_error}",
            custody.local_node_id()
        );
        return Ok((incomplete(), anchor));
    }
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge publish member custody ended"))?;
        step.bridge_confirmed = true;
        step.publication_pending = false;
    }
    token.check()?;
    Ok((published_outcome(&custody, &receipt)?, anchor))
}

/// This member persists its OWN catalogue activation over its own confirmed
/// durable bridge. Same shape as `publish_member_bridge`: first window,
/// post-quorum observation, the unchanged local activation (which recovers an
/// exact existing one), receipt and release owner retained, then a second
/// window that must report every other member unchanged before the reply
/// reports anything.
pub(super) fn open_member_bridge(
    steps: &NativeBridgeMemberSteps,
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    survivor_runtime: Option<&dyn Fn() -> Result<Arc<NativeJointHandoffRuntime>>>,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let registered = survivor_runtime.is_none();
    let binding = request.binding()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let opened_outcome = |custody: &NativeJointPreparationCustody,
                          bridge: &crate::db::NativeJointServingBridgeReceipt,
                          activation_sha256: &str|
     -> Result<NativeJointHandoffStepOutcomeV1> {
        let mut outcome = prepared_outcome(binding.clone(), custody.retained_facts()?, None);
        outcome.bridge_receipt_sha256 = Some(bridge.checksum_sha256().into());
        outcome.activation_sha256 = Some(activation_sha256.to_owned());
        Ok(outcome)
    };
    let anchor = fresh.anchor;
    let (custody, bridge, confirmed, claim) = {
        let mut held = steps.hold()?;
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge open member retains no preparation"))?;
        let custody = step
            .custody()
            .cloned()
            .ok_or_else(|| invalid("joint bridge open member retains no preparation"))?;
        // The decision this member itself protected, never request DATA.
        request.require_decision(custody.data())?;
        let bridge = step
            .bridge
            .clone()
            .filter(|_| step.bridge_confirmed && !step.publication_pending)
            .ok_or_else(|| {
                invalid("joint bridge open member holds no confirmed bridge publication")
            })?;
        if step.activation_confirmed && !step.activation_pending {
            (custody, bridge, step.activation_sha256.clone(), None)
        } else {
            if step.publishing.swap(true, Ordering::AcqRel) {
                return Err(invalid("joint bridge open member attempt already running"));
            }
            // Uncertain from here until a second window confirms.
            step.activation_pending = true;
            (
                custody,
                bridge,
                None,
                Some(PublishingClaim(step.publishing.clone())),
            )
        }
    };
    if let Some(activation_sha256) = confirmed {
        // An identical retry: the SAME retained activation, re-read as this
        // member's actual protected catalogue fact under its own lock.
        if custody.durable_activation_sha256(&bridge)?.as_deref()
            != Some(activation_sha256.as_str())
        {
            return Err(invalid(
                "joint bridge open retained activation is absent or changed in the store",
            ));
        }
        token.check()?;
        return Ok((
            opened_outcome(&custody, &bridge, &activation_sha256)?,
            anchor,
        ));
    }
    let _claim = claim;
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let BridgeStepOwn { own, business, .. } = fresh;
    // Retained BEFORE the release owner, the second window or any later
    // check can fail.
    let mut retain = |activation: &Arc<
        crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    >| {
        if let Some(step) = steps
            .hold_for_retention()
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
        {
            step.activation = Some(activation.clone());
        }
    };
    let (opened, release_owner) = custody.open_remote_activation(
        &bridge,
        transport,
        &leader,
        &own,
        business,
        registered,
        token,
        &mut retain,
    )?;
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge open member custody ended"))?;
        if step
            .activation_sha256
            .as_deref()
            .is_some_and(|old| old != opened.activation_sha256())
        {
            return Err(invalid(
                "joint bridge open changed this member's retained catalogue activation",
            ));
        }
        step.activation = Some(opened.activation().clone());
        step.activation_sha256 = Some(opened.activation_sha256().to_owned());
        if let Some(owner) = release_owner {
            step.release_owner = Some(owner);
        }
    }
    let incomplete = || {
        NativeJointHandoffStepOutcomeV1::bridge_progress(
            binding.clone(),
            Stage::BridgeActivation,
            decision.clone(),
        )
    };
    if bridge_remaining(token).map_or(true, |left| left < MIN_BRIDGE_SUBSTEP_BUDGET) {
        return Ok((incomplete(), anchor));
    }
    if let Err(_error) =
        custody.confirm_remote_open_second_window(&opened, &bridge, transport, &leader, token)
    {
        // Fail closed: the durable activation exists but is not reported.
        #[cfg(test)]
        eprintln!(
            "NATIVE_BRIDGE_OPEN_UNCONFIRMED node={} error={_error}",
            custody.local_node_id()
        );
        return Ok((incomplete(), anchor));
    }
    // This member's own release continuation entry, from the real freeze
    // owner it retained. Built outside the table latch; an error leaves the
    // activation unconfirmed and the same request identity resumes it.
    let (owner, session) = {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge open member custody ended"))?;
        let owner = match survivor_runtime {
            Some(runtime) => NativeJointMemberReleaseOwner::Survivor(runtime()?),
            None => {
                NativeJointMemberReleaseOwner::Registered(step.release_owner.take().ok_or_else(
                    || invalid("joint bridge open retained no original release owner"),
                )?)
            }
        };
        (owner, step.session().clone())
    };
    let release = custody.retain_member_release(&bridge, opened.activation(), owner, &session)?;
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge open member custody ended"))?;
        step.release = Some(release);
        step.activation_confirmed = true;
        step.activation_pending = false;
    }
    token.check()?;
    Ok((
        opened_outcome(&custody, &bridge, opened.activation_sha256())?,
        anchor,
    ))
}

fn released_outcome(
    binding: String,
    release: &NativeJointMemberRelease,
) -> NativeJointHandoffStepOutcomeV1 {
    let mut outcome = prepared_outcome(binding, release.retained_facts().clone(), None);
    outcome.bridge_receipt_sha256 = Some(release.bridge().checksum_sha256().into());
    outcome.activation_sha256 = Some(release.activation_sha256().into());
    outcome
}

/// This member's confirmed release continuation entry for `request`.
fn confirmed_release(
    steps: &NativeBridgeMemberSteps,
    request: &NativeJointHandoffStepRequestV1,
    decision: &str,
) -> Result<Arc<NativeJointMemberRelease>> {
    let held = steps.hold()?;
    let release = held
        .get(&request.range_id)
        .filter(|step| {
            step.decision_sha256 == decision
                && step.activation_confirmed
                && !step.activation_pending
        })
        .and_then(|step| step.release.clone())
        .ok_or_else(|| {
            invalid("joint bridge step member holds no confirmed catalogue activation")
        })?;
    // The decision this member itself protected, never request DATA.
    request.require_decision(release.data())?;
    Ok(release)
}

/// This member retires the obsolete OLD memory custody it still holds.
///
/// The member itself proves that EVERY NEW member is durably open: its own
/// fresh release observation under its service lock plus one authenticated
/// RELEASE observation from every other NEW member in one bounded window.
/// `retire` revalidates each OLD owner for the post-open point with that
/// proof and removes it; absent custody is success. No freeze is released,
/// nothing is armed and no source data is cleaned.
pub(super) fn retire_member_old_custody(
    steps: &NativeBridgeMemberSteps,
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    retire: &dyn Fn(&NativeJointRemoteOldRetirementProof) -> Result<()>,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let binding = request.binding()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let anchor = fresh.anchor;
    let release = confirmed_release(steps, request, &decision)?;
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    let BridgeStepOwn { own, business, .. } = fresh;
    // Every call re-proves: a retained flag is never the proof.
    let proof =
        release.observe_all_open_for_old_retirement(transport, &leader, &own, business, token)?;
    retire(&proof)?;
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge retire member custody ended"))?;
        step.old_custody_retired = true;
    }
    token.check()?;
    let mut outcome = released_outcome(binding, &release);
    outcome.old_custody_retired = Some(true);
    Ok((outcome, anchor))
}

/// This member releases its OWN retained freeze and arms its own same-open
/// generation.
///
/// Same shape as `publish_member_bridge`: own pre-quorum release observation,
/// first window over every other NEW member (each attesting its own durable
/// activation), only then this member's obsolete preparation custody ends so
/// its real freeze is uniquely owned, a distinct post-quorum observation, the
/// unchanged owner release, and a second window that must report every other
/// member unchanged and this member released before the reply reports it.
/// `old_custody_absent` refuses while this member still holds OLD custody.
pub(super) fn release_member_bridge(
    steps: &NativeBridgeMemberSteps,
    provider: &MetadataReadProvider,
    transport: &TcpClusterRelocationTransport,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    old_custody_absent: &dyn Fn() -> Result<()>,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let binding = request.binding()?;
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let anchor = fresh.anchor;
    let release = confirmed_release(steps, request, &decision)?;
    let complete = |generation: Uuid| {
        let mut outcome = released_outcome(binding.clone(), &release);
        outcome.released = Some(true);
        outcome.released_generation = Some(generation);
        outcome
    };
    let (confirmed, claim) = {
        let mut held = steps.hold()?;
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge release member custody ended"))?;
        if step.release_confirmed && !step.release_pending {
            (step.released_generation, None)
        } else {
            // A precondition this member can check locally: its own OLD
            // custody must be retired before its freeze can be released.
            old_custody_absent()?;
            if step.publishing.swap(true, Ordering::AcqRel) {
                return Err(invalid(
                    "joint bridge release member attempt already running",
                ));
            }
            // Uncertain from here until a second window confirms.
            step.release_pending = true;
            (None, Some(PublishingClaim(step.publishing.clone())))
        }
    };
    let BridgeStepOwn { own, business, .. } = fresh;
    if let Some(generation) = confirmed {
        // An identical retry: this member's actual armed generation, re-read
        // under its own lock by a fresh release observation.
        let facts = release.observe_fresh_release_phase(own, business, token)?;
        if facts.released_generation != Some(generation) {
            return Err(invalid(
                "joint bridge release retained generation is absent or changed in the store",
            ));
        }
        token.check()?;
        return Ok((complete(generation), anchor));
    }
    let _claim = claim;
    let leader = provider
        .current_metadata_leader_hint(token)?
        .ok_or_else(|| invalid("joint bridge step current metadata leader unavailable"))?;
    // Only AFTER every other NEW member attested its durable activation does
    // this member end its own preparation custody; no other member's open
    // window can need this member's preparation observation any more.
    let mut end_preparation = || -> Result<()> {
        let ended = {
            let mut held = steps.hold_for_retention();
            let step = held
                .get_mut(&request.range_id)
                .filter(|step| step.decision_sha256 == decision)
                .ok_or_else(|| invalid("joint bridge release member custody ended"))?;
            (step.custody.take(), step.lease.take())
        };
        // The real drop happens outside the table latch.
        drop(ended);
        Ok(())
    };
    let released = release.release_remote(
        transport,
        &leader,
        &own,
        business,
        token,
        &mut end_preparation,
    )?;
    let incomplete = || {
        NativeJointHandoffStepOutcomeV1::bridge_progress(
            binding.clone(),
            Stage::BridgeRelease,
            decision.clone(),
        )
    };
    if bridge_remaining(token).map_or(true, |left| left < MIN_BRIDGE_SUBSTEP_BUDGET) {
        return Ok((incomplete(), anchor));
    }
    let generation =
        match release.confirm_remote_release_second_window(&released, transport, &leader, token) {
            Ok(generation) => generation,
            Err(_error) => {
                // Fail closed: the release happened but is not reported.
                #[cfg(test)]
                eprintln!(
                    "NATIVE_BRIDGE_RELEASE_UNCONFIRMED node={} error={_error}",
                    release.local_node_id()
                );
                return Ok((incomplete(), anchor));
            }
        };
    {
        let mut held = steps.hold_for_retention();
        let step = held
            .get_mut(&request.range_id)
            .filter(|step| step.decision_sha256 == decision)
            .ok_or_else(|| invalid("joint bridge release member custody ended"))?;
        if step
            .released_generation
            .is_some_and(|old| old != generation)
        {
            return Err(invalid(
                "joint bridge release changed this member's armed generation",
            ));
        }
        step.released_generation = Some(generation);
        step.release_confirmed = true;
        step.release_pending = false;
    }
    token.check()?;
    Ok((complete(generation), anchor))
}

/// The retired source records its OWN retirement from the range on its own
/// fresh committed metadata read, through the unchanged service function
/// (which confirms an existing record instead of rewriting it). The reply
/// reports that durable record; no row is deleted and no fence is released.
pub(super) fn retire_joint_source(
    service: &Arc<Mutex<ClusterDataNodeService>>,
    local: &ClusterNodeId,
    request: &NativeJointHandoffStepRequestV1,
    fresh: BridgeStepOwn,
    token: &CancellationToken,
) -> Result<(NativeJointHandoffStepOutcomeV1, (u64, u64, Uuid))> {
    let decision = request
        .operation
        .bridge_decision_sha256()
        .ok_or_else(|| invalid("joint bridge step needs a committed decision identity"))?
        .to_owned();
    let BridgeStepOwn {
        own,
        business,
        anchor,
        ..
    } = fresh;
    let retired = ClusterDataNodeService::retire_confirmed_native_joint_source(
        service,
        &own,
        business,
        request.relocation_id,
        request.range_id,
        token,
    )?;
    if retired.node_id() != local
        || retired.relocation_id() != request.relocation_id
        || retired.range_id() != request.range_id
        || retired.session_id() != request.session_id
        || retired.nonce() != request.nonce
        || retired.decision_sha256() != decision
    {
        return Err(invalid(
            "joint source retirement record differs from the requested committed decision",
        ));
    }
    let data = crate::distribution_range_handoff::NativeJointSourceRetirementDataV1 {
        cluster_id: retired.cluster_id().clone(),
        node_id: retired.node_id().clone(),
        node_incarnation: retired.node_incarnation(),
        relocation_id: retired.relocation_id(),
        range_id: retired.range_id(),
        session_id: retired.session_id(),
        nonce: retired.nonce(),
        activation_epoch: retired.activation_epoch(),
        decision_sha256: retired.decision_sha256().to_owned(),
        activated_quorum_sha256: retired.activated_quorum_sha256().to_owned(),
        checksum_sha256: retired.checksum_sha256().to_owned(),
    };
    retired.require_current()?;
    token.check()?;
    Ok((
        NativeJointHandoffStepOutcomeV1::source_retired(request.binding()?, decision, data),
        anchor,
    ))
}

pub(super) fn bridge_reply(
    request: &NativeJointHandoffStepRequestV1,
    node_id: ClusterNodeId,
    birth: crate::distribution::NativeStoreBirthV1,
    outcome: NativeJointHandoffStepOutcomeV1,
    anchor: (u64, u64, Uuid),
    token: &CancellationToken,
) -> Result<NativeJointHandoffStepReplyV1> {
    let mut reply = NativeJointHandoffStepReplyV1 {
        request: request.clone(),
        node_id,
        birth,
        outcome,
        metadata_term: anchor.0,
        metadata_read_index: anchor.1,
        metadata_context_id: anchor.2,
        source_manifest: None,
        checksum_sha256: String::new(),
    };
    reply.checksum_sha256 = reply.checksum()?;
    reply.require_request(request)?;
    token.check()?;
    Ok(reply)
}
