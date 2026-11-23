//! A NEW member publishes its own native serving bridge when the other NEW
//! members live in other processes. Nothing here is built from decoded DATA:
//! the local member is its actual retained preparation, re-proved under its
//! own service lock, and every other member is a frame the pinned transport
//! minted after authenticating that member in THIS member's own exchange.
//!
//! No foreign lock can be taken, so the in-process "re-read every other
//! member under its lock immediately before the journal" is replaced by:
//! 1. every counted frame is still inside the bounded window in which that
//!    member re-proved its own state under ITS lock;
//! 2. the identity each member reported covers what the in-process check
//!    compares, including the exact writer registration object;
//! 3. a SECOND window after the local durable write must report the same
//!    identity for every other member, or the publication stays unconfirmed.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointBridgePhaseKindV1, NativeJointHandoffStepOperationV1,
    NativeJointHandoffStepRequestV1,
};
use crate::distribution_transport::{
    AuthenticatedNativeJointBridgeStepFrame, TcpClusterRelocationTransport,
};
use std::time::{Duration, Instant};

/// One whole remote phase, as the in-process phase round is bounded. A
/// member's phase is also bounded by its own step request, which a receiver
/// serves for at most 5 s less the reply margin; the phase does not end
/// earlier than that request on its own.
pub(super) const REMOTE_PHASE_BUDGET: Duration = Duration::from_secs(5);

/// What one other NEW member reported about itself in one authenticated
/// exchange: everything the in-process phase compares for that member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeJointBridgeMemberIdentity {
    /// From the authenticated peer, never from reply DATA.
    pub(crate) member_incarnation: u64,
    pub(crate) birth: crate::distribution::NativeStoreBirthV1,
    pub(crate) native_database_incarnation: String,
    pub(crate) store_open_id: Uuid,
    pub(crate) decision_receipt_sha256: String,
    pub(crate) preparation_sha256: String,
    pub(crate) local_visibility_sequence: u64,
    pub(crate) writer_identity_sha256: String,
    /// OPEN phase only: the member's own durable bridge receipt and the
    /// post-quorum metadata anchor recorded in it.
    pub(crate) bridge: Option<(String, (u64, u64, Uuid))>,
    /// RELEASE phase only: the member's own durable catalogue activation
    /// and its armed same-open generation (`None` while it is not released).
    pub(crate) release: Option<(String, Option<Uuid>)>,
}

/// Every rule one counted phase frame must satisfy. The member is the peer
/// the transport authenticated, never an argument or reply DATA.
fn require_phase_frame(
    kind: NativeJointBridgePhaseKindV1,
    data: &NativeJointDecisionData,
    observer: &ClusterNodeId,
    node: &ClusterNodeId,
    frame: &AuthenticatedNativeJointBridgeStepFrame,
) -> Result<NativeJointBridgeMemberIdentity> {
    let reply = frame.reply();
    // The transport already bound this reply to the request it actually sent.
    reply.require_request(&reply.request)?;
    reply.request.require_decision(data)?;
    let member = data
        .new_members()
        .get(node)
        .ok_or_else(|| invalid("remote preparation frame member is outside the NEW set"))?;
    let outcome = frame.outcome();
    if node == observer
        || frame.actual_observer() != observer
        || frame.actual_peer() != node
        || &reply.node_id != node
        || frame.actual_peer_incarnation() != member.incarnation
        || &reply.birth != frame.actual_peer_birth()
        || !matches!(
            &reply.request.operation,
            NativeJointHandoffStepOperationV1::BridgePhaseObserve { kind: actual, .. }
                if *actual == kind
        )
        || outcome.phase_kind != Some(kind)
        || (kind != NativeJointBridgePhaseKindV1::Preparation)
            != (outcome.bridge_receipt_sha256.is_some() && outcome.bridge_anchor.is_some())
        || (kind == NativeJointBridgePhaseKindV1::Preparation
            && (outcome.bridge_receipt_sha256.is_some() || outcome.bridge_anchor.is_some()))
        || kind.reports_release()
            != (outcome.activation_sha256.is_some() && outcome.released.is_some())
        || (!kind.reports_release()
            && (outcome.activation_sha256.is_some()
                || outcome.released.is_some()
                || outcome.released_generation.is_some()))
        || outcome.released_generation.is_some() != (outcome.released == Some(true))
        || !outcome.complete
        || outcome.decision_sha256.as_deref() != Some(data.checksum_sha256())
        // Only a released member's WRITER AUDIT frame carries an audit.
        || reply.writer_audit.is_some()
            != (kind == NativeJointBridgePhaseKindV1::WriterAudit
                && outcome.released == Some(true))
    {
        return Err(invalid(
            "remote preparation frame differs from authenticated NEW member/decision/phase",
        ));
    }
    let missing = || invalid("remote preparation frame omits member identity DATA");
    Ok(NativeJointBridgeMemberIdentity {
        member_incarnation: frame.actual_peer_incarnation(),
        birth: frame.actual_peer_birth().clone(),
        native_database_incarnation: outcome
            .native_database_incarnation
            .clone()
            .ok_or_else(missing)?,
        store_open_id: outcome.store_open_id.ok_or_else(missing)?,
        decision_receipt_sha256: outcome
            .decision_receipt_sha256
            .clone()
            .ok_or_else(missing)?,
        preparation_sha256: outcome.preparation_sha256.clone().ok_or_else(missing)?,
        local_visibility_sequence: outcome.local_visibility_sequence.ok_or_else(missing)?,
        writer_identity_sha256: outcome.writer_identity_sha256.clone().ok_or_else(missing)?,
        bridge: outcome
            .bridge_receipt_sha256
            .clone()
            .zip(outcome.bridge_anchor),
        release: outcome
            .activation_sha256
            .clone()
            .map(|activation| (activation, outcome.released_generation)),
    })
}

/// One bounded window of authenticated phase observations of one kind, one
/// from every NEW member other than the observer. Constructed only by
/// `collect`, which performs the exchanges itself.
pub(super) struct RemotePreparationWindow {
    pub(super) kind: NativeJointBridgePhaseKindV1,
    pub(super) observer: ClusterNodeId,
    /// `true`: the observer is itself a NEW member and counts every OTHER
    /// one (every member-side proof). `false`: the observer is outside the
    /// NEW set and counts EVERY member (coordinator-side aggregation only).
    pub(super) member_observer: bool,
    pub(super) frames: BTreeMap<ClusterNodeId, AuthenticatedNativeJointBridgeStepFrame>,
    pub(super) identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
}
impl RemotePreparationWindow {
    /// `own` is the observer's fresh committed metadata every exchange of
    /// this window is bound to; every frame's window is at most its lifetime.
    pub(super) fn collect(
        kind: NativeJointBridgePhaseKindV1,
        data: &NativeJointDecisionData,
        client: &TcpClusterRelocationTransport,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        token: &crate::CancellationToken,
    ) -> Result<Self> {
        Self::collect_inner(kind, data, client, own, business, token, true)
    }
    /// Same window for an observer OUTSIDE the NEW set: one frame from EVERY
    /// NEW member. Each member admits such an observer only when its own
    /// metadata shows it is the coordinator.
    pub(super) fn collect_from_outside(
        kind: NativeJointBridgePhaseKindV1,
        data: &NativeJointDecisionData,
        client: &TcpClusterRelocationTransport,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        token: &crate::CancellationToken,
    ) -> Result<Self> {
        Self::collect_inner(kind, data, client, own, business, token, false)
    }
    #[allow(clippy::too_many_arguments)]
    fn collect_inner(
        kind: NativeJointBridgePhaseKindV1,
        data: &NativeJointDecisionData,
        client: &TcpClusterRelocationTransport,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        token: &crate::CancellationToken,
        member_observer: bool,
    ) -> Result<Self> {
        let observer = client.local_metadata_read_participant().node_id().clone();
        let mut frames = BTreeMap::new();
        let mut identities = BTreeMap::new();
        let nodes = data
            .new_members()
            .keys()
            .filter(|node| **node != observer)
            .cloned()
            .collect::<Vec<_>>();
        let observe = |node: &ClusterNodeId| -> Result<AuthenticatedNativeJointBridgeStepFrame> {
            let budget_ms = remaining(token)?.as_millis().clamp(1, 5000) as u64;
            let request = NativeJointHandoffStepRequestV1::from_decision(
                data,
                NativeJointHandoffStepOperationV1::BridgePhaseObserve {
                    decision_sha256: data.checksum_sha256().into(),
                    kind,
                },
                business,
                budget_ms,
            )?;
            let frame = client.request_native_joint_bridge_phase_observation(
                node, data, kind, &request, own, token,
            )?;
            frame.require_live()?;
            Ok(frame)
        };
        for (node, frame) in observe_every_member(&nodes, &observe) {
            let node = &node;
            let frame = frame?;
            // The counted member is the peer this exchange authenticated.
            let actual = frame.actual_peer().clone();
            if &actual != node || frames.contains_key(&actual) {
                return Err(invalid(
                    "remote preparation window peer differs or is counted twice",
                ));
            }
            identities.insert(
                actual.clone(),
                require_phase_frame(kind, data, &observer, &actual, &frame)?,
            );
            frames.insert(actual, frame);
        }
        let window = Self {
            kind,
            observer,
            member_observer,
            frames,
            identities,
        };
        window.require_complete(data)?;
        Ok(window)
    }
    /// EVERY other NEW member exactly once, each frame unchanged and live.
    #[track_caller]
    pub(super) fn require_complete(&self, data: &NativeJointDecisionData) -> Result<()> {
        if data.new_members().contains_key(&self.observer) != self.member_observer
            || self.frames.keys().ne(data
                .new_members()
                .keys()
                .filter(|node| **node != self.observer))
            || self.identities.keys().ne(self.frames.keys())
        {
            return Err(invalid(
                "remote preparation window omits or replaces a NEW member",
            ));
        }
        for (node, frame) in &self.frames {
            frame.require_live()?;
            if require_phase_frame(self.kind, data, &self.observer, node, frame)?
                != self.identities[node]
            {
                return Err(invalid("remote preparation window member identity changed"));
            }
        }
        Ok(())
    }
    #[track_caller]
    pub(super) fn require_live(&self) -> Result<()> {
        for frame in self.frames.values() {
            frame.require_live()?;
        }
        Ok(())
    }
    pub(super) fn contexts(&self) -> std::collections::BTreeSet<Uuid> {
        self.frames
            .values()
            .map(|frame| frame.metadata_anchor().2)
            .collect()
    }
}

/// One observation exchange per member, each on its own scoped thread, so
/// that every frame of one window is observed at about the same time. A
/// window is complete only while EVERY frame is still inside its own bounded
/// window; collected one after the other, the first frame's window had to
/// outlast all later exchanges, which on a slow host it never did, and the
/// same window was observed again on every repetition. Each exchange is the
/// unchanged authenticated request of the sequential collection; the
/// results keep the order of `nodes`, and the caller checks them in that
/// order as before. A thread that cannot be started runs its exchange on the
/// calling thread; a worker that panicked is an error of its member.
fn observe_every_member(
    nodes: &[ClusterNodeId],
    observe: &(dyn Fn(&ClusterNodeId) -> Result<AuthenticatedNativeJointBridgeStepFrame> + Sync),
) -> Vec<(
    ClusterNodeId,
    Result<AuthenticatedNativeJointBridgeStepFrame>,
)> {
    if nodes.len() < 2 {
        return nodes
            .iter()
            .map(|node| (node.clone(), observe(node)))
            .collect();
    }
    std::thread::scope(|scope| {
        let started = nodes
            .iter()
            .map(|node| {
                let worker = std::thread::Builder::new()
                    .name("joint-window-observation".into())
                    .spawn_scoped(scope, move || observe(node));
                (node, worker)
            })
            .collect::<Vec<_>>();
        started
            .into_iter()
            .map(|(node, worker)| {
                let answer = match worker {
                    Ok(worker) => worker.join().unwrap_or_else(|_| {
                        Err(invalid(&format!(
                            "the window observation worker of member {node} panicked"
                        )))
                    }),
                    Err(_) => observe(node),
                };
                (node.clone(), answer)
            })
            .collect()
    })
}

/// The budget to ask the metadata leader for, so that the observation lives
/// until `token`'s deadline. The leader serves an acquisition for its budget
/// less the reply margin (`SERVED_REPLY_MARGIN`), and the observation's
/// context ends there; asked for the bare remainder, every observation of a
/// phase ended half a second before the phase, and a member observed inside
/// another member's window (whose own step also ends half a second before
/// that window) a whole second before. The margin is added back here, so
/// the observation ends with `token` and never later: the acquisition is a
/// child of `token`, and the protocol's five-second cap still applies.
pub(crate) fn observation_budget(token: &crate::CancellationToken) -> Result<Duration> {
    Ok(
        (remaining(token)? + crate::distribution_transport::SERVED_REPLY_MARGIN)
            .min(Duration::from_secs(5)),
    )
}

pub(super) fn remaining(token: &crate::CancellationToken) -> Result<Duration> {
    token.check()?;
    token
        .deadline()
        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
        .filter(|left| !left.is_zero())
        .ok_or(crate::BicDbError::QueryTimedOut)
}

/// Backing of a remote-published activation admission. Private fields and no
/// constructor outside this file: it exists only inside
/// `NativeJointPreparationCustody::publish_remote_bridge`.
pub(super) struct RemoteActivationBacking {
    /// This member's own fresh pre-quorum phase observation and the writer
    /// registration it proved current under its own service lock.
    local_phase: CommittedNativeJointDecisionObservation,
    local_writer: crate::db::NativeJointPhaseOwner,
    window: RemotePreparationWindow,
    /// Set once this member's journal commit point is passed (see
    /// `pass_commit_point`); every frame was live at that point.
    past_commit_point: std::sync::atomic::AtomicBool,
}
impl RemoteActivationBacking {
    /// The commit point of this member's own durable bridge: every other
    /// member's frame must be live here, immediately before the journal
    /// commit (or, when an earlier attempt's exact bridge is recovered, at
    /// the recovery decision). After it, the frames no longer gate anything:
    /// what they attested decided only whether to write, the write is
    /// historical, and the publication stays unconfirmed until a SECOND
    /// window, observed after the write, reports every other member
    /// unchanged. Checking the first window again after the post-write
    /// proofs only refused a durable bridge on every repetition once those
    /// proofs took longer than the frames lasted.
    pub(super) fn pass_commit_point(&self) -> Result<()> {
        self.window.require_live()?;
        self.past_commit_point
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    pub(super) fn require_live(&self, local: &NativeJointPreparedServingObservation) -> Result<()> {
        self.local_phase.require_metadata_current()?;
        local
            .actual
            .require_phase_writer_lifetime(&self.local_writer)?;
        if let Some(custody) = &local.custody {
            custody.require_original_caller()?;
        }
        if &self.window.observer != local.local_node_id()
            || self.window.kind != NativeJointBridgePhaseKindV1::Preparation
        {
            return Err(invalid(
                "remote preparation window was observed by another member/phase",
            ));
        }
        if self
            .past_commit_point
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        self.window.require_live()
    }
    /// The same complete local check a fresh in-process phase performs for
    /// its local member, under the caller's own service guard.
    pub(super) fn require_local_with_service(
        &self,
        local: &NativeJointPreparedServingObservation,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        let writer = local.require_fresh_phase_current(service, &self.local_phase)?;
        if !writer.same_actual(&self.local_writer) {
            return Err(invalid(
                "remote publication local writer registration/generation changed",
            ));
        }
        Ok(())
    }
    pub(super) fn require_local_publication_writer(
        &self,
        actual: &crate::db::NativeJointPhaseOwner,
    ) -> Result<()> {
        if !self.local_writer.same_actual(actual) {
            return Err(invalid(
                "publication final scope actual writer registration changed",
            ));
        }
        Ok(())
    }
    /// Every pre-quorum metadata anchor the post-quorum observation must
    /// independently follow: the local phase and every other member's.
    pub(super) fn anchors(
        &self,
        data: &NativeJointDecisionData,
        local: &ClusterNodeId,
    ) -> Result<Vec<(u64, u64, Uuid)>> {
        if &self.window.observer != local
            || self.window.frames.len() + 1 != data.new_members().len()
        {
            return Err(invalid(
                "remote preparation window omits or replaces a NEW member",
            ));
        }
        let mut anchors = vec![self.local_phase.metadata_anchor()];
        anchors.extend(
            self.window
                .frames
                .values()
                .map(AuthenticatedNativeJointBridgeStepFrame::metadata_anchor),
        );
        Ok(anchors)
    }
    /// The final foreign boundary before the local journal: complete member
    /// set, every frame unchanged and still inside its own window.
    pub(super) fn require_external_current(
        &self,
        data: &NativeJointDecisionData,
        local: &ClusterNodeId,
    ) -> Result<()> {
        if &self.window.observer != local {
            return Err(invalid(
                "remote preparation window was observed by another member",
            ));
        }
        self.window.require_complete(data)
    }
    /// The same durable audit the in-process publisher writes: every NEW
    /// member's preparation identity and the anchor of ITS phase observation.
    pub(super) fn votes(
        &self,
        local: &NativeJointPreparedServingObservation,
    ) -> BTreeMap<ClusterNodeId, NativeJointPreparedServingVoteData> {
        let actual = local.actual.prepared();
        let (term, index, context) = self.local_phase.metadata_anchor();
        let mut votes = BTreeMap::new();
        votes.insert(
            local.local_node_id().clone(),
            NativeJointPreparedServingVoteData {
                preparation_sha256: actual.checksum_sha256().into(),
                native_database_incarnation: actual.native_database_incarnation().into(),
                actual_local_visibility_sequence: actual.actual_local_visibility_sequence(),
                metadata_context_id: context,
                metadata_term: term,
                metadata_read_index: index,
            },
        );
        for (node, frame) in &self.window.frames {
            let identity = &self.window.identities[node];
            let (term, index, context) = frame.metadata_anchor();
            votes.insert(
                node.clone(),
                NativeJointPreparedServingVoteData {
                    preparation_sha256: identity.preparation_sha256.clone(),
                    native_database_incarnation: identity.native_database_incarnation.clone(),
                    actual_local_visibility_sequence: identity.local_visibility_sequence,
                    metadata_context_id: context,
                    metadata_term: term,
                    metadata_read_index: index,
                },
            );
        }
        votes
    }
}

/// This member's durable native bridge receipt together with the identity
/// every other NEW member reported BEFORE the write. Not yet a confirmed
/// publication: `confirm_second_window` must still succeed.
pub(crate) struct NativeRemoteBridgePublished {
    receipt: Arc<crate::db::NativeJointServingBridgeReceipt>,
    identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
    first_contexts: std::collections::BTreeSet<Uuid>,
    post_quorum: (u64, u64, Uuid),
}
impl NativeRemoteBridgePublished {
    pub(crate) fn receipt(&self) -> &Arc<crate::db::NativeJointServingBridgeReceipt> {
        &self.receipt
    }
    pub(crate) fn post_quorum_anchor(&self) -> (u64, u64, Uuid) {
        self.post_quorum
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeBridgeSecondWindowFault {
    /// One member's second-window identity reports another registration.
    ChangedIdentity,
    /// The second window's first reply never arrives.
    LostReply,
}
#[cfg(test)]
static TEST_SECOND_WINDOW_FAULTS: Mutex<
    Vec<(ClusterNodeId, String, NativeBridgeSecondWindowFault)>,
> = Mutex::new(Vec::new());
/// One-shot fault for the next second window of `member` under `decision`.
#[cfg(test)]
pub(crate) fn test_arm_bridge_second_window_fault(
    member: &ClusterNodeId,
    decision_sha256: &str,
    fault: NativeBridgeSecondWindowFault,
) {
    TEST_SECOND_WINDOW_FAULTS
        .lock()
        .push((member.clone(), decision_sha256.to_owned(), fault));
}
#[cfg(test)]
pub(super) fn test_take_second_window_fault(
    member: &ClusterNodeId,
    decision_sha256: &str,
) -> Option<NativeBridgeSecondWindowFault> {
    let mut faults = TEST_SECOND_WINDOW_FAULTS.lock();
    let at = faults
        .iter()
        .position(|(node, decision, _)| node == member && decision == decision_sha256)?;
    Some(faults.remove(at).2)
}

impl NativeJointPreparationCustody {
    fn require_remote_actor(&self, client: &TcpClusterRelocationTransport) -> Result<()> {
        let actor = client.local_metadata_read_participant();
        if actor.node_id() != self.local_node_id()
            || self
                .data()
                .new_members()
                .get(actor.node_id())
                .is_none_or(|member| member.incarnation != actor.member_incarnation())
        {
            return Err(invalid(
                "remote bridge publication changed actual recipient/incarnation",
            ));
        }
        Ok(())
    }
    /// This member's own fresh pre-quorum phase observation for a remote
    /// publication: its own metadata observation, its attached MStore point
    /// (both before any engine guard), then the committed decision observed
    /// and the member proved current under its own service lock.
    fn observe_publication_local_phase(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        data: &NativeJointDecisionData,
        phase: &crate::CancellationToken,
    ) -> Result<(
        CommittedNativeJointDecisionObservation,
        crate::db::NativeJointPhaseOwner,
    )> {
        let part = std::cell::Cell::new(Instant::now());
        let traced = |what: &str| {
            crate::distribution_native_host::range_move_trace(part.replace(Instant::now()), || {
                format!("member publication local phase: {what}")
            })
        };
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(phase)?,
            phase,
        )?;
        traced("own metadata observation");
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            phase,
        )?;
        traced("metadata point captured");
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote publication phase actual service busy"))?;
        let committed = self.actual.observe_fresh_committed_with_point(
            &service,
            metadata,
            business,
            data.old_range().id,
            point,
        )?;
        traced("committed decision observed");
        let writer = self
            .actual
            .require_current_under_preparation_custody(&service, &committed, self)?;
        traced("member proved current");
        if committed.data() != data {
            return Err(invalid(
                "remote publication phase changed protected global decision",
            ));
        }
        Ok((committed, writer))
    }
    /// This member's own complete publication of its native serving bridge.
    ///
    /// The member takes its own fresh pre-quorum phase observation under its
    /// service lock, collects one authenticated PREPARATION observation from
    /// EVERY other NEW member over its own pinned transport (`collector` is
    /// its fresh committed metadata those exchanges are bound to), releases
    /// only its own read pin, takes a distinct post-quorum observation and
    /// then runs the unchanged native publisher. An existing exact bridge is
    /// reconciled by that same publisher, never rewritten.
    ///
    /// An error leaves an unknown durable effect; the caller keeps its
    /// uncertain marker and calls again under a distinct fresh phase.
    pub(crate) fn publish_remote_bridge(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        collector: &crate::distribution_consensus::HostMetadataReadObservation,
        collector_business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeRemoteBridgePublished> {
        // A fresh request may not detach from the ORIGINAL session ancestry.
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        self.require_remote_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let part = std::cell::Cell::new(Instant::now());
        let traced = |what: &str, phase: &crate::CancellationToken| {
            crate::distribution_native_host::range_move_trace(part.replace(Instant::now()), || {
                format!(
                    "member publication part: {what}; phase left {} ms",
                    phase.deadline().map_or(0, |deadline| deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis())
                )
            })
        };
        // (1) Own pre-quorum phase observation and (2) one observation of
        // every other NEW member, taken at the same time. Both are the
        // pre-quorum observations of this publication: the post-quorum one
        // below must follow every one of them (`anchors`), and no order is
        // required among them, as the in-process phase observes all its
        // members in one round. Taken one after the other, the window
        // started only after this member's own observation, which on a slow
        // host took up to 2 s, and its frames then ran out before the
        // durable write. The window's exchanges are bound to `collector`,
        // not to the local observation; the phase is bounded by the local
        // observation once both are in.
        let (local, window) = std::thread::scope(|scope| {
            let observe_local =
                || self.observe_publication_local_phase(client, metadata_leader, data, &phase);
            let local = std::thread::Builder::new()
                .name("joint-publication-local-phase".into())
                .spawn_scoped(scope, observe_local);
            let window = RemotePreparationWindow::collect(
                NativeJointBridgePhaseKindV1::Preparation,
                data,
                client,
                collector,
                collector_business,
                &phase,
            );
            let local = match local {
                Ok(worker) => worker.join().unwrap_or_else(|_| {
                    Err(invalid("the publication local phase worker panicked"))
                }),
                Err(_) => observe_local(),
            };
            (local, window)
        });
        let (local_phase, local_writer) = local?;
        let window = window?;
        let phase = phase.child_with_deadline(local_phase.deadline());
        phase.check()?;
        window.require_live()?;
        traced("window of every other member", &phase);
        // (3) Only THIS member's read pin. It is a slot of this database's
        // own mutation gate; no other member's pin gates this journal. It is
        // released only after a complete window observed every other NEW
        // member prepared. The release validates this member completely
        // before and after. A pin is monotone: a repeated publication whose
        // earlier attempt already released it skips the release and its two
        // complete checks, as the in-process phase does. Repeated anyway, it
        // spent about 0.7 s of a slow host's window, after the frames had
        // arrived, only to take nothing.
        // The released fact is read under the service guard, the same guard
        // the release takes; read without it, a concurrent holder of the pin
        // slot made this publication refuse ("pin transition busy").
        {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote publication pin release service busy"))?;
            if !self.actual.read_pin_already_released().unwrap_or(false) {
                self.actual.release_read_pin_under_preparation_custody(
                    &service,
                    &local_phase,
                    self,
                )?;
                traced("own read pin released", &phase);
            }
        }
        window.require_live()?;
        // (4) Distinct post-quorum observation, no guard across the I/O.
        let business = Uuid::now_v7();
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(&phase)?,
            &phase,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            &phase,
        )?;
        let fresh = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("remote publication post-quorum service busy"))?;
            self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?
        };
        let cancellation = phase.child_with_deadline(fresh.deadline());
        let identities = window.identities.clone();
        let first_contexts = window.contexts();
        let post_quorum = fresh.metadata_anchor();
        let admission = NativeJointActivationAdmission {
            backing: ActivationBacking::Remote(RemoteActivationBacking {
                local_phase,
                local_writer,
                window,
                past_commit_point: std::sync::atomic::AtomicBool::new(false),
            }),
            publication_claimed: std::sync::atomic::AtomicBool::new(false),
            local: Arc::new(NativeJointPreparedServingObservation {
                actual: self.actual.clone(),
                service: self.service.clone(),
                fresh: None,
                custody: Some(self.clone()),
            }),
            fresh,
            cancellation,
        };
        traced("post-quorum observation", &phase);
        admission.require_live()?;
        admission.claim_publication_attempt()?;
        // (5) The unchanged scoped native publisher: final foreign boundary,
        // local journal scope, reconcile-or-write, post-write proof.
        let receipt = admission.publish_local_native_bridge(parent);
        traced(
            &format!("native publication ok={}", receipt.is_ok()),
            &phase,
        );
        let receipt = receipt?;
        if receipt.data() != data || receipt.local_node_id() != self.local_node_id() {
            return Err(invalid(
                "remote bridge publication changed decision or local native history",
            ));
        }
        Ok(NativeRemoteBridgePublished {
            receipt: Arc::new(receipt),
            identities,
            first_contexts,
            post_quorum,
        })
    }
    /// The AFTER half: a second bounded window over every other NEW member,
    /// bound to a metadata observation taken after the durable write. Every
    /// member must report exactly the identity it reported before the write,
    /// under an observation that is not older than the post-quorum one and
    /// is distinct from every first-window observation. Any difference,
    /// refusal or timeout leaves the publication unconfirmed.
    pub(crate) fn confirm_remote_bridge_second_window(
        self: &Arc<Self>,
        published: &NativeRemoteBridgePublished,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        self.require_remote_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        #[cfg(test)]
        let fault = test_take_second_window_fault(self.local_node_id(), data.checksum_sha256());
        #[cfg(test)]
        if fault == Some(NativeBridgeSecondWindowFault::LostReply) {
            return Err(crate::BicDbError::QueryTimedOut);
        }
        let business = Uuid::now_v7();
        let own = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            observation_budget(&phase)?,
            &phase,
        )?;
        if own.term() < published.post_quorum.0
            || own.read_index() < published.post_quorum.1
            || own.context_id() == published.post_quorum.2
        {
            return Err(invalid(
                "remote publication second window is not after the post-quorum observation",
            ));
        }
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Preparation,
            data,
            client,
            &own,
            business,
            &phase,
        )?;
        #[allow(unused_mut)]
        let mut identities = window.identities.clone();
        #[cfg(test)]
        if fault == Some(NativeBridgeSecondWindowFault::ChangedIdentity) {
            let first = identities.values_mut().next().unwrap();
            first.writer_identity_sha256 = "f".repeat(64);
        }
        if identities != published.identities {
            return Err(invalid(
                "remote publication second window: a NEW member's identity changed across the durable write",
            ));
        }
        for frame in window.frames.values() {
            let (term, index, context) = frame.metadata_anchor();
            if term < published.post_quorum.0
                || index < published.post_quorum.1
                || context == published.post_quorum.2
                || published.first_contexts.contains(&context)
            {
                return Err(invalid(
                    "remote publication second window reused or preceded first-window metadata",
                ));
            }
        }
        // This member's own durable bridge, re-read under its own lock.
        self.require_published_bridge_current(&published.receipt)?;
        window.require_live()?;
        own.require_live_request(business)?;
        phase.check()
    }
    /// Re-read of this member's own protected bridge under its service lock.
    pub(crate) fn require_published_bridge_current(
        &self,
        receipt: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        self.require_original_caller()?;
        if receipt.data() != self.data() || receipt.local_node_id() != self.local_node_id() {
            return Err(invalid(
                "retained bridge receipt belongs to another decision/member",
            ));
        }
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("retained bridge receipt service busy"))?;
        service.require_joint_bridge_with_writer_domain(receipt, self.actual.writer_domain())
    }
}
