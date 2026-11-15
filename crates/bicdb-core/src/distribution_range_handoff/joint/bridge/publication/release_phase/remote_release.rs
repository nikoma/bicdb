//! One NEW member's own release continuation when the other NEW members live
//! in other processes. Same construction rule as the remote publication and
//! activation: the local member is its actual original continuation entry
//! (service, protected bridge, catalogue activation, original writer and the
//! real freeze owner), re-proved under its own service lock; every other
//! member is a frame the pinned transport minted after authenticating that
//! member in THIS member's own exchange, attesting that member's own release
//! observation under ITS lock.
//!
//! Nothing here is built from decoded DATA and nothing grants a release by
//! construction: the unchanged owner release runs under an admission whose
//! foreign half is the bounded window.
use super::super::super::remote_publication::{
    NativeJointBridgeMemberIdentity, RemotePreparationWindow, REMOTE_PHASE_BUDGET,
};
use super::*;
use crate::distribution_range_handoff::NativeJointBridgePhaseKindV1;
use crate::distribution_transport::TcpClusterRelocationTransport;
mod remote_policy;
pub(super) use remote_policy::RemotePolicyBacking;

/// The real freeze owner this member retained for its own release.
pub(crate) enum NativeJointMemberReleaseOwner {
    /// A surviving original member: the runtime that owns its OLD freeze.
    Survivor(Arc<NativeJointHandoffRuntime>),
    /// The added member: the original Registered receiver's release owner.
    Registered(crate::distribution_native_host::NativeRegisteredRemoteJointReleaseOwner),
}

/// This member's own entry of the release continuation. It holds no
/// Prepared/Frozen Arc itself; the real owner keeps its own lease. Retention
/// grants neither a release nor a quorum membership.
pub(crate) struct NativeJointMemberRelease {
    continuation: Arc<NativeJointReleaseContinuation>,
    local: ClusterNodeId,
    facts: NativeJointPreparationRetainedFacts,
    activation_sha256: String,
}

/// One member-local fresh RELEASE observation as historical DATA.
#[derive(Clone, Debug)]
pub(crate) struct NativeJointReleasePhaseFacts {
    pub(crate) retained: NativeJointPreparationRetainedFacts,
    pub(crate) metadata_anchor: (u64, u64, Uuid),
    pub(crate) deadline: Instant,
    /// This member's own durable bridge receipt and its recorded anchor.
    pub(crate) bridge: (String, (u64, u64, Uuid)),
    /// This member's own durable catalogue activation.
    pub(crate) activation_sha256: String,
    /// The armed same-open generation; `None` while Frozen or unarmed.
    pub(crate) released_generation: Option<Uuid>,
    /// WRITER AUDIT observation of a released member only: this member's own
    /// metadata read audit of the observation behind these facts.
    pub(crate) writer_audit: Option<serde_json::Value>,
}

impl NativeJointPreparationCustody {
    /// Build this member's own continuation entry from its retained
    /// preparation, its own durable bridge and its own catalogue activation,
    /// exactly as the in-process continuation builds one member's entry.
    /// The preparation custody is NOT consumed: it stays with the caller
    /// until the member's own release needs its real freeze uniquely owned.
    pub(crate) fn retain_member_release(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        activation: &Arc<NativeJointCatalogueActivationReceipt>,
        owner: NativeJointMemberReleaseOwner,
        session: &crate::CancellationToken,
    ) -> Result<Arc<NativeJointMemberRelease>> {
        self.require_original_caller()?;
        session.check()?;
        let data = self.data();
        let local = self.local_node_id().clone();
        let (writer, writer_root) = self.actual.original_phase_owner_for_continuation()?;
        writer.require_store(&writer_root, activation.store_open_id())?;
        if bridge.data() != data
            || bridge.local_node_id() != &local
            || !data.new_members().contains_key(&local)
            || activation.native_bridge_sha256() != bridge.checksum_sha256()
            || activation.native().native_database_incarnation()
                != bridge.native_database_incarnation()
            || activation.native().data() != data
            || activation.native().local_node_id() != &local
        {
            return Err(invalid(
                "member release continuation substituted actual activation/member/open",
            ));
        }
        let additions: Vec<_> = data
            .new_members()
            .keys()
            .filter(|node| !data.old_members().contains_key(*node))
            .collect();
        if additions.len() != 1 {
            return Err(invalid(
                "member release needs the actual single added image member",
            ));
        }
        let owner = match owner {
            NativeJointMemberReleaseOwner::Survivor(runtime) if additions[0] != &local => {
                runtime.require_same_service(&self.service)?;
                ReleaseOwner::Survivor(runtime)
            }
            NativeJointMemberReleaseOwner::Registered(image) if additions[0] == &local => {
                image.require_same_service(&self.service)?;
                ReleaseOwner::Registered(parking_lot::Mutex::new(image))
            }
            _ => {
                return Err(invalid(
                    "member release owner kind differs from this member's actual role",
                ))
            }
        };
        let activation_sha256 = activation.activation_sha256()?;
        // The actual protected catalogue fact, read under this member's lock.
        if self.durable_activation_sha256(bridge)?.as_deref() != Some(activation_sha256.as_str()) {
            return Err(invalid(
                "member release continuation activation is absent or changed in the store",
            ));
        }
        let facts = self.retained_facts()?;
        let mut members = BTreeMap::new();
        members.insert(
            local.clone(),
            ReleaseMember {
                service: self.service.clone(),
                native: bridge.clone(),
                activation: activation.clone(),
                writer,
                writer_domain: self.actual.writer_domain().clone(),
                writer_root,
                owner,
            },
        );
        let result = Arc::new(NativeJointMemberRelease {
            // Only the local entry: an in-process release phase can never be
            // begun over this continuation (it requires every final member).
            continuation: Arc::new(NativeJointReleaseContinuation {
                caller: session.child(),
                original_cancel: session.child(),
                preparation_lifetimes: vec![self.producer_memory_lifetime()],
                data: data.clone(),
                members,
            }),
            local,
            facts,
            activation_sha256,
        });
        result.continuation.require_original()?;
        Ok(result)
    }
}

/// The backing of a remote release admission. Private fields and no
/// constructor outside this file.
pub(super) struct RemoteReleaseBacking {
    release: Arc<NativeJointMemberRelease>,
    /// This member's own fresh pre-quorum RELEASE observation.
    local_phase: ActualServiceNativeJointReleaseEvidence,
    window: RemotePreparationWindow,
}
impl RemoteReleaseBacking {
    pub(super) fn member(&self) -> &ReleaseMember {
        self.release.member()
    }
    pub(super) fn local_phase(&self) -> &ActualServiceNativeJointReleaseEvidence {
        &self.local_phase
    }
    pub(super) fn require_live(
        &self,
        local: &ClusterNodeId,
        post: &ActualServiceNativeJointReleaseEvidence,
    ) -> Result<()> {
        self.release.continuation.require_original()?;
        self.local_phase.require_live()?;
        require_release_window(&self.window, local, self.release.data())?;
        self.window.require_live()?;
        require_post_all(
            post.committed().metadata_anchor(),
            self.local_phase.committed().metadata_anchor(),
            &self.window,
        )
    }
    /// The final foreign boundary before the local owner release: complete
    /// member set, every frame unchanged and still inside its own window.
    pub(super) fn require_external_current(&self, local: &ClusterNodeId) -> Result<()> {
        require_release_window(&self.window, local, self.release.data())?;
        self.window.require_complete(self.release.data())
    }
}

fn require_release_window(
    window: &RemotePreparationWindow,
    local: &ClusterNodeId,
    data: &NativeJointDecisionData,
) -> Result<()> {
    if &window.observer != local
        || window.kind != NativeJointBridgePhaseKindV1::Release
        || window.frames.len() + 1 != data.new_members().len()
    {
        return Err(invalid(
            "remote release window was observed by another member/phase or omits a NEW member",
        ));
    }
    Ok(())
}
/// The in-process rule: the post observation independently follows EVERY
/// member's phase observation, the local one and every counted frame.
fn require_post_all(
    post: (u64, u64, Uuid),
    local: (u64, u64, Uuid),
    window: &RemotePreparationWindow,
) -> Result<()> {
    let mut anchors = vec![local];
    anchors.extend(window.frames.values().map(|frame| frame.metadata_anchor()));
    for old in anchors {
        if post.0 < old.0 || post.1 < old.1 || post.2 == old.2 {
            return Err(invalid(
                "remote release needs independently post-all-member MQ",
            ));
        }
    }
    Ok(())
}
/// Every other member's reported RELEASE identity must carry its durable
/// activation; optionally every one must also be released and armed.
fn require_window_open(window: &RemotePreparationWindow, all_released: bool) -> Result<()> {
    for identity in window.identities.values() {
        let (_, generation) = identity
            .release
            .as_ref()
            .ok_or_else(|| invalid("remote release frame attests no durable activation"))?;
        if identity.bridge.is_none() {
            return Err(invalid("remote release frame attests no durable bridge"));
        }
        if all_released && generation.is_none() {
            return Err(invalid(
                "remote release window: a NEW member still has an actual Frozen or unarmed owner",
            ));
        }
    }
    Ok(())
}

/// Proof for retiring obsolete OLD memory custody only: this member's own
/// fresh release observation and one live authenticated RELEASE observation
/// of every other NEW member, all attesting a durable catalogue activation.
/// Not a Frozen/Serving grant and not source data cleanup.
pub(crate) struct NativeJointRemoteOldRetirementProof {
    release: Arc<NativeJointMemberRelease>,
    local_phase: ActualServiceNativeJointReleaseEvidence,
    window: RemotePreparationWindow,
    cancellation: crate::CancellationToken,
}
impl NativeJointRemoteOldRetirementProof {
    fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.release.continuation.require_original()?;
        self.local_phase.require_live()?;
        require_release_window(&self.window, &self.release.local, self.release.data())?;
        self.window.require_live()
    }
    /// Every final member is actually Open: the local one re-read under its
    /// own lock, every other one still inside the window of its own re-read.
    fn require_every_member_open(&self) -> Result<()> {
        self.require_live()?;
        self.release
            .continuation
            .with_owner(&self.release.local, |service, owner| {
                let actual = service
                    .try_lock()
                    .ok_or_else(|| invalid("remote retirement current service busy"))?;
                self.local_phase.require_current(&actual, owner)
            })?;
        self.window.require_complete(self.release.data())?;
        require_window_open(&self.window, false)?;
        self.require_live()
    }
    /// Same rule as the in-process phase: the remote OLD certificate is
    /// revalidated for the post-open point and must be this decision's.
    pub(crate) fn require_remote_old_owner_retirement(
        &self,
        old: &Arc<NativeRemoteOldCutCertificate>,
    ) -> Result<()> {
        self.require_live()?;
        let data = self.release.data();
        let facts = old.require_current_after_joint_open()?;
        if data.plan_sha256()? != old.plan().checksum_sha256()?
            || data.old_materialized_facts() != facts
        {
            return Err(invalid(
                "joint retirement changed actual remote OLD obligations",
            ));
        }
        self.require_every_member_open()
    }
    /// The added member held no OLD custody. What it can still hold of the
    /// decision round is its own participation custody of the remote
    /// decision (the retained materialization and decision attempt). It is
    /// obsolete under the same proof: this member is the added member of
    /// exactly this decision's relocation and session, and every final
    /// member is durably open.
    pub(crate) fn require_added_member_decision_custody_retirement(
        &self,
        local: &ClusterNodeId,
        relocation: crate::distribution::RelocationId,
        session: Uuid,
        nonce: Uuid,
    ) -> Result<()> {
        self.require_live()?;
        let data = self.release.data();
        let promise = data.promise();
        if local != &self.release.local
            || !data.new_members().contains_key(local)
            || data.old_members().contains_key(local)
            || promise.relocation_id != relocation
            || promise.session_id != session
            || promise.nonce != nonce
        {
            return Err(invalid(
                "joint retirement is not the added member's own decision participation",
            ));
        }
        self.require_every_member_open()
    }
    /// Same rule as the in-process phase for a survivor's own observation.
    pub(crate) fn require_actual_old_owner_retirement(
        &self,
        old: &Arc<NativeJointOldObservation>,
    ) -> Result<()> {
        self.require_live()?;
        let data = self.release.data();
        old.require_current_after_joint_open()?;
        if old.node_id() != &self.release.local
            || !data.new_members().contains_key(old.node_id())
            || data.plan_sha256()? != old.plan().checksum_sha256()?
            || data.old_materialized_fact(old.node_id()) != Some(&old.materialized_facts())
        {
            return Err(invalid("joint retirement must retain retired-source freeze and exact original survivor facts"));
        }
        self.require_every_member_open()
    }
}

/// What every other NEW member reported BEFORE this member's release.
pub(crate) struct NativeRemoteBridgeReleased {
    identities: BTreeMap<ClusterNodeId, NativeJointBridgeMemberIdentity>,
    first_contexts: std::collections::BTreeSet<Uuid>,
    post_quorum: (u64, u64, Uuid),
}

impl NativeJointMemberRelease {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.continuation.data
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.local
    }
    /// Historical identity of the preparation this entry was built from.
    pub(crate) fn retained_facts(&self) -> &NativeJointPreparationRetainedFacts {
        &self.facts
    }
    pub(crate) fn bridge(&self) -> &Arc<crate::db::NativeJointServingBridgeReceipt> {
        &self.member().native
    }
    pub(crate) fn activation_sha256(&self) -> &str {
        &self.activation_sha256
    }
    fn member(&self) -> &ReleaseMember {
        &self.continuation.members[&self.local]
    }
    /// A fresh request may not detach from the ORIGINAL session ancestry.
    fn bound(&self, parent: &crate::CancellationToken) -> crate::CancellationToken {
        parent.child_linked_to(&self.continuation.caller)
    }
    fn require_actor(&self, client: &TcpClusterRelocationTransport) -> Result<()> {
        let actor = client.local_metadata_read_participant();
        if actor.node_id() != &self.local
            || self
                .data()
                .new_members()
                .get(actor.node_id())
                .is_none_or(|member| member.incarnation != actor.member_incarnation())
        {
            return Err(invalid(
                "remote release changed actual recipient/incarnation",
            ));
        }
        Ok(())
    }
    /// Exactly what the in-process release round does for this member
    /// (`acquire_actual_member`), run by the member under metadata IT holds.
    fn observe(
        &self,
        host: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        token: &crate::CancellationToken,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        self.continuation.require_original()?;
        let member = self.member();
        if host.requester_node_id() != &self.local
            || self
                .data()
                .new_members()
                .get(&self.local)
                .is_none_or(|actual| actual.incarnation != host.requester_incarnation())
        {
            return Err(invalid(
                "fresh release phase changed actual recipient/incarnation",
            ));
        }
        // No owner/service/database/journal lock is held during MStore I/O.
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &member.service,
            &host,
            business,
            token,
        )?;
        self.continuation.with_owner(&self.local, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("remote release phase service busy"))?;
            let evidence = actual.observe_joint_release_member_with_domain_and_point(
                &member.writer_domain,
                host,
                business,
                member.native.clone(),
                owner,
                metadata_point,
            )?;
            evidence.require_original_writer_identity(&member.writer)?;
            Ok(evidence)
        })
    }
    fn acquire_and_observe(
        &self,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        token: &crate::CancellationToken,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        let business = Uuid::now_v7();
        let host = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining(token)?,
            token,
        )?;
        self.observe(host, business, token)
    }
    fn facts_of(
        &self,
        evidence: &ActualServiceNativeJointReleaseEvidence,
    ) -> Result<NativeJointReleasePhaseFacts> {
        evidence.require_live()?;
        evidence.committed().require_current_metadata_only()?;
        let member = self.member();
        let activation_sha256 = evidence.activation().activation_sha256()?;
        // The durable catalogue fact, re-read from this member's own store.
        let durable = {
            let service = member
                .service
                .try_lock()
                .ok_or_else(|| invalid("release phase durable activation service busy"))?;
            service.durable_native_joint_activation_sha256(member.native.clone())?
        };
        if activation_sha256 != self.activation_sha256
            || durable.as_deref() != Some(activation_sha256.as_str())
            || evidence.native().checksum_sha256() != member.native.checksum_sha256()
            || evidence.committed().data() != self.data()
        {
            return Err(invalid(
                "fresh release phase changed this member's durable activation/bridge/decision",
            ));
        }
        let mut retained = self.facts.clone();
        // The registration this observation itself just proved current.
        retained.writer_identity_sha256 = member.writer.identity_sha256()?;
        Ok(NativeJointReleasePhaseFacts {
            retained,
            metadata_anchor: evidence.committed().metadata_anchor(),
            deadline: evidence.committed().deadline(),
            bridge: (
                member.native.checksum_sha256().to_owned(),
                member.native.metadata_anchor(),
            ),
            activation_sha256,
            // Frozen, or dropped but not armed, is NOT released.
            released_generation: evidence.require_released_open().ok(),
            writer_audit: None,
        })
    }
    /// The member-local half of one fresh RELEASE phase. Read-only: nothing
    /// is released, armed, retired or granted; the result is historical DATA.
    pub(crate) fn observe_fresh_release_phase(
        &self,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointReleasePhaseFacts> {
        let request = self.bound(parent);
        request.check()?;
        let evidence = self.observe(metadata, business, &request)?;
        let facts = self.facts_of(&evidence)?;
        request.check()?;
        Ok(facts)
    }
    /// The same read-only RELEASE observation, additionally carrying this
    /// member's own metadata read audit of that very observation when the
    /// member is released and armed: exactly the value the in-process writer
    /// policy stores for this member. Historical DATA; grants nothing.
    pub(crate) fn observe_fresh_writer_audit_phase(
        &self,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointReleasePhaseFacts> {
        let request = self.bound(parent);
        request.check()?;
        let evidence = self.observe(metadata, business, &request)?;
        let mut facts = self.facts_of(&evidence)?;
        if facts.released_generation.is_some() {
            facts.writer_audit = Some(evidence.committed().writer_domain_metadata_audit()?);
        }
        request.check()?;
        Ok(facts)
    }
    /// This member's own proof that EVERY NEW member is durably open, for
    /// retiring its own obsolete OLD custody: own fresh release observation
    /// plus one authenticated RELEASE frame from every other NEW member.
    pub(crate) fn observe_all_open_for_old_retirement(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        collector: &crate::distribution_consensus::HostMetadataReadObservation,
        collector_business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointRemoteOldRetirementProof> {
        let request = self.bound(parent);
        request.check()?;
        self.require_actor(client)?;
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let local_phase = self.acquire_and_observe(client, metadata_leader, &phase)?;
        self.facts_of(&local_phase)?;
        let phase = phase.child_with_deadline(local_phase.committed().deadline());
        phase.check()?;
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Release,
            self.data(),
            client,
            collector,
            collector_business,
            &phase,
        )?;
        require_window_open(&window, false)?;
        let proof = NativeJointRemoteOldRetirementProof {
            release: self.clone(),
            local_phase,
            window,
            cancellation: phase,
        };
        proof.require_live()?;
        Ok(proof)
    }
    /// This member's own complete release.
    ///
    /// Own pre-quorum RELEASE observation under its service lock, one
    /// authenticated RELEASE observation from EVERY other NEW member (each
    /// attesting its durable activation), `after_first_window` (the caller
    /// drops only its own obsolete preparation custody there), a distinct
    /// post-quorum observation, then the unchanged owner release, which
    /// continues an already dropped owner instead of repeating the drop.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn release_remote(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        collector: &crate::distribution_consensus::HostMetadataReadObservation,
        collector_business: Uuid,
        parent: &crate::CancellationToken,
        after_first_window: &mut dyn FnMut() -> Result<()>,
    ) -> Result<NativeRemoteBridgeReleased> {
        let request = self.bound(parent);
        request.check()?;
        self.require_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        // (1) Own pre-quorum RELEASE observation.
        let local_phase = self.acquire_and_observe(client, metadata_leader, &phase)?;
        self.facts_of(&local_phase)?;
        let phase = phase.child_with_deadline(local_phase.committed().deadline());
        phase.check()?;
        // (2) Every other NEW member's own durable activation, under its lock.
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Release,
            data,
            client,
            collector,
            collector_business,
            &phase,
        )?;
        require_window_open(&window, false)?;
        after_first_window()?;
        window.require_live()?;
        // (3) Distinct post-quorum observation, no guard across the I/O.
        let post = self.acquire_and_observe(client, metadata_leader, &phase)?;
        let post_quorum = post.committed().metadata_anchor();
        require_post_all(
            post_quorum,
            local_phase.committed().metadata_anchor(),
            &window,
        )?;
        let identities = window.identities.clone();
        let first_contexts = window.contexts();
        let admission = NativeJointReleaseAdmission {
            cancellation: phase.child_with_deadline(post.committed().deadline()),
            backing: ReleaseBacking::Remote(RemoteReleaseBacking {
                release: self.clone(),
                local_phase,
                window,
            }),
            local: self.local.clone(),
            post,
            attempted: std::sync::atomic::AtomicBool::new(false),
        };
        admission.require_live()?;
        // (4) The unchanged owner release: final foreign boundary, complete
        // local validation under the journal, drop, arm.
        admission.release_actual_local()?;
        Ok(NativeRemoteBridgeReleased {
            identities,
            first_contexts,
            post_quorum,
        })
    }
    /// The AFTER half: a second bounded RELEASE window bound to metadata
    /// taken after the release. Every other member must report exactly the
    /// identity, bridge, activation and released generation it reported
    /// before; this member's own fresh observation must be released and
    /// armed. Returns that armed generation.
    pub(crate) fn confirm_remote_release_second_window(
        self: &Arc<Self>,
        released: &NativeRemoteBridgeReleased,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<Uuid> {
        let request = self.bound(parent);
        request.check()?;
        self.require_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        #[cfg(test)]
        let fault = super::super::super::remote_publication::test_take_second_window_fault(
            &self.local,
            data.checksum_sha256(),
        );
        #[cfg(test)]
        if fault
            == Some(crate::distribution_range_handoff::NativeBridgeSecondWindowFault::LostReply)
        {
            return Err(crate::BicDbError::QueryTimedOut);
        }
        let business = Uuid::now_v7();
        let own = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining(&phase)?,
            &phase,
        )?;
        if own.term() < released.post_quorum.0
            || own.read_index() < released.post_quorum.1
            || own.context_id() == released.post_quorum.2
        {
            return Err(invalid(
                "remote release second window is not after the post-quorum observation",
            ));
        }
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Release,
            data,
            client,
            &own,
            business,
            &phase,
        )?;
        #[allow(unused_mut)]
        let mut identities = window.identities.clone();
        #[cfg(test)]
        if fault
            == Some(
                crate::distribution_range_handoff::NativeBridgeSecondWindowFault::ChangedIdentity,
            )
        {
            let first = identities.values_mut().next().unwrap();
            first.writer_identity_sha256 = "f".repeat(64);
        }
        if identities != released.identities {
            return Err(invalid(
                "remote release second window: a NEW member's identity/activation/release changed across the release",
            ));
        }
        for frame in window.frames.values() {
            let (term, index, context) = frame.metadata_anchor();
            if term < released.post_quorum.0
                || index < released.post_quorum.1
                || context == released.post_quorum.2
                || released.first_contexts.contains(&context)
            {
                return Err(invalid(
                    "remote release second window reused or preceded first-window metadata",
                ));
            }
        }
        // This member's own state, re-observed under its own lock.
        let after = self.acquire_and_observe(client, metadata_leader, &phase)?;
        let generation = self
            .facts_of(&after)?
            .released_generation
            .ok_or_else(|| invalid("remote release second window: own owner is not released"))?;
        window.require_live()?;
        own.require_live_request(business)?;
        phase.check()?;
        Ok(generation)
    }
}

/// Who observed a remote all-released quorum.
enum ActivationObserver {
    /// A released member of the new set: its own member is its actual
    /// continuation entry re-observed under its own lock.
    Member {
        release: Arc<NativeJointMemberRelease>,
        local_phase: ActualServiceNativeJointReleaseEvidence,
    },
    /// The coordinator outside the new set (for example a metadata leader
    /// that holds no replica of the moved range): every member is a frame.
    Coordinator,
}
/// The remote counterpart of the in-process activated quorum: the fact that
/// EVERY member of the new set is durably activated, released and open.
/// Every member other than the observer's own is a transport-minted
/// authenticated RELEASE frame the observer collected itself inside one
/// bounded window, so each counted identity is the peer that exchange
/// authenticated. There is no constructor from digests, replies or decoded
/// state. It authorizes the metadata activation record only.
pub(crate) struct NativeRemoteJointActivationCertificate {
    data: NativeJointDecisionData,
    observer: ActivationObserver,
    window: RemotePreparationWindow,
    cancellation: crate::CancellationToken,
    sha256: String,
}
/// Exactly the per-member facts the in-process proof digests, in node
/// order. A counted remote member is its authenticated peer.
fn remote_activated_digest(
    data: &NativeJointDecisionData,
    local: Option<(ClusterNodeId, (u64, String, String))>,
    window: &RemotePreparationWindow,
) -> Result<String> {
    let mut members = BTreeMap::new();
    members.extend(local);
    for (node, identity) in &window.identities {
        let (bridge, _) = identity
            .bridge
            .as_ref()
            .ok_or_else(|| invalid("remote activation frame attests no durable bridge"))?;
        let (activation, _) = identity
            .release
            .as_ref()
            .ok_or_else(|| invalid("remote activation frame attests no durable activation"))?;
        if members
            .insert(
                node.clone(),
                (
                    identity.member_incarnation,
                    bridge.clone(),
                    activation.clone(),
                ),
            )
            .is_some()
        {
            return Err(invalid("remote activation record counted a member twice"));
        }
    }
    if members.keys().ne(data.new_members().keys()) {
        return Err(invalid(
            "remote activation record omitted/replaced a final member",
        ));
    }
    let members = members
        .into_iter()
        .map(|(node, (incarnation, bridge, activation))| (node, incarnation, bridge, activation))
        .collect::<Vec<_>>();
    super::activated_quorum::activated_quorum_digest(data, &members)
}
impl NativeJointMemberRelease {
    /// Observe the all-released quorum of this member's decision: own fresh
    /// release observation (released and armed) plus one authenticated
    /// RELEASE frame from every other NEW member inside one bounded window,
    /// each attesting its own durable bridge, durable catalogue activation
    /// and armed generation.
    pub(crate) fn certify_remote_activated_quorum(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeRemoteJointActivationCertificate> {
        let request = self.bound(parent);
        request.check()?;
        self.require_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let local_phase = self.acquire_and_observe(client, metadata_leader, &phase)?;
        let facts = self.facts_of(&local_phase)?;
        local_phase.require_released_open()?;
        let phase = phase.child_with_deadline(local_phase.committed().deadline());
        phase.check()?;
        let business = Uuid::now_v7();
        let collector = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining(&phase)?,
            &phase,
        )?;
        let window = RemotePreparationWindow::collect(
            NativeJointBridgePhaseKindV1::Release,
            data,
            client,
            &collector,
            business,
            &phase,
        )?;
        require_release_window(&window, &self.local, data)?;
        require_window_open(&window, true)?;
        let sha256 = remote_activated_digest(
            data,
            Some((
                self.local.clone(),
                (
                    data.new_members()[&self.local].incarnation,
                    facts.bridge.0,
                    facts.activation_sha256,
                ),
            )),
            &window,
        )?;
        let result = NativeRemoteJointActivationCertificate {
            data: data.clone(),
            observer: ActivationObserver::Member {
                release: self.clone(),
                local_phase,
            },
            window,
            cancellation: phase,
            sha256,
        };
        result.require_current()?;
        Ok(result)
    }
}
impl NativeRemoteJointActivationCertificate {
    /// The coordinator OUTSIDE the new set observes the all-released quorum
    /// of `data`, the decision it observed committed: one authenticated
    /// RELEASE frame from EVERY member of the new set inside one bounded
    /// window over its own pinned transport. Every member itself admits the
    /// observer only as the coordinator of its own committed metadata, and
    /// the activation stage still compares `data` with the committed record.
    pub(crate) fn observe_as_outside_coordinator(
        data: &NativeJointDecisionData,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        let actor = client.local_metadata_read_participant();
        if data.new_members().contains_key(actor.node_id()) {
            return Err(invalid(
                "a member of the new set must observe the activation through its own member state",
            ));
        }
        let phase = parent.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        let business = Uuid::now_v7();
        let collector = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            remaining(&phase)?,
            &phase,
        )?;
        let phase = phase.child_with_deadline(collector.deadline());
        let window = RemotePreparationWindow::collect_from_outside(
            NativeJointBridgePhaseKindV1::Release,
            data,
            client,
            &collector,
            business,
            &phase,
        )?;
        require_window_open(&window, true)?;
        let sha256 = remote_activated_digest(data, None, &window)?;
        let result = Self {
            data: data.clone(),
            observer: ActivationObserver::Coordinator,
            window,
            cancellation: phase,
            sha256,
        };
        result.require_current()?;
        Ok(result)
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.data
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.sha256
    }
    /// The window and every member's released open are still live: the
    /// observer's own member (if any) re-read under its own lock, every
    /// other member still inside the bounded window of its own re-read.
    pub(crate) fn require_current(&self) -> Result<()> {
        self.cancellation.check()?;
        if self.window.kind != NativeJointBridgePhaseKindV1::Release {
            return Err(invalid("remote activation window observed another phase"));
        }
        match &self.observer {
            ActivationObserver::Member {
                release,
                local_phase,
            } => {
                release.continuation.require_original()?;
                local_phase.require_released_open()?;
                release
                    .continuation
                    .with_owner(&release.local, |service, owner| {
                        let actual = service
                            .try_lock()
                            .ok_or_else(|| invalid("remote activation current service busy"))?;
                        local_phase.require_current(&actual, owner)
                    })?;
                if release.data() != &self.data {
                    return Err(invalid(
                        "remote activation observer member holds another decision",
                    ));
                }
                require_release_window(&self.window, &release.local, &self.data)?;
                local_phase.require_released_open()?;
            }
            ActivationObserver::Coordinator => {
                if self.window.member_observer
                    || self.data.new_members().contains_key(&self.window.observer)
                    || self.window.frames.len() != self.data.new_members().len()
                {
                    return Err(invalid(
                        "remote activation coordinator window omits a member of the new set",
                    ));
                }
            }
        }
        self.window.require_complete(&self.data)?;
        require_window_open(&self.window, true)?;
        self.cancellation.check()
    }
}
impl super::activated_quorum::activation_proof_seal::Sealed
    for NativeRemoteJointActivationCertificate
{
}
impl super::activated_quorum::NativeJointActivationProof
    for NativeRemoteJointActivationCertificate
{
    fn data(&self) -> &NativeJointDecisionData {
        NativeRemoteJointActivationCertificate::data(self)
    }
    fn checksum_sha256(&self) -> &str {
        NativeRemoteJointActivationCertificate::checksum_sha256(self)
    }
    fn require_current(&self) -> Result<()> {
        NativeRemoteJointActivationCertificate::require_current(self)
    }
}
