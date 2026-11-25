//! One released NEW member's own writer DATA policy admission when the other
//! NEW members live in other processes. Same construction rule as the remote
//! release: the local member is its actual original continuation entry and
//! its own fresh release observations under its service lock; every other
//! member is a frame the pinned transport minted after authenticating that
//! member in THIS member's own exchange.
//!
//! The durable serving record stores every member's own metadata read audit.
//! A member of another process supplies its audit as DATA in its WRITER AUDIT
//! observation. That DATA is never authority: the counted member is the
//! authenticated peer, the audit passes the same per-member validation the
//! durable record applies on every later read, its metadata point must be the
//! anchor of the frame that carried it, and the frame must still be inside
//! its own bounded window.
use super::*;

/// The backing of a remote writer policy admission. Private fields and no
/// constructor outside this file.
pub(in super::super) struct RemotePolicyBacking {
    release: Arc<NativeJointMemberRelease>,
    /// This member's own fresh pre-quorum RELEASE observation.
    local_phase: ActualServiceNativeJointReleaseEvidence,
    window: RemotePreparationWindow,
    /// Every other member's validated audit entry, by authenticated peer.
    audits: BTreeMap<ClusterNodeId, serde_json::Value>,
    phase: crate::CancellationToken,
}

fn require_policy_window(
    window: &RemotePreparationWindow,
    local: &ClusterNodeId,
    data: &NativeJointDecisionData,
) -> Result<()> {
    if &window.observer != local
        || !window.member_observer
        || window.kind != NativeJointBridgePhaseKindV1::WriterAudit
        || window.frames.len() + 1 != data.new_members().len()
    {
        return Err(invalid(
            "remote writer policy window was observed by another member/phase or omits a NEW member",
        ));
    }
    Ok(())
}

/// One other member's durable audit entry, composed from what its own
/// authenticated frame states and validated completely.
fn require_frame_audit(
    data: &NativeJointDecisionData,
    node: &ClusterNodeId,
    frame: &crate::distribution_transport::AuthenticatedNativeJointBridgeStepFrame,
    identity: &NativeJointBridgeMemberIdentity,
) -> Result<serde_json::Value> {
    frame.require_live()?;
    // (i) The counted member is the peer this exchange authenticated.
    if frame.actual_peer() != node
        || data
            .new_members()
            .get(node)
            .is_none_or(|member| member.incarnation != frame.actual_peer_incarnation())
    {
        return Err(invalid(
            "remote writer audit frame is not the authenticated NEW member",
        ));
    }
    // (iv) The in-process rule: a Frozen or unarmed member supplies no entry.
    let (bridge, _) = identity
        .bridge
        .as_ref()
        .ok_or_else(|| invalid("remote writer audit frame attests no durable bridge"))?;
    let (activation, generation) = identity
        .release
        .as_ref()
        .ok_or_else(|| invalid("remote writer audit frame attests no durable activation"))?;
    let generation = generation.ok_or_else(|| {
        invalid(
            "remote writer policy window: a NEW member still has an actual Frozen or unarmed owner",
        )
    })?;
    let metadata = frame.reply().writer_audit.as_ref().ok_or_else(|| {
        invalid("remote writer audit frame of a released member carries no audit")
    })?;
    // The bound the producing member's own projection applies.
    crate::distribution_range_image::bounded_json(
        metadata,
        crate::distribution_range_handoff::MAX_STEP_WRITER_AUDIT_BYTES,
    )?;
    // Exactly the shape the in-process path stores for one member.
    let entry = serde_json::json!({
        "metadata": metadata,
        "native_bridge_sha256": bridge,
        "catalogue_activation_sha256": activation,
        "released_generation": generation,
        "local_node_id": node,
    });
    // (ii) The same per-member validation the durable record runs.
    let stated = crate::db::require_remote_serving_member_audit(node, &entry, data)?;
    // (iii) The audit is of the very observation behind its frame.
    if stated != frame.metadata_anchor() {
        return Err(invalid(
            "remote writer audit metadata point differs from the anchor of its frame",
        ));
    }
    frame.require_live()?;
    Ok(entry)
}

impl RemotePolicyBacking {
    pub(in super::super) fn continuation(&self) -> &NativeJointReleaseContinuation {
        &self.release.continuation
    }
    pub(in super::super) fn local_phase(&self) -> &ActualServiceNativeJointReleaseEvidence {
        &self.local_phase
    }
    pub(in super::super) fn require_live(
        &self,
        local: &ClusterNodeId,
        post: &ActualServiceNativeJointReleaseEvidence,
    ) -> Result<()> {
        self.phase.check()?;
        self.release.continuation.require_original()?;
        self.local_phase.require_live()?;
        self.local_phase.require_released_open()?;
        require_policy_window(&self.window, local, self.release.data())?;
        self.window.require_live()?;
        require_window_open(&self.window, true)?;
        if self.audits.keys().ne(self.window.frames.keys()) {
            return Err(invalid(
                "remote writer policy window omits a NEW member's audit",
            ));
        }
        require_post_all(
            post.committed().metadata_anchor(),
            self.local_phase.committed().metadata_anchor(),
            &self.window,
        )
    }
    /// The final foreign boundary before the local durable policy: the local
    /// member re-read under its own lock, complete member set, every frame
    /// unchanged, released and still inside its own window.
    pub(in super::super) fn require_external_current(&self, local: &ClusterNodeId) -> Result<()> {
        require_policy_window(&self.window, local, self.release.data())?;
        self.release
            .continuation
            .with_owner(&self.release.local, |service, owner| {
                let actual = service
                    .try_lock()
                    .ok_or_else(|| invalid("remote writer policy current service busy"))?;
                self.local_phase.require_current(&actual, owner)
            })?;
        self.window.require_complete(self.release.data())?;
        require_window_open(&self.window, true)
    }
    /// Memory only: no lock, network or store access.
    pub(in super::super) fn require_publication_request_memory(&self) -> Result<()> {
        self.phase.check()?;
        self.local_phase
            .committed()
            .require_publication_request_memory()?;
        self.window.require_live()
    }
    pub(in super::super) fn open_quorum_audit(
        &self,
        local: &ClusterNodeId,
    ) -> Result<BTreeMap<ClusterNodeId, serde_json::Value>> {
        require_policy_window(&self.window, local, self.release.data())?;
        self.window.require_live()?;
        let mut members = self.audits.clone();
        if members
            .insert(local.clone(), self.local_phase.writer_domain_audit()?)
            .is_some()
            || members.keys().ne(self.release.data().new_members().keys())
        {
            return Err(invalid(
                "remote writer policy audit omits or counts a NEW member twice",
            ));
        }
        Ok(members)
    }
}

impl NativeJointMemberRelease {
    /// This released member's own writer DATA policy admission.
    ///
    /// Own pre-quorum RELEASE observation (released and armed), one
    /// authenticated WRITER AUDIT observation from EVERY other NEW member
    /// (each released and armed, each audit validated as described above),
    /// then a distinct post-quorum observation that independently follows
    /// every member's. The caller performs the unchanged local policy
    /// publication under the returned admission and then confirms with
    /// `confirm_remote_release_second_window` over the returned first-window
    /// identity that no other member changed across its durable write.
    pub(crate) fn acquire_remote_writer_policy(
        self: &Arc<Self>,
        client: &TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        collector: &crate::distribution_consensus::HostMetadataReadObservation,
        collector_business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<(
        NativeJointServingPolicyAdmission,
        NativeRemoteBridgeReleased,
    )> {
        let request = self.bound(parent);
        request.check()?;
        self.require_actor(client)?;
        let data = self.data();
        let phase = request.child_with_deadline(Instant::now() + REMOTE_PHASE_BUDGET);
        // (1) Own pre-quorum RELEASE observation (released and armed) and
        // (2) one WRITER AUDIT observation of every other NEW member, taken
        // at the same time. All are pre-quorum observations of this policy:
        // the post-quorum observation below must follow every one of them
        // (`require_post_all`), and no order is required among them. Taken
        // one after the other, the other members' frames ran out on a slow
        // host before the durable policy. The window's exchanges are bound
        // to `collector`; the phase is bounded by the local observation once
        // both are in.
        let (local, window) = std::thread::scope(|scope| {
            let observe_local = || self.acquire_and_observe(client, metadata_leader, &phase);
            let local = std::thread::Builder::new()
                .name("joint-policy-local-phase".into())
                .spawn_scoped(scope, observe_local);
            let window = RemotePreparationWindow::collect(
                NativeJointBridgePhaseKindV1::WriterAudit,
                data,
                client,
                collector,
                collector_business,
                &phase,
            );
            let local = match local {
                Ok(worker) => worker
                    .join()
                    .unwrap_or_else(|_| Err(invalid("the policy local phase worker panicked"))),
                Err(_) => observe_local(),
            };
            (local, window)
        });
        let local_phase = local?;
        self.facts_of(&local_phase)?;
        local_phase.require_released_open()?;
        let phase = phase.child_with_deadline(local_phase.committed().deadline());
        phase.check()?;
        let window = window?;
        require_policy_window(&window, &self.local, data)?;
        let mut audits = BTreeMap::new();
        for (node, frame) in &window.frames {
            audits.insert(
                node.clone(),
                require_frame_audit(data, node, frame, &window.identities[node])?,
            );
        }
        require_window_open(&window, true)?;
        // (3) Distinct post-quorum observation, no guard across the I/O.
        let post = self.acquire_and_observe(client, metadata_leader, &phase)?;
        post.require_released_open()?;
        let post_quorum = post.committed().metadata_anchor();
        require_post_all(
            post_quorum,
            local_phase.committed().metadata_anchor(),
            &window,
        )?;
        let observed = NativeRemoteBridgeReleased {
            identities: window.identities.clone(),
            first_contexts: window.contexts(),
            post_quorum,
        };
        let admission = NativeJointServingPolicyAdmission {
            cancellation: phase.child_with_deadline(post.committed().deadline()),
            backing: super::super::writer_policy::PolicyBacking::Remote(RemotePolicyBacking {
                release: self.clone(),
                local_phase,
                window,
                audits,
                phase,
            }),
            local: self.local.clone(),
            post,
        };
        admission.require_external_current()?;
        Ok((admission, observed))
    }
}
