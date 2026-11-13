//! Necessary DATA-policy publication after a real ALL-released Open quorum.
//! It is not an ordinary enrolled read quorum, Ready or APP permission.
use super::*;

pub(crate) struct NativeJointServingPolicyAdmission {
    phase: Arc<NativeJointReleasePhaseQuorum>,
    local: ClusterNodeId,
    post: ActualServiceNativeJointReleaseEvidence,
    cancellation: crate::CancellationToken,
}
impl NativeJointReleasePhaseQuorum {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn acquire_local_writer_policy(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        leader: &ClusterNodeId,
    ) -> Result<NativeJointServingPolicyAdmission> {
        self.require_live()?;
        for member in self.members.values() {
            member.require_released_open()?;
        }
        if self.claimed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(invalid(
                "joint all-released policy phase already selected an attempt",
            ));
        }
        let actor = client.local_metadata_read_participant();
        let local = actor.node_id().clone();
        let member = self
            .continuation
            .members
            .get(&local)
            .ok_or_else(|| invalid("joint writer policy recipient omitted"))?;
        if self.continuation.data.new_members()[&local].incarnation != actor.member_incarnation() {
            return Err(invalid("joint writer policy recipient incarnation changed"));
        }
        let business = Uuid::now_v7();
        let own = client.acquire_host_metadata_observation(
            leader,
            business,
            remaining(&self.cancellation)?,
            &self.cancellation,
        )?;
        let post = self.continuation.with_owner(&local, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint writer policy post-QQ service busy"))?;
            let evidence = actual.observe_joint_release_member_with_domain(
                &member.writer_domain,
                own,
                business,
                member.native.clone(),
                owner,
            )?;
            evidence.require_original_writer_identity(&member.writer)?;
            evidence.require_released_open()?;
            Ok(evidence)
        })?;
        let (term, index, context) = post.committed().metadata_anchor();
        for observed in self.members.values() {
            let old = observed.committed().metadata_anchor();
            if term < old.0 || index < old.1 || context == old.2 {
                return Err(invalid(
                    "writer policy needs independently post-all-released MQ",
                ));
            }
        }
        let result = NativeJointServingPolicyAdmission {
            cancellation: self
                .cancellation
                .child_with_deadline(post.committed().deadline()),
            phase: self.clone(),
            local,
            post,
        };
        result.require_external_current()?;
        Ok(result)
    }
}
impl NativeJointServingPolicyAdmission {
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        &self.phase.continuation.data
    }
    pub(crate) fn native(&self) -> &crate::db::NativeJointServingBridgeReceipt {
        self.post.native()
    }
    pub(crate) fn activation(
        &self,
    ) -> &crate::distribution_range_consensus::NativeJointCatalogueActivationFact {
        self.post.activation()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        &self.local
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.phase.require_live()?;
        for member in self.phase.members.values() {
            member.require_released_open()?;
        }
        self.post.require_released_open()?;
        self.cancellation.check()
    }
    /// Before taking any local service/DB/journal guard.
    pub(crate) fn require_external_current(&self) -> Result<()> {
        self.require_live()?;
        for node in self.phase.members.keys() {
            self.phase.require_member_current(node)?;
        }
        self.require_live()
    }
    /// Complete local checks borrow the actual same protected native scope;
    /// no network or metadata-store lock is acquired at this boundary.
    pub(crate) fn require_local_current(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        actual: &ClusterDataNodeService,
        db: &crate::BicDb,
        original: &Arc<crate::db::NativeWriterHostState>,
    ) -> Result<()> {
        self.require_live()?;
        let member = &self.phase.continuation.members[&self.local];
        if !Arc::ptr_eq(service, &member.service)
            || !Arc::ptr_eq(original, member.writer.actual_writer()?)
        {
            return Err(invalid(
                "writer policy changed actual original service/State",
            ));
        }
        let scope = self.native().historical_cut_scope(db)?;
        let owner = NativeJointReleaseOwnerView::Dropped(&member.activation);
        self.phase.members[&self.local].require_current_with_scope(actual, &scope, owner)?;
        self.post
            .require_current_with_scope(actual, &scope, owner)?;
        self.require_live()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_local_registered_current(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
        actual: &ClusterDataNodeService,
        db: &crate::BicDb,
        original: &Arc<crate::distribution_native_host::NativeRegisteredRemoteJointOwner>,
    ) -> Result<()> {
        self.require_live()?;
        let member = &self.phase.continuation.members[&self.local];
        let crate::db::NativeJointPhaseOwner::Registered(owner) = &member.writer else {
            return Err(invalid(
                "Registered first policy cannot use an existing writer's admission",
            ));
        };
        if !Arc::ptr_eq(service, &member.service)
            || !std::sync::Weak::ptr_eq(owner, &Arc::downgrade(original))
        {
            return Err(invalid(
                "Registered first policy changed original service/receiver owner",
            ));
        }
        let scope = self.native().historical_cut_scope(db)?;
        let dropped = NativeJointReleaseOwnerView::Dropped(&member.activation);
        self.phase.members[&self.local].require_current_with_scope(actual, &scope, dropped)?;
        self.post
            .require_current_with_scope(actual, &scope, dropped)?;
        self.require_live()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_post_policy_metadata(
        &self,
        metadata: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<()> {
        self.require_live()?;
        metadata.require_live_request(business)?;
        self.data().require_metadata_domain(
            metadata.committed_topology(business)?,
            metadata.restore_activation(business)?,
        )?;
        let anchor = self.post.committed().metadata_anchor();
        if metadata.requester_node_id() != &self.local
            || metadata.context_id() == anchor.2
            || metadata.term() < anchor.0
            || metadata.read_index() < anchor.1
        {
            return Err(invalid("Registered first policy requires independently post-all-released actual OWN metadata"));
        }
        self.require_live()
    }
    pub(crate) fn observe_local_continuity(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<crate::db::NativeJointOpenContinuity> {
        self.require_live()?;
        self.post.observe_released_continuity(store)
    }
    pub(crate) fn open_quorum_audit(&self) -> Result<serde_json::Value> {
        self.require_live()?;
        let members = self
            .phase
            .members
            .iter()
            .map(|(node, member)| Ok((node.clone(), member.writer_domain_audit()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(
            serde_json::json!({"version":1,"decision_sha256":self.data().checksum_sha256(),"members":members}),
        )
    }
    pub(crate) fn post_metadata_audit(&self) -> Result<serde_json::Value> {
        self.require_live()?;
        self.post.committed().writer_domain_metadata_audit()
    }
}
