//! Necessary DATA-policy publication after a real ALL-released Open quorum.
//! It is not an ordinary enrolled read quorum, Ready or APP permission.
use super::*;

pub(crate) struct NativeJointServingPolicyAdmission {
    authority: PolicyAuthority,
    local: ClusterNodeId,
    post: ActualServiceNativeJointReleaseEvidence,
    cancellation: crate::CancellationToken,
}
enum PolicyAuthority {
    Local(Arc<NativeJointReleasePhaseQuorum>),
    #[cfg(feature = "tls-replication")]
    Remote {
        owner: Arc<crate::distribution_data::NativeRemoteReleaseOwner>,
        proof: Arc<crate::distribution_range_handoff::NativeRemoteAllReleasedQuorum>,
        request: crate::distribution_native_host::NativeRemoteLocalPolicyAuthority,
    },
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
        let metadata_point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &member.service,
            &own,
            business,
            &self.cancellation,
        )?;
        let post = self.continuation.with_owner(&local, |service, owner| {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("joint writer policy post-QQ service busy"))?;
            let evidence = actual.observe_joint_release_member_with_domain_and_point(
                &member.writer_domain,
                own,
                business,
                member.native.clone(),
                owner,
                metadata_point,
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
            authority: PolicyAuthority::Local(self.clone()),
            local,
            post,
        };
        result.require_external_current()?;
        Ok(result)
    }
}
impl NativeJointServingPolicyAdmission {
    #[cfg(test)]
    pub(crate) fn test_policy_trace_clocks(&self) -> crate::native_policy_trace::Clocks {
        let mut clocks = crate::native_policy_trace::Clocks {
            admission: self.cancellation.deadline(),
            post_own: Some(self.post.committed().deadline()),
            ..Default::default()
        };
        match &self.authority {
            PolicyAuthority::Local(phase) => {
                clocks.authority = "actual_local_ALL_released";
                clocks.all_phase = phase.cancellation.deadline();
                clocks.role_source = "local phase retains no distinct Role object; OWN is not Role";
            }
            #[cfg(feature = "tls-replication")]
            PolicyAuthority::Remote { request, .. } => {
                let (role, own) = request.test_policy_trace_deadlines();
                clocks.authority = "actual_remote_ALL_released_local_policy";
                clocks.role = Some(role);
                clocks.role_own = Some(own);
                clocks.role_source = "same actual private local-policy Role";
                // No ALL-frame deadline getter/proof is manufactured here.
            }
        }
        clocks
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_actual_remote_all_released(
        proof: Arc<crate::distribution_range_handoff::NativeRemoteAllReleasedQuorum>,
        observed: crate::distribution_data::NativeRemoteReleaseObservation,
        participant: &Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
        request: crate::distribution_native_host::NativeRemoteLocalPolicyAuthority,
    ) -> Result<Self> {
        let owner = observed.owner().clone();
        owner.require_participant(participant)?;
        request.require_successor(&owner)?;
        proof.require_original(participant, &owner)?;
        proof.require_local_observation(&observed)?;
        request.require_current(observed.actual().committed())?;
        observed.inspect(
            |scope, store, _, _, _, _| {
                request.require_scope(observed.actual().committed(), scope, store)
            },
            || request.require_memory_current(),
        )?;
        let cancellation = request
            .bound_request()
            .child_with_deadline(observed.actual().committed().deadline());
        let local = owner.native().local_node_id().clone();
        let post = observed.into_admission_post(&owner)?;
        let result = Self {
            authority: PolicyAuthority::Remote {
                owner,
                proof,
                request,
            },
            local,
            post,
            cancellation,
        };
        result.require_external_current()?;
        Ok(result)
    }

    #[cfg(feature = "tls-replication")]
    pub(crate) fn bound_policy_request(&self) -> crate::CancellationToken {
        self.cancellation.child()
    }
    pub(crate) fn historical_native_bridge(
        &self,
    ) -> Arc<crate::db::NativeJointServingBridgeReceipt> {
        self.post.native().clone()
    }
    pub(crate) fn historical_released_publication_point(
        &self,
    ) -> Result<(Arc<crate::db::NativeJointActivatedOpenGuard>, Uuid, String)> {
        self.post.historical_released_publication_point()
    }
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        match &self.authority {
            PolicyAuthority::Local(phase) => &phase.continuation.data,
            #[cfg(feature = "tls-replication")]
            PolicyAuthority::Remote { owner, .. } => owner.native().data(),
        }
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
        match &self.authority {
            PolicyAuthority::Local(phase) => {
                phase.require_live()?;
                for member in phase.members.values() {
                    member.require_released_open()?;
                }
            }
            #[cfg(feature = "tls-replication")]
            PolicyAuthority::Remote {
                owner,
                proof,
                request,
            } => {
                owner.require_memory_lifetime()?;
                proof.require_live()?;
                request.require_memory_current()?;
            }
        }
        self.post.require_released_open()?;
        self.cancellation.check()
    }
    /// Before taking any local service/DB/journal guard.
    pub(crate) fn require_external_current(&self) -> Result<()> {
        self.require_live()?;
        match &self.authority {
            PolicyAuthority::Local(phase) => {
                for node in phase.members.keys() {
                    phase.require_member_current(node)?;
                }
            }
            #[cfg(feature = "tls-replication")]
            PolicyAuthority::Remote {
                owner,
                proof,
                request,
            } => {
                proof.require_post_all(self.post.committed())?;
                request.require_current(self.post.committed())?;
                owner.with_current_owner(|same, actual| {
                    let service = same
                        .try_lock()
                        .ok_or_else(|| invalid("Policy original current service busy"))?;
                    self.post.require_current(&service, actual)
                })?;
            }
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
        #[cfg(feature = "tls-replication")]
        if let PolicyAuthority::Remote { owner, request, .. } = &self.authority {
            self.require_live()?;
            request.require_service(actual)?;
            if !Arc::ptr_eq(service, owner.service())
                || !Arc::ptr_eq(original, owner.domain().actual_writer()?)
            {
                return Err(invalid(
                    "Policy changed actual original survivor State/service",
                ));
            }
            let scope = self.native().historical_cut_scope(db)?;
            return actual.with_writer_data_domain_store(db, |store| {
                self.require_local_with_store_scope(store, &scope)
            });
        }
        let PolicyAuthority::Local(phase) = &self.authority else {
            return Err(invalid("local Policy quorum absent"));
        };

        self.require_live()?;
        let member = &phase.continuation.members[&self.local];
        if !Arc::ptr_eq(service, &member.service)
            || !Arc::ptr_eq(original, member.writer.actual_writer()?)
        {
            return Err(invalid(
                "writer policy changed actual original service/State",
            ));
        }
        let scope = self.native().historical_cut_scope(db)?;
        let owner = NativeJointReleaseOwnerView::Dropped(&member.activation);
        phase.members[&self.local].require_current_with_scope(actual, &scope, owner)?;
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
        if let PolicyAuthority::Remote { owner, request, .. } = &self.authority {
            self.require_live()?;
            request.require_service(actual)?;
            let expected = owner.domain().phase_owner();
            if !Arc::ptr_eq(service, owner.service())
                || !expected.same_actual(&crate::db::NativeJointPhaseOwner::Registered(
                    Arc::downgrade(original),
                ))
            {
                return Err(invalid(
                    "Policy changed actual original Registered receiver/service",
                ));
            }
            let scope = self.native().historical_cut_scope(db)?;
            return actual.with_writer_data_domain_store(db, |store| {
                self.require_local_with_store_scope(store, &scope)
            });
        }
        let PolicyAuthority::Local(phase) = &self.authority else {
            return Err(invalid("local Registered Policy quorum absent"));
        };

        self.require_live()?;
        let member = &phase.continuation.members[&self.local];
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
        phase.members[&self.local].require_current_with_scope(actual, &scope, dropped)?;
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
    pub(crate) fn actual_released_publication_point(
        &self,
    ) -> Result<(Arc<crate::db::NativeJointActivatedOpenGuard>, Uuid, String)> {
        self.post.actual_released_publication_point()
    }
    pub(crate) fn require_publication_request_memory(&self) -> Result<()> {
        #[cfg(feature = "tls-replication")]
        if let PolicyAuthority::Remote {
            owner,
            proof,
            request,
        } = &self.authority
        {
            self.cancellation.check()?;
            owner.require_memory_lifetime()?;
            proof.require_live()?;
            request.require_memory_current()?;
            return self.post.committed().require_publication_request_memory();
        }
        let PolicyAuthority::Local(phase) = &self.authority else {
            return Err(invalid("local Policy memory quorum absent"));
        };

        self.cancellation.check()?;
        phase.cancellation.check()?;
        phase.continuation.caller.check()?;
        if phase.continuation.original_cancel.is_cancel_requested() {
            return Err(crate::BicDbError::QueryCanceled);
        }
        self.post.committed().require_publication_request_memory()?;
        for member in phase.members.values() {
            member.committed().require_publication_request_memory()?;
        }
        Ok(())
    }
    pub(crate) fn native_publication_guard(
        &self,
    ) -> Result<Option<crate::distribution_data::ActualNativeJointMetadataPublicationGuard<'_>>>
    {
        self.post.committed().native_publication_guard()
    }
    pub(crate) fn require_local_with_store_scope(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
    ) -> Result<()> {
        #[cfg(feature = "tls-replication")]
        if let PolicyAuthority::Remote { owner, request, .. } = &self.authority {
            self.require_live()?;
            request.require_scope(self.post.committed(), scope, store)?;
            self.post.require_current_with_store_scope(
                store,
                scope,
                NativeJointReleaseOwnerView::Dropped(owner.activation()),
            )?;
            return self.require_live();
        }
        let PolicyAuthority::Local(phase) = &self.authority else {
            return Err(invalid("local Policy scoped quorum absent"));
        };

        self.require_live()?;
        self.post
            .committed()
            .require_native_publication_metadata(scope.database(), store)?;
        let owner = NativeJointReleaseOwnerView::Dropped(
            &phase.continuation.members[&self.local].activation,
        );
        phase.members[&self.local].require_current_with_store_scope(store, scope, owner)?;
        self.post
            .require_current_with_store_scope(store, scope, owner)?;
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
        #[cfg(feature = "tls-replication")]
        if let PolicyAuthority::Remote { proof, .. } = &self.authority {
            self.require_live()?;
            return proof.historical_audit();
        }
        let PolicyAuthority::Local(phase) = &self.authority else {
            return Err(invalid("local Policy audit quorum absent"));
        };

        self.require_live()?;
        let members = phase
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
