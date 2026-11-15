//! Historical ownership is retained independently of a short metadata request.
//! Only a distinct current recipient MQ can produce an uncounted fresh candidate.
use super::*;
use std::time::{Duration, Instant};

/// This owns the actual native preparation, original State/A/lease and ONE
/// physical pin. Retention grants neither quorum membership nor publication.
pub(crate) struct NativeJointPreparationCustody {
    actual: Arc<ActualServiceNativeJointPreparedEvidence>,
    service: Arc<Mutex<ClusterDataNodeService>>,
}
impl NativeJointPreparationCustody {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn remote_release_vote(
        &self,
        metadata_term: u64,
        metadata_read_index: u64,
        metadata_context_id: Uuid,
    ) -> Result<NativeJointPreparedServingVoteData> {
        self.require_original_caller()?;
        let prepared = self.actual.prepared();
        Ok(NativeJointPreparedServingVoteData {
            preparation_sha256: prepared.checksum_sha256().into(),
            native_database_incarnation: prepared.native_database_incarnation().into(),
            actual_local_visibility_sequence: prepared.actual_local_visibility_sequence(),
            metadata_term,
            metadata_read_index,
            metadata_context_id,
        })
    }
    /// SAME actual reference, borrowed only for the fresh release comparison.
    /// This creates neither a Frozen clone nor a release admission.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn with_original_remote_release_owner<T>(
        &self,
        native: &crate::db::NativeJointServingBridgeReceipt,
        operation: impl for<'a> FnOnce(
            &Arc<Mutex<ClusterDataNodeService>>,
            crate::distribution_data::NativeJointReleaseOwnerView<'a>,
        ) -> Result<T>,
    ) -> Result<T> {
        self.require_original_caller()?;
        if native.data() != self.actual.committed().data()
            || native.native_preparation_sha256() != self.actual.prepared().checksum_sha256()
        {
            return Err(invalid(
                "release comparison changed original prepared/native owner",
            ));
        }
        self.actual
            .with_original_remote_release_owner(&self.service, operation)
    }

    #[cfg(feature = "tls-replication")]
    pub(crate) fn retain_remote_release_successor(
        self: &Arc<Self>,
        participant: &Arc<crate::distribution_range_handoff::NativeRemoteNewParticipant>,
        process: Arc<crate::distribution_native_host::NativeRemotePreparationProcess>,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        activation: Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        survivor: Option<Arc<crate::distribution_range_handoff::NativeJointHandoffRuntime>>,
    ) -> Result<Arc<crate::distribution_data::NativeRemoteReleaseOwner>> {
        crate::distribution_data::NativeRemoteReleaseOwner::retain_original(
            self.clone(),
            &self.actual,
            self.service.clone(),
            participant,
            process,
            native,
            activation,
            survivor,
        )
    }

    /// A private one-way custody move under the actual remote admission. A
    /// busy Arc is returned unchanged to the already-retained successor.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn try_into_remote_release_parts(
        self: Arc<Self>,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        successor: &crate::distribution_data::NativeRemoteReleaseOwner,
    ) -> std::result::Result<
        (
            Arc<ActualServiceNativeJointPreparedEvidence>,
            Arc<Mutex<ClusterDataNodeService>>,
        ),
        (BicDbError, Arc<Self>),
    > {
        if let Err(error) = admission.require_remote_successor(successor) {
            return Err((error, self));
        }
        match Arc::try_unwrap(self) {
            Ok(actual) => Ok((actual.actual, actual.service)),
            Err(original) => Err((
                invalid("release waits for original preparation custody readers"),
                original,
            )),
        }
    }
    /// Infallible custody transfer immediately after the genuine producer.
    /// The service association is UNTRUSTED until the full count boundary.
    pub(crate) fn retain(
        actual: ActualServiceNativeJointPreparedEvidence,
        service: Arc<Mutex<ClusterDataNodeService>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            actual: Arc::new(actual),
            service,
        })
    }
    /// After immediate actual-owner retention, before the original producer
    /// returns. Full strict evidence validation; no cached current claim.
    pub(crate) fn require_initial_return_with_service(
        &self,
        service: &ClusterDataNodeService,
    ) -> Result<()> {
        if !std::ptr::eq(service, self.service.data_ptr()) {
            return Err(invalid(
                "remote preparation substituted original service at return",
            ));
        }
        self.require_original_caller()?;
        self.actual.require_current(service)?;
        self.require_original_caller()
    }
    pub(super) fn require_original_caller(&self) -> Result<()> {
        self.actual.require_original_preparation_session()
    }
    pub(super) fn require_actual(
        &self,
        actual: &Arc<ActualServiceNativeJointPreparedEvidence>,
    ) -> Result<()> {
        if !Arc::ptr_eq(actual, &self.actual) {
            return Err(invalid(
                "fresh preparation replaced its original native custody",
            ));
        }
        self.require_original_caller()
    }
    pub(crate) fn require_actual_evidence(
        &self,
        actual: &ActualServiceNativeJointPreparedEvidence,
    ) -> Result<()> {
        if !std::ptr::eq(actual, self.actual.as_ref()) {
            return Err(invalid(
                "preparation custody substituted original actual completed owner",
            ));
        }
        self.require_original_caller()
    }
    pub(crate) fn local_node_id(&self) -> &ClusterNodeId {
        self.actual.prepared().local_node_id()
    }
    pub(super) fn producer_memory_lifetime(&self) -> crate::cancellation::CancellationLifetime {
        self.actual.preparation_memory_lifetime()
    }
    pub(super) fn require_service(
        &self,
        service: &Arc<Mutex<ClusterDataNodeService>>,
    ) -> Result<()> {
        if !Arc::ptr_eq(service, &self.service) {
            return Err(invalid(
                "fresh preparation substituted original actual service association",
            ));
        }
        self.require_original_caller()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn fresh_candidate(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointPreparedServingCandidate> {
        // A fresh request may not detach from the ORIGINAL session ancestry.
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        let actor = client.local_metadata_read_participant();
        let data = self.actual.committed().data();
        if actor.node_id() != self.local_node_id()
            || data
                .new_members()
                .get(actor.node_id())
                .is_none_or(|member| member.incarnation != actor.member_incarnation())
        {
            return Err(invalid(
                "fresh preparation changed actual recipient/incarnation",
            ));
        }
        let budget = request
            .deadline()
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| invalid("fresh preparation needs a bounded original request"))?
            .min(Duration::from_secs(5));
        let business = Uuid::now_v7();
        // Actual TLS OWN and MStore acquisition precede all engine guards.
        let metadata = client.acquire_host_metadata_observation(
            metadata_leader,
            business,
            budget,
            &request,
        )?;
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            &request,
        )?;
        let fresh = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh preparation actual service busy"))?;
            self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?
        };
        request.check()?;
        self.require_original_caller()?;
        // Still UNCOUNTED: the original round record performs the complete
        // protected native CURRENT/archive/current-State check and final guard.
        Ok(NativeJointPreparedServingCandidate {
            actual: self.actual.clone(),
            service: self.service.clone(),
            fresh: Some(fresh),
            custody: Some(self.clone()),
        })
    }
    #[cfg(test)]
    pub(crate) fn test_original_candidate(self: &Arc<Self>) -> NativeJointPreparedServingCandidate {
        NativeJointPreparedServingCandidate {
            actual: self.actual.clone(),
            service: self.service.clone(),
            fresh: None,
            custody: Some(self.clone()),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_evidence(&self) -> serde_json::Value {
        serde_json::json!({"node":self.local_node_id(),
            "original_metadata_context":self.actual.committed().metadata_anchor().2,
            "native_preparation_sha256":self.actual.prepared().checksum_sha256(),
            "actual_preparation_incarnation":self.actual.prepared().native_database_incarnation(),
            "original_session_error":self.actual.require_original_preparation_session().err().map(|e|e.to_string()),
            "original_metadata_remaining_us":self.actual.committed().deadline().saturating_duration_since(Instant::now()).as_micros(),
            "original_request":self.actual.committed().test_request_memory(),
            "custody_is_not_metadata_authority":true})
    }
    #[cfg(test)]
    pub(crate) fn test_native_evidence(&self) -> Result<serde_json::Value> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("preparation forensic service busy"))?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| invalid("preparation forensic DB busy"))?;
        Ok(serde_json::json!({"custody":self.test_evidence(),
            "actual_protected":database.test_joint_preparation_record(self.actual.committed().data().old_range().id)?}))
    }
    #[cfg(test)]
    pub(crate) fn test_remove_actual_archive(
        &self,
        parent: &crate::CancellationToken,
    ) -> Result<serde_json::Value> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("preparation archive fault service busy"))?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| invalid("preparation archive fault DB busy"))?;
        database.test_remove_joint_preparation_archive(self.actual.prepared(), parent)
    }
}

/// A genuine actual native preparation freshly reobserved on its own service.
/// No constructor from vote DATA, and no local quorum/admission conversion.
pub(crate) struct NativeRemotePreparedObservation {
    actual: NativeJointPreparedServingObservation,
    vote: NativeJointPreparedServingVoteData,
}
impl NativeRemotePreparedObservation {
    pub(crate) fn require_current(&self) -> Result<()> {
        self.actual.require_current()
    }
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_original_remote_participant(&self, sha: &str) -> Result<()> {
        self.actual
            .actual
            .require_remote_preparation_participant(sha)
    }
    pub(crate) fn vote(&self) -> &NativeJointPreparedServingVoteData {
        &self.vote
    }
}
impl NativeJointPreparationCustody {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_remote_preparation(
        self: &Arc<Self>,
        client: &crate::distribution_transport::TcpClusterRelocationTransport,
        metadata_leader: &ClusterNodeId,
        parent: &crate::CancellationToken,
    ) -> Result<NativeRemotePreparedObservation> {
        let candidate = self.fresh_candidate(client, metadata_leader, parent)?;
        let actual = NativeJointPreparedServingObservation {
            actual: candidate.actual,
            service: candidate.service,
            fresh: candidate.fresh,
            custody: candidate.custody,
        };
        actual.require_current()?;
        let prepared = actual.actual.prepared();
        let (term, index, context) = actual.committed().metadata_anchor();
        let vote = NativeJointPreparedServingVoteData {
            preparation_sha256: prepared.checksum_sha256().into(),
            native_database_incarnation: prepared.native_database_incarnation().into(),
            actual_local_visibility_sequence: prepared.actual_local_visibility_sequence(),
            metadata_context_id: context,
            metadata_term: term,
            metadata_read_index: index,
        };
        let guard = actual.committed().native_publication_guard()?;
        actual.committed().require_publication_request_memory()?;
        actual
            .custody
            .as_ref()
            .ok_or_else(|| invalid("remote preparation observation original custody absent"))?
            .require_original_caller()?;
        drop(guard);
        Ok(NativeRemotePreparedObservation { actual, vote })
    }
}

impl NativeRemotePreparedObservation {
    pub(crate) fn require_request_memory(&self) -> Result<()> {
        self.actual.committed().require_publication_point_memory()?;
        self.actual
            .custody
            .as_ref()
            .ok_or_else(|| invalid("remote prepared observation original custody absent"))?
            .require_original_caller()
    }
}
impl NativeJointPreparationCustody {
    pub(crate) fn require_observation_owner(
        &self,
        observation: &NativeRemotePreparedObservation,
    ) -> Result<()> {
        if observation
            .actual
            .custody
            .as_ref()
            .is_none_or(|actual| !std::ptr::eq(actual.as_ref(), self))
        {
            return Err(invalid(
                "remote preparation resolution substituted original completed native owner",
            ));
        }
        self.require_original_caller()
    }
}

impl NativeJointPreparationCustody {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn require_remote_participant(&self, sha: &str) -> Result<()> {
        self.require_original_caller()?;
        self.actual.require_remote_preparation_participant(sha)
    }
}

/// A separate remote publisher over actual authenticated ALL preparations and
/// this recipient's SAME actual preparation. No local QQ is reconstructed.
#[cfg(feature = "tls-replication")]
pub(crate) struct NativeRemoteBridgeAdmission<'a> {
    quorum: Arc<crate::distribution_range_handoff::NativeRemotePreparedQuorum>,
    local: NativeRemotePreparedObservation,
    request: crate::distribution_native_host::NativeRemoteBridgeRequestAdmission<'a>,
}
#[cfg(feature = "tls-replication")]
impl crate::distribution_range_handoff::NativeRemotePreparedQuorum {
    pub(crate) fn admit_local_bridge<'a>(
        self: &Arc<Self>,
        local: NativeRemotePreparedObservation,
        request: crate::distribution_native_host::NativeRemoteBridgeRequestAdmission<'a>,
    ) -> Result<NativeRemoteBridgeAdmission<'a>> {
        self.require_local(local.actual.committed(), &local)?;
        request.require_current(local.actual.committed())?;
        let actual = NativeRemoteBridgeAdmission {
            quorum: self.clone(),
            local,
            request,
        };
        actual.require_live()?;
        // Only this recipient's owned pin drains; actual foreign preparation
        // frames remain live, and all original Frozen ownership is retained.
        actual.local.actual.release_read_pin()?;
        actual.require_live()?;
        Ok(actual)
    }
}
#[cfg(feature = "tls-replication")]
impl NativeRemoteBridgeAdmission<'_> {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.request.require_memory_current()?;
        self.quorum.require_local_memory(&self.local)
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        self.local.actual.committed()
    }
    pub(crate) fn votes(&self) -> BTreeMap<ClusterNodeId, NativeJointPreparedServingVoteData> {
        self.quorum.votes()
    }
    pub(crate) fn bound_request(&self) -> crate::CancellationToken {
        self.request.bound_request()
    }
    pub(crate) fn require_service(&self, service: &ClusterDataNodeService) -> Result<()> {
        self.require_live()?;
        if !std::ptr::eq(service, self.local.actual.service.data_ptr()) {
            return Err(invalid(
                "remote bridge substituted actual local preparation service",
            ));
        }
        self.local.actual.require_current_with_service(service)?;
        self.request.require_current(self.committed())
    }
    pub(crate) fn actual(&self) -> &ActualServiceNativeJointPreparedEvidence {
        &self.local.actual.actual
    }
    pub(crate) fn require_scope(
        &self,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_live()?;
        self.committed()
            .require_current_with_publication_scope(scope)?;
        self.local
            .actual
            .require_local_publication_scope(scope, store, self.committed())?;
        self.require_live()
    }
    pub(crate) fn require_native_local(
        &self,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        prepared: &crate::db::NativeJointServingPreparation,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.quorum
            .require_native_local(db, store, prepared, frozen)
    }
}

#[cfg(feature = "tls-replication")]
impl NativeRemotePreparedObservation {
    pub(crate) fn metadata_anchor(&self) -> (u64, u64, Uuid) {
        self.actual.committed().metadata_anchor()
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_protected_wire_boundary(
        &self,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        own: &crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        metadata: &crate::distribution_data::ActualNativeJointMetadataPoint,
        parent: &crate::CancellationToken,
    ) -> Result<()> {
        let custody = self
            .actual
            .custody
            .as_ref()
            .ok_or_else(|| invalid("remote prepared wire original custody missing"))?;
        custody.require_actual(&self.actual.actual)?;
        let service = self
            .actual
            .service
            .try_lock()
            .ok_or_else(|| invalid("remote prepared wire original service busy"))?;
        custody.require_service(&self.actual.service)?;
        self.actual.actual.require_remote_prepared_wire_boundary(
            &service,
            self.actual.committed(),
            custody,
            role,
            own,
            business,
            metadata,
            parent,
        )
    }
}

#[cfg(feature = "tls-replication")]
pub(crate) struct NativeRemoteBridgeReadAdmission<'a> {
    quorum: Arc<crate::distribution_range_handoff::NativeRemotePreparedQuorum>,
    local: NativeRemotePreparedObservation,
    request: crate::distribution_native_host::NativeRemoteBridgeReadRequestAdmission<'a>,
}
#[cfg(feature = "tls-replication")]
impl crate::distribution_range_handoff::NativeRemotePreparedQuorum {
    pub(crate) fn admit_local_bridge_read<'a>(
        self: &Arc<Self>,
        local: NativeRemotePreparedObservation,
        request: crate::distribution_native_host::NativeRemoteBridgeReadRequestAdmission<'a>,
    ) -> Result<NativeRemoteBridgeReadAdmission<'a>> {
        self.require_local(local.actual.committed(), &local)?;
        request.require_current(local.actual.committed())?;
        let actual = NativeRemoteBridgeReadAdmission {
            quorum: self.clone(),
            local,
            request,
        };
        actual.require_live()?;
        // Read-only: no pin acquisition/drain, Controlled permit or mutation.
        actual.require_live()?;
        Ok(actual)
    }
}
#[cfg(feature = "tls-replication")]
impl NativeRemoteBridgeReadAdmission<'_> {
    pub(crate) fn require_live(&self) -> Result<()> {
        self.request.require_memory_current()?;
        self.quorum.require_local_memory(&self.local)
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        self.local.actual.committed()
    }
    pub(crate) fn bound_request(&self) -> crate::CancellationToken {
        self.request.bound_request()
    }
    pub(crate) fn require_service(&self, service: &ClusterDataNodeService) -> Result<()> {
        self.require_live()?;
        if !std::ptr::eq(service, self.local.actual.service.data_ptr()) {
            return Err(invalid(
                "remote bridge substituted actual local preparation service",
            ));
        }
        self.local.actual.require_current_with_service(service)?;
        self.request.require_current(self.committed())
    }
    pub(crate) fn actual(&self) -> &ActualServiceNativeJointPreparedEvidence {
        &self.local.actual.actual
    }
    pub(crate) fn require_scope(
        &self,
        scope: &crate::db::NativeJointPreparedPublicationScope<'_>,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<()> {
        self.require_live()?;
        self.committed()
            .require_current_with_publication_scope(scope)?;
        self.local
            .actual
            .require_local_publication_scope(scope, store, self.committed())?;
        self.require_live()
    }
    pub(crate) fn require_native_local(
        &self,
        db: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        prepared: &crate::db::NativeJointServingPreparation,
        frozen: &crate::distribution_range_mutation::FrozenRangeMutationPermit,
    ) -> Result<()> {
        self.quorum
            .require_native_local(db, store, prepared, frozen)
    }
}

#[cfg(all(test, feature = "tls-replication"))]
impl NativeRemoteBridgeAdmission<'_> {
    pub(crate) fn test_cancel_original_server_request(&self) -> serde_json::Value {
        self.request.test_cancel_original_server_request()
    }
}

#[cfg(feature = "tls-replication")]
impl NativeRemoteBridgeReadAdmission<'_> {
    pub(crate) fn require_participant(&self, sha: &str) -> Result<()> {
        self.require_live()?;
        self.local.require_original_remote_participant(sha)
    }
}

impl NativeJointPreparationCustody {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn reconcile_remote_open(
        &self,
        request: &crate::distribution_native_host::NativeRemoteOpenRequestAdmission,
        native: &crate::db::NativeJointServingBridgeReceipt,
    ) -> Result<()> {
        self.require_original_caller()?;
        let mut service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("Open reconcile original service busy"))?;
        request.reconcile_actual_preparation(&mut service, &self.actual, native)
    }
}

#[cfg(test)]
impl NativeJointPreparationCustody {
    pub(crate) fn test_remote_open_io(
        &self,
        native: &crate::db::NativeJointServingBridgeReceipt,
        fail: bool,
    ) -> Result<serde_json::Value> {
        self.require_original_caller()?;
        let mut service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("Open test original service busy"))?;
        self.actual.test_remote_open_io(&mut service, native, fail)
    }
}
