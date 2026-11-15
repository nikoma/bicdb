//! Historical ownership is retained independently of a short metadata request.
//! Only a distinct current recipient MQ can produce an uncounted fresh candidate.
use super::*;
use std::time::{Duration, Instant};

/// This owns the actual native preparation, original State/A/lease and ONE
/// physical pin. Retention grants neither quorum membership nor publication.
pub(crate) struct NativeJointPreparationCustody {
    pub(super) actual: Arc<ActualServiceNativeJointPreparedEvidence>,
    pub(super) service: Arc<Mutex<ClusterDataNodeService>>,
}
impl NativeJointPreparationCustody {
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
    pub(crate) fn data(&self) -> &NativeJointDecisionData {
        self.actual.committed().data()
    }
    /// Historical identity of the retained native preparation. Read from
    /// memory only; it proves nothing current and grants nothing.
    pub(crate) fn retained_facts(&self) -> Result<NativeJointPreparationRetainedFacts> {
        let prepared = self.actual.prepared();
        Ok(NativeJointPreparationRetainedFacts {
            decision_sha256: self.data().checksum_sha256().into(),
            preparation_sha256: prepared.checksum_sha256().into(),
            native_database_incarnation: prepared.native_database_incarnation().into(),
            local_visibility_sequence: prepared.actual_local_visibility_sequence(),
            decision_receipt_sha256: self
                .actual
                .committed()
                .native_receipt()
                .checksum_sha256()
                .into(),
            store_open_id: self.actual.store_open_id(),
            // The registration object captured BEFORE the native preparation.
            writer_identity_sha256: self.actual.original_writer_identity_sha256()?,
        })
    }
    /// The member-local half of one fresh PREPARATION phase: exactly what the
    /// in-process phase round does for this member (`acquire_inner`), run by
    /// the member itself under an observation IT acquired. Read-only: no pin,
    /// freeze, promise or catalogue changes; the result is historical DATA.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_fresh_preparation_phase(
        self: &Arc<Self>,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointPreparationPhaseFacts> {
        self.observe_fresh_phase(None, metadata, business, parent)
    }
    /// The member-local half of one fresh OPEN phase over this member's own
    /// durable bridge: the complete protected check the in-process open
    /// phase performs for this member under its lock. Strictly read-only:
    /// the store's own persistence uncertainty is NOT reconciled here (only
    /// this member's own Open step does that), so an uncertain member
    /// refuses to attest.
    #[cfg(feature = "tls-replication")]
    pub(crate) fn observe_fresh_open_phase(
        self: &Arc<Self>,
        bridge: &Arc<crate::db::NativeJointServingBridgeReceipt>,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointPreparationPhaseFacts> {
        self.observe_fresh_phase(Some(bridge), metadata, business, parent)
    }
    #[cfg(feature = "tls-replication")]
    fn observe_fresh_phase(
        self: &Arc<Self>,
        bridge: Option<&Arc<crate::db::NativeJointServingBridgeReceipt>>,
        metadata: crate::distribution_consensus::HostMetadataReadObservation,
        business: Uuid,
        parent: &crate::CancellationToken,
    ) -> Result<NativeJointPreparationPhaseFacts> {
        // A fresh request may not detach from the ORIGINAL session ancestry.
        let request = self.actual.bound_preparation_request(parent);
        request.check()?;
        self.require_original_caller()?;
        let data = self.data();
        if metadata.requester_node_id() != self.local_node_id()
            || data
                .new_members()
                .get(self.local_node_id())
                .is_none_or(|member| member.incarnation != metadata.requester_incarnation())
        {
            return Err(invalid(
                "fresh preparation phase changed actual recipient/incarnation",
            ));
        }
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &self.service,
            &metadata,
            business,
            &request,
        )?;
        let facts = {
            let service = self
                .service
                .try_lock()
                .ok_or_else(|| invalid("fresh preparation phase actual service busy"))?;
            let fresh = self.actual.observe_fresh_committed_with_point(
                &service,
                metadata,
                business,
                data.old_range().id,
                point,
            )?;
            // The complete protected CURRENT/archive/State/writer check the
            // in-process phase performs for this member under this lock.
            let writer = match bridge {
                None => self
                    .actual
                    .require_current_under_preparation_custody(&service, &fresh, self)?,
                Some(bridge) => {
                    if bridge.data() != data || bridge.local_node_id() != self.local_node_id() {
                        return Err(invalid(
                            "fresh open phase bridge belongs to another decision/member",
                        ));
                    }
                    self.actual.require_current_under_open_preparation_custody(
                        &service,
                        &fresh,
                        bridge.clone(),
                        self,
                    )?
                }
            };
            if fresh.data() != data {
                return Err(invalid(
                    "fresh preparation phase changed protected global decision",
                ));
            }
            fresh.require_current_metadata_only()?;
            let (metadata_term, metadata_read_index, metadata_context_id) = fresh.metadata_anchor();
            let mut retained = self.retained_facts()?;
            // Report the registration this observation itself just proved
            // current under the service lock, not a remembered value.
            retained.writer_identity_sha256 = writer.identity_sha256()?;
            NativeJointPreparationPhaseFacts {
                retained,
                metadata_term,
                metadata_read_index,
                metadata_context_id,
                deadline: fresh.deadline(),
                bridge: bridge.map(|bridge| {
                    (
                        bridge.checksum_sha256().to_owned(),
                        bridge.metadata_anchor(),
                    )
                }),
            }
        };
        request.check()?;
        self.require_original_caller()?;
        Ok(facts)
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
    /// This member's actual protected bridge record as DATA; grants nothing.
    #[cfg(test)]
    pub(crate) fn test_durable_bridge_record(&self) -> Result<Option<serde_json::Value>> {
        let service = self
            .service
            .try_lock()
            .ok_or_else(|| invalid("bridge forensic service busy"))?;
        let database = service.database();
        let database = database
            .try_read()
            .ok_or_else(|| invalid("bridge forensic DB busy"))?;
        database.test_joint_bridge_record(self.data().old_range().id)
    }
    #[cfg(test)]
    pub(crate) fn test_read_pin_released(&self) -> Result<bool> {
        self.actual.read_pin_already_released()
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

/// Historical identity DATA of one retained native preparation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NativeJointPreparationRetainedFacts {
    pub(crate) decision_sha256: String,
    pub(crate) preparation_sha256: String,
    pub(crate) native_database_incarnation: String,
    pub(crate) local_visibility_sequence: u64,
    pub(crate) decision_receipt_sha256: String,
    pub(crate) store_open_id: Uuid,
    /// Digest of the exact writer/owner registration object; see
    /// `NativeJointPhaseOwner::identity_sha256`.
    pub(crate) writer_identity_sha256: String,
}
/// One member-local fresh phase observation: the retained identity plus the
/// metadata anchor and absolute deadline of the observation that re-proved it.
#[derive(Clone, Debug)]
pub(crate) struct NativeJointPreparationPhaseFacts {
    pub(crate) retained: NativeJointPreparationRetainedFacts,
    pub(crate) metadata_term: u64,
    pub(crate) metadata_read_index: u64,
    pub(crate) metadata_context_id: Uuid,
    pub(crate) deadline: Instant,
    /// OPEN phase only: this member's own durable bridge receipt checksum
    /// and the post-quorum metadata anchor recorded in it.
    pub(crate) bridge: Option<(String, (u64, u64, Uuid))>,
}
