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
