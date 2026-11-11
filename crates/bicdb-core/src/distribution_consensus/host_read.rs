//! Fresh whole-metadata observation for a registered host, including learners.
//! This is not range leadership, recovery admission or a row-read permission.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostMetadataReadAcquireRequest {
    pub protocol_version: u32,
    pub acquisition_id: Uuid,
    pub read_request_id: Uuid,
    pub requester_node_id: ClusterNodeId,
    pub requester_incarnation: u64,
    pub budget_ms: u64,
}
impl HostMetadataReadAcquireRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol_version != METADATA_READ_ACQUISITION_PROTOCOL_VERSION
            || self.acquisition_id.is_nil()
            || self.read_request_id.is_nil()
            || self.requester_incarnation == 0
            || !(1..=5000).contains(&self.budget_ms)
        {
            return Err(metadata_error("invalid host metadata acquisition request"));
        }
        Ok(())
    }
    fn validate_member(&self, topology: &ClusterTopology) -> Result<()> {
        self.validate()?;
        let node = topology
            .nodes
            .get(&self.requester_node_id)
            .ok_or_else(|| metadata_error("host metadata requester is not registered"))?;
        if node.incarnation != self.requester_incarnation
            || node.lifecycle != ClusterNodeLifecycle::Active
        {
            return Err(metadata_error(
                "host metadata requester incarnation/lifecycle changed",
            ));
        }
        // Both Voter and Learner are deliberately eligible. No data-range or
        // metadata vote is conferred by an observation of committed metadata.
        Ok(())
    }
    pub(crate) fn subject(&self, topology: &ClusterTopology) -> Result<MetadataReadAcquireRequest> {
        self.validate_member(topology)?;
        let range = topology
            .ranges
            .values()
            .next()
            .ok_or_else(|| metadata_error("host metadata topology has no ranges"))?;
        // Internal evidence carrier only, selected by the actual provider.
        // These fields are never exposed as a generic range observation.
        Ok(MetadataReadAcquireRequest {
            protocol_version: self.protocol_version,
            acquisition_id: self.acquisition_id,
            read_request_id: self.read_request_id,
            requester_node_id: self.requester_node_id.clone(),
            requester_incarnation: self.requester_incarnation,
            range_id: range.id,
            range_epoch: range.epoch,
            budget_ms: self.budget_ms,
        })
    }
    fn require_subject(&self, subject: &MetadataReadAcquireRequest) -> Result<()> {
        self.validate()?;
        subject.validate()?;
        if subject.protocol_version != self.protocol_version
            || subject.acquisition_id != self.acquisition_id
            || subject.read_request_id != self.read_request_id
            || subject.requester_node_id != self.requester_node_id
            || subject.requester_incarnation != self.requester_incarnation
            || subject.budget_ms != self.budget_ms
        {
            return Err(metadata_error(
                "host metadata evidence request was relabeled",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostMetadataReadAcquireResponse {
    pub request: HostMetadataReadAcquireRequest,
    pub evidence: MetadataReadAcquireResponse,
}
impl HostMetadataReadAcquireResponse {
    pub(crate) fn validate(&self, request: &HostMetadataReadAcquireRequest) -> Result<()> {
        if &self.request != request {
            return Err(metadata_error(
                "host metadata response does not echo its request",
            ));
        }
        request.require_subject(&self.evidence.request)?;
        self.evidence
            .validate_quorum_evidence(&self.evidence.request)?;
        request.validate_member(&self.evidence.committed_topology)
    }
}

impl MetadataConsensusStore {
    pub(crate) fn validate_host_metadata_caller(
        &self,
        host: &HostMetadataReadAcquireRequest,
        subject: &MetadataReadAcquireRequest,
        caller: &MetadataReadParticipant,
    ) -> Result<crate::RangeDescriptor> {
        host.require_subject(subject)?;
        host.validate_member(&self.state.committed_topology)?;
        let node = &self.state.committed_topology.nodes[&host.requester_node_id];
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if caller.node_id != host.requester_node_id || !identity.accepts(caller) {
            return Err(metadata_error(
                "host metadata caller is not transport-authenticated",
            ));
        }
        Ok(self
            .state
            .committed_topology
            .ranges
            .values()
            .next()
            .ok_or_else(|| metadata_error("host metadata topology has no ranges"))?
            .clone())
    }
}
impl MetadataReadAuthority {
    pub(crate) fn into_host_metadata_response(
        self,
        request: HostMetadataReadAcquireRequest,
        subject: MetadataReadAcquireRequest,
    ) -> Result<HostMetadataReadAcquireResponse> {
        request.require_subject(&subject)?;
        if self.read_request_id != request.read_request_id {
            return Err(metadata_error(
                "host metadata authority belongs to another request",
            ));
        }
        let evidence = self.into_quorum_evidence(subject)?;
        let response = HostMetadataReadAcquireResponse { request, evidence };
        response.validate(&response.request)?;
        Ok(response)
    }
}

/// Request-bound actual transport observation. No Clone, Deserialize, numeric
/// constructor, generic range-observation conversion, or role conversion.
#[derive(Debug)]
pub(crate) struct HostMetadataReadObservation {
    response: HostMetadataReadAcquireResponse,
    deadline: Instant,
    cancellation: crate::CancellationToken,
}
impl HostMetadataReadObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        authenticated: crate::distribution_transport::AuthenticatedHostMetadataReadReply,
        request: &HostMetadataReadAcquireRequest,
    ) -> Result<Self> {
        let (
            response,
            participant,
            original_request,
            cluster,
            destination,
            started,
            deadline,
            cancellation,
        ) = authenticated.into_parts();
        cancellation.check()?;
        if &original_request != request {
            return Err(metadata_error(
                "actual host transport request cannot be relabeled",
            ));
        }
        response.validate(request)?;
        let evidence = &response.evidence;
        let deadline = deadline.min(started + Duration::from_millis(evidence.context_budget_ms));
        if Instant::now() >= deadline
            || evidence.committed_topology.cluster_id != cluster
            || evidence.metadata_leader_id != destination
            || participant.node_id != destination
            || participant.member_incarnation != evidence.metadata_leader_incarnation
        {
            return Err(metadata_error(
                "host metadata source/identity/deadline differs",
            ));
        }
        let node = &evidence.committed_topology.nodes[&destination];
        let identity = MetadataReadVoterIdentity {
            incarnation: node.incarnation,
            active_certificate: node.tls_certificate_sha256.clone(),
            pending_certificate: node.pending_tls_certificate_sha256.clone(),
        };
        if !identity.accepts(&participant) {
            return Err(metadata_error("host metadata source certificate differs"));
        }
        let cancellation = cancellation.child_with_deadline(deadline);
        cancellation.check()?;
        Ok(Self {
            response,
            deadline,
            cancellation,
        })
    }
    pub(crate) fn require_live_request(&self, business: Uuid) -> Result<()> {
        self.cancellation.check()?;
        if business != self.response.request.read_request_id || Instant::now() >= self.deadline {
            return Err(metadata_error(
                "host metadata observation request changed or expired",
            ));
        }
        Ok(())
    }
    pub(crate) fn committed_topology(&self, business: Uuid) -> Result<&ClusterTopology> {
        self.require_live_request(business)?;
        Ok(&self.response.evidence.committed_topology)
    }
    pub(crate) fn restore_activation(
        &self,
        business: Uuid,
    ) -> Result<Option<&MetadataRestoreActivation>> {
        self.require_live_request(business)?;
        Ok(self.response.evidence.restore_activation.as_ref())
    }
    pub(crate) fn requester_node_id(&self) -> &ClusterNodeId {
        &self.response.request.requester_node_id
    }
    pub(crate) fn requester_incarnation(&self) -> u64 {
        self.response.request.requester_incarnation
    }
    pub(crate) fn context_id(&self) -> Uuid {
        self.response.evidence.context_id
    }
    pub(crate) fn term(&self) -> u64 {
        self.response.evidence.term
    }
    pub(crate) fn read_index(&self) -> u64 {
        self.response.evidence.read_index
    }
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }
}
