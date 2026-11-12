//! Existing-cluster learner factory FACTS, never initial-all-C or enrollment
//! authority. Both the receiver and requester retain their own real Host MQ.
use super::*;
use crate::distribution::{ClusterNetworkTransportConfig, NativeStoreBirthV1};
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataReadParticipant};
use crate::distribution_data::ClusterServiceOpenGuard;
use crate::distribution_transport::{ClusterDataServerHandle, TcpClusterRelocationTransport};
use parking_lot::Mutex;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeLearnerBirthRequest {
    pub version: u32,
    pub challenge: Uuid,
    pub business: Uuid,
    pub requester: ClusterNodeId,
    pub requester_incarnation: u64,
    pub requester_metadata_context: Uuid,
    pub requester_metadata_term: u64,
    pub requester_metadata_read_index: u64,
    pub bootstrap_id: Uuid,
    pub restore_activation: Option<crate::distribution_consensus::MetadataRestoreActivation>,
    pub target: ClusterNodeId,
    pub target_incarnation: u64,
    pub expected_birth: Option<NativeStoreBirthV1>,
    pub expected_schema_sha256: String,
    pub budget_ms: u64,
}
impl NativeLearnerBirthRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.challenge.is_nil()
            || self.business.is_nil()
            || self.requester_metadata_context.is_nil()
            || self.requester_incarnation == 0
            || self.requester_metadata_term == 0
            || self.requester_metadata_read_index == 0
            || self.bootstrap_id.is_nil()
            || self.target_incarnation == 0
            || !(1..=5000).contains(&self.budget_ms)
            || self.expected_schema_sha256.len() != 64
            || !self
                .expected_schema_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid bounded existing-learner birth request"));
        }
        if let Some(birth) = &self.expected_birth {
            birth.validate_for(Some(self.bootstrap_id), self.target_incarnation)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeLearnerBirthResponse {
    pub request: NativeLearnerBirthRequest,
    pub cluster: crate::ClusterId,
    pub node: ClusterNodeId,
    pub incarnation: u64,
    pub birth: NativeStoreBirthV1,
    pub actual_schema_sha256: String,
    // Audit labels only. They cannot construct HostMQ or a metadata ACK.
    pub source_business: Uuid,
    pub source_metadata_context: Uuid,
    pub source_metadata_term: u64,
    pub source_metadata_read_index: u64,
    pub source_remaining_budget_ms: u64,
}
struct SourceMetadata {
    observed: HostMetadataReadObservation,
    business: Uuid,
}
pub(crate) struct NativeLearnerBirthProvider {
    factory: Arc<NativeUnassignedLearnerFactory>,
    metadata: Mutex<SourceMetadata>,
    network: ClusterNetworkTransportConfig,
    actual_open: ClusterServiceOpenGuard,
    schema: crate::SchemaCompatibilityAuthority,
    schema_sha256: String,
    birth: NativeStoreBirthV1,
}
impl NativeLearnerBirthProvider {
    /// Identity derives from this receiver's OWN live HostMQ and the actual
    /// presented TLS certificate, never the request's incarnation label.
    pub(crate) fn authenticated_peer(
        &self,
        caller: &ClusterNodeId,
        presented: Option<&str>,
        plaintext: bool,
    ) -> Result<MetadataReadParticipant> {
        let source = self
            .metadata
            .try_lock()
            .ok_or_else(|| invalid("learner source metadata busy"))?;
        source.observed.require_live_request(source.business)?;
        self.actual_open.require_live()?;
        let topology = source.observed.committed_topology(source.business)?;
        let member = topology
            .nodes
            .get(caller)
            .ok_or_else(|| invalid("learner birth actual caller absent"))?;
        if plaintext
            || member.lifecycle != ClusterNodeLifecycle::Active
            || member.metadata_role != MetadataMemberRole::Voter
            || !presented.is_some_and(|pin| member.accepts_tls_certificate_sha256(pin))
        {
            return Err(invalid(
                "learner birth requires an actual pinned registered voter",
            ));
        }
        Ok(MetadataReadParticipant::from_transport(
            caller.clone(),
            member.incarnation,
            presented.map(str::to_owned),
            false,
        ))
    }
    pub(crate) fn require_same_database(
        &self,
        database: &Arc<RwLock<BicDb>>,
        cluster: &crate::ClusterId,
        node: &ClusterNodeId,
    ) -> Result<()> {
        self.actual_open.require_live()?;
        if !Arc::ptr_eq(database, &self.factory.database)
            || cluster != &self.factory.spec.cluster_id
            || node != &self.factory.spec.node_id
        {
            return Err(invalid(
                "learner-birth provider belongs to another actual service/open",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe(
        &self,
        peer: &MetadataReadParticipant,
        request: NativeLearnerBirthRequest,
        parent: &CancellationToken,
    ) -> Result<NativeLearnerBirthResponse> {
        request.validate()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        token.check()?;
        let source = self
            .metadata
            .try_lock()
            .ok_or_else(|| invalid("learner source metadata busy"))?;
        source.observed.require_live_request(source.business)?;
        self.actual_open.require_metadata_destination(
            &self.factory.spec.cluster_id,
            &self.factory.spec.node_id,
            Some(&self.birth),
            false,
        )?;
        let topology = source.observed.committed_topology(source.business)?;
        let caller = topology
            .nodes
            .get(&request.requester)
            .ok_or_else(|| invalid("learner birth requester absent"))?;
        if request.target != self.factory.spec.node_id
            || request.target_incarnation != self.factory.spec.member_incarnation
            || request.bootstrap_id != self.factory.spec.bootstrap_id
            || request.restore_activation != self.factory.spec.restore_activation
            || request.restore_activation.as_ref()
                != source.observed.restore_activation(source.business)?
            || request.expected_schema_sha256 != self.schema_sha256
            || request
                .expected_birth
                .as_ref()
                .is_some_and(|birth| birth != &self.birth)
            || caller.lifecycle != ClusterNodeLifecycle::Active
            || caller.metadata_role != MetadataMemberRole::Voter
            || caller.incarnation != request.requester_incarnation
            || peer.node_id() != &request.requester
            || peer.member_incarnation() != request.requester_incarnation
            || peer.plaintext_loopback()
            || !peer
                .certificate_sha256()
                .is_some_and(|pin| caller.accepts_tls_certificate_sha256(pin))
            || self.schema.verified_sha256().as_deref() != Some(self.schema_sha256.as_str())
        {
            return Err(invalid(
                "learner birth actual peer/factory/schema scope differs",
            ));
        }
        // Existing typed local producer rechecks actual physical factory,
        // no metadata initialization, no data assignment, schema and birth.
        // Its resulting proposal is never returned or cast into authority.
        let proposal =
            self.factory
                .enrollment_proposal(&source.observed, source.business, &self.network)?;
        if proposal.nodes[&self.factory.spec.node_id]
            .native_store_birth_v1
            .as_ref()
            != Some(&self.birth)
        {
            return Err(invalid(
                "learner birth changed during actual factory validation",
            ));
        }
        source.observed.require_live_request(source.business)?;
        let remaining = source
            .observed
            .deadline()
            .checked_duration_since(Instant::now())
            .ok_or_else(|| invalid("learner source metadata expired"))?;
        let response = NativeLearnerBirthResponse {
            request,
            cluster: self.factory.spec.cluster_id.clone(),
            node: self.factory.spec.node_id.clone(),
            incarnation: self.factory.spec.member_incarnation,
            birth: self.birth.clone(),
            actual_schema_sha256: self.schema_sha256.clone(),
            source_business: source.business,
            source_metadata_context: source.observed.context_id(),
            source_metadata_term: source.observed.term(),
            source_metadata_read_index: source.observed.read_index(),
            source_remaining_budget_ms: u64::try_from(remaining.as_millis())
                .unwrap_or(5000)
                .min(5000),
        };
        token.check()?;
        Ok(response)
    }
}

/// Same service/store, permanently provisional listener. No metadata backend,
/// coordinator, ordinary read/write API or numeric birth constructor is exposed.
pub(crate) struct NativeUnassignedLearnerHost {
    provider: Arc<NativeLearnerBirthProvider>,
    listener: Option<ClusterDataServerHandle>,
    _service: Arc<Mutex<crate::ClusterDataNodeService>>,
}
impl NativeUnassignedLearnerHost {
    #[cfg(test)]
    pub(super) fn test_service(&self) -> Arc<Mutex<crate::ClusterDataNodeService>> {
        Arc::clone(&self._service)
    }
    #[cfg(test)]
    pub(super) fn test_take_listener(&mut self) -> ClusterDataServerHandle {
        self.listener.take().expect("actual test listener")
    }
    pub(crate) fn start(
        factory: Arc<NativeUnassignedLearnerFactory>,
        observed: HostMetadataReadObservation,
        business: Uuid,
        network: ClusterNetworkTransportConfig,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        let proposal = factory.enrollment_proposal(&observed, business, &network)?;
        let topology = observed.committed_topology(business)?.clone();
        let birth = proposal.nodes[&factory.spec.node_id]
            .native_store_birth_v1
            .clone()
            .ok_or_else(|| invalid("actual learner proposal lacks birth"))?;
        let mut service = crate::ClusterDataNodeService::open(
            factory.spec.cluster_id.clone(),
            factory.spec.node_id.clone(),
            factory
                .database
                .try_read()
                .ok_or_else(|| invalid("learner DB busy at host start"))?
                .data_path(),
            Arc::clone(&factory.database),
            true,
        )?;
        let (schema, schema_sha256) = {
            let db = factory
                .database
                .try_read()
                .ok_or_else(|| invalid("learner DB busy before listener"))?;
            (
                db.schema_compatibility_authority(),
                db.schema_compatibility_fingerprint()?.sha256,
            )
        };
        let actual_open = service.listener_open_guard();
        let address: std::net::SocketAddr = topology.nodes[&factory.spec.node_id]
            .address
            .parse()
            .map_err(|_| {
            invalid("learner provisional listener needs a numeric registered endpoint")
        })?;
        let provider = Arc::new(NativeLearnerBirthProvider {
            factory,
            metadata: Mutex::new(SourceMetadata { observed, business }),
            network: network.clone(),
            actual_open,
            schema,
            schema_sha256,
            birth,
        });
        service.attach_learner_birth_provider(&provider)?;
        let service = Arc::new(Mutex::new(service));
        let listener = crate::distribution_transport::start_cluster_data_server(
            address,
            network,
            Arc::new(RwLock::new(topology)),
            Arc::clone(&service),
            None,
            None,
            None,
        )?;
        parent.check()?;
        Ok(Self {
            provider,
            listener: Some(listener),
            _service: service,
        })
    }
    pub(crate) fn refresh_metadata(
        &self,
        observed: HostMetadataReadObservation,
        business: Uuid,
        parent: &CancellationToken,
    ) -> Result<()> {
        parent.check()?;
        let proposal = self.provider.factory.enrollment_proposal(
            &observed,
            business,
            &self.provider.network,
        )?;
        if proposal.nodes[&self.provider.factory.spec.node_id]
            .native_store_birth_v1
            .as_ref()
            != Some(&self.provider.birth)
        {
            return Err(invalid(
                "fresh learner metadata cannot substitute another physical birth",
            ));
        }
        let mut slot = self
            .provider
            .metadata
            .try_lock()
            .ok_or_else(|| invalid("learner source metadata busy"))?;
        observed.require_live_request(business)?;
        parent.check()?;
        *slot = SourceMetadata { observed, business };
        Ok(())
    }
    pub(crate) fn shutdown(mut self) -> Result<()> {
        if let Some(listener) = self.listener.take() {
            listener.join()?;
        }
        Ok(())
    }
    /// A later host transition must use this real drain. On error this owner
    /// retains its stopping listener/counter so no drain fact is fabricated.
    pub(crate) fn stop_and_drain(
        &mut self,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_transport::DrainedClusterDataListener> {
        parent.check()?;
        self.listener
            .as_mut()
            .ok_or_else(|| invalid("learner listener already released"))?
            .stop_and_drain(parent)
    }
}

/// An actual authenticated factory observation; no Deserialize/Clone/raw DTO
/// constructor. It is not a metadata ACK, enrollment, promotion or data grant.
pub(crate) struct NativeLearnerBirthObservation {
    response: NativeLearnerBirthResponse,
    requester_metadata: HostMetadataReadObservation,
    business: Uuid,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl NativeLearnerBirthObservation {
    pub(crate) fn from_transport(
        authenticated: crate::distribution_transport::AuthenticatedNativeLearnerBirthReply,
        requester_metadata: HostMetadataReadObservation,
        business: Uuid,
    ) -> Result<Self> {
        let (response, original, peer, cluster, destination, started, deadline, cancellation) =
            authenticated.into_parts();
        original.validate()?;
        requester_metadata.require_live_request(business)?;
        let topology = requester_metadata.committed_topology(business)?;
        let target = topology
            .nodes
            .get(&destination)
            .ok_or_else(|| invalid("learner peer absent from actual requester metadata"))?;
        let deadline = deadline
            .min(requester_metadata.deadline())
            .min(started + Duration::from_millis(response.source_remaining_budget_ms));
        if response.request != original
            || original.business != business
            || original.requester != *requester_metadata.requester_node_id()
            || original.requester_incarnation != requester_metadata.requester_incarnation()
            || original.requester_metadata_context != requester_metadata.context_id()
            || original.requester_metadata_term != requester_metadata.term()
            || original.requester_metadata_read_index != requester_metadata.read_index()
            || original.restore_activation.as_ref()
                != requester_metadata.restore_activation(business)?
            || topology.range_image_bootstrap_id != Some(original.bootstrap_id)
            || original.target != destination
            || original.target_incarnation != target.incarnation
            || original.expected_birth != target.native_store_birth_v1
            || topology.ranges.is_empty()
            || topology.ranges.values().any(|range| {
                range.required_schema_sha256(&topology.nodes)
                    != Some(original.expected_schema_sha256.as_str())
            })
            || topology.cluster_id != cluster
            || response.cluster != cluster
            || response.node != destination
            || target.lifecycle != ClusterNodeLifecycle::Active
            || target.metadata_role != MetadataMemberRole::Learner
            || topology.ranges.values().any(|r| {
                r.replicas
                    .iter()
                    .any(|replica| replica.node_id == destination)
            })
            || response.incarnation != target.incarnation
            || response.birth.writer_enforcement_version() != 1
            || target
                .native_store_birth_v1
                .as_ref()
                .is_some_and(|birth| birth != &response.birth)
            || peer.node_id() != &destination
            || peer.member_incarnation() != target.incarnation
            || peer.plaintext_loopback()
            || !peer
                .certificate_sha256()
                .is_some_and(|pin| target.accepts_tls_certificate_sha256(pin))
            || response.actual_schema_sha256 != original.expected_schema_sha256
            || response.source_business.is_nil()
            || response.source_metadata_context.is_nil()
            || response.source_metadata_term == 0
            || response.source_metadata_read_index == 0
            || !(1..=5000).contains(&response.source_remaining_budget_ms)
            || Instant::now() >= deadline
        {
            return Err(invalid(
                "actual learner birth exchange scope/provenance differs",
            ));
        }
        response
            .birth
            .validate_for(Some(original.bootstrap_id), target.incarnation)?;
        cancellation.check()?;
        let result = Self {
            response,
            requester_metadata,
            business,
            deadline,
            cancellation,
        };
        result.require_live()?;
        Ok(result)
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        self.requester_metadata
            .require_live_request(self.business)?;
        if Instant::now() >= self.deadline {
            return Err(invalid("learner birth observation expired"));
        }
        Ok(())
    }
    pub(crate) fn birth(&self) -> Result<&NativeStoreBirthV1> {
        self.require_live()?;
        Ok(&self.response.birth)
    }
    pub(crate) fn schema_sha256(&self) -> Result<&str> {
        self.require_live()?;
        Ok(&self.response.actual_schema_sha256)
    }
    pub(crate) fn node(&self) -> Result<&ClusterNodeId> {
        self.require_live()?;
        Ok(&self.response.node)
    }
}
