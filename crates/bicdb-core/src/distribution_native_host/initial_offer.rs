//! Actual still-unregistered factory facts for initial configuration only.
//! A response is data. Only the actual authenticated exchange may seal a peer
//! observation; neither one grants a metadata vote or data/read/write role.
use super::*;
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_consensus::MetadataReadParticipant;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialBirthOfferRequest {
    pub version: u32,
    pub challenge: Uuid,
    pub requester: ClusterNodeId,
    pub requester_incarnation: u64,
    pub plan_sha256: String,
    pub budget_ms: u64,
}
impl NativeInitialBirthOfferRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.challenge.is_nil()
            || self.requester_incarnation == 0
            || !(1..=5000).contains(&self.budget_ms)
            || self.plan_sha256.len() != 64
            || !self
                .plan_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid bounded initial-birth request"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialBirthOfferResponse {
    pub request: NativeInitialBirthOfferRequest,
    pub cluster: crate::ClusterId,
    pub node: ClusterNodeId,
    pub incarnation: u64,
    pub birth: NativeStoreBirthV1,
    pub actual_schema_sha256: String,
    pub planned_local_ranges_sha256: String,
}

fn ranges_sha256(plan: &NativeClusterBootstrapPlan, node: &ClusterNodeId) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
        "bicdb-native-initial-ranges-v1",
        plan.local_spec(node)?.initial_ranges,
    ))?)))
}
pub(super) fn require_pinned_peer(
    peer: &MetadataReadParticipant,
    plan: &NativeClusterBootstrapPlan,
    node: &ClusterNodeId,
    incarnation: u64,
) -> Result<()> {
    let expected = plan
        .topology
        .nodes
        .get(node)
        .ok_or_else(|| invalid("initial birth peer is absent from the operator plan"))?;
    if peer.node_id() != node
        || peer.member_incarnation() != incarnation
        || expected.incarnation != incarnation
        || peer.plaintext_loopback()
        || expected.tls_certificate_sha256.as_deref() != peer.certificate_sha256()
    {
        return Err(invalid(
            "initial birth exchange needs the actual pinned TLS member",
        ));
    }
    Ok(())
}

impl NativeProvisionalNode {
    pub(crate) fn require_same_provisional_database(
        &self,
        actual: &Arc<RwLock<BicDb>>,
        cluster: &crate::ClusterId,
        node: &ClusterNodeId,
    ) -> Result<()> {
        self.require_initial_preparation_surface()?;
        if !Arc::ptr_eq(actual, &self.database)
            || cluster != &self.spec.cluster_id
            || node != &self.spec.node_id
        {
            return Err(invalid(
                "birth provider belongs to another actual database/open",
            ));
        }
        Ok(())
    }

    /// Called by the listener after TLS/envelope authentication, outside its
    /// service latch. This cannot initialize metadata or permit an app write.
    pub(crate) fn observe_initial_birth(
        &self,
        peer: &MetadataReadParticipant,
        request: NativeInitialBirthOfferRequest,
        parent: &CancellationToken,
    ) -> Result<NativeInitialBirthOfferResponse> {
        request.validate()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        token.check()?;
        self.require_unregistered()?;
        if request.plan_sha256 != self.plan.checksum()? {
            return Err(invalid(
                "initial birth request names a different complete plan",
            ));
        }
        require_pinned_peer(
            peer,
            &self.plan,
            &request.requester,
            request.requester_incarnation,
        )?;
        let database = self
            .database
            .try_read()
            .ok_or_else(|| invalid("actual initial factory is busy; retry without enrolling"))?;
        let native = self
            .factory
            .require_unwritten_enrollment_database(&database, &self.plan.topology)?;
        let schema = database.schema_compatibility_fingerprint()?.sha256;
        if native.writer_enforcement_version() != 1
            || native.abort_protocol_version() != 1
            || native.birth_id() != self.manifest.birth_id
            || native.birth_sha256() != self.manifest.birth_sha256
            || schema != self.manifest.schema_sha256
        {
            return Err(invalid("actual initial factory birth/schema changed"));
        }
        let response = NativeInitialBirthOfferResponse {
            request,
            cluster: self.plan.topology.cluster_id.clone(),
            node: self.spec.node_id.clone(),
            incarnation: self.spec.member_incarnation,
            birth: NativeStoreBirthV1::from_actual_native(&native)?,
            actual_schema_sha256: schema,
            planned_local_ranges_sha256: ranges_sha256(&self.plan, &self.spec.node_id)?,
        };
        self.require_unregistered()?;
        token.check()?;
        Ok(response)
    }
}

/// Necessary actual-peer factory facts, not an enrollment publication, read
/// barrier, store factory, role or metadata-authority conversion. No raw-parts
/// constructor, Clone or Deserialize exists.
pub(crate) struct NativeInitialBirthOfferObservation {
    response: NativeInitialBirthOfferResponse,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl NativeInitialBirthOfferObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        authenticated: crate::distribution_transport::AuthenticatedNativeInitialBirthReply,
        plan: &NativeClusterBootstrapPlan,
    ) -> Result<Self> {
        let (response, original, peer, cluster, destination, deadline, cancellation) =
            authenticated.into_parts();
        cancellation.check()?;
        original.validate()?;
        plan.validate()?;
        if Instant::now() >= deadline
            || response.request != original
            || original.plan_sha256 != plan.checksum()?
            || cluster != plan.topology.cluster_id
            || response.cluster != cluster
            || response.node != destination
            || response.planned_local_ranges_sha256 != ranges_sha256(plan, &destination)?
            || response.actual_schema_sha256.len() != 64
            || !response
                .actual_schema_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid(
                "actual initial-birth exchange was relabeled, changed or expired",
            ));
        }
        require_pinned_peer(&peer, plan, &destination, response.incarnation)?;
        let member = &plan.topology.nodes[&destination];
        response
            .birth
            .validate_for(plan.topology.range_image_bootstrap_id, member.incarnation)?;
        if response.birth.writer_enforcement_version() != 1
            || response.birth.abort_protocol_version() != 1
        {
            return Err(invalid("initial peer is not an enforced-writer factory"));
        }
        Ok(Self {
            response,
            deadline,
            cancellation,
        })
    }
    fn require_live_plan(&self, plan: &NativeClusterBootstrapPlan) -> Result<()> {
        self.cancellation.check()?;
        if Instant::now() >= self.deadline
            || self.response.request.plan_sha256 != plan.checksum()?
        {
            return Err(invalid(
                "initial factory observation is stale or belongs to another plan",
            ));
        }
        Ok(())
    }
}

impl NativeInitialMetadataConfiguration {
    /// Assemble INITIAL CONFIGURATION from actual pinned exchanges, before
    /// fresh metadata-store creation. Existing stores must still refuse any
    /// known unbound-voter upgrade; this object cannot bypass that rule.
    pub(crate) fn from_authenticated_offers(
        plan: &NativeClusterBootstrapPlan,
        offers: Vec<NativeInitialBirthOfferObservation>,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        plan.validate()?;
        cancellation.check()?;
        if offers.len() != plan.topology.nodes.len() {
            return Err(invalid(
                "initial configuration needs every planned actual peer",
            ));
        }
        let mut topology = plan.topology.clone();
        let mut seen = BTreeSet::new();
        let mut schema = None;
        for offer in &offers {
            offer.require_live_plan(plan)?;
            let value = &offer.response;
            if !seen.insert(value.node.clone())
                || schema
                    .as_ref()
                    .is_some_and(|s| s != &value.actual_schema_sha256)
            {
                return Err(invalid(
                    "initial factory offers repeat or disagree on actual schema",
                ));
            }
            let node = topology
                .nodes
                .get_mut(&value.node)
                .ok_or_else(|| invalid("initial offer is outside the planned membership"))?;
            if node.incarnation != value.incarnation
                || node
                    .labels
                    .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                    .is_some_and(|s| s != &value.actual_schema_sha256)
            {
                return Err(invalid("initial offer changes declared incarnation/schema"));
            }
            node.native_store_birth_v1 = Some(value.birth.clone());
            node.labels.insert(
                crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                value.actual_schema_sha256.clone(),
            );
            schema = Some(value.actual_schema_sha256.clone());
        }
        topology.format_version = crate::distribution::NATIVE_STORE_BIRTH_TOPOLOGY_FORMAT_VERSION;
        topology.validate()?;
        for offer in &offers {
            offer.require_live_plan(plan)?;
        }
        cancellation.check()?;
        Ok(Self {
            topology,
            plan_sha256: plan.checksum()?,
            schema_sha256: schema.ok_or_else(|| invalid("initial peer schema is missing"))?,
        })
    }
}
