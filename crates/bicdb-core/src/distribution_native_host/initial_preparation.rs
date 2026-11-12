//! All-member acceptance of INITIAL configuration, never a metadata read,
//! voter quorum, serving role, data grant or claim that every peer is live.
use super::*;
use crate::db::{NativeInitialPreparationRecord, NativeRecoveryAdmission};
use crate::distribution_consensus::{MetadataConsensusStore, MetadataReadParticipant};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::time::Instant;

pub(crate) const INITIAL_METADATA_DIRECTORY: &str = "native-initial-metadata";
pub(crate) fn configuration_sha256(c: &ClusterTopology) -> Result<String> {
    Ok(hex::encode(Sha256::digest(
        crate::distribution_range_image::bounded_json(
            &("NativeInitialConfiguration-v1", c),
            MAX_PLAN_BYTES + 256 * 1024,
        )?,
    )))
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeInitialPreparationStage {
    PreparedNoVote,
    ConsensusExposedHistorical,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialPreparationPeerFact {
    pub node: ClusterNodeId,
    pub incarnation: u64,
    pub birth: crate::distribution::NativeStoreBirthV1,
    pub store_id: Uuid,
    pub configuration_sha256: String,
    pub initial_state_sha256: String,
    pub acceptance_sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialPreparationRequest {
    pub version: u32,
    pub challenge: Uuid,
    pub requester: ClusterNodeId,
    pub requester_incarnation: u64,
    pub plan_sha256: String,
    /// None discovers only already-protected accepted C as CONFIG data.
    /// Some proposes exact C for local durable acceptance; it is not authority.
    pub configuration: Option<ClusterTopology>,
    pub budget_ms: u64,
}
impl NativeInitialPreparationRequest {
    pub(crate) fn response_header(&self) -> Self {
        let mut h = self.clone();
        h.configuration = None;
        h
    }
    pub(crate) fn proposed_configuration_sha256(&self) -> Result<Option<String>> {
        self.configuration
            .as_ref()
            .map(configuration_sha256)
            .transpose()
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.challenge.is_nil()
            || self.requester_incarnation == 0
            || !(1..=5000).contains(&self.budget_ms)
            || self.plan_sha256.len() != 64
        {
            return Err(invalid("invalid initial preparation request"));
        }
        if let Some(c) = &self.configuration {
            c.validate()?;
            configuration_sha256(c)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialPreparationResponse {
    pub request: NativeInitialPreparationRequest,
    pub requested_configuration_sha256: Option<String>,
    pub configuration: ClusterTopology,
    pub stage: NativeInitialPreparationStage,
    pub fact: NativeInitialPreparationPeerFact,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum NativeInitialConfigurationDataSource {
    ProtectedProposal,
    ProtectedAcceptance,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialConfigurationDataResponse {
    pub request: NativeInitialPreparationRequest,
    pub configuration: ClusterTopology,
    pub node: ClusterNodeId,
    pub incarnation: u64,
    pub birth: crate::distribution::NativeStoreBirthV1,
    pub source: NativeInitialConfigurationDataSource,
    pub protected_record_sha256: String,
}
pub(crate) enum NativeInitialPreparationExchange {
    Accepted(NativeInitialPreparationResponse),
    ConfigurationData(NativeInitialConfigurationDataResponse),
}
/// CONFIG-only sealed exchange, deliberately incapable of entering the barrier.
pub(crate) struct NativeInitialConfigurationDataObservation {
    response: NativeInitialConfigurationDataResponse,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl NativeInitialConfigurationDataObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        reply: crate::distribution_transport::AuthenticatedNativeInitialConfigurationDataReply,
        plan: &NativeClusterBootstrapPlan,
    ) -> Result<Self> {
        let (response, original, peer, cluster, destination, deadline, cancellation) =
            reply.into_parts();
        cancellation.check()?;
        original.validate()?;
        if Instant::now() >= deadline
            || original.configuration.is_some()
            || response.request != original
            || original.plan_sha256 != plan.checksum()?
            || cluster != plan.topology.cluster_id
            || response.configuration.cluster_id != cluster
            || response.node != destination
            || response.protected_record_sha256.len() != 64
        {
            return Err(invalid("initial CONFIG discovery relabeled/expired"));
        }
        super::initial_offer::require_pinned_peer(&peer, plan, &destination, response.incarnation)?;
        let local = response
            .configuration
            .nodes
            .get(&destination)
            .ok_or_else(|| invalid("initial CONFIG omits source"))?;
        let schema = local
            .labels
            .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
            .ok_or_else(|| invalid("CONFIG source schema missing"))?;
        validate_configuration_shape(plan, &response.configuration, schema)?;
        if local.native_store_birth_v1.as_ref() != Some(&response.birth) {
            return Err(invalid("CONFIG source birth mismatch"));
        }
        Ok(Self {
            response,
            deadline,
            cancellation,
        })
    }
    fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if Instant::now() >= self.deadline {
            return Err(invalid("CONFIG discovery expired"));
        }
        Ok(())
    }
}

/// Only an actual local factory/current DB can mint this proposal validation.
/// It permits PREPARATION/refusal, never voting or adoption of remote birth data.
pub(crate) struct ValidatedInitialConfigurationProposal<'a> {
    seed: &'a NativeProvisionalNode,
    configuration: ClusterTopology,
    native: NativeRecoveryAdmission,
}
impl ValidatedInitialConfigurationProposal<'_> {
    pub(crate) fn local_node(&self) -> &ClusterNodeId {
        self.native.local_node_id()
    }
    pub(crate) fn configuration(&self) -> &ClusterTopology {
        &self.configuration
    }
    pub(crate) fn plan_sha256(&self) -> Result<String> {
        self.seed.plan.checksum()
    }
    pub(crate) fn schema_sha256(&self) -> &str {
        &self.seed.manifest.schema_sha256
    }
    pub(crate) fn require_database(&self, db: &BicDb) -> Result<()> {
        self.native.require_current(db)?;
        let actual = self
            .seed
            .factory
            .require_unwritten_enrollment_database(db, &self.seed.plan.topology)?;
        if actual.birth_id() != self.native.birth_id()
            || actual.metadata_initialization_version() != 1
            || db.schema_compatibility_fingerprint()?.sha256 != self.seed.manifest.schema_sha256
        {
            return Err(invalid("actual initial factory changed during preparation"));
        }
        Ok(())
    }
}
fn validate_configuration_shape(
    plan: &NativeClusterBootstrapPlan,
    c: &ClusterTopology,
    schema: &str,
) -> Result<()> {
    plan.validate()?;
    c.validate()?;
    let mut expected = plan.topology.clone();
    for (id, node) in &mut expected.nodes {
        let proposed = c
            .nodes
            .get(id)
            .ok_or_else(|| invalid("initial C omits planned member"))?;
        let birth = proposed
            .native_store_birth_v1
            .as_ref()
            .ok_or_else(|| invalid("initial C omits enrolled birth"))?;
        birth.validate_for(plan.topology.range_image_bootstrap_id, node.incarnation)?;
        if birth.writer_enforcement_version() != 1 {
            return Err(invalid("initial C contains unenforced birth"));
        }
        node.native_store_birth_v1 = Some(birth.clone());
        node.labels.insert(
            crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
            schema.to_owned(),
        );
    }
    expected.format_version = crate::distribution::NATIVE_STORE_BIRTH_TOPOLOGY_FORMAT_VERSION;
    if &expected != c {
        return Err(invalid("initial C changes fixed plan/roster/ranges/schema"));
    }
    configuration_sha256(c)?;
    Ok(())
}
impl NativeProvisionalNode {
    fn validate_initial_configuration(
        &self,
        c: ClusterTopology,
    ) -> Result<ValidatedInitialConfigurationProposal<'_>> {
        validate_configuration_shape(&self.plan, &c, &self.manifest.schema_sha256)?;
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("factory busy before initial preparation"))?;
        let native = self
            .factory
            .require_unwritten_enrollment_database(&db, &self.plan.topology)?;
        let local = &c.nodes[&self.spec.node_id];
        if local.native_store_birth_v1.as_ref()
            != Some(&crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?)
            || native.metadata_initialization_version() != 1
            || native.birth_id() != self.manifest.birth_id
            || native.birth_sha256() != self.manifest.birth_sha256
            || db.schema_compatibility_fingerprint()?.sha256 != self.manifest.schema_sha256
        {
            return Err(invalid(
                "initial C does not bind actual LOCAL factory/birth/schema",
            ));
        }
        Ok(ValidatedInitialConfigurationProposal {
            seed: self,
            configuration: c,
            native,
        })
    }
    pub(crate) fn observe_initial_preparation(
        &self,
        peer: &MetadataReadParticipant,
        request: NativeInitialPreparationRequest,
        parent: &CancellationToken,
    ) -> Result<NativeInitialPreparationResponse> {
        request.validate()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        token.check()?;
        super::initial_offer::require_pinned_peer(
            peer,
            &self.plan,
            &request.requester,
            request.requester_incarnation,
        )?;
        if request.plan_sha256 != self.plan.checksum()? {
            return Err(invalid("initial preparation names foreign plan"));
        }
        let mut slot = self
            .prepared_metadata
            .try_lock()
            .ok_or_else(|| invalid("initial preparation owner busy"))?;
        let record = {
            let db = self
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DB busy"))?;
            db.read_native_initial_preparation(
                &self.plan.topology.cluster_id,
                self.spec.bootstrap_id,
                &self.spec.node_id,
                self.spec.member_incarnation,
            )?
        };
        let record = if let Some(record) = record {
            if request
                .configuration
                .as_ref()
                .is_some_and(|c| c != record.configuration())
            {
                return Err(invalid(
                    "initial preparation already accepted a DIFFERENT exact C",
                ));
            }
            if record.plan_sha256() != request.plan_sha256 {
                return Err(invalid("protected preparation belongs to foreign plan"));
            }
            if record.stage() == NativeInitialPreparationStage::PreparedNoVote {
                if slot.is_none() {
                    *slot = Some(MetadataConsensusStore::reopen_native_owned(
                        self.root.join(INITIAL_METADATA_DIRECTORY),
                        Arc::clone(&self.database),
                        self.spec.node_id.clone(),
                        record.configuration().clone(),
                        &token,
                    )?);
                }
                let fact = slot.as_ref().unwrap().native_initial_pre_vote_fact()?;
                fact.require_current()?;
                if fact.store_id() != record.peer_fact().store_id
                    || fact.topology() != record.configuration()
                {
                    return Err(invalid(
                        "prepared owned metadata state changed from accepted C",
                    ));
                }
                // Prepared is only refreshed from an actual still-unvoted owner.
                self.validate_initial_configuration(record.configuration().clone())?;
            } else {
                // Historical acceptance is not current topology/term or data readiness.
                MetadataConsensusStore::inspect_native_store_identity(
                    self.root.join(INITIAL_METADATA_DIRECTORY),
                    record.peer_fact().store_id,
                    &self.plan.topology.cluster_id,
                    &self.spec.node_id,
                )?;
            }
            record
        } else {
            self.require_initial_preparation_surface()?;
            let c = request
                .configuration
                .clone()
                .ok_or_else(|| invalid("no initial configuration accepted yet"))?;
            let validated = self.validate_initial_configuration(c.clone())?;
            self.database
                .try_read()
                .ok_or_else(|| invalid("initial DB busy before CONFIG proposal"))?
                .persist_native_initial_configuration_proposal(&validated)?;
            if slot.is_none() {
                let path = self.root.join(INITIAL_METADATA_DIRECTORY);
                *slot = Some(MetadataConsensusStore::prepare_initial_native_owned(
                    &path,
                    Arc::clone(&self.database),
                    &self.factory,
                    c.clone(),
                    &token,
                )?);
            }
            let actual = slot.as_ref().unwrap().native_initial_pre_vote_fact()?;
            let db = self
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DB busy before protected acceptance"))?;
            db.persist_native_initial_preparation(&validated, &actual)?
        };
        token.check()?;
        Ok(NativeInitialPreparationResponse {
            requested_configuration_sha256: request.proposed_configuration_sha256()?,
            request: request.response_header(),
            configuration: record.configuration().clone(),
            stage: record.stage(),
            fact: record.peer_fact().clone(),
        })
    }
    pub(crate) fn initial_prepared_configuration(
        &self,
    ) -> Result<Option<NativeInitialMetadataConfiguration>> {
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("initial DB busy"))?;
        let Some(record) = db.read_native_initial_preparation(
            &self.plan.topology.cluster_id,
            self.spec.bootstrap_id,
            &self.spec.node_id,
            self.spec.member_incarnation,
        )?
        else {
            let Some(proposal) = db.read_native_initial_configuration_proposal(
                &self.spec.cluster_id,
                self.spec.bootstrap_id,
                &self.spec.node_id,
                self.spec.member_incarnation,
            )?
            else {
                return Ok(None);
            };
            if proposal.plan_sha256() != self.plan.checksum()? {
                return Err(invalid("orphan CONFIG belongs to foreign plan"));
            }
            validate_configuration_shape(
                &self.plan,
                proposal.configuration(),
                &self.manifest.schema_sha256,
            )?;
            return Ok(Some(NativeInitialMetadataConfiguration {
                topology: proposal.configuration().clone(),
                plan_sha256: self.plan.checksum()?,
                schema_sha256: self.manifest.schema_sha256.clone(),
            }));
        };
        validate_configuration_shape(
            &self.plan,
            record.configuration(),
            &self.manifest.schema_sha256,
        )?;
        Ok(Some(NativeInitialMetadataConfiguration {
            topology: record.configuration().clone(),
            plan_sha256: self.plan.checksum()?,
            schema_sha256: self.manifest.schema_sha256.clone(),
        }))
    }
}

/// Immutable actual-peer acceptance sealed by the original authenticated exchange.
/// The protected fact is historical; ORIGINAL operation cancellation/deadline apply.
pub(crate) struct NativeInitialPreparationObservation {
    response: NativeInitialPreparationResponse,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl NativeInitialPreparationObservation {
    #[cfg(feature = "tls-replication")]
    pub(crate) fn from_transport(
        reply: crate::distribution_transport::AuthenticatedNativeInitialPreparationReply,
        plan: &NativeClusterBootstrapPlan,
    ) -> Result<Self> {
        let (response, original, peer, cluster, destination, deadline, cancellation) =
            reply.into_parts();
        cancellation.check()?;
        original.validate()?;
        if Instant::now() >= deadline
            || response.request != original.response_header()
            || response.requested_configuration_sha256
                != original.proposed_configuration_sha256()?
            || original.plan_sha256 != plan.checksum()?
            || cluster != plan.topology.cluster_id
            || response.configuration.cluster_id != cluster
            || response.fact.node != destination
            || response.fact.configuration_sha256 != configuration_sha256(&response.configuration)?
            || original
                .configuration
                .as_ref()
                .is_some_and(|c| c != &response.configuration)
            || response.fact.store_id.is_nil()
            || response.fact.initial_state_sha256.len() != 64
            || response.fact.acceptance_sha256.len() != 64
        {
            return Err(invalid(
                "initial preparation exchange relabeled/expired/damaged",
            ));
        }
        super::initial_offer::require_pinned_peer(
            &peer,
            plan,
            &destination,
            response.fact.incarnation,
        )?;
        let destination_member = response
            .configuration
            .nodes
            .get(&destination)
            .ok_or_else(|| invalid("prepared response omits authenticated destination"))?;
        let schema = destination_member
            .labels
            .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
            .ok_or_else(|| invalid("prepared schema missing"))?;
        validate_configuration_shape(plan, &response.configuration, schema)?;
        if destination_member.native_store_birth_v1.as_ref() != Some(&response.fact.birth) {
            return Err(invalid("protected peer birth differs from initial C"));
        }
        Ok(Self {
            response,
            deadline,
            cancellation,
        })
    }
    pub(crate) fn stage(&self) -> NativeInitialPreparationStage {
        self.response.stage
    }
    pub(crate) fn configuration_data(&self) -> &ClusterTopology {
        &self.response.configuration
    }
    fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        if Instant::now() >= self.deadline {
            return Err(invalid("initial acceptance observation expired"));
        }
        Ok(())
    }
}
/// Permission to start ONLY metadata consensus after every member accepted C.
/// No Deserialize/Clone/raw-parts constructor, lease or general-read fallback.
pub(crate) struct NativeInitialConsensusPermit {
    configuration: ClusterTopology,
    plan_sha256: String,
    facts: BTreeMap<ClusterNodeId, NativeInitialPreparationPeerFact>,
    cancellation: CancellationToken,
    original_cancellations: Vec<CancellationToken>,
}
impl NativeInitialConsensusPermit {
    pub(crate) fn from_observations(
        seed: &NativeProvisionalNode,
        configuration: &NativeInitialMetadataConfiguration,
        observations: Vec<NativeInitialPreparationObservation>,
        parent: &CancellationToken,
    ) -> Result<Self> {
        parent.check()?;
        if configuration.plan_sha256 != seed.plan.checksum()?
            || observations.len() != seed.plan.topology.nodes.len()
        {
            return Err(invalid(
                "initial barrier needs ALL planned members for exact C",
            ));
        }
        let mut facts = BTreeMap::new();
        let mut deadline = parent.deadline();
        let mut original_cancellations = Vec::new();
        for observation in observations {
            deadline = Some(deadline.map_or(observation.deadline, |d| d.min(observation.deadline)));
            original_cancellations.push(observation.cancellation.clone());
            observation.require_live()?;
            if observation.response.configuration != configuration.topology
                || facts
                    .insert(
                        observation.response.fact.node.clone(),
                        observation.response.fact,
                    )
                    .is_some()
            {
                return Err(invalid(
                    "initial barrier repeats member or mixes configurations",
                ));
            }
        }
        if facts.keys().ne(seed.plan.topology.nodes.keys()) {
            return Err(invalid("initial barrier misses planned member"));
        }
        Ok(Self {
            configuration: configuration.topology.clone(),
            plan_sha256: configuration.plan_sha256.clone(),
            facts,
            cancellation: parent.child_with_deadline(
                deadline
                    .ok_or_else(|| invalid("initial barrier has no original bounded deadline"))?,
            ),
            original_cancellations,
        })
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.cancellation.check()?;
        for token in &self.original_cancellations {
            token.check()?;
        }
        Ok(())
    }
    pub(crate) fn deadline(&self) -> Result<Instant> {
        self.require_live()?;
        self.cancellation
            .deadline()
            .ok_or_else(|| invalid("initial barrier deadline absent"))
    }
    pub(crate) fn configuration(&self) -> &ClusterTopology {
        &self.configuration
    }
    pub(crate) fn facts(&self) -> &BTreeMap<ClusterNodeId, NativeInitialPreparationPeerFact> {
        &self.facts
    }
    pub(crate) fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }
}

impl NativeProvisionalNode {
    pub(crate) fn require_same_initial_database(
        &self,
        actual: &Arc<RwLock<BicDb>>,
        cluster: &crate::ClusterId,
        node: &ClusterNodeId,
    ) -> Result<()> {
        if !Arc::ptr_eq(actual, &self.database)
            || cluster != &self.spec.cluster_id
            || node != &self.spec.node_id
        {
            return Err(invalid(
                "initial preparation belongs to another actual DB/service",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_initial_observer_identity(
        &self,
        current: &ClusterTopology,
    ) -> Result<()> {
        let member = current
            .nodes
            .get(&self.spec.node_id)
            .ok_or_else(|| invalid("initial observer removed from listener identity"))?;
        if member.lifecycle != ClusterNodeLifecycle::Active
            || member.incarnation != self.spec.member_incarnation
            || current.cluster_id != self.spec.cluster_id
            || current.range_image_bootstrap_id != Some(self.spec.bootstrap_id)
        {
            return Err(invalid(
                "initial observer is no longer the actual active member incarnation",
            ));
        }
        Ok(())
    }
}
impl NativeInitialMetadataConfiguration {
    pub(crate) fn from_preparation_observation(
        seed: &NativeProvisionalNode,
        observation: NativeInitialPreparationObservation,
    ) -> Result<Self> {
        observation.require_live()?;
        // This validates CONFIG only. ALL actual peer acceptances are still
        // mandatory before any consensus endpoint can be released.
        let c = observation.response.configuration;
        seed.validate_initial_configuration(c.clone())?;
        Ok(Self {
            topology: c,
            plan_sha256: seed.plan.checksum()?,
            schema_sha256: seed.manifest.schema_sha256.clone(),
        })
    }
}

impl NativeProvisionalNode {
    pub(crate) fn require_initial_preparation_surface(&self) -> Result<()> {
        for path in [
            crate::distribution::DEFAULT_CLUSTER_TOPOLOGY,
            crate::distribution::DEFAULT_DISTRIBUTION_CONFIG,
            crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE,
        ] {
            if self.root.join(path).exists() {
                return Err(invalid(
                    "root was already registered outside initial preparation",
                ));
            }
        }
        let db = self
            .database
            .try_read()
            .ok_or_else(|| invalid("initial preparation DB busy"))?;
        let native = self
            .factory
            .require_unwritten_enrollment_database(&db, &self.plan.topology)?;
        if native.birth_id() != self.manifest.birth_id
            || native.birth_sha256() != self.manifest.birth_sha256
            || native.metadata_initialization_version() != 1
            || db.schema_compatibility_fingerprint()?.sha256 != self.manifest.schema_sha256
        {
            return Err(invalid(
                "initial preparation surface lost actual unwritten factory",
            ));
        }
        if db
            .read_native_initial_preparation(
                &self.spec.cluster_id,
                self.spec.bootstrap_id,
                &self.spec.node_id,
                self.spec.member_incarnation,
            )?
            .is_some_and(|r| r.stage() == NativeInitialPreparationStage::ConsensusExposedHistorical)
        {
            return Err(invalid(
                "consensus MAY HAVE BEEN EXPOSED; ordinary owned metadata recovery required",
            ));
        }
        let identity = db.native_metadata_initialization_record(
            &self.spec.cluster_id,
            self.spec.bootstrap_id,
            &self.spec.node_id,
            self.spec.member_incarnation,
        )?;
        let path = self.root.join(INITIAL_METADATA_DIRECTORY);
        if path
            .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
            .exists()
        {
            let identity = identity.ok_or_else(|| {
                invalid("metadata state has no protected initialization identity")
            })?;
            MetadataConsensusStore::inspect_native_store_identity(
                &path,
                identity.store_id(),
                &self.spec.cluster_id,
                &self.spec.node_id,
            )?;
            let status = MetadataConsensusStore::inspect(&path)?;
            if status.current_term != 0
                || status.voted_for.is_some()
                || status.commit_index != 0
                || status.last_log_index != 0
                || status.role != crate::distribution_consensus::MetadataConsensusRole::Follower
            {
                return Err(invalid(
                    "previously voting metadata cannot reenter pre-vote preparation",
                ));
            }
        } else if identity.is_some_and(|r| r.registered()) {
            return Err(invalid(
                "registered metadata state missing; NEVER reset to pre-vote",
            ));
        }
        Ok(())
    }
    /// Strict preparation recovery; never recreates genesis or resets votes.
    /// A historical-exposure root must use ordinary metadata recovery instead.
    pub(crate) fn open_initial_preparation(
        root: &Path,
        config: DbConfig,
        plan: NativeClusterBootstrapPlan,
        local: &ClusterNodeId,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        plan.validate()?;
        cancellation.check()?;
        let manifest: NativeSeedManifest =
            serde_json::from_slice(&std::fs::read(root.join(SEED_MANIFEST))?)?;
        let spec = plan.local_spec(local)?;
        if manifest.version != 1
            || manifest.plan_sha256 != plan.checksum()?
            || manifest.node != *local
            || manifest.incarnation != spec.member_incarnation
        {
            return Err(invalid("initial preparation manifest/plan mismatch"));
        }
        let (database, factory) = BicDb::open_range_image_database(root, config, &spec)?;
        let seed = Self {
            database: Arc::new(RwLock::new(database)),
            factory,
            plan,
            spec,
            root: root.canonicalize()?,
            manifest,
            prepared_metadata: Mutex::new(None),
        };
        seed.require_initial_preparation_surface()?;
        cancellation.check()?;
        Ok(seed)
    }
}

impl NativeProvisionalNode {
    pub(crate) fn initial_preparation_exchange(
        &self,
        peer: &MetadataReadParticipant,
        request: NativeInitialPreparationRequest,
        parent: &CancellationToken,
    ) -> Result<NativeInitialPreparationExchange> {
        request.validate()?;
        let token =
            parent.child_with_deadline(Instant::now() + Duration::from_millis(request.budget_ms));
        token.check()?;
        super::initial_offer::require_pinned_peer(
            peer,
            &self.plan,
            &request.requester,
            request.requester_incarnation,
        )?;
        if request.plan_sha256 != self.plan.checksum()? {
            return Err(invalid("initial exchange foreign plan"));
        }
        if request.configuration.is_none() {
            let record = self
                .database
                .try_read()
                .ok_or_else(|| invalid("initial DB busy"))?
                .read_native_initial_preparation(
                    &self.spec.cluster_id,
                    self.spec.bootstrap_id,
                    &self.spec.node_id,
                    self.spec.member_incarnation,
                )?;
            if record.is_none() {
                self.require_initial_preparation_surface()?;
                let db = self
                    .database
                    .try_read()
                    .ok_or_else(|| invalid("CONFIG discovery DB busy"))?;
                let proposal = db
                    .read_native_initial_configuration_proposal(
                        &self.spec.cluster_id,
                        self.spec.bootstrap_id,
                        &self.spec.node_id,
                        self.spec.member_incarnation,
                    )?
                    .ok_or_else(|| invalid("no protected initial configuration exists"))?;
                if proposal.plan_sha256() != self.plan.checksum()? {
                    return Err(invalid("CONFIG proposal plan mismatch"));
                }
                token.check()?;
                return Ok(NativeInitialPreparationExchange::ConfigurationData(
                    NativeInitialConfigurationDataResponse {
                        request,
                        configuration: proposal.configuration().clone(),
                        node: self.spec.node_id.clone(),
                        incarnation: self.spec.member_incarnation,
                        birth: proposal.birth().clone(),
                        source: NativeInitialConfigurationDataSource::ProtectedProposal,
                        protected_record_sha256: proposal.checksum_sha256().to_owned(),
                    },
                ));
            }
        }
        self.observe_initial_preparation(peer, request, &token)
            .map(NativeInitialPreparationExchange::Accepted)
    }
}
impl NativeInitialMetadataConfiguration {
    pub(crate) fn from_configuration_data_observation(
        seed: &NativeProvisionalNode,
        observation: NativeInitialConfigurationDataObservation,
    ) -> Result<Self> {
        observation.require_live()?;
        let c = observation.response.configuration;
        seed.validate_initial_configuration(c.clone())?;
        Ok(Self {
            topology: c,
            plan_sha256: seed.plan.checksum()?,
            schema_sha256: seed.manifest.schema_sha256.clone(),
        })
    }
}

#[cfg(test)]
impl NativeProvisionalNode {
    pub(crate) fn test_initial_metadata_preparation_cut(
        &self,
        c: &NativeInitialMetadataConfiguration,
        after_state: bool,
        parent: &CancellationToken,
    ) -> Result<()> {
        self.require_initial_preparation_surface()?;
        let slot = self
            .prepared_metadata
            .try_lock()
            .ok_or_else(|| invalid("initial test preparation busy"))?;
        if slot.is_some() {
            return Err(invalid("initial test cut needs unopened local metadata"));
        }
        let validated = self.validate_initial_configuration(c.topology.clone())?;
        self.database
            .try_read()
            .ok_or_else(|| invalid("initial DB busy"))?
            .persist_native_initial_configuration_proposal(&validated)?;
        let point = if after_state {
            crate::distribution_consensus::NativeMetadataInitializationCut::AfterStateFile
        } else {
            crate::distribution_consensus::NativeMetadataInitializationCut::AfterProtectedPrepare
        };
        let result = MetadataConsensusStore::test_initialize_native_owned_with(
            &self.root.join(INITIAL_METADATA_DIRECTORY),
            Arc::clone(&self.database),
            &self.factory,
            c.topology.clone(),
            parent,
            |cut| {
                if cut == point {
                    Err(invalid(
                        "injected INITIAL metadata prepare cut after actual durable publication",
                    ))
                } else {
                    Ok(())
                }
            },
        );
        match result {
            Ok(_) => Err(invalid(
                "expected actual initial preparation cut not reached",
            )),
            Err(e) => Err(e),
        }
    }
}
