//! Protected initial configuration acceptance/exposure facts. They are not
//! read-index, data quorum, current role or serving authority.
use super::*;
use crate::distribution::{ClusterId, ClusterNodeId, ClusterTopology, NativeStoreBirthV1};
use crate::distribution_consensus::NativeMetadataPreVoteFact;
use crate::distribution_native_host::{
    NativeInitialConsensusPermit, NativeInitialPreparationPeerFact, NativeInitialPreparationStage,
    ValidatedInitialConfigurationProposal,
};
use sha2::Sha256;
const KEY: &str = "metadata-store:initial-preparation";
const MAX_BYTES: usize = 8 * 1024 * 1024;
fn invalid(s: &str) -> BicDbError {
    BicDbError::Cluster(format!("native initial preparation: {s}"))
}
const PROPOSAL_KEY: &str = "metadata-store:initial-configuration-proposal";
/// CONFIG data only. Its presence cannot become an Accepted fact or permit.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialConfigurationProposalRecord {
    version: u32,
    plan_sha256: String,
    configuration: ClusterTopology,
    local_node: ClusterNodeId,
    birth: NativeStoreBirthV1,
    schema_sha256: String,
    checksum_sha256: String,
}
impl NativeInitialConfigurationProposalRecord {
    fn checksum(&self) -> Result<String> {
        let mut c = self.clone();
        c.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("NativeInitialConfigurationProposal-v1", c),
                MAX_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.configuration.validate()?;
        if self.version != 1
            || self.plan_sha256.len() != 64
            || self.schema_sha256.len() != 64
            || self
                .configuration
                .nodes
                .get(&self.local_node)
                .is_none_or(|m| {
                    m.native_store_birth_v1.as_ref() != Some(&self.birth)
                        || m.labels
                            .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                            != Some(&self.schema_sha256)
                })
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected initial proposal"));
        }
        Ok(())
    }
    pub(crate) fn configuration(&self) -> &ClusterTopology {
        &self.configuration
    }
    pub(crate) fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }
    pub(crate) fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }
    pub(crate) fn birth(&self) -> &NativeStoreBirthV1 {
        &self.birth
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeInitialPreparationRecord {
    version: u32,
    plan_sha256: String,
    proposal_sha256: String,
    configuration: ClusterTopology,
    local: NativeInitialPreparationPeerFact,
    stage: NativeInitialPreparationStage,
    barrier: BTreeMap<ClusterNodeId, NativeInitialPreparationPeerFact>,
    checksum_sha256: String,
}
impl NativeInitialPreparationRecord {
    fn acceptance_sha256(&self) -> Result<String> {
        let mut local = self.local.clone();
        local.acceptance_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &(
                    "NativeInitialAcceptedConfiguration-v1",
                    self.version,
                    &self.plan_sha256,
                    &self.proposal_sha256,
                    &self.configuration,
                    local,
                ),
                MAX_BYTES,
            )?,
        )))
    }
    fn checksum(&self) -> Result<String> {
        let mut c = self.clone();
        c.checksum_sha256.clear();
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(
                &("NativeInitialPreparationRecord-v1", c),
                MAX_BYTES,
            )?,
        )))
    }
    fn validate(&self) -> Result<()> {
        self.configuration.validate()?;
        let csha =
            crate::distribution_native_host::initial_configuration_sha256(&self.configuration)?;
        if self.version != 1
            || self.plan_sha256.len() != 64
            || self.proposal_sha256.len() != 64
            || self.local.store_id.is_nil()
            || self.local.initial_state_sha256.len() != 64
            || self.local.configuration_sha256 != csha
            || self.local.acceptance_sha256 != self.acceptance_sha256()?
            || self
                .configuration
                .nodes
                .get(&self.local.node)
                .is_none_or(|m| {
                    m.incarnation != self.local.incarnation
                        || m.native_store_birth_v1.as_ref() != Some(&self.local.birth)
                })
            || self.checksum()? != self.checksum_sha256
        {
            return Err(invalid("damaged protected initial acceptance"));
        }
        match self.stage {
            NativeInitialPreparationStage::PreparedNoVote if self.barrier.is_empty() => (),
            NativeInitialPreparationStage::ConsensusExposedHistorical => {
                if self.barrier.keys().ne(self.configuration.nodes.keys()) {
                    return Err(invalid("exposure lacks complete same-C barrier"));
                }
                for (id, f) in &self.barrier {
                    let m = &self.configuration.nodes[id];
                    if f.node != *id
                        || f.incarnation != m.incarnation
                        || m.native_store_birth_v1.as_ref() != Some(&f.birth)
                        || f.configuration_sha256 != csha
                        || f.store_id.is_nil()
                        || f.initial_state_sha256.len() != 64
                        || f.acceptance_sha256.len() != 64
                    {
                        return Err(invalid("exposure barrier mixes identities/configurations"));
                    }
                }
                if self.barrier.get(&self.local.node) != Some(&self.local) {
                    return Err(invalid("exposure misses actual local acceptance"));
                }
            }
            _ => return Err(invalid("preparation phase/audit mismatch")),
        }
        Ok(())
    }
    pub(crate) fn configuration(&self) -> &ClusterTopology {
        &self.configuration
    }
    pub(crate) fn plan_sha256(&self) -> &str {
        &self.plan_sha256
    }
    pub(crate) fn stage(&self) -> NativeInitialPreparationStage {
        self.stage
    }
    pub(crate) fn peer_fact(&self) -> &NativeInitialPreparationPeerFact {
        &self.local
    }
}
impl BicDb {
    pub(crate) fn read_native_initial_configuration_proposal(
        &self,
        cluster: &ClusterId,
        bootstrap: Uuid,
        node: &ClusterNodeId,
        incarnation: u64,
    ) -> Result<Option<NativeInitialConfigurationProposalRecord>> {
        let native = self.native_recovery_admission(cluster, bootstrap, node, incarnation)?;
        let record = self
            .read_native_range_image_record::<NativeInitialConfigurationProposalRecord>(
                PROPOSAL_KEY,
            )?;
        if let Some(r) = &record {
            r.validate()?;
            if !crate::format::native_initial_preparation_active(&self.path)?
                || native.metadata_initialization_version() != 1
                || r.local_node != *node
                || r.birth != NativeStoreBirthV1::from_actual_native(&native)?
                || r.configuration.cluster_id != *cluster
                || r.configuration.range_image_bootstrap_id != Some(bootstrap)
            {
                return Err(invalid(
                    "protected initial proposal belongs to another actual birth/domain",
                ));
            }
        }
        Ok(record)
    }
    pub(crate) fn persist_native_initial_configuration_proposal(
        &self,
        proposal: &ValidatedInitialConfigurationProposal<'_>,
    ) -> Result<NativeInitialConfigurationProposalRecord> {
        proposal.require_database(self)?;
        let c = proposal.configuration();
        let native = self.metadata_native_admission(c, proposal.local_node())?;
        let mut record = NativeInitialConfigurationProposalRecord {
            version: 1,
            plan_sha256: proposal.plan_sha256()?,
            configuration: c.clone(),
            local_node: proposal.local_node().clone(),
            birth: NativeStoreBirthV1::from_actual_native(&native)?,
            schema_sha256: proposal.schema_sha256().to_owned(),
            checksum_sha256: String::new(),
        };
        record.checksum_sha256 = record.checksum()?;
        record.validate()?;
        let _journal = self.try_native_image_journal()?;
        if let Some(old) = self
            .read_native_range_image_record::<NativeInitialConfigurationProposalRecord>(
                PROPOSAL_KEY,
            )?
        {
            old.validate()?;
            if old != record {
                return Err(invalid(
                    "protected initial proposal already names another exact C",
                ));
            }
            return Ok(old);
        }
        crate::format::enable_native_initial_preparation(&self.path)?;
        self.write_native_range_image_record(PROPOSAL_KEY, &record)?;
        let read: NativeInitialConfigurationProposalRecord = self
            .read_native_range_image_record(PROPOSAL_KEY)?
            .ok_or_else(|| invalid("initial proposal readback missing"))?;
        if read != record {
            return Err(invalid("initial proposal readback changed"));
        }
        drop(_journal);
        proposal.require_database(self)?;
        Ok(record)
    }
    pub(crate) fn read_native_initial_preparation(
        &self,
        cluster: &ClusterId,
        bootstrap: Uuid,
        node: &ClusterNodeId,
        incarnation: u64,
    ) -> Result<Option<NativeInitialPreparationRecord>> {
        let native = self.native_recovery_admission(cluster, bootstrap, node, incarnation)?;
        let Some(record) =
            self.read_native_range_image_record::<NativeInitialPreparationRecord>(KEY)?
        else {
            return Ok(None);
        };
        if !crate::format::native_initial_preparation_active(&self.path)? {
            return Err(invalid("protected preparation reader fence absent"));
        }
        record.validate()?;
        let proposed = self
            .read_native_initial_configuration_proposal(cluster, bootstrap, node, incarnation)?
            .ok_or_else(|| invalid("accepted configuration has no protected proposal"))?;
        if proposed.checksum_sha256 != record.proposal_sha256
            || proposed.configuration != record.configuration
            || proposed.plan_sha256 != record.plan_sha256
        {
            return Err(invalid("accepted configuration/proposal pointer differs"));
        }
        let identity = self.read_native_metadata_store(&record.configuration, node)?;
        if !identity.registered()
            || identity.store_id() != record.local.store_id
            || record.local.birth != NativeStoreBirthV1::from_actual_native(&native)?
            || record.configuration.cluster_id != *cluster
            || record.configuration.range_image_bootstrap_id != Some(bootstrap)
            || record.local.node != *node
            || record.local.incarnation != incarnation
        {
            return Err(invalid(
                "protected initial acceptance belongs to another actual store/birth",
            ));
        }
        // Historical C intentionally does not equal later live schema/topology.
        Ok(Some(record))
    }
    pub(crate) fn persist_native_initial_preparation(
        &self,
        proposal: &ValidatedInitialConfigurationProposal<'_>,
        fact: &NativeMetadataPreVoteFact<'_>,
    ) -> Result<NativeInitialPreparationRecord> {
        proposal.require_database(self)?;
        fact.require_current()?;
        let c = proposal.configuration();
        if fact.topology() != c {
            return Err(invalid("owned term-zero store names another C"));
        }
        let local = proposal.local_node().clone();
        let native = self.metadata_native_admission(c, &local)?;
        let identity = self.read_native_metadata_store(c, &local)?;
        let plain = hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(c, 4 * 1024 * 1024)?,
        ));
        if !identity.registered()
            || identity.store_id() != fact.store_id()
            || identity.initial_topology_sha256() != plain
        {
            return Err(invalid(
                "actual metadata identity was not initialized with exact C",
            ));
        }
        let proposed = self
            .read_native_initial_configuration_proposal(
                native.cluster_id(),
                native.bootstrap_id(),
                native.local_node_id(),
                native.local_member_incarnation(),
            )?
            .ok_or_else(|| invalid("initial acceptance requires a durable CONFIG proposal"))?;
        if proposed.configuration != *c || proposed.plan_sha256 != proposal.plan_sha256()? {
            return Err(invalid("initial proposal differs from actual unvoted C"));
        }
        let _journal = self.try_native_image_journal()?;
        if self
            .read_native_range_image_record::<NativeInitialPreparationRecord>(KEY)?
            .is_some()
        {
            return Err(invalid("initial acceptance already exists"));
        }
        crate::format::enable_native_initial_preparation(&self.path)?;
        let mut record = NativeInitialPreparationRecord {
            version: 1,
            plan_sha256: proposal.plan_sha256()?,
            proposal_sha256: proposed.checksum_sha256.clone(),
            configuration: c.clone(),
            local: NativeInitialPreparationPeerFact {
                node: local,
                incarnation: native.local_member_incarnation(),
                birth: NativeStoreBirthV1::from_actual_native(&native)?,
                store_id: fact.store_id(),
                configuration_sha256:
                    crate::distribution_native_host::initial_configuration_sha256(c)?,
                initial_state_sha256: fact.state_sha256()?,
                acceptance_sha256: String::new(),
            },
            stage: NativeInitialPreparationStage::PreparedNoVote,
            barrier: BTreeMap::new(),
            checksum_sha256: String::new(),
        };
        record.local.acceptance_sha256 = record.acceptance_sha256()?;
        record.checksum_sha256 = record.checksum()?;
        record.validate()?;
        self.write_native_range_image_record(KEY, &record)?;
        let stored: NativeInitialPreparationRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("protected preparation readback missing"))?;
        if stored != record {
            return Err(invalid("protected preparation changed"));
        }
        drop(_journal);
        fact.require_current()?;
        proposal.require_database(self)?;
        Ok(record)
    }
    pub(crate) fn expose_native_initial_consensus(
        &self,
        permit: &NativeInitialConsensusPermit,
        fact: &NativeMetadataPreVoteFact<'_>,
    ) -> Result<NativeInitialPreparationRecord> {
        permit.require_live()?;
        fact.require_current()?;
        let c = permit.configuration();
        if fact.topology() != c {
            return Err(invalid("exposure owned metadata store changed C"));
        }
        let existing: NativeInitialPreparationRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("exposure has no protected acceptance"))?;
        let native = self.metadata_native_admission(c, &existing.local.node)?;
        let actual = self
            .read_native_initial_preparation(
                native.cluster_id(),
                native.bootstrap_id(),
                native.local_node_id(),
                native.local_member_incarnation(),
            )?
            .ok_or_else(|| invalid("exposure lost actual preparation"))?;
        if actual != existing
            || existing.stage != NativeInitialPreparationStage::PreparedNoVote
            || existing.configuration != *c
            || existing.plan_sha256 != permit.plan_sha256()
            || permit.facts().get(&existing.local.node) != Some(&existing.local)
            || fact.store_id() != existing.local.store_id
            || fact.state_sha256()? != existing.local.initial_state_sha256
        {
            return Err(invalid("exposure permit/actual unvoted acceptance changed"));
        }
        let _journal = self.try_native_image_journal()?;
        let mut exposed = existing;
        exposed.stage = NativeInitialPreparationStage::ConsensusExposedHistorical;
        exposed.barrier = permit.facts().clone();
        exposed.checksum_sha256 = exposed.checksum()?;
        exposed.validate()?;
        // Persist BEFORE returning any endpoint/vote handle. This is MAY have
        // been exposed, never a running/deployed acknowledgement.
        self.write_native_range_image_record(KEY, &exposed)?;
        let stored: NativeInitialPreparationRecord = self
            .read_native_range_image_record(KEY)?
            .ok_or_else(|| invalid("exposure readback absent"))?;
        if stored != exposed {
            return Err(invalid("protected exposure changed"));
        }
        permit.require_live()?;
        fact.require_current()?;
        Ok(exposed)
    }
}
