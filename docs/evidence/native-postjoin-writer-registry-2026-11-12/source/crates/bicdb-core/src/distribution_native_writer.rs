//! From-birth host writer enrollment. Registry facts are not a leadership lease.
use crate::db::{BicDb, NativeRecoveryAdmission};
use crate::distribution::{
    ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, NativeStoreBirthV1,
    RangeDescriptor, RangeId, RangeReplicaRole, SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_consensus::{HostMetadataReadObservation, MetadataRestoreActivation};
use crate::{BicDbError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub(crate) fn invalid(detail: impl AsRef<str>) -> BicDbError {
    BicDbError::Cluster(format!(
        "native host writer enrollment: {}",
        detail.as_ref()
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriterMemberPolicy {
    pub(crate) incarnation: u64,
    pub(crate) birth: NativeStoreBirthV1,
    pub(crate) active_pin: Option<String>,
    pub(crate) pending_pin: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostWriterPolicyFacts {
    pub(crate) cluster_id: ClusterId,
    pub(crate) bootstrap_id: Uuid,
    pub(crate) local_node_id: ClusterNodeId,
    pub(crate) local_incarnation: u64,
    pub(crate) local_birth_id: Uuid,
    pub(crate) local_birth_sha256: String,
    pub(crate) metadata_voters: BTreeSet<ClusterNodeId>,
    pub(crate) members: BTreeMap<ClusterNodeId, WriterMemberPolicy>,
    pub(crate) ranges: BTreeMap<RangeId, RangeDescriptor>,
    pub(crate) schema_sha256: String,
    pub(crate) restore_activation: Option<MetadataRestoreActivation>,
}
fn canonical_range(range: &RangeDescriptor) -> RangeDescriptor {
    let mut range = range.clone();
    range.approximate_bytes = 0;
    range.approximate_qps = 0;
    range.replicas.retain(|r| r.role == RangeReplicaRole::Voter);
    range.replicas.sort_by_key(|r| r.id);
    range
}
impl HostWriterPolicyFacts {
    fn from_actual(
        topology: &ClusterTopology,
        native: &NativeRecoveryAdmission,
        schema: String,
        restore: Option<MetadataRestoreActivation>,
    ) -> Result<Self> {
        topology.validate()?;
        if native.writer_enforcement_version() != 1
            || topology.range_image_bootstrap_id != Some(native.bootstrap_id())
            || topology.cluster_id != *native.cluster_id()
        {
            return Err(invalid(
                "actual birth is not in this enforced metadata domain",
            ));
        }
        let local = topology
            .nodes
            .get(native.local_node_id())
            .ok_or_else(|| invalid("actual local member absent"))?;
        if local.incarnation != native.local_member_incarnation()
            || local.lifecycle != ClusterNodeLifecycle::Active
        {
            return Err(invalid("local incarnation/lifecycle changed"));
        }
        let metadata_voters = topology.metadata_voters();
        let mut needed = metadata_voters.clone();
        for range in topology.ranges.values() {
            needed.extend(
                range
                    .replicas
                    .iter()
                    .filter(|r| r.role == RangeReplicaRole::Voter)
                    .map(|r| r.node_id.clone()),
            );
        }
        needed.insert(native.local_node_id().clone());
        let mut members = BTreeMap::new();
        for id in needed {
            let node = topology
                .nodes
                .get(&id)
                .ok_or_else(|| invalid("current voter is absent"))?;
            let birth = node
                .native_store_birth_v1
                .as_ref()
                .ok_or_else(|| invalid("current voter has no enrolled native birth"))?;
            birth.validate_for(topology.range_image_bootstrap_id, node.incarnation)?;
            if birth.writer_enforcement_version() != 1
                || node.lifecycle == ClusterNodeLifecycle::Decommissioned
            {
                return Err(invalid("current voter is not from-birth writer enforced"));
            }
            if node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(&schema) {
                return Err(invalid(
                    "current voter schema differs from actual local schema",
                ));
            }
            members.insert(
                id,
                WriterMemberPolicy {
                    incarnation: node.incarnation,
                    birth: birth.clone(),
                    active_pin: node.tls_certificate_sha256.clone(),
                    pending_pin: node.pending_tls_certificate_sha256.clone(),
                },
            );
        }
        let local_birth = &members[native.local_node_id()].birth;
        if local_birth.birth_id() != native.birth_id()
            || local_birth.birth_sha256() != native.birth_sha256()
        {
            return Err(invalid(
                "registered local birth differs from actual database",
            ));
        }
        let facts = Self {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: native.bootstrap_id(),
            local_node_id: native.local_node_id().clone(),
            local_incarnation: native.local_member_incarnation(),
            local_birth_id: native.birth_id(),
            local_birth_sha256: native.birth_sha256().into(),
            metadata_voters,
            members,
            ranges: topology
                .ranges
                .values()
                .map(|r| (r.id, canonical_range(r)))
                .collect(),
            schema_sha256: schema,
            restore_activation: restore,
        };
        crate::distribution_range_image::bounded_json(&facts, 256 * 1024)?;
        Ok(facts)
    }
    pub(crate) fn checksum_sha256(&self) -> Result<String> {
        Ok(hex::encode(Sha256::digest(
            crate::distribution_range_image::bounded_json(self, 256 * 1024)?,
        )))
    }
    pub(crate) fn require_range(&self, range: &RangeDescriptor) -> Result<()> {
        if self.ranges.get(&range.id) != Some(&canonical_range(range)) {
            return Err(invalid(
                "range configuration changed; refresh actual host metadata policy",
            ));
        }
        Ok(())
    }
    /// Cached topology is only an extra refusal check, never a policy publisher.
    pub(crate) fn require_topology(&self, topology: &ClusterTopology) -> Result<()> {
        if topology.cluster_id != self.cluster_id
            || topology.range_image_bootstrap_id != Some(self.bootstrap_id)
            || topology.metadata_voters() != self.metadata_voters
        {
            return Err(invalid("current voter registry changed"));
        }
        for (id, expected) in &self.members {
            let node = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("registered member disappeared"))?;
            if node.incarnation != expected.incarnation
                || node.native_store_birth_v1.as_ref() != Some(&expected.birth)
                || node.tls_certificate_sha256 != expected.active_pin
                || node.pending_tls_certificate_sha256 != expected.pending_pin
                || node.lifecycle == ClusterNodeLifecycle::Decommissioned
                || node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(&self.schema_sha256)
            {
                return Err(invalid("registered birth/member/schema identity changed"));
            }
        }
        for range in topology.ranges.values() {
            for replica in range
                .replicas
                .iter()
                .filter(|r| r.role == RangeReplicaRole::Voter)
            {
                if !self.members.contains_key(&replica.node_id) {
                    return Err(invalid("new data voter is outside installed host policy"));
                }
            }
        }
        Ok(())
    }
}

/// The only policy constructor consumes actual fresh host-MQ evidence and an
/// actual native DB. Neither facts/JSON nor a cached topology constructs this.
#[derive(Debug)]
pub(crate) struct SealedHostWriterEnrollment {
    facts: HostWriterPolicyFacts,
    metadata: HostMetadataReadObservation,
    business: Uuid,
    database_incarnation: String,
    store_open_id: Uuid,
}
impl SealedHostWriterEnrollment {
    pub(crate) fn from_actual(
        metadata: HostMetadataReadObservation,
        business: Uuid,
        database: &BicDb,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<Self> {
        metadata.require_live_request(business)?;
        let topology = metadata.committed_topology(business)?;
        let local = topology
            .nodes
            .get(metadata.requester_node_id())
            .ok_or_else(|| invalid("host-MQ requester is absent"))?;
        if local.incarnation != metadata.requester_incarnation() {
            return Err(invalid("host-MQ local incarnation changed"));
        }
        let native = database.native_recovery_admission(
            &topology.cluster_id,
            topology
                .range_image_bootstrap_id
                .ok_or_else(|| invalid("host-MQ has no native bootstrap domain"))?,
            &local.id,
            local.incarnation,
        )?;
        store.require_native_catalog_durability(database.data_path())?;
        database.require_native_writer_catalogue_custody(store)?;
        store.sync_verified_native_catalogue_before_policy(database.data_path())?;
        let schema = database.schema_compatibility_fingerprint()?.sha256;
        let facts = HostWriterPolicyFacts::from_actual(
            topology,
            &native,
            schema,
            metadata.restore_activation(business)?.cloned(),
        )?;
        if facts.restore_activation.as_ref() != native.restore_activation() {
            return Err(invalid("actual restore domain differs from host metadata"));
        }
        metadata.require_live_request(business)?;
        Ok(Self {
            facts,
            metadata,
            business,
            database_incarnation: database.incarnation_id().into(),
            store_open_id: store.native_writer_store_open_id(),
        })
    }
    pub(crate) fn facts(&self) -> &HostWriterPolicyFacts {
        &self.facts
    }
    pub(crate) fn database_incarnation(&self) -> &str {
        &self.database_incarnation
    }
    pub(crate) fn store_open_id(&self) -> Uuid {
        self.store_open_id
    }
    pub(crate) fn metadata_context_id(&self) -> Uuid {
        self.metadata.context_id()
    }
    pub(crate) fn metadata_term(&self) -> u64 {
        self.metadata.term()
    }
    pub(crate) fn metadata_read_index(&self) -> u64 {
        self.metadata.read_index()
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.metadata.require_live_request(self.business)
    }
}
