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
pub(crate) struct WriterMemberContinuity {
    address: String,
    lifecycle: ClusterNodeLifecycle,
    labels: BTreeMap<String, String>,
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
    // None preserves prior policy bytes/checksum. It permits original policy
    // checks only, NEVER retrospective member-baseline capture for refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    member_continuity: Option<BTreeMap<ClusterNodeId, WriterMemberContinuity>>,
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
        let mut continuity = BTreeMap::new();
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
            continuity.insert(
                id.clone(),
                WriterMemberContinuity {
                    address: node.address.clone(),
                    lifecycle: node.lifecycle.clone(),
                    labels: node.labels.clone(),
                },
            );
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
            member_continuity: Some(continuity),
        };
        crate::distribution_range_image::bounded_json(&facts, 256 * 1024)?;
        Ok(facts)
    }
    /// The DATA part of a registry successor.
    ///
    /// What the rule protects. A registry successor is a metadata-only
    /// change: it adds metadata voters and their identities and must never
    /// be the way a DATA configuration reaches a writer. A writer's
    /// permission to acknowledge on a range comes only from the policy it
    /// was enrolled with, or from the Serving audit it published itself as a
    /// new member of a handoff; a successor's `ranges` are never consulted
    /// for that. If a successor could carry an arbitrary range change, a
    /// member would record, as "its" domain, a voter set it never enrolled
    /// for and never took part in forming, and every later successor would
    /// be measured against that record.
    ///
    /// So a range is either exactly the previous one, or it is reached from
    /// the previous one through completed native joint handoffs that
    /// `committed` itself records: for each step exactly one relocation of
    /// that range whose allocation expected the current epoch, `Completed`
    /// with its source retirement recorded, whose certified scope names the
    /// current voters (at the learner epoch, one above) as its old range and
    /// the next configuration as its final range. `committed` is the
    /// configuration of this member's own fresh quorum read (or the audit of
    /// that read it stored itself); nothing here comes from a request. The
    /// chain must end at the range `committed` has now. No range appears or
    /// disappears. Any other difference is refused as before.
    ///
    /// This grants nothing: `require_range` and `require_command` keep
    /// answering from the enrolled policy or the member's own Serving audit.
    fn require_data_successor(&self, previous: &Self, committed: &ClusterTopology) -> Result<()> {
        if self.ranges == previous.ranges {
            return Ok(());
        }
        let refuse = |detail: String| {
            invalid(format!(
                "registry extension changed original DATA/birth/schema/restore/member domain: {detail}"
            ))
        };
        if committed.cluster_id != self.cluster_id
            || committed.range_image_bootstrap_id != Some(self.bootstrap_id)
            || !self.ranges.keys().eq(previous.ranges.keys())
        {
            return Err(refuse("a range appeared or disappeared, or the committed configuration is of another cluster".into()));
        }
        for (id, range) in &self.ranges {
            if committed.range_by_id(*id).map(canonical_range).as_ref() != Some(range) {
                return Err(refuse(format!(
                    "range {id} is not the committed configuration"
                )));
            }
            let mut current = previous.ranges[id].clone();
            let mut steps = 0;
            while &current != range {
                let mut next = None;
                for plan in committed
                    .relocations
                    .values()
                    .filter(|plan| plan.range_id == *id && plan.expected_epoch == current.epoch)
                {
                    let (old, activation) = plan
                        .completed_native_joint_change()
                        .map_err(|error| refuse(error.to_string()))?;
                    let mut old = canonical_range(old);
                    old.epoch = plan.expected_epoch;
                    let activation = canonical_range(activation);
                    if old != current || activation.epoch <= current.epoch || next.is_some() {
                        return Err(refuse(format!(
                            "{} of range {id} does not continue the previously accepted configuration at epoch {}",
                            plan.id, current.epoch
                        )));
                    }
                    next = Some(activation);
                }
                steps += 1;
                current = match next {
                    Some(next) if steps <= 64 => next,
                    _ => {
                        return Err(refuse(format!(
                            "range {id} changed from epoch {} to epoch {} without a completed committed handoff from the previously accepted configuration",
                            current.epoch, range.epoch
                        )))
                    }
                };
            }
        }
        Ok(())
    }
    /// Additions only, with ALL original and previously added identities
    /// unchanged, and the DATA policy either unchanged or changed exactly by
    /// completed handoffs `committed` records (`require_data_successor`).
    /// `committed` is the configuration this successor was read from.
    /// Neither facts object mints a grant.
    pub(crate) fn require_registry_successor(
        &self,
        previous: &Self,
        committed: &ClusterTopology,
    ) -> Result<()> {
        self.require_data_successor(previous, committed)?;
        if self.cluster_id != previous.cluster_id
            || self.bootstrap_id != previous.bootstrap_id
            || self.local_node_id != previous.local_node_id
            || self.local_incarnation != previous.local_incarnation
            || self.local_birth_id != previous.local_birth_id
            || self.local_birth_sha256 != previous.local_birth_sha256
            || self.schema_sha256 != previous.schema_sha256
            || self.restore_activation != previous.restore_activation
            || !previous.metadata_voters.is_subset(&self.metadata_voters)
            || self.metadata_voters.len() > 64
            || previous
                .members
                .iter()
                .any(|(id, member)| self.members.get(id) != Some(member))
            || self
                .members
                .keys()
                .any(|id| !previous.members.contains_key(id) && !self.metadata_voters.contains(id))
        {
            return Err(invalid(
                "registry extension changed original DATA/birth/schema/restore/member domain",
            ));
        }
        let old_continuity = previous.member_continuity.as_ref().ok_or_else(|| {
            invalid(
                "original native policy has no recorded member continuity; no retroactive refresh",
            )
        })?;
        let new_continuity = self
            .member_continuity
            .as_ref()
            .ok_or_else(|| invalid("registry successor lacks actual recorded member continuity"))?;
        if old_continuity.len() != previous.members.len()
            || old_continuity
                .keys()
                .any(|id| !previous.members.contains_key(id))
            || new_continuity.len() != self.members.len()
            || new_continuity
                .keys()
                .any(|id| !self.members.contains_key(id))
            || old_continuity
                .iter()
                .any(|(id, member)| new_continuity.get(id) != Some(member))
        {
            return Err(invalid(
                "registry extension changed previously accepted member address/lifecycle/labels",
            ));
        }
        if self
            .metadata_voters
            .iter()
            .any(|id| !self.members.contains_key(id))
        {
            return Err(invalid(
                "registry successor omits an actual metadata voter identity",
            ));
        }
        for member in self.members.values() {
            member
                .birth
                .validate_for(Some(self.bootstrap_id), member.incarnation)?;
            if member.birth.writer_enforcement_version() != 1 {
                return Err(invalid(
                    "registry successor member is not from-birth writer enforced",
                ));
            }
        }
        Ok(())
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
    /// Exact snapshot binding for NEW registry refresh and stored audit only.
    /// Ordinary APP/Strong retain their existing identity/schema checks below.
    pub(crate) fn require_refresh_topology(&self, topology: &ClusterTopology) -> Result<()> {
        self.require_topology(topology)?;
        let continuity = self
            .member_continuity
            .as_ref()
            .ok_or_else(|| invalid("registry audit lacks an actual member continuity baseline"))?;
        for (id, expected) in continuity {
            let actual = topology
                .nodes
                .get(id)
                .ok_or_else(|| invalid("recorded writer member disappeared"))?;
            if actual.address != expected.address
                || actual.lifecycle != expected.lifecycle
                || actual.labels != expected.labels
            {
                return Err(invalid(
                    "recorded refresh member address/lifecycle/labels changed",
                ));
            }
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
    #[cfg(test)]
    pub(crate) fn test_legacy_policy_without_member_continuity(&mut self) {
        // Actual fresh MQ/native production remains unchanged; encode only the
        // older OPTIONAL-None contract as a NEGATIVE, never a refresh grant.
        self.facts.member_continuity = None;
    }
    pub(crate) fn joint_domain_audit(
        &self,
        decision: &crate::distribution_range_handoff::NativeJointDecisionData,
    ) -> Result<serde_json::Value> {
        self.metadata
            .joint_writer_domain_audit(self.business, decision)
    }
    pub(crate) fn stable_registry_audit(&self) -> Result<serde_json::Value> {
        let topology = self.metadata.committed_topology(self.business)?;
        if topology.ranges.values().any(|range| {
            range
                .replicas
                .iter()
                .any(|r| r.role != RangeReplicaRole::Voter)
        }) {
            return Err(invalid(
                "metadata-only registry refresh refuses a DATA learner transition",
            ));
        }
        self.metadata.stable_writer_registry_audit(self.business)
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
