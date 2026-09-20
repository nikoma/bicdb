use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{BicDbError, Result};
use crate::record::Record;
use crate::replication::{CommitFrame, ReplicationTlsConfig};

pub const DISTRIBUTION_FORMAT_VERSION: u32 = 1;
pub const DISTRIBUTION_HASH_VERSION: u32 = 1;
pub const CLUSTER_DATA_PROTOCOL_VERSION: u32 = 19;
/// Reserved heartbeat label carrying the canonical local schema identity.
/// Once the range leader advertises this label, placement and data operations
/// are fenced to nodes advertising the identical schema.
pub const SCHEMA_COMPATIBILITY_NODE_LABEL: &str = "bicdb.schema.sha256";
/// Quorum-published additive target accepted only while the active fingerprint
/// remains authoritative. Nodes may advertise verified signed prefixes of this
/// target during a coordinated compatibility window.
pub const SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL: &str = "bicdb.schema.pending.sha256";
/// An empty node may receive a bounded leader schema bundle before snapshot
/// data. This claim is host-derived and never sufficient for reads, writes, or
/// promotion; those still require an exact compatibility fingerprint.
pub const SCHEMA_BOOTSTRAP_NODE_LABEL: &str = "bicdb.schema.bootstrap";
pub const DEFAULT_CLUSTER_TOPOLOGY: &str = "cluster-topology.json";
pub const DEFAULT_DISTRIBUTION_CONFIG: &str = "bicdb-distribution.json";
const DEFAULT_TOPOLOGY_HISTORY_LIMIT: usize = 64;
const DEFAULT_APPLIED_PLAN_HISTORY_LIMIT: usize = 64;
const DEFAULT_RELOCATION_HISTORY_LIMIT: usize = 4_096;
const MIN_CLUSTER_FRAME_BYTES: usize = 64 * 1024;
const MAX_CLUSTER_FRAME_BYTES: usize = 512 * 1024 * 1024;

fn default_next_relocation_id() -> u64 {
    1
}

fn default_metadata_election_timeout_ms() -> u64 {
    1_500
}

fn default_metadata_heartbeat_interval_ms() -> u64 {
    300
}

fn cluster_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNetworkTransportConfig {
    pub connect_timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub max_frame_bytes: usize,
    pub max_inbound_connections: usize,
    pub dev_localhost_plaintext: bool,
    pub tls: Option<ReplicationTlsConfig>,
}

impl Default for ClusterNetworkTransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            io_timeout_ms: 30_000,
            max_frame_bytes: 128 * 1024 * 1024,
            max_inbound_connections: 64,
            // The default distribution address is loopback. Any non-loopback
            // configuration fails validation until mTLS is supplied.
            dev_localhost_plaintext: true,
            tls: None,
        }
    }
}

impl ClusterNetworkTransportConfig {
    pub fn validate(&self) -> Result<()> {
        if self.connect_timeout_ms == 0 || self.connect_timeout_ms > 3_600_000 {
            return Err(cluster_error(
                "cluster connect timeout must be between 1ms and 1h",
            ));
        }
        if self.io_timeout_ms == 0 || self.io_timeout_ms > 3_600_000 {
            return Err(cluster_error(
                "cluster I/O timeout must be between 1ms and 1h",
            ));
        }
        if !(MIN_CLUSTER_FRAME_BYTES..=MAX_CLUSTER_FRAME_BYTES).contains(&self.max_frame_bytes) {
            return Err(cluster_error(format!(
                "cluster max frame bytes must be between {MIN_CLUSTER_FRAME_BYTES} and {MAX_CLUSTER_FRAME_BYTES}"
            )));
        }
        if self.max_inbound_connections == 0 || self.max_inbound_connections > 65_536 {
            return Err(cluster_error(
                "cluster inbound connection limit must be between 1 and 65536",
            ));
        }
        if self.dev_localhost_plaintext {
            if self.tls.is_some() {
                return Err(cluster_error(
                    "cluster localhost plaintext and TLS are mutually exclusive",
                ));
            }
            return Ok(());
        }
        let tls = self
            .tls
            .as_ref()
            .ok_or_else(|| cluster_error("mTLS is mandatory for cluster data RPC"))?;
        if !tls.require_client_cert {
            return Err(cluster_error(
                "cluster data RPC requires mTLS client certificates",
            ));
        }
        if tls.dev_localhost_plaintext {
            return Err(cluster_error(
                "cluster TLS identity cannot enable replication plaintext mode",
            ));
        }
        Ok(())
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(cluster_error(format!(
            "invalid {kind} `{value}`: use 1-128 ASCII letters, digits, '-', '_', or '.'"
        )));
    }
    Ok(())
}

fn validate_certificate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(cluster_error(
            "cluster node TLS certificate SHA-256 must be 64 lowercase hexadecimal digits",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(String);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("cluster id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for ClusterId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ClusterId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterNodeId(String);

impl ClusterNodeId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_identifier("cluster node id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClusterNodeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for ClusterNodeId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ClusterNodeId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterNodeLifecycle {
    Active,
    Draining,
    Decommissioned,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetadataMemberRole {
    Learner,
    #[default]
    Voter,
}

impl MetadataMemberRole {
    fn is_voter(role: &Self) -> bool {
        *role == Self::Voter
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterNodeLiveness {
    Live,
    Suspect,
    Dead,
    Decommissioned,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RangeId(u64);

impl RangeId {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(cluster_error("range id must be greater than zero"));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RangeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "r{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ReplicaId(u64);

impl ReplicaId {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(cluster_error("replica id must be greater than zero"));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ReplicaId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "replica-{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RelocationId(u64);

impl RelocationId {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(cluster_error("relocation id must be greater than zero"));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RelocationId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "relocation-{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeReplicaRole {
    Voter,
    Learner,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeReplica {
    pub id: ReplicaId,
    pub node_id: ClusterNodeId,
    pub role: RangeReplicaRole,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementPolicy {
    #[serde(default)]
    pub required_node_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub distinct_failure_domains: Vec<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StandardFailureDomain {
    Server,
    Rack,
    Zone,
    Region,
}

impl StandardFailureDomain {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Rack => "rack",
            Self::Zone => "zone",
            Self::Region => "region",
        }
    }
}

impl PlacementPolicy {
    pub fn with_standard_failure_domains(
        mut self,
        domains: impl IntoIterator<Item = StandardFailureDomain>,
    ) -> Self {
        self.distinct_failure_domains = domains
            .into_iter()
            .map(|domain| domain.label().to_string())
            .collect();
        self.distinct_failure_domains.sort();
        self.distinct_failure_domains.dedup();
        self
    }

    pub fn validate(&self) -> Result<()> {
        for (key, value) in &self.required_node_labels {
            validate_label(key, value)?;
        }
        let mut seen = std::collections::BTreeSet::new();
        for label in &self.distinct_failure_domains {
            validate_identifier("failure-domain label", label)?;
            if !seen.insert(label) {
                return Err(cluster_error(format!(
                    "duplicate failure-domain label `{label}`"
                )));
            }
        }
        Ok(())
    }

    pub fn node_is_eligible(&self, node: &ClusterNode) -> bool {
        self.required_node_labels
            .iter()
            .all(|(key, value)| node.labels.get(key) == Some(value))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub start_token: u64,
    /// Exclusive end. `None` represents the end of the 64-bit token space.
    pub end_token: Option<u64>,
    pub epoch: u64,
    pub replicas: Vec<RangeReplica>,
    pub leader: ClusterNodeId,
    pub approximate_bytes: u64,
    pub approximate_qps: u64,
    pub placement: PlacementPolicy,
}

impl RangeDescriptor {
    pub fn contains_token(&self, token: u64) -> bool {
        token >= self.start_token && self.end_token.map(|end| token < end).unwrap_or(true)
    }

    pub fn voter_count(&self) -> usize {
        self.replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .count()
    }

    pub fn required_schema_sha256<'a>(
        &self,
        nodes: &'a BTreeMap<ClusterNodeId, ClusterNode>,
    ) -> Option<&'a str> {
        nodes
            .get(&self.leader)
            .and_then(|node| node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL))
            .map(String::as_str)
    }

    /// Rolling-upgrade compatible schema placement fence. Legacy topologies
    /// without an advertised leader fingerprint retain their existing
    /// placement. As soon as the leader advertises one, every candidate and
    /// existing replica must match it exactly.
    pub fn node_schema_is_compatible(
        &self,
        nodes: &BTreeMap<ClusterNodeId, ClusterNode>,
        node: &ClusterNode,
    ) -> bool {
        self.required_schema_sha256(nodes).is_none_or(|required| {
            node.labels
                .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                .is_some_and(|observed| observed == required)
        })
    }

    pub fn node_accepts_schema_bootstrap(node: &ClusterNode) -> bool {
        node.labels
            .get(SCHEMA_BOOTSTRAP_NODE_LABEL)
            .is_some_and(|value| value == "true")
    }

    fn validate(&self, nodes: &BTreeMap<ClusterNodeId, ClusterNode>) -> Result<()> {
        if self.id.0 == 0 || self.epoch == 0 {
            return Err(cluster_error(format!(
                "range {} must have non-zero id and epoch",
                self.id
            )));
        }
        if self.end_token.is_some_and(|end| end <= self.start_token) {
            return Err(cluster_error(format!(
                "range {} has invalid token interval [{}, {:?})",
                self.id, self.start_token, self.end_token
            )));
        }
        self.placement.validate()?;
        if self.replicas.is_empty() {
            return Err(cluster_error(format!(
                "range {} must have at least one replica",
                self.id
            )));
        }
        let mut replica_ids = std::collections::BTreeSet::new();
        let mut replica_nodes = std::collections::BTreeSet::new();
        let mut leader_is_voter = false;
        for replica in &self.replicas {
            if !replica_ids.insert(replica.id) {
                return Err(cluster_error(format!(
                    "range {} has duplicate replica id {}",
                    self.id, replica.id
                )));
            }
            if !replica_nodes.insert(replica.node_id.clone()) {
                return Err(cluster_error(format!(
                    "range {} has multiple replicas on node {}",
                    self.id, replica.node_id
                )));
            }
            if !nodes.contains_key(&replica.node_id) {
                return Err(cluster_error(format!(
                    "range {} replica {} references unknown node {}",
                    self.id, replica.id, replica.node_id
                )));
            }
            if nodes[&replica.node_id].metadata_role != MetadataMemberRole::Voter {
                return Err(cluster_error(format!(
                    "range {} replica {} is placed on metadata learner {}",
                    self.id, replica.id, replica.node_id
                )));
            }
            if replica.node_id == self.leader && replica.role == RangeReplicaRole::Voter {
                leader_is_voter = true;
            }
        }
        if !leader_is_voter {
            return Err(cluster_error(format!(
                "range {} leader {} is not a voting replica",
                self.id, self.leader
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaMoveReason {
    UnderReplicated,
    DeadReplica,
    DrainingReplica,
    PlacementViolation,
    LoadBalance,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannedReplicaMove {
    pub range_id: RangeId,
    pub expected_epoch: u64,
    pub source: Option<ClusterNodeId>,
    pub target: ClusterNodeId,
    pub estimated_bytes: u64,
    pub reason: ReplicaMoveReason,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannedLeaderTransfer {
    pub range_id: RangeId,
    pub expected_epoch: u64,
    pub source: ClusterNodeId,
    pub target: ClusterNodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnplacedRange {
    pub range_id: RangeId,
    pub missing_replicas: u8,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalancePlan {
    pub id: String,
    pub topology_generation: u64,
    pub created_at_ms: u64,
    pub replica_moves: Vec<PlannedReplicaMove>,
    pub leader_transfers: Vec<PlannedLeaderTransfer>,
    pub unplaced_ranges: Vec<UnplacedRange>,
    pub estimated_bytes_in_flight: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeAvailability {
    pub range_id: RangeId,
    pub live_voters: u8,
    pub suspect_voters: u8,
    pub dead_voters: u8,
    pub required_quorum: u8,
    pub live_leader: bool,
}

impl RangeAvailability {
    pub fn available(&self) -> bool {
        self.live_voters >= self.required_quorum && self.live_leader
    }

    pub fn has_live_quorum(&self) -> bool {
        self.live_voters >= self.required_quorum
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureRepairPlan {
    pub created_at_ms: u64,
    pub node_liveness: BTreeMap<ClusterNodeId, ClusterNodeLiveness>,
    pub range_availability: Vec<RangeAvailability>,
    pub unavailable_ranges: Vec<RangeId>,
    pub under_replicated_ranges: Vec<RangeId>,
    pub rebalance: RebalancePlan,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRemovalSafety {
    pub node_id: ClusterNodeId,
    pub can_decommission: bool,
    pub replica_ranges: Vec<RangeId>,
    pub leader_ranges: Vec<RangeId>,
    pub active_relocations: Vec<RelocationId>,
    pub placement_blocked_ranges: Vec<RangeId>,
    pub remaining_active_nodes: usize,
    pub required_active_nodes: usize,
    pub reasons: Vec<String>,
}

impl RebalancePlan {
    pub fn is_empty(&self) -> bool {
        self.replica_moves.is_empty()
            && self.leader_transfers.is_empty()
            && self.unplaced_ranges.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelocationPhase {
    LearnerAllocated,
    SnapshotCopying,
    CatchingUp,
    ReadyToPromote,
    Promoted,
    CleaningUp,
    Completed,
    Failed,
}

impl RelocationPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeRelocation {
    pub id: RelocationId,
    pub plan_id: String,
    pub range_id: RangeId,
    /// Epoch observed by the planner before the learner was allocated.
    pub expected_epoch: u64,
    /// Epoch installed with the learner. Promotion is rejected if it changed.
    pub learner_epoch: u64,
    pub promoted_epoch: Option<u64>,
    pub source: Option<ClusterNodeId>,
    pub target: ClusterNodeId,
    pub target_replica_id: ReplicaId,
    pub reason: ReplicaMoveReason,
    pub phase: RelocationPhase,
    pub resume_phase: Option<RelocationPhase>,
    pub snapshot_id: Option<String>,
    pub snapshot_sha256: Option<String>,
    pub snapshot_bytes_copied: u64,
    pub snapshot_resume_after_key: Option<String>,
    pub snapshot_commit_sequence: Option<u64>,
    pub destination_durable_commit_sequence: Option<u64>,
    pub source_commit_sequence: Option<u64>,
    #[serde(default)]
    pub cleanup_resume_after_key: Option<String>,
    #[serde(default)]
    pub cleanup_records_deleted: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_error: Option<String>,
}

impl RangeRelocation {
    pub fn is_active(&self) -> bool {
        !self.phase.is_terminal()
    }

    fn validate(&self, topology: &ClusterTopology) -> Result<()> {
        if self.id.0 == 0 || self.expected_epoch == 0 || self.learner_epoch == 0 {
            return Err(cluster_error(format!(
                "relocation {} has an invalid zero id or epoch",
                self.id
            )));
        }
        if self.plan_id.is_empty() {
            return Err(cluster_error(format!(
                "relocation {} has an empty plan id",
                self.id
            )));
        }
        if self.phase == RelocationPhase::Completed {
            if self.promoted_epoch.is_none() {
                return Err(cluster_error(format!(
                    "completed relocation {} has no promoted epoch",
                    self.id
                )));
            }
            if self.snapshot_id.is_none()
                || self.snapshot_sha256.is_none()
                || self.snapshot_commit_sequence.is_none()
            {
                return Err(cluster_error(format!(
                    "completed relocation {} has no durable snapshot metadata",
                    self.id
                )));
            }
            return Ok(());
        }
        let range = topology.range_by_id(self.range_id).ok_or_else(|| {
            cluster_error(format!(
                "relocation {} references unknown range {}",
                self.id, self.range_id
            ))
        })?;
        let target = range
            .replicas
            .iter()
            .find(|replica| replica.id == self.target_replica_id)
            .ok_or_else(|| {
                cluster_error(format!(
                    "relocation {} target replica {} is absent from range {}",
                    self.id, self.target_replica_id, self.range_id
                ))
            })?;
        if target.node_id != self.target {
            return Err(cluster_error(format!(
                "relocation {} target replica belongs to {}, expected {}",
                self.id, target.node_id, self.target
            )));
        }
        let should_be_voter = matches!(
            self.phase,
            RelocationPhase::Promoted | RelocationPhase::CleaningUp | RelocationPhase::Completed
        ) || (self.phase == RelocationPhase::Failed
            && self.resume_phase.is_some_and(|phase| {
                matches!(
                    phase,
                    RelocationPhase::Promoted | RelocationPhase::CleaningUp
                )
            }));
        let expected_role = if should_be_voter {
            RangeReplicaRole::Voter
        } else {
            RangeReplicaRole::Learner
        };
        if target.role != expected_role {
            return Err(cluster_error(format!(
                "relocation {} target replica role is {:?}, expected {:?}",
                self.id, target.role, expected_role
            )));
        }
        if matches!(
            self.phase,
            RelocationPhase::Promoted | RelocationPhase::CleaningUp
        ) && range.epoch != self.promoted_epoch.unwrap_or(0)
        {
            return Err(cluster_error(format!(
                "promoted relocation {} does not match range epoch {}",
                self.id, range.epoch
            )));
        } else if !matches!(
            self.phase,
            RelocationPhase::Promoted
                | RelocationPhase::CleaningUp
                | RelocationPhase::Completed
                | RelocationPhase::Failed
        ) && range.epoch != self.learner_epoch
        {
            return Err(cluster_error(format!(
                "relocation {} learner epoch {} is stale; range is at {}",
                self.id, self.learner_epoch, range.epoch
            )));
        }
        if self.phase == RelocationPhase::Failed && self.resume_phase.is_none() {
            return Err(cluster_error(format!(
                "failed relocation {} has no resumable phase",
                self.id
            )));
        }
        if self.phase != RelocationPhase::Failed && self.resume_phase.is_some() {
            return Err(cluster_error(format!(
                "non-failed relocation {} has a resume phase",
                self.id
            )));
        }
        if matches!(
            self.phase,
            RelocationPhase::CatchingUp
                | RelocationPhase::ReadyToPromote
                | RelocationPhase::Promoted
                | RelocationPhase::CleaningUp
                | RelocationPhase::Completed
        ) && (self.snapshot_id.is_none()
            || self.snapshot_sha256.is_none()
            || self.snapshot_commit_sequence.is_none())
        {
            return Err(cluster_error(format!(
                "relocation {} advanced without durable snapshot metadata",
                self.id
            )));
        }
        if self.phase == RelocationPhase::ReadyToPromote {
            let source = self.source_commit_sequence.unwrap_or(u64::MAX);
            let durable = self.destination_durable_commit_sequence.unwrap_or(0);
            if durable < source {
                return Err(cluster_error(format!(
                    "relocation {} is ready before destination parity",
                    self.id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSnapshotOptions {
    /// Maximum matching records retained before invoking the visitor.
    pub max_records_per_batch: usize,
    /// Maximum serialized matching-record bytes retained per batch.
    pub max_bytes_per_batch: usize,
    /// Reject a pathological record rather than violating the memory bound.
    pub max_record_bytes: usize,
}

impl Default for RangeSnapshotOptions {
    fn default() -> Self {
        Self {
            max_records_per_batch: 1_024,
            max_bytes_per_batch: 8 * 1024 * 1024,
            max_record_bytes: 8 * 1024 * 1024,
        }
    }
}

impl RangeSnapshotOptions {
    pub fn validate(&self) -> Result<()> {
        if self.max_records_per_batch == 0
            || self.max_bytes_per_batch == 0
            || self.max_record_bytes == 0
        {
            return Err(cluster_error(
                "range snapshot record and byte limits must be greater than zero",
            ));
        }
        if self.max_record_bytes > self.max_bytes_per_batch {
            return Err(cluster_error(
                "range snapshot max_record_bytes cannot exceed max_bytes_per_batch",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeSnapshotBatch {
    pub collection: String,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub snapshot_commit_sequence: u64,
    /// Resume the source scan strictly after this collection key.
    pub resume_after_key: String,
    pub serialized_record_bytes: usize,
    pub records: Vec<Record>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeSnapshotExport {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub snapshot_commit_sequence: u64,
    pub records_exported: u64,
    pub serialized_record_bytes: u64,
    pub resume_after_key: Option<String>,
    pub completed: bool,
}

/// Keep the commit sequence continuous while retaining only writes owned by a
/// range. Empty filtered frames are intentional: they advance the destination
/// watermark and prove parity without applying another range's writes.
pub fn filter_commit_frame_for_range(
    frame: &CommitFrame,
    range: &RangeDescriptor,
) -> Result<CommitFrame> {
    frame.verify_checksum()?;
    let mut filtered = frame.clone();
    filtered.writes.retain(|write| {
        range.contains_token(distribution_key_token(&write.collection, &write.record_id))
    });
    filtered.checksum = filtered.calculate_checksum();
    Ok(filtered)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalanceOptions {
    pub max_replica_moves: usize,
    pub max_moves_per_node: usize,
    pub max_leader_transfers: usize,
    pub max_bytes_in_flight: u64,
    pub max_target_utilization_per_million: u64,
    pub unknown_range_bytes: u64,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            max_replica_moves: 64,
            max_moves_per_node: 4,
            max_leader_transfers: 64,
            max_bytes_in_flight: 256 * 1024 * 1024 * 1024,
            max_target_utilization_per_million: 850_000,
            unknown_range_bytes: 64 * 1024 * 1024,
        }
    }
}

impl RebalanceOptions {
    pub fn validate(&self) -> Result<()> {
        if self.max_replica_moves == 0 || self.max_moves_per_node == 0 {
            return Err(cluster_error(
                "rebalance move limits must be greater than zero",
            ));
        }
        if self.max_leader_transfers == 0 {
            return Err(cluster_error(
                "rebalance max_leader_transfers must be greater than zero",
            ));
        }
        if self.max_bytes_in_flight == 0 || self.unknown_range_bytes == 0 {
            return Err(cluster_error(
                "rebalance byte limits must be greater than zero",
            ));
        }
        if !(1..=1_000_000).contains(&self.max_target_utilization_per_million) {
            return Err(cluster_error(
                "rebalance target utilization must be between 1 and 1000000",
            ));
        }
        Ok(())
    }
}

/// Public policy seam for controllers that choose cluster placement and
/// failure-repair work.
///
/// BicDB owns and validates the topology, plan formats, relocation state
/// machine, fencing, and plan application. A controller owns only the policy
/// decision that produces a proposed plan. Third-party and commercial
/// controllers can therefore replace placement policy without patching the
/// database engine or receiving unchecked storage authority.
pub trait ClusterPlacementPlanner: std::fmt::Debug + Send + Sync {
    fn plan_failure_repair(
        &self,
        topology: &ClusterTopology,
        config: &DistributionConfig,
        options: &RebalanceOptions,
        now_ms: u64,
    ) -> Result<FailureRepairPlan>;
}

/// Stable, versioned placement token for a logical namespace and key.
///
/// This deliberately does not use Rust's `Hash` implementations: those are
/// process/runtime details and would move data after an upgrade. The namespace
/// should normally be the database/table or collection identity.
pub fn distribution_key_token(namespace: &str, key: &str) -> u64 {
    let mut digest = Sha256::new();
    digest.update(b"bicdb-distribution-key-v1\0");
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    digest.update((key.len() as u64).to_be_bytes());
    digest.update(key.as_bytes());
    let digest = digest.finalize();
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    )
}

fn bootstrap_ranges(
    count: u32,
    local_node: &ClusterNodeId,
    placement: &PlacementPolicy,
) -> Result<(BTreeMap<u64, RangeDescriptor>, u64, u64)> {
    if count == 0 || count > 65_536 || !count.is_power_of_two() {
        return Err(cluster_error(
            "distribution initial_ranges must be a power of two between 1 and 65536",
        ));
    }
    placement.validate()?;
    let width = (u128::from(u64::MAX) + 1) / u128::from(count);
    let mut ranges = BTreeMap::new();
    let mut next_replica_id = 1_u64;
    for index in 0..count {
        let start_token = (u128::from(index) * width) as u64;
        let end_token = if index + 1 == count {
            None
        } else {
            Some((u128::from(index + 1) * width) as u64)
        };
        let range_id = u64::from(index) + 1;
        let descriptor = RangeDescriptor {
            id: RangeId(range_id),
            start_token,
            end_token,
            epoch: 1,
            replicas: vec![RangeReplica {
                id: ReplicaId(next_replica_id),
                node_id: local_node.clone(),
                role: RangeReplicaRole::Voter,
            }],
            leader: local_node.clone(),
            approximate_bytes: 0,
            approximate_qps: 0,
            placement: placement.clone(),
        };
        next_replica_id = next_replica_id.saturating_add(1);
        ranges.insert(start_token, descriptor);
    }
    Ok((ranges, u64::from(count) + 1, next_replica_id))
}

fn validate_range_map(
    ranges: &BTreeMap<u64, RangeDescriptor>,
    nodes: &BTreeMap<ClusterNodeId, ClusterNode>,
) -> Result<()> {
    if ranges.is_empty() {
        return Err(cluster_error(
            "cluster topology must contain at least one range",
        ));
    }
    let mut expected_start = 0_u64;
    let mut saw_open_end = false;
    let mut range_ids = std::collections::BTreeSet::new();
    let mut global_replica_ids = std::collections::BTreeSet::new();
    for (start, range) in ranges {
        if *start != range.start_token {
            return Err(cluster_error(format!(
                "range map key {start} does not match range {} start {}",
                range.id, range.start_token
            )));
        }
        if saw_open_end {
            return Err(cluster_error("range appears after the end of token space"));
        }
        if *start != expected_start {
            return Err(cluster_error(format!(
                "token-space gap or overlap: expected range start {expected_start}, got {start}"
            )));
        }
        if !range_ids.insert(range.id) {
            return Err(cluster_error(format!("duplicate range id {}", range.id)));
        }
        range.validate(nodes)?;
        for replica in &range.replicas {
            if !global_replica_ids.insert(replica.id) {
                return Err(cluster_error(format!(
                    "replica id {} is reused across ranges",
                    replica.id
                )));
            }
        }
        match range.end_token {
            Some(end) => expected_start = end,
            None => saw_open_end = true,
        }
    }
    if !saw_open_end {
        return Err(cluster_error(
            "final range does not cover the end of token space",
        ));
    }
    Ok(())
}

fn range_estimated_bytes(range: &RangeDescriptor, options: &RebalanceOptions) -> u64 {
    range.approximate_bytes.max(options.unknown_range_bytes)
}

fn node_can_receive(
    nodes: &BTreeMap<ClusterNodeId, ClusterNode>,
    node: &ClusterNode,
    range: &RangeDescriptor,
    now_ms: u64,
    config: &DistributionConfig,
    options: &RebalanceOptions,
    projected_used: u64,
    incoming_bytes: u64,
) -> bool {
    if node.lifecycle != ClusterNodeLifecycle::Active
        || node.metadata_role != MetadataMemberRole::Voter
        || node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms)
            != ClusterNodeLiveness::Live
        || !range.placement.node_is_eligible(node)
        || !(range.node_schema_is_compatible(nodes, node)
            || RangeDescriptor::node_accepts_schema_bootstrap(node))
        || range
            .placement
            .distinct_failure_domains
            .iter()
            .any(|label| !node.labels.contains_key(label))
    {
        return false;
    }
    let after = projected_used.saturating_add(incoming_bytes);
    after <= node.capacity_bytes
        && after.saturating_mul(1_000_000)
            <= node
                .capacity_bytes
                .saturating_mul(options.max_target_utilization_per_million)
}

fn failure_domain_conflicts(
    range: &RangeDescriptor,
    topology: &ClusterTopology,
    candidate: &ClusterNode,
    excluded: Option<&ClusterNodeId>,
    additional_nodes: &std::collections::BTreeSet<ClusterNodeId>,
) -> u64 {
    let mut conflicts = 0_u64;
    for label in &range.placement.distinct_failure_domains {
        let Some(candidate_value) = candidate.labels.get(label) else {
            return u64::MAX;
        };
        let mut values = std::collections::BTreeSet::new();
        for replica in &range.replicas {
            if excluded == Some(&replica.node_id) {
                continue;
            }
            let Some(node) = topology.nodes.get(&replica.node_id) else {
                continue;
            };
            if let Some(value) = node.labels.get(label) {
                values.insert(value);
            }
        }
        for node_id in additional_nodes {
            if excluded == Some(node_id) {
                continue;
            }
            if let Some(value) = topology
                .nodes
                .get(node_id)
                .and_then(|node| node.labels.get(label))
            {
                values.insert(value);
            }
        }
        if values.contains(candidate_value) {
            conflicts = conflicts.saturating_add(1);
        }
    }
    conflicts
}

fn replica_invalid_reason(
    topology: &ClusterTopology,
    range: &RangeDescriptor,
    replica: &RangeReplica,
    config: &DistributionConfig,
    now_ms: u64,
) -> Option<ReplicaMoveReason> {
    let node = topology.nodes.get(&replica.node_id)?;
    match node.lifecycle {
        ClusterNodeLifecycle::Draining => return Some(ReplicaMoveReason::DrainingReplica),
        ClusterNodeLifecycle::Decommissioned => return Some(ReplicaMoveReason::DeadReplica),
        ClusterNodeLifecycle::Active => {}
    }
    match node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms) {
        ClusterNodeLiveness::Dead | ClusterNodeLiveness::Decommissioned => {
            return Some(ReplicaMoveReason::DeadReplica);
        }
        ClusterNodeLiveness::Suspect | ClusterNodeLiveness::Live => {}
    }
    if !range.placement.node_is_eligible(node)
        || !range.node_schema_is_compatible(&topology.nodes, node)
        || range
            .placement
            .distinct_failure_domains
            .iter()
            .any(|label| !node.labels.contains_key(label))
    {
        return Some(ReplicaMoveReason::PlacementViolation);
    }
    None
}

fn choose_target(
    topology: &ClusterTopology,
    range: &RangeDescriptor,
    excluded_source: Option<&ClusterNodeId>,
    occupied: &std::collections::BTreeSet<ClusterNodeId>,
    now_ms: u64,
    config: &DistributionConfig,
    options: &RebalanceOptions,
    projected_used: &BTreeMap<ClusterNodeId, u64>,
    projected_replicas: &BTreeMap<ClusterNodeId, usize>,
    per_node_moves: &BTreeMap<ClusterNodeId, usize>,
    incoming_bytes: u64,
) -> Option<ClusterNodeId> {
    topology
        .nodes
        .values()
        .filter(|node| {
            !occupied.contains(&node.id)
                && per_node_moves.get(&node.id).copied().unwrap_or(0) < options.max_moves_per_node
                && node_can_receive(
                    &topology.nodes,
                    node,
                    range,
                    now_ms,
                    config,
                    options,
                    projected_used
                        .get(&node.id)
                        .copied()
                        .unwrap_or(node.used_bytes),
                    incoming_bytes,
                )
        })
        .min_by_key(|node| {
            let conflicts =
                failure_domain_conflicts(range, topology, node, excluded_source, occupied);
            let after = projected_used
                .get(&node.id)
                .copied()
                .unwrap_or(node.used_bytes)
                .saturating_add(incoming_bytes);
            let utilization = after
                .saturating_mul(1_000_000)
                .checked_div(node.capacity_bytes)
                .unwrap_or(1_000_000);
            (
                conflicts,
                utilization,
                projected_replicas.get(&node.id).copied().unwrap_or(0),
                node.id.clone(),
            )
        })
        .map(|node| node.id.clone())
}

pub fn build_rebalance_plan(
    topology: &ClusterTopology,
    config: &DistributionConfig,
    options: &RebalanceOptions,
    now_ms: u64,
) -> Result<RebalancePlan> {
    topology.validate()?;
    config.validate()?;
    options.validate()?;
    if topology.cluster_id != config.cluster_id {
        return Err(cluster_error(format!(
            "cannot plan cluster {} with config for {}",
            topology.cluster_id, config.cluster_id
        )));
    }

    let active_relocation_ranges = topology
        .relocations
        .values()
        .filter(|relocation| relocation.is_active())
        .map(|relocation| relocation.range_id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut replica_moves = Vec::new();
    let mut unplaced_ranges = Vec::new();
    let mut projected_used = topology
        .nodes
        .iter()
        .map(|(id, node)| (id.clone(), node.used_bytes))
        .collect::<BTreeMap<_, _>>();
    let mut projected_replicas = topology
        .nodes
        .keys()
        .map(|id| (id.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for range in topology.ranges.values() {
        for replica in &range.replicas {
            *projected_replicas
                .entry(replica.node_id.clone())
                .or_default() += 1;
        }
    }
    let mut per_node_moves = BTreeMap::<ClusterNodeId, usize>::new();
    let mut bytes_in_flight = 0_u64;

    for range in topology.ranges.values() {
        if replica_moves.len() >= options.max_replica_moves {
            break;
        }
        if active_relocation_ranges.contains(&range.id) {
            continue;
        }
        let bytes = range_estimated_bytes(range, options);
        let mut occupied = range
            .replicas
            .iter()
            .map(|replica| replica.node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut invalid = range
            .replicas
            .iter()
            .filter_map(|replica| {
                replica_invalid_reason(topology, range, replica, config, now_ms)
                    .map(|reason| (replica.node_id.clone(), reason))
            })
            .collect::<Vec<_>>();
        invalid.sort_by(|left, right| left.0.cmp(&right.0));
        let valid_replica_count = range.replicas.len().saturating_sub(invalid.len());
        let desired = usize::from(topology.replication_factor);
        let missing = desired.saturating_sub(valid_replica_count);
        let required_moves = missing.max(invalid.len());
        let mut placed = 0_usize;
        // A range may have only one active relocation at a time. Scheduling
        // one replacement per range also keeps the range epoch in a plan
        // meaningful: after the learner is installed the next controller
        // pass observes the new epoch and can safely plan another replica.
        for index in 0..required_moves.min(1) {
            if replica_moves.len() >= options.max_replica_moves
                || bytes_in_flight.saturating_add(bytes) > options.max_bytes_in_flight
            {
                break;
            }
            let (source, reason) = invalid
                .get(index)
                .cloned()
                .map(|(node, reason)| (Some(node), reason))
                .unwrap_or((None, ReplicaMoveReason::UnderReplicated));
            let Some(target) = choose_target(
                topology,
                range,
                source.as_ref(),
                &occupied,
                now_ms,
                config,
                options,
                &projected_used,
                &projected_replicas,
                &per_node_moves,
                bytes,
            ) else {
                continue;
            };
            occupied.insert(target.clone());
            *projected_used.entry(target.clone()).or_default() = projected_used
                .get(&target)
                .copied()
                .unwrap_or(0)
                .saturating_add(bytes);
            *projected_replicas.entry(target.clone()).or_default() += 1;
            *per_node_moves.entry(target.clone()).or_default() += 1;
            if let Some(source) = &source {
                *projected_used.entry(source.clone()).or_default() = projected_used
                    .get(source)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(bytes);
                *projected_replicas.entry(source.clone()).or_default() = projected_replicas
                    .get(source)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1);
                *per_node_moves.entry(source.clone()).or_default() += 1;
            }
            replica_moves.push(PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source,
                target,
                estimated_bytes: bytes,
                reason,
            });
            bytes_in_flight = bytes_in_flight.saturating_add(bytes);
            placed += 1;
        }
        if required_moves > 0 && placed == 0 {
            let missing_replicas = required_moves.min(usize::from(u8::MAX)) as u8;
            unplaced_ranges.push(UnplacedRange {
                range_id: range.id,
                missing_replicas,
                reason: "no eligible target within placement, capacity, or movement limits"
                    .to_string(),
            });
        }
    }

    // Once every range has enough healthy replicas, adding a node must still
    // redistribute data. Move one replica at a time from nodes above the
    // ceiling to nodes below it; target selection keeps the same capacity and
    // failure-domain rules as repair.
    if unplaced_ranges.is_empty() && replica_moves.len() < options.max_replica_moves {
        let eligible_nodes = topology
            .nodes
            .values()
            .filter(|node| {
                node.lifecycle == ClusterNodeLifecycle::Active
                    && node.metadata_role == MetadataMemberRole::Voter
                    && node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms)
                        == ClusterNodeLiveness::Live
            })
            .map(|node| node.id.clone())
            .collect::<Vec<_>>();
        if !eligible_nodes.is_empty() {
            let total_replicas: usize = projected_replicas.values().sum();
            let ceiling = total_replicas.div_ceil(eligible_nodes.len());
            loop {
                if replica_moves.len() >= options.max_replica_moves {
                    break;
                }
                let Some(source) = eligible_nodes
                    .iter()
                    .filter(|node| projected_replicas.get(*node).copied().unwrap_or(0) > ceiling)
                    .max_by_key(|node| {
                        (
                            projected_replicas.get(*node).copied().unwrap_or(0),
                            projected_used.get(*node).copied().unwrap_or(0),
                            std::cmp::Reverse((*node).clone()),
                        )
                    })
                    .cloned()
                else {
                    break;
                };
                let mut candidate_ranges = topology
                    .ranges
                    .values()
                    .filter(|range| {
                        !active_relocation_ranges.contains(&range.id)
                            && range
                                .replicas
                                .iter()
                                .any(|replica| replica.node_id == source)
                            && !replica_moves
                                .iter()
                                .any(|planned| planned.range_id == range.id)
                    })
                    .collect::<Vec<_>>();
                candidate_ranges.sort_by_key(|range| {
                    (
                        std::cmp::Reverse(range_estimated_bytes(range, options)),
                        range.id,
                    )
                });
                let mut selected = None;
                for range in candidate_ranges {
                    let bytes = range_estimated_bytes(range, options);
                    if bytes_in_flight.saturating_add(bytes) > options.max_bytes_in_flight {
                        continue;
                    }
                    let occupied = range
                        .replicas
                        .iter()
                        .map(|replica| replica.node_id.clone())
                        .collect::<std::collections::BTreeSet<_>>();
                    let Some(target) = choose_target(
                        topology,
                        range,
                        Some(&source),
                        &occupied,
                        now_ms,
                        config,
                        options,
                        &projected_used,
                        &projected_replicas,
                        &per_node_moves,
                        bytes,
                    ) else {
                        continue;
                    };
                    if projected_replicas.get(&target).copied().unwrap_or(0) >= ceiling {
                        continue;
                    }
                    selected = Some((range, target, bytes));
                    break;
                }
                let Some((range, target, bytes)) = selected else {
                    break;
                };
                *projected_replicas.entry(source.clone()).or_default() = projected_replicas
                    .get(&source)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(1);
                *projected_replicas.entry(target.clone()).or_default() += 1;
                *projected_used.entry(source.clone()).or_default() = projected_used
                    .get(&source)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(bytes);
                *projected_used.entry(target.clone()).or_default() = projected_used
                    .get(&target)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(bytes);
                *per_node_moves.entry(source.clone()).or_default() += 1;
                *per_node_moves.entry(target.clone()).or_default() += 1;
                replica_moves.push(PlannedReplicaMove {
                    range_id: range.id,
                    expected_epoch: range.epoch,
                    source: Some(source),
                    target,
                    estimated_bytes: bytes,
                    reason: ReplicaMoveReason::LoadBalance,
                });
                bytes_in_flight = bytes_in_flight.saturating_add(bytes);
            }
        }
    }

    let mut projected_range_nodes = topology
        .ranges
        .values()
        .map(|range| {
            (
                range.id,
                range
                    .replicas
                    .iter()
                    .filter(|replica| replica.role == RangeReplicaRole::Voter)
                    .map(|replica| replica.node_id.clone())
                    .collect::<std::collections::BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for movement in &replica_moves {
        let nodes = projected_range_nodes.entry(movement.range_id).or_default();
        if let Some(source) = &movement.source {
            nodes.remove(source);
        }
        nodes.insert(movement.target.clone());
    }
    let mut projected_leaders = topology
        .nodes
        .keys()
        .map(|node| (node.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for range in topology.ranges.values() {
        *projected_leaders.entry(range.leader.clone()).or_default() += 1;
    }
    let active_node_count = topology
        .nodes
        .values()
        .filter(|node| {
            node.lifecycle == ClusterNodeLifecycle::Active
                && node.metadata_role == MetadataMemberRole::Voter
                && node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms)
                    == ClusterNodeLiveness::Live
        })
        .count()
        .max(1);
    let leader_ceiling = topology.ranges.len().div_ceil(active_node_count);
    let mut leader_transfers = Vec::new();
    for range in topology.ranges.values() {
        if leader_transfers.len() >= options.max_leader_transfers {
            break;
        }
        if active_relocation_ranges.contains(&range.id) {
            continue;
        }
        let source_node = topology.nodes.get(&range.leader);
        let forced = source_node.is_none_or(|node| {
            node.lifecycle != ClusterNodeLifecycle::Active
                || node.metadata_role != MetadataMemberRole::Voter
                || node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms)
                    != ClusterNodeLiveness::Live
        });
        let overloaded =
            projected_leaders.get(&range.leader).copied().unwrap_or(0) > leader_ceiling;
        let source_is_moving = replica_moves.iter().any(|movement| {
            movement.range_id == range.id && movement.source.as_ref() == Some(&range.leader)
        });
        if !forced && !overloaded && !source_is_moving {
            continue;
        }
        let existing_voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let Some(target) = projected_range_nodes
            .get(&range.id)
            .into_iter()
            .flatten()
            .filter(|node| *node != &range.leader)
            .filter(|node| {
                topology.nodes.get(*node).is_some_and(|candidate| {
                    candidate.lifecycle == ClusterNodeLifecycle::Active
                        && candidate.metadata_role == MetadataMemberRole::Voter
                        && candidate.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms)
                            == ClusterNodeLiveness::Live
                })
            })
            .min_by_key(|node| {
                (
                    !existing_voters.contains(*node),
                    projected_leaders.get(*node).copied().unwrap_or(0),
                    (*node).clone(),
                )
            })
            .cloned()
        else {
            continue;
        };
        *projected_leaders.entry(range.leader.clone()).or_default() = projected_leaders
            .get(&range.leader)
            .copied()
            .unwrap_or(0)
            .saturating_sub(1);
        *projected_leaders.entry(target.clone()).or_default() += 1;
        leader_transfers.push(PlannedLeaderTransfer {
            range_id: range.id,
            expected_epoch: range.epoch,
            source: range.leader.clone(),
            target,
        });
    }

    let mut plan = RebalancePlan {
        id: String::new(),
        topology_generation: topology.generation,
        created_at_ms: now_ms,
        replica_moves,
        leader_transfers,
        unplaced_ranges,
        estimated_bytes_in_flight: bytes_in_flight,
    };
    let digest = Sha256::digest(serde_json::to_vec(&plan)?);
    plan.id = hex::encode(&digest[..16]);
    Ok(plan)
}

pub fn build_failure_repair_plan(
    topology: &ClusterTopology,
    config: &DistributionConfig,
    options: &RebalanceOptions,
    now_ms: u64,
) -> Result<FailureRepairPlan> {
    topology.validate()?;
    config.validate()?;
    let node_liveness = topology
        .nodes
        .iter()
        .map(|(node_id, node)| {
            (
                node_id.clone(),
                node.liveness(now_ms, config.suspect_after_ms, config.dead_after_ms),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let required_quorum = topology.replication_factor / 2 + 1;
    let mut range_availability = Vec::with_capacity(topology.ranges.len());
    let mut unavailable_ranges = Vec::new();
    let mut under_replicated_ranges = Vec::new();
    for range in topology.ranges.values() {
        let mut live_voters = 0_u8;
        let mut suspect_voters = 0_u8;
        let mut dead_voters = 0_u8;
        for replica in range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
        {
            match node_liveness
                .get(&replica.node_id)
                .copied()
                .unwrap_or(ClusterNodeLiveness::Dead)
            {
                ClusterNodeLiveness::Live => live_voters = live_voters.saturating_add(1),
                ClusterNodeLiveness::Suspect => suspect_voters = suspect_voters.saturating_add(1),
                ClusterNodeLiveness::Dead | ClusterNodeLiveness::Decommissioned => {
                    dead_voters = dead_voters.saturating_add(1)
                }
            }
        }
        let live_leader = node_liveness
            .get(&range.leader)
            .is_some_and(|liveness| *liveness == ClusterNodeLiveness::Live);
        let availability = RangeAvailability {
            range_id: range.id,
            live_voters,
            suspect_voters,
            dead_voters,
            required_quorum,
            live_leader,
        };
        if !availability.available() {
            unavailable_ranges.push(range.id);
        }
        if usize::from(live_voters) < usize::from(topology.replication_factor) {
            under_replicated_ranges.push(range.id);
        }
        range_availability.push(availability);
    }
    Ok(FailureRepairPlan {
        created_at_ms: now_ms,
        node_liveness,
        range_availability,
        unavailable_ranges,
        under_replicated_ranges,
        rebalance: build_rebalance_plan(topology, config, options, now_ms)?,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: ClusterNodeId,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_certificate_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_tls_certificate_sha256: Option<String>,
    pub incarnation: u64,
    pub lifecycle: ClusterNodeLifecycle,
    #[serde(default, skip_serializing_if = "MetadataMemberRole::is_voter")]
    pub metadata_role: MetadataMemberRole,
    pub labels: BTreeMap<String, String>,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub joined_at_ms: u64,
    pub last_heartbeat_ms: u64,
}

impl ClusterNode {
    pub fn new(
        id: ClusterNodeId,
        address: impl Into<String>,
        incarnation: u64,
        capacity_bytes: u64,
        now_ms: u64,
    ) -> Result<Self> {
        let address = address.into();
        if address.trim().is_empty() {
            return Err(cluster_error("cluster node address must not be empty"));
        }
        if incarnation == 0 {
            return Err(cluster_error(
                "cluster node incarnation must be greater than zero",
            ));
        }
        if capacity_bytes == 0 {
            return Err(cluster_error(
                "cluster node capacity_bytes must be greater than zero",
            ));
        }
        Ok(Self {
            id,
            address,
            tls_certificate_sha256: None,
            pending_tls_certificate_sha256: None,
            incarnation,
            lifecycle: ClusterNodeLifecycle::Active,
            metadata_role: MetadataMemberRole::Voter,
            labels: BTreeMap::new(),
            capacity_bytes,
            used_bytes: 0,
            joined_at_ms: now_ms,
            last_heartbeat_ms: now_ms,
        })
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Result<Self> {
        let key = key.into();
        let value = value.into();
        validate_label(&key, &value)?;
        self.labels.insert(key, value);
        Ok(self)
    }

    pub fn with_tls_certificate_sha256(mut self, sha256: impl Into<String>) -> Result<Self> {
        let sha256 = sha256.into();
        validate_certificate_sha256(&sha256)?;
        self.tls_certificate_sha256 = Some(sha256);
        Ok(self)
    }

    pub fn accepts_tls_certificate_sha256(&self, sha256: &str) -> bool {
        self.tls_certificate_sha256.as_deref() == Some(sha256)
            || self.pending_tls_certificate_sha256.as_deref() == Some(sha256)
    }

    pub fn as_metadata_learner(mut self) -> Self {
        self.metadata_role = MetadataMemberRole::Learner;
        self
    }

    pub fn available_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }

    pub fn utilization_per_million(&self) -> u64 {
        if self.capacity_bytes == 0 {
            return 1_000_000;
        }
        self.used_bytes
            .saturating_mul(1_000_000)
            .checked_div(self.capacity_bytes)
            .unwrap_or(1_000_000)
            .min(1_000_000)
    }

    pub fn liveness(
        &self,
        now_ms: u64,
        suspect_after_ms: u64,
        dead_after_ms: u64,
    ) -> ClusterNodeLiveness {
        if self.lifecycle == ClusterNodeLifecycle::Decommissioned {
            return ClusterNodeLiveness::Decommissioned;
        }
        let elapsed = now_ms.saturating_sub(self.last_heartbeat_ms);
        if elapsed >= dead_after_ms {
            ClusterNodeLiveness::Dead
        } else if elapsed >= suspect_after_ms {
            ClusterNodeLiveness::Suspect
        } else {
            ClusterNodeLiveness::Live
        }
    }
}

fn validate_label(key: &str, value: &str) -> Result<()> {
    validate_identifier("node label key", key)?;
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b':')
        })
    {
        return Err(cluster_error(format!(
            "invalid node label value `{value}` for `{key}`"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyChange {
    pub generation: u64,
    pub at_ms: u64,
    pub actor: ClusterNodeId,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterTopology {
    pub format_version: u32,
    pub hash_version: u32,
    pub cluster_id: ClusterId,
    pub generation: u64,
    pub replication_factor: u8,
    pub nodes: BTreeMap<ClusterNodeId, ClusterNode>,
    /// Ranges keyed by inclusive start token. The final range has no end token.
    pub ranges: BTreeMap<u64, RangeDescriptor>,
    pub next_range_id: u64,
    pub next_replica_id: u64,
    #[serde(default = "default_next_relocation_id")]
    pub next_relocation_id: u64,
    #[serde(default)]
    pub relocations: BTreeMap<RelocationId, RangeRelocation>,
    #[serde(default)]
    pub applied_rebalance_plans: VecDeque<String>,
    pub history: VecDeque<TopologyChange>,
}

fn validate_schema_transition(
    topology: &ClusterTopology,
    voters: &BTreeSet<ClusterNodeId>,
    expected_base_sha256: &str,
    target_sha256: &str,
    actor: &ClusterNodeId,
) -> Result<()> {
    let valid_digest = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if !valid_digest(expected_base_sha256)
        || !valid_digest(target_sha256)
        || expected_base_sha256 == target_sha256
        || voters.is_empty()
        || voters != &topology.metadata_voters()
        || !voters.contains(actor)
        || topology
            .relocations
            .values()
            .any(RangeRelocation::is_active)
    {
        return Err(cluster_error(
            "schema transition has invalid digests, voters, actor, or active relocation",
        ));
    }
    for voter in voters {
        let node = topology
            .nodes
            .get(voter)
            .ok_or_else(|| cluster_error(format!("schema transition voter {voter} is absent")))?;
        if node.lifecycle != ClusterNodeLifecycle::Active
            || node.metadata_role != MetadataMemberRole::Voter
            || node
                .labels
                .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                .map(String::as_str)
                != Some(expected_base_sha256)
        {
            return Err(cluster_error(format!(
                "schema transition voter {voter} is inactive or does not advertise the exact base"
            )));
        }
    }
    Ok(())
}

impl ClusterTopology {
    fn new(config: &DistributionConfig, local: ClusterNode, now_ms: u64) -> Result<Self> {
        if local.id != config.node_id {
            return Err(cluster_error(format!(
                "local node id mismatch: config={}, node={}",
                config.node_id, local.id
            )));
        }
        let mut nodes = BTreeMap::new();
        nodes.insert(local.id.clone(), local);
        let (ranges, next_range_id, next_replica_id) = bootstrap_ranges(
            config.initial_ranges,
            &config.node_id,
            &config.default_placement,
        )?;
        let history = VecDeque::from([TopologyChange {
            generation: 1,
            at_ms: now_ms,
            actor: config.node_id.clone(),
            summary: "cluster initialized".to_string(),
        }]);
        let topology = Self {
            format_version: DISTRIBUTION_FORMAT_VERSION,
            hash_version: DISTRIBUTION_HASH_VERSION,
            cluster_id: config.cluster_id.clone(),
            generation: 1,
            replication_factor: config.replication_factor,
            nodes,
            ranges,
            next_range_id,
            next_replica_id,
            next_relocation_id: 1,
            relocations: BTreeMap::new(),
            applied_rebalance_plans: VecDeque::new(),
            history,
        };
        topology.validate()?;
        Ok(topology)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != DISTRIBUTION_FORMAT_VERSION {
            return Err(cluster_error(format!(
                "unsupported distribution format version {}; expected {}",
                self.format_version, DISTRIBUTION_FORMAT_VERSION
            )));
        }
        if self.hash_version != DISTRIBUTION_HASH_VERSION {
            return Err(cluster_error(format!(
                "unsupported distribution hash version {}; expected {}",
                self.hash_version, DISTRIBUTION_HASH_VERSION
            )));
        }
        if self.generation == 0 {
            return Err(cluster_error(
                "cluster topology generation must be greater than zero",
            ));
        }
        if self.replication_factor == 0 {
            return Err(cluster_error(
                "cluster replication factor must be greater than zero",
            ));
        }
        if self.nodes.is_empty() {
            return Err(cluster_error(
                "cluster topology must contain at least one node",
            ));
        }
        if !self.nodes.values().any(|node| {
            node.lifecycle != ClusterNodeLifecycle::Decommissioned
                && node.metadata_role == MetadataMemberRole::Voter
        }) {
            return Err(cluster_error(
                "cluster topology must contain at least one metadata voter",
            ));
        }
        if self.next_range_id == 0 || self.next_replica_id == 0 || self.next_relocation_id == 0 {
            return Err(cluster_error(
                "cluster topology next range/replica/relocation ids must be greater than zero",
            ));
        }
        for (id, node) in &self.nodes {
            if id != &node.id {
                return Err(cluster_error(format!(
                    "cluster node map key {id} does not match node id {}",
                    node.id
                )));
            }
            if node.address.trim().is_empty() {
                return Err(cluster_error(format!(
                    "cluster node {id} has an empty address"
                )));
            }
            if let Some(fingerprint) = &node.tls_certificate_sha256 {
                validate_certificate_sha256(fingerprint)?;
            }
            if let Some(fingerprint) = &node.pending_tls_certificate_sha256 {
                validate_certificate_sha256(fingerprint)?;
                if node.tls_certificate_sha256.is_none() {
                    return Err(cluster_error(format!(
                        "cluster node {id} cannot rotate an unbound TLS certificate"
                    )));
                }
                if node.tls_certificate_sha256.as_ref() == Some(fingerprint) {
                    return Err(cluster_error(format!(
                        "cluster node {id} pending TLS certificate matches its active certificate"
                    )));
                }
            }
            if node.incarnation == 0 {
                return Err(cluster_error(format!(
                    "cluster node {id} has an invalid zero incarnation"
                )));
            }
            if node.capacity_bytes == 0 || node.used_bytes > node.capacity_bytes {
                return Err(cluster_error(format!(
                    "cluster node {id} has invalid capacity: used={} capacity={}",
                    node.used_bytes, node.capacity_bytes
                )));
            }
            for (key, value) in &node.labels {
                validate_label(key, value)?;
                if (key == SCHEMA_COMPATIBILITY_NODE_LABEL
                    || key == SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                    && (value.len() != 64
                        || !value
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
                {
                    return Err(cluster_error(format!(
                        "cluster node {id} has an invalid schema compatibility fingerprint"
                    )));
                }
                if key == SCHEMA_BOOTSTRAP_NODE_LABEL && value != "true" {
                    return Err(cluster_error(format!(
                        "cluster node {id} has an invalid schema bootstrap claim"
                    )));
                }
            }
        }
        validate_range_map(&self.ranges, &self.nodes)?;
        let max_range_id = self
            .ranges
            .values()
            .map(|range| range.id.0)
            .max()
            .unwrap_or(0);
        let max_replica_id = self
            .ranges
            .values()
            .flat_map(|range| range.replicas.iter().map(|replica| replica.id.0))
            .max()
            .unwrap_or(0);
        if self.next_range_id <= max_range_id || self.next_replica_id <= max_replica_id {
            return Err(cluster_error(
                "cluster topology next range/replica ids would reuse an existing id",
            ));
        }
        let max_relocation_id = self.relocations.keys().map(|id| id.0).max().unwrap_or(0);
        if self.next_relocation_id <= max_relocation_id {
            return Err(cluster_error(
                "cluster topology next relocation id would reuse an existing id",
            ));
        }
        if self.relocations.len() > DEFAULT_RELOCATION_HISTORY_LIMIT {
            return Err(cluster_error("relocation history is unbounded"));
        }
        let mut active_ranges = std::collections::BTreeSet::new();
        for (id, relocation) in &self.relocations {
            if id != &relocation.id {
                return Err(cluster_error(format!(
                    "relocation map key {id} does not match {}",
                    relocation.id
                )));
            }
            relocation.validate(self)?;
            if relocation.is_active() && !active_ranges.insert(relocation.range_id) {
                return Err(cluster_error(format!(
                    "range {} has more than one active relocation",
                    relocation.range_id
                )));
            }
        }
        if self.applied_rebalance_plans.len() > DEFAULT_APPLIED_PLAN_HISTORY_LIMIT {
            return Err(cluster_error("applied rebalance plan history is unbounded"));
        }
        let mut previous_generation = 0;
        for change in &self.history {
            if change.generation == 0
                || change.generation > self.generation
                || change.generation < previous_generation
            {
                return Err(cluster_error(
                    "cluster topology history is not generation ordered",
                ));
            }
            previous_generation = change.generation;
        }
        Ok(())
    }

    pub fn node(&self, node_id: &ClusterNodeId) -> Option<&ClusterNode> {
        self.nodes.get(node_id)
    }

    pub fn metadata_voters(&self) -> BTreeSet<ClusterNodeId> {
        self.nodes
            .values()
            .filter(|node| {
                node.lifecycle != ClusterNodeLifecycle::Decommissioned
                    && node.metadata_role == MetadataMemberRole::Voter
            })
            .map(|node| node.id.clone())
            .collect()
    }

    pub fn metadata_learners(&self) -> BTreeSet<ClusterNodeId> {
        self.nodes
            .values()
            .filter(|node| {
                node.lifecycle != ClusterNodeLifecycle::Decommissioned
                    && node.metadata_role == MetadataMemberRole::Learner
            })
            .map(|node| node.id.clone())
            .collect()
    }

    pub fn is_metadata_voter(&self, node_id: &ClusterNodeId) -> bool {
        self.nodes.get(node_id).is_some_and(|node| {
            node.lifecycle != ClusterNodeLifecycle::Decommissioned
                && node.metadata_role == MetadataMemberRole::Voter
        })
    }

    /// Deterministic control-plane owner until metadata consensus elects a
    /// term leader. Every member independently chooses the oldest active
    /// member (then node id), so later joins cannot steal control.
    pub fn controller_node_id(&self) -> Option<&ClusterNodeId> {
        self.nodes
            .values()
            .filter(|node| {
                node.lifecycle == ClusterNodeLifecycle::Active
                    && node.metadata_role == MetadataMemberRole::Voter
            })
            .min_by_key(|node| (node.joined_at_ms, &node.id))
            .map(|node| &node.id)
    }

    pub fn liveness(
        &self,
        node_id: &ClusterNodeId,
        now_ms: u64,
        suspect_after_ms: u64,
        dead_after_ms: u64,
    ) -> Option<ClusterNodeLiveness> {
        self.nodes
            .get(node_id)
            .map(|node| node.liveness(now_ms, suspect_after_ms, dead_after_ms))
    }

    pub fn range_for_token(&self, token: u64) -> Result<&RangeDescriptor> {
        let range = self
            .ranges
            .range(..=token)
            .next_back()
            .map(|(_, range)| range)
            .ok_or_else(|| cluster_error(format!("no range owns token {token}")))?;
        if !range.contains_token(token) {
            return Err(cluster_error(format!("no range owns token {token}")));
        }
        Ok(range)
    }

    pub fn range_for_key(&self, namespace: &str, key: &str) -> Result<&RangeDescriptor> {
        self.range_for_token(distribution_key_token(namespace, key))
    }

    pub fn range_by_id(&self, range_id: RangeId) -> Option<&RangeDescriptor> {
        self.ranges.values().find(|range| range.id == range_id)
    }

    pub fn active_relocation_for_range(&self, range_id: RangeId) -> Option<&RangeRelocation> {
        self.relocations
            .values()
            .find(|relocation| relocation.range_id == range_id && relocation.is_active())
    }

    /// Build one metadata-consensus topology generation that changes the
    /// expected executable-schema digest for every current voter at once.
    /// Existing nodes immediately fail the strict live-vs-published schema
    /// fence until their signed additive activation reaches `target_sha256`.
    pub fn fence_schema_activation(
        &mut self,
        voters: &BTreeSet<ClusterNodeId>,
        expected_base_sha256: &str,
        target_sha256: &str,
        actor: ClusterNodeId,
        at_ms: u64,
    ) -> Result<()> {
        let valid_digest = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        if !valid_digest(expected_base_sha256)
            || !valid_digest(target_sha256)
            || expected_base_sha256 == target_sha256
            || voters.is_empty()
            || voters != &self.metadata_voters()
            || !voters.contains(&actor)
            || self.relocations.values().any(RangeRelocation::is_active)
        {
            return Err(cluster_error(
                "schema activation fence has invalid digests, voters, actor, or active relocation",
            ));
        }
        for voter in voters {
            let node = self.nodes.get(voter).ok_or_else(|| {
                cluster_error(format!("schema activation voter {voter} is absent"))
            })?;
            if node.lifecycle != ClusterNodeLifecycle::Active
                || node.metadata_role != MetadataMemberRole::Voter
                || node
                    .labels
                    .get(SCHEMA_COMPATIBILITY_NODE_LABEL)
                    .map(String::as_str)
                    != Some(expected_base_sha256)
            {
                return Err(cluster_error(format!(
                    "schema activation voter {voter} is inactive or does not advertise the exact base"
                )));
            }
        }
        for voter in voters {
            self.nodes
                .get_mut(voter)
                .expect("validated schema activation voter")
                .labels
                .insert(
                    SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                    target_sha256.to_string(),
                );
        }
        self.record_change(
            actor,
            at_ms,
            format!(
                "fenced schema activation {} -> {} for {} voters",
                expected_base_sha256,
                target_sha256,
                voters.len()
            ),
            DEFAULT_TOPOLOGY_HISTORY_LIMIT,
        );
        self.validate()
    }

    /// Open one quorum-committed additive compatibility window without
    /// changing the active schema fingerprint. Existing traffic continues to
    /// use `expected_base_sha256`; only host-verified prefixes of the signed
    /// `target_sha256` gain local admission authority.
    pub fn open_schema_compatibility_window(
        &mut self,
        voters: &BTreeSet<ClusterNodeId>,
        expected_base_sha256: &str,
        target_sha256: &str,
        actor: ClusterNodeId,
        at_ms: u64,
    ) -> Result<()> {
        validate_schema_transition(self, voters, expected_base_sha256, target_sha256, &actor)?;
        for voter in voters {
            let node = self.nodes.get(voter).expect("validated schema voter");
            if node
                .labels
                .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
            {
                return Err(cluster_error(format!(
                    "schema compatibility voter {voter} already has a pending target"
                )));
            }
        }
        for voter in voters {
            self.nodes
                .get_mut(voter)
                .expect("validated schema voter")
                .labels
                .insert(
                    SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL.to_string(),
                    target_sha256.to_string(),
                );
        }
        self.record_change(
            actor,
            at_ms,
            format!(
                "opened additive schema compatibility window {} -> {} for {} voters",
                expected_base_sha256,
                target_sha256,
                voters.len()
            ),
            DEFAULT_TOPOLOGY_HISTORY_LIMIT,
        );
        self.validate()
    }

    /// Promote a completed additive compatibility window in one topology
    /// generation. Every voter must still advertise the common base and exact
    /// pending target; promotion switches the active digest and removes the
    /// pending label atomically in metadata consensus.
    pub fn promote_schema_compatibility_window(
        &mut self,
        voters: &BTreeSet<ClusterNodeId>,
        expected_base_sha256: &str,
        target_sha256: &str,
        actor: ClusterNodeId,
        at_ms: u64,
    ) -> Result<()> {
        validate_schema_transition(self, voters, expected_base_sha256, target_sha256, &actor)?;
        for voter in voters {
            let node = self.nodes.get(voter).expect("validated schema voter");
            if node
                .labels
                .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                .map(String::as_str)
                != Some(target_sha256)
            {
                return Err(cluster_error(format!(
                    "schema compatibility voter {voter} does not advertise the exact pending target"
                )));
            }
        }
        for voter in voters {
            let labels = &mut self
                .nodes
                .get_mut(voter)
                .expect("validated schema voter")
                .labels;
            labels.insert(
                SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                target_sha256.to_string(),
            );
            labels.remove(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL);
        }
        self.record_change(
            actor,
            at_ms,
            format!(
                "promoted additive schema compatibility window {} -> {} for {} voters",
                expected_base_sha256,
                target_sha256,
                voters.len()
            ),
            DEFAULT_TOPOLOGY_HISTORY_LIMIT,
        );
        self.validate()
    }

    fn record_change(
        &mut self,
        actor: ClusterNodeId,
        at_ms: u64,
        summary: impl Into<String>,
        history_limit: usize,
    ) {
        self.generation = self.generation.saturating_add(1);
        self.history.push_back(TopologyChange {
            generation: self.generation,
            at_ms,
            actor,
            summary: summary.into(),
        });
        while self.history.len() > history_limit {
            self.history.pop_front();
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TopologyEnvelope {
    topology: ClusterTopology,
    checksum_sha256: String,
}

impl TopologyEnvelope {
    fn from_topology(topology: ClusterTopology) -> Result<Self> {
        topology.validate()?;
        let checksum_sha256 = topology_checksum(&topology)?;
        Ok(Self {
            topology,
            checksum_sha256,
        })
    }

    fn verify(self) -> Result<ClusterTopology> {
        let actual = topology_checksum(&self.topology)?;
        if actual != self.checksum_sha256 {
            return Err(cluster_error(format!(
                "cluster topology checksum mismatch: expected {}, computed {actual}",
                self.checksum_sha256
            )));
        }
        self.topology.validate()?;
        Ok(self.topology)
    }
}

fn topology_checksum(topology: &ClusterTopology) -> Result<String> {
    let bytes = serde_json::to_vec(topology)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributionConfig {
    pub enabled: bool,
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub node_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_tls_certificate_sha256: Option<String>,
    pub node_incarnation: u64,
    pub node_capacity_bytes: u64,
    pub replication_factor: u8,
    pub initial_ranges: u32,
    pub default_placement: PlacementPolicy,
    pub suspect_after_ms: u64,
    pub dead_after_ms: u64,
    pub topology_history_limit: usize,
    #[serde(default = "default_metadata_election_timeout_ms")]
    pub metadata_election_timeout_ms: u64,
    #[serde(default = "default_metadata_heartbeat_interval_ms")]
    pub metadata_heartbeat_interval_ms: u64,
    #[serde(default)]
    pub transport: ClusterNetworkTransportConfig,
}

impl Default for DistributionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_id: ClusterId("default".to_string()),
            node_id: ClusterNodeId("local".to_string()),
            node_address: "127.0.0.1:9444".to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 1,
            replication_factor: 3,
            initial_ranges: 256,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 5_000,
            dead_after_ms: 15_000,
            topology_history_limit: DEFAULT_TOPOLOGY_HISTORY_LIMIT,
            metadata_election_timeout_ms: default_metadata_election_timeout_ms(),
            metadata_heartbeat_interval_ms: default_metadata_heartbeat_interval_ms(),
            transport: ClusterNetworkTransportConfig::default(),
        }
    }
}

impl DistributionConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("cluster id", self.cluster_id.as_str())?;
        validate_identifier("cluster node id", self.node_id.as_str())?;
        if self.node_address.trim().is_empty() {
            return Err(cluster_error("distribution node_address must not be empty"));
        }
        if let Some(fingerprint) = &self.node_tls_certificate_sha256 {
            validate_certificate_sha256(fingerprint)?;
        }
        self.transport.validate()?;
        if self.node_incarnation == 0 {
            return Err(cluster_error(
                "distribution node_incarnation must be greater than zero",
            ));
        }
        if self.node_capacity_bytes == 0 {
            return Err(cluster_error(
                "distribution node_capacity_bytes must be greater than zero",
            ));
        }
        if self.replication_factor == 0 {
            return Err(cluster_error(
                "distribution replication_factor must be greater than zero",
            ));
        }
        if self.initial_ranges == 0
            || self.initial_ranges > 65_536
            || !self.initial_ranges.is_power_of_two()
        {
            return Err(cluster_error(
                "distribution initial_ranges must be a power of two between 1 and 65536",
            ));
        }
        self.default_placement.validate()?;
        if self.suspect_after_ms == 0 || self.dead_after_ms <= self.suspect_after_ms {
            return Err(cluster_error(
                "distribution dead_after_ms must be greater than non-zero suspect_after_ms",
            ));
        }
        if self.topology_history_limit == 0 || self.topology_history_limit > 4096 {
            return Err(cluster_error(
                "distribution topology_history_limit must be between 1 and 4096",
            ));
        }
        if !(250..=120_000).contains(&self.metadata_election_timeout_ms) {
            return Err(cluster_error(
                "metadata election timeout must be between 250ms and 120s",
            ));
        }
        if self.metadata_heartbeat_interval_ms == 0
            || self.metadata_heartbeat_interval_ms >= self.metadata_election_timeout_ms
        {
            return Err(cluster_error(
                "metadata heartbeat interval must be non-zero and less than the election timeout",
            ));
        }
        Ok(())
    }
}

/// Quorum-derived cluster metadata sufficient for an empty server to
/// provision its own member directory without shared filesystem access.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBootstrapSnapshot {
    pub topology: ClusterTopology,
    pub config_template: DistributionConfig,
    pub metadata_leader_id: Option<ClusterNodeId>,
}

impl ClusterBootstrapSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.topology.validate()?;
        self.config_template.validate()?;
        if !self.config_template.enabled
            || self.config_template.cluster_id != self.topology.cluster_id
            || self.config_template.replication_factor != self.topology.replication_factor
        {
            return Err(cluster_error(
                "cluster bootstrap configuration does not match its topology",
            ));
        }
        if !self
            .topology
            .nodes
            .contains_key(&self.config_template.node_id)
        {
            return Err(cluster_error(
                "cluster bootstrap template node is absent from its topology",
            ));
        }
        if self.metadata_leader_id.as_ref().is_some_and(|leader| {
            !self.topology.is_metadata_voter(leader)
                || self
                    .topology
                    .nodes
                    .get(leader)
                    .is_none_or(|node| node.lifecycle != ClusterNodeLifecycle::Active)
        }) {
            return Err(cluster_error(
                "cluster bootstrap metadata leader is not an active voter",
            ));
        }
        Ok(())
    }
}

/// Provision an empty member locally from a snapshot fetched over the
/// authenticated cluster transport. Existing conflicting metadata is never
/// overwritten.
pub fn provision_cluster_member_directory(
    root: impl AsRef<Path>,
    snapshot: &ClusterBootstrapSnapshot,
    node_id: &ClusterNodeId,
    transport: ClusterNetworkTransportConfig,
    fsync: bool,
) -> Result<DistributionConfig> {
    snapshot.validate()?;
    transport.validate()?;
    let node = snapshot
        .topology
        .nodes
        .get(node_id)
        .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
    if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
        return Err(cluster_error(format!(
            "cannot provision decommissioned cluster node {node_id}"
        )));
    }
    let mut config = snapshot.config_template.clone();
    config.node_id = node.id.clone();
    config.node_address = node.address.clone();
    config.node_tls_certificate_sha256 = node.tls_certificate_sha256.clone();
    config.node_incarnation = node.incarnation;
    config.node_capacity_bytes = node.capacity_bytes;
    config.replication_factor = snapshot.topology.replication_factor;
    config.transport = transport;
    config.validate()?;

    let root = root.as_ref();
    let topology_path = root.join(DEFAULT_CLUSTER_TOPOLOGY);
    if topology_path.exists() {
        let existing = load_topology(&topology_path)?;
        if existing != snapshot.topology {
            return Err(cluster_error(format!(
                "refusing to overwrite conflicting cluster topology in {}",
                root.display()
            )));
        }
    }
    let config_path = root.join(DEFAULT_DISTRIBUTION_CONFIG);
    if config_path.exists() {
        let existing = load_distribution_config(root)?;
        if existing != config {
            return Err(cluster_error(format!(
                "refusing to overwrite conflicting distribution configuration in {}",
                root.display()
            )));
        }
    }
    save_distribution_config(root, &config, fsync)?;
    persist_topology(&topology_path, &snapshot.topology, fsync)?;
    Ok(config)
}

pub fn save_distribution_config(
    root: impl AsRef<Path>,
    config: &DistributionConfig,
    fsync: bool,
) -> Result<()> {
    config.validate()?;
    if !config.enabled {
        return Err(cluster_error(
            "cannot save a disabled distribution configuration",
        ));
    }
    let path = root.as_ref().join(DEFAULT_DISTRIBUTION_CONFIG);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(config)?;
    crate::storage::write_atomic(&path, &bytes, fsync)
}

pub fn load_distribution_config(root: impl AsRef<Path>) -> Result<DistributionConfig> {
    let path = root.as_ref().join(DEFAULT_DISTRIBUTION_CONFIG);
    let bytes = fs::read(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            cluster_error(format!(
                "distribution configuration {} does not exist; run `bicdb cluster init` first",
                path.display()
            ))
        } else {
            error.into()
        }
    })?;
    let config = serde_json::from_slice::<DistributionConfig>(&bytes)?;
    config.validate()?;
    if !config.enabled {
        return Err(cluster_error(
            "persisted distribution configuration is disabled",
        ));
    }
    Ok(config)
}

#[derive(Clone, Debug)]
pub struct DistributionStore {
    root: PathBuf,
    fsync: bool,
    ephemeral: bool,
    pending_heartbeats: BTreeMap<ClusterNodeId, PendingClusterHeartbeat>,
    pending_metadata_learners: BTreeMap<ClusterNodeId, PendingMetadataLearner>,
    pending_metadata_promotions: BTreeMap<ClusterNodeId, PendingMetadataPromotion>,
    pending_certificate_rotations: BTreeMap<ClusterNodeId, PendingCertificateRotation>,
    config: DistributionConfig,
    topology: ClusterTopology,
}

#[derive(Clone, Debug)]
struct PendingClusterHeartbeat {
    incarnation: u64,
    used_bytes: u64,
    capacity_bytes: u64,
    labels: BTreeMap<String, String>,
    tls_certificate_sha256: Option<String>,
    now_ms: u64,
}

#[derive(Clone, Debug)]
struct PendingMetadataLearner {
    node: ClusterNode,
    actor: ClusterNodeId,
    now_ms: u64,
}

#[derive(Clone, Debug)]
struct PendingMetadataPromotion {
    actor: ClusterNodeId,
    now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingCertificateRotationKind {
    Stage(String),
    Abort,
}

#[derive(Clone, Debug)]
struct PendingCertificateRotation {
    kind: PendingCertificateRotationKind,
    actor: ClusterNodeId,
    now_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataMutationToken {
    heartbeat_versions: BTreeMap<ClusterNodeId, u64>,
    learner_versions: BTreeMap<ClusterNodeId, u64>,
    promotion_versions: BTreeMap<ClusterNodeId, u64>,
    certificate_rotation_versions: BTreeMap<ClusterNodeId, u64>,
}

impl MetadataMutationToken {
    pub fn is_empty(&self) -> bool {
        self.heartbeat_versions.is_empty()
            && self.learner_versions.is_empty()
            && self.promotion_versions.is_empty()
            && self.certificate_rotation_versions.is_empty()
    }
}

impl DistributionStore {
    pub fn initialize(
        root: impl AsRef<Path>,
        config: DistributionConfig,
        fsync: bool,
    ) -> Result<Self> {
        Self::initialize_at(root, config, fsync, unix_time_ms())
    }

    pub fn initialize_at(
        root: impl AsRef<Path>,
        config: DistributionConfig,
        fsync: bool,
        now_ms: u64,
    ) -> Result<Self> {
        config.validate()?;
        if !config.enabled {
            return Err(cluster_error(
                "distribution must be enabled before initializing a cluster",
            ));
        }
        let root = root.as_ref().to_path_buf();
        let topology_path = root.join(DEFAULT_CLUSTER_TOPOLOGY);
        if topology_path.exists() {
            return Self::open(root, config, fsync);
        }
        let mut local = ClusterNode::new(
            config.node_id.clone(),
            config.node_address.clone(),
            config.node_incarnation,
            config.node_capacity_bytes,
            now_ms,
        )?;
        if let Some(fingerprint) = &config.node_tls_certificate_sha256 {
            local = local.with_tls_certificate_sha256(fingerprint.clone())?;
        }
        let topology = ClusterTopology::new(&config, local, now_ms)?;
        save_distribution_config(&root, &config, fsync)?;
        persist_topology(&topology_path, &topology, fsync)?;
        Ok(Self {
            root,
            fsync,
            ephemeral: false,
            pending_heartbeats: BTreeMap::new(),
            pending_metadata_learners: BTreeMap::new(),
            pending_metadata_promotions: BTreeMap::new(),
            pending_certificate_rotations: BTreeMap::new(),
            config,
            topology,
        })
    }

    pub fn open(root: impl AsRef<Path>, config: DistributionConfig, fsync: bool) -> Result<Self> {
        config.validate()?;
        if !config.enabled {
            return Err(cluster_error(
                "distribution must be enabled before opening cluster topology",
            ));
        }
        let root = root.as_ref().to_path_buf();
        let topology = load_topology(&root.join(DEFAULT_CLUSTER_TOPOLOGY))?;
        ensure_config_matches(&config, &topology)?;
        Ok(Self {
            root,
            fsync,
            ephemeral: false,
            pending_heartbeats: BTreeMap::new(),
            pending_metadata_learners: BTreeMap::new(),
            pending_metadata_promotions: BTreeMap::new(),
            pending_certificate_rotations: BTreeMap::new(),
            config,
            topology,
        })
    }

    pub fn topology(&self) -> &ClusterTopology {
        &self.topology
    }

    /// Fork an in-memory planning copy. Mutating methods preserve every
    /// validation and generation transition but do not publish topology files.
    /// The resulting topology can be proposed to metadata consensus and only
    /// installed in the durable store after quorum.
    pub fn fork_ephemeral(&self) -> Self {
        let mut staged = self.clone();
        staged.ephemeral = true;
        staged.pending_heartbeats.clear();
        staged.pending_metadata_learners.clear();
        staged.pending_metadata_promotions.clear();
        staged.pending_certificate_rotations.clear();
        staged
    }

    /// Validate and coalesce a member heartbeat for the elected metadata
    /// leader. The supervisor includes the latest heartbeat for every member
    /// in its next bounded topology proposal, avoiding one consensus entry per
    /// inbound RPC.
    pub fn queue_heartbeat(
        &mut self,
        node_id: &ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        labels: BTreeMap<String, String>,
        now_ms: u64,
    ) -> Result<bool> {
        self.queue_heartbeat_with_certificate(
            node_id,
            incarnation,
            used_bytes,
            capacity_bytes,
            labels,
            None,
            now_ms,
        )
    }

    pub fn queue_heartbeat_with_certificate(
        &mut self,
        node_id: &ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        labels: BTreeMap<String, String>,
        tls_certificate_sha256: Option<String>,
        now_ms: u64,
    ) -> Result<bool> {
        let mut validation = self.fork_ephemeral();
        if !validation.heartbeat_with_certificate(
            node_id,
            incarnation,
            used_bytes,
            capacity_bytes,
            labels.clone(),
            tls_certificate_sha256.clone(),
            now_ms,
        )? {
            return Ok(false);
        }
        let activates_rotation = self
            .topology
            .nodes
            .get(node_id)
            .and_then(|node| node.pending_tls_certificate_sha256.as_ref())
            .is_some_and(|fingerprint| tls_certificate_sha256.as_ref() == Some(fingerprint));
        if let Some(pending) = self.pending_heartbeats.get(node_id) {
            let pending_activates_rotation = self
                .topology
                .nodes
                .get(node_id)
                .and_then(|node| node.pending_tls_certificate_sha256.as_ref())
                .is_some_and(|fingerprint| {
                    pending.tls_certificate_sha256.as_ref() == Some(fingerprint)
                });
            if pending_activates_rotation && !activates_rotation {
                return Ok(false);
            }
            if (pending_activates_rotation || !activates_rotation) && pending.now_ms >= now_ms {
                return Ok(false);
            }
        }
        self.pending_heartbeats.insert(
            node_id.clone(),
            PendingClusterHeartbeat {
                incarnation,
                used_bytes,
                capacity_bytes,
                labels,
                tls_certificate_sha256,
                now_ms,
            },
        );
        Ok(true)
    }

    pub fn queue_metadata_learner(
        &mut self,
        node: ClusterNode,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        let mut validation = self.fork_ephemeral();
        if !validation.join_metadata_learner(node.clone(), actor, now_ms)? {
            return Ok(false);
        }
        if let Some(pending) = self.pending_metadata_learners.get(&node.id) {
            if pending.node == node {
                return Ok(false);
            }
            return Err(cluster_error(format!(
                "conflicting metadata learner registration is already pending for {}",
                node.id
            )));
        }
        self.pending_metadata_learners.insert(
            node.id.clone(),
            PendingMetadataLearner {
                node,
                actor: actor.clone(),
                now_ms,
            },
        );
        Ok(true)
    }

    pub fn queue_metadata_promotion(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        let mut validation = self.fork_ephemeral();
        if !validation.promote_metadata_learner(node_id, actor, now_ms)? {
            return Ok(false);
        }
        if self.pending_metadata_promotions.contains_key(node_id) {
            return Ok(false);
        }
        self.pending_metadata_promotions.insert(
            node_id.clone(),
            PendingMetadataPromotion {
                actor: actor.clone(),
                now_ms,
            },
        );
        Ok(true)
    }

    pub fn queue_tls_certificate_rotation(
        &mut self,
        node_id: &ClusterNodeId,
        next_tls_certificate_sha256: String,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        let mut validation = self.fork_ephemeral();
        if !validation.stage_tls_certificate_rotation(
            node_id,
            next_tls_certificate_sha256.clone(),
            actor,
            now_ms,
        )? {
            return Ok(false);
        }
        let kind = PendingCertificateRotationKind::Stage(next_tls_certificate_sha256);
        if let Some(pending) = self.pending_certificate_rotations.get(node_id) {
            if pending.kind == kind {
                return Ok(false);
            }
            return Err(cluster_error(format!(
                "conflicting TLS certificate rotation is already pending for {node_id}"
            )));
        }
        self.pending_certificate_rotations.insert(
            node_id.clone(),
            PendingCertificateRotation {
                kind,
                actor: actor.clone(),
                now_ms,
            },
        );
        Ok(true)
    }

    pub fn queue_tls_certificate_rotation_abort(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        let mut validation = self.fork_ephemeral();
        if !validation.abort_tls_certificate_rotation(node_id, actor, now_ms)? {
            return Ok(false);
        }
        let kind = PendingCertificateRotationKind::Abort;
        if let Some(pending) = self.pending_certificate_rotations.get(node_id) {
            if pending.kind == kind {
                return Ok(false);
            }
            return Err(cluster_error(format!(
                "conflicting TLS certificate rotation is already pending for {node_id}"
            )));
        }
        self.pending_certificate_rotations.insert(
            node_id.clone(),
            PendingCertificateRotation {
                kind,
                actor: actor.clone(),
                now_ms,
            },
        );
        Ok(true)
    }

    /// Fork a planning topology with all currently queued membership changes
    /// and member heartbeats applied. The opaque token identifies exactly the
    /// versions that may be acknowledged after a proposal is accepted.
    pub fn fork_ephemeral_with_pending_metadata_mutations(
        &self,
    ) -> Result<(Self, MetadataMutationToken)> {
        let mut staged = self.fork_ephemeral();
        let mut token = MetadataMutationToken::default();
        for (node_id, pending) in &self.pending_metadata_learners {
            staged.join_metadata_learner(pending.node.clone(), &pending.actor, pending.now_ms)?;
            token
                .learner_versions
                .insert(node_id.clone(), pending.now_ms);
        }
        for (node_id, pending) in &self.pending_metadata_promotions {
            staged.promote_metadata_learner(node_id, &pending.actor, pending.now_ms)?;
            token
                .promotion_versions
                .insert(node_id.clone(), pending.now_ms);
        }
        for (node_id, pending) in &self.pending_certificate_rotations {
            match &pending.kind {
                PendingCertificateRotationKind::Stage(fingerprint) => {
                    staged.stage_tls_certificate_rotation(
                        node_id,
                        fingerprint.clone(),
                        &pending.actor,
                        pending.now_ms,
                    )?;
                }
                PendingCertificateRotationKind::Abort => {
                    staged.abort_tls_certificate_rotation(
                        node_id,
                        &pending.actor,
                        pending.now_ms,
                    )?;
                }
            }
            token
                .certificate_rotation_versions
                .insert(node_id.clone(), pending.now_ms);
        }
        for (node_id, pending) in &self.pending_heartbeats {
            staged.heartbeat_with_certificate(
                node_id,
                pending.incarnation,
                pending.used_bytes,
                pending.capacity_bytes,
                pending.labels.clone(),
                pending.tls_certificate_sha256.clone(),
                pending.now_ms,
            )?;
            token
                .heartbeat_versions
                .insert(node_id.clone(), pending.now_ms);
        }
        Ok((staged, token))
    }

    pub fn acknowledge_pending_metadata_mutations(&mut self, token: &MetadataMutationToken) {
        self.pending_heartbeats.retain(|node_id, pending| {
            token
                .heartbeat_versions
                .get(node_id)
                .is_none_or(|accepted_at_ms| pending.now_ms != *accepted_at_ms)
        });
        self.pending_metadata_learners.retain(|node_id, pending| {
            token
                .learner_versions
                .get(node_id)
                .is_none_or(|accepted_at_ms| pending.now_ms != *accepted_at_ms)
        });
        self.pending_metadata_promotions.retain(|node_id, pending| {
            token
                .promotion_versions
                .get(node_id)
                .is_none_or(|accepted_at_ms| pending.now_ms != *accepted_at_ms)
        });
        self.pending_certificate_rotations
            .retain(|node_id, pending| {
                token
                    .certificate_rotation_versions
                    .get(node_id)
                    .is_none_or(|accepted_at_ms| pending.now_ms != *accepted_at_ms)
            });
    }

    pub fn config_for_member(&self, node_id: &ClusterNodeId) -> Result<DistributionConfig> {
        let node = self
            .topology
            .nodes
            .get(node_id)
            .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
        if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
            return Err(cluster_error(format!(
                "cannot provision decommissioned cluster node {node_id}"
            )));
        }
        let mut config = self.config.clone();
        config.node_id = node.id.clone();
        config.node_address = node.address.clone();
        config.node_tls_certificate_sha256 = node.tls_certificate_sha256.clone();
        config.node_incarnation = node.incarnation;
        config.node_capacity_bytes = node.capacity_bytes;
        config.replication_factor = self.topology.replication_factor;
        Ok(config)
    }

    /// Atomically publish this topology and a member-specific local
    /// configuration into a new node's data directory. Existing conflicting
    /// cluster metadata is never overwritten.
    pub fn provision_member_directory(
        &self,
        root: impl AsRef<Path>,
        node_id: &ClusterNodeId,
        fsync: bool,
    ) -> Result<DistributionConfig> {
        self.provision_member_directory_with_transport(
            root,
            node_id,
            self.config.transport.clone(),
            fsync,
        )
    }

    /// Provision a node from an authenticated, quorum-committed topology
    /// snapshot returned by the live metadata leader.
    pub fn provision_member_directory_from_snapshot(
        &self,
        root: impl AsRef<Path>,
        topology: ClusterTopology,
        node_id: &ClusterNodeId,
        transport: ClusterNetworkTransportConfig,
        fsync: bool,
    ) -> Result<DistributionConfig> {
        topology.validate()?;
        if topology.cluster_id != self.topology.cluster_id {
            return Err(cluster_error(
                "member bootstrap snapshot belongs to another cluster",
            ));
        }
        if topology.generation < self.topology.generation {
            return Err(cluster_error(
                "member bootstrap snapshot is older than the local topology",
            ));
        }
        let mut source = self.clone();
        source.topology = topology;
        source.provision_member_directory_with_transport(root, node_id, transport, fsync)
    }

    pub fn provision_member_directory_with_transport(
        &self,
        root: impl AsRef<Path>,
        node_id: &ClusterNodeId,
        transport: ClusterNetworkTransportConfig,
        fsync: bool,
    ) -> Result<DistributionConfig> {
        provision_cluster_member_directory(
            root,
            &ClusterBootstrapSnapshot {
                topology: self.topology.clone(),
                config_template: self.config.clone(),
                metadata_leader_id: None,
            },
            node_id,
            transport,
            fsync,
        )
    }

    pub fn plan_rebalance(&self, options: &RebalanceOptions, now_ms: u64) -> Result<RebalancePlan> {
        build_rebalance_plan(&self.topology, &self.config, options, now_ms)
    }

    pub fn plan_failure_repair(
        &self,
        options: &RebalanceOptions,
        now_ms: u64,
    ) -> Result<FailureRepairPlan> {
        build_failure_repair_plan(&self.topology, &self.config, options, now_ms)
    }

    /// Run one idempotent controller cycle after the configured liveness grace
    /// period. This immediately transfers leaders to existing voters and
    /// durably allocates bounded replacement learners; snapshot execution
    /// continues through the relocation state machine.
    pub fn start_failure_repair_cycle(
        &mut self,
        options: &RebalanceOptions,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<(FailureRepairPlan, Vec<RelocationId>)> {
        let repair = self.plan_failure_repair(options, now_ms)?;
        if repair.rebalance.replica_moves.is_empty() && repair.rebalance.leader_transfers.is_empty()
        {
            return Ok((repair, Vec::new()));
        }
        let relocations = self.apply_rebalance_plan(&repair.rebalance, actor, now_ms)?;
        Ok((repair, relocations))
    }

    pub fn assess_node_removal(&self, node_id: &ClusterNodeId) -> Result<NodeRemovalSafety> {
        let node = self
            .topology
            .nodes
            .get(node_id)
            .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
        let mut replica_ranges = Vec::new();
        let mut leader_ranges = Vec::new();
        let mut placement_blocked_ranges = Vec::new();
        for range in self.topology.ranges.values() {
            if range
                .replicas
                .iter()
                .any(|replica| &replica.node_id == node_id)
            {
                replica_ranges.push(range.id);
            }
            if &range.leader == node_id {
                leader_ranges.push(range.id);
            }
            let eligible_after_removal = self
                .topology
                .nodes
                .values()
                .filter(|candidate| {
                    &candidate.id != node_id
                        && candidate.lifecycle == ClusterNodeLifecycle::Active
                        && candidate.metadata_role == MetadataMemberRole::Voter
                        && range.placement.node_is_eligible(candidate)
                        && (range.node_schema_is_compatible(&self.topology.nodes, candidate)
                            || RangeDescriptor::node_accepts_schema_bootstrap(candidate))
                })
                .count();
            if eligible_after_removal < usize::from(self.topology.replication_factor) {
                placement_blocked_ranges.push(range.id);
            }
        }
        let active_relocations = self
            .topology
            .relocations
            .values()
            .filter(|relocation| {
                relocation.is_active()
                    && (relocation.source.as_ref() == Some(node_id)
                        || &relocation.target == node_id)
            })
            .map(|relocation| relocation.id)
            .collect::<Vec<_>>();
        let remaining_active_nodes = self
            .topology
            .nodes
            .values()
            .filter(|candidate| {
                &candidate.id != node_id
                    && candidate.lifecycle == ClusterNodeLifecycle::Active
                    && candidate.metadata_role == MetadataMemberRole::Voter
            })
            .count();
        let required_active_nodes = usize::from(self.topology.replication_factor);
        let mut reasons = Vec::new();
        if node.lifecycle != ClusterNodeLifecycle::Draining
            && node.lifecycle != ClusterNodeLifecycle::Decommissioned
        {
            reasons.push("node must enter draining state first".to_string());
        }
        if !replica_ranges.is_empty() {
            reasons.push(format!(
                "node still hosts {} range replicas",
                replica_ranges.len()
            ));
        }
        if !leader_ranges.is_empty() {
            reasons.push(format!("node still leads {} ranges", leader_ranges.len()));
        }
        if !active_relocations.is_empty() {
            reasons.push(format!(
                "node participates in {} active relocations",
                active_relocations.len()
            ));
        }
        if remaining_active_nodes < required_active_nodes {
            reasons.push(format!(
                "only {remaining_active_nodes} active nodes would remain; replication factor requires {required_active_nodes}"
            ));
        }
        if !placement_blocked_ranges.is_empty() {
            reasons.push(format!(
                "{} ranges would lack enough placement-eligible nodes",
                placement_blocked_ranges.len()
            ));
        }
        let can_decommission =
            reasons.is_empty() || node.lifecycle == ClusterNodeLifecycle::Decommissioned;
        Ok(NodeRemovalSafety {
            node_id: node_id.clone(),
            can_decommission,
            replica_ranges,
            leader_ranges,
            active_relocations,
            placement_blocked_ranges,
            remaining_active_nodes,
            required_active_nodes,
            reasons,
        })
    }

    /// Durably allocate learners for a balance plan and apply independent
    /// leader transfers. Replaying an already-applied plan is idempotent.
    pub fn apply_rebalance_plan(
        &mut self,
        plan: &RebalancePlan,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<Vec<RelocationId>> {
        self.ensure_actor(actor)?;
        if self
            .topology
            .applied_rebalance_plans
            .iter()
            .any(|applied| applied == &plan.id)
        {
            return Ok(self
                .topology
                .relocations
                .values()
                .filter(|relocation| relocation.plan_id == plan.id)
                .map(|relocation| relocation.id)
                .collect());
        }
        if plan.id.is_empty() {
            return Err(cluster_error("rebalance plan id must not be empty"));
        }
        if plan.topology_generation != self.topology.generation {
            return Err(cluster_error(format!(
                "stale rebalance plan generation: plan={}, topology={}",
                plan.topology_generation, self.topology.generation
            )));
        }

        let mut moving_ranges = std::collections::BTreeSet::new();
        for movement in &plan.replica_moves {
            if !moving_ranges.insert(movement.range_id) {
                return Err(cluster_error(format!(
                    "rebalance plan {} moves range {} more than once",
                    plan.id, movement.range_id
                )));
            }
            let range = self
                .topology
                .range_by_id(movement.range_id)
                .ok_or_else(|| cluster_error(format!("unknown range {}", movement.range_id)))?;
            if range.epoch != movement.expected_epoch {
                return Err(cluster_error(format!(
                    "range {} epoch changed: plan={}, current={}",
                    movement.range_id, movement.expected_epoch, range.epoch
                )));
            }
            if self.topology.relocations.values().any(|relocation| {
                relocation.range_id == movement.range_id && relocation.is_active()
            }) {
                return Err(cluster_error(format!(
                    "range {} already has an active relocation",
                    movement.range_id
                )));
            }
            let target = self.topology.nodes.get(&movement.target).ok_or_else(|| {
                cluster_error(format!("unknown relocation target {}", movement.target))
            })?;
            if target.lifecycle != ClusterNodeLifecycle::Active
                || target.metadata_role != MetadataMemberRole::Voter
                || target.liveness(
                    now_ms,
                    self.config.suspect_after_ms,
                    self.config.dead_after_ms,
                ) != ClusterNodeLiveness::Live
                || !range.placement.node_is_eligible(target)
                || !(range.node_schema_is_compatible(&self.topology.nodes, target)
                    || RangeDescriptor::node_accepts_schema_bootstrap(target))
            {
                return Err(cluster_error(format!(
                    "relocation target {} is no longer eligible for range {}",
                    movement.target, movement.range_id
                )));
            }
            if range
                .replicas
                .iter()
                .any(|replica| replica.node_id == movement.target)
            {
                return Err(cluster_error(format!(
                    "range {} already has a replica on target {}",
                    movement.range_id, movement.target
                )));
            }
            if let Some(source) = &movement.source {
                if !range
                    .replicas
                    .iter()
                    .any(|replica| &replica.node_id == source)
                {
                    return Err(cluster_error(format!(
                        "range {} no longer has planned source {}",
                        movement.range_id, source
                    )));
                }
            }
        }
        let mut immediate_leader_transfers = std::collections::BTreeSet::new();
        for transfer in &plan.leader_transfers {
            let range = self
                .topology
                .range_by_id(transfer.range_id)
                .ok_or_else(|| cluster_error(format!("unknown range {}", transfer.range_id)))?;
            if range.epoch != transfer.expected_epoch || range.leader != transfer.source {
                return Err(cluster_error(format!(
                    "leader transfer for range {} is stale",
                    transfer.range_id
                )));
            }
            let target_is_current_voter = range.replicas.iter().any(|replica| {
                replica.node_id == transfer.target && replica.role == RangeReplicaRole::Voter
            });
            if !target_is_current_voter && !moving_ranges.contains(&transfer.range_id) {
                return Err(cluster_error(format!(
                    "leader transfer target {} is not a voter for range {}",
                    transfer.target, transfer.range_id
                )));
            }
            if target_is_current_voter {
                let target = self.topology.nodes.get(&transfer.target).ok_or_else(|| {
                    cluster_error(format!("unknown leader target {}", transfer.target))
                })?;
                if target.lifecycle == ClusterNodeLifecycle::Decommissioned
                    || !range.node_schema_is_compatible(&self.topology.nodes, target)
                    || target.liveness(
                        now_ms,
                        self.config.suspect_after_ms,
                        self.config.dead_after_ms,
                    ) == ClusterNodeLiveness::Dead
                {
                    return Err(cluster_error(format!(
                        "leader transfer target {} is not live for range {}",
                        transfer.target, transfer.range_id
                    )));
                }
                immediate_leader_transfers.insert(transfer.range_id);
            }
        }

        let mut relocation_ids = Vec::with_capacity(plan.replica_moves.len());
        while self
            .topology
            .relocations
            .len()
            .saturating_add(plan.replica_moves.len())
            > DEFAULT_RELOCATION_HISTORY_LIMIT
        {
            let Some(completed_id) = self
                .topology
                .relocations
                .iter()
                .find(|(_, relocation)| relocation.phase == RelocationPhase::Completed)
                .map(|(id, _)| *id)
            else {
                return Err(cluster_error(
                    "too many active relocations to retain bounded relocation history",
                ));
            };
            self.topology.relocations.remove(&completed_id);
        }
        // Elect an already-synchronized voter before allocating learners. A
        // dead/draining leader should not make the range wait for a snapshot
        // when surviving voters already have quorum.
        for transfer in &plan.leader_transfers {
            if !immediate_leader_transfers.contains(&transfer.range_id) {
                continue;
            }
            let range = self
                .topology
                .ranges
                .values_mut()
                .find(|range| range.id == transfer.range_id)
                .expect("validated range exists");
            range.leader = transfer.target.clone();
            range.epoch = range
                .epoch
                .checked_add(1)
                .ok_or_else(|| cluster_error("range epoch space exhausted"))?;
        }
        for movement in &plan.replica_moves {
            let target_replica_id = ReplicaId(self.topology.next_replica_id);
            self.topology.next_replica_id = self
                .topology
                .next_replica_id
                .checked_add(1)
                .ok_or_else(|| cluster_error("replica id space exhausted"))?;
            let relocation_id = RelocationId(self.topology.next_relocation_id);
            self.topology.next_relocation_id = self
                .topology
                .next_relocation_id
                .checked_add(1)
                .ok_or_else(|| cluster_error("relocation id space exhausted"))?;
            let range = self
                .topology
                .ranges
                .values_mut()
                .find(|range| range.id == movement.range_id)
                .expect("validated range exists");
            range.epoch = range
                .epoch
                .checked_add(1)
                .ok_or_else(|| cluster_error("range epoch space exhausted"))?;
            range.replicas.push(RangeReplica {
                id: target_replica_id,
                node_id: movement.target.clone(),
                role: RangeReplicaRole::Learner,
            });
            let relocation = RangeRelocation {
                id: relocation_id,
                plan_id: plan.id.clone(),
                range_id: movement.range_id,
                expected_epoch: movement.expected_epoch,
                learner_epoch: range.epoch,
                promoted_epoch: None,
                source: movement.source.clone(),
                target: movement.target.clone(),
                target_replica_id,
                reason: movement.reason,
                phase: RelocationPhase::LearnerAllocated,
                resume_phase: None,
                snapshot_id: None,
                snapshot_sha256: None,
                snapshot_bytes_copied: 0,
                snapshot_resume_after_key: None,
                snapshot_commit_sequence: None,
                destination_durable_commit_sequence: None,
                source_commit_sequence: None,
                cleanup_resume_after_key: None,
                cleanup_records_deleted: 0,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                last_error: None,
            };
            self.topology.relocations.insert(relocation_id, relocation);
            relocation_ids.push(relocation_id);
        }
        self.topology
            .applied_rebalance_plans
            .push_back(plan.id.clone());
        while self.topology.applied_rebalance_plans.len() > DEFAULT_APPLIED_PLAN_HISTORY_LIMIT {
            self.topology.applied_rebalance_plans.pop_front();
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "rebalance plan {} applied: {} relocations, {} leader transfers",
                plan.id,
                relocation_ids.len(),
                immediate_leader_transfers.len()
            ),
        )?;
        Ok(relocation_ids)
    }

    pub fn relocation(&self, relocation_id: RelocationId) -> Option<&RangeRelocation> {
        self.topology.relocations.get(&relocation_id)
    }

    /// Publish the schema installed by a successful authenticated learner
    /// preparation. Ordinary heartbeats cannot rewrite either protected
    /// schema label; only the controller that owns the exact relocation may
    /// consume the target's one-time bootstrap claim.
    pub(crate) fn certify_relocation_schema_install(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let relocation = self
            .topology
            .relocations
            .get(&relocation_id)
            .cloned()
            .ok_or_else(|| cluster_error(format!("unknown relocation {relocation_id}")))?;
        if relocation.phase != RelocationPhase::LearnerAllocated {
            return Err(cluster_error(format!(
                "cannot certify schema install for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        let required = {
            let range = self
                .topology
                .range_by_id(relocation.range_id)
                .ok_or_else(|| cluster_error(format!("unknown range {}", relocation.range_id)))?;
            if range.epoch != relocation.learner_epoch
                || !range.replicas.iter().any(|replica| {
                    replica.id == relocation.target_replica_id
                        && replica.node_id == relocation.target
                        && replica.role == RangeReplicaRole::Learner
                })
            {
                return Err(cluster_error(format!(
                    "schema install for {relocation_id} has no exact learner allocation"
                )));
            }
            range
                .required_schema_sha256(&self.topology.nodes)
                .map(str::to_string)
        };
        let Some(required) = required else {
            // Rolling-upgrade compatibility: a legacy range with no leader
            // fingerprint has no new schema authority to publish.
            return Ok(false);
        };
        let target = self
            .topology
            .nodes
            .get_mut(&relocation.target)
            .ok_or_else(|| cluster_error(format!("target for {relocation_id} disappeared")))?;
        if target.lifecycle == ClusterNodeLifecycle::Decommissioned
            || target
                .labels
                .contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
        {
            return Err(cluster_error(format!(
                "schema install target for {relocation_id} is fenced or in a schema transition"
            )));
        }
        let already_certified = target.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL)
            == Some(&required)
            && !target.labels.contains_key(SCHEMA_BOOTSTRAP_NODE_LABEL);
        if already_certified {
            return Ok(false);
        }
        if !RangeDescriptor::node_accepts_schema_bootstrap(target) {
            return Err(cluster_error(format!(
                "schema install target for {relocation_id} has no one-time bootstrap authority"
            )));
        }
        target.labels.insert(
            SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
            required.clone(),
        );
        target.labels.remove(SCHEMA_BOOTSTRAP_NODE_LABEL);
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "{relocation_id} certified installed schema {required} for learner {}",
                relocation.target
            ),
        )?;
        Ok(true)
    }

    pub fn begin_relocation_snapshot(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        match relocation.phase {
            RelocationPhase::LearnerAllocated => {
                relocation.phase = RelocationPhase::SnapshotCopying;
                relocation.updated_at_ms = now_ms;
            }
            RelocationPhase::SnapshotCopying => return Ok(false),
            phase => {
                return Err(cluster_error(format!(
                    "cannot begin snapshot for {relocation_id} in phase {phase:?}"
                )));
            }
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} began bounded snapshot copy"),
        )?;
        Ok(true)
    }

    pub fn checkpoint_relocation_snapshot(
        &mut self,
        relocation_id: RelocationId,
        bytes_copied: u64,
        resume_after_key: Option<String>,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        if relocation.phase != RelocationPhase::SnapshotCopying {
            return Err(cluster_error(format!(
                "cannot checkpoint snapshot for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        if bytes_copied < relocation.snapshot_bytes_copied {
            return Err(cluster_error(format!(
                "snapshot progress for {relocation_id} cannot move backwards"
            )));
        }
        if bytes_copied == relocation.snapshot_bytes_copied
            && resume_after_key == relocation.snapshot_resume_after_key
        {
            return Ok(false);
        }
        relocation.snapshot_bytes_copied = bytes_copied;
        relocation.snapshot_resume_after_key = resume_after_key;
        relocation.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} snapshot checkpointed at {bytes_copied} bytes"),
        )?;
        Ok(true)
    }

    pub fn finish_relocation_snapshot(
        &mut self,
        relocation_id: RelocationId,
        snapshot_id: impl Into<String>,
        snapshot_sha256: impl Into<String>,
        snapshot_commit_sequence: u64,
        bytes_copied: u64,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<()> {
        self.ensure_actor(actor)?;
        let snapshot_id = snapshot_id.into();
        let snapshot_sha256 = snapshot_sha256.into();
        if snapshot_id.is_empty() || snapshot_id.len() > 256 {
            return Err(cluster_error("snapshot id must contain 1-256 bytes"));
        }
        if snapshot_sha256.len() != 64
            || !snapshot_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(cluster_error(
                "snapshot SHA-256 must be 64 hexadecimal characters",
            ));
        }
        let relocation = self.relocation_mut(relocation_id)?;
        if relocation.phase != RelocationPhase::SnapshotCopying {
            return Err(cluster_error(format!(
                "cannot finish snapshot for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        if bytes_copied < relocation.snapshot_bytes_copied {
            return Err(cluster_error(format!(
                "final snapshot size for {relocation_id} is below its checkpoint"
            )));
        }
        relocation.snapshot_id = Some(snapshot_id);
        relocation.snapshot_sha256 = Some(snapshot_sha256.to_ascii_lowercase());
        relocation.snapshot_bytes_copied = bytes_copied;
        relocation.snapshot_resume_after_key = None;
        relocation.snapshot_commit_sequence = Some(snapshot_commit_sequence);
        relocation.destination_durable_commit_sequence = Some(snapshot_commit_sequence);
        relocation.source_commit_sequence = Some(snapshot_commit_sequence);
        relocation.phase = RelocationPhase::CatchingUp;
        relocation.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "{relocation_id} durable snapshot completed at commit {snapshot_commit_sequence}"
            ),
        )
    }

    pub fn checkpoint_relocation_catch_up(
        &mut self,
        relocation_id: RelocationId,
        destination_durable_commit_sequence: u64,
        source_commit_sequence: u64,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<RelocationPhase> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        if !matches!(
            relocation.phase,
            RelocationPhase::CatchingUp | RelocationPhase::ReadyToPromote
        ) {
            return Err(cluster_error(format!(
                "cannot checkpoint catch-up for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        let watermark = relocation.snapshot_commit_sequence.ok_or_else(|| {
            cluster_error(format!("{relocation_id} has no snapshot commit watermark"))
        })?;
        if destination_durable_commit_sequence < watermark
            || destination_durable_commit_sequence
                < relocation
                    .destination_durable_commit_sequence
                    .unwrap_or(watermark)
            || source_commit_sequence < relocation.source_commit_sequence.unwrap_or(watermark)
        {
            return Err(cluster_error(format!(
                "catch-up progress for {relocation_id} cannot move backwards"
            )));
        }
        if destination_durable_commit_sequence > source_commit_sequence {
            return Err(cluster_error(format!(
                "destination for {relocation_id} cannot be ahead of its reported source"
            )));
        }
        relocation.destination_durable_commit_sequence = Some(destination_durable_commit_sequence);
        relocation.source_commit_sequence = Some(source_commit_sequence);
        relocation.phase = if destination_durable_commit_sequence == source_commit_sequence {
            RelocationPhase::ReadyToPromote
        } else {
            RelocationPhase::CatchingUp
        };
        relocation.updated_at_ms = now_ms;
        let phase = relocation.phase;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "{relocation_id} catch-up durable={destination_durable_commit_sequence} source={source_commit_sequence}"
            ),
        )?;
        Ok(phase)
    }

    pub fn promote_relocation(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<u64> {
        self.ensure_actor(actor)?;
        let relocation = self
            .topology
            .relocations
            .get(&relocation_id)
            .cloned()
            .ok_or_else(|| cluster_error(format!("unknown relocation {relocation_id}")))?;
        if relocation.phase != RelocationPhase::ReadyToPromote {
            return Err(cluster_error(format!(
                "cannot promote {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        if relocation.destination_durable_commit_sequence < relocation.source_commit_sequence {
            return Err(cluster_error(format!(
                "cannot promote {relocation_id} before durable source parity"
            )));
        }
        {
            let range = self
                .topology
                .range_by_id(relocation.range_id)
                .ok_or_else(|| cluster_error(format!("unknown range {}", relocation.range_id)))?;
            let target_node =
                self.topology.nodes.get(&relocation.target).ok_or_else(|| {
                    cluster_error(format!("target for {relocation_id} disappeared"))
                })?;
            if !range.node_schema_is_compatible(&self.topology.nodes, target_node) {
                return Err(cluster_error(format!(
                    "cannot promote {relocation_id}: target {} no longer has the leader schema",
                    relocation.target
                )));
            }
        }
        let range = self
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == relocation.range_id)
            .ok_or_else(|| cluster_error(format!("unknown range {}", relocation.range_id)))?;
        if range.epoch != relocation.learner_epoch {
            return Err(cluster_error(format!(
                "cannot promote {relocation_id}: learner epoch {} is stale at {}",
                relocation.learner_epoch, range.epoch
            )));
        }
        let target = range
            .replicas
            .iter_mut()
            .find(|replica| replica.id == relocation.target_replica_id)
            .ok_or_else(|| cluster_error(format!("target for {relocation_id} disappeared")))?;
        if target.role != RangeReplicaRole::Learner {
            return Err(cluster_error(format!(
                "target for {relocation_id} is not a learner"
            )));
        }
        target.role = RangeReplicaRole::Voter;
        if relocation
            .source
            .as_ref()
            .is_some_and(|source| &range.leader == source)
        {
            range.leader = relocation.target.clone();
        }
        range.epoch = range
            .epoch
            .checked_add(1)
            .ok_or_else(|| cluster_error("range epoch space exhausted"))?;
        let promoted_epoch = range.epoch;
        let state = self
            .topology
            .relocations
            .get_mut(&relocation_id)
            .expect("validated relocation exists");
        state.phase = RelocationPhase::Promoted;
        state.promoted_epoch = Some(promoted_epoch);
        state.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} promoted at range epoch {promoted_epoch}"),
        )?;
        Ok(promoted_epoch)
    }

    pub fn begin_relocation_cleanup(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        match relocation.phase {
            RelocationPhase::Promoted => {
                relocation.phase = RelocationPhase::CleaningUp;
                relocation.updated_at_ms = now_ms;
            }
            RelocationPhase::CleaningUp => return Ok(false),
            phase => {
                return Err(cluster_error(format!(
                    "cannot begin cleanup for {relocation_id} in phase {phase:?}"
                )));
            }
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} began source cleanup"),
        )?;
        Ok(true)
    }

    pub fn finish_relocation_cleanup(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<u64> {
        self.ensure_actor(actor)?;
        let relocation = self
            .topology
            .relocations
            .get(&relocation_id)
            .cloned()
            .ok_or_else(|| cluster_error(format!("unknown relocation {relocation_id}")))?;
        if relocation.phase != RelocationPhase::CleaningUp {
            return Err(cluster_error(format!(
                "cannot finish cleanup for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        let promoted_epoch = relocation
            .promoted_epoch
            .ok_or_else(|| cluster_error(format!("{relocation_id} was not promoted")))?;
        let range = self
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == relocation.range_id)
            .ok_or_else(|| cluster_error(format!("unknown range {}", relocation.range_id)))?;
        if range.epoch != promoted_epoch {
            return Err(cluster_error(format!(
                "cannot clean up {relocation_id}: promoted epoch {promoted_epoch} is stale at {}",
                range.epoch
            )));
        }
        if relocation
            .source
            .as_ref()
            .is_some_and(|source| &range.leader == source)
        {
            return Err(cluster_error(format!(
                "cannot remove the leader source for {relocation_id}"
            )));
        }
        if let Some(source) = &relocation.source {
            range.replicas.retain(|replica| &replica.node_id != source);
        }
        range.epoch = range
            .epoch
            .checked_add(1)
            .ok_or_else(|| cluster_error("range epoch space exhausted"))?;
        let completed_epoch = range.epoch;
        let state = self
            .topology
            .relocations
            .get_mut(&relocation_id)
            .expect("validated relocation exists");
        state.phase = RelocationPhase::Completed;
        state.updated_at_ms = now_ms;
        state.last_error = None;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} completed at range epoch {completed_epoch}"),
        )?;
        Ok(completed_epoch)
    }

    pub fn checkpoint_relocation_cleanup(
        &mut self,
        relocation_id: RelocationId,
        records_deleted: u64,
        resume_after_key: Option<String>,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        if relocation.phase != RelocationPhase::CleaningUp {
            return Err(cluster_error(format!(
                "cannot checkpoint cleanup for {relocation_id} in phase {:?}",
                relocation.phase
            )));
        }
        if records_deleted < relocation.cleanup_records_deleted {
            return Err(cluster_error(format!(
                "cleanup progress for {relocation_id} cannot move backwards"
            )));
        }
        if records_deleted == relocation.cleanup_records_deleted
            && resume_after_key == relocation.cleanup_resume_after_key
        {
            return Ok(false);
        }
        relocation.cleanup_records_deleted = records_deleted;
        relocation.cleanup_resume_after_key = resume_after_key;
        relocation.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "{relocation_id} cleanup checkpointed after {} records",
                records_deleted
            ),
        )?;
        Ok(true)
    }

    pub fn fail_relocation(
        &mut self,
        relocation_id: RelocationId,
        message: impl Into<String>,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<()> {
        self.ensure_actor(actor)?;
        let message = message.into();
        if message.trim().is_empty() || message.len() > 4_096 {
            return Err(cluster_error(
                "relocation failure message must contain 1-4096 bytes",
            ));
        }
        let relocation = self.relocation_mut(relocation_id)?;
        if relocation.phase == RelocationPhase::Completed {
            return Err(cluster_error(format!(
                "completed relocation {relocation_id} cannot fail"
            )));
        }
        if relocation.phase != RelocationPhase::Failed {
            relocation.resume_phase = Some(relocation.phase);
        }
        relocation.phase = RelocationPhase::Failed;
        relocation.last_error = Some(message);
        relocation.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} recorded a resumable failure"),
        )
    }

    pub fn retry_relocation(
        &mut self,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<RelocationPhase> {
        self.ensure_actor(actor)?;
        let relocation = self.relocation_mut(relocation_id)?;
        if relocation.phase != RelocationPhase::Failed {
            return Err(cluster_error(format!(
                "relocation {relocation_id} is not failed"
            )));
        }
        let phase = relocation.resume_phase.take().ok_or_else(|| {
            cluster_error(format!("relocation {relocation_id} has no resume phase"))
        })?;
        relocation.phase = phase;
        relocation.last_error = None;
        relocation.updated_at_ms = now_ms;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("{relocation_id} resumed in phase {phase:?}"),
        )?;
        Ok(phase)
    }

    pub fn config(&self) -> &DistributionConfig {
        &self.config
    }

    pub fn refresh(&mut self) -> Result<&ClusterTopology> {
        let topology = load_topology(&self.root.join(DEFAULT_CLUSTER_TOPOLOGY))?;
        ensure_config_matches(&self.config, &topology)?;
        self.topology = topology;
        Ok(&self.topology)
    }

    /// Install an authoritative controller snapshot into a member cache.
    /// Generations are monotonic and equal-generation conflicts are rejected;
    /// publication uses the same atomic checksummed topology envelope as local
    /// controller changes.
    pub fn install_authoritative_topology(&mut self, topology: ClusterTopology) -> Result<bool> {
        topology.validate()?;
        ensure_config_matches(&self.config, &topology)?;
        if topology.generation < self.topology.generation {
            return Ok(false);
        }
        if topology.generation == self.topology.generation {
            if topology != self.topology {
                return Err(cluster_error(
                    "conflicting authoritative topology at the same generation",
                ));
            }
            return Ok(false);
        }
        persist_topology(
            &self.root.join(DEFAULT_CLUSTER_TOPOLOGY),
            &topology,
            self.fsync,
        )?;
        self.topology = topology;
        Ok(true)
    }

    pub fn join_node(
        &mut self,
        node: ClusterNode,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        if node.metadata_role != MetadataMemberRole::Voter {
            return Err(cluster_error(
                "join_node requires a metadata voter; use join_metadata_learner for bootstrap",
            ));
        }
        self.join_node_with_role(node, actor, now_ms)
    }

    pub fn join_metadata_learner(
        &mut self,
        node: ClusterNode,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        if node.metadata_role != MetadataMemberRole::Learner {
            return Err(cluster_error(
                "metadata bootstrap node must have the learner role",
            ));
        }
        self.join_node_with_role(node, actor, now_ms)
    }

    fn join_node_with_role(
        &mut self,
        node: ClusterNode,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        if node.lifecycle != ClusterNodeLifecycle::Active {
            return Err(cluster_error("a joining node must be active"));
        }
        if let Some(existing) = self.topology.nodes.get(&node.id) {
            // A join is a registration operation, not a heartbeat update.
            // Ignore observation-only fields so retrying the same command
            // after an uncertain response remains idempotent.
            if existing.lifecycle == ClusterNodeLifecycle::Active
                && existing.address == node.address
                && existing.tls_certificate_sha256 == node.tls_certificate_sha256
                && existing.pending_tls_certificate_sha256 == node.pending_tls_certificate_sha256
                && existing.incarnation == node.incarnation
                && existing.capacity_bytes == node.capacity_bytes
                && existing.labels == node.labels
                && (existing.metadata_role == node.metadata_role
                    || (existing.metadata_role == MetadataMemberRole::Voter
                        && node.metadata_role == MetadataMemberRole::Learner))
            {
                return Ok(false);
            }
            if existing.lifecycle != ClusterNodeLifecycle::Decommissioned {
                return Err(cluster_error(format!(
                    "cluster node {} already exists at incarnation {} and address {}",
                    node.id, existing.incarnation, existing.address
                )));
            }
            if node.incarnation <= existing.incarnation {
                return Err(cluster_error(format!(
                    "cluster node {} must rejoin with incarnation greater than {}",
                    node.id, existing.incarnation
                )));
            }
        }
        let node_id = node.id.clone();
        let role = node.metadata_role;
        self.topology.nodes.insert(node_id.clone(), node);
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} joined as metadata {role:?}"),
        )?;
        Ok(true)
    }

    pub fn promote_metadata_learner(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let node = self
            .topology
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
        if node.lifecycle != ClusterNodeLifecycle::Active {
            return Err(cluster_error(format!(
                "metadata learner {node_id} is not active"
            )));
        }
        if node.metadata_role == MetadataMemberRole::Voter {
            return Ok(false);
        }
        node.metadata_role = MetadataMemberRole::Voter;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} promoted to metadata voter"),
        )?;
        Ok(true)
    }

    pub fn stage_tls_certificate_rotation(
        &mut self,
        node_id: &ClusterNodeId,
        next_tls_certificate_sha256: String,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        if actor != node_id {
            return Err(cluster_error(
                "a node may stage a TLS certificate rotation only for its own identity",
            ));
        }
        validate_certificate_sha256(&next_tls_certificate_sha256)?;
        let node = self
            .topology
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
        if node.lifecycle != ClusterNodeLifecycle::Active {
            return Err(cluster_error(format!(
                "cluster node {node_id} is not active"
            )));
        }
        let active = node.tls_certificate_sha256.as_ref().ok_or_else(|| {
            cluster_error(format!(
                "cluster node {node_id} must bind its active TLS certificate before rotation"
            ))
        })?;
        if active == &next_tls_certificate_sha256 {
            if node.pending_tls_certificate_sha256.is_some() {
                return Err(cluster_error(format!(
                    "cluster node {node_id} cannot stage its active certificate while another rotation is pending"
                )));
            }
            return Ok(false);
        }
        if let Some(pending) = &node.pending_tls_certificate_sha256 {
            if pending == &next_tls_certificate_sha256 {
                return Ok(false);
            }
            return Err(cluster_error(format!(
                "cluster node {node_id} already has a different pending TLS certificate"
            )));
        }
        node.pending_tls_certificate_sha256 = Some(next_tls_certificate_sha256);
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} staged a TLS certificate rotation"),
        )?;
        Ok(true)
    }

    pub fn abort_tls_certificate_rotation(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        if actor != node_id {
            return Err(cluster_error(
                "a node may abort a TLS certificate rotation only for its own identity",
            ));
        }
        let node = self
            .topology
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| cluster_error(format!("unknown cluster node {node_id}")))?;
        if node.lifecycle != ClusterNodeLifecycle::Active {
            return Err(cluster_error(format!(
                "cluster node {node_id} is not active"
            )));
        }
        if node.pending_tls_certificate_sha256.take().is_none() {
            return Ok(false);
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} aborted its TLS certificate rotation"),
        )?;
        Ok(true)
    }

    pub fn split_range(
        &mut self,
        range_id: RangeId,
        split_token: u64,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<RangeId> {
        self.ensure_actor(actor)?;
        if let Some(relocation) = self.topology.active_relocation_for_range(range_id) {
            return Err(cluster_error(format!(
                "cannot split range {range_id} during active relocation {}",
                relocation.id
            )));
        }
        let (start, original) = self
            .topology
            .ranges
            .iter()
            .find(|(_, range)| range.id == range_id)
            .map(|(start, range)| (*start, range.clone()))
            .ok_or_else(|| cluster_error(format!("unknown range {range_id}")))?;
        if split_token <= original.start_token
            || original.end_token.is_some_and(|end| split_token >= end)
        {
            return Err(cluster_error(format!(
                "split token {split_token} is not inside range {} [{}, {:?})",
                original.id, original.start_token, original.end_token
            )));
        }
        let new_range_id = RangeId(self.topology.next_range_id);
        self.topology.next_range_id = self
            .topology
            .next_range_id
            .checked_add(1)
            .ok_or_else(|| cluster_error("range id space exhausted"))?;
        let next_epoch = original
            .epoch
            .checked_add(1)
            .ok_or_else(|| cluster_error(format!("range {range_id} epoch space exhausted")))?;
        let mut left = original.clone();
        left.end_token = Some(split_token);
        left.epoch = next_epoch;
        left.approximate_bytes = original.approximate_bytes / 2;
        left.approximate_qps = original.approximate_qps / 2;

        let mut right_replicas = Vec::with_capacity(original.replicas.len());
        for replica in &original.replicas {
            let replica_id = ReplicaId(self.topology.next_replica_id);
            self.topology.next_replica_id = self
                .topology
                .next_replica_id
                .checked_add(1)
                .ok_or_else(|| cluster_error("replica id space exhausted"))?;
            right_replicas.push(RangeReplica {
                id: replica_id,
                node_id: replica.node_id.clone(),
                role: replica.role,
            });
        }
        let right = RangeDescriptor {
            id: new_range_id,
            start_token: split_token,
            end_token: original.end_token,
            epoch: next_epoch,
            replicas: right_replicas,
            leader: original.leader,
            approximate_bytes: original
                .approximate_bytes
                .saturating_sub(left.approximate_bytes),
            approximate_qps: original
                .approximate_qps
                .saturating_sub(left.approximate_qps),
            placement: original.placement,
        };
        self.topology.ranges.insert(start, left);
        self.topology.ranges.insert(split_token, right);
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("range {range_id} split at token {split_token} into {new_range_id}"),
        )?;
        Ok(new_range_id)
    }

    pub fn merge_ranges(
        &mut self,
        first_id: RangeId,
        second_id: RangeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<RangeId> {
        self.ensure_actor(actor)?;
        if first_id == second_id {
            return Err(cluster_error("cannot merge a range with itself"));
        }
        for range_id in [first_id, second_id] {
            if let Some(relocation) = self.topology.active_relocation_for_range(range_id) {
                return Err(cluster_error(format!(
                    "cannot merge range {range_id} during active relocation {}",
                    relocation.id
                )));
            }
        }
        let first = self
            .topology
            .ranges
            .iter()
            .find(|(_, range)| range.id == first_id)
            .map(|(start, range)| (*start, range.clone()))
            .ok_or_else(|| cluster_error(format!("unknown range {first_id}")))?;
        let second = self
            .topology
            .ranges
            .iter()
            .find(|(_, range)| range.id == second_id)
            .map(|(start, range)| (*start, range.clone()))
            .ok_or_else(|| cluster_error(format!("unknown range {second_id}")))?;
        let ((left_start, mut left), (right_start, right)) = if first.0 < second.0 {
            (first, second)
        } else {
            (second, first)
        };
        if left.end_token != Some(right.start_token) {
            return Err(cluster_error(format!(
                "ranges {} and {} are not adjacent",
                left.id, right.id
            )));
        }
        let left_layout = left
            .replicas
            .iter()
            .map(|replica| (&replica.node_id, replica.role))
            .collect::<Vec<_>>();
        let right_layout = right
            .replicas
            .iter()
            .map(|replica| (&replica.node_id, replica.role))
            .collect::<Vec<_>>();
        if left_layout != right_layout
            || left.leader != right.leader
            || left.placement != right.placement
        {
            return Err(cluster_error(format!(
                "ranges {} and {} have incompatible replica or placement state",
                left.id, right.id
            )));
        }
        left.end_token = right.end_token;
        left.epoch = left
            .epoch
            .max(right.epoch)
            .checked_add(1)
            .ok_or_else(|| cluster_error("range epoch space exhausted"))?;
        left.approximate_bytes = left
            .approximate_bytes
            .saturating_add(right.approximate_bytes);
        left.approximate_qps = left.approximate_qps.saturating_add(right.approximate_qps);
        let retained_id = left.id;
        self.topology.ranges.insert(left_start, left);
        self.topology.ranges.remove(&right_start);
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("ranges {first_id} and {second_id} merged into retained range {retained_id}"),
        )?;
        Ok(retained_id)
    }

    pub fn set_range_placement(
        &mut self,
        range_id: RangeId,
        mut placement: PlacementPolicy,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        if let Some(relocation) = self.topology.active_relocation_for_range(range_id) {
            return Err(cluster_error(format!(
                "cannot change placement for range {range_id} during active relocation {}",
                relocation.id
            )));
        }
        placement.distinct_failure_domains.sort();
        placement.distinct_failure_domains.dedup();
        placement.validate()?;
        let Some(range) = self
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
        else {
            return Err(cluster_error(format!("unknown range {range_id}")));
        };
        if range.placement == placement {
            return Ok(false);
        }
        range.placement = placement;
        range.epoch = range
            .epoch
            .checked_add(1)
            .ok_or_else(|| cluster_error(format!("range {range_id} epoch space exhausted")))?;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("range {range_id} placement policy changed"),
        )?;
        Ok(true)
    }

    pub fn update_range_load(
        &mut self,
        range_id: RangeId,
        approximate_bytes: u64,
        approximate_qps: u64,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let Some(range) = self
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
        else {
            return Err(cluster_error(format!("unknown range {range_id}")));
        };
        if range.approximate_bytes == approximate_bytes && range.approximate_qps == approximate_qps
        {
            return Ok(false);
        }
        range.approximate_bytes = approximate_bytes;
        range.approximate_qps = approximate_qps;
        self.commit_change(
            actor.clone(),
            now_ms,
            format!(
                "range {range_id} load updated: bytes={approximate_bytes} qps={approximate_qps}"
            ),
        )?;
        Ok(true)
    }

    pub fn heartbeat(
        &mut self,
        node_id: &ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        labels: BTreeMap<String, String>,
        now_ms: u64,
    ) -> Result<bool> {
        self.heartbeat_with_certificate(
            node_id,
            incarnation,
            used_bytes,
            capacity_bytes,
            labels,
            None,
            now_ms,
        )
    }

    pub fn heartbeat_with_certificate(
        &mut self,
        node_id: &ClusterNodeId,
        incarnation: u64,
        used_bytes: u64,
        capacity_bytes: u64,
        mut labels: BTreeMap<String, String>,
        tls_certificate_sha256: Option<String>,
        now_ms: u64,
    ) -> Result<bool> {
        let Some(node) = self.topology.nodes.get_mut(node_id) else {
            return Err(cluster_error(format!("unknown cluster node {node_id}")));
        };
        if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
            return Err(cluster_error(format!(
                "decommissioned cluster node {node_id} is fenced"
            )));
        }
        if incarnation != node.incarnation {
            return Err(cluster_error(format!(
                "cluster node {node_id} incarnation mismatch: expected {}, got {incarnation}",
                node.incarnation
            )));
        }
        if capacity_bytes == 0 || used_bytes > capacity_bytes {
            return Err(cluster_error(format!(
                "invalid heartbeat capacity for {node_id}: used={used_bytes} capacity={capacity_bytes}"
            )));
        }
        if let Some(pending) = node
            .labels
            .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
            .cloned()
        {
            if labels
                .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                .is_some_and(|requested| requested != &pending)
            {
                return Err(cluster_error(format!(
                    "cluster node {node_id} cannot replace its quorum-managed pending schema target"
                )));
            }
            labels.insert(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL.to_string(), pending);
            if let Some(active) = node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL).cloned() {
                labels.insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), active);
            }
        } else if labels.contains_key(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL) {
            return Err(cluster_error(format!(
                "cluster node {node_id} cannot create a quorum-managed pending schema target by heartbeat"
            )));
        } else if let Some(current) = node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) {
            if labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(current) {
                return Err(cluster_error(format!(
                    "cluster node {node_id} cannot remove or replace its quorum-managed active schema fingerprint by heartbeat"
                )));
            }
        }
        for (key, value) in &labels {
            validate_label(key, value)?;
        }
        if let Some(fingerprint) = &tls_certificate_sha256 {
            validate_certificate_sha256(fingerprint)?;
            if node.tls_certificate_sha256.is_some()
                && !node.accepts_tls_certificate_sha256(fingerprint)
            {
                return Err(cluster_error(format!(
                    "cluster node {node_id} TLS certificate fingerprint does not match membership"
                )));
            }
        }
        if now_ms < node.last_heartbeat_ms {
            return Ok(false);
        }
        let changed = node.used_bytes != used_bytes
            || node.capacity_bytes != capacity_bytes
            || node.labels != labels
            || (node.tls_certificate_sha256.is_none() && tls_certificate_sha256.is_some())
            || node
                .pending_tls_certificate_sha256
                .as_ref()
                .is_some_and(|pending| tls_certificate_sha256.as_ref() == Some(pending))
            || node.last_heartbeat_ms != now_ms;
        if !changed {
            return Ok(false);
        }
        node.used_bytes = used_bytes;
        node.capacity_bytes = capacity_bytes;
        node.labels = labels;
        if let Some(fingerprint) = tls_certificate_sha256 {
            if node.pending_tls_certificate_sha256.as_ref() == Some(&fingerprint) {
                node.tls_certificate_sha256 = Some(fingerprint);
                node.pending_tls_certificate_sha256 = None;
            } else if node.tls_certificate_sha256.is_none() {
                node.tls_certificate_sha256 = Some(fingerprint);
            }
        }
        node.last_heartbeat_ms = now_ms;
        self.commit_change(
            node_id.clone(),
            now_ms,
            format!("node {node_id} heartbeat updated"),
        )?;
        Ok(true)
    }

    pub fn begin_drain(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        let Some(node) = self.topology.nodes.get_mut(node_id) else {
            return Err(cluster_error(format!("unknown cluster node {node_id}")));
        };
        match node.lifecycle {
            ClusterNodeLifecycle::Active => {
                node.lifecycle = ClusterNodeLifecycle::Draining;
            }
            ClusterNodeLifecycle::Draining => return Ok(false),
            ClusterNodeLifecycle::Decommissioned => {
                return Err(cluster_error(format!(
                    "cluster node {node_id} is already decommissioned"
                )));
            }
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} began draining"),
        )?;
        Ok(true)
    }

    pub fn decommission_node(
        &mut self,
        node_id: &ClusterNodeId,
        actor: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<bool> {
        self.ensure_actor(actor)?;
        if node_id == &self.config.node_id {
            return Err(cluster_error(
                "the local cluster node cannot decommission itself",
            ));
        }
        let safety = self.assess_node_removal(node_id)?;
        if !safety.can_decommission {
            return Err(cluster_error(format!(
                "cluster node {node_id} cannot be decommissioned safely: {}",
                safety.reasons.join("; ")
            )));
        }
        let Some(node) = self.topology.nodes.get_mut(node_id) else {
            return Err(cluster_error(format!("unknown cluster node {node_id}")));
        };
        match node.lifecycle {
            ClusterNodeLifecycle::Active => {
                return Err(cluster_error(format!(
                    "cluster node {node_id} must be drained before decommission"
                )));
            }
            ClusterNodeLifecycle::Draining => {
                node.lifecycle = ClusterNodeLifecycle::Decommissioned;
            }
            ClusterNodeLifecycle::Decommissioned => return Ok(false),
        }
        self.commit_change(
            actor.clone(),
            now_ms,
            format!("node {node_id} decommissioned"),
        )?;
        Ok(true)
    }

    fn ensure_actor(&self, actor: &ClusterNodeId) -> Result<()> {
        let Some(node) = self.topology.nodes.get(actor) else {
            return Err(cluster_error(format!("unknown cluster actor {actor}")));
        };
        if node.lifecycle == ClusterNodeLifecycle::Decommissioned {
            return Err(cluster_error(format!(
                "decommissioned cluster actor {actor} is fenced"
            )));
        }
        Ok(())
    }

    fn relocation_mut(&mut self, relocation_id: RelocationId) -> Result<&mut RangeRelocation> {
        self.topology
            .relocations
            .get_mut(&relocation_id)
            .ok_or_else(|| cluster_error(format!("unknown relocation {relocation_id}")))
    }

    fn commit_change(
        &mut self,
        actor: ClusterNodeId,
        now_ms: u64,
        summary: impl Into<String>,
    ) -> Result<()> {
        if self.ephemeral {
            self.topology
                .record_change(actor, now_ms, summary, self.config.topology_history_limit);
            return self.topology.validate();
        }
        let on_disk = load_topology(&self.root.join(DEFAULT_CLUSTER_TOPOLOGY))?;
        if on_disk.generation != self.topology.generation {
            let memory_generation = self.topology.generation;
            let disk_generation = on_disk.generation;
            self.topology = on_disk;
            return Err(cluster_error(format!(
                "stale cluster topology generation: memory={}, disk={}; refresh and retry",
                memory_generation, disk_generation
            )));
        }
        self.topology
            .record_change(actor, now_ms, summary, self.config.topology_history_limit);
        self.topology.validate()?;
        persist_topology(
            &self.root.join(DEFAULT_CLUSTER_TOPOLOGY),
            &self.topology,
            self.fsync,
        )
    }
}

fn ensure_config_matches(config: &DistributionConfig, topology: &ClusterTopology) -> Result<()> {
    if topology.cluster_id != config.cluster_id {
        return Err(cluster_error(format!(
            "cluster id mismatch: config={}, topology={}",
            config.cluster_id, topology.cluster_id
        )));
    }
    let Some(local) = topology.nodes.get(&config.node_id) else {
        return Err(cluster_error(format!(
            "local node {} is not a member of cluster {}",
            config.node_id, config.cluster_id
        )));
    };
    if local.incarnation != config.node_incarnation {
        return Err(cluster_error(format!(
            "local node incarnation mismatch: config={}, topology={}",
            config.node_incarnation, local.incarnation
        )));
    }
    if local.address != config.node_address {
        return Err(cluster_error(format!(
            "local node address mismatch: config={}, topology={}",
            config.node_address, local.address
        )));
    }
    if let Some(config_fingerprint) = &config.node_tls_certificate_sha256 {
        if local.tls_certificate_sha256.is_some()
            && !local.accepts_tls_certificate_sha256(config_fingerprint)
        {
            return Err(cluster_error(format!(
                "local node TLS certificate mismatch: config fingerprint is not active or pending in topology"
            )));
        }
    }
    if local.lifecycle == ClusterNodeLifecycle::Decommissioned {
        return Err(cluster_error(format!(
            "local node {} has been decommissioned and is fenced",
            config.node_id
        )));
    }
    Ok(())
}

fn load_topology(path: &Path) -> Result<ClusterTopology> {
    let bytes = fs::read(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            cluster_error(format!(
                "cluster topology {} does not exist; initialize the cluster first",
                path.display()
            ))
        } else {
            error.into()
        }
    })?;
    let envelope = serde_json::from_slice::<TopologyEnvelope>(&bytes)?;
    envelope.verify()
}

fn persist_topology(path: &Path, topology: &ClusterTopology, fsync: bool) -> Result<()> {
    let envelope = TopologyEnvelope::from_topology(topology.clone())?;
    let bytes = serde_json::to_vec_pretty(&envelope)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

pub(crate) fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(node_id: &str, address: &str) -> DistributionConfig {
        DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("cluster-a").unwrap(),
            node_id: ClusterNodeId::new(node_id).unwrap(),
            node_address: address.to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000,
            replication_factor: 3,
            initial_ranges: 4,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 100,
            dead_after_ms: 200,
            topology_history_limit: 4,
            metadata_election_timeout_ms: default_metadata_election_timeout_ms(),
            metadata_heartbeat_interval_ms: default_metadata_heartbeat_interval_ms(),
            transport: ClusterNetworkTransportConfig::default(),
        }
    }

    fn node(id: &str, address: &str, now_ms: u64) -> ClusterNode {
        ClusterNode::new(ClusterNodeId::new(id).unwrap(), address, 1, 10_000, now_ms).unwrap()
    }

    #[test]
    fn topology_persists_membership_and_idempotent_join() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        assert_eq!(store.topology().generation, 1);
        assert!(store
            .join_node(
                node("n2", "10.0.0.2:9444", 20),
                &ClusterNodeId::new("n1").unwrap(),
                20,
            )
            .unwrap());
        assert_eq!(store.topology().generation, 2);
        assert!(!store
            .join_node(
                node("n2", "10.0.0.2:9444", 30),
                &ClusterNodeId::new("n1").unwrap(),
                30,
            )
            .unwrap());
        assert_eq!(store.topology().generation, 2);

        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        assert_eq!(reopened.topology(), store.topology());
        assert_eq!(reopened.topology().nodes.len(), 2);
    }

    #[test]
    fn corrupted_topology_checksum_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        let path = dir.path().join(DEFAULT_CLUSTER_TOPOLOGY);
        let original = fs::read(&path).unwrap();
        let mut envelope: TopologyEnvelope = serde_json::from_slice(&original).unwrap();
        envelope.topology.generation = 99;
        fs::write(&path, serde_json::to_vec_pretty(&envelope).unwrap()).unwrap();

        let error = DistributionStore::open(dir.path(), config, false).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(
            fs::read(&path).unwrap(),
            serde_json::to_vec_pretty(&envelope).unwrap()
        );
    }

    #[test]
    fn config_identity_mismatch_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let original = config("n1", "10.0.0.1:9444");
        DistributionStore::initialize_at(dir.path(), original, false, 10).unwrap();

        let wrong_cluster = DistributionConfig {
            cluster_id: ClusterId::new("cluster-b").unwrap(),
            ..config("n1", "10.0.0.1:9444")
        };
        assert!(DistributionStore::open(dir.path(), wrong_cluster, false)
            .unwrap_err()
            .to_string()
            .contains("cluster id mismatch"));

        let wrong_address = config("n1", "10.0.0.9:9444");
        assert!(DistributionStore::open(dir.path(), wrong_address, false)
            .unwrap_err()
            .to_string()
            .contains("address mismatch"));
    }

    #[test]
    fn heartbeat_liveness_drain_and_incarnation_fencing_are_durable() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        store
            .join_node(node("n3", "10.0.0.3:9444", 21), &actor, 21)
            .unwrap();
        store
            .join_node(node("n4", "10.0.0.4:9444", 22), &actor, 22)
            .unwrap();
        assert_eq!(
            store.topology().liveness(&n2, 119, 100, 200),
            Some(ClusterNodeLiveness::Live)
        );
        assert_eq!(
            store.topology().liveness(&n2, 120, 100, 200),
            Some(ClusterNodeLiveness::Suspect)
        );
        assert_eq!(
            store.topology().liveness(&n2, 220, 100, 200),
            Some(ClusterNodeLiveness::Dead)
        );
        assert!(store.begin_drain(&n2, &actor, 30).unwrap());
        assert!(store.decommission_node(&n2, &actor, 40).unwrap());
        assert!(store
            .heartbeat(&n2, 1, 0, 10_000, BTreeMap::new(), 50)
            .unwrap_err()
            .to_string()
            .contains("fenced"));

        let mut stale = node("n2", "10.0.0.2:9444", 60);
        stale.incarnation = 1;
        assert!(store
            .join_node(stale, &actor, 60)
            .unwrap_err()
            .to_string()
            .contains("incarnation greater"));
        let mut fresh = node("n2", "10.0.0.2:9444", 70);
        fresh.incarnation = 2;
        assert!(store.join_node(fresh, &actor, 70).unwrap());

        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        assert_eq!(reopened.topology().nodes[&n2].incarnation, 2);
        assert_eq!(
            reopened.topology().nodes[&n2].lifecycle,
            ClusterNodeLifecycle::Active
        );
        assert!(reopened.topology().history.len() <= 4);
    }

    #[test]
    fn stale_writer_is_rejected_by_topology_generation() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut first =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        let mut stale = DistributionStore::open(dir.path(), config, false).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        first
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        let error = stale
            .join_node(node("n3", "10.0.0.3:9444", 30), &actor, 30)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("stale cluster topology generation"));
        stale.refresh().unwrap();
        assert!(stale
            .join_node(node("n3", "10.0.0.3:9444", 30), &actor, 30)
            .unwrap());
    }

    #[test]
    fn member_provisioning_keeps_target_specific_tls_identity() {
        let controller_dir = tempfile::tempdir().unwrap();
        let member_dir = tempfile::tempdir().unwrap();
        let controller_config = config("n1", "10.0.0.1:9444");
        let mut store = DistributionStore::initialize_at(
            controller_dir.path(),
            controller_config.clone(),
            false,
            10,
        )
        .unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let member_id = ClusterNodeId::new("n2").unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        let member_transport = ClusterNetworkTransportConfig {
            dev_localhost_plaintext: false,
            tls: Some(ReplicationTlsConfig {
                cert_path: PathBuf::from("/etc/bicdb/n2.crt"),
                key_path: PathBuf::from("/etc/bicdb/n2.key"),
                ca_path: PathBuf::from("/etc/bicdb/cluster-ca.crt"),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            }),
            ..ClusterNetworkTransportConfig::default()
        };

        let provisioned = store
            .provision_member_directory_with_transport(
                member_dir.path(),
                &member_id,
                member_transport.clone(),
                false,
            )
            .unwrap();

        assert_eq!(provisioned.node_id, member_id);
        assert_eq!(provisioned.transport, member_transport);
        assert_eq!(
            load_distribution_config(member_dir.path())
                .unwrap()
                .transport,
            member_transport
        );
        assert_eq!(store.config(), &controller_config);
    }

    #[test]
    fn stable_hash_and_bootstrap_ranges_cover_every_boundary() {
        assert_eq!(
            distribution_key_token("public.documents", "doc-42"),
            8_072_627_585_289_240_671
        );
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        let topology = store.topology();
        assert_eq!(topology.ranges.len(), 4);
        assert_eq!(topology.range_for_token(0).unwrap().start_token, 0);
        assert!(topology
            .range_for_token(u64::MAX)
            .unwrap()
            .end_token
            .is_none());
        for (start, range) in &topology.ranges {
            assert_eq!(topology.range_for_token(*start).unwrap().id, range.id);
            if let Some(end) = range.end_token {
                assert_eq!(
                    topology.range_for_token(end.saturating_sub(1)).unwrap().id,
                    range.id
                );
                assert_ne!(topology.range_for_token(end).unwrap().id, range.id);
            }
        }
        let routed = topology
            .range_for_key("public.documents", "doc-42")
            .unwrap();
        assert!(routed.contains_token(distribution_key_token("public.documents", "doc-42")));
    }

    #[test]
    fn split_and_merge_preserve_total_token_coverage_and_fence_epochs() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let original = store.topology().range_for_token(0).unwrap().clone();
        let split_token = original.end_token.unwrap() / 2;
        let right_id = store
            .split_range(original.id, split_token, &actor, 20)
            .unwrap();
        assert_eq!(store.topology().ranges.len(), 5);
        let left = store.topology().range_by_id(original.id).unwrap();
        let right = store.topology().range_by_id(right_id).unwrap();
        assert_eq!(left.end_token, Some(split_token));
        assert_eq!(right.start_token, split_token);
        assert_eq!(left.epoch, original.epoch + 1);
        assert_eq!(right.epoch, original.epoch + 1);
        assert_ne!(left.replicas[0].id, right.replicas[0].id);
        assert_eq!(
            store
                .topology()
                .range_for_token(split_token - 1)
                .unwrap()
                .id,
            original.id
        );
        assert_eq!(
            store.topology().range_for_token(split_token).unwrap().id,
            right_id
        );

        // A controller may disappear immediately after publishing the split.
        // Reopen before the merge so the latter consumes only the durable
        // split state, never the in-memory mutation that produced it.
        drop(store);
        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert_eq!(
            store.topology().range_for_token(split_token).unwrap().id,
            right_id
        );
        let retained = store
            .merge_ranges(right_id, original.id, &actor, 30)
            .unwrap();
        assert_eq!(retained, original.id);
        assert_eq!(store.topology().ranges.len(), 4);
        let merged = store.topology().range_by_id(retained).unwrap();
        assert_eq!(merged.start_token, original.start_token);
        assert_eq!(merged.end_token, original.end_token);
        assert!(merged.epoch > original.epoch);

        drop(store);
        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        validate_range_map(&reopened.topology().ranges, &reopened.topology().nodes).unwrap();
    }

    #[test]
    fn placement_policy_validates_labels_and_is_epoch_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let range = store.topology().range_for_token(0).unwrap().clone();
        let placement = PlacementPolicy {
            required_node_labels: BTreeMap::from([("storage".to_string(), "nvme".to_string())]),
            distinct_failure_domains: vec!["rack".to_string(), "zone".to_string()],
        };
        assert!(store
            .set_range_placement(range.id, placement.clone(), &actor, 20)
            .unwrap());
        let updated = store.topology().range_by_id(range.id).unwrap();
        assert_eq!(updated.placement, placement);
        assert_eq!(updated.epoch, range.epoch + 1);
        assert!(!updated
            .placement
            .node_is_eligible(store.topology().node(&actor).unwrap()));
    }

    #[test]
    fn schema_fingerprint_is_validated_and_incompatible_targets_are_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let n3 = ClusterNodeId::new("n3").unwrap();
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        store
            .join_node(node("n3", "10.0.0.3:9444", 21), &actor, 21)
            .unwrap();

        store
            .topology
            .nodes
            .get_mut(&actor)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64));
        store
            .topology
            .nodes
            .get_mut(&n2)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64));
        store
            .topology
            .nodes
            .get_mut(&n3)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "b".repeat(64));
        store.topology.validate().unwrap();

        let plan = build_rebalance_plan(
            &store.topology,
            &store.config,
            &generous_rebalance_options(),
            30,
        )
        .unwrap();
        assert!(!plan.replica_moves.is_empty());
        assert!(plan
            .replica_moves
            .iter()
            .all(|movement| movement.target == n2));

        let range = store.topology.range_for_token(0).unwrap().clone();
        let incompatible = RebalancePlan {
            id: "incompatible-schema-target".to_string(),
            topology_generation: store.topology.generation,
            created_at_ms: 30,
            replica_moves: vec![PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source: None,
                target: n3,
                estimated_bytes: 100,
                reason: ReplicaMoveReason::UnderReplicated,
            }],
            leader_transfers: Vec::new(),
            unplaced_ranges: Vec::new(),
            estimated_bytes_in_flight: 100,
        };
        assert!(store
            .apply_rebalance_plan(&incompatible, &actor, 30)
            .unwrap_err()
            .to_string()
            .contains("no longer eligible"));

        store.topology.nodes.get_mut(&n2).unwrap().labels.insert(
            SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
            "not-a-sha".to_string(),
        );
        assert!(store
            .topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("invalid schema compatibility fingerprint"));
    }

    #[test]
    fn heartbeat_cannot_remove_or_forge_quorum_managed_schema_window() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let mut durable = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        durable
            .topology
            .nodes
            .get_mut(&actor)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64));
        let mut store = durable.fork_ephemeral();
        store
            .topology
            .open_schema_compatibility_window(
                &BTreeSet::from([actor.clone()]),
                &"a".repeat(64),
                &"b".repeat(64),
                actor.clone(),
                20,
            )
            .unwrap();
        let node = store.topology.nodes[&actor].clone();
        store
            .heartbeat(
                &actor,
                node.incarnation,
                node.used_bytes,
                node.capacity_bytes,
                BTreeMap::from([(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "b".repeat(64))]),
                21,
            )
            .unwrap();
        assert_eq!(
            store.topology.nodes[&actor]
                .labels
                .get(SCHEMA_COMPATIBILITY_NODE_LABEL),
            Some(&"a".repeat(64))
        );
        assert_eq!(
            store.topology.nodes[&actor]
                .labels
                .get(SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL),
            Some(&"b".repeat(64))
        );

        let node = store.topology.nodes[&actor].clone();
        let error = store
            .heartbeat(
                &actor,
                node.incarnation,
                node.used_bytes,
                node.capacity_bytes,
                BTreeMap::from([
                    (SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64)),
                    (
                        SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL.to_string(),
                        "c".repeat(64),
                    ),
                ]),
                22,
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot replace its quorum-managed pending schema target"));

        store
            .topology
            .promote_schema_compatibility_window(
                &BTreeSet::from([actor.clone()]),
                &"a".repeat(64),
                &"b".repeat(64),
                actor.clone(),
                23,
            )
            .unwrap();
        let node = store.topology.nodes[&actor].clone();
        let error = store
            .heartbeat(
                &actor,
                node.incarnation,
                node.used_bytes,
                node.capacity_bytes,
                BTreeMap::from([(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64))]),
                24,
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot remove or replace its quorum-managed active schema fingerprint"));

        let node = store.topology.nodes[&actor].clone();
        let error = store
            .heartbeat(
                &actor,
                node.incarnation,
                node.used_bytes,
                node.capacity_bytes,
                BTreeMap::new(),
                25,
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("cannot remove or replace its quorum-managed active schema fingerprint"));
    }

    #[test]
    fn relocation_install_ack_consumes_bootstrap_authority_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let target = ClusterNodeId::new("n2").unwrap();
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        store
            .topology
            .nodes
            .get_mut(&actor)
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64));
        let target_node = store.topology.nodes.get_mut(&target).unwrap();
        target_node
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "b".repeat(64));
        target_node
            .labels
            .insert(SCHEMA_BOOTSTRAP_NODE_LABEL.to_string(), "true".to_string());
        store.topology.validate().unwrap();

        let (_, relocation_ids) = store
            .start_failure_repair_cycle(&generous_rebalance_options(), &actor, 30)
            .unwrap();
        let relocation_id = *relocation_ids.first().unwrap();
        assert_eq!(store.relocation(relocation_id).unwrap().target, target);
        assert!(store
            .certify_relocation_schema_install(relocation_id, &actor, 31)
            .unwrap());
        let certified = &store.topology.nodes[&target];
        assert_eq!(
            certified.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL),
            Some(&"a".repeat(64))
        );
        assert!(!certified.labels.contains_key(SCHEMA_BOOTSTRAP_NODE_LABEL));
        assert!(!store
            .certify_relocation_schema_install(relocation_id, &actor, 32)
            .unwrap());

        let certified = store.topology.nodes[&target].clone();
        assert!(store
            .heartbeat(
                &target,
                certified.incarnation,
                certified.used_bytes,
                certified.capacity_bytes,
                BTreeMap::from([(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "b".repeat(64),)]),
                33,
            )
            .is_err());
        assert!(store
            .heartbeat(
                &target,
                certified.incarnation,
                certified.used_bytes,
                certified.capacity_bytes,
                BTreeMap::new(),
                34,
            )
            .is_err());
    }

    fn generous_rebalance_options() -> RebalanceOptions {
        RebalanceOptions {
            max_replica_moves: 100,
            max_moves_per_node: 100,
            max_leader_transfers: 100,
            max_bytes_in_flight: 100_000,
            max_target_utilization_per_million: 950_000,
            unknown_range_bytes: 100,
        }
    }

    fn join_four_more(store: &mut DistributionStore) {
        let actor = ClusterNodeId::new("n1").unwrap();
        for number in 2_u64..=5 {
            store
                .join_node(
                    node(
                        &format!("n{number}"),
                        &format!("10.0.0.{number}:9444"),
                        number * 10,
                    ),
                    &actor,
                    number * 10,
                )
                .unwrap();
        }
    }

    #[test]
    fn repair_plan_is_deterministic_bounded_and_balances_new_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        join_four_more(&mut store);
        let options = generous_rebalance_options();
        let first = store.plan_rebalance(&options, 60).unwrap();
        let second = store.plan_rebalance(&options, 60).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.replica_moves.len(), 4);
        assert_eq!(first.estimated_bytes_in_flight, 400);
        assert!(first.unplaced_ranges.is_empty());
        assert!(first
            .replica_moves
            .iter()
            .all(|movement| movement.source.is_none()
                && movement.reason == ReplicaMoveReason::UnderReplicated));
        let mut targets_per_range = BTreeMap::<RangeId, std::collections::BTreeSet<_>>::new();
        let mut target_counts = BTreeMap::<ClusterNodeId, usize>::new();
        for movement in &first.replica_moves {
            assert_eq!(movement.expected_epoch, 1);
            assert!(targets_per_range
                .entry(movement.range_id)
                .or_default()
                .insert(movement.target.clone()));
            *target_counts.entry(movement.target.clone()).or_default() += 1;
        }
        assert_eq!(target_counts.values().copied().min(), Some(1));
        assert_eq!(target_counts.values().copied().max(), Some(1));
        assert!(first.replica_moves.len() <= options.max_replica_moves);
        assert!(first.estimated_bytes_in_flight <= options.max_bytes_in_flight);
    }

    #[test]
    fn fully_replicated_cluster_moves_replicas_to_a_new_empty_node() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        join_four_more(&mut store);
        let n1 = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let n3 = ClusterNodeId::new("n3").unwrap();
        let mut next_replica = 1_000_u64;
        for range in store.topology.ranges.values_mut() {
            range.replicas = [&n1, &n2, &n3]
                .into_iter()
                .map(|node_id| {
                    let replica = RangeReplica {
                        id: ReplicaId(next_replica),
                        node_id: node_id.clone(),
                        role: RangeReplicaRole::Voter,
                    };
                    next_replica += 1;
                    replica
                })
                .collect();
            range.leader = n1.clone();
            range.approximate_bytes = 100;
        }
        store.topology.next_replica_id = next_replica;
        let plan = build_rebalance_plan(
            &store.topology,
            &store.config,
            &generous_rebalance_options(),
            60,
        )
        .unwrap();
        let n5 = ClusterNodeId::new("n5").unwrap();
        let balance_moves = plan
            .replica_moves
            .iter()
            .filter(|movement| movement.reason == ReplicaMoveReason::LoadBalance)
            .collect::<Vec<_>>();
        assert_eq!(balance_moves.len(), 3);
        assert!(balance_moves.iter().all(|movement| movement.target == n5
            || movement.target == ClusterNodeId::new("n4").unwrap()));
        let target_counts = balance_moves
            .iter()
            .fold(BTreeMap::new(), |mut counts, movement| {
                *counts.entry(movement.target.clone()).or_insert(0_usize) += 1;
                counts
            });
        assert_eq!(target_counts.values().sum::<usize>(), 3);
        assert!(target_counts.values().all(|count| *count <= 2));
        assert!(!plan.leader_transfers.is_empty());
    }

    #[test]
    fn draining_replica_is_replaced_and_leader_is_transferred() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        join_four_more(&mut store);
        let n1 = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let n3 = ClusterNodeId::new("n3").unwrap();
        let actor = n1.clone();
        let range_id = store.topology.range_for_token(0).unwrap().id;
        {
            let range = store
                .topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.replicas = vec![
                RangeReplica {
                    id: ReplicaId(1_000),
                    node_id: n1.clone(),
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId(1_001),
                    node_id: n2,
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId(1_002),
                    node_id: n3,
                    role: RangeReplicaRole::Voter,
                },
            ];
            range.leader = n1.clone();
            range.approximate_bytes = 100;
        }
        store.topology.next_replica_id = 2_000;
        assert!(store.begin_drain(&n1, &actor, 70).unwrap());
        let plan = build_rebalance_plan(
            &store.topology,
            &store.config,
            &generous_rebalance_options(),
            80,
        )
        .unwrap();
        assert!(plan.replica_moves.iter().any(|movement| {
            movement.range_id == range_id
                && movement.source.as_ref() == Some(&n1)
                && movement.reason == ReplicaMoveReason::DrainingReplica
        }));
        assert!(plan.leader_transfers.iter().any(|transfer| {
            transfer.range_id == range_id
                && transfer.source == n1
                && transfer.target != transfer.source
        }));
    }

    #[test]
    fn relocation_reopens_and_resumes_at_every_durable_phase() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let target = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        let range = store.topology().range_for_token(0).unwrap().clone();
        let plan = RebalancePlan {
            id: "relocation-test-plan".to_string(),
            topology_generation: store.topology().generation,
            created_at_ms: 30,
            replica_moves: vec![PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source: Some(actor.clone()),
                target: target.clone(),
                estimated_bytes: 100,
                reason: ReplicaMoveReason::LoadBalance,
            }],
            leader_transfers: Vec::new(),
            unplaced_ranges: Vec::new(),
            estimated_bytes_in_flight: 100,
        };
        let ids = store.apply_rebalance_plan(&plan, &actor, 30).unwrap();
        assert_eq!(ids.len(), 1);
        let relocation_id = ids[0];
        let range_id = range.id;
        let learner_epoch = store.relocation(relocation_id).unwrap().learner_epoch;
        {
            let allocated = store.topology().range_by_id(range_id).unwrap();
            assert_eq!(allocated.epoch, learner_epoch);
            assert!(allocated.replicas.iter().any(|replica| {
                replica.node_id == actor && replica.role == RangeReplicaRole::Voter
            }));
            assert!(allocated.replicas.iter().any(|replica| {
                replica.node_id == target && replica.role == RangeReplicaRole::Learner
            }));
        }
        // Replaying a durably applied plan returns the same work rather than
        // allocating another learner.
        assert_eq!(store.apply_rebalance_plan(&plan, &actor, 31).unwrap(), ids);

        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert!(store
            .begin_relocation_snapshot(relocation_id, &actor, 40)
            .unwrap());
        assert!(store
            .checkpoint_relocation_snapshot(
                relocation_id,
                42,
                Some("doc-0042".to_string()),
                &actor,
                41,
            )
            .unwrap());

        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert_eq!(
            store.relocation(relocation_id).unwrap().phase,
            RelocationPhase::SnapshotCopying
        );
        assert_eq!(
            store
                .relocation(relocation_id)
                .unwrap()
                .snapshot_resume_after_key
                .as_deref(),
            Some("doc-0042")
        );
        store
            .finish_relocation_snapshot(
                relocation_id,
                "snapshot-a",
                "ab".repeat(32),
                100,
                80,
                &actor,
                50,
            )
            .unwrap();

        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert_eq!(
            store
                .checkpoint_relocation_catch_up(relocation_id, 105, 110, &actor, 60)
                .unwrap(),
            RelocationPhase::CatchingUp
        );
        store
            .fail_relocation(relocation_id, "network reset", &actor, 61)
            .unwrap();
        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert_eq!(
            store.retry_relocation(relocation_id, &actor, 62).unwrap(),
            RelocationPhase::CatchingUp
        );
        assert_eq!(
            store
                .checkpoint_relocation_catch_up(relocation_id, 110, 110, &actor, 70)
                .unwrap(),
            RelocationPhase::ReadyToPromote
        );

        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        let promoted_epoch = store.promote_relocation(relocation_id, &actor, 80).unwrap();
        let promoted = store.topology().range_by_id(range_id).unwrap();
        assert_eq!(promoted.leader, target);
        assert!(promoted.replicas.iter().any(|replica| {
            replica.node_id == actor && replica.role == RangeReplicaRole::Voter
        }));
        assert!(promoted.replicas.iter().any(|replica| {
            replica.node_id == target && replica.role == RangeReplicaRole::Voter
        }));

        let mut store = DistributionStore::open(dir.path(), config.clone(), false).unwrap();
        assert!(store
            .begin_relocation_cleanup(relocation_id, &actor, 90)
            .unwrap());
        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        assert_eq!(
            reopened.relocation(relocation_id).unwrap().phase,
            RelocationPhase::CleaningUp
        );
        assert_eq!(
            reopened.relocation(relocation_id).unwrap().promoted_epoch,
            Some(promoted_epoch)
        );
        assert!(reopened
            .topology()
            .range_by_id(range_id)
            .unwrap()
            .replicas
            .iter()
            .any(|replica| replica.node_id == actor));
    }

    #[test]
    fn relocation_cleanup_removes_source_only_after_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let target = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();
        let range = store.topology().range_for_token(0).unwrap().clone();
        let plan = RebalancePlan {
            id: "cleanup-test-plan".to_string(),
            topology_generation: store.topology().generation,
            created_at_ms: 30,
            replica_moves: vec![PlannedReplicaMove {
                range_id: range.id,
                expected_epoch: range.epoch,
                source: Some(actor.clone()),
                target: target.clone(),
                estimated_bytes: 100,
                reason: ReplicaMoveReason::LoadBalance,
            }],
            leader_transfers: Vec::new(),
            unplaced_ranges: Vec::new(),
            estimated_bytes_in_flight: 100,
        };
        let relocation_id = store.apply_rebalance_plan(&plan, &actor, 30).unwrap()[0];
        store
            .begin_relocation_snapshot(relocation_id, &actor, 40)
            .unwrap();
        store
            .finish_relocation_snapshot(
                relocation_id,
                "snapshot-b",
                "cd".repeat(32),
                100,
                1,
                &actor,
                50,
            )
            .unwrap();
        store
            .checkpoint_relocation_catch_up(relocation_id, 100, 100, &actor, 60)
            .unwrap();
        let promoted_epoch = store.promote_relocation(relocation_id, &actor, 70).unwrap();
        assert!(store
            .topology()
            .range_by_id(range.id)
            .unwrap()
            .replicas
            .iter()
            .any(|replica| replica.node_id == actor));
        assert!(store
            .begin_relocation_cleanup(relocation_id, &actor, 80)
            .unwrap());

        let mut store = DistributionStore::open(dir.path(), config, false).unwrap();
        let completed_epoch = store
            .finish_relocation_cleanup(relocation_id, &actor, 90)
            .unwrap();
        assert_eq!(completed_epoch, promoted_epoch + 1);
        let completed = store.topology().range_by_id(range.id).unwrap();
        assert!(!completed
            .replicas
            .iter()
            .any(|replica| replica.node_id == actor));
        assert!(completed
            .replicas
            .iter()
            .any(|replica| replica.node_id == target));
        assert_eq!(
            store.relocation(relocation_id).unwrap().phase,
            RelocationPhase::Completed
        );
    }

    #[test]
    fn commit_suffix_filter_preserves_sequence_and_recomputes_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            DistributionStore::initialize_at(dir.path(), config("n1", "10.0.0.1:9444"), false, 10)
                .unwrap();
        let range = store.topology().range_for_token(0).unwrap();
        let inside = (0..10_000)
            .map(|number| format!("inside-{number}"))
            .find(|id| range.contains_token(distribution_key_token("documents", id)))
            .unwrap();
        let outside = (0..10_000)
            .map(|number| format!("outside-{number}"))
            .find(|id| !range.contains_token(distribution_key_token("documents", id)))
            .unwrap();
        let frame = CommitFrame::new(
            "cluster-a",
            "n1",
            "range-stream",
            9,
            90,
            123,
            vec![
                crate::replication::ReplicationWrite {
                    collection: "documents".to_string(),
                    record_id: inside.clone(),
                    operation: crate::replication::ReplicationOperationType::Upsert,
                    payload: vec![1],
                    schema_version: 1,
                    collection_meta: None,
                },
                crate::replication::ReplicationWrite {
                    collection: "documents".to_string(),
                    record_id: outside,
                    operation: crate::replication::ReplicationOperationType::Delete,
                    payload: Vec::new(),
                    schema_version: 1,
                    collection_meta: None,
                },
            ],
        );
        let filtered = filter_commit_frame_for_range(&frame, range).unwrap();
        filtered.verify_checksum().unwrap();
        assert_eq!(filtered.commit_seq, frame.commit_seq);
        assert_eq!(filtered.previous_commit_seq, frame.previous_commit_seq);
        assert_eq!(filtered.writes.len(), 1);
        assert_eq!(filtered.writes[0].record_id, inside);
        assert_ne!(filtered.checksum, frame.checksum);
    }

    #[test]
    fn dead_leader_with_quorum_transfers_before_replacement_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let n1 = actor.clone();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let n3 = ClusterNodeId::new("n3").unwrap();
        let n4 = ClusterNodeId::new("n4").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        for (node_id, at_ms) in [("n2", 20), ("n3", 21), ("n4", 22)] {
            store
                .join_node(
                    node(node_id, &format!("10.0.0.{}:9444", &node_id[1..]), at_ms),
                    &actor,
                    at_ms,
                )
                .unwrap();
        }
        let range_id = store.topology().range_for_token(0).unwrap().id;
        {
            let range = store
                .topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.replicas = vec![
                RangeReplica {
                    id: ReplicaId(1_000),
                    node_id: n1.clone(),
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId(1_001),
                    node_id: n2.clone(),
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId(1_002),
                    node_id: n3.clone(),
                    role: RangeReplicaRole::Voter,
                },
            ];
            range.leader = n1.clone();
            range.approximate_bytes = 100;
        }
        store.topology.next_replica_id = 2_000;
        for node_id in [&n2, &n3, &n4] {
            store
                .heartbeat(node_id, 1, 0, 10_000, BTreeMap::new(), 250)
                .unwrap();
        }

        let (repair, relocation_ids) = store
            .start_failure_repair_cycle(&generous_rebalance_options(), &actor, 300)
            .unwrap();
        let availability = repair
            .range_availability
            .iter()
            .find(|availability| availability.range_id == range_id)
            .unwrap();
        assert!(availability.has_live_quorum());
        assert!(!availability.live_leader);
        assert!(repair.unavailable_ranges.contains(&range_id));
        let transfer = repair
            .rebalance
            .leader_transfers
            .iter()
            .find(|transfer| transfer.range_id == range_id)
            .unwrap();
        assert!(transfer.target == n2 || transfer.target == n3);
        assert!(store
            .topology()
            .range_by_id(range_id)
            .unwrap()
            .replicas
            .iter()
            .any(|replica| {
                replica.node_id == transfer.target && replica.role == RangeReplicaRole::Voter
            }));

        assert!(!relocation_ids.is_empty());
        let range = store.topology().range_by_id(range_id).unwrap();
        assert_eq!(range.leader, transfer.target);
        assert!(range
            .replicas
            .iter()
            .any(|replica| { replica.node_id == n4 && replica.role == RangeReplicaRole::Learner }));
        let relocation = store
            .topology()
            .active_relocation_for_range(range_id)
            .unwrap();
        assert_eq!(relocation.phase, RelocationPhase::LearnerAllocated);
        assert_eq!(relocation.learner_epoch, range.epoch);
    }

    #[test]
    fn queued_member_heartbeats_coalesce_without_regressing_liveness() {
        let dir = tempfile::tempdir().unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let member = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config("n1", "10.0.0.1:9444"), false, 10)
                .unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();

        assert!(store
            .queue_heartbeat(&member, 1, 5, 10_000, BTreeMap::new(), 100)
            .unwrap());
        assert!(!store
            .queue_heartbeat(&member, 1, 4, 10_000, BTreeMap::new(), 99)
            .unwrap());
        assert_eq!(store.topology().nodes[&member].last_heartbeat_ms, 20);

        let (staged, token) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert_eq!(staged.topology().nodes[&member].last_heartbeat_ms, 100);
        assert_eq!(staged.topology().nodes[&member].used_bytes, 5);
        store.acknowledge_pending_metadata_mutations(&token);
        let (staged, token) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert!(token.is_empty());
        assert_eq!(staged.topology(), store.topology());

        store
            .heartbeat(&member, 1, 5, 10_000, BTreeMap::new(), 100)
            .unwrap();
        assert!(!store
            .heartbeat(&member, 1, 1, 10_000, BTreeMap::new(), 90)
            .unwrap());
        assert_eq!(store.topology().nodes[&member].last_heartbeat_ms, 100);
        assert_eq!(store.topology().nodes[&member].used_bytes, 5);
    }

    #[test]
    fn member_certificate_binding_is_quorum_staged_durable_and_immutable() {
        const CERT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const CERT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let member = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        store
            .join_node(node("n2", "10.0.0.2:9444", 20), &actor, 20)
            .unwrap();

        assert!(store
            .queue_heartbeat_with_certificate(
                &member,
                1,
                5,
                10_000,
                BTreeMap::new(),
                Some(CERT_A.to_string()),
                100,
            )
            .unwrap());
        assert!(store.topology().nodes[&member]
            .tls_certificate_sha256
            .is_none());
        let (staged, token) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert_eq!(
            staged.topology().nodes[&member]
                .tls_certificate_sha256
                .as_deref(),
            Some(CERT_A)
        );
        store.acknowledge_pending_metadata_mutations(&token);

        assert!(store
            .heartbeat_with_certificate(
                &member,
                1,
                5,
                10_000,
                BTreeMap::new(),
                Some(CERT_A.to_string()),
                100,
            )
            .unwrap());
        let error = store
            .heartbeat_with_certificate(
                &member,
                1,
                6,
                10_000,
                BTreeMap::new(),
                Some(CERT_B.to_string()),
                101,
            )
            .unwrap_err();
        assert!(error.to_string().contains("does not match membership"));
        assert_eq!(
            store.topology().nodes[&member]
                .tls_certificate_sha256
                .as_deref(),
            Some(CERT_A)
        );

        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        assert_eq!(
            reopened.topology().nodes[&member]
                .tls_certificate_sha256
                .as_deref(),
            Some(CERT_A)
        );
        assert!(node("invalid", "10.0.0.3:9444", 30)
            .with_tls_certificate_sha256(CERT_A.to_uppercase())
            .unwrap_err()
            .to_string()
            .contains("lowercase hexadecimal"));
    }

    #[test]
    fn certificate_rotation_is_two_phase_quorum_staged_and_resume_safe() {
        const CERT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const CERT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        const CERT_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let controller = ClusterNodeId::new("n1").unwrap();
        let member = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config.clone(), false, 10).unwrap();
        store
            .join_node(
                node("n2", "10.0.0.2:9444", 20)
                    .with_tls_certificate_sha256(CERT_A)
                    .unwrap(),
                &controller,
                20,
            )
            .unwrap();

        let error = store
            .stage_tls_certificate_rotation(&member, CERT_B.to_string(), &controller, 30)
            .unwrap_err();
        assert!(error.to_string().contains("only for its own identity"));
        assert!(store
            .queue_tls_certificate_rotation(&member, CERT_B.to_string(), &member, 31)
            .unwrap());
        let (staged, token) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert_eq!(
            staged.topology().nodes[&member]
                .pending_tls_certificate_sha256
                .as_deref(),
            Some(CERT_B)
        );
        assert!(store.topology().nodes[&member]
            .pending_tls_certificate_sha256
            .is_none());
        store.acknowledge_pending_metadata_mutations(&token);
        store
            .stage_tls_certificate_rotation(&member, CERT_B.to_string(), &member, 31)
            .unwrap();

        // The new certificate's heartbeat is activation proof. Once queued,
        // a later heartbeat from the old certificate cannot coalesce it away.
        assert!(store
            .queue_heartbeat_with_certificate(
                &member,
                1,
                5,
                10_000,
                BTreeMap::new(),
                Some(CERT_B.to_string()),
                40,
            )
            .unwrap());
        assert!(!store
            .queue_heartbeat_with_certificate(
                &member,
                1,
                6,
                10_000,
                BTreeMap::new(),
                Some(CERT_A.to_string()),
                41,
            )
            .unwrap());
        let (activated, token) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        let activated_node = &activated.topology().nodes[&member];
        assert_eq!(
            activated_node.tls_certificate_sha256.as_deref(),
            Some(CERT_B)
        );
        assert!(activated_node.pending_tls_certificate_sha256.is_none());
        store.acknowledge_pending_metadata_mutations(&token);
        store
            .heartbeat_with_certificate(
                &member,
                1,
                5,
                10_000,
                BTreeMap::new(),
                Some(CERT_B.to_string()),
                40,
            )
            .unwrap();

        assert!(store
            .stage_tls_certificate_rotation(&member, CERT_C.to_string(), &member, 50)
            .unwrap());
        assert!(store
            .abort_tls_certificate_rotation(&member, &member, 51)
            .unwrap());
        let node = &store.topology().nodes[&member];
        assert_eq!(node.tls_certificate_sha256.as_deref(), Some(CERT_B));
        assert!(node.pending_tls_certificate_sha256.is_none());
        let error = store
            .heartbeat_with_certificate(
                &member,
                1,
                6,
                10_000,
                BTreeMap::new(),
                Some(CERT_A.to_string()),
                60,
            )
            .unwrap_err();
        assert!(error.to_string().contains("does not match membership"));

        let reopened = DistributionStore::open(dir.path(), config, false).unwrap();
        assert_eq!(
            reopened.topology().nodes[&member]
                .tls_certificate_sha256
                .as_deref(),
            Some(CERT_B)
        );
        assert!(reopened.topology().nodes[&member]
            .pending_tls_certificate_sha256
            .is_none());
    }

    #[test]
    fn metadata_learner_is_staged_promoted_and_never_receives_ranges_early() {
        let dir = tempfile::tempdir().unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        let learner_id = ClusterNodeId::new("n2").unwrap();
        let mut store =
            DistributionStore::initialize_at(dir.path(), config("n1", "10.0.0.1:9444"), false, 10)
                .unwrap();
        let learner = node("n2", "10.0.0.2:9444", 20).as_metadata_learner();
        let voter_json = serde_json::to_value(node("legacy", "10.0.0.9:9444", 20)).unwrap();
        assert!(voter_json.get("metadata_role").is_none());
        assert_eq!(
            serde_json::to_value(&learner).unwrap()["metadata_role"],
            "learner"
        );

        assert!(store
            .queue_metadata_learner(learner.clone(), &actor, 20)
            .unwrap());
        assert!(!store.queue_metadata_learner(learner, &actor, 21).unwrap());
        let (staged, registration) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert!(staged.topology().metadata_learners().contains(&learner_id));
        assert!(!staged.topology().metadata_voters().contains(&learner_id));
        assert!(staged.topology().ranges.values().all(|range| {
            range
                .replicas
                .iter()
                .all(|replica| replica.node_id != learner_id)
        }));

        store.acknowledge_pending_metadata_mutations(&registration);
        store
            .install_authoritative_topology(staged.topology().clone())
            .unwrap();
        assert!(store
            .queue_metadata_promotion(&learner_id, &learner_id, 30)
            .unwrap());
        let (promoted, promotion) = store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap();
        assert!(promoted.topology().metadata_voters().contains(&learner_id));
        assert!(!promoted
            .topology()
            .metadata_learners()
            .contains(&learner_id));
        store.acknowledge_pending_metadata_mutations(&promotion);
        assert!(store
            .fork_ephemeral_with_pending_metadata_mutations()
            .unwrap()
            .1
            .is_empty());
    }

    #[test]
    fn decommission_refuses_replicas_active_moves_and_insufficient_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let config = config("n1", "10.0.0.1:9444");
        let actor = ClusterNodeId::new("n1").unwrap();
        let n2 = ClusterNodeId::new("n2").unwrap();
        let mut store = DistributionStore::initialize_at(dir.path(), config, false, 10).unwrap();
        for (node_id, at_ms) in [("n2", 20), ("n3", 21)] {
            store
                .join_node(
                    node(node_id, &format!("10.0.0.{}:9444", &node_id[1..]), at_ms),
                    &actor,
                    at_ms,
                )
                .unwrap();
        }
        assert!(store.begin_drain(&n2, &actor, 30).unwrap());
        let capacity_blocked = store.assess_node_removal(&n2).unwrap();
        assert!(!capacity_blocked.can_decommission);
        assert_eq!(capacity_blocked.remaining_active_nodes, 2);
        assert!(store
            .decommission_node(&n2, &actor, 31)
            .unwrap_err()
            .to_string()
            .contains("replication factor"));

        store
            .join_node(node("n4", "10.0.0.4:9444", 40), &actor, 40)
            .unwrap();
        let range_id = store.topology().range_for_token(0).unwrap().id;
        let added_replica_id = ReplicaId(store.topology.next_replica_id);
        {
            let range = store
                .topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.replicas.push(RangeReplica {
                id: added_replica_id,
                node_id: n2.clone(),
                role: RangeReplicaRole::Voter,
            });
        }
        store.topology.next_replica_id += 1;
        let replica_blocked = store.assess_node_removal(&n2).unwrap();
        assert!(!replica_blocked.can_decommission);
        assert_eq!(replica_blocked.replica_ranges, vec![range_id]);
        assert!(store
            .decommission_node(&n2, &actor, 41)
            .unwrap_err()
            .to_string()
            .contains("still hosts"));

        store
            .topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap()
            .replicas
            .retain(|replica| replica.node_id != n2);
        assert!(store.assess_node_removal(&n2).unwrap().can_decommission);
        assert!(store.decommission_node(&n2, &actor, 42).unwrap());
        assert_eq!(
            store.topology().nodes[&n2].lifecycle,
            ClusterNodeLifecycle::Decommissioned
        );
    }
}
