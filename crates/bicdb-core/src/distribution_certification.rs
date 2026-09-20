//! Reproducible production-scale certification contracts for distributed BicDB.
//!
//! The unit tests in this module validate the verifier, not the production
//! claim. A scale profile passes only when a five-node operator run supplies
//! the requested logical corpus and every raw artifact verifies from disk.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::distribution::{
    build_failure_repair_plan, ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology,
    DistributionConfig, MetadataMemberRole, RangeReplicaRole, RebalanceOptions,
    CLUSTER_DATA_PROTOCOL_VERSION, DISTRIBUTION_FORMAT_VERSION, DISTRIBUTION_HASH_VERSION,
    SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_consensus::{
    MetadataConsensusRole, MetadataConsensusStatus, CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
};
use crate::distribution_query::{
    DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION, DISTRIBUTED_QUERY_PROTOCOL_VERSION,
};
use crate::distribution_supervisor::ClusterOperationalMetrics;
use crate::error::{BicDbError, Result};
use crate::format::StorageMode;
use crate::{
    ResourceCapacity, ResourceDemand, ResourceGovernorConfig, ResourceGovernorSnapshot,
    ResourceLane, ResourceLaneSnapshot, ResourceUsage,
};

pub const CLUSTER_CERTIFICATION_FORMAT_VERSION: u32 = 8;
pub const CLUSTER_CERTIFICATION_STATE_FORMAT_VERSION: u32 = 8;
pub const CLUSTER_CERTIFICATION_PUBLICATION_FORMAT_VERSION: u32 = 1;
pub const CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_CERTIFICATION_PLAN: &str = "cluster-certification-plan.json";
pub const DEFAULT_CLUSTER_CERTIFICATION_REPORT: &str = "cluster-certification-report.json";
pub const DEFAULT_CLUSTER_CERTIFICATION_STATE: &str = "cluster-certification-state.json";
pub const DEFAULT_CLUSTER_CERTIFICATION_MANIFEST: &str = "cluster-certification-manifest.json";
const MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES: usize = 64 * 1024;
const CLUSTER_CERTIFICATION_ARTIFACT_COPY_BUFFER_BYTES: usize = 1024 * 1024;
static CLUSTER_CERTIFICATION_ARTIFACT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const CERTIFICATION_BACKGROUND_LANES: [ResourceLane; 4] = [
    ResourceLane::AntiEntropy,
    ResourceLane::BackupRestore,
    ResourceLane::Compaction,
    ResourceLane::IndexBuild,
];

fn certification_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("cluster certification: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterScaleProfile {
    OneTb,
    FiveTb,
    TwentyTb,
}

impl ClusterScaleProfile {
    pub fn target_logical_bytes(self) -> u64 {
        match self {
            Self::OneTb => 1_000_000_000_000,
            Self::FiveTb => 5_000_000_000_000,
            Self::TwentyTb => 20_000_000_000_000,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneTb => "one-tb",
            Self::FiveTb => "five-tb",
            Self::TwentyTb => "twenty-tb",
        }
    }
}

impl std::fmt::Display for ClusterScaleProfile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterCertificationGates {
    pub required_nodes: u64,
    pub required_replication_factor: u8,
    pub minimum_ranges: u64,
    pub minimum_logical_dataset_bytes: u64,
    pub maximum_peak_rss_bytes_per_node: u64,
    pub maximum_disk_amplification_basis_points: u64,
    pub maximum_rebalance_amplification_basis_points: u64,
    pub maximum_foreground_p99_regression_basis_points: u64,
    pub maximum_hotspot_p99_regression_basis_points: u64,
    pub minimum_background_saturation_duration_ms: u64,
    pub minimum_background_saturation_operations: u64,
    pub minimum_background_saturation_quorum_checks: u64,
    pub minimum_background_rejections_per_node: u64,
    pub minimum_failure_quorum_operations: u64,
    pub minimum_measurement_window_ms: u64,
    pub minimum_latency_samples: u64,
    pub minimum_measurement_quorum_operations: u64,
    pub minimum_measurement_minority_write_attempts: u64,
    pub minimum_rebalance_logical_basis_points: u64,
    pub minimum_restore_quorum_operations: u64,
    pub minimum_expansion_quorum_operations: u64,
    pub minimum_fts_query_probes: u64,
    pub minimum_identity_fence_attempts: u64,
    pub maximum_recovery_time_ms: u64,
    pub maximum_replica_skew: u64,
    pub maximum_leader_skew: u64,
    pub maximum_replica_byte_skew_basis_points: u64,
}

impl ClusterCertificationGates {
    pub fn for_profile(profile: ClusterScaleProfile) -> Self {
        Self {
            required_nodes: 5,
            required_replication_factor: 3,
            minimum_ranges: 256,
            minimum_logical_dataset_bytes: profile.target_logical_bytes(),
            // The same bound applies at every corpus size. This is an
            // external-memory scale gate, not a percentage-of-corpus budget.
            maximum_peak_rss_bytes_per_node: 32 * 1024 * 1024 * 1024,
            // Physical database bytes include three replicas and indexes.
            maximum_disk_amplification_basis_points: 50_000,
            // Read + network + destination write should remain within 4x the
            // logical bytes moved.
            maximum_rebalance_amplification_basis_points: 40_000,
            maximum_foreground_p99_regression_basis_points: 2_500,
            maximum_hotspot_p99_regression_basis_points: 2_500,
            // Five minutes and ten thousand foreground/range operations are
            // enough to make a p99 claim meaningful rather than accepting a
            // momentary snapshot with a handful of lucky requests.
            minimum_background_saturation_duration_ms: 5 * 60 * 1_000,
            minimum_background_saturation_operations: 10_000,
            minimum_background_saturation_quorum_checks: 1_000,
            minimum_background_rejections_per_node: 1,
            minimum_failure_quorum_operations: 1_000,
            minimum_measurement_window_ms: 5 * 60 * 1_000,
            minimum_latency_samples: 10_000,
            minimum_measurement_quorum_operations: 10_000,
            minimum_measurement_minority_write_attempts: 1_000,
            minimum_rebalance_logical_basis_points: 1_000,
            minimum_restore_quorum_operations: 10_000,
            minimum_expansion_quorum_operations: 10_000,
            minimum_fts_query_probes: 1_000,
            minimum_identity_fence_attempts: 1_000,
            maximum_recovery_time_ms: 15 * 60 * 1_000,
            maximum_replica_skew: 1,
            maximum_leader_skew: 1,
            maximum_replica_byte_skew_basis_points: 1_500,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ClusterFailurePoint {
    Snapshot,
    CatchUp,
    ReplicaPromotion,
    Cleanup,
    Split,
    Merge,
    Backup,
    IndexRebuild,
}

impl ClusterFailurePoint {
    pub fn required() -> Vec<Self> {
        vec![
            Self::Snapshot,
            Self::CatchUp,
            Self::ReplicaPromotion,
            Self::Cleanup,
            Self::Split,
            Self::Merge,
            Self::Backup,
            Self::IndexRebuild,
        ]
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterNodeLossMechanism {
    ProcessKill,
    HostPowerCut,
    HypervisorCrash,
    NetworkBlackhole,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterNodeRecoveryMode {
    RestartSameIncarnation,
    RejoinHigherIncarnation,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ClusterCertificationArtifactKind {
    Hardware,
    EffectiveConfiguration,
    TopologyBefore,
    TopologyAfter,
    Commands,
    ResourceSamples,
    WorkloadLatency,
    BackgroundSaturation,
    RebalanceTimeline,
    FailureTimeline,
    Restore,
    FullTextIndex,
    Checksums,
}

impl ClusterCertificationArtifactKind {
    pub fn required() -> Vec<Self> {
        vec![
            Self::Hardware,
            Self::EffectiveConfiguration,
            Self::TopologyBefore,
            Self::TopologyAfter,
            Self::Commands,
            Self::ResourceSamples,
            Self::WorkloadLatency,
            Self::BackgroundSaturation,
            Self::RebalanceTimeline,
            Self::FailureTimeline,
            Self::Restore,
            Self::FullTextIndex,
            Self::Checksums,
        ]
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterCertificationProtocolVersions {
    pub distribution_format: u32,
    pub distribution_hash: u32,
    pub cluster_data: u32,
    pub metadata_consensus: u32,
    pub distributed_query: u32,
    pub distributed_fts_statistics: u32,
}

impl Default for ClusterCertificationProtocolVersions {
    fn default() -> Self {
        Self {
            distribution_format: DISTRIBUTION_FORMAT_VERSION,
            distribution_hash: DISTRIBUTION_HASH_VERSION,
            cluster_data: CLUSTER_DATA_PROTOCOL_VERSION,
            metadata_consensus: CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
            distributed_query: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
            distributed_fts_statistics: DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION,
        }
    }
}

/// Immutable proof that a certification plan was frozen from a healthy live
/// cluster rather than from a merely well-shaped or stale topology file.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCertificationPreflight {
    pub storage_mode: StorageMode,
    pub distribution_config_sha256: String,
    pub local_node_id: ClusterNodeId,
    pub schema_sha256: String,
    pub metadata_current_term: u64,
    pub metadata_commit_index: u64,
    pub metadata_last_log_index: u64,
    pub metadata_leader_id: ClusterNodeId,
    pub metadata_voters: Vec<ClusterNodeId>,
    pub node_incarnations: BTreeMap<ClusterNodeId, u64>,
    pub metrics: ClusterOperationalMetrics,
    pub replicas_by_node: BTreeMap<ClusterNodeId, u64>,
    pub leaders_by_node: BTreeMap<ClusterNodeId, u64>,
    pub replica_bytes_by_node: BTreeMap<ClusterNodeId, u64>,
    pub pending_rebalance_replica_moves: u64,
    pub pending_rebalance_leader_transfers: u64,
    pub unplaced_ranges: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCertificationPlan {
    pub format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub bicdb_version: String,
    pub created_at_ms: u64,
    pub cluster_id: ClusterId,
    pub topology_generation: u64,
    pub topology_sha256: String,
    pub node_ids: Vec<ClusterNodeId>,
    pub ranges: u64,
    pub replication_factor: u8,
    pub preflight: ClusterCertificationPreflight,
    pub protocol_versions: ClusterCertificationProtocolVersions,
    pub gates: ClusterCertificationGates,
    pub required_failure_points: Vec<ClusterFailurePoint>,
    pub required_artifact_kinds: Vec<ClusterCertificationArtifactKind>,
}

impl ClusterCertificationPlan {
    /// Topology files alone cannot prove a live production preflight.
    #[deprecated(
        since = "1.0.65-beta",
        note = "use ClusterCertificationPlan::from_live_cluster with storage, configuration, and metadata-consensus evidence"
    )]
    pub fn from_topology(
        _profile: ClusterScaleProfile,
        _run_id: impl Into<String>,
        _topology: &ClusterTopology,
        _now_ms: u64,
    ) -> Result<Self> {
        Err(certification_error(
            "topology-only certification planning is unsafe; use from_live_cluster",
        ))
    }

    /// Freeze a production certification plan from one locally observed,
    /// quorum-committed, fully healthy cluster snapshot.
    pub fn from_live_cluster(
        profile: ClusterScaleProfile,
        run_id: impl Into<String>,
        topology: &ClusterTopology,
        config: &DistributionConfig,
        metadata: &MetadataConsensusStatus,
        storage_mode: StorageMode,
        now_ms: u64,
    ) -> Result<Self> {
        topology.validate()?;
        config.validate()?;
        let run_id = run_id.into();
        validate_run_id(&run_id)?;
        let node_ids = topology
            .nodes
            .values()
            .filter(|node| {
                node.lifecycle == ClusterNodeLifecycle::Active
                    && node.metadata_role == MetadataMemberRole::Voter
            })
            .map(|node| node.id.clone())
            .collect::<Vec<_>>();
        let gates = ClusterCertificationGates::for_profile(profile);
        if node_ids.len() as u64 != gates.required_nodes {
            return Err(certification_error(format!(
                "{} requires exactly {} active metadata voters; topology has {}",
                profile,
                gates.required_nodes,
                node_ids.len()
            )));
        }
        if topology.replication_factor != gates.required_replication_factor {
            return Err(certification_error(format!(
                "{} requires replication factor {}; topology uses {}",
                profile, gates.required_replication_factor, topology.replication_factor
            )));
        }
        if (topology.ranges.len() as u64) < gates.minimum_ranges {
            return Err(certification_error(format!(
                "{} requires at least {} ranges; topology has {}",
                profile,
                gates.minimum_ranges,
                topology.ranges.len()
            )));
        }
        if !topology.metadata_learners().is_empty() {
            return Err(certification_error(
                "certification plan requires every member to be promoted",
            ));
        }
        if topology
            .relocations
            .values()
            .any(|relocation| relocation.is_active())
        {
            return Err(certification_error(
                "certification plan requires a stable topology with no active relocation",
            ));
        }
        let preflight = build_cluster_certification_preflight(
            topology,
            config,
            metadata,
            storage_mode,
            now_ms,
            &node_ids,
            &gates,
        )?;
        let topology_sha256 = sha256_bytes(&serde_json::to_vec(topology)?);
        Ok(Self {
            format_version: CLUSTER_CERTIFICATION_FORMAT_VERSION,
            run_id,
            profile,
            bicdb_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at_ms: now_ms,
            cluster_id: topology.cluster_id.clone(),
            topology_generation: topology.generation,
            topology_sha256,
            node_ids,
            ranges: topology.ranges.len() as u64,
            replication_factor: topology.replication_factor,
            preflight,
            protocol_versions: ClusterCertificationProtocolVersions::default(),
            gates,
            required_failure_points: ClusterFailurePoint::required(),
            required_artifact_kinds: ClusterCertificationArtifactKind::required(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != CLUSTER_CERTIFICATION_FORMAT_VERSION {
            return Err(certification_error(format!(
                "unsupported plan format {}; expected {}",
                self.format_version, CLUSTER_CERTIFICATION_FORMAT_VERSION
            )));
        }
        validate_run_id(&self.run_id)?;
        ClusterId::new(self.cluster_id.as_str().to_string())?;
        if self.created_at_ms == 0 || self.topology_generation == 0 {
            return Err(certification_error(
                "plan creation time and topology generation must be nonzero",
            ));
        }
        if self.bicdb_version != env!("CARGO_PKG_VERSION") {
            return Err(certification_error(format!(
                "plan BicDB version {} does not match verifier {}",
                self.bicdb_version,
                env!("CARGO_PKG_VERSION")
            )));
        }
        if self.gates != ClusterCertificationGates::for_profile(self.profile) {
            return Err(certification_error(
                "plan gates differ from the canonical profile and may not be weakened",
            ));
        }
        if self.protocol_versions != ClusterCertificationProtocolVersions::default() {
            return Err(certification_error(
                "plan protocol versions do not match this verifier",
            ));
        }
        if self.required_failure_points != ClusterFailurePoint::required()
            || self.required_artifact_kinds != ClusterCertificationArtifactKind::required()
        {
            return Err(certification_error(
                "plan omits or reorders required evidence",
            ));
        }
        let mut canonical_node_ids = self.node_ids.clone();
        canonical_node_ids.sort();
        if self.node_ids.len() as u64 != self.gates.required_nodes
            || self.node_ids.iter().collect::<BTreeSet<_>>().len() != self.node_ids.len()
            || self.node_ids != canonical_node_ids
            || self
                .node_ids
                .iter()
                .any(|node_id| ClusterNodeId::new(node_id.as_str().to_string()).is_err())
        {
            return Err(certification_error(
                "plan must contain five unique node identities",
            ));
        }
        if self.ranges < self.gates.minimum_ranges
            || self.replication_factor != self.gates.required_replication_factor
        {
            return Err(certification_error(
                "plan topology does not meet the profile",
            ));
        }
        validate_sha256(&self.topology_sha256, "topology SHA-256")?;
        validate_cluster_certification_preflight(self)
    }
}

fn build_cluster_certification_preflight(
    topology: &ClusterTopology,
    config: &DistributionConfig,
    metadata: &MetadataConsensusStatus,
    storage_mode: StorageMode,
    now_ms: u64,
    node_ids: &[ClusterNodeId],
    gates: &ClusterCertificationGates,
) -> Result<ClusterCertificationPreflight> {
    if storage_mode != StorageMode::ServerPaged {
        return Err(certification_error(format!(
            "production certification requires server_paged storage; found {}",
            storage_mode.as_str()
        )));
    }
    if !config.enabled
        || config.cluster_id != topology.cluster_id
        || config.replication_factor != topology.replication_factor
        || !topology.nodes.contains_key(&config.node_id)
    {
        return Err(certification_error(
            "distribution configuration does not identify this live cluster",
        ));
    }
    let local = &topology.nodes[&config.node_id];
    if local.incarnation != config.node_incarnation
        || local.address != config.node_address
        || local.capacity_bytes != config.node_capacity_bytes
    {
        return Err(certification_error(
            "local distribution identity, incarnation, address, or capacity differs from topology",
        ));
    }
    if topology.nodes.len() as u64 != gates.required_nodes
        || topology.nodes.values().any(|node| {
            node.lifecycle != ClusterNodeLifecycle::Active
                || node.metadata_role != MetadataMemberRole::Voter
        })
    {
        return Err(certification_error(
            "certification requires exactly five active data and metadata voters",
        ));
    }
    if !config
        .default_placement
        .distinct_failure_domains
        .iter()
        .any(|label| label == "server")
        || topology.ranges.values().any(|range| {
            !range
                .placement
                .distinct_failure_domains
                .iter()
                .any(|label| label == "server")
        })
    {
        return Err(certification_error(
            "production certification requires distinct server failure-domain placement",
        ));
    }
    let server_domains = topology
        .nodes
        .values()
        .map(|node| node.labels.get("server").cloned())
        .collect::<Option<BTreeSet<_>>>()
        .ok_or_else(|| {
            certification_error("every certification node must advertise a server identity")
        })?;
    if server_domains.len() != topology.nodes.len() {
        return Err(certification_error(
            "certification nodes must occupy distinct server identities",
        ));
    }
    if topology.nodes.values().any(|node| {
        node.joined_at_ms == 0
            || node.joined_at_ms > node.last_heartbeat_ms
            || node.last_heartbeat_ms > now_ms
    }) {
        return Err(certification_error(
            "cluster node join and heartbeat timestamps must be ordered and not in the future",
        ));
    }
    let schema_fingerprints = topology
        .nodes
        .values()
        .map(|node| node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL).cloned())
        .collect::<Option<BTreeSet<_>>>()
        .ok_or_else(|| {
            certification_error(
                "every certification node must advertise its active schema fingerprint",
            )
        })?;
    if schema_fingerprints.len() != 1 {
        return Err(certification_error(
            "all certification nodes must advertise one identical active schema fingerprint",
        ));
    }
    let schema_sha256 = schema_fingerprints
        .into_iter()
        .next()
        .expect("one schema fingerprint was required");
    validate_sha256(&schema_sha256, "active schema SHA-256")?;

    let planned = node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut metadata_voters = metadata.voters.clone();
    metadata_voters.sort();
    if metadata.cluster_id != topology.cluster_id
        || metadata.node_id != config.node_id
        || metadata.topology_generation != topology.generation
        || metadata.current_term == 0
        || metadata.commit_index == 0
        || metadata.commit_index != metadata.last_log_index
        || !metadata.learners.is_empty()
        || metadata_voters.iter().cloned().collect::<BTreeSet<_>>() != planned
        || metadata_voters.len() != node_ids.len()
    {
        return Err(certification_error(
            "metadata consensus must be current, fully committed, and contain exactly the five planned voters",
        ));
    }
    let metadata_leader_id = metadata.leader_id.clone().ok_or_else(|| {
        certification_error("metadata consensus has no established leader during preflight")
    })?;
    if !planned.contains(&metadata_leader_id)
        || metadata.role == MetadataConsensusRole::Candidate
        || (metadata.role == MetadataConsensusRole::Leader
            && metadata_leader_id != metadata.node_id)
    {
        return Err(certification_error(
            "metadata consensus role and leader identity are not stable",
        ));
    }

    let repair = build_failure_repair_plan(topology, config, &RebalanceOptions::default(), now_ms)?;
    let metrics = ClusterOperationalMetrics::from_topology(topology, &repair, now_ms);
    if metrics.nodes_total != gates.required_nodes
        || metrics.nodes_live != gates.required_nodes
        || metrics.nodes_suspect != 0
        || metrics.nodes_dead != 0
        || metrics.nodes_draining != 0
        || metrics.nodes_decommissioned != 0
        || metrics.under_replicated_ranges != 0
        || metrics.unavailable_ranges != 0
        || metrics.active_relocations != 0
        || metrics.failed_relocations != 0
    {
        return Err(certification_error(
            "cluster preflight requires five live nodes, full availability and replication, and no relocation work",
        ));
    }
    if metrics.replica_skew > gates.maximum_replica_skew
        || metrics.leader_skew > gates.maximum_leader_skew
        || !repair.rebalance.is_empty()
    {
        return Err(certification_error(
            "cluster preflight requires balanced placement with no pending repair or rebalance plan",
        ));
    }
    let expected_replicas = (topology.ranges.len() as u64)
        .checked_mul(u64::from(topology.replication_factor))
        .ok_or_else(|| certification_error("certification replica count overflowed"))?;
    if metrics.ranges_total != topology.ranges.len() as u64
        || metrics.replicas_total != expected_replicas
        || metrics.leaders_total != topology.ranges.len() as u64
        || topology.ranges.values().any(|range| {
            range.replicas.len() != usize::from(topology.replication_factor)
                || range
                    .replicas
                    .iter()
                    .any(|replica| replica.role != RangeReplicaRole::Voter)
        })
    {
        return Err(certification_error(
            "every certification range must have exactly RF3 voting replicas and one leader",
        ));
    }

    let mut replicas_by_node = node_ids
        .iter()
        .cloned()
        .map(|node_id| (node_id, 0_u64))
        .collect::<BTreeMap<_, _>>();
    let mut leaders_by_node = replicas_by_node.clone();
    let mut replica_bytes_by_node = replicas_by_node.clone();
    for range in topology.ranges.values() {
        let leaders = leaders_by_node
            .get_mut(&range.leader)
            .ok_or_else(|| certification_error("range leader is outside the plan"))?;
        *leaders = leaders.saturating_add(1);
        for replica in &range.replicas {
            let replicas = replicas_by_node
                .get_mut(&replica.node_id)
                .ok_or_else(|| certification_error("range replica is outside the plan"))?;
            *replicas = replicas.saturating_add(1);
            let bytes = replica_bytes_by_node
                .get_mut(&replica.node_id)
                .ok_or_else(|| certification_error("range replica is outside the plan"))?;
            *bytes = bytes.saturating_add(range.approximate_bytes);
        }
    }
    if map_absolute_skew(&replicas_by_node) != metrics.replica_skew
        || map_absolute_skew(&leaders_by_node) != metrics.leader_skew
        || map_absolute_skew(&replica_bytes_by_node) != metrics.replica_byte_skew
    {
        return Err(certification_error(
            "cluster preflight placement metrics are internally inconsistent",
        ));
    }
    if replica_bytes_by_node.values().any(|bytes| *bytes > 0)
        && !map_within_skew_basis_points(
            &replica_bytes_by_node,
            gates.maximum_replica_byte_skew_basis_points,
        )
    {
        return Err(certification_error(
            "cluster preflight replica bytes exceed the allowed placement skew",
        ));
    }

    let node_incarnations = topology
        .nodes
        .iter()
        .map(|(node_id, node)| (node_id.clone(), node.incarnation))
        .collect();
    Ok(ClusterCertificationPreflight {
        storage_mode,
        distribution_config_sha256: sha256_bytes(&serde_json::to_vec_pretty(config)?),
        local_node_id: config.node_id.clone(),
        schema_sha256,
        metadata_current_term: metadata.current_term,
        metadata_commit_index: metadata.commit_index,
        metadata_last_log_index: metadata.last_log_index,
        metadata_leader_id,
        metadata_voters,
        node_incarnations,
        metrics,
        replicas_by_node,
        leaders_by_node,
        replica_bytes_by_node,
        pending_rebalance_replica_moves: repair.rebalance.replica_moves.len() as u64,
        pending_rebalance_leader_transfers: repair.rebalance.leader_transfers.len() as u64,
        unplaced_ranges: repair.rebalance.unplaced_ranges.len() as u64,
    })
}

fn validate_cluster_certification_preflight(plan: &ClusterCertificationPlan) -> Result<()> {
    let preflight = &plan.preflight;
    let planned = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    validate_sha256(
        &preflight.distribution_config_sha256,
        "distribution configuration SHA-256",
    )?;
    validate_sha256(&preflight.schema_sha256, "active schema SHA-256")?;
    if preflight.storage_mode != StorageMode::ServerPaged
        || !planned.contains(&preflight.local_node_id)
        || !planned.contains(&preflight.metadata_leader_id)
        || preflight.metadata_current_term == 0
        || preflight.metadata_commit_index == 0
        || preflight.metadata_commit_index != preflight.metadata_last_log_index
        || preflight.metadata_voters != plan.node_ids
        || preflight
            .node_incarnations
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != planned
        || preflight
            .node_incarnations
            .values()
            .any(|incarnation| *incarnation == 0)
    {
        return Err(certification_error(
            "plan preflight identity, storage, incarnation, or consensus proof is invalid",
        ));
    }
    let metrics = &preflight.metrics;
    let expected_replicas = plan
        .ranges
        .checked_mul(u64::from(plan.replication_factor))
        .ok_or_else(|| certification_error("plan replica count overflowed"))?;
    if metrics.generated_at_ms != plan.created_at_ms
        || metrics.topology_generation != plan.topology_generation
        || metrics.nodes_total != plan.gates.required_nodes
        || metrics.nodes_live != plan.gates.required_nodes
        || metrics.nodes_suspect != 0
        || metrics.nodes_dead != 0
        || metrics.nodes_draining != 0
        || metrics.nodes_decommissioned != 0
        || metrics.ranges_total != plan.ranges
        || metrics.replicas_total != expected_replicas
        || metrics.leaders_total != plan.ranges
        || metrics.active_relocations != 0
        || metrics.failed_relocations != 0
        || metrics.under_replicated_ranges != 0
        || metrics.unavailable_ranges != 0
        || metrics.replica_skew > plan.gates.maximum_replica_skew
        || metrics.leader_skew > plan.gates.maximum_leader_skew
        || preflight.pending_rebalance_replica_moves != 0
        || preflight.pending_rebalance_leader_transfers != 0
        || preflight.unplaced_ranges != 0
        || preflight
            .replicas_by_node
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != planned
        || preflight
            .leaders_by_node
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != planned
        || preflight
            .replica_bytes_by_node
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != planned
    {
        return Err(certification_error(
            "plan preflight does not prove a fully healthy and balanced cluster",
        ));
    }
    let replica_total = preflight
        .replicas_by_node
        .values()
        .copied()
        .fold(0_u64, u64::saturating_add);
    let leader_total = preflight
        .leaders_by_node
        .values()
        .copied()
        .fold(0_u64, u64::saturating_add);
    if replica_total != expected_replicas
        || leader_total != plan.ranges
        || map_absolute_skew(&preflight.replicas_by_node) != metrics.replica_skew
        || map_absolute_skew(&preflight.leaders_by_node) != metrics.leader_skew
        || map_absolute_skew(&preflight.replica_bytes_by_node) != metrics.replica_byte_skew
    {
        return Err(certification_error(
            "plan preflight placement counts, bytes, and metrics are inconsistent",
        ));
    }
    if preflight
        .replica_bytes_by_node
        .values()
        .any(|bytes| *bytes > 0)
        && !map_within_skew_basis_points(
            &preflight.replica_bytes_by_node,
            plan.gates.maximum_replica_byte_skew_basis_points,
        )
    {
        return Err(certification_error(
            "plan preflight replica-byte placement is outside the canonical skew gate",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeHardwareEvidence {
    pub node_id: ClusterNodeId,
    pub hostname: String,
    pub cpu_model: String,
    pub physical_cores: u32,
    pub memory_bytes: u64,
    pub storage_model: String,
    pub storage_bytes: u64,
    pub filesystem: String,
    pub mount_options: String,
    pub operating_system: String,
    pub kernel: String,
    pub nic_bits_per_second: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterArtifactEvidence {
    pub kind: ClusterCertificationArtifactKind,
    pub relative_path: PathBuf,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterCertificationRawArtifactPayloadFormat {
    Json,
    JsonLines,
    PrometheusText,
    Text,
    Binary,
}

/// Bounded first-line metadata for every raw certification artifact.
///
/// The UTF-8 JSON header is terminated by one newline and may not exceed
/// 64 KiB. All remaining bytes are the payload covered by `payload_sha256`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCertificationRawArtifactHeader {
    pub format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub bicdb_version: String,
    pub source_commit: String,
    pub source_dirty: bool,
    pub kind: ClusterCertificationArtifactKind,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub producer_node_ids: Vec<ClusterNodeId>,
    pub payload_format: ClusterCertificationRawArtifactPayloadFormat,
    pub record_count: u64,
    pub payload_bytes: u64,
    pub payload_sha256: String,
}

struct VerifiedClusterCertificationRawArtifact {
    header: ClusterCertificationRawArtifactHeader,
    size_bytes: u64,
    sha256: String,
}

/// The single portable publication root for a completed certification bundle.
///
/// `bundle_sha256` is computed over the canonical JSON encoding of this
/// structure with that field set to an empty string. The manifest is written
/// only after the report passes and the collector becomes immutable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterCertificationPublicationManifest {
    pub format_version: u32,
    pub certification_format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub bicdb_version: String,
    pub source_commit: String,
    pub source_dirty: bool,
    pub published_at_ms: u64,
    pub plan_relative_path: PathBuf,
    pub plan_size_bytes: u64,
    pub plan_sha256: String,
    pub report_relative_path: PathBuf,
    pub report_size_bytes: u64,
    pub report_sha256: String,
    pub artifact_count: u64,
    pub artifact_bytes: u64,
    pub artifacts: Vec<ClusterArtifactEvidence>,
    pub bundle_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterFailureTrialEvidence {
    pub failure_point: ClusterFailurePoint,
    pub failed_node_id: ClusterNodeId,
    pub recovered_node_id: ClusterNodeId,
    pub failed_node_incarnation: u64,
    pub recovered_node_incarnation: u64,
    pub loss_mechanism: ClusterNodeLossMechanism,
    pub recovery_mode: ClusterNodeRecoveryMode,
    pub maintenance_operation_id: String,
    pub maintenance_checkpoint_before_sha256: String,
    pub maintenance_checkpoint_after_sha256: String,
    pub started_at_ms: u64,
    pub failure_injected_at_ms: u64,
    pub node_loss_detected_at_ms: u64,
    pub old_identity_fenced_at_ms: u64,
    pub repair_started_at_ms: u64,
    pub policy_converged_at_ms: u64,
    pub recovered_at_ms: u64,
    pub passed: bool,
    pub data_loss_detected: bool,
    pub stale_epoch_write_accepted: bool,
    pub affected_ranges: u64,
    pub repaired_ranges: u64,
    pub digest_verified_ranges: u64,
    pub verification_commit_sequence: u64,
    pub verified_records: u64,
    pub verified_logical_bytes: u64,
    pub pre_failure_digest_sha256: String,
    pub post_recovery_digest_sha256: String,
    pub quorum_operations_attempted: u64,
    pub quorum_operations_succeeded: u64,
    pub old_identity_writes_attempted: u64,
    pub old_identity_writes_rejected: u64,
    pub maximum_under_replicated_ranges: u64,
    pub final_under_replicated_ranges: u64,
    pub maximum_unavailable_ranges: u64,
    pub artifact_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterRestoreEvidence {
    pub passed: bool,
    pub backup_id: String,
    pub backup_manifest_sha256: String,
    pub restored_node_id: ClusterNodeId,
    pub source_node_incarnation: u64,
    pub restored_node_incarnation: u64,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub elapsed_ms: u64,
    pub verification_commit_sequence: u64,
    pub source_records: u64,
    pub restored_records: u64,
    pub source_logical_bytes: u64,
    pub restored_logical_bytes: u64,
    pub peak_rss_bytes: u64,
    pub source_digest_sha256: String,
    pub restored_digest_sha256: String,
    pub source_full_text_documents: u64,
    pub restored_full_text_documents: u64,
    pub source_full_text_index_bytes: u64,
    pub restored_full_text_index_bytes: u64,
    pub source_full_text_digest_sha256: String,
    pub restored_full_text_digest_sha256: String,
    pub full_text_queries_attempted: u64,
    pub full_text_queries_succeeded: u64,
    pub quorum_operations_attempted: u64,
    pub quorum_operations_succeeded: u64,
    pub old_identity_writes_attempted: u64,
    pub old_identity_writes_rejected: u64,
    pub maximum_unavailable_ranges: u64,
    pub integrity_verified: bool,
    pub artifact_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterExpansionEvidence {
    pub passed: bool,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub elapsed_ms: u64,
    pub nodes_before: u64,
    pub nodes_after: u64,
    pub topology_generation_before: u64,
    pub topology_generation_after: u64,
    pub topology_before_sha256: String,
    pub topology_after_sha256: String,
    pub added_node_hardware: Vec<ClusterNodeHardwareEvidence>,
    pub moved_logical_bytes: u64,
    pub source_read_bytes: u64,
    pub network_bytes: u64,
    pub destination_write_bytes: u64,
    pub peak_rss_bytes_by_node: BTreeMap<ClusterNodeId, u64>,
    pub foreground_baseline_p99_ms: f64,
    pub foreground_expansion_p99_ms: f64,
    pub foreground_baseline_samples: u64,
    pub foreground_expansion_samples: u64,
    pub hotspot_baseline_p99_ms: f64,
    pub hotspot_expansion_p99_ms: f64,
    pub hotspot_baseline_samples: u64,
    pub hotspot_expansion_samples: u64,
    pub quorum_operations_attempted: u64,
    pub quorum_operations_succeeded: u64,
    pub verification_commit_sequence: u64,
    pub verified_records: u64,
    pub verified_logical_bytes: u64,
    pub pre_expansion_digest_sha256: String,
    pub post_expansion_digest_sha256: String,
    pub full_text_source_bytes_before: u64,
    pub full_text_source_bytes_after: u64,
    pub full_text_documents_before: u64,
    pub full_text_documents_after: u64,
    pub full_text_index_bytes_before: u64,
    pub full_text_index_bytes_after: u64,
    pub full_text_digest_sha256_before: String,
    pub full_text_digest_sha256_after: String,
    pub full_text_queries_attempted: u64,
    pub full_text_queries_succeeded: u64,
    pub final_replica_skew: u64,
    pub final_leader_skew: u64,
    pub final_active_relocations: u64,
    pub final_failed_relocations: u64,
    pub final_replica_bytes_by_node: BTreeMap<ClusterNodeId, u64>,
    pub maximum_under_replicated_ranges: u64,
    pub final_under_replicated_ranges: u64,
    pub maximum_unavailable_ranges: u64,
    pub final_unavailable_ranges: u64,
    pub artifact_paths: Vec<PathBuf>,
}

/// Fixed-cardinality governor snapshots proving that one node was genuinely
/// saturated by every high-amplification background lane. The timestamps bind
/// the snapshots to the enclosing continuous foreground/quorum workload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeBackgroundSaturationEvidence {
    pub node_id: ClusterNodeId,
    pub governor_config: ResourceGovernorConfig,
    pub rejected_lane: ResourceLane,
    pub rejected_demand: ResourceDemand,
    pub before_at_ms: u64,
    pub saturated_at_ms: u64,
    pub after_at_ms: u64,
    pub before: ResourceGovernorSnapshot,
    pub saturated: ResourceGovernorSnapshot,
    pub after: ResourceGovernorSnapshot,
}

/// Evidence for the destructive background-saturation phase. All operations
/// are counted only inside `started_at_ms..=completed_at_ms`; raw latency and
/// governor samples are referenced separately and cryptographically verified.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterBackgroundSaturationEvidence {
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub nodes: Vec<ClusterNodeBackgroundSaturationEvidence>,
    pub foreground_read_baseline_p99_ms: f64,
    pub foreground_read_saturated_p99_ms: f64,
    pub foreground_read_operations_attempted: u64,
    pub foreground_read_operations_succeeded: u64,
    pub foreground_write_baseline_p99_ms: f64,
    pub foreground_write_saturated_p99_ms: f64,
    pub foreground_write_operations_attempted: u64,
    pub foreground_write_operations_succeeded: u64,
    pub metadata_quorum_checks_attempted: u64,
    pub metadata_quorum_checks_succeeded: u64,
    pub range_quorum_operations_attempted: u64,
    pub range_quorum_operations_succeeded: u64,
    pub maximum_unavailable_ranges: u64,
    pub artifact_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterCertificationMeasurements {
    pub logical_dataset_bytes: u64,
    pub total_physical_database_bytes: u64,
    pub peak_rss_bytes_by_node: BTreeMap<ClusterNodeId, u64>,
    pub baseline_started_at_ms: u64,
    pub baseline_completed_at_ms: u64,
    pub rebalance_started_at_ms: u64,
    pub rebalance_completed_at_ms: u64,
    pub recovery_started_at_ms: u64,
    pub recovery_completed_at_ms: u64,
    pub foreground_baseline_p99_ms: f64,
    pub foreground_rebalance_p99_ms: f64,
    pub foreground_baseline_samples: u64,
    pub foreground_rebalance_samples: u64,
    pub hotspot_baseline_p99_ms: f64,
    pub hotspot_rebalance_p99_ms: f64,
    pub hotspot_baseline_samples: u64,
    pub hotspot_rebalance_samples: u64,
    pub quorum_operations_attempted: u64,
    pub quorum_operations_succeeded: u64,
    pub minority_writes_attempted: u64,
    pub minority_writes_rejected: u64,
    pub logical_rebalance_bytes: u64,
    pub rebalance_source_read_bytes: u64,
    pub rebalance_network_bytes: u64,
    pub rebalance_destination_write_bytes: u64,
    pub recovery_time_ms: u64,
    pub maximum_unavailable_ranges: u64,
    pub final_metrics: ClusterOperationalMetrics,
    pub final_replica_bytes_by_node: BTreeMap<ClusterNodeId, u64>,
    pub full_text_index_source_bytes: u64,
    pub full_text_index_documents: u64,
    pub full_text_index_bytes: u64,
    pub artifact_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterCertificationReport {
    pub format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub plan_sha256: String,
    pub bicdb_version: String,
    pub source_commit: String,
    pub source_dirty: bool,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub completed: bool,
    pub hardware: Vec<ClusterNodeHardwareEvidence>,
    pub measurements: ClusterCertificationMeasurements,
    pub background_saturation: ClusterBackgroundSaturationEvidence,
    pub failure_trials: Vec<ClusterFailureTrialEvidence>,
    pub restore: ClusterRestoreEvidence,
    pub expansion: ClusterExpansionEvidence,
    pub artifacts: Vec<ClusterArtifactEvidence>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterCertificationVerification {
    pub format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub passed: bool,
    pub verified_artifacts: u64,
    pub verified_artifact_bytes: u64,
    pub publication_manifest_sha256: String,
    pub failures: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterCertificationState {
    pub format_version: u32,
    pub run_id: String,
    pub profile: ClusterScaleProfile,
    pub plan_sha256: String,
    pub bicdb_version: String,
    pub source_commit: String,
    pub source_dirty: bool,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub hardware: BTreeMap<ClusterNodeId, ClusterNodeHardwareEvidence>,
    pub measurements: Option<ClusterCertificationMeasurements>,
    pub background_saturation: Option<ClusterBackgroundSaturationEvidence>,
    pub failure_trials: BTreeMap<ClusterFailurePoint, ClusterFailureTrialEvidence>,
    pub restore: Option<ClusterRestoreEvidence>,
    pub expansion: Option<ClusterExpansionEvidence>,
    pub artifacts: BTreeMap<PathBuf, ClusterArtifactEvidence>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "evidence", rename_all = "snake_case")]
pub enum ClusterCertificationObservation {
    Hardware(ClusterNodeHardwareEvidence),
    Measurements(ClusterCertificationMeasurements),
    BackgroundSaturation(ClusterBackgroundSaturationEvidence),
    FailureTrial(ClusterFailureTrialEvidence),
    Restore(ClusterRestoreEvidence),
    Expansion(ClusterExpansionEvidence),
}

pub fn save_cluster_certification_plan(
    path: impl AsRef<Path>,
    plan: &ClusterCertificationPlan,
    fsync: bool,
) -> Result<()> {
    plan.validate()?;
    let bytes = serde_json::to_vec_pretty(plan)?;
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_cluster_certification_plan(path: impl AsRef<Path>) -> Result<ClusterCertificationPlan> {
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&fs::read(path)?)?;
    plan.validate()?;
    Ok(plan)
}

pub fn load_cluster_certification_report(
    path: impl AsRef<Path>,
) -> Result<ClusterCertificationReport> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

pub fn load_cluster_certification_publication_manifest(
    path: impl AsRef<Path>,
) -> Result<ClusterCertificationPublicationManifest> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn verify_cluster_certification_raw_artifact(
    path: &Path,
    plan: &ClusterCertificationPlan,
    source_commit: &str,
    source_dirty: bool,
    expected_kind: ClusterCertificationArtifactKind,
    minimum_started_at_ms: u64,
    maximum_completed_at_ms: u64,
) -> Result<VerifiedClusterCertificationRawArtifact> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut header_line = Vec::new();
    {
        let mut limited = reader
            .by_ref()
            .take((MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES + 1) as u64);
        limited.read_until(b'\n', &mut header_line)?;
    }
    if header_line.is_empty()
        || header_line.len() > MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES
        || header_line.last() != Some(&b'\n')
    {
        return Err(certification_error(
            "raw artifact must start with a newline-terminated JSON header no larger than 64 KiB",
        ));
    }
    let mut header_json = header_line.as_slice();
    header_json = &header_json[..header_json.len() - 1];
    if header_json.last() == Some(&b'\r') {
        header_json = &header_json[..header_json.len() - 1];
    }
    let header = serde_json::from_slice::<ClusterCertificationRawArtifactHeader>(header_json)
        .map_err(|error| certification_error(format!("raw artifact header is invalid: {error}")))?;
    let planned_nodes = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let producer_nodes = header
        .producer_node_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if header.format_version != CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION
        || header.run_id != plan.run_id
        || header.profile != plan.profile
        || header.bicdb_version != plan.bicdb_version
        || header.source_commit != source_commit
        || header.source_dirty != source_dirty
        || header.kind != expected_kind
    {
        return Err(certification_error(
            "raw artifact header does not match this run, profile, source, version, or kind",
        ));
    }
    if producer_nodes != planned_nodes || header.producer_node_ids.len() != plan.node_ids.len() {
        return Err(certification_error(
            "raw artifact header must name every planned producer node exactly once",
        ));
    }
    if header.started_at_ms < minimum_started_at_ms
        || header.completed_at_ms < header.started_at_ms
        || header.completed_at_ms > maximum_completed_at_ms
    {
        return Err(certification_error(
            "raw artifact observation timestamps fall outside the certification run",
        ));
    }
    if header.record_count == 0
        || header.payload_bytes == 0
        || validate_sha256(&header.payload_sha256, "raw artifact payload SHA-256").is_err()
    {
        return Err(certification_error(
            "raw artifact must declare nonzero records and payload with a valid SHA-256",
        ));
    }
    if expected_kind == ClusterCertificationArtifactKind::EffectiveConfiguration
        && !header
            .payload_sha256
            .eq_ignore_ascii_case(&plan.preflight.distribution_config_sha256)
    {
        return Err(certification_error(
            "effective-configuration artifact payload does not match the plan preflight configuration",
        ));
    }
    if expected_kind == ClusterCertificationArtifactKind::TopologyBefore
        && !header
            .payload_sha256
            .eq_ignore_ascii_case(&plan.topology_sha256)
    {
        return Err(certification_error(
            "topology-before artifact payload does not match the topology frozen by the plan",
        ));
    }

    let mut payload_digest = Sha256::new();
    let mut artifact_digest = Sha256::new();
    artifact_digest.update(&header_line);
    let mut payload_bytes = 0_u64;
    let mut buffer = vec![0_u8; CLUSTER_CERTIFICATION_ARTIFACT_COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        payload_bytes = payload_bytes
            .checked_add(read as u64)
            .ok_or_else(|| certification_error("raw artifact payload byte count overflowed"))?;
        payload_digest.update(&buffer[..read]);
        artifact_digest.update(&buffer[..read]);
    }
    let payload_sha256 = hex::encode(payload_digest.finalize());
    if payload_bytes != header.payload_bytes
        || !payload_sha256.eq_ignore_ascii_case(&header.payload_sha256)
    {
        return Err(certification_error(
            "raw artifact payload size or SHA-256 does not match its header",
        ));
    }
    let size_bytes = (header_line.len() as u64)
        .checked_add(payload_bytes)
        .ok_or_else(|| certification_error("raw artifact total byte count overflowed"))?;
    Ok(VerifiedClusterCertificationRawArtifact {
        header,
        size_bytes,
        sha256: hex::encode(artifact_digest.finalize()),
    })
}

pub fn initialize_cluster_certification_bundle(
    bundle_root: impl AsRef<Path>,
    plan_path: impl AsRef<Path>,
    source_commit: impl Into<String>,
    source_dirty: bool,
    started_at_ms: u64,
    fsync: bool,
) -> Result<ClusterCertificationState> {
    let bundle_root = bundle_root.as_ref();
    fs::create_dir_all(bundle_root)?;
    let plan_bytes = fs::read(plan_path)?;
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&plan_bytes)?;
    plan.validate()?;
    let source_commit = source_commit.into();
    if !valid_source_commit(&source_commit) {
        return Err(certification_error(
            "source commit must be a 40-64 digit hexadecimal object ID",
        ));
    }
    if source_dirty {
        return Err(certification_error(
            "production certification cannot start from a dirty source tree",
        ));
    }
    if started_at_ms == 0 {
        return Err(certification_error("start time must be nonzero"));
    }
    let plan_sha256 = sha256_bytes(&plan_bytes);
    let bundled_plan_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
    match fs::read(&bundled_plan_path) {
        Ok(existing) if existing != plan_bytes => {
            return Err(certification_error(
                "bundle already contains a different certification plan",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            crate::storage::write_atomic(&bundled_plan_path, &plan_bytes, fsync)?;
        }
        Err(error) => return Err(error.into()),
    }

    let state_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_STATE);
    let manifest_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
    if manifest_path.exists() && !state_path.exists() {
        return Err(certification_error(
            "published bundle has no matching immutable collector state",
        ));
    }
    if state_path.exists() {
        let state = load_cluster_certification_state(bundle_root)?;
        if manifest_path.exists() && state.completed_at_ms.is_none() {
            return Err(certification_error(
                "open collector unexpectedly contains a publication manifest",
            ));
        }
        if state.run_id != plan.run_id
            || state.profile != plan.profile
            || state.plan_sha256 != plan_sha256
            || state.bicdb_version != plan.bicdb_version
            || state.source_commit != source_commit
            || state.source_dirty != source_dirty
        {
            return Err(certification_error(
                "existing collector state is bound to different run inputs",
            ));
        }
        return Ok(state);
    }

    let state = ClusterCertificationState {
        format_version: CLUSTER_CERTIFICATION_STATE_FORMAT_VERSION,
        run_id: plan.run_id,
        profile: plan.profile,
        plan_sha256,
        bicdb_version: plan.bicdb_version,
        source_commit,
        source_dirty,
        started_at_ms,
        updated_at_ms: started_at_ms,
        completed_at_ms: None,
        hardware: BTreeMap::new(),
        measurements: None,
        background_saturation: None,
        failure_trials: BTreeMap::new(),
        restore: None,
        expansion: None,
        artifacts: BTreeMap::new(),
    };
    save_cluster_certification_state(bundle_root, &state, fsync)?;
    Ok(state)
}

pub fn load_cluster_certification_state(
    bundle_root: impl AsRef<Path>,
) -> Result<ClusterCertificationState> {
    let bundle_root = bundle_root.as_ref();
    let plan_bytes = fs::read(bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN))?;
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&plan_bytes)?;
    plan.validate()?;
    let state = serde_json::from_slice::<ClusterCertificationState>(&fs::read(
        bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_STATE),
    )?)?;
    validate_cluster_certification_state(&state, &plan, &plan_bytes)?;
    Ok(state)
}

pub fn record_cluster_certification_observation(
    bundle_root: impl AsRef<Path>,
    observation: ClusterCertificationObservation,
    now_ms: u64,
    fsync: bool,
) -> Result<ClusterCertificationState> {
    let bundle_root = bundle_root.as_ref();
    let plan =
        load_cluster_certification_plan(bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN))?;
    let mut state = load_cluster_certification_state(bundle_root)?;
    ensure_collector_open(&state)?;
    validate_cluster_certification_observation(&plan, &state, &observation, now_ms)?;
    match observation {
        ClusterCertificationObservation::Hardware(evidence) => {
            if !plan.node_ids.contains(&evidence.node_id) {
                return Err(certification_error(format!(
                    "hardware observation names unplanned node {}",
                    evidence.node_id
                )));
            }
            state.hardware.insert(evidence.node_id.clone(), evidence);
        }
        ClusterCertificationObservation::Measurements(evidence) => {
            state.measurements = Some(evidence);
        }
        ClusterCertificationObservation::BackgroundSaturation(evidence) => {
            if evidence
                .nodes
                .iter()
                .any(|node| !plan.node_ids.contains(&node.node_id))
            {
                return Err(certification_error(
                    "background saturation observation names an unplanned node",
                ));
            }
            state.background_saturation = Some(evidence);
        }
        ClusterCertificationObservation::FailureTrial(evidence) => {
            if !plan
                .required_failure_points
                .contains(&evidence.failure_point)
                || !plan.node_ids.contains(&evidence.failed_node_id)
            {
                return Err(certification_error(
                    "failure observation names an unplanned phase or node",
                ));
            }
            state
                .failure_trials
                .insert(evidence.failure_point, evidence);
        }
        ClusterCertificationObservation::Restore(evidence) => {
            if !plan.node_ids.contains(&evidence.restored_node_id) {
                return Err(certification_error(
                    "restore observation names an unplanned node",
                ));
            }
            state.restore = Some(evidence);
        }
        ClusterCertificationObservation::Expansion(evidence) => {
            state.expansion = Some(evidence);
        }
    }
    state.updated_at_ms = now_ms.max(state.updated_at_ms);
    save_cluster_certification_state(bundle_root, &state, fsync)?;
    Ok(state)
}

fn validate_cluster_certification_observation(
    plan: &ClusterCertificationPlan,
    state: &ClusterCertificationState,
    observation: &ClusterCertificationObservation,
    now_ms: u64,
) -> Result<()> {
    if now_ms < state.started_at_ms {
        return Err(certification_error(
            "observation time precedes the certification run",
        ));
    }
    let artifacts = state
        .artifacts
        .values()
        .map(|artifact| (artifact.relative_path.clone(), artifact.kind))
        .collect::<BTreeMap<_, _>>();
    let mut failures = Vec::new();
    match observation {
        ClusterCertificationObservation::Hardware(evidence) => {
            if !plan.node_ids.contains(&evidence.node_id) {
                failures.push(format!(
                    "hardware observation names unplanned node {}",
                    evidence.node_id
                ));
            }
            verify_hardware_node(plan, evidence, &mut failures);
        }
        ClusterCertificationObservation::Measurements(evidence) => verify_measurements(
            plan,
            evidence,
            state.started_at_ms,
            now_ms,
            &artifacts,
            &mut failures,
        ),
        ClusterCertificationObservation::BackgroundSaturation(evidence) => {
            verify_background_saturation(
                plan,
                evidence,
                state.started_at_ms,
                now_ms,
                &artifacts,
                &mut failures,
            );
        }
        ClusterCertificationObservation::FailureTrial(evidence) => verify_failure_trials(
            plan,
            std::slice::from_ref(evidence),
            state.started_at_ms,
            now_ms,
            &artifacts,
            false,
            &mut failures,
        ),
        ClusterCertificationObservation::Restore(evidence) => {
            validate_restore_observation_before_checkpoint(
                plan,
                state,
                evidence,
                now_ms,
                &artifacts,
                &mut failures,
            );
        }
        ClusterCertificationObservation::Expansion(evidence) => {
            validate_expansion_observation_before_checkpoint(
                plan,
                state,
                evidence,
                now_ms,
                &artifacts,
                &mut failures,
            );
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(certification_error(format!(
            "observation rejected before checkpoint: {}",
            failures.join("; ")
        )))
    }
}

fn validate_restore_observation_before_checkpoint(
    plan: &ClusterCertificationPlan,
    state: &ClusterCertificationState,
    restore: &ClusterRestoreEvidence,
    now_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    failures: &mut Vec<String>,
) {
    let Some(measurements) = state.measurements.as_ref() else {
        failures.push("measurements must be checkpointed before restore evidence".to_string());
        return;
    };
    if !restore.passed || !restore.integrity_verified {
        failures.push("whole-node restore did not report a successful integrity pass".to_string());
    }
    if validate_run_id(&restore.backup_id).is_err()
        || validate_sha256(
            &restore.backup_manifest_sha256,
            "restore backup manifest SHA-256",
        )
        .is_err()
    {
        failures.push("whole-node restore has invalid backup provenance".to_string());
    }
    if restore.started_at_ms < state.started_at_ms
        || restore.completed_at_ms <= restore.started_at_ms
        || restore.completed_at_ms > now_ms
        || restore
            .completed_at_ms
            .saturating_sub(restore.started_at_ms)
            != restore.elapsed_ms
    {
        failures.push(
            "whole-node restore timestamps must be ordered, exact, and inside the report"
                .to_string(),
        );
    }
    if !plan.node_ids.contains(&restore.restored_node_id)
        || restore.source_node_incarnation == 0
        || restore.restored_node_incarnation <= restore.source_node_incarnation
    {
        failures.push(
            "whole-node restore must replace a planned node with a higher incarnation".to_string(),
        );
    }
    verify_artifact_references(
        &restore.artifact_paths,
        artifacts,
        Some(ClusterCertificationArtifactKind::Restore),
        "restore evidence",
        failures,
    );
    verify_exact_artifact_kinds(
        &restore.artifact_paths,
        artifacts,
        &[
            ClusterCertificationArtifactKind::TopologyBefore,
            ClusterCertificationArtifactKind::TopologyAfter,
            ClusterCertificationArtifactKind::ResourceSamples,
            ClusterCertificationArtifactKind::WorkloadLatency,
            ClusterCertificationArtifactKind::FullTextIndex,
            ClusterCertificationArtifactKind::Checksums,
        ],
        "restore evidence",
        failures,
    );
    if let Some(expansion) = state.expansion.as_ref() {
        verify_restore_and_expansion(
            plan,
            measurements,
            restore,
            expansion,
            state.started_at_ms,
            now_ms,
            artifacts,
            failures,
        );
    }
}

fn validate_expansion_observation_before_checkpoint(
    plan: &ClusterCertificationPlan,
    state: &ClusterCertificationState,
    expansion: &ClusterExpansionEvidence,
    now_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    failures: &mut Vec<String>,
) {
    let Some(measurements) = state.measurements.as_ref() else {
        failures.push("measurements must be checkpointed before expansion evidence".to_string());
        return;
    };
    if !expansion.passed {
        failures.push("cluster expansion did not report success".to_string());
    }
    if expansion.started_at_ms < state.started_at_ms
        || expansion.completed_at_ms <= expansion.started_at_ms
        || expansion.completed_at_ms > now_ms
        || expansion
            .completed_at_ms
            .saturating_sub(expansion.started_at_ms)
            != expansion.elapsed_ms
        || expansion.elapsed_ms < plan.gates.minimum_measurement_window_ms
    {
        failures.push(
            "cluster expansion timestamps must describe an exact five-minute-or-longer report window"
                .to_string(),
        );
    }
    if expansion.nodes_before != plan.gates.required_nodes
        || expansion.nodes_after <= expansion.nodes_before
        || expansion.topology_generation_before != plan.topology_generation
        || expansion.topology_generation_after <= expansion.topology_generation_before
        || validate_sha256(
            &expansion.topology_before_sha256,
            "pre-expansion topology SHA-256",
        )
        .is_err()
        || validate_sha256(
            &expansion.topology_after_sha256,
            "post-expansion topology SHA-256",
        )
        .is_err()
        || !expansion
            .topology_before_sha256
            .eq_ignore_ascii_case(&plan.topology_sha256)
        || expansion
            .topology_before_sha256
            .eq_ignore_ascii_case(&expansion.topology_after_sha256)
    {
        failures.push(
            "cluster expansion does not prove one newer, distinct topology generation".to_string(),
        );
    }
    verify_artifact_references(
        &expansion.artifact_paths,
        artifacts,
        Some(ClusterCertificationArtifactKind::RebalanceTimeline),
        "expansion evidence",
        failures,
    );
    verify_exact_artifact_kinds(
        &expansion.artifact_paths,
        artifacts,
        &[
            ClusterCertificationArtifactKind::Hardware,
            ClusterCertificationArtifactKind::EffectiveConfiguration,
            ClusterCertificationArtifactKind::TopologyBefore,
            ClusterCertificationArtifactKind::TopologyAfter,
            ClusterCertificationArtifactKind::ResourceSamples,
            ClusterCertificationArtifactKind::WorkloadLatency,
            ClusterCertificationArtifactKind::FullTextIndex,
            ClusterCertificationArtifactKind::Checksums,
        ],
        "expansion evidence",
        failures,
    );
    if let Some(restore) = state.restore.as_ref() {
        verify_restore_and_expansion(
            plan,
            measurements,
            restore,
            expansion,
            state.started_at_ms,
            now_ms,
            artifacts,
            failures,
        );
    }
}

pub fn register_cluster_certification_artifact(
    bundle_root: impl AsRef<Path>,
    kind: ClusterCertificationArtifactKind,
    relative_path: impl AsRef<Path>,
    now_ms: u64,
    fsync: bool,
) -> Result<ClusterArtifactEvidence> {
    let bundle_root = bundle_root.as_ref();
    let relative_path = relative_path.as_ref();
    if !safe_relative_path(relative_path) {
        return Err(certification_error(
            "artifact must be a safe path relative to the bundle",
        ));
    }
    let plan =
        load_cluster_certification_plan(bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN))?;
    if !plan.required_artifact_kinds.contains(&kind) {
        return Err(certification_error(
            "artifact kind is not required by the immutable plan",
        ));
    }
    let mut state = load_cluster_certification_state(bundle_root)?;
    ensure_collector_open(&state)?;
    let canonical_root = fs::canonicalize(bundle_root)?;
    let candidate = bundle_root.join(relative_path);
    if fs::symlink_metadata(&candidate).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(certification_error("artifact may not be a symlink"));
    }
    let canonical = fs::canonicalize(&candidate)?;
    if !canonical.starts_with(&canonical_root) {
        return Err(certification_error("artifact escapes the bundle root"));
    }
    let metadata = fs::metadata(&canonical)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(certification_error(
            "artifact must be a non-empty regular file",
        ));
    }
    let verified = verify_cluster_certification_raw_artifact(
        &canonical,
        &plan,
        &state.source_commit,
        state.source_dirty,
        kind,
        state.started_at_ms,
        now_ms,
    )?;
    if verified.size_bytes != metadata.len() {
        return Err(certification_error(
            "raw artifact size changed while it was being registered",
        ));
    }
    let evidence = ClusterArtifactEvidence {
        kind,
        relative_path: relative_path.to_path_buf(),
        size_bytes: verified.size_bytes,
        sha256: verified.sha256,
    };
    state
        .artifacts
        .insert(evidence.relative_path.clone(), evidence.clone());
    state.updated_at_ms = now_ms.max(state.updated_at_ms);
    save_cluster_certification_state(bundle_root, &state, fsync)?;
    Ok(evidence)
}

/// Capture an existing payload as one immutable, run-bound raw artifact and
/// durably register it with the open certification collector.
///
/// Payload bytes are copied exactly once through a fixed 1 MiB buffer into a
/// private file. The final header is then written into a reserved 64 KiB first
/// line, the file is synced, and a same-filesystem hard link publishes it
/// without ever replacing an existing artifact. If the process stops after
/// publication but before collector registration, repeating the same call
/// verifies and registers the already-published file.
#[allow(clippy::too_many_arguments)]
pub fn capture_cluster_certification_artifact(
    bundle_root: impl AsRef<Path>,
    kind: ClusterCertificationArtifactKind,
    payload_path: impl AsRef<Path>,
    relative_output_path: impl AsRef<Path>,
    payload_format: ClusterCertificationRawArtifactPayloadFormat,
    record_count: u64,
    started_at_ms: u64,
    completed_at_ms: u64,
    now_ms: u64,
    fsync: bool,
) -> Result<ClusterArtifactEvidence> {
    let bundle_root = bundle_root.as_ref();
    let payload_path = payload_path.as_ref();
    let relative_output_path = relative_output_path.as_ref();
    if !safe_relative_path(relative_output_path) {
        return Err(certification_error(
            "captured artifact output must be a safe path relative to the bundle",
        ));
    }
    if record_count == 0 {
        return Err(certification_error(
            "captured artifact record count must be nonzero",
        ));
    }

    let plan =
        load_cluster_certification_plan(bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN))?;
    if !plan.required_artifact_kinds.contains(&kind) {
        return Err(certification_error(
            "captured artifact kind is not required by the immutable plan",
        ));
    }
    let state = load_cluster_certification_state(bundle_root)?;
    ensure_collector_open(&state)?;
    if started_at_ms < state.started_at_ms
        || completed_at_ms < started_at_ms
        || completed_at_ms < state.started_at_ms
        || completed_at_ms > now_ms
    {
        return Err(certification_error(
            "captured artifact observation timestamps fall outside the open certification run",
        ));
    }

    let output_path =
        prepare_cluster_certification_artifact_output(bundle_root, relative_output_path, fsync)?;
    if fs::symlink_metadata(&output_path).is_ok() {
        return resume_cluster_certification_artifact_capture(
            bundle_root,
            &plan,
            &state,
            kind,
            relative_output_path,
            &output_path,
            payload_format,
            record_count,
            started_at_ms,
            completed_at_ms,
            now_ms,
            fsync,
        );
    }

    let payload_metadata = fs::symlink_metadata(payload_path).map_err(|error| {
        certification_error(format!(
            "cannot inspect artifact payload {}: {error}",
            payload_path.display()
        ))
    })?;
    if payload_metadata.file_type().is_symlink() || !payload_metadata.is_file() {
        return Err(certification_error(
            "artifact payload must be a non-symlink regular file",
        ));
    }
    if payload_metadata.len() == 0 {
        return Err(certification_error(
            "artifact payload must contain at least one byte",
        ));
    }
    let mut payload = open_cluster_certification_payload(payload_path)?;
    let (mut temporary, temporary_path) = create_cluster_certification_artifact_temp(&output_path)?;
    let mut temporary_guard = ClusterCertificationArtifactTempGuard {
        path: temporary_path.clone(),
        published: false,
    };

    let mut header_line = vec![b' '; MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES];
    *header_line
        .last_mut()
        .expect("certification header reservation is non-empty") = b'\n';
    temporary.write_all(&header_line)?;

    let mut payload_sha256 = Sha256::new();
    let mut payload_bytes = 0_u64;
    let mut buffer = vec![0_u8; CLUSTER_CERTIFICATION_ARTIFACT_COPY_BUFFER_BYTES];
    loop {
        let read = payload.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        payload_bytes = payload_bytes
            .checked_add(read as u64)
            .ok_or_else(|| certification_error("captured artifact payload size overflowed"))?;
        payload_sha256.update(&buffer[..read]);
        temporary.write_all(&buffer[..read])?;
    }
    if payload_bytes == 0 {
        return Err(certification_error(
            "artifact payload became empty while it was captured",
        ));
    }
    if payload_bytes != payload_metadata.len() {
        return Err(certification_error(
            "artifact payload size changed while it was captured",
        ));
    }
    let final_payload_metadata = payload.metadata()?;
    if final_payload_metadata.len() != payload_metadata.len()
        || final_payload_metadata.modified().ok() != payload_metadata.modified().ok()
    {
        return Err(certification_error(
            "artifact payload changed while it was captured",
        ));
    }

    let header = ClusterCertificationRawArtifactHeader {
        format_version: CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION,
        run_id: plan.run_id.clone(),
        profile: plan.profile,
        bicdb_version: plan.bicdb_version.clone(),
        source_commit: state.source_commit.clone(),
        source_dirty: state.source_dirty,
        kind,
        started_at_ms,
        completed_at_ms,
        producer_node_ids: plan.node_ids.clone(),
        payload_format,
        record_count,
        payload_bytes,
        payload_sha256: hex::encode(payload_sha256.finalize()),
    };
    let serialized_header = serde_json::to_vec(&header)?;
    if serialized_header.len() + 1 > MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES {
        return Err(certification_error(
            "captured artifact header exceeds the 64 KiB format bound",
        ));
    }
    header_line[..serialized_header.len()].copy_from_slice(&serialized_header);
    temporary.seek(SeekFrom::Start(0))?;
    temporary.write_all(&header_line)?;
    if fsync {
        temporary.sync_all()?;
    }
    drop(temporary);

    match fs::hard_link(&temporary_path, &output_path) {
        Ok(()) => {
            fs::remove_file(&temporary_path)?;
            temporary_guard.published = true;
            if fsync {
                sync_cluster_certification_artifact_parent(&output_path)?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            drop(temporary_guard);
            return resume_cluster_certification_artifact_capture(
                bundle_root,
                &plan,
                &state,
                kind,
                relative_output_path,
                &output_path,
                payload_format,
                record_count,
                started_at_ms,
                completed_at_ms,
                now_ms,
                fsync,
            );
        }
        Err(error) => return Err(error.into()),
    }

    register_cluster_certification_artifact(bundle_root, kind, relative_output_path, now_ms, fsync)
}

pub fn finalize_cluster_certification_bundle(
    bundle_root: impl AsRef<Path>,
    completed_at_ms: u64,
    fsync: bool,
) -> Result<ClusterCertificationVerification> {
    let bundle_root = bundle_root.as_ref();
    let plan_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
    let report_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_REPORT);
    let plan = load_cluster_certification_plan(&plan_path)?;
    let mut state = load_cluster_certification_state(bundle_root)?;
    if let Some(published_at_ms) = state.completed_at_ms {
        let verification =
            verify_cluster_certification_evidence(bundle_root, &plan_path, &report_path)?;
        if !verification.passed {
            return Ok(verification);
        }
        publish_cluster_certification_manifest(
            bundle_root,
            &plan_path,
            &report_path,
            published_at_ms,
            fsync,
        )?;
        return verify_cluster_certification_bundle(bundle_root, &plan_path, &report_path);
    }
    if completed_at_ms <= state.started_at_ms {
        return Err(certification_error(
            "completion time must be later than start time",
        ));
    }
    let measurements = state
        .measurements
        .clone()
        .ok_or_else(|| certification_error("measurements observation is missing"))?;
    let background_saturation = state
        .background_saturation
        .clone()
        .ok_or_else(|| certification_error("background saturation observation is missing"))?;
    let restore = state
        .restore
        .clone()
        .ok_or_else(|| certification_error("restore observation is missing"))?;
    let expansion = state
        .expansion
        .clone()
        .ok_or_else(|| certification_error("expansion observation is missing"))?;
    let failure_trials = plan
        .required_failure_points
        .iter()
        .map(|failure_point| {
            state
                .failure_trials
                .get(failure_point)
                .cloned()
                .ok_or_else(|| {
                    certification_error(format!("failure observation {failure_point:?} is missing"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let report = ClusterCertificationReport {
        format_version: CLUSTER_CERTIFICATION_FORMAT_VERSION,
        run_id: state.run_id.clone(),
        profile: state.profile,
        plan_sha256: state.plan_sha256.clone(),
        bicdb_version: state.bicdb_version.clone(),
        source_commit: state.source_commit.clone(),
        source_dirty: state.source_dirty,
        started_at_ms: state.started_at_ms,
        completed_at_ms,
        completed: true,
        hardware: state.hardware.values().cloned().collect(),
        measurements,
        background_saturation,
        failure_trials,
        restore,
        expansion,
        artifacts: state.artifacts.values().cloned().collect(),
    };
    crate::storage::write_atomic(&report_path, &serde_json::to_vec_pretty(&report)?, fsync)?;
    let verification =
        verify_cluster_certification_evidence(bundle_root, &plan_path, &report_path)?;
    if verification.passed {
        state.completed_at_ms = Some(completed_at_ms);
        state.updated_at_ms = completed_at_ms.max(state.updated_at_ms);
        save_cluster_certification_state(bundle_root, &state, fsync)?;
        publish_cluster_certification_manifest(
            bundle_root,
            &plan_path,
            &report_path,
            completed_at_ms,
            fsync,
        )?;
        return verify_cluster_certification_bundle(bundle_root, &plan_path, &report_path);
    }
    Ok(verification)
}

fn publish_cluster_certification_manifest(
    bundle_root: &Path,
    plan_path: &Path,
    report_path: &Path,
    published_at_ms: u64,
    fsync: bool,
) -> Result<ClusterCertificationPublicationManifest> {
    let plan_bytes = fs::read(plan_path)?;
    let report_bytes = fs::read(report_path)?;
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&plan_bytes)?;
    let report = serde_json::from_slice::<ClusterCertificationReport>(&report_bytes)?;
    if report.completed_at_ms != published_at_ms
        || report.run_id != plan.run_id
        || report.profile != plan.profile
        || report.bicdb_version != plan.bicdb_version
        || report.plan_sha256 != sha256_bytes(&plan_bytes)
    {
        return Err(certification_error(
            "cannot publish a manifest for mismatched plan, report, or completion time",
        ));
    }
    let mut artifacts = report.artifacts.clone();
    artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let artifact_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
        total.checked_add(artifact.size_bytes).ok_or_else(|| {
            certification_error("publication artifact byte total exceeds the supported range")
        })
    })?;
    let mut manifest = ClusterCertificationPublicationManifest {
        format_version: CLUSTER_CERTIFICATION_PUBLICATION_FORMAT_VERSION,
        certification_format_version: CLUSTER_CERTIFICATION_FORMAT_VERSION,
        run_id: report.run_id,
        profile: report.profile,
        bicdb_version: report.bicdb_version,
        source_commit: report.source_commit,
        source_dirty: report.source_dirty,
        published_at_ms,
        plan_relative_path: PathBuf::from(DEFAULT_CLUSTER_CERTIFICATION_PLAN),
        plan_size_bytes: plan_bytes.len() as u64,
        plan_sha256: sha256_bytes(&plan_bytes),
        report_relative_path: PathBuf::from(DEFAULT_CLUSTER_CERTIFICATION_REPORT),
        report_size_bytes: report_bytes.len() as u64,
        report_sha256: sha256_bytes(&report_bytes),
        artifact_count: artifacts.len() as u64,
        artifact_bytes,
        artifacts,
        bundle_sha256: String::new(),
    };
    manifest.bundle_sha256 = cluster_certification_manifest_bundle_sha256(&manifest)?;
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let manifest_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
    match fs::read(&manifest_path) {
        Ok(existing) if existing == manifest_bytes => return Ok(manifest),
        Ok(_) => {
            return Err(certification_error(
                "existing publication manifest differs from the immutable bundle",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    crate::storage::write_atomic(&manifest_path, &manifest_bytes, fsync)?;
    Ok(manifest)
}

fn cluster_certification_manifest_bundle_sha256(
    manifest: &ClusterCertificationPublicationManifest,
) -> Result<String> {
    let mut canonical = manifest.clone();
    canonical.bundle_sha256.clear();
    Ok(sha256_bytes(&serde_json::to_vec(&canonical)?))
}

fn save_cluster_certification_state(
    bundle_root: &Path,
    state: &ClusterCertificationState,
    fsync: bool,
) -> Result<()> {
    crate::storage::write_atomic(
        &bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_STATE),
        &serde_json::to_vec_pretty(state)?,
        fsync,
    )
}

fn validate_cluster_certification_state(
    state: &ClusterCertificationState,
    plan: &ClusterCertificationPlan,
    plan_bytes: &[u8],
) -> Result<()> {
    if state.format_version != CLUSTER_CERTIFICATION_STATE_FORMAT_VERSION {
        return Err(certification_error(format!(
            "unsupported collector state format {}",
            state.format_version
        )));
    }
    if state.run_id != plan.run_id
        || state.profile != plan.profile
        || state.plan_sha256 != sha256_bytes(plan_bytes)
        || state.bicdb_version != plan.bicdb_version
    {
        return Err(certification_error(
            "collector state does not match its immutable plan",
        ));
    }
    if !valid_source_commit(&state.source_commit)
        || state.started_at_ms == 0
        || state.updated_at_ms < state.started_at_ms
        || state
            .completed_at_ms
            .is_some_and(|completed| completed <= state.started_at_ms)
    {
        return Err(certification_error(
            "collector state has invalid source or timestamps",
        ));
    }
    if state
        .hardware
        .iter()
        .any(|(node_id, evidence)| node_id != &evidence.node_id || !plan.node_ids.contains(node_id))
        || state
            .failure_trials
            .iter()
            .any(|(failure_point, evidence)| {
                failure_point != &evidence.failure_point
                    || !plan.required_failure_points.contains(failure_point)
                    || !plan.node_ids.contains(&evidence.failed_node_id)
            })
        || state
            .background_saturation
            .as_ref()
            .is_some_and(|evidence| {
                evidence
                    .nodes
                    .iter()
                    .any(|node| !plan.node_ids.contains(&node.node_id))
            })
        || state
            .artifacts
            .iter()
            .any(|(path, evidence)| path != &evidence.relative_path || !safe_relative_path(path))
    {
        return Err(certification_error(
            "collector state contains evidence outside its plan",
        ));
    }
    Ok(())
}

fn ensure_collector_open(state: &ClusterCertificationState) -> Result<()> {
    if state.completed_at_ms.is_some() {
        return Err(certification_error(
            "certification bundle is finalized and immutable",
        ));
    }
    Ok(())
}

pub fn verify_cluster_certification_bundle(
    bundle_root: impl AsRef<Path>,
    plan_path: impl AsRef<Path>,
    report_path: impl AsRef<Path>,
) -> Result<ClusterCertificationVerification> {
    let bundle_root = bundle_root.as_ref();
    let plan_path = plan_path.as_ref();
    let report_path = report_path.as_ref();
    let mut verification =
        verify_cluster_certification_evidence(bundle_root, plan_path, report_path)?;
    if let Some(manifest_sha256) = verify_cluster_certification_publication(
        bundle_root,
        plan_path,
        report_path,
        &mut verification.failures,
    )? {
        verification.publication_manifest_sha256 = manifest_sha256;
    }
    verification.passed = verification.failures.is_empty();
    Ok(verification)
}

fn verify_cluster_certification_evidence(
    bundle_root: impl AsRef<Path>,
    plan_path: impl AsRef<Path>,
    report_path: impl AsRef<Path>,
) -> Result<ClusterCertificationVerification> {
    let bundle_root = bundle_root.as_ref();
    let plan_bytes = fs::read(plan_path)?;
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&plan_bytes)?;
    plan.validate()?;
    let report = load_cluster_certification_report(report_path)?;
    let mut failures = Vec::new();
    let gates = &plan.gates;

    if report.format_version != CLUSTER_CERTIFICATION_FORMAT_VERSION {
        failures.push(format!(
            "report format {} does not match {}",
            report.format_version, CLUSTER_CERTIFICATION_FORMAT_VERSION
        ));
    }
    if report.run_id != plan.run_id {
        failures.push("report run ID does not match plan".to_string());
    }
    if report.profile != plan.profile {
        failures.push("report profile does not match plan".to_string());
    }
    if report.plan_sha256 != sha256_bytes(&plan_bytes) {
        failures.push("report plan SHA-256 does not match the plan file".to_string());
    }
    if report.bicdb_version != plan.bicdb_version {
        failures.push("report BicDB version does not match plan".to_string());
    }
    if !valid_source_commit(&report.source_commit) {
        failures.push("source commit must be a 40-64 digit hexadecimal object ID".to_string());
    }
    if report.source_dirty {
        failures.push("source worktree was dirty".to_string());
    }
    if !report.completed || report.completed_at_ms <= report.started_at_ms {
        failures.push("certification run is incomplete or has invalid timestamps".to_string());
    }

    verify_hardware(&plan, &report.hardware, &mut failures);
    let artifacts = verify_artifacts(
        bundle_root,
        &plan,
        &report,
        &report.artifacts,
        &mut failures,
    )?;
    verify_measurements(
        &plan,
        &report.measurements,
        report.started_at_ms,
        report.completed_at_ms,
        &artifacts,
        &mut failures,
    );
    verify_background_saturation(
        &plan,
        &report.background_saturation,
        report.started_at_ms,
        report.completed_at_ms,
        &artifacts,
        &mut failures,
    );
    verify_failure_trials(
        &plan,
        &report.failure_trials,
        report.started_at_ms,
        report.completed_at_ms,
        &artifacts,
        true,
        &mut failures,
    );
    verify_restore_and_expansion(
        &plan,
        &report.measurements,
        &report.restore,
        &report.expansion,
        report.started_at_ms,
        report.completed_at_ms,
        &artifacts,
        &mut failures,
    );

    if report.measurements.recovery_time_ms > gates.maximum_recovery_time_ms {
        failures.push(format!(
            "overall recovery time {}ms exceeds {}ms",
            report.measurements.recovery_time_ms, gates.maximum_recovery_time_ms
        ));
    }
    let verified_artifact_bytes = report
        .artifacts
        .iter()
        .filter(|artifact| artifacts.contains_key(&artifact.relative_path))
        .try_fold(0_u64, |total, artifact| {
            total.checked_add(artifact.size_bytes)
        });
    let verified_artifact_bytes = match verified_artifact_bytes {
        Some(total) => total,
        None => {
            failures.push("verified artifact byte total exceeds the supported range".to_string());
            0
        }
    };

    Ok(ClusterCertificationVerification {
        format_version: CLUSTER_CERTIFICATION_FORMAT_VERSION,
        run_id: plan.run_id,
        profile: plan.profile,
        passed: failures.is_empty(),
        verified_artifacts: artifacts.len() as u64,
        verified_artifact_bytes,
        publication_manifest_sha256: String::new(),
        failures,
    })
}

fn verify_cluster_certification_publication(
    bundle_root: &Path,
    supplied_plan_path: &Path,
    supplied_report_path: &Path,
    failures: &mut Vec<String>,
) -> Result<Option<String>> {
    let failures_before = failures.len();
    let canonical_root = fs::canonicalize(bundle_root)?;
    let expected_plan_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
    let expected_report_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_REPORT);
    for (label, expected, supplied) in [
        ("plan", expected_plan_path.as_path(), supplied_plan_path),
        (
            "report",
            expected_report_path.as_path(),
            supplied_report_path,
        ),
    ] {
        match (
            fs::symlink_metadata(expected),
            fs::canonicalize(expected),
            fs::canonicalize(supplied),
        ) {
            (Ok(metadata), Ok(canonical_expected), Ok(canonical_supplied))
                if metadata.file_type().is_file()
                    && !metadata.file_type().is_symlink()
                    && canonical_expected.starts_with(&canonical_root)
                    && canonical_expected == canonical_supplied => {}
            _ => failures.push(format!(
                "publication {label} must be the regular in-bundle default file"
            )),
        }
    }

    let manifest_path = bundle_root.join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
    let manifest_metadata = match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {
            metadata
        }
        Ok(_) => {
            failures.push(
                "publication manifest must be a regular file and may not be a symlink".to_string(),
            );
            return Ok(None);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            failures.push("completed bundle has no publication manifest".to_string());
            return Ok(None);
        }
        Err(error) => {
            failures.push(format!("publication manifest metadata failed: {error}"));
            return Ok(None);
        }
    };
    if manifest_metadata.len() == 0 {
        failures.push("publication manifest is empty".to_string());
        return Ok(None);
    }
    let manifest_bytes = match fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            failures.push(format!("publication manifest cannot be read: {error}"));
            return Ok(None);
        }
    };
    let manifest =
        match serde_json::from_slice::<ClusterCertificationPublicationManifest>(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                failures.push(format!("publication manifest is invalid JSON: {error}"));
                return Ok(None);
            }
        };
    let plan_bytes = fs::read(&expected_plan_path)?;
    let report_bytes = fs::read(&expected_report_path)?;
    let plan = serde_json::from_slice::<ClusterCertificationPlan>(&plan_bytes)?;
    let report = serde_json::from_slice::<ClusterCertificationReport>(&report_bytes)?;

    if manifest.format_version != CLUSTER_CERTIFICATION_PUBLICATION_FORMAT_VERSION
        || manifest.certification_format_version != CLUSTER_CERTIFICATION_FORMAT_VERSION
        || manifest.run_id != report.run_id
        || manifest.profile != report.profile
        || manifest.bicdb_version != report.bicdb_version
        || manifest.source_commit != report.source_commit
        || manifest.source_dirty != report.source_dirty
        || manifest.published_at_ms != report.completed_at_ms
        || manifest.run_id != plan.run_id
        || manifest.profile != plan.profile
    {
        failures.push(
            "publication manifest identity, source, profile, or completion metadata does not match"
                .to_string(),
        );
    }
    if manifest.plan_relative_path != PathBuf::from(DEFAULT_CLUSTER_CERTIFICATION_PLAN)
        || manifest.plan_size_bytes != plan_bytes.len() as u64
        || validate_sha256(&manifest.plan_sha256, "publication plan SHA-256").is_err()
        || manifest.plan_sha256 != sha256_bytes(&plan_bytes)
    {
        failures.push("publication manifest plan binding does not match".to_string());
    }
    if manifest.report_relative_path != PathBuf::from(DEFAULT_CLUSTER_CERTIFICATION_REPORT)
        || manifest.report_size_bytes != report_bytes.len() as u64
        || validate_sha256(&manifest.report_sha256, "publication report SHA-256").is_err()
        || manifest.report_sha256 != sha256_bytes(&report_bytes)
    {
        failures.push("publication manifest report binding does not match".to_string());
    }
    let mut expected_artifacts = report.artifacts.clone();
    expected_artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let expected_artifact_bytes = expected_artifacts
        .iter()
        .try_fold(0_u64, |total, artifact| {
            total.checked_add(artifact.size_bytes)
        });
    if manifest.artifact_count != expected_artifacts.len() as u64
        || expected_artifact_bytes != Some(manifest.artifact_bytes)
        || manifest.artifacts != expected_artifacts
    {
        failures.push(
            "publication manifest artifact set, ordering, count, or byte total does not match"
                .to_string(),
        );
    }
    if validate_sha256(&manifest.bundle_sha256, "publication bundle SHA-256").is_err()
        || manifest.bundle_sha256 != cluster_certification_manifest_bundle_sha256(&manifest)?
    {
        failures.push("publication manifest bundle digest does not match".to_string());
    }

    if failures.len() == failures_before {
        Ok(Some(sha256_bytes(&manifest_bytes)))
    } else {
        Ok(None)
    }
}

fn verify_hardware(
    plan: &ClusterCertificationPlan,
    hardware: &[ClusterNodeHardwareEvidence],
    failures: &mut Vec<String>,
) {
    let planned = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let reported = hardware
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    if hardware.len() != plan.node_ids.len() || reported != planned {
        failures.push("hardware evidence must cover every planned node exactly once".to_string());
    }
    for node in hardware {
        verify_hardware_node(plan, node, failures);
    }
}

fn verify_hardware_node(
    plan: &ClusterCertificationPlan,
    node: &ClusterNodeHardwareEvidence,
    failures: &mut Vec<String>,
) {
    if node.hostname.trim().is_empty()
        || node.cpu_model.trim().is_empty()
        || node.storage_model.trim().is_empty()
        || node.filesystem.trim().is_empty()
        || node.mount_options.trim().is_empty()
        || node.operating_system.trim().is_empty()
        || node.kernel.trim().is_empty()
    {
        failures.push(format!(
            "hardware evidence for {} contains an empty identity field",
            node.node_id
        ));
    }
    if node.physical_cores == 0
        || node.memory_bytes < plan.gates.maximum_peak_rss_bytes_per_node
        || node.storage_bytes == 0
        || node.nic_bits_per_second == 0
    {
        failures.push(format!(
            "hardware evidence for {} has insufficient or zero capacity",
            node.node_id
        ));
    }
}

fn verify_artifacts(
    bundle_root: &Path,
    plan: &ClusterCertificationPlan,
    report: &ClusterCertificationReport,
    artifacts: &[ClusterArtifactEvidence],
    failures: &mut Vec<String>,
) -> Result<BTreeMap<PathBuf, ClusterCertificationArtifactKind>> {
    let canonical_root = fs::canonicalize(bundle_root)?;
    let mut seen_paths = BTreeSet::new();
    let mut verified_paths = BTreeMap::new();
    let mut verified_kinds = BTreeSet::new();
    for artifact in artifacts {
        if !safe_relative_path(&artifact.relative_path) {
            failures.push(format!(
                "artifact path {} is not a safe relative path",
                artifact.relative_path.display()
            ));
            continue;
        }
        if !seen_paths.insert(artifact.relative_path.clone()) {
            failures.push(format!(
                "artifact path {} is duplicated",
                artifact.relative_path.display()
            ));
            continue;
        }
        if validate_sha256(&artifact.sha256, "artifact SHA-256").is_err() {
            failures.push(format!(
                "artifact {} has an invalid SHA-256",
                artifact.relative_path.display()
            ));
            continue;
        }
        let candidate = bundle_root.join(&artifact.relative_path);
        if fs::symlink_metadata(&candidate).is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            failures.push(format!(
                "artifact {} may not be a symlink",
                artifact.relative_path.display()
            ));
            continue;
        }
        let canonical = match fs::canonicalize(&candidate) {
            Ok(path) => path,
            Err(error) => {
                failures.push(format!(
                    "artifact {} cannot be opened: {error}",
                    artifact.relative_path.display()
                ));
                continue;
            }
        };
        if !canonical.starts_with(&canonical_root) {
            failures.push(format!(
                "artifact {} escapes the bundle root",
                artifact.relative_path.display()
            ));
            continue;
        }
        let metadata = match fs::metadata(&canonical) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                failures.push(format!(
                    "artifact {} is not a regular file",
                    artifact.relative_path.display()
                ));
                continue;
            }
            Err(error) => {
                failures.push(format!(
                    "artifact {} metadata failed: {error}",
                    artifact.relative_path.display()
                ));
                continue;
            }
        };
        if artifact.size_bytes == 0 || metadata.len() != artifact.size_bytes {
            failures.push(format!(
                "artifact {} size {} does not match recorded {}",
                artifact.relative_path.display(),
                metadata.len(),
                artifact.size_bytes
            ));
            continue;
        }
        match verify_cluster_certification_raw_artifact(
            &canonical,
            plan,
            &report.source_commit,
            report.source_dirty,
            artifact.kind,
            report.started_at_ms,
            report.completed_at_ms,
        ) {
            Ok(verified)
                if verified.size_bytes == artifact.size_bytes
                    && verified.sha256 == artifact.sha256 =>
            {
                verified_paths.insert(artifact.relative_path.clone(), artifact.kind);
                verified_kinds.insert(artifact.kind);
            }
            Ok(_) => failures.push(format!(
                "artifact {} envelope size or checksum mismatch",
                artifact.relative_path.display()
            )),
            Err(error) => failures.push(format!(
                "artifact {} envelope verification failed: {error}",
                artifact.relative_path.display()
            )),
        }
    }
    for required in &plan.required_artifact_kinds {
        if !verified_kinds.contains(required) {
            failures.push(format!(
                "required artifact kind {required:?} has no verified artifact"
            ));
        }
    }
    Ok(verified_paths)
}

fn verify_measurements(
    plan: &ClusterCertificationPlan,
    measurements: &ClusterCertificationMeasurements,
    report_started_at_ms: u64,
    report_completed_at_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    failures: &mut Vec<String>,
) {
    let gates = &plan.gates;
    let baseline_duration = measurements
        .baseline_completed_at_ms
        .saturating_sub(measurements.baseline_started_at_ms);
    let rebalance_duration = measurements
        .rebalance_completed_at_ms
        .saturating_sub(measurements.rebalance_started_at_ms);
    if measurements.baseline_started_at_ms < report_started_at_ms
        || measurements.baseline_completed_at_ms <= measurements.baseline_started_at_ms
        || measurements.rebalance_started_at_ms < measurements.baseline_completed_at_ms
        || measurements.rebalance_completed_at_ms <= measurements.rebalance_started_at_ms
        || measurements.rebalance_completed_at_ms > report_completed_at_ms
        || baseline_duration < gates.minimum_measurement_window_ms
        || rebalance_duration < gates.minimum_measurement_window_ms
    {
        failures.push(format!(
            "performance baseline and rebalance windows must be ordered, inside the report, and at least {}ms each",
            gates.minimum_measurement_window_ms
        ));
    }
    if measurements.recovery_started_at_ms < measurements.rebalance_started_at_ms
        || measurements.recovery_completed_at_ms <= measurements.recovery_started_at_ms
        || measurements.recovery_completed_at_ms > measurements.rebalance_completed_at_ms
        || measurements
            .recovery_completed_at_ms
            .saturating_sub(measurements.recovery_started_at_ms)
            != measurements.recovery_time_ms
    {
        failures.push(
            "recovery timing must be positive, exact, and inside the rebalance window".to_string(),
        );
    }
    for (label, samples) in [
        (
            "foreground baseline",
            measurements.foreground_baseline_samples,
        ),
        (
            "foreground rebalance",
            measurements.foreground_rebalance_samples,
        ),
        ("hotspot baseline", measurements.hotspot_baseline_samples),
        ("hotspot rebalance", measurements.hotspot_rebalance_samples),
    ] {
        if samples < gates.minimum_latency_samples {
            failures.push(format!(
                "{label} has {samples} samples; at least {} are required",
                gates.minimum_latency_samples
            ));
        }
    }
    if measurements.logical_dataset_bytes < gates.minimum_logical_dataset_bytes {
        failures.push(format!(
            "logical dataset {} bytes is below {}",
            measurements.logical_dataset_bytes, gates.minimum_logical_dataset_bytes
        ));
    }
    if measurements.logical_dataset_bytes == 0
        || ratio_basis_points(
            measurements.total_physical_database_bytes,
            measurements.logical_dataset_bytes,
        ) > gates.maximum_disk_amplification_basis_points
    {
        failures.push("physical database amplification exceeds the profile gate".to_string());
    }
    let planned = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let rss_nodes = measurements
        .peak_rss_bytes_by_node
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if rss_nodes != planned {
        failures.push("peak RSS measurements must cover every node".to_string());
    }
    for (node_id, rss) in &measurements.peak_rss_bytes_by_node {
        if *rss == 0 || *rss > gates.maximum_peak_rss_bytes_per_node {
            failures.push(format!(
                "peak RSS for {node_id} is {rss}, outside the bounded-memory gate"
            ));
        }
    }
    verify_latency_regression(
        "foreground",
        measurements.foreground_baseline_p99_ms,
        measurements.foreground_rebalance_p99_ms,
        gates.maximum_foreground_p99_regression_basis_points,
        failures,
    );
    verify_latency_regression(
        "hotspot",
        measurements.hotspot_baseline_p99_ms,
        measurements.hotspot_rebalance_p99_ms,
        gates.maximum_hotspot_p99_regression_basis_points,
        failures,
    );
    if measurements.quorum_operations_attempted < gates.minimum_measurement_quorum_operations
        || measurements.quorum_operations_succeeded != measurements.quorum_operations_attempted
    {
        failures.push(format!(
            "majority-quorum workload must attempt at least {} operations and complete every one",
            gates.minimum_measurement_quorum_operations
        ));
    }
    if measurements.minority_writes_attempted < gates.minimum_measurement_minority_write_attempts
        || measurements.minority_writes_rejected != measurements.minority_writes_attempted
    {
        failures.push(format!(
            "minority-partition workload must attempt at least {} writes and reject every one",
            gates.minimum_measurement_minority_write_attempts
        ));
    }
    let rebalance_io = measurements
        .rebalance_source_read_bytes
        .saturating_add(measurements.rebalance_network_bytes)
        .saturating_add(measurements.rebalance_destination_write_bytes);
    if measurements.logical_rebalance_bytes == 0
        || ratio_basis_points(
            measurements.logical_rebalance_bytes,
            measurements.logical_dataset_bytes,
        ) < gates.minimum_rebalance_logical_basis_points
        || measurements.rebalance_source_read_bytes == 0
        || measurements.rebalance_network_bytes == 0
        || measurements.rebalance_destination_write_bytes == 0
        || ratio_basis_points(rebalance_io, measurements.logical_rebalance_bytes)
            > gates.maximum_rebalance_amplification_basis_points
    {
        failures.push("rebalance amplification exceeds the profile gate".to_string());
    }
    if measurements.maximum_unavailable_ranges != 0 {
        failures.push("one-node failure made at least one range unavailable".to_string());
    }
    let metrics = &measurements.final_metrics;
    if metrics.nodes_total != gates.required_nodes
        || metrics.nodes_live != gates.required_nodes
        || metrics.nodes_dead != 0
        || metrics.nodes_suspect != 0
        || metrics.under_replicated_ranges != 0
        || metrics.unavailable_ranges != 0
        || metrics.active_relocations != 0
        || metrics.failed_relocations != 0
    {
        failures.push("final cluster metrics are not fully healthy and converged".to_string());
    }
    if metrics.replica_skew > gates.maximum_replica_skew {
        failures.push("final replica-count skew exceeds the profile gate".to_string());
    }
    if metrics.leader_skew > gates.maximum_leader_skew {
        failures.push("final leader-count skew exceeds the profile gate".to_string());
    }
    let replica_nodes = measurements
        .final_replica_bytes_by_node
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if replica_nodes != planned {
        failures.push("final replica-byte measurements must cover every node".to_string());
    } else {
        let minimum = measurements
            .final_replica_bytes_by_node
            .values()
            .copied()
            .min()
            .unwrap_or(0);
        let maximum = measurements
            .final_replica_bytes_by_node
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        let average = measurements
            .final_replica_bytes_by_node
            .values()
            .copied()
            .sum::<u64>()
            / gates.required_nodes.max(1);
        if average == 0
            || ratio_basis_points(maximum.saturating_sub(minimum), average)
                > gates.maximum_replica_byte_skew_basis_points
        {
            failures.push("final replica-byte skew exceeds the profile gate".to_string());
        }
    }
    if measurements.full_text_index_source_bytes < gates.minimum_logical_dataset_bytes
        || measurements.full_text_index_documents == 0
        || measurements.full_text_index_bytes == 0
    {
        failures.push("production-sized FTS index measurement is absent".to_string());
    }
    for kind in [
        ClusterCertificationArtifactKind::ResourceSamples,
        ClusterCertificationArtifactKind::WorkloadLatency,
        ClusterCertificationArtifactKind::RebalanceTimeline,
    ] {
        if !measurements
            .artifact_paths
            .iter()
            .any(|path| artifacts.get(path) == Some(&kind))
        {
            failures.push(format!(
                "performance measurements do not reference a verified {kind:?} artifact"
            ));
        }
    }
    verify_artifact_references(
        &measurements.artifact_paths,
        artifacts,
        None,
        "performance measurements",
        failures,
    );
}

fn verify_background_saturation(
    plan: &ClusterCertificationPlan,
    evidence: &ClusterBackgroundSaturationEvidence,
    report_started_at_ms: u64,
    report_completed_at_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    failures: &mut Vec<String>,
) {
    let gates = &plan.gates;
    if evidence.started_at_ms < report_started_at_ms
        || evidence.completed_at_ms > report_completed_at_ms
        || evidence.started_at_ms == 0
        || evidence.completed_at_ms <= evidence.started_at_ms
        || evidence
            .completed_at_ms
            .saturating_sub(evidence.started_at_ms)
            < gates.minimum_background_saturation_duration_ms
    {
        failures.push(format!(
            "background saturation must be inside the report window and run continuously for at least {}ms",
            gates.minimum_background_saturation_duration_ms
        ));
    }

    verify_successful_operation_sample(
        "foreground read during background saturation",
        evidence.foreground_read_operations_attempted,
        evidence.foreground_read_operations_succeeded,
        gates.minimum_background_saturation_operations,
        failures,
    );
    verify_successful_operation_sample(
        "foreground write during background saturation",
        evidence.foreground_write_operations_attempted,
        evidence.foreground_write_operations_succeeded,
        gates.minimum_background_saturation_operations,
        failures,
    );
    verify_successful_operation_sample(
        "metadata quorum during background saturation",
        evidence.metadata_quorum_checks_attempted,
        evidence.metadata_quorum_checks_succeeded,
        gates.minimum_background_saturation_quorum_checks,
        failures,
    );
    verify_successful_operation_sample(
        "range quorum during background saturation",
        evidence.range_quorum_operations_attempted,
        evidence.range_quorum_operations_succeeded,
        gates.minimum_background_saturation_operations,
        failures,
    );
    verify_latency_regression(
        "background-saturated foreground read",
        evidence.foreground_read_baseline_p99_ms,
        evidence.foreground_read_saturated_p99_ms,
        gates.maximum_foreground_p99_regression_basis_points,
        failures,
    );
    verify_latency_regression(
        "background-saturated foreground write",
        evidence.foreground_write_baseline_p99_ms,
        evidence.foreground_write_saturated_p99_ms,
        gates.maximum_foreground_p99_regression_basis_points,
        failures,
    );
    if evidence.maximum_unavailable_ranges != 0 {
        failures.push("background saturation made at least one range unavailable".to_string());
    }

    let planned = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let observed = evidence
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    if evidence.nodes.len() != plan.node_ids.len() || observed != planned {
        failures.push(
            "background saturation evidence must cover every planned node exactly once".to_string(),
        );
    }

    for node in &evidence.nodes {
        let context = format!("background saturation node {}", node.node_id);
        if node.governor_config.validate().is_err() {
            failures.push(format!("{context} has an invalid governor configuration"));
        }
        if !node.rejected_lane.is_background() || node.rejected_demand.validate().is_err() {
            failures.push(format!(
                "{context} must identify a valid rejected background demand"
            ));
        }
        if node.before_at_ms < evidence.started_at_ms
            || node.saturated_at_ms <= node.before_at_ms
            || node.after_at_ms <= node.saturated_at_ms
            || node.after_at_ms > evidence.completed_at_ms
        {
            failures.push(format!(
                "{context} snapshot timestamps are outside or unordered within the saturation window"
            ));
        }
        let Some(before) = verify_governor_snapshot(
            &node.before,
            &format!("{context} before snapshot"),
            failures,
        ) else {
            continue;
        };
        let Some(saturated) = verify_governor_snapshot(
            &node.saturated,
            &format!("{context} saturated snapshot"),
            failures,
        ) else {
            continue;
        };
        let Some(after) =
            verify_governor_snapshot(&node.after, &format!("{context} after snapshot"), failures)
        else {
            continue;
        };

        for (label, snapshot, lanes) in [
            ("before", &node.before, &before),
            ("saturated", &node.saturated, &saturated),
            ("after", &node.after, &after),
        ] {
            verify_snapshot_with_config(
                snapshot,
                lanes,
                &node.governor_config,
                &format!("{context} {label} snapshot"),
                failures,
            );
        }

        for lane in ResourceLane::ALL {
            let before_lane = before[&lane];
            let saturated_lane = saturated[&lane];
            let after_lane = after[&lane];
            if saturated_lane.admitted < before_lane.admitted
                || saturated_lane.rejected < before_lane.rejected
                || after_lane.admitted < saturated_lane.admitted
                || after_lane.rejected < saturated_lane.rejected
            {
                failures.push(format!(
                    "{context} {lane:?} counters regress across snapshots"
                ));
            }
            if lane.is_background() {
                if before_lane.usage != ResourceUsage::default()
                    || after_lane.usage != ResourceUsage::default()
                {
                    failures.push(format!(
                        "{context} {lane:?} is not quiescent before and after the trial"
                    ));
                }
                if saturated_lane.usage.active == 0
                    || saturated_lane.admitted <= before_lane.admitted
                {
                    failures.push(format!(
                        "{context} {lane:?} was not active and newly admitted at saturation"
                    ));
                }
            }
        }
        let rejected_before = before[&node.rejected_lane].rejected;
        let rejected_at_saturation = saturated[&node.rejected_lane].rejected;
        let background_rejections = rejected_at_saturation.saturating_sub(rejected_before);
        if background_rejections < gates.minimum_background_rejections_per_node {
            failures.push(format!(
                "{context} recorded {background_rejections} rejections for {:?}; at least {} are required to prove saturation",
                node.rejected_lane,
                gates.minimum_background_rejections_per_node
            ));
        }
        verify_rejected_demand(
            &node.governor_config,
            &saturated,
            node.rejected_lane,
            node.rejected_demand,
            &context,
            failures,
        );
    }

    verify_artifact_references(
        &evidence.artifact_paths,
        artifacts,
        Some(ClusterCertificationArtifactKind::BackgroundSaturation),
        "background saturation evidence",
        failures,
    );
}

fn verify_successful_operation_sample(
    label: &str,
    attempted: u64,
    succeeded: u64,
    minimum: u64,
    failures: &mut Vec<String>,
) {
    if attempted < minimum || succeeded != attempted {
        failures.push(format!(
            "{label} must attempt at least {minimum} operations and complete every one"
        ));
    }
}

fn verify_governor_snapshot<'a>(
    snapshot: &'a ResourceGovernorSnapshot,
    context: &str,
    failures: &mut Vec<String>,
) -> Option<BTreeMap<ResourceLane, &'a ResourceLaneSnapshot>> {
    let expected = ResourceLane::ALL.into_iter().collect::<BTreeSet<_>>();
    let observed = snapshot
        .lanes
        .iter()
        .map(|lane| lane.lane)
        .collect::<BTreeSet<_>>();
    if snapshot.lanes.len() != ResourceLane::ALL.len() || observed != expected {
        failures.push(format!(
            "{context} must contain every fixed resource lane exactly once"
        ));
        return None;
    }
    let lanes = snapshot
        .lanes
        .iter()
        .map(|lane| (lane.lane, lane))
        .collect::<BTreeMap<_, _>>();
    for lane in lanes.values() {
        if lane.usage.active == 0 && lane.usage != ResourceUsage::default() {
            failures.push(format!(
                "{context} {:?} has resource usage without an active permit",
                lane.lane
            ));
        }
        if lane.usage.active > 0
            && (lane.usage.memory_bytes == 0
                || lane.usage.io_bytes == 0
                || lane.usage.cpu_slots == 0)
        {
            failures.push(format!(
                "{context} {:?} has an active permit with zero resource demand",
                lane.lane
            ));
        }
        if lane.admitted < lane.usage.active as u64 {
            failures.push(format!(
                "{context} {:?} has more active permits than admissions",
                lane.lane
            ));
        }
    }

    let total = checked_sum_resource_usage(lanes.values().map(|lane| &lane.usage));
    let noncritical = checked_sum_resource_usage(
        lanes
            .values()
            .filter(|lane| !lane.lane.is_critical())
            .map(|lane| &lane.usage),
    );
    let background = checked_sum_resource_usage(
        CERTIFICATION_BACKGROUND_LANES
            .iter()
            .map(|lane| &lanes[lane].usage),
    );
    if total != Some(snapshot.total)
        || noncritical != Some(snapshot.noncritical)
        || background != Some(snapshot.background)
    {
        failures.push(format!(
            "{context} aggregate resource usage does not match its lane samples"
        ));
        return None;
    }
    Some(lanes)
}

fn verify_snapshot_with_config(
    snapshot: &ResourceGovernorSnapshot,
    lanes: &BTreeMap<ResourceLane, &ResourceLaneSnapshot>,
    config: &ResourceGovernorConfig,
    context: &str,
    failures: &mut Vec<String>,
) {
    let noncritical_capacity = subtract_capacity(config.node, config.critical_reserve);
    if !usage_fits(snapshot.total, config.node)
        || !usage_fits(snapshot.noncritical, noncritical_capacity)
        || !usage_fits(snapshot.background, config.background)
    {
        failures.push(format!("{context} exceeds its configured node envelope"));
    }
    for lane in ResourceLane::ALL {
        let Some(limit) = config.lanes.get(&lane) else {
            failures.push(format!("{context} has no configured {lane:?} lane"));
            continue;
        };
        let sample = lanes[&lane];
        if !usage_fits(sample.usage, limit.capacity)
            || sample.usage.active > limit.max_active
            || sample.io_tokens > limit.io_burst_bytes
        {
            failures.push(format!(
                "{context} {lane:?} usage exceeds its configured lane envelope"
            ));
        }
    }
}

fn verify_rejected_demand(
    config: &ResourceGovernorConfig,
    saturated: &BTreeMap<ResourceLane, &ResourceLaneSnapshot>,
    lane: ResourceLane,
    demand: ResourceDemand,
    context: &str,
    failures: &mut Vec<String>,
) {
    let Some(limit) = config.lanes.get(&lane) else {
        return;
    };
    let demand_capacity = ResourceCapacity {
        memory_bytes: demand.memory_bytes,
        io_bytes: demand.io_bytes,
        cpu_slots: demand.cpu_slots,
    };
    if !capacity_fits(demand_capacity, limit.capacity)
        || demand.io_charge_bytes > limit.io_burst_bytes
    {
        failures.push(format!(
            "{context} rejected demand exceeds the lane hard bound and does not prove saturation"
        ));
        return;
    }
    let lane_sample = saturated[&lane];
    let lane_fits = usage_can_add(lane_sample.usage, demand, limit.capacity)
        && lane_sample.usage.active < limit.max_active
        && lane_sample.io_tokens >= demand.io_charge_bytes;
    let total_fits = usage_can_add(saturated_usage(saturated), demand, config.node);
    let noncritical_fits = lane.is_critical()
        || usage_can_add(
            noncritical_usage(saturated),
            demand,
            subtract_capacity(config.node, config.critical_reserve),
        );
    let background_fits = !lane.is_background()
        || usage_can_add(background_usage(saturated), demand, config.background);
    if lane_fits && total_fits && noncritical_fits && background_fits {
        failures.push(format!(
            "{context} rejected demand still fits every captured resource bound"
        ));
    }
}

fn saturated_usage(lanes: &BTreeMap<ResourceLane, &ResourceLaneSnapshot>) -> ResourceUsage {
    checked_sum_resource_usage(lanes.values().map(|lane| &lane.usage)).unwrap_or_default()
}

fn noncritical_usage(lanes: &BTreeMap<ResourceLane, &ResourceLaneSnapshot>) -> ResourceUsage {
    checked_sum_resource_usage(
        lanes
            .values()
            .filter(|lane| !lane.lane.is_critical())
            .map(|lane| &lane.usage),
    )
    .unwrap_or_default()
}

fn background_usage(lanes: &BTreeMap<ResourceLane, &ResourceLaneSnapshot>) -> ResourceUsage {
    checked_sum_resource_usage(
        CERTIFICATION_BACKGROUND_LANES
            .iter()
            .map(|lane| &lanes[lane].usage),
    )
    .unwrap_or_default()
}

fn usage_can_add(usage: ResourceUsage, demand: ResourceDemand, capacity: ResourceCapacity) -> bool {
    usage
        .memory_bytes
        .checked_add(demand.memory_bytes)
        .is_some_and(|value| value <= capacity.memory_bytes)
        && usage
            .io_bytes
            .checked_add(demand.io_bytes)
            .is_some_and(|value| value <= capacity.io_bytes)
        && usage
            .cpu_slots
            .checked_add(demand.cpu_slots)
            .is_some_and(|value| value <= capacity.cpu_slots)
}

fn usage_fits(usage: ResourceUsage, capacity: ResourceCapacity) -> bool {
    usage.memory_bytes <= capacity.memory_bytes
        && usage.io_bytes <= capacity.io_bytes
        && usage.cpu_slots <= capacity.cpu_slots
}

fn capacity_fits(inner: ResourceCapacity, outer: ResourceCapacity) -> bool {
    inner.memory_bytes <= outer.memory_bytes
        && inner.io_bytes <= outer.io_bytes
        && inner.cpu_slots <= outer.cpu_slots
}

fn subtract_capacity(total: ResourceCapacity, reserved: ResourceCapacity) -> ResourceCapacity {
    ResourceCapacity {
        memory_bytes: total.memory_bytes.saturating_sub(reserved.memory_bytes),
        io_bytes: total.io_bytes.saturating_sub(reserved.io_bytes),
        cpu_slots: total.cpu_slots.saturating_sub(reserved.cpu_slots),
    }
}

fn checked_sum_resource_usage<'a>(
    usages: impl IntoIterator<Item = &'a ResourceUsage>,
) -> Option<ResourceUsage> {
    let mut total = ResourceUsage::default();
    for usage in usages {
        total.memory_bytes = total.memory_bytes.checked_add(usage.memory_bytes)?;
        total.io_bytes = total.io_bytes.checked_add(usage.io_bytes)?;
        total.cpu_slots = total.cpu_slots.checked_add(usage.cpu_slots)?;
        total.active = total.active.checked_add(usage.active)?;
    }
    Some(total)
}

fn verify_latency_regression(
    label: &str,
    baseline_p99_ms: f64,
    impaired_p99_ms: f64,
    maximum_regression_basis_points: u64,
    failures: &mut Vec<String>,
) {
    if !baseline_p99_ms.is_finite()
        || baseline_p99_ms <= 0.0
        || !impaired_p99_ms.is_finite()
        || impaired_p99_ms <= 0.0
    {
        failures.push(format!(
            "{label} p99 measurements must be finite and positive"
        ));
        return;
    }
    let regression = ((impaired_p99_ms / baseline_p99_ms) - 1.0).max(0.0) * 10_000.0;
    if regression > maximum_regression_basis_points as f64 {
        failures.push(format!(
            "{label} p99 regression {regression:.0}bp exceeds {maximum_regression_basis_points}bp"
        ));
    }
}

fn verify_failure_trials(
    plan: &ClusterCertificationPlan,
    trials: &[ClusterFailureTrialEvidence],
    report_started_at_ms: u64,
    report_completed_at_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    require_every_failure_point: bool,
    failures: &mut Vec<String>,
) {
    let mut observed = BTreeSet::new();
    for trial in trials {
        if !observed.insert(trial.failure_point) {
            failures.push(format!(
                "failure trial {:?} appears more than once",
                trial.failure_point
            ));
        }
        if !plan.node_ids.contains(&trial.failed_node_id) {
            failures.push(format!(
                "failure trial {:?} names an unknown node",
                trial.failure_point
            ));
        }
        if trial.recovered_node_id != trial.failed_node_id
            || trial.failed_node_incarnation == 0
            || match trial.recovery_mode {
                ClusterNodeRecoveryMode::RestartSameIncarnation => {
                    trial.recovered_node_incarnation != trial.failed_node_incarnation
                }
                ClusterNodeRecoveryMode::RejoinHigherIncarnation => {
                    trial.recovered_node_incarnation <= trial.failed_node_incarnation
                }
            }
        {
            failures.push(format!(
                "failure trial {:?} has an invalid recovery identity or incarnation",
                trial.failure_point
            ));
        }
        if validate_run_id(&trial.maintenance_operation_id).is_err()
            || validate_sha256(
                &trial.maintenance_checkpoint_before_sha256,
                "pre-failure maintenance checkpoint SHA-256",
            )
            .is_err()
            || validate_sha256(
                &trial.maintenance_checkpoint_after_sha256,
                "post-recovery maintenance checkpoint SHA-256",
            )
            .is_err()
            || trial
                .maintenance_checkpoint_before_sha256
                .eq_ignore_ascii_case(&trial.maintenance_checkpoint_after_sha256)
        {
            failures.push(format!(
                "failure trial {:?} lacks distinct valid maintenance checkpoints",
                trial.failure_point
            ));
        }
        if trial.started_at_ms < report_started_at_ms
            || trial.started_at_ms >= trial.failure_injected_at_ms
            || trial.node_loss_detected_at_ms < trial.failure_injected_at_ms
            || trial.old_identity_fenced_at_ms < trial.node_loss_detected_at_ms
            || trial.repair_started_at_ms < trial.old_identity_fenced_at_ms
            || trial.policy_converged_at_ms <= trial.repair_started_at_ms
            || trial.recovered_at_ms < trial.policy_converged_at_ms
            || trial.recovered_at_ms > report_completed_at_ms
        {
            failures.push(format!(
                "failure trial {:?} has timestamps outside the report or destructive recovery order",
                trial.failure_point
            ));
        }
        if !trial.passed
            || trial.data_loss_detected
            || trial.stale_epoch_write_accepted
            || trial.maximum_unavailable_ranges != 0
            || trial.recovered_at_ms <= trial.started_at_ms
            || trial.recovered_at_ms.saturating_sub(trial.started_at_ms)
                > plan.gates.maximum_recovery_time_ms
        {
            failures.push(format!(
                "failure trial {:?} did not satisfy recovery invariants",
                trial.failure_point
            ));
        }
        if trial.affected_ranges == 0
            || trial.repaired_ranges < trial.affected_ranges
            || trial.digest_verified_ranges < trial.affected_ranges
            || trial.maximum_under_replicated_ranges == 0
            || trial.final_under_replicated_ranges != 0
        {
            failures.push(format!(
                "failure trial {:?} did not prove affected-range repair and final policy convergence",
                trial.failure_point
            ));
        }
        if trial.verification_commit_sequence == 0
            || trial.verified_records == 0
            || trial.verified_logical_bytes
                < plan.gates.minimum_logical_dataset_bytes / plan.gates.required_nodes.max(1)
            || validate_sha256(
                &trial.pre_failure_digest_sha256,
                "pre-failure logical digest SHA-256",
            )
            .is_err()
            || validate_sha256(
                &trial.post_recovery_digest_sha256,
                "post-recovery logical digest SHA-256",
            )
            .is_err()
            || !trial
                .pre_failure_digest_sha256
                .eq_ignore_ascii_case(&trial.post_recovery_digest_sha256)
        {
            failures.push(format!(
                "failure trial {:?} did not prove fixed-watermark logical checksum preservation",
                trial.failure_point
            ));
        }
        verify_successful_operation_sample(
            &format!("failure trial {:?} quorum workload", trial.failure_point),
            trial.quorum_operations_attempted,
            trial.quorum_operations_succeeded,
            plan.gates.minimum_failure_quorum_operations,
            failures,
        );
        if trial.old_identity_writes_attempted == 0
            || trial.old_identity_writes_rejected != trial.old_identity_writes_attempted
        {
            failures.push(format!(
                "failure trial {:?} must reject every old-identity write attempt",
                trial.failure_point
            ));
        }
        verify_artifact_references(
            &trial.artifact_paths,
            artifacts,
            Some(ClusterCertificationArtifactKind::FailureTimeline),
            &format!("failure trial {:?}", trial.failure_point),
            failures,
        );
        if !trial
            .artifact_paths
            .iter()
            .any(|path| artifacts.get(path) == Some(&ClusterCertificationArtifactKind::Checksums))
        {
            failures.push(format!(
                "failure trial {:?} does not reference a verified Checksums artifact",
                trial.failure_point
            ));
        }
    }
    if require_every_failure_point {
        for required in &plan.required_failure_points {
            if !observed.contains(required) {
                failures.push(format!("required failure trial {required:?} is absent"));
            }
        }
    }
}

fn verify_restore_and_expansion(
    plan: &ClusterCertificationPlan,
    measurements: &ClusterCertificationMeasurements,
    restore: &ClusterRestoreEvidence,
    expansion: &ClusterExpansionEvidence,
    report_started_at_ms: u64,
    report_completed_at_ms: u64,
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    failures: &mut Vec<String>,
) {
    let gates = &plan.gates;
    let node_logical_share = ceiling_div(
        measurements.logical_dataset_bytes,
        gates.required_nodes.max(1),
    );
    let node_fts_document_share = ceiling_div(
        measurements.full_text_index_documents,
        gates.required_nodes.max(1),
    );
    let node_fts_index_share = ceiling_div(
        measurements.full_text_index_bytes,
        gates.required_nodes.max(1),
    );
    if !restore.passed || !restore.integrity_verified {
        failures.push("whole-node restore did not report a successful integrity pass".to_string());
    }
    if validate_run_id(&restore.backup_id).is_err()
        || validate_sha256(
            &restore.backup_manifest_sha256,
            "restore backup manifest SHA-256",
        )
        .is_err()
    {
        failures.push("whole-node restore has invalid backup provenance".to_string());
    }
    if restore.started_at_ms < report_started_at_ms
        || restore.completed_at_ms <= restore.started_at_ms
        || restore.completed_at_ms > report_completed_at_ms
        || restore
            .completed_at_ms
            .saturating_sub(restore.started_at_ms)
            != restore.elapsed_ms
    {
        failures.push(
            "whole-node restore timestamps must be ordered, exact, and inside the report"
                .to_string(),
        );
    }
    if !plan.node_ids.contains(&restore.restored_node_id)
        || restore.source_node_incarnation == 0
        || restore.restored_node_incarnation <= restore.source_node_incarnation
    {
        failures.push(
            "whole-node restore must replace a planned node with a higher incarnation".to_string(),
        );
    }
    if restore.verification_commit_sequence == 0
        || restore.source_records == 0
        || restore.source_records != restore.restored_records
        || restore.source_logical_bytes < node_logical_share
        || restore.source_logical_bytes != restore.restored_logical_bytes
        || validate_sha256(&restore.source_digest_sha256, "restore source digest").is_err()
        || validate_sha256(&restore.restored_digest_sha256, "restore result digest").is_err()
        || !restore
            .source_digest_sha256
            .eq_ignore_ascii_case(&restore.restored_digest_sha256)
    {
        failures.push(
            "whole-node restore does not prove equal source and restored data at one commit watermark"
                .to_string(),
        );
    }
    if restore.peak_rss_bytes == 0 || restore.peak_rss_bytes > gates.maximum_peak_rss_bytes_per_node
    {
        failures.push("whole-node restore exceeds the bounded-memory gate".to_string());
    }
    if restore.source_full_text_documents < node_fts_document_share
        || restore.source_full_text_documents != restore.restored_full_text_documents
        || restore.source_full_text_index_bytes < node_fts_index_share
        || restore.source_full_text_index_bytes != restore.restored_full_text_index_bytes
        || validate_sha256(
            &restore.source_full_text_digest_sha256,
            "restore source FTS digest",
        )
        .is_err()
        || validate_sha256(
            &restore.restored_full_text_digest_sha256,
            "restore result FTS digest",
        )
        .is_err()
        || !restore
            .source_full_text_digest_sha256
            .eq_ignore_ascii_case(&restore.restored_full_text_digest_sha256)
    {
        failures.push(
            "whole-node restore does not prove an equal production-sized FTS index".to_string(),
        );
    }
    verify_successful_operation_sample(
        "whole-node restore FTS probes",
        restore.full_text_queries_attempted,
        restore.full_text_queries_succeeded,
        gates.minimum_fts_query_probes,
        failures,
    );
    verify_successful_operation_sample(
        "whole-node restore quorum workload",
        restore.quorum_operations_attempted,
        restore.quorum_operations_succeeded,
        gates.minimum_restore_quorum_operations,
        failures,
    );
    if restore.old_identity_writes_attempted < gates.minimum_identity_fence_attempts
        || restore.old_identity_writes_rejected != restore.old_identity_writes_attempted
    {
        failures.push(format!(
            "whole-node restore must reject at least {} old-identity writes",
            gates.minimum_identity_fence_attempts
        ));
    }
    if restore.maximum_unavailable_ranges != 0 {
        failures.push("whole-node restore made at least one range unavailable".to_string());
    }
    verify_artifact_references(
        &restore.artifact_paths,
        artifacts,
        Some(ClusterCertificationArtifactKind::Restore),
        "restore evidence",
        failures,
    );
    verify_exact_artifact_kinds(
        &restore.artifact_paths,
        artifacts,
        &[
            ClusterCertificationArtifactKind::TopologyBefore,
            ClusterCertificationArtifactKind::TopologyAfter,
            ClusterCertificationArtifactKind::ResourceSamples,
            ClusterCertificationArtifactKind::WorkloadLatency,
            ClusterCertificationArtifactKind::FullTextIndex,
            ClusterCertificationArtifactKind::Checksums,
        ],
        "restore evidence",
        failures,
    );

    if !expansion.passed {
        failures.push("cluster expansion did not report success".to_string());
    }
    if expansion.started_at_ms < report_started_at_ms
        || expansion.completed_at_ms <= expansion.started_at_ms
        || expansion.completed_at_ms > report_completed_at_ms
        || expansion
            .completed_at_ms
            .saturating_sub(expansion.started_at_ms)
            != expansion.elapsed_ms
        || expansion.elapsed_ms < gates.minimum_measurement_window_ms
    {
        failures.push(
            "cluster expansion timestamps must describe an exact five-minute-or-longer report window"
                .to_string(),
        );
    }
    if expansion.nodes_before != gates.required_nodes
        || expansion.nodes_after <= expansion.nodes_before
        || expansion.topology_generation_before != plan.topology_generation
        || expansion.topology_generation_after <= expansion.topology_generation_before
        || validate_sha256(
            &expansion.topology_before_sha256,
            "pre-expansion topology SHA-256",
        )
        .is_err()
        || validate_sha256(
            &expansion.topology_after_sha256,
            "post-expansion topology SHA-256",
        )
        .is_err()
        || !expansion
            .topology_before_sha256
            .eq_ignore_ascii_case(&plan.topology_sha256)
        || expansion
            .topology_before_sha256
            .eq_ignore_ascii_case(&expansion.topology_after_sha256)
    {
        failures.push(
            "cluster expansion does not prove one newer, distinct topology generation".to_string(),
        );
    }
    let planned_nodes = plan.node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let added_nodes = expansion
        .added_node_hardware
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    let required_added_nodes = expansion.nodes_after.saturating_sub(expansion.nodes_before);
    if expansion.added_node_hardware.len() as u64 != required_added_nodes
        || added_nodes.len() != expansion.added_node_hardware.len()
        || !planned_nodes.is_disjoint(&added_nodes)
    {
        failures.push(
            "cluster expansion hardware must identify every newly added node exactly once"
                .to_string(),
        );
    }
    let minimum_added_storage = ceiling_div(
        measurements.total_physical_database_bytes,
        expansion.nodes_after.max(1),
    );
    for node in &expansion.added_node_hardware {
        if node.hostname.trim().is_empty()
            || node.cpu_model.trim().is_empty()
            || node.storage_model.trim().is_empty()
            || node.filesystem.trim().is_empty()
            || node.mount_options.trim().is_empty()
            || node.operating_system.trim().is_empty()
            || node.kernel.trim().is_empty()
            || node.physical_cores == 0
            || node.memory_bytes < gates.maximum_peak_rss_bytes_per_node
            || node.storage_bytes < minimum_added_storage
            || node.nic_bits_per_second == 0
        {
            failures.push(format!(
                "new expansion node {} has incomplete or insufficient hardware evidence",
                node.node_id
            ));
        }
    }
    let all_expansion_nodes = planned_nodes
        .union(&added_nodes)
        .cloned()
        .collect::<BTreeSet<_>>();
    if all_expansion_nodes.len() as u64 != expansion.nodes_after
        || expansion
            .peak_rss_bytes_by_node
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != all_expansion_nodes
        || expansion
            .peak_rss_bytes_by_node
            .values()
            .any(|rss| *rss == 0 || *rss > gates.maximum_peak_rss_bytes_per_node)
    {
        failures.push(
            "cluster expansion must prove bounded peak RSS for every old and new node".to_string(),
        );
    }
    let expansion_io = expansion
        .source_read_bytes
        .saturating_add(expansion.network_bytes)
        .saturating_add(expansion.destination_write_bytes);
    if expansion.moved_logical_bytes
        < ceiling_div(
            measurements.logical_dataset_bytes,
            expansion.nodes_after.max(1),
        )
        || expansion.source_read_bytes == 0
        || expansion.network_bytes == 0
        || expansion.destination_write_bytes == 0
        || ratio_basis_points(expansion_io, expansion.moved_logical_bytes)
            > gates.maximum_rebalance_amplification_basis_points
    {
        failures.push("cluster expansion relocation I/O exceeds the profile gate".to_string());
    }
    for (label, samples) in [
        (
            "expansion foreground baseline",
            expansion.foreground_baseline_samples,
        ),
        (
            "expansion foreground active",
            expansion.foreground_expansion_samples,
        ),
        (
            "expansion hotspot baseline",
            expansion.hotspot_baseline_samples,
        ),
        (
            "expansion hotspot active",
            expansion.hotspot_expansion_samples,
        ),
    ] {
        if samples < gates.minimum_latency_samples {
            failures.push(format!(
                "{label} has {samples} samples; at least {} are required",
                gates.minimum_latency_samples
            ));
        }
    }
    verify_latency_regression(
        "cluster expansion foreground",
        expansion.foreground_baseline_p99_ms,
        expansion.foreground_expansion_p99_ms,
        gates.maximum_foreground_p99_regression_basis_points,
        failures,
    );
    verify_latency_regression(
        "cluster expansion hotspot",
        expansion.hotspot_baseline_p99_ms,
        expansion.hotspot_expansion_p99_ms,
        gates.maximum_hotspot_p99_regression_basis_points,
        failures,
    );
    verify_successful_operation_sample(
        "cluster expansion quorum workload",
        expansion.quorum_operations_attempted,
        expansion.quorum_operations_succeeded,
        gates.minimum_expansion_quorum_operations,
        failures,
    );
    if expansion.verification_commit_sequence == 0
        || expansion.verified_records == 0
        || expansion.verified_logical_bytes < measurements.logical_dataset_bytes
        || validate_sha256(
            &expansion.pre_expansion_digest_sha256,
            "pre-expansion corpus digest",
        )
        .is_err()
        || validate_sha256(
            &expansion.post_expansion_digest_sha256,
            "post-expansion corpus digest",
        )
        .is_err()
        || !expansion
            .pre_expansion_digest_sha256
            .eq_ignore_ascii_case(&expansion.post_expansion_digest_sha256)
    {
        failures.push(
            "cluster expansion does not prove full-corpus integrity at one commit watermark"
                .to_string(),
        );
    }
    if expansion.full_text_source_bytes_before < gates.minimum_logical_dataset_bytes
        || expansion.full_text_source_bytes_before != expansion.full_text_source_bytes_after
        || expansion.full_text_documents_before < measurements.full_text_index_documents
        || expansion.full_text_documents_before != expansion.full_text_documents_after
        || expansion.full_text_index_bytes_before < measurements.full_text_index_bytes
        || expansion.full_text_index_bytes_before != expansion.full_text_index_bytes_after
        || validate_sha256(
            &expansion.full_text_digest_sha256_before,
            "pre-expansion FTS digest",
        )
        .is_err()
        || validate_sha256(
            &expansion.full_text_digest_sha256_after,
            "post-expansion FTS digest",
        )
        .is_err()
        || !expansion
            .full_text_digest_sha256_before
            .eq_ignore_ascii_case(&expansion.full_text_digest_sha256_after)
    {
        failures.push(
            "cluster expansion does not prove an equal production-sized FTS corpus".to_string(),
        );
    }
    verify_successful_operation_sample(
        "cluster expansion FTS probes",
        expansion.full_text_queries_attempted,
        expansion.full_text_queries_succeeded,
        gates.minimum_fts_query_probes,
        failures,
    );
    if expansion.final_replica_skew > gates.maximum_replica_skew
        || expansion.final_leader_skew > gates.maximum_leader_skew
        || expansion.final_active_relocations != 0
        || expansion.final_failed_relocations != 0
        || expansion.final_under_replicated_ranges != 0
        || expansion.maximum_unavailable_ranges != 0
        || expansion.final_unavailable_ranges != 0
    {
        failures.push("cluster expansion did not finish healthy and converged".to_string());
    }
    if expansion
        .final_replica_bytes_by_node
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != all_expansion_nodes
    {
        failures.push(
            "cluster expansion replica-byte evidence must cover every old and new node".to_string(),
        );
    } else {
        let minimum = expansion
            .final_replica_bytes_by_node
            .values()
            .copied()
            .min()
            .unwrap_or(0);
        let maximum = expansion
            .final_replica_bytes_by_node
            .values()
            .copied()
            .max()
            .unwrap_or(0);
        let average = expansion
            .final_replica_bytes_by_node
            .values()
            .copied()
            .sum::<u64>()
            / expansion.nodes_after.max(1);
        if average == 0
            || ratio_basis_points(maximum.saturating_sub(minimum), average)
                > gates.maximum_replica_byte_skew_basis_points
        {
            failures.push(
                "cluster expansion final replica-byte skew exceeds the profile gate".to_string(),
            );
        }
    }
    verify_artifact_references(
        &expansion.artifact_paths,
        artifacts,
        Some(ClusterCertificationArtifactKind::RebalanceTimeline),
        "expansion evidence",
        failures,
    );
    verify_exact_artifact_kinds(
        &expansion.artifact_paths,
        artifacts,
        &[
            ClusterCertificationArtifactKind::Hardware,
            ClusterCertificationArtifactKind::EffectiveConfiguration,
            ClusterCertificationArtifactKind::TopologyBefore,
            ClusterCertificationArtifactKind::TopologyAfter,
            ClusterCertificationArtifactKind::ResourceSamples,
            ClusterCertificationArtifactKind::WorkloadLatency,
            ClusterCertificationArtifactKind::FullTextIndex,
            ClusterCertificationArtifactKind::Checksums,
        ],
        "expansion evidence",
        failures,
    );
}

fn verify_exact_artifact_kinds(
    references: &[PathBuf],
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    required_kinds: &[ClusterCertificationArtifactKind],
    context: &str,
    failures: &mut Vec<String>,
) {
    for kind in required_kinds {
        if !references
            .iter()
            .any(|reference| artifacts.get(reference) == Some(kind))
        {
            failures.push(format!(
                "{context} does not reference a verified {kind:?} artifact"
            ));
        }
    }
}

fn verify_artifact_references(
    references: &[PathBuf],
    artifacts: &BTreeMap<PathBuf, ClusterCertificationArtifactKind>,
    required_kind: Option<ClusterCertificationArtifactKind>,
    context: &str,
    failures: &mut Vec<String>,
) {
    if references.is_empty() {
        failures.push(format!("{context} has no raw artifact references"));
    }
    for reference in references {
        if !artifacts.contains_key(reference) {
            failures.push(format!(
                "{context} references unverified artifact {}",
                reference.display()
            ));
        }
    }
    if let Some(required_kind) = required_kind {
        if !references
            .iter()
            .any(|reference| artifacts.get(reference) == Some(&required_kind))
        {
            failures.push(format!(
                "{context} does not reference a verified {required_kind:?} artifact"
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn resume_cluster_certification_artifact_capture(
    bundle_root: &Path,
    plan: &ClusterCertificationPlan,
    state: &ClusterCertificationState,
    kind: ClusterCertificationArtifactKind,
    relative_output_path: &Path,
    output_path: &Path,
    payload_format: ClusterCertificationRawArtifactPayloadFormat,
    record_count: u64,
    started_at_ms: u64,
    completed_at_ms: u64,
    now_ms: u64,
    fsync: bool,
) -> Result<ClusterArtifactEvidence> {
    if fs::symlink_metadata(output_path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(certification_error(
            "captured artifact output may not be a symlink",
        ));
    }
    let verified = verify_cluster_certification_raw_artifact(
        output_path,
        plan,
        &state.source_commit,
        state.source_dirty,
        kind,
        state.started_at_ms,
        now_ms,
    )?;
    if verified.header.payload_format != payload_format
        || verified.header.record_count != record_count
        || verified.header.started_at_ms != started_at_ms
        || verified.header.completed_at_ms != completed_at_ms
    {
        return Err(certification_error(
            "existing captured artifact does not match the requested format, record count, or interval",
        ));
    }
    register_cluster_certification_artifact(bundle_root, kind, relative_output_path, now_ms, fsync)
}

fn prepare_cluster_certification_artifact_output(
    bundle_root: &Path,
    relative_output_path: &Path,
    fsync: bool,
) -> Result<PathBuf> {
    let canonical_root = fs::canonicalize(bundle_root)?;
    let file_name = relative_output_path
        .file_name()
        .ok_or_else(|| certification_error("captured artifact output has no file name"))?;
    let mut parent = canonical_root.clone();
    if let Some(relative_parent) = relative_output_path.parent() {
        for component in relative_parent.components() {
            let Component::Normal(name) = component else {
                return Err(certification_error(
                    "captured artifact output contains an unsafe directory component",
                ));
            };
            parent.push(name);
            match fs::symlink_metadata(&parent) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    return Err(certification_error(
                        "captured artifact output parent must contain only real directories",
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&parent)?;
                    if fsync {
                        sync_cluster_certification_artifact_parent(&parent)?;
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    let canonical_parent = fs::canonicalize(&parent)?;
    if !canonical_parent.starts_with(&canonical_root) {
        return Err(certification_error(
            "captured artifact output escapes the bundle root",
        ));
    }
    Ok(canonical_parent.join(file_name))
}

fn create_cluster_certification_artifact_temp(output_path: &Path) -> Result<(File, PathBuf)> {
    let parent = output_path
        .parent()
        .ok_or_else(|| certification_error("captured artifact output has no parent"))?;
    let output_name = output_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    for _ in 0..64 {
        let sequence = CLUSTER_CERTIFICATION_ARTIFACT_TEMP_SEQUENCE
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let temporary_path = parent.join(format!(
            ".{output_name}.bicdb-certification.tmp.{}.{}",
            std::process::id(),
            sequence
        ));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary_path) {
            Ok(file) => return Ok((file, temporary_path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(certification_error(
        "could not allocate a private artifact capture file",
    ))
}

#[cfg(unix)]
fn open_cluster_certification_payload(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(Into::into)
}

#[cfg(not(unix))]
fn open_cluster_certification_payload(path: &Path) -> Result<File> {
    OpenOptions::new().read(true).open(path).map_err(Into::into)
}

#[cfg(unix)]
fn sync_cluster_certification_artifact_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_cluster_certification_artifact_parent(_path: &Path) -> Result<()> {
    Ok(())
}

struct ClusterCertificationArtifactTempGuard {
    path: PathBuf,
    published: bool,
}

impl Drop for ClusterCertificationArtifactTempGuard {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty()
        || run_id.len() > 128
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(certification_error(
            "run ID must be 1-128 ASCII letters, digits, '.', '-', or '_'",
        ));
    }
    Ok(())
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(certification_error(format!(
            "{label} must contain 64 hexadecimal digits"
        )));
    }
    Ok(())
}

fn valid_source_commit(value: &str) -> bool {
    (40..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn ratio_basis_points(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return u64::MAX;
    }
    u128::from(numerator)
        .saturating_mul(10_000)
        .checked_div(u128::from(denominator))
        .unwrap_or(u128::MAX)
        .min(u128::from(u64::MAX)) as u64
}

fn map_within_skew_basis_points(
    values: &BTreeMap<ClusterNodeId, u64>,
    maximum_basis_points: u64,
) -> bool {
    if values.is_empty() {
        return false;
    }
    let minimum = values.values().copied().min().unwrap_or(0);
    let maximum = values.values().copied().max().unwrap_or(0);
    let total = values.values().copied().fold(0_u64, u64::saturating_add);
    let average = total / values.len() as u64;
    average > 0
        && ratio_basis_points(maximum.saturating_sub(minimum), average) <= maximum_basis_points
}

fn map_absolute_skew(values: &BTreeMap<ClusterNodeId, u64>) -> u64 {
    let minimum = values.values().copied().min().unwrap_or(0);
    let maximum = values.values().copied().max().unwrap_or(0);
    maximum.saturating_sub(minimum)
}

fn ceiling_div(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return u64::MAX;
    }
    numerator / denominator + u64::from(numerator % denominator != 0)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{
        ClusterNetworkTransportConfig, ClusterNode, DistributionConfig, DistributionStore,
        PlacementPolicy, RangeReplica, ReplicaId, StandardFailureDomain,
    };

    fn five_node_cluster() -> (ClusterTopology, DistributionConfig, MetadataConsensusStatus) {
        let root = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("cert-cluster").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "127.0.0.1:11001".to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000_000_000_000,
            replication_factor: 3,
            initial_ranges: 256,
            default_placement: PlacementPolicy::default()
                .with_standard_failure_domains([StandardFailureDomain::Server]),
            suspect_after_ms: 10_000,
            dead_after_ms: 20_000,
            topology_history_limit: 64,
            metadata_election_timeout_ms: 1_500,
            metadata_heartbeat_interval_ms: 300,
            transport: ClusterNetworkTransportConfig::default(),
        };
        let mut store =
            DistributionStore::initialize_at(root.path(), config.clone(), false, 100).unwrap();
        let actor = ClusterNodeId::new("n1").unwrap();
        store
            .heartbeat(
                &actor,
                1,
                0,
                config.node_capacity_bytes,
                BTreeMap::from([
                    ("server".to_string(), "server-1".to_string()),
                    (SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64)),
                ]),
                101,
            )
            .unwrap();
        for number in 2..=5 {
            store
                .join_node(
                    ClusterNode::new(
                        ClusterNodeId::new(format!("n{number}")).unwrap(),
                        format!("127.0.0.1:{}", 11_000 + number),
                        1,
                        10_000_000_000_000,
                        100 + number,
                    )
                    .unwrap()
                    .with_label("server", format!("server-{number}"))
                    .unwrap()
                    .with_label(SCHEMA_COMPATIBILITY_NODE_LABEL, "a".repeat(64))
                    .unwrap(),
                    &actor,
                    100 + number,
                )
                .unwrap();
        }
        let mut topology = store.topology().clone();
        let node_ids = topology.nodes.keys().cloned().collect::<Vec<_>>();
        let mut next_replica_id = 1_u64;
        for (index, range) in topology.ranges.values_mut().enumerate() {
            range.replicas = (0..3)
                .map(|offset| {
                    let replica = RangeReplica {
                        id: ReplicaId::new(next_replica_id).unwrap(),
                        node_id: node_ids[(index + offset) % node_ids.len()].clone(),
                        role: RangeReplicaRole::Voter,
                    };
                    next_replica_id += 1;
                    replica
                })
                .collect();
            range.leader = node_ids[index % node_ids.len()].clone();
            range.approximate_bytes = 1_000_000;
        }
        topology.next_replica_id = next_replica_id;
        topology.validate().unwrap();
        let voters = topology.metadata_voters().into_iter().collect::<Vec<_>>();
        let metadata = MetadataConsensusStatus {
            cluster_id: topology.cluster_id.clone(),
            node_id: actor.clone(),
            role: MetadataConsensusRole::Leader,
            current_term: 3,
            voted_for: Some(actor.clone()),
            leader_id: Some(actor),
            commit_index: 10,
            last_log_index: 10,
            last_log_term: 3,
            topology_generation: topology.generation,
            voters,
            learners: Vec::new(),
        };
        (topology, config, metadata)
    }

    fn live_plan(
        profile: ClusterScaleProfile,
        run_id: &str,
        now_ms: u64,
    ) -> ClusterCertificationPlan {
        let (topology, config, metadata) = five_node_cluster();
        ClusterCertificationPlan::from_live_cluster(
            profile,
            run_id,
            &topology,
            &config,
            &metadata,
            StorageMode::ServerPaged,
            now_ms,
        )
        .unwrap()
    }

    fn write_artifacts(
        root: &Path,
        plan: &ClusterCertificationPlan,
        source_commit: &str,
    ) -> (
        Vec<ClusterArtifactEvidence>,
        BTreeMap<ClusterCertificationArtifactKind, PathBuf>,
    ) {
        let (canonical_topology, canonical_config, _) = five_node_cluster();
        let mut artifacts = Vec::new();
        let mut paths = BTreeMap::new();
        for kind in ClusterCertificationArtifactKind::required() {
            let path = PathBuf::from(format!("{kind:?}.txt").to_ascii_lowercase());
            let payload = match kind {
                ClusterCertificationArtifactKind::EffectiveConfiguration => {
                    serde_json::to_vec_pretty(&canonical_config).unwrap()
                }
                ClusterCertificationArtifactKind::TopologyBefore => {
                    serde_json::to_vec(&canonical_topology).unwrap()
                }
                _ => format!("measured evidence for {kind:?}\n").into_bytes(),
            };
            let header = ClusterCertificationRawArtifactHeader {
                format_version: CLUSTER_CERTIFICATION_RAW_ARTIFACT_FORMAT_VERSION,
                run_id: plan.run_id.clone(),
                profile: plan.profile,
                bicdb_version: plan.bicdb_version.clone(),
                source_commit: source_commit.to_string(),
                source_dirty: false,
                kind,
                started_at_ms: 2_000,
                completed_at_ms: 2_500,
                producer_node_ids: plan.node_ids.clone(),
                payload_format: ClusterCertificationRawArtifactPayloadFormat::Text,
                record_count: 1,
                payload_bytes: payload.len() as u64,
                payload_sha256: sha256_bytes(&payload),
            };
            let mut bytes = serde_json::to_vec(&header).unwrap();
            bytes.push(b'\n');
            bytes.extend_from_slice(&payload);
            fs::write(root.join(&path), &bytes).unwrap();
            paths.insert(kind, path.clone());
            artifacts.push(ClusterArtifactEvidence {
                kind,
                relative_path: path,
                size_bytes: bytes.len() as u64,
                sha256: sha256_bytes(&bytes),
            });
        }
        (artifacts, paths)
    }

    fn governor_snapshot(stage: &str) -> ResourceGovernorSnapshot {
        let lanes = ResourceLane::ALL
            .into_iter()
            .map(|lane| {
                let active = stage == "saturated" && lane.is_background();
                ResourceLaneSnapshot {
                    lane,
                    usage: if active {
                        ResourceUsage {
                            memory_bytes: 1,
                            io_bytes: 1,
                            cpu_slots: 1,
                            active: 1,
                        }
                    } else {
                        ResourceUsage::default()
                    },
                    admitted: u64::from(stage != "before" && lane.is_background()),
                    rejected: u64::from(stage != "before" && lane == ResourceLane::AntiEntropy),
                    io_tokens: 1,
                }
            })
            .collect::<Vec<_>>();
        let background_active = if stage == "saturated" { 4 } else { 0 };
        let usage = ResourceUsage {
            memory_bytes: background_active,
            io_bytes: background_active,
            cpu_slots: background_active as usize,
            active: background_active as usize,
        };
        ResourceGovernorSnapshot {
            total: usage,
            noncritical: usage,
            background: usage,
            lanes,
        }
    }

    fn governor_config() -> ResourceGovernorConfig {
        let node = ResourceCapacity {
            memory_bytes: 10,
            io_bytes: 10,
            cpu_slots: 10,
        };
        let background = ResourceCapacity {
            memory_bytes: 4,
            io_bytes: 4,
            cpu_slots: 4,
        };
        let lanes = ResourceLane::ALL
            .into_iter()
            .map(|lane| {
                (
                    lane,
                    crate::ResourceLaneLimit {
                        capacity: if lane.is_background() {
                            background
                        } else {
                            node
                        },
                        max_active: 10,
                        io_bytes_per_second: 10,
                        io_burst_bytes: 10,
                    },
                )
            })
            .collect();
        ResourceGovernorConfig {
            node,
            critical_reserve: ResourceCapacity {
                memory_bytes: 1,
                io_bytes: 1,
                cpu_slots: 1,
            },
            background,
            lanes,
        }
    }

    fn valid_bundle(
        root: &Path,
    ) -> (
        ClusterCertificationPlan,
        ClusterCertificationReport,
        PathBuf,
        PathBuf,
    ) {
        let plan = live_plan(ClusterScaleProfile::OneTb, "one-tb-test", 1_000);
        let plan_path = root.join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
        save_cluster_certification_plan(&plan_path, &plan, false).unwrap();
        let plan_sha256 = sha256_bytes(&fs::read(&plan_path).unwrap());
        let source_commit = "a".repeat(40);
        let (artifacts, paths) = write_artifacts(root, &plan, &source_commit);
        let hardware = plan
            .node_ids
            .iter()
            .map(|node_id| ClusterNodeHardwareEvidence {
                node_id: node_id.clone(),
                hostname: format!("{node_id}.example"),
                cpu_model: "test-cpu".to_string(),
                physical_cores: 32,
                memory_bytes: 64 * 1024 * 1024 * 1024,
                storage_model: "test-nvme".to_string(),
                storage_bytes: 10_000_000_000_000,
                filesystem: "xfs".to_string(),
                mount_options: "rw,noatime".to_string(),
                operating_system: "Test Linux".to_string(),
                kernel: "6.0".to_string(),
                nic_bits_per_second: 100_000_000_000,
            })
            .collect::<Vec<_>>();
        let peak_rss_bytes_by_node = plan
            .node_ids
            .iter()
            .map(|node_id| (node_id.clone(), 8 * 1024 * 1024 * 1024))
            .collect();
        let final_replica_bytes_by_node = plan
            .node_ids
            .iter()
            .map(|node_id| (node_id.clone(), 600_000_000_000))
            .collect();
        let expansion_node_id = ClusterNodeId::new("n6").unwrap();
        let expansion_hardware = ClusterNodeHardwareEvidence {
            node_id: expansion_node_id.clone(),
            hostname: "n6.example".to_string(),
            cpu_model: "test-cpu".to_string(),
            physical_cores: 32,
            memory_bytes: 64 * 1024 * 1024 * 1024,
            storage_model: "test-nvme".to_string(),
            storage_bytes: 10_000_000_000_000,
            filesystem: "xfs".to_string(),
            mount_options: "rw,noatime".to_string(),
            operating_system: "Test Linux".to_string(),
            kernel: "6.0".to_string(),
            nic_bits_per_second: 100_000_000_000,
        };
        let expansion_peak_rss_bytes_by_node = plan
            .node_ids
            .iter()
            .cloned()
            .chain(std::iter::once(expansion_node_id.clone()))
            .map(|node_id| (node_id, 8 * 1024 * 1024 * 1024))
            .collect();
        let expansion_replica_bytes_by_node = plan
            .node_ids
            .iter()
            .cloned()
            .chain(std::iter::once(expansion_node_id))
            .map(|node_id| (node_id, 500_000_000_000))
            .collect();
        let failure_artifact = paths[&ClusterCertificationArtifactKind::FailureTimeline].clone();
        let checksums_artifact = paths[&ClusterCertificationArtifactKind::Checksums].clone();
        let loss_mechanisms = [
            ClusterNodeLossMechanism::ProcessKill,
            ClusterNodeLossMechanism::HostPowerCut,
            ClusterNodeLossMechanism::HypervisorCrash,
            ClusterNodeLossMechanism::NetworkBlackhole,
        ];
        let failure_trials = ClusterFailurePoint::required()
            .into_iter()
            .enumerate()
            .map(|(index, failure_point)| {
                let node_id = plan.node_ids[index % plan.node_ids.len()].clone();
                let started_at_ms = 2_000 + index as u64 * 100;
                let recovery_mode = if index % 2 == 0 {
                    ClusterNodeRecoveryMode::RestartSameIncarnation
                } else {
                    ClusterNodeRecoveryMode::RejoinHigherIncarnation
                };
                ClusterFailureTrialEvidence {
                    failure_point,
                    failed_node_id: node_id.clone(),
                    recovered_node_id: node_id,
                    failed_node_incarnation: 1,
                    recovered_node_incarnation: if index % 2 == 0 { 1 } else { 2 },
                    loss_mechanism: loss_mechanisms[index % loss_mechanisms.len()],
                    recovery_mode,
                    maintenance_operation_id: format!("operation-{failure_point:?}"),
                    maintenance_checkpoint_before_sha256: "b".repeat(64),
                    maintenance_checkpoint_after_sha256: "c".repeat(64),
                    started_at_ms,
                    failure_injected_at_ms: started_at_ms + 10,
                    node_loss_detected_at_ms: started_at_ms + 20,
                    old_identity_fenced_at_ms: started_at_ms + 30,
                    repair_started_at_ms: started_at_ms + 35,
                    policy_converged_at_ms: started_at_ms + 45,
                    recovered_at_ms: started_at_ms + 50,
                    passed: true,
                    data_loss_detected: false,
                    stale_epoch_write_accepted: false,
                    affected_ranges: 1,
                    repaired_ranges: 1,
                    digest_verified_ranges: 1,
                    verification_commit_sequence: 1,
                    verified_records: 1,
                    verified_logical_bytes: 200_000_000_000,
                    pre_failure_digest_sha256: "d".repeat(64),
                    post_recovery_digest_sha256: "d".repeat(64),
                    quorum_operations_attempted: 1_000,
                    quorum_operations_succeeded: 1_000,
                    old_identity_writes_attempted: 1,
                    old_identity_writes_rejected: 1,
                    maximum_under_replicated_ranges: 1,
                    final_under_replicated_ranges: 0,
                    maximum_unavailable_ranges: 0,
                    artifact_paths: vec![failure_artifact.clone(), checksums_artifact.clone()],
                }
            })
            .collect();
        let report = ClusterCertificationReport {
            format_version: CLUSTER_CERTIFICATION_FORMAT_VERSION,
            run_id: plan.run_id.clone(),
            profile: plan.profile,
            plan_sha256,
            bicdb_version: plan.bicdb_version.clone(),
            source_commit,
            source_dirty: false,
            started_at_ms: 1_100,
            completed_at_ms: 1_100_000,
            completed: true,
            hardware,
            measurements: ClusterCertificationMeasurements {
                logical_dataset_bytes: 1_000_000_000_000,
                total_physical_database_bytes: 4_000_000_000_000,
                peak_rss_bytes_by_node,
                baseline_started_at_ms: 5_000,
                baseline_completed_at_ms: 305_000,
                rebalance_started_at_ms: 320_000,
                rebalance_completed_at_ms: 620_000,
                recovery_started_at_ms: 400_000,
                recovery_completed_at_ms: 450_000,
                foreground_baseline_p99_ms: 100.0,
                foreground_rebalance_p99_ms: 120.0,
                foreground_baseline_samples: 10_000,
                foreground_rebalance_samples: 10_000,
                hotspot_baseline_p99_ms: 125.0,
                hotspot_rebalance_p99_ms: 150.0,
                hotspot_baseline_samples: 10_000,
                hotspot_rebalance_samples: 10_000,
                quorum_operations_attempted: 10_000,
                quorum_operations_succeeded: 10_000,
                minority_writes_attempted: 1_000,
                minority_writes_rejected: 1_000,
                logical_rebalance_bytes: 100_000_000_000,
                rebalance_source_read_bytes: 100_000_000_000,
                rebalance_network_bytes: 100_000_000_000,
                rebalance_destination_write_bytes: 100_000_000_000,
                recovery_time_ms: 50_000,
                maximum_unavailable_ranges: 0,
                final_metrics: ClusterOperationalMetrics {
                    nodes_total: 5,
                    nodes_live: 5,
                    ranges_total: 256,
                    replicas_total: 768,
                    leaders_total: 256,
                    replica_skew: 1,
                    leader_skew: 1,
                    ..ClusterOperationalMetrics::default()
                },
                final_replica_bytes_by_node,
                full_text_index_source_bytes: 1_000_000_000_000,
                full_text_index_documents: 40_017_289,
                full_text_index_bytes: 100_000_000_000,
                artifact_paths: vec![
                    paths[&ClusterCertificationArtifactKind::ResourceSamples].clone(),
                    paths[&ClusterCertificationArtifactKind::WorkloadLatency].clone(),
                    paths[&ClusterCertificationArtifactKind::RebalanceTimeline].clone(),
                ],
            },
            background_saturation: ClusterBackgroundSaturationEvidence {
                started_at_ms: 10_000,
                completed_at_ms: 310_000,
                nodes: plan
                    .node_ids
                    .iter()
                    .map(|node_id| ClusterNodeBackgroundSaturationEvidence {
                        node_id: node_id.clone(),
                        governor_config: governor_config(),
                        rejected_lane: ResourceLane::AntiEntropy,
                        rejected_demand: ResourceDemand {
                            memory_bytes: 1,
                            io_bytes: 1,
                            cpu_slots: 1,
                            io_charge_bytes: 1,
                        },
                        before_at_ms: 10_000,
                        saturated_at_ms: 20_000,
                        after_at_ms: 310_000,
                        before: governor_snapshot("before"),
                        saturated: governor_snapshot("saturated"),
                        after: governor_snapshot("after"),
                    })
                    .collect(),
                foreground_read_baseline_p99_ms: 100.0,
                foreground_read_saturated_p99_ms: 120.0,
                foreground_read_operations_attempted: 10_000,
                foreground_read_operations_succeeded: 10_000,
                foreground_write_baseline_p99_ms: 100.0,
                foreground_write_saturated_p99_ms: 120.0,
                foreground_write_operations_attempted: 10_000,
                foreground_write_operations_succeeded: 10_000,
                metadata_quorum_checks_attempted: 1_000,
                metadata_quorum_checks_succeeded: 1_000,
                range_quorum_operations_attempted: 10_000,
                range_quorum_operations_succeeded: 10_000,
                maximum_unavailable_ranges: 0,
                artifact_paths: vec![paths
                    [&ClusterCertificationArtifactKind::BackgroundSaturation]
                    .clone()],
            },
            failure_trials,
            restore: ClusterRestoreEvidence {
                passed: true,
                backup_id: "backup-one-tb-test".to_string(),
                backup_manifest_sha256: "e".repeat(64),
                restored_node_id: plan.node_ids[0].clone(),
                source_node_incarnation: 1,
                restored_node_incarnation: 2,
                started_at_ms: 630_000,
                completed_at_ms: 690_000,
                elapsed_ms: 60_000,
                verification_commit_sequence: 1,
                source_records: 10_000_000,
                restored_records: 10_000_000,
                source_logical_bytes: 200_000_000_000,
                restored_logical_bytes: 200_000_000_000,
                peak_rss_bytes: 8 * 1024 * 1024 * 1024,
                source_digest_sha256: "f".repeat(64),
                restored_digest_sha256: "f".repeat(64),
                source_full_text_documents: 10_000_000,
                restored_full_text_documents: 10_000_000,
                source_full_text_index_bytes: 20_000_000_000,
                restored_full_text_index_bytes: 20_000_000_000,
                source_full_text_digest_sha256: "8".repeat(64),
                restored_full_text_digest_sha256: "8".repeat(64),
                full_text_queries_attempted: 1_000,
                full_text_queries_succeeded: 1_000,
                quorum_operations_attempted: 10_000,
                quorum_operations_succeeded: 10_000,
                old_identity_writes_attempted: 1_000,
                old_identity_writes_rejected: 1_000,
                maximum_unavailable_ranges: 0,
                integrity_verified: true,
                artifact_paths: vec![
                    paths[&ClusterCertificationArtifactKind::Restore].clone(),
                    paths[&ClusterCertificationArtifactKind::TopologyBefore].clone(),
                    paths[&ClusterCertificationArtifactKind::TopologyAfter].clone(),
                    paths[&ClusterCertificationArtifactKind::ResourceSamples].clone(),
                    paths[&ClusterCertificationArtifactKind::WorkloadLatency].clone(),
                    paths[&ClusterCertificationArtifactKind::FullTextIndex].clone(),
                    paths[&ClusterCertificationArtifactKind::Checksums].clone(),
                ],
            },
            expansion: ClusterExpansionEvidence {
                passed: true,
                started_at_ms: 700_000,
                completed_at_ms: 1_000_000,
                elapsed_ms: 300_000,
                nodes_before: 5,
                nodes_after: 6,
                topology_generation_before: plan.topology_generation,
                topology_generation_after: plan.topology_generation + 1,
                topology_before_sha256: plan.topology_sha256.clone(),
                topology_after_sha256: "1".repeat(64),
                added_node_hardware: vec![expansion_hardware],
                moved_logical_bytes: 200_000_000_000,
                source_read_bytes: 200_000_000_000,
                network_bytes: 200_000_000_000,
                destination_write_bytes: 200_000_000_000,
                peak_rss_bytes_by_node: expansion_peak_rss_bytes_by_node,
                foreground_baseline_p99_ms: 100.0,
                foreground_expansion_p99_ms: 120.0,
                foreground_baseline_samples: 10_000,
                foreground_expansion_samples: 10_000,
                hotspot_baseline_p99_ms: 125.0,
                hotspot_expansion_p99_ms: 150.0,
                hotspot_baseline_samples: 10_000,
                hotspot_expansion_samples: 10_000,
                quorum_operations_attempted: 10_000,
                quorum_operations_succeeded: 10_000,
                verification_commit_sequence: 1,
                verified_records: 40_017_289,
                verified_logical_bytes: 1_000_000_000_000,
                pre_expansion_digest_sha256: "9".repeat(64),
                post_expansion_digest_sha256: "9".repeat(64),
                full_text_source_bytes_before: 1_000_000_000_000,
                full_text_source_bytes_after: 1_000_000_000_000,
                full_text_documents_before: 40_017_289,
                full_text_documents_after: 40_017_289,
                full_text_index_bytes_before: 100_000_000_000,
                full_text_index_bytes_after: 100_000_000_000,
                full_text_digest_sha256_before: "7".repeat(64),
                full_text_digest_sha256_after: "7".repeat(64),
                full_text_queries_attempted: 1_000,
                full_text_queries_succeeded: 1_000,
                final_replica_skew: 1,
                final_leader_skew: 1,
                final_active_relocations: 0,
                final_failed_relocations: 0,
                final_replica_bytes_by_node: expansion_replica_bytes_by_node,
                maximum_under_replicated_ranges: 1,
                final_under_replicated_ranges: 0,
                maximum_unavailable_ranges: 0,
                final_unavailable_ranges: 0,
                artifact_paths: vec![
                    paths[&ClusterCertificationArtifactKind::Hardware].clone(),
                    paths[&ClusterCertificationArtifactKind::EffectiveConfiguration].clone(),
                    paths[&ClusterCertificationArtifactKind::TopologyBefore].clone(),
                    paths[&ClusterCertificationArtifactKind::TopologyAfter].clone(),
                    paths[&ClusterCertificationArtifactKind::ResourceSamples].clone(),
                    paths[&ClusterCertificationArtifactKind::WorkloadLatency].clone(),
                    paths[&ClusterCertificationArtifactKind::RebalanceTimeline].clone(),
                    paths[&ClusterCertificationArtifactKind::FullTextIndex].clone(),
                    paths[&ClusterCertificationArtifactKind::Checksums].clone(),
                ],
            },
            artifacts,
        };
        let report_path = root.join(DEFAULT_CLUSTER_CERTIFICATION_REPORT);
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        publish_cluster_certification_manifest(
            root,
            &plan_path,
            &report_path,
            report.completed_at_ms,
            false,
        )
        .unwrap();
        (plan, report, plan_path, report_path)
    }

    #[test]
    #[allow(deprecated)]
    fn canonical_plan_requires_a_stable_five_node_rf3_topology() {
        let (topology, config, metadata) = five_node_cluster();
        assert!(ClusterCertificationPlan::from_topology(
            ClusterScaleProfile::TwentyTb,
            "unsafe-topology-only",
            &topology,
            1_000,
        )
        .unwrap_err()
        .to_string()
        .contains("topology-only certification planning is unsafe"));
        let plan = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::TwentyTb,
            "production-20tb",
            &topology,
            &config,
            &metadata,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap();
        assert_eq!(plan.gates.minimum_logical_dataset_bytes, 20_000_000_000_000);
        assert_eq!(plan.gates.maximum_peak_rss_bytes_per_node, 32 << 30);
        assert_eq!(
            plan.gates.minimum_background_saturation_duration_ms,
            300_000
        );
        assert_eq!(plan.gates.minimum_measurement_window_ms, 300_000);
        assert_eq!(plan.gates.minimum_rebalance_logical_basis_points, 1_000);
        assert_eq!(plan.gates.minimum_restore_quorum_operations, 10_000);
        assert_eq!(plan.gates.minimum_expansion_quorum_operations, 10_000);
        assert_eq!(plan.gates.minimum_fts_query_probes, 1_000);
        assert_eq!(plan.gates.minimum_identity_fence_attempts, 1_000);
        assert_eq!(plan.required_failure_points.len(), 8);
        assert!(plan
            .required_artifact_kinds
            .contains(&ClusterCertificationArtifactKind::BackgroundSaturation));

        let mut weakened = plan.clone();
        weakened.gates.minimum_background_saturation_operations = 1;
        assert!(weakened
            .validate()
            .unwrap_err()
            .to_string()
            .contains("may not be weakened"));

        let mut invalid = topology;
        invalid.replication_factor = 2;
        assert!(ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "invalid",
            &invalid,
            &config,
            &metadata,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap_err()
        .to_string()
        .contains("replication factor"));
    }

    #[test]
    fn plan_preflight_rejects_unhealthy_stale_or_uncommitted_clusters() {
        let (topology, config, metadata) = five_node_cluster();

        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "resident-storage",
            &topology,
            &config,
            &metadata,
            StorageMode::EmbeddedMemory,
            1_000,
        )
        .unwrap_err();
        assert!(error.to_string().contains("server_paged"));

        let mut stale = topology.clone();
        for node in stale.nodes.values_mut() {
            node.last_heartbeat_ms = 10_499;
        }
        let stale_node = stale
            .nodes
            .get_mut(&ClusterNodeId::new("n5").unwrap())
            .unwrap();
        stale_node.last_heartbeat_ms = stale_node.joined_at_ms;
        let mut stale_metadata = metadata.clone();
        stale_metadata.topology_generation = stale.generation;
        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "stale-node",
            &stale,
            &config,
            &stale_metadata,
            StorageMode::ServerPaged,
            10_500,
        )
        .unwrap_err();
        assert!(error.to_string().contains("five live nodes"));

        let mut under_replicated = topology.clone();
        under_replicated
            .ranges
            .values_mut()
            .next()
            .unwrap()
            .replicas
            .pop();
        let mut under_replicated_metadata = metadata.clone();
        under_replicated_metadata.topology_generation = under_replicated.generation;
        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "under-replicated",
            &under_replicated,
            &config,
            &under_replicated_metadata,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("full availability and replication"));

        let mut divergent_schema = topology.clone();
        divergent_schema
            .nodes
            .get_mut(&ClusterNodeId::new("n5").unwrap())
            .unwrap()
            .labels
            .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "b".repeat(64));
        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "schema-mismatch",
            &divergent_schema,
            &config,
            &metadata,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap_err();
        assert!(error.to_string().contains("identical active schema"));

        let mut uncommitted = metadata.clone();
        uncommitted.last_log_index += 1;
        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "uncommitted-metadata",
            &topology,
            &config,
            &uncommitted,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap_err();
        assert!(error.to_string().contains("fully committed"));

        let mut no_server_placement = config.clone();
        no_server_placement
            .default_placement
            .distinct_failure_domains
            .clear();
        let error = ClusterCertificationPlan::from_live_cluster(
            ClusterScaleProfile::OneTb,
            "no-server-domain",
            &topology,
            &no_server_placement,
            &metadata,
            StorageMode::ServerPaged,
            1_000,
        )
        .unwrap_err();
        assert!(error.to_string().contains("server failure-domain"));

        let mut forged = live_plan(ClusterScaleProfile::OneTb, "forged-preflight", 1_000);
        forged.preflight.metrics.nodes_live = 4;
        assert!(forged
            .validate()
            .unwrap_err()
            .to_string()
            .contains("fully healthy and balanced"));
    }

    #[test]
    fn collector_rejects_dirty_source_before_creating_state() {
        let root = tempfile::tempdir().unwrap();
        let plan = live_plan(ClusterScaleProfile::OneTb, "dirty-source", 1_000);
        let plan_path = root.path().join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
        save_cluster_certification_plan(&plan_path, &plan, true).unwrap();
        let error = initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            "d".repeat(40),
            true,
            1_100,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("dirty source tree"));
        assert!(!root
            .path()
            .join(DEFAULT_CLUSTER_CERTIFICATION_STATE)
            .exists());
    }

    #[test]
    fn complete_measured_bundle_passes_and_tampering_fails() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(verification.passed, "{:?}", verification.failures);
        assert_eq!(
            verification.verified_artifacts,
            ClusterCertificationArtifactKind::required().len() as u64
        );
        assert!(verification.verified_artifact_bytes > 0);
        assert_eq!(verification.publication_manifest_sha256.len(), 64);

        let tampered = &report.artifacts[0].relative_path;
        fs::write(root.path().join(tampered), b"tampered evidence").unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert_eq!(
            verification.verified_artifacts,
            ClusterCertificationArtifactKind::required().len() as u64 - 1
        );
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("checksum") || failure.contains("size")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("has no verified artifact")));
    }

    #[test]
    fn publication_manifest_binds_default_files_and_the_complete_artifact_set() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        let manifest_path = root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
        let manifest = load_cluster_certification_publication_manifest(&manifest_path).unwrap();
        assert_eq!(
            manifest.artifact_count,
            ClusterCertificationArtifactKind::required().len() as u64
        );
        assert_eq!(manifest.published_at_ms, report.completed_at_ms);
        assert_eq!(
            manifest.bundle_sha256,
            cluster_certification_manifest_bundle_sha256(&manifest).unwrap()
        );

        let outside = tempfile::tempdir().unwrap();
        let outside_plan = outside.path().join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
        fs::copy(&plan_path, &outside_plan).unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &outside_plan, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("plan must be the regular in-bundle default file")));

        let mut self_consistent_substitution = manifest;
        self_consistent_substitution.artifacts.pop();
        self_consistent_substitution.artifact_count =
            self_consistent_substitution.artifacts.len() as u64;
        self_consistent_substitution.artifact_bytes = self_consistent_substitution
            .artifacts
            .iter()
            .map(|artifact| artifact.size_bytes)
            .sum();
        self_consistent_substitution.bundle_sha256 =
            cluster_certification_manifest_bundle_sha256(&self_consistent_substitution).unwrap();
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&self_consistent_substitution).unwrap(),
        )
        .unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("manifest artifact set")));

        fs::remove_file(&manifest_path).unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("no publication manifest")));
    }

    #[test]
    fn raw_artifacts_are_run_bound_and_payload_authenticated_even_after_republication() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let artifact_index = report
            .artifacts
            .iter()
            .position(|artifact| artifact.kind == ClusterCertificationArtifactKind::Commands)
            .unwrap();
        let artifact_path = root
            .path()
            .join(&report.artifacts[artifact_index].relative_path);
        let bytes = fs::read(&artifact_path).unwrap();
        let newline = bytes.iter().position(|byte| *byte == b'\n').unwrap();
        let mut header =
            serde_json::from_slice::<ClusterCertificationRawArtifactHeader>(&bytes[..newline])
                .unwrap();
        header.run_id = "other-run".to_string();
        let mut substituted = serde_json::to_vec(&header).unwrap();
        substituted.push(b'\n');
        substituted.extend_from_slice(&bytes[newline + 1..]);
        fs::write(&artifact_path, &substituted).unwrap();
        report.artifacts[artifact_index].size_bytes = substituted.len() as u64;
        report.artifacts[artifact_index].sha256 = sha256_bytes(&substituted);
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
        publish_cluster_certification_manifest(
            root.path(),
            &plan_path,
            &report_path,
            report.completed_at_ms,
            false,
        )
        .unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("does not match this run")));

        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let artifact_index = report
            .artifacts
            .iter()
            .position(|artifact| artifact.kind == ClusterCertificationArtifactKind::Commands)
            .unwrap();
        let artifact_path = root
            .path()
            .join(&report.artifacts[artifact_index].relative_path);
        let mut corrupted_payload = fs::read(&artifact_path).unwrap();
        corrupted_payload.push(b'x');
        fs::write(&artifact_path, &corrupted_payload).unwrap();
        report.artifacts[artifact_index].size_bytes = corrupted_payload.len() as u64;
        report.artifacts[artifact_index].sha256 = sha256_bytes(&corrupted_payload);
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
        publish_cluster_certification_manifest(
            root.path(),
            &plan_path,
            &report_path,
            report.completed_at_ms,
            false,
        )
        .unwrap();
        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("payload size or SHA-256")));
    }

    #[test]
    fn preflight_artifacts_must_match_the_frozen_config_and_topology() {
        for (kind, expected) in [
            (
                ClusterCertificationArtifactKind::EffectiveConfiguration,
                "does not match the plan preflight configuration",
            ),
            (
                ClusterCertificationArtifactKind::TopologyBefore,
                "does not match the topology frozen by the plan",
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
            let artifact_index = report
                .artifacts
                .iter()
                .position(|artifact| artifact.kind == kind)
                .unwrap();
            let artifact_path = root
                .path()
                .join(&report.artifacts[artifact_index].relative_path);
            let bytes = fs::read(&artifact_path).unwrap();
            let newline = bytes.iter().position(|byte| *byte == b'\n').unwrap();
            let mut header =
                serde_json::from_slice::<ClusterCertificationRawArtifactHeader>(&bytes[..newline])
                    .unwrap();
            let forged_payload = b"self-consistent but unrelated payload\n";
            header.payload_bytes = forged_payload.len() as u64;
            header.payload_sha256 = sha256_bytes(forged_payload);
            let mut forged = serde_json::to_vec(&header).unwrap();
            forged.push(b'\n');
            forged.extend_from_slice(forged_payload);
            fs::write(&artifact_path, &forged).unwrap();
            report.artifacts[artifact_index].size_bytes = forged.len() as u64;
            report.artifacts[artifact_index].sha256 = sha256_bytes(&forged);
            fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
            fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
            publish_cluster_certification_manifest(
                root.path(),
                &plan_path,
                &report_path,
                report.completed_at_ms,
                false,
            )
            .unwrap();

            let verification =
                verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
            assert!(!verification.passed);
            assert!(
                verification
                    .failures
                    .iter()
                    .any(|failure| failure.contains(expected)),
                "{:?}",
                verification.failures
            );
        }
    }

    #[test]
    fn artifact_registration_rejects_opaque_or_unbounded_headers() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        fs::remove_file(&report_path).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
        initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            report.source_commit,
            false,
            report.started_at_ms,
            false,
        )
        .unwrap();

        fs::write(root.path().join("opaque.txt"), b"opaque evidence").unwrap();
        let error = register_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::Commands,
            "opaque.txt",
            2_500,
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("newline-terminated JSON header"));

        fs::write(
            root.path().join("oversized.txt"),
            vec![b' '; MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES + 1],
        )
        .unwrap();
        let error = register_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::Commands,
            "oversized.txt",
            2_500,
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("no larger than 64 KiB"));
    }

    #[test]
    fn undersized_incomplete_or_unreferenced_evidence_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        report.measurements.logical_dataset_bytes = 999;
        report.failure_trials.pop();
        report.restore.artifact_paths = vec![PathBuf::from("not-recorded.log")];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("logical dataset")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("required failure trial")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("unverified artifact")));
    }

    #[test]
    fn destructive_failure_trials_require_real_loss_fencing_repair_and_checksums() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let failure_path = report
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ClusterCertificationArtifactKind::FailureTimeline)
            .unwrap()
            .relative_path
            .clone();
        let wrong_recovery_node = report.hardware[1].node_id.clone();
        let trial = &mut report.failure_trials[0];
        trial.recovered_node_id = wrong_recovery_node;
        trial.recovery_mode = ClusterNodeRecoveryMode::RejoinHigherIncarnation;
        trial.recovered_node_incarnation = trial.failed_node_incarnation;
        trial.maintenance_checkpoint_after_sha256 =
            trial.maintenance_checkpoint_before_sha256.clone();
        trial.node_loss_detected_at_ms = trial.failure_injected_at_ms - 1;
        trial.affected_ranges = 0;
        trial.maximum_under_replicated_ranges = 0;
        trial.final_under_replicated_ranges = 1;
        trial.verified_logical_bytes = 1;
        trial.post_recovery_digest_sha256 = "e".repeat(64);
        trial.quorum_operations_succeeded = trial.quorum_operations_attempted - 1;
        trial.old_identity_writes_rejected = 0;
        trial.artifact_paths = vec![failure_path];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        for expected in [
            "invalid recovery identity or incarnation",
            "lacks distinct valid maintenance checkpoints",
            "destructive recovery order",
            "affected-range repair and final policy convergence",
            "fixed-watermark logical checksum preservation",
            "quorum workload",
            "reject every old-identity write attempt",
            "does not reference a verified Checksums artifact",
        ] {
            assert!(
                verification
                    .failures
                    .iter()
                    .any(|failure| failure.contains(expected)),
                "missing failure containing {expected:?}: {:?}",
                verification.failures
            );
        }
    }

    #[test]
    fn performance_measurements_require_windows_samples_work_and_exact_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let wrong_artifact = report
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ClusterCertificationArtifactKind::Checksums)
            .unwrap()
            .relative_path
            .clone();
        let measurements = &mut report.measurements;
        measurements.baseline_completed_at_ms = measurements.baseline_started_at_ms + 299_999;
        measurements.rebalance_started_at_ms = measurements.baseline_completed_at_ms - 1;
        measurements.recovery_completed_at_ms = measurements.recovery_started_at_ms + 1;
        measurements.foreground_baseline_samples = 9_999;
        measurements.hotspot_rebalance_samples = 9_999;
        measurements.quorum_operations_attempted = 9_999;
        measurements.quorum_operations_succeeded = 9_999;
        measurements.minority_writes_attempted = 999;
        measurements.minority_writes_rejected = 999;
        measurements.logical_rebalance_bytes = 99_999_999_999;
        measurements.artifact_paths = vec![wrong_artifact];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        for expected in [
            "performance baseline and rebalance windows",
            "recovery timing",
            "foreground baseline has 9999 samples",
            "hotspot rebalance has 9999 samples",
            "majority-quorum workload must attempt at least",
            "minority-partition workload must attempt at least",
            "rebalance amplification exceeds",
            "verified ResourceSamples artifact",
            "verified WorkloadLatency artifact",
            "verified RebalanceTimeline artifact",
        ] {
            assert!(
                verification
                    .failures
                    .iter()
                    .any(|failure| failure.contains(expected)),
                "missing failure containing {expected:?}: {:?}",
                verification.failures
            );
        }
    }

    #[test]
    fn restore_requires_provenance_fixed_watermark_fts_workload_and_exact_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let restore_artifact = report
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ClusterCertificationArtifactKind::Restore)
            .unwrap()
            .relative_path
            .clone();
        let restore = &mut report.restore;
        restore.backup_id = "../forged-backup".to_string();
        restore.completed_at_ms += 1;
        restore.restored_node_incarnation = restore.source_node_incarnation;
        restore.restored_records -= 1;
        restore.restored_digest_sha256 = "0".repeat(64);
        restore.peak_rss_bytes = 33 * 1024 * 1024 * 1024;
        restore.restored_full_text_documents -= 1;
        restore.restored_full_text_digest_sha256 = "0".repeat(64);
        restore.full_text_queries_attempted = 999;
        restore.full_text_queries_succeeded = 999;
        restore.quorum_operations_attempted = 9_999;
        restore.quorum_operations_succeeded = 9_999;
        restore.old_identity_writes_attempted = 999;
        restore.old_identity_writes_rejected = 999;
        restore.maximum_unavailable_ranges = 1;
        restore.artifact_paths = vec![restore_artifact];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        for expected in [
            "invalid backup provenance",
            "timestamps must be ordered, exact",
            "higher incarnation",
            "equal source and restored data",
            "bounded-memory gate",
            "equal production-sized FTS index",
            "whole-node restore FTS probes",
            "whole-node restore quorum workload",
            "reject at least 1000 old-identity writes",
            "made at least one range unavailable",
            "verified TopologyBefore artifact",
            "verified Checksums artifact",
        ] {
            assert!(
                verification
                    .failures
                    .iter()
                    .any(|failure| failure.contains(expected)),
                "missing failure containing {expected:?}: {:?}",
                verification.failures
            );
        }
    }

    #[test]
    fn expansion_requires_new_hardware_integrity_fts_workload_balance_and_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        let rebalance_artifact = report
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ClusterCertificationArtifactKind::RebalanceTimeline)
            .unwrap()
            .relative_path
            .clone();
        let expansion = &mut report.expansion;
        expansion.completed_at_ms = expansion.started_at_ms + 299_999;
        expansion.elapsed_ms = 299_999;
        expansion.nodes_after = 7;
        expansion.topology_generation_after = expansion.topology_generation_before;
        expansion.topology_after_sha256 = expansion.topology_before_sha256.clone();
        expansion.added_node_hardware[0].storage_bytes = 1;
        let added_node = expansion.added_node_hardware[0].node_id.clone();
        expansion.peak_rss_bytes_by_node.remove(&added_node);
        expansion.moved_logical_bytes = 1;
        expansion.source_read_bytes = 0;
        expansion.foreground_expansion_samples = 9_999;
        expansion.foreground_expansion_p99_ms = 130.0;
        expansion.quorum_operations_attempted = 9_999;
        expansion.quorum_operations_succeeded = 9_999;
        expansion.verified_logical_bytes = 1;
        expansion.post_expansion_digest_sha256 = "0".repeat(64);
        expansion.full_text_documents_after -= 1;
        expansion.full_text_digest_sha256_after = "0".repeat(64);
        expansion.full_text_queries_attempted = 999;
        expansion.full_text_queries_succeeded = 999;
        expansion.final_replica_skew = 2;
        expansion.final_active_relocations = 1;
        expansion.maximum_unavailable_ranges = 1;
        expansion.final_replica_bytes_by_node.remove(&added_node);
        expansion.artifact_paths = vec![rebalance_artifact];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        for expected in [
            "five-minute-or-longer report window",
            "newer, distinct topology generation",
            "identify every newly added node",
            "insufficient hardware evidence",
            "bounded peak RSS",
            "relocation I/O exceeds",
            "expansion foreground active has 9999 samples",
            "cluster expansion foreground p99 regression",
            "cluster expansion quorum workload",
            "full-corpus integrity",
            "equal production-sized FTS corpus",
            "cluster expansion FTS probes",
            "finish healthy and converged",
            "replica-byte evidence must cover",
            "verified Hardware artifact",
            "verified Checksums artifact",
        ] {
            assert!(
                verification
                    .failures
                    .iter()
                    .any(|failure| failure.contains(expected)),
                "missing failure containing {expected:?}: {:?}",
                verification.failures
            );
        }
    }

    #[test]
    fn background_saturation_requires_real_pressure_quorum_and_latency_evidence() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        report
            .background_saturation
            .foreground_read_saturated_p99_ms = 130.0;
        report
            .background_saturation
            .metadata_quorum_checks_succeeded = 999;
        let node = &mut report.background_saturation.nodes[0];
        node.saturated
            .lanes
            .iter_mut()
            .find(|lane| lane.lane == ResourceLane::AntiEntropy)
            .unwrap()
            .usage = ResourceUsage::default();
        let three_active = ResourceUsage {
            memory_bytes: 3,
            io_bytes: 3,
            cpu_slots: 3,
            active: 3,
        };
        node.saturated.total = three_active;
        node.saturated.noncritical = three_active;
        node.saturated.background = three_active;
        report.background_saturation.artifact_paths = vec![report
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == ClusterCertificationArtifactKind::FailureTimeline)
            .unwrap()
            .relative_path
            .clone()];
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("metadata quorum during background saturation")));
        assert!(
            verification
                .failures
                .iter()
                .any(|failure| failure
                    .contains("background-saturated foreground read p99 regression"))
        );
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("AntiEntropy was not active")));
        assert!(verification.failures.iter().any(|failure| failure
            .contains("does not reference a verified BackgroundSaturation artifact")));
    }

    #[test]
    fn background_saturation_rejects_short_partial_or_malformed_trials() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, mut report, plan_path, report_path) = valid_bundle(root.path());
        report.background_saturation.completed_at_ms = 309_999;
        report
            .background_saturation
            .foreground_write_operations_attempted = 9_999;
        report
            .background_saturation
            .foreground_write_operations_succeeded = 9_999;
        report.background_saturation.nodes[4].node_id =
            report.background_saturation.nodes[0].node_id.clone();
        report.background_saturation.nodes[0].saturated.lanes.pop();
        report.background_saturation.nodes[1]
            .rejected_demand
            .memory_bytes = 5;
        fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

        let verification =
            verify_cluster_certification_bundle(root.path(), &plan_path, &report_path).unwrap();
        assert!(!verification.passed);
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("run continuously for at least")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("foreground write during background saturation")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("every planned node exactly once")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("every fixed resource lane exactly once")));
        assert!(verification
            .failures
            .iter()
            .any(|failure| failure.contains("exceeds the lane hard bound")));
    }

    #[test]
    fn collector_refuses_to_finalize_without_saturation_observation() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        fs::remove_file(&report_path).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
        initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            report.source_commit,
            false,
            report.started_at_ms,
            false,
        )
        .unwrap();
        for artifact in &report.artifacts {
            register_cluster_certification_artifact(
                root.path(),
                artifact.kind,
                &artifact.relative_path,
                report.completed_at_ms,
                false,
            )
            .unwrap();
        }
        let error =
            finalize_cluster_certification_bundle(root.path(), report.completed_at_ms, false)
                .unwrap_err();
        assert!(error
            .to_string()
            .contains("measurements observation is missing"));

        // Prove saturation is independently required after measurements exist.
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Measurements(report.measurements),
            report.completed_at_ms,
            false,
        )
        .unwrap();
        let error =
            finalize_cluster_certification_bundle(root.path(), report.completed_at_ms, false)
                .unwrap_err();
        assert!(error
            .to_string()
            .contains("background saturation observation is missing"));
    }

    #[test]
    fn resumable_collector_checkpoints_observations_and_finalizes_atomically() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        fs::remove_file(&report_path).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();

        let state = initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            report.source_commit.clone(),
            false,
            report.started_at_ms,
            true,
        )
        .unwrap();
        assert_eq!(state.hardware.len(), 0);

        for artifact in &report.artifacts {
            let registered = register_cluster_certification_artifact(
                root.path(),
                artifact.kind,
                &artifact.relative_path,
                report.completed_at_ms,
                true,
            )
            .unwrap();
            assert_eq!(&registered, artifact);
        }

        for hardware in report.hardware.clone() {
            record_cluster_certification_observation(
                root.path(),
                ClusterCertificationObservation::Hardware(hardware),
                2_000,
                true,
            )
            .unwrap();
        }
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Measurements(report.measurements.clone()),
            report.completed_at_ms,
            true,
        )
        .unwrap();
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::BackgroundSaturation(
                report.background_saturation.clone(),
            ),
            report.completed_at_ms,
            true,
        )
        .unwrap();
        for trial in report.failure_trials.clone() {
            record_cluster_certification_observation(
                root.path(),
                ClusterCertificationObservation::FailureTrial(trial),
                report.completed_at_ms,
                true,
            )
            .unwrap();
        }

        // Model a collector restart between phases. No in-memory state is
        // carried into the remaining records.
        let reopened = load_cluster_certification_state(root.path()).unwrap();
        assert_eq!(reopened.hardware.len(), 5);
        assert_eq!(reopened.failure_trials.len(), 8);
        assert!(reopened.measurements.is_some());
        assert!(reopened.background_saturation.is_some());

        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Restore(report.restore.clone()),
            report.completed_at_ms,
            true,
        )
        .unwrap();
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Expansion(report.expansion.clone()),
            report.completed_at_ms,
            true,
        )
        .unwrap();

        let verification =
            finalize_cluster_certification_bundle(root.path(), report.completed_at_ms, true)
                .unwrap();
        assert!(verification.passed, "{:?}", verification.failures);
        assert_eq!(verification.publication_manifest_sha256.len(), 64);
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .completed_at_ms
            .is_some());
        let manifest_path = root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST);
        let published_manifest = fs::read(&manifest_path).unwrap();
        fs::remove_file(&manifest_path).unwrap();
        let resumed =
            finalize_cluster_certification_bundle(root.path(), report.completed_at_ms, true)
                .unwrap();
        assert!(resumed.passed, "{:?}", resumed.failures);
        assert_eq!(fs::read(&manifest_path).unwrap(), published_manifest);
        fs::write(&manifest_path, b"{}").unwrap();
        let error =
            finalize_cluster_certification_bundle(root.path(), report.completed_at_ms, true)
                .unwrap_err();
        assert!(error.to_string().contains("publication manifest differs"));
        fs::write(&manifest_path, &published_manifest).unwrap();
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Measurements(report.measurements),
            6_000,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("finalized and immutable"));
    }

    #[test]
    fn captured_artifact_is_bounded_atomic_registered_and_restart_resumable() {
        let root = tempfile::tempdir().unwrap();
        let plan = live_plan(ClusterScaleProfile::OneTb, "capture-test", 1_000);
        let plan_path = root.path().join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
        save_cluster_certification_plan(&plan_path, &plan, true).unwrap();
        initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            "a".repeat(40),
            false,
            1_100,
            true,
        )
        .unwrap();

        let payload_path = root.path().join("source.ndjson");
        let mut payload = vec![b'x'; CLUSTER_CERTIFICATION_ARTIFACT_COPY_BUFFER_BYTES * 3 + 17];
        payload[0] = b'{';
        payload[1] = b'}';
        payload[2] = b'\n';
        fs::write(&payload_path, &payload).unwrap();
        let output = PathBuf::from("raw/resource-samples.bicdb-artifact");
        let evidence = capture_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::ResourceSamples,
            &payload_path,
            &output,
            ClusterCertificationRawArtifactPayloadFormat::JsonLines,
            1,
            1_200,
            1_300,
            1_400,
            true,
        )
        .unwrap();
        assert_eq!(
            evidence.size_bytes,
            MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES as u64 + payload.len() as u64
        );
        assert_eq!(
            load_cluster_certification_state(root.path())
                .unwrap()
                .artifacts[&output],
            evidence
        );

        let mut captured = File::open(root.path().join(&output)).unwrap();
        let mut header_line = vec![0; MAX_CLUSTER_CERTIFICATION_RAW_ARTIFACT_HEADER_BYTES];
        captured.read_exact(&mut header_line).unwrap();
        assert_eq!(header_line.last(), Some(&b'\n'));
        let header = serde_json::from_slice::<ClusterCertificationRawArtifactHeader>(
            &header_line[..header_line.len() - 1],
        )
        .unwrap();
        assert_eq!(header.run_id, plan.run_id);
        assert_eq!(header.record_count, 1);
        assert_eq!(header.payload_bytes, payload.len() as u64);
        assert_eq!(header.payload_sha256, sha256_bytes(&payload));
        let mut captured_payload = Vec::new();
        captured.read_to_end(&mut captured_payload).unwrap();
        assert_eq!(captured_payload, payload);

        // Model process loss after the immutable file was published but before
        // its collector-state checkpoint became durable.
        let mut interrupted = load_cluster_certification_state(root.path()).unwrap();
        interrupted.artifacts.clear();
        save_cluster_certification_state(root.path(), &interrupted, true).unwrap();
        fs::remove_file(&payload_path).unwrap();
        let resumed = capture_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::ResourceSamples,
            &payload_path,
            &output,
            ClusterCertificationRawArtifactPayloadFormat::JsonLines,
            1,
            1_200,
            1_300,
            1_500,
            true,
        )
        .unwrap();
        assert_eq!(resumed, evidence);
        assert_eq!(
            load_cluster_certification_state(root.path())
                .unwrap()
                .artifacts[&output],
            evidence
        );
    }

    #[test]
    fn artifact_capture_never_overwrites_and_rejects_forged_inputs() {
        let root = tempfile::tempdir().unwrap();
        let plan = live_plan(ClusterScaleProfile::OneTb, "capture-rejection-test", 1_000);
        let plan_path = root.path().join(DEFAULT_CLUSTER_CERTIFICATION_PLAN);
        save_cluster_certification_plan(&plan_path, &plan, false).unwrap();
        initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            "b".repeat(40),
            false,
            1_100,
            false,
        )
        .unwrap();
        let payload_path = root.path().join("payload.txt");
        fs::write(&payload_path, b"trusted measurements\n").unwrap();
        fs::create_dir(root.path().join("raw")).unwrap();
        let output = PathBuf::from("raw/hardware.bicdb-artifact");
        fs::write(root.path().join(&output), b"do not overwrite").unwrap();

        let error = capture_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::Hardware,
            &payload_path,
            &output,
            ClusterCertificationRawArtifactPayloadFormat::Text,
            1,
            1_200,
            1_300,
            1_400,
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("raw artifact"));
        assert_eq!(
            fs::read(root.path().join(&output)).unwrap(),
            b"do not overwrite"
        );
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .artifacts
            .is_empty());

        for (records, started, completed, now, expected) in [
            (0, 1_200, 1_300, 1_400, "record count"),
            (1, 1_000, 1_300, 1_400, "timestamps"),
            (1, 1_200, 1_500, 1_400, "timestamps"),
        ] {
            let error = capture_cluster_certification_artifact(
                root.path(),
                ClusterCertificationArtifactKind::Checksums,
                &payload_path,
                "checksums.bicdb-artifact",
                ClusterCertificationRawArtifactPayloadFormat::Text,
                records,
                started,
                completed,
                now,
                false,
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
        assert!(capture_cluster_certification_artifact(
            root.path(),
            ClusterCertificationArtifactKind::Checksums,
            &payload_path,
            "../escape.bicdb-artifact",
            ClusterCertificationRawArtifactPayloadFormat::Text,
            1,
            1_200,
            1_300,
            1_400,
            false,
        )
        .unwrap_err()
        .to_string()
        .contains("safe path"));
    }

    #[test]
    fn invalid_observations_fail_before_the_durable_checkpoint() {
        let root = tempfile::tempdir().unwrap();
        let (_plan, report, plan_path, report_path) = valid_bundle(root.path());
        fs::remove_file(&report_path).unwrap();
        fs::remove_file(root.path().join(DEFAULT_CLUSTER_CERTIFICATION_MANIFEST)).unwrap();
        initialize_cluster_certification_bundle(
            root.path(),
            &plan_path,
            report.source_commit.clone(),
            false,
            report.started_at_ms,
            true,
        )
        .unwrap();
        for artifact in &report.artifacts {
            register_cluster_certification_artifact(
                root.path(),
                artifact.kind,
                &artifact.relative_path,
                report.completed_at_ms,
                true,
            )
            .unwrap();
        }

        let mut bad_hardware = report.hardware[0].clone();
        bad_hardware.memory_bytes = 1;
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Hardware(bad_hardware),
            report.completed_at_ms,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("insufficient or zero capacity"));
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .hardware
            .is_empty());

        let mut bad_measurements = report.measurements.clone();
        bad_measurements.foreground_rebalance_p99_ms = 200.0;
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Measurements(bad_measurements),
            report.completed_at_ms,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("foreground p99 regression"));
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .measurements
            .is_none());
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Measurements(report.measurements.clone()),
            report.completed_at_ms,
            true,
        )
        .unwrap();

        let mut bad_trial = report.failure_trials[0].clone();
        bad_trial.affected_ranges = 0;
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::FailureTrial(bad_trial),
            report.completed_at_ms,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("affected-range repair"));
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .failure_trials
            .is_empty());

        let mut bad_restore = report.restore.clone();
        bad_restore.restored_node_incarnation = bad_restore.source_node_incarnation;
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Restore(bad_restore),
            report.completed_at_ms,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("higher incarnation"));
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .restore
            .is_none());
        record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Restore(report.restore.clone()),
            report.completed_at_ms,
            true,
        )
        .unwrap();

        let mut bad_expansion = report.expansion.clone();
        bad_expansion.nodes_after = bad_expansion.nodes_before;
        let error = record_cluster_certification_observation(
            root.path(),
            ClusterCertificationObservation::Expansion(bad_expansion),
            report.completed_at_ms,
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("topology generation"));
        assert!(load_cluster_certification_state(root.path())
            .unwrap()
            .expansion
            .is_none());
    }
}
