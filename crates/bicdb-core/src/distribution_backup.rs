//! Quorum-certified cluster backup cuts.
//!
//! A collection of node archives is not a cluster backup unless every range is
//! tied to one stable topology generation and a current-voter quorum observed
//! the same resolved data-log cut. This module is the fail-closed contract
//! between cluster orchestration and the bounded `BICBAK03` node archives.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, MetadataMemberRole, RangeId,
    RangeReplicaRole, SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_consensus::{MetadataConsensusRole, MetadataConsensusStatus};
use crate::distribution_range_consensus::RangeWriteProgress;
use crate::distribution_supervisor::ClusterBackupMetadata;
use crate::error::{BicDbError, Result};

pub const CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_BACKUP_PLAN: &str = "cluster-backup-plan.json";
pub const DEFAULT_CLUSTER_BACKUP_CERTIFICATE: &str = "cluster-backup-certificate.json";
pub const DEFAULT_CLUSTER_NODE_BACKUP_ARTIFACT: &str = "cluster-node-backup-artifact.json";
pub const MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CLUSTER_BACKUP_PLAN_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

fn backup_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("cluster backup: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupLimits {
    pub max_ranges: usize,
    pub max_nodes: usize,
    pub max_range_observations: usize,
}

impl Default for ClusterBackupLimits {
    fn default() -> Self {
        Self {
            max_ranges: 1_000_000,
            max_nodes: 10_000,
            max_range_observations: 3_000_000,
        }
    }
}

impl ClusterBackupLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_ranges == 0
            || self.max_ranges > 1_000_000
            || self.max_nodes == 0
            || self.max_nodes > 100_000
            || self.max_range_observations == 0
            || self.max_range_observations > 10_000_000
        {
            return Err(backup_error(
                "limits require 1..=1,000,000 ranges, 1..=100,000 nodes, and 1..=10,000,000 observations",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupPlan {
    pub format_version: u32,
    pub plan_id: Uuid,
    pub cluster_id: ClusterId,
    pub topology_generation: u64,
    pub topology_sha256: String,
    pub metadata_term: u64,
    pub metadata_commit_index: u64,
    pub target_timestamp: Option<i64>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub range_epochs: BTreeMap<RangeId, u64>,
    pub checksum_sha256: String,
}

impl ClusterBackupPlan {
    pub fn create(
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        target_timestamp: Option<i64>,
        created_at_ms: u64,
        ttl_ms: u64,
        limits: &ClusterBackupLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if ttl_ms == 0 || ttl_ms > MAX_CLUSTER_BACKUP_PLAN_TTL_MS {
            return Err(backup_error(format!(
                "plan TTL must be within 1..={MAX_CLUSTER_BACKUP_PLAN_TTL_MS}ms"
            )));
        }
        let expires_at_ms = created_at_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| backup_error("plan expiration overflow"))?;
        let topology_metadata = ClusterBackupMetadata::capture(topology)?;
        let mut plan = Self {
            format_version: CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION,
            plan_id: Uuid::now_v7(),
            cluster_id: topology.cluster_id.clone(),
            topology_generation: topology.generation,
            topology_sha256: topology_metadata.topology_sha256,
            metadata_term: metadata.current_term,
            metadata_commit_index: metadata.commit_index,
            target_timestamp,
            created_at_ms,
            expires_at_ms,
            range_epochs: topology_metadata.range_epochs,
            checksum_sha256: String::new(),
        };
        plan.checksum_sha256 = plan.calculate_checksum()?;
        plan.validate_against(topology, metadata, created_at_ms, limits)?;
        Ok(plan)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            plan_id: Uuid,
            cluster_id: &'a ClusterId,
            topology_generation: u64,
            topology_sha256: &'a str,
            metadata_term: u64,
            metadata_commit_index: u64,
            target_timestamp: Option<i64>,
            created_at_ms: u64,
            expires_at_ms: u64,
            range_epochs: &'a BTreeMap<RangeId, u64>,
        }
        let payload = Payload {
            format_version: self.format_version,
            plan_id: self.plan_id,
            cluster_id: &self.cluster_id,
            topology_generation: self.topology_generation,
            topology_sha256: &self.topology_sha256,
            metadata_term: self.metadata_term,
            metadata_commit_index: self.metadata_commit_index,
            target_timestamp: self.target_timestamp,
            created_at_ms: self.created_at_ms,
            expires_at_ms: self.expires_at_ms,
            range_epochs: &self.range_epochs,
        };
        sha256_json(&payload)
    }

    pub fn validate_against(
        &self,
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        now_ms: u64,
        limits: &ClusterBackupLimits,
    ) -> Result<()> {
        self.validate_topology_snapshot(topology, now_ms, limits)?;
        validate_metadata_authority(topology, metadata)?;
        if self.metadata_term != metadata.current_term
            || self.metadata_commit_index != metadata.commit_index
        {
            return Err(backup_error(
                "plan metadata authority differs from the current committed leader",
            ));
        }
        Ok(())
    }

    /// Validate the immutable topology and lifetime captured by a plan without
    /// requiring a live elected leader. Restore admission uses this after a
    /// whole-cluster outage, before any restored node is allowed to campaign or
    /// serve traffic.
    pub fn validate_topology_snapshot(
        &self,
        topology: &ClusterTopology,
        observed_at_ms: u64,
        limits: &ClusterBackupLimits,
    ) -> Result<()> {
        limits.validate()?;
        validate_stable_topology(topology, limits)?;
        if self.format_version != CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION
            || self.plan_id.is_nil()
            || self.cluster_id != topology.cluster_id
            || self.topology_generation != topology.generation
            || self.created_at_ms >= self.expires_at_ms
            || self.expires_at_ms.saturating_sub(self.created_at_ms)
                > MAX_CLUSTER_BACKUP_PLAN_TTL_MS
            || observed_at_ms < self.created_at_ms
            || observed_at_ms > self.expires_at_ms
            || self.range_epochs.len() > limits.max_ranges
        {
            return Err(backup_error(
                "plan identity, topology, lifetime, or bounds are invalid",
            ));
        }
        let captured = ClusterBackupMetadata::capture(topology)?;
        if self.topology_sha256 != captured.topology_sha256
            || self.range_epochs != captured.range_epochs
        {
            return Err(backup_error(
                "plan topology hash or range epochs differ from committed metadata",
            ));
        }
        validate_sha256("plan topology", &self.topology_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(backup_error("plan checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeBackupArtifact {
    pub format_version: u32,
    pub plan_id: Uuid,
    pub node_id: ClusterNodeId,
    pub backup_id: Uuid,
    pub backup_manifest_sha256: String,
    pub encrypted_artifact_sha256: String,
    pub schema_sha256: String,
    pub topology_generation: u64,
    pub topology_sha256: String,
    pub metadata_commit_index: u64,
    pub captured_at_ms: u64,
    pub ranges: Vec<RangeWriteProgress>,
    pub checksum_sha256: String,
}

impl ClusterNodeBackupArtifact {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        plan: &ClusterBackupPlan,
        node_id: ClusterNodeId,
        backup_id: Uuid,
        backup_manifest_sha256: String,
        encrypted_artifact_sha256: String,
        schema_sha256: String,
        captured_at_ms: u64,
        mut ranges: Vec<RangeWriteProgress>,
        limits: &ClusterBackupLimits,
    ) -> Result<Self> {
        ranges.sort_by_key(|progress| progress.range_id);
        let mut artifact = Self {
            format_version: CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION,
            plan_id: plan.plan_id,
            node_id,
            backup_id,
            backup_manifest_sha256,
            encrypted_artifact_sha256,
            schema_sha256,
            topology_generation: plan.topology_generation,
            topology_sha256: plan.topology_sha256.clone(),
            metadata_commit_index: plan.metadata_commit_index,
            captured_at_ms,
            ranges,
            checksum_sha256: String::new(),
        };
        artifact.checksum_sha256 = artifact.calculate_checksum()?;
        artifact.validate_against(plan, limits)?;
        Ok(artifact)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            plan_id: Uuid,
            node_id: &'a ClusterNodeId,
            backup_id: Uuid,
            backup_manifest_sha256: &'a str,
            encrypted_artifact_sha256: &'a str,
            schema_sha256: &'a str,
            topology_generation: u64,
            topology_sha256: &'a str,
            metadata_commit_index: u64,
            captured_at_ms: u64,
            ranges: &'a [RangeWriteProgress],
        }
        let payload = Payload {
            format_version: self.format_version,
            plan_id: self.plan_id,
            node_id: &self.node_id,
            backup_id: self.backup_id,
            backup_manifest_sha256: &self.backup_manifest_sha256,
            encrypted_artifact_sha256: &self.encrypted_artifact_sha256,
            schema_sha256: &self.schema_sha256,
            topology_generation: self.topology_generation,
            topology_sha256: &self.topology_sha256,
            metadata_commit_index: self.metadata_commit_index,
            captured_at_ms: self.captured_at_ms,
            ranges: &self.ranges,
        };
        sha256_json(&payload)
    }

    pub fn validate_against(
        &self,
        plan: &ClusterBackupPlan,
        limits: &ClusterBackupLimits,
    ) -> Result<()> {
        limits.validate()?;
        if self.format_version != CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION
            || self.plan_id != plan.plan_id
            || self.backup_id.is_nil()
            || self.topology_generation != plan.topology_generation
            || self.topology_sha256 != plan.topology_sha256
            || self.metadata_commit_index != plan.metadata_commit_index
            || self.captured_at_ms < plan.created_at_ms
            || self.captured_at_ms > plan.expires_at_ms
            || self.ranges.len() > limits.max_ranges
        {
            return Err(backup_error(format!(
                "node artifact {} does not belong to the active plan",
                self.node_id
            )));
        }
        validate_sha256("backup manifest", &self.backup_manifest_sha256)?;
        validate_sha256("encrypted artifact", &self.encrypted_artifact_sha256)?;
        validate_sha256("artifact schema", &self.schema_sha256)?;
        validate_sha256("artifact topology", &self.topology_sha256)?;
        let mut range_ids = BTreeSet::new();
        let mut previous_range_id = None;
        for progress in &self.ranges {
            if progress.node_id != self.node_id
                || !range_ids.insert(progress.range_id)
                || previous_range_id.is_some_and(|previous| previous >= progress.range_id)
                || progress.current_epoch == 0
                || progress.resolved_through > progress.last_index
                || progress.compacted_through > progress.resolved_through
                || progress.last_index != progress.resolved_through
            {
                return Err(backup_error(format!(
                    "node artifact {} has duplicate, unresolved, or invalid range progress",
                    self.node_id
                )));
            }
            previous_range_id = Some(progress.range_id);
        }
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(backup_error(format!(
                "node artifact {} checksum mismatch",
                self.node_id
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeBackupBarrier {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub required_quorum: usize,
    pub artifact_nodes: Vec<ClusterNodeId>,
    pub artifact_backup_ids: BTreeMap<ClusterNodeId, Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupCertificate {
    pub format_version: u32,
    pub certificate_id: Uuid,
    pub certified_at_ms: u64,
    pub plan: ClusterBackupPlan,
    pub artifacts: Vec<ClusterNodeBackupArtifact>,
    pub range_barriers: BTreeMap<RangeId, RangeBackupBarrier>,
    pub checksum_sha256: String,
}

impl ClusterBackupCertificate {
    pub fn certify(
        plan: ClusterBackupPlan,
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        artifacts: Vec<ClusterNodeBackupArtifact>,
        certified_at_ms: u64,
        limits: &ClusterBackupLimits,
    ) -> Result<Self> {
        let (artifacts, range_barriers) = validate_and_build_barriers(
            &plan,
            topology,
            metadata,
            &artifacts,
            certified_at_ms,
            limits,
        )?;

        let mut certificate = Self {
            format_version: CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION,
            certificate_id: Uuid::now_v7(),
            certified_at_ms,
            plan,
            artifacts,
            range_barriers,
            checksum_sha256: String::new(),
        };
        certificate.checksum_sha256 = certificate.calculate_checksum()?;
        Ok(certificate)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            certificate_id: Uuid,
            certified_at_ms: u64,
            plan: &'a ClusterBackupPlan,
            artifacts: &'a [ClusterNodeBackupArtifact],
            range_barriers: &'a BTreeMap<RangeId, RangeBackupBarrier>,
        }
        let payload = Payload {
            format_version: self.format_version,
            certificate_id: self.certificate_id,
            certified_at_ms: self.certified_at_ms,
            plan: &self.plan,
            artifacts: &self.artifacts,
            range_barriers: &self.range_barriers,
        };
        sha256_json(&payload)
    }

    pub fn validate(
        &self,
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        limits: &ClusterBackupLimits,
    ) -> Result<()> {
        if self.format_version != CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION
            || self.certificate_id.is_nil()
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(backup_error("certificate identity or checksum is invalid"));
        }
        let (artifacts, range_barriers) = validate_and_build_barriers(
            &self.plan,
            topology,
            metadata,
            &self.artifacts,
            self.certified_at_ms,
            limits,
        )?;
        if artifacts != self.artifacts || range_barriers != self.range_barriers {
            return Err(backup_error(
                "certificate artifacts or range barriers are not canonical",
            ));
        }
        Ok(())
    }

    /// Validate a completed certificate against its restored topology without
    /// treating one offline node's persisted consensus role as a new live
    /// leader election. Node-local consensus watermarks are verified
    /// separately by restore admission before readiness is published.
    pub fn validate_for_restore(
        &self,
        topology: &ClusterTopology,
        limits: &ClusterBackupLimits,
    ) -> Result<()> {
        if self.format_version != CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION
            || self.certificate_id.is_nil()
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(backup_error("certificate identity or checksum is invalid"));
        }
        let (artifacts, range_barriers) = validate_and_build_barriers_inner(
            &self.plan,
            topology,
            None,
            &self.artifacts,
            self.certified_at_ms,
            limits,
        )?;
        if artifacts != self.artifacts || range_barriers != self.range_barriers {
            return Err(backup_error(
                "certificate artifacts or range barriers are not canonical",
            ));
        }
        Ok(())
    }
}

fn validate_and_build_barriers(
    plan: &ClusterBackupPlan,
    topology: &ClusterTopology,
    metadata: &MetadataConsensusStatus,
    artifacts: &[ClusterNodeBackupArtifact],
    certified_at_ms: u64,
    limits: &ClusterBackupLimits,
) -> Result<(
    Vec<ClusterNodeBackupArtifact>,
    BTreeMap<RangeId, RangeBackupBarrier>,
)> {
    validate_and_build_barriers_inner(
        plan,
        topology,
        Some(metadata),
        artifacts,
        certified_at_ms,
        limits,
    )
}

fn validate_and_build_barriers_inner(
    plan: &ClusterBackupPlan,
    topology: &ClusterTopology,
    metadata: Option<&MetadataConsensusStatus>,
    artifacts: &[ClusterNodeBackupArtifact],
    certified_at_ms: u64,
    limits: &ClusterBackupLimits,
) -> Result<(
    Vec<ClusterNodeBackupArtifact>,
    BTreeMap<RangeId, RangeBackupBarrier>,
)> {
    if let Some(metadata) = metadata {
        plan.validate_against(topology, metadata, certified_at_ms, limits)?;
    } else {
        plan.validate_topology_snapshot(topology, certified_at_ms, limits)?;
    }
    if artifacts.is_empty() || artifacts.len() > limits.max_nodes {
        return Err(backup_error(
            "node artifact count is empty or exceeds its bound",
        ));
    }
    let mut artifacts = artifacts.to_vec();
    artifacts.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    let mut seen = BTreeSet::new();
    let mut observations = 0_usize;
    let mut observations_by_range =
        BTreeMap::<RangeId, BTreeMap<u64, Vec<(ClusterNodeId, Uuid)>>>::new();
    for artifact in &artifacts {
        artifact.validate_against(plan, limits)?;
        if artifact.captured_at_ms > certified_at_ms {
            return Err(backup_error(format!(
                "node artifact {} was captured after certificate time",
                artifact.node_id
            )));
        }
        if !seen.insert(artifact.node_id.clone()) {
            return Err(backup_error(format!(
                "node {} submitted more than one artifact",
                artifact.node_id
            )));
        }
        let node = topology.nodes.get(&artifact.node_id).ok_or_else(|| {
            backup_error(format!(
                "artifact came from unknown node {}",
                artifact.node_id
            ))
        })?;
        if node.lifecycle != ClusterNodeLifecycle::Active {
            return Err(backup_error(format!(
                "artifact node {} is not active",
                artifact.node_id
            )));
        }
        if node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(&artifact.schema_sha256) {
            return Err(backup_error(format!(
                "artifact node {} schema differs from committed placement metadata",
                artifact.node_id
            )));
        }
        observations = observations
            .checked_add(artifact.ranges.len())
            .ok_or_else(|| backup_error("range observation count overflow"))?;
        if observations > limits.max_range_observations {
            return Err(backup_error(
                "range observations exceed their configured bound",
            ));
        }
        for progress in &artifact.ranges {
            let range = topology.range_by_id(progress.range_id).ok_or_else(|| {
                backup_error(format!(
                    "artifact references unknown range {}",
                    progress.range_id
                ))
            })?;
            if progress.current_epoch != range.epoch
                || !range.replicas.iter().any(|replica| {
                    replica.node_id == artifact.node_id && replica.role == RangeReplicaRole::Voter
                })
            {
                return Err(backup_error(format!(
                    "artifact node {} is not a current voter for range {} epoch {}",
                    artifact.node_id, progress.range_id, progress.current_epoch
                )));
            }
            observations_by_range
                .entry(progress.range_id)
                .or_default()
                .entry(progress.resolved_through)
                .or_default()
                .push((artifact.node_id.clone(), artifact.backup_id));
        }
    }
    let mut barriers = BTreeMap::new();
    for range in topology.ranges.values() {
        let quorum = range.voter_count() / 2 + 1;
        let groups = observations_by_range.remove(&range.id).unwrap_or_default();
        let (resolved_through, mut group) = groups
            .into_iter()
            .rev()
            .find(|(_, group)| {
                group.len() >= quorum && group.iter().any(|(node_id, _)| node_id == &range.leader)
            })
            .ok_or_else(|| {
                backup_error(format!(
                    "range {} has no leader-inclusive voter quorum at one resolved cut",
                    range.id
                ))
            })?;
        group.sort_by(|left, right| left.0.cmp(&right.0));
        barriers.insert(
            range.id,
            RangeBackupBarrier {
                range_id: range.id,
                range_epoch: range.epoch,
                resolved_through,
                required_quorum: quorum,
                artifact_nodes: group.iter().map(|(node_id, _)| node_id.clone()).collect(),
                artifact_backup_ids: group.into_iter().collect(),
            },
        );
    }
    Ok((artifacts, barriers))
}

fn validate_metadata_authority(
    topology: &ClusterTopology,
    metadata: &MetadataConsensusStatus,
) -> Result<()> {
    if metadata.cluster_id != topology.cluster_id
        || metadata.topology_generation != topology.generation
        || metadata.role != MetadataConsensusRole::Leader
        || metadata.leader_id.as_ref() != Some(&metadata.node_id)
        || metadata.current_term == 0
        || metadata.commit_index > metadata.last_log_index
    {
        return Err(backup_error(
            "backup plan requires the current committed metadata leader",
        ));
    }
    let expected_voters = topology
        .nodes
        .values()
        .filter(|node| {
            node.lifecycle != ClusterNodeLifecycle::Decommissioned
                && node.metadata_role == MetadataMemberRole::Voter
        })
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let observed_voters = metadata.voters.iter().cloned().collect::<BTreeSet<_>>();
    let expected_learners = topology
        .nodes
        .values()
        .filter(|node| {
            node.lifecycle != ClusterNodeLifecycle::Decommissioned
                && node.metadata_role == MetadataMemberRole::Learner
        })
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let observed_learners = metadata.learners.iter().cloned().collect::<BTreeSet<_>>();
    if observed_voters != expected_voters
        || observed_learners != expected_learners
        || metadata.voters.len() != observed_voters.len()
        || metadata.learners.len() != observed_learners.len()
        || !observed_voters.contains(&metadata.node_id)
    {
        return Err(backup_error(
            "metadata membership differs from the committed stable topology",
        ));
    }
    Ok(())
}

fn validate_stable_topology(
    topology: &ClusterTopology,
    limits: &ClusterBackupLimits,
) -> Result<()> {
    topology.validate()?;
    if topology.ranges.is_empty() || topology.ranges.len() > limits.max_ranges {
        return Err(backup_error(
            "topology range count is empty or exceeds its bound",
        ));
    }
    if topology.nodes.len() > limits.max_nodes {
        return Err(backup_error("topology node count exceeds its bound"));
    }
    if topology
        .relocations
        .values()
        .any(|relocation| relocation.is_active())
    {
        return Err(backup_error(
            "cannot plan a cluster backup during active relocation",
        ));
    }
    Ok(())
}

fn validate_sha256(kind: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(backup_error(format!(
            "{kind} SHA-256 must be 64 lowercase hexadecimal digits"
        )));
    }
    Ok(())
}

pub fn save_cluster_backup_plan(
    path: impl AsRef<Path>,
    plan: &ClusterBackupPlan,
    fsync: bool,
) -> Result<()> {
    if plan.calculate_checksum()? != plan.checksum_sha256 {
        return Err(backup_error(
            "refusing to persist a plan with a bad checksum",
        ));
    }
    write_control_file_atomic(path.as_ref(), plan, fsync)
}

pub fn load_cluster_backup_plan(path: impl AsRef<Path>) -> Result<ClusterBackupPlan> {
    let plan: ClusterBackupPlan = serde_json::from_slice(&read_control_file(path.as_ref())?)?;
    if plan.calculate_checksum()? != plan.checksum_sha256 {
        return Err(backup_error("persisted plan checksum mismatch"));
    }
    Ok(plan)
}

pub fn save_cluster_backup_certificate(
    path: impl AsRef<Path>,
    certificate: &ClusterBackupCertificate,
    fsync: bool,
) -> Result<()> {
    if certificate.calculate_checksum()? != certificate.checksum_sha256 {
        return Err(backup_error(
            "refusing to persist a certificate with a bad checksum",
        ));
    }
    write_control_file_atomic(path.as_ref(), certificate, fsync)
}

pub fn save_cluster_node_backup_artifact(
    path: impl AsRef<Path>,
    artifact: &ClusterNodeBackupArtifact,
    fsync: bool,
) -> Result<()> {
    if artifact.calculate_checksum()? != artifact.checksum_sha256 {
        return Err(backup_error(
            "refusing to persist a node artifact with a bad checksum",
        ));
    }
    write_control_file_atomic(path.as_ref(), artifact, fsync)
}

pub fn load_cluster_node_backup_artifact(
    path: impl AsRef<Path>,
) -> Result<ClusterNodeBackupArtifact> {
    let artifact: ClusterNodeBackupArtifact =
        serde_json::from_slice(&read_control_file(path.as_ref())?)?;
    if artifact.calculate_checksum()? != artifact.checksum_sha256 {
        return Err(backup_error("persisted node artifact checksum mismatch"));
    }
    Ok(artifact)
}

pub fn load_cluster_backup_certificate(path: impl AsRef<Path>) -> Result<ClusterBackupCertificate> {
    let certificate: ClusterBackupCertificate =
        serde_json::from_slice(&read_control_file(path.as_ref())?)?;
    if certificate.calculate_checksum()? != certificate.checksum_sha256 {
        return Err(backup_error("persisted certificate checksum mismatch"));
    }
    Ok(certificate)
}

fn read_control_file(path: &Path) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES {
        return Err(backup_error(format!(
            "control file {} is {} bytes, exceeding the {} byte bound",
            path.display(),
            metadata.len(),
            MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES
        )));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES {
        return Err(backup_error(format!(
            "control file {} grew past the {} byte bound while reading",
            path.display(),
            MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES
        )));
    }
    Ok(bytes)
}

fn write_control_file_atomic(path: &Path, value: &impl Serialize, fsync: bool) -> Result<()> {
    struct BoundedWriter(Vec<u8>);

    impl Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next_len = self
                .0
                .len()
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("cluster backup control size overflow"))?;
            if next_len as u64 > MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES {
                return Err(std::io::Error::other(format!(
                    "cluster backup control file exceeds {MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES} bytes"
                )));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = BoundedWriter(Vec::new());
    serde_json::to_writer_pretty(&mut writer, value)?;
    crate::storage::write_atomic(path, &writer.0, fsync)
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    struct HashWriter(Sha256);

    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = HashWriter(Sha256::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(hex::encode(writer.0.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{
        ClusterNode, DistributionConfig, DistributionStore, RangeReplica, ReplicaId,
    };

    const CREATED_AT_MS: u64 = 10_000;
    const CERTIFIED_AT_MS: u64 = 10_500;

    fn fixture() -> (
        tempfile::TempDir,
        ClusterTopology,
        MetadataConsensusStatus,
        Vec<ClusterNodeId>,
        RangeId,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let nodes = ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|value| ClusterNodeId::new(value).unwrap())
            .collect::<Vec<_>>();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("backup-test").unwrap(),
            node_id: nodes[0].clone(),
            node_address: "127.0.0.1:9301".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor: 3,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        let mut topology = store.topology().clone();
        for (index, node_id) in nodes.iter().enumerate().skip(1) {
            let node = ClusterNode::new(
                node_id.clone(),
                format!("127.0.0.1:{}", 9301 + index),
                1,
                1_000_000,
                index as u64 + 1,
            )
            .unwrap();
            topology.nodes.insert(node_id.clone(), node);
        }
        let schema_sha256 = "a".repeat(64);
        for node in topology.nodes.values_mut() {
            node.labels.insert(
                SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                schema_sha256.clone(),
            );
        }
        let range = topology.ranges.values_mut().next().unwrap();
        let range_id = range.id;
        for (index, node_id) in nodes.iter().enumerate().skip(1) {
            range.replicas.push(RangeReplica {
                id: ReplicaId::new(index as u64 + 1).unwrap(),
                node_id: node_id.clone(),
                role: RangeReplicaRole::Voter,
            });
        }
        topology.next_replica_id = 4;
        topology.validate().unwrap();
        let metadata = MetadataConsensusStatus {
            cluster_id: topology.cluster_id.clone(),
            node_id: nodes[0].clone(),
            role: MetadataConsensusRole::Leader,
            current_term: 3,
            voted_for: Some(nodes[0].clone()),
            leader_id: Some(nodes[0].clone()),
            commit_index: 5,
            last_log_index: 5,
            last_log_term: 3,
            topology_generation: topology.generation,
            voters: nodes.clone(),
            learners: Vec::new(),
        };
        (directory, topology, metadata, nodes, range_id)
    }

    fn plan(topology: &ClusterTopology, metadata: &MetadataConsensusStatus) -> ClusterBackupPlan {
        ClusterBackupPlan::create(
            topology,
            metadata,
            Some(123),
            CREATED_AT_MS,
            1_000,
            &ClusterBackupLimits::default(),
        )
        .unwrap()
    }

    fn artifact(
        plan: &ClusterBackupPlan,
        node_id: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        resolved_through: u64,
    ) -> ClusterNodeBackupArtifact {
        ClusterNodeBackupArtifact::create(
            plan,
            node_id.clone(),
            Uuid::now_v7(),
            "b".repeat(64),
            "c".repeat(64),
            "a".repeat(64),
            CERTIFIED_AT_MS,
            vec![RangeWriteProgress {
                node_id: node_id.clone(),
                range_id,
                current_epoch: range_epoch,
                last_index: resolved_through,
                resolved_through,
                compacted_through: resolved_through.saturating_sub(1),
            }],
            &ClusterBackupLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn certifies_one_exact_leader_inclusive_quorum_cut() {
        let (_directory, topology, metadata, nodes, range_id) = fixture();
        let plan = plan(&topology, &metadata);
        let epoch = topology.range_by_id(range_id).unwrap().epoch;
        let artifacts = nodes
            .iter()
            .map(|node_id| artifact(&plan, node_id, range_id, epoch, 7))
            .collect();

        let certificate = ClusterBackupCertificate::certify(
            plan,
            &topology,
            &metadata,
            artifacts,
            CERTIFIED_AT_MS,
            &ClusterBackupLimits::default(),
        )
        .unwrap();

        let barrier = certificate.range_barriers.get(&range_id).unwrap();
        assert_eq!(barrier.resolved_through, 7);
        assert_eq!(barrier.required_quorum, 2);
        assert_eq!(barrier.artifact_nodes, nodes);
        certificate
            .validate(&topology, &metadata, &ClusterBackupLimits::default())
            .unwrap();
        certificate
            .validate_for_restore(&topology, &ClusterBackupLimits::default())
            .unwrap();

        let mut offline_status = metadata.clone();
        offline_status.role = MetadataConsensusRole::Follower;
        assert!(certificate
            .validate(&topology, &offline_status, &ClusterBackupLimits::default())
            .is_err());
        certificate
            .validate_for_restore(&topology, &ClusterBackupLimits::default())
            .unwrap();

        let mut wrong_topology = topology.clone();
        wrong_topology.generation += 1;
        assert!(certificate
            .validate_for_restore(&wrong_topology, &ClusterBackupLimits::default())
            .is_err());
    }

    #[test]
    fn rejects_missing_or_divergent_leader_quorum() {
        let (_directory, topology, metadata, nodes, range_id) = fixture();
        let plan = plan(&topology, &metadata);
        let epoch = topology.range_by_id(range_id).unwrap().epoch;

        let missing_quorum = vec![artifact(&plan, &nodes[0], range_id, epoch, 7)];
        assert!(ClusterBackupCertificate::certify(
            plan.clone(),
            &topology,
            &metadata,
            missing_quorum,
            CERTIFIED_AT_MS,
            &ClusterBackupLimits::default(),
        )
        .is_err());

        let divergent = vec![
            artifact(&plan, &nodes[0], range_id, epoch, 8),
            artifact(&plan, &nodes[1], range_id, epoch, 7),
            artifact(&plan, &nodes[2], range_id, epoch, 7),
        ];
        assert!(ClusterBackupCertificate::certify(
            plan,
            &topology,
            &metadata,
            divergent,
            CERTIFIED_AT_MS,
            &ClusterBackupLimits::default(),
        )
        .is_err());
    }

    #[test]
    fn rejects_non_leader_plans_and_tampered_artifacts() {
        let (_directory, topology, mut metadata, nodes, range_id) = fixture();
        metadata.role = MetadataConsensusRole::Follower;
        assert!(ClusterBackupPlan::create(
            &topology,
            &metadata,
            None,
            CREATED_AT_MS,
            1_000,
            &ClusterBackupLimits::default(),
        )
        .is_err());

        metadata.role = MetadataConsensusRole::Leader;
        let plan = plan(&topology, &metadata);
        let epoch = topology.range_by_id(range_id).unwrap().epoch;
        let mut tampered = artifact(&plan, &nodes[0], range_id, epoch, 7);
        tampered.ranges[0].resolved_through = 6;
        assert!(tampered
            .validate_against(&plan, &ClusterBackupLimits::default())
            .is_err());
    }

    #[test]
    fn atomically_persists_and_detects_control_file_tampering() {
        let (directory, topology, metadata, nodes, range_id) = fixture();
        let plan = plan(&topology, &metadata);
        let epoch = topology.range_by_id(range_id).unwrap().epoch;
        let artifacts = nodes
            .iter()
            .take(2)
            .map(|node_id| artifact(&plan, node_id, range_id, epoch, 7))
            .collect::<Vec<_>>();
        let node_artifact = artifacts[0].clone();
        let certificate = ClusterBackupCertificate::certify(
            plan.clone(),
            &topology,
            &metadata,
            artifacts,
            CERTIFIED_AT_MS,
            &ClusterBackupLimits::default(),
        )
        .unwrap();
        let plan_path = directory.path().join(DEFAULT_CLUSTER_BACKUP_PLAN);
        let certificate_path = directory.path().join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE);
        let artifact_path = directory.path().join(DEFAULT_CLUSTER_NODE_BACKUP_ARTIFACT);
        save_cluster_backup_plan(&plan_path, &plan, false).unwrap();
        save_cluster_node_backup_artifact(&artifact_path, &node_artifact, false).unwrap();
        save_cluster_backup_certificate(&certificate_path, &certificate, false).unwrap();
        assert_eq!(load_cluster_backup_plan(&plan_path).unwrap(), plan);
        assert_eq!(
            load_cluster_node_backup_artifact(&artifact_path).unwrap(),
            node_artifact
        );
        assert_eq!(
            load_cluster_backup_certificate(&certificate_path).unwrap(),
            certificate
        );

        let mut bytes = std::fs::read(&certificate_path).unwrap();
        let offset = bytes.iter().position(|byte| *byte == b'b').unwrap();
        bytes[offset] = b'd';
        std::fs::write(&certificate_path, bytes).unwrap();
        assert!(load_cluster_backup_certificate(&certificate_path).is_err());

        let oversized_path = directory.path().join("oversized-control.json");
        std::fs::File::create(&oversized_path)
            .unwrap()
            .set_len(MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES + 1)
            .unwrap();
        assert!(load_cluster_backup_plan(&oversized_path).is_err());
    }
}
