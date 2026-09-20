//! Fail-closed admission for whole-cluster restores.
//!
//! A restored directory is not ready merely because its files decrypt. This
//! module binds each archive and restored node back to one quorum-certified
//! backup, verifies database/schema/topology/consensus/range-fence state while
//! the nodes are offline, and emits one checksummed admission record. Cluster
//! services must require that record before campaigning or serving traffic.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::backup::verify_backup_from_reader;
use crate::db::{BicDb, DbConfig};
use crate::distribution::{
    load_distribution_config, ClusterId, ClusterNodeId, ClusterTopology, DistributionStore,
    SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use crate::distribution_backup::{
    ClusterBackupCertificate, ClusterBackupLimits, ClusterNodeBackupArtifact,
};
use crate::distribution_consensus::MetadataConsensusStore;
use crate::distribution_range_consensus::RangeWriteStore;
use crate::encryption::EncryptionConfig;
use crate::error::{BicDbError, Result};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION: u32 = 1;
pub const CLUSTER_RESTORE_ADMISSION_RUN_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_RESTORE_ADMISSION: &str = "cluster-restore-admission.json";
pub const DEFAULT_CLUSTER_RESTORE_ADMISSION_RUN: &str = "cluster-restore-admission-run.json";
pub const MAX_CLUSTER_RESTORE_ADMISSION_BYTES: u64 = 64 * 1024 * 1024;

fn restore_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("cluster restore admission: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreAdmissionLimits {
    pub cluster_limits: ClusterBackupLimits,
    pub archive_hash_buffer_bytes: usize,
    pub max_archive_bytes_per_node: u64,
    pub max_range_catalog_bytes_per_node: u64,
}

impl Default for ClusterRestoreAdmissionLimits {
    fn default() -> Self {
        Self {
            cluster_limits: ClusterBackupLimits::default(),
            archive_hash_buffer_bytes: 1024 * 1024,
            // Large enough for multi-PB deployments, finite so a mistaken
            // object/path cannot make admission scan an unbounded source.
            max_archive_bytes_per_node: 8 * 1024 * 1024 * 1024 * 1024 * 1024,
            max_range_catalog_bytes_per_node: 512 * 1024 * 1024,
        }
    }
}

impl ClusterRestoreAdmissionLimits {
    pub fn validate(&self) -> Result<()> {
        self.cluster_limits.validate()?;
        if self.archive_hash_buffer_bytes < 4 * 1024
            || self.archive_hash_buffer_bytes > 4 * 1024 * 1024
            || self.max_archive_bytes_per_node < 1024 * 1024
            || self.max_archive_bytes_per_node > 64 * 1024 * 1024 * 1024 * 1024 * 1024
            || self.max_range_catalog_bytes_per_node < 1024 * 1024
            || self.max_range_catalog_bytes_per_node > 4 * 1024 * 1024 * 1024
        {
            return Err(restore_error(
                "archive hash buffers must be 4KiB..=4MiB, per-node archives 1MiB..=64PiB, and range catalogs 1MiB..=4GiB",
            ));
        }
        Ok(())
    }
}

/// Offline inputs for one restored node. Secrets are borrowed and this type
/// deliberately implements neither `Debug` nor serialization.
pub struct ClusterRestoreNodeCandidate<'a> {
    pub node_id: ClusterNodeId,
    pub archive_path: PathBuf,
    pub restored_root: PathBuf,
    pub backup_passphrase: &'a str,
    pub database_encryption: Option<EncryptionConfig>,
    /// Must be copied from the successful `BackupRestoreReport`; it must equal
    /// the cluster plan's PITR target on every node.
    pub restored_target_timestamp: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreCandidateDescriptor {
    pub node_id: ClusterNodeId,
    pub archive_path: String,
    pub restored_root: String,
    pub restored_target_timestamp: Option<i64>,
}

impl ClusterRestoreCandidateDescriptor {
    fn from_candidate(
        candidate: &ClusterRestoreNodeCandidate<'_>,
        max_path_bytes: usize,
    ) -> Result<Self> {
        let archive_path = candidate
            .archive_path
            .to_str()
            .ok_or_else(|| restore_error("restore archive path must be UTF-8"))?;
        let restored_root = candidate
            .restored_root
            .to_str()
            .ok_or_else(|| restore_error("restored root path must be UTF-8"))?;
        if archive_path.is_empty()
            || restored_root.is_empty()
            || archive_path.len() > max_path_bytes
            || restored_root.len() > max_path_bytes
        {
            return Err(restore_error("restore candidate path exceeds its bound"));
        }
        Ok(Self {
            node_id: candidate.node_id.clone(),
            archive_path: archive_path.to_string(),
            restored_root: restored_root.to_string(),
            restored_target_timestamp: candidate.restored_target_timestamp,
        })
    }

    fn matches(&self, candidate: &ClusterRestoreNodeCandidate<'_>) -> bool {
        self.node_id == candidate.node_id
            && candidate.archive_path.to_str() == Some(self.archive_path.as_str())
            && candidate.restored_root.to_str() == Some(self.restored_root.as_str())
            && self.restored_target_timestamp == candidate.restored_target_timestamp
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreAdmissionRunLimits {
    pub admission: ClusterRestoreAdmissionLimits,
    pub max_path_bytes: usize,
    pub max_state_bytes: u64,
}

impl Default for ClusterRestoreAdmissionRunLimits {
    fn default() -> Self {
        Self {
            admission: ClusterRestoreAdmissionLimits::default(),
            max_path_bytes: 16 * 1024,
            max_state_bytes: 128 * 1024 * 1024,
        }
    }
}

impl ClusterRestoreAdmissionRunLimits {
    pub fn validate(self) -> Result<()> {
        self.admission.validate()?;
        if !(256..=64 * 1024).contains(&self.max_path_bytes)
            || !(64 * 1024..=512 * 1024 * 1024).contains(&self.max_state_bytes)
        {
            return Err(restore_error(
                "restore run path or state bounds are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterRestoreAdmissionRunPhase {
    VerifyingNodes,
    Publishing,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreAdmissionRunState {
    pub format_version: u32,
    pub run_id: Uuid,
    pub phase: ClusterRestoreAdmissionRunPhase,
    pub certificate_id: Uuid,
    pub certificate_checksum_sha256: String,
    pub admitted_at_ms: u64,
    pub limits: ClusterRestoreAdmissionRunLimits,
    pub candidates: Vec<ClusterRestoreCandidateDescriptor>,
    pub next_candidate: usize,
    pub topology: Option<ClusterTopology>,
    pub evidence: Vec<ClusterRestoreNodeEvidence>,
    pub report: Option<ClusterRestoreAdmissionReport>,
    pub updated_at_ms: u64,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterRestoreAdmissionRunAdvance {
    Progress {
        phase: ClusterRestoreAdmissionRunPhase,
        verified_nodes: usize,
        total_nodes: usize,
    },
    Complete(ClusterRestoreAdmissionReport),
}

impl ClusterRestoreAdmissionRunState {
    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.format_version != CLUSTER_RESTORE_ADMISSION_RUN_FORMAT_VERSION
            || self.run_id.is_nil()
            || self.certificate_id.is_nil()
            || self.certificate_checksum_sha256.len() != 64
            || self.candidates.is_empty()
            || self.candidates.len() > self.limits.admission.cluster_limits.max_nodes
            || self.next_candidate > self.candidates.len()
            || self.evidence.len() != self.next_candidate
            || self
                .candidates
                .windows(2)
                .any(|pair| pair[0].node_id >= pair[1].node_id)
        {
            return Err(restore_error(
                "restore run identity, cursor, or bounds are invalid",
            ));
        }
        for candidate in &self.candidates {
            if candidate.archive_path.is_empty()
                || candidate.restored_root.is_empty()
                || candidate.archive_path.len() > self.limits.max_path_bytes
                || candidate.restored_root.len() > self.limits.max_path_bytes
            {
                return Err(restore_error("persisted restore candidate path is invalid"));
            }
        }
        if self
            .evidence
            .iter()
            .zip(&self.candidates)
            .any(|(evidence, candidate)| evidence.node_id != candidate.node_id)
        {
            return Err(restore_error(
                "restore evidence does not match candidate order",
            ));
        }
        if let Some(topology) = &self.topology {
            topology.validate()?;
        }
        if (!self.evidence.is_empty() && self.topology.is_none())
            || (self.phase != ClusterRestoreAdmissionRunPhase::VerifyingNodes
                && (self.next_candidate != self.candidates.len() || self.topology.is_none()))
            || (self.phase == ClusterRestoreAdmissionRunPhase::Complete) != self.report.is_some()
        {
            return Err(restore_error(
                "restore run phase, topology, evidence, or report disagrees",
            ));
        }
        if let Some(report) = &self.report {
            if report.certificate_id != self.certificate_id
                || report.certificate_checksum_sha256 != self.certificate_checksum_sha256
                || report.admitted_at_ms != self.admitted_at_ms
                || report.calculate_checksum()? != report.checksum_sha256
            {
                return Err(restore_error("restore run report binding is invalid"));
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256
            || serde_json::to_vec(self)?.len() as u64 > self.limits.max_state_bytes
        {
            return Err(restore_error(
                "restore run checksum or state bound is invalid",
            ));
        }
        Ok(())
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            run_id: Uuid,
            phase: ClusterRestoreAdmissionRunPhase,
            certificate_id: Uuid,
            certificate_checksum_sha256: &'a str,
            admitted_at_ms: u64,
            limits: ClusterRestoreAdmissionRunLimits,
            candidates: &'a [ClusterRestoreCandidateDescriptor],
            next_candidate: usize,
            topology: &'a Option<ClusterTopology>,
            evidence: &'a [ClusterRestoreNodeEvidence],
            report: &'a Option<ClusterRestoreAdmissionReport>,
            updated_at_ms: u64,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            run_id: self.run_id,
            phase: self.phase,
            certificate_id: self.certificate_id,
            certificate_checksum_sha256: &self.certificate_checksum_sha256,
            admitted_at_ms: self.admitted_at_ms,
            limits: self.limits,
            candidates: &self.candidates,
            next_candidate: self.next_candidate,
            topology: &self.topology,
            evidence: &self.evidence,
            report: &self.report,
            updated_at_ms: self.updated_at_ms,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreNodeEvidence {
    pub node_id: ClusterNodeId,
    pub backup_id: Uuid,
    pub archive_bytes: u64,
    pub archive_sha256: String,
    pub manifest_sha256: String,
    pub artifact_checksum_sha256: String,
    pub schema_sha256: String,
    pub source_format_version: u32,
    pub restored_target_timestamp: Option<i64>,
    pub metadata_term: u64,
    pub metadata_commit_index: u64,
    pub range_fences_verified: usize,
    pub database_integrity_verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreAdmissionReport {
    pub format_version: u32,
    pub admission_id: Uuid,
    pub admitted_at_ms: u64,
    pub certificate_id: Uuid,
    pub certificate_checksum_sha256: String,
    pub plan_id: Uuid,
    pub cluster_id: ClusterId,
    pub topology_generation: u64,
    pub topology_sha256: String,
    pub range_barriers_verified: usize,
    pub nodes: Vec<ClusterRestoreNodeEvidence>,
    pub checksum_sha256: String,
}

impl ClusterRestoreAdmissionReport {
    fn admit(
        certificate: &ClusterBackupCertificate,
        topology: &ClusterTopology,
        mut nodes: Vec<ClusterRestoreNodeEvidence>,
        admitted_at_ms: u64,
        limits: &ClusterRestoreAdmissionLimits,
    ) -> Result<Self> {
        limits.validate()?;
        certificate.validate_for_restore(topology, &limits.cluster_limits)?;
        if admitted_at_ms < certificate.certified_at_ms
            || nodes.is_empty()
            || nodes.len() > limits.cluster_limits.max_nodes
        {
            return Err(restore_error(
                "admission time or restored node count is invalid",
            ));
        }
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        validate_node_evidence(certificate, topology, &nodes, limits)?;
        let mut report = Self {
            format_version: CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION,
            admission_id: Uuid::now_v7(),
            admitted_at_ms,
            certificate_id: certificate.certificate_id,
            certificate_checksum_sha256: certificate.checksum_sha256.clone(),
            plan_id: certificate.plan.plan_id,
            cluster_id: certificate.plan.cluster_id.clone(),
            topology_generation: certificate.plan.topology_generation,
            topology_sha256: certificate.plan.topology_sha256.clone(),
            range_barriers_verified: certificate.range_barriers.len(),
            nodes,
            checksum_sha256: String::new(),
        };
        report.checksum_sha256 = report.calculate_checksum()?;
        Ok(report)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            admission_id: Uuid,
            admitted_at_ms: u64,
            certificate_id: Uuid,
            certificate_checksum_sha256: &'a str,
            plan_id: Uuid,
            cluster_id: &'a ClusterId,
            topology_generation: u64,
            topology_sha256: &'a str,
            range_barriers_verified: usize,
            nodes: &'a [ClusterRestoreNodeEvidence],
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            admission_id: self.admission_id,
            admitted_at_ms: self.admitted_at_ms,
            certificate_id: self.certificate_id,
            certificate_checksum_sha256: &self.certificate_checksum_sha256,
            plan_id: self.plan_id,
            cluster_id: &self.cluster_id,
            topology_generation: self.topology_generation,
            topology_sha256: &self.topology_sha256,
            range_barriers_verified: self.range_barriers_verified,
            nodes: &self.nodes,
        })
    }

    pub fn validate(
        &self,
        certificate: &ClusterBackupCertificate,
        topology: &ClusterTopology,
        limits: &ClusterRestoreAdmissionLimits,
    ) -> Result<()> {
        limits.validate()?;
        certificate.validate_for_restore(topology, &limits.cluster_limits)?;
        if self.format_version != CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION
            || self.admission_id.is_nil()
            || self.admitted_at_ms < certificate.certified_at_ms
            || self.certificate_id != certificate.certificate_id
            || self.certificate_checksum_sha256 != certificate.checksum_sha256
            || self.plan_id != certificate.plan.plan_id
            || self.cluster_id != certificate.plan.cluster_id
            || self.topology_generation != certificate.plan.topology_generation
            || self.topology_sha256 != certificate.plan.topology_sha256
            || self.range_barriers_verified != certificate.range_barriers.len()
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(restore_error(
                "admission identity, certificate binding, or checksum is invalid",
            ));
        }
        validate_node_evidence(certificate, topology, &self.nodes, limits)
    }

    /// Cheap startup gate after the expensive offline admission pass. It binds
    /// this node's persisted config, topology, and consensus watermark back to
    /// the admitted certificate. The full archive/database verification is the
    /// prerequisite that created this report.
    pub fn validate_node_start(
        &self,
        certificate: &ClusterBackupCertificate,
        node_id: &ClusterNodeId,
        restored_root: impl AsRef<Path>,
        limits: &ClusterRestoreAdmissionLimits,
    ) -> Result<()> {
        let restored_root = restored_root.as_ref();
        reject_symlink(restored_root, "restored node directory")?;
        let config = load_distribution_config(restored_root)?;
        if &config.node_id != node_id {
            return Err(restore_error(format!(
                "restored config identifies {}, not requested node {node_id}",
                config.node_id
            )));
        }
        let store = DistributionStore::open(restored_root, config, false)?;
        self.validate(certificate, store.topology(), limits)?;
        let evidence = self
            .nodes
            .iter()
            .find(|evidence| &evidence.node_id == node_id)
            .ok_or_else(|| restore_error(format!("node {node_id} has no admission evidence")))?;
        let metadata = MetadataConsensusStore::inspect(restored_root)?;
        if metadata.node_id != *node_id
            || metadata.cluster_id != self.cluster_id
            || metadata.topology_generation != self.topology_generation
            || metadata.current_term < evidence.metadata_term
            || metadata.commit_index < evidence.metadata_commit_index
            || metadata.last_log_index < metadata.commit_index
        {
            return Err(restore_error(format!(
                "node {node_id} metadata consensus state regressed after admission"
            )));
        }
        Ok(())
    }
}

pub fn verify_cluster_restore_admission(
    certificate: &ClusterBackupCertificate,
    candidates: &[ClusterRestoreNodeCandidate<'_>],
    admitted_at_ms: u64,
    limits: &ClusterRestoreAdmissionLimits,
) -> Result<ClusterRestoreAdmissionReport> {
    limits.validate()?;
    if candidates.is_empty() || candidates.len() > limits.cluster_limits.max_nodes {
        return Err(restore_error(
            "restored candidate count is empty or exceeds its bound",
        ));
    }
    let mut candidate_ids = BTreeSet::new();
    let mut topology = None;
    let mut evidence = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !candidate_ids.insert(candidate.node_id.clone()) {
            return Err(restore_error(format!(
                "node {} appears more than once in restore candidates",
                candidate.node_id
            )));
        }
        let artifact = certificate
            .artifacts
            .iter()
            .find(|artifact| artifact.node_id == candidate.node_id)
            .ok_or_else(|| {
                restore_error(format!(
                    "node {} has no artifact in certificate {}",
                    candidate.node_id, certificate.certificate_id
                ))
            })?;
        reject_symlink(&candidate.restored_root, "restored node directory")?;
        let config = load_distribution_config(&candidate.restored_root)?;
        if config.node_id != candidate.node_id || config.cluster_id != certificate.plan.cluster_id {
            return Err(restore_error(format!(
                "restored configuration for {} has another node or cluster identity",
                candidate.node_id
            )));
        }
        let distribution = DistributionStore::open(&candidate.restored_root, config, false)?;
        if let Some(expected) = topology.as_ref() {
            if expected != distribution.topology() {
                return Err(restore_error(
                    "restored nodes do not contain one identical committed topology",
                ));
            }
        } else {
            topology = Some(distribution.topology().clone());
        }
        evidence.push(inspect_restored_node(
            candidate,
            artifact,
            &certificate.plan.cluster_id,
            certificate.plan.metadata_term,
            certificate.plan.target_timestamp,
            limits,
        )?);
    }
    let topology = topology.ok_or_else(|| restore_error("restore topology is missing"))?;
    ClusterRestoreAdmissionReport::admit(certificate, &topology, evidence, admitted_at_ms, limits)
}

/// Resource-admitted offline restore verification. The permit covers archive
/// hashing, database integrity verification, and range-catalog inspection and
/// is deterministically released on every error. Callers that require
/// per-node checkpointing should submit one bounded restore run rather than
/// widening this whole-certificate compatibility API.
pub fn verify_cluster_restore_admission_governed(
    certificate: &ClusterBackupCertificate,
    candidates: &[ClusterRestoreNodeCandidate<'_>],
    admitted_at_ms: u64,
    limits: &ClusterRestoreAdmissionLimits,
    governor: &ResourceGovernor,
    demand: ResourceDemand,
    now_ms: u64,
) -> Result<ClusterRestoreAdmissionReport> {
    let permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
    let result = verify_cluster_restore_admission(certificate, candidates, admitted_at_ms, limits);
    drop(permit);
    result
}

/// Single-owner, one-node-per-step restore admission coordinator. Secret
/// passphrases remain invocation-only and never enter the durable state.
pub struct ClusterRestoreAdmissionRun {
    path: PathBuf,
    fsync: bool,
    _lock: File,
    state: ClusterRestoreAdmissionRunState,
}

impl ClusterRestoreAdmissionRun {
    pub fn create(
        path: impl AsRef<Path>,
        certificate: &ClusterBackupCertificate,
        candidates: &[ClusterRestoreNodeCandidate<'_>],
        admitted_at_ms: u64,
        limits: ClusterRestoreAdmissionRunLimits,
        fsync: bool,
    ) -> Result<Self> {
        limits.validate()?;
        if certificate.certificate_id.is_nil()
            || certificate.calculate_checksum()? != certificate.checksum_sha256
            || admitted_at_ms < certificate.certified_at_ms
            || candidates.is_empty()
            || candidates.len() > limits.admission.cluster_limits.max_nodes
        {
            return Err(restore_error(
                "restore run certificate, time, or candidate count is invalid",
            ));
        }
        let path = path.as_ref().to_path_buf();
        reject_symlink_if_exists(&path, "restore admission run")?;
        if path.exists() {
            return Err(restore_error("restore admission run already exists"));
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let lock_path = restore_run_lock_path(&path);
        reject_symlink_if_exists(&lock_path, "restore admission run lock")?;
        let lock = open_restore_run_lock(&lock_path)?;
        let mut descriptors = candidates
            .iter()
            .map(|candidate| {
                ClusterRestoreCandidateDescriptor::from_candidate(candidate, limits.max_path_bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        descriptors.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        if descriptors
            .windows(2)
            .any(|pair| pair[0].node_id == pair[1].node_id)
        {
            return Err(restore_error(
                "restore run contains duplicate candidate nodes",
            ));
        }
        let artifact_nodes = certificate
            .artifacts
            .iter()
            .map(|artifact| artifact.node_id.clone())
            .collect::<BTreeSet<_>>();
        let candidate_nodes = descriptors
            .iter()
            .map(|candidate| candidate.node_id.clone())
            .collect::<BTreeSet<_>>();
        if artifact_nodes != candidate_nodes {
            return Err(restore_error(
                "restore run candidates must exactly match certificate artifact nodes",
            ));
        }
        let mut state = ClusterRestoreAdmissionRunState {
            format_version: CLUSTER_RESTORE_ADMISSION_RUN_FORMAT_VERSION,
            run_id: Uuid::now_v7(),
            phase: ClusterRestoreAdmissionRunPhase::VerifyingNodes,
            certificate_id: certificate.certificate_id,
            certificate_checksum_sha256: certificate.checksum_sha256.clone(),
            admitted_at_ms,
            limits,
            candidates: descriptors,
            next_candidate: 0,
            topology: None,
            evidence: Vec::new(),
            report: None,
            updated_at_ms: admitted_at_ms,
            checksum_sha256: String::new(),
        };
        state.refresh_checksum()?;
        save_cluster_restore_admission_run_state(&path, &state, fsync)?;
        Ok(Self {
            path,
            fsync,
            _lock: lock,
            state,
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        limits: ClusterRestoreAdmissionRunLimits,
        fsync: bool,
    ) -> Result<Self> {
        limits.validate()?;
        let path = path.as_ref().to_path_buf();
        reject_symlink(&path, "restore admission run")?;
        let lock_path = restore_run_lock_path(&path);
        reject_symlink_if_exists(&lock_path, "restore admission run lock")?;
        let lock = open_restore_run_lock(&lock_path)?;
        let state = load_cluster_restore_admission_run_state(&path, limits)?;
        Ok(Self {
            path,
            fsync,
            _lock: lock,
            state,
        })
    }

    pub fn state(&self) -> &ClusterRestoreAdmissionRunState {
        &self.state
    }

    /// Verifies at most one restored node or publishes the final report. The
    /// candidate passphrase is borrowed for this call and is never persisted.
    pub fn advance_governed(
        &mut self,
        certificate: &ClusterBackupCertificate,
        candidate: Option<&ClusterRestoreNodeCandidate<'_>>,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<ClusterRestoreAdmissionRunAdvance> {
        self.advance_with_verifier(
            certificate,
            candidate,
            governor,
            demand,
            now_ms,
            |candidate,
             artifact,
             expected_cluster_id,
             minimum_metadata_term,
             expected_target,
             limits| {
                reject_symlink(&candidate.restored_root, "restored node directory")?;
                let config = load_distribution_config(&candidate.restored_root)?;
                if config.node_id != candidate.node_id || config.cluster_id != *expected_cluster_id
                {
                    return Err(restore_error(format!(
                        "restored configuration for {} has another node or cluster identity",
                        candidate.node_id
                    )));
                }
                let distribution =
                    DistributionStore::open(&candidate.restored_root, config, false)?;
                let topology = distribution.topology().clone();
                let evidence = inspect_restored_node(
                    candidate,
                    artifact,
                    expected_cluster_id,
                    minimum_metadata_term,
                    expected_target,
                    limits,
                )?;
                Ok((topology, evidence))
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn advance_with_verifier<F>(
        &mut self,
        certificate: &ClusterBackupCertificate,
        candidate: Option<&ClusterRestoreNodeCandidate<'_>>,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
        mut verifier: F,
    ) -> Result<ClusterRestoreAdmissionRunAdvance>
    where
        F: FnMut(
            &ClusterRestoreNodeCandidate<'_>,
            &ClusterNodeBackupArtifact,
            &ClusterId,
            u64,
            Option<i64>,
            &ClusterRestoreAdmissionLimits,
        ) -> Result<(ClusterTopology, ClusterRestoreNodeEvidence)>,
    {
        self.state.validate()?;
        if certificate.certificate_id != self.state.certificate_id
            || certificate.checksum_sha256 != self.state.certificate_checksum_sha256
            || certificate.calculate_checksum()? != certificate.checksum_sha256
        {
            return Err(restore_error(
                "restore run certificate changed across resume",
            ));
        }
        if self.state.phase == ClusterRestoreAdmissionRunPhase::Complete {
            return Ok(ClusterRestoreAdmissionRunAdvance::Complete(
                self.state
                    .report
                    .clone()
                    .ok_or_else(|| restore_error("completed restore run has no report"))?,
            ));
        }
        if now_ms < self.state.updated_at_ms {
            return Err(restore_error("restore admission run clock regressed"));
        }
        let minimum_buffer = self.state.limits.admission.archive_hash_buffer_bytes as u64;
        if demand.memory_bytes < minimum_buffer
            || demand.io_bytes < minimum_buffer
            || demand.io_charge_bytes < minimum_buffer
        {
            return Err(BicDbError::ResourceGovernance(format!(
                "restore demand must cover the {minimum_buffer}-byte archive buffer"
            )));
        }
        let _permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
        match self.state.phase {
            ClusterRestoreAdmissionRunPhase::VerifyingNodes => {
                if self.state.next_candidate == self.state.candidates.len() {
                    self.state.phase = ClusterRestoreAdmissionRunPhase::Publishing;
                } else {
                    let expected = &self.state.candidates[self.state.next_candidate];
                    let candidate = candidate
                        .filter(|candidate| expected.matches(candidate))
                        .ok_or_else(|| {
                            restore_error(format!(
                                "restore run requires exact candidate {} at cursor {}",
                                expected.node_id, self.state.next_candidate
                            ))
                        })?;
                    let artifact = certificate
                        .artifacts
                        .iter()
                        .find(|artifact| artifact.node_id == expected.node_id)
                        .ok_or_else(|| restore_error("certificate artifact disappeared"))?;
                    let (topology, evidence) = verifier(
                        candidate,
                        artifact,
                        &certificate.plan.cluster_id,
                        certificate.plan.metadata_term,
                        certificate.plan.target_timestamp,
                        &self.state.limits.admission,
                    )?;
                    if evidence.node_id != expected.node_id {
                        return Err(restore_error(
                            "node verifier returned evidence for another node",
                        ));
                    }
                    if let Some(existing) = &self.state.topology {
                        if existing != &topology {
                            return Err(restore_error(
                                "restored nodes do not contain one identical committed topology",
                            ));
                        }
                    } else {
                        topology.validate()?;
                        self.state.topology = Some(topology);
                    }
                    self.state.evidence.push(evidence);
                    self.state.next_candidate += 1;
                    if self.state.next_candidate == self.state.candidates.len() {
                        self.state.phase = ClusterRestoreAdmissionRunPhase::Publishing;
                    }
                }
            }
            ClusterRestoreAdmissionRunPhase::Publishing => {
                let topology = self
                    .state
                    .topology
                    .as_ref()
                    .ok_or_else(|| restore_error("restore run has no verified topology"))?;
                let report = ClusterRestoreAdmissionReport::admit(
                    certificate,
                    topology,
                    self.state.evidence.clone(),
                    self.state.admitted_at_ms,
                    &self.state.limits.admission,
                )?;
                self.state.report = Some(report);
                self.state.phase = ClusterRestoreAdmissionRunPhase::Complete;
            }
            ClusterRestoreAdmissionRunPhase::Complete => unreachable!(),
        }
        self.state.updated_at_ms = now_ms;
        self.state.refresh_checksum()?;
        save_cluster_restore_admission_run_state(&self.path, &self.state, self.fsync)?;
        if let Some(report) = &self.state.report {
            Ok(ClusterRestoreAdmissionRunAdvance::Complete(report.clone()))
        } else {
            Ok(ClusterRestoreAdmissionRunAdvance::Progress {
                phase: self.state.phase,
                verified_nodes: self.state.next_candidate,
                total_nodes: self.state.candidates.len(),
            })
        }
    }
}

pub fn save_cluster_restore_admission_run_state(
    path: impl AsRef<Path>,
    state: &ClusterRestoreAdmissionRunState,
    fsync: bool,
) -> Result<()> {
    state.validate()?;
    reject_symlink_if_exists(path.as_ref(), "restore admission run")?;
    let bytes = serialize_bounded(state, state.limits.max_state_bytes as usize)?;
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_cluster_restore_admission_run_state(
    path: impl AsRef<Path>,
    limits: ClusterRestoreAdmissionRunLimits,
) -> Result<ClusterRestoreAdmissionRunState> {
    limits.validate()?;
    let bytes = read_bounded(path.as_ref(), limits.max_state_bytes)?;
    let state: ClusterRestoreAdmissionRunState = serde_json::from_slice(&bytes)?;
    if state.limits != limits {
        return Err(restore_error(
            "restore admission run limits changed across resume",
        ));
    }
    state.validate()?;
    Ok(state)
}

pub fn save_cluster_restore_admission(
    path: impl AsRef<Path>,
    report: &ClusterRestoreAdmissionReport,
    fsync: bool,
) -> Result<()> {
    reject_symlink_if_exists(path.as_ref(), "restore admission report")?;
    if report.calculate_checksum()? != report.checksum_sha256 {
        return Err(restore_error(
            "refusing to persist an admission report with a bad checksum",
        ));
    }
    let bytes = serialize_bounded(report, MAX_CLUSTER_RESTORE_ADMISSION_BYTES as usize)?;
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_cluster_restore_admission(
    path: impl AsRef<Path>,
) -> Result<ClusterRestoreAdmissionReport> {
    let path = path.as_ref();
    reject_symlink(path, "restore admission report")?;
    let bytes = read_bounded(path, MAX_CLUSTER_RESTORE_ADMISSION_BYTES)?;
    let report: ClusterRestoreAdmissionReport = serde_json::from_slice(&bytes)?;
    if report.calculate_checksum()? != report.checksum_sha256 {
        return Err(restore_error("persisted admission checksum mismatch"));
    }
    Ok(report)
}

fn inspect_restored_node(
    candidate: &ClusterRestoreNodeCandidate<'_>,
    artifact: &ClusterNodeBackupArtifact,
    expected_cluster_id: &ClusterId,
    minimum_metadata_term: u64,
    expected_target_timestamp: Option<i64>,
    limits: &ClusterRestoreAdmissionLimits,
) -> Result<ClusterRestoreNodeEvidence> {
    if candidate.restored_target_timestamp != expected_target_timestamp {
        return Err(restore_error(format!(
            "restored PITR target differs for {}",
            candidate.node_id
        )));
    }
    let file = open_regular_file_no_follow(&candidate.archive_path, "node backup archive")?;
    let archive_metadata = file.metadata()?;
    if archive_metadata.len() > limits.max_archive_bytes_per_node {
        return Err(restore_error(format!(
            "archive for {} is not a bounded regular file",
            candidate.node_id
        )));
    }
    let mut reader = HashingBoundedReader::new(
        BufReader::new(file),
        limits.archive_hash_buffer_bytes,
        limits.max_archive_bytes_per_node,
    );
    let verified = verify_backup_from_reader(&mut reader, candidate.backup_passphrase)?;
    let archive_sha256 = reader.finish_hashing()?;
    if reader.total_bytes != archive_metadata.len() {
        return Err(restore_error(format!(
            "archive for {} changed size during verification",
            candidate.node_id
        )));
    }
    if !verified.full
        || verified.backup_id != artifact.backup_id
        || verified.manifest_hash != artifact.backup_manifest_sha256
        || archive_sha256 != artifact.encrypted_artifact_sha256
    {
        return Err(restore_error(format!(
            "archive identity, manifest, or encrypted hash differs for {}",
            candidate.node_id
        )));
    }

    let metadata = MetadataConsensusStore::inspect(&candidate.restored_root)?;
    if metadata.node_id != candidate.node_id
        || metadata.cluster_id != *expected_cluster_id
        || metadata.topology_generation != artifact.topology_generation
        || metadata.current_term < minimum_metadata_term
        || metadata.commit_index < artifact.metadata_commit_index
        || metadata.last_log_index < metadata.commit_index
    {
        return Err(restore_error(format!(
            "metadata consensus watermark is stale for {}",
            candidate.node_id
        )));
    }

    let config = DbConfig::default()
        .with_storage_mode(crate::format::storage_mode(&candidate.restored_root)?);
    BicDb::verify_path(
        &candidate.restored_root,
        config.clone(),
        candidate.database_encryption.clone(),
    )?;
    let db = match candidate.database_encryption.clone() {
        Some(encryption) => {
            BicDb::open_with_encryption(&candidate.restored_root, config, encryption)?
        }
        None => BicDb::open_with_config(&candidate.restored_root, config)?,
    };
    let integrity = db.verify_integrity()?;
    let schema = db.schema_compatibility_fingerprint()?;
    db.close()?;
    if schema.sha256 != artifact.schema_sha256 {
        return Err(restore_error(format!(
            "restored schema for {} differs from its artifact",
            candidate.node_id
        )));
    }

    let range_catalog = RangeWriteStore::inspect(
        &candidate.restored_root,
        metadata.cluster_id.clone(),
        candidate.node_id.clone(),
        limits.max_range_catalog_bytes_per_node,
    )?;
    let progress = range_catalog
        .ranges
        .into_iter()
        .map(|progress| (progress.range_id, progress))
        .collect::<BTreeMap<_, _>>();
    let fences = range_catalog
        .backup_fences
        .into_iter()
        .map(|fence| (fence.range_id, fence))
        .collect::<BTreeMap<_, _>>();
    if fences.len() != artifact.ranges.len() {
        return Err(restore_error(format!(
            "restored node {} has unexpected or missing active backup fences",
            candidate.node_id
        )));
    }
    for expected in &artifact.ranges {
        if progress.get(&expected.range_id) != Some(expected) {
            return Err(restore_error(format!(
                "restored range {} progress differs on {}",
                expected.range_id, candidate.node_id
            )));
        }
        let fence = fences.get(&expected.range_id).ok_or_else(|| {
            restore_error(format!(
                "restored range {} has no backup fence on {}",
                expected.range_id, candidate.node_id
            ))
        })?;
        if fence.plan_id != artifact.plan_id
            || fence.range_epoch != expected.current_epoch
            || fence.resolved_through != expected.resolved_through
        {
            return Err(restore_error(format!(
                "restored range {} fence differs on {}",
                expected.range_id, candidate.node_id
            )));
        }
    }

    Ok(ClusterRestoreNodeEvidence {
        node_id: candidate.node_id.clone(),
        backup_id: artifact.backup_id,
        archive_bytes: archive_metadata.len(),
        archive_sha256,
        manifest_sha256: verified.manifest_hash,
        artifact_checksum_sha256: artifact.checksum_sha256.clone(),
        schema_sha256: schema.sha256,
        source_format_version: verified.source_format_version,
        restored_target_timestamp: candidate.restored_target_timestamp,
        metadata_term: metadata.current_term,
        metadata_commit_index: metadata.commit_index,
        range_fences_verified: artifact.ranges.len(),
        database_integrity_verified: integrity_is_valid(&integrity),
    })
}

fn validate_node_evidence(
    certificate: &ClusterBackupCertificate,
    topology: &ClusterTopology,
    nodes: &[ClusterRestoreNodeEvidence],
    limits: &ClusterRestoreAdmissionLimits,
) -> Result<()> {
    if nodes.is_empty() || nodes.len() > limits.cluster_limits.max_nodes {
        return Err(restore_error("node evidence count is outside its bound"));
    }
    let artifacts = certificate
        .artifacts
        .iter()
        .map(|artifact| (artifact.node_id.clone(), artifact))
        .collect::<BTreeMap<_, _>>();
    if nodes.len() != artifacts.len() {
        return Err(restore_error(
            "admission requires exactly every certificate artifact node",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut previous = None;
    for evidence in nodes {
        let artifact = artifacts.get(&evidence.node_id).ok_or_else(|| {
            restore_error(format!(
                "node {} has no certificate artifact",
                evidence.node_id
            ))
        })?;
        let node = topology.nodes.get(&evidence.node_id).ok_or_else(|| {
            restore_error(format!("node {} is absent from topology", evidence.node_id))
        })?;
        if !seen.insert(evidence.node_id.clone())
            || previous
                .as_ref()
                .is_some_and(|previous| previous >= &evidence.node_id)
            || evidence.backup_id != artifact.backup_id
            || evidence.archive_bytes == 0
            || evidence.archive_bytes > limits.max_archive_bytes_per_node
            || evidence.archive_sha256 != artifact.encrypted_artifact_sha256
            || evidence.manifest_sha256 != artifact.backup_manifest_sha256
            || evidence.artifact_checksum_sha256 != artifact.checksum_sha256
            || evidence.schema_sha256 != artifact.schema_sha256
            || evidence.source_format_version == 0
            || evidence.restored_target_timestamp != certificate.plan.target_timestamp
            || node.labels.get(SCHEMA_COMPATIBILITY_NODE_LABEL) != Some(&evidence.schema_sha256)
            || evidence.metadata_term < certificate.plan.metadata_term
            || evidence.metadata_commit_index < certificate.plan.metadata_commit_index
            || evidence.range_fences_verified != artifact.ranges.len()
            || !evidence.database_integrity_verified
        {
            return Err(restore_error(format!(
                "node {} evidence is incomplete, stale, or non-canonical",
                evidence.node_id
            )));
        }
        previous = Some(evidence.node_id.clone());
    }
    Ok(())
}

struct HashingBoundedReader<R> {
    inner: R,
    hasher: Sha256,
    total_bytes: u64,
    max_read_bytes: usize,
    max_total_bytes: u64,
}

impl<R> HashingBoundedReader<R> {
    fn new(inner: R, max_read_bytes: usize, max_total_bytes: u64) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            total_bytes: 0,
            max_read_bytes,
            max_total_bytes,
        }
    }
}

impl<R: Read> HashingBoundedReader<R> {
    fn finish_hashing(&mut self) -> Result<String> {
        let mut buffer = vec![0_u8; self.max_read_bytes];
        while self.read(&mut buffer)? != 0 {}
        Ok(hex::encode(self.hasher.clone().finalize()))
    }
}

impl<R: Read> Read for HashingBoundedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let limit = buffer.len().min(self.max_read_bytes);
        let read = self.inner.read(&mut buffer[..limit])?;
        self.total_bytes = self
            .total_bytes
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("archive byte count overflow"))?;
        if self.total_bytes > self.max_total_bytes {
            return Err(std::io::Error::other(
                "archive grew beyond its configured bound",
            ));
        }
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
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

fn serialize_bounded(value: &impl Serialize, max_bytes: usize) -> Result<Vec<u8>> {
    struct BoundedWriter {
        bytes: Vec<u8>,
        max_bytes: usize,
    }
    impl Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self
                .bytes
                .len()
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("admission size overflow"))?;
            if next > self.max_bytes {
                return Err(std::io::Error::other("admission exceeds its byte bound"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        max_bytes,
    };
    serde_json::to_writer_pretty(&mut writer, value)?;
    Ok(writer.bytes)
}

fn read_bounded(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    let file = open_regular_file_no_follow(path, "restore admission report")?;
    let length = file.metadata()?.len();
    if length > max_bytes {
        return Err(restore_error(format!(
            "{} exceeds the {max_bytes} byte admission bound",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(restore_error("admission file grew beyond its read bound"));
    }
    Ok(bytes)
}

fn open_regular_file_no_follow(path: &Path, kind: &str) -> Result<File> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?
    };
    #[cfg(not(unix))]
    let file = {
        reject_symlink(path, kind)?;
        OpenOptions::new().read(true).open(path)?
    };
    if !file.metadata()?.is_file() {
        return Err(restore_error(format!(
            "refusing non-regular {kind} {}",
            path.display()
        )));
    }
    Ok(file)
}

fn reject_symlink(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(restore_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(restore_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(restore_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

fn restore_run_lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".lock");
    PathBuf::from(value)
}

#[cfg(unix)]
fn open_restore_run_lock(path: &Path) -> Result<File> {
    use std::os::fd::AsRawFd;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(restore_error(format!(
            "another restore admission coordinator owns {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        )));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_restore_run_lock(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
        .map_err(|error| {
            restore_error(format!(
                "another restore admission coordinator owns {}: {error}",
                path.display()
            ))
        })
}

#[cfg(not(any(unix, windows)))]
fn open_restore_run_lock(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(Into::into)
}

fn integrity_is_valid(report: &crate::db::IntegrityReport) -> bool {
    report.large_value_checksum_failures.is_empty()
        && report
            .paged_storage
            .as_ref()
            .is_none_or(|paged| paged.valid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{
        ClusterNode, DistributionConfig, DistributionStore, RangeReplica, RangeReplicaRole,
        ReplicaId,
    };
    use crate::distribution_backup::ClusterBackupPlan;
    use crate::distribution_consensus::{MetadataConsensusRole, MetadataConsensusStatus};
    use crate::distribution_range_consensus::RangeWriteProgress;

    fn fixture() -> (
        tempfile::TempDir,
        ClusterTopology,
        MetadataConsensusStatus,
        Vec<ClusterNodeId>,
        ClusterBackupCertificate,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let nodes = ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|node| ClusterNodeId::new(node).unwrap())
            .collect::<Vec<_>>();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("restore-test").unwrap(),
            node_id: nodes[0].clone(),
            node_address: "127.0.0.1:9501".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor: 3,
            initial_ranges: 1,
            ..DistributionConfig::default()
        };
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        let mut topology = store.topology().clone();
        for (index, node_id) in nodes.iter().enumerate().skip(1) {
            topology.nodes.insert(
                node_id.clone(),
                ClusterNode::new(
                    node_id.clone(),
                    format!("127.0.0.1:{}", 9501 + index),
                    1,
                    1_000_000,
                    index as u64 + 1,
                )
                .unwrap(),
            );
        }
        for node in topology.nodes.values_mut() {
            node.labels
                .insert(SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(), "a".repeat(64));
        }
        let range = topology.ranges.values_mut().next().unwrap();
        let mut next_replica = topology.next_replica_id;
        for node_id in nodes.iter().skip(1) {
            range.replicas.push(RangeReplica {
                id: ReplicaId::new(next_replica).unwrap(),
                node_id: node_id.clone(),
                role: RangeReplicaRole::Voter,
            });
            next_replica += 1;
        }
        topology.next_replica_id = next_replica;
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
        let plan = ClusterBackupPlan::create(
            &topology,
            &metadata,
            None,
            10_000,
            1_000,
            &ClusterBackupLimits::default(),
        )
        .unwrap();
        let range = topology.ranges.values().next().unwrap();
        let artifacts = nodes
            .iter()
            .take(2)
            .map(|node_id| {
                ClusterNodeBackupArtifact::create(
                    &plan,
                    node_id.clone(),
                    Uuid::now_v7(),
                    "b".repeat(64),
                    "c".repeat(64),
                    "a".repeat(64),
                    10_500,
                    vec![RangeWriteProgress {
                        node_id: node_id.clone(),
                        range_id: range.id,
                        current_epoch: range.epoch,
                        last_index: 7,
                        resolved_through: 7,
                        compacted_through: 6,
                    }],
                    &ClusterBackupLimits::default(),
                )
                .unwrap()
            })
            .collect();
        let certificate = ClusterBackupCertificate::certify(
            plan,
            &topology,
            &metadata,
            artifacts,
            10_500,
            &ClusterBackupLimits::default(),
        )
        .unwrap();
        (directory, topology, metadata, nodes, certificate)
    }

    fn evidence(
        certificate: &ClusterBackupCertificate,
        node_id: &ClusterNodeId,
    ) -> ClusterRestoreNodeEvidence {
        let artifact = certificate
            .artifacts
            .iter()
            .find(|artifact| &artifact.node_id == node_id)
            .unwrap();
        ClusterRestoreNodeEvidence {
            node_id: node_id.clone(),
            backup_id: artifact.backup_id,
            archive_bytes: 1_000_000,
            archive_sha256: artifact.encrypted_artifact_sha256.clone(),
            manifest_sha256: artifact.backup_manifest_sha256.clone(),
            artifact_checksum_sha256: artifact.checksum_sha256.clone(),
            schema_sha256: artifact.schema_sha256.clone(),
            source_format_version: 1,
            restored_target_timestamp: certificate.plan.target_timestamp,
            metadata_term: certificate.plan.metadata_term,
            metadata_commit_index: certificate.plan.metadata_commit_index,
            range_fences_verified: artifact.ranges.len(),
            database_integrity_verified: true,
        }
    }

    #[test]
    fn governed_restore_rejects_before_archive_or_candidate_validation() {
        let (_directory, _topology, _metadata, _nodes, certificate) = fixture();
        let demand = ResourceDemand {
            memory_bytes: 8 * 1024 * 1024,
            io_bytes: 8 * 1024 * 1024,
            cpu_slots: 1,
            io_charge_bytes: 8 * 1024 * 1024,
        };
        let mut config = crate::ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::BackupRestore)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, 11_500).unwrap();
        let held = governor
            .try_admit(ResourceLane::BackupRestore, demand, 11_500)
            .unwrap();
        assert!(matches!(
            verify_cluster_restore_admission_governed(
                &certificate,
                &[],
                11_500,
                &ClusterRestoreAdmissionLimits::default(),
                &governor,
                demand,
                11_500,
            ),
            Err(BicDbError::ResourceGovernance(_))
        ));
        drop(held);
        assert_eq!(governor.snapshot().background.active, 0);
    }

    #[test]
    fn restore_run_is_single_owner_resource_gated_and_resumes_by_node() {
        let (directory, topology, _metadata, _nodes, certificate) = fixture();
        let run_path = directory.path().join(DEFAULT_CLUSTER_RESTORE_ADMISSION_RUN);
        let candidates = certificate
            .artifacts
            .iter()
            .enumerate()
            .map(|(index, artifact)| ClusterRestoreNodeCandidate {
                node_id: artifact.node_id.clone(),
                archive_path: directory.path().join(format!("archive-{index}.bin")),
                restored_root: directory.path().join(format!("node-{index}")),
                backup_passphrase: "never-persist-this-secret",
                database_encryption: None,
                restored_target_timestamp: certificate.plan.target_timestamp,
            })
            .collect::<Vec<_>>();
        let limits = ClusterRestoreAdmissionRunLimits::default();
        let mut run = ClusterRestoreAdmissionRun::create(
            &run_path,
            &certificate,
            &candidates,
            11_500,
            limits,
            false,
        )
        .unwrap();
        assert!(!String::from_utf8(fs::read(&run_path).unwrap())
            .unwrap()
            .contains("never-persist-this-secret"));
        assert!(ClusterRestoreAdmissionRun::open(&run_path, limits, false).is_err());

        let mut config = crate::ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::BackupRestore)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, 11_500).unwrap();
        let demand = ResourceDemand {
            memory_bytes: limits.admission.archive_hash_buffer_bytes as u64,
            io_bytes: limits.admission.archive_hash_buffer_bytes as u64,
            cpu_slots: 1,
            io_charge_bytes: limits.admission.archive_hash_buffer_bytes as u64,
        };
        let held = governor
            .try_admit(ResourceLane::BackupRestore, demand, 11_500)
            .unwrap();
        let first_node = run.state().candidates[0].node_id.clone();
        let first = candidates
            .iter()
            .find(|candidate| candidate.node_id == first_node)
            .unwrap();
        let mut verifier_calls = 0;
        assert!(matches!(
            run.advance_with_verifier(
                &certificate,
                Some(first),
                &governor,
                demand,
                11_501,
                |_candidate, _artifact, _cluster, _term, _target, _limits| {
                    verifier_calls += 1;
                    Ok((topology.clone(), evidence(&certificate, &first_node)))
                },
            ),
            Err(BicDbError::ResourceGovernance(_))
        ));
        assert_eq!(verifier_calls, 0);
        assert_eq!(run.state().next_candidate, 0);
        drop(held);
        run.advance_with_verifier(
            &certificate,
            Some(first),
            &governor,
            demand,
            11_502,
            |_candidate, _artifact, _cluster, _term, _target, _limits| {
                Ok((topology.clone(), evidence(&certificate, &first_node)))
            },
        )
        .unwrap();
        assert_eq!(run.state().next_candidate, 1);
        drop(run);

        let mut reopened = ClusterRestoreAdmissionRun::open(&run_path, limits, false).unwrap();
        let second_node = reopened.state().candidates[1].node_id.clone();
        let second = candidates
            .iter()
            .find(|candidate| candidate.node_id == second_node)
            .unwrap();
        reopened
            .advance_with_verifier(
                &certificate,
                Some(second),
                &governor,
                demand,
                11_503,
                |_candidate, _artifact, _cluster, _term, _target, _limits| {
                    Ok((topology.clone(), evidence(&certificate, &second_node)))
                },
            )
            .unwrap();
        let complete = reopened
            .advance_with_verifier(
                &certificate,
                None,
                &governor,
                demand,
                11_504,
                |_candidate, _artifact, _cluster, _term, _target, _limits| {
                    panic!("publishing must not re-run node verification")
                },
            )
            .unwrap();
        let ClusterRestoreAdmissionRunAdvance::Complete(report) = complete else {
            panic!("restore run did not publish")
        };
        report
            .validate(&certificate, &topology, &limits.admission)
            .unwrap();
        assert_eq!(governor.snapshot().background.active, 0);
        drop(reopened);
        load_cluster_restore_admission_run_state(&run_path, limits).unwrap();
    }

    #[test]
    fn admits_exact_certificate_nodes_and_persists_a_tamper_evident_gate() {
        let (directory, topology, _metadata, nodes, certificate) = fixture();
        let evidence = nodes
            .iter()
            .take(2)
            .map(|node_id| evidence(&certificate, node_id))
            .collect();
        let report = ClusterRestoreAdmissionReport::admit(
            &certificate,
            &topology,
            evidence,
            11_500,
            &ClusterRestoreAdmissionLimits::default(),
        )
        .unwrap();
        report
            .validate(
                &certificate,
                &topology,
                &ClusterRestoreAdmissionLimits::default(),
            )
            .unwrap();
        let path = directory.path().join(DEFAULT_CLUSTER_RESTORE_ADMISSION);
        save_cluster_restore_admission(&path, &report, false).unwrap();
        assert_eq!(load_cluster_restore_admission(&path).unwrap(), report);

        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(load_cluster_restore_admission(&path).is_err());
    }

    #[test]
    fn rejects_missing_nodes_schema_drift_and_stale_consensus_evidence() {
        let (_directory, topology, _metadata, nodes, certificate) = fixture();
        let limits = ClusterRestoreAdmissionLimits::default();
        assert!(ClusterRestoreAdmissionReport::admit(
            &certificate,
            &topology,
            vec![evidence(&certificate, &nodes[0])],
            11_500,
            &limits,
        )
        .is_err());

        let mut invalid = nodes
            .iter()
            .take(2)
            .map(|node_id| evidence(&certificate, node_id))
            .collect::<Vec<_>>();
        invalid[0].schema_sha256 = "d".repeat(64);
        assert!(ClusterRestoreAdmissionReport::admit(
            &certificate,
            &topology,
            invalid,
            11_500,
            &limits,
        )
        .is_err());

        let mut invalid = nodes
            .iter()
            .take(2)
            .map(|node_id| evidence(&certificate, node_id))
            .collect::<Vec<_>>();
        invalid[0].metadata_commit_index = certificate.plan.metadata_commit_index - 1;
        assert!(ClusterRestoreAdmissionReport::admit(
            &certificate,
            &topology,
            invalid,
            11_500,
            &limits,
        )
        .is_err());

        let mut invalid = nodes
            .iter()
            .take(2)
            .map(|node_id| evidence(&certificate, node_id))
            .collect::<Vec<_>>();
        invalid[0].restored_target_timestamp = Some(42);
        assert!(ClusterRestoreAdmissionReport::admit(
            &certificate,
            &topology,
            invalid,
            11_500,
            &limits,
        )
        .is_err());
    }

    #[test]
    fn archive_hashing_is_single_pass_bounded_and_size_fenced() {
        let bytes = (0_u8..=127).collect::<Vec<_>>();
        let mut reader = HashingBoundedReader::new(std::io::Cursor::new(&bytes), 7, 128);
        let hash = reader.finish_hashing().unwrap();
        assert_eq!(reader.total_bytes, 128);
        assert_eq!(hash, hex::encode(Sha256::digest(&bytes)));

        let mut oversized = HashingBoundedReader::new(std::io::Cursor::new(&bytes), 7, 127);
        assert!(oversized.finish_hashing().is_err());
    }
}
