//! Crash-resumable orchestration for quorum-certified cluster backups.
//!
//! The immutable backup plan and node artifacts use the control formats from
//! [`crate::distribution_backup`]. Progress is recorded in an append-only,
//! checksummed journal. Only compact ordered range IDs and file offsets are
//! retained in memory, so a run can cover very large range maps without
//! retaining every decoded quorum record.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, RangeId, RangeReplicaRole,
};
use crate::distribution_backup::{
    load_cluster_backup_certificate, load_cluster_backup_plan, load_cluster_node_backup_artifact,
    save_cluster_backup_certificate, save_cluster_backup_plan, save_cluster_node_backup_artifact,
    ClusterBackupCertificate, ClusterBackupLimits, ClusterBackupPlan, ClusterNodeBackupArtifact,
    DEFAULT_CLUSTER_BACKUP_CERTIFICATE, DEFAULT_CLUSTER_BACKUP_PLAN,
};
use crate::distribution_consensus::MetadataConsensusStatus;
use crate::distribution_range_consensus::RangeBackupFenceQuorum;
use crate::distribution_supervisor::ClusterBackupMetadata;
use crate::error::{BicDbError, Result};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const CLUSTER_BACKUP_RUN_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_BACKUP_RUN_DIR: &str = "cluster-backup-run";
pub const DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL: &str = "cluster-backup-run.journal";
pub const DEFAULT_CLUSTER_BACKUP_RUN_MANIFEST: &str = "cluster-backup-run-manifest.json";

const RUN_JOURNAL_MAGIC: &[u8; 8] = b"BICCBR01";
const RUN_JOURNAL_HEADER_BYTES: u64 = RUN_JOURNAL_MAGIC.len() as u64;
const RUN_FRAME_HEADER_BYTES: usize = 4 + 32;
const RUN_LOCK_FILE: &str = "cluster-backup-run.lock";
const RUN_ARTIFACT_DIR: &str = "artifacts";
const MAX_ABORT_REASON_BYTES: usize = 4 * 1024;

fn run_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("cluster backup run: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupRunLimits {
    /// Certificate topology/observation bounds are pinned into the immutable
    /// run manifest so a restart cannot silently widen them.
    pub cluster_limits: ClusterBackupLimits,
    /// Maximum number of ranges or nodes attempted by one `*_next_batch` call.
    pub max_range_batch: usize,
    pub max_node_batch: usize,
    /// Maximum serialized size of one journal event. This bounds recovery and
    /// append scratch memory independently of corpus size.
    pub max_event_bytes: usize,
    /// Hard bound for the append-only progress journal.
    pub max_journal_bytes: u64,
}

impl Default for ClusterBackupRunLimits {
    fn default() -> Self {
        Self {
            cluster_limits: ClusterBackupLimits::default(),
            max_range_batch: 256,
            max_node_batch: 16,
            max_event_bytes: 16 * 1024 * 1024,
            max_journal_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

impl ClusterBackupRunLimits {
    pub fn validate(&self) -> Result<()> {
        self.cluster_limits.validate()?;
        if self.max_range_batch == 0
            || self.max_range_batch > 4_096
            || self.max_node_batch == 0
            || self.max_node_batch > 1_024
            || self.max_event_bytes < 4 * 1024
            || self.max_event_bytes > 64 * 1024 * 1024
            || self.max_journal_bytes < 1024 * 1024
            || self.max_journal_bytes > 64 * 1024 * 1024 * 1024
        {
            return Err(run_error(
                "limits require range batches 1..=4096, node batches 1..=1024, event bytes 4KiB..=64MiB, and journal bytes 1MiB..=64GiB",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterBackupRunPhase {
    Fencing,
    Capturing,
    Certifying,
    Releasing,
    Complete,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupRunStatus {
    pub plan_id: Uuid,
    pub phase: ClusterBackupRunPhase,
    pub total_ranges: usize,
    pub fenced_ranges: usize,
    pub artifact_nodes: usize,
    pub released_ranges: usize,
    pub certificate_id: Option<Uuid>,
    pub abort_reason: Option<String>,
    pub journal_bytes: u64,
    pub recovered_trailing_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ClusterBackupRunManifest {
    format_version: u32,
    plan_id: Uuid,
    plan_checksum_sha256: String,
    limits: ClusterBackupRunLimits,
    checksum_sha256: String,
}

impl ClusterBackupRunManifest {
    fn create(plan: &ClusterBackupPlan, limits: ClusterBackupRunLimits) -> Result<Self> {
        limits.validate()?;
        if plan.range_epochs.len() > limits.cluster_limits.max_ranges {
            return Err(run_error(
                "backup plan exceeds the run's immutable range bound",
            ));
        }
        let mut manifest = Self {
            format_version: CLUSTER_BACKUP_RUN_FORMAT_VERSION,
            plan_id: plan.plan_id,
            plan_checksum_sha256: plan.checksum_sha256.clone(),
            limits,
            checksum_sha256: String::new(),
        };
        manifest.checksum_sha256 = manifest.calculate_checksum()?;
        Ok(manifest)
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            plan_id: Uuid,
            plan_checksum_sha256: &'a str,
            limits: ClusterBackupRunLimits,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            plan_id: self.plan_id,
            plan_checksum_sha256: &self.plan_checksum_sha256,
            limits: self.limits,
        })
    }

    fn validate(&self, plan: &ClusterBackupPlan) -> Result<()> {
        self.limits.validate()?;
        if self.format_version != CLUSTER_BACKUP_RUN_FORMAT_VERSION
            || self.plan_id != plan.plan_id
            || self.plan_checksum_sha256 != plan.checksum_sha256
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(run_error(
                "run manifest is damaged or belongs to another backup plan",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ArtifactReference {
    node_id: ClusterNodeId,
    backup_id: Uuid,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CertificateReference {
    certificate_id: Uuid,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ClusterBackupRunEvent {
    FenceInstalled {
        ordinal: u64,
        fence: RangeBackupFenceQuorum,
    },
    FencingComplete,
    ArtifactRegistered {
        artifact: ArtifactReference,
    },
    CaptureComplete,
    CertificatePublished {
        certificate: CertificateReference,
    },
    AbortStarted {
        reason: String,
    },
    FenceReleased {
        ordinal: u64,
        range_id: RangeId,
    },
    RunFinished,
}

/// Exclusive, restart-safe coordinator state for one cluster backup plan.
///
/// The journal is authoritative. An incomplete final frame is truncated on
/// open; a complete frame with a bad checksum is treated as corruption. The
/// OS lock prevents two processes from advancing one run concurrently.
pub struct ClusterBackupRun {
    root: PathBuf,
    fsync: bool,
    _lock_file: File,
    journal: File,
    plan: ClusterBackupPlan,
    limits: ClusterBackupRunLimits,
    phase: ClusterBackupRunPhase,
    range_ids: Vec<RangeId>,
    fence_offsets: Vec<u64>,
    capture_nodes: Option<Vec<ClusterNodeId>>,
    artifact_refs: BTreeMap<ClusterNodeId, ArtifactReference>,
    released_ranges: usize,
    certificate: Option<CertificateReference>,
    abort_reason: Option<String>,
    journal_bytes: u64,
    recovered_trailing_bytes: u64,
}

impl ClusterBackupRun {
    pub fn create(
        root: impl AsRef<Path>,
        plan: ClusterBackupPlan,
        fsync: bool,
        limits: ClusterBackupRunLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if plan.calculate_checksum()? != plan.checksum_sha256 || plan.plan_id.is_nil() {
            return Err(run_error("refusing to start from a damaged backup plan"));
        }
        let root = root.as_ref().to_path_buf();
        if root.exists() {
            return Err(run_error(format!(
                "run directory {} already exists; open it to resume",
                root.display()
            )));
        }
        let parent = root
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let file_name = root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| run_error("run directory must have a UTF-8 final component"))?;
        let staging = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));
        fs::create_dir(&staging)?;
        let mut initialization = RunInitializationGuard {
            path: staging.clone(),
            published: false,
        };
        fs::create_dir(staging.join(RUN_ARTIFACT_DIR))?;
        save_cluster_backup_plan(staging.join(DEFAULT_CLUSTER_BACKUP_PLAN), &plan, fsync)?;
        let manifest = ClusterBackupRunManifest::create(&plan, limits)?;
        write_json_atomic(
            &staging.join(DEFAULT_CLUSTER_BACKUP_RUN_MANIFEST),
            &manifest,
            fsync,
        )?;
        crate::storage::write_atomic(
            &staging.join(DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL),
            RUN_JOURNAL_MAGIC,
            fsync,
        )?;
        fs::rename(&staging, &root)?;
        initialization.published = true;
        if fsync {
            sync_parent_directory(&root)?;
        }
        Self::open(root, fsync)
    }

    pub fn open(root: impl AsRef<Path>, fsync: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        reject_symlink(&root, "run directory")?;
        let plan_path = root.join(DEFAULT_CLUSTER_BACKUP_PLAN);
        reject_symlink(&plan_path, "backup plan")?;
        reject_symlink(&root.join(RUN_ARTIFACT_DIR), "artifact directory")?;
        let plan = load_cluster_backup_plan(plan_path)?;
        let manifest_path = root.join(DEFAULT_CLUSTER_BACKUP_RUN_MANIFEST);
        reject_symlink(&manifest_path, "run manifest")?;
        let manifest: ClusterBackupRunManifest =
            serde_json::from_slice(&read_bounded_file(&manifest_path, 1024 * 1024)?)?;
        manifest.validate(&plan)?;
        let lock_path = root.join(RUN_LOCK_FILE);
        reject_symlink_if_exists(&lock_path, "run lock")?;
        let lock_file = open_run_lock(&lock_path)?;
        let journal_path = root.join(DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL);
        reject_symlink(&journal_path, "run journal")?;
        let journal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&journal_path)?;
        let range_ids = plan.range_epochs.keys().copied().collect::<Vec<_>>();
        let mut run = Self {
            root,
            fsync,
            _lock_file: lock_file,
            journal,
            plan,
            limits: manifest.limits,
            phase: ClusterBackupRunPhase::Fencing,
            range_ids,
            fence_offsets: Vec::new(),
            capture_nodes: None,
            artifact_refs: BTreeMap::new(),
            released_ranges: 0,
            certificate: None,
            abort_reason: None,
            journal_bytes: 0,
            recovered_trailing_bytes: 0,
        };
        run.recover_journal()?;
        run.validate_external_references()?;
        Ok(run)
    }

    pub fn plan(&self) -> &ClusterBackupPlan {
        &self.plan
    }

    pub fn limits(&self) -> ClusterBackupRunLimits {
        self.limits
    }

    pub fn status(&self) -> ClusterBackupRunStatus {
        ClusterBackupRunStatus {
            plan_id: self.plan.plan_id,
            phase: self.phase,
            total_ranges: self.range_ids.len(),
            fenced_ranges: self.fence_offsets.len(),
            artifact_nodes: self.artifact_refs.len(),
            released_ranges: self.released_ranges,
            certificate_id: self
                .certificate
                .as_ref()
                .map(|certificate| certificate.certificate_id),
            abort_reason: self.abort_reason.clone(),
            journal_bytes: self.journal_bytes,
            recovered_trailing_bytes: self.recovered_trailing_bytes,
        }
    }

    pub fn next_range_batch(&self, requested: usize) -> Result<Vec<RangeId>> {
        self.require_phase(ClusterBackupRunPhase::Fencing)?;
        let count = self.bounded_range_batch(requested)?;
        Ok(self.range_ids[self.fence_offsets.len()..]
            .iter()
            .take(count)
            .copied()
            .collect())
    }

    pub fn fence_next_batch<F>(
        &mut self,
        topology: &ClusterTopology,
        requested: usize,
        mut fence_range: F,
    ) -> Result<usize>
    where
        F: FnMut(RangeId) -> Result<RangeBackupFenceQuorum>,
    {
        self.validate_topology(topology)?;
        let batch = self.next_range_batch(requested)?;
        let mut completed = 0;
        for range_id in batch {
            let fence = fence_range(range_id)?;
            self.record_fence(topology, fence)?;
            completed += 1;
        }
        Ok(completed)
    }

    /// Resource-admitted form of [`Self::fence_next_batch`]. No range callback
    /// is invoked unless the backup/restore lane admits the complete bounded
    /// batch demand.
    pub fn fence_next_batch_governed<F>(
        &mut self,
        topology: &ClusterTopology,
        requested: usize,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
        fence_range: F,
    ) -> Result<usize>
    where
        F: FnMut(RangeId) -> Result<RangeBackupFenceQuorum>,
    {
        let permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
        let result = self.fence_next_batch(topology, requested, fence_range);
        drop(permit);
        result
    }

    pub fn record_fence(
        &mut self,
        topology: &ClusterTopology,
        fence: RangeBackupFenceQuorum,
    ) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Fencing)?;
        self.validate_topology(topology)?;
        let ordinal = self.fence_offsets.len();
        let expected = self.range_ids.get(ordinal).ok_or_else(|| {
            run_error("all planned ranges are already fenced; finish the fencing phase")
        })?;
        if fence.range_id != *expected {
            return Err(run_error(format!(
                "expected fence for range {expected}, received {}",
                fence.range_id
            )));
        }
        validate_fence(&self.plan, topology, &fence)?;
        self.append_event(ClusterBackupRunEvent::FenceInstalled {
            ordinal: ordinal as u64,
            fence,
        })
    }

    pub fn finish_fencing(&mut self) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Fencing)?;
        if self.fence_offsets.len() != self.range_ids.len() {
            return Err(run_error(format!(
                "cannot finish fencing with {}/{} ranges",
                self.fence_offsets.len(),
                self.range_ids.len()
            )));
        }
        self.append_event(ClusterBackupRunEvent::FencingComplete)
    }

    pub fn next_node_batch(
        &mut self,
        topology: &ClusterTopology,
        requested: usize,
    ) -> Result<Vec<ClusterNodeId>> {
        self.require_phase(ClusterBackupRunPhase::Capturing)?;
        self.validate_topology(topology)?;
        let count = self.bounded_node_batch(requested)?;
        self.ensure_capture_nodes(topology)?;
        Ok(self
            .capture_nodes
            .as_ref()
            .expect("capture nodes initialized")
            .iter()
            .filter(|node_id| !self.artifact_refs.contains_key(node_id))
            .take(count)
            .cloned()
            .collect())
    }

    pub fn capture_next_batch<F>(
        &mut self,
        topology: &ClusterTopology,
        requested: usize,
        mut capture_node: F,
    ) -> Result<usize>
    where
        F: FnMut(&ClusterNodeId) -> Result<ClusterNodeBackupArtifact>,
    {
        let batch = self.next_node_batch(topology, requested)?;
        let mut completed = 0;
        for node_id in batch {
            let artifact = capture_node(&node_id)?;
            if artifact.node_id != node_id {
                return Err(run_error(format!(
                    "capture for {node_id} returned artifact for {}",
                    artifact.node_id
                )));
            }
            self.register_artifact(topology, artifact)?;
            completed += 1;
        }
        Ok(completed)
    }

    /// Resource-admitted form of [`Self::capture_next_batch`]. The permit is
    /// scoped to this bounded batch and is released on every callback or
    /// journal error.
    pub fn capture_next_batch_governed<F>(
        &mut self,
        topology: &ClusterTopology,
        requested: usize,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
        capture_node: F,
    ) -> Result<usize>
    where
        F: FnMut(&ClusterNodeId) -> Result<ClusterNodeBackupArtifact>,
    {
        let permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
        let result = self.capture_next_batch(topology, requested, capture_node);
        drop(permit);
        result
    }

    pub fn register_artifact(
        &mut self,
        topology: &ClusterTopology,
        artifact: ClusterNodeBackupArtifact,
    ) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Capturing)?;
        self.validate_topology(topology)?;
        artifact.validate_against(&self.plan, &self.limits.cluster_limits)?;
        self.ensure_capture_nodes(topology)?;
        if !self
            .capture_nodes
            .as_ref()
            .expect("capture nodes initialized")
            .contains(&artifact.node_id)
        {
            return Err(run_error(format!(
                "node {} is not part of a durable range-fence quorum",
                artifact.node_id
            )));
        }
        if let Some(existing) = self.artifact_refs.get(&artifact.node_id) {
            if existing.backup_id == artifact.backup_id
                && existing.checksum_sha256 == artifact.checksum_sha256
            {
                return Ok(());
            }
            return Err(run_error(format!(
                "node {} already registered a different artifact",
                artifact.node_id
            )));
        }
        let reference = ArtifactReference {
            node_id: artifact.node_id.clone(),
            backup_id: artifact.backup_id,
            checksum_sha256: artifact.checksum_sha256.clone(),
        };
        let artifact_path = self.artifact_path(&artifact.node_id);
        reject_symlink_if_exists(&artifact_path, "node artifact")?;
        save_cluster_node_backup_artifact(&artifact_path, &artifact, self.fsync)?;
        // A crash after the atomic artifact write and before this event leaves
        // an inert orphan that an idempotent retry safely replaces.
        self.append_event(ClusterBackupRunEvent::ArtifactRegistered {
            artifact: reference,
        })
    }

    pub fn finish_capture(&mut self, topology: &ClusterTopology) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Capturing)?;
        self.validate_topology(topology)?;
        self.ensure_capture_nodes(topology)?;
        let required = self
            .capture_nodes
            .as_ref()
            .expect("capture nodes initialized");
        if required
            .iter()
            .any(|node_id| !self.artifact_refs.contains_key(node_id))
        {
            return Err(run_error(format!(
                "cannot finish capture with {}/{} node artifacts",
                self.artifact_refs.len(),
                required.len()
            )));
        }
        self.append_event(ClusterBackupRunEvent::CaptureComplete)
    }

    pub fn publish_certificate(
        &mut self,
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        certified_at_ms: u64,
    ) -> Result<ClusterBackupCertificate> {
        self.require_phase(ClusterBackupRunPhase::Certifying)?;
        self.validate_topology(topology)?;
        let mut artifacts = Vec::with_capacity(self.artifact_refs.len());
        for node_id in self.artifact_refs.keys() {
            artifacts.push(load_cluster_node_backup_artifact(
                self.artifact_path(node_id),
            )?);
        }
        let certificate_path = self.root.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE);
        reject_symlink_if_exists(&certificate_path, "cluster backup certificate")?;
        let certificate = if certificate_path.exists() {
            let existing = load_cluster_backup_certificate(&certificate_path)?;
            existing.validate(topology, metadata, &self.limits.cluster_limits)?;
            if existing.plan != self.plan || existing.artifacts != artifacts {
                return Err(run_error(
                    "existing certificate does not match the journaled plan and artifacts",
                ));
            }
            existing
        } else {
            let certificate = ClusterBackupCertificate::certify(
                self.plan.clone(),
                topology,
                metadata,
                artifacts,
                certified_at_ms,
                &self.limits.cluster_limits,
            )?;
            self.validate_certificate_fences(&certificate)?;
            save_cluster_backup_certificate(&certificate_path, &certificate, self.fsync)?;
            certificate
        };
        self.validate_certificate_fences(&certificate)?;
        self.append_event(ClusterBackupRunEvent::CertificatePublished {
            certificate: CertificateReference {
                certificate_id: certificate.certificate_id,
                checksum_sha256: certificate.checksum_sha256.clone(),
            },
        })?;
        Ok(certificate)
    }

    pub fn publish_certificate_governed(
        &mut self,
        topology: &ClusterTopology,
        metadata: &MetadataConsensusStatus,
        certified_at_ms: u64,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<ClusterBackupCertificate> {
        let permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
        let result = self.publish_certificate(topology, metadata, certified_at_ms);
        drop(permit);
        result
    }

    pub fn begin_abort(&mut self, reason: impl Into<String>) -> Result<()> {
        if !matches!(
            self.phase,
            ClusterBackupRunPhase::Fencing
                | ClusterBackupRunPhase::Capturing
                | ClusterBackupRunPhase::Certifying
        ) {
            return Err(run_error("only an active backup run can be aborted"));
        }
        let reason = reason.into();
        if reason.trim().is_empty() || reason.len() > MAX_ABORT_REASON_BYTES {
            return Err(run_error("abort reason must contain 1..=4096 bytes"));
        }
        self.append_event(ClusterBackupRunEvent::AbortStarted { reason })
    }

    pub fn next_release_batch(&mut self, requested: usize) -> Result<Vec<RangeBackupFenceQuorum>> {
        self.require_phase(ClusterBackupRunPhase::Releasing)?;
        let count = self.bounded_range_batch(requested)?;
        let end = self
            .released_ranges
            .saturating_add(count)
            .min(self.fence_offsets.len());
        let mut fences = Vec::with_capacity(end.saturating_sub(self.released_ranges));
        for ordinal in self.released_ranges..end {
            fences.push(self.read_fence(ordinal)?);
        }
        Ok(fences)
    }

    pub fn release_next_batch<F>(&mut self, requested: usize, mut release: F) -> Result<usize>
    where
        F: FnMut(&RangeBackupFenceQuorum) -> Result<()>,
    {
        let fences = self.next_release_batch(requested)?;
        let mut completed = 0;
        for fence in fences {
            release(&fence)?;
            self.record_release(fence.range_id)?;
            completed += 1;
        }
        Ok(completed)
    }

    pub fn release_next_batch_governed<F>(
        &mut self,
        requested: usize,
        governor: &ResourceGovernor,
        demand: ResourceDemand,
        now_ms: u64,
        release: F,
    ) -> Result<usize>
    where
        F: FnMut(&RangeBackupFenceQuorum) -> Result<()>,
    {
        let permit = governor.try_admit(ResourceLane::BackupRestore, demand, now_ms)?;
        let result = self.release_next_batch(requested, release);
        drop(permit);
        result
    }

    pub fn record_release(&mut self, range_id: RangeId) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Releasing)?;
        let expected = self
            .range_ids
            .get(self.released_ranges)
            .filter(|_| self.released_ranges < self.fence_offsets.len())
            .ok_or_else(|| run_error("every installed range fence is already released"))?;
        if range_id != *expected {
            return Err(run_error(format!(
                "expected release for range {expected}, received {range_id}"
            )));
        }
        self.append_event(ClusterBackupRunEvent::FenceReleased {
            ordinal: self.released_ranges as u64,
            range_id,
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        self.require_phase(ClusterBackupRunPhase::Releasing)?;
        if self.released_ranges != self.fence_offsets.len() {
            return Err(run_error(format!(
                "cannot finish with {}/{} installed fences released",
                self.released_ranges,
                self.fence_offsets.len()
            )));
        }
        if self.certificate.is_none() && self.abort_reason.is_none() {
            return Err(run_error(
                "run has neither a published certificate nor an abort decision",
            ));
        }
        self.append_event(ClusterBackupRunEvent::RunFinished)
    }

    pub fn load_certificate(&self) -> Result<Option<ClusterBackupCertificate>> {
        if self.certificate.is_none() {
            return Ok(None);
        }
        let certificate_path = self.root.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE);
        reject_symlink(&certificate_path, "cluster backup certificate")?;
        Ok(Some(load_cluster_backup_certificate(certificate_path)?))
    }

    fn bounded_range_batch(&self, requested: usize) -> Result<usize> {
        if requested == 0 || requested > self.limits.max_range_batch {
            return Err(run_error(format!(
                "range batch must be within 1..={}",
                self.limits.max_range_batch
            )));
        }
        Ok(requested)
    }

    fn bounded_node_batch(&self, requested: usize) -> Result<usize> {
        if requested == 0 || requested > self.limits.max_node_batch {
            return Err(run_error(format!(
                "node batch must be within 1..={}",
                self.limits.max_node_batch
            )));
        }
        Ok(requested)
    }

    fn require_phase(&self, expected: ClusterBackupRunPhase) -> Result<()> {
        if self.phase != expected {
            return Err(run_error(format!(
                "operation requires {expected:?} phase; run is {:?}",
                self.phase
            )));
        }
        Ok(())
    }

    fn validate_topology(&self, topology: &ClusterTopology) -> Result<()> {
        topology.validate()?;
        let captured = ClusterBackupMetadata::capture(topology)?;
        if topology.cluster_id != self.plan.cluster_id
            || topology.generation != self.plan.topology_generation
            || captured.topology_sha256 != self.plan.topology_sha256
            || captured.range_epochs != self.plan.range_epochs
        {
            return Err(run_error(
                "committed topology changed from the immutable backup plan",
            ));
        }
        Ok(())
    }

    fn append_event(&mut self, event: ClusterBackupRunEvent) -> Result<()> {
        self.validate_event(&event)?;
        let payload = serialize_event_bounded(&event, self.limits.max_event_bytes)?;
        let frame_bytes = (RUN_FRAME_HEADER_BYTES as u64)
            .checked_add(payload.len() as u64)
            .ok_or_else(|| run_error("journal frame size overflow"))?;
        let next_bytes = self
            .journal_bytes
            .checked_add(frame_bytes)
            .ok_or_else(|| run_error("journal size overflow"))?;
        if next_bytes > self.limits.max_journal_bytes {
            return Err(run_error(format!(
                "journal would exceed its {} byte bound",
                self.limits.max_journal_bytes
            )));
        }
        let offset = self.journal.seek(SeekFrom::End(0))?;
        if offset != self.journal_bytes {
            return Err(run_error("journal length changed outside the coordinator"));
        }
        let length = u32::try_from(payload.len())
            .map_err(|_| run_error("journal event length exceeds u32"))?;
        let checksum = frame_checksum(length, &payload);
        self.journal.write_all(&length.to_le_bytes())?;
        self.journal.write_all(&checksum)?;
        self.journal.write_all(&payload)?;
        if self.fsync {
            self.journal.sync_data()?;
        }
        self.journal_bytes = next_bytes;
        self.apply_event(event, offset)
    }

    fn recover_journal(&mut self) -> Result<()> {
        let file_len = self.journal.metadata()?.len();
        if file_len > self.limits.max_journal_bytes {
            return Err(run_error(format!(
                "journal is {file_len} bytes, exceeding its {} byte bound",
                self.limits.max_journal_bytes
            )));
        }
        self.journal.seek(SeekFrom::Start(0))?;
        let mut magic = [0_u8; RUN_JOURNAL_MAGIC.len()];
        self.journal.read_exact(&mut magic)?;
        if &magic != RUN_JOURNAL_MAGIC {
            return Err(run_error("journal header is missing or invalid"));
        }
        let mut valid_up_to = RUN_JOURNAL_HEADER_BYTES;
        while valid_up_to < file_len {
            let remaining = file_len - valid_up_to;
            if remaining < RUN_FRAME_HEADER_BYTES as u64 {
                break;
            }
            self.journal.seek(SeekFrom::Start(valid_up_to))?;
            let mut length_bytes = [0_u8; 4];
            let mut expected_checksum = [0_u8; 32];
            self.journal.read_exact(&mut length_bytes)?;
            self.journal.read_exact(&mut expected_checksum)?;
            let length = u32::from_le_bytes(length_bytes) as usize;
            if length == 0 || length > self.limits.max_event_bytes {
                return Err(run_error(format!(
                    "journal frame at byte {valid_up_to} has invalid length {length}"
                )));
            }
            let frame_bytes = (RUN_FRAME_HEADER_BYTES as u64)
                .checked_add(length as u64)
                .ok_or_else(|| run_error("journal frame size overflow"))?;
            if remaining < frame_bytes {
                break;
            }
            let mut payload = vec![0_u8; length];
            self.journal.read_exact(&mut payload)?;
            if frame_checksum(length as u32, &payload) != expected_checksum {
                return Err(run_error(format!(
                    "journal frame at byte {valid_up_to} failed checksum verification"
                )));
            }
            let event: ClusterBackupRunEvent =
                serde_json::from_slice(&payload).map_err(|error| {
                    run_error(format!(
                        "journal frame at byte {valid_up_to} is not a valid event: {error}"
                    ))
                })?;
            self.validate_event(&event)?;
            self.apply_event(event, valid_up_to)?;
            valid_up_to += frame_bytes;
        }
        self.recovered_trailing_bytes = file_len.saturating_sub(valid_up_to);
        if self.recovered_trailing_bytes > 0 {
            self.journal.set_len(valid_up_to)?;
            if self.fsync {
                self.journal.sync_data()?;
            }
        }
        self.journal_bytes = valid_up_to;
        Ok(())
    }

    fn validate_event(&self, event: &ClusterBackupRunEvent) -> Result<()> {
        match event {
            ClusterBackupRunEvent::FenceInstalled { ordinal, fence } => {
                self.require_phase(ClusterBackupRunPhase::Fencing)?;
                if *ordinal != self.fence_offsets.len() as u64
                    || self.range_ids.get(*ordinal as usize) != Some(&fence.range_id)
                {
                    return Err(run_error("range fence journal sequence is not contiguous"));
                }
                validate_fence_shape(&self.plan, fence)
            }
            ClusterBackupRunEvent::FencingComplete => {
                self.require_phase(ClusterBackupRunPhase::Fencing)?;
                if self.fence_offsets.len() != self.range_ids.len() {
                    return Err(run_error("fencing completed before every planned range"));
                }
                Ok(())
            }
            ClusterBackupRunEvent::ArtifactRegistered { artifact } => {
                self.require_phase(ClusterBackupRunPhase::Capturing)?;
                if artifact.backup_id.is_nil()
                    || artifact.checksum_sha256.len() != 64
                    || self.artifact_refs.contains_key(&artifact.node_id)
                {
                    return Err(run_error(
                        "artifact journal reference is invalid or duplicate",
                    ));
                }
                Ok(())
            }
            ClusterBackupRunEvent::CaptureComplete => {
                self.require_phase(ClusterBackupRunPhase::Capturing)?;
                if self.artifact_refs.is_empty() {
                    return Err(run_error("capture completed without node artifacts"));
                }
                Ok(())
            }
            ClusterBackupRunEvent::CertificatePublished { certificate } => {
                self.require_phase(ClusterBackupRunPhase::Certifying)?;
                if certificate.certificate_id.is_nil()
                    || certificate.checksum_sha256.len() != 64
                    || self.certificate.is_some()
                    || self.abort_reason.is_some()
                {
                    return Err(run_error("certificate journal reference is invalid"));
                }
                Ok(())
            }
            ClusterBackupRunEvent::AbortStarted { reason } => {
                if !matches!(
                    self.phase,
                    ClusterBackupRunPhase::Fencing
                        | ClusterBackupRunPhase::Capturing
                        | ClusterBackupRunPhase::Certifying
                ) || reason.trim().is_empty()
                    || reason.len() > MAX_ABORT_REASON_BYTES
                    || self.certificate.is_some()
                    || self.abort_reason.is_some()
                {
                    return Err(run_error("abort journal decision is invalid"));
                }
                Ok(())
            }
            ClusterBackupRunEvent::FenceReleased { ordinal, range_id } => {
                self.require_phase(ClusterBackupRunPhase::Releasing)?;
                if *ordinal != self.released_ranges as u64
                    || self.released_ranges >= self.fence_offsets.len()
                    || self.range_ids.get(*ordinal as usize) != Some(range_id)
                {
                    return Err(run_error(
                        "range release journal sequence is not contiguous",
                    ));
                }
                Ok(())
            }
            ClusterBackupRunEvent::RunFinished => {
                self.require_phase(ClusterBackupRunPhase::Releasing)?;
                if self.released_ranges != self.fence_offsets.len()
                    || (self.certificate.is_none() == self.abort_reason.is_none())
                {
                    return Err(run_error("run finished before a terminal safe state"));
                }
                Ok(())
            }
        }
    }

    fn apply_event(&mut self, event: ClusterBackupRunEvent, offset: u64) -> Result<()> {
        match event {
            ClusterBackupRunEvent::FenceInstalled { .. } => self.fence_offsets.push(offset),
            ClusterBackupRunEvent::FencingComplete => self.phase = ClusterBackupRunPhase::Capturing,
            ClusterBackupRunEvent::ArtifactRegistered { artifact } => {
                self.artifact_refs
                    .insert(artifact.node_id.clone(), artifact);
            }
            ClusterBackupRunEvent::CaptureComplete => {
                self.phase = ClusterBackupRunPhase::Certifying
            }
            ClusterBackupRunEvent::CertificatePublished { certificate } => {
                self.certificate = Some(certificate);
                self.phase = ClusterBackupRunPhase::Releasing;
            }
            ClusterBackupRunEvent::AbortStarted { reason } => {
                self.abort_reason = Some(reason);
                self.phase = ClusterBackupRunPhase::Releasing;
            }
            ClusterBackupRunEvent::FenceReleased { .. } => self.released_ranges += 1,
            ClusterBackupRunEvent::RunFinished => {
                self.phase = if self.certificate.is_some() {
                    ClusterBackupRunPhase::Complete
                } else {
                    ClusterBackupRunPhase::Aborted
                };
            }
        }
        Ok(())
    }

    fn read_fence(&mut self, ordinal: usize) -> Result<RangeBackupFenceQuorum> {
        let offset = *self
            .fence_offsets
            .get(ordinal)
            .ok_or_else(|| run_error("range fence ordinal is outside the journal index"))?;
        let event = read_event_at(
            &mut self.journal,
            offset,
            self.limits.max_event_bytes,
            self.journal_bytes,
        )?;
        match event {
            ClusterBackupRunEvent::FenceInstalled {
                ordinal: stored,
                fence,
            } if stored == ordinal as u64 => Ok(fence),
            _ => Err(run_error("journal fence offset points to another event")),
        }
    }

    fn validate_certificate_fences(
        &mut self,
        certificate: &ClusterBackupCertificate,
    ) -> Result<()> {
        if certificate.range_barriers.len() != self.fence_offsets.len() {
            return Err(run_error(
                "certificate range count differs from installed backup fences",
            ));
        }
        for ordinal in 0..self.fence_offsets.len() {
            let fence = self.read_fence(ordinal)?;
            let barrier = certificate
                .range_barriers
                .get(&fence.range_id)
                .ok_or_else(|| run_error("certificate omitted a fenced range"))?;
            if barrier.range_epoch != fence.range_epoch
                || barrier.resolved_through != fence.resolved_through
                || barrier.required_quorum != fence.required_quorum
            {
                return Err(run_error(format!(
                    "certificate barrier for {} differs from the durable write fence",
                    fence.range_id
                )));
            }
        }
        Ok(())
    }

    fn ensure_capture_nodes(&mut self, topology: &ClusterTopology) -> Result<()> {
        if self.capture_nodes.is_some() {
            return Ok(());
        }
        let mut nodes = BTreeSet::new();
        for ordinal in 0..self.fence_offsets.len() {
            let fence = self.read_fence(ordinal)?;
            validate_fence(&self.plan, topology, &fence)?;
            for observation in fence.observations {
                let node = topology.nodes.get(&observation.node_id).ok_or_else(|| {
                    run_error(format!(
                        "fence for {} references unknown node {}",
                        fence.range_id, observation.node_id
                    ))
                })?;
                if node.lifecycle != ClusterNodeLifecycle::Active {
                    return Err(run_error(format!(
                        "fenced voter {} for {} is not active during capture",
                        node.id, fence.range_id
                    )));
                }
                nodes.insert(node.id.clone());
            }
        }
        if nodes.is_empty() {
            return Err(run_error("backup run has no fenced artifact nodes"));
        }
        self.capture_nodes = Some(nodes.into_iter().collect());
        Ok(())
    }

    fn validate_external_references(&self) -> Result<()> {
        for (node_id, reference) in &self.artifact_refs {
            let artifact_path = self.artifact_path(node_id);
            reject_symlink(&artifact_path, "node artifact")?;
            let artifact = load_cluster_node_backup_artifact(artifact_path)?;
            if artifact.node_id != *node_id
                || artifact.backup_id != reference.backup_id
                || artifact.checksum_sha256 != reference.checksum_sha256
            {
                return Err(run_error(format!(
                    "artifact file for {node_id} differs from its durable journal reference"
                )));
            }
        }
        if let Some(reference) = &self.certificate {
            let certificate_path = self.root.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE);
            reject_symlink(&certificate_path, "cluster backup certificate")?;
            let certificate = load_cluster_backup_certificate(certificate_path)?;
            if certificate.certificate_id != reference.certificate_id
                || certificate.checksum_sha256 != reference.checksum_sha256
            {
                return Err(run_error(
                    "certificate file differs from its durable journal reference",
                ));
            }
        }
        Ok(())
    }

    fn artifact_path(&self, node_id: &ClusterNodeId) -> PathBuf {
        self.root
            .join(RUN_ARTIFACT_DIR)
            .join(format!("{}.json", node_id.as_str()))
    }
}

fn validate_fence_shape(plan: &ClusterBackupPlan, fence: &RangeBackupFenceQuorum) -> Result<()> {
    let expected_epoch = plan.range_epochs.get(&fence.range_id);
    let mut nodes = BTreeSet::new();
    if fence.plan_id != plan.plan_id
        || expected_epoch != Some(&fence.range_epoch)
        || fence.required_quorum == 0
        || fence.observations.len() != fence.required_quorum
        || fence.installed_at_ms < plan.created_at_ms
        || fence.installed_at_ms > plan.expires_at_ms
        || fence.expires_at_ms != plan.expires_at_ms
    {
        return Err(run_error(format!(
            "range {} fence identity, lifetime, or quorum is invalid",
            fence.range_id
        )));
    }
    let mut previous_node = None;
    for observation in &fence.observations {
        if observation.range_id != fence.range_id
            || observation.current_epoch != fence.range_epoch
            || observation.last_index != fence.resolved_through
            || observation.resolved_through != fence.resolved_through
            || observation.compacted_through > observation.resolved_through
            || !nodes.insert(observation.node_id.clone())
            || previous_node
                .as_ref()
                .is_some_and(|previous| previous >= &observation.node_id)
        {
            return Err(run_error(format!(
                "range {} fence observations are divergent or non-canonical",
                fence.range_id
            )));
        }
        previous_node = Some(observation.node_id.clone());
    }
    Ok(())
}

fn validate_fence(
    plan: &ClusterBackupPlan,
    topology: &ClusterTopology,
    fence: &RangeBackupFenceQuorum,
) -> Result<()> {
    validate_fence_shape(plan, fence)?;
    let range = topology
        .range_by_id(fence.range_id)
        .ok_or_else(|| run_error(format!("unknown fenced range {}", fence.range_id)))?;
    let required_quorum = range.voter_count() / 2 + 1;
    if fence.range_epoch != range.epoch
        || fence.required_quorum != required_quorum
        || !fence
            .observations
            .iter()
            .any(|observation| observation.node_id == range.leader)
        || fence.observations.iter().any(|observation| {
            !range.replicas.iter().any(|replica| {
                replica.node_id == observation.node_id && replica.role == RangeReplicaRole::Voter
            })
        })
    {
        return Err(run_error(format!(
            "range {} fence is not a current leader-inclusive voter quorum",
            fence.range_id
        )));
    }
    Ok(())
}

fn read_event_at(
    journal: &mut File,
    offset: u64,
    max_event_bytes: usize,
    journal_bytes: u64,
) -> Result<ClusterBackupRunEvent> {
    if offset < RUN_JOURNAL_HEADER_BYTES
        || offset
            .checked_add(RUN_FRAME_HEADER_BYTES as u64)
            .is_none_or(|end| end > journal_bytes)
    {
        return Err(run_error(
            "journal event offset is outside the durable file",
        ));
    }
    journal.seek(SeekFrom::Start(offset))?;
    let mut length_bytes = [0_u8; 4];
    let mut expected_checksum = [0_u8; 32];
    journal.read_exact(&mut length_bytes)?;
    journal.read_exact(&mut expected_checksum)?;
    let length = u32::from_le_bytes(length_bytes) as usize;
    let end = offset
        .checked_add(RUN_FRAME_HEADER_BYTES as u64)
        .and_then(|value| value.checked_add(length as u64))
        .ok_or_else(|| run_error("journal event offset overflow"))?;
    if length == 0 || length > max_event_bytes || end > journal_bytes {
        return Err(run_error("journal event length is invalid"));
    }
    let mut payload = vec![0_u8; length];
    journal.read_exact(&mut payload)?;
    if frame_checksum(length as u32, &payload) != expected_checksum {
        return Err(run_error("journal event checksum mismatch"));
    }
    serde_json::from_slice(&payload)
        .map_err(|error| run_error(format!("journal event decode failed: {error}")))
}

fn frame_checksum(length: u32, payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(RUN_JOURNAL_MAGIC);
    hasher.update(length.to_le_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

fn serialize_event_bounded(
    event: &ClusterBackupRunEvent,
    max_event_bytes: usize,
) -> Result<Vec<u8>> {
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
                .ok_or_else(|| std::io::Error::other("journal event size overflow"))?;
            if next > self.max_bytes {
                return Err(std::io::Error::other(format!(
                    "journal event exceeds {} bytes",
                    self.max_bytes
                )));
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
        max_bytes: max_event_bytes,
    };
    serde_json::to_writer(&mut writer, event)?;
    if writer.bytes.is_empty() {
        return Err(run_error("journal event serialized to an empty payload"));
    }
    Ok(writer.bytes)
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

fn write_json_atomic(path: &Path, value: &impl Serialize, fsync: bool) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    if bytes.len() > 1024 * 1024 {
        return Err(run_error("run manifest exceeds its 1MiB bound"));
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn read_bounded_file(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length > max_bytes {
        return Err(run_error(format!(
            "{} is {length} bytes, exceeding its {max_bytes} byte bound",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(run_error(format!(
            "{} grew beyond its read bound",
            path.display()
        )));
    }
    Ok(bytes)
}

struct RunInitializationGuard {
    path: PathBuf,
    published: bool,
}

impl Drop for RunInitializationGuard {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn reject_symlink(path: &Path, kind: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(run_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(run_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn open_run_lock(path: &Path) -> Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(run_error(format!(
            "run lock {} is not a regular file",
            path.display()
        )));
    }
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(run_error(format!(
            "another coordinator owns {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        )));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_run_lock(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .custom_flags(0x0020_0000) // FILE_FLAG_OPEN_REPARSE_POINT
        .open(path)
        .map_err(|error| {
            run_error(format!(
                "another coordinator owns {}: {error}",
                path.display()
            ))
        })
}

#[cfg(not(any(unix, windows)))]
fn open_run_lock(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{
        ClusterId, ClusterNode, DistributionConfig, DistributionStore, RangeReplica, ReplicaId,
        SCHEMA_COMPATIBILITY_NODE_LABEL,
    };
    use crate::distribution_consensus::MetadataConsensusRole;
    use crate::distribution_range_consensus::RangeWriteProgress;

    const CREATED_AT_MS: u64 = 10_000;
    const CAPTURED_AT_MS: u64 = 10_500;

    fn fixture() -> (
        tempfile::TempDir,
        ClusterTopology,
        MetadataConsensusStatus,
        Vec<ClusterNodeId>,
        ClusterBackupPlan,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let nodes = ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|node| ClusterNodeId::new(node).unwrap())
            .collect::<Vec<_>>();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("backup-run-test").unwrap(),
            node_id: nodes[0].clone(),
            node_address: "127.0.0.1:9401".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor: 3,
            initial_ranges: 2,
            ..DistributionConfig::default()
        };
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        let mut topology = store.topology().clone();
        for (index, node_id) in nodes.iter().enumerate().skip(1) {
            topology.nodes.insert(
                node_id.clone(),
                ClusterNode::new(
                    node_id.clone(),
                    format!("127.0.0.1:{}", 9401 + index),
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
        let mut next_replica = topology.next_replica_id;
        for range in topology.ranges.values_mut() {
            for node_id in nodes.iter().skip(1) {
                range.replicas.push(RangeReplica {
                    id: ReplicaId::new(next_replica).unwrap(),
                    node_id: node_id.clone(),
                    role: RangeReplicaRole::Voter,
                });
                next_replica += 1;
            }
        }
        topology.next_replica_id = next_replica;
        topology.validate().unwrap();
        let metadata = MetadataConsensusStatus {
            cluster_id: topology.cluster_id.clone(),
            node_id: nodes[0].clone(),
            role: MetadataConsensusRole::Leader,
            current_term: 2,
            voted_for: Some(nodes[0].clone()),
            leader_id: Some(nodes[0].clone()),
            commit_index: 4,
            last_log_index: 4,
            last_log_term: 2,
            topology_generation: topology.generation,
            voters: nodes.clone(),
            learners: Vec::new(),
        };
        let plan = ClusterBackupPlan::create(
            &topology,
            &metadata,
            None,
            CREATED_AT_MS,
            1_000,
            &ClusterBackupLimits::default(),
        )
        .unwrap();
        (directory, topology, metadata, nodes, plan)
    }

    fn fence(
        plan: &ClusterBackupPlan,
        topology: &ClusterTopology,
        range_id: RangeId,
        nodes: &[ClusterNodeId],
    ) -> RangeBackupFenceQuorum {
        let range = topology.range_by_id(range_id).unwrap();
        let observations = nodes
            .iter()
            .take(2)
            .map(|node_id| RangeWriteProgress {
                node_id: node_id.clone(),
                range_id,
                current_epoch: range.epoch,
                last_index: 7,
                resolved_through: 7,
                compacted_through: 6,
            })
            .collect();
        RangeBackupFenceQuorum {
            plan_id: plan.plan_id,
            range_id,
            range_epoch: range.epoch,
            resolved_through: 7,
            required_quorum: 2,
            installed_at_ms: CREATED_AT_MS,
            expires_at_ms: plan.expires_at_ms,
            observations,
        }
    }

    fn artifact(
        plan: &ClusterBackupPlan,
        topology: &ClusterTopology,
        node_id: &ClusterNodeId,
    ) -> ClusterNodeBackupArtifact {
        artifact_at(plan, topology, node_id, 7)
    }

    fn artifact_at(
        plan: &ClusterBackupPlan,
        topology: &ClusterTopology,
        node_id: &ClusterNodeId,
        resolved_through: u64,
    ) -> ClusterNodeBackupArtifact {
        let ranges = topology
            .ranges
            .values()
            .map(|range| RangeWriteProgress {
                node_id: node_id.clone(),
                range_id: range.id,
                current_epoch: range.epoch,
                last_index: resolved_through,
                resolved_through,
                compacted_through: resolved_through.saturating_sub(1),
            })
            .collect();
        ClusterNodeBackupArtifact::create(
            plan,
            node_id.clone(),
            Uuid::now_v7(),
            "b".repeat(64),
            "c".repeat(64),
            "a".repeat(64),
            CAPTURED_AT_MS,
            ranges,
            &ClusterBackupLimits::default(),
        )
        .unwrap()
    }

    fn governed_demand() -> ResourceDemand {
        ResourceDemand {
            memory_bytes: 8 * 1024 * 1024,
            io_bytes: 8 * 1024 * 1024,
            cpu_slots: 1,
            io_charge_bytes: 8 * 1024 * 1024,
        }
    }

    #[test]
    fn governed_backup_batch_rejects_before_callback_and_releases_permit() {
        let (directory, topology, _metadata, nodes, plan) = fixture();
        let mut run = ClusterBackupRun::create(
            directory.path().join("governed-run"),
            plan.clone(),
            false,
            ClusterBackupRunLimits::default(),
        )
        .unwrap();
        let mut config = crate::ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::BackupRestore)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, CREATED_AT_MS).unwrap();
        let held = governor
            .try_admit(
                ResourceLane::BackupRestore,
                governed_demand(),
                CREATED_AT_MS,
            )
            .unwrap();
        let mut callbacks = 0;
        assert!(matches!(
            run.fence_next_batch_governed(
                &topology,
                1,
                &governor,
                governed_demand(),
                CREATED_AT_MS,
                |range_id| {
                    callbacks += 1;
                    Ok(fence(&plan, &topology, range_id, &nodes))
                },
            ),
            Err(BicDbError::ResourceGovernance(_))
        ));
        assert_eq!(callbacks, 0);
        assert_eq!(run.status().fenced_ranges, 0);
        drop(held);

        assert_eq!(
            run.fence_next_batch_governed(
                &topology,
                1,
                &governor,
                governed_demand(),
                CREATED_AT_MS,
                |range_id| Ok(fence(&plan, &topology, range_id, &nodes)),
            )
            .unwrap(),
            1
        );
        assert_eq!(governor.snapshot().background.active, 0);
    }

    #[test]
    fn resumes_every_phase_and_releases_fences_idempotently() {
        let (directory, topology, metadata, nodes, plan) = fixture();
        let root = directory.path().join("run");
        let mut run = ClusterBackupRun::create(
            &root,
            plan.clone(),
            true,
            ClusterBackupRunLimits {
                max_range_batch: 1,
                max_node_batch: 1,
                ..ClusterBackupRunLimits::default()
            },
        )
        .unwrap();
        assert_eq!(run.next_range_batch(1).unwrap().len(), 1);
        run.fence_next_batch(&topology, 1, |range_id| {
            Ok(fence(&plan, &topology, range_id, &nodes))
        })
        .unwrap();
        drop(run);

        // A torn final frame is discarded without losing the last complete
        // range checkpoint.
        let journal_path = root.join(DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL);
        OpenOptions::new()
            .append(true)
            .open(&journal_path)
            .unwrap()
            .write_all(&[1, 2, 3])
            .unwrap();
        let mut run = ClusterBackupRun::open(&root, true).unwrap();
        assert_eq!(run.status().fenced_ranges, 1);
        assert_eq!(run.status().recovered_trailing_bytes, 3);
        run.fence_next_batch(&topology, 1, |range_id| {
            Ok(fence(&plan, &topology, range_id, &nodes))
        })
        .unwrap();
        run.finish_fencing().unwrap();

        while !run.next_node_batch(&topology, 1).unwrap().is_empty() {
            run.capture_next_batch(&topology, 1, |node_id| {
                Ok(artifact(&plan, &topology, node_id))
            })
            .unwrap();
        }
        assert_eq!(run.status().artifact_nodes, 2);
        assert!(!run.artifact_refs.contains_key(&nodes[2]));
        run.finish_capture(&topology).unwrap();
        let certificate = run
            .publish_certificate(&topology, &metadata, CAPTURED_AT_MS)
            .unwrap();
        assert_eq!(certificate.range_barriers.len(), 2);
        drop(run);

        let mut run = ClusterBackupRun::open(&root, true).unwrap();
        assert_eq!(run.status().phase, ClusterBackupRunPhase::Releasing);
        let mut calls = 0;
        let error = run
            .release_next_batch(1, |_| {
                calls += 1;
                Err(run_error("simulated release interruption"))
            })
            .unwrap_err();
        assert!(error.to_string().contains("simulated release interruption"));
        assert_eq!(calls, 1);
        drop(run);

        let mut run = ClusterBackupRun::open(&root, true).unwrap();
        while !run.next_release_batch(1).unwrap().is_empty() {
            run.release_next_batch(1, |_| Ok(())).unwrap();
            drop(run);
            run = ClusterBackupRun::open(&root, true).unwrap();
        }
        run.finish().unwrap();
        assert_eq!(run.status().phase, ClusterBackupRunPhase::Complete);
        assert_eq!(run.load_certificate().unwrap().unwrap(), certificate);
    }

    #[test]
    fn abort_releases_only_durably_recorded_fences_and_survives_restart() {
        let (directory, topology, _metadata, nodes, plan) = fixture();
        let root = directory.path().join("abort-run");
        let mut run = ClusterBackupRun::create(
            &root,
            plan.clone(),
            false,
            ClusterBackupRunLimits::default(),
        )
        .unwrap();
        run.fence_next_batch(&topology, 1, |range_id| {
            Ok(fence(&plan, &topology, range_id, &nodes))
        })
        .unwrap();
        run.begin_abort("operator canceled capture").unwrap();
        drop(run);

        let mut run = ClusterBackupRun::open(&root, false).unwrap();
        assert_eq!(run.next_release_batch(256).unwrap().len(), 1);
        run.release_next_batch(256, |_| Ok(())).unwrap();
        run.finish().unwrap();
        assert_eq!(run.status().phase, ClusterBackupRunPhase::Aborted);
        assert_eq!(run.status().released_ranges, 1);
    }

    #[test]
    fn complete_frame_tampering_and_concurrent_open_fail_closed() {
        let (directory, topology, _metadata, nodes, plan) = fixture();
        let root = directory.path().join("tamper-run");
        let mut run = ClusterBackupRun::create(
            &root,
            plan.clone(),
            false,
            ClusterBackupRunLimits::default(),
        )
        .unwrap();
        assert!(ClusterBackupRun::open(&root, false).is_err());
        run.fence_next_batch(&topology, 1, |range_id| {
            Ok(fence(&plan, &topology, range_id, &nodes))
        })
        .unwrap();
        drop(run);

        let journal_path = root.join(DEFAULT_CLUSTER_BACKUP_RUN_JOURNAL);
        let mut bytes = fs::read(&journal_path).unwrap();
        *bytes.last_mut().unwrap() ^= 0x01;
        fs::write(&journal_path, bytes).unwrap();
        assert!(ClusterBackupRun::open(&root, false).is_err());
    }

    #[test]
    fn refuses_to_publish_artifacts_from_a_cut_other_than_the_durable_fence() {
        let (directory, topology, metadata, nodes, plan) = fixture();
        let root = directory.path().join("wrong-cut-run");
        let mut run = ClusterBackupRun::create(
            &root,
            plan.clone(),
            false,
            ClusterBackupRunLimits::default(),
        )
        .unwrap();
        run.fence_next_batch(&topology, 2, |range_id| {
            Ok(fence(&plan, &topology, range_id, &nodes))
        })
        .unwrap();
        run.finish_fencing().unwrap();
        while !run.next_node_batch(&topology, 16).unwrap().is_empty() {
            run.capture_next_batch(&topology, 16, |node_id| {
                Ok(artifact_at(&plan, &topology, node_id, 8))
            })
            .unwrap();
        }
        run.finish_capture(&topology).unwrap();
        let error = run
            .publish_certificate(&topology, &metadata, CAPTURED_AT_MS)
            .unwrap_err();
        assert!(error.to_string().contains("durable write fence"));
        assert_eq!(run.status().phase, ClusterBackupRunPhase::Certifying);
        assert!(!root.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE).exists());
    }
}
