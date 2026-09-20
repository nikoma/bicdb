//! Crash-resumable activation of a quorum-admitted cluster restore.
//!
//! Admission proves that offline bytes are coherent. Metadata consensus then
//! commits one compact activation decision. This module connects those two
//! facts to idempotent, bounded release of the restored write fences and emits
//! the only readiness artifact that may open external listeners.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{ClusterNodeId, ClusterTopology, RangeId};
use crate::distribution_backup::{
    save_cluster_backup_certificate, ClusterBackupCertificate, ClusterNodeBackupArtifact,
    DEFAULT_CLUSTER_BACKUP_CERTIFICATE, MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES,
};
use crate::distribution_consensus::{
    MetadataConsensusStore, MetadataRestoreActivation, METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
};
use crate::distribution_range_consensus::{
    RangeBackupFenceQuorum, RangeWriteProgress, RangeWriteStore,
};
use crate::distribution_restore::{
    save_cluster_restore_admission, ClusterRestoreAdmissionLimits, ClusterRestoreAdmissionReport,
    DEFAULT_CLUSTER_RESTORE_ADMISSION, MAX_CLUSTER_RESTORE_ADMISSION_BYTES,
};
use crate::error::{BicDbError, Result};

pub const CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_CLUSTER_RESTORE_ACTIVATION_DIR: &str = "cluster-restore-activation";
pub const DEFAULT_CLUSTER_RESTORE_ACTIVATION_PLAN: &str = "cluster-restore-activation-plan.json";
pub const DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE: &str = "cluster-restore-activation-state.json";
pub const DEFAULT_CLUSTER_RESTORE_READINESS: &str = "cluster-restore-readiness.json";

const ACTIVATION_LOCK_FILE: &str = "cluster-restore-activation.lock";
const MAX_ACTIVATION_PLAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ACTIVATION_STATE_BYTES: u64 = 1024 * 1024;
const MAX_RESTORE_READINESS_BYTES: u64 = 1024 * 1024;

fn activation_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("cluster restore activation: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreActivationLimits {
    pub admission: ClusterRestoreAdmissionLimits,
    pub max_release_batch: usize,
}

impl Default for ClusterRestoreActivationLimits {
    fn default() -> Self {
        Self {
            admission: ClusterRestoreAdmissionLimits::default(),
            max_release_batch: 256,
        }
    }
}

impl ClusterRestoreActivationLimits {
    pub fn validate(&self) -> Result<()> {
        self.admission.validate()?;
        if self.max_release_batch == 0 || self.max_release_batch > 4_096 {
            return Err(activation_error(
                "restore fence-release batches must be within 1..=4096",
            ));
        }
        Ok(())
    }
}

/// Evidence produced on the restored node after the expensive admission pass.
/// Construction is private: callers receive one only through
/// `acknowledge_cluster_restore_node`, which re-runs the node startup gate.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreNodeAcknowledgement {
    format_version: u32,
    node_id: ClusterNodeId,
    certificate_id: Uuid,
    certificate_checksum_sha256: String,
    admission_id: Uuid,
    admission_checksum_sha256: String,
    plan_id: Uuid,
    topology_generation: u64,
    topology_sha256: String,
    metadata_term: u64,
    metadata_commit_index: u64,
    acknowledged_at_ms: u64,
    checksum_sha256: String,
}

impl ClusterRestoreNodeAcknowledgement {
    pub fn node_id(&self) -> &ClusterNodeId {
        &self.node_id
    }

    pub fn acknowledged_at_ms(&self) -> u64 {
        self.acknowledged_at_ms
    }

    pub fn checksum_sha256(&self) -> &str {
        &self.checksum_sha256
    }

    fn create(
        certificate: &ClusterBackupCertificate,
        report: &ClusterRestoreAdmissionReport,
        node_id: ClusterNodeId,
        metadata_term: u64,
        metadata_commit_index: u64,
        acknowledged_at_ms: u64,
    ) -> Result<Self> {
        let mut acknowledgement = Self {
            format_version: CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION,
            node_id,
            certificate_id: certificate.certificate_id,
            certificate_checksum_sha256: certificate.checksum_sha256.clone(),
            admission_id: report.admission_id,
            admission_checksum_sha256: report.checksum_sha256.clone(),
            plan_id: certificate.plan.plan_id,
            topology_generation: certificate.plan.topology_generation,
            topology_sha256: certificate.plan.topology_sha256.clone(),
            metadata_term,
            metadata_commit_index,
            acknowledged_at_ms,
            checksum_sha256: String::new(),
        };
        acknowledgement.checksum_sha256 = acknowledgement.calculate_checksum()?;
        acknowledgement.validate(certificate, report)?;
        Ok(acknowledgement)
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            node_id: &'a ClusterNodeId,
            certificate_id: Uuid,
            certificate_checksum_sha256: &'a str,
            admission_id: Uuid,
            admission_checksum_sha256: &'a str,
            plan_id: Uuid,
            topology_generation: u64,
            topology_sha256: &'a str,
            metadata_term: u64,
            metadata_commit_index: u64,
            acknowledged_at_ms: u64,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            node_id: &self.node_id,
            certificate_id: self.certificate_id,
            certificate_checksum_sha256: &self.certificate_checksum_sha256,
            admission_id: self.admission_id,
            admission_checksum_sha256: &self.admission_checksum_sha256,
            plan_id: self.plan_id,
            topology_generation: self.topology_generation,
            topology_sha256: &self.topology_sha256,
            metadata_term: self.metadata_term,
            metadata_commit_index: self.metadata_commit_index,
            acknowledged_at_ms: self.acknowledged_at_ms,
        })
    }

    fn validate(
        &self,
        certificate: &ClusterBackupCertificate,
        report: &ClusterRestoreAdmissionReport,
    ) -> Result<()> {
        let evidence = report
            .nodes
            .iter()
            .find(|evidence| evidence.node_id == self.node_id)
            .ok_or_else(|| {
                activation_error(format!(
                    "node {} is absent from restore admission",
                    self.node_id
                ))
            })?;
        if self.format_version != CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION
            || self.certificate_id != certificate.certificate_id
            || self.certificate_checksum_sha256 != certificate.checksum_sha256
            || self.admission_id != report.admission_id
            || self.admission_checksum_sha256 != report.checksum_sha256
            || self.plan_id != certificate.plan.plan_id
            || self.topology_generation != certificate.plan.topology_generation
            || self.topology_sha256 != certificate.plan.topology_sha256
            || self.metadata_term < evidence.metadata_term
            || self.metadata_commit_index < evidence.metadata_commit_index
            || self.acknowledged_at_ms < report.admitted_at_ms
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(activation_error(format!(
                "node {} acknowledgement is stale, damaged, or belongs to another restore",
                self.node_id
            )));
        }
        Ok(())
    }
}

pub fn acknowledge_cluster_restore_node(
    certificate: &ClusterBackupCertificate,
    report: &ClusterRestoreAdmissionReport,
    node_id: ClusterNodeId,
    restored_root: impl AsRef<Path>,
    acknowledged_at_ms: u64,
    limits: &ClusterRestoreActivationLimits,
) -> Result<ClusterRestoreNodeAcknowledgement> {
    limits.validate()?;
    report.validate_node_start(
        certificate,
        &node_id,
        restored_root.as_ref(),
        &limits.admission,
    )?;
    let metadata = MetadataConsensusStore::inspect(restored_root)?;
    ClusterRestoreNodeAcknowledgement::create(
        certificate,
        report,
        node_id,
        metadata.current_term,
        metadata.commit_index,
        acknowledged_at_ms,
    )
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreActivationPlan {
    pub format_version: u32,
    pub limits: ClusterRestoreActivationLimits,
    pub activation: MetadataRestoreActivation,
    pub acknowledgements: Vec<ClusterRestoreNodeAcknowledgement>,
    pub checksum_sha256: String,
}

impl ClusterRestoreActivationPlan {
    fn create(
        certificate: &ClusterBackupCertificate,
        report: &ClusterRestoreAdmissionReport,
        topology: &ClusterTopology,
        mut acknowledgements: Vec<ClusterRestoreNodeAcknowledgement>,
        activated_at_ms: u64,
        limits: &ClusterRestoreActivationLimits,
    ) -> Result<Self> {
        limits.validate()?;
        certificate.validate_for_restore(topology, &limits.admission.cluster_limits)?;
        report.validate(certificate, topology, &limits.admission)?;
        acknowledgements.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        validate_acknowledgements(certificate, report, &acknowledgements, activated_at_ms)?;
        let node_ack_set_sha256 = acknowledgement_set_sha256(&acknowledgements)?;
        let activation = MetadataRestoreActivation {
            format_version: METADATA_RESTORE_ACTIVATION_FORMAT_VERSION,
            activation_id: Uuid::now_v7(),
            cluster_id: report.cluster_id.clone(),
            certificate_id: certificate.certificate_id,
            certificate_checksum_sha256: certificate.checksum_sha256.clone(),
            admission_id: report.admission_id,
            admission_checksum_sha256: report.checksum_sha256.clone(),
            plan_id: report.plan_id,
            topology_generation: report.topology_generation,
            topology_sha256: report.topology_sha256.clone(),
            acknowledged_nodes: acknowledgements.len() as u64,
            node_ack_set_sha256,
            activated_at_ms,
        };
        activation.validate()?;
        let mut plan = Self {
            format_version: CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION,
            limits: *limits,
            activation,
            acknowledgements,
            checksum_sha256: String::new(),
        };
        plan.checksum_sha256 = plan.calculate_checksum()?;
        plan.validate(certificate, report, topology)?;
        Ok(plan)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            limits: ClusterRestoreActivationLimits,
            activation: &'a MetadataRestoreActivation,
            acknowledgements: &'a [ClusterRestoreNodeAcknowledgement],
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            limits: self.limits,
            activation: &self.activation,
            acknowledgements: &self.acknowledgements,
        })
    }

    pub fn validate(
        &self,
        certificate: &ClusterBackupCertificate,
        report: &ClusterRestoreAdmissionReport,
        topology: &ClusterTopology,
    ) -> Result<()> {
        self.limits.validate()?;
        certificate.validate_for_restore(topology, &self.limits.admission.cluster_limits)?;
        report.validate(certificate, topology, &self.limits.admission)?;
        self.activation.validate()?;
        validate_acknowledgements(
            certificate,
            report,
            &self.acknowledgements,
            self.activation.activated_at_ms,
        )?;
        if self.format_version != CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION
            || self.activation.cluster_id != report.cluster_id
            || self.activation.certificate_id != certificate.certificate_id
            || self.activation.certificate_checksum_sha256 != certificate.checksum_sha256
            || self.activation.admission_id != report.admission_id
            || self.activation.admission_checksum_sha256 != report.checksum_sha256
            || self.activation.plan_id != report.plan_id
            || self.activation.topology_generation != report.topology_generation
            || self.activation.topology_sha256 != report.topology_sha256
            || self.activation.acknowledged_nodes != self.acknowledgements.len() as u64
            || self.activation.node_ack_set_sha256
                != acknowledgement_set_sha256(&self.acknowledgements)?
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(activation_error(
                "activation plan identity, acknowledgement set, or checksum is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClusterRestoreActivationPhase {
    AwaitingMetadataCommit,
    ReleasingFences,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ClusterRestoreActivationState {
    format_version: u32,
    activation_id: Uuid,
    phase: ClusterRestoreActivationPhase,
    metadata_term: Option<u64>,
    metadata_commit_index: Option<u64>,
    released_ranges: usize,
    readiness_id: Option<Uuid>,
    readiness_checksum_sha256: Option<String>,
    checksum_sha256: String,
}

impl ClusterRestoreActivationState {
    fn new(activation_id: Uuid) -> Result<Self> {
        let mut state = Self {
            format_version: CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION,
            activation_id,
            phase: ClusterRestoreActivationPhase::AwaitingMetadataCommit,
            metadata_term: None,
            metadata_commit_index: None,
            released_ranges: 0,
            readiness_id: None,
            readiness_checksum_sha256: None,
            checksum_sha256: String::new(),
        };
        state.checksum_sha256 = state.calculate_checksum()?;
        Ok(state)
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            activation_id: Uuid,
            phase: ClusterRestoreActivationPhase,
            metadata_term: Option<u64>,
            metadata_commit_index: Option<u64>,
            released_ranges: usize,
            readiness_id: Option<Uuid>,
            readiness_checksum_sha256: &'a Option<String>,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            activation_id: self.activation_id,
            phase: self.phase,
            metadata_term: self.metadata_term,
            metadata_commit_index: self.metadata_commit_index,
            released_ranges: self.released_ranges,
            readiness_id: self.readiness_id,
            readiness_checksum_sha256: &self.readiness_checksum_sha256,
        })
    }

    fn validate(&self, plan: &ClusterRestoreActivationPlan, range_count: usize) -> Result<()> {
        let commit_present = self.metadata_term.is_some() && self.metadata_commit_index.is_some();
        let readiness_present = self.readiness_id.is_some()
            && self
                .readiness_checksum_sha256
                .as_ref()
                .is_some_and(|hash| hash.len() == 64);
        let shape_valid = match self.phase {
            ClusterRestoreActivationPhase::AwaitingMetadataCommit => {
                !commit_present && self.released_ranges == 0 && !readiness_present
            }
            ClusterRestoreActivationPhase::ReleasingFences => {
                commit_present && self.released_ranges <= range_count && !readiness_present
            }
            ClusterRestoreActivationPhase::Complete => {
                commit_present && self.released_ranges == range_count && readiness_present
            }
        };
        if self.format_version != CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION
            || self.activation_id != plan.activation.activation_id
            || !shape_valid
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(activation_error(
                "activation state is damaged, non-canonical, or belongs to another plan",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreActivationStatus {
    pub activation_id: Uuid,
    pub phase: ClusterRestoreActivationPhase,
    pub total_ranges: usize,
    pub released_ranges: usize,
    pub metadata_term: Option<u64>,
    pub metadata_commit_index: Option<u64>,
    pub readiness_id: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRestoreReadiness {
    pub format_version: u32,
    pub readiness_id: Uuid,
    pub ready_at_ms: u64,
    pub activation: MetadataRestoreActivation,
    pub activation_plan_checksum_sha256: String,
    pub metadata_term: u64,
    pub metadata_commit_index: u64,
    pub released_ranges: usize,
    pub checksum_sha256: String,
}

impl ClusterRestoreReadiness {
    fn create(
        plan: &ClusterRestoreActivationPlan,
        state: &ClusterRestoreActivationState,
        range_count: usize,
        ready_at_ms: u64,
    ) -> Result<Self> {
        if state.phase != ClusterRestoreActivationPhase::ReleasingFences
            || state.released_ranges != range_count
            || ready_at_ms < plan.activation.activated_at_ms
        {
            return Err(activation_error(
                "readiness requires every fence release after activation",
            ));
        }
        let metadata_term = state
            .metadata_term
            .ok_or_else(|| activation_error("readiness has no committed metadata term"))?;
        let metadata_commit_index = state
            .metadata_commit_index
            .ok_or_else(|| activation_error("readiness has no committed metadata commit index"))?;
        let mut readiness = Self {
            format_version: CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION,
            readiness_id: Uuid::now_v7(),
            ready_at_ms,
            activation: plan.activation.clone(),
            activation_plan_checksum_sha256: plan.checksum_sha256.clone(),
            metadata_term,
            metadata_commit_index,
            released_ranges: range_count,
            checksum_sha256: String::new(),
        };
        readiness.checksum_sha256 = readiness.calculate_checksum()?;
        Ok(readiness)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            readiness_id: Uuid,
            ready_at_ms: u64,
            activation: &'a MetadataRestoreActivation,
            activation_plan_checksum_sha256: &'a str,
            metadata_term: u64,
            metadata_commit_index: u64,
            released_ranges: usize,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            readiness_id: self.readiness_id,
            ready_at_ms: self.ready_at_ms,
            activation: &self.activation,
            activation_plan_checksum_sha256: &self.activation_plan_checksum_sha256,
            metadata_term: self.metadata_term,
            metadata_commit_index: self.metadata_commit_index,
            released_ranges: self.released_ranges,
        })
    }

    pub fn validate(
        &self,
        plan: &ClusterRestoreActivationPlan,
        certificate: &ClusterBackupCertificate,
    ) -> Result<()> {
        if self.format_version != CLUSTER_RESTORE_ACTIVATION_FORMAT_VERSION
            || self.readiness_id.is_nil()
            || self.ready_at_ms < plan.activation.activated_at_ms
            || self.activation != plan.activation
            || self.activation_plan_checksum_sha256 != plan.checksum_sha256
            || self.metadata_term <= certificate.plan.metadata_term
            || self.metadata_commit_index <= certificate.plan.metadata_commit_index
            || self.released_ranges != certificate.range_barriers.len()
            || self.calculate_checksum()? != self.checksum_sha256
        {
            return Err(activation_error(
                "restore readiness is stale, damaged, or belongs to another activation",
            ));
        }
        Ok(())
    }

    /// Final per-node listener gate. The node must still contain the admitted
    /// database, have observed this exact metadata decision, and have no
    /// remaining fence owned by the restored backup plan.
    pub fn validate_node_ready(
        &self,
        plan: &ClusterRestoreActivationPlan,
        certificate: &ClusterBackupCertificate,
        report: &ClusterRestoreAdmissionReport,
        node_id: &ClusterNodeId,
        restored_root: impl AsRef<Path>,
    ) -> Result<()> {
        plan.limits.validate()?;
        self.validate(plan, certificate)?;
        report.validate_node_start(
            certificate,
            node_id,
            restored_root.as_ref(),
            &plan.limits.admission,
        )?;
        let committed = MetadataConsensusStore::inspect_restore_activation(restored_root.as_ref())?
            .ok_or_else(|| activation_error("node has no committed restore activation"))?;
        let metadata = MetadataConsensusStore::inspect(restored_root.as_ref())?;
        if committed != self.activation
            || metadata.current_term < self.metadata_term
            || metadata.commit_index < self.metadata_commit_index
        {
            return Err(activation_error(
                "node has not observed the readiness metadata decision",
            ));
        }
        let catalog = RangeWriteStore::inspect(
            restored_root,
            report.cluster_id.clone(),
            node_id.clone(),
            plan.limits.admission.max_range_catalog_bytes_per_node,
        )?;
        if !catalog.backup_fences.is_empty() {
            return Err(activation_error(
                "node retains one or more write fences after restore activation",
            ));
        }
        Ok(())
    }
}

/// Exclusive coordinator for one activation. Progress is a constant-size
/// atomic state record. A crash may repeat at most the current release batch;
/// exact plan-owned fence release is idempotent.
pub struct ClusterRestoreActivationRun {
    root: PathBuf,
    fsync: bool,
    _lock_file: File,
    limits: ClusterRestoreActivationLimits,
    certificate: ClusterBackupCertificate,
    plan: ClusterRestoreActivationPlan,
    state: ClusterRestoreActivationState,
    range_ids: Vec<RangeId>,
}

impl ClusterRestoreActivationRun {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        root: impl AsRef<Path>,
        certificate: ClusterBackupCertificate,
        report: ClusterRestoreAdmissionReport,
        topology: &ClusterTopology,
        acknowledgements: Vec<ClusterRestoreNodeAcknowledgement>,
        activated_at_ms: u64,
        fsync: bool,
        limits: ClusterRestoreActivationLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let plan = ClusterRestoreActivationPlan::create(
            &certificate,
            &report,
            topology,
            acknowledgements,
            activated_at_ms,
            &limits,
        )?;
        let state = ClusterRestoreActivationState::new(plan.activation.activation_id)?;
        let root = root.as_ref().to_path_buf();
        if root.exists() {
            return Err(activation_error(format!(
                "activation directory {} already exists; open it to resume",
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
            .ok_or_else(|| activation_error("activation directory must have a UTF-8 name"))?;
        let staging = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));
        fs::create_dir(&staging)?;
        let mut guard = ActivationInitializationGuard {
            path: staging.clone(),
            published: false,
        };
        save_cluster_backup_certificate(
            staging.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE),
            &certificate,
            fsync,
        )?;
        save_cluster_restore_admission(
            staging.join(DEFAULT_CLUSTER_RESTORE_ADMISSION),
            &report,
            fsync,
        )?;
        write_json_atomic(
            &staging.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_PLAN),
            &plan,
            MAX_ACTIVATION_PLAN_BYTES,
            fsync,
        )?;
        write_json_atomic(
            &staging.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE),
            &state,
            MAX_ACTIVATION_STATE_BYTES,
            fsync,
        )?;
        fs::rename(&staging, &root)?;
        guard.published = true;
        if fsync {
            sync_parent_directory(&root)?;
        }
        Self::open(root, topology, fsync)
    }

    pub fn open(root: impl AsRef<Path>, topology: &ClusterTopology, fsync: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        reject_symlink(&root, "activation directory")?;
        let lock_path = root.join(ACTIVATION_LOCK_FILE);
        reject_symlink_if_exists(&lock_path, "activation lock")?;
        let lock_file = open_activation_lock(&lock_path)?;
        let certificate: ClusterBackupCertificate = read_json_bounded_no_link(
            &root.join(DEFAULT_CLUSTER_BACKUP_CERTIFICATE),
            MAX_CLUSTER_BACKUP_CONTROL_FILE_BYTES,
            "backup certificate",
        )?;
        let report: ClusterRestoreAdmissionReport = read_json_bounded_no_link(
            &root.join(DEFAULT_CLUSTER_RESTORE_ADMISSION),
            MAX_CLUSTER_RESTORE_ADMISSION_BYTES,
            "restore admission",
        )?;
        let plan: ClusterRestoreActivationPlan = read_json_bounded_no_link(
            &root.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_PLAN),
            MAX_ACTIVATION_PLAN_BYTES,
            "activation plan",
        )?;
        plan.validate(&certificate, &report, topology)?;
        let limits = plan.limits;
        let state: ClusterRestoreActivationState = read_json_bounded_no_link(
            &root.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE),
            MAX_ACTIVATION_STATE_BYTES,
            "activation state",
        )?;
        let range_ids = certificate
            .range_barriers
            .keys()
            .copied()
            .collect::<Vec<_>>();
        state.validate(&plan, range_ids.len())?;
        let run = Self {
            root,
            fsync,
            _lock_file: lock_file,
            limits,
            certificate,
            plan,
            state,
            range_ids,
        };
        run.validate_readiness_reference()?;
        Ok(run)
    }

    pub fn activation(&self) -> &MetadataRestoreActivation {
        &self.plan.activation
    }

    pub fn plan(&self) -> &ClusterRestoreActivationPlan {
        &self.plan
    }

    pub fn status(&self) -> ClusterRestoreActivationStatus {
        ClusterRestoreActivationStatus {
            activation_id: self.plan.activation.activation_id,
            phase: self.state.phase,
            total_ranges: self.range_ids.len(),
            released_ranges: self.state.released_ranges,
            metadata_term: self.state.metadata_term,
            metadata_commit_index: self.state.metadata_commit_index,
            readiness_id: self.state.readiness_id,
        }
    }

    /// Check that metadata consensus has committed this exact activation. The
    /// write fences remain installed until this transition is durable.
    pub fn record_metadata_commit(&mut self, metadata: &MetadataConsensusStore) -> Result<bool> {
        if self.state.phase != ClusterRestoreActivationPhase::AwaitingMetadataCommit {
            let committed = metadata.committed_restore_activation();
            if committed == Some(&self.plan.activation) {
                return Ok(false);
            }
            return Err(activation_error(
                "metadata decision changed after activation advanced",
            ));
        }
        let status = metadata.status();
        if metadata.committed_restore_activation() != Some(&self.plan.activation)
            || status.cluster_id != self.plan.activation.cluster_id
            || status.topology_generation != self.plan.activation.topology_generation
            || status.current_term <= self.certificate.plan.metadata_term
            || status.commit_index <= self.certificate.plan.metadata_commit_index
        {
            return Err(activation_error(
                "exact restore activation is not quorum committed by a new metadata term/index",
            ));
        }
        let previous = self.state.clone();
        self.state.phase = ClusterRestoreActivationPhase::ReleasingFences;
        self.state.metadata_term = Some(status.current_term);
        self.state.metadata_commit_index = Some(status.commit_index);
        if let Err(error) = self.persist_state() {
            self.state = previous;
            return Err(error);
        }
        Ok(true)
    }

    pub fn next_release_batch(&self, requested: usize) -> Result<Vec<RangeBackupFenceQuorum>> {
        if self.state.phase != ClusterRestoreActivationPhase::ReleasingFences {
            return Err(activation_error(
                "fences can be released only after metadata activation commits",
            ));
        }
        if requested == 0 || requested > self.limits.max_release_batch {
            return Err(activation_error(format!(
                "release batch must be within 1..={}",
                self.limits.max_release_batch
            )));
        }
        self.range_ids[self.state.released_ranges..]
            .iter()
            .take(requested)
            .map(|range_id| fence_from_certificate(&self.certificate, *range_id))
            .collect()
    }

    /// Release one bounded batch and checkpoint only after it succeeds. If the
    /// process dies mid-batch, the next invocation repeats that batch; exact
    /// plan-owned release is deliberately idempotent.
    pub fn release_next_batch<F>(&mut self, requested: usize, mut release: F) -> Result<usize>
    where
        F: FnMut(&RangeBackupFenceQuorum) -> Result<()>,
    {
        let fences = self.next_release_batch(requested)?;
        for fence in &fences {
            release(fence)?;
        }
        let previous = self.state.clone();
        self.state.released_ranges = self
            .state
            .released_ranges
            .checked_add(fences.len())
            .ok_or_else(|| activation_error("released range count overflow"))?;
        if let Err(error) = self.persist_state() {
            self.state = previous;
            return Err(error);
        }
        Ok(fences.len())
    }

    pub fn finish(&mut self, ready_at_ms: u64) -> Result<ClusterRestoreReadiness> {
        if self.state.phase == ClusterRestoreActivationPhase::Complete {
            return self.load_readiness();
        }
        self.state.validate(&self.plan, self.range_ids.len())?;
        let path = self.root.join(DEFAULT_CLUSTER_RESTORE_READINESS);
        let readiness = if path.exists() {
            let existing: ClusterRestoreReadiness =
                read_json_bounded_no_link(&path, MAX_RESTORE_READINESS_BYTES, "restore readiness")?;
            existing.validate(&self.plan, &self.certificate)?;
            existing
        } else {
            let readiness = ClusterRestoreReadiness::create(
                &self.plan,
                &self.state,
                self.range_ids.len(),
                ready_at_ms,
            )?;
            write_json_atomic(&path, &readiness, MAX_RESTORE_READINESS_BYTES, self.fsync)?;
            readiness
        };
        let previous = self.state.clone();
        self.state.phase = ClusterRestoreActivationPhase::Complete;
        self.state.readiness_id = Some(readiness.readiness_id);
        self.state.readiness_checksum_sha256 = Some(readiness.checksum_sha256.clone());
        if let Err(error) = self.persist_state() {
            self.state = previous;
            return Err(error);
        }
        Ok(readiness)
    }

    pub fn load_readiness(&self) -> Result<ClusterRestoreReadiness> {
        if self.state.phase != ClusterRestoreActivationPhase::Complete {
            return Err(activation_error("restore activation is not complete"));
        }
        let readiness: ClusterRestoreReadiness = read_json_bounded_no_link(
            &self.root.join(DEFAULT_CLUSTER_RESTORE_READINESS),
            MAX_RESTORE_READINESS_BYTES,
            "restore readiness",
        )?;
        readiness.validate(&self.plan, &self.certificate)?;
        if self.state.readiness_id != Some(readiness.readiness_id)
            || self.state.readiness_checksum_sha256.as_deref()
                != Some(readiness.checksum_sha256.as_str())
        {
            return Err(activation_error(
                "readiness file differs from the durable activation state",
            ));
        }
        Ok(readiness)
    }

    fn persist_state(&mut self) -> Result<()> {
        self.state.checksum_sha256 = self.state.calculate_checksum()?;
        self.state.validate(&self.plan, self.range_ids.len())?;
        write_json_atomic(
            &self.root.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE),
            &self.state,
            MAX_ACTIVATION_STATE_BYTES,
            self.fsync,
        )
    }

    fn validate_readiness_reference(&self) -> Result<()> {
        let path = self.root.join(DEFAULT_CLUSTER_RESTORE_READINESS);
        if self.state.phase == ClusterRestoreActivationPhase::Complete {
            self.load_readiness().map(|_| ())
        } else if path.exists() {
            let readiness: ClusterRestoreReadiness =
                read_json_bounded_no_link(&path, MAX_RESTORE_READINESS_BYTES, "restore readiness")?;
            readiness.validate(&self.plan, &self.certificate)
        } else {
            Ok(())
        }
    }
}

fn validate_acknowledgements(
    certificate: &ClusterBackupCertificate,
    report: &ClusterRestoreAdmissionReport,
    acknowledgements: &[ClusterRestoreNodeAcknowledgement],
    activated_at_ms: u64,
) -> Result<()> {
    if activated_at_ms < report.admitted_at_ms || acknowledgements.len() != report.nodes.len() {
        return Err(activation_error(
            "activation time or acknowledgement count is invalid",
        ));
    }
    let expected = report
        .nodes
        .iter()
        .map(|evidence| evidence.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut previous = None;
    for acknowledgement in acknowledgements {
        acknowledgement.validate(certificate, report)?;
        if !expected.contains(&acknowledgement.node_id)
            || !seen.insert(acknowledgement.node_id.clone())
            || previous
                .as_ref()
                .is_some_and(|previous| previous >= &acknowledgement.node_id)
            || acknowledgement.acknowledged_at_ms > activated_at_ms
        {
            return Err(activation_error(
                "node acknowledgements are missing, duplicate, unordered, or from the future",
            ));
        }
        previous = Some(acknowledgement.node_id.clone());
    }
    if seen != expected {
        return Err(activation_error(
            "activation does not acknowledge exactly every admitted node",
        ));
    }
    Ok(())
}

fn acknowledgement_set_sha256(
    acknowledgements: &[ClusterRestoreNodeAcknowledgement],
) -> Result<String> {
    let bindings = acknowledgements
        .iter()
        .map(|acknowledgement| {
            (
                &acknowledgement.node_id,
                acknowledgement.checksum_sha256.as_str(),
            )
        })
        .collect::<Vec<_>>();
    sha256_json(&bindings)
}

fn fence_from_certificate(
    certificate: &ClusterBackupCertificate,
    range_id: RangeId,
) -> Result<RangeBackupFenceQuorum> {
    let barrier = certificate.range_barriers.get(&range_id).ok_or_else(|| {
        activation_error(format!("certificate has no barrier for range {range_id}"))
    })?;
    let artifacts = certificate
        .artifacts
        .iter()
        .map(|artifact| (&artifact.node_id, artifact))
        .collect::<BTreeMap<_, _>>();
    let mut observations = Vec::with_capacity(barrier.artifact_nodes.len());
    for node_id in &barrier.artifact_nodes {
        let artifact = artifacts.get(node_id).ok_or_else(|| {
            activation_error(format!(
                "range {range_id} barrier references missing artifact node {node_id}"
            ))
        })?;
        let progress = artifact_progress(artifact, range_id).ok_or_else(|| {
            activation_error(format!(
                "artifact node {node_id} has no range {range_id} progress"
            ))
        })?;
        observations.push(progress.clone());
    }
    observations.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(RangeBackupFenceQuorum {
        plan_id: certificate.plan.plan_id,
        range_id,
        range_epoch: barrier.range_epoch,
        resolved_through: barrier.resolved_through,
        required_quorum: barrier.required_quorum,
        installed_at_ms: certificate.plan.created_at_ms,
        expires_at_ms: certificate.plan.expires_at_ms,
        observations,
    })
}

fn artifact_progress(
    artifact: &ClusterNodeBackupArtifact,
    range_id: RangeId,
) -> Option<&RangeWriteProgress> {
    artifact
        .ranges
        .binary_search_by_key(&range_id, |progress| progress.range_id)
        .ok()
        .map(|index| &artifact.ranges[index])
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

fn write_json_atomic(
    path: &Path,
    value: &impl Serialize,
    max_bytes: u64,
    fsync: bool,
) -> Result<()> {
    reject_symlink_if_exists(path, "activation control file")?;
    let bytes = serialize_json_bounded(value, max_bytes)?;
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn serialize_json_bounded(value: &impl Serialize, max_bytes: u64) -> Result<Vec<u8>> {
    struct BoundedWriter {
        bytes: Vec<u8>,
        max_bytes: u64,
    }
    impl Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = (self.bytes.len() as u64)
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("activation JSON size overflow"))?;
            if next > self.max_bytes {
                return Err(std::io::Error::other(
                    "activation JSON exceeds its byte bound",
                ));
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

fn read_json_bounded_no_link<T: for<'de> Deserialize<'de>>(
    path: &Path,
    max_bytes: u64,
    kind: &str,
) -> Result<T> {
    let file = open_regular_no_follow(path, kind)?;
    let length = file.metadata()?.len();
    if length > max_bytes {
        return Err(activation_error(format!(
            "{} exceeds its {max_bytes} byte bound",
            path.display()
        )));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| activation_error(format!("{kind} does not fit address space")))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(activation_error(format!(
            "{kind} grew beyond its read bound"
        )));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn open_regular_no_follow(path: &Path, kind: &str) -> Result<File> {
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
        return Err(activation_error(format!(
            "refusing non-regular {kind} {}",
            path.display()
        )));
    }
    Ok(file)
}

fn reject_symlink(path: &Path, kind: &str) -> Result<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(activation_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        )));
    }
    Ok(())
}

fn reject_symlink_if_exists(path: &Path, kind: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(activation_error(format!(
            "refusing symlink {kind} {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
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

struct ActivationInitializationGuard {
    path: PathBuf,
    published: bool,
}

impl Drop for ActivationInitializationGuard {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(unix)]
fn open_activation_lock(path: &Path) -> Result<File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(activation_error(format!(
            "another activation coordinator owns {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        )));
    }
    Ok(file)
}

#[cfg(windows)]
fn open_activation_lock(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
        .map_err(|error| {
            activation_error(format!(
                "another activation coordinator owns {}: {error}",
                path.display()
            ))
        })
}

#[cfg(not(any(unix, windows)))]
fn open_activation_lock(path: &Path) -> Result<File> {
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
        ClusterId, ClusterNode, DistributionConfig, DistributionStore, RangeReplica,
        RangeReplicaRole, ReplicaId, SCHEMA_COMPATIBILITY_NODE_LABEL,
    };
    use crate::distribution_backup::{
        ClusterBackupLimits, ClusterBackupPlan, ClusterNodeBackupArtifact,
    };
    use crate::distribution_consensus::{
        MetadataAppendResponse, MetadataConsensusRole, MetadataConsensusStatus,
    };
    use crate::distribution_restore::ClusterRestoreNodeEvidence;

    struct Fixture {
        _directory: tempfile::TempDir,
        topology: ClusterTopology,
        nodes: Vec<ClusterNodeId>,
        certificate: ClusterBackupCertificate,
        report: ClusterRestoreAdmissionReport,
        acknowledgements: Vec<ClusterRestoreNodeAcknowledgement>,
    }

    fn fixture() -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let nodes = ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|node| ClusterNodeId::new(node).unwrap())
            .collect::<Vec<_>>();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("activation-test").unwrap(),
            node_id: nodes[0].clone(),
            node_address: "127.0.0.1:9601".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor: 3,
            initial_ranges: 4,
            ..DistributionConfig::default()
        };
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        let mut topology = store.topology().clone();
        for (index, node_id) in nodes.iter().enumerate().skip(1) {
            topology.nodes.insert(
                node_id.clone(),
                ClusterNode::new(
                    node_id.clone(),
                    format!("127.0.0.1:{}", 9601 + index),
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
            current_term: 1,
            voted_for: Some(nodes[0].clone()),
            leader_id: Some(nodes[0].clone()),
            commit_index: 0,
            last_log_index: 0,
            last_log_term: 0,
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
                    topology
                        .ranges
                        .values()
                        .map(|range| RangeWriteProgress {
                            node_id: node_id.clone(),
                            range_id: range.id,
                            current_epoch: range.epoch,
                            last_index: 7,
                            resolved_through: 7,
                            compacted_through: 6,
                        })
                        .collect(),
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
        let evidence = certificate
            .artifacts
            .iter()
            .map(|artifact| ClusterRestoreNodeEvidence {
                node_id: artifact.node_id.clone(),
                backup_id: artifact.backup_id,
                archive_bytes: 1_000_000,
                archive_sha256: artifact.encrypted_artifact_sha256.clone(),
                manifest_sha256: artifact.backup_manifest_sha256.clone(),
                artifact_checksum_sha256: artifact.checksum_sha256.clone(),
                schema_sha256: artifact.schema_sha256.clone(),
                source_format_version: 3,
                restored_target_timestamp: certificate.plan.target_timestamp,
                metadata_term: certificate.plan.metadata_term,
                metadata_commit_index: certificate.plan.metadata_commit_index,
                range_fences_verified: artifact.ranges.len(),
                database_integrity_verified: true,
            })
            .collect::<Vec<_>>();
        let mut report = ClusterRestoreAdmissionReport {
            format_version: crate::CLUSTER_RESTORE_ADMISSION_FORMAT_VERSION,
            admission_id: Uuid::now_v7(),
            admitted_at_ms: 11_500,
            certificate_id: certificate.certificate_id,
            certificate_checksum_sha256: certificate.checksum_sha256.clone(),
            plan_id: certificate.plan.plan_id,
            cluster_id: certificate.plan.cluster_id.clone(),
            topology_generation: certificate.plan.topology_generation,
            topology_sha256: certificate.plan.topology_sha256.clone(),
            range_barriers_verified: certificate.range_barriers.len(),
            nodes: evidence,
            checksum_sha256: String::new(),
        };
        report.checksum_sha256 = report.calculate_checksum().unwrap();
        report
            .validate(
                &certificate,
                &topology,
                &ClusterRestoreAdmissionLimits::default(),
            )
            .unwrap();
        let acknowledgements = report
            .nodes
            .iter()
            .map(|evidence| {
                ClusterRestoreNodeAcknowledgement::create(
                    &certificate,
                    &report,
                    evidence.node_id.clone(),
                    evidence.metadata_term,
                    evidence.metadata_commit_index,
                    11_750,
                )
                .unwrap()
            })
            .collect();
        Fixture {
            _directory: directory,
            topology,
            nodes,
            certificate,
            report,
            acknowledgements,
        }
    }

    fn commit_activation(
        root: &Path,
        fixture: &Fixture,
        activation: MetadataRestoreActivation,
    ) -> MetadataConsensusStore {
        let mut metadata = MetadataConsensusStore::open(
            root,
            fixture.topology.cluster_id.clone(),
            fixture.nodes[0].clone(),
            fixture.topology.clone(),
            false,
        )
        .unwrap();
        metadata.start_election().unwrap();
        metadata.start_election().unwrap();
        metadata
            .become_leader(&BTreeSet::from([
                fixture.nodes[0].clone(),
                fixture.nodes[1].clone(),
            ]))
            .unwrap();
        let barrier_index = metadata.status().last_log_index;
        let response = MetadataAppendResponse {
            cluster_id: fixture.topology.cluster_id.clone(),
            term: metadata.status().current_term,
            node_id: fixture.nodes[1].clone(),
            success: true,
            match_index: barrier_index,
            conflict_index: barrier_index + 1,
        };
        metadata
            .record_append_response(&fixture.nodes[1], &response)
            .unwrap();
        let entry = metadata.propose_restore_activation(activation).unwrap();
        let response = MetadataAppendResponse {
            match_index: entry.index,
            conflict_index: entry.index + 1,
            ..response
        };
        assert!(metadata
            .record_append_response(&fixture.nodes[1], &response)
            .unwrap());
        metadata
    }

    #[test]
    fn activation_releases_in_bounded_resumable_batches_and_publishes_readiness() {
        let fixture = fixture();
        let directory = tempfile::tempdir().unwrap();
        let run_root = directory.path().join("activation");
        let limits = ClusterRestoreActivationLimits {
            max_release_batch: 2,
            ..ClusterRestoreActivationLimits::default()
        };
        let mut run = ClusterRestoreActivationRun::create(
            &run_root,
            fixture.certificate.clone(),
            fixture.report.clone(),
            &fixture.topology,
            fixture.acknowledgements.clone(),
            12_000,
            false,
            limits,
        )
        .unwrap();
        assert_eq!(
            run.status().phase,
            ClusterRestoreActivationPhase::AwaitingMetadataCommit
        );
        assert!(run.next_release_batch(1).is_err());
        assert!(ClusterRestoreActivationRun::open(&run_root, &fixture.topology, false).is_err());

        let metadata_root = directory.path().join("metadata");
        let metadata = commit_activation(&metadata_root, &fixture, run.activation().clone());
        assert!(run.record_metadata_commit(&metadata).unwrap());
        assert!(!run.record_metadata_commit(&metadata).unwrap());
        assert!(run.finish(12_100).is_err());

        let mut attempted = Vec::new();
        let mut calls = 0;
        assert!(run
            .release_next_batch(2, |fence| {
                calls += 1;
                attempted.push(fence.range_id);
                if calls == 2 {
                    return Err(activation_error("injected release interruption"));
                }
                Ok(())
            })
            .is_err());
        assert_eq!(run.status().released_ranges, 0);
        let first_attempt = attempted.clone();
        run.release_next_batch(2, |fence| {
            attempted.push(fence.range_id);
            assert_eq!(fence.plan_id, fixture.certificate.plan.plan_id);
            assert_eq!(fence.observations.len(), fence.required_quorum);
            Ok(())
        })
        .unwrap();
        assert_eq!(&attempted[2..], first_attempt.as_slice());
        assert_eq!(run.status().released_ranges, 2);
        drop(run);

        let mut resumed =
            ClusterRestoreActivationRun::open(&run_root, &fixture.topology, false).unwrap();
        assert_eq!(resumed.status().released_ranges, 2);
        assert_eq!(resumed.release_next_batch(2, |_| Ok(())).unwrap(), 2);
        assert_eq!(resumed.release_next_batch(2, |_| Ok(())).unwrap(), 0);
        let readiness = resumed.finish(12_500).unwrap();
        readiness
            .validate(resumed.plan(), &fixture.certificate)
            .unwrap();
        assert_eq!(
            resumed.status().phase,
            ClusterRestoreActivationPhase::Complete
        );
        assert_eq!(resumed.finish(12_600).unwrap(), readiness);
        drop(resumed);

        let reopened =
            ClusterRestoreActivationRun::open(&run_root, &fixture.topology, false).unwrap();
        assert_eq!(reopened.load_readiness().unwrap(), readiness);
    }

    #[test]
    fn activation_rejects_missing_acknowledgements_and_tampered_state() {
        let fixture = fixture();
        let directory = tempfile::tempdir().unwrap();
        let limits = ClusterRestoreActivationLimits::default();
        assert!(ClusterRestoreActivationRun::create(
            directory.path().join("missing"),
            fixture.certificate.clone(),
            fixture.report.clone(),
            &fixture.topology,
            vec![fixture.acknowledgements[0].clone()],
            12_000,
            false,
            limits,
        )
        .is_err());

        let root = directory.path().join("tamper");
        let run = ClusterRestoreActivationRun::create(
            &root,
            fixture.certificate,
            fixture.report,
            &fixture.topology,
            fixture.acknowledgements,
            12_000,
            false,
            limits,
        )
        .unwrap();
        drop(run);
        let state_path = root.join(DEFAULT_CLUSTER_RESTORE_ACTIVATION_STATE);
        let mut bytes = fs::read(&state_path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&state_path, bytes).unwrap();
        assert!(ClusterRestoreActivationRun::open(&root, &fixture.topology, false).is_err());
    }
}
