//! Durable, epoch-fenced quorum admission for one data range.
//!
//! Metadata consensus decides which voters own a range. This module is the
//! separate data-plane boundary: the current range leader replicates one exact
//! logical mutation command to a majority of those voters before BicDB applies
//! and acknowledges the originating SQL transaction.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::CancellationToken;
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::{
    CommitAdmission, CommitAdmissionIntent, CommitAdmissionMutation, CommitAdmissionTicket,
};
use crate::distribution::{
    ClusterId, ClusterNodeId, ClusterTopology, RangeDescriptor, RangeId, RangeReplicaRole,
};
use crate::distribution_anti_entropy_auto::{
    RangeAntiEntropyFenceAuthority, RangeAntiEntropyFencePlan,
};
use crate::distribution_anti_entropy_repair_certificate::RangeDigestRepairCertificate;
use crate::distribution_backup::{ClusterBackupPlan, CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION};
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_range_handoff::{
    RangeHandoffPromise, HANDOFF_RANGE_LOG_FORMAT, MAX_HANDOFF_PROMISES,
};
use crate::distribution_range_mutation::{
    RangeMutationAuthority, RangeMutationGate, RangeMutationPermit,
};
use crate::distribution_supervisor::ClusterBackupMetadata;
use crate::error::{BicDbError, Result};

mod native_image_base;
#[cfg(test)]
pub(crate) use native_prefix::test_validate_ordinary_native_intent;
mod native_image_ready;
pub(crate) use native_image_base::NativeImageCatalogueAdoptionReceipt;
use native_image_base::{NativeImageCatalogueBase, NATIVE_IMAGE_CATALOG_FORMAT};
pub(crate) use native_image_ready::FinalizedNativeImageCatalogueObservation;
mod image_install_obligations;
pub(crate) use image_install_obligations::TargetImageCatalogHistoryObligations;
mod image_install_resume;
mod image_install_state;
mod image_proposal_state;
pub(crate) use image_install_resume::CurrentImageInstallingAdmission;
use image_proposal_state::IMAGE_PROPOSAL_CATALOG_FORMAT;
pub(crate) use image_proposal_state::{
    NativeCurrentImageProposalPointer, NativeImageProposalIntentBinding,
};
pub(crate) mod native_abort;
mod native_completion_observation;
mod native_read_catalog;
pub use native_abort::NativePrepareOnlyRejection;
use native_abort::{NativeAbortProtocol, NATIVE_ABORT_CATALOG_FORMAT};
mod native_ready_catalog;
#[cfg(feature = "tls-replication")]
mod strong_point_read;
pub(crate) use native_completion_observation::LocalNativeCompletionEvidence;
pub(crate) use native_read_catalog::{NativeRangeReadCatalogue, NativeReadCatalogueDecision};
mod native_ready_observation;
pub(crate) use native_ready_observation::LocalNativeReadyEvidence;
mod native_activation;
#[cfg(test)]
mod native_activation_domain_tests;
pub(crate) use native_activation::NativeActivationReceipt;
use native_activation::NATIVE_ACTIVATED_FORMAT;
mod native_ready_completion;
mod native_ready_prepare;
#[cfg(test)]
pub(crate) use native_ready_catalog::native_ready_test_command;
pub(crate) use native_ready_completion::NativeRecoveryCompletedFrozenReceipt;
use native_ready_completion::NATIVE_COMPLETED_CATALOG_FORMAT;
pub(crate) use native_ready_prepare::PreparedNativeRecoveryReady;
use native_ready_prepare::NATIVE_READY_CATALOG_FORMAT;
mod recovery_state;
mod sparse_certification;
pub(crate) use image_install_state::ImageInstallPurpose;
use image_install_state::{ImageInstallIntent, IMAGE_INSTALL_RANGE_LOG_FORMAT};
pub(crate) use sparse_certification::{
    SparseDecisionAudit, SparseDecisionVoteAudit, SparseHistoricalDecisionProjection,
};
use sparse_certification::{SparseDecisions, SPARSE_HISTORY_CATALOG_FORMAT};
mod image_install_fence;
pub(crate) use image_install_fence::{
    NativeImageInstallRole, NativeImageInstallTransferPreflight, TargetImageInstallFence,
    UntouchedNativeImageTransferReturn,
};
use recovery_state::RecoveryFenceRecord;
pub(crate) use recovery_state::{
    HistoricalRangeCertification, RecoveryFenceScope, RecoveryImageInstallSessionObservation,
    RecoveryLocalHistoricalApplyObservation, RecoveryResolvedCatalogObservation,
};

mod native_limits;
pub(crate) use native_limits::{
    validate_native_json_size, validate_native_recovery_command_size,
    validate_native_recovery_entry_size, MAX_NATIVE_RECOVERY_ENTRY_BYTES,
    MIN_NATIVE_RECOVERY_FRAME_BYTES,
};
mod native_durability;
mod native_normal;
mod native_prefix;
mod native_writer;
#[cfg(test)]
pub(crate) use native_prefix::tests::{
    fixture as native_origin_fixture, seed_actual_origin, test_catalog_command,
};
pub(crate) use native_prefix::{
    NativeRangeCertificateEvidence, NativeRangeCommandIntent, NativeRangeCommitCertificate,
};
use native_writer::NATIVE_WRITER_CATALOG_FORMAT;

pub const RANGE_WRITE_LOG_FORMAT_VERSION: u32 = 3;
pub const RANGE_WRITE_PROTOCOL_VERSION: u32 = 2;
pub const DEFAULT_RANGE_WRITE_LOG: &str = "cluster-range-write-log.json";
const KNOWN_RANGE_WRITE_CATALOG_FORMATS: &[u32] = &[
    1,
    2,
    RANGE_WRITE_LOG_FORMAT_VERSION,
    HANDOFF_RANGE_LOG_FORMAT,
    IMAGE_INSTALL_RANGE_LOG_FORMAT,
    NATIVE_IMAGE_CATALOG_FORMAT,
    NATIVE_READY_CATALOG_FORMAT,
    NATIVE_COMPLETED_CATALOG_FORMAT,
    NATIVE_ACTIVATED_FORMAT,
    SPARSE_HISTORY_CATALOG_FORMAT,
    IMAGE_PROPOSAL_CATALOG_FORMAT,
    NATIVE_WRITER_CATALOG_FORMAT,
    NATIVE_ABORT_CATALOG_FORMAT,
];

// Recognizing a format is not a phase, writer or recovery grant. Each caller
// must independently enforce its minimum, actual feature fences and proof.
fn range_write_catalog_format_supported(version: u32) -> bool {
    KNOWN_RANGE_WRITE_CATALOG_FORMATS.contains(&version)
}
const RETAIN_APPLIED_COMMANDS_PER_RANGE: u64 = 1_024;
const RANGE_ADMISSION_WAIT: Duration = Duration::from_secs(30);
const MAX_RANGE_BACKUP_FENCES: usize = 1_000_000;
const MAX_RANGE_BACKUP_FENCE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeWriteRepairLimits {
    pub max_commands_per_batch: usize,
    pub max_bytes_per_batch: usize,
    pub max_batches_per_admission: usize,
}

impl Default for RangeWriteRepairLimits {
    fn default() -> Self {
        Self {
            max_commands_per_batch: 128,
            max_bytes_per_batch: 4 * 1024 * 1024,
            max_batches_per_admission: 8,
        }
    }
}

impl RangeWriteRepairLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_commands_per_batch == 0
            || self.max_commands_per_batch > RETAIN_APPLIED_COMMANDS_PER_RANGE as usize
            || self.max_bytes_per_batch == 0
            || self.max_bytes_per_batch > 64 * 1024 * 1024
            || self.max_batches_per_admission == 0
            || self.max_batches_per_admission > 64
        {
            return Err(range_write_error(
                "range-write repair limits must use 1..=1024 commands, 1..=64 MiB, and 1..=64 batches",
            ));
        }
        Ok(())
    }
}

fn range_write_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeWriteState {
    Prepared,
    Committed,
    QuorumCommitted,
    Applied,
    Aborted,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeWriteCommand {
    pub protocol_version: u32,
    pub cluster_id: ClusterId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub index: u64,
    pub command_id: String,
    pub leader_node_id: ClusterNodeId,
    pub transaction_id: u64,
    pub mutations: Vec<CommitAdmissionMutation>,
    pub checksum_sha256: String,
}

impl RangeWriteCommand {
    fn new(
        cluster_id: ClusterId,
        range: &RangeDescriptor,
        index: u64,
        leader_node_id: ClusterNodeId,
        intent: &CommitAdmissionIntent,
    ) -> Result<Self> {
        let mut command = Self {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id,
            range_id: range.id,
            range_epoch: range.epoch,
            index,
            command_id: Uuid::now_v7().to_string(),
            leader_node_id,
            transaction_id: intent.transaction_id,
            mutations: intent.mutations.clone(),
            checksum_sha256: String::new(),
        };
        command.checksum_sha256 = command.calculate_checksum()?;
        command.validate()?;
        Ok(command)
    }

    fn new_native_bounded(
        cluster_id: ClusterId,
        range: &RangeDescriptor,
        index: u64,
        leader_node_id: ClusterNodeId,
        intent: &CommitAdmissionIntent,
    ) -> Result<Self> {
        // Serialize complete shape by reference BEFORE cloning large records
        // or allocating a checksum buffer. Native refusals precede Prepare.
        #[derive(Serialize)]
        struct Candidate<'a> {
            protocol_version: u32,
            cluster_id: &'a ClusterId,
            range_id: RangeId,
            range_epoch: u64,
            index: u64,
            command_id: &'a str,
            leader_node_id: &'a ClusterNodeId,
            transaction_id: u64,
            mutations: &'a [CommitAdmissionMutation],
            checksum_sha256: &'a str,
        }
        let command_id = Uuid::now_v7().to_string();
        let checksum_placeholder = "0".repeat(64);
        native_limits::validate_native_recovery_command_shape(&Candidate {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id: &cluster_id,
            range_id: range.id,
            range_epoch: range.epoch,
            index,
            command_id: &command_id,
            leader_node_id: &leader_node_id,
            transaction_id: intent.transaction_id,
            mutations: &intent.mutations,
            checksum_sha256: &checksum_placeholder,
        })?;
        let mut command = Self {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id,
            range_id: range.id,
            range_epoch: range.epoch,
            index,
            command_id,
            leader_node_id,
            transaction_id: intent.transaction_id,
            mutations: intent.mutations.clone(),
            checksum_sha256: String::new(),
        };
        command.checksum_sha256 = command.calculate_checksum()?;
        command.validate()?;
        validate_native_recovery_command_size(&command)?;
        Ok(command)
    }

    pub fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct ChecksumPayload<'a> {
            protocol_version: u32,
            cluster_id: &'a ClusterId,
            range_id: RangeId,
            range_epoch: u64,
            index: u64,
            command_id: &'a str,
            leader_node_id: &'a ClusterNodeId,
            transaction_id: u64,
            mutations: &'a [CommitAdmissionMutation],
        }
        let payload = ChecksumPayload {
            protocol_version: self.protocol_version,
            cluster_id: &self.cluster_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            index: self.index,
            command_id: &self.command_id,
            leader_node_id: &self.leader_node_id,
            transaction_id: self.transaction_id,
            mutations: &self.mutations,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&payload)?)))
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version == 0 || self.protocol_version > RANGE_WRITE_PROTOCOL_VERSION {
            return Err(range_write_error(format!(
                "unsupported range-write protocol {}; supported range is 1..={}",
                self.protocol_version, RANGE_WRITE_PROTOCOL_VERSION
            )));
        }
        if self.range_epoch == 0 || self.index == 0 || self.command_id.trim().is_empty() {
            return Err(range_write_error(
                "range-write epoch, index, and command id must be set",
            ));
        }
        if self.mutations.is_empty() {
            return Err(range_write_error("range-write command has no mutations"));
        }
        for mutation in &self.mutations {
            if mutation.collection.trim().is_empty() || mutation.record_id.is_empty() {
                return Err(range_write_error(
                    "range-write collection and record id must not be empty",
                ));
            }
            if let Some(record) = &mutation.record {
                if record.id != mutation.record_id {
                    return Err(range_write_error(format!(
                        "range-write record id mismatch for {}/{}",
                        mutation.collection, mutation.record_id
                    )));
                }
            }
        }
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(range_write_error("range-write command checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeWriteLogEntry {
    pub command: RangeWriteCommand,
    pub state: RangeWriteState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeWriteProgress {
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub current_epoch: u64,
    pub last_index: u64,
    pub resolved_through: u64,
    pub compacted_through: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeWriteCatalogInspection {
    pub format_version: u32,
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub ranges: Vec<RangeWriteProgress>,
    pub backup_fences: Vec<RangeBackupWriteFence>,
}

/// Durable write barrier owned by one expiring cluster-backup plan. The range
/// log may resolve or replay work at or below `resolved_through`, but no new
/// prepare may advance the log while this fence is active.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeBackupWriteFence {
    pub plan_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub installed_at_ms: u64,
    pub expires_at_ms: u64,
}

impl RangeBackupWriteFence {
    fn validate(&self) -> Result<()> {
        if self.plan_id.is_nil()
            || self.range_epoch == 0
            || self.installed_at_ms >= self.expires_at_ms
            || self.expires_at_ms.saturating_sub(self.installed_at_ms)
                > MAX_RANGE_BACKUP_FENCE_TTL_MS
        {
            return Err(range_write_error("invalid range backup write fence"));
        }
        Ok(())
    }
}

/// Authenticated observation of one range-log index. Leadership recovery uses
/// probes from a current voter quorum to resolve an old-epoch tail without
/// trusting client input or one replica's incomplete history.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeWriteProbe {
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub route_epoch: u64,
    pub index: u64,
    pub progress: RangeWriteProgress,
    pub entry: Option<RangeWriteLogEntry>,
}

impl RangeWriteProbe {
    pub fn validate(&self) -> Result<()> {
        if self.route_epoch == 0
            || self.index == 0
            || self.progress.node_id != self.node_id
            || self.progress.range_id != self.range_id
            || self.progress.current_epoch > self.route_epoch
            || self.progress.resolved_through > self.progress.last_index
            || self.progress.compacted_through > self.progress.resolved_through
        {
            return Err(range_write_error("invalid range-write probe identity"));
        }
        match &self.entry {
            Some(entry) => {
                entry.command.validate()?;
                if entry.command.range_id != self.range_id
                    || entry.command.index != self.index
                    || entry.command.range_epoch > self.route_epoch
                    || self.index <= self.progress.compacted_through
                    || self.index > self.progress.last_index
                {
                    return Err(range_write_error(
                        "range-write probe entry disagrees with its durable progress",
                    ));
                }
            }
            None => {
                if self.index > self.progress.compacted_through
                    && self.index <= self.progress.last_index
                {
                    return Err(range_write_error(
                        "range-write probe is missing a retained log entry",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RangeWriteRepairBatch {
    pub source_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub previous_resolved_index: u64,
    pub source_resolved_through: u64,
    pub entries: Vec<RangeWriteLogEntry>,
    pub serialized_entry_bytes: usize,
    pub checksum_sha256: String,
}

impl RangeWriteRepairBatch {
    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct ChecksumPayload<'a> {
            source_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            previous_resolved_index: u64,
            source_resolved_through: u64,
            entries: &'a [RangeWriteLogEntry],
            serialized_entry_bytes: usize,
        }
        let payload = ChecksumPayload {
            source_node_id: &self.source_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            previous_resolved_index: self.previous_resolved_index,
            source_resolved_through: self.source_resolved_through,
            entries: &self.entries,
            serialized_entry_bytes: self.serialized_entry_bytes,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&payload)?)))
    }

    pub fn validate(&self, limits: &RangeWriteRepairLimits) -> Result<()> {
        limits.validate()?;
        if self.range_epoch == 0
            || self.entries.len() > limits.max_commands_per_batch
            || self.serialized_entry_bytes > limits.max_bytes_per_batch
            || self.source_resolved_through < self.previous_resolved_index
        {
            return Err(range_write_error("invalid range-write repair batch bounds"));
        }
        let actual_bytes = self.entries.iter().try_fold(0usize, |total, entry| {
            Ok::<_, BicDbError>(total.saturating_add(serde_json::to_vec(entry)?.len()))
        })?;
        if actual_bytes != self.serialized_entry_bytes {
            return Err(range_write_error(
                "range-write repair batch byte accounting mismatch",
            ));
        }
        let mut expected = self.previous_resolved_index.saturating_add(1);
        let mut previous_epoch = 0;
        for entry in &self.entries {
            entry.command.validate()?;
            if entry.command.range_id != self.range_id
                || entry.command.index != expected
                || entry.command.range_epoch > self.range_epoch
                || entry.command.range_epoch < previous_epoch
                || !matches!(
                    entry.state,
                    RangeWriteState::Applied | RangeWriteState::Aborted
                )
            {
                return Err(range_write_error(
                    "range-write repair batch is not a contiguous resolved suffix",
                ));
            }
            previous_epoch = entry.command.range_epoch;
            expected = expected.saturating_add(1);
        }
        if self
            .entries
            .last()
            .is_some_and(|entry| entry.command.index > self.source_resolved_through)
        {
            return Err(range_write_error(
                "range-write repair batch exceeds source resolved watermark",
            ));
        }
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(range_write_error(
                "range-write repair batch checksum mismatch",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RangeWriteLog {
    current_epoch: u64,
    last_index: u64,
    resolved_through: u64,
    compacted_through: u64,
    entries: BTreeMap<u64, RangeWriteLogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct RangeWriteCatalog {
    format_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_writer_enforcement_version: Option<u32>,
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    ranges: BTreeMap<RangeId, RangeWriteLog>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    backup_fences: BTreeMap<RangeId, RangeBackupWriteFence>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    handoff_promises: BTreeMap<RangeId, RangeHandoffPromise>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    image_install_intents: BTreeMap<RangeId, ImageInstallIntent>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    recovery_fences: BTreeMap<RangeId, RecoveryFenceRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    native_image_bases: BTreeMap<RangeId, NativeImageCatalogueBase>,
    // None is legacy ONLY. Format 10 requires Some, including an empty map:
    // a missing/null field can never silently erase sparse committed history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sparse_committed_decisions: Option<SparseDecisions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_abort_protocol: Option<NativeAbortProtocol>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RangeWriteEnvelope {
    catalog: RangeWriteCatalog,
    checksum_sha256: String,
}

impl RangeWriteEnvelope {
    fn new(catalog: RangeWriteCatalog) -> Result<Self> {
        let checksum_sha256 = catalog_checksum(&catalog)?;
        Ok(Self {
            catalog,
            checksum_sha256,
        })
    }

    fn verify(self) -> Result<RangeWriteCatalog> {
        if catalog_checksum(&self.catalog)? != self.checksum_sha256 {
            return Err(range_write_error(
                "range-write catalog checksum mismatch; refusing recovery",
            ));
        }
        if !range_write_catalog_format_supported(self.catalog.format_version) {
            return Err(range_write_error(format!(
                "unsupported range-write log format {}; maximum supported {}",
                self.catalog.format_version,
                KNOWN_RANGE_WRITE_CATALOG_FORMATS
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0)
            )));
        }
        if self
            .catalog
            .native_writer_enforcement_version
            .is_some_and(|version| version != 1)
            || (self.catalog.format_version == NATIVE_WRITER_CATALOG_FORMAT
                && self.catalog.native_writer_enforcement_version != Some(1))
            || (self.catalog.native_writer_enforcement_version.is_some()
                && self.catalog.format_version < NATIVE_WRITER_CATALOG_FORMAT)
        {
            return Err(range_write_error(
                "native writer catalog marker/format mismatch",
            ));
        }
        if (self.catalog.format_version == NATIVE_ABORT_CATALOG_FORMAT
            && self.catalog.native_abort_protocol.is_none())
            || (self.catalog.format_version < NATIVE_ABORT_CATALOG_FORMAT
                && self.catalog.native_abort_protocol.is_some())
        {
            return Err(range_write_error(
                "native abort protocol marker/format mismatch",
            ));
        }
        if let Some(protocol) = &self.catalog.native_abort_protocol {
            protocol.validate()?;
        }
        sparse_certification::validate_sparse_catalog(&self.catalog)?;
        if self.catalog.recovery_fences.len() > recovery_state::MAX_RECOVERY_FENCES {
            return Err(range_write_error("recovery refusal catalog exceeds bound"));
        }
        for (range_id, fence) in &self.catalog.recovery_fences {
            fence.validate()?;
            if fence.scope.range_id != *range_id
                || self.catalog.format_version < 5
                || fence.local_node_id != self.catalog.node_id
                || fence.scope.cluster_id != self.catalog.cluster_id
            {
                return Err(range_write_error("damaged recovery refusal scope"));
            }
            let log = self
                .catalog
                .ranges
                .get(range_id)
                .ok_or_else(|| range_write_error("recovery refusal has no actual range log"))?;
            for marker in fence.historical_certifications.values() {
                let sparse = self
                    .catalog
                    .sparse_committed_decisions
                    .as_ref()
                    .and_then(|ranges| ranges.get(range_id))
                    .and_then(|entries| entries.get(&marker.index));
                let sparse_matches = sparse
                    .is_some_and(|entry| entry.command_checksum_sha256() == marker.command_sha256);
                if (marker.index > log.last_index && !sparse_matches)
                    || log.entries.get(&marker.index).is_some_and(|entry| {
                        entry.command.checksum_sha256 != marker.command_sha256
                            || !matches!(
                                entry.state,
                                RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                            )
                    })
                    || sparse.is_some_and(|entry| {
                        entry.command_checksum_sha256() != marker.command_sha256
                    })
                {
                    return Err(range_write_error(
                        "historical certification conflicts with retained command state",
                    ));
                }
            }
        }
        for log in self.catalog.ranges.values() {
            validate_log(log)?;
        }
        if self.catalog.backup_fences.len() > MAX_RANGE_BACKUP_FENCES {
            return Err(range_write_error(
                "range backup fence catalog exceeds its hard bound",
            ));
        }
        for (range_id, fence) in &self.catalog.backup_fences {
            fence.validate()?;
            let log = self.catalog.ranges.get(range_id).ok_or_else(|| {
                range_write_error(format!("backup fence references unknown range {range_id}"))
            })?;
            if fence.range_id != *range_id
                || fence.range_epoch != log.current_epoch
                || fence.resolved_through != log.resolved_through
                || log.last_index != log.resolved_through
            {
                return Err(range_write_error(format!(
                    "backup fence for {range_id} disagrees with its resolved range log"
                )));
            }
        }
        if self.catalog.handoff_promises.len() > MAX_HANDOFF_PROMISES {
            return Err(range_write_error(
                "range handoff promise catalog exceeds its bound",
            ));
        }
        for (id, promise) in &self.catalog.handoff_promises {
            promise.validate()?;
            let log = self
                .catalog
                .ranges
                .get(id)
                .ok_or_else(|| range_write_error("handoff promise has no range log"))?;
            if self.catalog.format_version < HANDOFF_RANGE_LOG_FORMAT
                || promise.range_id != *id
                || log.current_epoch < promise.activation_epoch
                || log.resolved_through < promise.resolved_through
            {
                return Err(range_write_error(
                    "handoff promise disagrees with durable range log",
                ));
            }
        }
        image_install_state::validate_catalog_install_intents(&self.catalog)?;
        native_image_base::validate_catalog_native_image_bases(&self.catalog)?;
        native_ready_prepare::validate_native_prepared_catalog(&self.catalog)?;
        Ok(self.catalog)
    }
}

fn catalog_checksum(catalog: &RangeWriteCatalog) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(catalog)?)))
}

fn validate_log(log: &RangeWriteLog) -> Result<()> {
    if log.resolved_through > log.last_index || log.compacted_through > log.resolved_through {
        return Err(range_write_error("invalid range-write log watermarks"));
    }
    for (index, entry) in &log.entries {
        if *index != entry.command.index || *index <= log.compacted_through {
            return Err(range_write_error(
                "range-write log index disagrees with command or compaction watermark",
            ));
        }
        entry.command.validate()?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct RangeWriteStore {
    root: PathBuf,
    fsync: bool,
    catalog: RangeWriteCatalog,
    open_id: Uuid,
    recovery_persistence_uncertain: bool,
    #[cfg(test)]
    test_native_commit_write_error_once: bool,
    native_activated_opens: BTreeMap<RangeId, crate::db::NativeActivatedOpenGuard>,
    native_writer_host: Option<std::sync::Weak<crate::db::NativeWriterHostState>>,
    _native_abort_owner: Option<bicdb_page::DirectoryLock>,
}

impl RangeWriteStore {
    /// Read and verify a durable range-write catalog without migrating or
    /// rewriting it. Restore admission uses this before the restored database
    /// is allowed to start cluster services.
    pub fn inspect(
        root: impl AsRef<Path>,
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        max_catalog_bytes: u64,
    ) -> Result<RangeWriteCatalogInspection> {
        let path = root.as_ref().join(DEFAULT_RANGE_WRITE_LOG);
        if max_catalog_bytes == 0 {
            return Err(range_write_error(
                "range-write inspection byte limit must be positive",
            ));
        }
        let file = open_range_catalog_no_follow(&path)?;
        let length = file.metadata()?.len();
        if length > max_catalog_bytes {
            return Err(range_write_error(format!(
                "range-write catalog {} exceeds its {max_catalog_bytes} byte inspection bound",
                path.display()
            )));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| range_write_error("range-write catalog does not fit address space"))?;
        let mut bytes = Vec::with_capacity(capacity);
        file.take(max_catalog_bytes.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max_catalog_bytes {
            return Err(range_write_error(
                "range-write catalog grew beyond its inspection bound",
            ));
        }
        let envelope: RangeWriteEnvelope = serde_json::from_slice(&bytes)?;
        let catalog = envelope.verify()?;
        if catalog.cluster_id != cluster_id || catalog.node_id != node_id {
            return Err(range_write_error(
                "range-write inspection identity does not match the restored node",
            ));
        }
        let ranges = catalog
            .ranges
            .iter()
            .map(|(range_id, log)| RangeWriteProgress {
                node_id: catalog.node_id.clone(),
                range_id: *range_id,
                current_epoch: log.current_epoch,
                last_index: log.last_index,
                resolved_through: log.resolved_through,
                compacted_through: log.compacted_through,
            })
            .collect();
        Ok(RangeWriteCatalogInspection {
            format_version: catalog.format_version,
            cluster_id: catalog.cluster_id,
            node_id: catalog.node_id,
            ranges,
            backup_fences: catalog.backup_fences.into_values().collect(),
        })
    }

    pub fn open(
        root: impl AsRef<Path>,
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        fsync: bool,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let path = root.join(DEFAULT_RANGE_WRITE_LOG);
        let existing_catalog = path.exists();
        let abort_protocol = crate::format::native_abort_history_active(&root)?;
        if abort_protocol && !existing_catalog {
            return Err(range_write_error(
                "abort-aware range catalogue missing; strict factory/reopen required",
            ));
        }
        let abort_owner = if abort_protocol {
            Some(Self::abort_catalogue_owner(&root)?)
        } else {
            None
        };
        let mut catalog = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<RangeWriteEnvelope>(&bytes)?.verify()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RangeWriteCatalog {
                format_version: RANGE_WRITE_LOG_FORMAT_VERSION,
                native_writer_enforcement_version: None,
                cluster_id: cluster_id.clone(),
                node_id: node_id.clone(),
                ranges: BTreeMap::new(),
                backup_fences: BTreeMap::new(),
                handoff_promises: BTreeMap::new(),
                image_install_intents: BTreeMap::new(),
                recovery_fences: BTreeMap::new(),
                native_image_bases: BTreeMap::new(),
                sparse_committed_decisions: None,
                native_abort_protocol: None,
            },
            Err(error) => return Err(error.into()),
        };
        if catalog.cluster_id != cluster_id || catalog.node_id != node_id {
            return Err(range_write_error(
                "range-write catalog cluster or node identity mismatch",
            ));
        }
        if catalog.format_version >= HANDOFF_RANGE_LOG_FORMAT
            && (!fsync || !crate::format::range_image_install_active(&root)?)
        {
            return Err(range_write_error(
                "handoff range catalog requires its durable image reader fence and fsync",
            ));
        }
        if (catalog.format_version == IMAGE_PROPOSAL_CATALOG_FORMAT
            || catalog
                .image_install_intents
                .values()
                .any(|i| i.protected_proposal.is_some()))
            && !crate::format::current_image_proposal_active(&root)?
        {
            return Err(range_write_error(
                "protected proposal catalog lacks its mandatory native reader fence",
            ));
        }
        if catalog.native_abort_protocol.is_some()
            && !crate::format::native_abort_history_active(&root)?
        {
            return Err(range_write_error(
                "native abort catalogue lacks its mandatory reader fence",
            ));
        }
        let enforced = crate::format::native_writer_enrollment_active(&root)?;
        if enforced {
            if !crate::format::current_image_proposal_active(&root)?
                || !fsync
                || (existing_catalog
                    && (catalog.format_version < NATIVE_WRITER_CATALOG_FORMAT
                        || catalog.native_writer_enforcement_version != Some(1)))
            {
                return Err(range_write_error("enforced native store cannot adopt an older decision catalog or missing proposal reader fence"));
            }
            catalog.format_version = catalog.format_version.max(NATIVE_WRITER_CATALOG_FORMAT);
            catalog.native_writer_enforcement_version = Some(1);
            catalog
                .sparse_committed_decisions
                .get_or_insert_with(BTreeMap::new);
        } else if catalog.native_writer_enforcement_version.is_some()
            || catalog.format_version == NATIVE_WRITER_CATALOG_FORMAT
        {
            return Err(range_write_error(
                "enforced writer catalog requires its actual sticky reader fence",
            ));
        }
        let migrated_v1_decisions = catalog.format_version == 1;
        let migrated = catalog.format_version < RANGE_WRITE_LOG_FORMAT_VERSION;
        if migrated_v1_decisions {
            // Format v1 used `Committed` as its crash-replayable terminal
            // decision. Preserve that published meaning while moving new
            // writes to the v2 provisional/certified split.
            for log in catalog.ranges.values_mut() {
                for entry in log.entries.values_mut() {
                    if entry.state == RangeWriteState::Committed {
                        entry.state = RangeWriteState::QuorumCommitted;
                    }
                }
            }
        }
        catalog.format_version = catalog.format_version.max(RANGE_WRITE_LOG_FORMAT_VERSION);
        let store = Self {
            root,
            fsync,
            catalog,
            open_id: Uuid::now_v7(),
            native_activated_opens: BTreeMap::new(),
            native_writer_host: None,
            _native_abort_owner: abort_owner,
            recovery_persistence_uncertain: false,
            #[cfg(test)]
            test_native_commit_write_error_once: false,
        };
        // A previous service instance may have observed rename success but
        // failed directory sync while the same live DB remained open. Reading
        // that visible JSON is not durability evidence: re-fsync unchanged
        // validated refusals before this new store-open can expose proofs.
        if migrated
            || !store.catalog.recovery_fences.is_empty()
            || !store.catalog.native_image_bases.is_empty()
        {
            store.persist_verified_open_catalog()?;
        }
        Ok(store)
    }

    pub fn next_index(&self, range_id: RangeId) -> u64 {
        self.catalog
            .ranges
            .get(&range_id)
            .map(|log| log.last_index.saturating_add(1))
            .unwrap_or(1)
    }

    pub fn progress(&self, range_id: RangeId) -> RangeWriteProgress {
        let (current_epoch, last_index, resolved_through, compacted_through) = self
            .catalog
            .ranges
            .get(&range_id)
            .map(|log| {
                (
                    log.current_epoch,
                    log.last_index,
                    log.resolved_through,
                    log.compacted_through,
                )
            })
            .unwrap_or_default();
        RangeWriteProgress {
            node_id: self.catalog.node_id.clone(),
            range_id,
            current_epoch,
            last_index,
            resolved_through,
            compacted_through,
        }
    }

    /// Current metadata and the exclusive range gate are validated by the
    /// caller. This persistence boundary records its own local resolved cut.
    pub(crate) fn install_handoff_promise(
        &mut self,
        request: &RangeHandoffPromise,
    ) -> Result<RangeHandoffPromise> {
        self.install_handoff_promise_with(request, Self::persist)
    }

    // The private adapter keeps the production writer unchanged while allowing
    // tests to exercise a real rename followed by an uncertain directory sync.
    fn install_handoff_promise_with(
        &mut self,
        request: &RangeHandoffPromise,
        mut persist: impl FnMut(&Self) -> Result<()>,
    ) -> Result<RangeHandoffPromise> {
        request.validate()?;
        self.require_no_image_install(request.range_id)?;
        self.require_no_active_recovery(request.range_id)?;
        if !self.fsync || !crate::format::range_image_install_active(&self.root)? {
            return Err(range_write_error(
                "durable handoff requires fsync and the sticky image reader fence",
            ));
        }
        if let Some(existing) = self.catalog.handoff_promises.get(&request.range_id) {
            return if existing.same_request(request) {
                // An earlier failure may have happened after rename but before
                // directory fsync. Never turn an unconfirmed in-memory fence
                // into quorum evidence without re-persisting it durably.
                persist(self)?;
                Ok(existing.clone())
            } else {
                Err(range_write_error(
                    "range is already promised to another handoff",
                ))
            };
        }
        if self.catalog.handoff_promises.len() >= MAX_HANDOFF_PROMISES {
            return Err(range_write_error(
                "handoff promise catalog reached its bound",
            ));
        }
        let progress = self.progress(request.range_id);
        if progress.current_epoch > request.old_epoch
            || progress.last_index != progress.resolved_through
            || self.catalog.backup_fences.contains_key(&request.range_id)
        {
            return Err(range_write_error(
                "handoff requires the current epoch, no backup fence and a resolved local tail",
            ));
        }
        let mut promise = request.clone();
        promise.resolved_through = progress.resolved_through;
        let log = self
            .catalog
            .ranges
            .entry(request.range_id)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: request.old_epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            });
        log.current_epoch = promise.activation_epoch;
        self.catalog.format_version = self.catalog.format_version.max(HANDOFF_RANGE_LOG_FORMAT);
        self.catalog
            .handoff_promises
            .insert(request.range_id, promise.clone());
        // On any persistence error retain the conservative fence in memory.
        // write_atomic may have renamed before failing a durability operation;
        // reverting memory could otherwise admit an old writer against it.
        // No response/certificate is issued unless persistence succeeds.
        persist(self)?;
        Ok(promise)
    }

    pub fn active_backup_fence(&self, range_id: RangeId) -> Option<&RangeBackupWriteFence> {
        self.catalog.backup_fences.get(&range_id)
    }

    pub fn backup_fences(&self) -> Vec<RangeBackupWriteFence> {
        self.catalog.backup_fences.values().cloned().collect()
    }

    pub fn install_backup_fence(
        &mut self,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        installed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RangeWriteProgress> {
        self.require_no_image_install(range_id)?;
        self.require_no_active_recovery(range_id)?;
        let candidate = RangeBackupWriteFence {
            plan_id,
            range_id,
            range_epoch,
            resolved_through: 0,
            installed_at_ms,
            expires_at_ms,
        };
        candidate.validate()?;
        if self.catalog.handoff_promises.contains_key(&range_id) {
            return Err(range_write_error(
                "range handoff must finish before a backup fence can be installed",
            ));
        }
        if installed_at_ms >= expires_at_ms {
            return Err(range_write_error("range backup fence is already expired"));
        }
        let previous = self.catalog.clone();
        if let Some(existing) = self.catalog.backup_fences.get(&range_id).cloned() {
            if existing.plan_id == plan_id
                && existing.range_epoch == range_epoch
                && existing.expires_at_ms == expires_at_ms
            {
                return Ok(self.progress(range_id));
            }
            self.catalog = previous;
            return Err(range_write_error(format!(
                "range {range_id} is already fenced by backup plan {}",
                existing.plan_id
            )));
        }
        if self.catalog.backup_fences.len() >= MAX_RANGE_BACKUP_FENCES {
            self.catalog = previous;
            return Err(range_write_error(
                "range backup fence catalog reached its hard bound",
            ));
        }
        let current = self
            .catalog
            .ranges
            .get(&range_id)
            .map(|log| (log.current_epoch, log.last_index, log.resolved_through));
        if current.is_some_and(|(current_epoch, _, _)| range_epoch < current_epoch) {
            let current_epoch = current.expect("checked range log").0;
            self.catalog = previous;
            return Err(range_write_error(format!(
                "stale backup fence epoch {range_epoch} for {range_id}; current epoch is {}",
                current_epoch
            )));
        }
        if current.is_some_and(|(_, last_index, resolved_through)| last_index != resolved_through) {
            let (_, last_index, resolved_through) = current.expect("checked range log");
            self.catalog = previous;
            return Err(range_write_error(format!(
                "range {range_id} has an unresolved tail at {}/{}; resolve it before backup fencing",
                resolved_through, last_index
            )));
        }
        let log = self
            .catalog
            .ranges
            .entry(range_id)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: range_epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
                entries: BTreeMap::new(),
            });
        log.current_epoch = range_epoch;
        let resolved_through = log.resolved_through;
        self.catalog.backup_fences.insert(
            range_id,
            RangeBackupWriteFence {
                resolved_through,
                ..candidate
            },
        );
        self.persist_or_restore(previous, self.progress(range_id))
    }

    pub fn release_backup_fence(
        &mut self,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
    ) -> Result<bool> {
        if plan_id.is_nil() || range_epoch == 0 {
            return Err(range_write_error(
                "invalid range backup fence release identity",
            ));
        }
        let Some(existing) = self.catalog.backup_fences.get(&range_id) else {
            return Ok(false);
        };
        if existing.plan_id != plan_id || existing.range_epoch != range_epoch {
            return Err(range_write_error(format!(
                "backup plan {plan_id} cannot release range {range_id} fence owned by {} epoch {}",
                existing.plan_id, existing.range_epoch
            )));
        }
        let previous = self.catalog.clone();
        self.catalog.backup_fences.remove(&range_id);
        self.persist_or_restore(previous, true)
    }

    pub fn release_expired_backup_fences(&mut self, now_ms: u64) -> Result<usize> {
        let previous = self.catalog.clone();
        let removed = remove_expired_backup_fences(&mut self.catalog, now_ms);
        if removed == 0 {
            return Ok(0);
        }
        self.persist_or_restore(previous, removed)
    }

    pub fn probe(
        &self,
        range_id: RangeId,
        route_epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        let progress = self.progress(range_id);
        let probe = RangeWriteProbe {
            node_id: self.catalog.node_id.clone(),
            range_id,
            route_epoch,
            index,
            progress,
            entry: self
                .catalog
                .ranges
                .get(&range_id)
                .and_then(|log| log.entries.get(&index))
                .cloned(),
        };
        probe.validate()?;
        Ok(probe)
    }

    pub fn export_repair_batch(
        &self,
        source_node_id: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        previous_resolved_index: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        limits.validate()?;
        let progress = self.progress(range_id);
        if progress.current_epoch > range_epoch {
            return Err(range_write_error(format!(
                "range-write repair epoch mismatch for {range_id}: log={}, route={range_epoch}",
                progress.current_epoch
            )));
        }
        if previous_resolved_index < progress.compacted_through {
            return Err(range_write_error(format!(
                "range-write repair for {range_id} requires a range snapshot: follower resolved {previous_resolved_index}, leader compacted through {}",
                progress.compacted_through
            )));
        }
        if previous_resolved_index > progress.resolved_through {
            return Err(range_write_error(format!(
                "range-write follower for {range_id} is ahead of leader resolved watermark {}",
                progress.resolved_through
            )));
        }
        let mut entries = Vec::new();
        let mut serialized_entry_bytes = 0usize;
        if let Some(log) = self.catalog.ranges.get(&range_id) {
            for entry in log
                .entries
                .range(previous_resolved_index.saturating_add(1)..)
            {
                let entry = entry.1;
                if entry.command.index > log.resolved_through {
                    break;
                }
                if entry.command.range_epoch > range_epoch {
                    return Err(range_write_error(format!(
                        "range-write repair for {range_id} contains future epoch {} above route {range_epoch}",
                        entry.command.range_epoch
                    )));
                }
                let entry_bytes = serde_json::to_vec(entry)?.len();
                if entry_bytes > limits.max_bytes_per_batch {
                    return Err(range_write_error(format!(
                        "range-write repair entry {} is {entry_bytes} bytes, above the {} byte batch limit",
                        entry.command.index, limits.max_bytes_per_batch
                    )));
                }
                if entries.len() >= limits.max_commands_per_batch
                    || serialized_entry_bytes.saturating_add(entry_bytes)
                        > limits.max_bytes_per_batch
                {
                    break;
                }
                serialized_entry_bytes = serialized_entry_bytes.saturating_add(entry_bytes);
                entries.push(entry.clone());
            }
        }
        let mut batch = RangeWriteRepairBatch {
            source_node_id: source_node_id.clone(),
            range_id,
            range_epoch,
            previous_resolved_index,
            source_resolved_through: progress.resolved_through,
            entries,
            serialized_entry_bytes,
            checksum_sha256: String::new(),
        };
        batch.checksum_sha256 = batch.calculate_checksum()?;
        batch.validate(limits)?;
        Ok(batch)
    }

    pub fn verify_resolved_entry(&self, expected: &RangeWriteLogEntry) -> Result<()> {
        let log = self
            .catalog
            .ranges
            .get(&expected.command.range_id)
            .ok_or_else(|| range_write_error("range-write repair references an unknown range"))?;
        if expected.command.index <= log.compacted_through {
            return Ok(());
        }
        let actual = log.entries.get(&expected.command.index).ok_or_else(|| {
            range_write_error(format!(
                "resolved range-write entry {} is missing above compacted watermark {}",
                expected.command.index, log.compacted_through
            ))
        })?;
        if actual.command.command_id != expected.command.command_id
            || actual.command.checksum_sha256 != expected.command.checksum_sha256
            || actual.state != expected.state
        {
            return Err(range_write_error(format!(
                "range-write repair diverges at {} index {}",
                expected.command.range_id, expected.command.index
            )));
        }
        Ok(())
    }

    pub fn prepare(&mut self, command: RangeWriteCommand) -> Result<RangeWriteState> {
        self.require_native_writer_command(&command)?;
        self.require_no_native_abort_promise(command.range_id, command.index)?;
        command.validate()?;
        self.require_no_image_install(command.range_id)?;
        self.require_no_active_recovery(command.range_id)?;
        if command.cluster_id != self.catalog.cluster_id {
            return Err(range_write_error(
                "range-write command belongs to another cluster",
            ));
        }
        if self
            .catalog
            .handoff_promises
            .contains_key(&command.range_id)
        {
            return Err(range_write_error(format!(
                "range {} is unavailable while its durable handoff is incomplete",
                command.range_id
            )));
        }
        if let Some(fence) = self.catalog.backup_fences.get(&command.range_id) {
            if command.index > fence.resolved_through {
                return Err(range_write_error(format!(
                    "range {} is fenced at resolved index {} by backup plan {}; new writes are unavailable",
                    command.range_id, fence.resolved_through, fence.plan_id
                )));
            }
        }
        if let Some(log) = self.catalog.ranges.get(&command.range_id) {
            if command.range_epoch < log.current_epoch {
                return Err(range_write_error(format!(
                    "stale range-write epoch {} for {}; current epoch is {}",
                    command.range_epoch, command.range_id, log.current_epoch
                )));
            }
            if command.index <= log.compacted_through {
                return Err(range_write_error(format!(
                    "range-write index {} was already compacted through {}",
                    command.index, log.compacted_through
                )));
            }
            if let Some(existing) = log.entries.get(&command.index) {
                if existing.command.command_id != command.command_id
                    || existing.command.checksum_sha256 != command.checksum_sha256
                {
                    return Err(range_write_error(format!(
                        "conflicting range-write command at {} index {}",
                        command.range_id, command.index
                    )));
                }
                return Ok(existing.state);
            }
            if command.index != log.last_index.saturating_add(1) {
                return Err(range_write_error(format!(
                    "range-write gap for {}: expected {}, got {}",
                    command.range_id,
                    log.last_index.saturating_add(1),
                    command.index
                )));
            }
        }
        let previous = self.catalog.clone();
        let log = self
            .catalog
            .ranges
            .entry(command.range_id)
            .or_insert_with(|| RangeWriteLog {
                current_epoch: command.range_epoch,
                last_index: command.index.saturating_sub(1),
                resolved_through: command.index.saturating_sub(1),
                compacted_through: command.index.saturating_sub(1),
                entries: BTreeMap::new(),
            });
        if command.range_epoch > log.current_epoch {
            for entry in log.entries.values_mut() {
                if matches!(
                    entry.state,
                    RangeWriteState::Prepared | RangeWriteState::Committed
                ) {
                    entry.state = RangeWriteState::Aborted;
                }
            }
            advance_resolved(log);
            log.current_epoch = command.range_epoch;
        }
        log.last_index = command.index;
        log.entries.insert(
            command.index,
            RangeWriteLogEntry {
                command,
                state: RangeWriteState::Prepared,
            },
        );
        self.persist_or_restore(previous, RangeWriteState::Prepared)
    }

    pub fn commit(&mut self, command: &RangeWriteCommand) -> Result<RangeWriteState> {
        // This persisted reader fence is a conservative storage refusal rule,
        // not native admission authority. Actual service/DB checks still apply.
        let native_catalog = crate::format::range_image_install_active(&self.root)?;
        if native_catalog {
            // Even an idempotent raw Commit cannot ACK retained-but-uncertain
            // memory before the exact catalogue has actually been re-fsynced.
            self.require_durable_recovery_catalog()?;
        }
        self.preflight_native_commit_slot(command)?;
        let previous = self.catalog.clone();
        let entry = self.match_entry_mut(command)?;
        match entry.state {
            RangeWriteState::Prepared => entry.state = RangeWriteState::Committed,
            RangeWriteState::Committed
            | RangeWriteState::QuorumCommitted
            | RangeWriteState::Applied => return Ok(entry.state),
            RangeWriteState::Aborted => {
                return Err(range_write_error(format!(
                    "cannot commit aborted range-write command {}",
                    command.command_id
                )))
            }
        }
        if native_catalog {
            self.mark_native_ever_commit(command)?;
            // A failed rename/fsync may already have published Commit. Keep
            // the in-memory decision and fail closed until exact re-fsync or
            // strict reopen; never restore Prepared over that uncertainty.
            #[cfg(test)]
            if std::mem::take(&mut self.test_native_commit_write_error_once) {
                return self
                    .persist_recovery_catalog_with(|path, bytes, fsync| {
                        crate::storage::write_atomic(path, bytes, fsync)?;
                        Err(std::io::Error::other(
                            "injected native Commit error after actual durable publication",
                        )
                        .into())
                    })
                    .map(|()| RangeWriteState::Committed);
            }
            self.persist_recovery_catalog()?;
            Ok(RangeWriteState::Committed)
        } else {
            self.persist_or_restore(previous, RangeWriteState::Committed)
        }
    }

    #[cfg(test)]
    pub(crate) fn test_fail_native_commit_after_publication(&mut self) {
        self.test_native_commit_write_error_once = true;
    }

    pub fn certify(&mut self, command: &RangeWriteCommand) -> Result<RangeWriteState> {
        let previous = self.catalog.clone();
        let entry = self.match_entry_mut(command)?;
        match entry.state {
            RangeWriteState::Committed => entry.state = RangeWriteState::QuorumCommitted,
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied => return Ok(entry.state),
            RangeWriteState::Prepared => {
                return Err(range_write_error(format!(
                    "cannot certify uncommitted range-write command {}",
                    command.command_id
                )))
            }
            RangeWriteState::Aborted => {
                return Err(range_write_error(format!(
                    "cannot certify aborted range-write command {}",
                    command.command_id
                )))
            }
        }
        self.persist_or_restore(previous, RangeWriteState::QuorumCommitted)
    }

    pub fn state(&mut self, command: &RangeWriteCommand) -> Result<RangeWriteState> {
        Ok(self.match_entry_mut(command)?.state)
    }

    pub fn mark_applied(&mut self, command: &RangeWriteCommand) -> Result<()> {
        let previous = self.catalog.clone();
        let range_id = command.range_id;
        let entry = self.match_entry_mut(command)?;
        match entry.state {
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied => {
                entry.state = RangeWriteState::Applied
            }
            RangeWriteState::Prepared | RangeWriteState::Committed | RangeWriteState::Aborted => {
                return Err(range_write_error(format!(
                    "range-write command {} is not quorum-certified",
                    command.command_id
                )))
            }
        }
        let log = self.catalog.ranges.get_mut(&range_id).expect("matched log");
        advance_resolved(log);
        compact_resolved(log);
        self.persist_or_restore(previous, ())
    }

    pub fn abort(&mut self, command: &RangeWriteCommand) -> Result<RangeWriteState> {
        let previous = self.catalog.clone();
        let range_id = command.range_id;
        let entry = self.match_entry_mut(command)?;
        match entry.state {
            RangeWriteState::Prepared | RangeWriteState::Committed => {
                entry.state = RangeWriteState::Aborted
            }
            RangeWriteState::Aborted => return Ok(RangeWriteState::Aborted),
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied => {
                return Err(range_write_error(format!(
                    "cannot abort quorum-certified range-write command {}",
                    command.command_id
                )))
            }
        }
        let log = self.catalog.ranges.get_mut(&range_id).expect("matched log");
        advance_resolved(log);
        compact_resolved(log);
        self.persist_or_restore(previous, RangeWriteState::Aborted)
    }

    pub fn committed_not_applied(&self) -> Vec<RangeWriteCommand> {
        self.catalog
            .ranges
            .values()
            .flat_map(|log| log.entries.values())
            .filter(|entry| entry.state == RangeWriteState::QuorumCommitted)
            .map(|entry| entry.command.clone())
            .collect()
    }

    pub fn prepared_for_range(&self, range_id: RangeId) -> Vec<RangeWriteCommand> {
        self.catalog
            .ranges
            .get(&range_id)
            .into_iter()
            .flat_map(|log| log.entries.values())
            .filter(|entry| {
                matches!(
                    entry.state,
                    RangeWriteState::Prepared | RangeWriteState::Committed
                )
            })
            .map(|entry| entry.command.clone())
            .collect()
    }

    pub fn command_by_id(&self, command_id: &str) -> Option<RangeWriteCommand> {
        self.catalog
            .ranges
            .values()
            .flat_map(|log| log.entries.values())
            .find(|entry| entry.command.command_id == command_id)
            .map(|entry| entry.command.clone())
    }

    fn match_entry_mut(&mut self, command: &RangeWriteCommand) -> Result<&mut RangeWriteLogEntry> {
        self.require_native_writer_command(command)?;
        self.require_no_native_abort_promise(command.range_id, command.index)?;
        command.validate()?;
        self.require_no_image_install(command.range_id)?;
        self.require_no_active_recovery(command.range_id)?;
        let entry = self
            .catalog
            .ranges
            .get_mut(&command.range_id)
            .and_then(|log| log.entries.get_mut(&command.index))
            .ok_or_else(|| {
                range_write_error(format!(
                    "unknown range-write command {} at {} index {}",
                    command.command_id, command.range_id, command.index
                ))
            })?;
        if entry.command.command_id != command.command_id
            || entry.command.checksum_sha256 != command.checksum_sha256
        {
            return Err(range_write_error("range-write command identity mismatch"));
        }
        Ok(entry)
    }

    fn persist(&self) -> Result<()> {
        self.persist_with(crate::storage::write_atomic)
    }

    fn persist_with(&self, write: impl FnOnce(&Path, &[u8], bool) -> Result<()>) -> Result<()> {
        self.require_native_writer_host_base()?;
        fs::create_dir_all(&self.root)?;
        let envelope = RangeWriteEnvelope::new(self.catalog.clone())?;
        write(
            &self.root.join(DEFAULT_RANGE_WRITE_LOG),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )?;
        self.require_native_writer_host_base()
    }

    fn persist_or_restore<T>(&mut self, previous: RangeWriteCatalog, value: T) -> Result<T> {
        if let Err(error) = self.persist() {
            self.catalog = previous;
            return Err(error);
        }
        Ok(value)
    }
}

fn open_range_catalog_no_follow(path: &Path) -> Result<File> {
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
        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(range_write_error(format!(
                "refusing symlink range-write catalog {}",
                path.display()
            )));
        }
        OpenOptions::new().read(true).open(path)?
    };
    if !file.metadata()?.is_file() {
        return Err(range_write_error(format!(
            "refusing non-regular range-write catalog {}",
            path.display()
        )));
    }
    Ok(file)
}

fn advance_resolved(log: &mut RangeWriteLog) {
    loop {
        let next = log.resolved_through.saturating_add(1);
        let resolved = log.entries.get(&next).is_some_and(|entry| {
            matches!(
                entry.state,
                RangeWriteState::Applied | RangeWriteState::Aborted
            )
        });
        if !resolved {
            break;
        }
        log.resolved_through = next;
    }
}

fn compact_resolved(log: &mut RangeWriteLog) {
    let keep_from = log
        .resolved_through
        .saturating_sub(RETAIN_APPLIED_COMMANDS_PER_RANGE);
    if keep_from <= log.compacted_through {
        return;
    }
    log.entries.retain(|index, entry| {
        *index > keep_from
            || !matches!(
                entry.state,
                RangeWriteState::Applied | RangeWriteState::Aborted
            )
    });
    log.compacted_through = keep_from;
}

fn remove_expired_backup_fences(catalog: &mut RangeWriteCatalog, now_ms: u64) -> usize {
    let before = catalog.backup_fences.len();
    catalog
        .backup_fences
        .retain(|_, fence| fence.expires_at_ms > now_ms);
    before.saturating_sub(catalog.backup_fences.len())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeWriteAck {
    pub node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub index: u64,
    pub state: RangeWriteState,
}

pub trait RangeWriteTransport: std::fmt::Debug + Send + Sync + 'static {
    fn observe_current_native_range(
        &self,
        _destination: &ClusterNodeId,
        _request: &crate::distribution_native_read::NativeRangeReadProbeRequest,
        _parent: &CancellationToken,
    ) -> Result<crate::distribution_native_read::NativeRangeReadObservation> {
        Err(range_write_error(
            "authenticated current native read observation transport is not wired",
        ))
    }

    fn apply_native_origin(
        &self,
        _destination: &ClusterNodeId,
        _command: &RangeWriteCommand,
        _certificate: &NativeRangeCommitCertificate,
        _parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        Err(range_write_error(
            "authenticated ordinary native Apply transport is not wired",
        ))
    }
    fn validate_native_origin_pre_admission(
        &self,
        _voters: &[ClusterNodeId],
        command: &RangeWriteCommand,
        _intent: &NativeRangeCommandIntent,
    ) -> Result<()> {
        validate_native_recovery_command_size(command)
    }
    fn observe_native_origin(
        &self,
        _destination: &ClusterNodeId,
        _request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        _parent: &CancellationToken,
    ) -> Result<crate::distribution_native_apply::NativeReplicaApplyObservation> {
        Err(range_write_error(
            "authenticated ordinary native status transport is not wired",
        ))
    }
    fn recertify_native_origin(
        &self,
        _destination: &ClusterNodeId,
        _command: &RangeWriteCommand,
        _parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        Err(range_write_error(
            "bounded ordinary native recertification transport is not wired",
        ))
    }
    fn observe_native_completion_observation(
        &self,
        _destination: &ClusterNodeId,
        _request: &crate::distribution_range_recovery::NativeCompletionRequest,
        _parent: &crate::CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeCompletionObservation> {
        Err(range_write_error(
            "authenticated native completion transport is not wired",
        ))
    }
    fn prepare_native_ready_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        let _ = (destination, request, parent);
        Err(range_write_error(
            "transport does not support actual native preparation",
        ))
    }
    fn observe_native_ready_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::NativeReadyRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::NativeReadyObservation> {
        let _ = (destination, request, parent);
        Err(range_write_error(
            "transport does not support actual native-ready observations",
        ))
    }

    fn recovery_historical_command_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::RecoveryHistoricalCommandRequest,
        apply: bool,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::RecoveryHistoryObservation> {
        let _ = (destination, request, apply, parent);
        Err(range_write_error(
            "transport does not support authenticated historical recovery",
        ))
    }

    /// Explicit sparse certification: the receiver independently selects the
    /// committed decision and persists its full body without an ordered Apply.
    fn recovery_sparse_historical_command_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::RecoveryHistoricalCommandRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::RecoveryHistoryObservation> {
        let _ = (destination, request, parent);
        Err(range_write_error(
            "transport does not support sparse historical certification",
        ))
    }

    /// Validation only, before any durable native Prepare. Embedded transports
    /// have no TCP frame; production TCP overrides its configured envelope cap.
    fn validate_native_command_pre_admission(
        &self,
        _voters: &[ClusterNodeId],
        command: &RangeWriteCommand,
    ) -> Result<()> {
        validate_native_recovery_command_size(command)
    }

    fn recovery_fenced_history_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::RecoveryFencedProbeRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::RecoveryHistoryObservation> {
        let _ = (destination, request, parent);
        Err(range_write_error(
            "transport does not support fenced recovery history observations",
        ))
    }
    fn recovery_epoch_fence_observation(
        &self,
        destination: &ClusterNodeId,
        request: &crate::distribution_range_recovery::RecoveryEpochFenceRequest,
        parent: &CancellationToken,
    ) -> Result<crate::distribution_range_recovery::RecoveryFenceObservation> {
        let _ = (destination, request, parent);
        Err(range_write_error(
            "transport does not support native recovery epoch fences",
        ))
    }
    fn install_range_backup_fence(
        &self,
        destination: &ClusterNodeId,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        installed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RangeWriteProgress> {
        let _ = (
            destination,
            plan_id,
            range_id,
            range_epoch,
            installed_at_ms,
            expires_at_ms,
        );
        Err(range_write_error(
            "range-write transport does not support backup fences",
        ))
    }
    fn release_range_backup_fence(
        &self,
        destination: &ClusterNodeId,
        plan_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
    ) -> Result<bool> {
        let _ = (destination, plan_id, range_id, range_epoch);
        Err(range_write_error(
            "range-write transport does not support backup fence release",
        ))
    }
    fn range_write_progress(
        &self,
        destination: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
    ) -> Result<RangeWriteProgress>;
    fn apply_range_write_repair(
        &self,
        destination: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress>;
    fn fetch_range_write_repair(
        &self,
        source: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        previous_resolved_index: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch>;
    fn probe_range_write(
        &self,
        source: &ClusterNodeId,
        range_id: RangeId,
        range_epoch: u64,
        index: u64,
    ) -> Result<RangeWriteProbe>;
    fn prepare_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck>;
    fn commit_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck>;
    fn certify_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck>;
    fn apply_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck>;
    fn abort_range_write(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck>;
}

/// Production commit authority installed into `BicDb` by the cluster host.
#[derive(Debug)]
pub struct RangeWriteCoordinator<Transport: RangeWriteTransport> {
    cluster_id: ClusterId,
    local_node_id: ClusterNodeId,
    topology: Arc<RwLock<ClusterTopology>>,
    local_service: Arc<Mutex<ClusterDataNodeService>>,
    transport: Arc<Transport>,
    repair_limits: RangeWriteRepairLimits,
    range_mutations: RangeMutationGate,
    active_ranges: Mutex<BTreeMap<RangeId, String>>,
    active_range_changed: Condvar,
    recovered_epochs: Mutex<BTreeMap<RangeId, u64>>,
    recovery_cursor: Mutex<Option<RangeId>>,
    require_schema_compatibility: bool,
    local_incarnation: String,
    native_image_mode: bool,
    native_coordinator_id: Uuid,
    native_writer_host: Mutex<Option<std::sync::Weak<crate::db::NativeWriterHostState>>>,
    pending_acknowledgements: Mutex<BTreeMap<String, RangeAcknowledgementEvidence>>,
}

// Bounded by active range admissions. This is a callback handoff, not durable
// recovery evidence: durable certificate states remain in each replica log.
#[derive(Debug)]
struct RangeAcknowledgementEvidence {
    range_id: RangeId,
    transaction_id: u64,
    certified: usize,
    required_quorum: usize,
    mutation: RangeMutationPermit,
    native: Option<NativeRangeAcknowledgementEvidence>,
}

#[derive(Debug)]
struct NativeRangeAcknowledgementEvidence {
    pending: crate::db::NativeRangePendingApply,
    mutations_sha256: String,
    certificate: Option<NativeRangeCommitCertificate>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeBackupFenceQuorum {
    pub plan_id: Uuid,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub required_quorum: usize,
    pub installed_at_ms: u64,
    pub expires_at_ms: u64,
    pub observations: Vec<RangeWriteProgress>,
}

/// In-process authority created after validating one complete backup plan
/// against the coordinator's committed topology. Its fields are private and it
/// is not deserializable, so callers cannot forge a cheaper per-range check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeBackupFenceSession {
    plan_id: Uuid,
    cluster_id: ClusterId,
    topology_generation: u64,
    created_at_ms: u64,
    expires_at_ms: u64,
}

impl RangeBackupFenceSession {
    pub fn plan_id(&self) -> Uuid {
        self.plan_id
    }

    pub fn topology_generation(&self) -> u64 {
        self.topology_generation
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}

struct RangeAdmissionReservation<'a, Transport: RangeWriteTransport> {
    coordinator: &'a RangeWriteCoordinator<Transport>,
    range_id: RangeId,
    reservation_id: String,
    release_on_drop: bool,
}

impl<Transport: RangeWriteTransport> RangeAdmissionReservation<'_, Transport> {
    fn preserve_for_recovery(&mut self, command_id: &str) {
        let mut active = self.coordinator.active_ranges.lock();
        if active.get(&self.range_id) == Some(&self.reservation_id) {
            active.insert(self.range_id, command_id.to_string());
            self.release_on_drop = false;
        }
    }
}

impl<Transport: RangeWriteTransport> Drop for RangeAdmissionReservation<'_, Transport> {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        let mut active = self.coordinator.active_ranges.lock();
        if active.get(&self.range_id) == Some(&self.reservation_id) {
            active.remove(&self.range_id);
            self.coordinator.active_range_changed.notify_all();
        }
    }
}

impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    pub fn new(
        cluster_id: ClusterId,
        local_node_id: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        local_service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<Transport>,
    ) -> Result<Self> {
        Self::with_repair_limits(
            cluster_id,
            local_node_id,
            topology,
            local_service,
            transport,
            RangeWriteRepairLimits::default(),
        )
    }

    /// Production constructor. Write admission and leader recovery remain
    /// closed until metadata consensus advertises the exact fingerprint most
    /// recently verified against this node's live database.
    pub fn new_schema_fenced(
        cluster_id: ClusterId,
        local_node_id: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        local_service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<Transport>,
    ) -> Result<Self> {
        let mut coordinator = Self::with_repair_limits(
            cluster_id,
            local_node_id,
            topology,
            local_service,
            transport,
            RangeWriteRepairLimits::default(),
        )?;
        coordinator.require_schema_compatibility = true;
        Ok(coordinator)
    }

    pub fn with_repair_limits(
        cluster_id: ClusterId,
        local_node_id: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        local_service: Arc<Mutex<ClusterDataNodeService>>,
        transport: Arc<Transport>,
        repair_limits: RangeWriteRepairLimits,
    ) -> Result<Self> {
        repair_limits.validate()?;
        let current = topology.read();
        current.validate()?;
        if current.cluster_id != cluster_id || !current.nodes.contains_key(&local_node_id) {
            return Err(range_write_error(
                "range-write coordinator identity does not match topology",
            ));
        }
        drop(current);
        // Construction must not wait behind a caller-owned database write lock.
        // The incarnation is immutable for this database handle; callbacks run
        // under database guards and must not attempt to reacquire that lock.
        let (database, range_mutations) = {
            let service = local_service.lock();
            (service.database(), service.range_mutation_gate())
        };
        let database_guard = database.try_read().ok_or_else(|| {
            range_write_error("database is busy while constructing range authority")
        })?;
        let local_incarnation = database_guard.incarnation_id().to_string();
        let native_image_mode = database_guard.native_range_image_birth_present();
        if native_image_mode {
            local_service
                .try_lock()
                .ok_or_else(|| {
                    range_write_error("service busy while checking native catalog configuration")
                })?
                .require_native_catalog_database(&database_guard)?;
        }
        drop(database_guard);
        Ok(Self {
            cluster_id,
            local_node_id,
            topology,
            local_service,
            transport,
            repair_limits,
            range_mutations,
            active_ranges: Mutex::new(BTreeMap::new()),
            active_range_changed: Condvar::new(),
            recovered_epochs: Mutex::new(BTreeMap::new()),
            recovery_cursor: Mutex::new(None),
            require_schema_compatibility: false,
            local_incarnation,
            native_image_mode,
            native_coordinator_id: Uuid::now_v7(),
            native_writer_host: Mutex::new(None),
            pending_acknowledgements: Mutex::new(BTreeMap::new()),
        })
    }

    /// Validate the complete checksummed plan once. Per-range fencing then
    /// remains logarithmic in range count rather than rehashing a million-range
    /// topology for every range.
    pub fn begin_backup_fence_session(
        &self,
        plan: &ClusterBackupPlan,
        now_ms: u64,
    ) -> Result<RangeBackupFenceSession> {
        let topology = self.topology.read();
        let captured = ClusterBackupMetadata::capture(&topology)?;
        if plan.format_version != CLUSTER_BACKUP_CERTIFICATE_FORMAT_VERSION
            || plan.plan_id.is_nil()
            || plan.calculate_checksum()? != plan.checksum_sha256
            || plan.cluster_id != self.cluster_id
            || plan.topology_generation != topology.generation
            || plan.topology_sha256 != captured.topology_sha256
            || plan.range_epochs != captured.range_epochs
            || plan.created_at_ms >= plan.expires_at_ms
            || now_ms < plan.created_at_ms
            || now_ms > plan.expires_at_ms
        {
            return Err(range_write_error(
                "backup fence plan is stale, damaged, expired, or belongs to another topology",
            ));
        }
        Ok(RangeBackupFenceSession {
            plan_id: plan.plan_id,
            cluster_id: plan.cluster_id.clone(),
            topology_generation: plan.topology_generation,
            created_at_ms: plan.created_at_ms,
            expires_at_ms: plan.expires_at_ms,
        })
    }

    /// Stop one locally led range at a fully resolved cut and install that
    /// same durable fence on a current-voter quorum. The admission reservation
    /// closes the race between draining an in-flight command and persisting the
    /// leader fence. New SQL/internal writes then fail in `RangeWriteStore`,
    /// below the caller-facing protocol layer.
    pub fn fence_range_for_backup(
        &self,
        session: &RangeBackupFenceSession,
        range_id: RangeId,
        installed_at_ms: u64,
    ) -> Result<RangeBackupFenceQuorum> {
        let plan_id = session.plan_id;
        let expires_at_ms = session.expires_at_ms;
        if plan_id.is_nil() {
            return Err(range_write_error("backup fence plan id must not be nil"));
        }
        let (topology_generation, range) = {
            let topology = self.topology.read();
            if session.cluster_id != self.cluster_id
                || session.topology_generation != topology.generation
                || installed_at_ms < session.created_at_ms
                || installed_at_ms > session.expires_at_ms
            {
                return Err(range_write_error(
                    "backup fence session is stale, expired, or belongs to another topology",
                ));
            }
            let range = topology.range_by_id(range_id).cloned().ok_or_else(|| {
                range_write_error(format!("cannot fence unknown range {range_id}"))
            })?;
            (topology.generation, range)
        };
        if range.leader != self.local_node_id {
            return Err(range_write_error(format!(
                "node {} cannot fence {} led by {}",
                self.local_node_id, range.id, range.leader
            )));
        }
        self.require_local_schema_compatibility(&range)?;
        self.require_recovered_epoch(&range)?;
        let mutation = self.range_mutations.try_enter(range.id)?;
        let authority = RangeMutationAuthority::Mutation(&mutation);
        let _reservation = self.reserve_range(range.id)?;
        let voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        let required_quorum = voters.len() / 2 + 1;
        let mut repair_ready = Vec::new();
        let mut failures = Vec::new();
        for voter in voters.iter().filter(|node| **node != self.local_node_id) {
            match self.repair_voter(&range, voter) {
                Ok(()) => repair_ready.push(voter.clone()),
                Err(error) => failures.push(format!("{voter}: {error}")),
            }
        }
        if repair_ready.len().saturating_add(1) < required_quorum {
            return Err(range_write_error(format!(
                "backup fence for {} repaired only {}/{} voters: {}",
                range.id,
                repair_ready.len().saturating_add(1),
                required_quorum,
                failures.join(" | ")
            )));
        }

        let local_progress = self
            .local_service
            .lock()
            .install_range_backup_fence_with_mutation(
                &range,
                &self.local_node_id,
                plan_id,
                installed_at_ms,
                expires_at_ms,
                &authority,
            )?;
        let cut = local_progress.resolved_through;
        let mut observations = vec![local_progress];
        let mut fenced_remotes = Vec::new();
        let mut divergent_fence = false;
        for voter in repair_ready {
            if observations.len() >= required_quorum {
                break;
            }
            match self.transport.install_range_backup_fence(
                &voter,
                plan_id,
                range.id,
                range.epoch,
                installed_at_ms,
                expires_at_ms,
            ) {
                Ok(progress)
                    if progress.node_id == voter
                        && progress.range_id == range.id
                        && progress.current_epoch == range.epoch
                        && progress.last_index == cut
                        && progress.resolved_through == cut =>
                {
                    fenced_remotes.push(voter);
                    observations.push(progress);
                }
                Ok(progress) => {
                    fenced_remotes.push(voter.clone());
                    divergent_fence = true;
                    failures.push(format!(
                        "{voter}: fence returned divergent progress {}/{} epoch {}",
                        progress.resolved_through, progress.last_index, progress.current_epoch
                    ));
                }
                Err(error) => failures.push(format!("{voter}: {error}")),
            }
        }
        if divergent_fence || observations.len() < required_quorum {
            self.rollback_backup_fences(&range, plan_id, &fenced_remotes, &authority);
            return Err(range_write_error(format!(
                "backup fence for {} reached only {}/{} exact-cut voters: {}",
                range.id,
                observations.len(),
                required_quorum,
                failures.join(" | ")
            )));
        }
        let still_current = {
            let topology = self.topology.read();
            topology.generation == topology_generation
                && topology.range_by_id(range.id).is_some_and(|current| {
                    current.epoch == range.epoch && current.leader == self.local_node_id
                })
        };
        if !still_current {
            self.rollback_backup_fences(&range, plan_id, &fenced_remotes, &authority);
            return Err(range_write_error(format!(
                "topology changed while fencing {} epoch {}",
                range.id, range.epoch
            )));
        }
        observations.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(RangeBackupFenceQuorum {
            plan_id,
            range_id: range.id,
            range_epoch: range.epoch,
            resolved_through: cut,
            required_quorum,
            installed_at_ms,
            expires_at_ms,
            observations,
        })
    }

    /// Release remote fences before the leader fence. A failed remote release
    /// leaves the leader closed so an operator can retry without allowing the
    /// range to advance while a selected archive is still being captured.
    pub fn release_range_backup_fence(&self, fence: &RangeBackupFenceQuorum) -> Result<usize> {
        let plan_id = fence.plan_id;
        let range_id = fence.range_id;
        let range_epoch = fence.range_epoch;
        let range = self
            .topology
            .read()
            .range_by_id(range_id)
            .filter(|range| range.epoch == range_epoch)
            .cloned()
            .ok_or_else(|| {
                range_write_error(format!(
                    "cannot release unknown or stale range {range_id} epoch {range_epoch}"
                ))
            })?;
        if range.leader != self.local_node_id {
            return Err(range_write_error(format!(
                "node {} cannot release backup fence for {} led by {}",
                self.local_node_id, range.id, range.leader
            )));
        }
        let required_quorum = range.voter_count() / 2 + 1;
        let mut selected_nodes = BTreeSet::new();
        for progress in &fence.observations {
            if !selected_nodes.insert(progress.node_id.clone())
                || progress.range_id != range.id
                || progress.current_epoch != range.epoch
                || progress.last_index != fence.resolved_through
                || progress.resolved_through != fence.resolved_through
                || !range.replicas.iter().any(|replica| {
                    replica.node_id == progress.node_id && replica.role == RangeReplicaRole::Voter
                })
            {
                return Err(range_write_error(
                    "backup fence release evidence is stale, duplicate, or divergent",
                ));
            }
        }
        if plan_id.is_nil()
            || fence.required_quorum != required_quorum
            || fence.observations.len() < required_quorum
            || !selected_nodes.contains(&self.local_node_id)
        {
            return Err(range_write_error(
                "backup fence release evidence has no current leader-inclusive quorum",
            ));
        }
        let mutation = self.range_mutations.try_enter(range.id)?;
        let authority = RangeMutationAuthority::Mutation(&mutation);
        let _reservation = self.reserve_range(range.id)?;
        let mut released = 0usize;
        let mut failures = Vec::new();
        for voter in selected_nodes
            .iter()
            .filter(|node_id| **node_id != self.local_node_id)
        {
            match self
                .transport
                .release_range_backup_fence(voter, plan_id, range.id, range.epoch)
            {
                Ok(was_released) => released = released.saturating_add(usize::from(was_released)),
                Err(error) => failures.push(format!("{voter}: {error}")),
            }
        }
        if !failures.is_empty() {
            return Err(range_write_error(format!(
                "backup fence release for {} failed closed: {}",
                range.id,
                failures.join(" | ")
            )));
        }
        if self
            .local_service
            .lock()
            .release_range_backup_fence_with_mutation(
                &range,
                &self.local_node_id,
                plan_id,
                &authority,
            )?
        {
            released = released.saturating_add(1);
        }
        Ok(released)
    }

    /// Release an anti-entropy fence only after one range-level certificate
    /// covers every voter observed by the original fence and reconstructs the
    /// quorum-certified source root for every divergent voter.
    pub fn release_repaired_range_fence(
        &self,
        fence: &RangeBackupFenceQuorum,
        certificate: &RangeDigestRepairCertificate,
    ) -> Result<usize> {
        certificate.validate()?;
        let mut fenced_nodes = fence
            .observations
            .iter()
            .map(|progress| progress.node_id.clone())
            .collect::<Vec<_>>();
        fenced_nodes.sort();
        if certificate.report.session_id != fence.plan_id
            || certificate.report.cluster_id != self.cluster_id
            || certificate.report.range_id != fence.range_id
            || certificate.report.range_epoch != fence.range_epoch
            || certificate.report.resolved_through != fence.resolved_through
            || certificate.report.required_quorum != fence.required_quorum
            || certificate.covered_nodes != fenced_nodes
        {
            return Err(range_write_error(
                "range repair certificate does not exactly cover the active fence quorum",
            ));
        }
        self.release_range_backup_fence(fence)
    }

    fn rollback_backup_fences(
        &self,
        range: &RangeDescriptor,
        plan_id: Uuid,
        fenced_remotes: &[ClusterNodeId],
        authority: &RangeMutationAuthority<'_>,
    ) {
        for voter in fenced_remotes {
            let _ =
                self.transport
                    .release_range_backup_fence(voter, plan_id, range.id, range.epoch);
        }
        let _ = self
            .local_service
            .lock()
            .release_range_backup_fence_with_mutation(
                range,
                &self.local_node_id,
                plan_id,
                authority,
            );
    }

    /// Recover a bounded number of ranges for which this node is the current
    /// leader. The cluster supervisor invokes this outside SQL/database locks;
    /// writes remain fail-closed until their exact range epoch is recovered.
    pub fn recover_local_leadership(&self, max_ranges: usize) -> Result<usize> {
        self.require_expected_native_recovery_store()?;
        if max_ranges == 0 || max_ranges > 1_024 {
            return Err(range_write_error(
                "range leadership recovery limit must be in 1..=1024",
            ));
        }
        let mut ranges = {
            let topology = self.topology.read();
            let recovered = self.recovered_epochs.lock();
            topology
                .ranges
                .values()
                .filter(|range| {
                    range.leader == self.local_node_id
                        && recovered.get(&range.id) != Some(&range.epoch)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        ranges.sort_by_key(|range| range.id);
        if let Some(cursor) = *self.recovery_cursor.lock() {
            let split = ranges.partition_point(|range| range.id <= cursor);
            if split < ranges.len() {
                ranges.rotate_left(split);
            }
        }
        ranges.truncate(max_ranges);
        if let Some(last) = ranges.last() {
            *self.recovery_cursor.lock() = Some(last.id);
        } else {
            *self.recovery_cursor.lock() = None;
        }
        let mut completed = 0usize;
        let mut failures = Vec::new();
        for range in ranges {
            if let Err(error) = self.recover_one_range_leadership(&range) {
                failures.push(format!("{}: {error}", range.id));
            } else {
                self.recovered_epochs.lock().insert(range.id, range.epoch);
                completed = completed.saturating_add(1);
            }
        }
        let topology = self.topology.read();
        self.recovered_epochs.lock().retain(|range_id, epoch| {
            topology
                .range_by_id(*range_id)
                .is_some_and(|range| range.leader == self.local_node_id && range.epoch == *epoch)
        });
        if !failures.is_empty() {
            return Err(range_write_error(format!(
                "range leadership recovery deferred for {} range(s): {}",
                failures.len(),
                failures.join(" | ")
            )));
        }
        Ok(completed)
    }

    fn recover_one_range_leadership(&self, range: &RangeDescriptor) -> Result<()> {
        self.require_local_schema_compatibility(range)?;
        let mutation = self.range_mutations.try_enter(range.id)?;
        let authority = &RangeMutationAuthority::Mutation(&mutation);
        let _reservation = self.try_reserve_range(range.id)?;
        let voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();
        self.recover_leader_prefix(range, &voters, authority)?;
        self.recover_leader_tail(range, &voters, authority)?;
        let still_current = self
            .topology
            .read()
            .range_by_id(range.id)
            .is_some_and(|current| {
                current.epoch == range.epoch && current.leader == self.local_node_id
            });
        if !still_current {
            return Err(range_write_error(format!(
                "range {} leadership changed during epoch {} recovery",
                range.id, range.epoch
            )));
        }
        Ok(())
    }

    fn require_recovered_epoch(&self, range: &RangeDescriptor) -> Result<()> {
        if self.recovered_epochs.lock().get(&range.id) != Some(&range.epoch) {
            return Err(range_write_error(format!(
                "range {} epoch {} leader recovery is not complete; writes remain unavailable",
                range.id, range.epoch
            )));
        }
        Ok(())
    }

    fn require_local_schema_compatibility(&self, range: &RangeDescriptor) -> Result<()> {
        if !self.require_schema_compatibility {
            return Ok(());
        }
        let (required, pending_target, advertised, advertised_pending) = {
            let topology = self.topology.read();
            let current = topology
                .range_by_id(range.id)
                .filter(|current| {
                    current.epoch == range.epoch && current.leader == self.local_node_id
                })
                .ok_or_else(|| {
                    range_write_error(format!(
                        "range {} epoch {} changed before schema validation",
                        range.id, range.epoch
                    ))
                })?;
            let required = current
                .required_schema_sha256(&topology.nodes)
                .ok_or_else(|| {
                    range_write_error(format!(
                        "range {} epoch {} has no leader schema fingerprint; writes remain unavailable",
                        range.id, range.epoch
                    ))
                })?
                .to_string();
            let advertised = topology
                .nodes
                .get(&self.local_node_id)
                .and_then(|node| {
                    node.labels
                        .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL)
                })
                .cloned();
            let pending_target = topology
                .nodes
                .get(&current.leader)
                .and_then(|node| {
                    node.labels
                        .get(crate::distribution::SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                })
                .cloned();
            let advertised_pending = topology
                .nodes
                .get(&self.local_node_id)
                .and_then(|node| {
                    node.labels
                        .get(crate::distribution::SCHEMA_COMPATIBILITY_TARGET_NODE_LABEL)
                })
                .cloned();
            (required, pending_target, advertised, advertised_pending)
        };
        if advertised.as_deref() != Some(required.as_str()) {
            return Err(range_write_error(format!(
                "range {} schema fence requires {required}, but node {} advertises {}",
                range.id,
                self.local_node_id,
                advertised.as_deref().unwrap_or("missing")
            )));
        }
        if advertised_pending != pending_target {
            return Err(range_write_error(format!(
                "range {} schema fence pending target differs between leader and node {}",
                range.id, self.local_node_id
            )));
        }
        let service = self.local_service.lock();
        if !service.schema_compatibility_allows(&required, pending_target.as_deref()) {
            return Err(range_write_error(format!(
                "range {} schema fence requires active {required}, pending {}, but node {} has verified {}",
                range.id,
                pending_target.as_deref().unwrap_or("none"),
                self.local_node_id,
                service
                    .verified_schema_compatibility_sha256()
                    .as_deref()
                    .unwrap_or("stale or missing")
            )));
        }
        Ok(())
    }

    fn reserve_range(&self, range_id: RangeId) -> Result<RangeAdmissionReservation<'_, Transport>> {
        let deadline = Instant::now() + RANGE_ADMISSION_WAIT;
        let mut active = self.active_ranges.lock();
        while let Some(command_id) = active.get(&range_id) {
            let now = Instant::now();
            if now >= deadline {
                return Err(range_write_error(format!(
                    "range {range_id} is waiting for local durability of command {command_id}; recovery is required before admitting later writes"
                )));
            }
            self.active_range_changed
                .wait_for(&mut active, deadline.saturating_duration_since(now));
        }
        let reservation_id = format!("reservation:{}", Uuid::now_v7());
        active.insert(range_id, reservation_id.clone());
        drop(active);
        Ok(RangeAdmissionReservation {
            coordinator: self,
            range_id,
            reservation_id,
            release_on_drop: true,
        })
    }

    fn reserve_recovered_range(
        &self,
        range: &RangeDescriptor,
    ) -> Result<RangeAdmissionReservation<'_, Transport>> {
        let reservation = self.reserve_range(range.id)?;
        // A writer may have observed readiness before an earlier callback
        // invalidated it while this writer waited for the reservation.
        self.require_recovered_epoch(range)?;
        Ok(reservation)
    }

    fn try_reserve_range(
        &self,
        range_id: RangeId,
    ) -> Result<RangeAdmissionReservation<'_, Transport>> {
        let mut active = self.active_ranges.lock();
        if let Some(command_id) = active.get(&range_id) {
            return Err(range_write_error(format!(
                "range {range_id} is still applying command {command_id}; leadership recovery deferred"
            )));
        }
        let reservation_id = format!("recovery:reservation:{}", Uuid::now_v7());
        active.insert(range_id, reservation_id.clone());
        drop(active);
        Ok(RangeAdmissionReservation {
            coordinator: self,
            range_id,
            reservation_id,
            release_on_drop: true,
        })
    }

    fn resolve_range(&self, intent: &CommitAdmissionIntent) -> Result<RangeDescriptor> {
        if intent.mutations.is_empty() {
            return Err(range_write_error("distributed commit intent is empty"));
        }
        let topology = self.topology.read();
        let mut resolved = None;
        for mutation in &intent.mutations {
            let range = topology.range_for_key(&mutation.collection, &mutation.record_id)?;
            if resolved.is_some_and(|range_id| range_id != range.id) {
                return Err(range_write_error(
                    "cross-range write rejected: use the explicit distributed commit protocol",
                ));
            }
            resolved = Some(range.id);
        }
        let range = topology
            .range_by_id(resolved.expect("non-empty intent resolved a range"))
            .expect("resolved range remains present")
            .clone();
        if range.leader != self.local_node_id {
            return Err(range_write_error(format!(
                "node {} is not leader for {} epoch {}; current leader is {}",
                self.local_node_id, range.id, range.epoch, range.leader
            )));
        }
        if !range.replicas.iter().any(|replica| {
            replica.node_id == self.local_node_id && replica.role == RangeReplicaRole::Voter
        }) {
            return Err(range_write_error(format!(
                "range {} leader {} is not a voter",
                range.id, self.local_node_id
            )));
        }
        drop(topology);
        self.require_local_schema_compatibility(&range)?;
        Ok(range)
    }

    fn validate_ack(
        &self,
        destination: &ClusterNodeId,
        command: &RangeWriteCommand,
        ack: &RangeWriteAck,
    ) -> Result<()> {
        if &ack.node_id != destination
            || ack.range_id != command.range_id
            || ack.range_epoch != command.range_epoch
            || ack.index != command.index
        {
            return Err(range_write_error(format!(
                "range-write acknowledgement identity mismatch from {destination}"
            )));
        }
        Ok(())
    }

    fn abort_prepared(
        &self,
        range: &RangeDescriptor,
        voters: &[ClusterNodeId],
        command: &RangeWriteCommand,
        authority: &RangeMutationAuthority<'_>,
    ) {
        for voter in voters.iter().filter(|node| **node != self.local_node_id) {
            let _ = self.transport.abort_range_write(voter, command);
        }
        let _ = self.local_service.lock().abort_range_write_with_mutation(
            range,
            &self.local_node_id,
            command,
            authority,
        );
    }

    fn recover_leader_prefix(
        &self,
        range: &RangeDescriptor,
        voters: &[ClusterNodeId],
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let quorum = voters.len() / 2 + 1;
        let mut progress_by_node = BTreeMap::new();
        let local = self.local_service.lock().range_write_progress(range.id);
        progress_by_node.insert(self.local_node_id.clone(), local.clone());
        for voter in voters.iter().filter(|node| **node != self.local_node_id) {
            if let Ok(progress) = self
                .transport
                .range_write_progress(voter, range.id, range.epoch)
            {
                if progress.node_id != *voter
                    || progress.range_id != range.id
                    || progress.current_epoch > range.epoch
                {
                    return Err(range_write_error(format!(
                        "range-write leader recovery received invalid progress from {voter}"
                    )));
                }
                progress_by_node.insert(voter.clone(), progress);
            }
        }
        if progress_by_node.len() < quorum {
            return Err(range_write_error(format!(
                "range-write leader recovery for {} reached only {}/{} voters",
                range.id,
                progress_by_node.len(),
                quorum
            )));
        }
        let mut resolved_watermarks = progress_by_node
            .values()
            .map(|progress| progress.resolved_through)
            .collect::<Vec<_>>();
        resolved_watermarks.sort_unstable_by(|left, right| right.cmp(left));
        let quorum_resolved = resolved_watermarks[quorum - 1];
        if self.native_image_mode && local.resolved_through < quorum_resolved {
            return Err(range_write_error("native leader prefix requires fresh certified native recovery/image before generic replay"));
        }
        let mut local_resolved = local.resolved_through;
        for _ in 0..self.repair_limits.max_batches_per_admission {
            if local_resolved >= quorum_resolved {
                return Ok(());
            }
            let mut candidates = Vec::new();
            for (source, progress) in &progress_by_node {
                if source == &self.local_node_id
                    || progress.resolved_through <= local_resolved
                    || progress.compacted_through > local_resolved
                {
                    continue;
                }
                if let Ok(mut batch) = self.transport.fetch_range_write_repair(
                    source,
                    range.id,
                    range.epoch,
                    local_resolved,
                    &self.repair_limits,
                ) {
                    batch.validate(&self.repair_limits)?;
                    if batch.source_node_id != *source
                        || batch.previous_resolved_index != local_resolved
                    {
                        return Err(range_write_error(format!(
                            "range-write leader recovery batch identity mismatch from {source}"
                        )));
                    }
                    batch
                        .entries
                        .retain(|entry| entry.command.index <= quorum_resolved);
                    if batch.entries.is_empty() {
                        continue;
                    }
                    batch.source_resolved_through = quorum_resolved;
                    batch.serialized_entry_bytes =
                        batch.entries.iter().try_fold(0usize, |total, entry| {
                            Ok::<_, BicDbError>(
                                total.saturating_add(serde_json::to_vec(entry)?.len()),
                            )
                        })?;
                    batch.checksum_sha256 = batch.calculate_checksum()?;
                    batch.validate(&self.repair_limits)?;
                    candidates.push(batch);
                }
            }
            if candidates.is_empty() {
                return Err(range_write_error(format!(
                    "range-write leader recovery for {} requires a range snapshot at index {local_resolved}; quorum resolved through {quorum_resolved}",
                    range.id
                )));
            }
            candidates.sort_by(|left, right| right.entries.len().cmp(&left.entries.len()));
            let chosen = candidates.remove(0);
            for other in &candidates {
                for (expected, observed) in chosen.entries.iter().zip(&other.entries) {
                    if expected.command.index == observed.command.index && expected != observed {
                        return Err(range_write_error(format!(
                            "divergent range-write histories for {} at index {} between voters {} and {}",
                            range.id,
                            expected.command.index,
                            chosen.source_node_id,
                            other.source_node_id
                        )));
                    }
                }
            }
            let next = self
                .local_service
                .lock()
                .apply_range_write_leader_recovery_with_mutation(
                    range,
                    &chosen.source_node_id,
                    &chosen,
                    &self.repair_limits,
                    authority,
                )?;
            if next.resolved_through <= local_resolved || next.resolved_through > quorum_resolved {
                return Err(range_write_error(
                    "range-write leader recovery made invalid progress",
                ));
            }
            local_resolved = next.resolved_through;
        }
        if local_resolved < quorum_resolved {
            return Err(range_write_error(format!(
                "range-write leader recovery budget exhausted for {} at {local_resolved} of {quorum_resolved}",
                range.id
            )));
        }
        Ok(())
    }

    fn recover_leader_tail(
        &self,
        range: &RangeDescriptor,
        voters: &[ClusterNodeId],
        authority: &RangeMutationAuthority<'_>,
    ) -> Result<()> {
        let quorum = voters.len() / 2 + 1;
        let command_budget = self
            .repair_limits
            .max_commands_per_batch
            .saturating_mul(self.repair_limits.max_batches_per_admission);
        for _ in 0..command_budget {
            let index = self
                .local_service
                .lock()
                .range_write_progress(range.id)
                .resolved_through
                .saturating_add(1);
            let mut probes = Vec::new();
            for voter in voters {
                let probe = if *voter == self.local_node_id {
                    self.local_service.lock().probe_range_write_for_peer(
                        range,
                        &self.local_node_id,
                        index,
                    )
                } else {
                    self.transport
                        .probe_range_write(voter, range.id, range.epoch, index)
                };
                if let Ok(probe) = probe {
                    probe.validate()?;
                    if probe.node_id != *voter
                        || probe.range_id != range.id
                        || probe.route_epoch != range.epoch
                        || probe.index != index
                    {
                        return Err(range_write_error(format!(
                            "range-write tail probe identity mismatch from {voter}"
                        )));
                    }
                    probes.push(probe);
                }
            }
            if probes.len() < quorum {
                return Err(range_write_error(format!(
                    "range-write tail recovery for {} index {index} reached only {}/{} voters",
                    range.id,
                    probes.len(),
                    quorum
                )));
            }

            let mut selected: Option<(ClusterNodeId, RangeWriteCommand)> = None;
            let mut decision_count = 0usize;
            let mut has_certificate = false;
            let mut has_abort = false;
            let mut opaque = 0usize;
            let mut maximum_last_index = 0u64;
            for probe in &probes {
                maximum_last_index = maximum_last_index.max(probe.progress.last_index);
                if index <= probe.progress.compacted_through {
                    opaque = opaque.saturating_add(1);
                    continue;
                }
                let Some(entry) = &probe.entry else {
                    continue;
                };
                if let Some((selected_node, selected_command)) = &selected {
                    if selected_command.command_id != entry.command.command_id
                        || selected_command.checksum_sha256 != entry.command.checksum_sha256
                    {
                        return Err(range_write_error(format!(
                            "divergent unresolved range-write histories for {} index {index} between voters {selected_node} and {}",
                            range.id, probe.node_id
                        )));
                    }
                } else {
                    selected = Some((probe.node_id.clone(), entry.command.clone()));
                }
                match entry.state {
                    RangeWriteState::Committed => decision_count = decision_count.saturating_add(1),
                    RangeWriteState::QuorumCommitted | RangeWriteState::Applied => {
                        decision_count = decision_count.saturating_add(1);
                        has_certificate = true;
                    }
                    RangeWriteState::Aborted => has_abort = true,
                    RangeWriteState::Prepared => {}
                }
            }

            let unknown = voters
                .len()
                .saturating_sub(probes.len())
                .saturating_add(opaque);
            let Some((evidence_voter, command)) = selected else {
                if maximum_last_index >= index {
                    return Err(range_write_error(format!(
                        "range-write tail recovery for {} index {index} has only compacted or missing evidence; a range snapshot is required",
                        range.id
                    )));
                }
                // A current voter quorum observed no tail at this index. Any
                // inaccessible minority cannot have formed a prior decision
                // quorum, so this index is safe to assign.
                return Ok(());
            };

            if has_abort && (has_certificate || decision_count >= quorum) {
                return Err(range_write_error(format!(
                    "conflicting abort and commit evidence for {} index {index}",
                    range.id
                )));
            }
            let outcome = if has_certificate || decision_count >= quorum {
                RangeWriteState::Applied
            } else if decision_count.saturating_add(unknown) >= quorum {
                return Err(range_write_error(format!(
                    "ambiguous provisional decision for {} index {index}: {decision_count} durable decisions, {unknown} unavailable or compacted voters",
                    range.id
                )));
            } else {
                RangeWriteState::Aborted
            };
            if self.native_image_mode {
                return Err(range_write_error("native unresolved leader tail requires fresh native recovery; generic resolution cannot publish a native prefix"));
            }
            let progress = self
                .local_service
                .lock()
                .resolve_range_write_leader_tail_with_mutation(
                    range,
                    &evidence_voter,
                    &command,
                    outcome,
                    authority,
                )?;
            if progress.resolved_through != index {
                return Err(range_write_error(format!(
                    "range-write tail resolution for {} made invalid progress at index {index}",
                    range.id
                )));
            }
            for voter in voters.iter().filter(|node| **node != self.local_node_id) {
                let _ = self.repair_voter(range, voter);
            }
        }
        Err(range_write_error(format!(
            "range-write tail recovery budget exhausted for {}",
            range.id
        )))
    }

    fn repair_voter(&self, range: &RangeDescriptor, voter: &ClusterNodeId) -> Result<()> {
        if self.native_image_mode {
            return Err(range_write_error("native repair requires actual source context and authenticated native materialization"));
        }
        let local_progress = self.local_service.lock().range_write_progress(range.id);
        let mut remote = self
            .transport
            .range_write_progress(voter, range.id, range.epoch)?;
        if remote.node_id != *voter || remote.range_id != range.id {
            return Err(range_write_error(format!(
                "range-write progress identity mismatch from {voter}"
            )));
        }
        if remote.current_epoch > range.epoch {
            return Err(range_write_error(format!(
                "range-write voter {voter} is at future epoch {}, current route is {}",
                remote.current_epoch, range.epoch
            )));
        }
        if remote.resolved_through > local_progress.resolved_through
            || remote.last_index > local_progress.last_index
        {
            return Err(range_write_error(format!(
                "range-write voter {voter} is ahead of leader {} for {}; leader recovery is required",
                self.local_node_id, range.id
            )));
        }
        for _ in 0..self.repair_limits.max_batches_per_admission {
            if remote.resolved_through >= local_progress.resolved_through {
                return Ok(());
            }
            let batch = self.local_service.lock().export_range_write_repair(
                range,
                &self.local_node_id,
                remote.resolved_through,
                &self.repair_limits,
            )?;
            if batch.entries.is_empty() {
                break;
            }
            let next =
                self.transport
                    .apply_range_write_repair(voter, &batch, &self.repair_limits)?;
            if next.node_id != *voter
                || next.range_id != range.id
                || next.resolved_through <= remote.resolved_through
                || next.resolved_through > local_progress.resolved_through
            {
                return Err(range_write_error(format!(
                    "range-write repair progress from {voter} is invalid"
                )));
            }
            remote = next;
        }
        if remote.resolved_through < local_progress.resolved_through {
            return Err(range_write_error(format!(
                "range-write repair budget exhausted for {voter}/{} at {} of {}",
                range.id, remote.resolved_through, local_progress.resolved_through
            )));
        }
        Ok(())
    }
}

impl<Transport: RangeWriteTransport> RangeAntiEntropyFenceAuthority
    for RangeWriteCoordinator<Transport>
{
    fn install_range_anti_entropy_fence(
        &self,
        plan: &RangeAntiEntropyFencePlan,
        now_ms: u64,
    ) -> Result<RangeBackupFenceQuorum> {
        let topology_generation = {
            let topology = self.topology.read();
            plan.validate(&topology, &self.local_node_id, now_ms)?;
            topology.generation
        };
        let session = RangeBackupFenceSession {
            plan_id: plan.plan_id,
            cluster_id: plan.cluster_id.clone(),
            topology_generation,
            created_at_ms: plan.created_at_ms,
            expires_at_ms: plan.expires_at_ms,
        };
        self.fence_range_for_backup(&session, plan.range_id, plan.created_at_ms)
    }

    fn release_range_anti_entropy_fence(&self, fence: &RangeBackupFenceQuorum) -> Result<usize> {
        self.release_range_backup_fence(fence)
    }
}

impl<Transport: RangeWriteTransport> RangeWriteCoordinator<Transport> {
    fn admit_with_database(
        &self,
        intent: &CommitAdmissionIntent,
        database: Option<&crate::BicDb>,
        prepare_rejection: &mut Option<NativePrepareOnlyRejection>,
    ) -> Result<CommitAdmissionTicket> {
        self.require_expected_native_database(database)?;
        let native_database = match (self.native_image_mode, database) {
            (false, None) => None,
            (false, Some(database)) if !database.native_range_image_birth_present() => None,
            (true, Some(database))
                if database.native_range_image_birth_present()
                    && database.incarnation_id() == self.local_incarnation =>
            {
                Some(database)
            }
            _ => {
                return Err(range_write_error(
                    "native admission requires its actual originating database context",
                ))
            }
        };
        if let Some(database) = native_database {
            self.local_service
                .lock()
                .require_native_catalog_database(database)?;
        }
        let range = self.resolve_range(intent)?;
        if let Some(database) = native_database {
            self.require_native_writer_host_context(database, &range)?;
        }
        self.require_recovered_epoch(&range)?;
        let mut reservation = if native_database.is_some() {
            // Native callbacks may borrow an actual outer DB.write guard.
            // Never wait on a point reader while holding that guard: fail
            // before Prepare so the caller can retry after releasing it.
            let reservation = self.try_reserve_range(range.id)?;
            let current = self.resolve_range(intent)?;
            if crate::distribution_native_read::canonical_range(&current)
                != crate::distribution_native_read::canonical_range(&range)
            {
                return Err(range_write_error(
                    "native range route changed before mutation admission; retry",
                ));
            }
            self.require_recovered_epoch(&range)?;
            reservation
        } else {
            self.reserve_recovered_range(&range)?
        };
        // A waiting writer must not enter native mutation admission while an
        // actual point read holds this same logical range reservation.
        let mutation = self.range_mutations.try_enter(range.id)?;
        let authority = &RangeMutationAuthority::Mutation(&mutation);
        let voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<Vec<_>>();

        let index = self.local_service.lock().next_range_write_index(range.id);
        let command = if native_database.is_some() {
            let command = RangeWriteCommand::new_native_bounded(
                self.cluster_id.clone(),
                &range,
                index,
                self.local_node_id.clone(),
                intent,
            )?;
            self.transport
                .validate_native_command_pre_admission(&voters, &command)?;
            command
        } else {
            RangeWriteCommand::new(
                self.cluster_id.clone(),
                &range,
                index,
                self.local_node_id.clone(),
                intent,
            )?
        };
        let native_plan = if let Some(database) = native_database {
            let topology = self.topology.read();
            let bootstrap = topology.range_image_bootstrap_id.ok_or_else(|| {
                range_write_error("native admission has no committed bootstrap domain")
            })?;
            let incarnation = topology
                .nodes
                .get(&self.local_node_id)
                .ok_or_else(|| range_write_error("native local member missing"))?
                .incarnation;
            database.native_recovery_admission(
                &self.cluster_id,
                bootstrap,
                &self.local_node_id,
                incarnation,
            )?;
            let lineage = database.current_native_range_source_lineage(range.id)?;
            let plan =
                NativeRangeCommandIntent::plan_source(&command, &range, &topology, &lineage)?;
            plan.validate_native_base(&lineage)?;
            self.transport
                .validate_native_origin_pre_admission(&voters, &command, &plan)?;
            Some(plan)
        } else {
            None
        };
        let native_abort_attempt = if let Some(database) = native_database {
            if database.native_abort_catalogue_store_id()?.is_some() {
                Some(
                    self.local_service
                        .lock()
                        .begin_native_abort_attempt(database, &range, &command, authority)?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let local_prepare = self.local_service.lock().prepare_range_write_with_mutation(
            &range,
            &self.local_node_id,
            command.clone(),
            authority,
        );
        if let Err(error) = local_prepare {
            if let Some(attempt) = native_abort_attempt {
                self.recovered_epochs.lock().remove(&range.id);
                *prepare_rejection = Some(attempt.retain_mutation(mutation));
            }
            return Err(error);
        }

        let quorum = voters.len() / 2 + 1;
        let mut prepared = 1usize;
        let mut prepared_nodes = Vec::new();
        let mut repair_failures = Vec::new();
        for voter in voters.iter().filter(|node| **node != self.local_node_id) {
            let repaired = if let Some(database) = native_database {
                self.repair_native_voter(database, &range, voter, authority)
            } else {
                self.repair_voter(&range, voter)
            };
            if let Err(error) = repaired {
                repair_failures.push(format!("{voter}: {error}"));
            } else if let Ok(ack) = self.transport.prepare_range_write(voter, &command) {
                if self.validate_ack(voter, &command, &ack).is_ok()
                    && matches!(
                        ack.state,
                        RangeWriteState::Prepared
                            | RangeWriteState::Committed
                            | RangeWriteState::QuorumCommitted
                            | RangeWriteState::Applied
                    )
                {
                    prepared += 1;
                    prepared_nodes.push(voter.clone());
                }
            }
        }
        if prepared < quorum {
            if let Some(attempt) = native_abort_attempt {
                self.recovered_epochs.lock().remove(&range.id);
                *prepare_rejection = Some(attempt.retain_mutation(mutation));
                return Err(range_write_error(format!(
                    "range {} command {} failed prepare quorum {prepared}/{quorum}; actual local cleanup/no-effect intent required before abort recovery", range.id, command.command_id,
                )));
            }
            // A prepare may have become durable even when its response was
            // lost or malformed. Broadcast the abort to every voter so those
            // unknown outcomes cannot accumulate unresolved entries forever.
            self.abort_prepared(&range, &voters, &command, authority);
            return Err(range_write_error(format!(
                "range {} write {} failed prepare quorum: {prepared}/{quorum}; repair failures: {}",
                range.id,
                command.command_id,
                if repair_failures.is_empty() {
                    "none".to_string()
                } else {
                    repair_failures.join(" | ")
                }
            )));
        }

        // Native uncertainty starts BEFORE attempting the local Commit write:
        // persistence can succeed even when its error/reply is lost. No native
        // abort may be inferred from any failure after this boundary.
        let local_commit = self
            .local_service
            .lock()
            .commit_range_write_decision_with_mutation(
                &range,
                &self.local_node_id,
                &command,
                authority,
            );
        if let Err(error) = local_commit {
            if native_database.is_some() {
                self.recovered_epochs.lock().remove(&range.id);
                return Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None,
                    transaction_id: Some(command.transaction_id),
                    incarnation: self.local_incarnation.clone(),
                    detail: format!(
                        "range {} command {} local Commit persistence is uncertain: {error}; local application WAL has not started; retained for recovery",
                        range.id, command.command_id,
                    ),
                });
            }
            return Err(error);
        }
        let mut committed = 1usize;
        let mut committed_nodes = Vec::new();
        for voter in &prepared_nodes {
            if let Ok(ack) = self.transport.commit_range_write(voter, &command) {
                if self.validate_ack(voter, &command, &ack).is_ok()
                    && matches!(
                        ack.state,
                        RangeWriteState::Committed
                            | RangeWriteState::QuorumCommitted
                            | RangeWriteState::Applied
                    )
                {
                    committed += 1;
                    committed_nodes.push(voter.clone());
                }
            }
        }
        if committed < quorum {
            if native_database.is_some() {
                self.recovered_epochs.lock().remove(&range.id);
                return Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None,
                    transaction_id: Some(command.transaction_id),
                    incarnation: self.local_incarnation.clone(),
                    detail: format!(
                        "range {} command {} counted Commit replies {committed}/{quorum}; additional Commit decisions may be durable despite missing replies; local application WAL has not started; retained for recovery",
                        range.id, command.command_id,
                    ),
                });
            }
            // Preserve the legacy classification separately. Native missing
            // Commit replies above never establish a prepare-only rollback.
            self.abort_prepared(&range, &voters, &command, authority);
            return Err(range_write_error(format!(
                "range {} write {} failed decision quorum: {committed}/{quorum}; provisional decisions were aborted",
                range.id, command.command_id
            )));
        }

        // The leader persists the quorum certificate before any replica is
        // allowed to apply. From this point onward the command must be
        // recovered/applied and may not be overtaken by a later command.
        let local_certificate = self.local_service.lock().certify_range_write_with_mutation(
            &range,
            &self.local_node_id,
            &command,
            authority,
        );
        if let Err(error) = local_certificate {
            if native_database.is_some() {
                self.recovered_epochs.lock().remove(&range.id);
                return Err(BicDbError::CommitOutcomeUnknown {
                    sequence: None, transaction_id: Some(command.transaction_id), incarnation:self.local_incarnation.clone(),
                    detail:format!("range {} command {} reached a decision quorum, but local certificate persistence is uncertain: {error}; local row apply has not started", range.id,command.command_id),
                });
            }
            return Err(error);
        }
        reservation.preserve_for_recovery(&command.command_id);
        let mut certified_voters = 1usize;
        let mut certified_nodes = BTreeSet::from([self.local_node_id.clone()]);
        for voter in &committed_nodes {
            let certified = self
                .transport
                .certify_range_write(voter, &command)
                .ok()
                .filter(|ack| {
                    self.validate_ack(voter, &command, ack).is_ok()
                        && matches!(
                            ack.state,
                            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
                        )
                });
            if certified.is_some() {
                certified_voters += 1;
                certified_nodes.insert(voter.clone());
                if native_database.is_none() {
                    let _ = self.transport.apply_range_write(voter, &command);
                }
            }
        }
        let native = if let (Some(database), Some(plan)) = (native_database, native_plan) {
            let certificate = plan.certify(&certified_nodes).ok();
            let pending = match database.prepare_native_range_command(
                &plan,
                command.transaction_id,
                authority,
            ) {
                Ok(pending) => pending,
                Err(error) => {
                    self.recovered_epochs.lock().remove(&range.id);
                    let mut active = self.active_ranges.lock();
                    if active.get(&range.id) == Some(&command.command_id) {
                        active.remove(&range.id);
                        self.active_range_changed.notify_all();
                    }
                    return Err(BicDbError::CommitOutcomeUnknown {
                        sequence:None,transaction_id:Some(command.transaction_id),incarnation:self.local_incarnation.clone(),
                        detail:format!("range {} command {} is irrevocably certified but native pending persistence failed before local row apply: {error}; recovery is required",range.id,command.command_id),
                    });
                }
            };
            // A native Apply instruction is never emitted by an early ACK.
            // Actual distinct certificate quorum must already be established.
            if let Some(certificate) = &certificate {
                let budget = CancellationToken::uncancelable().child_with_deadline(
                    std::time::Instant::now()
                        + std::time::Duration::from_millis(
                            crate::distribution_native_apply::NATIVE_ORDINARY_BUDGET_MS,
                        ),
                );
                for voter in certified_nodes
                    .iter()
                    .filter(|node| **node != self.local_node_id)
                {
                    // The actual QQ exists before any authenticated native
                    // instruction. A lost materialization reply is not rollback.
                    let _ =
                        self.transport
                            .apply_native_origin(voter, &command, certificate, &budget);
                }
            }
            Some(NativeRangeAcknowledgementEvidence {
                pending,
                mutations_sha256: plan.mutations_sha256().to_string(),
                certificate,
            })
        } else {
            None
        };
        // An irrevocable certificate already exists. Continue through the
        // ordinary local WAL/receipt boundary even if certificate replies were
        // lost; the callback must report uncertainty rather than a rollback.
        self.pending_acknowledgements.lock().insert(
            command.command_id.clone(),
            RangeAcknowledgementEvidence {
                range_id: command.range_id,
                transaction_id: command.transaction_id,
                certified: certified_voters,
                required_quorum: quorum,
                mutation,
                native,
            },
        );
        Ok(CommitAdmissionTicket {
            authority: format!("range-quorum-v{RANGE_WRITE_PROTOCOL_VERSION}:{}", range.id),
            command_id: command.command_id,
        })
    }

    fn finish_local_application(
        &self,
        ticket: &CommitAdmissionTicket,
        commit_seq: u64,
        native_context: Option<(&crate::db::NativeWalApplyReceipt, &crate::BicDb)>,
    ) -> Result<()> {
        if !ticket
            .authority
            .starts_with(&format!("range-quorum-v{RANGE_WRITE_PROTOCOL_VERSION}:"))
        {
            return Err(range_write_error("commit ticket authority mismatch"));
        }
        let evidence = self
            .pending_acknowledgements
            .lock()
            .remove(&ticket.command_id)
            .ok_or_else(|| {
                range_write_error("commit ticket has no pending certificate evidence")
            })?;
        if ticket.authority
            != format!(
                "range-quorum-v{RANGE_WRITE_PROTOCOL_VERSION}:{}",
                evidence.range_id
            )
        {
            return Err(range_write_error("commit ticket range authority mismatch"));
        }
        let authority = &RangeMutationAuthority::Mutation(&evidence.mutation);
        let mut problems = Vec::new();
        let mut actual_native_apply_verified = !self.native_image_mode;
        if let Some(native) = &evidence.native {
            if let Some((receipt, database)) = native_context {
                let current_range =
                    { self.topology.read().range_by_id(evidence.range_id).cloned() };
                let validation = current_range
                    .ok_or_else(|| range_write_error("native callback range disappeared"))
                    .and_then(|range| self.require_native_writer_host_context(database,&range))
                    .and_then(|()| database.validate_native_wal_apply_receipt(receipt)).and_then(|()| {
                    if database.incarnation_id() != self.local_incarnation
                        || receipt.transaction_id() != evidence.transaction_id
                        || receipt.mutations_sha256() != native.mutations_sha256
                    {
                        return Err(range_write_error("native callback actual database/transaction/mutations differ from admission"));
                    }
                    Ok(())
                });
                match validation {
                    Err(error) => {
                        problems.push(format!("native actual-WAL binding failed: {error}"))
                    }
                    Ok(()) => {
                        actual_native_apply_verified = true;
                        let completion = database.record_native_range_materialized(&native.pending, receipt, authority)
                            .and_then(|()| {
                                let certificate = native.certificate.as_ref().ok_or_else(|| range_write_error("native prefix has no actual certificate quorum; materialized pending remains unresolved"))?;
                                database.complete_native_range_command(&native.pending, certificate, receipt, authority)
                            });
                        if let Err(error) = completion {
                            problems.push(format!("native prefix completion failed: {error}"));
                        }
                    }
                }
            } else {
                problems.push("native callback requires the actual borrowed DB and private WAL apply receipt; a sequence is insufficient".to_string());
            }
        } else if self.native_image_mode {
            problems.push("native callback has no admitted native command journal".to_string());
        }
        if actual_native_apply_verified {
            if let Err(error) = self
                .local_service
                .lock()
                .mark_range_write_applied_with_mutation(&ticket.command_id, authority)
            {
                problems.push(format!("local range applied checkpoint failed: {error}"));
            }
        }
        if evidence.certified < evidence.required_quorum {
            problems.push(format!(
                "durable range certificate acknowledgments reached only {}/{} voters",
                evidence.certified, evidence.required_quorum
            ));
        }
        if let Some((_, database)) = native_context {
            let current_range = { self.topology.read().range_by_id(evidence.range_id).cloned() };
            let validation = current_range
                .ok_or_else(|| range_write_error("native callback range disappeared"))
                .and_then(|range| self.require_native_writer_host_context(database, &range));
            if let Err(error) = validation {
                problems.push(format!("native host policy changed before ACK: {error}"));
            }
        }
        let uncertainty = (!problems.is_empty()).then(|| problems.join("; "));
        if uncertainty.is_some() {
            // Local WAL is durable and the range decision is irrevocable. Do
            // not claim rollback or let another command overtake recovery.
            // Releasing this completed local callback's reservation lets the
            // background supervisor repair the certified tail; the invalidated
            // epoch refuses all later admissions until that repair succeeds.
            self.recovered_epochs.lock().remove(&evidence.range_id);
        }
        let mut active = self.active_ranges.lock();
        if active.get(&evidence.range_id) != Some(&ticket.command_id) {
            return Err(range_write_error(
                "commit ticket is not the active range command",
            ));
        }
        active.remove(&evidence.range_id);
        self.active_range_changed.notify_all();
        drop(active);
        if let Some(detail) = uncertainty {
            let local_detail = if actual_native_apply_verified {
                "local application is durable"
            } else {
                "local application requires independent verification"
            };
            return Err(BicDbError::CommitOutcomeUnknown {
                sequence: Some(commit_seq),
                transaction_id: Some(evidence.transaction_id),
                incarnation: self.local_incarnation.clone(),
                detail: format!("range {} command {}: {detail}; {local_detail}, cluster recovery/verification is still required before treating this as acknowledged", evidence.range_id, ticket.command_id),
            });
        }
        Ok(())
    }
}

impl<Transport: RangeWriteTransport> CommitAdmission for RangeWriteCoordinator<Transport> {
    fn admit(&self, intent: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
        self.admit_with_database(intent, None, &mut None)
    }
    fn admit_with_native_context(
        &self,
        intent: &CommitAdmissionIntent,
        database: &crate::BicDb,
    ) -> Result<CommitAdmissionTicket> {
        self.admit_with_database(intent, Some(database), &mut None)
    }
    fn admit_native_with_rejection(
        &self,
        intent: &CommitAdmissionIntent,
        database: &crate::BicDb,
    ) -> Result<crate::NativeCommitAdmissionResult> {
        let mut rejected = None;
        match self.admit_with_database(intent, Some(database), &mut rejected) {
            Ok(ticket) => Ok(crate::NativeCommitAdmissionResult::Accepted(ticket)),
            Err(error) => {
                match rejected {
                    Some(rejection) => {
                        Ok(crate::NativeCommitAdmissionResult::PrepareOnlyRejected {
                            rejection,
                            error,
                        })
                    }
                    None => Err(error),
                }
            }
        }
    }
    fn native_prepare_only_cleaned(
        &self,
        rejected: NativePrepareOnlyRejection,
        cleaned: &crate::NativeTransactionNoEffect,
        database: &crate::BicDb,
    ) -> Result<()> {
        self.local_service
            .lock()
            .finish_native_prepare_only_rejection(database, rejected, cleaned)
    }
    fn local_applied(&self, ticket: &CommitAdmissionTicket, commit_seq: u64) -> Result<()> {
        self.finish_local_application(ticket, commit_seq, None)
    }
    fn local_applied_with_native_context(
        &self,
        ticket: &CommitAdmissionTicket,
        receipt: &crate::db::NativeWalApplyReceipt,
        database: &crate::BicDb,
    ) -> Result<()> {
        self.finish_local_application(ticket, receipt.sequence(), Some((receipt, database)))
    }
    fn release_native_application_for_recovery(
        &self,
        ticket: &CommitAdmissionTicket,
    ) -> Result<()> {
        if !self.native_image_mode {
            return Ok(());
        }
        let evidence = {
            let mut pending = self.pending_acknowledgements.lock();
            let Some(evidence) = pending.get(&ticket.command_id) else {
                return Ok(());
            };
            if ticket.authority
                != format!(
                    "range-quorum-v{RANGE_WRITE_PROTOCOL_VERSION}:{}",
                    evidence.range_id
                )
            {
                return Err(range_write_error(
                    "native recovery release ticket range mismatch",
                ));
            }
            pending
                .remove(&ticket.command_id)
                .expect("checked pending native ticket")
        };
        self.recovered_epochs.lock().remove(&evidence.range_id);
        {
            let mut active = self.active_ranges.lock();
            if active.get(&evidence.range_id) == Some(&ticket.command_id) {
                active.remove(&evidence.range_id);
                self.active_range_changed.notify_all();
            }
        }
        // Drop owned local mutation/scope leases only after the caller has
        // completed actual local cleanup. Durable CQ/pending remain untouched.
        drop(evidence);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterNode, DbConfig, DistributionConfig, DistributionStore, RangeReplica, Record,
        ReplicaId,
    };
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    pub(super) struct TestTransport {
        pub(super) available: bool,
    }

    impl RangeWriteTransport for TestTransport {
        fn range_write_progress(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
        ) -> Result<RangeWriteProgress> {
            if !self.available {
                return Err(range_write_error("injected unavailable voter"));
            }
            Ok(RangeWriteProgress {
                node_id: destination.clone(),
                range_id,
                current_epoch: range_epoch,
                last_index: 0,
                resolved_through: 0,
                compacted_through: 0,
            })
        }

        fn apply_range_write_repair(
            &self,
            destination: &ClusterNodeId,
            batch: &RangeWriteRepairBatch,
            limits: &RangeWriteRepairLimits,
        ) -> Result<RangeWriteProgress> {
            if !self.available {
                return Err(range_write_error("injected unavailable voter"));
            }
            batch.validate(limits)?;
            let resolved = batch
                .entries
                .last()
                .map(|entry| entry.command.index)
                .unwrap_or(batch.previous_resolved_index);
            Ok(RangeWriteProgress {
                node_id: destination.clone(),
                range_id: batch.range_id,
                current_epoch: batch.range_epoch,
                last_index: resolved,
                resolved_through: resolved,
                compacted_through: 0,
            })
        }

        fn fetch_range_write_repair(
            &self,
            _source: &ClusterNodeId,
            _range_id: RangeId,
            _range_epoch: u64,
            _previous_resolved_index: u64,
            _limits: &RangeWriteRepairLimits,
        ) -> Result<RangeWriteRepairBatch> {
            Err(range_write_error(
                "stateless test transport has no repair source",
            ))
        }

        fn probe_range_write(
            &self,
            source: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            index: u64,
        ) -> Result<RangeWriteProbe> {
            if !self.available {
                return Err(range_write_error("injected unavailable voter"));
            }
            Ok(RangeWriteProbe {
                node_id: source.clone(),
                range_id,
                route_epoch: range_epoch,
                index,
                progress: RangeWriteProgress {
                    node_id: source.clone(),
                    range_id,
                    current_epoch: range_epoch,
                    last_index: 0,
                    resolved_through: 0,
                    compacted_through: 0,
                },
                entry: None,
            })
        }

        fn prepare_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            if !self.available {
                return Err(range_write_error("injected unavailable voter"));
            }
            Ok(RangeWriteAck {
                node_id: destination.clone(),
                range_id: command.range_id,
                range_epoch: command.range_epoch,
                index: command.index,
                state: RangeWriteState::Prepared,
            })
        }

        fn commit_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            if !self.available {
                return Err(range_write_error("injected unavailable voter"));
            }
            Ok(RangeWriteAck {
                node_id: destination.clone(),
                range_id: command.range_id,
                range_epoch: command.range_epoch,
                index: command.index,
                state: RangeWriteState::Committed,
            })
        }

        fn certify_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            Ok(RangeWriteAck {
                node_id: destination.clone(),
                range_id: command.range_id,
                range_epoch: command.range_epoch,
                index: command.index,
                state: RangeWriteState::QuorumCommitted,
            })
        }

        fn apply_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            Ok(RangeWriteAck {
                node_id: destination.clone(),
                range_id: command.range_id,
                range_epoch: command.range_epoch,
                index: command.index,
                state: RangeWriteState::Applied,
            })
        }

        fn abort_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            Ok(RangeWriteAck {
                node_id: destination.clone(),
                range_id: command.range_id,
                range_epoch: command.range_epoch,
                index: command.index,
                state: RangeWriteState::Aborted,
            })
        }
    }

    #[derive(Debug)]
    struct ServiceTransport {
        available: AtomicBool,
        commit_available: AtomicBool,
        caller: ClusterNodeId,
        topology: Arc<RwLock<ClusterTopology>>,
        nodes: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
    }

    impl ServiceTransport {
        fn service(
            &self,
            destination: &ClusterNodeId,
        ) -> Result<&Arc<Mutex<ClusterDataNodeService>>> {
            if !self.available.load(Ordering::SeqCst) {
                return Err(range_write_error("injected unavailable voter"));
            }
            self.nodes
                .get(destination)
                .ok_or_else(|| range_write_error(format!("unknown test destination {destination}")))
        }

        fn range(&self, range_id: RangeId, range_epoch: u64) -> Result<RangeDescriptor> {
            self.topology
                .read()
                .range_by_id(range_id)
                .filter(|range| range.epoch == range_epoch)
                .cloned()
                .ok_or_else(|| range_write_error("unknown test range or epoch"))
        }
    }

    impl RangeWriteTransport for ServiceTransport {
        fn install_range_backup_fence(
            &self,
            destination: &ClusterNodeId,
            plan_id: Uuid,
            range_id: RangeId,
            range_epoch: u64,
            installed_at_ms: u64,
            expires_at_ms: u64,
        ) -> Result<RangeWriteProgress> {
            let range = self.range(range_id, range_epoch)?;
            self.service(destination)?
                .lock()
                .install_range_backup_fence(
                    &range,
                    &self.caller,
                    plan_id,
                    installed_at_ms,
                    expires_at_ms,
                )
        }

        fn release_range_backup_fence(
            &self,
            destination: &ClusterNodeId,
            plan_id: Uuid,
            range_id: RangeId,
            range_epoch: u64,
        ) -> Result<bool> {
            let range = self.range(range_id, range_epoch)?;
            self.service(destination)?
                .lock()
                .release_range_backup_fence(&range, &self.caller, plan_id)
        }

        fn range_write_progress(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
        ) -> Result<RangeWriteProgress> {
            let range = self.range(range_id, range_epoch)?;
            self.service(destination)?
                .lock()
                .range_write_progress_for_peer(&range, &self.caller)
        }

        fn apply_range_write_repair(
            &self,
            destination: &ClusterNodeId,
            batch: &RangeWriteRepairBatch,
            limits: &RangeWriteRepairLimits,
        ) -> Result<RangeWriteProgress> {
            let range = self.range(batch.range_id, batch.range_epoch)?;
            self.service(destination)?.lock().apply_range_write_repair(
                &range,
                &self.caller,
                batch,
                limits,
            )
        }

        fn fetch_range_write_repair(
            &self,
            source: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            previous_resolved_index: u64,
            limits: &RangeWriteRepairLimits,
        ) -> Result<RangeWriteRepairBatch> {
            let range = self.range(range_id, range_epoch)?;
            self.service(source)?.lock().export_range_write_repair(
                &range,
                &self.caller,
                previous_resolved_index,
                limits,
            )
        }

        fn probe_range_write(
            &self,
            source: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            index: u64,
        ) -> Result<RangeWriteProbe> {
            let range = self.range(range_id, range_epoch)?;
            self.service(source)?
                .lock()
                .probe_range_write_for_peer(&range, &self.caller, index)
        }

        fn prepare_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(destination)?.lock().prepare_range_write(
                &range,
                &self.caller,
                command.clone(),
            )
        }

        fn commit_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            if !self.commit_available.load(Ordering::SeqCst) {
                return Err(range_write_error("injected decision replication failure"));
            }
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(destination)?
                .lock()
                .commit_range_write(&range, &self.caller, command)
        }

        fn certify_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(destination)?
                .lock()
                .certify_range_write(&range, &self.caller, command)
        }

        fn apply_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(destination)?
                .lock()
                .apply_certified_range_write(&range, &self.caller, command)
        }

        fn abort_range_write(
            &self,
            destination: &ClusterNodeId,
            command: &RangeWriteCommand,
        ) -> Result<RangeWriteAck> {
            let range = self.range(command.range_id, command.range_epoch)?;
            self.service(destination)?
                .lock()
                .abort_range_write(&range, &self.caller, command)
        }
    }

    fn topology(replication_factor: u8) -> ClusterTopology {
        let directory = tempfile::tempdir().unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("range-hook-test").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "n1.invalid:9444".to_string(),
            node_capacity_bytes: 1_000_000,
            replication_factor,
            initial_ranges: 4,
            ..DistributionConfig::default()
        };
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        let mut topology = store.topology().clone();
        if replication_factor == 3 {
            for (offset, id) in [(0_u64, "n2"), (1, "n3")] {
                let node_id = ClusterNodeId::new(id).unwrap();
                topology.nodes.insert(
                    node_id.clone(),
                    ClusterNode::new(
                        node_id.clone(),
                        format!("{id}.invalid:9444"),
                        1,
                        1_000_000,
                        1,
                    )
                    .unwrap(),
                );
                for range in topology.ranges.values_mut() {
                    let replica_id = ReplicaId::new(topology.next_replica_id + offset).unwrap();
                    topology.next_replica_id += 2;
                    range.replicas.push(RangeReplica {
                        id: replica_id,
                        node_id: node_id.clone(),
                        role: RangeReplicaRole::Voter,
                    });
                }
            }
        }
        topology.validate().unwrap();
        topology
    }

    fn open_range_node(
        root: &Path,
        cluster_id: &ClusterId,
        node_id: &ClusterNodeId,
    ) -> (
        Arc<RwLock<crate::BicDb>>,
        Arc<Mutex<ClusterDataNodeService>>,
    ) {
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(root, DbConfig::default().with_fsync(false)).unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                node_id.clone(),
                root,
                Arc::clone(&db),
                false,
            )
            .unwrap(),
        ));
        (db, service)
    }

    fn seed_applied_command(
        service: &Arc<Mutex<ClusterDataNodeService>>,
        range: &RangeDescriptor,
        leader: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) {
        seed_command_state(service, range, leader, command, RangeWriteState::Applied);
    }

    fn seed_command_state(
        service: &Arc<Mutex<ClusterDataNodeService>>,
        range: &RangeDescriptor,
        leader: &ClusterNodeId,
        command: &RangeWriteCommand,
        state: RangeWriteState,
    ) {
        let mut service = service.lock();
        service
            .prepare_range_write(range, leader, command.clone())
            .unwrap();
        if matches!(
            state,
            RangeWriteState::Committed
                | RangeWriteState::QuorumCommitted
                | RangeWriteState::Applied
        ) {
            service.commit_range_write(range, leader, command).unwrap();
        }
        if matches!(
            state,
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
        ) {
            service.certify_range_write(range, leader, command).unwrap();
        }
        match state {
            RangeWriteState::Applied => {
                service
                    .apply_certified_range_write(range, leader, command)
                    .unwrap();
            }
            RangeWriteState::Aborted => {
                service.abort_range_write(range, leader, command).unwrap();
            }
            RangeWriteState::Prepared
            | RangeWriteState::Committed
            | RangeWriteState::QuorumCommitted => {}
        }
    }

    fn coordinated_database(
        topology: ClusterTopology,
        transport_available: bool,
    ) -> (tempfile::TempDir, Arc<RwLock<crate::BicDb>>) {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                directory.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_required_commit_admission(true),
            )
            .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let cluster_id = topology.cluster_id.clone();
        let local = ClusterNodeId::new("n1").unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                local.clone(),
                directory.path(),
                Arc::clone(&db),
                false,
            )
            .unwrap(),
        ));
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                local,
                topology,
                service,
                Arc::new(TestTransport {
                    available: transport_available,
                }),
            )
            .unwrap(),
        );
        if transport_available {
            coordinator.recover_local_leadership(1_024).unwrap();
        }
        db.read().install_commit_admission(coordinator);
        (directory, db)
    }

    #[test]
    fn strict_schema_fence_closes_writes_until_fresh_digest_is_advertised() {
        let directory = tempfile::tempdir().unwrap();
        let mut topology = topology(1);
        let cluster_id = topology.cluster_id.clone();
        let local = ClusterNodeId::new("n1").unwrap();
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                directory.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_required_commit_admission(true),
            )
            .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                local.clone(),
                directory.path(),
                Arc::clone(&db),
                false,
            )
            .unwrap(),
        ));
        let initial = db.read().schema_compatibility_fingerprint().unwrap();
        topology.nodes.get_mut(&local).unwrap().labels.insert(
            crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
            initial.sha256,
        );
        let topology = Arc::new(RwLock::new(topology));
        let coordinator = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                cluster_id,
                local.clone(),
                Arc::clone(&topology),
                service,
                Arc::new(TestTransport { available: true }),
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1_024).unwrap();
        db.read().install_commit_admission(coordinator);
        db.write()
            .insert("items", crate::Record::new("first"))
            .unwrap();

        db.write().create_collection("new_schema_object").unwrap();
        let error = db
            .write()
            .insert("items", crate::Record::new("fenced"))
            .unwrap_err();
        assert!(error.to_string().contains("stale or missing"), "{error}");

        let refreshed = db.read().schema_compatibility_fingerprint().unwrap();
        topology
            .write()
            .nodes
            .get_mut(&local)
            .unwrap()
            .labels
            .insert(
                crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                refreshed.sha256,
            );
        db.write()
            .insert("items", crate::Record::new("after-refresh"))
            .unwrap();
    }

    #[test]
    fn signed_additive_compatibility_window_keeps_production_writes_available() {
        let directory = tempfile::tempdir().unwrap();
        let mut topology = topology(1);
        let cluster_id = topology.cluster_id.clone();
        let local = ClusterNodeId::new("n1").unwrap();
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                directory.path().join("data"),
                DbConfig::default()
                    .with_fsync(false)
                    .with_required_commit_admission(true),
            )
            .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let base = db.read().schema_compatibility_fingerprint().unwrap();
        topology.nodes.get_mut(&local).unwrap().labels.insert(
            crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
            base.sha256.clone(),
        );

        let mut desired = crate::BicDb::open_with_config(
            directory.path().join("desired"),
            DbConfig::default().with_fsync(false),
        )
        .unwrap();
        desired.create_collection("items").unwrap();
        desired.create_collection("new_items").unwrap();
        desired
            .create_index(crate::IndexDefinition {
                nulls_not_distinct: false,
                name: "items_kind".to_string(),
                collection: "items".to_string(),
                fields: vec![crate::IndexField::MetadataPath(vec!["kind".to_string()])],
                unique: false,
                kind: crate::IndexKind::BTree,
                predicate: None,
                exclusion: None,
            })
            .unwrap();
        let bundle = desired.cluster_schema_bundle().unwrap();
        let signing_key = SigningKey::from_bytes(&[41; 32]);
        let signed = crate::SignedClusterSchemaBundle {
            format_version: crate::SIGNED_CLUSTER_SCHEMA_BUNDLE_FORMAT_VERSION,
            signer_key_id: "release-key".to_string(),
            signature_ed25519_hex: hex::encode(
                signing_key
                    .sign(&bundle.signing_message().unwrap())
                    .to_bytes(),
            ),
            bundle,
        };
        let trusted = BTreeMap::from([(
            "release-key".to_string(),
            signing_key.verifying_key().to_bytes(),
        )]);
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open_with_schema_trust(
                cluster_id.clone(),
                local.clone(),
                directory.path().join("data"),
                Arc::clone(&db),
                trusted,
                crate::ClusterSchemaStageLimits::default(),
                false,
            )
            .unwrap(),
        ));
        let stage = service
            .lock()
            .stage_signed_schema_bundle(signed.clone(), 10)
            .unwrap();
        topology
            .open_schema_compatibility_window(
                &BTreeSet::from([local.clone()]),
                &base.sha256,
                &signed.bundle.fingerprint.sha256,
                local.clone(),
                11,
            )
            .unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let coordinator = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                cluster_id,
                local.clone(),
                Arc::clone(&topology),
                Arc::clone(&service),
                Arc::new(TestTransport { available: true }),
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1_024).unwrap();
        db.read().install_commit_admission(coordinator);
        db.write()
            .insert(
                "items",
                crate::Record::new("before-activation").with_metadata(json!({"kind": "existing"})),
            )
            .unwrap();

        let rollout_id = Uuid::now_v7();
        let mut activation = None;
        for now_ms in 12..20 {
            let step = service
                .lock()
                .advance_signed_schema_activation(
                    rollout_id,
                    stage.stage_id,
                    &signed.bundle.fingerprint.sha256,
                    now_ms,
                )
                .unwrap();
            db.write()
                .insert(
                    "items",
                    crate::Record::new(format!("during-window-{now_ms}"))
                        .with_metadata(json!({"kind": "concurrent"})),
                )
                .unwrap();
            let complete = step.complete();
            activation = Some(step);
            if complete {
                break;
            }
        }
        let activation = activation.unwrap();
        assert!(activation.complete());
        assert_eq!(
            topology.read().nodes[&local]
                .labels
                .get(crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL),
            Some(&base.sha256),
            "activation must not promote the active topology digest"
        );
        assert!(!db
            .read()
            .lookup_index("items_kind", &[crate::IndexValue::from("concurrent")])
            .unwrap()
            .is_empty());

        topology
            .write()
            .promote_schema_compatibility_window(
                &BTreeSet::from([local.clone()]),
                &base.sha256,
                &signed.bundle.fingerprint.sha256,
                local,
                13,
            )
            .unwrap();
        db.write()
            .insert("items", crate::Record::new("after-promotion"))
            .unwrap();
    }

    fn command(index: u64, epoch: u64) -> RangeWriteCommand {
        let cluster_id = ClusterId::new("range-log-test").unwrap();
        let range_id = RangeId::new(1).unwrap();
        let mut command = RangeWriteCommand {
            protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
            cluster_id,
            range_id,
            range_epoch: epoch,
            index,
            command_id: format!("command-{epoch}-{index}"),
            leader_node_id: ClusterNodeId::new("n1").unwrap(),
            transaction_id: index,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: format!("item-{index}"),
                record: Some(
                    Record::new(format!("item-{index}")).with_metadata(json!({"index": index})),
                ),
            }],
            checksum_sha256: String::new(),
        };
        command.checksum_sha256 = command.calculate_checksum().unwrap();
        command
    }

    fn handoff_request() -> RangeHandoffPromise {
        RangeHandoffPromise {
            session_id: Uuid::new_v4(),
            relocation_id: crate::RelocationId::new(1).unwrap(),
            plan_id: "actual-relocation-plan".into(),
            range_id: RangeId::new(1).unwrap(),
            old_epoch: 1,
            activation_epoch: 3,
            old_config_sha256: "a".repeat(64),
            new_config_sha256: "b".repeat(64),
            nonce: Uuid::new_v4(),
            resolved_through: u64::MAX,
        }
    }

    #[test]
    fn handoff_promise_survives_restart_and_blocks_old_intermediate_and_future_prepares() {
        let directory = tempfile::tempdir().unwrap();
        crate::format::enable_range_image_install(directory.path()).unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let mut store =
            RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), true).unwrap();
        let first = command(1, 1);
        store.prepare(first.clone()).unwrap();
        store.commit(&first).unwrap();
        store.certify(&first).unwrap();
        store.mark_applied(&first).unwrap();
        let request = handoff_request();
        let promise = store.install_handoff_promise(&request).unwrap();
        assert_eq!(
            promise.resolved_through, 1,
            "request cannot invent a local watermark"
        );
        assert_eq!(store.install_handoff_promise(&request).unwrap(), promise);
        drop(store);
        let mut store =
            RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), true).unwrap();
        assert_eq!(store.catalog.format_version, HANDOFF_RANGE_LOG_FORMAT);
        for epoch in [1, 2, 3, 999] {
            assert!(store
                .prepare(command(2, epoch))
                .unwrap_err()
                .to_string()
                .contains("handoff"));
        }
        assert_eq!(store.install_handoff_promise(&request).unwrap(), promise);
        let mut conflicting = request.clone();
        conflicting.nonce = Uuid::new_v4();
        assert!(store.install_handoff_promise(&conflicting).is_err());
        assert!(RangeWriteStore::open(directory.path(), cluster, node, false).is_err());
        let mut other = command(1, 1);
        other.range_id = RangeId::new(2).unwrap();
        other.checksum_sha256 = other.calculate_checksum().unwrap();
        store.prepare(other).unwrap();
    }

    #[test]
    fn handoff_refuses_provisional_or_certified_unapplied_tail_and_unfenced_format() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            true,
        )
        .unwrap();
        let request = handoff_request();
        assert!(store.install_handoff_promise(&request).is_err());
        crate::format::enable_range_image_install(directory.path()).unwrap();
        let first = command(1, 1);
        store.prepare(first.clone()).unwrap();
        assert!(store.install_handoff_promise(&request).is_err());
        store.commit(&first).unwrap();
        store.certify(&first).unwrap();
        assert!(store.install_handoff_promise(&request).is_err());
        assert!(store.catalog.handoff_promises.is_empty());
        store.mark_applied(&first).unwrap();
        store.install_handoff_promise(&request).unwrap();
    }

    #[test]
    fn handoff_failed_persistence_blocks_locally_without_issuing_quorum_evidence() {
        let directory = tempfile::tempdir().unwrap();
        crate::format::enable_range_image_install(directory.path()).unwrap();
        let mut store = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            true,
        )
        .unwrap();
        let first = command(1, 1);
        store.prepare(first.clone()).unwrap();
        store.abort(&first).unwrap();
        let before = store.catalog.clone();
        let log_path = directory.path().join(DEFAULT_RANGE_WRITE_LOG);
        let saved = directory.path().join("retained-before-interruption.json");
        std::fs::rename(&log_path, &saved).unwrap();
        std::fs::create_dir(&log_path).unwrap();
        let request = handoff_request();
        assert!(store.install_handoff_promise(&request).is_err());
        assert!(store
            .catalog
            .handoff_promises
            .contains_key(&request.range_id));
        assert!(store.prepare(command(2, 1)).is_err());
        std::fs::remove_dir(&log_path).unwrap();
        std::fs::rename(saved, &log_path).unwrap();
        drop(store);
        let reopened = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(reopened.catalog, before);
    }

    #[test]
    fn handoff_postrename_directory_sync_failure_survives_reopen_and_cannot_ack() {
        use std::cell::Cell;
        use std::io::Write;

        // This reproduces the actual replacement ordering up to the fault:
        // synced new file -> atomic rename -> failed directory fsync. It is not
        // a power-loss simulation and does not claim the unsynced rename would
        // survive one; its outcome is deliberately treated as uncertain.
        fn replace_then_fail_directory_sync(path: &Path, bytes: &[u8], fsync: bool) -> Result<()> {
            assert!(fsync, "handoff evidence requires the durable writer");
            let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, path)?;
            assert_eq!(
                fs::read(path)?,
                bytes,
                "new catalog is visible after rename"
            );
            Err(std::io::Error::other("injected post-rename directory fsync failure").into())
        }

        let directory = tempfile::tempdir().unwrap();
        crate::format::enable_range_image_install(directory.path()).unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let mut store =
            RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), true).unwrap();
        let first = command(1, 1);
        store.prepare(first.clone()).unwrap();
        store.abort(&first).unwrap();
        let request = handoff_request();
        let persistence_calls = Cell::new(0);
        let failed = store.install_handoff_promise_with(&request, |store| {
            persistence_calls.set(persistence_calls.get() + 1);
            store.persist_with(replace_then_fail_directory_sync)
        });
        assert!(failed.unwrap_err().to_string().contains("post-rename"));
        assert_eq!(persistence_calls.get(), 1);
        let expected = store.catalog.handoff_promises[&request.range_id].clone();
        assert_eq!(expected.resolved_through, 1);
        assert!(store.prepare(command(2, 1)).is_err());
        drop(store);

        let mut reopened =
            RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), true).unwrap();
        assert_eq!(reopened.catalog.format_version, HANDOFF_RANGE_LOG_FORMAT);
        assert_eq!(
            reopened.catalog.handoff_promises[&request.range_id],
            expected
        );
        for epoch in [1, 2, 3, 999] {
            assert!(reopened.prepare(command(2, epoch)).is_err());
        }

        // A matching in-memory/on-disk promise is not by itself permission to
        // return quorum evidence: every idempotent retry must re-persist it.
        let retry = reopened.install_handoff_promise_with(&request, |store| {
            persistence_calls.set(persistence_calls.get() + 1);
            store.persist_with(replace_then_fail_directory_sync)
        });
        assert!(retry.unwrap_err().to_string().contains("post-rename"));
        assert_eq!(
            persistence_calls.get(),
            2,
            "retry really reached persistence"
        );
        assert!(reopened.prepare(command(2, 1)).is_err());
        drop(reopened);
        let mut reopened =
            RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), true).unwrap();
        assert_eq!(
            reopened.catalog.handoff_promises[&request.range_id],
            expected
        );

        let synced = Cell::new(false);
        let evidence = reopened
            .install_handoff_promise_with(&request, |store| {
                persistence_calls.set(persistence_calls.get() + 1);
                store.persist_with(|path, bytes, fsync| {
                    assert!(fsync);
                    crate::storage::write_atomic(path, bytes, fsync)?;
                    synced.set(true);
                    Ok(())
                })
            })
            .unwrap();
        assert!(synced.get(), "proof follows successful durable replacement");
        assert_eq!(persistence_calls.get(), 3);
        assert_eq!(evidence, expected);
        drop(reopened);
        let mut reopened = RangeWriteStore::open(directory.path(), cluster, node, true).unwrap();
        assert_eq!(
            reopened.catalog.handoff_promises[&request.range_id],
            expected
        );
        assert!(reopened.prepare(command(2, 3)).is_err());
    }

    #[test]
    fn durable_log_recovers_committed_command_and_idempotent_retries() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let first = command(1, 1);
        {
            let mut store =
                RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), false)
                    .unwrap();
            assert_eq!(
                store.prepare(first.clone()).unwrap(),
                RangeWriteState::Prepared
            );
            assert_eq!(
                store.prepare(first.clone()).unwrap(),
                RangeWriteState::Prepared
            );
            assert_eq!(store.commit(&first).unwrap(), RangeWriteState::Committed);
            assert_eq!(
                store.certify(&first).unwrap(),
                RangeWriteState::QuorumCommitted
            );
        }
        let mut reopened = RangeWriteStore::open(directory.path(), cluster, node, false).unwrap();
        assert_eq!(reopened.committed_not_applied(), vec![first.clone()]);
        assert_eq!(
            reopened.commit(&first).unwrap(),
            RangeWriteState::QuorumCommitted
        );
        reopened.mark_applied(&first).unwrap();
        assert!(reopened.committed_not_applied().is_empty());
    }

    #[test]
    fn backup_fence_survives_restart_blocks_new_prepares_and_has_one_owner() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let range_id = RangeId::new(1).unwrap();
        let first = command(1, 1);
        let second = command(2, 1);
        let plan_id = Uuid::now_v7();
        let installed_at_ms = 10_000_u64;
        let expires_at_ms = installed_at_ms.saturating_add(60_000);
        {
            let mut store =
                RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), false)
                    .unwrap();
            store.prepare(first.clone()).unwrap();
            store.commit(&first).unwrap();
            store.certify(&first).unwrap();
            store.mark_applied(&first).unwrap();
            let progress = store
                .install_backup_fence(plan_id, range_id, 1, installed_at_ms, expires_at_ms)
                .unwrap();
            assert_eq!(progress.last_index, 1);
            assert_eq!(progress.resolved_through, 1);
            assert_eq!(
                store.prepare(first.clone()).unwrap(),
                RangeWriteState::Applied
            );
            let error = store.prepare(second.clone()).unwrap_err();
            assert!(error.to_string().contains("fenced at resolved index 1"));
        }

        let inspection =
            RangeWriteStore::inspect(directory.path(), cluster.clone(), node.clone(), 1024 * 1024)
                .unwrap();
        assert_eq!(inspection.format_version, RANGE_WRITE_LOG_FORMAT_VERSION);
        assert_eq!(inspection.ranges.len(), 1);
        assert_eq!(inspection.ranges[0].resolved_through, 1);
        assert_eq!(inspection.backup_fences.len(), 1);
        assert_eq!(inspection.backup_fences[0].plan_id, plan_id);
        assert!(
            RangeWriteStore::inspect(directory.path(), cluster.clone(), node.clone(), 1,).is_err()
        );

        let mut reopened = RangeWriteStore::open(directory.path(), cluster, node, false).unwrap();
        assert_eq!(reopened.backup_fences().len(), 1);
        assert!(reopened.prepare(second.clone()).is_err());
        assert!(reopened
            .release_backup_fence(Uuid::now_v7(), range_id, 1)
            .is_err());
        assert!(reopened.release_backup_fence(plan_id, range_id, 1).unwrap());
        assert_eq!(reopened.prepare(second).unwrap(), RangeWriteState::Prepared);
    }

    #[test]
    fn backup_fence_rejects_unresolved_tails_and_expiration_reopens_writes() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            false,
        )
        .unwrap();
        let first = command(1, 1);
        store.prepare(first.clone()).unwrap();
        assert!(store
            .install_backup_fence(Uuid::now_v7(), first.range_id, 1, 1, 2)
            .is_err());
        store.abort(&first).unwrap();
        store
            .install_backup_fence(Uuid::now_v7(), first.range_id, 1, 1, 2)
            .unwrap();
        assert_eq!(store.release_expired_backup_fences(2).unwrap(), 1);
        assert_eq!(
            store.prepare(command(2, 1)).unwrap(),
            RangeWriteState::Prepared
        );
    }

    #[test]
    fn provisional_decision_never_enters_crash_replay_and_remains_abortable() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let command = command(1, 1);
        {
            let mut store =
                RangeWriteStore::open(directory.path(), cluster.clone(), node.clone(), false)
                    .unwrap();
            store.prepare(command.clone()).unwrap();
            assert_eq!(store.commit(&command).unwrap(), RangeWriteState::Committed);
        }
        let mut reopened = RangeWriteStore::open(directory.path(), cluster, node, false).unwrap();
        assert!(reopened.committed_not_applied().is_empty());
        assert_eq!(reopened.abort(&command).unwrap(), RangeWriteState::Aborted);
    }

    #[test]
    fn v1_committed_log_is_atomically_migrated_to_quorum_certified_current_format() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let mut legacy = command(1, 1);
        legacy.protocol_version = 1;
        legacy.checksum_sha256 = legacy.calculate_checksum().unwrap();
        let catalog = RangeWriteCatalog {
            format_version: 1,
            native_writer_enforcement_version: None,
            cluster_id: cluster.clone(),
            node_id: node.clone(),
            ranges: BTreeMap::from([(
                legacy.range_id,
                RangeWriteLog {
                    current_epoch: 1,
                    last_index: 1,
                    resolved_through: 0,
                    compacted_through: 0,
                    entries: BTreeMap::from([(
                        1,
                        RangeWriteLogEntry {
                            command: legacy.clone(),
                            state: RangeWriteState::Committed,
                        },
                    )]),
                },
            )]),
            backup_fences: BTreeMap::new(),
            handoff_promises: BTreeMap::new(),
            image_install_intents: BTreeMap::new(),
            recovery_fences: BTreeMap::new(),
            native_image_bases: BTreeMap::new(),
            sparse_committed_decisions: None,
            native_abort_protocol: None,
        };
        let envelope = RangeWriteEnvelope::new(catalog).unwrap();
        fs::write(
            directory.path().join(DEFAULT_RANGE_WRITE_LOG),
            serde_json::to_vec_pretty(&envelope).unwrap(),
        )
        .unwrap();

        let migrated = RangeWriteStore::open(directory.path(), cluster, node, false).unwrap();
        assert_eq!(migrated.committed_not_applied(), vec![legacy]);
        let bytes = fs::read(directory.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
        let persisted: RangeWriteEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            persisted.catalog.format_version,
            RANGE_WRITE_LOG_FORMAT_VERSION
        );
        assert!(persisted.catalog.ranges.values().all(|log| log
            .entries
            .values()
            .all(|entry| entry.state != RangeWriteState::Committed)));
    }

    #[test]
    fn v2_log_without_a_fence_field_migrates_without_checksum_drift() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("range-log-test").unwrap();
        let node = ClusterNodeId::new("n1").unwrap();
        let envelope = RangeWriteEnvelope::new(RangeWriteCatalog {
            format_version: 2,
            native_writer_enforcement_version: None,
            cluster_id: cluster.clone(),
            node_id: node.clone(),
            ranges: BTreeMap::new(),
            backup_fences: BTreeMap::new(),
            handoff_promises: BTreeMap::new(),
            image_install_intents: BTreeMap::new(),
            recovery_fences: BTreeMap::new(),
            native_image_bases: BTreeMap::new(),
            sparse_committed_decisions: None,
            native_abort_protocol: None,
        })
        .unwrap();
        let legacy_bytes = serde_json::to_vec_pretty(&envelope).unwrap();
        assert!(!String::from_utf8_lossy(&legacy_bytes).contains("backup_fences"));
        fs::write(directory.path().join(DEFAULT_RANGE_WRITE_LOG), legacy_bytes).unwrap();

        let migrated = RangeWriteStore::open(directory.path(), cluster, node, false).unwrap();
        assert!(migrated.backup_fences().is_empty());
        let persisted: RangeWriteEnvelope = serde_json::from_slice(
            &fs::read(directory.path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            persisted.catalog.format_version,
            RANGE_WRITE_LOG_FORMAT_VERSION
        );
    }

    #[test]
    fn conflicting_duplicate_and_stale_epoch_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            false,
        )
        .unwrap();
        let first = command(1, 2);
        store.prepare(first.clone()).unwrap();
        let mut conflict = first.clone();
        conflict.command_id = "evil".to_string();
        conflict.checksum_sha256 = conflict.calculate_checksum().unwrap();
        assert!(store.prepare(conflict).is_err());
        assert!(store.prepare(command(2, 1)).is_err());
    }

    #[test]
    fn checksum_tampering_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut store = RangeWriteStore::open(
            directory.path(),
            ClusterId::new("range-log-test").unwrap(),
            ClusterNodeId::new("n1").unwrap(),
            false,
        )
        .unwrap();
        let mut tampered = command(1, 1);
        tampered.mutations[0].record_id = "other".to_string();
        assert!(store.prepare(tampered).is_err());
    }

    #[test]
    fn required_authority_fails_closed_before_mutating_rows() {
        let directory = tempfile::tempdir().unwrap();
        let mut db = crate::BicDb::open_with_config(
            directory.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_required_commit_admission(true),
        )
        .unwrap();
        db.create_collection("items").unwrap();
        assert!(db.insert("items", Record::new("blocked")).is_err());
        assert!(db.get("items", "blocked").unwrap().is_none());
    }

    #[test]
    fn single_voter_command_crosses_durable_hook_and_is_checkpointed_applied() {
        let topology = topology(1);
        let cluster_id = topology.cluster_id.clone();
        let (_directory, db) = coordinated_database(topology, true);
        db.write()
            .insert(
                "items",
                Record::new("accepted").with_metadata(json!({"value": 1})),
            )
            .unwrap();
        assert_eq!(
            db.read()
                .get("items", "accepted")
                .unwrap()
                .unwrap()
                .metadata["value"],
            1
        );
        let store = RangeWriteStore::open(
            _directory.path(),
            cluster_id,
            ClusterNodeId::new("n1").unwrap(),
            false,
        )
        .unwrap();
        assert!(store.committed_not_applied().is_empty());
    }

    #[test]
    fn range_image_gate_keeps_owned_leader_permit_through_actual_wal_callback() {
        let topology = topology(1);
        let range_id = topology
            .range_for_key("items", "owned-until-wal-callback")
            .unwrap()
            .id;
        let other_range = topology
            .ranges
            .values()
            .find(|range| range.id != range_id)
            .unwrap()
            .id;
        let (_directory, db) = coordinated_database(topology, true);
        let (gate, log, sequence, mut tx) = {
            let database = db.read();
            let gate = database.range_mutation_gate();
            let mut tx = database.begin_transaction().unwrap();
            tx.insert("items", Record::new("owned-until-wal-callback"))
                .unwrap();
            let sequence = database.commit_buffered_transaction(&mut tx).unwrap();
            (gate, database.tx_log_handle(), sequence, tx)
        };
        assert!(gate.try_freeze(range_id, Uuid::now_v7()).is_err());
        let other_frozen = gate.try_freeze(other_range, Uuid::now_v7()).unwrap();
        log.write_durable(sequence).unwrap();
        assert!(gate.try_freeze(range_id, Uuid::now_v7()).is_err());
        tx.finalize_commit_admission().unwrap();
        let frozen = gate.try_freeze(range_id, Uuid::now_v7()).unwrap();
        assert!(db
            .read()
            .get("items", "owned-until-wal-callback")
            .unwrap()
            .is_some());
        drop(frozen);
        drop(other_frozen);
    }

    #[test]
    fn three_voter_write_does_not_apply_without_majority() {
        let (_directory, db) = coordinated_database(topology(3), false);
        let error = db
            .write()
            .insert("items", Record::new("minority"))
            .unwrap_err();
        assert!(error.to_string().contains("leader recovery"));
        assert!(db.read().get("items", "minority").unwrap().is_none());
    }

    #[test]
    fn committed_decision_replays_after_restart_before_local_apply() {
        let directory = tempfile::tempdir().unwrap();
        let topology = topology(1);
        let cluster_id = topology.cluster_id.clone();
        let local = ClusterNodeId::new("n1").unwrap();
        let range = topology
            .range_for_key("items", "recovered")
            .unwrap()
            .clone();
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(directory.path(), DbConfig::default().with_fsync(false))
                .unwrap(),
        ));
        db.write().create_collection("items").unwrap();
        let intent = CommitAdmissionIntent {
            transaction_id: 99,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: "recovered".to_string(),
                record: Some(
                    Record::new("recovered").with_metadata(json!({"source": "range-log"})),
                ),
            }],
        };
        let command =
            RangeWriteCommand::new(cluster_id.clone(), &range, 1, local.clone(), &intent).unwrap();
        {
            let mut service = ClusterDataNodeService::open(
                cluster_id.clone(),
                local.clone(),
                directory.path(),
                Arc::clone(&db),
                false,
            )
            .unwrap();
            service
                .prepare_range_write(&range, &local, command.clone())
                .unwrap();
            service
                .commit_range_write_decision(&range, &local, &command)
                .unwrap();
            service
                .certify_range_write(&range, &local, &command)
                .unwrap();
        }
        assert!(db.read().get("items", "recovered").unwrap().is_none());
        let _recovered_service = ClusterDataNodeService::open(
            cluster_id,
            local,
            directory.path(),
            Arc::clone(&db),
            false,
        )
        .unwrap();
        assert_eq!(
            db.read()
                .get("items", "recovered")
                .unwrap()
                .unwrap()
                .metadata["source"],
            "range-log"
        );
    }

    #[test]
    fn bounded_resolved_suffix_repairs_a_follower_and_rejects_tampering() {
        let leader_root = tempfile::tempdir().unwrap();
        let follower_root = tempfile::tempdir().unwrap();
        let topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let leader_id = ClusterNodeId::new("n1").unwrap();
        let follower_id = ClusterNodeId::new("n2").unwrap();
        let range = topology.ranges.values().next().unwrap().clone();
        let mut current_range = range.clone();
        let record_ids = (0..100_000)
            .map(|number| format!("repair-{number}"))
            .filter(|record_id| {
                range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(record_ids.len(), 3);

        let leader_db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                leader_root.path(),
                DbConfig::default().with_fsync(false),
            )
            .unwrap(),
        ));
        let follower_db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                follower_root.path(),
                DbConfig::default().with_fsync(false),
            )
            .unwrap(),
        ));
        leader_db.write().create_collection("items").unwrap();
        follower_db.write().create_collection("items").unwrap();
        let mut leader = ClusterDataNodeService::open(
            cluster_id.clone(),
            leader_id.clone(),
            leader_root.path(),
            Arc::clone(&leader_db),
            false,
        )
        .unwrap();
        let mut follower = ClusterDataNodeService::open(
            cluster_id,
            follower_id,
            follower_root.path(),
            Arc::clone(&follower_db),
            false,
        )
        .unwrap();

        for (offset, record_id) in record_ids.iter().enumerate() {
            if offset == 2 {
                current_range.epoch = current_range.epoch.saturating_add(1);
            }
            let intent = CommitAdmissionIntent {
                transaction_id: offset as u64 + 1,
                mutations: vec![CommitAdmissionMutation {
                    collection: "items".to_string(),
                    record_id: record_id.clone(),
                    record: Some(
                        Record::new(record_id).with_metadata(json!({"repair_index": offset + 1})),
                    ),
                }],
            };
            let command = RangeWriteCommand::new(
                leader.cluster_id().clone(),
                &current_range,
                offset as u64 + 1,
                leader_id.clone(),
                &intent,
            )
            .unwrap();
            leader
                .prepare_range_write(&current_range, &leader_id, command.clone())
                .unwrap();
            if offset == 1 {
                leader
                    .abort_range_write(&current_range, &leader_id, &command)
                    .unwrap();
            } else {
                leader
                    .commit_range_write_decision(&current_range, &leader_id, &command)
                    .unwrap();
                leader
                    .certify_range_write(&current_range, &leader_id, &command)
                    .unwrap();
                {
                    let database = leader_db.read();
                    let gate = database.range_mutation_gate();
                    let permit = gate.try_enter(command.range_id).unwrap();
                    database
                        .apply_admitted_mutations_with_mutation(
                            command.range_id,
                            &command.mutations,
                            &RangeMutationAuthority::Mutation(&permit),
                        )
                        .unwrap();
                }
                leader
                    .mark_range_write_applied(&command.command_id)
                    .unwrap();
            }
        }

        let limits = RangeWriteRepairLimits {
            max_commands_per_batch: 1,
            max_bytes_per_batch: 1024 * 1024,
            max_batches_per_admission: 4,
        };
        let first = leader
            .export_range_write_repair(&current_range, &leader_id, 0, &limits)
            .unwrap();
        assert_eq!(first.entries.len(), 1);
        let mut tampered = first.clone();
        tampered.entries[0].state = RangeWriteState::Aborted;
        assert!(tampered.validate(&limits).is_err());

        let first_progress = follower
            .apply_range_write_repair(&current_range, &leader_id, &first, &limits)
            .unwrap();
        assert_eq!(first_progress.resolved_through, 1);
        // An ambiguous response may replay the same exact bounded batch.
        assert_eq!(
            follower
                .apply_range_write_repair(&current_range, &leader_id, &first, &limits)
                .unwrap()
                .resolved_through,
            1
        );
        let second = leader
            .export_range_write_repair(&current_range, &leader_id, 1, &limits)
            .unwrap();
        assert_eq!(second.entries[0].state, RangeWriteState::Aborted);
        follower
            .apply_range_write_repair(&current_range, &leader_id, &second, &limits)
            .unwrap();
        let third = leader
            .export_range_write_repair(&current_range, &leader_id, 2, &limits)
            .unwrap();
        assert!(third.entries[0].command.range_epoch > first.entries[0].command.range_epoch);
        let progress = follower
            .apply_range_write_repair(&current_range, &leader_id, &third, &limits)
            .unwrap();
        assert_eq!(progress.resolved_through, 3);
        assert!(follower_db
            .read()
            .get("items", &record_ids[0])
            .unwrap()
            .is_some());
        assert!(follower_db
            .read()
            .get("items", &record_ids[1])
            .unwrap()
            .is_none());
        assert!(follower_db
            .read()
            .get("items", &record_ids[2])
            .unwrap()
            .is_some());
    }

    #[test]
    fn coordinator_repairs_missed_resolved_commands_before_next_prepare() {
        let leader_root = tempfile::tempdir().unwrap();
        let follower_two_root = tempfile::tempdir().unwrap();
        let follower_three_root = tempfile::tempdir().unwrap();
        let topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let leader_id = ClusterNodeId::new("n1").unwrap();
        let follower_two_id = ClusterNodeId::new("n2").unwrap();
        let follower_three_id = ClusterNodeId::new("n3").unwrap();
        let range = topology.ranges.values().next().unwrap().clone();
        let record_ids = (0..100_000)
            .map(|number| format!("coordinator-repair-{number}"))
            .filter(|record_id| {
                range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(record_ids.len(), 3);

        let open_db = |root: &Path| {
            let db = Arc::new(RwLock::new(
                crate::BicDb::open_with_config(
                    root,
                    DbConfig::default()
                        .with_fsync(false)
                        .with_required_commit_admission(true),
                )
                .unwrap(),
            ));
            db.write().create_collection("items").unwrap();
            db
        };
        let leader_db = open_db(leader_root.path());
        let follower_two_db = open_db(follower_two_root.path());
        let follower_three_db = open_db(follower_three_root.path());
        let leader_service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                leader_id.clone(),
                leader_root.path(),
                Arc::clone(&leader_db),
                false,
            )
            .unwrap(),
        ));
        let follower_two_service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                follower_two_id.clone(),
                follower_two_root.path(),
                Arc::clone(&follower_two_db),
                false,
            )
            .unwrap(),
        ));
        let follower_three_service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                cluster_id.clone(),
                follower_three_id.clone(),
                follower_three_root.path(),
                Arc::clone(&follower_three_db),
                false,
            )
            .unwrap(),
        ));
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(false),
            commit_available: AtomicBool::new(true),
            caller: leader_id.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([
                (follower_two_id.clone(), follower_two_service),
                (follower_three_id.clone(), follower_three_service),
            ]),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                leader_id,
                topology,
                Arc::clone(&leader_service),
                Arc::clone(&transport),
            )
            .unwrap(),
        );
        leader_db
            .read()
            .install_commit_admission(coordinator.clone());

        let unavailable = leader_db
            .write()
            .insert("items", Record::new(&record_ids[0]))
            .unwrap_err();
        assert!(unavailable.to_string().contains("leader recovery"));
        assert_eq!(
            leader_service
                .lock()
                .range_write_progress(range.id)
                .resolved_through,
            0
        );

        transport.available.store(true, Ordering::SeqCst);
        coordinator.recover_local_leadership(1_024).unwrap();
        transport.commit_available.store(false, Ordering::SeqCst);
        let (provisional, legacy_outcome) = {
            let db = leader_db.write();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", Record::new(&record_ids[1])).unwrap();
            let observer = tx.observe_outcome();
            (tx.commit().unwrap_err(), observer)
        };
        assert!(provisional.to_string().contains("decision quorum"));
        assert!(!matches!(
            provisional,
            BicDbError::CommitOutcomeUnknown { .. }
        ));
        assert_eq!(legacy_outcome.outcome(), crate::TransactionOutcome::Aborted);
        for follower in [&follower_two_db, &follower_three_db] {
            assert!(follower
                .read()
                .get("items", &record_ids[1])
                .unwrap()
                .is_none());
        }

        transport.commit_available.store(true, Ordering::SeqCst);
        leader_db
            .write()
            .insert(
                "items",
                Record::new(&record_ids[2]).with_metadata(json!({"repaired_first": true})),
            )
            .unwrap();
        for follower in [&follower_two_db, &follower_three_db] {
            assert_eq!(
                follower
                    .read()
                    .get("items", &record_ids[2])
                    .unwrap()
                    .unwrap()
                    .metadata["repaired_first"],
                true
            );
        }
        for service in transport.nodes.values() {
            assert_eq!(
                service
                    .lock()
                    .range_write_progress(range.id)
                    .resolved_through,
                2
            );
        }
        leader_db.read().clear_commit_admission();
    }

    #[test]
    fn new_leader_recovers_a_quorum_resolved_prefix_before_assigning_an_index() {
        let node_one_root = tempfile::tempdir().unwrap();
        let node_two_root = tempfile::tempdir().unwrap();
        let node_three_root = tempfile::tempdir().unwrap();
        let mut topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let node_one = ClusterNodeId::new("n1").unwrap();
        let node_two = ClusterNodeId::new("n2").unwrap();
        let node_three = ClusterNodeId::new("n3").unwrap();
        let old_range = topology.ranges.values().next().unwrap().clone();
        let record_ids = (0..100_000)
            .map(|number| format!("leader-recovery-{number}"))
            .filter(|record_id| {
                old_range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(2)
            .collect::<Vec<_>>();
        let (node_one_db, node_one_service) =
            open_range_node(node_one_root.path(), &cluster_id, &node_one);
        let (node_two_db, node_two_service) =
            open_range_node(node_two_root.path(), &cluster_id, &node_two);
        let (node_three_db, node_three_service) =
            open_range_node(node_three_root.path(), &cluster_id, &node_three);
        let intent = CommitAdmissionIntent {
            transaction_id: 1,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: record_ids[0].clone(),
                record: Some(
                    Record::new(&record_ids[0]).with_metadata(json!({"old_leader": true})),
                ),
            }],
        };
        let historical =
            RangeWriteCommand::new(cluster_id.clone(), &old_range, 1, node_one.clone(), &intent)
                .unwrap();
        seed_applied_command(&node_one_service, &old_range, &node_one, &historical);
        seed_applied_command(&node_two_service, &old_range, &node_one, &historical);
        // A replica may become leader only after its physical range snapshot is
        // caught up. Deliberately leave its command log empty so this test
        // proves promotion recovers log/index authority without replaying the
        // physical row through the admission lock.
        node_three_db
            .write()
            .insert(
                "items",
                Record::new(&record_ids[0]).with_metadata(json!({"old_leader": true})),
            )
            .unwrap();
        assert_eq!(
            node_three_service
                .lock()
                .range_write_progress(old_range.id)
                .resolved_through,
            0
        );

        let range_id = old_range.id;
        {
            let range = topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.leader = node_three.clone();
            range.epoch = range.epoch.saturating_add(1);
        }
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: node_three.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([
                (node_one.clone(), Arc::clone(&node_one_service)),
                (node_two.clone(), Arc::clone(&node_two_service)),
            ]),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                node_three,
                topology,
                Arc::clone(&node_three_service),
                transport,
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1_024).unwrap();
        node_three_db.read().install_commit_admission(coordinator);
        node_three_db
            .write()
            .insert(
                "items",
                Record::new(&record_ids[1]).with_metadata(json!({"new_leader": true})),
            )
            .unwrap();
        assert_eq!(
            node_three_db
                .read()
                .get("items", &record_ids[0])
                .unwrap()
                .unwrap()
                .metadata["old_leader"],
            true
        );
        for db in [&node_one_db, &node_two_db, &node_three_db] {
            assert_eq!(
                db.read()
                    .get("items", &record_ids[1])
                    .unwrap()
                    .unwrap()
                    .metadata["new_leader"],
                true
            );
        }
        assert_eq!(
            node_three_service
                .lock()
                .range_write_progress(range_id)
                .resolved_through,
            2
        );
        node_three_db.read().clear_commit_admission();
    }

    #[test]
    fn new_leader_adopts_and_applies_a_quorum_provisional_tail() {
        let node_one_root = tempfile::tempdir().unwrap();
        let node_two_root = tempfile::tempdir().unwrap();
        let node_three_root = tempfile::tempdir().unwrap();
        let mut topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let node_one = ClusterNodeId::new("n1").unwrap();
        let node_two = ClusterNodeId::new("n2").unwrap();
        let node_three = ClusterNodeId::new("n3").unwrap();
        let old_range = topology.ranges.values().next().unwrap().clone();
        let record_ids = (0..100_000)
            .map(|number| format!("provisional-tail-{number}"))
            .filter(|record_id| {
                old_range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(2)
            .collect::<Vec<_>>();
        let (node_one_db, node_one_service) =
            open_range_node(node_one_root.path(), &cluster_id, &node_one);
        let (node_two_db, node_two_service) =
            open_range_node(node_two_root.path(), &cluster_id, &node_two);
        let (node_three_db, node_three_service) =
            open_range_node(node_three_root.path(), &cluster_id, &node_three);
        let intent = CommitAdmissionIntent {
            transaction_id: 1,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: record_ids[0].clone(),
                record: Some(
                    Record::new(&record_ids[0]).with_metadata(json!({"recovered_tail": true})),
                ),
            }],
        };
        let provisional =
            RangeWriteCommand::new(cluster_id.clone(), &old_range, 1, node_one.clone(), &intent)
                .unwrap();
        for service in [&node_one_service, &node_two_service] {
            seed_command_state(
                service,
                &old_range,
                &node_one,
                &provisional,
                RangeWriteState::Committed,
            );
        }
        for db in [&node_one_db, &node_two_db, &node_three_db] {
            assert!(db.read().get("items", &record_ids[0]).unwrap().is_none());
        }

        let range_id = old_range.id;
        {
            let range = topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.leader = node_three.clone();
            range.epoch = range.epoch.saturating_add(1);
        }
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: node_three.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([
                (node_one.clone(), Arc::clone(&node_one_service)),
                (node_two.clone(), Arc::clone(&node_two_service)),
            ]),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                node_three,
                topology,
                Arc::clone(&node_three_service),
                transport,
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1_024).unwrap();
        for db in [&node_one_db, &node_two_db, &node_three_db] {
            assert_eq!(
                db.read()
                    .get("items", &record_ids[0])
                    .unwrap()
                    .unwrap()
                    .metadata["recovered_tail"],
                true
            );
        }
        node_three_db
            .read()
            .install_commit_admission(coordinator.clone());
        node_three_db
            .write()
            .insert(
                "items",
                Record::new(&record_ids[1]).with_metadata(json!({"after_recovery": true})),
            )
            .unwrap();
        assert_eq!(
            node_three_service
                .lock()
                .range_write_progress(range_id)
                .resolved_through,
            2
        );
        node_three_db.read().clear_commit_admission();
    }

    #[test]
    fn new_leader_aborts_an_abandoned_prepared_tail_before_reusing_the_range() {
        let node_one_root = tempfile::tempdir().unwrap();
        let node_two_root = tempfile::tempdir().unwrap();
        let node_three_root = tempfile::tempdir().unwrap();
        let mut topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let node_one = ClusterNodeId::new("n1").unwrap();
        let node_two = ClusterNodeId::new("n2").unwrap();
        let node_three = ClusterNodeId::new("n3").unwrap();
        let old_range = topology.ranges.values().next().unwrap().clone();
        let record_ids = (0..100_000)
            .map(|number| format!("abandoned-tail-{number}"))
            .filter(|record_id| {
                old_range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(2)
            .collect::<Vec<_>>();
        let (node_one_db, node_one_service) =
            open_range_node(node_one_root.path(), &cluster_id, &node_one);
        let (node_two_db, node_two_service) =
            open_range_node(node_two_root.path(), &cluster_id, &node_two);
        let (node_three_db, node_three_service) =
            open_range_node(node_three_root.path(), &cluster_id, &node_three);
        let intent = CommitAdmissionIntent {
            transaction_id: 1,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: record_ids[0].clone(),
                record: Some(Record::new(&record_ids[0])),
            }],
        };
        let abandoned =
            RangeWriteCommand::new(cluster_id.clone(), &old_range, 1, node_one.clone(), &intent)
                .unwrap();
        seed_command_state(
            &node_one_service,
            &old_range,
            &node_one,
            &abandoned,
            RangeWriteState::Prepared,
        );

        let range_id = old_range.id;
        {
            let range = topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.leader = node_three.clone();
            range.epoch = range.epoch.saturating_add(1);
        }
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: node_three.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([
                (node_one.clone(), Arc::clone(&node_one_service)),
                (node_two.clone(), Arc::clone(&node_two_service)),
            ]),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                node_three,
                topology,
                Arc::clone(&node_three_service),
                transport,
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1_024).unwrap();
        for service in [&node_one_service, &node_two_service, &node_three_service] {
            assert_eq!(
                service
                    .lock()
                    .range_write_progress(range_id)
                    .resolved_through,
                1
            );
        }
        for db in [&node_one_db, &node_two_db, &node_three_db] {
            assert!(db.read().get("items", &record_ids[0]).unwrap().is_none());
        }
        node_three_db
            .read()
            .install_commit_admission(coordinator.clone());
        node_three_db
            .write()
            .insert("items", Record::new(&record_ids[1]))
            .unwrap();
        assert_eq!(
            node_three_service
                .lock()
                .range_write_progress(range_id)
                .resolved_through,
            2
        );
        node_three_db.read().clear_commit_admission();
    }

    #[test]
    fn new_leader_fails_closed_on_an_ambiguous_provisional_tail() {
        let node_one_root = tempfile::tempdir().unwrap();
        let node_three_root = tempfile::tempdir().unwrap();
        let mut topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let node_one = ClusterNodeId::new("n1").unwrap();
        let node_three = ClusterNodeId::new("n3").unwrap();
        let old_range = topology.ranges.values().next().unwrap().clone();
        let record_id = (0..100_000)
            .map(|number| format!("ambiguous-tail-{number}"))
            .find(|record_id| {
                old_range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .unwrap();
        let (_node_one_db, node_one_service) =
            open_range_node(node_one_root.path(), &cluster_id, &node_one);
        let (_node_three_db, node_three_service) =
            open_range_node(node_three_root.path(), &cluster_id, &node_three);
        let intent = CommitAdmissionIntent {
            transaction_id: 1,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: record_id.clone(),
                record: Some(Record::new(&record_id)),
            }],
        };
        let provisional =
            RangeWriteCommand::new(cluster_id.clone(), &old_range, 1, node_one.clone(), &intent)
                .unwrap();
        seed_command_state(
            &node_one_service,
            &old_range,
            &node_one,
            &provisional,
            RangeWriteState::Committed,
        );

        let range_id = old_range.id;
        {
            let range = topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.leader = node_three.clone();
            range.epoch = range.epoch.saturating_add(1);
        }
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: node_three.clone(),
            topology: Arc::clone(&topology),
            // n2 is intentionally unreachable. n1's provisional decision plus
            // that unknown voter could still have formed the old quorum.
            nodes: BTreeMap::from([(node_one, node_one_service)]),
        });
        let coordinator = RangeWriteCoordinator::new(
            cluster_id,
            node_three,
            topology,
            node_three_service,
            transport,
        )
        .unwrap();
        let error = coordinator.recover_local_leadership(1_024).unwrap_err();
        assert!(error.to_string().contains("ambiguous provisional decision"));
    }

    #[test]
    fn new_leader_rejects_divergent_quorum_resolved_histories() {
        let node_one_root = tempfile::tempdir().unwrap();
        let node_two_root = tempfile::tempdir().unwrap();
        let node_three_root = tempfile::tempdir().unwrap();
        let mut topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let node_one = ClusterNodeId::new("n1").unwrap();
        let node_two = ClusterNodeId::new("n2").unwrap();
        let node_three = ClusterNodeId::new("n3").unwrap();
        let old_range = topology.ranges.values().next().unwrap().clone();
        let record_ids = (0..100_000)
            .map(|number| format!("divergence-{number}"))
            .filter(|record_id| {
                old_range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .take(3)
            .collect::<Vec<_>>();
        let (_node_one_db, node_one_service) =
            open_range_node(node_one_root.path(), &cluster_id, &node_one);
        let (_node_two_db, node_two_service) =
            open_range_node(node_two_root.path(), &cluster_id, &node_two);
        let (node_three_db, node_three_service) =
            open_range_node(node_three_root.path(), &cluster_id, &node_three);
        for (service, record_id) in [
            (&node_one_service, &record_ids[0]),
            (&node_two_service, &record_ids[1]),
        ] {
            let intent = CommitAdmissionIntent {
                transaction_id: 1,
                mutations: vec![CommitAdmissionMutation {
                    collection: "items".to_string(),
                    record_id: record_id.clone(),
                    record: Some(Record::new(record_id)),
                }],
            };
            let command = RangeWriteCommand::new(
                cluster_id.clone(),
                &old_range,
                1,
                node_one.clone(),
                &intent,
            )
            .unwrap();
            seed_applied_command(service, &old_range, &node_one, &command);
        }

        let range_id = old_range.id;
        {
            let range = topology
                .ranges
                .values_mut()
                .find(|range| range.id == range_id)
                .unwrap();
            range.leader = node_three.clone();
            range.epoch = range.epoch.saturating_add(1);
        }
        topology.generation = topology.generation.saturating_add(1);
        topology.validate().unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: node_three.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([(node_one, node_one_service), (node_two, node_two_service)]),
        });
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                node_three,
                topology,
                node_three_service,
                transport,
            )
            .unwrap(),
        );
        let error = coordinator.recover_local_leadership(1_024).unwrap_err();
        assert!(error
            .to_string()
            .contains("divergent range-write histories"));
        assert!(node_three_db
            .read()
            .get("items", &record_ids[2])
            .unwrap()
            .is_none());
        node_three_db.read().clear_commit_admission();
    }

    #[test]
    fn coordinator_installs_and_releases_an_exact_distributed_backup_fence() {
        let topology = topology(3);
        let cluster_id = topology.cluster_id.clone();
        let leader_id = ClusterNodeId::new("n1").unwrap();
        let follower_two_id = ClusterNodeId::new("n2").unwrap();
        let follower_three_id = ClusterNodeId::new("n3").unwrap();
        let range = topology.ranges.values().next().unwrap().clone();
        let leader_root = tempfile::tempdir().unwrap();
        let follower_two_root = tempfile::tempdir().unwrap();
        let follower_three_root = tempfile::tempdir().unwrap();
        let (_leader_db, leader_service) =
            open_range_node(leader_root.path(), &cluster_id, &leader_id);
        let (_follower_two_db, follower_two_service) =
            open_range_node(follower_two_root.path(), &cluster_id, &follower_two_id);
        let (_follower_three_db, follower_three_service) =
            open_range_node(follower_three_root.path(), &cluster_id, &follower_three_id);
        let metadata = crate::MetadataConsensusStatus {
            cluster_id: cluster_id.clone(),
            node_id: leader_id.clone(),
            role: crate::MetadataConsensusRole::Leader,
            current_term: 1,
            voted_for: Some(leader_id.clone()),
            leader_id: Some(leader_id.clone()),
            commit_index: 0,
            last_log_index: 0,
            last_log_term: 0,
            topology_generation: topology.generation,
            voters: topology.nodes.keys().cloned().collect(),
            learners: Vec::new(),
        };
        let installed_at_ms = 10_000_u64;
        let plan = crate::ClusterBackupPlan::create(
            &topology,
            &metadata,
            None,
            installed_at_ms,
            60_000,
            &crate::ClusterBackupLimits::default(),
        )
        .unwrap();
        let topology = Arc::new(RwLock::new(topology));
        let transport = Arc::new(ServiceTransport {
            available: AtomicBool::new(true),
            commit_available: AtomicBool::new(true),
            caller: leader_id.clone(),
            topology: Arc::clone(&topology),
            nodes: BTreeMap::from([
                (follower_two_id, Arc::clone(&follower_two_service)),
                (follower_three_id, Arc::clone(&follower_three_service)),
            ]),
        });
        let coordinator = RangeWriteCoordinator::new(
            cluster_id.clone(),
            leader_id.clone(),
            Arc::clone(&topology),
            Arc::clone(&leader_service),
            Arc::clone(&transport),
        )
        .unwrap();
        coordinator.recover_local_leadership(1_024).unwrap();
        let session = coordinator
            .begin_backup_fence_session(&plan, installed_at_ms)
            .unwrap();
        let fence = coordinator
            .fence_range_for_backup(&session, range.id, installed_at_ms)
            .unwrap();
        assert_eq!(fence.required_quorum, 2);
        assert_eq!(fence.observations.len(), 2);
        assert!(fence
            .observations
            .iter()
            .all(|progress| progress.last_index == 0 && progress.resolved_through == 0));
        for service in [&leader_service, &follower_two_service] {
            assert_eq!(service.lock().range_backup_fences().len(), 1);
        }
        assert!(follower_three_service
            .lock()
            .range_backup_fences()
            .is_empty());

        let record_id = (0..100_000)
            .map(|number| format!("backup-fenced-{number}"))
            .find(|record_id| {
                range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .unwrap();
        let intent = CommitAdmissionIntent {
            transaction_id: 1,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".to_string(),
                record_id: record_id.clone(),
                record: Some(Record::new(&record_id)),
            }],
        };
        let command =
            RangeWriteCommand::new(cluster_id, &range, 1, leader_id.clone(), &intent).unwrap();
        let error = leader_service
            .lock()
            .prepare_range_write(&range, &leader_id, command.clone())
            .unwrap_err();
        assert!(error.to_string().contains("fenced at resolved index 0"));

        let mut forged_release = fence.clone();
        forged_release.observations[0].last_index = 1;
        assert!(coordinator
            .release_range_backup_fence(&forged_release)
            .is_err());
        assert_eq!(leader_service.lock().range_backup_fences().len(), 1);
        assert_eq!(coordinator.release_range_backup_fence(&fence).unwrap(), 2);
        for service in [
            &leader_service,
            &follower_two_service,
            &follower_three_service,
        ] {
            assert!(service.lock().range_backup_fences().is_empty());
        }
        leader_service
            .lock()
            .prepare_range_write(&range, &leader_id, command.clone())
            .unwrap();
        leader_service
            .lock()
            .abort_range_write(&range, &leader_id, &command)
            .unwrap();

        transport.available.store(false, Ordering::SeqCst);
        let other_range = topology
            .read()
            .ranges
            .values()
            .find(|candidate| candidate.id != range.id)
            .unwrap()
            .clone();
        let mut stale_plan = plan.clone();
        stale_plan.topology_generation = stale_plan.topology_generation.saturating_add(1);
        stale_plan.checksum_sha256 = stale_plan.calculate_checksum().unwrap();
        assert!(coordinator
            .begin_backup_fence_session(&stale_plan, installed_at_ms)
            .is_err());
        assert!(leader_service.lock().range_backup_fences().is_empty());
        let error = coordinator
            .fence_range_for_backup(&session, other_range.id, installed_at_ms)
            .unwrap_err();
        assert!(error.to_string().contains("repaired only 1/2 voters"));
        assert!(leader_service.lock().range_backup_fences().is_empty());
    }
    #[test]
    fn waiting_writer_rechecks_invalidated_readiness_after_reservation() {
        let root = tempfile::tempdir().unwrap();
        let topology = topology(1);
        let cluster_id = topology.cluster_id.clone();
        let range = topology.ranges.values().next().unwrap().clone();
        let (database, service) = open_range_node(root.path(), &cluster_id, &range.leader);
        let coordinator = Arc::new(
            RangeWriteCoordinator::new(
                cluster_id,
                range.leader.clone(),
                Arc::new(RwLock::new(topology)),
                service,
                Arc::new(TestTransport { available: false }),
            )
            .unwrap(),
        );
        coordinator.recover_local_leadership(1).unwrap();
        let reservation = coordinator.reserve_range(range.id).unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = Arc::clone(&coordinator);
        let waiting_range = range.clone();
        let waiter = std::thread::spawn(move || {
            worker.require_recovered_epoch(&waiting_range).unwrap();
            sender.send(()).unwrap();
            worker
                .reserve_recovered_range(&waiting_range)
                .err()
                .map(|error| error.to_string())
        });
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        coordinator.recovered_epochs.lock().remove(&range.id);
        drop(reservation);
        assert!(waiter
            .join()
            .unwrap()
            .unwrap()
            .contains("recovery is not complete"));
        assert!(coordinator.active_ranges.lock().is_empty());
        assert_eq!(coordinator.recover_local_leadership(1).unwrap(), 1);
        drop(database);
    }
}
