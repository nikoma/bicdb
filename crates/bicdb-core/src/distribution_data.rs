//! Durable destination-side state for bounded range relocation.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, RangeDescriptor, RangeId, RangeRelocation, RangeSnapshotBatch,
    RangeSnapshotOptions, RelocationId,
};
use crate::distribution_anti_entropy::{
    load_range_digest_state, range_digest_bucket_for_record, save_range_digest_state,
    RangeDigestBucketScanStep, RangeDigestLimits, RangeDigestManifest, RangeDigestState,
};
use crate::distribution_anti_entropy_repair::{
    load_range_digest_repair_state, save_range_digest_repair_state, RangeDigestRepairBatch,
    RangeDigestRepairLimits, RangeDigestRepairState,
};
use crate::distribution_range_consensus::{
    RangeBackupWriteFence, RangeWriteAck, RangeWriteCommand, RangeWriteProbe, RangeWriteProgress,
    RangeWriteRepairBatch, RangeWriteRepairLimits, RangeWriteState, RangeWriteStore,
};
use crate::distribution_supervisor::{
    CatchUpProgress, CleanupProgress, ClusterRelocationTransport, SnapshotCopyProgress,
};
use crate::error::{BicDbError, Result};
use crate::record::CollectionMeta;
use crate::replication::CommitFrame;
use crate::{
    BicDb, CancellationToken, ClusterSchemaActivationLimits, ClusterSchemaActivationPhase,
    ClusterSchemaBundle, ClusterSchemaCompatibilityWindow, ClusterSchemaStageLimits,
    SchemaCompatibilityAuthority, SchemaCompatibilityFingerprint, SignedClusterSchemaBundle,
};

pub const RANGE_LEARNER_APPLY_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_RANGE_LEARNER_APPLY_STATE: &str = "cluster-range-learner-apply.json";
pub const DEFAULT_RANGE_DIGEST_STATE_DIR: &str = "cluster-range-digests";
pub const DEFAULT_RANGE_DIGEST_REPAIR_STATE_DIR: &str = "cluster-range-digest-repairs";
const MAX_RETAINED_LEARNER_STATES: usize = 4_096;
const MAX_RETAINED_RANGE_DIGEST_STATES: usize = 4_096;

fn data_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeLearnerApplyState {
    pub relocation_id: RelocationId,
    pub range_id: RangeId,
    pub learner_epoch: u64,
    pub source: Option<ClusterNodeId>,
    pub target: ClusterNodeId,
    pub snapshot_id: String,
    pub snapshot_bytes_copied: u64,
    pub snapshot_resume_after_key: Option<String>,
    pub snapshot_commit_sequence: Option<u64>,
    pub snapshot_chain_sha256: String,
    pub snapshot_complete: bool,
    pub durable_source_commit_sequence: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RangeLearnerApplyCatalog {
    format_version: u32,
    cluster_id: ClusterId,
    states: BTreeMap<RelocationId, RangeLearnerApplyState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RangeLearnerApplyEnvelope {
    catalog: RangeLearnerApplyCatalog,
    checksum_sha256: String,
}

impl RangeLearnerApplyEnvelope {
    fn new(catalog: RangeLearnerApplyCatalog) -> Result<Self> {
        let checksum_sha256 = catalog_checksum(&catalog)?;
        Ok(Self {
            catalog,
            checksum_sha256,
        })
    }

    fn verify(self) -> Result<RangeLearnerApplyCatalog> {
        let actual = catalog_checksum(&self.catalog)?;
        if actual != self.checksum_sha256 {
            return Err(data_error(
                "range learner apply-state checksum mismatch; refusing recovery",
            ));
        }
        if self.catalog.format_version != RANGE_LEARNER_APPLY_FORMAT_VERSION {
            return Err(data_error(format!(
                "unsupported range learner apply format {}; expected {}",
                self.catalog.format_version, RANGE_LEARNER_APPLY_FORMAT_VERSION
            )));
        }
        Ok(self.catalog)
    }
}

fn catalog_checksum(catalog: &RangeLearnerApplyCatalog) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(catalog)?)))
}

#[derive(Debug)]
pub struct RangeLearnerStore {
    root: PathBuf,
    fsync: bool,
    catalog: RangeLearnerApplyCatalog,
}

impl RangeLearnerStore {
    pub fn open(root: impl AsRef<Path>, cluster_id: ClusterId, fsync: bool) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let path = root.join(DEFAULT_RANGE_LEARNER_APPLY_STATE);
        let catalog = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<RangeLearnerApplyEnvelope>(&bytes)?.verify()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                RangeLearnerApplyCatalog {
                    format_version: RANGE_LEARNER_APPLY_FORMAT_VERSION,
                    cluster_id: cluster_id.clone(),
                    states: BTreeMap::new(),
                }
            }
            Err(error) => return Err(error.into()),
        };
        if catalog.cluster_id != cluster_id {
            return Err(data_error(format!(
                "range learner cluster {} does not match requested {}",
                catalog.cluster_id, cluster_id
            )));
        }
        Ok(Self {
            root,
            fsync,
            catalog,
        })
    }

    pub fn state(&self, relocation_id: RelocationId) -> Option<&RangeLearnerApplyState> {
        self.catalog.states.get(&relocation_id)
    }

    pub fn prepare_learner(
        &mut self,
        relocation: &RangeRelocation,
    ) -> Result<RangeLearnerApplyState> {
        if let Some(existing) = self.catalog.states.get(&relocation.id) {
            ensure_same_relocation(existing, relocation)?;
            return Ok(existing.clone());
        }
        if self.catalog.states.len() >= MAX_RETAINED_LEARNER_STATES {
            return Err(data_error(
                "range learner apply-state limit reached; complete old relocations first",
            ));
        }
        let snapshot_id = format!(
            "{}-{}-{}",
            self.catalog.cluster_id, relocation.id, relocation.target
        );
        let state = RangeLearnerApplyState {
            relocation_id: relocation.id,
            range_id: relocation.range_id,
            learner_epoch: relocation.learner_epoch,
            source: relocation.source.clone(),
            target: relocation.target.clone(),
            snapshot_id,
            snapshot_bytes_copied: 0,
            snapshot_resume_after_key: None,
            snapshot_commit_sequence: None,
            snapshot_chain_sha256: hex::encode(Sha256::digest([])),
            snapshot_complete: false,
            durable_source_commit_sequence: None,
        };
        self.catalog.states.insert(relocation.id, state.clone());
        self.persist()?;
        Ok(state)
    }

    pub fn apply_snapshot_batch(
        &mut self,
        db: &mut BicDb,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        expected_previous_resume: Option<&str>,
        collection_meta: &CollectionMeta,
        batch: &RangeSnapshotBatch,
    ) -> Result<SnapshotCopyProgress> {
        let current = self
            .catalog
            .states
            .get(&relocation.id)
            .cloned()
            .ok_or_else(|| data_error(format!("learner {} is not prepared", relocation.id)))?;
        ensure_same_relocation(&current, relocation)?;
        if current.snapshot_complete {
            return Err(data_error(format!(
                "snapshot for {} is already complete",
                relocation.id
            )));
        }
        if expected_previous_resume != current.snapshot_resume_after_key.as_deref() {
            if Some(batch.resume_after_key.as_str()) == current.snapshot_resume_after_key.as_deref()
            {
                return Ok(incomplete_snapshot_progress(&current));
            }
            return Err(data_error(format!(
                "snapshot resume mismatch for {}: destination={:?}, request={:?}",
                relocation.id, current.snapshot_resume_after_key, expected_previous_resume
            )));
        }
        if let Some(watermark) = current.snapshot_commit_sequence {
            if batch.snapshot_commit_sequence < watermark {
                return Err(data_error(format!(
                    "snapshot watermark for {} moved backwards from {} to {}",
                    relocation.id, watermark, batch.snapshot_commit_sequence
                )));
            }
        }
        let actual_bytes = batch.records.iter().try_fold(0_usize, |total, record| {
            Ok::<_, BicDbError>(total.saturating_add(serde_json::to_vec(record)?.len()))
        })?;
        if actual_bytes != batch.serialized_record_bytes {
            return Err(data_error(format!(
                "snapshot byte accounting mismatch for {}: declared {}, actual {}",
                relocation.id, batch.serialized_record_bytes, actual_bytes
            )));
        }

        db.apply_range_snapshot_batch(range, collection_meta, batch)?;

        let mut hasher = Sha256::new();
        hasher.update(
            hex::decode(&current.snapshot_chain_sha256).map_err(|error| {
                data_error(format!("invalid stored snapshot chain checksum: {error}"))
            })?,
        );
        hasher.update(serde_json::to_vec(collection_meta)?);
        hasher.update(serde_json::to_vec(batch)?);
        let state = self
            .catalog
            .states
            .get_mut(&relocation.id)
            .expect("validated learner state exists");
        state.snapshot_bytes_copied = state
            .snapshot_bytes_copied
            .saturating_add(batch.serialized_record_bytes as u64);
        state.snapshot_resume_after_key = Some(batch.resume_after_key.clone());
        state.snapshot_commit_sequence = Some(
            state
                .snapshot_commit_sequence
                .unwrap_or(batch.snapshot_commit_sequence),
        );
        state.snapshot_chain_sha256 = hex::encode(hasher.finalize());
        let progress = incomplete_snapshot_progress(state);
        self.persist()?;
        Ok(progress)
    }

    pub fn finish_snapshot(
        &mut self,
        relocation: &RangeRelocation,
        expected_resume_after_key: Option<&str>,
        source_snapshot_commit_sequence: u64,
    ) -> Result<SnapshotCopyProgress> {
        let state = self
            .catalog
            .states
            .get_mut(&relocation.id)
            .ok_or_else(|| data_error(format!("learner {} is not prepared", relocation.id)))?;
        ensure_same_relocation(state, relocation)?;
        if expected_resume_after_key != state.snapshot_resume_after_key.as_deref() {
            return Err(data_error(format!(
                "cannot finish snapshot {} at a different resume cursor",
                relocation.id
            )));
        }
        if state.snapshot_complete {
            return Ok(completed_snapshot_progress(state));
        }
        let watermark = state
            .snapshot_commit_sequence
            .unwrap_or(source_snapshot_commit_sequence);
        if source_snapshot_commit_sequence < watermark {
            return Err(data_error(format!(
                "snapshot completion watermark {} is behind first batch watermark {}",
                source_snapshot_commit_sequence, watermark
            )));
        }
        state.snapshot_commit_sequence = Some(watermark);
        state.durable_source_commit_sequence = Some(watermark);
        state.snapshot_complete = true;
        let progress = completed_snapshot_progress(state);
        self.persist()?;
        Ok(progress)
    }

    pub fn checkpoint_snapshot_cursor(
        &mut self,
        relocation: &RangeRelocation,
        expected_resume_after_key: Option<&str>,
        resume_after_key: Option<String>,
        source_snapshot_commit_sequence: u64,
    ) -> Result<SnapshotCopyProgress> {
        let state = self
            .catalog
            .states
            .get_mut(&relocation.id)
            .ok_or_else(|| data_error(format!("learner {} is not prepared", relocation.id)))?;
        ensure_same_relocation(state, relocation)?;
        if state.snapshot_complete {
            return Err(data_error(format!(
                "snapshot for {} is already complete",
                relocation.id
            )));
        }
        if expected_resume_after_key != state.snapshot_resume_after_key.as_deref() {
            if resume_after_key.as_deref() == state.snapshot_resume_after_key.as_deref() {
                return Ok(incomplete_snapshot_progress(state));
            }
            return Err(data_error(format!(
                "snapshot cursor checkpoint for {} is stale",
                relocation.id
            )));
        }
        if let Some(watermark) = state.snapshot_commit_sequence {
            if source_snapshot_commit_sequence < watermark {
                return Err(data_error(format!(
                    "snapshot cursor watermark for {} moved backwards",
                    relocation.id
                )));
            }
        } else {
            state.snapshot_commit_sequence = Some(source_snapshot_commit_sequence);
        }
        state.snapshot_resume_after_key = resume_after_key;
        let progress = incomplete_snapshot_progress(state);
        self.persist()?;
        Ok(progress)
    }

    pub fn apply_commit_frames(
        &mut self,
        db: &mut BicDb,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        frames: &[CommitFrame],
        source_commit_sequence: u64,
    ) -> Result<CatchUpProgress> {
        let initial = self
            .catalog
            .states
            .get(&relocation.id)
            .cloned()
            .ok_or_else(|| data_error(format!("learner {} is not prepared", relocation.id)))?;
        ensure_same_relocation(&initial, relocation)?;
        if !initial.snapshot_complete {
            return Err(data_error(format!(
                "cannot apply catch-up before snapshot {} completes",
                relocation.id
            )));
        }
        let mut durable = initial.durable_source_commit_sequence.ok_or_else(|| {
            data_error(format!(
                "snapshot {} has no durable watermark",
                relocation.id
            ))
        })?;
        if source_commit_sequence < durable {
            return Err(data_error(format!(
                "source watermark for {} moved backwards from {} to {}",
                relocation.id, durable, source_commit_sequence
            )));
        }
        for frame in frames {
            frame.verify_checksum()?;
            if frame.cluster_id != self.catalog.cluster_id.as_str() {
                return Err(data_error(format!(
                    "catch-up frame cluster {} does not match {}",
                    frame.cluster_id, self.catalog.cluster_id
                )));
            }
            if frame.commit_seq <= durable {
                continue;
            }
            if frame.previous_commit_seq != durable || frame.commit_seq != durable.saturating_add(1)
            {
                return Err(data_error(format!(
                    "range catch-up gap for {}: durable {}, frame {} after {}",
                    relocation.id, durable, frame.commit_seq, frame.previous_commit_seq
                )));
            }
            if frame.commit_seq > source_commit_sequence {
                return Err(data_error(format!(
                    "catch-up frame {} is ahead of reported source {}",
                    frame.commit_seq, source_commit_sequence
                )));
            }
            db.apply_range_commit_frame_as_local(range, frame)?;
            durable = frame.commit_seq;
            self.catalog
                .states
                .get_mut(&relocation.id)
                .expect("validated learner state exists")
                .durable_source_commit_sequence = Some(durable);
            // Persist after every source commit. Replaying a data commit after
            // a crash before this write is idempotent, while skipping one is
            // not.
            self.persist()?;
        }
        Ok(CatchUpProgress {
            destination_durable_commit_sequence: durable,
            source_commit_sequence,
        })
    }

    pub fn complete_relocation(&mut self, relocation_id: RelocationId) -> Result<bool> {
        let removed = self.catalog.states.remove(&relocation_id).is_some();
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    fn persist(&self) -> Result<()> {
        fs::create_dir_all(&self.root)?;
        let envelope = RangeLearnerApplyEnvelope::new(self.catalog.clone())?;
        crate::storage::write_atomic(
            &self.root.join(DEFAULT_RANGE_LEARNER_APPLY_STATE),
            &serde_json::to_vec_pretty(&envelope)?,
            self.fsync,
        )
    }
}

fn ensure_same_relocation(
    state: &RangeLearnerApplyState,
    relocation: &RangeRelocation,
) -> Result<()> {
    if state.relocation_id != relocation.id
        || state.range_id != relocation.range_id
        || state.learner_epoch != relocation.learner_epoch
        || state.source != relocation.source
        || state.target != relocation.target
    {
        return Err(data_error(format!(
            "learner state for {} conflicts with relocation identity",
            relocation.id
        )));
    }
    Ok(())
}

fn incomplete_snapshot_progress(state: &RangeLearnerApplyState) -> SnapshotCopyProgress {
    SnapshotCopyProgress {
        bytes_copied: state.snapshot_bytes_copied,
        resume_after_key: state.snapshot_resume_after_key.clone(),
        completed: false,
        snapshot_id: None,
        snapshot_sha256: None,
        snapshot_commit_sequence: None,
    }
}

fn completed_snapshot_progress(state: &RangeLearnerApplyState) -> SnapshotCopyProgress {
    SnapshotCopyProgress {
        bytes_copied: state.snapshot_bytes_copied,
        resume_after_key: None,
        completed: true,
        snapshot_id: Some(state.snapshot_id.clone()),
        snapshot_sha256: Some(state.snapshot_chain_sha256.clone()),
        snapshot_commit_sequence: state.snapshot_commit_sequence,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CollectionScanCursor {
    collection: String,
    after_id: Option<String>,
}

fn encode_cursor(cursor: &CollectionScanCursor) -> Result<String> {
    Ok(hex::encode(serde_json::to_vec(cursor)?))
}

fn decode_cursor(value: &str) -> Result<CollectionScanCursor> {
    let bytes =
        hex::decode(value).map_err(|error| data_error(format!("invalid range cursor: {error}")))?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SnapshotSourceStep {
    Batch {
        collection_meta: CollectionMeta,
        batch: RangeSnapshotBatch,
    },
    Advance {
        resume_after_key: String,
        source_snapshot_commit_sequence: u64,
    },
    Complete {
        source_snapshot_commit_sequence: u64,
    },
}

/// Result of one bounded, range-fenced anti-entropy scan step.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RangeDigestAdvance {
    Progress {
        resume_after_key: String,
        scanned_records: u64,
        serialized_bytes: u64,
    },
    Complete {
        manifest: RangeDigestManifest,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatchUpSourceBatch {
    pub source_commit_sequence: u64,
    pub frames: Vec<CommitFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSchemaStageReceipt {
    pub stage_id: Uuid,
    pub base_fingerprint_sha256: String,
    pub target_fingerprint_sha256: String,
    pub stage_checksum_sha256: String,
}

impl ClusterSchemaStageReceipt {
    pub fn validate_for(&self, signed_bundle: &SignedClusterSchemaBundle) -> Result<()> {
        if self.stage_id.is_nil()
            || self.base_fingerprint_sha256 == self.target_fingerprint_sha256
            || self.target_fingerprint_sha256 != signed_bundle.bundle.fingerprint.sha256
            || self.stage_checksum_sha256.len() != 64
            || hex::decode(&self.stage_checksum_sha256).map_or(true, |bytes| bytes.len() != 32)
        {
            return Err(data_error(
                "cluster schema stage receipt identity or checksum is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSchemaActivationReceipt {
    pub rollout_id: Uuid,
    pub node_id: ClusterNodeId,
    pub stage_id: Uuid,
    pub activation_id: Uuid,
    pub phase: ClusterSchemaActivationPhase,
    pub target_fingerprint_sha256: String,
    pub live_fingerprint_sha256: String,
    pub activation_state_checksum_sha256: String,
    pub inspected_items: u64,
    pub applied_change: bool,
    pub updated_at_ms: u64,
}

impl ClusterSchemaActivationReceipt {
    pub fn complete(&self) -> bool {
        self.phase == ClusterSchemaActivationPhase::Complete
    }

    pub fn validate_completion_for_node(&self, node_id: &ClusterNodeId) -> Result<()> {
        let valid_digest = |value: &str| {
            value.len() == 64 && hex::decode(value).is_ok_and(|bytes| bytes.len() == 32)
        };
        if &self.node_id != node_id
            || self.rollout_id.is_nil()
            || self.stage_id.is_nil()
            || self.activation_id.is_nil()
            || !self.complete()
            || !valid_digest(&self.target_fingerprint_sha256)
            || self.live_fingerprint_sha256 != self.target_fingerprint_sha256
            || !valid_digest(&self.activation_state_checksum_sha256)
            || self.updated_at_ms == 0
        {
            return Err(data_error(
                "cluster schema activation completion receipt is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        node_id: &ClusterNodeId,
        rollout_id: Uuid,
        stage: &ClusterSchemaStageReceipt,
        signed_bundle: &SignedClusterSchemaBundle,
    ) -> Result<()> {
        let target = &signed_bundle.bundle.fingerprint.sha256;
        let valid_digest = |value: &str| {
            value.len() == 64 && hex::decode(value).is_ok_and(|bytes| bytes.len() == 32)
        };
        if rollout_id.is_nil()
            || self.rollout_id != rollout_id
            || &self.node_id != node_id
            || self.stage_id != stage.stage_id
            || self.activation_id.is_nil()
            || self.target_fingerprint_sha256 != *target
            || !valid_digest(&self.live_fingerprint_sha256)
            || !valid_digest(&self.activation_state_checksum_sha256)
            || (self.complete() && self.live_fingerprint_sha256 != *target)
            || (!self.complete() && self.live_fingerprint_sha256 == *target)
            || self.updated_at_ms == 0
        {
            return Err(data_error(
                "cluster schema activation receipt identity, phase, or checksum is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSchemaFinalizationReceipt {
    pub rollout_id: Uuid,
    pub node_id: ClusterNodeId,
    pub stage_id: Uuid,
    pub activation_id: Uuid,
    pub target_fingerprint_sha256: String,
    pub activation_state_checksum_sha256: String,
    pub finalization_checksum_sha256: String,
    pub finalized_at_ms: u64,
}

impl ClusterSchemaFinalizationReceipt {
    pub fn validate_for(
        &self,
        node_id: &ClusterNodeId,
        activation: &ClusterSchemaActivationReceipt,
    ) -> Result<()> {
        activation.validate_completion_for_node(node_id)?;
        let valid_digest = |value: &str| {
            value.len() == 64 && hex::decode(value).is_ok_and(|bytes| bytes.len() == 32)
        };
        if !activation.complete()
            || &self.node_id != node_id
            || self.rollout_id != activation.rollout_id
            || self.stage_id != activation.stage_id
            || self.activation_id != activation.activation_id
            || self.target_fingerprint_sha256 != activation.target_fingerprint_sha256
            || self.activation_state_checksum_sha256 != activation.activation_state_checksum_sha256
            || !valid_digest(&self.finalization_checksum_sha256)
            || self.finalized_at_ms == 0
        {
            return Err(data_error(
                "cluster schema finalization receipt identity or checksum is invalid",
            ));
        }
        Ok(())
    }
}

/// Node-local, bounded relocation operations. Both the in-process transport
/// and the authenticated network service use this exact implementation so
/// persistence, cursor validation, and range fencing cannot drift between
/// deployment modes.
#[derive(Debug)]
pub struct ClusterDataNodeService {
    cluster_id: ClusterId,
    node_id: ClusterNodeId,
    db: Arc<RwLock<BicDb>>,
    schema_compatibility: SchemaCompatibilityAuthority,
    schema_compatibility_window: Option<ClusterSchemaCompatibilityWindow>,
    learner: RangeLearnerStore,
    range_writes: RangeWriteStore,
    range_digest_root: PathBuf,
    range_digest_repair_root: PathBuf,
    schema_signing_keys: BTreeMap<String, [u8; 32]>,
    schema_stage_limits: ClusterSchemaStageLimits,
    schema_activation_limits: ClusterSchemaActivationLimits,
    fsync: bool,
}

impl ClusterDataNodeService {
    pub fn open(
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        root: impl AsRef<Path>,
        db: Arc<RwLock<BicDb>>,
        fsync: bool,
    ) -> Result<Self> {
        Self::open_with_schema_trust(
            cluster_id,
            node_id,
            root,
            db,
            BTreeMap::new(),
            ClusterSchemaStageLimits::default(),
            fsync,
        )
    }

    pub fn open_with_schema_trust(
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        root: impl AsRef<Path>,
        db: Arc<RwLock<BicDb>>,
        schema_signing_keys: BTreeMap<String, [u8; 32]>,
        schema_stage_limits: ClusterSchemaStageLimits,
        fsync: bool,
    ) -> Result<Self> {
        let schema_activation_limits = ClusterSchemaActivationLimits {
            stage: schema_stage_limits,
            ..ClusterSchemaActivationLimits::default()
        };
        Self::open_with_schema_runtime(
            cluster_id,
            node_id,
            root,
            db,
            schema_signing_keys,
            schema_stage_limits,
            schema_activation_limits,
            fsync,
        )
    }

    pub fn open_with_schema_runtime(
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        root: impl AsRef<Path>,
        db: Arc<RwLock<BicDb>>,
        schema_signing_keys: BTreeMap<String, [u8; 32]>,
        schema_stage_limits: ClusterSchemaStageLimits,
        schema_activation_limits: ClusterSchemaActivationLimits,
        fsync: bool,
    ) -> Result<Self> {
        schema_stage_limits.validate()?;
        schema_activation_limits.validate()?;
        if schema_activation_limits.stage != schema_stage_limits {
            return Err(data_error(
                "cluster schema activation and staging limits must agree",
            ));
        }
        let root = root.as_ref();
        let learner = RangeLearnerStore::open(root, cluster_id.clone(), fsync)?;
        let range_writes = RangeWriteStore::open(root, cluster_id.clone(), node_id.clone(), fsync)?;
        let range_digest_root = root.join(DEFAULT_RANGE_DIGEST_STATE_DIR);
        fs::create_dir_all(&range_digest_root)?;
        if fs::symlink_metadata(&range_digest_root)?
            .file_type()
            .is_symlink()
        {
            return Err(data_error(
                "range digest state directory must not be a symbolic link",
            ));
        }
        let range_digest_repair_root = root.join(DEFAULT_RANGE_DIGEST_REPAIR_STATE_DIR);
        fs::create_dir_all(&range_digest_repair_root)?;
        if fs::symlink_metadata(&range_digest_repair_root)?
            .file_type()
            .is_symlink()
        {
            return Err(data_error(
                "range digest repair state directory must not be a symbolic link",
            ));
        }
        let schema_compatibility = db.read().schema_compatibility_authority();
        let schema_compatibility_window = if schema_signing_keys.is_empty() {
            None
        } else {
            db.read()
                .verify_cluster_schema_compatibility_window(
                    &schema_signing_keys,
                    schema_activation_limits,
                )
                .ok()
        };
        let mut service = Self {
            cluster_id,
            node_id,
            db,
            schema_compatibility,
            schema_compatibility_window,
            learner,
            range_writes,
            range_digest_root,
            range_digest_repair_root,
            schema_signing_keys,
            schema_stage_limits,
            schema_activation_limits,
            fsync,
        };
        service.recover_committed_range_writes()?;
        Ok(service)
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    pub fn node_id(&self) -> &ClusterNodeId {
        &self.node_id
    }

    pub fn database(&self) -> Arc<RwLock<BicDb>> {
        Arc::clone(&self.db)
    }

    pub fn schema_compatibility_fingerprint(&self) -> Result<SchemaCompatibilityFingerprint> {
        self.db.read().schema_compatibility_fingerprint()
    }

    pub fn verified_schema_compatibility_sha256(&self) -> Option<String> {
        self.schema_compatibility.verified_sha256()
    }

    pub fn schema_compatibility_allows(
        &self,
        active_sha256: &str,
        pending_target_sha256: Option<&str>,
    ) -> bool {
        let Some(verified) = self.schema_compatibility.verified_sha256() else {
            return false;
        };
        if verified == active_sha256 {
            return true;
        }
        let Some(pending_target_sha256) = pending_target_sha256 else {
            return false;
        };
        self.schema_compatibility_window
            .as_ref()
            .is_some_and(|window| {
                window.base_fingerprint_sha256 == active_sha256
                    && window.target_fingerprint_sha256 == pending_target_sha256
                    && window.verified_live_fingerprint_sha256 == verified
            })
    }

    pub fn install_schema_bundle(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        bundle: &ClusterSchemaBundle,
    ) -> Result<bool> {
        self.ensure_target(relocation)?;
        if relocation.range_id != range.id || relocation.learner_epoch != range.epoch {
            return Err(data_error(format!(
                "schema bootstrap relocation {}/{} does not match range {}/{}",
                relocation.range_id, relocation.learner_epoch, range.id, range.epoch
            )));
        }
        self.db.write().install_cluster_schema_bundle(bundle)
    }

    pub fn stage_signed_schema_bundle(
        &mut self,
        signed_bundle: SignedClusterSchemaBundle,
        now_ms: u64,
    ) -> Result<ClusterSchemaStageReceipt> {
        if self.schema_signing_keys.is_empty() {
            return Err(data_error(
                "cluster schema staging has no host-configured trusted signing keys",
            ));
        }
        let state = self.db.write().stage_signed_cluster_schema_bundle(
            signed_bundle,
            &self.schema_signing_keys,
            self.schema_stage_limits,
            now_ms,
        )?;
        self.schema_compatibility_window =
            Some(self.db.read().verify_cluster_schema_compatibility_window(
                &self.schema_signing_keys,
                self.schema_activation_limits,
            )?);
        Ok(ClusterSchemaStageReceipt {
            stage_id: state.stage_id,
            base_fingerprint_sha256: state.plan.base_fingerprint_sha256,
            target_fingerprint_sha256: state.plan.target_fingerprint_sha256,
            stage_checksum_sha256: state.checksum_sha256,
        })
    }

    pub fn advance_signed_schema_activation(
        &mut self,
        rollout_id: Uuid,
        expected_stage_id: Uuid,
        expected_target_fingerprint_sha256: &str,
        now_ms: u64,
    ) -> Result<ClusterSchemaActivationReceipt> {
        if rollout_id.is_nil() || expected_stage_id.is_nil() {
            return Err(data_error(
                "cluster schema activation request identity is invalid",
            ));
        }
        if self.schema_signing_keys.is_empty() {
            return Err(data_error(
                "cluster schema activation has no host-configured trusted signing keys",
            ));
        }
        let mut db = self.db.write();
        let stage =
            db.load_cluster_schema_stage(&self.schema_signing_keys, self.schema_stage_limits)?;
        if stage.stage_id != expected_stage_id
            || stage.plan.target_fingerprint_sha256 != expected_target_fingerprint_sha256
        {
            return Err(data_error(
                "cluster schema activation request does not match the durable signed stage",
            ));
        }
        db.begin_cluster_schema_activation(
            &self.schema_signing_keys,
            self.schema_activation_limits,
            now_ms,
        )?;
        let advance = db.advance_cluster_schema_activation(
            &self.schema_signing_keys,
            self.schema_activation_limits,
            now_ms,
        )?;
        let live = db.schema_compatibility_fingerprint()?;
        let receipt = ClusterSchemaActivationReceipt {
            rollout_id,
            node_id: self.node_id.clone(),
            stage_id: stage.stage_id,
            activation_id: advance.state.activation_id,
            phase: advance.state.phase,
            target_fingerprint_sha256: stage.plan.target_fingerprint_sha256.clone(),
            live_fingerprint_sha256: live.sha256,
            activation_state_checksum_sha256: advance.state.checksum_sha256,
            inspected_items: u64::try_from(advance.inspected_items).unwrap_or(u64::MAX),
            applied_change: advance.applied_change,
            updated_at_ms: advance.state.updated_at_ms,
        };
        let stage_receipt = ClusterSchemaStageReceipt {
            stage_id: stage.stage_id,
            base_fingerprint_sha256: stage.plan.base_fingerprint_sha256.clone(),
            target_fingerprint_sha256: stage.plan.target_fingerprint_sha256.clone(),
            stage_checksum_sha256: stage.checksum_sha256.clone(),
        };
        receipt.validate_for(
            &self.node_id,
            rollout_id,
            &stage_receipt,
            &stage.signed_bundle,
        )?;
        let window = db.verify_cluster_schema_compatibility_window(
            &self.schema_signing_keys,
            self.schema_activation_limits,
        )?;
        drop(db);
        self.schema_compatibility_window = Some(window);
        Ok(receipt)
    }

    pub fn finalize_signed_schema_activation(
        &mut self,
        activation: ClusterSchemaActivationReceipt,
        now_ms: u64,
    ) -> Result<ClusterSchemaFinalizationReceipt> {
        if self.schema_signing_keys.is_empty() {
            return Err(data_error(
                "cluster schema finalization has no host-configured trusted signing keys",
            ));
        }
        activation.validate_completion_for_node(&self.node_id)?;
        let mut db = self.db.write();
        let finalized = db.finalize_cluster_schema_activation(
            &self.schema_signing_keys,
            self.schema_activation_limits,
            activation.rollout_id,
            activation.stage_id,
            activation.activation_id,
            &activation.target_fingerprint_sha256,
            &activation.activation_state_checksum_sha256,
            now_ms,
        )?;
        let receipt = ClusterSchemaFinalizationReceipt {
            rollout_id: finalized.rollout_id,
            node_id: self.node_id.clone(),
            stage_id: finalized.stage_id,
            activation_id: finalized.activation_id,
            target_fingerprint_sha256: finalized.target_fingerprint_sha256,
            activation_state_checksum_sha256: finalized.activation_state_checksum_sha256,
            finalization_checksum_sha256: finalized.checksum_sha256,
            finalized_at_ms: finalized.finalized_at_ms,
        };
        receipt.validate_for(&self.node_id, &activation)?;
        drop(db);
        self.schema_compatibility_window = None;
        Ok(receipt)
    }

    pub fn next_range_write_index(&self, range_id: RangeId) -> u64 {
        self.range_writes.next_index(range_id)
    }

    pub fn range_write_progress(&self, range_id: RangeId) -> RangeWriteProgress {
        self.range_writes.progress(range_id)
    }

    pub fn range_write_progress_for_peer(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
    ) -> Result<RangeWriteProgress> {
        self.validate_range_write_peer(range, caller)?;
        Ok(self.range_writes.progress(range.id))
    }

    pub fn install_range_backup_fence(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        plan_id: Uuid,
        installed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<RangeWriteProgress> {
        self.validate_range_write_peer(range, caller)?;
        self.range_writes.install_backup_fence(
            plan_id,
            range.id,
            range.epoch,
            installed_at_ms,
            expires_at_ms,
        )
    }

    pub fn release_range_backup_fence(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        plan_id: Uuid,
    ) -> Result<bool> {
        self.validate_range_write_peer(range, caller)?;
        self.range_writes
            .release_backup_fence(plan_id, range.id, range.epoch)
    }

    pub fn range_backup_fences(&self) -> Vec<RangeBackupWriteFence> {
        self.range_writes.backup_fences()
    }

    pub fn probe_range_write_for_peer(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        index: u64,
    ) -> Result<RangeWriteProbe> {
        self.validate_range_write_peer(range, caller)?;
        self.range_writes.probe(range.id, range.epoch, index)
    }

    pub fn export_range_write_repair(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        previous_resolved_index: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.validate_range_write_peer(range, caller)?;
        self.range_writes.export_repair_batch(
            &self.node_id,
            range.id,
            range.epoch,
            previous_resolved_index,
            limits,
        )
    }

    pub fn apply_range_write_repair(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.validate_range_write_peer(range, caller)?;
        batch.validate(limits)?;
        if batch.source_node_id != *caller
            || batch.range_id != range.id
            || batch.range_epoch != range.epoch
        {
            return Err(data_error(
                "range-write repair source/range/epoch fencing failed",
            ));
        }
        self.apply_range_write_suffix(range, batch, limits)
    }

    /// Install a quorum-evidenced resolved prefix on the newly designated
    /// leader. The coordinator may choose any current voter as the physical
    /// source, but the source identity remains authenticated and range-bound.
    pub fn apply_range_write_leader_recovery(
        &mut self,
        range: &RangeDescriptor,
        source_voter: &ClusterNodeId,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        let source_is_voter = range.replicas.iter().any(|replica| {
            replica.node_id == *source_voter
                && replica.role == crate::distribution::RangeReplicaRole::Voter
        });
        if !source_is_voter
            || batch.source_node_id != *source_voter
            || batch.range_id != range.id
            || batch.range_epoch != range.epoch
        {
            return Err(data_error(
                "range-write leader recovery source/range/epoch fencing failed",
            ));
        }
        // Leadership recovery is run by the cluster supervisor outside a SQL
        // commit lock. Replaying exact resolved mutations here closes the gap
        // between physical snapshot parity and commands committed during the
        // final promotion boundary.
        self.apply_range_write_suffix(range, batch, limits)
    }

    /// Resolve one old-epoch tail command from quorum probe evidence. A
    /// committed outcome is applied through BicDB before its log checkpoint;
    /// an abort never touches row data.
    pub fn resolve_range_write_leader_tail(
        &mut self,
        range: &RangeDescriptor,
        evidence_voter: &ClusterNodeId,
        command: &RangeWriteCommand,
        outcome: RangeWriteState,
    ) -> Result<RangeWriteProgress> {
        let source_is_voter = range.replicas.iter().any(|replica| {
            replica.node_id == *evidence_voter
                && replica.role == crate::distribution::RangeReplicaRole::Voter
        });
        if !source_is_voter
            || command.range_id != range.id
            || command.range_epoch > range.epoch
            || !matches!(outcome, RangeWriteState::Applied | RangeWriteState::Aborted)
        {
            return Err(data_error(
                "range-write tail resolution source/range/epoch/outcome fencing failed",
            ));
        }
        let progress = self.range_writes.progress(range.id);
        if command.index != progress.resolved_through.saturating_add(1) {
            return Err(data_error(format!(
                "range-write tail resolution gap for {}: expected {}, got {}",
                range.id,
                progress.resolved_through.saturating_add(1),
                command.index
            )));
        }
        self.validate_range_write_repair_entry(range, command)?;
        self.range_writes.prepare(command.clone())?;
        match outcome {
            RangeWriteState::Applied => {
                self.range_writes.commit(command)?;
                let state = self.range_writes.certify(command)?;
                if state != RangeWriteState::Applied {
                    self.db
                        .read()
                        .apply_admitted_mutations(&command.mutations)?;
                    self.range_writes.mark_applied(command)?;
                }
            }
            RangeWriteState::Aborted => {
                self.range_writes.abort(command)?;
            }
            RangeWriteState::Prepared
            | RangeWriteState::Committed
            | RangeWriteState::QuorumCommitted => unreachable!("outcome fenced above"),
        }
        Ok(self.range_writes.progress(range.id))
    }

    fn apply_range_write_suffix(
        &mut self,
        range: &RangeDescriptor,
        batch: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        batch.validate(limits)?;
        let mut progress = self.range_writes.progress(range.id);
        if progress.resolved_through < batch.previous_resolved_index {
            return Err(data_error(format!(
                "range-write repair gap for {}: local {}, batch starts after {}",
                range.id, progress.resolved_through, batch.previous_resolved_index
            )));
        }
        for entry in &batch.entries {
            if entry.command.index <= progress.resolved_through {
                self.range_writes.verify_resolved_entry(entry)?;
                continue;
            }
            if entry.command.index != progress.resolved_through.saturating_add(1) {
                return Err(data_error(format!(
                    "range-write repair gap for {}: expected {}, got {}",
                    range.id,
                    progress.resolved_through.saturating_add(1),
                    entry.command.index
                )));
            }
            self.validate_range_write_repair_entry(range, &entry.command)?;
            self.range_writes.prepare(entry.command.clone())?;
            match entry.state {
                RangeWriteState::Applied => {
                    self.range_writes.commit(&entry.command)?;
                    let state = self.range_writes.certify(&entry.command)?;
                    if state != RangeWriteState::Applied {
                        self.db
                            .read()
                            .apply_admitted_mutations(&entry.command.mutations)?;
                        self.range_writes.mark_applied(&entry.command)?;
                    }
                }
                RangeWriteState::Aborted => {
                    self.range_writes.abort(&entry.command)?;
                }
                RangeWriteState::Prepared
                | RangeWriteState::Committed
                | RangeWriteState::QuorumCommitted => {
                    return Err(data_error(
                        "range-write repair may contain only resolved commands",
                    ));
                }
            }
            progress = self.range_writes.progress(range.id);
        }
        Ok(progress)
    }

    pub fn prepared_range_writes(&self, range_id: RangeId) -> Vec<RangeWriteCommand> {
        self.range_writes.prepared_for_range(range_id)
    }

    pub fn prepare_range_write(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, &command)?;
        let state = self.range_writes.prepare(command.clone())?;
        Ok(self.range_write_ack(&command, state))
    }

    /// Persist the leader's commit decision without recursively applying the
    /// outer SQL transaction. The SQL commit calls `mark_range_write_applied`
    /// after its ordinary WAL/index/record path succeeds.
    pub fn commit_range_write_decision(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, command)?;
        let state = self.range_writes.commit(command)?;
        Ok(self.range_write_ack(command, state))
    }

    pub fn certify_range_write(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, command)?;
        let state = self.range_writes.certify(command)?;
        Ok(self.range_write_ack(command, state))
    }

    /// Persist a provisional decision. It is not replayed or applied until the
    /// leader supplies a separate quorum certificate.
    pub fn commit_range_write(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, command)?;
        let state = self.range_writes.commit(command)?;
        Ok(self.range_write_ack(command, state))
    }

    /// Apply only a quorum-certified decision. The applied checkpoint remains
    /// durable before acknowledgement, and startup replays only this certified
    /// state rather than a minority provisional decision.
    pub fn apply_certified_range_write(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, command)?;
        let state = self.range_writes.state(command)?;
        if !matches!(
            state,
            RangeWriteState::QuorumCommitted | RangeWriteState::Applied
        ) {
            return Err(data_error(format!(
                "range-write command {} has no quorum certificate",
                command.command_id
            )));
        }
        if state != RangeWriteState::Applied {
            self.db
                .read()
                .apply_admitted_mutations(&command.mutations)?;
            self.range_writes.mark_applied(command)?;
        }
        Ok(self.range_write_ack(command, RangeWriteState::Applied))
    }

    pub fn abort_range_write(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.validate_range_write(range, caller, command)?;
        let state = self.range_writes.abort(command)?;
        Ok(self.range_write_ack(command, state))
    }

    pub fn mark_range_write_applied(&mut self, command_id: &str) -> Result<()> {
        let command = self
            .range_writes
            .command_by_id(command_id)
            .ok_or_else(|| data_error(format!("unknown range-write command {command_id}")))?;
        self.range_writes.mark_applied(&command)
    }

    fn recover_committed_range_writes(&mut self) -> Result<()> {
        for command in self.range_writes.committed_not_applied() {
            self.db
                .read()
                .apply_admitted_mutations(&command.mutations)?;
            self.range_writes.mark_applied(&command)?;
        }
        Ok(())
    }

    fn validate_range_write(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        command.validate()?;
        self.validate_range_write_peer(range, caller)?;
        if command.cluster_id != self.cluster_id
            || command.range_id != range.id
            || command.range_epoch != range.epoch
            || &command.leader_node_id != caller
        {
            return Err(data_error(format!(
                "range-write command {} failed cluster/range/epoch/leader fencing",
                command.command_id
            )));
        }
        for mutation in &command.mutations {
            if !range.contains_token(crate::distribution::distribution_key_token(
                &mutation.collection,
                &mutation.record_id,
            )) {
                return Err(data_error(format!(
                    "range-write mutation {}/{} is outside {}",
                    mutation.collection, mutation.record_id, range.id
                )));
            }
        }
        Ok(())
    }

    fn validate_range_write_repair_entry(
        &self,
        range: &RangeDescriptor,
        command: &RangeWriteCommand,
    ) -> Result<()> {
        command.validate()?;
        if command.cluster_id != self.cluster_id
            || command.range_id != range.id
            || command.range_epoch > range.epoch
        {
            return Err(data_error(format!(
                "range-write repair command {} failed cluster/range/epoch fencing",
                command.command_id
            )));
        }
        for mutation in &command.mutations {
            if !range.contains_token(crate::distribution::distribution_key_token(
                &mutation.collection,
                &mutation.record_id,
            )) {
                return Err(data_error(format!(
                    "range-write repair mutation {}/{} is outside {}",
                    mutation.collection, mutation.record_id, range.id
                )));
            }
        }
        Ok(())
    }

    fn validate_range_write_peer(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
    ) -> Result<()> {
        if &range.leader != caller {
            return Err(data_error(format!(
                "node {caller} is not the current leader for {} epoch {}",
                range.id, range.epoch
            )));
        }
        let local_is_voter = range.replicas.iter().any(|replica| {
            replica.node_id == self.node_id
                && replica.role == crate::distribution::RangeReplicaRole::Voter
        });
        if !local_is_voter {
            return Err(data_error(format!(
                "node {} is not a voting replica for {}",
                self.node_id, range.id
            )));
        }
        Ok(())
    }

    fn range_write_ack(
        &self,
        command: &RangeWriteCommand,
        state: RangeWriteState,
    ) -> RangeWriteAck {
        RangeWriteAck {
            node_id: self.node_id.clone(),
            range_id: command.range_id,
            range_epoch: command.range_epoch,
            index: command.index,
            state,
        }
    }

    pub fn prepare_learner(&mut self, relocation: &RangeRelocation) -> Result<()> {
        self.ensure_target(relocation)?;
        self.learner.prepare_learner(relocation)?;
        Ok(())
    }

    pub fn export_snapshot_step(
        &self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        options: &RangeSnapshotOptions,
    ) -> Result<SnapshotSourceStep> {
        self.ensure_snapshot_source(range)?;
        self.export_snapshot_step_from(
            range,
            relocation.snapshot_resume_after_key.as_deref(),
            options,
        )
    }

    /// Advance one bounded digest step only while this replica is protected by
    /// the exact durable range fence and resolved consensus prefix pinned into
    /// the digest session. The caller is authenticated as the current leader;
    /// followers never gain an unauthenticated scan/control surface.
    pub fn advance_range_digest(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        state: &mut RangeDigestState,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestAdvance> {
        limits.validate()?;
        state.validate(limits)?;
        self.ensure_range_digest_fence(range, caller, state, now_ms)?;
        let expected_previous = state.resume_after_key.clone();
        let step = self.export_snapshot_step_from(
            range,
            expected_previous.as_deref(),
            &RangeSnapshotOptions {
                max_records_per_batch: limits.max_records_per_batch,
                max_bytes_per_batch: limits.max_bytes_per_batch,
                max_record_bytes: limits.max_record_bytes,
            },
        )?;

        // A supervisor may release/replace a fence between bounded calls. Check
        // again immediately before mutating the resumable digest state.
        self.ensure_range_digest_fence(range, caller, state, now_ms)?;
        match step {
            SnapshotSourceStep::Batch {
                collection_meta,
                batch,
            } => {
                let resume_after_key = batch.resume_after_key.clone();
                state.apply_batch(
                    expected_previous.as_deref(),
                    resume_after_key.clone(),
                    &collection_meta.name,
                    batch.range_id,
                    batch.range_epoch,
                    &batch.records,
                    batch.serialized_record_bytes,
                    limits,
                )?;
                Ok(RangeDigestAdvance::Progress {
                    resume_after_key,
                    scanned_records: state.scanned_records,
                    serialized_bytes: state.serialized_bytes,
                })
            }
            SnapshotSourceStep::Advance {
                resume_after_key, ..
            } => {
                state.advance_cursor(
                    expected_previous.as_deref(),
                    resume_after_key.clone(),
                    limits,
                )?;
                Ok(RangeDigestAdvance::Progress {
                    resume_after_key,
                    scanned_records: state.scanned_records,
                    serialized_bytes: state.serialized_bytes,
                })
            }
            SnapshotSourceStep::Complete { .. } => Ok(RangeDigestAdvance::Complete {
                manifest: state.finish(expected_previous.as_deref(), limits)?,
            }),
        }
    }

    /// Node-owned, crash-resumable digest step used by authenticated cluster
    /// RPC. Callers never submit bucket contents: they present only the last
    /// checksum observed from this node. Passing `None` creates a fresh session
    /// or reads its current state after an uncertain response without advancing
    /// it. This prevents a leader from forging a follower's integrity evidence.
    pub fn advance_persisted_range_digest(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        session_id: Uuid,
        expected_checksum_sha256: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestState> {
        limits.validate()?;
        if session_id.is_nil() {
            return Err(data_error("range digest session ID must not be nil"));
        }
        if expected_checksum_sha256.is_some_and(|value| {
            value.len() != 64
                || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
                || value.bytes().any(|byte| byte.is_ascii_uppercase())
        }) {
            return Err(data_error(
                "expected range digest checksum is not canonical SHA-256",
            ));
        }
        let path = self.range_digest_state_path(range.id, session_id);
        let mut state = match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(data_error("range digest checkpoint is not a regular file"));
                }
                let state = load_range_digest_state(&path, limits)?;
                if state.session_id != session_id {
                    return Err(data_error("range digest checkpoint identity mismatch"));
                }
                state
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if expected_checksum_sha256.is_some() {
                    return Err(data_error(
                        "range digest checkpoint is missing for the expected checksum",
                    ));
                }
                self.ensure_range_digest_capacity()?;
                let progress = self.range_writes.progress(range.id);
                let state = RangeDigestState::create(
                    session_id,
                    self.cluster_id.clone(),
                    self.node_id.clone(),
                    range.id,
                    range.epoch,
                    range.start_token,
                    range.end_token,
                    progress.resolved_through,
                    limits,
                )?;
                self.ensure_range_digest_fence(range, caller, &state, now_ms)?;
                save_range_digest_state(&path, &state, limits, self.fsync)?;
                return Ok(state);
            }
            Err(error) => return Err(error.into()),
        };
        self.ensure_range_digest_fence(range, caller, &state, now_ms)?;
        let Some(expected_checksum_sha256) = expected_checksum_sha256 else {
            return Ok(state);
        };
        if state.checksum_sha256 != expected_checksum_sha256 {
            return Err(data_error(
                "stale range digest checkpoint checksum; fetch current state before retrying",
            ));
        }
        if !state.completed {
            self.advance_range_digest(range, caller, &mut state, limits, now_ms)?;
            save_range_digest_state(&path, &state, limits, self.fsync)?;
        }
        Ok(state)
    }

    /// Export one bounded physical scan step for a single divergent digest
    /// bucket. The node must have completed the matching node-owned digest and
    /// the original write fence must still be active at the same prefix.
    #[allow(clippy::too_many_arguments)]
    pub fn export_range_digest_bucket_step(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        session_id: Uuid,
        bucket: u32,
        resume_after_key: Option<&str>,
        limits: &RangeDigestLimits,
        now_ms: u64,
    ) -> Result<RangeDigestBucketScanStep> {
        limits.validate()?;
        if bucket as usize >= limits.bucket_count {
            return Err(data_error(
                "range digest bucket is outside its fixed fanout",
            ));
        }
        let path = self.range_digest_state_path(range.id, session_id);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(data_error("range digest checkpoint is not a regular file"));
        }
        let state = load_range_digest_state(&path, limits)?;
        if state.session_id != session_id || !state.completed {
            return Err(data_error(
                "range digest bucket export requires the completed matching checkpoint",
            ));
        }
        state.manifest(limits)?;
        self.ensure_range_digest_fence(range, caller, &state, now_ms)?;
        let step = self.export_snapshot_step_from(
            range,
            resume_after_key,
            &RangeSnapshotOptions {
                max_records_per_batch: limits.max_records_per_batch,
                max_bytes_per_batch: limits.max_bytes_per_batch,
                max_record_bytes: limits.max_record_bytes,
            },
        )?;
        self.ensure_range_digest_fence(range, caller, &state, now_ms)?;
        let expected_previous_resume = resume_after_key.map(str::to_string);
        match step {
            SnapshotSourceStep::Batch {
                collection_meta,
                batch,
            } => {
                let mut records = Vec::new();
                let mut serialized_record_bytes = 0_usize;
                for record in batch.records {
                    if range_digest_bucket_for_record(&collection_meta.name, &record.id, limits)?
                        != bucket
                    {
                        continue;
                    }
                    serialized_record_bytes = serialized_record_bytes
                        .checked_add(serde_json::to_vec(&record)?.len())
                        .ok_or_else(|| data_error("range digest bucket byte total overflow"))?;
                    records.push(record);
                }
                RangeDigestBucketScanStep::create(
                    session_id,
                    self.node_id.clone(),
                    range.id,
                    range.epoch,
                    state.resolved_through,
                    bucket,
                    expected_previous_resume,
                    Some(batch.resume_after_key),
                    Some(collection_meta.name),
                    records,
                    serialized_record_bytes,
                    false,
                    limits,
                )
            }
            SnapshotSourceStep::Advance {
                resume_after_key, ..
            } => RangeDigestBucketScanStep::create(
                session_id,
                self.node_id.clone(),
                range.id,
                range.epoch,
                state.resolved_through,
                bucket,
                expected_previous_resume,
                Some(resume_after_key),
                None,
                Vec::new(),
                0,
                false,
                limits,
            ),
            SnapshotSourceStep::Complete { .. } => RangeDigestBucketScanStep::create(
                session_id,
                self.node_id.clone(),
                range.id,
                range.epoch,
                state.resolved_through,
                bucket,
                expected_previous_resume,
                None,
                None,
                Vec::new(),
                0,
                true,
                limits,
            ),
        }
    }

    /// Apply one certified, bounded repair batch to this divergent voter. The
    /// operation is idempotent across the commit/checkpoint crash window: the
    /// same last batch may be replayed, while every other sequence mismatch
    /// fails closed. Completing input does not declare the range healthy.
    pub fn apply_range_digest_repair_batch(
        &mut self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        batch: &RangeDigestRepairBatch,
        limits: &RangeDigestRepairLimits,
        now_ms: u64,
    ) -> Result<RangeDigestRepairState> {
        batch.validate(limits)?;
        let current_voters = range
            .replicas
            .iter()
            .filter(|replica| replica.role == crate::distribution::RangeReplicaRole::Voter)
            .map(|replica| replica.node_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if batch.cluster_id != self.cluster_id
            || batch.destination_node_id != self.node_id
            || batch.range_id != range.id
            || batch.range_epoch != range.epoch
            || batch.authority.required_quorum != range.voter_count() / 2 + 1
            || !current_voters.contains(&batch.source_node_id)
            || !current_voters.contains(&batch.destination_node_id)
            || batch
                .authority
                .root_evidence
                .iter()
                .flat_map(|evidence| evidence.node_ids.iter())
                .any(|node_id| !current_voters.contains(node_id))
        {
            return Err(data_error(
                "range digest repair authority is outside the current cluster/range/voters",
            ));
        }

        let digest_path = self.range_digest_state_path(range.id, batch.digest_session_id);
        let digest_metadata = fs::symlink_metadata(&digest_path)?;
        if digest_metadata.file_type().is_symlink() || !digest_metadata.is_file() {
            return Err(data_error(
                "range digest repair requires a regular destination digest checkpoint",
            ));
        }
        let digest_state = load_range_digest_state(&digest_path, &limits.digest)?;
        let destination_manifest = digest_state.manifest(&limits.digest)?;
        if digest_state.node_id != self.node_id
            || digest_state.session_id != batch.digest_session_id
            || destination_manifest.root_sha256 != batch.destination_root_sha256
        {
            return Err(data_error(
                "range digest repair destination root does not match node-owned evidence",
            ));
        }
        self.ensure_range_digest_fence(range, caller, &digest_state, now_ms)?;

        let state_path =
            self.range_digest_repair_state_path(range.id, batch.bucket, batch.repair_id);
        let state = match fs::symlink_metadata(&state_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(data_error(
                        "range digest repair checkpoint is not a regular file",
                    ));
                }
                load_range_digest_repair_state(&state_path, limits)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.ensure_range_digest_repair_capacity(limits)?;
                let state = RangeDigestRepairState::create(batch)?;
                state.validate(limits)?;
                state
            }
            Err(error) => return Err(error.into()),
        };
        state.validate_batch_identity(batch)?;
        if batch.sequence == state.applied_batches
            && state.last_batch_sha256.as_deref() == Some(batch.checksum_sha256.as_str())
        {
            return Ok(state);
        }

        // Precompute the exact next durable state before touching rows. This
        // rejects gaps, conflicting retries, and post-completion writes first.
        let mut next = state;
        next.record_applied(batch, limits)?;
        self.ensure_range_digest_fence(range, caller, &digest_state, now_ms)?;
        self.db
            .read()
            .apply_range_anti_entropy_mutations_as_local(range, &batch.mutations)?;
        // The data RPC holds the node service mutex for this whole method, so a
        // prepare cannot release the fence between the two checks and commit.
        self.ensure_range_digest_fence(range, caller, &digest_state, now_ms)?;
        save_range_digest_repair_state(&state_path, &next, limits, self.fsync)?;
        Ok(next)
    }

    fn range_digest_state_path(&self, range_id: RangeId, session_id: Uuid) -> PathBuf {
        self.range_digest_root.join(format!(
            "range-{}-{}.json",
            range_id.get(),
            session_id.hyphenated()
        ))
    }

    fn range_digest_repair_state_path(
        &self,
        range_id: RangeId,
        bucket: u32,
        repair_id: Uuid,
    ) -> PathBuf {
        self.range_digest_repair_root.join(format!(
            "range-{}-bucket-{}-{}.json",
            range_id.get(),
            bucket,
            repair_id.hyphenated()
        ))
    }

    fn ensure_range_digest_repair_capacity(&self, limits: &RangeDigestRepairLimits) -> Result<()> {
        let mut retained = 0_usize;
        for entry in fs::read_dir(&self.range_digest_repair_root)? {
            entry?;
            retained = retained.saturating_add(1);
            if retained >= limits.max_retained_sessions {
                return Err(data_error(
                    "range digest repair checkpoint limit reached; archive completed sessions first",
                ));
            }
        }
        Ok(())
    }

    fn ensure_range_digest_capacity(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.range_digest_root)?;
        for retained in 0..MAX_RETAINED_RANGE_DIGEST_STATES {
            match entries.next() {
                Some(Ok(_)) if retained + 1 == MAX_RETAINED_RANGE_DIGEST_STATES => {
                    return Err(data_error(
                        "range digest checkpoint limit reached; archive completed sessions first",
                    ));
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
                None => return Ok(()),
            }
        }
        Err(data_error("range digest checkpoint capacity check failed"))
    }

    fn export_snapshot_step_from(
        &self,
        range: &RangeDescriptor,
        resume_after_key: Option<&str>,
        options: &RangeSnapshotOptions,
    ) -> Result<SnapshotSourceStep> {
        options.validate()?;
        let (collections, source_commit_sequence) = {
            let source = self.db.read();
            (source.collections(), source.current_commit_seq())
        };
        let cursor = resume_after_key.map(decode_cursor).transpose()?;
        let collection_index = match cursor.as_ref() {
            Some(cursor) => collections
                .iter()
                .position(|meta| meta.name == cursor.collection)
                .ok_or_else(|| {
                    data_error(format!(
                        "snapshot cursor collection `{}` disappeared",
                        cursor.collection
                    ))
                })?,
            None if collections.is_empty() => {
                return Ok(SnapshotSourceStep::Complete {
                    source_snapshot_commit_sequence: source_commit_sequence,
                });
            }
            None => 0,
        };
        let collection_meta = collections[collection_index].clone();
        let after_id = cursor
            .as_ref()
            .and_then(|cursor| cursor.after_id.as_deref());
        let mut exported_batch = None;
        let export = self.db.read().for_each_range_snapshot_batch(
            &collection_meta.name,
            range,
            after_id,
            options,
            |batch| {
                exported_batch = Some(batch);
                Ok(false)
            },
        )?;
        if let Some(mut batch) = exported_batch {
            batch.resume_after_key = encode_cursor(&CollectionScanCursor {
                collection: collection_meta.name.clone(),
                after_id: Some(batch.resume_after_key.clone()),
            })?;
            return Ok(SnapshotSourceStep::Batch {
                collection_meta,
                batch,
            });
        }
        debug_assert!(export.completed);
        if let Some(next) = collections.get(collection_index.saturating_add(1)) {
            return Ok(SnapshotSourceStep::Advance {
                resume_after_key: encode_cursor(&CollectionScanCursor {
                    collection: next.name.clone(),
                    after_id: None,
                })?,
                source_snapshot_commit_sequence: export.snapshot_commit_sequence,
            });
        }
        Ok(SnapshotSourceStep::Complete {
            source_snapshot_commit_sequence: source_commit_sequence,
        })
    }

    fn ensure_range_digest_fence(
        &self,
        range: &RangeDescriptor,
        caller: &ClusterNodeId,
        state: &RangeDigestState,
        now_ms: u64,
    ) -> Result<()> {
        self.validate_range_write_peer(range, caller)?;
        if state.cluster_id != self.cluster_id
            || state.node_id != self.node_id
            || state.range_id != range.id
            || state.range_epoch != range.epoch
            || state.start_token != range.start_token
            || state.end_token != range.end_token
        {
            return Err(data_error(
                "range digest identity does not match this replica or descriptor",
            ));
        }
        let fence = self
            .range_writes
            .active_backup_fence(range.id)
            .ok_or_else(|| data_error("range digest requires an active durable write fence"))?;
        if fence.plan_id != state.session_id
            || fence.range_id != range.id
            || fence.range_epoch != range.epoch
            || fence.resolved_through != state.resolved_through
            || now_ms < fence.installed_at_ms
            || now_ms >= fence.expires_at_ms
        {
            return Err(data_error(
                "range digest write fence identity, prefix, or lifetime mismatch",
            ));
        }
        let progress = self.range_writes.progress(range.id);
        if progress.current_epoch != range.epoch
            || progress.last_index != state.resolved_through
            || progress.resolved_through != state.resolved_through
        {
            return Err(data_error(
                "range digest consensus prefix moved or contains an unresolved tail",
            ));
        }
        Ok(())
    }

    pub fn apply_snapshot_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        step: &SnapshotSourceStep,
    ) -> Result<SnapshotCopyProgress> {
        self.ensure_target(relocation)?;
        match step {
            SnapshotSourceStep::Batch {
                collection_meta,
                batch,
            } => {
                let mut db = self.db.write();
                self.learner.apply_snapshot_batch(
                    &mut db,
                    relocation,
                    range,
                    relocation.snapshot_resume_after_key.as_deref(),
                    collection_meta,
                    batch,
                )
            }
            SnapshotSourceStep::Advance {
                resume_after_key,
                source_snapshot_commit_sequence,
            } => self.learner.checkpoint_snapshot_cursor(
                relocation,
                relocation.snapshot_resume_after_key.as_deref(),
                Some(resume_after_key.clone()),
                *source_snapshot_commit_sequence,
            ),
            SnapshotSourceStep::Complete {
                source_snapshot_commit_sequence,
            } => self.learner.finish_snapshot(
                relocation,
                relocation.snapshot_resume_after_key.as_deref(),
                *source_snapshot_commit_sequence,
            ),
        }
    }

    pub fn learner_durable_commit_sequence(&self, relocation: &RangeRelocation) -> Result<u64> {
        self.ensure_target(relocation)?;
        self.learner
            .state(relocation.id)
            .and_then(|state| state.durable_source_commit_sequence)
            .ok_or_else(|| {
                data_error(format!(
                    "learner {} has no durable watermark",
                    relocation.id
                ))
            })
    }

    pub fn export_catch_up(
        &self,
        range: &RangeDescriptor,
        durable_commit_sequence: u64,
        max_commit_frames: usize,
    ) -> Result<CatchUpSourceBatch> {
        self.ensure_snapshot_source(range)?;
        if max_commit_frames == 0 {
            return Err(data_error(
                "range catch-up frame limit must be greater than zero",
            ));
        }
        let source = self.db.read();
        let source_commit_sequence = source.current_commit_seq();
        if durable_commit_sequence > source_commit_sequence {
            return Err(data_error(format!(
                "destination watermark {durable_commit_sequence} is ahead of source {source_commit_sequence}"
            )));
        }
        let frames = if durable_commit_sequence < source_commit_sequence {
            source.export_replication_frames_since(durable_commit_sequence, max_commit_frames)?
        } else {
            Vec::new()
        };
        Ok(CatchUpSourceBatch {
            source_commit_sequence,
            frames,
        })
    }

    pub fn apply_catch_up(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        batch: &CatchUpSourceBatch,
    ) -> Result<CatchUpProgress> {
        self.ensure_target(relocation)?;
        let mut db = self.db.write();
        self.learner.apply_commit_frames(
            &mut db,
            relocation,
            range,
            &batch.frames,
            batch.source_commit_sequence,
        )
    }

    pub fn cleanup_source_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        resume_after_key: Option<&str>,
        max_records: usize,
    ) -> Result<CleanupProgress> {
        if relocation.source.as_ref() != Some(&self.node_id) {
            return Err(data_error(format!(
                "node {} is not cleanup source for {}",
                self.node_id, relocation.id
            )));
        }
        if max_records == 0 {
            return Err(data_error(
                "range cleanup record limit must be greater than zero",
            ));
        }
        let collections = self.db.read().collections();
        let cursor = resume_after_key.map(decode_cursor).transpose()?;
        let collection_index = match cursor.as_ref() {
            Some(cursor) => collections
                .iter()
                .position(|meta| meta.name == cursor.collection)
                .ok_or_else(|| {
                    data_error(format!(
                        "cleanup cursor collection `{}` disappeared",
                        cursor.collection
                    ))
                })?,
            None if collections.is_empty() => {
                return Ok(CleanupProgress {
                    resume_after_key: None,
                    records_deleted: relocation.cleanup_records_deleted,
                    completed: true,
                });
            }
            None => 0,
        };
        let collection = collections[collection_index].name.clone();
        let after_id = cursor
            .as_ref()
            .and_then(|cursor| cursor.after_id.as_deref());
        let options = RangeSnapshotOptions {
            max_records_per_batch: max_records,
            max_bytes_per_batch: 8 * 1024 * 1024,
            max_record_bytes: 8 * 1024 * 1024,
        };
        let mut ids = Vec::new();
        let export = self.db.read().for_each_range_snapshot_batch(
            &collection,
            range,
            after_id,
            &options,
            |batch| {
                ids = batch.records.into_iter().map(|record| record.id).collect();
                Ok(false)
            },
        )?;
        if !ids.is_empty() {
            let after_id = ids.last().cloned().expect("non-empty cleanup batch");
            let deleted =
                self.db
                    .write()
                    .delete_range_records_as_local(range, &collection, &ids)?;
            return Ok(CleanupProgress {
                resume_after_key: Some(encode_cursor(&CollectionScanCursor {
                    collection,
                    after_id: Some(after_id),
                })?),
                records_deleted: relocation
                    .cleanup_records_deleted
                    .saturating_add(deleted as u64),
                completed: false,
            });
        }
        debug_assert!(export.completed);
        if let Some(next) = collections.get(collection_index.saturating_add(1)) {
            return Ok(CleanupProgress {
                resume_after_key: Some(encode_cursor(&CollectionScanCursor {
                    collection: next.name.clone(),
                    after_id: None,
                })?),
                records_deleted: relocation.cleanup_records_deleted,
                completed: false,
            });
        }
        Ok(CleanupProgress {
            resume_after_key: None,
            records_deleted: relocation.cleanup_records_deleted,
            completed: true,
        })
    }

    pub fn complete_relocation(&mut self, relocation: &RangeRelocation) -> Result<bool> {
        self.ensure_target(relocation)?;
        self.learner.complete_relocation(relocation.id)
    }

    fn ensure_target(&self, relocation: &RangeRelocation) -> Result<()> {
        if relocation.target != self.node_id {
            return Err(data_error(format!(
                "node {} is not learner target for {}",
                self.node_id, relocation.id
            )));
        }
        Ok(())
    }

    fn ensure_snapshot_source(&self, range: &RangeDescriptor) -> Result<()> {
        if range.leader != self.node_id {
            return Err(data_error(format!(
                "node {} is not snapshot source leader for {}",
                self.node_id, range.id
            )));
        }
        Ok(())
    }
}

/// A complete physical relocation transport for embedded/multi-node tests and
/// single-process deployments. Network servers expose the same bounded node
/// operations over their authenticated cluster channel.
#[derive(Debug)]
pub struct InProcessClusterRelocationTransport {
    cluster_id: ClusterId,
    nodes: BTreeMap<ClusterNodeId, ClusterDataNodeService>,
}

impl InProcessClusterRelocationTransport {
    pub fn new(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id,
            nodes: BTreeMap::new(),
        }
    }

    pub fn register_node(
        &mut self,
        node_id: ClusterNodeId,
        root: impl AsRef<Path>,
        db: Arc<RwLock<BicDb>>,
        fsync: bool,
    ) -> Result<bool> {
        if self.nodes.contains_key(&node_id) {
            return Ok(false);
        }
        let service = ClusterDataNodeService::open(
            self.cluster_id.clone(),
            node_id.clone(),
            root,
            db,
            fsync,
        )?;
        self.nodes.insert(node_id, service);
        Ok(true)
    }

    pub fn database(&self, node_id: &ClusterNodeId) -> Option<Arc<RwLock<BicDb>>> {
        self.nodes
            .get(node_id)
            .map(ClusterDataNodeService::database)
    }

    fn node(&self, node_id: &ClusterNodeId) -> Result<&ClusterDataNodeService> {
        self.nodes
            .get(node_id)
            .ok_or_else(|| data_error(format!("cluster data node {node_id} is not registered")))
    }

    fn node_mut(&mut self, node_id: &ClusterNodeId) -> Result<&mut ClusterDataNodeService> {
        self.nodes
            .get_mut(node_id)
            .ok_or_else(|| data_error(format!("cluster data node {node_id} is not registered")))
    }

    fn snapshot_source(&self, range: &RangeDescriptor) -> Result<&ClusterDataNodeService> {
        self.node(&range.leader)
    }
}

impl ClusterRelocationTransport for InProcessClusterRelocationTransport {
    fn prepare_learner(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        let source_db = self.snapshot_source(range)?.database();
        let bundle = source_db.read().cluster_schema_bundle()?;
        let target = self.node_mut(&relocation.target)?;
        target.install_schema_bundle(relocation, range, &bundle)?;
        target.prepare_learner(relocation)
    }

    fn copy_snapshot_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        options: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotCopyProgress> {
        cancellation.check()?;
        let step = self
            .snapshot_source(range)?
            .export_snapshot_step(relocation, range, options)?;
        self.node_mut(&relocation.target)?
            .apply_snapshot_step(relocation, range, &step)
    }

    fn catch_up_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        max_commit_frames: usize,
        cancellation: &CancellationToken,
    ) -> Result<CatchUpProgress> {
        cancellation.check()?;
        let durable = self
            .node(&relocation.target)?
            .learner_durable_commit_sequence(relocation)?;
        let batch =
            self.snapshot_source(range)?
                .export_catch_up(range, durable, max_commit_frames)?;
        self.node_mut(&relocation.target)?
            .apply_catch_up(relocation, range, &batch)
    }

    fn cleanup_source(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        resume_after_key: Option<&str>,
        max_records: usize,
        cancellation: &CancellationToken,
    ) -> Result<CleanupProgress> {
        cancellation.check()?;
        let Some(source_id) = relocation.source.as_ref() else {
            self.node_mut(&relocation.target)?
                .complete_relocation(relocation)?;
            return Ok(CleanupProgress {
                resume_after_key: None,
                records_deleted: relocation.cleanup_records_deleted,
                completed: true,
            });
        };
        let progress = self.node_mut(source_id)?.cleanup_source_step(
            relocation,
            range,
            resume_after_key,
            max_records,
        )?;
        if progress.completed {
            self.node_mut(&relocation.target)?
                .complete_relocation(relocation)?;
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::{PlacementPolicy, RangeReplica, RangeReplicaRole, ReplicaId};
    use crate::distribution_anti_entropy_run::{
        RangeDigestRootEvidence, RangeDigestRunOutcome, RangeDigestRunReport,
    };
    use crate::CommitAdmissionMutation;
    use crate::{DbConfig, Record, StorageMode};
    use serde_json::json;

    fn whole_keyspace_range(node_id: &ClusterNodeId) -> RangeDescriptor {
        let peer_b = ClusterNodeId::new("node-b").unwrap();
        let peer_c = ClusterNodeId::new("node-c").unwrap();
        RangeDescriptor {
            id: RangeId::new(1).unwrap(),
            start_token: 0,
            end_token: None,
            epoch: 1,
            replicas: vec![
                RangeReplica {
                    id: ReplicaId::new(1).unwrap(),
                    node_id: node_id.clone(),
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId::new(2).unwrap(),
                    node_id: peer_b,
                    role: RangeReplicaRole::Voter,
                },
                RangeReplica {
                    id: ReplicaId::new(3).unwrap(),
                    node_id: peer_c,
                    role: RangeReplicaRole::Voter,
                },
            ],
            leader: node_id.clone(),
            approximate_bytes: 0,
            approximate_qps: 0,
            placement: PlacementPolicy::default(),
        }
    }

    #[test]
    fn live_digest_scan_requires_exact_unexpired_fence_and_resumes_in_bounds() {
        let directory = tempfile::tempdir().unwrap();
        let db_root = directory.path().join("database");
        let cluster_root = directory.path().join("cluster");
        std::fs::create_dir_all(&cluster_root).unwrap();
        let mut db = BicDb::open_with_config(
            &db_root,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();
        db.create_collection("empty_first").unwrap();
        db.create_collection("patients").unwrap();
        db.batch_insert(
            "patients",
            (0..7).map(|id| {
                Record::new(format!("patient-{id:02}"))
                    .with_metadata(json!({"ordinal": id, "tenant": "tenant-a"}))
            }),
        )
        .unwrap();
        drop(db);
        let db = BicDb::open_with_config(
            &db_root,
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap();

        let cluster_id = ClusterId::new("digest-cluster").unwrap();
        let node_id = ClusterNodeId::new("node-a").unwrap();
        let range = whole_keyspace_range(&node_id);
        let mut service = ClusterDataNodeService::open(
            cluster_id.clone(),
            node_id.clone(),
            &cluster_root,
            Arc::new(RwLock::new(db)),
            false,
        )
        .unwrap();
        let session_id = Uuid::new_v4();
        service
            .install_range_backup_fence(&range, &node_id, session_id, 100, 1_000)
            .unwrap();
        let limits = RangeDigestLimits {
            bucket_count: 16,
            max_records_per_batch: 2,
            max_bytes_per_batch: 4 * 1024,
            max_record_bytes: 4 * 1024,
            max_state_bytes: 64 * 1024,
        };
        let mut state = service
            .advance_persisted_range_digest(&range, &node_id, session_id, None, &limits, 200)
            .unwrap();
        assert_eq!(state.scanned_records, 0);

        let mut steps = 0;
        loop {
            steps += 1;
            assert!(steps < 20, "bounded digest scan did not terminate");
            let previous_checksum = state.checksum_sha256.clone();
            state = service
                .advance_persisted_range_digest(
                    &range,
                    &node_id,
                    session_id,
                    Some(&previous_checksum),
                    &limits,
                    200,
                )
                .unwrap();
            if steps == 2 {
                let db = service.database();
                drop(service);
                service = ClusterDataNodeService::open(
                    cluster_id.clone(),
                    node_id.clone(),
                    &cluster_root,
                    db,
                    false,
                )
                .unwrap();
                let recovered = service
                    .advance_persisted_range_digest(
                        &range, &node_id, session_id, None, &limits, 200,
                    )
                    .unwrap();
                assert_eq!(
                    recovered, state,
                    "restart must recover the exact checkpoint"
                );
            }
            if state.completed {
                break;
            }
        }
        let manifest = state.manifest(&limits).unwrap();
        assert!(steps >= 5, "tiny record bound should force several steps");
        assert_eq!(manifest.scanned_records, 7);
        assert_eq!(manifest.root_sha256, state.root_sha256.clone().unwrap());

        let bucket = range_digest_bucket_for_record("patients", "patient-00", &limits).unwrap();
        let mut bucket_cursor = None::<String>;
        let mut exported_ids = Vec::new();
        let mut last_nonempty_step = None;
        let mut bucket_complete = false;
        for _ in 0..20 {
            let step = service
                .export_range_digest_bucket_step(
                    &range,
                    &node_id,
                    session_id,
                    bucket,
                    bucket_cursor.as_deref(),
                    &limits,
                    200,
                )
                .unwrap();
            step.validate(&limits).unwrap();
            assert_eq!(
                step.expected_previous_resume.as_deref(),
                bucket_cursor.as_deref()
            );
            if !step.records.is_empty() {
                exported_ids.extend(step.records.iter().map(|record| record.id.clone()));
                last_nonempty_step = Some(step.clone());
            }
            if step.completed {
                bucket_complete = true;
                break;
            }
            bucket_cursor = step.resume_after_key.clone();
        }
        assert!(bucket_complete, "bounded bucket export did not terminate");
        assert!(exported_ids.iter().any(|id| id == "patient-00"));
        assert!(exported_ids.iter().all(|id| {
            range_digest_bucket_for_record("patients", id, &limits).unwrap() == bucket
        }));
        let mut tampered = last_nonempty_step.expect("target bucket must export a record");
        tampered.records[0].metadata = json!({"tampered": true});
        assert!(tampered.validate(&limits).is_err());

        let source_node = ClusterNodeId::new("node-b").unwrap();
        let source_peer = ClusterNodeId::new("node-c").unwrap();
        let source_root = "1".repeat(64);
        let mut root_evidence = vec![
            RangeDigestRootEvidence {
                root_sha256: source_root.clone(),
                node_ids: vec![source_node.clone(), source_peer],
            },
            RangeDigestRootEvidence {
                root_sha256: manifest.root_sha256.clone(),
                node_ids: vec![node_id.clone()],
            },
        ];
        root_evidence.sort_by(|left, right| left.root_sha256.cmp(&right.root_sha256));
        let authority = RangeDigestRunReport::create(
            session_id,
            cluster_id.clone(),
            range.id,
            range.epoch,
            manifest.resolved_through,
            2,
            RangeDigestRunOutcome::DivergentCertifiedSource,
            root_evidence,
            Some(source_root),
            vec![bucket],
        )
        .unwrap();
        let repair_limits = RangeDigestRepairLimits {
            digest: limits,
            max_mutations_per_batch: 2,
            max_bytes_per_batch: 4 * 1024,
            max_record_bytes: 4 * 1024,
            max_state_bytes: 64 * 1024,
            max_retained_sessions: 8,
        };
        let replacement = Record::new("patient-00")
            .with_metadata(json!({"ordinal": 0, "tenant": "tenant-a", "repaired": true}));
        let mutation = CommitAdmissionMutation {
            collection: "patients".to_string(),
            record_id: replacement.id.clone(),
            record: Some(replacement),
        };
        let mutation_bytes = serde_json::to_vec(&mutation).unwrap().len();
        let repair_id = Uuid::new_v4();
        let first_repair = RangeDigestRepairBatch::create(
            repair_id,
            source_node.clone(),
            node_id.clone(),
            bucket,
            1,
            None,
            vec![mutation],
            mutation_bytes,
            false,
            authority.clone(),
            &repair_limits,
        )
        .unwrap();
        let first_state = service
            .apply_range_digest_repair_batch(&range, &node_id, &first_repair, &repair_limits, 200)
            .unwrap();
        assert_eq!(first_state.applied_batches, 1);
        assert_eq!(first_state.applied_mutations, 1);
        assert_eq!(
            service
                .database()
                .read()
                .get("patients", "patient-00")
                .unwrap()
                .unwrap()
                .metadata["repaired"],
            json!(true)
        );
        assert_eq!(
            service
                .apply_range_digest_repair_batch(
                    &range,
                    &node_id,
                    &first_repair,
                    &repair_limits,
                    200,
                )
                .unwrap(),
            first_state
        );

        let final_repair = RangeDigestRepairBatch::create(
            repair_id,
            source_node,
            node_id.clone(),
            bucket,
            2,
            Some(first_repair.checksum_sha256.clone()),
            Vec::new(),
            0,
            true,
            authority,
            &repair_limits,
        )
        .unwrap();
        let completed_repair = service
            .apply_range_digest_repair_batch(&range, &node_id, &final_repair, &repair_limits, 200)
            .unwrap();
        assert!(completed_repair.input_complete);
        assert_eq!(completed_repair.applied_batches, 2);

        let db = service.database();
        drop(service);
        service = ClusterDataNodeService::open(
            cluster_id.clone(),
            node_id.clone(),
            &cluster_root,
            db,
            false,
        )
        .unwrap();
        assert_eq!(
            service
                .apply_range_digest_repair_batch(
                    &range,
                    &node_id,
                    &final_repair,
                    &repair_limits,
                    200,
                )
                .unwrap(),
            completed_repair
        );
        assert!(service
            .apply_range_digest_repair_batch(&range, &node_id, &first_repair, &repair_limits, 200,)
            .is_err());

        let wrong_session = Uuid::new_v4();
        assert!(service
            .advance_persisted_range_digest(&range, &node_id, wrong_session, None, &limits, 200,)
            .is_err());
        assert!(!service
            .range_digest_state_path(range.id, wrong_session)
            .exists());

        assert!(service
            .advance_persisted_range_digest(&range, &node_id, session_id, None, &limits, 1_000,)
            .is_err());
    }
}
