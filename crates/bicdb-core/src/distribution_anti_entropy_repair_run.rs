//! Disk-backed, crash-resumable merge orchestration for one divergent bucket.
//!
//! Source and destination streams are staged as immutable bounded frames. A
//! bounded two-way merge then emits canonical upsert/delete batches through
//! the destination-owned repair capability. No vector grows with bucket size.

use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::CommitAdmissionMutation;
use crate::distribution::{ClusterId, ClusterNodeId, RangeId};
use crate::distribution_anti_entropy::{
    append_range_digest_bucket_records, RangeDigestBucket, RangeDigestBucketScanStep,
};
use crate::distribution_anti_entropy_repair::{
    RangeDigestRepairBatch, RangeDigestRepairLimits, RangeDigestRepairTransport,
};
use crate::distribution_anti_entropy_run::{RangeDigestRun, RangeDigestRunReport};
use crate::error::{BicDbError, Result};
use crate::record::Record;

pub const RANGE_DIGEST_REPAIR_RUN_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE: &str = "repair-run.json";
const RANGE_DIGEST_REPAIR_RUN_LOCK: &str = ".repair-run.lock";

fn run_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range anti-entropy repair run: {}", message.into()))
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairRunLimits {
    pub repair: RangeDigestRepairLimits,
    pub max_steps: u64,
    pub max_stage_steps_per_side: u64,
    pub max_staged_bytes: u64,
    pub max_stage_file_bytes: u64,
    pub max_merge_work_per_step: usize,
    pub max_run_state_bytes: u64,
}

impl Default for RangeDigestRepairRunLimits {
    fn default() -> Self {
        Self {
            repair: RangeDigestRepairLimits::default(),
            max_steps: 1_000_000_000,
            max_stage_steps_per_side: 100_000_000,
            max_staged_bytes: 256 * 1024 * 1024 * 1024,
            max_stage_file_bytes: 16 * 1024 * 1024,
            max_merge_work_per_step: 8_192,
            max_run_state_bytes: 16 * 1024 * 1024,
        }
    }
}

impl RangeDigestRepairRunLimits {
    pub fn validate(&self) -> Result<()> {
        self.repair.validate()?;
        let minimum_stage_file = (self.repair.digest.max_bytes_per_batch as u64)
            .checked_add(64 * 1024)
            .ok_or_else(|| run_error("stage file bound overflow"))?;
        let minimum_batch_file = (self.repair.max_bytes_per_batch as u64)
            .checked_add(self.repair.max_state_bytes)
            .and_then(|value| value.checked_add(64 * 1024))
            .ok_or_else(|| run_error("repair batch file bound overflow"))?;
        if !(1..=1_000_000_000_000).contains(&self.max_steps)
            || !(1..=1_000_000_000).contains(&self.max_stage_steps_per_side)
            || !(1024 * 1024..=16 * 1024 * 1024 * 1024 * 1024 * 1024)
                .contains(&self.max_staged_bytes)
            || self.max_stage_file_bytes < minimum_stage_file.max(minimum_batch_file)
            || self.max_stage_file_bytes > 512 * 1024 * 1024
            || !(4..=10_000_000).contains(&self.max_merge_work_per_step)
            || !(64 * 1024..=64 * 1024 * 1024).contains(&self.max_run_state_bytes)
        {
            return Err(run_error(
                "step, staged-disk, file, merge-work, or state bound is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RangeDigestRepairRunPhase {
    StageSource,
    StageDestination,
    MergeApply,
    /// Compatibility boundary written by 1.0.36-beta. The next advance moves
    /// into a fresh destination scan; it never treats this phase as healthy.
    AwaitingVerification,
    VerifyDestination,
    CompareVerification,
    Verified,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeDigestRepairRun {
    pub format_version: u32,
    pub repair_id: Uuid,
    pub digest_session_id: Uuid,
    pub cluster_id: ClusterId,
    pub source_node_id: ClusterNodeId,
    pub destination_node_id: ClusterNodeId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub resolved_through: u64,
    pub bucket: u32,
    pub installed_at_ms: u64,
    pub expires_at_ms: u64,
    pub authority: RangeDigestRunReport,
    pub limits: RangeDigestRepairRunLimits,
    pub phase: RangeDigestRepairRunPhase,
    pub source_steps: u64,
    pub source_resume_after_key: Option<String>,
    pub source_last_collection: Option<String>,
    pub source_last_record_id: Option<String>,
    pub source_staged_bytes: u64,
    pub destination_steps: u64,
    pub destination_resume_after_key: Option<String>,
    pub destination_last_collection: Option<String>,
    pub destination_last_record_id: Option<String>,
    pub destination_staged_bytes: u64,
    pub repair_batch_bytes: u64,
    #[serde(default)]
    pub verification_steps: u64,
    #[serde(default)]
    pub verification_resume_after_key: Option<String>,
    #[serde(default)]
    pub verification_last_collection: Option<String>,
    #[serde(default)]
    pub verification_last_record_id: Option<String>,
    #[serde(default)]
    pub verification_staged_bytes: u64,
    #[serde(default)]
    pub verification_source_step: u64,
    #[serde(default)]
    pub verification_source_record: usize,
    #[serde(default)]
    pub verification_destination_step: u64,
    #[serde(default)]
    pub verification_destination_record: usize,
    pub merge_source_step: u64,
    pub merge_source_record: usize,
    pub merge_destination_step: u64,
    pub merge_destination_record: usize,
    pub next_batch_sequence: u64,
    pub previous_batch_sha256: Option<String>,
    pub applied_mutations: u64,
    pub completed_steps: u64,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeDigestRepairRunAdvance {
    Progress {
        phase: RangeDigestRepairRunPhase,
        completed_steps: u64,
        staged_bytes: u64,
        applied_mutations: u64,
    },
    AwaitingVerification {
        repair_id: Uuid,
        destination_node_id: ClusterNodeId,
        bucket: u32,
        applied_mutations: u64,
    },
    Verified {
        repair_id: Uuid,
        destination_node_id: ClusterNodeId,
        bucket: u32,
        applied_mutations: u64,
    },
}

#[derive(Clone, Copy, Debug)]
enum StageSide {
    Source,
    Destination,
    Verification,
}

impl StageSide {
    fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
            Self::Verification => "verification",
        }
    }
}

impl RangeDigestRepairRun {
    pub fn create(
        digest_run: &RangeDigestRun,
        repair_id: Uuid,
        source_node_id: ClusterNodeId,
        destination_node_id: ClusterNodeId,
        bucket: u32,
        limits: RangeDigestRepairRunLimits,
    ) -> Result<Self> {
        digest_run.validate()?;
        let authority = digest_run
            .report
            .clone()
            .ok_or_else(|| run_error("digest run is not complete"))?;
        Self::create_from_report(
            repair_id,
            source_node_id,
            destination_node_id,
            bucket,
            digest_run.installed_at_ms,
            digest_run.expires_at_ms,
            authority,
            limits,
        )
    }

    /// Rebuild the certified source bucket summary from immutable staged
    /// frames after the fresh destination comparison has completed.
    pub fn verified_source_bucket(&self, run_dir: impl AsRef<Path>) -> Result<RangeDigestBucket> {
        self.validate()?;
        if self.phase != RangeDigestRepairRunPhase::Verified {
            return Err(run_error(
                "source bucket evidence requires a verified repair run",
            ));
        }
        let run_dir = run_dir.as_ref();
        ensure_existing_run_directory(run_dir)?;
        let mut summary = RangeDigestBucket::empty(self.bucket, &self.limits.repair.digest)?;
        let mut expected_resume = None::<String>;
        let mut last_key = None::<(String, String)>;
        for index in 0..self.source_steps {
            let step = load_stage_step(
                &stage_step_path(run_dir, StageSide::Source, index),
                &self.limits,
            )?;
            self.validate_stage_step(
                StageSide::Source,
                &self.source_node_id,
                expected_resume.as_deref(),
                &step,
            )?;
            if step.completed {
                if index + 1 != self.source_steps {
                    return Err(run_error("source terminal frame is not last"));
                }
                expected_resume = None;
                continue;
            }
            if let (Some(collection), Some(first)) =
                (step.collection.as_ref(), step.records.first())
            {
                if last_key
                    .as_ref()
                    .is_some_and(|previous| previous >= &(collection.clone(), first.id.clone()))
                {
                    return Err(run_error(
                        "source evidence frames are globally out of order",
                    ));
                }
            }
            if let Some(collection) = step.collection.as_deref() {
                append_range_digest_bucket_records(
                    &mut summary,
                    collection,
                    &step.records,
                    &self.limits.repair.digest,
                )?;
                if let Some(last) = step.records.last() {
                    last_key = Some((collection.to_string(), last.id.clone()));
                }
            }
            expected_resume = step.resume_after_key.clone();
        }
        if expected_resume.is_some() {
            return Err(run_error("source evidence has no terminal frame"));
        }
        Ok(summary)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_from_report(
        repair_id: Uuid,
        source_node_id: ClusterNodeId,
        destination_node_id: ClusterNodeId,
        bucket: u32,
        installed_at_ms: u64,
        expires_at_ms: u64,
        authority: RangeDigestRunReport,
        limits: RangeDigestRepairRunLimits,
    ) -> Result<Self> {
        limits.validate()?;
        // Reuse the destination batch's complete authority validation without
        // granting health or applying data.
        RangeDigestRepairBatch::create(
            repair_id,
            source_node_id.clone(),
            destination_node_id.clone(),
            bucket,
            1,
            None,
            Vec::new(),
            0,
            true,
            authority.clone(),
            &limits.repair,
        )?;
        if installed_at_ms >= expires_at_ms {
            return Err(run_error("repair fence lifetime is invalid"));
        }
        let mut run = Self {
            format_version: RANGE_DIGEST_REPAIR_RUN_FORMAT_VERSION,
            repair_id,
            digest_session_id: authority.session_id,
            cluster_id: authority.cluster_id.clone(),
            source_node_id,
            destination_node_id,
            range_id: authority.range_id,
            range_epoch: authority.range_epoch,
            resolved_through: authority.resolved_through,
            bucket,
            installed_at_ms,
            expires_at_ms,
            authority,
            limits,
            phase: RangeDigestRepairRunPhase::StageSource,
            source_steps: 0,
            source_resume_after_key: None,
            source_last_collection: None,
            source_last_record_id: None,
            source_staged_bytes: 0,
            destination_steps: 0,
            destination_resume_after_key: None,
            destination_last_collection: None,
            destination_last_record_id: None,
            destination_staged_bytes: 0,
            repair_batch_bytes: 0,
            verification_steps: 0,
            verification_resume_after_key: None,
            verification_last_collection: None,
            verification_last_record_id: None,
            verification_staged_bytes: 0,
            verification_source_step: 0,
            verification_source_record: 0,
            verification_destination_step: 0,
            verification_destination_record: 0,
            merge_source_step: 0,
            merge_source_record: 0,
            merge_destination_step: 0,
            merge_destination_record: 0,
            next_batch_sequence: 1,
            previous_batch_sha256: None,
            applied_mutations: 0,
            completed_steps: 0,
            checksum_sha256: String::new(),
        };
        run.refresh_checksum()?;
        run.validate()?;
        Ok(run)
    }

    fn advance_uncheckpointed<T: RangeDigestRepairTransport>(
        &mut self,
        run_dir: &Path,
        transport: &T,
        now_ms: u64,
        fsync_artifacts: bool,
    ) -> Result<RangeDigestRepairRunAdvance> {
        self.validate()?;
        ensure_run_directory(run_dir)?;
        if self.phase == RangeDigestRepairRunPhase::Verified {
            return Ok(self.verified());
        }
        if now_ms < self.installed_at_ms || now_ms >= self.expires_at_ms {
            return Err(run_error("repair run fence is not currently valid"));
        }
        if self.completed_steps >= self.limits.max_steps {
            return Err(run_error("repair run exhausted its step budget"));
        }
        match self.phase {
            RangeDigestRepairRunPhase::StageSource => {
                self.advance_stage(
                    run_dir,
                    transport,
                    StageSide::Source,
                    now_ms,
                    fsync_artifacts,
                )?;
            }
            RangeDigestRepairRunPhase::StageDestination => {
                self.advance_stage(
                    run_dir,
                    transport,
                    StageSide::Destination,
                    now_ms,
                    fsync_artifacts,
                )?;
            }
            RangeDigestRepairRunPhase::MergeApply => {
                self.advance_merge(run_dir, transport, now_ms, fsync_artifacts)?;
            }
            RangeDigestRepairRunPhase::AwaitingVerification => {
                // Upgrade a durable 1.0.36 completion boundary. No previous
                // destination data is accepted as verification evidence.
                self.phase = RangeDigestRepairRunPhase::VerifyDestination;
            }
            RangeDigestRepairRunPhase::VerifyDestination => {
                self.advance_stage(
                    run_dir,
                    transport,
                    StageSide::Verification,
                    now_ms,
                    fsync_artifacts,
                )?;
            }
            RangeDigestRepairRunPhase::CompareVerification => {
                self.advance_verification_compare(run_dir)?;
            }
            RangeDigestRepairRunPhase::Verified => unreachable!(),
        }
        self.completed_steps = self
            .completed_steps
            .checked_add(1)
            .ok_or_else(|| run_error("repair run step total overflow"))?;
        self.refresh_checksum()?;
        self.validate()?;
        if self.phase == RangeDigestRepairRunPhase::Verified {
            Ok(self.verified())
        } else {
            Ok(RangeDigestRepairRunAdvance::Progress {
                phase: self.phase,
                completed_steps: self.completed_steps,
                staged_bytes: self
                    .source_staged_bytes
                    .saturating_add(self.destination_staged_bytes)
                    .saturating_add(self.repair_batch_bytes)
                    .saturating_add(self.verification_staged_bytes),
                applied_mutations: self.applied_mutations,
            })
        }
    }

    pub fn advance_and_checkpoint<T: RangeDigestRepairTransport>(
        &mut self,
        run_dir: impl AsRef<Path>,
        transport: &T,
        now_ms: u64,
        fsync: bool,
    ) -> Result<RangeDigestRepairRunAdvance> {
        let run_dir = run_dir.as_ref();
        ensure_run_directory(run_dir)?;
        // Keep one process as the sole owner across artifact creation, remote
        // apply, and the corresponding local checkpoint. The OS releases the
        // advisory lock automatically if that process dies.
        let _run_lock = acquire_run_lock(run_dir)?;
        let result = self.advance_uncheckpointed(run_dir, transport, now_ms, fsync)?;
        save_range_digest_repair_run(run_dir, self, fsync)?;
        Ok(result)
    }

    fn advance_stage<T: RangeDigestRepairTransport>(
        &mut self,
        run_dir: &Path,
        transport: &T,
        side: StageSide,
        now_ms: u64,
        fsync_artifacts: bool,
    ) -> Result<()> {
        let (node_id, step_index, expected_resume) = match side {
            StageSide::Source => (
                self.source_node_id.clone(),
                self.source_steps,
                self.source_resume_after_key.clone(),
            ),
            StageSide::Destination => (
                self.destination_node_id.clone(),
                self.destination_steps,
                self.destination_resume_after_key.clone(),
            ),
            StageSide::Verification => (
                self.destination_node_id.clone(),
                self.verification_steps,
                self.verification_resume_after_key.clone(),
            ),
        };
        if step_index >= self.limits.max_stage_steps_per_side {
            return Err(run_error(format!(
                "{} staging exhausted its step bound",
                side.label()
            )));
        }
        let path = stage_step_path(run_dir, side, step_index);
        let (step, needs_write) = match fs::symlink_metadata(&path) {
            Ok(_) => (load_stage_step(&path, &self.limits)?, false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let step = transport.export_range_digest_bucket(
                    &node_id,
                    self.range_id,
                    self.range_epoch,
                    self.digest_session_id,
                    self.bucket,
                    expected_resume.as_deref(),
                    &self.limits.repair.digest,
                    now_ms,
                )?;
                (step, true)
            }
            Err(error) => return Err(error.into()),
        };
        self.validate_stage_step(side, &node_id, expected_resume.as_deref(), &step)?;
        let encoded_bytes = serde_json::to_vec(&step)?.len() as u64;
        let total_staged = self
            .source_staged_bytes
            .checked_add(self.destination_staged_bytes)
            .and_then(|value| value.checked_add(self.repair_batch_bytes))
            .and_then(|value| value.checked_add(self.verification_staged_bytes))
            .and_then(|value| value.checked_add(encoded_bytes))
            .ok_or_else(|| run_error("staged byte total overflow"))?;
        if total_staged > self.limits.max_staged_bytes {
            return Err(run_error("repair staging exceeded its disk byte bound"));
        }
        self.validate_cross_frame_order(side, &step)?;
        if needs_write {
            save_stage_step(&path, &step, &self.limits, fsync_artifacts)?;
        }
        match side {
            StageSide::Source => {
                self.source_steps = self.source_steps.saturating_add(1);
                self.source_resume_after_key = step.resume_after_key.clone();
                self.source_staged_bytes = self
                    .source_staged_bytes
                    .checked_add(encoded_bytes)
                    .ok_or_else(|| run_error("source staged byte total overflow"))?;
                if let (Some(collection), Some(record)) =
                    (step.collection.as_ref(), step.records.last())
                {
                    self.source_last_collection = Some(collection.clone());
                    self.source_last_record_id = Some(record.id.clone());
                }
                if step.completed {
                    self.phase = RangeDigestRepairRunPhase::StageDestination;
                }
            }
            StageSide::Destination => {
                self.destination_steps = self.destination_steps.saturating_add(1);
                self.destination_resume_after_key = step.resume_after_key.clone();
                self.destination_staged_bytes = self
                    .destination_staged_bytes
                    .checked_add(encoded_bytes)
                    .ok_or_else(|| run_error("destination staged byte total overflow"))?;
                if let (Some(collection), Some(record)) =
                    (step.collection.as_ref(), step.records.last())
                {
                    self.destination_last_collection = Some(collection.clone());
                    self.destination_last_record_id = Some(record.id.clone());
                }
                if step.completed {
                    self.phase = RangeDigestRepairRunPhase::MergeApply;
                }
            }
            StageSide::Verification => {
                self.verification_steps = self.verification_steps.saturating_add(1);
                self.verification_resume_after_key = step.resume_after_key.clone();
                self.verification_staged_bytes = self
                    .verification_staged_bytes
                    .checked_add(encoded_bytes)
                    .ok_or_else(|| run_error("verification staged byte total overflow"))?;
                if let (Some(collection), Some(record)) =
                    (step.collection.as_ref(), step.records.last())
                {
                    self.verification_last_collection = Some(collection.clone());
                    self.verification_last_record_id = Some(record.id.clone());
                }
                if step.completed {
                    self.phase = RangeDigestRepairRunPhase::CompareVerification;
                }
            }
        }
        Ok(())
    }

    fn advance_merge<T: RangeDigestRepairTransport>(
        &mut self,
        run_dir: &Path,
        transport: &T,
        now_ms: u64,
        fsync_artifacts: bool,
    ) -> Result<()> {
        let mut source = StagedStreamReader::new(
            run_dir,
            StageSide::Source,
            self.source_steps,
            self.merge_source_step,
            self.merge_source_record,
            &self.limits,
        );
        let mut destination = StagedStreamReader::new(
            run_dir,
            StageSide::Destination,
            self.destination_steps,
            self.merge_destination_step,
            self.merge_destination_record,
            &self.limits,
        );
        let mut work = 0_usize;
        let mut mutations = Vec::new();
        let mut mutation_bytes = 0_usize;
        let mut input_complete = false;

        loop {
            if mutations.len() >= self.limits.repair.max_mutations_per_batch {
                break;
            }
            let source_record = source.peek(&mut work)?;
            let destination_record = destination.peek(&mut work)?;
            if matches!(source_record, StreamPeek::BudgetExhausted)
                || matches!(destination_record, StreamPeek::BudgetExhausted)
            {
                break;
            }
            if work >= self.limits.max_merge_work_per_step {
                break;
            }
            work += 1;
            let candidate = match (source_record, destination_record) {
                (StreamPeek::End, StreamPeek::End) => {
                    input_complete = true;
                    None
                }
                (StreamPeek::Record(source_record), StreamPeek::End) => {
                    let mutation = CommitAdmissionMutation {
                        collection: source_record.collection,
                        record_id: source_record.record.id.clone(),
                        record: Some(source_record.record),
                    };
                    Some((mutation, true, false))
                }
                (StreamPeek::End, StreamPeek::Record(destination_record)) => {
                    let mutation = CommitAdmissionMutation {
                        collection: destination_record.collection,
                        record_id: destination_record.record.id,
                        record: None,
                    };
                    Some((mutation, false, true))
                }
                (StreamPeek::Record(source_record), StreamPeek::Record(destination_record)) => {
                    let source_key = (
                        source_record.collection.as_str(),
                        source_record.record.id.as_str(),
                    );
                    let destination_key = (
                        destination_record.collection.as_str(),
                        destination_record.record.id.as_str(),
                    );
                    match source_key.cmp(&destination_key) {
                        Ordering::Less => {
                            let mutation = CommitAdmissionMutation {
                                collection: source_record.collection,
                                record_id: source_record.record.id.clone(),
                                record: Some(source_record.record),
                            };
                            Some((mutation, true, false))
                        }
                        Ordering::Greater => {
                            let mutation = CommitAdmissionMutation {
                                collection: destination_record.collection,
                                record_id: destination_record.record.id,
                                record: None,
                            };
                            Some((mutation, false, true))
                        }
                        Ordering::Equal => {
                            source.pop()?;
                            destination.pop()?;
                            if source_record.record == destination_record.record {
                                None
                            } else {
                                let mutation = CommitAdmissionMutation {
                                    collection: source_record.collection,
                                    record_id: source_record.record.id.clone(),
                                    record: Some(source_record.record),
                                };
                                let bytes = serde_json::to_vec(&mutation)?.len();
                                if !can_append_mutation(
                                    &mutations,
                                    mutation_bytes,
                                    bytes,
                                    &self.limits.repair,
                                )? {
                                    // Equal keys were consumed above. Rewind the
                                    // in-memory positions so the persisted cursor
                                    // cannot skip an unapplied replacement.
                                    source.rewind_one()?;
                                    destination.rewind_one()?;
                                    break;
                                }
                                mutation_bytes += bytes;
                                mutations.push(mutation);
                                continue;
                            }
                        }
                    }
                }
                (StreamPeek::BudgetExhausted, _) | (_, StreamPeek::BudgetExhausted) => {
                    unreachable!("budget handled above")
                }
            };
            if input_complete {
                break;
            }
            let Some((mutation, pop_source, pop_destination)) = candidate else {
                continue;
            };
            let bytes = serde_json::to_vec(&mutation)?.len();
            if !can_append_mutation(&mutations, mutation_bytes, bytes, &self.limits.repair)? {
                break;
            }
            if pop_source {
                source.pop()?;
            }
            if pop_destination {
                destination.pop()?;
            }
            mutation_bytes += bytes;
            mutations.push(mutation);
        }

        let (next_source_step, next_source_record) = source.position();
        let (next_destination_step, next_destination_record) = destination.position();
        if mutations.is_empty() && !input_complete {
            if (next_source_step, next_source_record)
                == (self.merge_source_step, self.merge_source_record)
                && (next_destination_step, next_destination_record)
                    == (self.merge_destination_step, self.merge_destination_record)
            {
                return Err(run_error(
                    "merge step exhausted work without cursor progress",
                ));
            }
            self.merge_source_step = next_source_step;
            self.merge_source_record = next_source_record;
            self.merge_destination_step = next_destination_step;
            self.merge_destination_record = next_destination_record;
            return Ok(());
        }

        let batch = RangeDigestRepairBatch::create(
            self.repair_id,
            self.source_node_id.clone(),
            self.destination_node_id.clone(),
            self.bucket,
            self.next_batch_sequence,
            self.previous_batch_sha256.clone(),
            mutations,
            mutation_bytes,
            input_complete,
            self.authority.clone(),
            &self.limits.repair,
        )?;
        let batch_path = repair_batch_path(run_dir, batch.sequence);
        let batch_bytes = serde_json::to_vec(&batch)?.len() as u64;
        let total_artifact_bytes = self
            .source_staged_bytes
            .checked_add(self.destination_staged_bytes)
            .and_then(|value| value.checked_add(self.repair_batch_bytes))
            .and_then(|value| value.checked_add(self.verification_staged_bytes))
            .and_then(|value| value.checked_add(batch_bytes))
            .ok_or_else(|| run_error("repair artifact byte total overflow"))?;
        if total_artifact_bytes > self.limits.max_staged_bytes {
            return Err(run_error("repair run exceeded its disk byte bound"));
        }
        match fs::symlink_metadata(&batch_path) {
            Ok(_) => {
                let existing: RangeDigestRepairBatch =
                    read_bounded_json(&batch_path, self.limits.max_stage_file_bytes)?;
                existing.validate(&self.limits.repair)?;
                if existing != batch {
                    return Err(run_error(
                        "pending repair batch differs from deterministic merge replay",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_bounded_json(
                    &batch_path,
                    &batch,
                    self.limits.max_stage_file_bytes,
                    fsync_artifacts,
                )?;
            }
            Err(error) => return Err(error.into()),
        }
        let destination_state = transport.apply_range_digest_repair(
            &self.destination_node_id,
            self.range_id,
            self.range_epoch,
            &batch,
            &self.limits.repair,
            now_ms,
        )?;
        if destination_state.applied_batches != batch.sequence
            || destination_state.last_batch_sha256.as_deref()
                != Some(batch.checksum_sha256.as_str())
        {
            return Err(run_error("destination acknowledged the wrong repair batch"));
        }
        self.merge_source_step = next_source_step;
        self.merge_source_record = next_source_record;
        self.merge_destination_step = next_destination_step;
        self.merge_destination_record = next_destination_record;
        self.next_batch_sequence = self
            .next_batch_sequence
            .checked_add(1)
            .ok_or_else(|| run_error("repair batch sequence overflow"))?;
        self.previous_batch_sha256 = Some(batch.checksum_sha256);
        self.applied_mutations = self
            .applied_mutations
            .checked_add(batch.mutations.len() as u64)
            .ok_or_else(|| run_error("repair mutation total overflow"))?;
        self.repair_batch_bytes = self
            .repair_batch_bytes
            .checked_add(batch_bytes)
            .ok_or_else(|| run_error("repair batch byte total overflow"))?;
        if input_complete {
            self.phase = RangeDigestRepairRunPhase::VerifyDestination;
        }
        Ok(())
    }

    fn advance_verification_compare(&mut self, run_dir: &Path) -> Result<()> {
        let mut source = StagedStreamReader::new(
            run_dir,
            StageSide::Source,
            self.source_steps,
            self.verification_source_step,
            self.verification_source_record,
            &self.limits,
        );
        let mut destination = StagedStreamReader::new(
            run_dir,
            StageSide::Verification,
            self.verification_steps,
            self.verification_destination_step,
            self.verification_destination_record,
            &self.limits,
        );
        let mut work = 0_usize;
        let mut complete = false;
        loop {
            let source_record = source.peek(&mut work)?;
            let destination_record = destination.peek(&mut work)?;
            if matches!(source_record, StreamPeek::BudgetExhausted)
                || matches!(destination_record, StreamPeek::BudgetExhausted)
                || work >= self.limits.max_merge_work_per_step
            {
                break;
            }
            work += 1;
            match (source_record, destination_record) {
                (StreamPeek::End, StreamPeek::End) => {
                    complete = true;
                    break;
                }
                (StreamPeek::Record(source_row), StreamPeek::Record(destination_row))
                    if source_row.collection == destination_row.collection
                        && source_row.record == destination_row.record =>
                {
                    source.pop()?;
                    destination.pop()?;
                }
                (StreamPeek::Record(source_row), StreamPeek::Record(destination_row)) => {
                    return Err(run_error(format!(
                        "fresh destination verification differs at {}/{} versus {}/{}",
                        source_row.collection,
                        source_row.record.id,
                        destination_row.collection,
                        destination_row.record.id
                    )));
                }
                (StreamPeek::End, StreamPeek::Record(destination_row)) => {
                    return Err(run_error(format!(
                        "fresh destination verification contains extra record {}/{}",
                        destination_row.collection, destination_row.record.id
                    )));
                }
                (StreamPeek::Record(source_row), StreamPeek::End) => {
                    return Err(run_error(format!(
                        "fresh destination verification is missing record {}/{}",
                        source_row.collection, source_row.record.id
                    )));
                }
                (StreamPeek::BudgetExhausted, _) | (_, StreamPeek::BudgetExhausted) => {
                    unreachable!("budget handled above")
                }
            }
        }
        let source_position = source.position();
        let destination_position = destination.position();
        if !complete
            && source_position
                == (
                    self.verification_source_step,
                    self.verification_source_record,
                )
            && destination_position
                == (
                    self.verification_destination_step,
                    self.verification_destination_record,
                )
        {
            return Err(run_error(
                "verification comparison exhausted work without cursor progress",
            ));
        }
        self.verification_source_step = source_position.0;
        self.verification_source_record = source_position.1;
        self.verification_destination_step = destination_position.0;
        self.verification_destination_record = destination_position.1;
        if complete {
            self.phase = RangeDigestRepairRunPhase::Verified;
        }
        Ok(())
    }

    fn validate_stage_step(
        &self,
        _side: StageSide,
        node_id: &ClusterNodeId,
        expected_resume: Option<&str>,
        step: &RangeDigestBucketScanStep,
    ) -> Result<()> {
        step.validate(&self.limits.repair.digest)?;
        if step.source_node_id != *node_id
            || step.session_id != self.digest_session_id
            || step.range_id != self.range_id
            || step.range_epoch != self.range_epoch
            || step.resolved_through != self.resolved_through
            || step.bucket != self.bucket
            || step.expected_previous_resume.as_deref() != expected_resume
        {
            return Err(run_error("staged bucket frame identity or cursor mismatch"));
        }
        Ok(())
    }

    fn validate_cross_frame_order(
        &self,
        side: StageSide,
        step: &RangeDigestBucketScanStep,
    ) -> Result<()> {
        let Some(first) = step.records.first() else {
            return Ok(());
        };
        let collection = step
            .collection
            .as_deref()
            .ok_or_else(|| run_error("staged records have no collection"))?;
        let previous = match side {
            StageSide::Source => self
                .source_last_collection
                .as_deref()
                .zip(self.source_last_record_id.as_deref()),
            StageSide::Destination => self
                .destination_last_collection
                .as_deref()
                .zip(self.destination_last_record_id.as_deref()),
            StageSide::Verification => self
                .verification_last_collection
                .as_deref()
                .zip(self.verification_last_record_id.as_deref()),
        };
        if previous.is_some_and(|previous| previous >= (collection, first.id.as_str())) {
            return Err(run_error(
                "staged bucket frames are duplicated or globally out of order",
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.authority.validate()?;
        RangeDigestRepairBatch::create(
            self.repair_id,
            self.source_node_id.clone(),
            self.destination_node_id.clone(),
            self.bucket,
            1,
            None,
            Vec::new(),
            0,
            true,
            self.authority.clone(),
            &self.limits.repair,
        )?;
        if self.format_version != RANGE_DIGEST_REPAIR_RUN_FORMAT_VERSION
            || self.repair_id.is_nil()
            || self.digest_session_id != self.authority.session_id
            || self.cluster_id != self.authority.cluster_id
            || self.range_id != self.authority.range_id
            || self.range_epoch != self.authority.range_epoch
            || self.resolved_through != self.authority.resolved_through
            || self.installed_at_ms >= self.expires_at_ms
            || self.source_steps > self.limits.max_stage_steps_per_side
            || self.destination_steps > self.limits.max_stage_steps_per_side
            || self.verification_steps > self.limits.max_stage_steps_per_side
            || self.completed_steps > self.limits.max_steps
            || self.source_staged_bytes > self.limits.max_staged_bytes
            || self.destination_staged_bytes > self.limits.max_staged_bytes
            || self.repair_batch_bytes > self.limits.max_staged_bytes
            || self.verification_staged_bytes > self.limits.max_staged_bytes
            || self
                .source_staged_bytes
                .saturating_add(self.destination_staged_bytes)
                .saturating_add(self.repair_batch_bytes)
                .saturating_add(self.verification_staged_bytes)
                > self.limits.max_staged_bytes
            || self.merge_source_step > self.source_steps
            || self.merge_destination_step > self.destination_steps
            || self.verification_source_step > self.source_steps
            || self.verification_destination_step > self.verification_steps
            || self.next_batch_sequence == 0
            || (self.next_batch_sequence == 1) != self.previous_batch_sha256.is_none()
        {
            return Err(run_error(
                "repair run identity, cursor, or bounds are invalid",
            ));
        }
        validate_optional_cursor(&self.source_resume_after_key)?;
        validate_optional_cursor(&self.destination_resume_after_key)?;
        validate_optional_cursor(&self.verification_resume_after_key)?;
        validate_last_key(&self.source_last_collection, &self.source_last_record_id)?;
        validate_last_key(
            &self.destination_last_collection,
            &self.destination_last_record_id,
        )?;
        validate_last_key(
            &self.verification_last_collection,
            &self.verification_last_record_id,
        )?;
        if let Some(previous) = &self.previous_batch_sha256 {
            validate_sha256(previous)?;
        }
        match self.phase {
            RangeDigestRepairRunPhase::StageSource => {
                if self.destination_steps != 0
                    || self.merge_source_step != 0
                    || self.merge_destination_step != 0
                    || self.verification_steps != 0
                {
                    return Err(run_error("source staging carries later-phase progress"));
                }
            }
            RangeDigestRepairRunPhase::StageDestination => {
                if self.source_steps == 0
                    || self.source_resume_after_key.is_some()
                    || self.merge_source_step != 0
                    || self.merge_destination_step != 0
                    || self.verification_steps != 0
                {
                    return Err(run_error(
                        "destination staging lacks completed source input",
                    ));
                }
            }
            RangeDigestRepairRunPhase::MergeApply
            | RangeDigestRepairRunPhase::AwaitingVerification => {
                if self.source_steps == 0
                    || self.destination_steps == 0
                    || self.source_resume_after_key.is_some()
                    || self.destination_resume_after_key.is_some()
                {
                    return Err(run_error("merge phase lacks completed staged inputs"));
                }
                if self.verification_steps != 0 {
                    return Err(run_error(
                        "pre-verification phase carries verification data",
                    ));
                }
            }
            RangeDigestRepairRunPhase::VerifyDestination => {
                if self.source_steps == 0
                    || self.destination_steps == 0
                    || self.source_resume_after_key.is_some()
                    || self.destination_resume_after_key.is_some()
                {
                    return Err(run_error("verification lacks completed repair inputs"));
                }
            }
            RangeDigestRepairRunPhase::CompareVerification
            | RangeDigestRepairRunPhase::Verified => {
                if self.verification_steps == 0
                    || self.verification_resume_after_key.is_some()
                    || self.source_resume_after_key.is_some()
                    || self.destination_resume_after_key.is_some()
                {
                    return Err(run_error(
                        "verification comparison lacks completed staged inputs",
                    ));
                }
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(run_error("repair run checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_run_state_bytes {
            return Err(run_error("repair run state exceeds its byte bound"));
        }
        Ok(())
    }

    fn verified(&self) -> RangeDigestRepairRunAdvance {
        RangeDigestRepairRunAdvance::Verified {
            repair_id: self.repair_id,
            destination_node_id: self.destination_node_id.clone(),
            bucket: self.bucket,
            applied_mutations: self.applied_mutations,
        }
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            repair_id: Uuid,
            digest_session_id: Uuid,
            cluster_id: &'a ClusterId,
            source_node_id: &'a ClusterNodeId,
            destination_node_id: &'a ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            resolved_through: u64,
            bucket: u32,
            installed_at_ms: u64,
            expires_at_ms: u64,
            authority: &'a RangeDigestRunReport,
            limits: RangeDigestRepairRunLimits,
            phase: RangeDigestRepairRunPhase,
            source_steps: u64,
            source_resume_after_key: &'a Option<String>,
            source_last_collection: &'a Option<String>,
            source_last_record_id: &'a Option<String>,
            source_staged_bytes: u64,
            destination_steps: u64,
            destination_resume_after_key: &'a Option<String>,
            destination_last_collection: &'a Option<String>,
            destination_last_record_id: &'a Option<String>,
            destination_staged_bytes: u64,
            repair_batch_bytes: u64,
            merge_source_step: u64,
            merge_source_record: usize,
            merge_destination_step: u64,
            merge_destination_record: usize,
            next_batch_sequence: u64,
            previous_batch_sha256: &'a Option<String>,
            applied_mutations: u64,
            completed_steps: u64,
            #[serde(skip_serializing_if = "is_zero_u64")]
            verification_steps: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            verification_resume_after_key: &'a Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            verification_last_collection: &'a Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            verification_last_record_id: &'a Option<String>,
            #[serde(skip_serializing_if = "is_zero_u64")]
            verification_staged_bytes: u64,
            #[serde(skip_serializing_if = "is_zero_u64")]
            verification_source_step: u64,
            #[serde(skip_serializing_if = "is_zero_usize")]
            verification_source_record: usize,
            #[serde(skip_serializing_if = "is_zero_u64")]
            verification_destination_step: u64,
            #[serde(skip_serializing_if = "is_zero_usize")]
            verification_destination_record: usize,
        }
        sha256_json(&Payload {
            format_version: self.format_version,
            repair_id: self.repair_id,
            digest_session_id: self.digest_session_id,
            cluster_id: &self.cluster_id,
            source_node_id: &self.source_node_id,
            destination_node_id: &self.destination_node_id,
            range_id: self.range_id,
            range_epoch: self.range_epoch,
            resolved_through: self.resolved_through,
            bucket: self.bucket,
            installed_at_ms: self.installed_at_ms,
            expires_at_ms: self.expires_at_ms,
            authority: &self.authority,
            limits: self.limits,
            phase: self.phase,
            source_steps: self.source_steps,
            source_resume_after_key: &self.source_resume_after_key,
            source_last_collection: &self.source_last_collection,
            source_last_record_id: &self.source_last_record_id,
            source_staged_bytes: self.source_staged_bytes,
            destination_steps: self.destination_steps,
            destination_resume_after_key: &self.destination_resume_after_key,
            destination_last_collection: &self.destination_last_collection,
            destination_last_record_id: &self.destination_last_record_id,
            destination_staged_bytes: self.destination_staged_bytes,
            repair_batch_bytes: self.repair_batch_bytes,
            merge_source_step: self.merge_source_step,
            merge_source_record: self.merge_source_record,
            merge_destination_step: self.merge_destination_step,
            merge_destination_record: self.merge_destination_record,
            next_batch_sequence: self.next_batch_sequence,
            previous_batch_sha256: &self.previous_batch_sha256,
            applied_mutations: self.applied_mutations,
            completed_steps: self.completed_steps,
            verification_steps: self.verification_steps,
            verification_resume_after_key: &self.verification_resume_after_key,
            verification_last_collection: &self.verification_last_collection,
            verification_last_record_id: &self.verification_last_record_id,
            verification_staged_bytes: self.verification_staged_bytes,
            verification_source_step: self.verification_source_step,
            verification_source_record: self.verification_source_record,
            verification_destination_step: self.verification_destination_step,
            verification_destination_record: self.verification_destination_record,
        })
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct StagedRecord {
    collection: String,
    record: Record,
}

#[derive(Clone, Debug)]
enum StreamPeek {
    Record(StagedRecord),
    End,
    BudgetExhausted,
}

struct StagedStreamReader<'a> {
    run_dir: &'a Path,
    side: StageSide,
    total_steps: u64,
    step_index: u64,
    record_index: usize,
    current: Option<RangeDigestBucketScanStep>,
    limits: &'a RangeDigestRepairRunLimits,
    last_pop: Option<(u64, usize)>,
}

impl<'a> StagedStreamReader<'a> {
    fn new(
        run_dir: &'a Path,
        side: StageSide,
        total_steps: u64,
        step_index: u64,
        record_index: usize,
        limits: &'a RangeDigestRepairRunLimits,
    ) -> Self {
        Self {
            run_dir,
            side,
            total_steps,
            step_index,
            record_index,
            current: None,
            limits,
            last_pop: None,
        }
    }

    fn peek(&mut self, work: &mut usize) -> Result<StreamPeek> {
        loop {
            if self.current.is_none() {
                if *work >= self.limits.max_merge_work_per_step {
                    return Ok(StreamPeek::BudgetExhausted);
                }
                if self.step_index >= self.total_steps {
                    return Err(run_error(format!(
                        "{} staged stream has no terminal frame",
                        self.side.label()
                    )));
                }
                let step = load_stage_step(
                    &stage_step_path(self.run_dir, self.side, self.step_index),
                    self.limits,
                )?;
                *work += 1;
                self.current = Some(step);
            }
            let current = self.current.as_ref().expect("loaded staged frame");
            if current.completed {
                if self.record_index != 0 || self.step_index + 1 != self.total_steps {
                    return Err(run_error("terminal staged frame is not canonical"));
                }
                return Ok(StreamPeek::End);
            }
            if let Some(record) = current.records.get(self.record_index) {
                let collection = current
                    .collection
                    .clone()
                    .ok_or_else(|| run_error("staged record frame has no collection"))?;
                return Ok(StreamPeek::Record(StagedRecord {
                    collection,
                    record: record.clone(),
                }));
            }
            self.step_index = self.step_index.saturating_add(1);
            self.record_index = 0;
            self.current = None;
        }
    }

    fn pop(&mut self) -> Result<()> {
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| run_error("cannot consume an unloaded staged record"))?;
        if self.record_index >= current.records.len() {
            return Err(run_error("cannot consume past a staged frame"));
        }
        self.last_pop = Some((self.step_index, self.record_index));
        self.record_index = self.record_index.saturating_add(1);
        Ok(())
    }

    fn rewind_one(&mut self) -> Result<()> {
        let Some((step, record)) = self.last_pop.take() else {
            return Err(run_error("merge cursor has no record to rewind"));
        };
        if self.step_index != step || self.record_index != record.saturating_add(1) {
            return Err(run_error("merge cursor rewind crossed a frame boundary"));
        }
        self.record_index = record;
        Ok(())
    }

    fn position(&self) -> (u64, usize) {
        (self.step_index, self.record_index)
    }
}

fn can_append_mutation(
    mutations: &[CommitAdmissionMutation],
    current_bytes: usize,
    candidate_bytes: usize,
    limits: &RangeDigestRepairLimits,
) -> Result<bool> {
    if candidate_bytes > limits.max_record_bytes || candidate_bytes > limits.max_bytes_per_batch {
        return Err(run_error(
            "one merged mutation exceeds its configured bound",
        ));
    }
    if mutations.len() >= limits.max_mutations_per_batch {
        return Ok(false);
    }
    let next = current_bytes
        .checked_add(candidate_bytes)
        .ok_or_else(|| run_error("merged mutation byte total overflow"))?;
    Ok(next <= limits.max_bytes_per_batch)
}

pub fn save_range_digest_repair_run(
    run_dir: impl AsRef<Path>,
    run: &RangeDigestRepairRun,
    fsync: bool,
) -> Result<()> {
    run.validate()?;
    let run_dir = run_dir.as_ref();
    ensure_run_directory(run_dir)?;
    write_bounded_json(
        &run_dir.join(DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE),
        run,
        run.limits.max_run_state_bytes,
        fsync,
    )
}

pub fn load_range_digest_repair_run(
    run_dir: impl AsRef<Path>,
    limits: &RangeDigestRepairRunLimits,
) -> Result<RangeDigestRepairRun> {
    limits.validate()?;
    let run_dir = run_dir.as_ref();
    ensure_existing_run_directory(run_dir)?;
    let run: RangeDigestRepairRun = read_bounded_json(
        &run_dir.join(DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE),
        limits.max_run_state_bytes,
    )?;
    if run.limits != *limits {
        return Err(run_error("repair run limits changed across restart"));
    }
    run.validate()?;
    Ok(run)
}

fn acquire_run_lock(run_dir: &Path) -> Result<File> {
    let path = run_dir.join(RANGE_DIGEST_REPAIR_RUN_LOCK);
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;

        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?
    };
    #[cfg(not(unix))]
    let file = {
        if fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(run_error("repair run lock must not be a symbolic link"));
        }
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?
    };
    file.try_lock()
        .map_err(|error| run_error(format!("repair run is already owned: {error}")))?;
    Ok(file)
}

fn save_stage_step(
    path: &Path,
    step: &RangeDigestBucketScanStep,
    limits: &RangeDigestRepairRunLimits,
    fsync: bool,
) -> Result<()> {
    step.validate(&limits.repair.digest)?;
    write_bounded_json(path, step, limits.max_stage_file_bytes, fsync)
}

fn load_stage_step(
    path: &Path,
    limits: &RangeDigestRepairRunLimits,
) -> Result<RangeDigestBucketScanStep> {
    let step: RangeDigestBucketScanStep = read_bounded_json(path, limits.max_stage_file_bytes)?;
    step.validate(&limits.repair.digest)?;
    Ok(step)
}

fn write_bounded_json(
    path: &Path,
    value: &impl Serialize,
    max_bytes: u64,
    fsync: bool,
) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.is_empty() || bytes.len() as u64 > max_bytes {
        return Err(run_error("repair run artifact exceeds its write bound"));
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn read_bounded_json<T: DeserializeOwned>(path: &Path, max_bytes: u64) -> Result<T> {
    let file = open_no_follow(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(run_error(
            "repair run artifact is unsafe or outside its bound",
        ));
    }
    let length = metadata.len();
    let mut bytes = Vec::with_capacity(
        usize::try_from(length).map_err(|_| run_error("repair run artifact is too large"))?,
    );
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > max_bytes {
        return Err(run_error(
            "repair run artifact changed or grew while reading",
        ));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn open_no_follow(path: &Path) -> Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(Into::into)
    }
    #[cfg(not(unix))]
    {
        if fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(run_error("repair run artifact must not be a symbolic link"));
        }
        OpenOptions::new().read(true).open(path).map_err(Into::into)
    }
}

fn ensure_run_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(run_error("repair run path is not a safe directory"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)?;
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(run_error("repair run path is not a safe directory"));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn ensure_existing_run_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(run_error("repair run path is not a safe directory"));
    }
    Ok(())
}

fn stage_step_path(run_dir: &Path, side: StageSide, index: u64) -> PathBuf {
    run_dir.join(format!("{}-step-{index:020}.json", side.label()))
}

fn repair_batch_path(run_dir: &Path, sequence: u64) -> PathBuf {
    run_dir.join(format!("repair-batch-{sequence:020}.json"))
}

fn validate_optional_cursor(cursor: &Option<String>) -> Result<()> {
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 16 * 1024)
    {
        return Err(run_error("repair run cursor is outside its bound"));
    }
    Ok(())
}

fn validate_last_key(collection: &Option<String>, record_id: &Option<String>) -> Result<()> {
    if collection.is_some() != record_id.is_some()
        || collection
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 1024)
        || record_id
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 16 * 1024)
    {
        return Err(run_error("repair run last staged key is invalid"));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(run_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

fn sha256_json(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use parking_lot::Mutex;
    use serde_json::json;

    use crate::distribution::RangeId;
    use crate::distribution_anti_entropy::{
        range_digest_bucket_for_record, RangeDigestLimits, RangeDigestState, RangeDigestTransport,
    };
    use crate::distribution_anti_entropy_repair::{
        RangeDigestRepairState, RangeDigestRepairTransport,
    };
    use crate::distribution_anti_entropy_run::{
        RangeDigestRootEvidence, RangeDigestRunOutcome, RangeDigestRunReport,
    };

    #[derive(Debug)]
    struct TestRepairTransport {
        session_id: Uuid,
        range_id: RangeId,
        range_epoch: u64,
        resolved_through: u64,
        bucket: u32,
        source_node_id: ClusterNodeId,
        destination_node_id: ClusterNodeId,
        source: BTreeMap<String, Record>,
        destination: Mutex<BTreeMap<String, Record>>,
        destination_state: Mutex<Option<RangeDigestRepairState>>,
        export_calls: Mutex<BTreeMap<ClusterNodeId, u64>>,
        lose_apply_response_once: Mutex<bool>,
    }

    impl RangeDigestTransport for TestRepairTransport {
        fn advance_range_digest(
            &self,
            _destination: &ClusterNodeId,
            _range_id: RangeId,
            _range_epoch: u64,
            _session_id: Uuid,
            _expected_checksum_sha256: Option<&str>,
            _limits: &RangeDigestLimits,
            _now_ms: u64,
        ) -> Result<RangeDigestState> {
            Err(run_error("test repair transport does not advance digests"))
        }

        fn export_range_digest_bucket(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            session_id: Uuid,
            bucket: u32,
            resume_after_key: Option<&str>,
            limits: &RangeDigestLimits,
            _now_ms: u64,
        ) -> Result<RangeDigestBucketScanStep> {
            if range_id != self.range_id
                || range_epoch != self.range_epoch
                || session_id != self.session_id
                || bucket != self.bucket
            {
                return Err(run_error("test export identity mismatch"));
            }
            *self
                .export_calls
                .lock()
                .entry(destination.clone())
                .or_default() += 1;
            let records = if destination == &self.source_node_id {
                self.source.values().cloned().collect::<Vec<_>>()
            } else if destination == &self.destination_node_id {
                self.destination
                    .lock()
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                return Err(run_error("test export names an unknown node"));
            };
            let index = match resume_after_key {
                Some(value) => value
                    .strip_prefix("cursor-")
                    .ok_or_else(|| run_error("test cursor prefix mismatch"))?
                    .parse::<usize>()
                    .map_err(|_| run_error("test cursor is not numeric"))?,
                None => 0,
            };
            let start = index.saturating_mul(2);
            if start >= records.len() {
                return RangeDigestBucketScanStep::create(
                    session_id,
                    destination.clone(),
                    range_id,
                    range_epoch,
                    self.resolved_through,
                    bucket,
                    resume_after_key.map(str::to_string),
                    None,
                    None,
                    Vec::new(),
                    0,
                    true,
                    limits,
                );
            }
            let batch = records.into_iter().skip(start).take(2).collect::<Vec<_>>();
            let bytes = batch
                .iter()
                .map(|record| serde_json::to_vec(record).unwrap().len())
                .sum();
            RangeDigestBucketScanStep::create(
                session_id,
                destination.clone(),
                range_id,
                range_epoch,
                self.resolved_through,
                bucket,
                resume_after_key.map(str::to_string),
                Some(format!("cursor-{}", index + 1)),
                Some("items".to_string()),
                batch,
                bytes,
                false,
                limits,
            )
        }
    }

    impl RangeDigestRepairTransport for TestRepairTransport {
        fn apply_range_digest_repair(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            batch: &RangeDigestRepairBatch,
            limits: &RangeDigestRepairLimits,
            _now_ms: u64,
        ) -> Result<RangeDigestRepairState> {
            batch.validate(limits)?;
            if destination != &self.destination_node_id
                || range_id != self.range_id
                || range_epoch != self.range_epoch
            {
                return Err(run_error("test apply identity mismatch"));
            }
            let mut state_guard = self.destination_state.lock();
            if let Some(existing) = state_guard.as_ref() {
                existing.validate_batch_identity(batch)?;
                if batch.sequence == existing.applied_batches
                    && existing.last_batch_sha256.as_deref() == Some(batch.checksum_sha256.as_str())
                {
                    return Ok(existing.clone());
                }
            }
            let mut next = state_guard
                .clone()
                .unwrap_or(RangeDigestRepairState::create(batch)?);
            next.record_applied(batch, limits)?;
            let mut rows = self.destination.lock();
            for mutation in &batch.mutations {
                match &mutation.record {
                    Some(record) => {
                        rows.insert(mutation.record_id.clone(), record.clone());
                    }
                    None => {
                        rows.remove(&mutation.record_id);
                    }
                }
            }
            *state_guard = Some(next.clone());
            if std::mem::take(&mut *self.lose_apply_response_once.lock()) {
                return Err(run_error("simulated lost repair apply response"));
            }
            Ok(next)
        }
    }

    fn bucket_records(bucket_count: usize) -> (u32, Vec<String>) {
        let first = "item-00000";
        let limits = RangeDigestLimits {
            bucket_count,
            ..RangeDigestLimits::default()
        };
        let bucket = range_digest_bucket_for_record("items", first, &limits).unwrap();
        let mut ids = (0..100_000)
            .map(|value| format!("item-{value:05}"))
            .filter(|id| range_digest_bucket_for_record("items", id, &limits).unwrap() == bucket)
            .take(5)
            .collect::<Vec<_>>();
        ids.sort();
        (bucket, ids)
    }

    #[test]
    fn disk_backed_merge_recovers_orphan_frames_and_lost_apply_responses() {
        let digest_limits = RangeDigestLimits {
            bucket_count: 16,
            max_records_per_batch: 2,
            max_bytes_per_batch: 4 * 1024,
            max_record_bytes: 4 * 1024,
            max_state_bytes: 64 * 1024,
        };
        let limits = RangeDigestRepairRunLimits {
            repair: RangeDigestRepairLimits {
                digest: digest_limits,
                max_mutations_per_batch: 1,
                max_bytes_per_batch: 4 * 1024,
                max_record_bytes: 4 * 1024,
                max_state_bytes: 64 * 1024,
                max_retained_sessions: 8,
            },
            max_steps: 100,
            max_stage_steps_per_side: 20,
            max_staged_bytes: 1024 * 1024,
            max_stage_file_bytes: 256 * 1024,
            max_merge_work_per_step: 4,
            max_run_state_bytes: 128 * 1024,
        };
        let (bucket, ids) = bucket_records(digest_limits.bucket_count);
        let source_node = ClusterNodeId::new("node-a").unwrap();
        let source_peer = ClusterNodeId::new("node-b").unwrap();
        let destination_node = ClusterNodeId::new("node-c").unwrap();
        let source_root = "1".repeat(64);
        let destination_root = "e".repeat(64);
        let session_id = Uuid::new_v4();
        let cluster_id = ClusterId::new("repair-run-cluster").unwrap();
        let range_id = RangeId::new(7).unwrap();
        let authority = RangeDigestRunReport::create(
            session_id,
            cluster_id,
            range_id,
            2,
            11,
            2,
            RangeDigestRunOutcome::DivergentCertifiedSource,
            vec![
                RangeDigestRootEvidence {
                    root_sha256: source_root.clone(),
                    node_ids: vec![source_node.clone(), source_peer],
                },
                RangeDigestRootEvidence {
                    root_sha256: destination_root,
                    node_ids: vec![destination_node.clone()],
                },
            ],
            Some(source_root),
            vec![bucket],
        )
        .unwrap();
        let record = |id: &str, value: u64| Record::new(id).with_metadata(json!({"value": value}));
        let source = BTreeMap::from([
            (ids[0].clone(), record(&ids[0], 0)),
            (ids[1].clone(), record(&ids[1], 100)),
            (ids[2].clone(), record(&ids[2], 2)),
            (ids[4].clone(), record(&ids[4], 4)),
        ]);
        let destination = BTreeMap::from([
            (ids[0].clone(), record(&ids[0], 0)),
            (ids[1].clone(), record(&ids[1], 1)),
            (ids[3].clone(), record(&ids[3], 3)),
            (ids[4].clone(), record(&ids[4], 4)),
        ]);
        let transport = TestRepairTransport {
            session_id,
            range_id,
            range_epoch: 2,
            resolved_through: 11,
            bucket,
            source_node_id: source_node.clone(),
            destination_node_id: destination_node.clone(),
            source: source.clone(),
            destination: Mutex::new(destination),
            destination_state: Mutex::new(None),
            export_calls: Mutex::new(BTreeMap::new()),
            lose_apply_response_once: Mutex::new(true),
        };
        let directory = tempfile::tempdir().unwrap();
        let run_dir = directory.path().join("repair-run");
        let run = RangeDigestRepairRun::create_from_report(
            Uuid::new_v4(),
            source_node.clone(),
            destination_node,
            bucket,
            10,
            10_000,
            authority,
            limits,
        )
        .unwrap();

        // A 1.0.36 checkpoint has none of the verification fields. Defaulted
        // fields are omitted from the checksum payload so it resumes into the
        // fresh scan instead of forcing a bucket rebuild.
        let legacy_dir = directory.path().join("legacy-repair-run");
        fs::create_dir_all(&legacy_dir).unwrap();
        let mut legacy_json = serde_json::to_value(&run).unwrap();
        let legacy_object = legacy_json.as_object_mut().unwrap();
        for field in [
            "verification_steps",
            "verification_resume_after_key",
            "verification_last_collection",
            "verification_last_record_id",
            "verification_staged_bytes",
            "verification_source_step",
            "verification_source_record",
            "verification_destination_step",
            "verification_destination_record",
        ] {
            legacy_object.remove(field);
        }
        fs::write(
            legacy_dir.join(DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE),
            serde_json::to_vec(&legacy_json).unwrap(),
        )
        .unwrap();
        assert_eq!(
            load_range_digest_repair_run(&legacy_dir, &limits).unwrap(),
            run
        );

        save_range_digest_repair_run(&run_dir, &run, false).unwrap();

        let competing_owner = acquire_run_lock(&run_dir).unwrap();
        let mut refused = run.clone();
        assert!(refused
            .advance_and_checkpoint(&run_dir, &transport, 100, false)
            .unwrap_err()
            .to_string()
            .contains("already owned"));
        drop(competing_owner);

        // Write the first immutable frame but simulate coordinator death before
        // its state checkpoint. Recovery must adopt it without another export.
        let mut abandoned = run.clone();
        abandoned
            .advance_uncheckpointed(&run_dir, &transport, 100, false)
            .unwrap();
        assert_eq!(transport.export_calls.lock()[&source_node], 1);
        let mut recovered = load_range_digest_repair_run(&run_dir, &limits).unwrap();
        recovered
            .advance_and_checkpoint(&run_dir, &transport, 100, false)
            .unwrap();
        assert_eq!(transport.export_calls.lock()[&source_node], 1);

        let mut finished = false;
        for _ in 0..100 {
            match recovered.advance_and_checkpoint(&run_dir, &transport, 100, false) {
                Ok(RangeDigestRepairRunAdvance::Progress { .. }) => {}
                Ok(RangeDigestRepairRunAdvance::Verified { .. }) => {
                    finished = true;
                    break;
                }
                Ok(RangeDigestRepairRunAdvance::AwaitingVerification { .. }) => {}
                Err(error) if error.to_string().contains("simulated lost") => {
                    recovered = load_range_digest_repair_run(&run_dir, &limits).unwrap();
                }
                Err(error) => panic!("unexpected repair run error: {error}"),
            }
        }
        assert!(finished, "repair run did not reach verification boundary");
        assert_eq!(*transport.destination.lock(), source);
        assert_eq!(recovered.applied_mutations, 3);
        assert!(recovered.repair_batch_bytes > 0);
        assert!(
            recovered
                .source_staged_bytes
                .saturating_add(recovered.destination_staged_bytes)
                .saturating_add(recovered.repair_batch_bytes)
                .saturating_add(recovered.verification_staged_bytes)
                <= limits.max_staged_bytes
        );
        assert_eq!(recovered.phase, RangeDigestRepairRunPhase::Verified);
        let verified_bucket = recovered.verified_source_bucket(&run_dir).unwrap();
        assert_eq!(verified_bucket.bucket, bucket);
        assert_eq!(verified_bucket.records, source.len() as u64);
        assert_eq!(
            load_range_digest_repair_run(&run_dir, &limits).unwrap(),
            recovered
        );

        let state_path = run_dir.join(DEFAULT_RANGE_DIGEST_REPAIR_RUN_STATE);
        let mut damaged = fs::read(&state_path).unwrap();
        let middle = damaged.len() / 2;
        damaged[middle] ^= 1;
        fs::write(&state_path, damaged).unwrap();
        assert!(load_range_digest_repair_run(&run_dir, &limits).is_err());
    }
}
