//! Durable, resource-governed supervision for bounded server-paged checkpoints.
//!
//! Page code owns the idempotent drain/freeze/finalize state machine. This
//! module makes its inclusive cursor durable, fences stale workers with an
//! operation UUID, admits every step through the compaction lane, and publishes
//! retry, pause, or completion state atomically.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bicdb_page::{
    PagedCheckpointCursor, PagedCheckpointLimits, PagedCheckpointPhase, PagedCheckpointStepReport,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::paged_collection::PagedRecords;
use crate::paged_maintenance::{load_identity, load_or_create_identity};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION: u32 = 2;
pub const DEFAULT_PAGED_CHECKPOINT_SCHEDULE: &str = "maintenance/paged/checkpoint.json";

const MIN_SCHEDULE_BYTES: u64 = 4 * 1024;
const MAX_SCHEDULE_BYTES: u64 = 1024 * 1024;
const MAX_ERROR_BYTES: usize = 4 * 1024;
const MAX_DELAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;

fn checkpoint_error(message: impl Into<String>) -> BicDbError {
    BicDbError::PagedStorage(format!("paged checkpoint maintenance: {}", message.into()))
}

/// Complete non-weakenable policy for one supervised checkpoint operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointScheduleLimits {
    pub checkpoint: PagedCheckpointLimits,
    pub step_interval_ms: u64,
    pub saturation_retry_ms: u64,
    pub failure_retry_base_ms: u64,
    pub failure_retry_max_ms: u64,
    pub max_consecutive_failures: u32,
    pub max_schedule_state_bytes: u64,
    pub demand: ResourceDemand,
}

impl Default for PagedCheckpointScheduleLimits {
    fn default() -> Self {
        let checkpoint = PagedCheckpointLimits::default();
        let logical_io = checkpoint
            .writeback
            .max_io_bytes
            .saturating_add(checkpoint.tail_reclaim.max_io_bytes);
        Self {
            checkpoint,
            step_interval_ms: 100,
            saturation_retry_ms: 1_000,
            failure_retry_base_ms: 1_000,
            failure_retry_max_ms: 5 * 60 * 1_000,
            max_consecutive_failures: 32,
            max_schedule_state_bytes: 64 * 1024,
            demand: ResourceDemand {
                memory_bytes: 2 * 1024 * 1024,
                io_bytes: logical_io,
                cpu_slots: 1,
                io_charge_bytes: logical_io,
            },
        }
    }
}

impl PagedCheckpointScheduleLimits {
    pub fn validate(&self) -> Result<()> {
        self.demand.validate()?;
        let candidate_bytes = self.checkpoint.writeback.max_candidates.saturating_mul(8);
        let minimum_memory = candidate_bytes.saturating_add(64 * 1024);
        let logical_io = self
            .checkpoint
            .writeback
            .max_io_bytes
            .checked_add(self.checkpoint.tail_reclaim.max_io_bytes)
            .ok_or_else(|| checkpoint_error("checkpoint I/O envelope overflowed"))?;
        if !(1..=MAX_DELAY_MILLIS).contains(&self.step_interval_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.saturation_retry_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.failure_retry_base_ms)
            || self.failure_retry_max_ms < self.failure_retry_base_ms
            || self.failure_retry_max_ms > MAX_DELAY_MILLIS
            || !(1..=1_000_000).contains(&self.max_consecutive_failures)
            || !(MIN_SCHEDULE_BYTES..=MAX_SCHEDULE_BYTES).contains(&self.max_schedule_state_bytes)
            || self.demand.memory_bytes < minimum_memory
            || self.demand.io_bytes < logical_io
            || self.demand.io_charge_bytes < logical_io
        {
            return Err(checkpoint_error(
                "schedule timing, state, checkpoint, or resource bounds are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointTotals {
    pub successful_steps: u64,
    pub pages_written: u64,
    pub logical_writeback_bytes: u64,
    /// Time spent inside admitted page-engine steps, excluding scheduler wait,
    /// resource deferral, pause, and durable-state publication.
    pub active_duration_nanos: u64,
    pub pinned_pages_skipped: u64,
    pub transactions_frozen: u64,
    /// Transactions frozen as durable abort exceptions rather than committed
    /// outright. Zero in state written before BicDB 1.0.85-beta.
    #[serde(default)]
    pub abort_exceptions_recorded: u64,
    pub checkpoints_completed: u64,
    pub wal_truncations: u64,
    pub wal_bytes_checkpointed: u64,
    pub tail_pages_truncated: u64,
    pub failures: u64,
    pub resource_deferrals: u64,
}

/// The exact totals contract written by BicDB 1.0.75-beta through
/// 1.0.79-beta. It is retained only so an in-progress operation can cross the
/// format-v2 upgrade without losing its cursor or accepted progress.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PagedCheckpointTotalsV1 {
    successful_steps: u64,
    pages_written: u64,
    logical_writeback_bytes: u64,
    pinned_pages_skipped: u64,
    transactions_frozen: u64,
    checkpoints_completed: u64,
    wal_truncations: u64,
    tail_pages_truncated: u64,
    failures: u64,
    resource_deferrals: u64,
}

impl From<PagedCheckpointTotalsV1> for PagedCheckpointTotals {
    fn from(legacy: PagedCheckpointTotalsV1) -> Self {
        Self {
            successful_steps: legacy.successful_steps,
            pages_written: legacy.pages_written,
            logical_writeback_bytes: legacy.logical_writeback_bytes,
            active_duration_nanos: 0,
            pinned_pages_skipped: legacy.pinned_pages_skipped,
            transactions_frozen: legacy.transactions_frozen,
            abort_exceptions_recorded: 0,
            checkpoints_completed: legacy.checkpoints_completed,
            wal_truncations: legacy.wal_truncations,
            wal_bytes_checkpointed: 0,
            tail_pages_truncated: legacy.tail_pages_truncated,
            failures: legacy.failures,
            resource_deferrals: legacy.resource_deferrals,
        }
    }
}

impl PagedCheckpointTotals {
    fn observe(&mut self, report: &PagedCheckpointStepReport, active_duration_nanos: u64) {
        self.successful_steps = self.successful_steps.saturating_add(1);
        self.active_duration_nanos = self
            .active_duration_nanos
            .saturating_add(active_duration_nanos);
        self.transactions_frozen = self
            .transactions_frozen
            .saturating_add(report.transactions_frozen);
        self.abort_exceptions_recorded = self
            .abort_exceptions_recorded
            .saturating_add(report.abort_exceptions_recorded);
        if let Some(writeback) = report.writeback {
            self.pages_written = self.pages_written.saturating_add(writeback.pages_written);
            self.logical_writeback_bytes = self
                .logical_writeback_bytes
                .saturating_add(writeback.logical_io_bytes);
            self.pinned_pages_skipped = self
                .pinned_pages_skipped
                .saturating_add(writeback.pinned_pages_skipped);
        }
        if let Some(checkpoint) = report.checkpoint {
            self.checkpoints_completed = self.checkpoints_completed.saturating_add(1);
            self.wal_bytes_checkpointed = self
                .wal_bytes_checkpointed
                .saturating_add(checkpoint.wal_bytes_before);
            if checkpoint.wal_truncated {
                self.wal_truncations = self.wal_truncations.saturating_add(1);
            }
        }
        if let Some(tail) = report.tail_reclaim {
            self.tail_pages_truncated = self
                .tail_pages_truncated
                .saturating_add(tail.pages_truncated);
        }
    }
}

/// Checksummed state for one explicitly started checkpoint operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedCheckpointSchedule {
    pub format_version: u32,
    pub store_id: Uuid,
    pub operation_id: Uuid,
    pub page_size: u32,
    pub limits: PagedCheckpointScheduleLimits,
    pub cursor: PagedCheckpointCursor,
    pub totals: PagedCheckpointTotals,
    pub last_report: Option<PagedCheckpointStepReport>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_observed_at_ms: u64,
    pub next_attempt_at_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub paused_reason: Option<String>,
    pub completed: bool,
    pub state_sequence: u64,
    pub checksum_sha256: String,
}

/// Strict reader for the durable format used before checkpoint timing and WAL
/// byte totals were added. The old checksum is verified before migration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PagedCheckpointScheduleV1 {
    format_version: u32,
    store_id: Uuid,
    operation_id: Uuid,
    page_size: u32,
    limits: PagedCheckpointScheduleLimits,
    cursor: PagedCheckpointCursor,
    totals: PagedCheckpointTotalsV1,
    last_report: Option<PagedCheckpointStepReport>,
    started_at_ms: u64,
    updated_at_ms: u64,
    last_observed_at_ms: u64,
    next_attempt_at_ms: Option<u64>,
    consecutive_failures: u32,
    last_error: Option<String>,
    paused_reason: Option<String>,
    completed: bool,
    state_sequence: u64,
    checksum_sha256: String,
}

impl PagedCheckpointScheduleV1 {
    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Input<'a> {
            format_version: u32,
            store_id: Uuid,
            operation_id: Uuid,
            page_size: u32,
            limits: &'a PagedCheckpointScheduleLimits,
            cursor: PagedCheckpointCursor,
            totals: PagedCheckpointTotalsV1,
            last_report: &'a Option<PagedCheckpointStepReport>,
            started_at_ms: u64,
            updated_at_ms: u64,
            last_observed_at_ms: u64,
            next_attempt_at_ms: Option<u64>,
            consecutive_failures: u32,
            last_error: &'a Option<String>,
            paused_reason: &'a Option<String>,
            completed: bool,
            state_sequence: u64,
        }

        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&Input {
            format_version: self.format_version,
            store_id: self.store_id,
            operation_id: self.operation_id,
            page_size: self.page_size,
            limits: &self.limits,
            cursor: self.cursor,
            totals: self.totals,
            last_report: &self.last_report,
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            last_observed_at_ms: self.last_observed_at_ms,
            next_attempt_at_ms: self.next_attempt_at_ms,
            consecutive_failures: self.consecutive_failures,
            last_error: &self.last_error,
            paused_reason: &self.paused_reason,
            completed: self.completed,
            state_sequence: self.state_sequence,
        })?)))
    }

    fn migrate(self) -> Result<PagedCheckpointSchedule> {
        if self.format_version != 1 {
            return Err(checkpoint_error(
                "legacy checkpoint schedule has the wrong format version",
            ));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(checkpoint_error("legacy schedule state checksum mismatch"));
        }

        let mut migrated = PagedCheckpointSchedule {
            format_version: PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION,
            store_id: self.store_id,
            operation_id: self.operation_id,
            page_size: self.page_size,
            limits: self.limits,
            cursor: self.cursor,
            totals: self.totals.into(),
            last_report: self.last_report,
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            last_observed_at_ms: self.last_observed_at_ms,
            next_attempt_at_ms: self.next_attempt_at_ms,
            consecutive_failures: self.consecutive_failures,
            last_error: self.last_error,
            paused_reason: self.paused_reason,
            completed: self.completed,
            state_sequence: self.state_sequence,
            checksum_sha256: String::new(),
        };
        migrated.refresh_checksum()?;
        migrated.validate()?;
        Ok(migrated)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PagedCheckpointScheduleAdvance {
    NotDue {
        next_attempt_at_ms: u64,
    },
    ResourceDeferred {
        retry_at_ms: u64,
    },
    Progress {
        next_attempt_at_ms: u64,
        report: PagedCheckpointStepReport,
    },
    RetryScheduled {
        retry_at_ms: u64,
        consecutive_failures: u32,
        error: String,
    },
    Paused {
        reason: String,
    },
    Complete {
        operation_id: Uuid,
        totals: PagedCheckpointTotals,
        report: PagedCheckpointStepReport,
    },
}

impl PagedCheckpointSchedule {
    fn create(
        store_id: Uuid,
        operation_id: Uuid,
        page_size: u32,
        now_ms: u64,
        limits: PagedCheckpointScheduleLimits,
    ) -> Result<Self> {
        limits.validate()?;
        limits
            .checkpoint
            .validate(page_size)
            .map_err(|error| checkpoint_error(error.to_string()))?;
        if store_id.is_nil() || operation_id.is_nil() {
            return Err(checkpoint_error("store and operation IDs must be non-nil"));
        }
        let mut schedule = Self {
            format_version: PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION,
            store_id,
            operation_id,
            page_size,
            limits,
            cursor: PagedCheckpointCursor::default(),
            totals: PagedCheckpointTotals::default(),
            last_report: None,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            last_observed_at_ms: now_ms,
            next_attempt_at_ms: Some(now_ms),
            consecutive_failures: 0,
            last_error: None,
            paused_reason: None,
            completed: false,
            state_sequence: 0,
            checksum_sha256: String::new(),
        };
        schedule.refresh_checksum()?;
        schedule.validate_for_store(store_id)?;
        Ok(schedule)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_for_store(self.store_id)
    }

    fn validate_for_store(&self, expected_store_id: Uuid) -> Result<()> {
        self.limits.validate()?;
        self.limits
            .checkpoint
            .validate(self.page_size)
            .map_err(|error| checkpoint_error(error.to_string()))?;
        if self.format_version != PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION
            || self.store_id.is_nil()
            || self.operation_id.is_nil()
            || self.store_id != expected_store_id
            || self.page_size == 0
            || self.updated_at_ms < self.started_at_ms
            || self.last_observed_at_ms < self.started_at_ms
            || self.consecutive_failures > self.limits.max_consecutive_failures
            || self
                .last_error
                .as_ref()
                .is_some_and(|error| error.is_empty() || error.len() > MAX_ERROR_BYTES)
            || self
                .paused_reason
                .as_ref()
                .is_some_and(|reason| reason.is_empty() || reason.len() > MAX_ERROR_BYTES)
        {
            return Err(checkpoint_error("schedule identity or bounds are invalid"));
        }
        if let Some(report) = self.last_report {
            report
                .validate(self.limits.checkpoint, self.page_size)
                .map_err(|error| checkpoint_error(error.to_string()))?;
        }
        let inactive = self.completed || self.paused_reason.is_some();
        if inactive != self.next_attempt_at_ms.is_none()
            || self.completed && self.paused_reason.is_some()
            || self.completed != (self.cursor.phase == PagedCheckpointPhase::Complete)
            || self.completed
                && self
                    .last_report
                    .is_none_or(|report| !report.complete || report.checkpoint.is_none())
            || self.totals.successful_steps == 0 && self.last_report.is_some()
            || self.totals.successful_steps > 0 && self.last_report.is_none()
            || self
                .last_report
                .is_some_and(|report| report.next_cursor != self.cursor)
            || self.completed && self.totals.checkpoints_completed != 1
            || !self.completed && self.totals.checkpoints_completed != 0
            || self
                .next_attempt_at_ms
                .is_some_and(|due| due < self.started_at_ms)
        {
            return Err(checkpoint_error(
                "schedule active, paused, completed, cursor, or report state disagrees",
            ));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(checkpoint_error("schedule state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_schedule_state_bytes {
            return Err(checkpoint_error("schedule state exceeds its byte bound"));
        }
        Ok(())
    }

    pub(crate) fn tick_and_checkpoint(
        &mut self,
        path: &Path,
        expected_operation_id: Uuid,
        target: &PagedRecords,
        governor: &ResourceGovernor,
        now_ms: u64,
        fsync: bool,
    ) -> Result<PagedCheckpointScheduleAdvance> {
        self.validate()?;
        if self.operation_id != expected_operation_id {
            return Err(checkpoint_error(
                "stale supervisor operation ID cannot advance the active schedule",
            ));
        }
        if now_ms < self.last_observed_at_ms {
            return Err(checkpoint_error("scheduler clock regressed"));
        }
        target.validate_checkpoint_limits(self.limits.checkpoint)?;
        if target.page_size() != self.page_size {
            return Err(checkpoint_error(
                "checkpoint schedule page size does not match the open store",
            ));
        }
        self.last_observed_at_ms = now_ms;
        self.updated_at_ms = now_ms;

        if self.completed {
            let report = self
                .last_report
                .ok_or_else(|| checkpoint_error("completed schedule lost its terminal report"))?;
            self.refresh_and_save(path, fsync)?;
            return Ok(PagedCheckpointScheduleAdvance::Complete {
                operation_id: self.operation_id,
                totals: self.totals,
                report,
            });
        }
        if let Some(reason) = &self.paused_reason {
            let outcome = PagedCheckpointScheduleAdvance::Paused {
                reason: reason.clone(),
            };
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        let due = self
            .next_attempt_at_ms
            .ok_or_else(|| checkpoint_error("active schedule has no next attempt"))?;
        if now_ms < due {
            self.refresh_and_save(path, fsync)?;
            return Ok(PagedCheckpointScheduleAdvance::NotDue {
                next_attempt_at_ms: due,
            });
        }

        let permit = match governor.try_admit(ResourceLane::Compaction, self.limits.demand, now_ms)
        {
            Ok(permit) => permit,
            Err(BicDbError::ResourceGovernance(_)) => {
                let retry_at_ms = bounded_due(now_ms, self.limits.saturation_retry_ms)?;
                self.totals.resource_deferrals = self.totals.resource_deferrals.saturating_add(1);
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                return Ok(PagedCheckpointScheduleAdvance::ResourceDeferred { retry_at_ms });
            }
            Err(error) => return Err(error),
        };

        let started = Instant::now();
        let result = target.checkpoint_step(self.cursor, self.limits.checkpoint);
        let active_duration_nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        drop(permit);
        match result {
            Ok(report) => {
                if report.phase_before != self.cursor.phase
                    || report.complete
                        != (report.next_cursor.phase == PagedCheckpointPhase::Complete)
                {
                    return Err(checkpoint_error(
                        "page engine returned a checkpoint report for the wrong phase",
                    ));
                }
                self.consecutive_failures = 0;
                self.last_error = None;
                self.cursor = report.next_cursor;
                self.totals.observe(&report, active_duration_nanos);
                self.last_report = Some(report);
                if report.complete {
                    self.completed = true;
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(PagedCheckpointScheduleAdvance::Complete {
                        operation_id: self.operation_id,
                        totals: self.totals,
                        report,
                    });
                }
                let next_attempt_at_ms = bounded_due(now_ms, self.limits.step_interval_ms)?;
                self.next_attempt_at_ms = Some(next_attempt_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedCheckpointScheduleAdvance::Progress {
                    next_attempt_at_ms,
                    report,
                })
            }
            Err(error) => {
                self.totals.active_duration_nanos = self
                    .totals
                    .active_duration_nanos
                    .saturating_add(active_duration_nanos);
                let message = bounded_error(error.to_string());
                self.totals.failures = self.totals.failures.saturating_add(1);
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                self.last_error = Some(message.clone());
                if self.consecutive_failures >= self.limits.max_consecutive_failures {
                    self.paused_reason = Some(message.clone());
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(PagedCheckpointScheduleAdvance::Paused { reason: message });
                }
                let shift = self.consecutive_failures.saturating_sub(1).min(63);
                let delay = self
                    .limits
                    .failure_retry_base_ms
                    .saturating_mul(1_u64 << shift)
                    .min(self.limits.failure_retry_max_ms);
                let retry_at_ms = bounded_due(now_ms, delay)?;
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedCheckpointScheduleAdvance::RetryScheduled {
                    retry_at_ms,
                    consecutive_failures: self.consecutive_failures,
                    error: message,
                })
            }
        }
    }

    pub(crate) fn pause_by_operator_and_checkpoint(
        &mut self,
        path: &Path,
        expected_operation_id: Uuid,
        reason: impl Into<String>,
        now_ms: u64,
        fsync: bool,
    ) -> Result<()> {
        self.validate()?;
        if self.operation_id != expected_operation_id
            || self.completed
            || self.paused_reason.is_some()
            || now_ms < self.last_observed_at_ms
        {
            return Err(checkpoint_error(
                "active schedule cannot be paused by that operation or at that time",
            ));
        }
        self.last_observed_at_ms = now_ms;
        self.updated_at_ms = now_ms;
        self.last_error = None;
        self.consecutive_failures = 0;
        self.paused_reason = Some(bounded_error(reason.into()));
        self.next_attempt_at_ms = None;
        self.refresh_and_save(path, fsync)
    }

    pub(crate) fn resume_and_checkpoint(
        &mut self,
        path: &Path,
        expected_operation_id: Uuid,
        next_attempt_at_ms: u64,
        now_ms: u64,
        fsync: bool,
    ) -> Result<()> {
        self.validate()?;
        if self.operation_id != expected_operation_id
            || self.completed
            || self.paused_reason.is_none()
            || now_ms < self.last_observed_at_ms
            || next_attempt_at_ms < now_ms
        {
            return Err(checkpoint_error(
                "paused schedule cannot be resumed by that operation or at that time",
            ));
        }
        self.paused_reason = None;
        self.last_error = None;
        self.consecutive_failures = 0;
        self.last_observed_at_ms = now_ms;
        self.updated_at_ms = now_ms;
        self.next_attempt_at_ms = Some(next_attempt_at_ms);
        self.refresh_and_save(path, fsync)
    }

    fn refresh_and_save(&mut self, path: &Path, fsync: bool) -> Result<()> {
        self.state_sequence = self
            .state_sequence
            .checked_add(1)
            .ok_or_else(|| checkpoint_error("schedule state sequence exhausted"))?;
        self.refresh_checksum()?;
        save_schedule(path, self, fsync)
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Input<'a> {
            format_version: u32,
            store_id: Uuid,
            operation_id: Uuid,
            page_size: u32,
            limits: &'a PagedCheckpointScheduleLimits,
            cursor: PagedCheckpointCursor,
            totals: PagedCheckpointTotals,
            last_report: &'a Option<PagedCheckpointStepReport>,
            started_at_ms: u64,
            updated_at_ms: u64,
            last_observed_at_ms: u64,
            next_attempt_at_ms: Option<u64>,
            consecutive_failures: u32,
            last_error: &'a Option<String>,
            paused_reason: &'a Option<String>,
            completed: bool,
            state_sequence: u64,
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&Input {
            format_version: self.format_version,
            store_id: self.store_id,
            operation_id: self.operation_id,
            page_size: self.page_size,
            limits: &self.limits,
            cursor: self.cursor,
            totals: self.totals,
            last_report: &self.last_report,
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            last_observed_at_ms: self.last_observed_at_ms,
            next_attempt_at_ms: self.next_attempt_at_ms,
            consecutive_failures: self.consecutive_failures,
            last_error: &self.last_error,
            paused_reason: &self.paused_reason,
            completed: self.completed,
            state_sequence: self.state_sequence,
        })?)))
    }
}

pub(crate) fn start_schedule(
    identity_path: &Path,
    schedule_path: &Path,
    protected_state_paths: &[&Path],
    operation_id: Uuid,
    now_ms: u64,
    limits: PagedCheckpointScheduleLimits,
    target: &PagedRecords,
    fsync: bool,
) -> Result<PagedCheckpointSchedule> {
    limits.validate()?;
    target.validate_checkpoint_limits(limits.checkpoint)?;
    if let Some(existing) = load_schedule_if_exists(schedule_path)? {
        let identity = load_identity(identity_path)?;
        existing.validate_for_store(identity.store_id)?;
        if !existing.completed && existing.paused_reason.is_none() {
            return Err(checkpoint_error(format!(
                "operation {} is still active",
                existing.operation_id
            )));
        }
    }
    let identity = load_or_create_identity(identity_path, protected_state_paths, now_ms, fsync)?;
    let schedule = PagedCheckpointSchedule::create(
        identity.store_id,
        operation_id,
        target.page_size(),
        now_ms,
        limits,
    )?;
    save_schedule(schedule_path, &schedule, fsync)?;
    Ok(schedule)
}

pub(crate) fn load_active_schedule(
    identity_path: &Path,
    schedule_path: &Path,
) -> Result<Option<PagedCheckpointSchedule>> {
    let Some(schedule) = load_schedule_if_exists(schedule_path)? else {
        return Ok(None);
    };
    let identity = load_identity(identity_path)?;
    schedule.validate_for_store(identity.store_id)?;
    Ok(Some(schedule))
}

fn save_schedule(path: &Path, schedule: &PagedCheckpointSchedule, fsync: bool) -> Result<()> {
    schedule.validate()?;
    let bytes = serde_json::to_vec(schedule)?;
    if bytes.len() as u64 > schedule.limits.max_schedule_state_bytes {
        return Err(checkpoint_error("schedule exceeds its write bound"));
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn load_schedule_if_exists(path: &Path) -> Result<Option<PagedCheckpointSchedule>> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_regular_file(path, MAX_SCHEDULE_BYTES, "schedule")?;
    // This pass reads only the discriminator. The selected complete contract
    // below remains strict and rejects every unknown or missing field.
    #[derive(Deserialize)]
    struct VersionProbe {
        format_version: u32,
    }

    let version = serde_json::from_slice::<VersionProbe>(&bytes)?.format_version;
    let schedule = match version {
        1 => serde_json::from_slice::<PagedCheckpointScheduleV1>(&bytes)?.migrate()?,
        PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION => serde_json::from_slice(&bytes)?,
        _ => {
            return Err(checkpoint_error(format!(
                "unsupported checkpoint schedule format version {version}"
            )))
        }
    };
    schedule.validate()?;
    Ok(Some(schedule))
}

fn read_bounded_regular_file(path: &Path, max_bytes: u64, label: &str) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > max_bytes
    {
        return Err(checkpoint_error(format!(
            "{label} file is unsafe or outside its byte bound"
        )));
    }
    let length = metadata.len();
    let file = open_read_only_no_follow(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != length {
        return Err(checkpoint_error(format!(
            "{label} changed while it was being opened"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > max_bytes {
        return Err(checkpoint_error(format!(
            "{label} changed or grew while it was being read"
        )));
    }
    Ok(bytes)
}

fn open_read_only_no_follow(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options.open(path)
}

fn bounded_due(now_ms: u64, delay_ms: u64) -> Result<u64> {
    now_ms
        .checked_add(delay_ms)
        .ok_or_else(|| checkpoint_error("scheduler due time overflow"))
}

fn bounded_error(mut message: String) -> String {
    if message.len() > MAX_ERROR_BYTES {
        let mut end = MAX_ERROR_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    if message.is_empty() {
        "maintenance step failed without an error message".to_string()
    } else {
        message
    }
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(checkpoint_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

pub(crate) fn maintenance_paths(root: &Path) -> (PathBuf, PathBuf) {
    (
        root.join(crate::DEFAULT_PAGED_MAINTENANCE_IDENTITY),
        root.join(DEFAULT_PAGED_CHECKPOINT_SCHEDULE),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_v1_schedule_migrates_without_losing_cursor_or_totals() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("checkpoint.json");
        let current = PagedCheckpointSchedule::create(
            Uuid::new_v4(),
            Uuid::new_v4(),
            8_192,
            100,
            PagedCheckpointScheduleLimits::default(),
        )
        .unwrap();
        let migrated_cursor = PagedCheckpointCursor {
            phase: PagedCheckpointPhase::Freeze,
            ..Default::default()
        };
        let last_report = PagedCheckpointStepReport {
            phase_before: PagedCheckpointPhase::Drain,
            next_cursor: migrated_cursor,
            writeback: None,
            transactions_frozen: 0,
            freeze_blocked: false,
            abort_exceptions_recorded: 0,
            freeze_exception_capacity_full: false,
            dirty_pages_remaining: 0,
            checkpoint: None,
            tail_reclaim: None,
            complete: false,
            stop_reason: bicdb_page::PagedCheckpointStopReason::PhaseBoundary,
        };
        let mut legacy = PagedCheckpointScheduleV1 {
            format_version: 1,
            store_id: current.store_id,
            operation_id: current.operation_id,
            page_size: current.page_size,
            limits: current.limits,
            cursor: migrated_cursor,
            totals: PagedCheckpointTotalsV1 {
                successful_steps: 7,
                pages_written: 5,
                logical_writeback_bytes: 40_960,
                pinned_pages_skipped: 2,
                transactions_frozen: 3,
                checkpoints_completed: 0,
                wal_truncations: 0,
                tail_pages_truncated: 0,
                failures: 1,
                resource_deferrals: 4,
            },
            last_report: Some(last_report),
            started_at_ms: current.started_at_ms,
            updated_at_ms: current.updated_at_ms,
            last_observed_at_ms: current.last_observed_at_ms,
            next_attempt_at_ms: current.next_attempt_at_ms,
            consecutive_failures: current.consecutive_failures,
            last_error: current.last_error,
            paused_reason: current.paused_reason,
            completed: current.completed,
            state_sequence: current.state_sequence,
            checksum_sha256: String::new(),
        };
        legacy.checksum_sha256 = legacy.calculate_checksum().unwrap();
        std::fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let migrated = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(
            migrated.format_version,
            PAGED_CHECKPOINT_SCHEDULE_FORMAT_VERSION
        );
        assert_eq!(migrated.store_id, legacy.store_id);
        assert_eq!(migrated.operation_id, legacy.operation_id);
        assert_eq!(migrated.cursor, legacy.cursor);
        assert_eq!(migrated.totals.successful_steps, 7);
        assert_eq!(migrated.totals.pages_written, 5);
        assert_eq!(migrated.totals.logical_writeback_bytes, 40_960);
        assert_eq!(migrated.totals.active_duration_nanos, 0);
        assert_eq!(migrated.totals.wal_bytes_checkpointed, 0);

        save_schedule(&path, &migrated, false).unwrap();
        let published: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(published["format_version"], serde_json::json!(2));
    }

    #[test]
    fn v1_schedule_checksum_is_verified_before_migration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("checkpoint.json");
        let current = PagedCheckpointSchedule::create(
            Uuid::new_v4(),
            Uuid::new_v4(),
            8_192,
            100,
            PagedCheckpointScheduleLimits::default(),
        )
        .unwrap();
        let mut legacy = PagedCheckpointScheduleV1 {
            format_version: 1,
            store_id: current.store_id,
            operation_id: current.operation_id,
            page_size: current.page_size,
            limits: current.limits,
            cursor: current.cursor,
            totals: PagedCheckpointTotalsV1::default(),
            last_report: None,
            started_at_ms: current.started_at_ms,
            updated_at_ms: current.updated_at_ms,
            last_observed_at_ms: current.last_observed_at_ms,
            next_attempt_at_ms: current.next_attempt_at_ms,
            consecutive_failures: 0,
            last_error: None,
            paused_reason: None,
            completed: false,
            state_sequence: 0,
            checksum_sha256: String::new(),
        };
        legacy.checksum_sha256 = legacy.calculate_checksum().unwrap();
        let mut value = serde_json::to_value(legacy).unwrap();
        value["totals"]["pages_written"] = serde_json::json!(1);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();

        assert!(load_schedule_if_exists(&path)
            .unwrap_err()
            .to_string()
            .contains("legacy schedule state checksum mismatch"));
    }
}
