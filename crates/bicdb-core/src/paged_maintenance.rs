//! Durable, resource-governed supervision for server-paged vacuum.
//!
//! `bicdb-page` owns the bounded, idempotent vacuum step. This module owns the
//! production orchestration contract around that step: an immutable store
//! identity, an operation identity, monotonic scheduling, resource admission,
//! bounded retry state, and atomic cursor publication. A crash after page work
//! but before schedule publication repeats the inclusive cursor, which is safe;
//! it can never skip a page whose progress was not durably acknowledged.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use bicdb_page::{
    VacuumCursor, VacuumLimits, VacuumReport, VacuumStopReason, MAX_VACUUM_BYTES_PER_STEP,
    MAX_VACUUM_DURATION_MILLIS_PER_STEP, MAX_VACUUM_PAGES_PER_STEP,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::paged_collection::PagedRecords;
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const PAGED_MAINTENANCE_IDENTITY_FORMAT_VERSION: u32 = 1;
pub const PAGED_VACUUM_SCHEDULE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_PAGED_MAINTENANCE_DIR: &str = "maintenance/paged";
pub const DEFAULT_PAGED_MAINTENANCE_IDENTITY: &str = "maintenance/paged/store-identity.json";
pub const DEFAULT_PAGED_VACUUM_SCHEDULE: &str = "maintenance/paged/vacuum.json";

const MAX_IDENTITY_BYTES: u64 = 4 * 1024;
const MIN_SCHEDULE_BYTES: u64 = 4 * 1024;
const MAX_SCHEDULE_BYTES: u64 = 1024 * 1024;
const MAX_ERROR_BYTES: usize = 4 * 1024;
const MAX_DELAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;
pub(crate) const PAGED_INTEGRITY_SCHEDULE_FILE: &str = "integrity.json";
pub(crate) const PAGED_CHECKPOINT_SCHEDULE_FILE: &str = "checkpoint.json";

fn maintenance_error(message: impl Into<String>) -> BicDbError {
    BicDbError::PagedStorage(format!("paged vacuum maintenance: {}", message.into()))
}

/// Complete non-weakenable policy for one supervised vacuum operation.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedVacuumScheduleLimits {
    pub vacuum: VacuumLimits,
    /// Nonzero delay between successful bounded steps. This prevents an entire
    /// database sweep from turning into an ungoverned tight loop.
    pub step_interval_ms: u64,
    pub saturation_retry_ms: u64,
    pub failure_retry_base_ms: u64,
    pub failure_retry_max_ms: u64,
    pub max_consecutive_failures: u32,
    pub max_schedule_state_bytes: u64,
    /// Peak capacity and token-bucket charge reserved before a page is touched.
    pub demand: ResourceDemand,
}

impl Default for PagedVacuumScheduleLimits {
    fn default() -> Self {
        let vacuum = VacuumLimits::default();
        Self {
            vacuum,
            step_interval_ms: 100,
            saturation_retry_ms: 1_000,
            failure_retry_base_ms: 1_000,
            failure_retry_max_ms: 5 * 60 * 1_000,
            max_consecutive_failures: 32,
            max_schedule_state_bytes: 64 * 1024,
            demand: ResourceDemand {
                memory_bytes: 16 * 1024 * 1024,
                io_bytes: vacuum.max_bytes,
                cpu_slots: 1,
                io_charge_bytes: vacuum.max_bytes,
            },
        }
    }
}

impl PagedVacuumScheduleLimits {
    pub fn validate(&self) -> Result<()> {
        self.demand.validate()?;
        let bounded_id_bytes = self.vacuum.max_pages.saturating_mul(8);
        if self.vacuum.max_pages == 0
            || self.vacuum.max_pages > MAX_VACUUM_PAGES_PER_STEP
            || self.vacuum.max_bytes == 0
            || self.vacuum.max_bytes > MAX_VACUUM_BYTES_PER_STEP
            || self.vacuum.max_duration_millis == 0
            || self.vacuum.max_duration_millis > MAX_VACUUM_DURATION_MILLIS_PER_STEP
            || !(1..=MAX_DELAY_MILLIS).contains(&self.step_interval_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.saturation_retry_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.failure_retry_base_ms)
            || self.failure_retry_max_ms < self.failure_retry_base_ms
            || self.failure_retry_max_ms > MAX_DELAY_MILLIS
            || !(1..=1_000_000).contains(&self.max_consecutive_failures)
            || !(MIN_SCHEDULE_BYTES..=MAX_SCHEDULE_BYTES).contains(&self.max_schedule_state_bytes)
            || self.demand.memory_bytes < bounded_id_bytes.saturating_add(2 * 1024 * 1024)
            || self.demand.io_bytes < self.vacuum.max_bytes
            || self.demand.io_charge_bytes < self.vacuum.max_bytes
        {
            return Err(maintenance_error(
                "schedule timing, state, vacuum, or resource bounds are inconsistent",
            ));
        }
        Ok(())
    }
}

/// Totals from step reports that were atomically published in the schedule.
/// Work completed immediately before a process crash is intentionally absent;
/// its inclusive cursor is replayed and the replay's report is counted instead.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedVacuumTotals {
    pub successful_steps: u64,
    pub pages_scanned: u64,
    pub bytes_examined: u64,
    pub versions_examined: u64,
    pub versions_reclaimed: u64,
    pub bytes_reclaimed: u64,
    pub pages_freed: u64,
    pub failures: u64,
    pub resource_deferrals: u64,
}

impl PagedVacuumTotals {
    fn observe(&mut self, report: &VacuumReport) {
        self.successful_steps = self.successful_steps.saturating_add(1);
        self.pages_scanned = self.pages_scanned.saturating_add(report.pages_scanned);
        self.bytes_examined = self.bytes_examined.saturating_add(report.bytes_examined);
        self.versions_examined = self
            .versions_examined
            .saturating_add(report.versions_examined);
        self.versions_reclaimed = self
            .versions_reclaimed
            .saturating_add(report.versions_reclaimed);
        self.bytes_reclaimed = self.bytes_reclaimed.saturating_add(report.bytes_reclaimed);
        self.pages_freed = self.pages_freed.saturating_add(report.pages_freed);
    }
}

/// Durable state for exactly one explicitly started vacuum sweep.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedVacuumSchedule {
    pub format_version: u32,
    pub store_id: Uuid,
    pub operation_id: Uuid,
    pub limits: PagedVacuumScheduleLimits,
    pub cursor: VacuumCursor,
    pub totals: PagedVacuumTotals,
    pub last_report: Option<VacuumReport>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PagedVacuumScheduleAdvance {
    NotDue {
        next_attempt_at_ms: u64,
    },
    ResourceDeferred {
        retry_at_ms: u64,
    },
    Progress {
        next_attempt_at_ms: u64,
        report: VacuumReport,
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
        totals: PagedVacuumTotals,
    },
}

impl PagedVacuumSchedule {
    pub(crate) fn create(
        store_id: Uuid,
        operation_id: Uuid,
        now_ms: u64,
        limits: PagedVacuumScheduleLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if store_id.is_nil() || operation_id.is_nil() {
            return Err(maintenance_error("store and operation IDs must be non-nil"));
        }
        let mut schedule = Self {
            format_version: PAGED_VACUUM_SCHEDULE_FORMAT_VERSION,
            store_id,
            operation_id,
            limits,
            cursor: VacuumCursor::default(),
            totals: PagedVacuumTotals::default(),
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

    pub(crate) fn validate_for_store(&self, expected_store_id: Uuid) -> Result<()> {
        self.limits.validate()?;
        if self.format_version != PAGED_VACUUM_SCHEDULE_FORMAT_VERSION
            || self.store_id.is_nil()
            || self.operation_id.is_nil()
            || self.store_id != expected_store_id
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
            return Err(maintenance_error("schedule identity or bounds are invalid"));
        }
        let inactive = self.completed || self.paused_reason.is_some();
        if inactive != self.next_attempt_at_ms.is_none()
            || self.completed && self.paused_reason.is_some()
            || self.completed
                && (self.cursor != VacuumCursor::default()
                    || self.last_report.is_none_or(|report| {
                        !report.complete || report.stop_reason != VacuumStopReason::Complete
                    }))
            || self.totals.successful_steps == 0 && self.last_report.is_some()
            || self.totals.successful_steps > 0 && self.last_report.is_none()
            || self
                .next_attempt_at_ms
                .is_some_and(|due| due < self.started_at_ms)
        {
            return Err(maintenance_error(
                "schedule active, paused, completed, cursor, or report state disagrees",
            ));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(maintenance_error("schedule state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_schedule_state_bytes {
            return Err(maintenance_error("schedule state exceeds its byte bound"));
        }
        Ok(())
    }

    pub(crate) fn tick_and_checkpoint<T: PagedVacuumTarget>(
        &mut self,
        path: impl AsRef<Path>,
        expected_operation_id: Uuid,
        target: &T,
        governor: &ResourceGovernor,
        now_ms: u64,
        fsync: bool,
    ) -> Result<PagedVacuumScheduleAdvance> {
        self.validate()?;
        if self.operation_id != expected_operation_id {
            return Err(maintenance_error(
                "stale supervisor operation ID cannot advance the active schedule",
            ));
        }
        if now_ms < self.last_observed_at_ms {
            return Err(maintenance_error("scheduler clock regressed"));
        }
        target.validate_vacuum_limits(&self.limits.vacuum)?;
        self.last_observed_at_ms = now_ms;
        self.updated_at_ms = now_ms;

        if self.completed {
            let outcome = PagedVacuumScheduleAdvance::Complete {
                operation_id: self.operation_id,
                totals: self.totals,
            };
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        if let Some(reason) = &self.paused_reason {
            let outcome = PagedVacuumScheduleAdvance::Paused {
                reason: reason.clone(),
            };
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        let due = self
            .next_attempt_at_ms
            .ok_or_else(|| maintenance_error("active schedule has no next attempt"))?;
        if now_ms < due {
            self.refresh_and_save(path, fsync)?;
            return Ok(PagedVacuumScheduleAdvance::NotDue {
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
                return Ok(PagedVacuumScheduleAdvance::ResourceDeferred { retry_at_ms });
            }
            Err(error) => return Err(error),
        };

        let previous_cursor = self.cursor;
        let result = target.vacuum_step(previous_cursor, self.limits.vacuum);
        drop(permit);
        match result {
            Ok(report) => {
                validate_step_report(&report, &self.limits.vacuum)?;
                self.consecutive_failures = 0;
                self.last_error = None;
                self.cursor = report.next_cursor;
                self.totals.observe(&report);
                self.last_report = Some(report);

                if report.complete {
                    self.completed = true;
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(PagedVacuumScheduleAdvance::Complete {
                        operation_id: self.operation_id,
                        totals: self.totals,
                    });
                }

                // NON-PROGRESS, defined by durable movement rather than by
                // counters.
                //
                // The previous guard keyed on `pages_scanned == 0`, which
                // missed the failure that actually happened: the sweep DID
                // enter a page, counted the same 127 versions as reclaimed on
                // every step — past 61,000 on a 600-row table — and still
                // reported the same cursor. Counters that can re-count the
                // same work cannot define progress. Cursor advancement and
                // durable structural change can.
                let progressed = report.next_cursor != previous_cursor
                    || report.pages_freed > 0
                    || report.bytes_reclaimed > 0;
                if !progressed && !report.complete {
                    return self.pause_and_save(
                        path,
                        &format!(
                            "vacuum made no durable progress at cursor {:?} (stop reason {:?});                              refusing to spend further I/O and checkpoints on a sweep that                              cannot advance",
                            previous_cursor.next_page_id, report.stop_reason
                        ),
                        fsync,
                    );
                }

                let next_attempt_at_ms = bounded_due(now_ms, self.limits.step_interval_ms)?;
                self.next_attempt_at_ms = Some(next_attempt_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedVacuumScheduleAdvance::Progress {
                    next_attempt_at_ms,
                    report,
                })
            }
            Err(error) => {
                let message = bounded_error(error.to_string());
                self.totals.failures = self.totals.failures.saturating_add(1);
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                self.last_error = Some(message.clone());
                if self.consecutive_failures >= self.limits.max_consecutive_failures {
                    return self.pause_and_save(path, message, fsync);
                }
                let shift = self.consecutive_failures.saturating_sub(1).min(63);
                let retry_delay = self
                    .limits
                    .failure_retry_base_ms
                    .saturating_mul(1_u64 << shift)
                    .min(self.limits.failure_retry_max_ms);
                let retry_at_ms = bounded_due(now_ms, retry_delay)?;
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedVacuumScheduleAdvance::RetryScheduled {
                    retry_at_ms,
                    consecutive_failures: self.consecutive_failures,
                    error: message,
                })
            }
        }
    }

    pub(crate) fn resume_and_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
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
            return Err(maintenance_error(
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

    pub(crate) fn pause_by_operator_and_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
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
            return Err(maintenance_error(
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

    fn pause_and_save(
        &mut self,
        path: impl AsRef<Path>,
        reason: impl Into<String>,
        fsync: bool,
    ) -> Result<PagedVacuumScheduleAdvance> {
        let reason = bounded_error(reason.into());
        self.paused_reason = Some(reason.clone());
        self.next_attempt_at_ms = None;
        self.refresh_and_save(path, fsync)?;
        Ok(PagedVacuumScheduleAdvance::Paused { reason })
    }

    fn refresh_and_save(&mut self, path: impl AsRef<Path>, fsync: bool) -> Result<()> {
        self.state_sequence = self
            .state_sequence
            .checked_add(1)
            .ok_or_else(|| maintenance_error("schedule state sequence exhausted"))?;
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
            limits: &'a PagedVacuumScheduleLimits,
            cursor: VacuumCursor,
            totals: PagedVacuumTotals,
            last_report: &'a Option<VacuumReport>,
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
        let input = Input {
            format_version: self.format_version,
            store_id: self.store_id,
            operation_id: self.operation_id,
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
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&input)?)))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PagedMaintenanceIdentity {
    format_version: u32,
    pub(crate) store_id: Uuid,
    created_at_ms: u64,
    checksum_sha256: String,
}

impl PagedMaintenanceIdentity {
    fn create(now_ms: u64) -> Result<Self> {
        let mut identity = Self {
            format_version: PAGED_MAINTENANCE_IDENTITY_FORMAT_VERSION,
            store_id: Uuid::new_v4(),
            created_at_ms: now_ms,
            checksum_sha256: String::new(),
        };
        identity.checksum_sha256 = identity.calculate_checksum()?;
        identity.validate()?;
        Ok(identity)
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != PAGED_MAINTENANCE_IDENTITY_FORMAT_VERSION
            || self.store_id.is_nil()
        {
            return Err(maintenance_error("store identity is invalid"));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(maintenance_error("store identity checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Input {
            format_version: u32,
            store_id: Uuid,
            created_at_ms: u64,
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&Input {
            format_version: self.format_version,
            store_id: self.store_id,
            created_at_ms: self.created_at_ms,
        })?)))
    }
}

pub(crate) fn start_schedule(
    identity_path: &Path,
    schedule_path: &Path,
    operation_id: Uuid,
    now_ms: u64,
    limits: PagedVacuumScheduleLimits,
    target: &PagedRecords,
    fsync: bool,
) -> Result<PagedVacuumSchedule> {
    limits.validate()?;
    target.validate_vacuum_limits(&limits.vacuum)?;
    if let Some(existing) = load_schedule_if_exists(schedule_path)? {
        let identity = load_identity(identity_path)?;
        existing.validate_for_store(identity.store_id)?;
        if !existing.completed && existing.paused_reason.is_none() {
            return Err(maintenance_error(format!(
                "operation {} is still active",
                existing.operation_id
            )));
        }
    }
    let integrity_path = schedule_path.with_file_name(PAGED_INTEGRITY_SCHEDULE_FILE);
    let checkpoint_path = schedule_path.with_file_name(PAGED_CHECKPOINT_SCHEDULE_FILE);
    let identity = load_or_create_identity(
        identity_path,
        &[
            schedule_path,
            integrity_path.as_path(),
            checkpoint_path.as_path(),
        ],
        now_ms,
        fsync,
    )?;
    let schedule = PagedVacuumSchedule::create(identity.store_id, operation_id, now_ms, limits)?;
    save_schedule(schedule_path, &schedule, fsync)?;
    Ok(schedule)
}

pub(crate) fn load_active_schedule(
    identity_path: &Path,
    schedule_path: &Path,
) -> Result<Option<PagedVacuumSchedule>> {
    let Some(schedule) = load_schedule_if_exists(schedule_path)? else {
        return Ok(None);
    };
    let identity = load_identity(identity_path)?;
    schedule.validate_for_store(identity.store_id)?;
    Ok(Some(schedule))
}

fn save_schedule(
    path: impl AsRef<Path>,
    schedule: &PagedVacuumSchedule,
    fsync: bool,
) -> Result<()> {
    schedule.validate()?;
    let bytes = serde_json::to_vec(schedule)?;
    if bytes.len() as u64 > schedule.limits.max_schedule_state_bytes {
        return Err(maintenance_error("schedule exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

fn load_schedule_if_exists(path: &Path) -> Result<Option<PagedVacuumSchedule>> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_regular_file(path, MAX_SCHEDULE_BYTES, "schedule")?;
    let schedule: PagedVacuumSchedule = serde_json::from_slice(&bytes)?;
    schedule.validate()?;
    Ok(Some(schedule))
}

pub(crate) fn load_or_create_identity(
    identity_path: &Path,
    protected_state_paths: &[&Path],
    now_ms: u64,
    fsync: bool,
) -> Result<PagedMaintenanceIdentity> {
    match std::fs::symlink_metadata(identity_path) {
        Ok(_) => load_identity(identity_path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            for state_path in protected_state_paths {
                match std::fs::symlink_metadata(state_path) {
                    Ok(_) => {
                        return Err(maintenance_error(
                            "maintenance state exists but its immutable store identity is missing",
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let identity = PagedMaintenanceIdentity::create(now_ms)?;
            let bytes = serde_json::to_vec(&identity)?;
            crate::storage::write_atomic(identity_path, &bytes, fsync)?;
            Ok(identity)
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn load_identity(path: &Path) -> Result<PagedMaintenanceIdentity> {
    let bytes = read_bounded_regular_file(path, MAX_IDENTITY_BYTES, "store identity")?;
    let identity: PagedMaintenanceIdentity = serde_json::from_slice(&bytes)?;
    identity.validate()?;
    Ok(identity)
}

fn read_bounded_regular_file(path: &Path, max_bytes: u64, label: &str) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > max_bytes
    {
        return Err(maintenance_error(format!(
            "{label} file is unsafe or outside its byte bound"
        )));
    }
    let length = metadata.len();
    let file = open_read_only_no_follow(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != length {
        return Err(maintenance_error(format!(
            "{label} changed while it was being opened"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > max_bytes {
        return Err(maintenance_error(format!(
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
        .ok_or_else(|| maintenance_error("scheduler due time overflow"))
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
        return Err(maintenance_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

fn validate_step_report(report: &VacuumReport, limits: &VacuumLimits) -> Result<()> {
    // The byte envelope is a SOFT bound: it is checked between pages, and a
    // page that has been entered runs to completion. Maximum overshoot is one
    // page's vacuum work, which is the price of a page-granular cursor that
    // can actually resume. `max_pages` stays hard — it is expressible.
    let byte_overshoot_allowed = report.pages_scanned > 0;
    if report.pages_scanned > limits.max_pages
        || (!byte_overshoot_allowed && report.bytes_examined > limits.max_bytes)
        || report.complete != (report.stop_reason == VacuumStopReason::Complete)
        || report.stopped_early == report.complete
        || report.complete && report.next_cursor != VacuumCursor::default()
        || !report.complete && report.next_cursor.next_page_id.is_none()
    {
        return Err(maintenance_error(
            "page engine returned an internally inconsistent or over-budget vacuum report",
        ));
    }
    Ok(())
}

pub(crate) trait PagedVacuumTarget {
    fn validate_vacuum_limits(&self, limits: &VacuumLimits) -> Result<()>;
    fn vacuum_step(&self, cursor: VacuumCursor, limits: VacuumLimits) -> Result<VacuumReport>;
}

impl PagedVacuumTarget for PagedRecords {
    fn validate_vacuum_limits(&self, limits: &VacuumLimits) -> Result<()> {
        PagedRecords::validate_vacuum_limits(self, limits)
    }

    fn vacuum_step(&self, cursor: VacuumCursor, limits: VacuumLimits) -> Result<VacuumReport> {
        PagedRecords::vacuum_step(self, cursor, limits)
    }
}

pub(crate) fn maintenance_paths(root: &Path) -> (PathBuf, PathBuf) {
    (
        root.join(DEFAULT_PAGED_MAINTENANCE_IDENTITY),
        root.join(DEFAULT_PAGED_VACUUM_SCHEDULE),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;
    use crate::ResourceGovernorConfig;

    enum FakeAction {
        Report(VacuumReport),
        Error(&'static str),
    }

    struct FakeTarget {
        actions: Mutex<VecDeque<FakeAction>>,
        cursors: Mutex<Vec<VacuumCursor>>,
    }

    impl FakeTarget {
        fn new(actions: impl IntoIterator<Item = FakeAction>) -> Self {
            Self {
                actions: Mutex::new(actions.into_iter().collect()),
                cursors: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            self.cursors.lock().unwrap().len()
        }
    }

    impl PagedVacuumTarget for FakeTarget {
        fn validate_vacuum_limits(&self, limits: &VacuumLimits) -> Result<()> {
            if limits.max_bytes < 16 * 1024 {
                return Err(maintenance_error("test vacuum byte limit is too small"));
            }
            Ok(())
        }

        fn vacuum_step(&self, cursor: VacuumCursor, _limits: VacuumLimits) -> Result<VacuumReport> {
            self.cursors.lock().unwrap().push(cursor);
            match self.actions.lock().unwrap().pop_front() {
                Some(FakeAction::Report(report)) => Ok(report),
                Some(FakeAction::Error(message)) => Err(maintenance_error(message)),
                None => Err(maintenance_error("fake target has no action")),
            }
        }
    }

    fn report(next_page_id: Option<u64>, complete: bool) -> VacuumReport {
        VacuumReport {
            pages_scanned: 1,
            bytes_examined: 16 * 1024,
            versions_examined: 2,
            versions_reclaimed: 1,
            bytes_reclaimed: 30,
            pages_freed: 0,
            oldest_active: 9,
            next_cursor: VacuumCursor { next_page_id },
            start_page: None,
            end_page: next_page_id.map(bicdb_page::PageId::from),
            stop_reason: if complete {
                VacuumStopReason::Complete
            } else {
                VacuumStopReason::PageLimit
            },
            elapsed_millis: 1,
            complete,
            stopped_early: !complete,
        }
    }

    fn byte_blocked(next_page_id: u64) -> VacuumReport {
        VacuumReport {
            pages_scanned: 0,
            bytes_examined: 8 * 1024,
            versions_examined: 0,
            versions_reclaimed: 0,
            bytes_reclaimed: 0,
            pages_freed: 0,
            oldest_active: 9,
            next_cursor: VacuumCursor {
                next_page_id: Some(next_page_id),
            },
            start_page: None,
            end_page: Some(bicdb_page::PageId::from(next_page_id)),
            stop_reason: VacuumStopReason::ByteLimit,
            elapsed_millis: 1,
            complete: false,
            stopped_early: true,
        }
    }

    fn governor(now_ms: u64) -> ResourceGovernor {
        ResourceGovernor::new(ResourceGovernorConfig::default(), now_ms).unwrap()
    }

    #[test]
    fn cursor_checkpoint_reloads_and_replay_is_inclusive_before_completion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let store_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let mut schedule = PagedVacuumSchedule::create(
            store_id,
            operation_id,
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        save_schedule(&path, &schedule, false).unwrap();
        let target = FakeTarget::new([
            FakeAction::Report(report(Some(42), false)),
            // This is the same page being repeated after the test restores the
            // prior durable schedule, modeling a crash before publication.
            FakeAction::Report(report(Some(42), false)),
            FakeAction::Report(report(None, true)),
        ]);
        let governor = governor(100);

        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(first, PagedVacuumScheduleAdvance::Progress { .. }));
        let durable_after_first = std::fs::read(&path).unwrap();
        let loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(loaded.cursor.next_page_id, Some(42));
        assert_eq!(loaded.totals.successful_steps, 1);

        // Run a step, then restore the pre-step checkpoint as if the process
        // crashed after page mutation but before the atomic rename.
        let mut lost = loaded;
        lost.tick_and_checkpoint(&path, operation_id, &target, &governor, 200, false)
            .unwrap();
        std::fs::write(&path, &durable_after_first).unwrap();
        let mut replay = load_schedule_if_exists(&path).unwrap().unwrap();
        replay
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 200, false)
            .unwrap();

        assert_eq!(
            *target.cursors.lock().unwrap(),
            vec![
                VacuumCursor::default(),
                VacuumCursor {
                    next_page_id: Some(42)
                },
                VacuumCursor {
                    next_page_id: Some(42)
                }
            ]
        );
        let completed = load_schedule_if_exists(&path).unwrap().unwrap();
        assert!(completed.completed);
        assert_eq!(completed.cursor, VacuumCursor::default());
        assert_eq!(completed.totals.successful_steps, 2);
    }

    #[test]
    fn resource_saturation_defers_without_touching_pages_and_persists_due_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let store_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let mut schedule = PagedVacuumSchedule::create(
            store_id,
            operation_id,
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        let target = FakeTarget::new([FakeAction::Report(report(None, true))]);

        let mut config = ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::Compaction)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, 100).unwrap();
        let held = governor
            .try_admit(ResourceLane::Compaction, schedule.limits.demand, 100)
            .unwrap();
        let outcome = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert_eq!(
            outcome,
            PagedVacuumScheduleAdvance::ResourceDeferred { retry_at_ms: 1_100 }
        );
        assert_eq!(target.calls(), 0);
        let loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(loaded.next_attempt_at_ms, Some(1_100));
        assert_eq!(loaded.totals.resource_deferrals, 1);
        drop(held);
        assert_eq!(governor.snapshot().background.active, 0);
    }

    #[test]
    fn failures_back_off_pause_durably_and_resume_is_operation_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let store_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let limits = PagedVacuumScheduleLimits {
            max_consecutive_failures: 2,
            ..PagedVacuumScheduleLimits::default()
        };
        let mut schedule =
            PagedVacuumSchedule::create(store_id, operation_id, 100, limits).unwrap();
        let target = FakeTarget::new([
            FakeAction::Error("first I/O failure"),
            FakeAction::Error("second I/O failure"),
            FakeAction::Report(report(None, true)),
        ]);
        let governor = governor(100);

        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(
            first,
            PagedVacuumScheduleAdvance::RetryScheduled {
                retry_at_ms: 1_100,
                consecutive_failures: 1,
                ..
            }
        ));
        let mut loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        let second = loaded
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 1_100, false)
            .unwrap();
        assert!(matches!(second, PagedVacuumScheduleAdvance::Paused { .. }));
        let mut paused = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(paused.totals.failures, 2);
        assert!(paused.paused_reason.is_some());
        assert!(paused
            .resume_and_checkpoint(&path, Uuid::new_v4(), 2_000, 2_000, false)
            .unwrap_err()
            .to_string()
            .contains("cannot be resumed"));
        paused
            .resume_and_checkpoint(&path, operation_id, 2_000, 2_000, false)
            .unwrap();
        let outcome = paused
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 2_000, false)
            .unwrap();
        assert!(matches!(
            outcome,
            PagedVacuumScheduleAdvance::Complete { .. }
        ));
        assert_eq!(governor.snapshot().background.active, 0);
    }

    #[test]
    fn byte_blocked_cursor_is_checkpointed_once_then_paused_without_hot_loop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let operation_id = Uuid::new_v4();
        let mut schedule = PagedVacuumSchedule::create(
            Uuid::new_v4(),
            operation_id,
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        let target = FakeTarget::new([
            FakeAction::Report(byte_blocked(77)),
            FakeAction::Report(byte_blocked(77)),
        ]);
        let governor = governor(100);
        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(first, PagedVacuumScheduleAdvance::Progress { .. }));
        let mut loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(loaded.cursor.next_page_id, Some(77));
        let second = loaded
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 200, false)
            .unwrap();
        assert!(matches!(second, PagedVacuumScheduleAdvance::Paused { .. }));
        let paused = load_schedule_if_exists(&path).unwrap().unwrap();
        assert!(paused
            .paused_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("no durable progress")));
        assert_eq!(target.calls(), 2);
    }

    /// The shape of the real liveness bug: the sweep DOES enter a page and its
    /// counters keep climbing, but the cursor never moves and nothing is
    /// durably freed. Counters that can re-count the same work must not be
    /// able to disguise a stalled sweep as a progressing one.
    fn spinning_on_page(page: u64) -> VacuumReport {
        VacuumReport {
            pages_scanned: 1,
            bytes_examined: 2 * 1024 * 1024,
            versions_examined: 128,
            // Climbing counters, and every one of them a re-count.
            versions_reclaimed: 127,
            bytes_reclaimed: 0,
            pages_freed: 0,
            oldest_active: 9,
            next_cursor: VacuumCursor {
                next_page_id: Some(page),
            },
            start_page: Some(bicdb_page::PageId::from(page)),
            end_page: Some(bicdb_page::PageId::from(page)),
            stop_reason: VacuumStopReason::ByteLimit,
            elapsed_millis: 1,
            complete: false,
            stopped_early: true,
        }
    }

    #[test]
    fn a_sweep_that_reenters_the_same_page_pauses_despite_climbing_counters() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let operation_id = Uuid::new_v4();
        let mut schedule = PagedVacuumSchedule::create(
            Uuid::new_v4(),
            operation_id,
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        let target = FakeTarget::new([
            FakeAction::Report(spinning_on_page(5)),
            FakeAction::Report(spinning_on_page(5)),
            FakeAction::Report(spinning_on_page(5)),
        ]);
        let governor = governor(100);

        // The first step moves None -> Some(5), which is real progress.
        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(first, PagedVacuumScheduleAdvance::Progress { .. }));

        // The second reports the same cursor with nothing durably freed. Under
        // the old guard this was accepted forever because `pages_scanned` was
        // not zero and `versions_reclaimed` kept rising.
        let mut loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        let second = loaded
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 200, false)
            .unwrap();
        assert!(
            matches!(second, PagedVacuumScheduleAdvance::Paused { .. }),
            "a stalled sweep was reported as progress: {second:?}"
        );
        assert_eq!(
            target.calls(),
            2,
            "the supervisor kept calling the target after it stopped advancing"
        );
    }

    #[test]
    fn corrupt_unsafe_or_store_mismatched_state_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let identity_path = dir.path().join("identity.json");
        let schedule_path = dir.path().join("vacuum.json");
        let identity = PagedMaintenanceIdentity::create(100).unwrap();
        crate::storage::write_atomic(
            &identity_path,
            &serde_json::to_vec(&identity).unwrap(),
            false,
        )
        .unwrap();
        let foreign = PagedVacuumSchedule::create(
            Uuid::new_v4(),
            Uuid::new_v4(),
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        save_schedule(&schedule_path, &foreign, false).unwrap();
        assert!(load_active_schedule(&identity_path, &schedule_path)
            .unwrap_err()
            .to_string()
            .contains("identity or bounds"));

        let local = PagedVacuumSchedule::create(
            identity.store_id,
            Uuid::new_v4(),
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        save_schedule(&schedule_path, &local, false).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&schedule_path).unwrap()).unwrap();
        value["forged_field"] = serde_json::json!(true);
        std::fs::write(&schedule_path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(load_active_schedule(&identity_path, &schedule_path).is_err());

        save_schedule(&schedule_path, &local, false).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&schedule_path).unwrap()).unwrap();
        value["state_sequence"] = serde_json::json!(999);
        std::fs::write(&schedule_path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(load_active_schedule(&identity_path, &schedule_path)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));

        std::fs::write(
            &schedule_path,
            vec![b'x'; (MAX_SCHEDULE_BYTES + 1) as usize],
        )
        .unwrap();
        assert!(load_active_schedule(&identity_path, &schedule_path)
            .unwrap_err()
            .to_string()
            .contains("unsafe or outside"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            std::fs::remove_file(&schedule_path).unwrap();
            let target = dir.path().join("target.json");
            std::fs::write(&target, b"{}").unwrap();
            symlink(&target, &schedule_path).unwrap();
            assert!(load_active_schedule(&identity_path, &schedule_path)
                .unwrap_err()
                .to_string()
                .contains("unsafe or outside"));
        }
    }

    #[test]
    fn clock_regression_and_stale_operation_fail_before_page_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vacuum.json");
        let store_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let mut schedule = PagedVacuumSchedule::create(
            store_id,
            operation_id,
            100,
            PagedVacuumScheduleLimits::default(),
        )
        .unwrap();
        let target = FakeTarget::new([FakeAction::Report(report(None, true))]);
        let governor = governor(100);
        assert!(schedule
            .tick_and_checkpoint(&path, Uuid::new_v4(), &target, &governor, 100, false)
            .unwrap_err()
            .to_string()
            .contains("stale supervisor"));
        assert!(schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 99, false)
            .unwrap_err()
            .to_string()
            .contains("clock regressed"));
        assert_eq!(target.calls(), 0);
    }

    #[test]
    fn missing_shared_identity_cannot_be_recreated_over_any_maintenance_state() {
        let dir = tempfile::tempdir().unwrap();
        let identity_path = dir.path().join("identity.json");
        let vacuum_path = dir.path().join("vacuum.json");
        let integrity_path = dir.path().join(PAGED_INTEGRITY_SCHEDULE_FILE);
        std::fs::write(&integrity_path, b"protected state").unwrap();
        let error = load_or_create_identity(
            &identity_path,
            &[vacuum_path.as_path(), integrity_path.as_path()],
            100,
            false,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("immutable store identity is missing"));
        assert!(!identity_path.exists());
    }
}
