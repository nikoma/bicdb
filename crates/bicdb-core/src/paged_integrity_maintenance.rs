//! Durable, resource-governed supervision for server-paged integrity sweeps.
//!
//! The page engine owns two independently bounded read-only primitives: B+
//! tree structure verification and reachable MVCC-chain verification. This
//! module composes them into one crash-resumable operation. The structural
//! phase must publish a valid completion checkpoint before the MVCC phase can
//! begin; a structural fault terminates fail-closed without walking row chains.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use bicdb_page::{
    BTreeVerifyCursor, BTreeVerifyLimits, BTreeVerifyStepReport, BTreeVerifyStopReason,
    VersionChainVerifyCursor, VersionChainVerifyLimits, VersionChainVerifyStepReport,
    VersionChainVerifyStopReason, VERSION_HEADER_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{BicDbError, Result};
use crate::paged_checkpoint_maintenance::DEFAULT_PAGED_CHECKPOINT_SCHEDULE;
use crate::paged_collection::PagedRecords;
use crate::paged_maintenance::{
    load_identity, load_or_create_identity, DEFAULT_PAGED_MAINTENANCE_IDENTITY,
    DEFAULT_PAGED_VACUUM_SCHEDULE,
};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const PAGED_INTEGRITY_SCHEDULE_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_PAGED_INTEGRITY_SCHEDULE: &str = "maintenance/paged/integrity.json";

const MIN_SCHEDULE_BYTES: u64 = 8 * 1024;
const MAX_SCHEDULE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 4 * 1024;
const MAX_DELAY_MILLIS: u64 = 24 * 60 * 60 * 1_000;

fn integrity_error(message: impl Into<String>) -> BicDbError {
    BicDbError::PagedStorage(format!("paged integrity maintenance: {}", message.into()))
}

/// Immutable resource and retry policy for one complete integrity sweep.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedIntegrityScheduleLimits {
    pub btree: BTreeVerifyLimits,
    pub version_chains: VersionChainVerifyLimits,
    pub step_interval_ms: u64,
    pub saturation_retry_ms: u64,
    pub failure_retry_base_ms: u64,
    pub failure_retry_max_ms: u64,
    pub max_consecutive_failures: u32,
    pub max_schedule_state_bytes: u64,
    /// Peak reservation and token-bucket charge acquired before a page is
    /// touched. Integrity runs use the anti-entropy background lane.
    pub demand: ResourceDemand,
}

impl Default for PagedIntegrityScheduleLimits {
    fn default() -> Self {
        let btree = BTreeVerifyLimits::default();
        let version_chains = VersionChainVerifyLimits {
            max_fault_sample_bytes: 256 * 1024,
            ..VersionChainVerifyLimits::default()
        };
        let io_bytes = btree.max_page_bytes.max(version_chains.max_bytes);
        Self {
            btree,
            version_chains,
            step_interval_ms: 100,
            saturation_retry_ms: 1_000,
            failure_retry_base_ms: 1_000,
            failure_retry_max_ms: 5 * 60 * 1_000,
            max_consecutive_failures: 32,
            max_schedule_state_bytes: 1024 * 1024,
            demand: ResourceDemand {
                memory_bytes: 64 * 1024 * 1024,
                io_bytes,
                cpu_slots: 1,
                io_charge_bytes: io_bytes,
            },
        }
    }
}

impl PagedIntegrityScheduleLimits {
    pub fn validate(&self) -> Result<()> {
        self.btree
            .validate()
            .map_err(|error| integrity_error(error.to_string()))?;
        self.version_chains
            .validate()
            .map_err(|error| integrity_error(error.to_string()))?;
        self.demand.validate()?;

        let cycle_bytes = self.btree.max_leaf_pages.saturating_mul(32);
        let minimum_memory = cycle_bytes
            .saturating_add(self.version_chains.max_fault_sample_bytes)
            .saturating_add(self.btree.max_cursor_key_bytes as u64)
            .saturating_add(self.version_chains.max_cursor_key_bytes as u64)
            .saturating_add(8 * 1024 * 1024);
        let minimum_io = self.btree.max_page_bytes.max(self.version_chains.max_bytes);
        let minimum_state = self
            .version_chains
            .max_fault_sample_bytes
            .saturating_add(self.btree.max_cursor_key_bytes as u64)
            .saturating_add(self.version_chains.max_cursor_key_bytes as u64)
            .saturating_add(64 * 1024);
        if !(1..=MAX_DELAY_MILLIS).contains(&self.step_interval_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.saturation_retry_ms)
            || !(1..=MAX_DELAY_MILLIS).contains(&self.failure_retry_base_ms)
            || self.failure_retry_max_ms < self.failure_retry_base_ms
            || self.failure_retry_max_ms > MAX_DELAY_MILLIS
            || !(1..=1_000_000).contains(&self.max_consecutive_failures)
            || !(MIN_SCHEDULE_BYTES..=MAX_SCHEDULE_BYTES).contains(&self.max_schedule_state_bytes)
            || self.max_schedule_state_bytes < minimum_state
            || self.demand.memory_bytes < minimum_memory
            || self.demand.io_bytes < minimum_io
            || self.demand.io_charge_bytes < minimum_io
        {
            return Err(integrity_error(
                "schedule timing, state, verification, or resource bounds are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PagedIntegrityPhase {
    Structural,
    VersionChains,
    Complete,
}

/// Monotonic totals from reports that reached an atomic schedule checkpoint.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedIntegrityTotals {
    pub successful_steps: u64,
    pub structural_steps: u64,
    pub version_chain_steps: u64,
    pub structural_entries: u64,
    pub structural_leaf_pages: u64,
    pub structural_descent_pages: u64,
    pub structural_page_bytes: u64,
    pub structural_key_bytes: u64,
    pub structural_faults: u64,
    pub keys_examined: u64,
    pub versions_examined: u64,
    pub version_header_bytes: u64,
    pub invalid_heads: u64,
    pub malformed_versions: u64,
    pub cycles: u64,
    pub chain_limits_exceeded: u64,
    pub fault_samples_dropped: u64,
    pub failures: u64,
    pub resource_deferrals: u64,
}

impl PagedIntegrityTotals {
    fn observe_btree(&mut self, report: &BTreeVerifyStepReport) {
        self.successful_steps = self.successful_steps.saturating_add(1);
        self.structural_steps = self.structural_steps.saturating_add(1);
        self.structural_entries = self
            .structural_entries
            .saturating_add(report.entries_examined);
        self.structural_leaf_pages = self
            .structural_leaf_pages
            .saturating_add(report.leaf_pages_examined);
        self.structural_descent_pages = self
            .structural_descent_pages
            .saturating_add(report.descent_pages_examined);
        self.structural_page_bytes = self
            .structural_page_bytes
            .saturating_add(report.page_bytes_examined);
        self.structural_key_bytes = self
            .structural_key_bytes
            .saturating_add(report.key_bytes_examined);
        self.structural_faults = self
            .structural_faults
            .saturating_add(u64::from(report.fault.is_some()));
    }

    fn observe_version_chains(&mut self, report: &VersionChainVerifyStepReport) {
        self.successful_steps = self.successful_steps.saturating_add(1);
        self.version_chain_steps = self.version_chain_steps.saturating_add(1);
        self.keys_examined = self
            .keys_examined
            .saturating_add(report.version_chains.keys_examined);
        self.versions_examined = self
            .versions_examined
            .saturating_add(report.version_chains.versions_examined);
        self.version_header_bytes = self
            .version_header_bytes
            .saturating_add(report.bytes_examined);
        self.invalid_heads = self
            .invalid_heads
            .saturating_add(report.version_chains.invalid_heads);
        self.malformed_versions = self
            .malformed_versions
            .saturating_add(report.version_chains.malformed_versions);
        self.cycles = self.cycles.saturating_add(report.version_chains.cycles);
        self.chain_limits_exceeded = self
            .chain_limits_exceeded
            .saturating_add(report.version_chains.limit_exceeded);
        self.fault_samples_dropped = self
            .fault_samples_dropped
            .saturating_add(report.fault_samples_dropped);
    }

    fn valid(&self) -> bool {
        self.structural_faults == 0
            && self.invalid_heads == 0
            && self.malformed_versions == 0
            && self.cycles == 0
            && self.chain_limits_exceeded == 0
    }
}

/// Durable state for one structural-then-MVCC integrity sweep.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PagedIntegritySchedule {
    pub format_version: u32,
    pub store_id: Uuid,
    pub operation_id: Uuid,
    pub limits: PagedIntegrityScheduleLimits,
    pub phase: PagedIntegrityPhase,
    pub btree_cursor: BTreeVerifyCursor,
    pub version_chain_cursor: VersionChainVerifyCursor,
    pub totals: PagedIntegrityTotals,
    pub last_btree_report: Option<BTreeVerifyStepReport>,
    pub last_version_chain_report: Option<VersionChainVerifyStepReport>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_observed_at_ms: u64,
    pub next_attempt_at_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub paused_reason: Option<String>,
    pub completed: bool,
    pub valid: bool,
    pub state_sequence: u64,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PagedIntegrityScheduleAdvance {
    NotDue {
        next_attempt_at_ms: u64,
    },
    ResourceDeferred {
        retry_at_ms: u64,
    },
    StructuralProgress {
        next_attempt_at_ms: u64,
        report: BTreeVerifyStepReport,
    },
    PhaseAdvanced {
        phase: PagedIntegrityPhase,
        next_attempt_at_ms: u64,
    },
    VersionChainProgress {
        next_attempt_at_ms: u64,
        report: VersionChainVerifyStepReport,
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
        valid: bool,
        totals: PagedIntegrityTotals,
    },
}

impl PagedIntegritySchedule {
    fn create(
        store_id: Uuid,
        operation_id: Uuid,
        now_ms: u64,
        limits: PagedIntegrityScheduleLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if store_id.is_nil() || operation_id.is_nil() {
            return Err(integrity_error("store and operation IDs must be non-nil"));
        }
        let mut schedule = Self {
            format_version: PAGED_INTEGRITY_SCHEDULE_FORMAT_VERSION,
            store_id,
            operation_id,
            limits,
            phase: PagedIntegrityPhase::Structural,
            btree_cursor: BTreeVerifyCursor::default(),
            version_chain_cursor: VersionChainVerifyCursor::default(),
            totals: PagedIntegrityTotals::default(),
            last_btree_report: None,
            last_version_chain_report: None,
            started_at_ms: now_ms,
            updated_at_ms: now_ms,
            last_observed_at_ms: now_ms,
            next_attempt_at_ms: Some(now_ms),
            consecutive_failures: 0,
            last_error: None,
            paused_reason: None,
            completed: false,
            valid: true,
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
        if self.format_version != PAGED_INTEGRITY_SCHEDULE_FORMAT_VERSION
            || self.store_id.is_nil()
            || self.operation_id.is_nil()
            || self.store_id != expected_store_id
            || self.updated_at_ms < self.started_at_ms
            || self.last_observed_at_ms < self.started_at_ms
            || self.consecutive_failures > self.limits.max_consecutive_failures
            || self.valid != self.totals.valid()
            || self
                .last_error
                .as_ref()
                .is_some_and(|error| error.is_empty() || error.len() > MAX_ERROR_BYTES)
            || self
                .paused_reason
                .as_ref()
                .is_some_and(|reason| reason.is_empty() || reason.len() > MAX_ERROR_BYTES)
        {
            return Err(integrity_error("schedule identity or bounds are invalid"));
        }
        let inactive = self.completed || self.paused_reason.is_some();
        if inactive != self.next_attempt_at_ms.is_none()
            || self.completed && self.paused_reason.is_some()
            || self.completed != (self.phase == PagedIntegrityPhase::Complete)
            || self.totals.structural_steps == 0 && self.last_btree_report.is_some()
            || self.totals.structural_steps > 0 && self.last_btree_report.is_none()
            || self.totals.version_chain_steps == 0 && self.last_version_chain_report.is_some()
            || self.totals.version_chain_steps > 0 && self.last_version_chain_report.is_none()
            || self
                .next_attempt_at_ms
                .is_some_and(|due| due < self.started_at_ms)
        {
            return Err(integrity_error(
                "schedule active, paused, completed, phase, or report state disagrees",
            ));
        }
        match self.phase {
            PagedIntegrityPhase::Structural => {
                if self.completed
                    || self.totals.version_chain_steps != 0
                    || self.version_chain_cursor != VersionChainVerifyCursor::default()
                    || self.last_version_chain_report.is_some()
                    || self.last_btree_report.as_ref().is_some_and(|report| {
                        report.complete || report.fault.is_some() || !report.valid
                    })
                {
                    return Err(integrity_error("structural phase state is inconsistent"));
                }
            }
            PagedIntegrityPhase::VersionChains => {
                if self.completed
                    || self.btree_cursor != BTreeVerifyCursor::default()
                    || self.last_btree_report.as_ref().is_none_or(|report| {
                        !report.complete || !report.valid || report.fault.is_some()
                    })
                    || self
                        .last_version_chain_report
                        .as_ref()
                        .is_some_and(|report| report.complete)
                {
                    return Err(integrity_error("MVCC phase state is inconsistent"));
                }
            }
            PagedIntegrityPhase::Complete => {
                let structural_terminal = self.last_btree_report.as_ref().is_some_and(|report| {
                    (report.complete && report.valid && report.fault.is_none())
                        || (report.stop_reason == BTreeVerifyStopReason::StructuralFault
                            && !report.valid
                            && report.fault.is_some())
                });
                let mvcc_terminal = if self.valid || self.totals.version_chain_steps > 0 {
                    self.last_version_chain_report
                        .as_ref()
                        .is_some_and(|report| report.complete)
                } else {
                    self.totals.structural_faults > 0
                        && self.totals.version_chain_steps == 0
                        && self.last_version_chain_report.is_none()
                };
                if !self.completed
                    || self.btree_cursor != BTreeVerifyCursor::default()
                    || self.version_chain_cursor != VersionChainVerifyCursor::default()
                    || !structural_terminal
                    || !mvcc_terminal
                {
                    return Err(integrity_error("completed phase state is inconsistent"));
                }
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(integrity_error("schedule state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_schedule_state_bytes {
            return Err(integrity_error("schedule state exceeds its byte bound"));
        }
        Ok(())
    }

    pub(crate) fn tick_and_checkpoint<T: PagedIntegrityTarget>(
        &mut self,
        path: &Path,
        expected_operation_id: Uuid,
        target: &T,
        governor: &ResourceGovernor,
        now_ms: u64,
        fsync: bool,
    ) -> Result<PagedIntegrityScheduleAdvance> {
        self.validate()?;
        if self.operation_id != expected_operation_id {
            return Err(integrity_error(
                "stale supervisor operation ID cannot advance the active schedule",
            ));
        }
        if now_ms < self.last_observed_at_ms {
            return Err(integrity_error("scheduler clock regressed"));
        }
        target.validate_integrity_limits(&self.limits)?;
        self.last_observed_at_ms = now_ms;
        self.updated_at_ms = now_ms;

        if self.completed {
            let outcome = self.completion();
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        if let Some(reason) = &self.paused_reason {
            let outcome = PagedIntegrityScheduleAdvance::Paused {
                reason: reason.clone(),
            };
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        let due = self
            .next_attempt_at_ms
            .ok_or_else(|| integrity_error("active schedule has no next attempt"))?;
        if now_ms < due {
            self.refresh_and_save(path, fsync)?;
            return Ok(PagedIntegrityScheduleAdvance::NotDue {
                next_attempt_at_ms: due,
            });
        }

        let permit = match governor.try_admit(ResourceLane::AntiEntropy, self.limits.demand, now_ms)
        {
            Ok(permit) => permit,
            Err(BicDbError::ResourceGovernance(_)) => {
                let retry_at_ms = bounded_due(now_ms, self.limits.saturation_retry_ms)?;
                self.totals.resource_deferrals = self.totals.resource_deferrals.saturating_add(1);
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                return Ok(PagedIntegrityScheduleAdvance::ResourceDeferred { retry_at_ms });
            }
            Err(error) => return Err(error),
        };

        let result = match self.phase {
            PagedIntegrityPhase::Structural => target
                .verify_btree_step(self.btree_cursor.clone(), self.limits.btree)
                .map(IntegrityStep::BTree),
            PagedIntegrityPhase::VersionChains => target
                .verify_version_chains_step(
                    self.version_chain_cursor.clone(),
                    self.limits.version_chains,
                )
                .map(IntegrityStep::VersionChains),
            PagedIntegrityPhase::Complete => {
                drop(permit);
                return Err(integrity_error("active schedule is in its completed phase"));
            }
        };
        drop(permit);

        match result {
            Ok(IntegrityStep::BTree(report)) => {
                validate_btree_report(&report, &self.limits.btree)?;
                self.consecutive_failures = 0;
                self.last_error = None;
                self.totals.observe_btree(&report);
                self.valid = self.totals.valid();
                self.last_btree_report = Some(report.clone());

                if report.fault.is_some() {
                    self.phase = PagedIntegrityPhase::Complete;
                    self.completed = true;
                    self.btree_cursor = BTreeVerifyCursor::default();
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(self.completion());
                }
                if report.complete {
                    self.btree_cursor = BTreeVerifyCursor::default();
                    self.phase = PagedIntegrityPhase::VersionChains;
                    let next_attempt_at_ms = bounded_due(now_ms, self.limits.step_interval_ms)?;
                    self.next_attempt_at_ms = Some(next_attempt_at_ms);
                    self.refresh_and_save(path, fsync)?;
                    return Ok(PagedIntegrityScheduleAdvance::PhaseAdvanced {
                        phase: self.phase,
                        next_attempt_at_ms,
                    });
                }

                let previous = self.btree_cursor.clone();
                self.btree_cursor = report.next_cursor.clone();
                if self.btree_cursor == previous
                    && report.entries_examined == 0
                    && report.leaf_pages_examined == 0
                {
                    return self.pause_and_save(
                        path,
                        "structural verifier made no cursor or page progress under its immutable envelope",
                        fsync,
                    );
                }
                let next_attempt_at_ms = bounded_due(now_ms, self.limits.step_interval_ms)?;
                self.next_attempt_at_ms = Some(next_attempt_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedIntegrityScheduleAdvance::StructuralProgress {
                    next_attempt_at_ms,
                    report,
                })
            }
            Ok(IntegrityStep::VersionChains(report)) => {
                validate_version_chain_report(&report, &self.limits.version_chains)?;
                self.consecutive_failures = 0;
                self.last_error = None;
                self.totals.observe_version_chains(&report);
                self.valid = self.totals.valid();
                self.last_version_chain_report = Some(report.clone());

                if report.complete {
                    self.version_chain_cursor = VersionChainVerifyCursor::default();
                    self.phase = PagedIntegrityPhase::Complete;
                    self.completed = true;
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(self.completion());
                }

                let previous = self.version_chain_cursor.clone();
                self.version_chain_cursor = report.next_cursor.clone();
                if self.version_chain_cursor == previous && report.version_chains.keys_examined == 0
                {
                    return self.pause_and_save(
                        path,
                        "MVCC verifier made no cursor progress under its immutable envelope",
                        fsync,
                    );
                }
                let next_attempt_at_ms = bounded_due(now_ms, self.limits.step_interval_ms)?;
                self.next_attempt_at_ms = Some(next_attempt_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(PagedIntegrityScheduleAdvance::VersionChainProgress {
                    next_attempt_at_ms,
                    report,
                })
            }
            Err(error) => self.record_failure(path, error, now_ms, fsync),
        }
    }

    fn completion(&self) -> PagedIntegrityScheduleAdvance {
        PagedIntegrityScheduleAdvance::Complete {
            operation_id: self.operation_id,
            valid: self.valid,
            totals: self.totals,
        }
    }

    fn record_failure(
        &mut self,
        path: &Path,
        error: BicDbError,
        now_ms: u64,
        fsync: bool,
    ) -> Result<PagedIntegrityScheduleAdvance> {
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
        Ok(PagedIntegrityScheduleAdvance::RetryScheduled {
            retry_at_ms,
            consecutive_failures: self.consecutive_failures,
            error: message,
        })
    }

    fn pause_and_save(
        &mut self,
        path: &Path,
        reason: impl Into<String>,
        fsync: bool,
    ) -> Result<PagedIntegrityScheduleAdvance> {
        let reason = bounded_error(reason.into());
        self.paused_reason = Some(reason.clone());
        self.next_attempt_at_ms = None;
        self.refresh_and_save(path, fsync)?;
        Ok(PagedIntegrityScheduleAdvance::Paused { reason })
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
            return Err(integrity_error(
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
            return Err(integrity_error(
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

    fn refresh_and_save(&mut self, path: &Path, fsync: bool) -> Result<()> {
        self.state_sequence = self
            .state_sequence
            .checked_add(1)
            .ok_or_else(|| integrity_error("schedule state sequence exhausted"))?;
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
            limits: &'a PagedIntegrityScheduleLimits,
            phase: PagedIntegrityPhase,
            btree_cursor: &'a BTreeVerifyCursor,
            version_chain_cursor: &'a VersionChainVerifyCursor,
            totals: PagedIntegrityTotals,
            last_btree_report: &'a Option<BTreeVerifyStepReport>,
            last_version_chain_report: &'a Option<VersionChainVerifyStepReport>,
            started_at_ms: u64,
            updated_at_ms: u64,
            last_observed_at_ms: u64,
            next_attempt_at_ms: Option<u64>,
            consecutive_failures: u32,
            last_error: &'a Option<String>,
            paused_reason: &'a Option<String>,
            completed: bool,
            valid: bool,
            state_sequence: u64,
        }
        let input = Input {
            format_version: self.format_version,
            store_id: self.store_id,
            operation_id: self.operation_id,
            limits: &self.limits,
            phase: self.phase,
            btree_cursor: &self.btree_cursor,
            version_chain_cursor: &self.version_chain_cursor,
            totals: self.totals,
            last_btree_report: &self.last_btree_report,
            last_version_chain_report: &self.last_version_chain_report,
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            last_observed_at_ms: self.last_observed_at_ms,
            next_attempt_at_ms: self.next_attempt_at_ms,
            consecutive_failures: self.consecutive_failures,
            last_error: &self.last_error,
            paused_reason: &self.paused_reason,
            completed: self.completed,
            valid: self.valid,
            state_sequence: self.state_sequence,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&input)?)))
    }
}

enum IntegrityStep {
    BTree(BTreeVerifyStepReport),
    VersionChains(VersionChainVerifyStepReport),
}

pub(crate) trait PagedIntegrityTarget {
    fn validate_integrity_limits(&self, limits: &PagedIntegrityScheduleLimits) -> Result<()>;
    fn verify_btree_step(
        &self,
        cursor: BTreeVerifyCursor,
        limits: BTreeVerifyLimits,
    ) -> Result<BTreeVerifyStepReport>;
    fn verify_version_chains_step(
        &self,
        cursor: VersionChainVerifyCursor,
        limits: VersionChainVerifyLimits,
    ) -> Result<VersionChainVerifyStepReport>;
}

impl PagedIntegrityTarget for PagedRecords {
    fn validate_integrity_limits(&self, limits: &PagedIntegrityScheduleLimits) -> Result<()> {
        limits.validate()?;
        self.validate_new_btree_verify_limits(&limits.btree)
    }

    fn verify_btree_step(
        &self,
        cursor: BTreeVerifyCursor,
        limits: BTreeVerifyLimits,
    ) -> Result<BTreeVerifyStepReport> {
        PagedRecords::verify_btree_step(self, cursor, limits)
    }

    fn verify_version_chains_step(
        &self,
        cursor: VersionChainVerifyCursor,
        limits: VersionChainVerifyLimits,
    ) -> Result<VersionChainVerifyStepReport> {
        PagedRecords::verify_version_chains_step(self, cursor, limits)
    }
}

pub(crate) fn start_schedule(
    identity_path: &Path,
    schedule_path: &Path,
    operation_id: Uuid,
    now_ms: u64,
    limits: PagedIntegrityScheduleLimits,
    target: &PagedRecords,
    fsync: bool,
) -> Result<PagedIntegritySchedule> {
    target.validate_integrity_limits(&limits)?;
    if let Some(existing) = load_schedule_if_exists(schedule_path)? {
        let identity = load_identity(identity_path)?;
        existing.validate_for_store(identity.store_id)?;
        if !existing.completed && existing.paused_reason.is_none() {
            return Err(integrity_error(format!(
                "operation {} is still active",
                existing.operation_id
            )));
        }
    }
    let vacuum_path = schedule_path.with_file_name(
        Path::new(DEFAULT_PAGED_VACUUM_SCHEDULE)
            .file_name()
            .expect("vacuum schedule has a file name"),
    );
    let checkpoint_path = schedule_path.with_file_name(
        Path::new(DEFAULT_PAGED_CHECKPOINT_SCHEDULE)
            .file_name()
            .expect("checkpoint schedule has a file name"),
    );
    let identity = load_or_create_identity(
        identity_path,
        &[
            vacuum_path.as_path(),
            schedule_path,
            checkpoint_path.as_path(),
        ],
        now_ms,
        fsync,
    )?;
    let schedule = PagedIntegritySchedule::create(identity.store_id, operation_id, now_ms, limits)?;
    save_schedule(schedule_path, &schedule, fsync)?;
    Ok(schedule)
}

pub(crate) fn load_active_schedule(
    identity_path: &Path,
    schedule_path: &Path,
) -> Result<Option<PagedIntegritySchedule>> {
    let Some(schedule) = load_schedule_if_exists(schedule_path)? else {
        return Ok(None);
    };
    let identity = load_identity(identity_path)?;
    schedule.validate_for_store(identity.store_id)?;
    Ok(Some(schedule))
}

fn save_schedule(path: &Path, schedule: &PagedIntegritySchedule, fsync: bool) -> Result<()> {
    schedule.validate()?;
    let bytes = serde_json::to_vec(schedule)?;
    if bytes.len() as u64 > schedule.limits.max_schedule_state_bytes {
        return Err(integrity_error("schedule exceeds its write bound"));
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn load_schedule_if_exists(path: &Path) -> Result<Option<PagedIntegritySchedule>> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_regular_file(path, MAX_SCHEDULE_BYTES, "schedule")?;
    let schedule: PagedIntegritySchedule = serde_json::from_slice(&bytes)?;
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
        return Err(integrity_error(format!(
            "{label} file is unsafe or outside its byte bound"
        )));
    }
    let length = metadata.len();
    let file = open_read_only_no_follow(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() != length {
        return Err(integrity_error(format!(
            "{label} changed while it was being opened"
        )));
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > max_bytes {
        return Err(integrity_error(format!(
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

fn validate_btree_report(report: &BTreeVerifyStepReport, limits: &BTreeVerifyLimits) -> Result<()> {
    let faulted = report.fault.is_some();
    if report.entries_examined > limits.max_entries
        || report.leaf_pages_examined > limits.max_leaf_pages
        || report.page_bytes_examined > limits.max_page_bytes
        || report.key_bytes_examined > limits.max_key_bytes
        || report
            .height
            .is_some_and(|height| height > limits.max_height)
        || report.valid == faulted
        || report.complete != (report.stop_reason == BTreeVerifyStopReason::Complete)
        || faulted != (report.stop_reason == BTreeVerifyStopReason::StructuralFault)
        || report.complete && report.next_cursor != BTreeVerifyCursor::default()
        || !report.complete && !faulted && report.next_cursor.next_leaf.is_none()
    {
        return Err(integrity_error(
            "page engine returned an inconsistent or over-budget structural report",
        ));
    }
    Ok(())
}

fn validate_version_chain_report(
    report: &VersionChainVerifyStepReport,
    limits: &VersionChainVerifyLimits,
) -> Result<()> {
    let chains = &report.version_chains;
    let faults = chains
        .invalid_heads
        .saturating_add(chains.malformed_versions)
        .saturating_add(chains.cycles)
        .saturating_add(chains.limit_exceeded);
    let expected_bytes = chains
        .versions_examined
        .checked_mul(VERSION_HEADER_BYTES as u64)
        .ok_or_else(|| integrity_error("MVCC report byte count overflowed"))?;
    let encoded_sample_bytes = chains
        .fault_samples
        .iter()
        .try_fold(0_u64, |total, sample| {
            let length = serde_json::to_vec(sample)?.len() as u64;
            Ok::<u64, BicDbError>(total.saturating_add(length))
        })?;
    if chains.keys_examined > limits.max_keys
        || chains.versions_examined > limits.max_versions
        || report.bytes_examined > limits.max_bytes
        || report.bytes_examined != expected_bytes
        || faults > chains.keys_examined
        || chains.valid != (faults == 0)
        || chains.fault_samples.len() > limits.max_fault_samples
        || report.fault_sample_bytes > limits.max_fault_sample_bytes
        || report.fault_sample_bytes != encoded_sample_bytes
        || report.complete != (report.stop_reason == VersionChainVerifyStopReason::Complete)
        || report.complete && report.next_cursor != VersionChainVerifyCursor::default()
        || !report.complete && report.next_cursor.next_key.is_none()
    {
        return Err(integrity_error(
            "page engine returned an inconsistent or over-budget MVCC report",
        ));
    }
    Ok(())
}

fn bounded_due(now_ms: u64, delay_ms: u64) -> Result<u64> {
    now_ms
        .checked_add(delay_ms)
        .ok_or_else(|| integrity_error("scheduler due time overflow"))
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
        return Err(integrity_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

pub(crate) fn maintenance_paths(root: &Path) -> (PathBuf, PathBuf) {
    (
        root.join(DEFAULT_PAGED_MAINTENANCE_IDENTITY),
        root.join(DEFAULT_PAGED_INTEGRITY_SCHEDULE),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use bicdb_page::{BTreeVerifyFault, VersionChainVerifyReport, VersionChainVerifyStopReason};

    use super::*;
    use crate::ResourceGovernorConfig;

    struct FakeTarget {
        btree: Mutex<VecDeque<Result<BTreeVerifyStepReport>>>,
        chains: Mutex<VecDeque<Result<VersionChainVerifyStepReport>>>,
        btree_cursors: Mutex<Vec<BTreeVerifyCursor>>,
        chain_cursors: Mutex<Vec<VersionChainVerifyCursor>>,
    }

    impl FakeTarget {
        fn new(
            btree: impl IntoIterator<Item = Result<BTreeVerifyStepReport>>,
            chains: impl IntoIterator<Item = Result<VersionChainVerifyStepReport>>,
        ) -> Self {
            Self {
                btree: Mutex::new(btree.into_iter().collect()),
                chains: Mutex::new(chains.into_iter().collect()),
                btree_cursors: Mutex::new(Vec::new()),
                chain_cursors: Mutex::new(Vec::new()),
            }
        }
    }

    impl PagedIntegrityTarget for FakeTarget {
        fn validate_integrity_limits(&self, limits: &PagedIntegrityScheduleLimits) -> Result<()> {
            limits.validate()
        }

        fn verify_btree_step(
            &self,
            cursor: BTreeVerifyCursor,
            _limits: BTreeVerifyLimits,
        ) -> Result<BTreeVerifyStepReport> {
            self.btree_cursors.lock().unwrap().push(cursor);
            self.btree
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(integrity_error("no fake structural action")))
        }

        fn verify_version_chains_step(
            &self,
            cursor: VersionChainVerifyCursor,
            _limits: VersionChainVerifyLimits,
        ) -> Result<VersionChainVerifyStepReport> {
            self.chain_cursors.lock().unwrap().push(cursor);
            self.chains
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(integrity_error("no fake MVCC action")))
        }
    }

    fn limits() -> PagedIntegrityScheduleLimits {
        let btree = BTreeVerifyLimits {
            max_entries: 4,
            max_leaf_pages: 4,
            max_page_bytes: 128 * 1024 * 1024,
            max_key_bytes: 64 * 1024,
            max_duration_millis: 1_000,
            max_cursor_key_bytes: 64,
            max_height: 64,
        };
        let version_chains = VersionChainVerifyLimits {
            max_keys: 8,
            max_versions: 16,
            max_versions_per_chain: 4,
            max_bytes: 16 * VERSION_HEADER_BYTES as u64,
            max_duration_millis: 1_000,
            max_fault_samples: 0,
            max_fault_sample_bytes: 0,
            max_cursor_key_bytes: 64,
        };
        let io_bytes = btree.max_page_bytes.max(version_chains.max_bytes);
        PagedIntegrityScheduleLimits {
            btree,
            version_chains,
            step_interval_ms: 1,
            saturation_retry_ms: 1,
            failure_retry_base_ms: 1,
            failure_retry_max_ms: 8,
            max_consecutive_failures: 2,
            max_schedule_state_bytes: 128 * 1024,
            demand: ResourceDemand {
                memory_bytes: 16 * 1024 * 1024,
                io_bytes,
                cpu_slots: 1,
                io_charge_bytes: io_bytes,
            },
        }
    }

    fn partial_btree() -> BTreeVerifyStepReport {
        BTreeVerifyStepReport {
            entries_examined: 4,
            leaf_pages_examined: 1,
            descent_pages_examined: 1,
            page_bytes_examined: 16 * 1024,
            key_bytes_examined: 32,
            height: Some(1),
            next_cursor: BTreeVerifyCursor {
                next_leaf: Some(2),
                after_key: Some(b"key-4".to_vec()),
                resume_within_leaf: true,
                leaf_pages_traversed: 0,
                page_count_bound: 8,
            },
            stop_reason: BTreeVerifyStopReason::EntryLimit,
            fault: None,
            elapsed_millis: 1,
            complete: false,
            valid: true,
        }
    }

    fn complete_btree() -> BTreeVerifyStepReport {
        BTreeVerifyStepReport {
            entries_examined: 2,
            leaf_pages_examined: 1,
            descent_pages_examined: 0,
            page_bytes_examined: 8 * 1024,
            key_bytes_examined: 16,
            height: None,
            next_cursor: BTreeVerifyCursor::default(),
            stop_reason: BTreeVerifyStopReason::Complete,
            fault: None,
            elapsed_millis: 1,
            complete: true,
            valid: true,
        }
    }

    fn complete_chains() -> VersionChainVerifyStepReport {
        VersionChainVerifyStepReport {
            version_chains: VersionChainVerifyReport {
                keys_examined: 6,
                versions_examined: 6,
                valid: true,
                ..VersionChainVerifyReport::default()
            },
            bytes_examined: 6 * VERSION_HEADER_BYTES as u64,
            fault_sample_bytes: 0,
            fault_samples_dropped: 0,
            next_cursor: VersionChainVerifyCursor::default(),
            stop_reason: VersionChainVerifyStopReason::Complete,
            elapsed_millis: 1,
            complete: true,
        }
    }

    fn governor(now_ms: u64) -> ResourceGovernor {
        ResourceGovernor::new(ResourceGovernorConfig::default(), now_ms).unwrap()
    }

    #[test]
    fn phase_boundary_and_cursors_are_atomically_restartable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let target = FakeTarget::new(
            [Ok(partial_btree()), Ok(complete_btree())],
            [Ok(complete_chains())],
        );
        let governor = governor(100);

        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(
            first,
            PagedIntegrityScheduleAdvance::StructuralProgress { .. }
        ));
        let mut loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(loaded.phase, PagedIntegrityPhase::Structural);
        assert_eq!(loaded.totals.structural_steps, 1);
        assert_eq!(loaded.btree_cursor.next_leaf, Some(2));

        let second = loaded
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 101, false)
            .unwrap();
        assert_eq!(
            second,
            PagedIntegrityScheduleAdvance::PhaseAdvanced {
                phase: PagedIntegrityPhase::VersionChains,
                next_attempt_at_ms: 102,
            }
        );
        let mut phase_checkpoint = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(phase_checkpoint.phase, PagedIntegrityPhase::VersionChains);
        assert_eq!(phase_checkpoint.totals.version_chain_steps, 0);
        assert_eq!(
            phase_checkpoint.version_chain_cursor,
            VersionChainVerifyCursor::default()
        );

        let terminal = phase_checkpoint
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 102, false)
            .unwrap();
        assert!(matches!(
            terminal,
            PagedIntegrityScheduleAdvance::Complete { valid: true, .. }
        ));
        let completed = load_schedule_if_exists(&path).unwrap().unwrap();
        assert!(completed.completed);
        assert!(completed.valid);
        assert_eq!(completed.phase, PagedIntegrityPhase::Complete);
        assert_eq!(completed.totals.structural_entries, 6);
        assert_eq!(completed.totals.keys_examined, 6);
        assert_eq!(governor.snapshot().background.active, 0);
    }

    #[test]
    fn lost_checkpoint_replays_the_same_read_only_cursor_without_double_counting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let target = FakeTarget::new(
            [
                Ok(partial_btree()),
                Ok(complete_btree()),
                Ok(complete_btree()),
            ],
            [],
        );
        let governor = governor(100);
        schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        let durable = std::fs::read(&path).unwrap();
        let mut lost = load_schedule_if_exists(&path).unwrap().unwrap();
        lost.tick_and_checkpoint(&path, operation_id, &target, &governor, 101, false)
            .unwrap();
        std::fs::write(&path, &durable).unwrap();
        let mut replay = load_schedule_if_exists(&path).unwrap().unwrap();
        replay
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 101, false)
            .unwrap();

        let calls = target.btree_cursors.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[1], calls[2]);
        let checkpoint = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(checkpoint.totals.structural_steps, 2);
        assert_eq!(checkpoint.totals.structural_entries, 6);
        assert_eq!(checkpoint.phase, PagedIntegrityPhase::VersionChains);
    }

    #[test]
    fn structural_fault_terminates_fail_closed_before_mvcc_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let fault = BTreeVerifyStepReport {
            entries_examined: 1,
            leaf_pages_examined: 1,
            descent_pages_examined: 1,
            page_bytes_examined: 16 * 1024,
            key_bytes_examined: 8,
            height: Some(1),
            next_cursor: BTreeVerifyCursor::default(),
            stop_reason: BTreeVerifyStopReason::StructuralFault,
            fault: Some(BTreeVerifyFault::LeafCycle { page_id: 2 }),
            elapsed_millis: 1,
            complete: false,
            valid: false,
        };
        let target = FakeTarget::new([Ok(fault)], [Ok(complete_chains())]);
        let outcome = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor(100), 100, false)
            .unwrap();
        assert!(matches!(
            outcome,
            PagedIntegrityScheduleAdvance::Complete { valid: false, .. }
        ));
        assert!(target.chain_cursors.lock().unwrap().is_empty());
        let completed = load_schedule_if_exists(&path).unwrap().unwrap();
        assert!(!completed.valid);
        assert_eq!(completed.totals.structural_faults, 1);
        assert_eq!(completed.totals.version_chain_steps, 0);
    }

    #[test]
    fn mvcc_faults_are_accumulated_through_terminal_invalid_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let mut faulty = complete_chains();
        faulty.version_chains.valid = false;
        faulty.version_chains.cycles = 1;
        let target = FakeTarget::new([Ok(complete_btree())], [Ok(faulty)]);
        let governor = governor(100);
        assert!(matches!(
            schedule
                .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
                .unwrap(),
            PagedIntegrityScheduleAdvance::PhaseAdvanced { .. }
        ));
        let mut phase = load_schedule_if_exists(&path).unwrap().unwrap();
        let outcome = phase
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 101, false)
            .unwrap();
        assert!(matches!(
            outcome,
            PagedIntegrityScheduleAdvance::Complete { valid: false, .. }
        ));
        let completed = load_schedule_if_exists(&path).unwrap().unwrap();
        assert!(!completed.valid);
        assert_eq!(completed.totals.cycles, 1);
        assert_eq!(completed.totals.version_chain_steps, 1);
    }

    #[test]
    fn saturation_defers_without_invoking_either_verifier() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let target = FakeTarget::new([Ok(complete_btree())], [Ok(complete_chains())]);
        let mut config = ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::AntiEntropy)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, 100).unwrap();
        let held = governor
            .try_admit(ResourceLane::AntiEntropy, schedule.limits.demand, 100)
            .unwrap();
        let outcome = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert_eq!(
            outcome,
            PagedIntegrityScheduleAdvance::ResourceDeferred { retry_at_ms: 101 }
        );
        assert!(target.btree_cursors.lock().unwrap().is_empty());
        assert!(target.chain_cursors.lock().unwrap().is_empty());
        assert_eq!(
            load_schedule_if_exists(&path)
                .unwrap()
                .unwrap()
                .totals
                .resource_deferrals,
            1
        );
        drop(held);
    }

    #[test]
    fn failures_back_off_pause_and_resume_under_the_same_operation_fence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let operation_id = Uuid::new_v4();
        let mut schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), operation_id, 100, limits()).unwrap();
        let target = FakeTarget::new(
            [
                Err(integrity_error("first read failure")),
                Err(integrity_error("second read failure")),
                Ok(complete_btree()),
            ],
            [],
        );
        let governor = governor(100);
        let first = schedule
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 100, false)
            .unwrap();
        assert!(matches!(
            first,
            PagedIntegrityScheduleAdvance::RetryScheduled {
                retry_at_ms: 101,
                consecutive_failures: 1,
                ..
            }
        ));
        let mut loaded = load_schedule_if_exists(&path).unwrap().unwrap();
        let second = loaded
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 101, false)
            .unwrap();
        assert!(matches!(
            second,
            PagedIntegrityScheduleAdvance::Paused { .. }
        ));
        let mut paused = load_schedule_if_exists(&path).unwrap().unwrap();
        assert_eq!(paused.totals.failures, 2);
        assert!(paused
            .resume_and_checkpoint(&path, Uuid::new_v4(), 200, 200, false)
            .unwrap_err()
            .to_string()
            .contains("cannot be resumed"));
        paused
            .resume_and_checkpoint(&path, operation_id, 200, 200, false)
            .unwrap();
        let outcome = paused
            .tick_and_checkpoint(&path, operation_id, &target, &governor, 200, false)
            .unwrap();
        assert!(matches!(
            outcome,
            PagedIntegrityScheduleAdvance::PhaseAdvanced {
                phase: PagedIntegrityPhase::VersionChains,
                ..
            }
        ));
    }

    #[test]
    fn tampered_oversized_and_unknown_schedule_state_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("integrity.json");
        let schedule =
            PagedIntegritySchedule::create(Uuid::new_v4(), Uuid::new_v4(), 100, limits()).unwrap();
        save_schedule(&path, &schedule, false).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["state_sequence"] = serde_json::json!(9);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(load_schedule_if_exists(&path)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));

        save_schedule(&path, &schedule, false).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["forged"] = serde_json::json!(true);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(load_schedule_if_exists(&path).is_err());

        std::fs::write(&path, vec![b'x'; (MAX_SCHEDULE_BYTES + 1) as usize]).unwrap();
        assert!(load_schedule_if_exists(&path)
            .unwrap_err()
            .to_string()
            .contains("outside its byte bound"));
    }
}
