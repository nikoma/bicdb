//! Durable, resource-governed scheduling for one bounded range digest run.
//!
//! A scheduler tick either performs no replica RPC or advances exactly one
//! digest step. Admission happens before the transport is called, and every
//! due time, backoff, pause, and run cursor is atomically checkpointed.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::distribution_anti_entropy::RangeDigestTransport;
use crate::distribution_anti_entropy_run::{
    RangeDigestRun, RangeDigestRunAdvance, RangeDigestRunLimits, RangeDigestRunReport,
};
use crate::error::{BicDbError, Result};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const RANGE_ANTI_ENTROPY_SCHEDULE_FORMAT_VERSION: u32 = 1;

fn schedule_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("range anti-entropy schedule: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeAntiEntropyScheduleLimits {
    pub run: RangeDigestRunLimits,
    /// Delay between successful bounded steps. This is deliberately nonzero so
    /// a large range cannot form a tight maintenance loop.
    pub step_interval_ms: u64,
    pub saturation_retry_ms: u64,
    pub failure_retry_base_ms: u64,
    pub failure_retry_max_ms: u64,
    pub max_consecutive_failures: u32,
    pub max_schedule_state_bytes: u64,
    pub demand: ResourceDemand,
}

impl Default for RangeAntiEntropyScheduleLimits {
    fn default() -> Self {
        Self {
            run: RangeDigestRunLimits::default(),
            step_interval_ms: 100,
            saturation_retry_ms: 1_000,
            failure_retry_base_ms: 1_000,
            failure_retry_max_ms: 5 * 60 * 1_000,
            max_consecutive_failures: 32,
            max_schedule_state_bytes: 160 * 1024 * 1024,
            demand: ResourceDemand {
                memory_bytes: 192 * 1024 * 1024,
                io_bytes: 32 * 1024 * 1024,
                cpu_slots: 1,
                io_charge_bytes: 8 * 1024 * 1024,
            },
        }
    }
}

impl RangeAntiEntropyScheduleLimits {
    pub fn validate(&self) -> Result<()> {
        self.run.validate()?;
        self.demand.validate()?;
        if !(1..=3_600_000).contains(&self.step_interval_ms)
            || !(1..=3_600_000).contains(&self.saturation_retry_ms)
            || !(1..=3_600_000).contains(&self.failure_retry_base_ms)
            || self.failure_retry_max_ms < self.failure_retry_base_ms
            || self.failure_retry_max_ms > 24 * 60 * 60 * 1_000
            || !(1..=1_000_000).contains(&self.max_consecutive_failures)
            || self.max_schedule_state_bytes <= self.run.max_run_state_bytes
            || self.max_schedule_state_bytes > 512 * 1024 * 1024
            || self.demand.memory_bytes < self.run.digest.max_bytes_per_batch as u64
            || self.demand.io_bytes < self.run.digest.max_bytes_per_batch as u64
            || self.demand.io_charge_bytes < self.run.digest.max_bytes_per_batch as u64
        {
            return Err(schedule_error(
                "schedule timing, state, failure, or resource bounds are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeAntiEntropySchedule {
    pub format_version: u32,
    pub run: RangeDigestRun,
    pub limits: RangeAntiEntropyScheduleLimits,
    pub next_attempt_at_ms: Option<u64>,
    pub last_observed_at_ms: u64,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub paused_reason: Option<String>,
    pub checksum_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RangeAntiEntropyScheduleAdvance {
    NotDue {
        next_attempt_at_ms: u64,
    },
    ResourceDeferred {
        retry_at_ms: u64,
    },
    Progress {
        next_attempt_at_ms: u64,
        advance: RangeDigestRunAdvance,
    },
    RetryScheduled {
        retry_at_ms: u64,
        consecutive_failures: u32,
        error: String,
    },
    Paused {
        reason: String,
    },
    Complete(RangeDigestRunReport),
}

impl RangeAntiEntropySchedule {
    pub fn create(
        run: RangeDigestRun,
        first_attempt_at_ms: u64,
        limits: RangeAntiEntropyScheduleLimits,
    ) -> Result<Self> {
        limits.validate()?;
        run.validate()?;
        if run.limits != limits.run
            || run.completed
            || first_attempt_at_ms < run.installed_at_ms
            || first_attempt_at_ms >= run.expires_at_ms
        {
            return Err(schedule_error(
                "initial run, limits, or first-attempt time is invalid",
            ));
        }
        let mut schedule = Self {
            format_version: RANGE_ANTI_ENTROPY_SCHEDULE_FORMAT_VERSION,
            run,
            limits,
            next_attempt_at_ms: Some(first_attempt_at_ms),
            last_observed_at_ms: 0,
            consecutive_failures: 0,
            last_error: None,
            paused_reason: None,
            checksum_sha256: String::new(),
        };
        schedule.refresh_checksum()?;
        schedule.validate()?;
        Ok(schedule)
    }

    /// Performs at most one resource-admitted remote digest step and always
    /// persists the resulting scheduler state before returning an outcome.
    pub fn tick_and_checkpoint<T: RangeDigestTransport>(
        &mut self,
        path: impl AsRef<Path>,
        transport: &T,
        governor: &ResourceGovernor,
        now_ms: u64,
        fsync: bool,
    ) -> Result<RangeAntiEntropyScheduleAdvance> {
        self.validate()?;
        if now_ms < self.last_observed_at_ms {
            return Err(schedule_error("scheduler clock regressed"));
        }
        self.last_observed_at_ms = now_ms;

        if self.run.completed {
            let report = self
                .run
                .report
                .clone()
                .ok_or_else(|| schedule_error("completed run has no report"))?;
            self.refresh_and_save(path, fsync)?;
            return Ok(RangeAntiEntropyScheduleAdvance::Complete(report));
        }
        if let Some(reason) = &self.paused_reason {
            let outcome = RangeAntiEntropyScheduleAdvance::Paused {
                reason: reason.clone(),
            };
            self.refresh_and_save(path, fsync)?;
            return Ok(outcome);
        }
        if now_ms >= self.run.expires_at_ms {
            return self.pause_and_save(path, "range digest fence expired", fsync);
        }
        let due = self
            .next_attempt_at_ms
            .ok_or_else(|| schedule_error("active schedule has no next attempt"))?;
        if now_ms < due {
            self.refresh_and_save(path, fsync)?;
            return Ok(RangeAntiEntropyScheduleAdvance::NotDue {
                next_attempt_at_ms: due,
            });
        }

        let permit = match governor.try_admit(ResourceLane::AntiEntropy, self.limits.demand, now_ms)
        {
            Ok(permit) => permit,
            Err(BicDbError::ResourceGovernance(_)) => {
                let retry_at_ms = self.bounded_due(now_ms, self.limits.saturation_retry_ms)?;
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                return Ok(RangeAntiEntropyScheduleAdvance::ResourceDeferred { retry_at_ms });
            }
            Err(error) => return Err(error),
        };

        let result = self.run.advance(transport, now_ms);
        drop(permit);
        match result {
            Ok(advance) => {
                self.consecutive_failures = 0;
                self.last_error = None;
                if let RangeDigestRunAdvance::Complete(report) = &advance {
                    self.next_attempt_at_ms = None;
                    self.refresh_and_save(path, fsync)?;
                    return Ok(RangeAntiEntropyScheduleAdvance::Complete(report.clone()));
                }
                let next_attempt_at_ms = self.bounded_due(now_ms, self.limits.step_interval_ms)?;
                self.next_attempt_at_ms = Some(next_attempt_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(RangeAntiEntropyScheduleAdvance::Progress {
                    next_attempt_at_ms,
                    advance,
                })
            }
            Err(error) => {
                let message = bounded_error(error.to_string());
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
                let retry_at_ms = self.bounded_due(now_ms, retry_delay)?;
                self.next_attempt_at_ms = Some(retry_at_ms);
                self.refresh_and_save(path, fsync)?;
                Ok(RangeAntiEntropyScheduleAdvance::RetryScheduled {
                    retry_at_ms,
                    consecutive_failures: self.consecutive_failures,
                    error: message,
                })
            }
        }
    }

    pub fn resume_and_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        next_attempt_at_ms: u64,
        fsync: bool,
    ) -> Result<()> {
        self.validate()?;
        if self.run.completed
            || self.paused_reason.is_none()
            || next_attempt_at_ms < self.last_observed_at_ms
            || next_attempt_at_ms >= self.run.expires_at_ms
        {
            return Err(schedule_error(
                "paused schedule cannot be resumed at that time",
            ));
        }
        self.paused_reason = None;
        self.last_error = None;
        self.consecutive_failures = 0;
        self.next_attempt_at_ms = Some(next_attempt_at_ms);
        self.refresh_and_save(path, fsync)
    }

    pub fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        self.run.validate()?;
        if self.format_version != RANGE_ANTI_ENTROPY_SCHEDULE_FORMAT_VERSION
            || self.run.limits != self.limits.run
            || self.consecutive_failures > self.limits.max_consecutive_failures
            || self
                .last_error
                .as_ref()
                .is_some_and(|error| error.len() > 4_096)
            || self
                .paused_reason
                .as_ref()
                .is_some_and(|reason| reason.is_empty() || reason.len() > 4_096)
        {
            return Err(schedule_error("schedule identity or bounds are invalid"));
        }
        let inactive = self.run.completed || self.paused_reason.is_some();
        if inactive != self.next_attempt_at_ms.is_none()
            || self.run.completed && self.paused_reason.is_some()
            || self
                .next_attempt_at_ms
                .is_some_and(|due| due < self.run.installed_at_ms || due >= self.run.expires_at_ms)
        {
            return Err(schedule_error(
                "schedule active, paused, or completed state disagrees",
            ));
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(schedule_error("schedule state checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_schedule_state_bytes {
            return Err(schedule_error("schedule state exceeds its byte bound"));
        }
        Ok(())
    }

    fn pause_and_save(
        &mut self,
        path: impl AsRef<Path>,
        reason: impl Into<String>,
        fsync: bool,
    ) -> Result<RangeAntiEntropyScheduleAdvance> {
        let reason = bounded_error(reason.into());
        self.paused_reason = Some(reason.clone());
        self.next_attempt_at_ms = None;
        self.refresh_and_save(path, fsync)?;
        Ok(RangeAntiEntropyScheduleAdvance::Paused { reason })
    }

    fn bounded_due(&self, now_ms: u64, delay_ms: u64) -> Result<u64> {
        let due = now_ms
            .checked_add(delay_ms)
            .ok_or_else(|| schedule_error("scheduler due time overflow"))?;
        Ok(due.min(self.run.expires_at_ms.saturating_sub(1)))
    }

    fn refresh_and_save(&mut self, path: impl AsRef<Path>, fsync: bool) -> Result<()> {
        self.refresh_checksum()?;
        save_range_anti_entropy_schedule(path, self, fsync)
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum_sha256 = self.calculate_checksum()?;
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct ChecksumInput<'a> {
            format_version: u32,
            run: &'a RangeDigestRun,
            limits: &'a RangeAntiEntropyScheduleLimits,
            next_attempt_at_ms: Option<u64>,
            last_observed_at_ms: u64,
            consecutive_failures: u32,
            last_error: &'a Option<String>,
            paused_reason: &'a Option<String>,
        }
        let input = ChecksumInput {
            format_version: self.format_version,
            run: &self.run,
            limits: &self.limits,
            next_attempt_at_ms: self.next_attempt_at_ms,
            last_observed_at_ms: self.last_observed_at_ms,
            consecutive_failures: self.consecutive_failures,
            last_error: &self.last_error,
            paused_reason: &self.paused_reason,
        };
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&input)?)))
    }
}

pub fn save_range_anti_entropy_schedule(
    path: impl AsRef<Path>,
    schedule: &RangeAntiEntropySchedule,
    fsync: bool,
) -> Result<()> {
    schedule.validate()?;
    let bytes = serde_json::to_vec(schedule)?;
    if bytes.len() as u64 > schedule.limits.max_schedule_state_bytes {
        return Err(schedule_error("schedule exceeds its write bound"));
    }
    crate::storage::write_atomic(path.as_ref(), &bytes, fsync)
}

pub fn load_range_anti_entropy_schedule(
    path: impl AsRef<Path>,
    limits: &RangeAntiEntropyScheduleLimits,
) -> Result<RangeAntiEntropySchedule> {
    limits.validate()?;
    let path = path.as_ref();
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > limits.max_schedule_state_bytes
    {
        return Err(schedule_error(
            "schedule file is unsafe or outside its bound",
        ));
    }
    let length = metadata.len();
    let mut bytes = Vec::with_capacity(length as usize);
    File::open(path)?
        .take(limits.max_schedule_state_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes.len() as u64 > limits.max_schedule_state_bytes {
        return Err(schedule_error("schedule changed or grew while reading"));
    }
    let schedule: RangeAntiEntropySchedule = serde_json::from_slice(&bytes)?;
    if schedule.limits != *limits {
        return Err(schedule_error(
            "schedule limits differ from requested limits",
        ));
    }
    schedule.validate()?;
    Ok(schedule)
}

fn bounded_error(mut message: String) -> String {
    if message.len() > 4_096 {
        let mut end = 4_096;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(schedule_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::distribution::{
        ClusterId, ClusterNodeId, PlacementPolicy, RangeDescriptor, RangeId, RangeReplica,
        RangeReplicaRole, ReplicaId,
    };
    use crate::distribution_anti_entropy::{RangeDigestLimits, RangeDigestState};
    use crate::distribution_range_consensus::{RangeBackupFenceQuorum, RangeWriteProgress};
    use crate::{Record, ResourceGovernorConfig};

    #[derive(Debug)]
    struct TestTransport {
        cluster_id: ClusterId,
        range: RangeDescriptor,
        records: BTreeMap<ClusterNodeId, Vec<Record>>,
        states: Mutex<BTreeMap<ClusterNodeId, RangeDigestState>>,
        calls: AtomicUsize,
        fail_calls: AtomicUsize,
    }

    impl RangeDigestTransport for TestTransport {
        fn advance_range_digest(
            &self,
            destination: &ClusterNodeId,
            range_id: RangeId,
            range_epoch: u64,
            session_id: Uuid,
            expected_checksum_sha256: Option<&str>,
            limits: &RangeDigestLimits,
            _now_ms: u64,
        ) -> Result<RangeDigestState> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .fail_calls
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                return Err(schedule_error("simulated transport failure"));
            }
            let mut states = self.states.lock();
            let state = states.entry(destination.clone()).or_insert_with(|| {
                RangeDigestState::create(
                    session_id,
                    self.cluster_id.clone(),
                    destination.clone(),
                    range_id,
                    range_epoch,
                    self.range.start_token,
                    self.range.end_token,
                    0,
                    limits,
                )
                .unwrap()
            });
            let Some(expected) = expected_checksum_sha256 else {
                return Ok(state.clone());
            };
            if state.checksum_sha256 != expected {
                return Err(schedule_error("stale test checksum"));
            }
            if state.scanned_records == 0 {
                let records = &self.records[destination];
                let bytes = records
                    .iter()
                    .map(|record| serde_json::to_vec(record).unwrap().len())
                    .sum();
                state.apply_batch(
                    None,
                    "done",
                    "items",
                    range_id,
                    range_epoch,
                    records,
                    bytes,
                    limits,
                )?;
            } else if !state.completed {
                let cursor = state.resume_after_key.clone();
                state.finish(cursor.as_deref(), limits)?;
            }
            Ok(state.clone())
        }
    }

    fn fixture() -> (RangeAntiEntropySchedule, TestTransport) {
        let cluster_id = ClusterId::new("scheduled-cluster").unwrap();
        let nodes = (0..3)
            .map(|index| ClusterNodeId::new(format!("node-{index}")).unwrap())
            .collect::<Vec<_>>();
        let range = RangeDescriptor {
            id: RangeId::new(1).unwrap(),
            start_token: 0,
            end_token: None,
            epoch: 1,
            replicas: nodes
                .iter()
                .enumerate()
                .map(|(index, node_id)| RangeReplica {
                    id: ReplicaId::new(index as u64 + 1).unwrap(),
                    node_id: node_id.clone(),
                    role: RangeReplicaRole::Voter,
                })
                .collect(),
            leader: nodes[0].clone(),
            approximate_bytes: 0,
            approximate_qps: 0,
            placement: PlacementPolicy::default(),
        };
        let fence = RangeBackupFenceQuorum {
            plan_id: Uuid::new_v4(),
            range_id: range.id,
            range_epoch: range.epoch,
            resolved_through: 0,
            required_quorum: 2,
            installed_at_ms: 10,
            expires_at_ms: 10_000,
            observations: nodes
                .iter()
                .take(2)
                .map(|node_id| RangeWriteProgress {
                    node_id: node_id.clone(),
                    range_id: range.id,
                    current_epoch: range.epoch,
                    last_index: 0,
                    resolved_through: 0,
                    compacted_through: 0,
                })
                .collect(),
        };
        let mut limits = RangeAntiEntropyScheduleLimits::default();
        limits.run.digest.bucket_count = 16;
        limits.run.digest.max_records_per_batch = 16;
        let run = RangeDigestRun::create(cluster_id.clone(), &range, &fence, limits.run).unwrap();
        let schedule = RangeAntiEntropySchedule::create(run, 100, limits).unwrap();
        let records = nodes
            .iter()
            .take(2)
            .map(|node| {
                (
                    node.clone(),
                    vec![Record::new("item-1").with_metadata(json!({"value": 1}))],
                )
            })
            .collect();
        let transport = TestTransport {
            cluster_id,
            range,
            records,
            states: Mutex::new(BTreeMap::new()),
            calls: AtomicUsize::new(0),
            fail_calls: AtomicUsize::new(0),
        };
        (schedule, transport)
    }

    #[test]
    fn saturation_defers_without_transport_and_restart_retains_due_time() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("schedule.json");
        let (mut schedule, transport) = fixture();
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
        assert_eq!(
            schedule
                .tick_and_checkpoint(&path, &transport, &governor, 100, false)
                .unwrap(),
            RangeAntiEntropyScheduleAdvance::ResourceDeferred { retry_at_ms: 1_100 }
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        let loaded = load_range_anti_entropy_schedule(&path, &schedule.limits).unwrap();
        assert_eq!(loaded.next_attempt_at_ms, Some(1_100));
        drop(held);
    }

    #[test]
    fn bounded_ticks_resume_and_complete_with_no_inflight_leak() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("schedule.json");
        let (mut schedule, transport) = fixture();
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut now = 100;
        for _ in 0..20 {
            let outcome = schedule
                .tick_and_checkpoint(&path, &transport, &governor, now, false)
                .unwrap();
            assert_eq!(governor.snapshot().background.active, 0);
            if matches!(outcome, RangeAntiEntropyScheduleAdvance::Complete(_)) {
                let loaded = load_range_anti_entropy_schedule(&path, &schedule.limits).unwrap();
                assert!(loaded.run.completed);
                assert!(loaded.next_attempt_at_ms.is_none());
                return;
            }
            schedule = load_range_anti_entropy_schedule(&path, &schedule.limits).unwrap();
            now = schedule.next_attempt_at_ms.unwrap();
        }
        panic!("scheduled digest did not finish");
    }

    #[test]
    fn transport_failure_is_checkpointed_with_exponential_backoff() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("schedule.json");
        let (mut schedule, transport) = fixture();
        transport.fail_calls.store(2, Ordering::SeqCst);
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let first = schedule
            .tick_and_checkpoint(&path, &transport, &governor, 100, false)
            .unwrap();
        assert!(matches!(
            first,
            RangeAntiEntropyScheduleAdvance::RetryScheduled {
                retry_at_ms: 1_100,
                consecutive_failures: 1,
                ..
            }
        ));
        let mut loaded = load_range_anti_entropy_schedule(&path, &schedule.limits).unwrap();
        let second = loaded
            .tick_and_checkpoint(&path, &transport, &governor, 1_100, false)
            .unwrap();
        assert!(matches!(
            second,
            RangeAntiEntropyScheduleAdvance::RetryScheduled {
                retry_at_ms: 3_100,
                consecutive_failures: 2,
                ..
            }
        ));
    }
}
