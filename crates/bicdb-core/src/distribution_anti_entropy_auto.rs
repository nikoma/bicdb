//! Automatic, durable, low-priority range integrity scheduling.
//!
//! The controller checkpoints a range-specific fence plan before acquiring any
//! write fence, runs at most one governed digest step per tick, and releases a
//! healthy or inconclusive fence before moving to the next locally led range.
//! Certified divergence remains fenced and requires the existing repair path.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::distribution::{
    ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology, RangeDescriptor, RangeId,
};
use crate::distribution_anti_entropy::RangeDigestTransport;
use crate::distribution_anti_entropy_run::{
    RangeDigestRun, RangeDigestRunOutcome, RangeDigestRunReport,
};
use crate::distribution_anti_entropy_scheduler::{
    load_range_anti_entropy_schedule, save_range_anti_entropy_schedule, RangeAntiEntropySchedule,
    RangeAntiEntropyScheduleAdvance, RangeAntiEntropyScheduleLimits,
};
use crate::distribution_range_consensus::RangeBackupFenceQuorum;
use crate::error::{BicDbError, Result};
use crate::{ResourceDemand, ResourceGovernor, ResourceLane};

pub const AUTOMATIC_RANGE_ANTI_ENTROPY_FORMAT_VERSION: u32 = 1;
pub const RANGE_ANTI_ENTROPY_FENCE_PLAN_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_DIR: &str = "cluster-anti-entropy";
pub const DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE: &str = "controller.json";
pub const DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_SCHEDULE: &str = "active-schedule.json";
const MAX_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE_BYTES: u64 = 512 * 1024 * 1024;

fn automatic_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(format!("automatic range anti-entropy: {}", message.into()))
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticRangeAntiEntropyLimits {
    pub schedule: RangeAntiEntropyScheduleLimits,
    pub between_ranges_ms: u64,
    pub sweep_interval_ms: u64,
    pub foreground_retry_ms: u64,
    pub fence_ttl_ms: u64,
    pub max_range_qps: u64,
    pub max_state_bytes: u64,
    pub fence_demand: ResourceDemand,
}

impl Default for AutomaticRangeAntiEntropyLimits {
    fn default() -> Self {
        let schedule = RangeAntiEntropyScheduleLimits::default();
        Self {
            schedule,
            between_ranges_ms: 1_000,
            sweep_interval_ms: 6 * 60 * 60 * 1_000,
            foreground_retry_ms: 5_000,
            fence_ttl_ms: 30 * 60 * 1_000,
            // Automatic write-fenced scans are intentionally restricted to
            // ranges the topology metrics currently classify as idle. An
            // operator may opt into a higher ceiling explicitly.
            max_range_qps: 0,
            max_state_bytes: 160 * 1024 * 1024,
            fence_demand: schedule.demand,
        }
    }
}

impl AutomaticRangeAntiEntropyLimits {
    pub fn validate(&self) -> Result<()> {
        self.schedule.validate()?;
        self.fence_demand.validate()?;
        if !(1..=3_600_000).contains(&self.between_ranges_ms)
            || !(1_000..=7 * 24 * 60 * 60 * 1_000).contains(&self.sweep_interval_ms)
            || !(1..=3_600_000).contains(&self.foreground_retry_ms)
            || !(10_000..=24 * 60 * 60 * 1_000).contains(&self.fence_ttl_ms)
            || self.max_state_bytes < 64 * 1024
            || self.max_state_bytes > MAX_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE_BYTES
            || self.max_state_bytes <= self.schedule.run.max_run_state_bytes
        {
            return Err(automatic_error(
                "automatic timing, state, or fence bounds are inconsistent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RangeAntiEntropyFencePlan {
    pub format_version: u32,
    pub plan_id: Uuid,
    pub cluster_id: ClusterId,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub range_sha256: String,
    pub leader_node_id: ClusterNodeId,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub checksum_sha256: String,
}

impl RangeAntiEntropyFencePlan {
    pub fn create(
        topology: &ClusterTopology,
        leader_node_id: &ClusterNodeId,
        range_id: RangeId,
        created_at_ms: u64,
        ttl_ms: u64,
    ) -> Result<Self> {
        topology.validate()?;
        if ttl_ms == 0 {
            return Err(automatic_error("range fence plan TTL must be nonzero"));
        }
        let range = topology
            .range_by_id(range_id)
            .ok_or_else(|| automatic_error(format!("range {range_id} is absent")))?;
        let expires_at_ms = created_at_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| automatic_error("range fence plan expiration overflow"))?;
        let mut plan = Self {
            format_version: RANGE_ANTI_ENTROPY_FENCE_PLAN_FORMAT_VERSION,
            plan_id: Uuid::now_v7(),
            cluster_id: topology.cluster_id.clone(),
            range_id,
            range_epoch: range.epoch,
            range_sha256: range_sha256(range)?,
            leader_node_id: leader_node_id.clone(),
            created_at_ms,
            expires_at_ms,
            checksum_sha256: String::new(),
        };
        plan.checksum_sha256 = plan.calculate_checksum()?;
        plan.validate(topology, leader_node_id, created_at_ms)?;
        Ok(plan)
    }

    pub fn validate(
        &self,
        topology: &ClusterTopology,
        leader_node_id: &ClusterNodeId,
        now_ms: u64,
    ) -> Result<()> {
        topology.validate()?;
        let range = topology
            .range_by_id(self.range_id)
            .ok_or_else(|| automatic_error(format!("range {} is absent", self.range_id)))?;
        let leader = topology
            .nodes
            .get(leader_node_id)
            .ok_or_else(|| automatic_error("range fence leader is absent"))?;
        if self.format_version != RANGE_ANTI_ENTROPY_FENCE_PLAN_FORMAT_VERSION
            || self.plan_id.is_nil()
            || self.cluster_id != topology.cluster_id
            || &self.leader_node_id != leader_node_id
            || range.leader != *leader_node_id
            || range.epoch != self.range_epoch
            || range_sha256(range)? != self.range_sha256
            || leader.lifecycle != ClusterNodeLifecycle::Active
            || self.created_at_ms >= self.expires_at_ms
            || self.expires_at_ms.saturating_sub(self.created_at_ms) > 24 * 60 * 60 * 1_000
            || now_ms < self.created_at_ms
            || now_ms >= self.expires_at_ms
            || topology
                .relocations
                .values()
                .any(|relocation| relocation.range_id == self.range_id && relocation.is_active())
        {
            return Err(automatic_error(
                "range fence plan is stale, expired, relocated, or belongs to another authority",
            ));
        }
        validate_sha256(&self.range_sha256)?;
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(automatic_error("range fence plan checksum mismatch"));
        }
        Ok(())
    }

    fn calculate_checksum(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Checksum<'a> {
            format_version: u32,
            plan_id: Uuid,
            cluster_id: &'a ClusterId,
            range_id: RangeId,
            range_epoch: u64,
            range_sha256: &'a str,
            leader_node_id: &'a ClusterNodeId,
            created_at_ms: u64,
            expires_at_ms: u64,
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(
            &Checksum {
                format_version: self.format_version,
                plan_id: self.plan_id,
                cluster_id: &self.cluster_id,
                range_id: self.range_id,
                range_epoch: self.range_epoch,
                range_sha256: &self.range_sha256,
                leader_node_id: &self.leader_node_id,
                created_at_ms: self.created_at_ms,
                expires_at_ms: self.expires_at_ms,
            },
        )?)))
    }
}

pub trait RangeAntiEntropyFenceAuthority {
    fn install_range_anti_entropy_fence(
        &self,
        plan: &RangeAntiEntropyFencePlan,
        now_ms: u64,
    ) -> Result<RangeBackupFenceQuorum>;

    fn release_range_anti_entropy_fence(&self, fence: &RangeBackupFenceQuorum) -> Result<usize>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticRangeAntiEntropyState {
    pub format_version: u32,
    pub cluster_id: ClusterId,
    pub node_id: ClusterNodeId,
    pub limits: AutomaticRangeAntiEntropyLimits,
    pub cursor_after_range: Option<RangeId>,
    pub next_attempt_at_ms: u64,
    pub last_observed_at_ms: u64,
    pub active_plan: Option<RangeAntiEntropyFencePlan>,
    pub active_fence: Option<RangeBackupFenceQuorum>,
    pub repair_required: Option<RangeDigestRunReport>,
    pub checksum_sha256: String,
}

impl AutomaticRangeAntiEntropyState {
    fn validate(&self) -> Result<()> {
        self.limits.validate()?;
        if self.format_version != AUTOMATIC_RANGE_ANTI_ENTROPY_FORMAT_VERSION
            || (self.active_fence.is_some() && self.active_plan.is_none())
            || (self.repair_required.is_some() && self.active_fence.is_none())
        {
            return Err(automatic_error(
                "automatic controller state is inconsistent",
            ));
        }
        if let (Some(plan), Some(fence)) = (&self.active_plan, &self.active_fence) {
            if plan.plan_id != fence.plan_id
                || plan.range_id != fence.range_id
                || plan.range_epoch != fence.range_epoch
                || plan.created_at_ms != fence.installed_at_ms
                || plan.expires_at_ms != fence.expires_at_ms
            {
                return Err(automatic_error(
                    "automatic controller fence differs from its durable plan",
                ));
            }
        }
        if let (Some(report), Some(fence)) = (&self.repair_required, &self.active_fence) {
            report.validate()?;
            if report.session_id != fence.plan_id
                || report.range_id != fence.range_id
                || report.range_epoch != fence.range_epoch
                || report.outcome == RangeDigestRunOutcome::Healthy
            {
                return Err(automatic_error(
                    "automatic repair report differs from its active fence",
                ));
            }
        }
        validate_sha256(&self.checksum_sha256)?;
        if self.calculate_checksum()? != self.checksum_sha256 {
            return Err(automatic_error("automatic controller checksum mismatch"));
        }
        if serde_json::to_vec(self)?.len() as u64 > self.limits.max_state_bytes {
            return Err(automatic_error(
                "automatic controller exceeds its state bound",
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
        struct Checksum<'a> {
            format_version: u32,
            cluster_id: &'a ClusterId,
            node_id: &'a ClusterNodeId,
            limits: &'a AutomaticRangeAntiEntropyLimits,
            cursor_after_range: Option<RangeId>,
            next_attempt_at_ms: u64,
            last_observed_at_ms: u64,
            active_plan: &'a Option<RangeAntiEntropyFencePlan>,
            active_fence: &'a Option<RangeBackupFenceQuorum>,
            repair_required: &'a Option<RangeDigestRunReport>,
        }
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(
            &Checksum {
                format_version: self.format_version,
                cluster_id: &self.cluster_id,
                node_id: &self.node_id,
                limits: &self.limits,
                cursor_after_range: self.cursor_after_range,
                next_attempt_at_ms: self.next_attempt_at_ms,
                last_observed_at_ms: self.last_observed_at_ms,
                active_plan: &self.active_plan,
                active_fence: &self.active_fence,
                repair_required: &self.repair_required,
            },
        )?)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AutomaticRangeAntiEntropyAdvance {
    NotDue { next_attempt_at_ms: u64 },
    ForegroundDeferred { retry_at_ms: u64 },
    ResourceDeferred { retry_at_ms: u64 },
    SweepDeferred { retry_at_ms: u64 },
    FencePlanned { range_id: RangeId },
    FenceInstalled { range_id: RangeId },
    Progress { range_id: RangeId },
    Healthy { range_id: RangeId },
    InconclusiveReleased { range_id: RangeId, reason: String },
    RepairRequired(RangeDigestRunReport),
}

#[derive(Debug)]
pub struct AutomaticRangeAntiEntropyController {
    root: PathBuf,
    state_path: PathBuf,
    schedule_path: PathBuf,
    fsync: bool,
    state: AutomaticRangeAntiEntropyState,
}

impl AutomaticRangeAntiEntropyController {
    pub fn open(
        root: impl AsRef<Path>,
        cluster_id: ClusterId,
        node_id: ClusterNodeId,
        first_attempt_at_ms: u64,
        limits: AutomaticRangeAntiEntropyLimits,
        fsync: bool,
    ) -> Result<Self> {
        limits.validate()?;
        let root = root.as_ref().join(DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_DIR);
        ensure_safe_directory(&root)?;
        let state_path = root.join(DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE);
        let schedule_path = root.join(DEFAULT_AUTOMATIC_RANGE_ANTI_ENTROPY_SCHEDULE);
        let mut state = if state_path.exists() {
            load_state(&state_path)?
        } else {
            let mut state = AutomaticRangeAntiEntropyState {
                format_version: AUTOMATIC_RANGE_ANTI_ENTROPY_FORMAT_VERSION,
                cluster_id: cluster_id.clone(),
                node_id: node_id.clone(),
                limits,
                cursor_after_range: None,
                next_attempt_at_ms: first_attempt_at_ms,
                last_observed_at_ms: 0,
                active_plan: None,
                active_fence: None,
                repair_required: None,
                checksum_sha256: String::new(),
            };
            state.refresh_checksum()?;
            save_state(&state_path, &state, fsync)?;
            state
        };
        if state.cluster_id != cluster_id || state.node_id != node_id {
            return Err(automatic_error(
                "automatic controller identity changed across restart",
            ));
        }
        if state.limits != limits {
            if state.active_plan.is_some()
                || state.active_fence.is_some()
                || state.repair_required.is_some()
                || schedule_path.exists()
            {
                return Err(automatic_error(
                    "automatic controller limits changed while durable work is active",
                ));
            }
            state.limits = limits;
            state.refresh_checksum()?;
            save_state(&state_path, &state, fsync)?;
        }
        if state.active_fence.is_some() && !schedule_path.exists() {
            return Err(automatic_error(
                "active automatic fence has no durable digest schedule",
            ));
        }
        Ok(Self {
            root,
            state_path,
            schedule_path,
            fsync,
            state,
        })
    }

    pub fn state(&self) -> &AutomaticRangeAntiEntropyState {
        &self.state
    }

    pub fn tick<A: RangeAntiEntropyFenceAuthority, T: RangeDigestTransport>(
        &mut self,
        topology: &ClusterTopology,
        authority: &A,
        transport: &T,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<AutomaticRangeAntiEntropyAdvance> {
        self.tick_with_foreground_pressure(
            topology,
            authority,
            transport,
            governor,
            foreground_active(governor),
            now_ms,
        )
    }

    /// Production entrypoint. The host supplies its real query/write admission
    /// pressure in addition to the shared resource-governor snapshot.
    pub fn tick_with_foreground_pressure<
        A: RangeAntiEntropyFenceAuthority,
        T: RangeDigestTransport,
    >(
        &mut self,
        topology: &ClusterTopology,
        authority: &A,
        transport: &T,
        governor: &ResourceGovernor,
        foreground_pressure: bool,
        now_ms: u64,
    ) -> Result<AutomaticRangeAntiEntropyAdvance> {
        let previous_state = self.state.clone();
        let outcome = self.tick_with_foreground_pressure_inner(
            topology,
            authority,
            transport,
            governor,
            foreground_pressure,
            now_ms,
        );
        if outcome.is_err() {
            self.state = previous_state;
        }
        outcome
    }

    fn tick_with_foreground_pressure_inner<
        A: RangeAntiEntropyFenceAuthority,
        T: RangeDigestTransport,
    >(
        &mut self,
        topology: &ClusterTopology,
        authority: &A,
        transport: &T,
        governor: &ResourceGovernor,
        foreground_pressure: bool,
        now_ms: u64,
    ) -> Result<AutomaticRangeAntiEntropyAdvance> {
        self.state.validate()?;
        topology.validate()?;
        if topology.cluster_id != self.state.cluster_id {
            return Err(automatic_error("controller and topology cluster differ"));
        }
        if now_ms < self.state.last_observed_at_ms {
            return Err(automatic_error("automatic controller clock regressed"));
        }
        self.state.last_observed_at_ms = now_ms;
        if let Some(report) = self.state.repair_required.clone() {
            self.persist()?;
            return Ok(AutomaticRangeAntiEntropyAdvance::RepairRequired(report));
        }
        if now_ms < self.state.next_attempt_at_ms {
            let next_attempt_at_ms = self.state.next_attempt_at_ms;
            self.persist()?;
            return Ok(AutomaticRangeAntiEntropyAdvance::NotDue { next_attempt_at_ms });
        }

        if self.state.active_fence.is_some() && (foreground_pressure || foreground_active(governor))
        {
            return self.defer_foreground(now_ms);
        }
        if self.state.active_fence.is_some() {
            return self.advance_active(authority, transport, governor, now_ms);
        }
        if let Some(plan) = self.state.active_plan.clone() {
            plan.validate(topology, &self.state.node_id, now_ms)?;
            if foreground_pressure || foreground_active(governor) {
                return self.defer_foreground(now_ms);
            }
            let permit = match governor.try_admit(
                ResourceLane::AntiEntropy,
                self.state.limits.fence_demand,
                now_ms,
            ) {
                Ok(permit) => permit,
                Err(BicDbError::ResourceGovernance(_)) => {
                    let retry_at_ms = now_ms
                        .checked_add(self.state.limits.foreground_retry_ms)
                        .ok_or_else(|| automatic_error("resource retry overflow"))?;
                    self.state.next_attempt_at_ms = retry_at_ms;
                    self.persist()?;
                    return Ok(AutomaticRangeAntiEntropyAdvance::ResourceDeferred { retry_at_ms });
                }
                Err(error) => return Err(error),
            };
            let fence = authority.install_range_anti_entropy_fence(&plan, now_ms);
            drop(permit);
            let fence = fence?;
            let schedule = if self.schedule_path.exists() {
                load_range_anti_entropy_schedule(&self.schedule_path, &self.state.limits.schedule)?
            } else {
                let range = topology
                    .range_by_id(plan.range_id)
                    .ok_or_else(|| automatic_error("planned range disappeared"))?;
                let run = RangeDigestRun::create(
                    topology.cluster_id.clone(),
                    range,
                    &fence,
                    self.state.limits.schedule.run,
                )?;
                let schedule =
                    RangeAntiEntropySchedule::create(run, now_ms, self.state.limits.schedule)?;
                save_range_anti_entropy_schedule(&self.schedule_path, &schedule, self.fsync)?;
                schedule
            };
            if schedule.run.session_id != plan.plan_id
                || schedule.run.range_id != plan.range_id
                || schedule.run.range_epoch != plan.range_epoch
            {
                return Err(automatic_error(
                    "durable digest schedule differs from its fence plan",
                ));
            }
            self.state.active_fence = Some(fence);
            self.state.next_attempt_at_ms = now_ms;
            self.persist()?;
            return Ok(AutomaticRangeAntiEntropyAdvance::FenceInstalled {
                range_id: plan.range_id,
            });
        }

        self.remove_orphan_schedule()?;
        let Some((range, wrapped)) = self.select_range(topology) else {
            let retry_at_ms = now_ms
                .checked_add(self.state.limits.sweep_interval_ms)
                .ok_or_else(|| automatic_error("empty sweep retry overflow"))?;
            self.state.cursor_after_range = None;
            self.state.next_attempt_at_ms = retry_at_ms;
            self.persist()?;
            return Ok(AutomaticRangeAntiEntropyAdvance::SweepDeferred { retry_at_ms });
        };
        if wrapped && self.state.cursor_after_range.is_some() {
            let retry_at_ms = now_ms
                .checked_add(self.state.limits.sweep_interval_ms)
                .ok_or_else(|| automatic_error("sweep interval overflow"))?;
            self.state.cursor_after_range = None;
            self.state.next_attempt_at_ms = retry_at_ms;
            self.persist()?;
            return Ok(AutomaticRangeAntiEntropyAdvance::SweepDeferred { retry_at_ms });
        }
        let plan = RangeAntiEntropyFencePlan::create(
            topology,
            &self.state.node_id,
            range.id,
            now_ms,
            self.state.limits.fence_ttl_ms,
        )?;
        self.state.active_plan = Some(plan);
        self.state.next_attempt_at_ms = now_ms;
        self.persist()?;
        Ok(AutomaticRangeAntiEntropyAdvance::FencePlanned { range_id: range.id })
    }

    fn advance_active<A: RangeAntiEntropyFenceAuthority, T: RangeDigestTransport>(
        &mut self,
        authority: &A,
        transport: &T,
        governor: &ResourceGovernor,
        now_ms: u64,
    ) -> Result<AutomaticRangeAntiEntropyAdvance> {
        let fence = self
            .state
            .active_fence
            .clone()
            .ok_or_else(|| automatic_error("active schedule has no fence"))?;
        let mut schedule =
            load_range_anti_entropy_schedule(&self.schedule_path, &self.state.limits.schedule)?;
        let outcome = schedule.tick_and_checkpoint(
            &self.schedule_path,
            transport,
            governor,
            now_ms,
            self.fsync,
        )?;
        match outcome {
            RangeAntiEntropyScheduleAdvance::Complete(report) => {
                if report.outcome == RangeDigestRunOutcome::Healthy {
                    authority.release_range_anti_entropy_fence(&fence)?;
                    self.finish_range(fence.range_id, now_ms)?;
                    Ok(AutomaticRangeAntiEntropyAdvance::Healthy {
                        range_id: fence.range_id,
                    })
                } else {
                    self.state.repair_required = Some(report.clone());
                    self.persist()?;
                    Ok(AutomaticRangeAntiEntropyAdvance::RepairRequired(report))
                }
            }
            RangeAntiEntropyScheduleAdvance::Paused { reason } => {
                authority.release_range_anti_entropy_fence(&fence)?;
                self.finish_range(fence.range_id, now_ms)?;
                Ok(AutomaticRangeAntiEntropyAdvance::InconclusiveReleased {
                    range_id: fence.range_id,
                    reason,
                })
            }
            RangeAntiEntropyScheduleAdvance::NotDue { next_attempt_at_ms }
            | RangeAntiEntropyScheduleAdvance::ResourceDeferred {
                retry_at_ms: next_attempt_at_ms,
            }
            | RangeAntiEntropyScheduleAdvance::RetryScheduled {
                retry_at_ms: next_attempt_at_ms,
                ..
            }
            | RangeAntiEntropyScheduleAdvance::Progress {
                next_attempt_at_ms, ..
            } => {
                self.state.next_attempt_at_ms = next_attempt_at_ms;
                self.persist()?;
                Ok(AutomaticRangeAntiEntropyAdvance::Progress {
                    range_id: fence.range_id,
                })
            }
        }
    }

    fn select_range<'a>(
        &self,
        topology: &'a ClusterTopology,
    ) -> Option<(&'a RangeDescriptor, bool)> {
        let eligible = |range: &&RangeDescriptor| {
            range.leader == self.state.node_id
                && range.approximate_qps <= self.state.limits.max_range_qps
                && !topology
                    .relocations
                    .values()
                    .any(|relocation| relocation.range_id == range.id && relocation.is_active())
        };
        let mut ranges = topology
            .ranges
            .values()
            .filter(eligible)
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.id);
        if let Some(after) = self.state.cursor_after_range {
            if let Some(range) = ranges.iter().copied().find(|range| range.id > after) {
                return Some((range, false));
            }
            ranges.first().copied().map(|range| (range, true))
        } else {
            ranges.first().copied().map(|range| (range, false))
        }
    }

    fn defer_foreground(&mut self, now_ms: u64) -> Result<AutomaticRangeAntiEntropyAdvance> {
        let retry_at_ms = now_ms
            .checked_add(self.state.limits.foreground_retry_ms)
            .ok_or_else(|| automatic_error("foreground retry overflow"))?;
        self.state.next_attempt_at_ms = retry_at_ms;
        self.persist()?;
        Ok(AutomaticRangeAntiEntropyAdvance::ForegroundDeferred { retry_at_ms })
    }

    fn finish_range(&mut self, range_id: RangeId, now_ms: u64) -> Result<()> {
        self.state.cursor_after_range = Some(range_id);
        self.state.next_attempt_at_ms = now_ms
            .checked_add(self.state.limits.between_ranges_ms)
            .ok_or_else(|| automatic_error("between-range delay overflow"))?;
        self.state.active_plan = None;
        self.state.active_fence = None;
        self.state.repair_required = None;
        self.persist()?;
        self.remove_orphan_schedule()
    }

    fn persist(&mut self) -> Result<()> {
        self.state.refresh_checksum()?;
        save_state(&self.state_path, &self.state, self.fsync)
    }

    fn remove_orphan_schedule(&self) -> Result<()> {
        if !self.schedule_path.exists() {
            return Ok(());
        }
        let metadata = std::fs::symlink_metadata(&self.schedule_path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(automatic_error("digest schedule path is unsafe"));
        }
        std::fs::remove_file(&self.schedule_path)?;
        sync_directory(&self.root, self.fsync)
    }
}

fn foreground_active(governor: &ResourceGovernor) -> bool {
    governor.snapshot().lanes.iter().any(|lane| {
        matches!(
            lane.lane,
            ResourceLane::Consensus | ResourceLane::ForegroundWrite | ResourceLane::ForegroundRead
        ) && lane.usage.active > 0
    })
}

fn range_sha256(range: &RangeDescriptor) -> Result<String> {
    #[derive(Serialize)]
    struct RangeAuthority<'a> {
        id: RangeId,
        start_token: u64,
        end_token: Option<u64>,
        epoch: u64,
        replicas: &'a [crate::distribution::RangeReplica],
        leader: &'a ClusterNodeId,
        placement: &'a crate::distribution::PlacementPolicy,
    }
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(
        &RangeAuthority {
            id: range.id,
            start_token: range.start_token,
            end_token: range.end_token,
            epoch: range.epoch,
            replicas: &range.replicas,
            leader: &range.leader,
            placement: &range.placement,
        },
    )?)))
}

fn save_state(path: &Path, state: &AutomaticRangeAntiEntropyState, fsync: bool) -> Result<()> {
    state.validate()?;
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() as u64 > state.limits.max_state_bytes {
        return Err(automatic_error(
            "automatic controller write exceeds its bound",
        ));
    }
    crate::storage::write_atomic(path, &bytes, fsync)
}

fn load_state(path: &Path) -> Result<AutomaticRangeAntiEntropyState> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE_BYTES
    {
        return Err(automatic_error("automatic controller state path is unsafe"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len()
        || bytes.len() as u64 > MAX_AUTOMATIC_RANGE_ANTI_ENTROPY_STATE_BYTES
    {
        return Err(automatic_error(
            "automatic controller state changed or grew while reading",
        ));
    }
    let state: AutomaticRangeAntiEntropyState = serde_json::from_slice(&bytes)?;
    state.validate()?;
    Ok(state)
}

fn ensure_safe_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(automatic_error("automatic controller directory is unsafe"));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        || value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(automatic_error("SHA-256 is not canonical lowercase hex"));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path, fsync: bool) -> Result<()> {
    if fsync {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path, _fsync: bool) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    use super::*;
    use crate::distribution::{DistributionConfig, DistributionStore};
    use crate::distribution_anti_entropy::{RangeDigestLimits, RangeDigestState};
    use crate::distribution_range_consensus::RangeWriteProgress;
    use crate::{ResourceGovernorConfig, ResourceLane};

    #[derive(Debug)]
    struct FakeAuthority {
        voters: Vec<ClusterNodeId>,
        installs: AtomicUsize,
        releases: AtomicUsize,
    }

    impl RangeAntiEntropyFenceAuthority for FakeAuthority {
        fn install_range_anti_entropy_fence(
            &self,
            plan: &RangeAntiEntropyFencePlan,
            _now_ms: u64,
        ) -> Result<RangeBackupFenceQuorum> {
            self.installs.fetch_add(1, Ordering::SeqCst);
            Ok(RangeBackupFenceQuorum {
                plan_id: plan.plan_id,
                range_id: plan.range_id,
                range_epoch: plan.range_epoch,
                resolved_through: 0,
                required_quorum: self.voters.len() / 2 + 1,
                installed_at_ms: plan.created_at_ms,
                expires_at_ms: plan.expires_at_ms,
                observations: self
                    .voters
                    .iter()
                    .map(|node_id| RangeWriteProgress {
                        node_id: node_id.clone(),
                        range_id: plan.range_id,
                        current_epoch: plan.range_epoch,
                        last_index: 0,
                        resolved_through: 0,
                        compacted_through: 0,
                    })
                    .collect(),
            })
        }

        fn release_range_anti_entropy_fence(
            &self,
            _fence: &RangeBackupFenceQuorum,
        ) -> Result<usize> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(1)
        }
    }

    #[derive(Debug)]
    struct FailingAuthority;

    impl RangeAntiEntropyFenceAuthority for FailingAuthority {
        fn install_range_anti_entropy_fence(
            &self,
            _plan: &RangeAntiEntropyFencePlan,
            _now_ms: u64,
        ) -> Result<RangeBackupFenceQuorum> {
            Err(automatic_error("injected fence failure"))
        }

        fn release_range_anti_entropy_fence(
            &self,
            _fence: &RangeBackupFenceQuorum,
        ) -> Result<usize> {
            Err(automatic_error("unexpected release"))
        }
    }

    #[derive(Debug)]
    struct EmptyDigestTransport {
        cluster_id: ClusterId,
        ranges: BTreeMap<RangeId, RangeDescriptor>,
        records: BTreeMap<(ClusterNodeId, RangeId), Vec<crate::Record>>,
        states: Mutex<BTreeMap<(ClusterNodeId, RangeId, Uuid), RangeDigestState>>,
        calls: AtomicUsize,
    }

    impl RangeDigestTransport for EmptyDigestTransport {
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
            let range = &self.ranges[&range_id];
            let mut states = self.states.lock();
            let state = states
                .entry((destination.clone(), range_id, session_id))
                .or_insert_with(|| {
                    RangeDigestState::create(
                        session_id,
                        self.cluster_id.clone(),
                        destination.clone(),
                        range_id,
                        range_epoch,
                        range.start_token,
                        range.end_token,
                        0,
                        limits,
                    )
                    .unwrap()
                });
            if let Some(expected) = expected_checksum_sha256 {
                if state.checksum_sha256 != expected {
                    return Err(automatic_error("test digest checksum is stale"));
                }
                if !state.completed {
                    let records = self
                        .records
                        .get(&(destination.clone(), range_id))
                        .cloned()
                        .unwrap_or_default();
                    if state.scanned_records == 0 && !records.is_empty() {
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
                            &records,
                            bytes,
                            limits,
                        )?;
                    } else {
                        let cursor = state.resume_after_key.clone();
                        state.finish(cursor.as_deref(), limits)?;
                    }
                }
            }
            Ok(state.clone())
        }
    }

    fn fixture(
        directory: &Path,
    ) -> (
        ClusterTopology,
        ClusterNodeId,
        FakeAuthority,
        EmptyDigestTransport,
        AutomaticRangeAntiEntropyLimits,
    ) {
        let node_id = ClusterNodeId::new("node-1").unwrap();
        let config = DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("automatic-digest-cluster").unwrap(),
            node_id: node_id.clone(),
            node_address: "127.0.0.1:9444".to_string(),
            replication_factor: 1,
            initial_ranges: 2,
            ..DistributionConfig::default()
        };
        let store =
            DistributionStore::initialize_at(directory.join("topology"), config, false, 1).unwrap();
        let topology = store.topology().clone();
        let authority = FakeAuthority {
            voters: vec![node_id.clone()],
            installs: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
        };
        let transport = EmptyDigestTransport {
            cluster_id: topology.cluster_id.clone(),
            ranges: topology
                .ranges
                .values()
                .cloned()
                .map(|range| (range.id, range))
                .collect(),
            records: BTreeMap::new(),
            states: Mutex::new(BTreeMap::new()),
            calls: AtomicUsize::new(0),
        };
        let mut limits = AutomaticRangeAntiEntropyLimits::default();
        limits.between_ranges_ms = 10;
        limits.sweep_interval_ms = 1_000;
        limits.foreground_retry_ms = 10;
        limits.schedule.step_interval_ms = 1;
        limits.schedule.run.digest.bucket_count = 16;
        (topology, node_id, authority, transport, limits)
    }

    #[test]
    fn automatic_scheduler_defers_foreground_recovers_and_releases_healthy_fence() {
        let directory = tempfile::tempdir().unwrap();
        let (topology, node_id, authority, transport, limits) = fixture(directory.path());
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut controller = AutomaticRangeAntiEntropyController::open(
            directory.path(),
            topology.cluster_id.clone(),
            node_id,
            100,
            limits,
            false,
        )
        .unwrap();
        let planned = controller
            .tick(&topology, &authority, &transport, &governor, 100)
            .unwrap();
        let range_id = match planned {
            AutomaticRangeAntiEntropyAdvance::FencePlanned { range_id } => range_id,
            other => panic!("unexpected plan outcome: {other:?}"),
        };

        let foreground = governor
            .try_admit(
                ResourceLane::ForegroundRead,
                ResourceDemand {
                    memory_bytes: 1,
                    io_bytes: 1,
                    cpu_slots: 1,
                    io_charge_bytes: 1,
                },
                100,
            )
            .unwrap();
        assert_eq!(
            controller
                .tick(&topology, &authority, &transport, &governor, 101)
                .unwrap(),
            AutomaticRangeAntiEntropyAdvance::ForegroundDeferred { retry_at_ms: 111 }
        );
        assert_eq!(authority.installs.load(Ordering::SeqCst), 0);
        drop(foreground);
        drop(controller);

        let mut controller = AutomaticRangeAntiEntropyController::open(
            directory.path(),
            topology.cluster_id.clone(),
            ClusterNodeId::new("node-1").unwrap(),
            111,
            limits,
            false,
        )
        .unwrap();
        assert_eq!(
            controller.state().active_plan.as_ref().unwrap().range_id,
            range_id
        );
        assert_eq!(
            controller
                .tick(&topology, &authority, &transport, &governor, 111)
                .unwrap(),
            AutomaticRangeAntiEntropyAdvance::FenceInstalled { range_id }
        );
        let foreground = governor
            .try_admit(
                ResourceLane::ForegroundWrite,
                ResourceDemand {
                    memory_bytes: 1,
                    io_bytes: 1,
                    cpu_slots: 1,
                    io_charge_bytes: 1,
                },
                111,
            )
            .unwrap();
        assert_eq!(
            controller
                .tick(&topology, &authority, &transport, &governor, 112)
                .unwrap(),
            AutomaticRangeAntiEntropyAdvance::ForegroundDeferred { retry_at_ms: 122 }
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        drop(foreground);
        for _ in 0..10 {
            let now_ms = controller.state().next_attempt_at_ms;
            let outcome = controller
                .tick(&topology, &authority, &transport, &governor, now_ms)
                .unwrap();
            if outcome == (AutomaticRangeAntiEntropyAdvance::Healthy { range_id }) {
                assert_eq!(authority.releases.load(Ordering::SeqCst), 1);
                assert!(controller.state().active_plan.is_none());
                assert!(controller.state().active_fence.is_none());
                assert!(!controller.schedule_path.exists());
                assert_eq!(governor.snapshot().background.active, 0);
                return;
            }
        }
        panic!("automatic range digest did not complete");
    }

    #[test]
    fn range_plan_rejects_epoch_or_replica_drift() {
        let directory = tempfile::tempdir().unwrap();
        let (mut topology, node_id, _, _, limits) = fixture(directory.path());
        let range_id = topology.ranges.values().next().unwrap().id;
        let plan = RangeAntiEntropyFencePlan::create(
            &topology,
            &node_id,
            range_id,
            100,
            limits.fence_ttl_ms,
        )
        .unwrap();
        topology
            .ranges
            .values_mut()
            .find(|range| range.id == range_id)
            .unwrap()
            .epoch += 1;
        assert!(plan.validate(&topology, &node_id, 101).is_err());
    }

    #[test]
    fn failed_tick_restores_the_last_valid_controller_state() {
        let directory = tempfile::tempdir().unwrap();
        let (topology, node_id, _, transport, limits) = fixture(directory.path());
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut controller = AutomaticRangeAntiEntropyController::open(
            directory.path().join("failed-tick"),
            topology.cluster_id.clone(),
            node_id,
            100,
            limits,
            false,
        )
        .unwrap();
        controller
            .tick(&topology, &FailingAuthority, &transport, &governor, 100)
            .unwrap();
        let before = controller.state().clone();
        assert!(controller
            .tick(&topology, &FailingAuthority, &transport, &governor, 101)
            .is_err());
        assert_eq!(controller.state(), &before);
        controller.state().validate().unwrap();
    }

    #[test]
    fn restart_adopts_new_limits_only_without_active_work() {
        let directory = tempfile::tempdir().unwrap();
        let (topology, node_id, authority, transport, limits) = fixture(directory.path());
        AutomaticRangeAntiEntropyController::open(
            directory.path().join("reconfigure"),
            topology.cluster_id.clone(),
            node_id.clone(),
            100,
            limits,
            false,
        )
        .unwrap();

        let mut changed = limits;
        changed.max_range_qps = 7;
        let mut controller = AutomaticRangeAntiEntropyController::open(
            directory.path().join("reconfigure"),
            topology.cluster_id.clone(),
            node_id.clone(),
            100,
            changed,
            false,
        )
        .unwrap();
        assert_eq!(controller.state().limits, changed);

        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        assert!(matches!(
            controller
                .tick(&topology, &authority, &transport, &governor, 100)
                .unwrap(),
            AutomaticRangeAntiEntropyAdvance::FencePlanned { .. }
        ));
        drop(controller);

        changed.max_range_qps = 8;
        assert!(AutomaticRangeAntiEntropyController::open(
            directory.path().join("reconfigure"),
            topology.cluster_id.clone(),
            node_id,
            100,
            changed,
            false,
        )
        .is_err());
    }

    #[test]
    fn automatic_scheduler_keeps_certified_divergence_fenced_for_repair() {
        use crate::distribution::{ClusterNode, RangeReplica, RangeReplicaRole, ReplicaId};

        let directory = tempfile::tempdir().unwrap();
        let (mut topology, node_id, _, mut transport, limits) = fixture(directory.path());
        let second = ClusterNodeId::new("node-2").unwrap();
        topology.nodes.insert(
            second.clone(),
            ClusterNode::new(second.clone(), "127.0.0.1:9445", 1, 10_000, 2).unwrap(),
        );
        topology.replication_factor = 2;
        let mut next_replica_id = topology.next_replica_id;
        for range in topology.ranges.values_mut() {
            range.replicas.push(RangeReplica {
                id: ReplicaId::new(next_replica_id).unwrap(),
                node_id: second.clone(),
                role: RangeReplicaRole::Voter,
            });
            next_replica_id += 1;
        }
        topology.next_replica_id = next_replica_id;
        topology.validate().unwrap();
        transport.ranges = topology
            .ranges
            .values()
            .cloned()
            .map(|range| (range.id, range))
            .collect();
        // Match the controller's stable selection order rather than the
        // topology map's token order.
        let range = topology
            .ranges
            .values()
            .min_by_key(|range| range.id)
            .unwrap()
            .clone();
        let record_id = (0..100_000)
            .map(|number| format!("divergent-{number}"))
            .find(|record_id| {
                range.contains_token(crate::distribution_key_token("items", record_id))
            })
            .unwrap();
        transport.records.insert(
            (second.clone(), range.id),
            vec![crate::Record::new(record_id)],
        );
        let authority = FakeAuthority {
            voters: vec![node_id.clone(), second],
            installs: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
        };
        let governor = ResourceGovernor::new(ResourceGovernorConfig::default(), 100).unwrap();
        let mut controller = AutomaticRangeAntiEntropyController::open(
            directory.path().join("divergent"),
            topology.cluster_id.clone(),
            node_id,
            100,
            limits,
            false,
        )
        .unwrap();
        for _ in 0..20 {
            let now_ms = controller.state().next_attempt_at_ms.max(100);
            let outcome = controller
                .tick(&topology, &authority, &transport, &governor, now_ms)
                .unwrap();
            if let AutomaticRangeAntiEntropyAdvance::RepairRequired(report) = outcome {
                assert_eq!(report.outcome, RangeDigestRunOutcome::DivergentUncertified);
                assert_eq!(authority.releases.load(Ordering::SeqCst), 0);
                assert!(controller.state().active_fence.is_some());
                assert_eq!(controller.state().repair_required, Some(report));
                return;
            }
        }
        panic!("automatic range digest did not surface divergence");
    }
}
