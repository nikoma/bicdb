//! Fixed-cardinality node resource governance.
//!
//! Every operation declares its peak resident memory, in-flight I/O, CPU slot,
//! and rate-charged I/O demand before work begins. Background lanes are capped
//! below node totals so consensus and foreground traffic retain configured
//! headroom under repair, backup, compaction, or index-build pressure.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::error::{BicDbError, Result};

fn governance_error(message: impl Into<String>) -> BicDbError {
    BicDbError::ResourceGovernance(message.into())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLane {
    Consensus,
    ForegroundWrite,
    ForegroundRead,
    AntiEntropy,
    BackupRestore,
    Compaction,
    IndexBuild,
}

impl ResourceLane {
    pub const ALL: [Self; 7] = [
        Self::Consensus,
        Self::ForegroundWrite,
        Self::ForegroundRead,
        Self::AntiEntropy,
        Self::BackupRestore,
        Self::Compaction,
        Self::IndexBuild,
    ];

    pub fn is_background(self) -> bool {
        matches!(
            self,
            Self::AntiEntropy | Self::BackupRestore | Self::Compaction | Self::IndexBuild
        )
    }

    pub fn is_critical(self) -> bool {
        matches!(self, Self::Consensus | Self::ForegroundWrite)
    }

    pub const fn metric_label(self) -> &'static str {
        match self {
            Self::Consensus => "consensus",
            Self::ForegroundWrite => "foreground_write",
            Self::ForegroundRead => "foreground_read",
            Self::AntiEntropy => "anti_entropy",
            Self::BackupRestore => "backup_restore",
            Self::Compaction => "compaction",
            Self::IndexBuild => "index_build",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceCapacity {
    pub memory_bytes: u64,
    pub io_bytes: u64,
    pub cpu_slots: usize,
}

impl ResourceCapacity {
    fn validate_nonzero(self, label: &str) -> Result<()> {
        if self.memory_bytes == 0 || self.io_bytes == 0 || self.cpu_slots == 0 {
            return Err(governance_error(format!(
                "{label} memory, I/O, and CPU capacity must be nonzero"
            )));
        }
        Ok(())
    }

    fn fits_within(self, outer: Self) -> bool {
        self.memory_bytes <= outer.memory_bytes
            && self.io_bytes <= outer.io_bytes
            && self.cpu_slots <= outer.cpu_slots
    }

    fn saturating_sub(self, reserved: Self) -> Self {
        Self {
            memory_bytes: self.memory_bytes.saturating_sub(reserved.memory_bytes),
            io_bytes: self.io_bytes.saturating_sub(reserved.io_bytes),
            cpu_slots: self.cpu_slots.saturating_sub(reserved.cpu_slots),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceLaneLimit {
    pub capacity: ResourceCapacity,
    pub max_active: usize,
    pub io_bytes_per_second: u64,
    pub io_burst_bytes: u64,
}

impl ResourceLaneLimit {
    fn validate(self, node: ResourceCapacity) -> Result<()> {
        self.capacity.validate_nonzero("lane")?;
        if !self.capacity.fits_within(node)
            || self.max_active == 0
            || self.max_active > 1_000_000
            || self.io_bytes_per_second == 0
            || self.io_burst_bytes == 0
            || self.io_burst_bytes > node.io_bytes.saturating_mul(1024)
        {
            return Err(governance_error("lane resource limits are invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceGovernorConfig {
    pub node: ResourceCapacity,
    pub critical_reserve: ResourceCapacity,
    pub background: ResourceCapacity,
    pub lanes: BTreeMap<ResourceLane, ResourceLaneLimit>,
}

impl Default for ResourceGovernorConfig {
    fn default() -> Self {
        let cpu_slots = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .max(2);
        let node = ResourceCapacity {
            memory_bytes: 8 * 1024 * 1024 * 1024,
            io_bytes: 2 * 1024 * 1024 * 1024,
            cpu_slots,
        };
        let standard = ResourceLaneLimit {
            capacity: node,
            max_active: 4_096,
            io_bytes_per_second: 1024 * 1024 * 1024,
            io_burst_bytes: 2 * 1024 * 1024 * 1024,
        };
        let background_limit = ResourceLaneLimit {
            capacity: ResourceCapacity {
                memory_bytes: 4 * 1024 * 1024 * 1024,
                io_bytes: 1024 * 1024 * 1024,
                cpu_slots: cpu_slots.saturating_sub(1).max(1),
            },
            max_active: 64,
            io_bytes_per_second: 256 * 1024 * 1024,
            io_burst_bytes: 512 * 1024 * 1024,
        };
        let lanes = ResourceLane::ALL
            .into_iter()
            .map(|lane| {
                (
                    lane,
                    if lane.is_background() {
                        background_limit
                    } else {
                        standard
                    },
                )
            })
            .collect();
        Self {
            node,
            critical_reserve: ResourceCapacity {
                memory_bytes: 2 * 1024 * 1024 * 1024,
                io_bytes: 512 * 1024 * 1024,
                cpu_slots: 1,
            },
            background: background_limit.capacity,
            lanes,
        }
    }
}

impl ResourceGovernorConfig {
    pub fn validate(&self) -> Result<()> {
        self.node.validate_nonzero("node")?;
        self.critical_reserve.validate_nonzero("critical reserve")?;
        self.background.validate_nonzero("background")?;
        if !self.critical_reserve.fits_within(self.node)
            || !self.background.fits_within(self.node)
            || self
                .background
                .memory_bytes
                .saturating_add(self.critical_reserve.memory_bytes)
                > self.node.memory_bytes
            || self
                .background
                .io_bytes
                .saturating_add(self.critical_reserve.io_bytes)
                > self.node.io_bytes
            || self
                .background
                .cpu_slots
                .saturating_add(self.critical_reserve.cpu_slots)
                > self.node.cpu_slots
            || self.lanes.len() != ResourceLane::ALL.len()
        {
            return Err(governance_error(
                "node, background, and critical-reserve capacities are inconsistent",
            ));
        }
        for lane in ResourceLane::ALL {
            self.lanes
                .get(&lane)
                .ok_or_else(|| governance_error("resource lane configuration is incomplete"))?
                .validate(self.node)?;
        }
        Ok(())
    }

    /// Prove one operation fits a lane's immutable hard envelope without
    /// consuming concurrency or rate tokens.
    ///
    /// Hosts use this when binding durable maintenance schedules to a possibly
    /// changed node policy. A demand that can never fit is a configuration
    /// error, not transient saturation, and must not enter an endless retry
    /// loop.
    pub fn validate_demand(&self, lane: ResourceLane, demand: ResourceDemand) -> Result<()> {
        self.validate()?;
        demand.validate()?;
        let limit = self
            .lanes
            .get(&lane)
            .ok_or_else(|| governance_error(format!("resource lane {lane:?} is not configured")))?;
        if !demand.capacity().fits_within(limit.capacity)
            || demand.io_charge_bytes > limit.io_burst_bytes
        {
            return Err(governance_error(format!(
                "resource demand exceeds the {lane:?} lane hard bound"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceDemand {
    pub memory_bytes: u64,
    pub io_bytes: u64,
    pub cpu_slots: usize,
    /// Bytes charged against the lane's token bucket when admitted.
    pub io_charge_bytes: u64,
}

impl ResourceDemand {
    pub fn validate(self) -> Result<()> {
        if self.memory_bytes == 0
            || self.io_bytes == 0
            || self.cpu_slots == 0
            || self.io_charge_bytes == 0
        {
            return Err(governance_error(
                "resource demand must be explicitly nonzero",
            ));
        }
        Ok(())
    }

    fn capacity(self) -> ResourceCapacity {
        ResourceCapacity {
            memory_bytes: self.memory_bytes,
            io_bytes: self.io_bytes,
            cpu_slots: self.cpu_slots,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub io_bytes: u64,
    pub cpu_slots: usize,
    pub active: usize,
}

impl ResourceUsage {
    fn can_add(self, demand: ResourceDemand, capacity: ResourceCapacity) -> bool {
        self.memory_bytes
            .checked_add(demand.memory_bytes)
            .is_some_and(|value| value <= capacity.memory_bytes)
            && self
                .io_bytes
                .checked_add(demand.io_bytes)
                .is_some_and(|value| value <= capacity.io_bytes)
            && self
                .cpu_slots
                .checked_add(demand.cpu_slots)
                .is_some_and(|value| value <= capacity.cpu_slots)
    }

    fn add(&mut self, demand: ResourceDemand) {
        self.memory_bytes += demand.memory_bytes;
        self.io_bytes += demand.io_bytes;
        self.cpu_slots += demand.cpu_slots;
        self.active += 1;
    }

    fn remove(&mut self, demand: ResourceDemand) {
        self.memory_bytes = self.memory_bytes.saturating_sub(demand.memory_bytes);
        self.io_bytes = self.io_bytes.saturating_sub(demand.io_bytes);
        self.cpu_slots = self.cpu_slots.saturating_sub(demand.cpu_slots);
        self.active = self.active.saturating_sub(1);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceLaneSnapshot {
    pub lane: ResourceLane,
    pub usage: ResourceUsage,
    pub admitted: u64,
    pub rejected: u64,
    pub io_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceGovernorSnapshot {
    pub total: ResourceUsage,
    pub noncritical: ResourceUsage,
    pub background: ResourceUsage,
    pub lanes: Vec<ResourceLaneSnapshot>,
}

impl ResourceGovernorSnapshot {
    /// Prometheus exposition with a compile-time fixed set of scope and lane
    /// labels. No tenant, range, query, path, or plugin data can enter it.
    pub fn prometheus_text(&self) -> String {
        fn write_usage(output: &mut String, scope: &str, usage: ResourceUsage) {
            let _ = writeln!(
                output,
                "bicdb_resource_memory_bytes{{scope=\"{scope}\"}} {}",
                usage.memory_bytes
            );
            let _ = writeln!(
                output,
                "bicdb_resource_io_bytes{{scope=\"{scope}\"}} {}",
                usage.io_bytes
            );
            let _ = writeln!(
                output,
                "bicdb_resource_cpu_slots{{scope=\"{scope}\"}} {}",
                usage.cpu_slots
            );
            let _ = writeln!(
                output,
                "bicdb_resource_active{{scope=\"{scope}\"}} {}",
                usage.active
            );
        }

        let mut output = String::with_capacity(4_096);
        write_usage(&mut output, "total", self.total);
        write_usage(&mut output, "noncritical", self.noncritical);
        write_usage(&mut output, "background", self.background);
        for lane in &self.lanes {
            let label = lane.lane.metric_label();
            let _ = writeln!(
                output,
                "bicdb_resource_lane_memory_bytes{{lane=\"{label}\"}} {}",
                lane.usage.memory_bytes
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_io_bytes{{lane=\"{label}\"}} {}",
                lane.usage.io_bytes
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_cpu_slots{{lane=\"{label}\"}} {}",
                lane.usage.cpu_slots
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_active{{lane=\"{label}\"}} {}",
                lane.usage.active
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_admitted_total{{lane=\"{label}\"}} {}",
                lane.admitted
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_rejected_total{{lane=\"{label}\"}} {}",
                lane.rejected
            );
            let _ = writeln!(
                output,
                "bicdb_resource_lane_io_tokens{{lane=\"{label}\"}} {}",
                lane.io_tokens
            );
        }
        output
    }
}

#[derive(Debug)]
struct LaneState {
    usage: ResourceUsage,
    admitted: u64,
    rejected: u64,
    io_tokens: u64,
    last_refill_ms: u64,
}

#[derive(Debug)]
struct GovernorState {
    total: ResourceUsage,
    noncritical: ResourceUsage,
    background: ResourceUsage,
    lanes: BTreeMap<ResourceLane, LaneState>,
}

#[derive(Debug)]
struct GovernorInner {
    config: ResourceGovernorConfig,
    state: Mutex<GovernorState>,
}

#[derive(Clone, Debug)]
pub struct ResourceGovernor {
    inner: Arc<GovernorInner>,
}

impl ResourceGovernor {
    pub fn new(config: ResourceGovernorConfig, now_ms: u64) -> Result<Self> {
        config.validate()?;
        let lanes = ResourceLane::ALL
            .into_iter()
            .map(|lane| {
                let limit = config.lanes[&lane];
                (
                    lane,
                    LaneState {
                        usage: ResourceUsage::default(),
                        admitted: 0,
                        rejected: 0,
                        io_tokens: limit.io_burst_bytes,
                        last_refill_ms: now_ms,
                    },
                )
            })
            .collect();
        Ok(Self {
            inner: Arc::new(GovernorInner {
                config,
                state: Mutex::new(GovernorState {
                    total: ResourceUsage::default(),
                    noncritical: ResourceUsage::default(),
                    background: ResourceUsage::default(),
                    lanes,
                }),
            }),
        })
    }

    /// Validate a demand against this governor's fixed hard configuration
    /// without altering admission state.
    pub fn validate_demand(&self, lane: ResourceLane, demand: ResourceDemand) -> Result<()> {
        self.inner.config.validate_demand(lane, demand)
    }

    pub fn try_admit(
        &self,
        lane: ResourceLane,
        demand: ResourceDemand,
        now_ms: u64,
    ) -> Result<ResourcePermit> {
        self.validate_demand(lane, demand)?;
        let limit = self.inner.config.lanes[&lane];
        let mut state = self.inner.state.lock();
        let lane_state = state
            .lanes
            .get_mut(&lane)
            .expect("validated fixed resource lane");
        if now_ms < lane_state.last_refill_ms {
            lane_state.rejected = lane_state.rejected.saturating_add(1);
            return Err(governance_error("resource governor clock regressed"));
        }
        let elapsed_ms = now_ms - lane_state.last_refill_ms;
        let refill = (elapsed_ms as u128).saturating_mul(limit.io_bytes_per_second as u128) / 1000;
        lane_state.io_tokens = lane_state
            .io_tokens
            .saturating_add(refill.min(u64::MAX as u128) as u64)
            .min(limit.io_burst_bytes);
        lane_state.last_refill_ms = now_ms;
        let lane_fits = lane_state.usage.can_add(demand, limit.capacity)
            && lane_state.usage.active < limit.max_active
            && lane_state.io_tokens >= demand.io_charge_bytes;
        let total_fits = state.total.can_add(demand, self.inner.config.node);
        let noncritical_fits = lane.is_critical()
            || state.noncritical.can_add(
                demand,
                self.inner
                    .config
                    .node
                    .saturating_sub(self.inner.config.critical_reserve),
            );
        let background_fits = !lane.is_background()
            || state
                .background
                .can_add(demand, self.inner.config.background);
        if !lane_fits || !total_fits || !noncritical_fits || !background_fits {
            state
                .lanes
                .get_mut(&lane)
                .expect("validated fixed resource lane")
                .rejected = state.lanes[&lane].rejected.saturating_add(1);
            return Err(governance_error(format!(
                "resource lane {lane:?} is saturated"
            )));
        }
        let lane_state = state
            .lanes
            .get_mut(&lane)
            .expect("validated fixed resource lane");
        lane_state.io_tokens -= demand.io_charge_bytes;
        lane_state.usage.add(demand);
        lane_state.admitted = lane_state.admitted.saturating_add(1);
        state.total.add(demand);
        if !lane.is_critical() {
            state.noncritical.add(demand);
        }
        if lane.is_background() {
            state.background.add(demand);
        }
        Ok(ResourcePermit {
            governor: self.clone(),
            lane,
            demand,
            released: false,
        })
    }

    pub fn snapshot(&self) -> ResourceGovernorSnapshot {
        let state = self.inner.state.lock();
        ResourceGovernorSnapshot {
            total: state.total,
            noncritical: state.noncritical,
            background: state.background,
            lanes: ResourceLane::ALL
                .into_iter()
                .map(|lane| {
                    let lane_state = &state.lanes[&lane];
                    ResourceLaneSnapshot {
                        lane,
                        usage: lane_state.usage,
                        admitted: lane_state.admitted,
                        rejected: lane_state.rejected,
                        io_tokens: lane_state.io_tokens,
                    }
                })
                .collect(),
        }
    }

    pub fn prometheus_text(&self) -> String {
        self.snapshot().prometheus_text()
    }

    fn release(&self, lane: ResourceLane, demand: ResourceDemand) {
        let mut state = self.inner.state.lock();
        state
            .lanes
            .get_mut(&lane)
            .expect("validated fixed resource lane")
            .usage
            .remove(demand);
        state.total.remove(demand);
        if !lane.is_critical() {
            state.noncritical.remove(demand);
        }
        if lane.is_background() {
            state.background.remove(demand);
        }
    }
}

#[derive(Debug)]
pub struct ResourcePermit {
    governor: ResourceGovernor,
    lane: ResourceLane,
    demand: ResourceDemand,
    released: bool,
}

impl ResourcePermit {
    pub fn release(mut self) {
        self.release_once();
    }

    fn release_once(&mut self) {
        if !self.released {
            self.governor.release(self.lane, self.demand);
            self.released = true;
        }
    }
}

impl Drop for ResourcePermit {
    fn drop(&mut self) {
        self.release_once();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ResourceGovernorConfig {
        let node = ResourceCapacity {
            memory_bytes: 100,
            io_bytes: 100,
            cpu_slots: 4,
        };
        let foreground = ResourceLaneLimit {
            capacity: node,
            max_active: 8,
            io_bytes_per_second: 100,
            io_burst_bytes: 100,
        };
        let background = ResourceLaneLimit {
            capacity: ResourceCapacity {
                memory_bytes: 70,
                io_bytes: 70,
                cpu_slots: 3,
            },
            max_active: 4,
            io_bytes_per_second: 10,
            io_burst_bytes: 20,
        };
        ResourceGovernorConfig {
            node,
            critical_reserve: ResourceCapacity {
                memory_bytes: 30,
                io_bytes: 30,
                cpu_slots: 1,
            },
            background: background.capacity,
            lanes: ResourceLane::ALL
                .into_iter()
                .map(|lane| {
                    (
                        lane,
                        if lane.is_background() {
                            background
                        } else {
                            foreground
                        },
                    )
                })
                .collect(),
        }
    }

    fn demand(memory: u64, io: u64, cpu: usize, charge: u64) -> ResourceDemand {
        ResourceDemand {
            memory_bytes: memory,
            io_bytes: io,
            cpu_slots: cpu,
            io_charge_bytes: charge,
        }
    }

    #[test]
    fn background_saturation_preserves_consensus_headroom_and_drop_releases() {
        let governor = ResourceGovernor::new(config(), 1_000).unwrap();
        let background = governor
            .try_admit(ResourceLane::AntiEntropy, demand(70, 70, 3, 10), 1_000)
            .unwrap();
        assert!(governor
            .try_admit(ResourceLane::IndexBuild, demand(1, 1, 1, 1), 1_000)
            .is_err());
        let consensus = governor
            .try_admit(ResourceLane::Consensus, demand(30, 30, 1, 10), 1_000)
            .unwrap();
        assert_eq!(governor.snapshot().total.memory_bytes, 100);
        drop(background);
        drop(consensus);
        assert_eq!(governor.snapshot().total, ResourceUsage::default());

        let reads = governor
            .try_admit(ResourceLane::ForegroundRead, demand(70, 70, 3, 10), 1_000)
            .unwrap();
        assert!(governor
            .try_admit(ResourceLane::ForegroundRead, demand(1, 1, 1, 1), 1_000)
            .is_err());
        let consensus = governor
            .try_admit(ResourceLane::Consensus, demand(30, 30, 1, 10), 1_000)
            .unwrap();
        drop(reads);
        drop(consensus);
        assert_eq!(governor.snapshot().total, ResourceUsage::default());
    }

    #[test]
    fn lane_rate_tokens_refill_monotonically_and_metrics_are_fixed_cardinality() {
        let governor = ResourceGovernor::new(config(), 100).unwrap();
        let permit = governor
            .try_admit(ResourceLane::AntiEntropy, demand(1, 1, 1, 20), 100)
            .unwrap();
        drop(permit);
        assert!(governor
            .try_admit(ResourceLane::AntiEntropy, demand(1, 1, 1, 1), 100)
            .is_err());
        assert!(governor
            .try_admit(ResourceLane::AntiEntropy, demand(1, 1, 1, 10), 1_100)
            .is_ok());
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.lanes.len(), ResourceLane::ALL.len());
        let anti_entropy = snapshot
            .lanes
            .iter()
            .find(|lane| lane.lane == ResourceLane::AntiEntropy)
            .unwrap();
        assert_eq!(anti_entropy.admitted, 2);
        assert_eq!(anti_entropy.rejected, 1);
        assert!(governor
            .try_admit(ResourceLane::AntiEntropy, demand(1, 1, 1, 1), 1_000)
            .unwrap_err()
            .to_string()
            .contains("clock regressed"));
    }

    #[test]
    fn hard_demand_validation_is_non_mutating_and_distinct_from_saturation() {
        let governor = ResourceGovernor::new(config(), 100).unwrap();
        let before = governor.snapshot();
        governor
            .validate_demand(ResourceLane::Compaction, demand(70, 70, 3, 20))
            .unwrap();
        assert_eq!(governor.snapshot(), before);

        let error = governor
            .validate_demand(ResourceLane::Compaction, demand(71, 70, 3, 20))
            .unwrap_err();
        assert!(error.to_string().contains("hard bound"));
        assert_eq!(governor.snapshot(), before);
    }

    #[test]
    fn prometheus_export_has_only_fixed_scope_and_lane_series() {
        let governor = ResourceGovernor::new(config(), 100).unwrap();
        let text = governor.prometheus_text();
        assert_eq!(text.lines().count(), 12 + ResourceLane::ALL.len() * 7);
        assert!(!text.contains("tenant"));
        assert!(!text.contains("range"));
        assert!(!text.contains("query"));
        for lane in ResourceLane::ALL {
            let label = format!("lane=\"{}\"", lane.metric_label());
            assert_eq!(text.matches(&label).count(), 7);
        }
        for scope in ["total", "noncritical", "background"] {
            let label = format!("scope=\"{scope}\"");
            assert_eq!(text.matches(&label).count(), 4);
        }
    }
}
