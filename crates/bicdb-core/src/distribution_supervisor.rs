//! Automatic cluster controller, bounded relocation driver boundary, metrics,
//! and operational compatibility contracts.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::distribution::{
    build_failure_repair_plan, ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterNodeLiveness,
    ClusterPlacementPlanner, ClusterTopology, DistributionStore, FailureRepairPlan,
    RangeDescriptor, RangeId, RangeRelocation, RangeSnapshotOptions, RebalanceOptions,
    RelocationId, RelocationPhase, CLUSTER_DATA_PROTOCOL_VERSION, DISTRIBUTION_FORMAT_VERSION,
    DISTRIBUTION_HASH_VERSION,
};
use crate::distribution_consensus::CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION;
use crate::distribution_query::{
    DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION, DISTRIBUTED_QUERY_PROTOCOL_VERSION,
};
use crate::error::{BicDbError, Result};
use crate::CancellationToken;

fn supervisor_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSupervisorConfig {
    pub tick_interval_ms: u64,
    pub heartbeat_each_tick: bool,
    pub refresh_topology_each_tick: bool,
    pub automatically_rebalance: bool,
    pub retry_failed_relocations: bool,
    pub relocation_retry_after_ms: u64,
    pub max_relocation_steps_per_tick: usize,
    pub rebalance: RebalanceOptions,
}

impl Default for ClusterSupervisorConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: 1_000,
            heartbeat_each_tick: true,
            refresh_topology_each_tick: true,
            automatically_rebalance: false,
            retry_failed_relocations: false,
            relocation_retry_after_ms: 5_000,
            max_relocation_steps_per_tick: 16,
            rebalance: RebalanceOptions::default(),
        }
    }
}

impl ClusterSupervisorConfig {
    pub fn validate(&self) -> Result<()> {
        if !(10..=3_600_000).contains(&self.tick_interval_ms) {
            return Err(supervisor_error(
                "cluster supervisor tick interval must be between 10ms and 1h",
            ));
        }
        if self.max_relocation_steps_per_tick == 0 || self.max_relocation_steps_per_tick > 4_096 {
            return Err(supervisor_error(
                "cluster supervisor relocation step limit must be between 1 and 4096",
            ));
        }
        self.rebalance.validate()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelocationDriveOutcome {
    Deferred,
    Progressed(RelocationPhase),
}

/// Supplies the physical snapshot/catch-up transport. The controller owns
/// scheduling and durable metadata; this driver owns bounded data movement.
pub trait ClusterRelocationDriver {
    fn drive_relocation(
        &mut self,
        store: &mut DistributionStore,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<RelocationDriveOutcome>;
}

/// Safe default for a control-plane-only process. It leaves every physical
/// relocation at its durable phase until a data transport is installed.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeferredClusterRelocationDriver;

impl ClusterRelocationDriver for DeferredClusterRelocationDriver {
    fn drive_relocation(
        &mut self,
        _store: &mut DistributionStore,
        _relocation_id: RelocationId,
        _actor: &ClusterNodeId,
        _now_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<RelocationDriveOutcome> {
        cancellation.check()?;
        Ok(RelocationDriveOutcome::Deferred)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterRelocationTransportConfig {
    pub snapshot: RangeSnapshotOptions,
    pub max_commit_frames_per_step: usize,
}

impl Default for ClusterRelocationTransportConfig {
    fn default() -> Self {
        Self {
            snapshot: RangeSnapshotOptions::default(),
            max_commit_frames_per_step: 1_024,
        }
    }
}

impl ClusterRelocationTransportConfig {
    pub fn validate(&self) -> Result<()> {
        self.snapshot.validate()?;
        if self.max_commit_frames_per_step == 0 || self.max_commit_frames_per_step > 1_000_000 {
            return Err(supervisor_error(
                "relocation catch-up frame limit must be between 1 and 1000000",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotCopyProgress {
    pub bytes_copied: u64,
    pub resume_after_key: Option<String>,
    pub completed: bool,
    pub snapshot_id: Option<String>,
    pub snapshot_sha256: Option<String>,
    pub snapshot_commit_sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatchUpProgress {
    pub destination_durable_commit_sequence: u64,
    pub source_commit_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CleanupProgress {
    pub resume_after_key: Option<String>,
    pub records_deleted: u64,
    pub completed: bool,
}

/// Idempotent, bounded physical operations used by the durable controller.
///
/// Implementations may use TCP/mTLS, an embedded test transport, or another
/// authenticated channel. A successful return means the destination has made
/// the reported progress durable.
pub trait ClusterRelocationTransport {
    fn prepare_learner(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        cancellation: &CancellationToken,
    ) -> Result<()>;

    fn copy_snapshot_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        options: &RangeSnapshotOptions,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotCopyProgress>;

    fn catch_up_step(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        max_commit_frames: usize,
        cancellation: &CancellationToken,
    ) -> Result<CatchUpProgress>;

    fn cleanup_source(
        &mut self,
        relocation: &RangeRelocation,
        range: &RangeDescriptor,
        resume_after_key: Option<&str>,
        max_records: usize,
        cancellation: &CancellationToken,
    ) -> Result<CleanupProgress>;
}

#[derive(Debug)]
pub struct TransportClusterRelocationDriver<Transport> {
    transport: Transport,
    config: ClusterRelocationTransportConfig,
}

impl<Transport> TransportClusterRelocationDriver<Transport> {
    pub fn new(transport: Transport, config: ClusterRelocationTransportConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { transport, config })
    }

    pub fn transport(&self) -> &Transport {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut Transport {
        &mut self.transport
    }

    pub fn into_transport(self) -> Transport {
        self.transport
    }
}

impl<Transport: ClusterRelocationTransport> ClusterRelocationDriver
    for TransportClusterRelocationDriver<Transport>
{
    fn drive_relocation(
        &mut self,
        store: &mut DistributionStore,
        relocation_id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<RelocationDriveOutcome> {
        cancellation.check()?;
        let relocation = store
            .relocation(relocation_id)
            .cloned()
            .ok_or_else(|| supervisor_error(format!("unknown relocation {relocation_id}")))?;
        let range = store
            .topology()
            .range_by_id(relocation.range_id)
            .cloned()
            .ok_or_else(|| {
                supervisor_error(format!(
                    "relocation {relocation_id} references missing range {}",
                    relocation.range_id
                ))
            })?;

        let next = match relocation.phase {
            RelocationPhase::LearnerAllocated => {
                self.transport
                    .prepare_learner(&relocation, &range, cancellation)?;
                store.certify_relocation_schema_install(relocation_id, actor, now_ms)?;
                store.begin_relocation_snapshot(relocation_id, actor, now_ms)?;
                RelocationPhase::SnapshotCopying
            }
            RelocationPhase::SnapshotCopying => {
                let progress = self.transport.copy_snapshot_step(
                    &relocation,
                    &range,
                    &self.config.snapshot,
                    cancellation,
                )?;
                if progress.bytes_copied < relocation.snapshot_bytes_copied {
                    return Err(supervisor_error(format!(
                        "snapshot transport moved {relocation_id} backwards from {} to {} bytes",
                        relocation.snapshot_bytes_copied, progress.bytes_copied
                    )));
                }
                if progress.completed {
                    let snapshot_id = progress
                        .snapshot_id
                        .ok_or_else(|| supervisor_error("completed snapshot has no snapshot id"))?;
                    let snapshot_sha256 = progress
                        .snapshot_sha256
                        .ok_or_else(|| supervisor_error("completed snapshot has no SHA-256"))?;
                    let snapshot_commit_sequence =
                        progress.snapshot_commit_sequence.ok_or_else(|| {
                            supervisor_error("completed snapshot has no commit watermark")
                        })?;
                    store.finish_relocation_snapshot(
                        relocation_id,
                        snapshot_id,
                        snapshot_sha256,
                        snapshot_commit_sequence,
                        progress.bytes_copied,
                        actor,
                        now_ms,
                    )?;
                    RelocationPhase::CatchingUp
                } else {
                    if progress.snapshot_id.is_some()
                        || progress.snapshot_sha256.is_some()
                        || progress.snapshot_commit_sequence.is_some()
                    {
                        return Err(supervisor_error(
                            "incomplete snapshot returned final snapshot metadata",
                        ));
                    }
                    store.checkpoint_relocation_snapshot(
                        relocation_id,
                        progress.bytes_copied,
                        progress.resume_after_key,
                        actor,
                        now_ms,
                    )?;
                    RelocationPhase::SnapshotCopying
                }
            }
            RelocationPhase::CatchingUp => {
                let progress = self.transport.catch_up_step(
                    &relocation,
                    &range,
                    self.config.max_commit_frames_per_step,
                    cancellation,
                )?;
                store.checkpoint_relocation_catch_up(
                    relocation_id,
                    progress.destination_durable_commit_sequence,
                    progress.source_commit_sequence,
                    actor,
                    now_ms,
                )?
            }
            RelocationPhase::ReadyToPromote => {
                store.promote_relocation(relocation_id, actor, now_ms)?;
                RelocationPhase::Promoted
            }
            RelocationPhase::Promoted => {
                store.begin_relocation_cleanup(relocation_id, actor, now_ms)?;
                RelocationPhase::CleaningUp
            }
            RelocationPhase::CleaningUp => {
                let cleanup = self.transport.cleanup_source(
                    &relocation,
                    &range,
                    relocation.cleanup_resume_after_key.as_deref(),
                    self.config.snapshot.max_records_per_batch,
                    cancellation,
                )?;
                if cleanup.completed {
                    store.finish_relocation_cleanup(relocation_id, actor, now_ms)?;
                    RelocationPhase::Completed
                } else {
                    let resume_after_key = cleanup
                        .resume_after_key
                        .ok_or_else(|| supervisor_error("incomplete cleanup has no resume key"))?;
                    store.checkpoint_relocation_cleanup(
                        relocation_id,
                        cleanup.records_deleted,
                        Some(resume_after_key),
                        actor,
                        now_ms,
                    )?;
                    RelocationPhase::CleaningUp
                }
            }
            RelocationPhase::Completed => return Ok(RelocationDriveOutcome::Deferred),
            RelocationPhase::Failed => {
                return Err(supervisor_error(format!(
                    "transport driver cannot advance failed relocation {relocation_id} before retry"
                )));
            }
        };
        Ok(RelocationDriveOutcome::Progressed(next))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterOperationalMetrics {
    pub generated_at_ms: u64,
    pub topology_generation: u64,
    pub nodes_total: u64,
    pub nodes_live: u64,
    pub nodes_suspect: u64,
    pub nodes_dead: u64,
    pub nodes_draining: u64,
    pub nodes_decommissioned: u64,
    pub ranges_total: u64,
    pub replicas_total: u64,
    pub leaders_total: u64,
    pub active_relocations: u64,
    pub failed_relocations: u64,
    pub under_replicated_ranges: u64,
    pub unavailable_ranges: u64,
    pub replica_skew: u64,
    pub leader_skew: u64,
    pub replica_byte_skew: u64,
}

impl ClusterOperationalMetrics {
    pub fn from_topology(
        topology: &ClusterTopology,
        failure_repair: &FailureRepairPlan,
        now_ms: u64,
    ) -> Self {
        let mut metrics = Self {
            generated_at_ms: now_ms,
            topology_generation: topology.generation,
            nodes_total: topology.nodes.len() as u64,
            ranges_total: topology.ranges.len() as u64,
            under_replicated_ranges: failure_repair.under_replicated_ranges.len() as u64,
            unavailable_ranges: failure_repair.unavailable_ranges.len() as u64,
            ..Self::default()
        };
        let mut replica_counts = BTreeMap::<ClusterNodeId, u64>::new();
        let mut leader_counts = BTreeMap::<ClusterNodeId, u64>::new();
        let mut replica_bytes = BTreeMap::<ClusterNodeId, u64>::new();
        for (node_id, node) in &topology.nodes {
            match failure_repair
                .node_liveness
                .get(node_id)
                .copied()
                .unwrap_or(ClusterNodeLiveness::Dead)
            {
                ClusterNodeLiveness::Live => metrics.nodes_live += 1,
                ClusterNodeLiveness::Suspect => metrics.nodes_suspect += 1,
                ClusterNodeLiveness::Dead => metrics.nodes_dead += 1,
                ClusterNodeLiveness::Decommissioned => metrics.nodes_decommissioned += 1,
            }
            if node.lifecycle == ClusterNodeLifecycle::Draining {
                metrics.nodes_draining += 1;
            }
            if node.lifecycle != ClusterNodeLifecycle::Decommissioned {
                replica_counts.insert(node_id.clone(), 0);
                leader_counts.insert(node_id.clone(), 0);
                replica_bytes.insert(node_id.clone(), 0);
            }
        }
        for range in topology.ranges.values() {
            metrics.replicas_total = metrics
                .replicas_total
                .saturating_add(range.replicas.len() as u64);
            metrics.leaders_total = metrics.leaders_total.saturating_add(1);
            *leader_counts.entry(range.leader.clone()).or_default() += 1;
            for replica in &range.replicas {
                *replica_counts.entry(replica.node_id.clone()).or_default() += 1;
                let bytes = replica_bytes.entry(replica.node_id.clone()).or_default();
                *bytes = bytes.saturating_add(range.approximate_bytes);
            }
        }
        for relocation in topology.relocations.values() {
            if relocation.phase == RelocationPhase::Failed {
                metrics.failed_relocations += 1;
            } else if relocation.is_active() {
                metrics.active_relocations += 1;
            }
        }
        metrics.replica_skew = map_skew(&replica_counts);
        metrics.leader_skew = map_skew(&leader_counts);
        metrics.replica_byte_skew = map_skew(&replica_bytes);
        metrics
    }

    pub fn to_prometheus(&self) -> String {
        let fields = [
            ("topology_generation", self.topology_generation),
            ("nodes_total", self.nodes_total),
            ("nodes_live", self.nodes_live),
            ("nodes_suspect", self.nodes_suspect),
            ("nodes_dead", self.nodes_dead),
            ("nodes_draining", self.nodes_draining),
            ("nodes_decommissioned", self.nodes_decommissioned),
            ("ranges_total", self.ranges_total),
            ("replicas_total", self.replicas_total),
            ("leaders_total", self.leaders_total),
            ("active_relocations", self.active_relocations),
            ("failed_relocations", self.failed_relocations),
            ("under_replicated_ranges", self.under_replicated_ranges),
            ("unavailable_ranges", self.unavailable_ranges),
            ("replica_skew", self.replica_skew),
            ("leader_skew", self.leader_skew),
            ("replica_byte_skew", self.replica_byte_skew),
        ];
        let mut output = String::new();
        for (name, value) in fields {
            output.push_str(&format!("bicdb_cluster_{name} {value}\n"));
        }
        output
    }
}

fn map_skew(values: &BTreeMap<ClusterNodeId, u64>) -> u64 {
    let minimum = values.values().copied().min().unwrap_or(0);
    let maximum = values.values().copied().max().unwrap_or(0);
    maximum.saturating_sub(minimum)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSupervisorReport {
    pub started_at_ms: u64,
    pub topology_generation_before: u64,
    pub topology_generation_after: u64,
    pub repair_plan_id: Option<String>,
    pub relocations_started: Vec<RelocationId>,
    pub relocations_retried: Vec<RelocationId>,
    pub relocations_progressed: Vec<RelocationId>,
    pub relocations_deferred: Vec<RelocationId>,
    pub relocation_failures: BTreeMap<RelocationId, String>,
    pub metrics: ClusterOperationalMetrics,
}

#[derive(Debug)]
pub struct ClusterSupervisor {
    config: ClusterSupervisorConfig,
    planner: Option<Box<dyn ClusterPlacementPlanner>>,
}

impl ClusterSupervisor {
    /// Construct a manual-convergence runtime.
    ///
    /// It advances explicitly created relocation state but does not choose new
    /// placement work or automatically retry failed operations. Fleet
    /// controllers opt into those policies through [`Self::with_planner`].
    pub fn new(config: ClusterSupervisorConfig) -> Result<Self> {
        config.validate()?;
        if config.automatically_rebalance || config.retry_failed_relocations {
            return Err(supervisor_error(
                "automatic placement and retry require an injected cluster planner",
            ));
        }
        Ok(Self {
            config,
            planner: None,
        })
    }

    pub fn with_planner(
        config: ClusterSupervisorConfig,
        planner: Box<dyn ClusterPlacementPlanner>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            planner: Some(planner),
        })
    }

    pub fn config(&self) -> &ClusterSupervisorConfig {
        &self.config
    }

    pub fn tick<Driver: ClusterRelocationDriver>(
        &self,
        store: &mut DistributionStore,
        driver: &mut Driver,
        now_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<ClusterSupervisorReport> {
        cancellation.check()?;
        if self.config.refresh_topology_each_tick {
            store.refresh()?;
        }
        let actor = store.config().node_id.clone();
        let generation_before = store.topology().generation;
        if self.config.heartbeat_each_tick {
            let local =
                store.topology().nodes.get(&actor).cloned().ok_or_else(|| {
                    supervisor_error(format!("local node {actor} is not a member"))
                })?;
            let local_tls_certificate_sha256 = store.config().node_tls_certificate_sha256.clone();
            store.heartbeat_with_certificate(
                &actor,
                local.incarnation,
                local.used_bytes,
                local.capacity_bytes,
                local.labels,
                local_tls_certificate_sha256,
                now_ms,
            )?;
        }

        let mut retried = Vec::new();
        if self.config.retry_failed_relocations {
            let retry_ids = store
                .topology()
                .relocations
                .values()
                .filter(|relocation| {
                    relocation.phase == RelocationPhase::Failed
                        && now_ms.saturating_sub(relocation.updated_at_ms)
                            >= self.config.relocation_retry_after_ms
                })
                .map(|relocation| relocation.id)
                .collect::<Vec<_>>();
            for relocation_id in retry_ids {
                cancellation.check()?;
                store.retry_relocation(relocation_id, &actor, now_ms)?;
                retried.push(relocation_id);
            }
        }

        let (repair_plan_id, relocations_started) = if self.config.automatically_rebalance {
            let planner = self.planner.as_ref().ok_or_else(|| {
                supervisor_error("automatic placement requires an injected cluster planner")
            })?;
            let repair = planner.plan_failure_repair(
                store.topology(),
                store.config(),
                &self.config.rebalance,
                now_ms,
            )?;
            let relocations = if repair.rebalance.replica_moves.is_empty()
                && repair.rebalance.leader_transfers.is_empty()
            {
                Vec::new()
            } else {
                store.apply_rebalance_plan(&repair.rebalance, &actor, now_ms)?
            };
            (Some(repair.rebalance.id), relocations)
        } else {
            (None, Vec::new())
        };

        let active = store
            .topology()
            .relocations
            .values()
            .filter(|relocation| {
                relocation.is_active() && relocation.phase != RelocationPhase::Failed
            })
            .map(|relocation| relocation.id)
            .take(self.config.max_relocation_steps_per_tick)
            .collect::<Vec<_>>();
        let mut progressed = Vec::new();
        let mut deferred = Vec::new();
        let mut failures = BTreeMap::new();
        for relocation_id in active {
            cancellation.check()?;
            match driver.drive_relocation(store, relocation_id, &actor, now_ms, cancellation) {
                Ok(RelocationDriveOutcome::Progressed(_)) => progressed.push(relocation_id),
                Ok(RelocationDriveOutcome::Deferred) => deferred.push(relocation_id),
                Err(error) => {
                    let message = error.to_string();
                    store.fail_relocation(relocation_id, &message, &actor, now_ms)?;
                    failures.insert(relocation_id, message);
                }
            }
        }
        let repair = build_failure_repair_plan(
            store.topology(),
            store.config(),
            &self.config.rebalance,
            now_ms,
        )?;
        let metrics = ClusterOperationalMetrics::from_topology(store.topology(), &repair, now_ms);
        Ok(ClusterSupervisorReport {
            started_at_ms: now_ms,
            topology_generation_before: generation_before,
            topology_generation_after: store.topology().generation,
            repair_plan_id,
            relocations_started,
            relocations_retried: retried,
            relocations_progressed: progressed,
            relocations_deferred: deferred,
            relocation_failures: failures,
            metrics,
        })
    }

    pub fn run_until<Driver: ClusterRelocationDriver>(
        &self,
        store: &mut DistributionStore,
        driver: &mut Driver,
        shutdown: &AtomicBool,
        cancellation: &CancellationToken,
        mut now_ms: impl FnMut() -> u64,
        mut on_tick: impl FnMut(&ClusterSupervisorReport),
    ) -> Result<()> {
        while !shutdown.load(Ordering::SeqCst) {
            cancellation.check()?;
            let report = self.tick(store, driver, now_ms(), cancellation)?;
            on_tick(&report);
            if shutdown.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_millis(self.config.tick_interval_ms));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterProtocolCapabilities {
    pub distribution_format_version: u32,
    pub distribution_hash_version: u32,
    pub cluster_data_protocol_version: u32,
    pub metadata_consensus_format_version: u32,
    pub distributed_query_protocol_version: u32,
    pub distributed_fts_statistics_format_version: u32,
}

impl ClusterProtocolCapabilities {
    pub const fn current() -> Self {
        Self {
            distribution_format_version: DISTRIBUTION_FORMAT_VERSION,
            distribution_hash_version: DISTRIBUTION_HASH_VERSION,
            cluster_data_protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
            metadata_consensus_format_version: CLUSTER_METADATA_CONSENSUS_FORMAT_VERSION,
            distributed_query_protocol_version: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
            distributed_fts_statistics_format_version: DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION,
        }
    }

    pub fn validate_rolling_upgrade_peer(&self, peer: &Self) -> Result<()> {
        if self.distribution_format_version != peer.distribution_format_version
            || self.distribution_hash_version != peer.distribution_hash_version
            || self.cluster_data_protocol_version != peer.cluster_data_protocol_version
            || self.metadata_consensus_format_version != peer.metadata_consensus_format_version
        {
            return Err(supervisor_error(format!(
                "rolling upgrade is unsafe: local distribution format/hash/data/metadata {}/{}/{}/{} differs from peer {}/{}/{}/{}",
                self.distribution_format_version,
                self.distribution_hash_version,
                self.cluster_data_protocol_version,
                self.metadata_consensus_format_version,
                peer.distribution_format_version,
                peer.distribution_hash_version,
                peer.cluster_data_protocol_version,
                peer.metadata_consensus_format_version,
            )));
        }
        if self
            .distributed_query_protocol_version
            .abs_diff(peer.distributed_query_protocol_version)
            > 1
            || self
                .distributed_fts_statistics_format_version
                .abs_diff(peer.distributed_fts_statistics_format_version)
                > 1
        {
            return Err(supervisor_error(
                "rolling upgrade supports at most one query/FTS protocol generation of skew",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterBackupMetadata {
    pub cluster_id: ClusterId,
    pub topology_generation: u64,
    pub distribution_format_version: u32,
    pub distribution_hash_version: u32,
    pub range_epochs: BTreeMap<RangeId, u64>,
    pub topology_sha256: String,
}

impl ClusterBackupMetadata {
    pub fn capture(topology: &ClusterTopology) -> Result<Self> {
        topology.validate()?;
        let topology_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(topology)?));
        Ok(Self {
            cluster_id: topology.cluster_id.clone(),
            topology_generation: topology.generation,
            distribution_format_version: topology.format_version,
            distribution_hash_version: topology.hash_version,
            range_epochs: topology
                .ranges
                .values()
                .map(|range| (range.id, range.epoch))
                .collect(),
            topology_sha256,
        })
    }

    pub fn validate_restored_topology(&self, topology: &ClusterTopology) -> Result<()> {
        let restored = Self::capture(topology)?;
        if &restored != self {
            return Err(supervisor_error(
                "restored cluster topology does not match backup metadata",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::distribution::{
        ClusterNode, DistributionConfig, PlacementPolicy, StandardFailureDomain,
    };

    fn config() -> DistributionConfig {
        DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("cluster-a").unwrap(),
            node_id: ClusterNodeId::new("n1").unwrap(),
            node_address: "10.0.0.1:9444".to_string(),
            node_tls_certificate_sha256: None,
            node_incarnation: 1,
            node_capacity_bytes: 10_000,
            replication_factor: 3,
            initial_ranges: 4,
            default_placement: PlacementPolicy::default(),
            suspect_after_ms: 1_000,
            dead_after_ms: 2_000,
            topology_history_limit: 128,
            metadata_election_timeout_ms: 1_500,
            metadata_heartbeat_interval_ms: 300,
            transport: Default::default(),
        }
    }

    #[derive(Debug)]
    struct RecordingPlanner {
        calls: Arc<AtomicUsize>,
    }

    impl ClusterPlacementPlanner for RecordingPlanner {
        fn plan_failure_repair(
            &self,
            topology: &ClusterTopology,
            config: &DistributionConfig,
            options: &RebalanceOptions,
            now_ms: u64,
        ) -> Result<FailureRepairPlan> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            build_failure_repair_plan(topology, config, options, now_ms)
        }
    }

    #[derive(Default)]
    struct AdvancingTransport {
        prepares: usize,
        snapshot_steps: usize,
        catch_up_steps: usize,
        cleanups: usize,
    }

    impl ClusterRelocationTransport for AdvancingTransport {
        fn prepare_learner(
            &mut self,
            _relocation: &RangeRelocation,
            _range: &RangeDescriptor,
            cancellation: &CancellationToken,
        ) -> Result<()> {
            cancellation.check()?;
            self.prepares += 1;
            Ok(())
        }

        fn copy_snapshot_step(
            &mut self,
            relocation: &RangeRelocation,
            _range: &RangeDescriptor,
            _options: &RangeSnapshotOptions,
            cancellation: &CancellationToken,
        ) -> Result<SnapshotCopyProgress> {
            cancellation.check()?;
            self.snapshot_steps += 1;
            if relocation.snapshot_bytes_copied == 0 {
                Ok(SnapshotCopyProgress {
                    bytes_copied: 128,
                    resume_after_key: Some("documents\u{0}doc-100".to_string()),
                    completed: false,
                    snapshot_id: None,
                    snapshot_sha256: None,
                    snapshot_commit_sequence: None,
                })
            } else {
                Ok(SnapshotCopyProgress {
                    bytes_copied: 256,
                    resume_after_key: None,
                    completed: true,
                    snapshot_id: Some(format!("snapshot-{}", relocation.id.get())),
                    snapshot_sha256: Some("ab".repeat(32)),
                    snapshot_commit_sequence: Some(0),
                })
            }
        }

        fn catch_up_step(
            &mut self,
            _relocation: &RangeRelocation,
            _range: &RangeDescriptor,
            max_commit_frames: usize,
            cancellation: &CancellationToken,
        ) -> Result<CatchUpProgress> {
            cancellation.check()?;
            assert!(max_commit_frames > 0);
            self.catch_up_steps += 1;
            Ok(CatchUpProgress {
                destination_durable_commit_sequence: 0,
                source_commit_sequence: 0,
            })
        }

        fn cleanup_source(
            &mut self,
            _relocation: &RangeRelocation,
            _range: &RangeDescriptor,
            _resume_after_key: Option<&str>,
            max_records: usize,
            cancellation: &CancellationToken,
        ) -> Result<CleanupProgress> {
            cancellation.check()?;
            assert!(max_records > 0);
            self.cleanups += 1;
            Ok(CleanupProgress {
                resume_after_key: None,
                records_deleted: 0,
                completed: true,
            })
        }
    }

    #[test]
    fn supervisor_rebalances_new_node_and_exports_bounded_metrics() {
        let directory = tempfile::tempdir().unwrap();
        let config = config();
        let actor = config.node_id.clone();
        let mut store =
            DistributionStore::initialize_at(directory.path(), config, false, 10).unwrap();
        for (number, at_ms) in [(2, 20), (3, 21), (4, 22)] {
            let node_id = ClusterNodeId::new(format!("n{number}")).unwrap();
            let node = ClusterNode::new(node_id, format!("10.0.0.{number}:9444"), 1, 10_000, at_ms)
                .unwrap()
                .with_label("server", format!("n{number}"))
                .unwrap();
            store.join_node(node, &actor, at_ms).unwrap();
        }
        let supervisor = ClusterSupervisor::with_planner(
            ClusterSupervisorConfig {
                refresh_topology_each_tick: false,
                automatically_rebalance: true,
                rebalance: RebalanceOptions {
                    max_replica_moves: 1,
                    max_moves_per_node: 1,
                    max_bytes_in_flight: 1_000_000,
                    unknown_range_bytes: 1,
                    ..RebalanceOptions::default()
                },
                ..ClusterSupervisorConfig::default()
            },
            Box::new(RecordingPlanner {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        )
        .unwrap();
        let cancellation = CancellationToken::uncancelable();
        let mut driver = TransportClusterRelocationDriver::new(
            AdvancingTransport::default(),
            ClusterRelocationTransportConfig::default(),
        )
        .unwrap();
        let first = supervisor
            .tick(&mut store, &mut driver, 30, &cancellation)
            .unwrap();
        assert_eq!(first.relocations_started.len(), 1);
        for now_ms in 31..50 {
            supervisor
                .tick(&mut store, &mut driver, now_ms, &cancellation)
                .unwrap();
        }
        assert!(store.topology().ranges.values().any(|range| {
            range
                .replicas
                .iter()
                .any(|replica| replica.node_id.as_str() == "n4")
        }));
        let repair = build_failure_repair_plan(
            store.topology(),
            store.config(),
            &RebalanceOptions::default(),
            50,
        )
        .unwrap();
        let metrics = ClusterOperationalMetrics::from_topology(store.topology(), &repair, 50);
        assert_eq!(metrics.ranges_total, 4);
        assert!(metrics.replicas_total >= 4);
        assert!(metrics
            .to_prometheus()
            .contains("bicdb_cluster_ranges_total 4"));
        assert!(driver.transport().prepares > 0);
        assert!(driver.transport().snapshot_steps > 0);
        assert!(driver.transport().catch_up_steps > 0);
        assert!(driver.transport().cleanups > 0);
    }

    #[test]
    fn supervisor_uses_an_injected_placement_policy() {
        let directory = tempfile::tempdir().unwrap();
        let config = config();
        let mut store =
            DistributionStore::initialize_at(directory.path(), config, false, 10).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let supervisor = ClusterSupervisor::with_planner(
            ClusterSupervisorConfig {
                heartbeat_each_tick: false,
                refresh_topology_each_tick: false,
                automatically_rebalance: true,
                ..ClusterSupervisorConfig::default()
            },
            Box::new(RecordingPlanner {
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
        let mut driver = DeferredClusterRelocationDriver;

        supervisor
            .tick(
                &mut store,
                &mut driver,
                20,
                &CancellationToken::uncancelable(),
            )
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn config_backup_upgrade_and_standard_failure_domains_are_fenced() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config();
        config.default_placement = PlacementPolicy::default().with_standard_failure_domains([
            StandardFailureDomain::Server,
            StandardFailureDomain::Rack,
            StandardFailureDomain::Zone,
            StandardFailureDomain::Region,
        ]);
        let store =
            DistributionStore::initialize_at(directory.path(), config.clone(), false, 10).unwrap();
        assert_eq!(
            crate::distribution::load_distribution_config(directory.path()).unwrap(),
            config
        );

        let metadata = ClusterBackupMetadata::capture(store.topology()).unwrap();
        metadata
            .validate_restored_topology(store.topology())
            .unwrap();
        let mut incompatible = store.topology().clone();
        incompatible.generation += 1;
        assert!(metadata.validate_restored_topology(&incompatible).is_err());

        let current = ClusterProtocolCapabilities::current();
        current.validate_rolling_upgrade_peer(&current).unwrap();
        let mut peer = current.clone();
        peer.distribution_hash_version += 1;
        assert!(current.validate_rolling_upgrade_peer(&peer).is_err());
        assert_eq!(
            config.default_placement.distinct_failure_domains,
            vec!["rack", "region", "server", "zone"]
        );
    }

    #[test]
    fn metrics_do_not_create_user_controlled_labels() {
        let metrics = ClusterOperationalMetrics {
            ranges_total: 9,
            ..ClusterOperationalMetrics::default()
        };
        let prometheus = metrics.to_prometheus();
        assert_eq!(
            prometheus
                .lines()
                .filter(|line| line.starts_with("bicdb_cluster_"))
                .count(),
            17
        );
        assert!(!prometheus.contains('{'));
        let _: BTreeMap<String, String> = BTreeMap::new();
    }
}
