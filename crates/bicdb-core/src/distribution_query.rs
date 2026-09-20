//! Bounded distributed-query planning, execution, merging, and transaction
//! protocol boundaries.
//!
//! Network transports deliberately sit behind traits. The coordinator owns
//! the invariants that must be identical for pgwire, internal RPC, and
//! embedded deployments: topology/epoch targets, bounded fan-out, bounded
//! memory, cancellation, deterministic merge behavior, and no implicit
//! cross-range writes.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::sync::{mpsc, Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::distribution::{
    distribution_key_token, ClusterId, ClusterNodeId, ClusterNodeLifecycle, ClusterTopology,
    RangeDescriptor, RangeId,
};
use crate::error::{BicDbError, Result};
use crate::fts_format::{FullTextCollectionStatistics, FullTextTermStatistics};
use crate::fts_scoring::bm25_inverse_document_frequency;
use crate::{CancellationToken, ResourceDemand, ResourceGovernor, ResourceLane};

pub const DISTRIBUTED_QUERY_PROTOCOL_VERSION: u32 = 1;
pub const DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION: u32 = 1;

fn query_error(message: impl Into<String>) -> BicDbError {
    BicDbError::Cluster(message.into())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedQueryLimits {
    pub max_shards: usize,
    pub max_concurrency: usize,
    pub max_rows_per_shard: usize,
    pub max_bytes_per_shard: u64,
    pub max_total_rows: usize,
    pub max_total_bytes: u64,
}

impl Default for DistributedQueryLimits {
    fn default() -> Self {
        Self {
            max_shards: 1_024,
            max_concurrency: 16,
            max_rows_per_shard: 10_000,
            max_bytes_per_shard: 16 * 1024 * 1024,
            max_total_rows: 100_000,
            max_total_bytes: 128 * 1024 * 1024,
        }
    }
}

impl DistributedQueryLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_shards == 0 || self.max_shards > 65_536 {
            return Err(query_error(
                "distributed query max_shards must be between 1 and 65536",
            ));
        }
        if self.max_concurrency == 0 || self.max_concurrency > 1_024 {
            return Err(query_error(
                "distributed query max_concurrency must be between 1 and 1024",
            ));
        }
        if self.max_rows_per_shard == 0
            || self.max_bytes_per_shard == 0
            || self.max_total_rows == 0
            || self.max_total_bytes == 0
        {
            return Err(query_error(
                "distributed query row and byte limits must be greater than zero",
            ));
        }
        if self.max_rows_per_shard > self.max_total_rows {
            return Err(query_error(
                "distributed query per-shard row limit exceeds total row limit",
            ));
        }
        if self.max_bytes_per_shard > self.max_total_bytes {
            return Err(query_error(
                "distributed query per-shard byte limit exceeds total byte limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "policy", rename_all = "snake_case")]
pub enum DistributedQueryFailurePolicy {
    FailFast,
    AllowPartial { max_failed_shards: usize },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum DistributedQueryScope {
    Point {
        namespace: String,
        key: String,
        token: u64,
    },
    Scatter,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedRangeTarget {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub start_token: u64,
    pub end_token: Option<u64>,
    pub leader_node: ClusterNodeId,
    pub leader_address: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedQueryPlan {
    pub protocol_version: u32,
    pub cluster_id: ClusterId,
    pub hash_version: u32,
    pub topology_generation: u64,
    pub scope: DistributedQueryScope,
    pub targets: Vec<DistributedRangeTarget>,
}

impl DistributedQueryPlan {
    pub fn is_single_range(&self) -> bool {
        self.targets.len() == 1
    }
}

fn target_for_range(
    topology: &ClusterTopology,
    range: &RangeDescriptor,
) -> Result<DistributedRangeTarget> {
    let leader = topology.nodes.get(&range.leader).ok_or_else(|| {
        query_error(format!(
            "range {} references unknown leader {}",
            range.id, range.leader
        ))
    })?;
    if leader.lifecycle == ClusterNodeLifecycle::Decommissioned {
        return Err(query_error(format!(
            "range {} leader {} is decommissioned",
            range.id, range.leader
        )));
    }
    Ok(DistributedRangeTarget {
        range_id: range.id,
        range_epoch: range.epoch,
        start_token: range.start_token,
        end_token: range.end_token,
        leader_node: range.leader.clone(),
        leader_address: leader.address.clone(),
    })
}

/// Plan a shard-key query without fan-out.
pub fn plan_point_query(
    topology: &ClusterTopology,
    namespace: &str,
    key: &str,
) -> Result<DistributedQueryPlan> {
    topology.validate()?;
    if namespace.trim().is_empty() || key.is_empty() {
        return Err(query_error(
            "distributed point query namespace and key must not be empty",
        ));
    }
    let token = distribution_key_token(namespace, key);
    let range = topology.range_for_token(token)?;
    Ok(DistributedQueryPlan {
        protocol_version: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
        cluster_id: topology.cluster_id.clone(),
        hash_version: topology.hash_version,
        topology_generation: topology.generation,
        scope: DistributedQueryScope::Point {
            namespace: namespace.to_string(),
            key: key.to_string(),
            token,
        },
        targets: vec![target_for_range(topology, range)?],
    })
}

/// Plan an all-range query and reject fan-out before dispatch when its
/// configured shard bound would be exceeded.
pub fn plan_scatter_query(
    topology: &ClusterTopology,
    limits: &DistributedQueryLimits,
) -> Result<DistributedQueryPlan> {
    topology.validate()?;
    limits.validate()?;
    if topology.ranges.len() > limits.max_shards {
        return Err(query_error(format!(
            "distributed query targets {} shards, exceeding configured maximum {}",
            topology.ranges.len(),
            limits.max_shards
        )));
    }
    let targets = topology
        .ranges
        .values()
        .map(|range| target_for_range(topology, range))
        .collect::<Result<Vec<_>>>()?;
    Ok(DistributedQueryPlan {
        protocol_version: DISTRIBUTED_QUERY_PROTOCOL_VERSION,
        cluster_id: topology.cluster_id.clone(),
        hash_version: topology.hash_version,
        topology_generation: topology.generation,
        scope: DistributedQueryScope::Scatter,
        targets,
    })
}

#[derive(Clone, Debug)]
pub struct ShardQueryOutput<Row> {
    pub rows: Vec<Row>,
    /// Encoded result bytes, measured by the shard before transport framing.
    pub encoded_bytes: u64,
}

pub trait DistributedShardExecutor<Query, Row>: Sync {
    fn execute_shard(
        &self,
        target: &DistributedRangeTarget,
        query: &Query,
        cancellation: &CancellationToken,
    ) -> Result<ShardQueryOutput<Row>>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedShardFailure {
    pub range_id: RangeId,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct DistributedShardRows<Row> {
    pub range_id: RangeId,
    pub rows: Vec<Row>,
    pub encoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct DistributedScatterResult<Row> {
    pub shards: Vec<DistributedShardRows<Row>>,
    pub failures: Vec<DistributedShardFailure>,
    pub total_rows: usize,
    pub total_bytes: u64,
    pub partial: bool,
}

/// Execute a bounded worker pool. The result channel is also bounded, so a
/// fast producer cannot accumulate one unbounded response queue in front of
/// the merger.
pub fn execute_scatter_gather<Query, Row, Executor>(
    plan: &DistributedQueryPlan,
    query: &Query,
    executor: &Executor,
    limits: &DistributedQueryLimits,
    failure_policy: DistributedQueryFailurePolicy,
    cancellation: &CancellationToken,
) -> Result<DistributedScatterResult<Row>>
where
    Query: Sync,
    Row: Send,
    Executor: DistributedShardExecutor<Query, Row>,
{
    limits.validate()?;
    if plan.protocol_version != DISTRIBUTED_QUERY_PROTOCOL_VERSION {
        return Err(query_error(format!(
            "unsupported distributed query protocol version {}",
            plan.protocol_version
        )));
    }
    if plan.targets.is_empty() {
        return Err(query_error("distributed query plan has no targets"));
    }
    if plan.targets.len() > limits.max_shards {
        return Err(query_error(format!(
            "distributed query plan has {} targets, exceeding configured maximum {}",
            plan.targets.len(),
            limits.max_shards
        )));
    }
    cancellation.check()?;

    let worker_count = limits.max_concurrency.min(plan.targets.len());
    let tasks = Arc::new(Mutex::new(VecDeque::from(plan.targets.clone())));
    let execution_cancellation = cancellation.child();
    let mut shard_rows = Vec::with_capacity(plan.targets.len());
    let mut failures = Vec::new();
    let mut total_rows = 0_usize;
    let mut total_bytes = 0_u64;
    let mut fatal_error = None;

    std::thread::scope(|scope| {
        let (sender, receiver) =
            mpsc::sync_channel::<(RangeId, Result<ShardQueryOutput<Row>>)>(worker_count);
        for _ in 0..worker_count {
            let sender = sender.clone();
            let tasks = Arc::clone(&tasks);
            let worker_cancellation = execution_cancellation.clone();
            scope.spawn(move || loop {
                if worker_cancellation.check().is_err() {
                    break;
                }
                let target = match tasks.lock() {
                    Ok(mut tasks) => tasks.pop_front(),
                    Err(_) => {
                        let _ = sender.send((
                            plan.targets[0].range_id,
                            Err(query_error("distributed query task lock is poisoned")),
                        ));
                        worker_cancellation.cancel();
                        break;
                    }
                };
                let Some(target) = target else {
                    break;
                };
                let result = executor.execute_shard(&target, query, &worker_cancellation);
                if sender.send((target.range_id, result)).is_err() {
                    break;
                }
            });
        }
        drop(sender);

        while let Ok((range_id, result)) = receiver.recv() {
            match result {
                Ok(output) => {
                    if output.rows.len() > limits.max_rows_per_shard {
                        fatal_error = Some(query_error(format!(
                            "range {range_id} returned {} rows, exceeding per-shard maximum {}",
                            output.rows.len(),
                            limits.max_rows_per_shard
                        )));
                        execution_cancellation.cancel();
                        continue;
                    }
                    if output.encoded_bytes > limits.max_bytes_per_shard {
                        fatal_error = Some(query_error(format!(
                            "range {range_id} returned {} bytes, exceeding per-shard maximum {}",
                            output.encoded_bytes, limits.max_bytes_per_shard
                        )));
                        execution_cancellation.cancel();
                        continue;
                    }
                    total_rows = match total_rows.checked_add(output.rows.len()) {
                        Some(total) => total,
                        None => {
                            fatal_error =
                                Some(query_error("distributed query row count overflowed"));
                            execution_cancellation.cancel();
                            continue;
                        }
                    };
                    total_bytes = match total_bytes.checked_add(output.encoded_bytes) {
                        Some(total) => total,
                        None => {
                            fatal_error =
                                Some(query_error("distributed query byte count overflowed"));
                            execution_cancellation.cancel();
                            continue;
                        }
                    };
                    if total_rows > limits.max_total_rows || total_bytes > limits.max_total_bytes {
                        fatal_error = Some(query_error(format!(
                            "distributed query exceeded total result limit: rows={total_rows}/{} bytes={total_bytes}/{}",
                            limits.max_total_rows, limits.max_total_bytes
                        )));
                        execution_cancellation.cancel();
                        continue;
                    }
                    shard_rows.push(DistributedShardRows {
                        range_id,
                        rows: output.rows,
                        encoded_bytes: output.encoded_bytes,
                    });
                }
                Err(error) => {
                    failures.push(DistributedShardFailure {
                        range_id,
                        message: error.to_string(),
                    });
                    match failure_policy {
                        DistributedQueryFailurePolicy::FailFast => {
                            execution_cancellation.cancel();
                        }
                        DistributedQueryFailurePolicy::AllowPartial { max_failed_shards }
                            if failures.len() > max_failed_shards =>
                        {
                            execution_cancellation.cancel();
                        }
                        DistributedQueryFailurePolicy::AllowPartial { .. } => {}
                    }
                }
            }
        }
    });

    cancellation.check()?;
    if let Some(error) = fatal_error {
        return Err(error);
    }
    if !failures.is_empty() {
        match failure_policy {
            DistributedQueryFailurePolicy::FailFast => {
                return Err(query_error(format!(
                    "distributed query failed on range {}: {}",
                    failures[0].range_id, failures[0].message
                )));
            }
            DistributedQueryFailurePolicy::AllowPartial { max_failed_shards }
                if failures.len() > max_failed_shards =>
            {
                return Err(query_error(format!(
                    "distributed query failed on {} shards, exceeding allowed maximum {}",
                    failures.len(),
                    max_failed_shards
                )));
            }
            DistributedQueryFailurePolicy::AllowPartial { .. } => {}
        }
    }
    shard_rows.sort_by_key(|shard| shard.range_id);
    failures.sort_by_key(|failure| failure.range_id);
    Ok(DistributedScatterResult {
        partial: !failures.is_empty(),
        shards: shard_rows,
        failures,
        total_rows,
        total_bytes,
    })
}

/// Resource-admitted scatter/gather. The declared envelope must cover the
/// configured bounded result set, concurrent shard responses, worker slots,
/// and the maximum rate-charged response bytes before any shard is contacted.
pub fn execute_scatter_gather_governed<Query, Row, Executor>(
    plan: &DistributedQueryPlan,
    query: &Query,
    executor: &Executor,
    limits: &DistributedQueryLimits,
    failure_policy: DistributedQueryFailurePolicy,
    cancellation: &CancellationToken,
    governor: &ResourceGovernor,
    demand: ResourceDemand,
    now_ms: u64,
) -> Result<DistributedScatterResult<Row>>
where
    Query: Sync,
    Row: Send,
    Executor: DistributedShardExecutor<Query, Row>,
{
    limits.validate()?;
    let worker_count = limits.max_concurrency.min(plan.targets.len());
    let concurrent_response_bytes = u64::try_from(worker_count)
        .ok()
        .and_then(|workers| workers.checked_mul(limits.max_bytes_per_shard))
        .unwrap_or(u64::MAX)
        .min(limits.max_total_bytes);
    let coordinator_overhead = u64::try_from(limits.max_shards)
        .ok()
        .and_then(|shards| shards.checked_mul(4 * 1024))
        .unwrap_or(u64::MAX);
    let required_memory = limits
        .max_total_bytes
        .checked_add(coordinator_overhead)
        .unwrap_or(u64::MAX);
    if demand.memory_bytes < required_memory
        || demand.io_bytes < concurrent_response_bytes
        || demand.cpu_slots < worker_count
        || demand.io_charge_bytes < limits.max_total_bytes
    {
        return Err(BicDbError::ResourceGovernance(format!(
            "distributed query demand must cover {required_memory} result/metadata bytes, {concurrent_response_bytes} concurrent I/O bytes, {worker_count} worker slots, and {} rate bytes",
            limits.max_total_bytes,
        )));
    }
    let permit = governor.try_admit(ResourceLane::ForegroundRead, demand, now_ms)?;
    let result =
        execute_scatter_gather(plan, query, executor, limits, failure_policy, cancellation);
    drop(permit);
    result
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributedSortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedOrderedRow<Key, Row> {
    pub sort_key: Key,
    pub range_id: RangeId,
    pub row: Row,
}

struct OrderedMergeHead<Key, Row> {
    key: Key,
    range_id: RangeId,
    shard: usize,
    ordinal: u64,
    direction: DistributedSortDirection,
    row: Row,
}

impl<Key: Ord, Row> PartialEq for OrderedMergeHead<Key, Row> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.range_id == other.range_id
            && self.shard == other.shard
            && self.ordinal == other.ordinal
    }
}

impl<Key: Ord, Row> Eq for OrderedMergeHead<Key, Row> {}

impl<Key: Ord, Row> PartialOrd for OrderedMergeHead<Key, Row> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Key: Ord, Row> Ord for OrderedMergeHead<Key, Row> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = match self.direction {
            DistributedSortDirection::Ascending => other.key.cmp(&self.key),
            DistributedSortDirection::Descending => self.key.cmp(&other.key),
        };
        ordering
            .then_with(|| other.range_id.cmp(&self.range_id))
            .then_with(|| other.ordinal.cmp(&self.ordinal))
            .then_with(|| other.shard.cmp(&self.shard))
    }
}

/// K-way merge over already-sorted shard iterators. Memory is O(shards + K);
/// rows beyond K are never materialized by the coordinator.
pub fn merge_ordered_top_k<Key, Row, Shard>(
    shards: Vec<(RangeId, Shard)>,
    limit: usize,
    direction: DistributedSortDirection,
    cancellation: &CancellationToken,
) -> Result<Vec<DistributedOrderedRow<Key, Row>>>
where
    Key: Ord,
    Shard: IntoIterator<Item = (Key, Row)>,
{
    if limit == 0 || shards.is_empty() {
        return Ok(Vec::new());
    }
    cancellation.check()?;
    let mut iterators = shards
        .into_iter()
        .map(|(range_id, shard)| (range_id, shard.into_iter()))
        .collect::<Vec<_>>();
    let mut ordinals = vec![0_u64; iterators.len()];
    let mut heap = BinaryHeap::with_capacity(iterators.len());
    for (shard, (range_id, iterator)) in iterators.iter_mut().enumerate() {
        if let Some((key, row)) = iterator.next() {
            heap.push(OrderedMergeHead {
                key,
                range_id: *range_id,
                shard,
                ordinal: 0,
                direction,
                row,
            });
        }
    }
    let mut merged = Vec::with_capacity(limit);
    while merged.len() < limit {
        cancellation.check()?;
        let Some(head) = heap.pop() else {
            break;
        };
        let shard = head.shard;
        merged.push(DistributedOrderedRow {
            sort_key: head.key,
            range_id: head.range_id,
            row: head.row,
        });
        if let Some((key, row)) = iterators[shard].1.next() {
            ordinals[shard] = ordinals[shard].saturating_add(1);
            heap.push(OrderedMergeHead {
                key,
                range_id: iterators[shard].0,
                shard,
                ordinal: ordinals[shard],
                direction,
                row,
            });
        }
    }
    Ok(merged)
}

#[derive(Clone, Debug, PartialEq)]
pub struct DistributedFtsHit<Row> {
    pub score: f32,
    pub range_id: RangeId,
    pub document_id: u64,
    pub topology_generation: u64,
    pub statistics_snapshot_version: u64,
    pub row: Row,
}

struct FtsMergeHead<Row> {
    hit: DistributedFtsHit<Row>,
    shard: usize,
}

impl<Row> PartialEq for FtsMergeHead<Row> {
    fn eq(&self, other: &Self) -> bool {
        self.hit.score.to_bits() == other.hit.score.to_bits()
            && self.hit.range_id == other.hit.range_id
            && self.hit.document_id == other.hit.document_id
            && self.shard == other.shard
    }
}

impl<Row> Eq for FtsMergeHead<Row> {}

impl<Row> PartialOrd for FtsMergeHead<Row> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<Row> Ord for FtsMergeHead<Row> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hit
            .score
            .total_cmp(&other.hit.score)
            .then_with(|| other.hit.range_id.cmp(&self.hit.range_id))
            .then_with(|| other.hit.document_id.cmp(&self.hit.document_id))
            .then_with(|| other.shard.cmp(&self.shard))
    }
}

/// Merge per-range descending FTS streams using scores computed from the same
/// global statistics snapshot.
pub fn merge_fts_top_k<Row, Shard>(
    shards: Vec<Shard>,
    limit: usize,
    statistics: &GlobalFtsStatisticsSnapshot,
    cancellation: &CancellationToken,
) -> Result<Vec<DistributedFtsHit<Row>>>
where
    Shard: IntoIterator<Item = DistributedFtsHit<Row>>,
{
    if limit == 0 || shards.is_empty() {
        return Ok(Vec::new());
    }
    cancellation.check()?;
    let mut iterators = shards
        .into_iter()
        .map(IntoIterator::into_iter)
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(iterators.len());
    for (shard, iterator) in iterators.iter_mut().enumerate() {
        if let Some(hit) = iterator.next() {
            validate_fts_hit_statistics(&hit, statistics)?;
            heap.push(FtsMergeHead { hit, shard });
        }
    }
    let mut merged = Vec::with_capacity(limit);
    while merged.len() < limit {
        cancellation.check()?;
        let Some(head) = heap.pop() else {
            break;
        };
        let shard = head.shard;
        merged.push(head.hit);
        if let Some(hit) = iterators[shard].next() {
            validate_fts_hit_statistics(&hit, statistics)?;
            heap.push(FtsMergeHead { hit, shard });
        }
    }
    Ok(merged)
}

fn validate_fts_hit_statistics<Row>(
    hit: &DistributedFtsHit<Row>,
    statistics: &GlobalFtsStatisticsSnapshot,
) -> Result<()> {
    if hit.topology_generation != statistics.topology_generation
        || hit.statistics_snapshot_version != statistics.snapshot_version
    {
        return Err(query_error(format!(
            "range {} FTS hit uses topology/statistics {}/{}, expected {}/{}",
            hit.range_id,
            hit.topology_generation,
            hit.statistics_snapshot_version,
            statistics.topology_generation,
            statistics.snapshot_version
        )));
    }
    if !hit.score.is_finite() {
        return Err(query_error(format!(
            "range {} returned a non-finite FTS score",
            hit.range_id
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DistributedNumericAggregate {
    pub count: u64,
    pub sum: f64,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
}

impl DistributedNumericAggregate {
    pub fn observe(&mut self, value: f64) -> Result<()> {
        if !value.is_finite() {
            return Err(query_error(
                "distributed numeric aggregate requires finite inputs",
            ));
        }
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| query_error("distributed aggregate count overflowed"))?;
        self.sum += value;
        if !self.sum.is_finite() {
            return Err(query_error("distributed aggregate sum overflowed"));
        }
        self.minimum = Some(
            self.minimum
                .map(|current| current.min(value))
                .unwrap_or(value),
        );
        self.maximum = Some(
            self.maximum
                .map(|current| current.max(value))
                .unwrap_or(value),
        );
        Ok(())
    }

    pub fn merge(&mut self, partial: Self) -> Result<()> {
        if !partial.sum.is_finite()
            || partial.minimum.is_some_and(|value| !value.is_finite())
            || partial.maximum.is_some_and(|value| !value.is_finite())
        {
            return Err(query_error(
                "distributed numeric aggregate contains a non-finite partial",
            ));
        }
        self.count = self
            .count
            .checked_add(partial.count)
            .ok_or_else(|| query_error("distributed aggregate count overflowed"))?;
        self.sum += partial.sum;
        if !self.sum.is_finite() {
            return Err(query_error("distributed aggregate sum overflowed"));
        }
        if let Some(value) = partial.minimum {
            self.minimum = Some(
                self.minimum
                    .map(|current| current.min(value))
                    .unwrap_or(value),
            );
        }
        if let Some(value) = partial.maximum {
            self.maximum = Some(
                self.maximum
                    .map(|current| current.max(value))
                    .unwrap_or(value),
            );
        }
        Ok(())
    }

    pub fn average(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }
}

pub fn merge_numeric_aggregates(
    partials: impl IntoIterator<Item = DistributedNumericAggregate>,
) -> Result<DistributedNumericAggregate> {
    let mut merged = DistributedNumericAggregate::default();
    for partial in partials {
        merged.merge(partial)?;
    }
    Ok(merged)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShardFtsStatisticsSnapshot {
    pub topology_generation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub collection: String,
    pub collection_statistics: FullTextCollectionStatistics,
    /// Statistics for every normalized query term. Absence means zero
    /// frequency on this range, not "unknown".
    pub terms: BTreeMap<String, FullTextTermStatistics>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GlobalFtsStatisticsPolicy {
    RequireExact,
    AllowVersionedApproximation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GlobalFtsStatisticsSnapshot {
    pub format_version: u32,
    pub snapshot_version: u64,
    pub topology_generation: u64,
    pub collection: String,
    pub exact: bool,
    pub covered_ranges: Vec<RangeId>,
    pub missing_ranges: Vec<RangeId>,
    pub document_count: u64,
    pub total_document_length: u64,
    pub average_document_length: f64,
    pub field_total_lengths: [u64; 4],
    pub terms: BTreeMap<String, FullTextTermStatistics>,
}

impl GlobalFtsStatisticsSnapshot {
    pub fn inverse_document_frequency(&self, term: &str) -> f32 {
        self.terms
            .get(term)
            .map(|statistics| {
                bm25_inverse_document_frequency(self.document_count, statistics.document_frequency)
            })
            .unwrap_or(0.0)
    }
}

pub fn merge_global_fts_statistics(
    topology: &ClusterTopology,
    collection: &str,
    snapshot_version: u64,
    shards: impl IntoIterator<Item = ShardFtsStatisticsSnapshot>,
    policy: GlobalFtsStatisticsPolicy,
) -> Result<GlobalFtsStatisticsSnapshot> {
    topology.validate()?;
    if collection.trim().is_empty() || snapshot_version == 0 {
        return Err(query_error(
            "global FTS collection and snapshot version must be set",
        ));
    }
    let expected_ranges = topology
        .ranges
        .values()
        .map(|range| (range.id, range.epoch))
        .collect::<BTreeMap<_, _>>();
    let mut covered_ranges = BTreeSet::new();
    let mut document_count = 0_u64;
    let mut total_document_length = 0_u64;
    let mut field_total_lengths = [0_u64; 4];
    let mut terms = BTreeMap::<String, FullTextTermStatistics>::new();
    for shard in shards {
        if shard.topology_generation != topology.generation {
            return Err(query_error(format!(
                "range {} FTS statistics use topology generation {}, expected {}",
                shard.range_id, shard.topology_generation, topology.generation
            )));
        }
        let expected_epoch = expected_ranges.get(&shard.range_id).ok_or_else(|| {
            query_error(format!(
                "FTS statistics reference unknown range {}",
                shard.range_id
            ))
        })?;
        if shard.range_epoch != *expected_epoch {
            return Err(query_error(format!(
                "range {} FTS statistics use epoch {}, expected {}",
                shard.range_id, shard.range_epoch, expected_epoch
            )));
        }
        if shard.collection != collection {
            return Err(query_error(format!(
                "range {} FTS statistics collection `{}` does not match `{collection}`",
                shard.range_id, shard.collection
            )));
        }
        if !covered_ranges.insert(shard.range_id) {
            return Err(query_error(format!(
                "duplicate FTS statistics for range {}",
                shard.range_id
            )));
        }
        document_count = document_count
            .checked_add(shard.collection_statistics.document_count)
            .ok_or_else(|| query_error("global FTS document count overflowed"))?;
        total_document_length = total_document_length
            .checked_add(shard.collection_statistics.total_document_length)
            .ok_or_else(|| query_error("global FTS document length overflowed"))?;
        for (total, shard_total) in field_total_lengths
            .iter_mut()
            .zip(shard.collection_statistics.field_total_lengths)
        {
            *total = total
                .checked_add(shard_total)
                .ok_or_else(|| query_error("global FTS field length overflowed"))?;
        }
        for (term, shard_term) in shard.terms {
            let global = terms.entry(term).or_default();
            global.document_frequency = global
                .document_frequency
                .checked_add(shard_term.document_frequency)
                .ok_or_else(|| query_error("global FTS document frequency overflowed"))?;
            global.collection_frequency = global
                .collection_frequency
                .checked_add(shard_term.collection_frequency)
                .ok_or_else(|| query_error("global FTS collection frequency overflowed"))?;
            global.posting_block_count = global
                .posting_block_count
                .checked_add(shard_term.posting_block_count)
                .ok_or_else(|| query_error("global FTS posting block count overflowed"))?;
            global.impact_block_count = global
                .impact_block_count
                .checked_add(shard_term.impact_block_count)
                .ok_or_else(|| query_error("global FTS impact block count overflowed"))?;
            global.posting_bytes = global
                .posting_bytes
                .checked_add(shard_term.posting_bytes)
                .ok_or_else(|| query_error("global FTS posting bytes overflowed"))?;
            global.impact_bytes = global
                .impact_bytes
                .checked_add(shard_term.impact_bytes)
                .ok_or_else(|| query_error("global FTS impact bytes overflowed"))?;
            global.maximum_contribution = global
                .maximum_contribution
                .max(shard_term.maximum_contribution);
        }
    }
    let missing_ranges = expected_ranges
        .keys()
        .filter(|range_id| !covered_ranges.contains(range_id))
        .copied()
        .collect::<Vec<_>>();
    if !missing_ranges.is_empty() && policy == GlobalFtsStatisticsPolicy::RequireExact {
        return Err(query_error(format!(
            "global FTS statistics are missing {} ranges",
            missing_ranges.len()
        )));
    }
    Ok(GlobalFtsStatisticsSnapshot {
        format_version: DISTRIBUTED_FTS_STATISTICS_FORMAT_VERSION,
        snapshot_version,
        topology_generation: topology.generation,
        collection: collection.to_string(),
        exact: missing_ranges.is_empty(),
        covered_ranges: covered_ranges.into_iter().collect(),
        missing_ranges,
        document_count,
        total_document_length,
        average_document_length: if document_count == 0 {
            0.0
        } else {
            total_document_length as f64 / document_count as f64
        },
        field_total_lengths,
        terms,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedWriteIntent<Write> {
    pub namespace: String,
    pub key: String,
    pub write: Write,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLocalWritePlan<Write> {
    pub topology_generation: u64,
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub leader_node: ClusterNodeId,
    pub writes: Vec<DistributedWriteIntent<Write>>,
}

/// Default SQL write planning is intentionally shard-local. All keys are
/// resolved before any write is dispatched, so a cross-range statement cannot
/// partially execute.
pub fn plan_shard_local_write<Write>(
    topology: &ClusterTopology,
    writes: Vec<DistributedWriteIntent<Write>>,
) -> Result<ShardLocalWritePlan<Write>> {
    topology.validate()?;
    if writes.is_empty() {
        return Err(query_error("distributed write contains no intents"));
    }
    let mut range = None;
    for write in &writes {
        if write.namespace.trim().is_empty() || write.key.is_empty() {
            return Err(query_error(
                "distributed write namespace and key must not be empty",
            ));
        }
        let owner = topology.range_for_key(&write.namespace, &write.key)?;
        if let Some(expected) = range {
            if expected != owner.id {
                return Err(query_error(format!(
                    "cross-range write rejected before execution: ranges {expected} and {} require an explicit distributed commit protocol",
                    owner.id
                )));
            }
        } else {
            range = Some(owner.id);
        }
    }
    let range_id = range.expect("non-empty writes resolve one range");
    let owner = topology
        .range_by_id(range_id)
        .expect("resolved range remains in validated topology");
    Ok(ShardLocalWritePlan {
        topology_generation: topology.generation,
        range_id,
        range_epoch: owner.epoch,
        leader_node: owner.leader.clone(),
        writes,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedTransactionParticipant<Write> {
    pub range_id: RangeId,
    pub range_epoch: u64,
    pub leader_node: ClusterNodeId,
    pub writes: Vec<DistributedWriteIntent<Write>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributedCommitDecision {
    Commit,
    Abort,
}

/// Separate protocol required for multi-range writes. Implementations must
/// durably record the decision before `commit_participant`; after a commit
/// decision, `recover` must keep retrying incomplete participants.
pub trait DistributedCommitProtocol<Write> {
    fn name(&self) -> &'static str;
    fn protocol_version(&self) -> u32;
    fn begin(
        &mut self,
        transaction_id: &str,
        participants: &[DistributedTransactionParticipant<Write>],
    ) -> Result<()>;
    fn prepare_participant(
        &mut self,
        transaction_id: &str,
        participant: &DistributedTransactionParticipant<Write>,
        cancellation: &CancellationToken,
    ) -> Result<()>;
    fn record_decision(
        &mut self,
        transaction_id: &str,
        decision: DistributedCommitDecision,
    ) -> Result<()>;
    fn commit_participant(
        &mut self,
        transaction_id: &str,
        participant: &DistributedTransactionParticipant<Write>,
    ) -> Result<()>;
    fn abort_participant(
        &mut self,
        transaction_id: &str,
        participant: &DistributedTransactionParticipant<Write>,
    ) -> Result<()>;
    fn finish(&mut self, transaction_id: &str) -> Result<()>;
}

fn group_transaction_participants<Write>(
    topology: &ClusterTopology,
    writes: Vec<DistributedWriteIntent<Write>>,
) -> Result<Vec<DistributedTransactionParticipant<Write>>> {
    topology.validate()?;
    if writes.is_empty() {
        return Err(query_error("distributed transaction contains no intents"));
    }
    let mut grouped = BTreeMap::<RangeId, Vec<DistributedWriteIntent<Write>>>::new();
    for write in writes {
        if write.namespace.trim().is_empty() || write.key.is_empty() {
            return Err(query_error(
                "distributed transaction namespace and key must not be empty",
            ));
        }
        let range_id = topology.range_for_key(&write.namespace, &write.key)?.id;
        grouped.entry(range_id).or_default().push(write);
    }
    grouped
        .into_iter()
        .map(|(range_id, writes)| {
            let range = topology
                .range_by_id(range_id)
                .expect("grouped range exists in validated topology");
            Ok(DistributedTransactionParticipant {
                range_id,
                range_epoch: range.epoch,
                leader_node: range.leader.clone(),
                writes,
            })
        })
        .collect()
}

/// Coordinate explicit two-phase commit through a separately supplied durable
/// protocol. The normal SQL path never calls this function implicitly.
pub fn execute_distributed_transaction<Write, Protocol>(
    topology: &ClusterTopology,
    transaction_id: &str,
    writes: Vec<DistributedWriteIntent<Write>>,
    protocol: &mut Protocol,
    cancellation: &CancellationToken,
) -> Result<()>
where
    Protocol: DistributedCommitProtocol<Write>,
{
    if transaction_id.trim().is_empty() || protocol.protocol_version() == 0 {
        return Err(query_error(
            "distributed transaction id and protocol version must be set",
        ));
    }
    let participants = group_transaction_participants(topology, writes)?;
    if participants.len() < 2 {
        return Err(query_error(
            "distributed commit protocol is reserved for multi-range writes; use shard-local execution",
        ));
    }
    cancellation.check()?;
    protocol.begin(transaction_id, &participants)?;
    let mut prepared = 0_usize;
    for participant in &participants {
        if let Err(error) = cancellation
            .check()
            .and_then(|_| protocol.prepare_participant(transaction_id, participant, cancellation))
        {
            protocol.record_decision(transaction_id, DistributedCommitDecision::Abort)?;
            for prepared_participant in participants[..prepared].iter().rev() {
                protocol.abort_participant(transaction_id, prepared_participant)?;
            }
            protocol.finish(transaction_id)?;
            return Err(query_error(format!(
                "distributed transaction `{transaction_id}` using {} aborted during prepare: {error}",
                protocol.name()
            )));
        }
        prepared += 1;
    }
    protocol.record_decision(transaction_id, DistributedCommitDecision::Commit)?;
    for participant in &participants {
        protocol
            .commit_participant(transaction_id, participant)
            .map_err(|error| {
                query_error(format!(
                    "distributed transaction `{transaction_id}` has a durable commit decision but participant {} needs protocol recovery: {error}",
                    participant.range_id
                ))
            })?;
    }
    protocol.finish(transaction_id)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use super::*;
    use crate::distribution::{ClusterNode, ClusterNodeId, DistributionConfig, DistributionStore};

    fn topology_with_ranges(range_count: u32) -> ClusterTopology {
        let directory = tempfile::tempdir().unwrap();
        let node_id = ClusterNodeId::new("n1").unwrap();
        let mut config = DistributionConfig::default();
        config.enabled = true;
        config.node_id = node_id;
        config.replication_factor = 1;
        config.initial_ranges = range_count;
        let store = DistributionStore::initialize_at(directory.path(), config, false, 1).unwrap();
        store.topology().clone()
    }

    #[test]
    fn point_plan_is_single_range_and_scatter_is_bounded() {
        let topology = topology_with_ranges(8);
        let point = plan_point_query(&topology, "documents", "doc-42").unwrap();
        assert!(point.is_single_range());
        let DistributedQueryScope::Point { token, .. } = point.scope else {
            panic!("point scope expected");
        };
        assert!(topology
            .range_by_id(point.targets[0].range_id)
            .unwrap()
            .contains_token(token));

        let mut limits = DistributedQueryLimits::default();
        limits.max_shards = 7;
        assert!(plan_scatter_query(&topology, &limits)
            .unwrap_err()
            .to_string()
            .contains("exceeding"));
        limits.max_shards = 8;
        assert_eq!(
            plan_scatter_query(&topology, &limits)
                .unwrap()
                .targets
                .len(),
            8
        );
    }

    struct CountingExecutor {
        active: AtomicUsize,
        peak: AtomicUsize,
        fail: Option<RangeId>,
    }

    impl DistributedShardExecutor<(), u64> for CountingExecutor {
        fn execute_shard(
            &self,
            target: &DistributedRangeTarget,
            _query: &(),
            cancellation: &CancellationToken,
        ) -> Result<ShardQueryOutput<u64>> {
            cancellation.check()?;
            let active = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            self.peak.fetch_max(active, AtomicOrdering::SeqCst);
            std::thread::yield_now();
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
            if self.fail == Some(target.range_id) {
                return Err(query_error("injected shard failure"));
            }
            Ok(ShardQueryOutput {
                rows: vec![target.range_id.get()],
                encoded_bytes: 8,
            })
        }
    }

    #[test]
    fn scatter_gather_enforces_concurrency_limits_and_partial_policy() {
        let topology = topology_with_ranges(16);
        let limits = DistributedQueryLimits {
            max_shards: 16,
            max_concurrency: 3,
            ..DistributedQueryLimits::default()
        };
        let plan = plan_scatter_query(&topology, &limits).unwrap();
        let failed_range = plan.targets[5].range_id;
        let executor = CountingExecutor {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            fail: Some(failed_range),
        };
        let result = execute_scatter_gather(
            &plan,
            &(),
            &executor,
            &limits,
            DistributedQueryFailurePolicy::AllowPartial {
                max_failed_shards: 1,
            },
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert!(executor.peak.load(AtomicOrdering::SeqCst) <= 3);
        assert!(result.partial);
        assert_eq!(result.failures[0].range_id, failed_range);
        assert_eq!(result.total_rows, 15);

        let executor = CountingExecutor {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            fail: Some(failed_range),
        };
        assert!(execute_scatter_gather(
            &plan,
            &(),
            &executor,
            &limits,
            DistributedQueryFailurePolicy::FailFast,
            &CancellationToken::uncancelable(),
        )
        .unwrap_err()
        .to_string()
        .contains("injected"));
    }

    #[test]
    fn governed_scatter_rejects_before_fanout_and_reclaims_foreground_permit() {
        let topology = topology_with_ranges(4);
        let limits = DistributedQueryLimits {
            max_shards: 4,
            max_concurrency: 1,
            max_rows_per_shard: 4,
            max_bytes_per_shard: 16,
            max_total_rows: 16,
            max_total_bytes: 64,
        };
        let plan = plan_scatter_query(&topology, &limits).unwrap();
        let executor = CountingExecutor {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            fail: None,
        };
        let demand = ResourceDemand {
            memory_bytes: 64 + 4 * 4_096,
            io_bytes: 16,
            cpu_slots: 1,
            io_charge_bytes: 64,
        };
        let mut config = crate::ResourceGovernorConfig::default();
        config
            .lanes
            .get_mut(&ResourceLane::ForegroundRead)
            .unwrap()
            .max_active = 1;
        let governor = ResourceGovernor::new(config, 100).unwrap();
        let held = governor
            .try_admit(ResourceLane::ForegroundRead, demand, 100)
            .unwrap();
        assert!(matches!(
            execute_scatter_gather_governed(
                &plan,
                &(),
                &executor,
                &limits,
                DistributedQueryFailurePolicy::FailFast,
                &CancellationToken::uncancelable(),
                &governor,
                demand,
                100,
            ),
            Err(BicDbError::ResourceGovernance(_))
        ));
        assert_eq!(executor.peak.load(AtomicOrdering::SeqCst), 0);
        drop(held);

        let result = execute_scatter_gather_governed(
            &plan,
            &(),
            &executor,
            &limits,
            DistributedQueryFailurePolicy::FailFast,
            &CancellationToken::uncancelable(),
            &governor,
            demand,
            100,
        )
        .unwrap();
        assert_eq!(result.total_rows, 4);
        assert_eq!(governor.snapshot().total.active, 0);

        let underdeclared = ResourceDemand {
            memory_bytes: demand.memory_bytes - 1,
            ..demand
        };
        assert!(execute_scatter_gather_governed(
            &plan,
            &(),
            &executor,
            &limits,
            DistributedQueryFailurePolicy::FailFast,
            &CancellationToken::uncancelable(),
            &governor,
            underdeclared,
            100,
        )
        .unwrap_err()
        .to_string()
        .contains("must cover"));
    }

    struct SlowExecutor;

    impl DistributedShardExecutor<(), ()> for SlowExecutor {
        fn execute_shard(
            &self,
            _target: &DistributedRangeTarget,
            _query: &(),
            cancellation: &CancellationToken,
        ) -> Result<ShardQueryOutput<()>> {
            std::thread::sleep(std::time::Duration::from_millis(15));
            cancellation.check()?;
            Ok(ShardQueryOutput {
                rows: vec![()],
                encoded_bytes: 1,
            })
        }
    }

    #[test]
    fn scatter_gather_propagates_deadlines_to_slow_shards() {
        let topology = topology_with_ranges(2);
        let limits = DistributedQueryLimits {
            max_shards: 2,
            max_concurrency: 2,
            ..DistributedQueryLimits::default()
        };
        let plan = plan_scatter_query(&topology, &limits).unwrap();
        let cancellation = CancellationToken::new(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some(std::time::Instant::now() + std::time::Duration::from_millis(2)),
        );
        assert!(matches!(
            execute_scatter_gather(
                &plan,
                &(),
                &SlowExecutor,
                &limits,
                DistributedQueryFailurePolicy::FailFast,
                &cancellation,
            ),
            Err(BicDbError::QueryTimedOut)
        ));
    }

    #[test]
    fn ordered_top_k_and_aggregates_match_single_node_oracle() {
        let ranges = [RangeId::new(1).unwrap(), RangeId::new(2).unwrap()];
        let merged = merge_ordered_top_k(
            vec![
                (ranges[0], vec![(1, "a"), (4, "d"), (7, "g")]),
                (ranges[1], vec![(2, "b"), (3, "c"), (8, "h")]),
            ],
            5,
            DistributedSortDirection::Ascending,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert_eq!(
            merged
                .into_iter()
                .map(|row| row.sort_key)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 7]
        );

        let mut left = DistributedNumericAggregate::default();
        let mut right = DistributedNumericAggregate::default();
        for value in [1.0, 2.0, 3.0] {
            left.observe(value).unwrap();
        }
        for value in [4.0, 5.0] {
            right.observe(value).unwrap();
        }
        let merged = merge_numeric_aggregates([left, right]).unwrap();
        assert_eq!(merged.count, 5);
        assert_eq!(merged.sum, 15.0);
        assert_eq!(merged.average(), Some(3.0));
        assert_eq!(merged.minimum, Some(1.0));
        assert_eq!(merged.maximum, Some(5.0));
    }

    fn shard_statistics(
        topology: &ClusterTopology,
        range: &RangeDescriptor,
        documents: u64,
        term_documents: u64,
    ) -> ShardFtsStatisticsSnapshot {
        ShardFtsStatisticsSnapshot {
            topology_generation: topology.generation,
            range_id: range.id,
            range_epoch: range.epoch,
            collection: "documents".to_string(),
            collection_statistics: FullTextCollectionStatistics {
                format_version: 2,
                document_count: documents,
                total_document_length: documents * 10,
                average_document_length: 10.0,
                field_total_lengths: [documents * 10, 0, 0, 0],
                term_count: 1,
                next_document_id: documents + 1,
            },
            terms: BTreeMap::from([(
                "heart".to_string(),
                FullTextTermStatistics {
                    document_frequency: term_documents,
                    collection_frequency: term_documents * 2,
                    ..FullTextTermStatistics::default()
                },
            )]),
        }
    }

    #[test]
    fn global_fts_statistics_are_epoch_fenced_and_versioned() {
        let topology = topology_with_ranges(2);
        let ranges = topology.ranges.values().collect::<Vec<_>>();
        let exact = merge_global_fts_statistics(
            &topology,
            "documents",
            7,
            vec![
                shard_statistics(&topology, ranges[0], 100, 10),
                shard_statistics(&topology, ranges[1], 200, 30),
            ],
            GlobalFtsStatisticsPolicy::RequireExact,
        )
        .unwrap();
        assert!(exact.exact);
        assert_eq!(exact.snapshot_version, 7);
        assert_eq!(exact.document_count, 300);
        assert_eq!(exact.terms["heart"].document_frequency, 40);
        assert!(exact.inverse_document_frequency("heart") > 0.0);
        let merged_hits = merge_fts_top_k(
            vec![
                vec![
                    DistributedFtsHit {
                        score: 9.0,
                        range_id: ranges[0].id,
                        document_id: 1,
                        topology_generation: topology.generation,
                        statistics_snapshot_version: exact.snapshot_version,
                        row: "first",
                    },
                    DistributedFtsHit {
                        score: 4.0,
                        range_id: ranges[0].id,
                        document_id: 2,
                        topology_generation: topology.generation,
                        statistics_snapshot_version: exact.snapshot_version,
                        row: "fourth",
                    },
                ],
                vec![
                    DistributedFtsHit {
                        score: 8.0,
                        range_id: ranges[1].id,
                        document_id: 1,
                        topology_generation: topology.generation,
                        statistics_snapshot_version: exact.snapshot_version,
                        row: "second",
                    },
                    DistributedFtsHit {
                        score: 7.0,
                        range_id: ranges[1].id,
                        document_id: 2,
                        topology_generation: topology.generation,
                        statistics_snapshot_version: exact.snapshot_version,
                        row: "third",
                    },
                ],
            ],
            3,
            &exact,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        assert_eq!(
            merged_hits.iter().map(|hit| hit.score).collect::<Vec<_>>(),
            vec![9.0, 8.0, 7.0]
        );

        let approximate = merge_global_fts_statistics(
            &topology,
            "documents",
            8,
            vec![shard_statistics(&topology, ranges[0], 100, 10)],
            GlobalFtsStatisticsPolicy::AllowVersionedApproximation,
        )
        .unwrap();
        assert!(!approximate.exact);
        assert_eq!(approximate.missing_ranges, vec![ranges[1].id]);
    }

    fn keys_in_different_ranges(topology: &ClusterTopology) -> (String, String) {
        let first = (0..100_000)
            .map(|index| format!("key-{index}"))
            .find(|key| {
                topology.range_for_key("documents", key).unwrap().id
                    == topology.ranges.values().next().unwrap().id
            })
            .unwrap();
        let first_range = topology.range_for_key("documents", &first).unwrap().id;
        let second = (0..100_000)
            .map(|index| format!("other-{index}"))
            .find(|key| topology.range_for_key("documents", key).unwrap().id != first_range)
            .unwrap();
        (first, second)
    }

    #[derive(Default)]
    struct RecordingCommitProtocol {
        events: Vec<String>,
        fail_commit: Option<RangeId>,
    }

    impl DistributedCommitProtocol<&'static str> for RecordingCommitProtocol {
        fn name(&self) -> &'static str {
            "recording-2pc"
        }

        fn protocol_version(&self) -> u32 {
            1
        }

        fn begin(
            &mut self,
            transaction_id: &str,
            _participants: &[DistributedTransactionParticipant<&'static str>],
        ) -> Result<()> {
            self.events.push(format!("begin:{transaction_id}"));
            Ok(())
        }

        fn prepare_participant(
            &mut self,
            _transaction_id: &str,
            participant: &DistributedTransactionParticipant<&'static str>,
            _cancellation: &CancellationToken,
        ) -> Result<()> {
            self.events
                .push(format!("prepare:{}", participant.range_id));
            Ok(())
        }

        fn record_decision(
            &mut self,
            _transaction_id: &str,
            decision: DistributedCommitDecision,
        ) -> Result<()> {
            self.events.push(format!("decision:{decision:?}"));
            Ok(())
        }

        fn commit_participant(
            &mut self,
            _transaction_id: &str,
            participant: &DistributedTransactionParticipant<&'static str>,
        ) -> Result<()> {
            self.events.push(format!("commit:{}", participant.range_id));
            if self.fail_commit == Some(participant.range_id) {
                return Err(query_error("injected commit transport failure"));
            }
            Ok(())
        }

        fn abort_participant(
            &mut self,
            _transaction_id: &str,
            participant: &DistributedTransactionParticipant<&'static str>,
        ) -> Result<()> {
            self.events.push(format!("abort:{}", participant.range_id));
            Ok(())
        }

        fn finish(&mut self, transaction_id: &str) -> Result<()> {
            self.events.push(format!("finish:{transaction_id}"));
            Ok(())
        }
    }

    #[test]
    fn cross_range_writes_reject_by_default_and_require_explicit_protocol() {
        let topology = topology_with_ranges(4);
        let (first, second) = keys_in_different_ranges(&topology);
        let writes = || {
            vec![
                DistributedWriteIntent {
                    namespace: "documents".to_string(),
                    key: first.clone(),
                    write: "one",
                },
                DistributedWriteIntent {
                    namespace: "documents".to_string(),
                    key: second.clone(),
                    write: "two",
                },
            ]
        };
        assert!(plan_shard_local_write(&topology, writes())
            .unwrap_err()
            .to_string()
            .contains("before execution"));

        let mut protocol = RecordingCommitProtocol::default();
        execute_distributed_transaction(
            &topology,
            "tx-1",
            writes(),
            &mut protocol,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
        let decision = protocol
            .events
            .iter()
            .position(|event| event == "decision:Commit")
            .unwrap();
        assert!(
            protocol.events[..decision]
                .iter()
                .filter(|event| event.starts_with("prepare:"))
                .count()
                == 2
        );
        assert!(
            protocol.events[decision + 1..]
                .iter()
                .filter(|event| event.starts_with("commit:"))
                .count()
                == 2
        );

        let failing_range = topology.range_for_key("documents", &first).unwrap().id;
        let mut recovery_protocol = RecordingCommitProtocol {
            fail_commit: Some(failing_range),
            ..RecordingCommitProtocol::default()
        };
        let error = execute_distributed_transaction(
            &topology,
            "tx-2",
            writes(),
            &mut recovery_protocol,
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("needs protocol recovery"));
        assert!(recovery_protocol
            .events
            .iter()
            .any(|event| event == "decision:Commit"));
        assert!(!recovery_protocol
            .events
            .iter()
            .any(|event| event == "finish:tx-2"));
    }

    #[test]
    fn child_cancellation_propagates_down_but_not_up() {
        let parent = CancellationToken::uncancelable();
        let child = parent.child();
        child.cancel();
        assert!(child.check().is_err());
        assert!(parent.check().is_ok());

        let parent = CancellationToken::uncancelable();
        let child = parent.child();
        parent.cancel();
        assert!(child.check().is_err());
    }

    #[test]
    fn node_type_remains_sendable_for_transport_implementations() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClusterNode>();
    }
}
