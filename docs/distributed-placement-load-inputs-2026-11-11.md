# Default placement: count balancing, estimated bytes and missing live feedback

Source examined: **1.0.461-beta**,
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`.
Status: source inspection plus allowlisted topology observations from the initial
Azure smoke. **No engine changes, new balancing capability or qualification.**

The default controller moves real data and balances replica/leader counts. It
must not be described as independently balancing measured physical bytes or
request load. Byte estimates participate in planning, but live byte/QPS feedback
and a bytes-only imbalance trigger are not integrated in the inspected host path.

## What the source actually uses

| Boundary | Implemented behavior and limit |
|---|---|
| [`distribution_controller.rs`](../crates/bicdb-pgwire/src/distribution_controller.rs), `BalancedDistributionHostService`, `CapacityPlacement` | Installs an automatic supervisor with `build_failure_repair_plan`; no separate live load sampler is installed here. |
| [`distribution_supervisor.rs`](../crates/bicdb-core/src/distribution_supervisor.rs), `ClusterSupervisor::tick` | Passes the existing topology/config/options to the injected planner and drives bounded relocations. Its heartbeat reuses the existing local node's `used_bytes`/capacity. |
| [`distribution.rs`](../crates/bicdb-core/src/distribution.rs), `build_rebalance_plan` | Initializes projected used bytes from persisted `node.used_bytes`, counts replicas, reserves estimated movement bytes and enforces eligibility/capacity/failure domains. The redistribution loop selects a source whose replica count exceeds the minimum by more than one. Equal replica counts with unequal bytes do not alone trigger this loop. |
| Same file, `range_estimated_bytes` | Uses `max(range.approximate_bytes, options.unknown_range_bytes)`. Default unknown/minimum estimate is **64 MiB**, including for known estimates smaller than that floor. This is not a measurement of each range's actual footprint. |
| Same file, `choose_target` and candidate sorting | Range candidates are ranked by descending estimated bytes; destination selection considers failure-domain conflicts, projected capacity utilization and replica count. These are real byte/capacity-aware planning mechanisms, not a complete bytes-only rebalancer. |
| Same file, leader-transfer planning | Uses leader counts/ceilings and eligible voters. It does not rank leaders by QPS, CPU demand or observed request latency. |
| Same file, `DistributionStore::update_range_load` | A callable API stores bytes/QPS and commits a topology change. A repository-wide search at this source found its declaration but **no caller** in the default host, CLI or other shipped path. An embedding can supply estimates through an appropriate authority boundary; that does not demonstrate live integration in `bicdb serve`. |
| [`checkpoint_driver.rs`](../crates/bicdb-pgwire/src/checkpoint_driver.rs), `start_distribution_supervisor` | Leader/follower member heartbeats reuse stored `local.used_bytes` and capacity; they do not substitute a filesystem or per-range byte sampler. Schema fingerprint sampling is separate from load measurement. |
| [`distribution_anti_entropy_auto.rs`](../crates/bicdb-core/src/distribution_anti_entropy_auto.rs), range selection | `approximate_qps` can constrain background anti-entropy selection. A stored QPS field and its update API are not a QPS-based placement planner; default placement does not consume that field. |
| `ClusterOperationalMetrics::from_topology` in `distribution_supervisor.rs` | Replica-byte skew sums raw `range.approximate_bytes`, without the planner's unknown-byte floor. Zero input estimates can therefore yield zero reported skew despite populated, unequal data. |

Bootstrap initializes both per-range estimates and node used bytes to zero.
Split/merge can derive estimates from previous descriptors. Neither behavior
constitutes a live feedback loop from rows, disk occupancy or measured traffic.
Snapshot transfer records actual serialized bytes, but the default host does not
feed those counters into `update_range_load` after movement. This conclusion
comes from tracing the host/planner/update paths, not merely from seeing one
snapshot with zero estimates. It is scoped to the shipped default host; it does
not rule out separate custom/private embeddings supplying their own statistics.

## Observed inputs versus physical movement

The [numeric projections](evidence/azure-placement-inputs-2026-11-11/provenance.json)
cover the four-node baseline, first five-node convergence and post-process-fault
five-node convergence from the initial smoke. At each checkpoint:

- All 64 descriptors had `approximate_bytes = 0` and `approximate_qps = 0`.
- Every node reported `used_bytes = 0`, with declared capacity 100,000,000,000 bytes.
- Reported replica-byte skew was zero; this is **unknown/zero estimate input**,
  not verified equality of physical bytes, disk utilization or traffic.

At the first recorded five-node converged checkpoint, 38 completed relocations to n4 reported 537,266
snapshot bytes. Native writes and prejoin payload reads also demonstrate actual
user records; populated data does not imply populated planner estimates. The
later checkpoint contains 76 completed historical relocations and 1,525,862
snapshot bytes; it is a different observation, not a replacement denominator
for the initial expansion.

The positive result is **automatic physical relocation under count/leader
placement**. It does not establish measured byte balancing, hotspot response,
QPS-aware leadership or increased capacity. The initial five-node throughput
was lower, with short windows and extra SSH/observation load; no causal claim
about this missing feedback alone follows from that timing result.

## Required future evidence

Before claiming live byte/load balancing, define authoritative bounded load
collection, sampling/staleness semantics, quorum publication and unknown-state
telemetry. Exercise unequal-sized ranges with already-equal replica counts, and
show automatic improvement in actual per-node byte load without manual topology
edits. Exercise equal-byte but skewed-QPS traffic separately before claiming
hotspot/CPU/latency-aware leadership. Preserve count, capacity, failure-domain,
movement-budget, epoch and repair safety constraints in both cases.

Tests of a planner supplied with synthetic estimates establish its behavior for
those inputs. They do not establish a default runtime sampler, bytes-only movement
trigger, physical qualification or application throughput gain. The roadmap now
separates those delivered mechanisms from the missing integration and trigger.

The inspected `distribution.rs`, `distribution_supervisor.rs` and default
`distribution_controller.rs` files are byte-identical between the original audit
runtime `2f36fe6900d1dd92aad5cb2b587757b1a670cbd3` and frozen `b05385d4`.
This corrects the earlier audit's byte-balancing interpretation; it is not a new
runtime regression introduced after that audit. The smoke's join-to-observed-
convergence interval is an upper observation bound: the harness waits for its
traffic/verification window before polling, so it does not establish first actual
balancing completion or RTO.
