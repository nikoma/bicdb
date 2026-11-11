# HA and horizontal scaling maturity audit — 2026-11-10

Initial audit of remote `main`: **1.0.455-beta**, commit
`2f36fe6900d1dd92aad5cb2b587757b1a670cbd3` (fetched from GitHub).
This document and its required source-version bump do not change runtime behavior.
Paths and symbols below refer to that revision unless an older revision is named.

**The ultimate requirement is not met:** adding a node can trigger automatic
range replica movement and leader balancing, but BicDB does not currently offer
transparent distributed SQL and recoverable multi-range SQL transactions to
ordinary PostgreSQL applications. Independent-machine sustained throughput
scaling and fault-safe rebalance of the current artifact remain unqualified.

## P0 source follow-up — 2026-11-11

The [P0 correctness and recovery report](p0-range-recovery-progress-2026-11-11.md)
records newer source fixes and exact local evidence for fair relocation turns,
certificate quorum before stable-membership write ACK, retry admission, fresh
metadata authority and client network deadlines. It also records unresolved
strong-read/image/handoff proofs. A completed snapshot is not evidence that the
new leader can recover compacted range history: an actual 1.0.463-beta
characterization retains the copied value but fails healthy-quorum recovery
after 1,025 commands. These source checks do not qualify a release or alter the
original failed physical experiment. Historical source and measurement identities
below remain at the revisions actually tested.

## Follow-up engineering work — 2026-11-11

Runtime baseline **1.0.460-beta**, `c173fa783fb8323fe02c41fb7eda96f130bc29c3`,
includes PR132's native HA voter safety repair. It validates nonzero synchronous
K against election majority, excludes observer acknowledgments/lease contacts,
and requires native handshake v4. Thus item 4 in this historical audit's defect
list is **source-repaired**, with configuration caveats for explicit async and
timeout degradation still applicable. The original 1.0.455 evidence is preserved.
This repair does not implement general distributed SQL or qualify physical scale.

The independent engineering tracks now have separate artifacts:

- [Current native HA test evidence](evidence/native-ha-current-2026-11-11/README.md):
  exact-source regression results, fault mechanisms, fsync settings and test
  executable hashes; physical host/VM qualification remains separate.
- [Distributed SQL design](distributed-sql-design-2026-11-11.md): typed gateway,
  durable decision/participant recovery, global read visibility and logical
  locality as an optimization. This is design, not an implemented SQL service.
- [Four-to-five experiment](scale-out-experiment-2026-11-11.md): runnable growth
  profile and sanitized Azure preflight. External per-ack ledger, operation-aware
  faults, quota/placement, bounded infrastructure authorization and cleanup
  remain execution gates. No independent cluster experiment has run.
- [Release capability declarations](release-capabilities.md): exact-artifact
  identity, separate implementation/test/qualification/deployment fields and
  fail-closed placement preflight. No qualification evidence is fabricated;
  the v1 generator cannot grant production admission.

The integration at source version **1.0.461-beta** adds release packaging/preflight
and exploratory runner support, not a distributed SQL engine. The independent
durability batch passed 78 targeted tests at exact runtime baseline `c173fa78`
(1.0.460-beta); it does not qualify a newly versioned release binary. The integrated
Python batch passed 34 tests, including 19 release-manifest tests and a real tiny-ELF
packaging fixture. Version identity (29 packages), syntax, local link targets and
whitespace checks passed. The fixture is not a BicDB release artifact.

These tracks do not alter the original audit's measured results or turn its
historical test runs into validation of a newer runtime artifact.

## Evidence rules and current CI

Implemented means an executable production code path exists; a public trait or
plan is insufficient. Test evidence names its source and scope. Physical evidence
must state actual failure domains and fault mechanism. Production qualification
requires a complete workload/topology/artifact-specific evidence bundle. Deployment
requires observing the actual running artifact and configuration. None implies the
next. Absence below means no supporting evidence found in this audit, not proof
that no private deployment exists.

The configured [CI workflow](../.github/workflows/tests.yml) compiles all targets,
runs core, SQL, pgwire and the remaining workspace, and tests the Python cluster
harness/offline notebook queue. Ordinary tests do not run ignored external-asset
VM tests. The GitHub connector returned no combined statuses or PR workflow runs
for the merge SHA; this is unavailable evidence, not a failed test or a green CI
result. For PR133 head `cd83f3a6161a3abaf70ad3184d694453f75136e8`,
[tests run 42847577842](https://github.com/nikoma/bicdb/actions/runs/42847577842)
was in progress and [Rails run 42847577843](https://github.com/nikoma/bicdb/actions/runs/42847577843)
was successful when queried. PR133 reports 54 focused release tests passed at
that head; that report does not prove all HA suites passed on the merged head.

## Deployment maturity matrix

| Mode | Actual configuration and implementation | Tests and measured scope | Physical qualification, deployment and blockers |
|---|---|---|---|
| 1a. Legacy file shipping | `bicdb ha ship/status/promote`; whole-database copy, `role=standby`, explicit fencing and client repointing. Async bootstrap/repair fallback. See [HA](high-availability.md), core `src/db/ha_node.rs`, `src/db/ha_bootstrap.rs`. | `crates/bicdb-core/tests/ha_state_durability.rs`, `ha_state_crash.rs`: promotion/apply failure ordering, persisted votes/fences, interrupted ship refuses ordinary promotion. | RPO is changes since last successful ship, not zero; RTO operator-dependent. No current-artifact independent-host RPO/RTO/latency bundle found. Adding replicas does not distribute primary writes. |
| 1b. Native single-primary streaming | `bicdb serve --replication-cluster-id/--replication-node-id`, pinned mTLS, `--allow-standby`, `--standby-of`; `--synchronous-standbys K` (0 async), timeout `fail` or `degrade`; `--ha-peer` and timeout enable majority elections/primary lease. `crates/bicdb-pgwire/src/embedded_ha.rs`, core `src/db/replication_live.rs`, `src/db/ha_node.rs`. | `crates/bicdb-cli/tests/ha_failover_cli.rs`: acknowledged-write kill and planned switchover; `crates/bicdb-pgwire/tests/embedded_ha.rs`: quorum loss, returning-writer fencing, journal identity. [HA](high-availability.md) reports three kills, 5–7 s recovery with 4 s timeout; no independent-host raw measurement bundle accompanies that statement. | Conditional zero-loss synchronous acknowledgment contract; async/degrade can lose acknowledged suffix. Configure enough synchronous copies for election-quorum intersection (majority total durable copies); arbitrary K in a larger group is not a universal zero-loss election guarantee. No current production qualification established. SQL compatibility/application deployments elsewhere do not establish topology-specific HA qualification. |
| 2. Distributed virtual ranges | `cluster init/join`, `cluster-topology.json`, `bicdb-distribution.json`; RF3, failure-domain labels, mTLS; default balanced `serve` controller, manual opt-out. Separate durable metadata quorum and range write quorum; bounded snapshot/catch-up/promotion/cleanup. Core `distribution*.rs`, pgwire `distribution_controller.rs`, `checkpoint_driver.rs`. | Metadata joint-quorum/restart/partition tests, range decision/recovery tests and relocation restart tests detailed below. Completed event-binding 3→5→9 **shared-host debug** run at `69866e48c02f28affe2e763da8bec5f756384dea`: all 353 acknowledged writes retained. | Automatic movement implemented and functionally exercised. No independent-machine scale-out proof found; no sustained throughput gain demonstrated. Ordinary SQL forwarding, distributed joins and concrete durable cross-range SQL commit service absent. All hardware Phase-9 certification items remain open. No current production cluster deployment verified. |
| 3. Cell-scoped HA/encryption | `bicdb-cell serve` / `BICDB_RUNTIME_MODE=cell`, signed manifest, one-shot key lease, HA trust/epoch/replica documents. `bicdb-cell-ha/src/lib.rs`, `bicdb-cell/src/lib.rs`; `CellHaCommitFence`, `CellFailoverSupervisor`. Distinct lease/recovery/auditor authority; encrypted signed per-destination async commit objects. | Unit tests check insufficient authority, expiry/fence promotion, rollback refusal, object scope, signed drill validation and route publication order. Synthetic signed drill fixtures test validation, not real measured RPO/RTO. | `SynchronousQuorum` explicitly refused; positive declared commit RPO is not continuously enforced by admission. Encrypted Cell refuses unencrypted `server_paged`. No archived current-artifact operational drill/independent audit/deployment proved here. Phase8 can admit only an exact valid evidence/attestation bundle; Phase5's denied-admission statement is phase-scoped. |
| 4. Application fleets/state groups | `bic serve --fleet-config` with operator workers, region/isolation/capacity; optional `--state-config` pins per-application 3/5/7/9-node HA groups. Worker leases, execution capability rotation and state receipts. `bicdb-app-host/src/fleet.rs`, `bicdb-fleet/src/lib.rs` (signed rollout authority, not SQL allocator). | `bicdb-app-host/tests/fleet.rs`: loopback separate processes, worker crash, reassignment, capacity, dedup, suspended-state lease expiry. Optional external-assets microVM test is ignored unless explicitly enabled. [State groups](platform-state-groups.md), [worker fleets](platform-worker-fleet.md). | Adds compute placement, not automatic sharding of an application's database. State groups remain one writer per application, not active/active SQL. Capacity counts applications, not RPS/memory. No measured independent-machine fleet scaling or current qualified deployment established. Cell fleet rollout authorizes immutable releases without database/key authority. |

## Direct answers: distributed cluster behavior

### Adding a node and automatic movement

**Yes at the range storage/control-plane level; not yet as the requested SQL product.**
`BalancedDistributionHostService` defaults to automatic rebalancing, 250 ms ticks,
four movement steps/tick, four planned replica moves and two moves/node.
A remote `cluster join` self-registers an authenticated learner; `serve` catches
it up and joint metadata quorum promotes it before placement. Placement balances
replica bytes/count and leader count, excludes unhealthy/draining nodes, checks
schema identity/capacity/failure domains and limits concurrent movement. This is
not proof of request-rate or latency-aware load balancing.

Core [supervisor](../crates/bicdb-core/src/distribution_supervisor.rs) test
`supervisor_rebalances_new_node_and_exports_bounded_metrics` and pgwire
[src/validation.rs](../crates/bicdb-pgwire/src/validation.rs)
`live_metadata_leader_bootstraps_and_promotes_a_new_learner` exercise admission.
[Metadata consensus](../crates/bicdb-core/src/distribution_consensus.rs) tests
`topology_commits_only_after_joint_old_and_new_quorums` and
`five_node_partition_and_process_restart_preserve_one_authority` test authority.
[Operations](distributed-cluster-operations.md) describes the deterministic
relocation restart matrix: learner allocation, partial snapshot/catch-up,
ready-to-promote, promoted and partial cleanup. Reopening in-process stores is
not physical host-loss evidence.

### Ordinary PostgreSQL clients, partition keys and locality

**No transparent scale-out for an unchanged ordinary PostgreSQL application.**
Pgwire exposes planning/router hooks (`server_cluster.rs`:
`plan_cluster_point_query`, `plan_cluster_scatter_query`, shard-local planning),
but does not turn arbitrary received SQL into distributed execution. A write
received on a nonleader is rejected rather than transparently forwarded.
A PostgreSQL multi-host connection can select a whole-database primary in mode 1;
it cannot choose many range leaders for mode 2 by itself.

[distribution.rs](../crates/bicdb-core/src/distribution.rs)
`distribution_key_token(namespace, key)` computes SHA-256 over a versioned domain,
length-delimited namespace and exact key, using the first eight bytes as the
64-bit token. Commit admission hashes the **actual collection and record ID**.
The initial token space is split into a configured power-of-two range count
(1–65,536), independent of server count. Application event allocation is an
operator-controlled unique namespace; the app chooses each immutable event key.

There is no demonstrated SQL `PARTITION BY tenant`/locality policy mapping all
records of a business transaction to a common routing key. A shared tenant prefix
or equal IDs in different collections do not guarantee colocation: the entire key
and namespace are hashed. Labels constrain replica geography, not tenant grouping.
A multi-record transaction is accepted by default only if **all final mutation
keys** resolve to the same current range. Hand-picking IDs that happen to collide
is not a stable tenant-locality contract, especially across splits. Initializing
one range co-locates everything at the cost of range-level write distribution.
Metadata split/merge APIs exist; automatic data/load-driven range splitting is
not wired into this event-serving controller. [Event contract](application-cluster-events.md)
explicitly requires enough initial ranges and says automatic splitting is absent.

### Cross-range queries, joins and writes

| Operation | Actual available guarantee |
|---|---|
| Point read/write | Routing/fencing primitives; immutable event host refreshes routes and retries. Ordinary pgwire forwarding remains absent. |
| Cross-range reads | Core `distribution_query.rs` has explicitly invoked bounded scatter/gather, ordered merge, numeric aggregates, FTS top-K and versioned global BM25 statistics. Limits include ranges, concurrency, rows/bytes, deadlines, cancellation and partial-result policy. These are host/library mechanisms, not a general distributed SQL SELECT executor. |
| Joins | No general cross-range SQL join execution demonstrated. Local SQL joins run over local data; neither local success nor a merge helper establishes cluster-wide completeness or a global snapshot. |
| Event list/get | Leader-applied, eventually consistent reads without quorum read barrier; pagination has no global order or cross-range snapshot. Unsuitable for read-then-write balances/uniqueness. |
| Default transaction | All mutations must resolve to one range; cross-range rejected before dispatch/quorum application. This includes multi-row/multi-table work whose physical record keys span ranges. |
| Multi-range atomic transaction | No concrete production SQL service found. `DistributedCommitProtocol` is a supplied durability contract and `execute_distributed_transaction` orders prepare → durable decision → participant completion. Repository search finds only `RecordingCommitProtocol` as its implementation, in tests. Its tests verify ordering/recovery-required errors, not crash-safe SQL atomicity. |
| SQL + external events/jobs | `env.DB` SQL effects/receipts/transactional jobs share local/native-HA boundaries. `env.EVENTS` append is separate; SQL-to-event outbox delivery is eventual/idempotent, not atomic across both databases. |

For a crashed multi-range coordinator, the API requires a host's durable decision
store and recovery worker to finish committed participants. Current normal SQL
provides no such coordinator service; there is no delivered general guarantee to
quote for that crash. Do not confuse this with the implemented **single-range**
coordinator recovery described next.

### Acknowledgments, fencing and recovery during movement

[distribution_range_consensus.rs](../crates/bicdb-core/src/distribution_range_consensus.rs)
`resolve_range` enforces single-range/current-leader/current-schema authority.
Writes durably prepare identical commands on a majority, collect provisional
decision quorum, persist certification, apply through normal row/index/WAL paths
and checkpoint local application before completion. Merely provisional local
state cannot become crash replay. Minority/stale-epoch/nonvoter writes fail closed.
Lost responses remain uncertain; use stable IDs/receipts, never infer rollback
from a transport timeout.

A new leader recovers a quorum-evidenced resolved prefix and probes the unresolved
tail before admitting writes. Certified/applied evidence or an identical
provisional quorum preserves a command; provably abandoned prepares abort.
Ambiguous/unavailable/conflicting history blocks writes rather than inventing an
outcome. Retained suffix is bounded (1,024 commands); outside it snapshot repair
is required. Relevant tests include
`committed_decision_replays_after_restart_before_local_apply`,
`three_voter_write_does_not_apply_without_majority`,
`new_leader_adopts_and_applies_a_quorum_provisional_tail`,
`new_leader_aborts_an_abandoned_prepared_tail_before_reusing_the_range`, and
`new_leader_fails_closed_on_an_ambiguous_provisional_tail`.

| Subsystem | Failover/rebalance behavior and qualification limit |
|---|---|
| Routing/fences | Quorum-published topology generations and range epochs invalidate old owners; certificate pins/incarnations fence identities. Destination promotion requires durability and parity; cleanup follows promotion and persists its cursor. Event host retries structured routing changes; arbitrary SQL clients do not acquire this behavior. |
| WAL | Range snapshot uses a commit watermark and bounded cursor; continuous range-filtered suffix includes empty frames. Learner apply checkpoints survive restart. Native HA holds WAL for confirmed/offline persisted standby positions; a missing retained prefix needs snapshot. Logical replication durability depends on storage/fsync and recovery, not only the command-log certificate. |
| Indexes/schema | Destination normal apply maintains indexes; schema fingerprints fence incompatible nodes. Empty nodes can receive schema bootstrap. Signed additive staged schema-rollout mechanisms exist (`distribution_schema_rollout.rs`); neither range replication alone nor an API certifies arbitrary online distributed DDL. Native whole-database standbys receive catalog/index definitions and build indexes locally. |
| FTS | Bounded FTS build/restart and distributed top-K/global-statistics mechanisms are tested. No physical current-cluster expansion/restore run with full production FTS corpus found. Immutable event binding offers no general global indexing API. |
| Queues/jobs | Application state groups replicate SQL receipts/job state and replace worker execution; delivery is at least once, external effects need idempotency. A range relocation is not a certified migration of global queue state. Broker outbox publication/retention recovery has its own fences and receipts; do not infer it from row checksums. |
| Audits/journals | Record-audit/broker publication completes inside local commit admission or fences recovery; journal-enabled whole-database HA preserves original signed identity/history. Selective range filtering refuses whole-database journal frames. No proof that all audit histories/queues transfer transparently with arbitrary distributed SQL or range relocation. |

## Recovery defects: current source versus historical evidence

1. **GAP-1 is source-repaired, not still an unimplemented checkbox.** Merged
   `36be15a2` (PR119) makes complete structural cuts unconditional. See
   [current algorithm](storage/structural-cuts.md), `bicdb-page/src/paged.rs`,
   `crates/bicdb-page/tests/gap1_no_steal.rs` (including W1, W3, W8, oversized
   pending/redo-backed writers and zero-steal witnesses), and
   `crates/bicdb-core/tests/gap1_page_only_batches.rs`. Historical
   [recovery semantics](recovery-semantics.md) explicitly names older source.
   Its steal-without-undo failure could lose reachable committed structure or
   refuse open despite durable logical commits. Those states block qualification
   of affected old binaries for crash-safe HA/rebalance/index builds. Current
   repair needs its own exact-artifact cluster qualification; logical quorum
   cannot compensate for every replica sharing a broken recovery algorithm.
2. **GAP-2 reset ordering is fixed.** Oldest sealed segments are deleted and
   directory synced before active checkpoint-log truncation. Historical GAP-3
   damage-as-tail behavior has fail-closed mid-file detection repairs documented
   in [durability failure handling](durability-failure-handling.md). Neither
   implies arbitrary media corruption is recoverable without restore.
3. **DDL recovery must be assessed after subsequent repairs.** Historical
   `0af1b55e` ordinary-main VM campaign refused startup after interrupted recycle
   move (`vm_ddl_75_pkey`). [Transactional DDL recovery](transactional-ddl-recovery.md)
   describes current durable before-image/outer-decision and namespace repairs,
   with 106 process cuts at `e3455a60`, full SQL 2,334 passes at `c53ec0a9`, and
   frontend corrections at `a72a3191`/`bf538d4e`. The old failure is retained;
   neither it nor an older clean randomized run proves the repaired current
   artifact's outcome. Partitioned physical-drop compensation, general catalog
   operations and replicated/distributed DDL retain separate qualification.
4. **Native HA configuration safety needs explicit qualification.**
   `bicdb-pgwire/src/high_availability.rs` configures synchronous K and majority
   liveness independently; core `src/db/replication_live.rs`
   `wait_for_confirmations` waits for K, not an implicitly upgraded majority.
   The larger-group caveat above is a code-based safety inference, not a newly
   executed acknowledged-loss reproduction. Validate/restrict configurations
   before treating arbitrary K as election-safe.
5. **Broker/audit recovery has independent blockers.**
   [Broker outbox durability](broker-outbox-durability.md) covers publication
   fencing, retained receipts and ambiguous legacy trim refusal. Current main's
   PR133 repairs deployed `BrokerAckedBatch`/`BrokerGroupDeleted` replay and mixed
   settlement trim. PR133 explicitly leaves stopped-copy old/new queue parity,
   settled-only trimming and live progress as deployment gates. A source merge
   does not prove deployed S6 recovery or replica parity.
6. **Other documented recovery limits remain relevant.**
   [Known gaps](durability-failure-handling.md#10-known-gaps-outside-this-change)
   include interrupted online extent migration (`SegmentMissing`), an archive
   retention setting that failed before becoming durable (new base backup may
   be needed), and concurrent page archivers sharing a staging name (one per
   database). No closure evidence was found here for those specific constraints.
   Ambiguous range tails, divergent histories and compacted-away repair prefixes
   deliberately require recovery/snapshot/operator action; fail-closed is not a
   bounded-RTO availability guarantee.

## What was actually measured

**There is no current-main independent-physical-node HA/rebalance dataset from
which to report actual RPO/RTO or p50/p95/p99.** The following retained results
must not be substituted for one.

[Functional cluster report](reports/cluster-events-functional-2026-10-16.json),
source `69866e48c02f28affe2e763da8bec5f756384dea`, debug SHA-256
`c33355e1c790b14a16a30a7dda3305092d2c0fd53a9f8e2e285ed75fb7c442b4`:
one Ryzen 7900X host, approximately 64 GiB RAM, ext4/980 PRO RAID1; RF3,
16 ranges, four clients, 1 KiB payloads, fsync on. Latencies below are milliseconds
for successful logical calls only. Sample counts are small; failures stay separate.

| Phase | Successful ops/s | Failures | Write p50/p95/p99 ms | Read p50/p95/p99 ms |
|---|---:|---:|---|---|
| steady 3 | 6.39 | 0 | 1130.75 / 1482.20 / 1740.10 | 38.39 / 89.19 / 100.72 |
| grow 3→5 | 4.87 | 0 | 1256.79 / 2259.23 / 4104.06 | 44.26 / 190.29 / 313.93 |
| steady 5 | 2.39 | 0 | 2474.38 / 5768.95 / 6001.50 | 8.70 / 75.86 / 75.86 |
| grow 5→9 | 5.10 | 0 | 1192.03 / 3017.20 / 3535.71 | 7.91 / 383.16 / 612.66 |
| steady 9 | 3.38 | 0 | 1196.48 / 6557.48 / 6870.83 | 6.14 / 62.17 / 98.64 |
| failure 9 | 4.68 | 3 | 1045.95 / 4477.20 / 7717.49 | 8.79 / 69.21 / 177.02 |
| recovered 9 | 0.85 | 0 | 7497.83 / 14703.77 / 14703.77 | 9.32 / 10.20 / 10.20 |

All 353 acknowledged writes verified after recovery; three failure-window calls
failed. The killed process restarted around 20 s later. That delay is not a
measured service RTO, and zero observed acknowledged loss in this run is not a
universal RPO result. Throughput did not increase with node count.

[Hard VM resets](evidence/hard-vm-reset-2026-11-08/README.md): exact earlier
ordinary-default candidate `6d225c1940cf13ffbb7693f8ebc2917e5abaad82`
retained 2,275 acknowledged transactions in its full campaign; the separate
large-transaction campaign retained the 8,192-row batch and 4,259,840-byte value
with a 2 MiB pool, 14,845 actual evictions and zero unlogged steals. Combined
21 saved crash cuts independently verified clean. QEMU guest RAM was lost;
host/device power was not. This is single-database source-pinned VM evidence,
not physical multi-node HA/rebalance, final-head deployment or general RTO.

[Paired timing report](evidence/hard-vm-reset-2026-11-08/performance/README.md):
18 15-second shared-host optimized microbenchmark runs, 36 row-count reopens,
actual main `0af1b55e` versus default candidate `6d225c19`. At one writer,
median-of-three-run commit p50/p95/p99 were **1.640/3.919/4.631 ms** versus
**1.651/3.612/3.914 ms**; throughput medians 366 versus 299 commits/s.
These are medians of run percentiles, not pooled percentiles. Wide ranges and
mixed paired directions make speedup/regression inconclusive. They do not
measure network HA or adding nodes.

For mode 1, documented synchronous RPO=0 is a conditional protocol contract;
5–7 s failover is a limited three-node result without an attributable independent
hardware bundle here. Async mode has no demonstrated time/commit-loss upper
bound. For Cell HA, declared positive RPO and signed drill fields are policy,
not measurements found by this audit. For fleets, five-second worker leases and
state failover timeouts are configured mechanisms, not measured application RTO.

[Phase9](automatic-distributed-sharding-todo.md#phase-9--scale-and-failure-certification)
still lacks completed 1/5/20 TB hardware, saturation, eight maintenance-loss
phases, rebalance latency/amplification, restore/expansion with full FTS, and raw
publication. Certification gates such as ≤15 min recovery or ≤25% p99 regression
are acceptance thresholds, **not observed results**. Operations guide examples
contain older certification format numbers; current source/roadmap must identify
the applicable format before a new run.

## Gaps against CockroachDB's model

CockroachDB's [distribution layer](https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer),
[replication layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer),
[SQL layer](https://www.cockroachlabs.com/docs/stable/architecture/sql-layer) and
[transaction layer](https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer)
describe SQL gateways over automatically located/split/rebalanced replicated
ranges, leaseholders and distributed transactions. This is an architectural
comparison, not a claim of workload-independent linear scaling or perfect
PostgreSQL compatibility; transaction retries and hot keys still matter.

| Required property | BicDB finding |
|---|---|
| Any SQL gateway, transparent cross-range SQL/joins | Missing general executor/forwarding integration. Planning and merge hooks do not meet it. |
| Recoverable atomic multi-range SQL | Supplied protocol trait only; normal SQL rejects spanning writes. |
| Automatic data and leader/lease load placement | Byte/count/leader placement implemented; physical traffic-aware convergence and throughput gain unproved. BicDB range epochs must not be equated with CockroachDB's entire lease/read-consistency model. |
| Automatic range splitting | Metadata operations exist; event serving lacks automatic splitting based on size/load. |
| Online relocation | Implemented bounded durable phases and fencing; independent physical fault/FTS/queue/audit qualification missing. |
| Node replacement/failover | Learners, incarnation fencing, repair and metadata elections implemented; hardware RTO and full current-artifact recovery gates open. |
| Add nodes without application-specific shard management | Event binding satisfies a narrow append/get contract; ordinary PostgreSQL business application does not. |
| Sustained aggregate read/write gain | No independent-machine evidence; retained shared-host run shows no gain. |

## Smallest independent four-to-five-node experiment

This is a proposed scoped experiment, not a completed run or production
certification. It can falsify automatic movement/safety claims today. It cannot
prove transparent SQL support that is absent in source.

1. **Isolation and topology.** Five dedicated identical Linux data VMs plus an
   independent client/ledger controller (an existing isolated runner is enough).
   Use a disposable Azure resource group/private network or dedicated local hosts;
   no production routes, disks, credentials, signing keys or cluster membership.
   For an independent-host claim require five verified physical failure domains;
   different VM machine IDs alone are insufficient. Record actual host/zone/disk
   placement. Single-host VMs qualify only as functional fault tests. Azure SKU,
   placement availability and spending must be verified at execution time; no
   resources were provisioned during this audit.
2. **Freeze bytes.** Build one optimized locked current-main artifact, record
   clean full commit/version, binary SHA-256, dependencies, fsync, page/pool/WAL
   settings, hardware and network/storage limits. Use `server_paged`, RF3,
   64 initial ranges, mTLS with distinct pinned identities, equal capacities and
   default automatic controller. Start four nodes and wait for all voters,
   recovered range leaders, schema parity and zero pending/unavailable ranges.
3. **Working set and baseline.** Preload a fixed uniform corpus larger than the
   configured aggregate working cache (for example 8 GiB with a 256 MiB pool/node;
   also measure/control OS-cache effects). Keep every transaction single-range
   for this storage experiment. Warm up; run at least 15 minutes at four nodes,
   enough for ≥10,000 successful read and write samples. Sweep client load to
   find stable saturation within a predeclared latency/error budget. Log attempted,
   acknowledged and uncertain operations with IDs/payload hashes to a separately
   fsynced ledger. Retain errors/timeouts, not only successful latency.
4. **One operator action.** Join/start empty node five while clients continue
   unchanged. No manual rebalance, shard-map edits or node-specific application
   routing changes. Existing event binding may manage its internal range routes;
   label this as event API evidence. Capture topology generation/epoch, replica
   bytes/count, leaders, physical disk/network I/O, CPU/RSS/pressure, admission
   rejections and per-node request counts continuously. Prove actual durable
   data bytes moved and node five acquired leaders and served traffic. Target
   roughly one fifth of leader/replica load, within the planner's documented
   bounded skew and placement constraints; a membership entry alone fails.
5. **Fault during movement.** On separate fresh repeats, kill/blackhole a
   source or destination at observed partial snapshot, catch-up, promotion and
   cleanup boundaries, with before/after durable phase evidence. Keep the host
   down through detection/repair rather than immediately restarting it. Include
   a partitioned old-leader write probe and metadata-leader loss. Permit only
   one data-node fault at a time with RF3. Preserve raw errors and stalled
   recovery as results. Returning same-incarnation restart and empty replacement
   with higher incarnation are separate cases. A process kill is not device
   power loss; a VM reset must discard guest RAM, and neither certifies the
   physical storage power-loss boundary.
6. **Reconcile before success.** Every externally acknowledged commit must be
   present exactly with expected payload/receipt after convergence and repeated
   restart; uncertain commits may be whole present or whole absent, never
   partial. Verify fixed-watermark data and index digests, no duplicates, no
   acknowledged rollback, stale-writer rejection, RF3 restored, bounded skew
   and zero unfinished/failed relocation. Record detection, first successful
   replacement-leader operation, repair start and full convergence separately.
   Define service RTO as fault→sustained successful workload recovery, and RPO
   as missing externally acknowledged commits; report every trial, not best case.
7. **Measure capacity separately.** After five-node convergence repeat the same
   load sweep and ≥15-minute steady window with unchanged corpus and durability.
   Run three independent 4→5 repeats, including a no-fault control, recording
   p50/p95/p99/max and successes/failures for each window and independent read,
   write and mixed workloads. A predeclared meaningful target such as ≥10%
   higher sustained goodput at the same latency/error budget is an experiment
   criterion, not a predicted result; ideal proportional capacity is only 25%.
   If clients/network/shared disks are limiting, the test is inconclusive about
   engine scaling. Aggregate bytes/leader balance alone does not prove throughput.
8. **Separate unchanged-SQL acceptance.** Use a stock PostgreSQL driver through
   one unchanged endpoint, with point operations, multi-row/cross-table atomic
   business transactions and cross-range joins. Verify global answers against
   a reference. Current source is expected to fail this prerequisite; record it
   without rewriting the app to route shards or silently restricting the workload.
   A passing event-storage trial cannot close the ultimate SQL requirement.
9. **Boundaries and cleanup.** FTS, broker queues and audits require additional
   workload-specific integrity cases before claiming those guarantees. The small
   trial does not pass the existing five-node 1 TB certification contract:
   `certify-plan` requires five already-converged nodes before its plan, so do
   not weaken it or label the four-node baseline certified. Preserve hashed
   raw artifacts and ledgers outside VMs, then destroy only the experiment's
   resource group/volumes after evidence capture and verify deletion.

The current [SSH harness](../scripts/platform/README.md) supplies binary digest,
mTLS, machine-ID checks and event-ledger verification, but `scale-cluster.py`
accepts only increasing selections of **3,5,9**. It cannot execute 4→5 unchanged,
provides no provisioning, and its default short process-kill run does not cover
all movement boundaries or ordinary SQL. A later, separately scoped harness
change or external runner is necessary; none was implemented for this audit.

## Original audit validation and delivery scope

At audited `2f36fe69`, this audit ran 12 Python platform harness unit tests (all
passed), the hard-VM portable ledger reconciliation, large-transaction portable
reconciliation (two completed sources, seven clean cuts each), and the paired
performance artifact audit (18 runs/36 row-count recoveries), all exit 0.
These revalidate retained artifacts and harness contracts; they do not rerun
VMs, Rust HA suites, physical hardware or current-main application acceptance.
Incomplete historical campaigns remain incomplete in their output.

For the documentation delivery at source version **1.0.456-beta**,
`bump-version.py --check`, locked Cargo metadata (29 BicDB package identities),
local Markdown link targets (36; anchors not checked), and `git diff --check`
passed. The version-only metadata change does not change runtime dependencies
or requalify the earlier evidence. Rust suites were not rerun for this audit.

Only documentation and synchronized source-version metadata are delivered.
No feature, binary, release, cloud experiment or production deployment is
produced by this audit. Final delivery commit/version and documentation checks
are reported separately from the audited runtime source above.
