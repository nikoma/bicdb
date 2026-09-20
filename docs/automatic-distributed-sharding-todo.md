# Automatic Distributed Sharding TODO

Status: implementation roadmap. A checked item is shipped with tests. An
unchecked item is not a BicDB capability and must not be described as one.

The goal is a cluster where an operator joins a server and BicDB automatically
places, replicates, moves, and balances data without application-level shard
routing:

```text
bicdb cluster init
        |
bicdb cluster join server-2 ... server-n
        |
durable membership + range catalog
        |
replica/leader placement
        |
online snapshot + WAL catch-up
        |
epoch-fenced ownership change
        |
automatic SQL routing
```

This work is additive. Single-node databases remain the default and retain
their current format and behavior.

After every exit gate in this roadmap is complete and certified, work proceeds
to the public controller/mechanism boundary in
[`public-controller-apis.md`](public-controller-apis.md).

## Non-negotiable invariants

- A key belongs to exactly one live range at a catalog generation.
- Every write carries the range ID and epoch selected by the router.
- A former owner rejects a write after ownership or epoch changes.
- A replica is never promoted until its snapshot and commit suffix are durable.
- Replica removal happens only after the replacement is promoted.
- Range metadata changes are linearizable and survive process or host failure.
- A node never hosts two replicas of the same range.
- Placement honors replication factor and failure-domain constraints.
- Rebalancing is restartable, idempotent, rate-limited, and observable.
- Losing a node must not make healthy ranges unavailable when quorum remains.
- Distributed queries have explicit fan-out, memory, deadline, and result
  limits.
- No implementation may claim multi-terabyte readiness before
  `server_paged` passes its bounded-memory production gates.

## Phase 1 — Durable membership and topology

- [x] Define stable cluster, node, and topology-generation IDs. Range and
  replica IDs land with Phase 2.
- [x] Persist cluster topology atomically with checksum and format version.
- [x] Add idempotent node join, heartbeat, capacity/label updates, drain, and
  removal state.
- [x] Reject cluster-ID and node-ID mismatches before mutating metadata.
- [x] Keep a bounded topology history sufficient for routing diagnostics.

Exit gate: reopen preserves identical membership and topology generation, and
a torn/invalid topology file fails closed.

## Phase 2 — Virtual ranges and placement constraints

- [x] Use a stable versioned key hash; never use a process/runtime hasher.
- [x] Bootstrap an even token space without tying ownership to server count.
- [x] Implement epoch-preserving range split and adjacent-range merge metadata.
- [x] Validate complete, non-overlapping token-space coverage.
- [x] Support node labels and required distinct failure domains.

Exit gate: every possible token routes to exactly one range before and after
split/merge, including `0` and `u64::MAX`.

## Phase 3 — Replica and leader balancing

- [x] Produce deterministic replica-placement plans from topology and capacity.
- [x] Balance bytes, replica count, and leaders separately.
- [x] Exclude dead, decommissioned, and draining nodes from new placement.
- [x] Prefer failure-domain diversity before load equality.
- [x] Limit concurrent moves per node and total bytes in flight.

Exit gate: adding an empty node produces a bounded plan that converges without
oscillation; replaying the same plan is idempotent.

## Phase 4 — Online relocation and epoch fencing

- [x] Persist a relocation state machine: planned, snapshot, catch-up, promote,
  cleanup, complete/failed.
- [x] Export range snapshots in bounded record/byte batches with an MVCC commit
  watermark and exact resume key.
- [x] Filter and apply the corresponding continuous commit-frame suffix,
  retaining empty frames so the durable watermark remains gap-free.
- [x] Promote only after destination durability and source/destination parity.
- [x] Atomically increment the range epoch during learner allocation, promotion,
  and source removal.
- [x] Resume interrupted relocation from its checksummed durable phase.
- [x] Keep the old replica until the replacement is promoted.

Exit gate: crash injection at every phase either resumes safely or leaves the
old replica authoritative, with no write accepted under a stale epoch.

## Phase 5 — Request routing

- [x] Route point reads/writes by stable key token, range, epoch, and leader.
- [x] Support explicit leader, commit-aware bounded-staleness, and
  locality-aware nearest-replica reads.
- [x] Return structured retry/redirect information for stale routes.
- [x] Cache topology monotonically by generation and fail closed on conflicting
  payloads at one generation.
- [x] Expose router/fencing hooks to pgwire server sessions and provide a
  bounded per-node connection pool with redirect refresh and read-only
  failover.

Exit gate: clients can use any gateway while leaders move, without application
shard awareness or accepted stale-epoch writes.

## Phase 6 — Failure repair and node lifecycle

- [x] Classify live, suspect, dead, draining, and decommissioned nodes.
- [x] Replace replicas automatically after a configurable grace period.
- [x] Transfer leaders immediately to a synchronized voter after failure and
  before planned drain.
- [x] Refuse unsafe removal that would lose quorum or replication factor.
- [x] Re-replicate under-replicated ranges after host loss through the resumable
  relocation state machine.
- [x] Fence removed identities from rejoining without a new incarnation.

Exit gate: one-node failure at replication factor three repairs automatically
while every range retains quorum.

## Phase 7 — Distributed SQL and FTS

- [x] Keep shard-key point queries single-range through topology/epoch-fenced
  pgwire planning hooks.
- [x] Build bounded scatter/gather with explicit shard, concurrency, row, byte,
  deadline, cancellation, and partial-result limits.
- [x] Merge ordered results, numeric aggregates, and FTS top-K without
  materializing every shard result.
- [x] Maintain epoch-fenced global BM25 corpus statistics or an explicitly
  versioned approximation so FTS scores are comparable across ranges.
- [x] Define shard-local transaction behavior and reject unsupported
  cross-range writes before partial execution.
- [x] Put distributed transactions behind a separate durable-decision protocol
  boundary, with two-phase ordering and recovery-required failures tested
  independently from the default SQL path.

Exit gate: distributed top-K and aggregate results match a single-node reference,
and bounded execution survives one slow or failed shard according to policy.

## Phase 8 — Automatic controller and operations

- [x] Run membership, placement, repair, and movement under one cluster
  supervisor integrated with `bicdb serve`.
- [x] Add `cluster init/join/status/route/rebalance/drain/remove` commands.
- [x] Expose range, replica, leader, relocation, skew, and under-replication
  metrics.
- [x] Add placement policies for server/rack/zone/region.
- [x] Add backup/restore and rolling-upgrade contracts for range metadata.
- [x] Document incident, drain, replacement, and split-brain runbooks.
- [x] Add checksummed durable metadata term/vote/log state, bounded committed
  snapshots, current-log voting, and joint old/new membership quorum rules.
- [x] Carry metadata vote, append, snapshot, and commit messages over the
  authenticated bounded cluster transport.
- [x] Stage every topology mutation and publish it only after metadata quorum;
  an isolated former leader must never publish a new range epoch.
- [x] Run randomized elections, heartbeats, leader leases, and automatic
  failover inside every `bicdb serve` member.
- [x] Exercise three-node controller loss and prove that a higher-term
  replacement commits a leadership barrier and converges the surviving
  follower before resuming topology changes.
- [x] Replace directory-copy joining with an authenticated seed-address
  bootstrap run on the empty server itself. Fetch a quorum-derived topology
  and configuration snapshot, allow the pre-member to register only its own
  learner identity, provision locally without shared storage, and become a
  metadata voter only after catching up to the committed snapshot.
- [x] Bind every production node ID to its exact mTLS leaf-certificate SHA-256
  in quorum-committed membership at both ends of every RPC. Reject another
  leaf signed by the same CA as either caller or destination, and migrate an
  older unbound member only through a matching bind heartbeat before
  permitting its other RPCs.
- [x] Rotate a member's mTLS leaf through two quorum publications: stage a
  bounded active/pending overlap, atomically switch local material, activate
  only after a matching new-leaf heartbeat, and retain an interruption-safe,
  idempotent abort path that restores the active configuration before fencing
  the pending leaf.
- [x] Prove controller loss and network partition behavior with deterministic
  process-restart and five-node quorum tests.

Exit gate: joining a new empty server causes automatic, throttled redistribution
and leaves the cluster balanced after restart without operator-authored shard
maps. Losing the current metadata leader elects a replacement without accepting
two authorities or requiring an operator to rewrite cluster files.

## Schema locality and compatibility

- [x] Compute a canonical versioned fingerprint over collection security and
  mutation policy, executable indexes, and structural SQL catalog records while
  excluding ordinary row data and volatile runtime catalogs.
- [x] Advertise the live fingerprint through quorum-published member
  heartbeats, validate its reserved label, and reject incompatible placement,
  relocation, and leader-transfer targets.
- [x] Fence authenticated range data RPC and production write admission against
  both the quorum-advertised digest and the node's freshly verified live digest.
- [x] Invalidate verified schema authority synchronously after collection,
  policy, index, vector-dimension, or structural-catalog changes so stale schema
  cannot continue admitting writes.
- [x] Let a host-verified empty node receive an authenticated, checksummed,
  64 MiB/one-million-record-bounded schema bundle before its first range
  snapshot; reject automatic replacement when any user collection contains
  rows, and keep partial installation quarantined until the exact digest is
  published.
- [x] Distribute, validate, and atomically activate signed schema bundles across
  non-empty prospective voters as part of coordinated online DDL. 1.0.14 safely
  bootstraps empty nodes but still requires operators or the application package
  manager to repair drift on nodes containing data. In 1.0.47-beta, a non-empty
  node can verify a domain-separated Ed25519 signature from an explicit trusted
  key, independently recompute the bundle fingerprint, reject every implicit
  replacement/contraction, and atomically persist a bounded, checksummed,
  idempotent additive stage without changing the live schema. In 1.0.48-beta,
  cluster protocol v16 lets only the current metadata leader transfer that
  stage to active voters over member/mTLS-bound RPC; each destination uses its
  own host trust store and returns a validated durable receipt. In
  1.0.49-beta, a bounded durable coordinator freezes the exact committed
  topology, metadata leader, sorted active-voter set, shared base fingerprint,
  signed target, and limits; advances one voter per call; atomically checkpoints
  every validated receipt; safely retries a lost response; and fails closed on
  authority or topology drift. In 1.0.50-beta, each fenced voter can apply that
  exact stage through a checksummed, bounded, one-change-per-step local
  activation cursor; retry reconciles a schema mutation that committed before
  its cursor checkpoint, preserves existing rows, refuses unsafe abort after
  any applied object, and completes only at the signed target fingerprint.
  In 1.0.51-beta, the coordinator constructs a single all-voter target-digest
  topology generation only after complete staging; existing metadata consensus
  gives that fence authority only at quorum commit, after which every old live
  digest fails the production write/data-RPC fence. Lost proposal responses and
  leader restart reconcile from the exact committed generation. In
  1.0.52-beta, cluster protocol v17 lets only the current metadata leader
  advance activation after proving that the published topology is the exact
  committed all-voter target fence. Each voter revalidates its host trust and
  bounds, advances one durable local step, and returns a node-, rollout-,
  stage-, activation-, target-, and state-bound receipt. The coordinator
  advances one voter only after an exact completion receipt, safely retries
  lost responses, survives restart, and becomes complete only with durable
  completion evidence from every frozen voter. In 1.0.53-beta, protocol v18
  verifies and finalizes those exact completed activations one voter at a time.
  Each destination atomically retains a compact finalization proof before
  durably removing the activation cursor and signed stage; different evidence
  cannot clean either file, interrupted cleanup is idempotent, lost responses
  retry safely, and the coordinator reaches `finalized` only with durable
  node-bound cleanup evidence for the entire frozen voter set. In 1.0.54-beta,
  protocol v19 publishes a protected additive compatibility window while the
  active base digest remains unchanged. Every voter can advance through an
  exact signed-schema prefix while foreground writes and schema-fenced data RPC
  continue; durable checkpoints recover that authority after restart, direct
  schema mutation invalidates it, and quorum-managed labels cannot be forged or
  removed by heartbeat. Completion evidence from all voters authorizes a second
  quorum generation that atomically promotes the target. Finalization is fenced
  behind that promotion, and stale heartbeats cannot reinstate the base digest.
  Concurrent-write certification covers a resumable new-index build before,
  during, and after publication. In 1.0.56-beta, a successful authenticated
  learner schema-install ACK lets only the metadata relocation controller
  consume the target's one-time bootstrap claim and quorum-stage the exact
  leader fingerprint. Ordinary heartbeats remain unable to create, remove, or
  replace schema authority. Lost proposals safely repeat the idempotent install
  and certification before snapshot copying, preventing schema bootstrap from
  wedging relocation.

Exit gate: no node with a different executable schema can receive, lead, repair,
or serve a range. Destructive replacement, data transformation, contraction,
and multi-edition compatibility remain separate online-migration gates.

## Replica integrity and anti-entropy

- [x] Build bounded, resumable Merkle summaries over live range contents at an
  epoch- and consensus-prefix fence. The fixed-fanout digest state, atomic
  checkpoints, batch idempotency, content-root validation, and bounded bucket
  comparison ship in 1.0.30-beta. In 1.0.31-beta, each local scan step is
  admitted only under the exact unexpired durable range fence, authenticated
  leader/voter identity, immutable range epoch, and unchanged resolved prefix;
  empty-collection cursor advances are resumable and idempotent. Cluster
  data protocol v13 adds membership/mTLS-authenticated, schema-fenced advances
  in 1.0.32-beta; every voter owns and atomically checkpoints its own state and
  leaders supply only a compare-and-advance checksum. The 1.0.33-beta durable
  multi-voter run advances one bounded step at a time, recovers lost responses,
  and emits healthy, certified-divergent, or explicitly uncertified quorum
  evidence. In 1.0.34-beta a completed voter can export exactly one bucket in
  authenticated, checksummed, cursor-resumable record/byte-bounded frames while
  the original fence and prefix remain exact. In 1.0.35-beta, protocol v15 adds
  node-owned contiguous repair checkpoints and an idempotent bounded apply
  primitive that requires a quorum-certified source, the destination's own
  divergent-root evidence, current voters, and the unchanged fence. In
  1.0.36-beta, a single-owner, disk-backed coordinator stages both bounded
  streams under one artifact budget and externally merges exact upserts and
  source-proven deletes through immutable idempotent batches. In 1.0.37-beta,
  each repaired bucket is freshly re-exported from the destination and compared
  exactly against the certified source stream under bounded, resumable work.
  In 1.0.38-beta, a bounded certificate requires complete verified bucket
  coverage for every divergent voter, reconstructs each full certified root,
  and gates the remote-first, leader-last fence release path. In 1.0.55-beta,
  the production server adds a durable per-node sweep cursor, exact pre-RPC
  fence plans, deterministic locally led range selection, and complete crash
  recovery around the existing run scheduler.
- [x] Schedule low-priority replica comparisons without competing with quorum
  writes or foreground reads. The 1.0.55-beta controller defaults to idle
  ranges only, admits one bounded anti-entropy step at a time, defers before
  fencing and between steps whenever real server or governor foreground
  pressure exists, preserves critical resource headroom, and spaces both
  ranges and complete sweeps. Healthy and inconclusive runs release their
  exact fences; divergence remains durably fenced for certified repair.
- [x] Require quorum/source evidence before repairing a divergent bucket; never
  select truth from one replica merely because it responds first. The digest
  report certifies a quorum root, bounded reads are in place, every destination
  apply makes that report mandatory, and the external merge derives complete
  bounded upsert/delete batches from the certified streams.
- [x] Recompute and certify repaired buckets before returning the range to a
  healthy state. Exact per-bucket post-repair scans and comparison ship in
  1.0.37-beta; 1.0.38-beta requires a range-level certificate covering every
  divergent bucket and voter before releasing the fence.

Exit gate: injected replica corruption is localized, repaired from certified
evidence under hard scan/network/write limits, and remains resumable through
process failure without hiding unrelated divergence.

## Node resource governance

- [x] Define a fixed-cardinality node/lane governor for resident memory,
  in-flight I/O, CPU slots, concurrency, and token-bucket I/O rate. In
  1.0.39-beta, background and other noncritical lanes cannot consume the
  explicit consensus/foreground-write reserve, and RAII permits reclaim every
  in-flight reservation.
- [x] Admit anti-entropy, backup/restore, compaction, index construction, and
  distributed query steps through their corresponding lane with measured peak
  demands and supervisor backoff. Range digest runs are admitted and durably
  scheduled in 1.0.40-beta. Backup fencing, bounded capture, certification, and
  fence-release batches plus whole-certificate restore admission have governed
  entrypoints in 1.0.41-beta. Compaction and every resumable FTS build boundary
  have governed entrypoints in 1.0.42-beta, including a declared-memory floor
  tied to builder configuration. Incremental offline compaction becomes a
  governed, collection-at-a-time durable state machine in 1.0.44-beta.
  Restore admission becomes a single-owner, one-node-per-tick durable run in
  1.0.45-beta. In 1.0.46-beta, paged B-tree creation uses explicitly row/byte-
  bounded ticks, checksummed restart cursors, source and physical verification,
  the index-build lane, commit-sequence fencing, an unreachable physical
  generation, catalog-point publication, safe discard, and disk-backed reads
  that do not reload the generation into resident memory. In 1.0.55-beta, the
  production anti-entropy supervisor automatically selects only locally led,
  non-relocating ranges under an explicit QPS ceiling, defers on live host
  pressure, and performs no idle checkpoint writes before its durable due time.
  In 1.0.67-beta, server-paged vacuum is likewise a one-step compaction-lane
  supervisor with an exact durable cursor, store/operation fencing, atomic
  progress publication, bounded saturation retry, exponential failure backoff,
  and durable operator pause/resume.
  In 1.0.68-beta, the effective page size, hard buffer-pool budget, and WAL
  checkpoint trigger become explicit host configuration and fixed-cardinality
  operational telemetry. Cache admission, dirty occupancy, I/O, free-space,
  WAL, checksum, and crash-tail pressure are visible through the same doctor,
  CLI/TUI, JSON, and Prometheus surfaces used by production supervision.
- [x] Admit bounded distributed scatter/gather before shard fan-out, requiring
  the declared foreground-read envelope to cover results, coordinator
  metadata, concurrent responses, worker slots, and rate charge (1.0.43-beta).
- [x] Make each range digest tick resource-admitted, single-step, atomically
  checkpointed, restart-resumable, and subject to bounded saturation retry and
  exponential transport-failure backoff.
- [x] Export governor snapshots as exactly fixed scope/lane Prometheus series,
  without tenant, range, query, path, or plugin labels (1.0.43-beta).
- [ ] Certify that background saturation preserves quorum and foreground p99
  targets on the destructive multi-node scale harness. The 1.0.57-beta
  certification format makes this a mandatory, resumable, fail-closed
  observation: every planned node must show quiescent/saturated/quiescent
  fixed-cardinality governor snapshots with anti-entropy, backup/restore,
  compaction, and index-build work simultaneously active, at least one rejected
  background admission whose valid demand is proven not to fit the captured
  live envelope, five continuous minutes of pressure, at least 10,000
  successful foreground reads, writes, and range-quorum operations, at least
  1,000 successful metadata-quorum checks, zero unavailable ranges, and at most
  25% foreground read/write p99 regression. A dedicated checksummed raw artifact
  is required. This freezes and enforces the physical gate; the checkbox remains
  open until a production hardware bundle supplies the measurements.

Exit gate: all high-amplification background work is admitted under one node
envelope and cannot starve consensus, writes, or bounded foreground reads.

## Phase 9 — Scale and failure certification

- [x] Ship immutable 1/5/20 TB certification plans and a fail-closed raw-bundle
  verifier that binds topology/protocol versions, canonical non-weakenable
  gates, artifact sizes/SHA-256, every failure phase, restore, expansion,
  quorum behavior, hotspot latency, bounded memory, and full-corpus FTS
  evidence. This is the evidence harness, not a substitute for the physical
  runs below.
- [x] Add an atomically checkpointed, resumable evidence collector that binds
  every observation to the immutable plan and source commit, hashes raw
  artifacts itself, reports missing phases, materializes the final report, and
  becomes immutable only after the fail-closed verifier passes.
- [x] Add a deterministic physical-relocation restart matrix that abruptly
  drops the relevant data node and reopens the controller at learner
  allocation, partial snapshot, partial catch-up, ready-to-promote, promoted,
  and partial-cleanup boundaries. Require exact catch-up contents, bounded
  resume, source cleanup, and durable completion after every restart.
- [x] Cover the remaining deterministic failure boundaries before hardware
  certification: reopen between split and merge publications; preserve the
  previous valid backup across an incomplete temporary archive; and abruptly
  reopen resumable FTS builds during tokenization, PK merge, impact merge, and
  replacement-generation construction while the old generation stays live.
- [ ] Certify 1 TB, 5 TB, and 20 TB logical datasets on bounded-memory
  `server_paged`.
- [ ] Exercise node loss during snapshot, catch-up, promotion, cleanup, split,
  merge, backup, and index rebuild. Certification format v3 in 1.0.58-beta
  makes every phase fail closed unless its typed evidence identifies an abrupt
  process kill, host power cut, hypervisor crash, or network blackhole; binds
  distinct durable maintenance checkpoints around the loss; observes loss,
  old-identity fencing, repair, and policy convergence in order; runs at least
  1,000 successful quorum operations during the outage; rejects every old-
  identity write; repairs and digest-verifies every affected range; restores
  zero under-replication and zero unavailability; and proves at least one
  fifth of the profile corpus unchanged at a fixed commit watermark with a
  dedicated checksummed raw artifact. This freezes the proof contract; the
  checkbox remains open until all eight production hardware trials pass it.
- [ ] Measure rebalance amplification, foreground p99 impact, recovery time,
  quorum availability, and hotspot response. Certification format v4 in
  1.0.59-beta rejects scalar-only claims: baseline and rebalance windows must
  each run for at least five minutes in order within the report; foreground and
  hotspot p99 values each need at least 10,000 samples per window; recovery
  duration must equal exact timestamps inside the rebalance window; at least
  10,000 quorum operations must all succeed; at least 1,000 minority writes
  must all be rejected; at least 10% of the logical corpus must move; source,
  network, and destination counters must be nonzero and remain within the 4x
  amplification gate; and the observation must reference exact checksummed
  resource, workload-latency, and rebalance-timeline artifacts. The checkbox
  remains open until production runs publish those measurements.
- [ ] Rehearse whole-node restore and cluster expansion with production-sized
  FTS indexes. Certification format v5 in 1.0.60-beta binds a restore to a
  named checksummed backup, a higher node incarnation, an exact report window,
  a fixed commit watermark, matching record/byte/digest values, a constant
  32 GiB peak-RSS ceiling, equal production-sized FTS contents, 1,000
  successful FTS probes, 10,000 successful quorum operations, 1,000 rejected
  old-identity writes, zero unavailable ranges, and exact raw artifacts.
  Expansion must identify and capacity-check every new node, publish a newer
  topology generation and digest, move at least one new node's fair share
  within 4x relocation I/O, keep every node under 32 GiB, collect 10,000
  foreground and hotspot samples, keep p99 regression within 25%, preserve the
  full corpus and FTS index at a fixed watermark, finish with bounded
  placement skew and no active/failed relocations, and reference exact
  hardware, configuration, topology, resource, latency, rebalance, FTS, and
  checksum artifacts. The checkbox remains open until physical restore and
  expansion trials pass this contract.
- [ ] Publish raw artifacts, hardware, topology, configuration, and failure
  timelines. Certification format v6 in 1.0.61-beta atomically writes one
  portable publication manifest only after the report passes and collector
  state becomes immutable. The manifest binds the exact default plan and
  report paths, their sizes and SHA-256 values, source revision, profile,
  completion time, every sorted artifact entry, artifact count and byte total,
  and a canonical whole-bundle digest. Independent verification rejects a
  missing, malformed, symlinked, path-substituted, truncated, reordered,
  self-consistently reduced, or checksum-mismatched manifest. A crash after
  collector finalization but before publication safely resumes manifest
  creation; an existing divergent manifest is never overwritten. The checkbox
  remains open until real run bundles and their immutable public locations are
  published. Certification format v7 in 1.0.62-beta additionally rejects
  opaque or recycled raw files. Every artifact begins with a bounded versioned
  JSON header that binds its run, profile, BicDB version, source revision,
  artifact kind, observation interval, and every planned producer node. The
  remaining payload declares its format, nonzero record and byte counts, and
  SHA-256. Registration and offline verification stream the payload once with
  a 1 MiB buffer and reject an oversized/malformed header, wrong run/kind/node,
  out-of-window observation, truncation, extension, or payload mismatch.
  BicDB 1.0.63-beta closes the producer-side gap with `certify-capture`: it
  copies each source payload exactly once with a fixed 1 MiB heap buffer,
  constructs the run/source/node-bound header itself, fsyncs a private
  completed file, publishes through an atomic no-overwrite hard link, and then
  checkpoints registration. A crash between publication and the state
  checkpoint resumes by verifying the existing output; divergent or opaque
  output is never overwritten. Source/output symlinks, path escapes, empty or
  changing payloads, zero records, invalid/future intervals, and unsafe parent
  components fail closed.
  BicDB 1.0.64-beta also validates typed observations before they can replace a
  durable collector checkpoint. Measurements, saturation, and individual
  failure trials run through the same non-weakenable gate logic used at final
  verification and must reference already verified artifact kinds. Hardware,
  restore, and expansion identity, time, capacity, topology, and artifact
  prerequisites fail early; restore and expansion receive full cross-
  validation as soon as both are present. Rejected evidence leaves the prior
  collector state byte-for-byte authoritative, preventing a multi-day
  physical run from discovering an obvious invalid checkpoint only at finish.
  Certification format v8 in BicDB 1.0.65-beta closes the run-start gap:
  `certify-plan` now requires `server_paged` and a live, fully committed
  metadata-consensus snapshot rather than accepting topology shape alone. The
  immutable plan binds the exact distribution configuration, active schema,
  node incarnations, consensus term/commit/leader/voters, live health and
  replica-byte metrics, and zero pending repair/rebalance work. Five distinct
  server identities, five live active voters, exact RF3 voting replicas,
  bounded skew, no unplaced ranges, and no active or failed relocation are
  mandatory. The effective-configuration and topology-before raw payload
  digests must equal the exact bytes frozen by that preflight, even after
  self-consistent envelope and manifest rehashing. Dirty source trees are
  rejected before collector state is created. This prevents stale,
  resident-memory, schema-divergent, partially replicated, or mid-election
  clusters from starting a multi-day physical campaign.

Exit gate: a five-node, replication-factor-three cluster holds the certified
logical corpus, survives any one node loss, replaces the missing replicas, and
returns to policy-compliant balance without application routing changes.
