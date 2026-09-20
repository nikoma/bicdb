# Range integrity and anti-entropy

BicDB 1.0.30-beta introduced the bounded data-digest primitive used to compare
replicas without retaining row keys or hashes proportional to range size.
BicDB 1.0.31-beta connects that primitive to live `server_paged` replicas under
the durable range-consensus write fence.
BicDB 1.0.32-beta makes those checkpoints node-owned and exposes their bounded
advance operation through the authenticated cluster data protocol v13.
BicDB 1.0.33-beta adds the durable multi-voter run coordinator and its
fail-closed quorum evidence report.
BicDB 1.0.34-beta adds authenticated, cursor-resumable export of one divergent
bucket from a completed node-owned digest.
BicDB 1.0.35-beta adds the destination-owned, quorum-evidence-gated apply
primitive for bounded repair batches.
BicDB 1.0.36-beta adds the crash-resumable external merge coordinator that
derives exact upserts and source-proven deletes without retaining a bucket in
memory.
BicDB 1.0.37-beta adds a fresh, bounded destination scan and exact post-repair
comparison for every repaired bucket.
BicDB 1.0.38-beta adds the bounded range-level repair certificate and makes it
mandatory for the anti-entropy fence-release path.
BicDB 1.0.55-beta adds the durable automatic sweep controller and wires it into
every distributed `bicdb serve` member.

## Digest model

`RangeDigestState` is bound to:

- cluster, node, range, and token interval;
- range epoch;
- a resolved range-consensus log prefix;
- a fixed, power-of-two token-bucket count; and
- an operation UUID.

Records are consumed in deterministic collection/primary-key order. Each
record's canonical serialized bytes are chained into the leaf selected by the
high bits of its stable distribution token. The final root binds every leaf's
index, record count, byte count, and hash as well as the range identity, token
interval, epoch, and resolved consensus index.

The default 4,096 leaves keep memory and comparison work constant as a range
grows. Operators may choose 16 through 65,536 power-of-two leaves under the
state-file byte limit. A different record changes one bounded repair bucket;
identical records produce the same root regardless of scan batch boundaries.

## Resumption and limits

Every scan call has hard record, batch-byte, single-record, bucket-count, and
state-file limits. State contains only bucket summaries and one opaque resume
cursor. `save_range_digest_state` validates and atomically replaces the
checksummed checkpoint. `load_range_digest_state` performs a bounded read and
revalidates all totals, bucket identities, root, and state checksum.

An uncertain batch response is safe to retry. The state binds the previous and
next cursor to an exact batch fingerprint: an identical replay is a no-op, while
different contents at the same cursor fail closed. Duplicated, out-of-order, or
oversized records are rejected.

`ClusterDataNodeService::advance_range_digest` executes exactly one bounded
scan step. It reuses the range relocation scanner, including its opaque
collection/primary-key cursor, and explicitly checkpoints collection advances
that contain no matching records. Empty collections therefore cannot create a
restart loop or a cursor gap.

## Live scan admission

Before reading and again before changing checkpoint state, a live digest step
requires all of the following to match exactly:

- authenticated current leader and local voting-replica membership;
- cluster, local node, range, token interval, and range epoch;
- digest session UUID and durable fence owner UUID;
- an unexpired fence lifetime; and
- range-log `last_index`, `resolved_through`, and the prefix recorded by the
  digest.

There may be no unresolved consensus tail. A missing, replaced, not-yet-valid,
or expired fence fails without advancing the digest. This keeps writes blocked
for the full scan and prevents a resumable job from silently continuing after
leadership, epoch, topology, or consensus-prefix movement.

## Node-owned authenticated checkpoints

Production callers use the `RangeDigestTransport` capability. A request names
the range, epoch, session, limits, current time, and optionally the last
checkpoint checksum observed from the destination. It never carries bucket
contents. The destination reconstructs the range descriptor from its published
topology, passes the ordinary schema compatibility fence, and authenticates the
caller node against its membership-bound mTLS leaf certificate before the data
service is invoked.

Each data node stores its own checkpoint below `cluster-range-digests/` using a
range/session-derived filename. Checkpoints are bounded, checksummed, refused
through symbolic links, and atomically replaced after one scan step. The
directory has a hard retained-session count. A process restart reloads the
exact state before continuing.

The advance checksum is compare-and-set admission against stale coordinators.
After an uncertain response, a request without a checksum returns the current
node-owned state without advancing it. A caller therefore cannot repair a
timeout by replaying stale state, and a malicious or faulty leader cannot
submit fabricated follower bucket hashes.

## Durable multi-voter runs

`RangeDigestRun` is created from a `RangeBackupFenceQuorum`, the current range
descriptor, and explicit digest/run limits. It rejects duplicated or non-voter
observations and any observation that differs from the exact fenced epoch and
resolved prefix. The selected nodes and their progress are canonical and
bounded.

One call advances at most one selected voter by one bounded node-owned step.
Before compare-and-advance, the coordinator always fetches that voter's current
state. If a voter durably advanced but the response was lost before the run
checkpoint, the next coordinator process observes monotonic forward progress
and adopts it. It never resubmits stale bucket state. `advance_and_checkpoint`
atomically saves the checksummed run after every successful step; loading is a
bounded regular-file read that rejects symbolic links, changed limits, damage,
and non-canonical evidence.

After every selected voter completes, the report groups authenticated roots and
unions only the bounded divergent bucket IDs. Outcomes are explicit:

- `healthy`: every selected voter has one root;
- `divergent_certified_source`: one root has the range's required quorum; or
- `divergent_uncertified`: data differs but no root is authoritative.

The last outcome is intentionally terminal for automatic repair. A two-node
disagreement in an RF3 quorum of two does not magically make either responder
truth. Gathering a third fenced voter can produce a certified two-node source
group.

## Governed scheduling

`RangeAntiEntropySchedule` embeds one durable digest run and its explicit peak
memory, in-flight I/O, CPU, and I/O-rate charge. A due tick first obtains an
anti-entropy lane permit from the node resource governor. Saturation performs
no replica RPC and atomically checkpoints a bounded retry time. Admission
permits cover exactly one run step and are released before scheduler state is
written, including when the transport fails.

Successful steps, completion, retry deadlines, failure counters, and pause
reasons are checksummed and atomically persisted together with the run cursor.
Transport failures use capped exponential backoff; too many consecutive
failures pause the schedule for explicit operator resumption. Fence expiration
also pauses before any RPC. A regressing clock fails closed. Schedule files are
bounded regular files and symbolic links, changed limits, corruption, and
oversized error strings are rejected.

## Automatic low-priority sweeps

`AutomaticRangeAntiEntropyController` owns one checksummed cursor per server.
It considers only ranges led by that server, visits them in deterministic range
ID order, and advances at most one bounded digest step per due supervisor tick.
The default QPS ceiling is zero: automatic scans select only ranges currently
reported idle. `PgWireConfig::automatic_anti_entropy_limits` exposes the QPS,
cadence, fence lifetime, state, run, scan, and resource bounds. BicDB adopts a
changed configuration on restart only while no durable run is active; it will
not reinterpret an existing fence or checkpoint under different limits.

Before contacting a voter, the controller atomically checkpoints a plan bound
to the exact cluster, range ID, epoch, token interval, replica set, leader,
placement policy, creation time, and fence expiration. It then obtains the
existing leader-inclusive quorum fence and creates the ordinary durable digest
schedule. A restart safely handles every boundary: plan before RPC, installed
fence before schedule, schedule before controller checkpoint, completed
release before local cleanup, and orphan schedule cleanup. A stale plan cannot
be silently rebound to a changed epoch, leader, replica set, placement, or
relocation.

The server supplies live query, read, and write admission pressure on every
tick. Any active or queued foreground operation defers work before fence
installation and between digest steps. The resource governor independently
reserves the anti-entropy step's declared memory, I/O, CPU, and rate charge
without consuming consensus or foreground-write headroom. Only one range is
active per local controller, every successful range has a configured delay,
and full sweeps have a configured interval. Not-due controllers perform no
checkpoint write. A newly initialized controller waits one complete sweep
interval before its first plan so bootstrap, schema convergence, and initial
rebalancing are not preempted by maintenance.

A healthy report releases the selected voter fences remote-first and leader
last. An inconclusive or exhausted schedule releases the fence and records the
reason. Certified or uncertified divergence is different: the complete report
is checkpointed, the exact fence remains held, further automatic ranges stop,
and an operator must complete the certified repair workflow. No responder is
selected as truth merely because it answered first.

## Bounded divergent-bucket export

`RangeDigestTransport::export_range_digest_bucket` streams one physical scan
step from one completed digest voter. Requests and responses bind the exact
source node, range, epoch, digest session, bucket, and previous opaque cursor.
The destination reconstructs the range from published topology and authenticates
the caller through the same membership-bound mTLS path as digest advancement.

The source loads its own regular-file digest checkpoint and refuses export
unless the manifest is complete. Before and after every bounded storage read it
revalidates the original unexpired write fence, leader and voter membership,
range identity, epoch, and resolved consensus prefix. A fence expiry, topology
change, prefix movement, or missing checkpoint therefore cannot produce a
partially trusted frame. Protocol v14 evaluates expiry with the destination
node's clock for both digest advancement and bucket export; caller-supplied time
is never trusted as fence authority.

Every frame advances the underlying collection/primary-key cursor even when no
record in that storage batch belongs to the requested token bucket. It carries
at most the digest's configured record and byte limits, declares its exact
canonical serialized-row byte total, and is checksummed over its full identity,
cursor, collection, and records. Records must be strictly primary-key ordered
and each is independently routed back to the requested bucket during
validation. Lost responses are replayable from the last accepted cursor;
damaged, reordered, oversized, cross-bucket, or wrong-session frames fail
closed.

## Certified destination apply

Cluster data protocol v15 adds `RangeDigestRepairTransport`. A repair batch is
accepted only when its complete digest report has a quorum-certified source
root, the selected source is a member of that quorum, the destination is a
current voter with a different reported root, and the requested bucket is in
the report's canonical divergent-bucket set. Every evidence node must still be
a voter in the exact current range epoch and the report's required quorum must
equal the range's current majority.

The destination independently loads its completed node-owned digest and
requires its root to equal the report's destination evidence. It then checks
the original durable fence, leader identity, range epoch, and resolved prefix
before admission, immediately before the row transaction, and after commit.
RPC admission uses destination-node time. A report or batch supplied by an
unrelated node, range, epoch, session, root, or expired fence cannot mutate
rows.

Each batch carries a repair UUID, a contiguous sequence, the previous batch
checksum, exact serialized mutation bytes, and a checksum over its authority
and full contents. Mutations are strictly ordered and unique, and every upsert
or delete is independently routed back to the one authorized bucket. Record,
mutation-count, batch-byte, checkpoint-byte, and retained-session limits are
explicit.

The destination applies an admitted batch as one ordinary BicDB transaction,
including WAL, indexes, and constraints, with only the narrow physical-
replication admission bypass. It then atomically replaces a checksummed,
regular-file node-owned checkpoint. A crash after row commit but before that
checkpoint is safe because exact upserts and deletes are idempotent; replay of
the last accepted batch returns its existing state, while gaps, conflicting
retries, older sequences, and writes after input completion fail closed.

`input_complete` means only that the external merge emitted all mutations. It
does not release the fence and does not mark the replica healthy. A fresh
destination digest must match the certified source root before either action
is permitted.

## Disk-backed repair merge

`RangeDigestRepairRun` stages the certified source and divergent destination
bucket streams as immutable, checksummed, cursor-bound files. Each advance
fetches at most one bounded frame or performs a configured amount of two-way
merge work. The merge holds only the current source frame, destination frame,
and one bounded mutation batch in memory. Source-only or changed records become
upserts; destination-only records become source-proven deletes.

Source frames, destination frames, and immutable repair-batch files share one
explicit disk-byte budget. They also have individual file bounds, per-side
frame-count bounds, a total step bound, a merge-work bound, and a bounded
checksummed state file. A per-run OS lock permits only one coordinator across
artifact creation, remote apply, and local checkpoint replacement. The lock is
released automatically after process failure.

With durable checkpointing enabled, a new frame or mutation batch reaches
stable storage before the state can reference it or the destination can apply
it. Recovery adopts a valid orphan frame without exporting it again and
replays an immutable pending batch after an uncertain apply response. The
destination's contiguous idempotency checkpoint makes row-commit/local-
checkpoint and RPC-response loss safe. Symbolic links, changed limits, damaged
files, non-canonical ordering, cursor gaps, and disk-budget exhaustion fail
closed.

Finishing the merge starts a new destination export; the pre-repair destination
stream is never reused as verification evidence. Its immutable frames share the
same disk budget as the source, old destination, and mutation batches. A
bounded two-stream comparison checkpoints both cursors and reaches `verified`
only when collection names, record IDs, and complete canonical records match
the certified source stream exactly. Missing, extra, or changed rows fail
closed. Checkpoints written by 1.0.36-beta resume into this new phase without
discarding staged data.

`verified` certifies one repaired bucket, not the entire range. Staged data is
not a new authority source, and applied writes are not by themselves a health
certificate. The original fence remains held until every reported divergent
bucket on every divergent voter has a verified run and the combined range root
matches the quorum-certified source root.

## Range-level certification and fence release

`RangeDigestVerifiedBucketEvidence` rebuilds the source bucket hash, row count,
and serialized-byte count from the verified run's immutable source frames using
the same hash chain as the full digest. It binds that summary to the repair run
checksum and complete quorum authority report.

`RangeDigestRepairCertificate` requires the original node-owned manifest for
every node in the report. Quorum-source manifests must retain the certified
root and may not carry repair evidence. Every divergent voter must provide
exactly one verified evidence item for every reported divergent bucket, with no
duplicates or extras. The certificate replaces only those buckets in that
voter's original manifest and recomputes the complete range root. If any voter
does not reconstruct the quorum-certified source root, certification fails.

Replica count, evidence count, digest fanout, and serialized certificate bytes
are explicitly bounded. Certificates are checksummed, atomically written, read
through regular-file/no-follow admission, and revalidated by reconstructing
their roots after restart. The range coordinator's anti-entropy release method
accepts a certificate only when its cluster, session, range, epoch, resolved
prefix, quorum, and sorted covered nodes exactly equal the active durable fence
evidence. It then uses the existing remote-first, leader-last idempotent fence
release path; partial release remains safe to retry.

## Safety boundary

A digest is comparable only when cluster, range, token interval, epoch,
resolved consensus prefix, and fanout match. `compare_range_digest_manifests`
refuses every other pairing and returns only the bounded list of divergent leaf
IDs.

The run certifies which root has quorum, nodes export bounded bucket records,
the disk-backed coordinator derives, applies, and freshly verifies complete
bounded upsert/delete streams, and a complete range certificate gates fence
release. Selecting the fastest responding replica as truth remains forbidden.
