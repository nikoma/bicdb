# Distributed SQL: clock-free locking alternative

**Proposal only; unimplemented and not tested.** This amendment supplies an
alternative missing from the [minimum execution contract](distributed-sql-minimum-execution-contract.md),
whose sections 3–4 require one global decision prefix and validate locks against
that prefix. The [original report](../distributed-sql-design-2026-11-11.md#5-isolation-snapshots-and-atomic-visibility)
mentions strict claims, but does not define a locking execution alternative to
its shared prefix or partitioned timestamp model. The existing prefix design
remains the documented default until a protocol choice is reviewed.

The objective remains ordinary PostgreSQL applications, arbitrary supported SQL
and joins, and atomic multi-record writes without application shard management.
Locality remains an optional optimization. This proposal does not deliver that
objective, silently substitute a narrower product requirement, or establish HA.

## Actual source boundary

Source inspection pins main **1.0.511-beta**,
`74f17b912cec7f2fe093d10dcf1618a43b35f454`, and the separate native candidate
`503964ae93aa22f4c76b698a549b6ef857c834be`. No SQL build or test ran.

- PGwire [`lib.rs`](../../crates/bicdb-pgwire/src/lib.rs),
  `acquire_postgres_statement_snapshot`, still acquires the local SQL view.
- [`RecordReadSnapshot`](../../crates/bicdb-core/src/db/record_read_snapshot.rs)
  is bound to one database instance and prefix. Neither its local MVCC sequence
  `B` nor a range log position `R` identifies a cluster-wide snapshot.
- [`distribution_query.rs`](../../crates/bicdb-core/src/distribution_query.rs),
  `execute_distributed_transaction`, explicitly requires a supplied protocol;
  its only implementation found is the cfg(test) `RecordingCommitProtocol`.
  Ordinary SQL does not call it. It also omits the uncertain failed-prepare
  recipient from its acknowledged-prepared-prefix abort loop.
- [`distribution_range_consensus.rs`](../../crates/bicdb-core/src/distribution_range_consensus.rs),
  `RangeWriteCoordinator::resolve_range`, still refuses spanning final-row
  writes. It supplies neither SQL read locks nor recoverable SQL prepared intents.

The native candidate does not turn these hooks into the proposed protocol.
No production SQL participant/lock service, replicated SQL transaction home,
coordinator-crash resolver or reference-tested cross-range SQL executor was found.

## Rigorous two-phase locking, without a global clock

A serializable implementation can use actual shared/exclusive locks over keys,
missing-key gaps and scan predicates, holding both read and write claims until
the transaction's terminal resolution. Every reader and writer of admitted SQL
objects must participate, including local fast paths, indexes, unique/FK
authorities, triggers, audit/outbox effects and other write APIs. Ordinary local
Get, stale followers or direct SQL execution cannot bypass those claims.

Before returning a value, the current range owner acquires the real lock and
establishes a committed/applied read barrier. It reads a locally pinned view
created after that barrier, plus the transaction's own intents. Reusing an older
MVCC snapshot after acquiring a new lock is unsound. Recipients independently
validate authority, schema/object identity and lock generation. Local `B_r`
values may differ: the conflict order, not equal numbers, provides serialization.
For a completed read, all relevant values/predicates remain protected until the
read has finished. Failures never become successful partial join results.

Hash partitioning does not make a tenant predicate into a key interval. Until
exact predicate/index claims exist, scans must conservatively lock every
relevant logical table/range domain, including absence and insertion space.
Schema/membership guards prevent newly created ranges or objects from escaping
the footprint. This can block heavily and is not a performance result.

Durable transaction homes may be partitioned by stable TxID across independently
replicated groups. Each home retains enlistment, exact participant/intention
digests, coordinator fencing generation and one immutable COMMIT or ABORT.
No total decision order across unrelated transactions, global HLC, or per-write
topology-metadata consensus entry is required by this locking proposal. Current
owner routing and restore/schema fencing remain necessary. Shared hot keys,
large scans and transaction-home placement can still limit capacity.

Participants persist prepared row/index/effect intents and their lock ownership
before replying. Leader replacement must recover those claims before admitting
conflicting reads or writes. After COMMIT, each participant applies the exact
decision and makes its mutations visible before releasing its conflicting locks.
A reader encountering a participant awaiting application waits for resolution
or fails explicitly; it cannot read the old value. Thus staggered application
does not become a completed fractured read. Client COMMIT success still waits
for all required durable visibility receipts. Lost replies remain uncertain.

Coordinator crash, caller cancellation and timeout cannot erase prepared claims
or choose a different outcome. Independent resolvers discover every enlisted or
uncertain recipient, recover the decision from the same replicated home and
finish idempotently. Decision unavailability can block progress; this proposal
does not claim nonblocking atomic commit. Deadlock handling needs stable durable
priorities or a real distributed detector. A prepared victim cannot be aborted
unilaterally. No automatic business-operation replay follows from a deadlock.

## Isolation and movement limits

Rigorous locking can supply serializable histories, but does **not** by itself
reproduce PostgreSQL's fixed first-statement MVCC snapshot. A transaction can
read A, overlap an unrelated commit to previously unread B, then lock/read B's
new value while remaining serializable. PostgreSQL REPEATABLE READ would require
the earlier snapshot of B. Preserve each requested isolation contract explicitly;
do not silently relabel that behavior as compatible REPEATABLE READ. Supporting
fixed views needs a separate retained-cut protocol or sufficiently broad locks
established at the view boundary. READ COMMITTED statement/portal behavior and
Oracle semantics also require independent validation. The global-prefix design
remains one option for those views without requiring a physical global clock.

Initially, actual range movement must wait for SQL claims/prepares to drain or
refuse. Online handoff requires durable transfer of lock ownership, intents,
transaction-home bindings and retry lineage before new authority can serve.
Native image receipts, ordinary MVCC pins and process-local lock handles do not
implement that transfer or reconstruct distributed SQL permission after restart.

## Required distinguishing controls

No controls below were executed by this review:

- Stock PG clients, joins and multi-table writes across separated ranges;
  phantom/absent-key, unique/FK, read-your-writes and exact-payload comparisons.
- Pause COMMIT application on B after A applies; a joined reader must block,
  resolve or fail, never complete with A-new/B-old. Crash the coordinator at
  every prepare/decision/apply boundary, including a lost Prepare reply.
- Lose/restart a range leader holding prepared locks; conflicting operations
  remain fenced until the same durable decision resolves. Check deadlock victims,
  cancellation, stale routes, portals and original transaction identities.
- Contrast dynamically acquired locks with an actual fixed-snapshot RR control;
  test PostgreSQL and Oracle separately. Preserve external fsynced attempt/ACK
  evidence and all failed/uncertain operations before any qualification claim.

This amendment changes no capability status, source version, runtime, release
or deployment. It does not remove the original physical scale-out, zero-lost-ACK,
read-consistency or bounded fault-safe recovery gates.
