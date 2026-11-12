# Distributed SQL design: ordinary clients, durable transactions and logical locality

Design baseline: **1.0.460-beta**, source
`c173fa783fb8323fe02c41fb7eda96f130bc29c3`.
Status: design for review, **not an implemented capability or qualification**.
This document adds no runtime behavior. The [maturity audit](ha-horizontal-scaling-audit-2026-11-10.md)
remains historical evidence at its named source; the native voter configuration
repair merged after it does not supply a distributed SQL executor.

The product contract is: an ordinary PostgreSQL client connects to any healthy
SQL gateway; business transactions remain atomic across arbitrary physical range
boundaries; joining a node changes placement without changing application SQL.
Logical locality can reduce network work, but moving a related record to another
range cannot weaken correctness. Neither clients nor generated application code
must carry range IDs, choose leaders, or retry partially completed business work.

## Subsequent integration constraints

Source inspection through public **1.0.478-beta**,
`f3a063902d2fe316c29264fe20549c2fdd270242`, leaves this document's distributed SQL
contract **design only**. Later local SSI, physical namespace identity and private
range-recovery work add useful components but do not implement its executor or
transaction registry. Keep the following boundaries explicit when composing them:

- Local PostgreSQL SSI dependencies and predicate tracking protect one database
  transaction domain. They do not collect conflicts, retain predicates or decide
  serialization across independent range participants.
- A physical target-local namespace UUID identifies an actual backing store.
  Replicas keep their own UUIDs. Distributed SQL additionally needs an
  authenticated mapping from stable logical/source object identity to that
  actual target backing, including rename, drop/recreate and restore. A table
  name, source OID or matching schema fingerprint cannot create that mapping.
- Native image `Prepared`, `Ready` and recovery `Complete` describe materialized
  range history and recovery refusal/activation boundaries. They are **not SQL
  two-phase-commit participant votes or global transaction decisions**. A
  recovered range cannot unilaterally resolve an in-doubt SQL prepare.
- Range positions and logical history digests remain range-scoped. The actual
  local database sequence used for an MVCC read is also local; copying a range
  can map range position 2 to target visibility sequence 1. Neither number is
  the global read-view prefix `S` proposed below.
- An authenticated host metadata quorum observes control-plane state. Even a
  future individually linearizable point-read API would not create a shared SQL
  snapshot across several ranges. The read-view service must still prove the
  appropriate common cut and retain its history.
- Native no-data abort recovery and a distributed SQL ABORT decision are
  distinct protocols. A timeout, raw `Aborted` slot or coordinator disappearance
  cannot replace the durable terminal decision and participant resolution in
  section 4.

The [component evidence matrix](recovery-component-evidence-2026-11-11.md)
separates exact tested revisions from uncompiled compositions. The
[operator integration audit](design/native-operator-four-to-five-gap-audit-2026-11-11.md)
keeps runtime startup, strong-read routing, image transfer and data-quorum handoff
open independently of this SQL design. None of those receipts qualifies the
single global decision domain's capacity, general joins, distributed isolation
or multi-range atomic SQL writes.

## 1. Existing code and missing boundaries

| Existing boundary at the baseline | What can be reused | What must be added |
|---|---|---|
| [`distribution.rs`](../crates/bicdb-core/src/distribution.rs), `distribution_key_token` | Versioned collection/record hash, complete token coverage, durable topology, range identities and epochs | Logical routing-key policy with versioned migration; query planning cannot infer tenant colocation from the current hash |
| [`distribution_routing.rs`](../crates/bicdb-core/src/distribution_routing.rs), `RoutedRequestHeader`, `RouteRetry`, `ClusterRequestRouter` | Monotonic generation cache, epoch validation, structured redirects | Transaction/fragment identity, schema and authorization context, read-view identity; retry classification tied to durable outcome |
| [`distribution_gateway.rs`](../crates/bicdb-core/src/distribution_gateway.rs), `ClusterPointConnector`, bounded pool | Route refresh and reusable bounded node connections | Typed SQL fragment RPC and session gateway; no general SQL data path is provided by the generic connector trait |
| [`bicdb-pgwire/src/server_cluster.rs`](../crates/bicdb-pgwire/src/server_cluster.rs), `plan_cluster_*` | Point/scatter and shard-local planning hooks | Bind parsed SQL to distributed plans in simple and extended protocol; preserve portals, COPY, errors and transaction state |
| [`distribution_query.rs`](../crates/bicdb-core/src/distribution_query.rs), `DistributedShardExecutor`, scatter/merge helpers | Bounded worker/result channels, explicit result limits, ordered merge, numeric/FTS helpers | Executable typed relational fragments, range-filtered scans, consistent snapshots, distributed joins and complete SQL type semantics |
| Same file, `DistributedCommitProtocol`, `execute_distributed_transaction` | Explicit prepare → durable decision → participant completion ordering | Actual replicated coordinator/status storage, persistent participant intents, recovery workers and visibility rules. The only implementation found is test `RecordingCommitProtocol` |
| [`distribution_range_consensus.rs`](../crates/bicdb-core/src/distribution_range_consensus.rs), `RangeWriteCoordinator::resolve_range` | Current leader/schema admission, quorum command durability, suffix repair, epoch recovery | Command variants for transactional intents/outcomes. Existing final-row commands reject spanning writes and cannot be used as invisible prepare records without changes |
| [`db/record_read_snapshot.rs`](../crates/bicdb-core/src/db/record_read_snapshot.rs), `RecordReadSnapshot` | Owned bounded local snapshot/pending overlay and pin lifetime | A cluster read-view certificate and global-to-local visibility mapping. The current handle is bound to one database instance; a local commit sequence is not a cluster timestamp |
| [`db/collection_write_identity.rs`](../crates/bicdb-core/src/db/collection_write_identity.rs) | Reject stale buffered writes after collection-name reuse | Durable globally stable schema object/backing identities; local `Arc` capability identity cannot be sent to another process |
| [`session/transaction_defaults.rs`](../crates/bicdb-sql/src/session/transaction_defaults.rs) | Existing profile-specific isolation validation | Enforce the same advertised level across all participants; never accept a label whose cluster semantics are absent |

The `DistributedCommitProtocol` trait has no recovery enumeration or decision
lookup method. Its comment calls for recovery, but returning an error containing
“needs protocol recovery” neither discovers abandoned transactions nor resolves
them. Replacing only the recording implementation leaves these omissions.

## 2. User-visible and wire contract

The gateway owns authentication, SQL parsing/binding, session settings, prepared
statement/portal identity and a cluster transaction ID. Remote peers receive a
bounded typed plan fragment with the exact schema version and a scoped execution
capability; they do not accept untrusted caller-supplied SQL roles as authority.
Membership mTLS authenticates a node, not the application's row/table permission.
Both gateway and participant validate policy/schema requirements. Current RLS,
tenant masking and provenance tests remain mandatory at the remote boundary.

A client sees one PostgreSQL result stream. Internal route changes are hidden
only when retry is safe: a refused-before-execution fragment may refresh route
and retry; an interrupted write first resolves the same operation identity.
Do not replay an arbitrary SQL transaction after it has emitted a result, run a
volatile function/trigger, consumed a sequence, or caused an external effect.
A connection loss around COMMIT is uncertain, as it is for ordinary database
clients. A durable transaction receipt can resolve it; automatically minting a
new transaction ID would risk duplicating the business operation.

Simple protocol, Parse/Bind/Execute/Sync, unnamed/named portals, transaction error
state, cancellation, cursor suspension, COPY, SET LOCAL, savepoints and function
calls need explicit capability gates. A local fast path may be retained only
when it proves the entire operation's scope, isolation and side effects; routing
one SQL statement locally is insufficient to pin a multi-statement transaction.
Unsupported operations fail before side effects with stable SQLSTATEs and clear
feature errors. Do not silently return the local subset of a global query.

## 3. Proposed service boundaries

Names below are proposed modules, not existing APIs:

- **Gateway planner/executor:** resolve logical objects, build distributed plans,
  register transaction/statement identities, retain session semantics and apply
  budgets before network fan-out.
- **Range SQL service:** execute range-filtered scans and intent operations under
  authenticated schema, read-view, epoch and resource authority. No peer opens a
  second writer on the same database directory.
- **Replicated transaction registry:** linearizable transaction state, frozen
  participant set, coordinator generation, terminal decision and acknowledgment
  receipt. Route/ownership follows normal cluster membership fencing.
- **Participant intent store:** durably prepared row/index/constraint mutations,
  read dependencies and conflict ownership, recovered through range quorum.
- **Recovery/resolution supervisor:** bounded enumeration of in-doubt work,
  decision lookup, finalization, intent cleanup, retry/backoff and observable
  blocked states independent of a client socket or SQL mutex.
- **Read-view service:** obtains and validates a consistent cluster cut, retains
  history pins, maps it to local snapshots and follows split/relocation lineage.

Topology metadata consensus remains control-plane authority. It must not become
an unbounded per-row transaction journal. Transaction records need their own
explicit replication/retention format and resource budgets, even if the first
implementation uses the same underlying durability machinery.

## 4. Durable transaction protocol

### Registry and participant records

A transaction ID includes cluster identity and a collision-resistant identifier.
A registry record contains protocol version, owner generation, schema epoch,
isolation mode, idempotency operation identity (if supplied), deadline, immutable
participant list/digests after freeze, terminal decision, and completion receipts.
Before freeze, participant enrollment is compare-and-swap durable: a participant
cannot prepare for a transaction whose registry excludes it.

The participant key includes transaction ID plus durable logical range identity.
Its record binds current range lineage, collection backing IDs, read/conflict
set, exact write intent digest, prepared quorum evidence and outcome. Repeated
requests for the same identity and digest are idempotent; different bytes under
the same identity fail closed. Node IDs and a transient leader epoch alone are
insufficient because leadership and placement may change during recovery.

### State machine and order

```text
registry:   ACTIVE -> PREPARING -> COMMIT_DECIDED -> COMPLETE -> GC_ELIGIBLE
               \          \-> ABORT_DECIDED -> COMPLETE -> GC_ELIGIBLE
                \------------> ABORT_DECIDED
participant: NONE -> INTENTS_DURABLE -> COMMITTED/APPLIED or ABORTED -> RESOLVED
```

1. Register before dispatching any persistent side effect. Enlist participants
   before they create intents. Reads see their own intents within the transaction.
2. Acquire/revalidate conflict ownership, including unique/FK/predicate claims.
   The final participant set includes hidden index/constraint/outbox writes,
   not only rows named by the SQL text. Freeze its canonical digest durably.
3. Each participant quorum-persists invisible intents and a prepared receipt.
   “Prepared” must survive leader/process replacement and preserve conflict
   protection; an in-memory `Transaction` is not the durable state.
4. The registry may record COMMIT only with valid prepared receipts for every
   frozen participant and successful isolation validation. A terminal decision
   is a linearizable single winner: it cannot be replaced by a newer owner,
   deadline, route generation or abort request.
5. Participants resolve against that exact registry authority and durably apply
   or discard intents through ordinary storage/index/outbox safety boundaries.
   Completion is idempotent after response loss. Readers must not observe a
   fractured transaction while physical applications occur at different times.
6. Initially return successful COMMIT only after terminal decision quorum and
   durable apply/visibility receipts from every participant. This conservative
   acknowledgment rule simplifies the first proof; it increases latency and
   means a missing participant may delay a known committed result. Any later
   early-ack optimization needs independent proof that failover can recover the
   decision and exact prepared payload on every necessary range.

Do not map an existing per-range command's provisional state directly to a SQL
prepare. Current range logic may abort a merely provisional decision. Once a
participant votes prepared in distributed commit, it cannot unilaterally abort
because a timer fired or the original coordinator vanished.

### Failure and takeover rules

| Boundary | Required outcome |
|---|---|
| Gateway dies before registry enrollment | No participant can prepare a hidden transaction |
| Participant prepares but its response is lost | Retry/probe yields the same durable receipt; abort decision must still reach it |
| New coordinator observes PREPARING | Recover exact frozen participants and receipts; decide only under a new fenced registry owner generation |
| COMMIT decision response is lost | Query the registry; timeout is not evidence of ABORT |
| Coordinator dies after decision but before applications | Independent resolver completes the immutable decision; no new business transaction is fabricated |
| Registry quorum unavailable | Prepared transactions remain in doubt; preserve intents/conflicts, report availability loss and bounded resource pressure |
| Late prepare after ABORT | Reject; it cannot resurrect or create a new participant |
| Expiry/cancellation | Ask registry to choose ABORT only if no terminal decision; cancellation after COMMIT cannot undo it |
| Client retries its stable operation ID | Return or await the same transaction receipt; different request digest conflicts |
| Cleanup crashes | Repeat bounded exact cleanup without dropping proof still needed by an unresolved participant |

A lease expiration can allow coordinator takeover, never prove that an absent
commit record is an abort. Registry GC requires participant-resolution receipts,
history/backup pins and a retention policy that rejects ancient unknown retries
instead of treating them as new. Local WAL/log truncation must retain all
unresolved intent/decision payload and authority, including across backup restore.

## 5. Isolation, snapshots and atomic visibility

Two-phase commit provides one decision; it does not by itself prevent write skew,
phantoms, stale reads or partly visible application. A concrete visibility design
must be selected and proved before implementing a claim of global SQL snapshots.

For the first correctness implementation, the recommended conservative design
is **one replicated transaction decision domain with a monotonic decision order**.
All writes, including single-range writes that can affect distributed SQL, enter
that domain. A read-view certificate names a quorum-observed decided prefix `S`.
Committed versions/intents carry their decision identity; a range serves `S` only
after proving all relevant decisions through `S` are materialized or resolvable.
The decision stream supplies participant lists and continuous progress, so a
range cannot guess that an unrelated missing sequence is safe to skip.

This is a deliberate design tradeoff, not a scale-out success: registry consensus
becomes a throughput/availability dependency. Batched decisions may amortize it,
but no throughput assertion follows. A single-range bypass without an equivalent
ordering proof would destroy the cluster snapshot. Do not promote this design as
meeting the user's capacity objective until the measured registry headroom and
fault behavior support the intended workload.

An alternative is partitioned transaction registries with a distributed timestamp
and closed-frontier protocol. That needs a specified timestamp allocation,
clock/uncertainty model (or clock-free ordering), dependency validation, safe
frontier advancement across outstanding prepares and history retention during
partitions. Independent local `commit_seq` values, wall clocks and the current
`RecordReadSnapshot` cannot stand in for that protocol. This alternative remains
an explicit design decision before removing the initial ordering bottleneck.

Snapshot semantics alone are insufficient for SERIALIZABLE. The first supported
read/write concurrency design must enforce conflict validation across ranges and
predicates. A conservative initial implementation can use durable strict
read/write/predicate conflict claims with a deterministic global transaction
priority and wait-die policy. Claims survive participant failover; no lease expiry
may release a prepared transaction's claims before a durable outcome. Broad
predicates may conservatively claim a whole range until finer interval/index
claims are proved. This limits concurrency but preserves phantom protection.

For READ COMMITTED, obtain a new consistent read view at each statement boundary;
for REPEATABLE READ, retain one transaction view plus own writes. Reject other
levels until their actual profile contract is implemented. Preserve Oracle's
existing separate semantics instead of inferring them from PostgreSQL labels.
No design here claims PostgreSQL SERIALIZABLE equivalence from the presence of
an isolation enum.

History pins have byte/time/count limits. A slow query exceeding its pin budget
fails with an explicit retryable outcome; compaction cannot silently invalidate
its view. For distributed writes, a pin/cancellation error before decision can
abort; after decision it only affects waiting, not the outcome.

## 6. Query execution and joins

Use the existing SQL parser/type binder to produce a typed relational plan.
Push predicates/projections to range services only when SQL semantics match
exactly: collation, NULL behavior, numeric overflow, time zones, tenant/RLS
filters and volatility are part of the fragment contract. Every scan includes
its range's token interval and pinned schema/read view; replica copies must not
produce duplicate rows during relocation.

Start with distributed scans and exact ordered/aggregate merge, then bounded
joins. Existing `DistributedNumericAggregate` uses `f64`; it cannot silently
replace exact NUMERIC/DECIMAL SQL aggregates. Global DISTINCT, GROUP BY, LIMIT,
ORDER BY and NULL ordering require global operators; a local top-K pushdown must
be proven valid for each plan shape. No partial result is allowed as an ordinary
successful SQL result; approximate/partial search must require an explicit API.

For joins, a coordinator may initially stream one bounded side and spill encrypted
partitions for a hash join, repartitioning both sides by typed join keys when
needed. Do not fetch whole ranges into unbounded `Vec`s or rely on whichever
side happens to be local. Correctness must cover INNER/LEFT/RIGHT/FULL, SEMI/ANTI,
NULL keys, duplicates, correlated subqueries, lateral dependencies and
cross-range foreign-key lookups. Unsupported forms are rejected before work,
then added with the same reference tests. Budgets include coordinator metadata,
network frames, buffered rows, spill bytes, concurrent fragments and retained
snapshot pins. Cancellation closes children and releases pins; it does not
abort a transaction already decided COMMIT.

A prepared plan binds durable logical object IDs and schema versions. DROP/CREATE
under the same name cannot retarget old work to a new backing. Online schema
rollout must invalidate/rebind or fence every outstanding fragment/transaction;
a heartbeat fingerprint alone does not resolve schema lifetime.

## 7. Locality, relocation and derived state

A proposed logical locality declaration names a stable domain plus key expression,
for example domain `tenant` with typed `tenant_id`. Related tables can reference
the same domain; the compiler must validate nullability, parent lookup, immutable
key rules and authorization rather than trust a free-form annotation. SQL syntax
and schema storage are not yet selected. Carrier annotations should compile into
versioned database metadata, never physical shard assignments.

Routing hashes `(locality policy version, domain, encoded logical key)` when the
policy is valid; rows without a common policy retain distributed correctness.
Changing a locality key or policy is an online routing/data migration with dual
lookup or fenced cutover and duplicate suppression, not a reinterpretation of old
keys. A locality hint may make invoice/lines likely to execute in one range;
it cannot guarantee they will forever fit one range or waive multi-range commit.
Cross-tenant transactions and globally unique business keys remain supported
through the same atomic protocol.

Initially, topology changes may conservatively defer a range's promotion/split
while it has unresolved prepares or snapshot pins. Other ranges continue moving.
This is safe only with bounded admission/backpressure and observable blocked
reasons; it does not yet provide unrestricted online relocation. The follow-up
mechanism must copy intents, conflict state, pinned version history and recovery
receipts before epoch cutover. Registry participant identities follow durable
range lineage; new leaders resolve old intents. Split/merge cannot orphan a
participant or reinterpret its write digest. A source can be removed only after
that recovery authority and required versions are durable on replacements.

| Subsystem | Required distributed transaction/movement integration |
|---|---|
| Unique indexes and foreign keys | Enlist conflict authorities even when their keys hash to different ranges; no per-range-only uniqueness advertised as global |
| Ordinary indexes | Intent changes must become visible with the row decision; rebuild/repair honors pending versions and schema epoch |
| FTS | Index tokens and global statistics need a stated snapshot/freshness contract. Eventual FTS must be explicit; transaction-sensitive search cannot silently use stale corpus statistics |
| Triggers, routines, sequences | Execute logical effects once per stable operation; preserve volatile results and sequence allocation semantics across safe retries. Remote routing must not invoke trigger bodies twice |
| Jobs/broker queues | Transactional enqueue/receipt is a participant or a durable outbox projection of the same global decision. Delivery remains at least once; external service effects need idempotency |
| Audit/original journals | Preserve origin identity and transaction outcome through prepare/apply/recovery. Do not publish an audit “commit” at prepare, omit it on later recovery, or duplicate it after acknowledgment loss |
| Backups/restore | Capture registry decisions, intents, lineage and history pins at a compatible cut. A restored node with rows but missing transaction authority cannot admit writes |
| Cell HA | Separate security boundary. This design neither grants Cell cross-database capabilities nor enables synchronous Cell quorum or encrypted `server_paged` |

## 8. Implementation sequence and executable acceptance gates

The target names below are **proposed tests**, not commands that currently pass.
Every gate records full source/version, exact binary SHA-256, topology/schema,
fsync/storage settings, command lines, failed calls and externally durable intent
and acknowledgment ledgers. A test harness may know ranges to force a boundary;
the application client must use stock SQL through a single endpoint.

| Stage | Deliverable | Required acceptance before enabling the capability |
|---|---|---|
| B0: contract and durability formats | Versioned typed RPC, logical IDs, registry/intent format, resource bounds, error map and recovery-state inspector | Model/state-machine tests exhaust crash/duplicate/reorder/late-abort paths; old readers reject new persistent formats; no undocumented optional field restores authority |
| B1: transparent point path | Authenticated SQL gateway forwarding and session transaction identity | Existing simple/extended/COPY/function-call regressions unchanged; move a leader between Parse, Bind and Execute; arbitrary gateway returns the exact result or an explicit failure, never a local subset |
| B2: registry/participants | Concrete durable decision service and prepared intents, independent resolver | Kill coordinator before/after every durable state publication; kill/restart participant at prepare/apply/receipt; every external acknowledgment survives; uncertain operations are all-or-none; no orphaned hidden transaction or unilateral abort |
| B3: read views/isolation | Decided-prefix visibility, history pins, cross-range conflict validation | Paired-row invariant under concurrent transfers; READ COMMITTED statement refresh; repeatable snapshot; write-skew/phantom/deadlock cases; stale replica/leader and slow pin cases. Run PostgreSQL and Oracle controls independently |
| B4: cross-range relational execution | Global scans, exact aggregates/order, bounded joins/subqueries | Compare seeded reference answers for NULL/duplicates/numeric/collation/security cases; cap memory/spill/fan-out; inject slow/failed range and cancel; no successful partial answer |
| B5: locality and online topology | Versioned logical locality plus intent/history-aware relocation/splits | Same accounting workload with no locality, colocated layout and forced cross-range layout; identical atomic results. Move/split with prepared transactions; epoch probes reject old authority; data/index/outbox/audit digests reconcile |
| B6: independent qualification | Pinned release on independent machines; unchanged business app | Four→five growth and failure experiments, fixed latency/error budget, read/write/mixed goodput and p50/p95/p99. A safe but globally bottlenecked registry does not pass elastic capacity merely because transactions commit |

A minimal reproducible SQL fixture has `accounts(id, balance)` and
`receipts(operation_id PRIMARY KEY, from_id, to_id, amount)` with account keys
chosen by the **test fixture** to span ranges. A stock PostgreSQL driver executes:

```sql
BEGIN;
UPDATE accounts SET balance = balance - $3 WHERE id = $1;
UPDATE accounts SET balance = balance + $3 WHERE id = $2;
INSERT INTO receipts VALUES ($4, $1, $2, $3);
COMMIT;
```

The harness durably records attempts before COMMIT and acknowledgments after
success. It verifies total balance, per-operation expected debits/credits and
exactly one matching receipt after each recovery; totals alone could conceal
one missing debit plus a compensating error. Concurrent reads must never observe
a successful fractured snapshot. Repeat with multi-table invoice/FK/unique
constraints, transactional jobs and original audit records, preserving original
application assertions. Unknown client outcomes are reconciled separately from
acknowledged loss. A retry is valid only with the same operation identity and
payload; ordinary client transaction retries remain whole-business-operation
retries, not app-managed shard routing.

## 9. Decisions still required and non-negotiable proof obligations

- Choose the initial decision-domain deployment and its measurable capacity
  envelope; define how registry quorum/leader recovery differs from the existing
  final-row range protocol. No off-the-shelf label proves this new state machine.
- Specify durable version tagging and read-view admission before promising global
  snapshots. Prove all reads/writes, including local fast paths, join one model.
- Specify global uniqueness/FK and predicate-conflict ownership, durable deadlock
  policy, savepoints and dynamic participant freeze. Refuse features whose hidden
  writes bypass enrollment.
- Define replicated DDL/schema pinning and locality migration. Current catalog
  recovery and local collection-incarnation guards remain necessary but are not
  a distributed schema protocol.
- Prove quorum intersection during participant movement and coordinator takeover;
  a checksummed local receipt is not a quorum certificate. Resolve old authority
  without trusting an isolated former leader or resetting uncertain history.
- Define complete format negotiation and downgrade refusal for registry, intents,
  fragments and visibility metadata. The current native HA handshake v4 is a
  different protocol and does not negotiate these future capabilities.
- Define operational limits and user-visible recovery-required states for registry
  outage, unresolved intent growth, history exhaustion and snapshot restore.
  Data safety by refusal must not be reported as measured RTO or availability.

Release capability metadata can express and refuse unsupported or unqualified
contracts. It must not mark these stages implemented because this design exists,
nor does deployment-tool admission implement runtime SQL correctness. Distributed
SQL, HA qualification and independent capacity testing can progress separately;
critical workload admission waits for all required exact-artifact evidence.

## 10. Validation of this design artifact

This document was checked against the baseline code paths above. Relative file
links and whitespace were validated; no Rust build, distributed SQL feature,
new protocol test or physical experiment was executed by this design track.
All future-stage acceptance gates remain open. Source merge, release artifact,
qualification bundle and observed deployment are separate states.
