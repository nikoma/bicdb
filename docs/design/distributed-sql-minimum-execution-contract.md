# Distributed SQL: minimum execution and recovery contract

**Design only.** This is a concrete implementation addendum to
[the existing distributed SQL report](../distributed-sql-design-2026-11-11.md),
particularly its gateway, decision-domain, isolation and acceptance sections.
It does not replace that report or add runtime behavior. Source inspection used
remote main `f43583da4c34209e810c5e6a8f5925831b2949a1`, version
**1.0.482-beta**. The separate unmerged read-foundation candidate
`92d5b30faa0ce67ced0baa0ff68c79ff3cbc78ee` is preserved; its per-range point
proofs do not establish the global SQL protocol proposed here.

The target is an ordinary PostgreSQL client using any healthy gateway without
shard IDs, leader selection or application-generated per-range statements.
Losing a gateway may break its client connection; connection migration and
transparent replay of an uncertain business transaction are not promised.
Locality is an optional placement optimization. Correctness must hold when
related rows, indexes and constraint authorities occupy different ranges.

## Source 502 recheck and execution-context constraint

The [separate source checkpoint](distributed-sql-main502-source-and-context-checkpoint.json)
pins main **1.0.502-beta**, `629857f5eaaeee5f5a9124d505406d8cd6a29535`,
and compares the separately unmerged native composition `3b4bef4eecbd`.
The nine inspected boundaries still do not supply transparent SQL execution,
a shared cross-range SQL snapshot or a production multi-range coordinator.
The new one-range fifth-member DATA fixture is a storage result; it does not
turn business SQL into the protocol proposed here. This recheck runs no SQL
tests and does not relabel the historical design baseline or qualify main.

Execution context also needs an explicit worker boundary. Identical public-SDK
and loopback TCP mock tests showed that an unmerged threaded event-client
candidate dispatched APP from an already-expired caller query scope
([failed worker control](https://github.com/nikoma/bicdb/actions/runs/43532705713)),
while the unmerged synchronous 505 candidate's test-only successor refused before TCP
([passing baseline](https://github.com/nikoma/bicdb/actions/runs/43532727697)).
These tests do not execute SQL or native database commits. Their narrow result
supports the following **design requirement**, not an implemented SQL capability:

- Local SQL workers must explicitly inherit the actual caller's tightest nested
  cancellation/deadline and memory envelope, security/session identity, and
  retained read view. A thread-local guard on the gateway thread does not
  automatically cover work on another thread. Numeric budget data cannot grant
  row access or replace a recipient-owned view pin.
- A remote fragment reauthenticates and admits its own bounded execution. It
  must not deserialize a process-local `Instant`, snapshot handle, writer guard
  or role label as authority. Retries retain the original operation and view;
  they cannot renew either deadline or snapshot by starting another exchange.
- After a durable COMMIT decision, caller cancellation stops waiting or new
  caller work; it cannot choose ABORT or erase the outcome. Independent bounded
  resolution must retain every enlisted/uncertain participant and exact payload.

The worker bug is not attributed to main502 SQL. Implementing its client fix
would not provide this gateway/participant contract. PostgreSQL and Oracle
acceptance remain separate, and P0 native HA qualification proceeds independently.

## 1. Existing implementation versus this proposal

| Inspected source | Current contract | Addition required |
| --- | --- | --- |
| [PGwire `lib.rs`](../../crates/bicdb-pgwire/src/lib.rs), `execute_server_query`, `execute_server_db_sql_for_state_inner`, `execute_server_transaction_sql`, `sql_session_for_server` | Connection/session state and SQL execution over the server's borrowed local `BicDb`; local synchronous-replication acknowledgment wait | A gateway execution branch before local DB admission, shared by simple and extended protocol and preserving transaction/error state |
| [`server_cluster.rs`](../../crates/bicdb-pgwire/src/server_cluster.rs), `plan_cluster_point_query`, `plan_cluster_scatter_query`, `plan_cluster_shard_local_write` | Explicit planning helpers | Bind client SQL to actual remote typed execution; these helpers alone do not forward ordinary queries |
| [`distribution_gateway.rs`](../../crates/bicdb-core/src/distribution_gateway.rs), `ClusterPointConnector`; [`distribution_routing.rs`](../../crates/bicdb-core/src/distribution_routing.rs), `ClusterRequestRouter` | Generic connection pooling, point routing and redirect handling | Authenticated SQL-fragment request/response, execution identity and durable retry classification |
| [`distribution_query.rs`](../../crates/bicdb-core/src/distribution_query.rs), `DistributedShardExecutor`, `execute_scatter_gather` | Generic bounded shard work and merge helpers, including explicitly representable partial results | SQL operators, exact type/security semantics, one shared read view and refusal of successful partial SQL answers |
| Same file, `DistributedCommitProtocol`, `execute_distributed_transaction` | Explicit caller-supplied protocol sequencing; the implementation found is test `RecordingCommitProtocol` | Replicated production transaction records, durable intents, status/enumeration and independent resolution |
| [`session.rs`](../../crates/bicdb-sql/src/session.rs), `acquire_postgres_statement_snapshot`; [`record_read_snapshot.rs`](../../crates/bicdb-core/src/db/record_read_snapshot.rs), `RecordReadSnapshot` | Local statement snapshots and instance-bound retained read handles | A cluster read cut and actual range-to-local visibility mapping; a local handle is not a remote capability |
| [`distribution_range_consensus.rs`](../../crates/bicdb-core/src/distribution_range_consensus.rs), `RangeWriteCoordinator::resolve_range` | One-range final-row admission; spanning mutations are rejected | Invisible SQL prepare/resolve commands with different lifetime and recovery semantics |

No transparent distributed SQL executor or production multi-range SQL
coordinator is established by these source hooks. The proposed names below are
contracts to implement, not callable APIs.

## 2. Smallest gateway boundary

Add one distributed execution interface at PGwire dispatch, before taking the
local DB guard. Parsing/binding produces a typed plan against a pinned logical
schema; network work must not occur under a SQL/DB/service mutex. A local fast
path invokes the same participant contract, not an independent local commit.

The gateway retains authenticated SQL identity, GUC/profile state, statement and
portal identity, SQL transaction ID, and a stable operation/fragment ID. A
fragment binds cluster/restore lineage, logical object IDs and schema version,
transaction generation, read-view ID, range interval/epoch, parameters, security
scope, attempt identity, deadline and result limits. Peers authenticate the node
and independently check application privileges/RLS. Client-supplied role names
or metadata topology bytes cannot grant execution.

Preserve simple Query and Parse/Bind/Describe/Execute/Sync behavior, including
named portals, row description, transaction error state and cancellation. Pin a
portal's view until exhausted or closed; never refresh it between fetches.
Reject unsupported COPY, volatile routines/triggers, external effects, savepoints
or DDL forms before side effects. Add them only with their actual state model;
this first boundary is not a claim of general SQL compatibility.

Retry a route refusal only when it proves no execution. After a lost reply,
resolve the **same** transaction/operation ID and payload digest. Do not rerun
SQL text, regenerate volatile values, repeat triggers or create a new business
operation. Once rows have been emitted, a failed fragment ends that execution;
it cannot quietly replace its snapshot and resume with a different answer.

## 3. A precise common read cut for scans and joins

Use the report's conservative initial choice: one quorum-replicated SQL
transaction/decision domain with a monotonic terminal decision order `S`.
Every write that can affect distributed SQL, including single-range fast paths,
indexes and effects, joins that order. Other write APIs must participate or be
refused for those objects. Metadata read-index, range-log position `R`, local
DB sequence `B` and wall-clock time are not `S`. All reads of admitted objects
must also use this model: a legacy local SQL/Get or individually strong point
path cannot expose a later physical apply outside the shared cut. Distributed
SQL mode therefore needs persistent reader/writer capability fencing; it cannot
be retroactively inferred from an existing table's counters or routing labels.

The minimum visibility implementation is deliberately restrictive:

1. Each range processes terminal decisions in order. It applies an actual
   participating decision, or validates its nonparticipation from the committed
   frozen participant manifest. Missing entries cannot be skipped by a scalar
   watermark. Intents remain invisible until their terminal outcome.
2. Before applying a later decision, record a boundary mapping the processed
   prefix to that range's **actual local** MVCC bound. Retain versions and
   pins required to serve a chosen prefix. This is new storage functionality;
   the current instance-bound prefix snapshot is not a whole-range SQL scan.
3. A quorum-published SQL visibility prefix advances only when all earlier
   committed decisions have actual durable materialization/visibility receipts
   from every required participant. A fresh registry read yields a read-view
   certificate for that prefix. It must cover writes acknowledged before the
   read starts; a cached or pending decision number is insufficient.
4. For each planned range, obtain a recipient-owned pin for the **same** `S`,
   exact logical objects/schema and actual current range lineage. The first
   implementation executes at current range leaders with independently
   verified data authority. The registry certificate alone grants no row read.
   Each pin maps `S` to its own local `B_r` and actual backing identities.
5. Execute every scan/join fragment at its pinned `B_r`, with the exact range
   token filter. No range may expose a later decision merely because it has
   already applied it. Either retain the earlier boundary and versions or
   refuse the read. A successful global answer requires every fragment.

The important distinction is **exact view**, not only a lower visibility floor.
Waiting until a local database can see `S` and then reading its latest snapshot
could include later writes on one range and fracture a transaction. Mapping an
old `S` after that range has already passed it is not allowed without retained
history proving the mapping. Different ranges may have different `B_r` values,
even when they share one DB; token-scoped scans and per-range decision order
must prevent another range's interleaved writes from leaking into the view.

Initially, range movement/split or schema publication must wait for active SQL
pins/prepares to drain, or refuse explicitly. Online movement requires a later
proof-preserving transfer of versions, mappings, intents and decision ancestry.
A process restart that loses a read pin returns unavailable/expired unless it
can reconstruct that exact view from retained durable history. It never invents
a fresh view under the same portal ID.

This global decision stream and all-participant visibility frontier are real
throughput/availability dependencies. They are the smallest correctness target,
not evidence of elastic capacity. Partitioning the registry or replacing it
with clock/timestamp assumptions is a separate design and qualification.

## 4. Joins and isolation

Start with read-only READ COMMITTED statement snapshots and read-only REPEATABLE
READ transactions. A transaction view plus its own intents is needed for
read/write SQL. Preserve Oracle's separate contract. Reject distributed
SERIALIZABLE until global conflict claims/validation exist; local SSI and an
isolation label cannot provide them.

For the later serializable implementation, use durable key/range/predicate
claims, held through the terminal outcome, across **all** participants. Bind
claims to the read cut and reject a conflicting committed change after `S`
rather than read an old snapshot under a newly acquired current lock. New
predicates discovered during execution require the same validation. Use a
durable registry-assigned start priority, separate from decision order `S`, for
deadlock prevention; prepared ownership
cannot expire because a coordinator disappears. These claims and their failover
reconstruction are additional acceptance gates, not implicit parts of 2PC.

For joins, every input uses the same read-view ID. Initially implement exact
range-filtered scans and gateway merge; bounded spillable hash joins or typed
repartition follow. Preserve NULL/duplicate/outer-join semantics, exact decimal
aggregates, collation, global ordering/limits and authorization. No locality or
per-shard top-K assumption may change the result. Bound buffered rows/bytes,
fan-out, spill and pins; a failed, missing or expired fragment produces an SQL
error, never a successful local subset. Spill data inherits application access
and cleanup requirements.

## 5. Durable multi-range transaction protocol

The coordinator is a replicated state machine independent of the PGwire socket.
Persist `TxID`, coordinator generation, logical schema/isolation, participant
manifest, exact intent digests and an immutable terminal decision. Participant
records quorum-persist invisible row/index/constraint/effect intents and their
conflict claims before a prepared receipt. Final-row replication, native image
Ready or a metadata majority is not a SQL prepared vote.

```text
registry:    ACTIVE -> FROZEN/PREPARING -> COMMIT_DECIDED(S) -> RESOLVED -> GC
                \-----------> ABORT_DECIDED(S) -----------> RESOLVED -> GC
participant: ENLISTED -> INTENTS_DURABLE -> PREPARED -> APPLIED / ABORTED
```

Enlist durably before any persistent intent. Hidden unique/FK/index/trigger/outbox
work participates too. At COMMIT, freeze the canonical manifest/digests; no later
enlistment or changed payload may enter it. Prepare every frozen participant.
COMMIT requires every exact prepared receipt and isolation validation. The
registry chooses one durable terminal outcome; takeover fences the old owner
for enrollment/preparation/decision changes but cannot rewrite a chosen outcome.

An important current helper gap: `execute_distributed_transaction` aborts only
its successfully acknowledged prepared prefix when a Prepare call fails, then
calls `finish`. The failed recipient may actually have prepared before losing
its reply. A production coordinator must retain and resolve **every enlisted or
contacted uncertain participant**, not just counted replies. Add durable
operation status lookup, bounded recovery enumeration/cursors, participant
self-registration and decision lookup; implementing the current trait alone
is insufficient.

Resolvers run without a client session or DB latch across network I/O. They
reauthenticate the current participant owner and apply the original durable
payload/outcome idempotently through actual storage/index/effect publication.
A request carries immutable decision evidence, not a caller's COMMIT boolean.
No participant unilaterally aborts a SQL prepared intent on timeout. Registry
quorum loss leaves it in doubt with claims retained and explicit backpressure.

For the first delivery, successful COMMIT waits for a durable terminal decision,
all participant visibility receipts and publication of the corresponding SQL
prefix. A lost reply is still an unknown client outcome, resolved by the same
receipt/operation identity. After COMMIT_DECIDED, cancellation can stop waiting
but cannot select ABORT. Before any decision it can request a quorum-chosen
ABORT, whose resolution includes recipients of lost Prepare replies.

GC needs terminal decision plus all participant resolution acknowledgments,
expired retry/read-view obligations and a retained outcome tombstone. Never
remove a decision while a recoverable prepared intent still needs it. Range
handoff, backup and restore must preserve these records together; an isolated
old leader or a restored rows-only copy supplies no decision authority.

## 6. First executable acceptance sequence

These are proposed controls, **not tests run by this design task**:

| Stage | Minimum control and decisive assertions |
| --- | --- |
| Gateway attachment | Stock PostgreSQL driver, any gateway, simple and extended protocol; move a leader between Parse/Bind/Execute; no app range IDs, no local subset, no duplicate side effects after lost reply |
| Shared snapshot | Two ranges, concurrent account transfers and a joined receipt query; pause one participant between decision/apply. Every completed result is one `S`; sums and exact per-operation debit/credit/receipt values agree. Include actual local `B_r != R != S`, later-version contamination, absent keys and paginated portals |
| Prepared-reply ambiguity | Persist a real participant prepare then drop its reply. Kill gateway/coordinator. A new owner discovers that recipient and resolves the one terminal outcome; no hidden orphan or unilateral abort |
| Crash decisions | Cut before/after manifest freeze, every prepare, decision quorum, each apply/visibility receipt and COMMIT reply. Lose coordinator quorum and a participant independently; keep in-doubt state and eventual resolution distinct from availability success |
| Constraints/security | Cross-range unique/FK/index writes, predicate/write-skew/deadlock controls when serializable is enabled, RLS/delegation and same-name drop/recreate. Reject unimplemented effects before work; preserve PostgreSQL and Oracle comparisons separately |
| Locality and movement | Identical workload with no locality, colocation and forced separation; exact outcomes unchanged. Prepared intents/pins block movement until its actual transfer protocol is qualified; later exercise movement/crash with immutable participant lineage |
| Operational qualification | Independent OS processes first, then pinned artifact on independent machines; external fsynced attempt/ACK ledger, unknown-outcome reconciliation, bounded recovery and measured decision-domain capacity. Neither a healthy example nor a safe refusal establishes production scale-out |

Retain source/version/binary identities, complete fault schedules, original
assertions, failed runs and custody of stopped successful/failed roots where
needed. A joined total alone can hide offsetting missing operations; compare
exact acknowledged operation payloads and receipts. This design track adds no
prerequisite for the P0 metadata/read delivery and must not delay that
independent qualification. No Cargo, distributed SQL runtime, production coordinator,
transparent SQL delivery or physical test was produced here.
