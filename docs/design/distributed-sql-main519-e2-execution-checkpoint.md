# Distributed SQL: main519 and candidate e2 execution checkpoint

This is a **source/design checkpoint**, not an implementation or acceptance result.
It extends [the original design](../distributed-sql-design-2026-11-11.md),
[the minimum execution contract](distributed-sql-minimum-execution-contract.md)
and [the main502 source checkpoint](distributed-sql-main502-source-and-context-checkpoint.json).
The [companion JSON](distributed-sql-main519-e2-execution-checkpoint.json) pins
the exact sources, line anchors, hashes and comparison results used here.

The inspected main is **1.0.519-beta**, `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`,
tree `e88a49e83f7f92fc1fea6293b88eb3bfb21bf7c9`. The separately inspected public
candidate is **1.0.520-beta**, `e2adb410befd41bd475f015fd6590c33d1afaa4f`, tree
`11e9da9b2888e9be5f7f7e4cb5c0ff00515a36f4`. Candidate e2's actual all-target
and Rails jobs failed before test execution with library `E0277`/`E0624`;
its format job also failed module resolution. The separately reviewed correction
`f4569d983fc04116a9dddeef769eb18fb1d2126e` is not an executed validation of e2.
The complete Native `132938081253` and Core `132938081301` logs subsequently
confirmed the same two library errors in the chaos-helper prerequisite, zero
executed cases and skipped runtime steps. REST `132938081249` then failed while
building the Core dependency for `bicdb-sync --lib`, after 564 actual passing
Lua/Page/Script/Workflow/Extension cases. Neither that passing prefix nor earlier
CI results are distributed SQL acceptance.

## Actual path and delta

An ordinary PGwire connection still reaches a server-owned local database.
`execute_server_query` preserves connection transaction state; the shared and
exclusive paths in `execute_server_db_sql_for_state_inner` instantiate a
`SqlSession` borrowing that server's `BicDb`. Explicit transactions take the
same connection-owned `Transaction` through `execute_server_transaction_sql`.
The extended/virtual execution path also invokes these transaction/session
helpers. Security context, GUC state, cancellation, catalog cache and the local
connection snapshot floor are propagated. This is useful local SQL behavior,
not a distributed execution attachment.

| Inspected boundary | What the source actually supplies | Missing execution/authority |
| --- | --- | --- |
| [PGwire dispatch/session](../../crates/bicdb-pgwire/src/lib.rs), `execute_server_db_sql_for_state_inner`, `sql_session_for_server` | Local SQL execution with shared/exclusive admission and connection state | A gateway branch **before** local DB admission, authenticated typed fragment execution and one transaction identity across gateways/ranges |
| [PGwire cluster helpers](../../crates/bicdb-pgwire/src/server_cluster.rs), `plan_cluster_*` | Explicit point/scatter/shard-local plans from topology | Ordinary parsed SQL does not thereby invoke remote execution; the inspected tree has no production `DistributedShardExecutor` implementation |
| [Gateway/routing](../../crates/bicdb-core/src/distribution_gateway.rs), `ClusterPointConnector` | Generic pooled connections and typed redirect handling | Its only implementation found is test `FakeConnector`; node mTLS and routing headers do not carry SQL authorization, view or durable transaction ownership |
| [Query helpers](../../crates/bicdb-core/src/distribution_query.rs) | Bounded generic scatter/merge and caller-supplied commit sequencing | Only test shard executors and `RecordingCommitProtocol` were found; no production replicated SQL decision/intent store or independent resolver is attached |
| [Local snapshots](../../crates/bicdb-core/src/db/record_read_snapshot.rs), `RecordReadSnapshot` | One instance-bound prefix snapshot and captured pending overlay with a retained pin | No common SQL decision prefix, range-to-local mapping or cross-process snapshot capability; independently fresh point reads do not form a joined snapshot |
| [Range admission](../../crates/bicdb-core/src/distribution_range_consensus.rs), `resolve_range` | Actual current leader/voter/schema admission for a complete final-row write set | Spanning sets are explicitly refused. Range `Prepared`/`Committed` states are final-row replication states, not invisible SQL participant votes |

The JSON compares the main502 checkpoint revision, main519 and e2. The generic
query, gateway, routing, cluster planning, local read-snapshot and ordinary query
execution helper files did not acquire a distributed SQL implementation between
main519 and e2. The searches establishing test-only implementations cover the
whole tracked Rust tree at both inspected revisions; this is a bounded source
finding, not a claim about external downstream code.

The meaningful SQL delta is local completion handling. Candidate e2 retains the
same original transaction long enough to distinguish actual `TxState::Committed`
from an error before commitment. `SqlSession::commit` detaches committed DDL undo
obligations and preserves committed GUC/catalog cleanup when a later finalizer
returns an error. PGwire advances `last_commit_seq` only after durability,
admission and memory-job finalization succeed. This avoids converting a local
completion error into either DDL rollback or a declared successful visibility
floor. It does not create a global decision or prove that remote participants
share the outcome. The new SQL committed-error control is a definition/evidence
scope separate from this source audit; no execution is claimed here.

Core's ordinary commit path separately makes a real logical WAL decision durable
before record/page/index apply, retains complete redo on materialization failure,
then publishes its contiguous **local** applied sequence. Native guarded local
completion and ordinary global-prefix waiting have distinct callers. These
existing owners and finalizers must be preserved; a future SQL coordinator must
not treat an issued XID, a checksummed row, a range receipt or `CommitOutcomeUnknown`
as a manufactured global prepare/commit proof.

## Smallest coherent production architecture

The following are required producers, not existing callable APIs. They narrow
the existing design into an end-to-end dependency order rather than enabling a
planner-only SQL mode.

1. **Persistent admission and logical schema.** Negotiate a new SQL protocol and
   mandatory reader/writer format contract. Bind stable logical table/index
   identities to actual current local backing/open/schema under the borrow used
   for execution. Every write/read API touching admitted objects must enter the
   same model or refuse. A local fast path cannot bypass it. Initially reject
   unsupported DDL, routines/volatile effects, COPY, savepoints and external
   effects before work; preserve existing local SQL behavior outside that mode.
2. **Gateway and participant context.** Attach one execution interface at both
   simple and extended dispatch before a DB lock. Retain SQL identity, GUC/profile,
   transaction/statement/portal IDs, parameters and stable operation digest.
   A recipient independently authenticates application permissions/RLS and binds
   actual range leader/epoch, process/open, schema, budget and resource bounds.
   Spawned work retains the original caller's actual cancellation, query-memory,
   security and view context; peers independently admit their own recipient
   scope. A node certificate or decoded topology is insufficient. No network
   call runs under a SQL session, DB, journal or metadata mutex.
3. **Actual replicated decision and intents.** Use the minimum contract's
   conservative single decision domain initially. Durably enlist participants,
   then freeze their exact manifest/digests. Each participant quorum-persistently
   stores invisible row, index, constraint and effect intents plus required
   conflict ownership before returning a prepared receipt. These are new
   semantics; current final-row `RangeWriteCommand` cannot stand in for them.
   Coordinator generation fences new work without erasing a terminal decision.
4. **Durable resolution independent of the client.** Persist one terminal
   COMMIT/ABORT and decision order `S`. Retain actual issued append/known outcome
   before postchecks; uncertain appends require exact same-operation status
   recovery, never new decision bytes. Enumerate all enlisted/contacted uncertain
   participants, including a recipient whose durable prepare reply was lost.
   Current `execute_distributed_transaction` aborts only its acknowledged prefix
   after a failed prepare and exposes no lookup/enumeration API; implementing its
   existing trait alone does not close this boundary. Prepared claims cannot
   expire into unilateral abort after coordinator loss.
5. **Visibility and SQL results.** Participants consume ordered decisions or
   prove nonparticipation from each committed frozen manifest. Retain actual
   mappings `S -> B_r` and versions/pins for exact range-filtered scans. Publish a
   quorum SQL visibility frontier only after every required participant's durable
   apply/visibility obligations finish. The first successful COMMIT waits for
   that frontier; cancellation after decision stops waiting, not commitment.
   Read-only statement/transaction views use one certificate and exact `B_r` per
   range. Reading latest after merely meeting a floor can fracture a transaction.
6. **Relational execution and recovery-safe lifecycle.** Execute all join inputs
   at that view, with bounded buffers/spill, exact types and complete results.
   Preserve PostgreSQL and Oracle isolation contracts separately. Global
   SERIALIZABLE additionally needs durable predicate/conflict validation;
   local SSI is insufficient. Initially block movement/schema publication with
   active pins/prepares, or refuse it explicitly. Later transfer mappings,
   versions, intents and decision ancestry through a separately proved handoff.

Logical locality may reduce participants and network work. It cannot be a
correctness condition: the same business transaction must retain its outcome
when related rows or tables are deliberately separated. The single decision
domain/frontier is a real throughput and availability dependency; this source
checkpoint establishes no elastic capacity.

## Native Strong, indexes, search and effects

The existing consuming [Native Strong point protocol](../../crates/bicdb-core/src/distribution_native_read/strong.rs)
validates fresh metadata, actual enrolled data quorum, original writer/Store,
one protected local snapshot and final currentness for a row **or absence**.
Keep those duties at each participant. A SQL read-view certificate cannot bypass
them, while independently successful Native Strong calls cannot replace one
shared SQL view. Range index `R`, local sequence `B_r`, metadata read index and
SQL decision `S` remain different identities.

Row visibility must cover the indexes and constraints used by that query at the
same `S`. Local unique-claim/apply code is useful storage machinery, not global
unique/FK ownership. A distributed unique/FK authority must enlist even if its
base row is on another range. Exact FTS requires index content and global
statistics at the pinned view; current versioned approximate statistics and
`f64` numeric merge helpers cannot silently implement exact SQL/DECIMAL results.
An asynchronous derived index must wait for its required visibility obligation,
use a proven complete base-scan fallback, or refuse; successful stale/partial
SQL answers are not allowed.

The local WAL already carries transactional broker redo, and local outbox
publication follows durability. Enforced native transactions deliberately
refuse broker effects **before range Prepare/application WAL**, because native
range commands do not replicate those effects yet. Preserve that refusal until
the distributed participant manifest includes actual outbox/audit/job/trigger
obligations and idempotent recovery. A local successful trigger or notification
is not proof of replicated business effects. GC/backup/restore must retain
decisions, unresolved intents, effects, outcome tombstones and snapshot lineage
together; rows-only recovery cannot reconstruct authority.

## First decisive acceptance cuts

These are **proposed controls, not tests executed by this checkpoint**. A stock
PostgreSQL client uses one endpoint and unchanged `BEGIN; UPDATE debit; UPDATE
credit; INSERT receipt; COMMIT` across `accounts` and `receipts`. Only the fixture
chooses physical range placement. Include a second unchanged multi-table
invoice/line/receipt workload when FK/unique/index/effect forms are enabled.

| Actual cut | Required producer and decisive oracle |
| --- | --- |
| Gateway attachment and routing | Run simple and extended protocol against any healthy gateway, including a non-owner; change leadership between Parse/Bind/Execute. Exact results or explicit unsupported/refusal, no local subset, no application range IDs and no changed business SQL |
| Lost prepared reply | Persist a real quorum participant intent, withhold its reply, then terminate the coordinator process. A fresh owner discovers the contacted recipient from durable state and resolves the same TxID/digest. No unregistered orphan, timeout abort or replay of SQL/volatile work |
| Durable decision crash | Terminate coordinator **after actual decision quorum, before any/all participant apply and before COMMIT reply**. Keep the original client outcome unknown. Fresh recovery reads the same decision and completes every exact debit/credit/receipt; before-decision fault lanes must converge to a lawful durable decision, not infer ABORT from absence |
| Partial physical apply | Hold one actual participant after decision while another applies. A completed joined query must show all-old or all-new at its exact view, or fail. Neither old/latest mixing nor one-sided business visibility may succeed. Compare exact payload/receipt identities as well as balance totals |
| Derived obligations and claims | Cut before/after unique/FK/index/FTS/outbox publication and recovery receipts. Check exact index-vs-base answers at `S`, one business receipt/effect per operation, no duplicate delivery/replay and no COMMIT success with an unresolved mandatory obligation |
| Reopen and authority | Restart registry and participants from actual durable roots, revoke the old coordinator, replace an actual database open and exercise same-name drop/recreate. The old capability fails; a fresh legitimate owner recovers only original durable outcomes. Missing quorum remains explicitly in doubt, not availability success |

An externally fsynced attempt ledger precedes COMMIT and an ACK ledger follows
success. After every recovery compare exact per-operation expected debit, credit,
receipt and effects; total balance alone can hide compensating errors. Every ACK
must survive. Unknown outcomes may eventually commit or abort according to the
one durable decision, but never remain partly committed as a business result.
Independently pinned read views must not expose physical partial apply. Preserve
client disconnect/SQLSTATE, original errors and fault-reached evidence separately
from later resolution. A failed Prepare/COMMIT call alone proves no rollback.

Begin with genuine same-process components to prove exact protocol cuts, then
independent OS-process coordinator/participant crashes and finally independent
machine/artifact qualification. A mock `RecordingCommitProtocol`, local SQL
suite, per-range SDK Strong tail or healthy handoff cannot substitute for these
acceptance cuts. Automatic controller, repeated movement, source loss, cleanup
and cold HA obligations remain separate unfinished work; none is waived by this
distributed SQL design.

## Validation of this checkpoint

Only Git source inspection, source/hash/line comparisons, JSON parsing, relative
link checks and diff checks were performed. No Cargo/Rust build, SQL execution,
runtime feature, test dispatch, physical experiment, version bump, source push,
release or deployment was produced. The companion JSON retains exact historical
main502/main519/e2 identities rather than relabeling old checks as current proof.
