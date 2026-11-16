# Ordinary PostgreSQL entry on native virtual-range roots: source inspection

Exact inspected source: **1.0.520-beta candidate**
`62b602ca852a4183c4aa7630e2593e10446ff35c`.
Public-main reference supplied for this review: **1.0.519-beta**
`587dd6e4775e454cc9e8224d5fed76e8795c6ff6`.
This is a narrow source audit and design addendum, not runtime implementation,
a test pass, a source push, a physical experiment or deployment evidence.
The [source index](ordinary-postgresql-native-entry-62b602ca.json) records exact
file hashes, symbols and line references from the candidate.

**There is no supported conventional PostgreSQL endpoint that turns this native
virtual-range lifecycle into transparent distributed SQL at this source.**
Native metadata/DATA/event serving and the generic PostgreSQL-wire server remain
distinct entry paths. Native Strong point-read admission is real executable code;
it is not consumed by ordinary SELECT execution.

## Entry paths and actual refusal boundaries

| Entry | Actual code path at the candidate | What it exposes or refuses |
|---|---|---|
| `bicdb serve ROOT` / `serve-pg ROOT` | CLI `prepare_server_command` / `run_serve_command` → `serve_with_host_services` → startup gate → `PgWireServer::open` → `BicDb::open_with_config` → `open_with_context` | Generic local SQL server construction, including profile/partition setup; no native-host facade or Strong SQL adapter is selected by these functions |
| `serve --cluster` | `serve_cluster_with_host_services` → `PgWireCluster::open` | Multi-database server root; this flag is not a transparent virtual-range SQL executor |
| Generic distribution controller attached by CLI | `cli_host_services` selects balanced/manual service; `checkpoint_driver.rs::start_distribution_supervisor_with_listener` uses generic `DistributionStore::open`, `MetadataConsensusStore::open`, data service and generic commit-hook installation | Known born-v2 metadata is explicitly refused by `MetadataConsensusStore::open` with “born-v2 metadata requires actual native owned initialization/reopen”; supervisor requests shutdown on failure. Selecting manual placement does not replace generic startup with native ownership |
| `bicdb cluster native-metadata serve-fresh` / learner/join/reopen variants | `native_metadata_command.rs` → actual native host/learner lifecycle; operator actions include `OpenInitialData` and replica receiver | Explicit lifecycle owns native DB/metadata/range services. Initial DATA/event service is not a PostgreSQL listener; fresh learner/replica receiver status does not grant APP, SQL or Strong permission |
| `PgWireServer::open_with_shared` / `open_with_cluster_router` | Public embedding construction around a raw already-open database or generic opener/router | They do not mint an opaque native SQL-serving capability. Router constructor explicitly says SQL semantics do not change by themselves |
| Ordinary local `BicDb`/`Transaction` reads | `get`, `get_unchecked`, `scan_collection`, local snapshot/read/collection helpers | Local policy, catalog and durability checks; no implicit metadata quorum, data quorum or cluster-complete absence proof. These must not be advertised as native Strong SQL reads |

The supported generic CLI path therefore cannot be described as a working native
SQL endpoint merely because a PostgreSQL TCP socket can be bound. The single-DB
startup gate can emit `57P03` before database open completes; the supervisor starts
on a separate thread and requests shutdown when its generic native-open path
fails. This source inspection does **not** claim a uniform first error, exact
socket timing, or that every embedding/open is rejected before all local work.
Those behaviors require dedicated execution tests. In particular, the core local
read API is not a blanket native-read refusal gate: `collection_state` checks
shared durability and local collection presence, and local `get_unchecked` reads
the local page snapshot. A local `None` is not authoritative cluster absence.

A conventional endpoint must explicitly consume the retained native owner and
its read/write authority before readiness; an asynchronous generic-supervisor
failure is not a substitute for a designed SQL admission boundary. Do not
work around native strict-open refusal by opening another metadata owner,
resetting the catalogue or exposing the raw shared database as a read fallback.

## Startup ordering and potential generic SQL exposure

There is **no mandatory synchronous native-profile refusal before the CLI binds
its PostgreSQL socket** in this source. The exact single-database order is:

1. CLI `Command::Serve` / `ServePg` calls `run_serve_command` after config-only
   validation (`--check-config` returns without serving).
2. `serve_with_host_services` calls `TcpListener::bind` **before**
   `open_with_startup_gate`. While `PgWireServer::open` runs in a background
   thread, the gate can return PostgreSQL `57P03`; it is not a native profile gate.
3. `PgWireServer::open` obtains local storage configuration and opens `BicDb`.
   `open_with_context` performs partition/profile/TLS/index setup, creates or
   loads the generic router and returns the server. None of those entry points
   performs a mandatory check rejecting the native virtual-range profile as a
   SQL host. Specific roots can fail during setup: missing migrations, pending
   catalog recovery or metadata writes needing native policy are still refused.
   Fully prepared existing local catalogs can avoid such mutations; their
   migrations are not a general native-read admission mechanism.
4. `install_host_services` only records the CLI-selected balanced/manual service.
   `serve_existing_listener` logs `server.listening` and calls
   `start_background_tasks` → `start_host_services`. The controller's `start`
   invokes `PgWireHostContext::start_distribution_controller`, which calls
   `start_distribution_supervisor` when a router exists and returns `Ok(())`.
5. The supervisor uses `thread::spawn`; its generic metadata open later refuses
   known born-v2 and calls `server.request_shutdown`. There is no readiness join
   between its successful native authority construction and the SQL accept loop.
   `serve_existing_listener_async` accepts until it observes shutdown.

Thus, **if generic database/SQL setup succeeds**, the CLI has a source-level
potential to accept local SQL before asynchronous supervisor refusal. This is an
ordering concern, not a reproduced query result or a demonstrated false absence.
It must not be upgraded to a claim that every existing native root opens or that
any particular query has succeeded during that interval.

The embedding bypass is broader than this transient window:

- Public `bicdb_pgwire::serve` calls `serve_with_host_services` with an **empty**
  service list. Public `serve_listener_with_config` opens a `PgWireServer` then
  directly calls `serve_existing_listener`, also without installing placement.
- Public `PgWireServer::open_with_shared` can wrap an already-open actual `BicDb`;
  `open_with_cluster_router` merely supplies routing metadata. A caller can pass
  either returned server to public `serve_existing_listener` without a native
  owner adapter or generic placement service.
- `start_background_tasks` starts the services actually registered; it does not
  automatically synthesize a missing native/legacy distribution controller.
  Without that controller, the eventual generic-MStore refusal does not occur
  through this path. Other storage/SQL checks still apply.

**Smallest early guard/control proposal (not implemented here):** add a Core
read-only generic-SQL-host admission predicate over the actual opened database
and sticky native format/birth profile. Invoke it at the common pgwire
`open_with_context` boundary **before partition/profile/index catalog setup** so
shared/router/embed constructors cannot bypass it. Add a read-only persisted
profile preflight before CLI library serving wrappers bind sockets; preserve
normal strict Core-open validation as well. Until an owner-retaining native SQL
adapter exists, a protected native range root receives a stable explicit
unsupported-profile error. Do not disable Core's internal local reads or change
the existing Strong protocol to implement this refusal.

The smallest controls are a native root with all ordinary SQL catalogs already
initialized (so incidental migration failure cannot mask the missing guard), a
paused generic supervisor (so its shutdown cannot mask it), and each public
constructor/serve wrapper. Require refusal before profile mutation/SQL query
execution; the pre-bind wrapper must leave its port unbound. Pair with ordinary
standalone, native single-primary HA and permitted legacy distribution SQL
controls, plus the unchanged native metadata/event Strong lifecycle. A root's
successful `57P03`, listener bind, or eventual shutdown alone is not an adequate
pass criterion. These are proposed tests; none was executed by this track.

## Where current native reads prevent false absence

The consuming native **event point-read** path has stronger semantics than the
raw local get path above:

1. `distribution_events.rs::execute_cluster_event_at_database` refuses born-v2
   `Get` fallback with `strong_point_read_unavailable` when the actual enrolled
   Strong dispatcher is absent. The native event client supports explicit Strong
   mode and deployment-default handling. This must not be generalized to every
   event operation, arbitrary SQL scans or an implicit global snapshot.
2. `NativeStrongEventDispatcher` binds weak actual service/runtime/transport,
   coordinator and installed writer-host identities. Publishing its slot is not
   permission to serve a request; the request executes its own bounded protocol.
3. `NativeStrongPointReadRequest::require_metadata` checks fresh request-bound
   metadata, exact key/range/epoch and local leader identity. The read quorum
   path establishes current actual native data/history observations and verifies
   enrolled writer births. A request DTO, node label or counter cannot mint that
   local evidence.
4. `EnrolledNativeReadQuorum::capture_actual_point_read` validates the actual
   service, installed host/coordinator and native physical point. It takes the
   visibility floor only from an actual **target-local materialization receipt**,
   opens a local transaction at that floor, captures a bounded `RecordReadSnapshot`
   and reads through object policy. Range position R and source-node B cannot be
   substituted for the target-local sequence.
5. `PendingNativePointRead::finalize` requires a distinct fresh final metadata
   context, nonregressing term/readIndex, unchanged scope and enrolled physical
   voter identities, still-current service/host and the original cancellation
   lifetime. Only it constructs `NativeStrongPointReadValue` containing a row
   **or `None`**. Failed readiness/currentness is Unavailable/error, not success
   with absent data.

`NativeRangeReadPhysical` also binds directory/open lifetime, writer hook,
serving/physical/history generations, catalogue checksum and a mutation-gate
observation stamp. These are necessary local evidence and invalidation seams;
neither a local observation nor a decoded wire fact is a read permit.
This protocol protects its own successful response from false absence. Ordinary
SQL execution has no call to this finalizer at the inspected source.

## SQL routing, queries and transactions: implementation versus abstraction

| Concern | Exact candidate finding | Missing producer/consumer seam for one conventional PG endpoint |
|---|---|---|
| Any-gateway point SQL | `route_cluster_point`, `plan_cluster_point_query`, `plan_cluster_scatter_query` are definitions in `server_cluster.rs`; source search found no ordinary SQL execution call site | Parse/bind execution must produce a typed point/fragment plan and consume route refresh plus admitted remote result under the user's SQL session identity |
| Remote SQL request | `ClusterPointConnector`/pooled gateway are generic mechanisms; native event RPCs and dispatchers are a different concrete protocol | Authenticated bounded SQL-fragment transport with schema/logical-object mapping, privileges/RLS, parameters, result/error/cancellation and session/transaction identity |
| Protected native read | Current concrete minters/finalizer are private Core native Strong point/event paths | A sealed public native SQL host facade must own and consume actual live native authority; pgwire must never mint it from JSON, root path, schema digest or `Arc<RwLock<BicDb>>` alone |
| Cross-range SELECT / joins | `distribution_query.rs` supplies bounded scatter/gather, ordered/numeric/FTS merge helpers. These do not bind arbitrary SQL to a cluster relational executor | Range-filtered snapshot scans and global relational operators with exact SQL type/NULL/collation/aggregate/security semantics; ordinary SQL must not return a local subset or silently accept partial results |
| Statement/transaction snapshot | `RecordReadSnapshot` remains database-instance scoped. Range index, source sequence and target-local visibility floor remain different domains | Cluster read-view producer with a common consistent cut and retained history, and consumers in every remote scan/join fragment. Individually admitted point reads are not a shared multi-range snapshot |
| Atomic multi-range write | `RangeWriteCoordinator::resolve_range` still rejects cross-range final mutations. `DistributedCommitProtocol` still has only test `RecordingCommitProtocol`; `execute_distributed_transaction` calls found only in tests | Concrete replicated transaction registry/terminal decision, durable prepared intents, isolation/conflict claims, participant resolution and independent crash-recovery worker consumed by normal COMMIT |
| Existing native handoff/recovery decisions | Native Prepared/Ready/Complete, source retirement, image materialization, joined voter handoff and native abort history are real range-lifecycle machinery | These are not SQL participant PREPARE votes/global COMMIT decisions. Their authority must be retained during relocation, but cannot resolve an unrelated in-doubt business transaction |

A regular PostgreSQL connection can use current local/native single-primary SQL
semantics on its appropriate server mode. That is separate from attaching it to
this native virtual-range root. No unchanged-client multi-range acceptance result
can be inferred from generic SQL compatibility suites or event-read success.

Logical locality remains an optimization: the compiler/database may later map a
tenant domain/key to favorable placement. An invoice, ledger or inventory
transaction must still have one atomic outcome when locality is absent, changes,
or spans ranges. Neither a shared tenant prefix nor forcing one physical range
can replace the missing distributed transaction decision protocol.

## Narrow next implementation boundary

Before a general executor, define an owner-retaining native SQL serving adapter
whose construction proves actual native lifetime and whose request methods
consume existing native admission. Gate PostgreSQL listener readiness on that
adapter, and fail explicit unsupported forms before executing side effects.
Then implement and test one point read/write with ordinary PostgreSQL simple and
extended protocol through one endpoint, including moved leader, absent key,
readiness loss and cancellation. The test must distinguish authoritative absent
from unavailable and must not give the client physical routing logic.

Do not mark transparent SQL implemented after this first point slice: global
scans/joins, consistent read views, hidden index/constraint/job/audit effects and
multi-range COMMIT need their own consumers and tests. The existing larger
[distributed SQL design](../distributed-sql-design-2026-11-11.md) defines those
proof obligations; this report only identifies the actual current entry seams.

## Evidence and limits

Only read-only source inspection and file/hash/link/whitespace validation were
performed by this track. No Cargo/compiler/Rust tests, SDK or database runs,
physical experiments, Azure actions, CI dispatch/rerun/cancel, production action
or source push occurred. The parent reports one current-candidate cfg(test)
E0308 and other useful CI jobs still running; this document does not claim those
jobs passed or equate preceding `4f` SQL counts with tests of `62b602ca`.
Implemented native components, source test definitions, exact executed-test
receipts, physical qualification, production admission and verified deployment
remain separate states.
