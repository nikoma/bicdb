# Server-paged, multi-terabyte storage TODO

Status: design and implementation roadmap. No item is a shipped capability
unless explicitly checked off. `[x]` means done and tested; `[~]` means
partially done with the remaining gap stated inline.

Progress as of 2026-07-24: Phase 0 complete (including its exit gate), Phase 1
substantially complete, Phases 2 and 3 largely complete. The page store lives in
`crates/bicdb-page` (179 tests) and provides disk pages, a bounded buffer pool,
slotted heap pages with stable locators, overflow chains, a page-backed B+ tree,
a durable page directory and free-space map, WAL with fuzzy checkpoints and
crash recovery, MVCC with snapshot isolation and write-conflict detection,
vacuum, and bounded scan cursors.

`server_paged` is now **runnable**, and SQL works on it: the cross-mode
conformance suite (19 tests) and `bicdb-sql/tests/server_paged_sql.rs` (9 tests)
pass identically in both modes, including inserts, updates, deletes, aggregates,
ordering, and survival across reopen.

**The mode does not yet bound resident memory**, which is its whole point. Rows
are written to pages *in addition to* the resident projection, because MVCC
version chains, secondary indexes, and the vector store are still built from
that projection. So today `server_paged` gives durability and crash recovery on
the page engine, not a database larger than RAM. Nothing here may be presented
as the multi-terabyte capability until that changes.

The next milestone is making the resident projection a bounded cache over the
page store rather than a complete copy of it — that is what turns this from a
durability change into a scale one.

Also not built: page-backed secondary indexes, spillable hash joins/aggregates,
disk-resident vectors, whole-server operator residency controls, and migration.
ORDER BY, GROUP BY, and DISTINCT do have a first spillable execution path: as
of 1.0.6-beta their sort runs are byte-bounded, quota-controlled, checksummed,
and merged with bounded fan-in.

## Decision

BicDB should support two storage modes behind the same logical database, SQL,
transaction, index, vector, backup, and replication contracts:

- `embedded_memory`: the current low-latency engine for browser, edge, desktop,
  and small/medium server databases. Live rows, MVCC versions, ordinary indexes,
  and vector search structures are resident in memory.
- `server_paged`: a native-server engine for databases much larger than RAM.
  Durable table, MVCC, index, and vector pages remain on disk and a bounded
  buffer pool keeps the active working set in memory.

`embedded_memory` must remain the default until `server_paged` passes the
correctness, recovery, performance, and operational gates below. The two modes
must not become two products with different SQL behavior or data models.

The production migration path may use PostgreSQL 18 while `server_paged` is
being completed. A multi-terabyte deployment must not be moved to BicDB merely
because a benchmark is promising; it needs staged shadowing, restore drills,
and workload-specific evidence.

## What the engine does today

These findings are grounded in the current implementation:

| Area | Current behavior | Consequence |
| --- | --- | --- |
| Canonical records | `StoredRecord` explicitly documents that every live record and every version-chain entry is resident in RAM (`crates/bicdb-core/src/record.rs`). | Resident memory grows with live data and retained MVCC versions. Compact raw JSON reduces the multiplier but does not bound it. |
| Startup | `BicDb::open_inner` reads/replays collection segments, decodes all current records, builds shards, applies unmaterialized WAL, and rebuilds indexes (`crates/bicdb-core/src/db.rs`). | Open time and peak memory grow with the database. |
| Buffered segment reads | `read_frames_buffered` reads a complete segment into a `Vec` before walking frames (`crates/bicdb-core/src/storage.rs`). | A large segment can create a large transient startup allocation in addition to the resident database. |
| Record identity | `RowId` is process-lifetime-only and is regenerated on every open (`crates/bicdb-core/src/db.rs`). | A disk index cannot safely persist current `RowId` values; server-paged mode needs a stable physical locator. |
| Ordinary indexes | `BTreeIndexStore` and `ShardedBTreeIndexStore` use in-memory `BTreeMap` instances; `build_indexes` reconstructs them from records at open. | Index memory and startup work grow with index size. |
| Exact vectors | `VectorStore` holds record IDs, vectors, and norms in contiguous resident vectors. | Exact vector search is fast for a fitting corpus but cannot remain the default representation for multi-terabyte data. |
| HNSW | The persisted JSON stores graph topology, then open reloads the topology and copies record vectors into resident `HnswNode` values. | Persistence avoids rebuilding graph relationships, but the active ANN index and vectors still consume memory proportional to the indexed corpus. |
| MVCC | Record and version maps are sharded in memory; active snapshots and write coordination are process-local. | Snapshot semantics are strong, but historical row versions need a disk representation before memory can be bounded. |
| Checkpointing | Segments/WAL are authoritative. The optional binary startup snapshot accelerates reconstruction but still reconstructs the complete resident heap. | A faster full load is not demand paging. |
| Query results | Pgwire portals and `COPY TO STDOUT` can stream output. ORDER BY, GROUP BY, and DISTINCT can spill their sort phase under explicit per-operator memory/temp/fan-in limits, but joins, aggregate state, final result assembly, and some storage paths still materialize intermediate rows; the pgwire spill metric is not wired yet (`docs/large-results.md`). | Bounded sort runs and output are necessary but do not yet guarantee bounded end-to-end query memory. |
| Large values | Attachments/large-value facilities already provide a precedent for keeping large payloads outside ordinary row metadata. | The page engine should reuse the ownership principle, not inline unbounded values into hot pages. |
| Browser | WASM is a 32-bit environment and intentionally uses bounded per-user working sets (`docs/wasm-browser-cache-todo.md`). | Native server paging must not destabilize the working embedded/WASM path. A browser page backend can be evaluated later. |

The correct summary is: disk is BicDB's durable authority today, but the live
database and its derived access structures are rebuilt as a resident in-memory
projection. It is not currently a disk-resident, demand-paged database.

## How this should compare with PostgreSQL and SQL Server

PostgreSQL stores relations and indexes in fixed-size disk pages. Frequently
used pages are retained by PostgreSQL's bounded shared buffer pool and the
operating-system page cache. Startup loads catalogs and performs bounded
recovery; it does not deserialize every user row. Sorts and hashes have memory
budgets and spill to temporary files. Vacuum reclaims obsolete MVCC versions.

SQL Server follows the same broad disk-page/buffer-pool pattern and also offers
separate memory-optimized table machinery. The useful product idea for BicDB is
not to promise that every table can be pinned without limit. It is:

1. use a bounded automatic page cache for all server-paged objects;
2. allow an operator to warm or prioritize a known hot working set;
3. allow explicitly memory-resident objects only when admission control proves
   they fit inside a reserved budget; and
4. preserve durable disk authority and recovery regardless of cache residency.

A warm BicDB working set may outperform PostgreSQL for a particular workload
because BicDB has a smaller stack or specialized record/vector paths. A cold
disk read cannot honestly be advertised as faster than RAM, and no blanket
performance claim should be made without an identical workload and durability
configuration.

## Non-negotiable constraints

- [x] Preserve current `embedded_memory` behavior and file compatibility unless
  an explicit, tested migration is invoked.
  — `embedded_memory` implies no new feature flag, so databases written today
  stay openable by binaries predating the change.
- [x] Make `storage_mode` durable database metadata. Reject a binary that cannot
  understand the selected mode before it mutates data.
  — [`ADR-004`](decisions/ADR-004-storage-mode-compatibility.md). The fence is
  read-only and ordered ahead of the format-version migration, and the tests
  assert that a refused open leaves the directory byte-identical.
- [ ] Keep SQL, authorization, transaction, backup, replication, and vector API
  semantics consistent between modes.
- [ ] Bound steady-state memory independently of total database size.
- [~] Make recovery cost proportional to WAL since the last durable checkpoint,
  not proportional to all table and index bytes.
  — BicDB 1.0.81-beta removes complete-WAL and decoded-record materialization
  from startup. Tail validation, final transaction outcomes, and page replay
  now share two streaming forward passes with one page-sized record buffer;
  replay moves the outcome tree directly into recent MVCC state. The remaining
  gate is a completed production-hardware recovery matrix and a spill/page path
  for an exceptional recent-outcome set blocked from freezing. BicDB
  1.0.82-beta supplies the clean-process, independently sized checkpointed-data
  and WAL-suffix benchmark needed to produce that matrix; see
  [`paged-wal-recovery-benchmark.md`](paged-wal-recovery-benchmark.md).
- [ ] Never require RocksDB, an LSM product, or another database engine.
- [ ] Add no reciprocal/copyleft or source-available dependency to the
  production storage path. GPL, AGPL, LGPL, SSPL, MPL, EPL, CDDL, and similar
  terms require an explicit dependency-policy exception before an experiment
  is merged.
- [ ] Implement the page store and persistent B+ tree as first-party BicDB code,
  using standard operating-system file primitives and already-approved
  permissive dependencies only.
- [x] Run automated dependency-license policy on every change. A technically
  attractive crate is not eligible until its complete transitive graph and
  bundled native code pass the repository's dependency-license policy.
  — `deny.toml` + `docs/ci/dependency-policy.workflow.yml`. The policy also
  bans embedded storage engines outright, so the "no LSM, no RocksDB"
  constraint below is enforced rather than merely stated.
- [ ] Do not copy implementation code from PostgreSQL, SQL Server, RocksDB, or
  other storage engines. Use public algorithms and BicDB-owned implementations.
- [ ] Treat vector-index algorithms and compression/native libraries as separate
  license and patent review gates.

## Proposed configuration and object policy

The initial contract should be small and explicit:

```text
storage_mode = embedded_memory | server_paged
buffer_pool_bytes = <bounded bytes>
query_work_memory_bytes = <per-query bytes>
query_total_memory_bytes = <server-wide bytes>
temp_space_max_bytes = <bounded bytes>
wal_max_bytes = <bounded bytes>
checkpoint_target_bytes = <bounded bytes>
```

Server-paged tables and indexes should default to `cache_policy = auto`.
Additional policies should be introduced only after the automatic cache works:

- `auto`: normal eviction and prefetch.
- `hot`: higher cache priority, but still evictable under pressure.
- `scan_resistant`: large scans do not evict the OLTP hot set.
- `pinned`: admitted only when a named reservation can hold the requested
  object/pages; otherwise the command fails without partially pinning it.

`pinned` is a cache policy, not a second source of truth. A future genuinely
memory-optimized table format is a separate milestone and should not delay the
server-paged MVP.

## Phase 0 — Evidence, RFC, and compatibility boundaries

- [x] Measure current open time, peak RSS, steady RSS, record overhead, index
  overhead, vector overhead, and WAL/segment read amplification at 1 GB, 10 GB,
  and the largest available fixture.
  — Harness shipped as `bicdb bench storage-baseline`; measured results in
  [`storage-baseline-2026-07-24.md`](storage-baseline-2026-07-24.md).
  **The 10 GB and larger-fixture points have not been run** (the measurement
  host had another tenant holding ~19 GB); the harness takes them unchanged.
- [x] Record separate resident costs for rows, version chains, primary-key maps,
  secondary indexes, exact vectors, HNSW, graphs, broker projections, and query
  intermediates.
  — `crates/bicdb-core/src/residency.rs`. **Broker projections and query
  intermediates are not instrumented**; the report names them in
  `not_instrumented` rather than reporting zero for them.
- [x] Add machine-readable memory accounting to `bicdb doctor`, server stats,
  and benchmark reports.
  — `BicDb::residency_report()`, `DoctorReport.residency`,
  `OperationalMetrics.memory`, `StorageBaselineReport.residency`. The full walk
  is O(resident rows), so `OperationalMetrics::from_db` reports only process RSS
  and the complete breakdown is opt-in via `from_db_with_residency`.
- [x] Write an ADR fixing the `storage_mode` compatibility contract and native
  server-only scope for the first implementation.
  — [`ADR-004`](decisions/ADR-004-storage-mode-compatibility.md), enforced by
  `crates/bicdb-core/tests/storage_mode.rs`.
- [x] Inventory every direct access to `CollectionState.shards`, `RecordMap`,
  `VersionMap`, and `IndexState`; group them into point read, range cursor,
  snapshot read, write, validation, maintenance, and recovery operations.
  — [`storage-access-inventory.md`](storage-access-inventory.md), generated and
  CI-checked by `scripts/storage-access-inventory.py`. 86 record-state sites
  plus 6 index-store sites; an unclassified new access fails the build.
- [x] Define behavioral conformance tests that run unchanged against both
  storage modes.
  — `crates/bicdb-core/tests/storage_conformance.rs`. Adding a runnable mode
  requires no edit to the suite.
- [x] Add a dependency policy file and CI license gate before evaluating any new
  storage or vector dependency.
  — `deny.toml`, `scripts/check-licenses.sh`,
  `docs/ci/dependency-policy.workflow.yml`.
- [x] Gate: no implementation phase starts until a prototype demonstrates
  random point read/write and crash recovery through a stable page identifier
  without changing public record semantics.
  — `crates/bicdb-page/src/paged.rs` (`PagedStore`) plus the 13-case crash
  matrix in `crates/bicdb-page/tests/crash_matrix.rs`. The stable identifier is
  `TupleLocator{page_id, slot, generation}`.

### Phase 0 findings that change later phases

- The access surface is smaller and better funnelled than expected.
  `CollectionState` already routes every access through six accessors rather
  than exposing `shards`, so extracting a storage trait is mechanical rather
  than architectural — 86 sites of mechanical, which is a schedulable task
  rather than an open-ended one.
- `OrderedIndexStore` is already a swappable trait with a `ShadowIndexStore`
  that validates a candidate against the proven `BTreeMap` implementation. Phase
  4 therefore starts with its seam already built, and is the cheapest phase
  relative to its value.
- The `server_paged_storage` feature flag makes the compatibility fence work
  **retroactively**: binaries predating `storage_mode` already reject unknown
  feature flags, so they refuse a server-paged database rather than
  misinterpreting it. This is why the contract was landed before any page-store
  code exists.
- `redb` and `fjall` are already in the shipped CLI's dependency graph as
  benchmark baselines (via the non-optional `bicdb-bench` dependency). Neither
  is on the storage path and both are permissively licensed, but the policy now
  permits them *only* under `bicdb-bench` — depending on either for real storage
  fails CI. Making `bicdb-bench` an optional dependency of `bicdb-cli` would
  remove two storage engines from the production binary.

## Phase 1 — First-party page file and buffer pool

- [x] Define an explicit on-disk page format with a new BicDB format version:
  magic, format version, page type, 64-bit page ID, generation, checksum, LSN,
  free-space metadata, and encryption/compression flags.
  — `crates/bicdb-page/src/page.rs`. Header plus an 8-byte generation trailer:
  the checksum catches rot, the trailer catches tears the checksum can miss.
- [ ] Benchmark candidate page sizes instead of assuming one. Include small
  OLTP rows, wide PubMed-style metadata, sequential scans, vector payloads, and
  the target storage devices.
- [x] Use 64-bit page IDs, lengths, counters, and offsets throughout. Add sparse
  file tests beyond 4 GB and 1 TB so accidental `usize`/32-bit truncation is
  caught without allocating that much physical disk in CI.
  — `tests/large_offsets.rs` round-trips real pages past 4 GiB, 1 TiB, and the
  2^32 page-id boundary on sparse files, with a test guarding sparseness.
- [x] Implement slotted heap pages so variable-size records can move within a
  page without changing their stable `(page_id, slot_id, generation)` locator.
  — `crates/bicdb-page/src/slotted.rs`. Per-slot generation makes a recycled
  slot reject the previous occupant's locator.
- [x] Implement overflow pages for values that do not fit the row/page policy.
  Stream them; never collect an unbounded value merely to satisfy a read.
  — `crates/bicdb-page/src/overflow.rs`. Streaming reader holds one page; the
  collecting variant takes a mandatory limit.
- [x] Implement a page manager with bounded `pread`/`pwrite`-style I/O, atomic
  metadata publication, checksums, torn-page detection, and fault injection.
  — `crates/bicdb-page/src/manager.rs`, including fault injection.
- [x] Implement a sharded buffer pool with pin counts, dirty tracking, access
  history, bounded metadata, and a scan-resistant eviction policy.
  — `crates/bicdb-page/src/pool.rs`. Simplified 2Q; a scan evicts only itself.
- [x] Make `buffer_pool_bytes` a hard admission budget with observable current,
  dirty, pinned, evictable, hit, miss, eviction, and wait counters.
  — Frames are allocated once and never grown; a fully pinned shard fails
  admission rather than allocating.
- [x] Add bounded asynchronous writeback, read-ahead, and checkpoint workers.
  Query traffic must retain reserved I/O and CPU capacity.
  — BicDB 1.0.74-beta adds the worker-safe writeback primitive: a
  buffer-bounded ordered dirty index, inclusive restart cursor, hard
  candidate/logical-I/O/duration limits, WAL-before-page ordering, pinned-page
  retry semantics, one batch sync, strict reports, compaction-lane admission,
  and fixed-cardinality pressure counters. BicDB 1.0.75-beta adds a bounded
  drain/freeze/finalize checkpoint state machine; replaces threshold-triggered
  all-buffer flushing with one bounded phase; and adds a checksummed, atomically
  checkpointed, compaction-lane-governed supervisor with retry, pause/resume,
  stale-worker fencing, strict reports, and fixed-cardinality metrics. BicDB
  1.0.76-beta wires that supervisor into every server-paged database hosted by
  `bicdb serve`: it resumes active schedules, preserves pauses, triggers from
  WAL pressure or cadence, shares one node governor with range repair, validates
  restored demands against current hard limits, backs off host errors, and
  stops on server shutdown. BicDB 1.0.77-beta completes the worker set with a
  hard-bounded deduplicated hint queue, exact B+ tree sibling and overflow-chain
  producers, scan-resistant clean-probationary admission, foreground
  cancellation, immutable candidate/I/O/duration envelopes, shared node
  governance, interruptible shutdown, bounded failure backoff, and
  fixed-cardinality usefulness/failure telemetry. See
  [`paged-read-ahead.md`](paged-read-ahead.md).
- [ ] Start with ordinary buffered filesystem I/O. Benchmark OS-cache
  interaction before considering direct I/O or whole-file `mmap`; do not expose
  multiple unproven I/O modes as production settings.
- [ ] Encrypt and compress at page/extent boundaries while preserving random
  access. Document key rotation and partial-page recovery.
- [~] Add immutable, content-addressed checkpoint extents for warm/cold tiered
  storage. Versioned descriptors, per-page verification, bounded streaming,
  atomic no-overwrite local publication, idempotency, and failure cleanup ship
  in 1.0.23-beta. Checksummed full-coverage manifests, previous-generation
  chaining, changed-extent verification, stale-publisher fencing, and atomic
  append-only activation ship in 1.0.24-beta. A hard-bounded, concurrent,
  crash-cleaned hydration cache with pinned LRU eviction, restart discovery,
  structural verification, and corruption recovery ships in 1.0.25-beta.
  Active-hash-fenced, rollback/backup-aware, prefix-checkpointed provider GC
  with hard reference/inventory/delete/prune limits ships in 1.0.26-beta.
  Transient provider-read retry with pinned attempts, exponential-backoff caps,
  fresh staging per attempt, and retry metrics ships in 1.0.27-beta.
  Generation-pinned point reads with logarithmic extent routing, one-page
  positioned I/O, per-read page verification, and corruption eviction ship in
  1.0.28-beta. Read-only snapshot/follower buffer pools over pluggable page
  sources, with a hard writable-guard fence, ship in 1.0.29-beta. Remote
  provider credentials and a durable mutable-overlay residency journal remain
  before live writable local pages may be evicted.
- [x] Gate: open and randomly access a database at least 10x larger than the
  configured buffer pool while RSS remains within the memory envelope.
  — Asserted at unit scale, not yet at production scale:
  `a_working_set_larger_than_the_pool_stays_within_budget` (paged.rs) and
  `a_scan_stays_within_the_buffer_pool_budget` (heap.rs, btree.rs) drive far
  more data than the pool holds and check the budget on every step. **A
  physical large-scale run is still outstanding** — see Phase 9.

## Phase 2 — Disk-resident rows, identity, and MVCC

- [x] Add a persistent physical locator for server-paged mode. Keep current
  process-local `RowId` unchanged in `embedded_memory`.
  — `TupleLocator{page_id, slot, generation}` in
  `crates/bicdb-page/src/slotted.rs`. `embedded_memory`'s `RowId` is untouched.
- [x] Persist a primary B+ tree from logical record ID to the current tuple
  locator. Do not use title, hash alone, or page position without generation
  validation as identity.
  — `crates/bicdb-page/src/btree.rs`, wired up in `paged.rs`. Generation
  validation is enforced on every read, not merely available.
- [x] Store tuple MVCC metadata on disk: creator transaction, deleter
  transaction, previous-version locator, and visibility/status information.
  — `crates/bicdb-page/src/mvcc.rs`. xmin, xmax, and a previous-version locator
  ahead of each value.
- [x] Keep only active transaction state, recent commit status, lock state, and
  bounded hot metadata resident. Older transaction visibility data must be
  checkpointed and pageable.
  — A `frozen_xid` watermark: everything below reads as committed, so only
  in-flight and recent transactions need a resident entry. The watermark is
  persisted in the meta page — without that, a checkpoint that truncates the log
  destroys the only evidence older transactions committed.
- [~] Re-express point get, upsert, delete, and snapshot reads over a storage
  cursor/record-access boundary that both modes implement.
  — All four exist on `PagedStore` with bounded cursors. **They are not yet
  behind a shared trait with `embedded_memory`**, so nothing in `bicdb-core`
  calls them; that is the next integration step.
- [~] Preserve current write-conflict, unique, foreign-key, RLS, and snapshot
  semantics. Run differential histories against the current engine and
  PostgreSQL 18 where applicable.
  — Snapshot isolation and first-updater-wins write-conflict detection are
  implemented and tested. **Unique, foreign-key, and RLS are not** — they live
  above this layer. No differential histories have been run.
- [x] Implement background vacuum/version reclamation using the oldest active
  snapshot. Bound each maintenance pass by pages, bytes, and time.
  — BicDB 1.0.66-beta adds `PagedStore::vacuum_step`: the durable catalog is
  traversed one B+tree leaf at a time instead of materializing every heap page
  ID; each row contributes only its fixed MVCC-header prefix rather than its
  complete overflow value; page, logical-byte, and cooperative-duration limits
  stop at an exact inclusive `VacuumCursor`; and a serialized cursor resumes
  safely after reopen. Oversized dead values remain intact at the same cursor
  until a step with a sufficient explicit byte envelope is admitted. BicDB
  1.0.67-beta adds the production-facing tick supervisor around this primitive:
  immutable store and operation IDs, checksummed and size-bounded state, atomic
  cursor publication, compaction-lane admission, monotonic due times,
  saturation deferral, exponential failure backoff, durable pause/resume, and
  fail-closed restart validation. See
  [`paged-vacuum-maintenance.md`](paged-vacuum-maintenance.md).
- [x] Add a free-space map and reuse policy that cannot expose stale tuple data
  after slot reuse.
  — Per-slot generations and durable page-generation floors prevent stale
  locators from aliasing reused slots or pages. The catalog-backed free-space
  map is complete and durable; the fixed-size in-memory ring is only a fast
  path. BicDB 1.0.69-beta makes the intrusive page free list fail closed:
  out-of-range, current-root, duplicate, and reserved-type operations are
  rejected before mutation; reuse validates constant-size head/successor
  invariants; and bounded vacuum segments are validated completely before
  atomic adoption. A damaged list can no longer hand out one physical page
  twice.
- [x] Keep large payload/attachment ownership outside hot heap pages and retain
  streaming reads.
  — Values above a quarter-page go to overflow chains; the heap tuple keeps a
  17-byte reference.
- [x] Gate: a crash/restart matrix at every WAL/page/checkpoint boundary loses no
  committed row, exposes no uncommitted row, and replays idempotently.
  — `crates/bicdb-page/tests/crash_matrix.rs`, 13 cases. Writing it found three
  silent-data-loss bugs (pool writing behind the WAL, index in an unlogged
  file, overflow chains bypassing the pool); all fixed.

## Phase 3 — WAL, checkpoint, recovery, and compaction

- [x] Extend the WAL with enough page/tuple intent to repeat committed changes
  idempotently and reject torn or out-of-order records.
  — `crates/bicdb-page/src/wal.rs`. Full page after-images, so replay is
  idempotent; out-of-order LSNs stop replay rather than being crossed.
- [x] Enforce write-ahead ordering: the relevant WAL is durable before a dirty
  data/index page may reach durable storage.
  — Enforced against **eviction** too, via `WritebackBarrier` — the pool used to
  write dirty pages straight to disk, silently losing committed rows.
- [x] Implement group commit without weakening `fsync` semantics.
  — Commits share one fsync; each returns only once `durable_lsn >=` its own LSN.
- [x] Implement fuzzy checkpoints that record a durable redo point while writes
  continue.
  — `Checkpointer::checkpoint`, ordered so a crash can never leave a checkpoint
  claiming pages are durable when they are not.
- [x] On open, load only format metadata, catalogs, root pages, checkpoint state,
  and the WAL suffix. Do not scan every heap or index page.
  — Recovery reads only the post-checkpoint WAL suffix and opens the durable
  catalog roots without walking the heap. BicDB 1.0.66-beta also removes the
  residual all-page `Vec<PageId>` from low-level heap scans and vacuum; both now
  advance with bounded catalog cursors. BicDB 1.0.81-beta replaces three
  whole-WAL materializations with two strict streaming passes, atomically
  truncates the torn tail, reuses one page-sized body buffer, and moves final
  outcomes into MVCC state without an intermediate vector. See
  [`paged-wal-recovery.md`](paged-wal-recovery.md).
- [ ] Replace whole-database compaction in server-paged mode with bounded vacuum,
  page cleanup, index cleanup, and optional file/extent rewrite operations.
  — BicDB 1.0.70-beta removes the unbounded free-list `Vec` + `BTreeSet`
  materialization from checkpoint tail reclamation. Opportunistic work now has
  hard 8,192-page-visit and 64 MiB logical-I/O ceilings, preflights without
  mutation, defers observably, and detaches allocation authority before link
  rewrites so interruption leaks space rather than duplicating a page. BicDB
  1.0.87-beta adds a bounded, cursor-resumable orphaned-free-page reclaim that
  relinks Free-typed pages an interrupted publication left off the intrusive
  free list, returning leaked reusable space to the allocator under hard
  page/byte/free-list-visit/adoption/duration envelopes. A durable resumable
  whole-file/extent rewrite remains required for deferred lists and live-page
  relocation and is intentionally still open.
- [~] Make every maintenance operation resumable and expose progress and I/O
  pressure.
  — Paged vacuum now satisfies this contract through a durable tick-driven
  supervisor, and incremental compaction, FTS construction, paged B-tree
  construction, range repair, backup, restore, and anti-entropy have their own
  bounded state machines. Remaining whole-file/extent rewrite and several
  legacy cleanup paths must be moved behind the same contract before this can
  be marked complete. BicDB 1.0.71-beta adds a production-sized MVCC integrity
  primitive with an inclusive restart cursor and hard key, version, per-chain,
  header-byte, duration, fault-sample, and cursor-key bounds. It reads only the
  fixed 30-byte version prefix and releases its structural lock between keys.
  BicDB 1.0.72-beta adds an independent structural B+ tree cursor with hard
  entry, leaf-page, logical-page-byte, key-byte, duration, cursor-key, and
  height bounds. It validates page layout before dereferencing entries,
  classifies root and leaf cycles, survives reopen, and holds the structure
  lock for only one finite step; see
  [`paged-btree-integrity-maintenance.md`](paged-btree-integrity-maintenance.md).
  BicDB 1.0.73-beta places both cursors behind one shared-store-identity,
  operation-fenced, SHA-256-protected supervisor. Structural completion is an
  atomic prerequisite for MVCC work; every step is admitted through the
  anti-entropy lane; crash replay, accepted totals, retry/backoff,
  automatic/operator pause, and terminal valid/invalid evidence are durable.
  See [`paged-integrity-maintenance.md`](paged-integrity-maintenance.md).
- [~] Extend full/incremental backup, PITR, replication, and restore drills to
  page files and checkpoint/WAL fencing.
  — `BICBAK03` now creates, verifies, and restores page files through bounded,
  independently authenticated chunks; v3 incrementals carry deletions and the
  paged online/restore drill passes. PITR now deletes and replays through
  explicit record/event/byte-bounded batches rather than a whole-database
  snapshot. Expiring cluster plans and independently reproducible certificates
  now bind exact topology/range epochs and require a leader-inclusive current
  voter quorum at one resolved cut per range. Range-write log v3 and cluster
  protocol v12 now durably install that write fence below every prepare path
  and retain it across restart. `ClusterBackupRun` now checkpoints the complete
  all-range lifecycle through bounded SHA-256 journal frames, resumes torn-tail
  recovery, derives only the selected quorum artifact nodes, atomically
  publishes the certificate, and resumes idempotent release after success or
  abort. The host still has to provide authenticated cross-leader routing and
  transfer each selected node's archive bytes. Whole-cluster restore admission
  now verifies every archive/database/schema/consensus watermark/range fence
  and emits one certificate-bound startup gate while leaving writes fenced.
  Metadata consensus commits a compact activation decision binding that gate
  and the canonical acknowledged-node set, preserving it through snapshot
  replication and restart. A single-owner activation run now checkpoints
  bounded idempotent fence release and atomically publishes final readiness;
  nodes revalidate metadata and require an empty fence set before listeners.
  The production host still has to route these primitives over authenticated
  cluster RPC. The event catalog remains resident.
- [ ] Keep a last-known-good format generation during migration and index
  rebuild so rollback is restore-based and explicit.
- [ ] Gate: recovery time and peak memory correlate with the post-checkpoint WAL
  suffix, not total database bytes.
  — 1.0.81-beta adds the bounded implementation and a 10+ MiB adversarial suffix
  test proving page-image memory remains one record. 1.0.82-beta adds
  `bicdb bench paged-recovery`: it builds independently sized checkpointed data
  and WAL suffixes, recovers in a clean process, samples RSS, verifies rows on
  both sides of the checkpoint, emits versioned JSON/CSV, and fails declared
  time/RSS ceilings. BicDB 1.0.83-beta records the exact executable digest,
  source declaration, kernel/CPU/memory, filesystem/mount/device, and explicit
  cache preparation; release mode fails before fixture creation when that
  attribution is incomplete. A normalized report checksum plus an independent,
  bounded offline verifier recompute every binding and pass/failure decision.
  The checked-in 64 MiB development smoke is not production certification; the
  documented production-hardware matrix still must run before this gate can be
  checked. BicDB 1.0.84-beta replaces recovery's allocation-heavy transaction
  outcome tree with sorted 9-byte entries, transfers the allocation directly
  into MVCC state, rejects contradictory decisions, and reports peak outcome
  allocation. BicDB 1.0.85-beta ships the durable abort exceptions the
  previous release named: the watermark now steps over aborted and
  crash-orphaned transactions by recording them in a bounded meta-page
  exception region, checkpoint truncation is no longer pinned by a single
  abort, and the open-time freeze collapses a retained suffix into the
  watermark at startup. BicDB 1.0.86-beta ships the disk-backed status spill
  that closes the long-running-transaction case: while a live transaction pins
  the watermark, checkpoints persist the commits above it to chained spill
  pages, evict them from the resident table, and truncate the WAL, resolving
   an evicted outcome with one demand page read. Resident recovery memory is
   therefore independent of the pinned suffix. BicDB 1.0.89-beta hardens the
   spill against the crash and reader windows a production review flagged:
   persist is now copy-on-write (merged outcomes go to freshly allocated,
   synced pages; the chain is swapped atomically and retired pages are freed
   only after the replacing descriptor is durable), and lookup holds the chain
   lock across its page read so a concurrent checkpoint cannot swap-and-reclaim
   mid-read. The same release makes the orphaned-free-page reclaim byte budget
   strict, caps the free-list membership walk, checks its deadline after the
   prerequisite flush/walk, and adds fault-injected crash, concurrent-reader,
   and oversized-free-list tests. The gate remains open pending the documented
   production-hardware certification run; exception-capacity overflow still
   degrades freezing to the stop behavior for the excess, now carried by the
   spill.

## Phase 4 — Persistent secondary indexes

Status 2026-07-25: **4a and 4b shipped (PRs #200, #201); 4c-1 (registry mode)
in progress.** The design that landed differs from the original sketch in one
deliberate way: instead of a second page-backed B+ tree implementation behind
`OrderedIndexStore`, the durable entries live in the SAME tree/WAL/transactions
as records, under a reserved `[0,0,1]` key namespace. One tree means one
recovery pass and one transactional publication point for heap and index —
crash consistency by construction rather than by coordination.

- [x] Durable index entries share the records' transactional publication point
  (written in the same paged transaction; recovery keeps or discards them
  together). Entry layout `(escaped memcomparable key, pk)`, values empty.
- [x] Persist entries against the primary key — the stable logical locator —
  not process-local `RowId`.
- [x] Maintained on every write path: live commits (in-order per-batch
  pre-image overlay), WAL-replay recovery, replicated/offline-sync writes,
  CREATE INDEX backfill (single transaction), DROP INDEX cleanup.
- [x] Open loads the resident index FROM the durable entries — no row decode,
  no key recompute; 500k indexed rows reopen in 1.1 s (was 97 s). Pre-4a
  databases without entries fall back to rebuild, correct at every open.
- [x] Gate (first half): restart never rebuilds a valid B-tree index from all
  records. `verify_index` recomputes from the page store and compares.
- [x] 4c-1 registry mode: indexed collections keep only a pk<->rowid registry
  (key-only walk at open, no heap reads) instead of stubs+chains; rowid reads
  resolve rowid -> pk -> pages. Measured A/B on one 500k-row indexed database,
  same probe binary, fresh process each time:

  | | 4b (stubs) | 4c-1 (registry) |
  |---|---|---|
  | RSS | 587.2 MiB | **371.2 MiB** |
  | open | 1.29 s | **0.93 s** |
  | resident rows / versions | 500,000 / 500,000 | **0 / 0** |
  | row + chain bytes | 170.2 MB | **0** |
  | pk maps | 37.3 MB | 67.2 MB |
  | accounted | 212.8 MB | **72.6 MB** |

  The reverse registry costs 30 MB and removes 170 MB. What remains above the
  256 MB buffer pool is now dominated by the registry itself (~134 B/row),
  which is what 4c-2 addresses.
- [ ] 4c-2: `OrderedIndexStore` implementation over the durable keyspace, so
  the index's own bytes leave RAM too (planner keeps rowids; the store maps
  them through the registry). Unique enforcement moves inside the batch
  transaction (the store write guard makes it atomic).

  Status 2026-08-05: **read side shipped (1.0.92-beta).** Read-through
  eligibility is now keyed on durable entries existing for the index on a
  registry-mode collection, rather than only on a restartable-build
  generation alias — so databases whose entries predate the aliased build
  path reopen read-through too. The headline fix is correctness: the
  planner's rowid range path (`range_index_with_prefix_filters_rowids`) had
  NO read-through branch and silently returned zero rows for any
  read-through index (reproduced on main before the fix; a SQL range plan
  over such an index returned an empty result). Also: read-through range
  scans are bounded by a new `scan_index_from` primitive (descend to the
  encoded start, stop at the prefix/upper boundary) instead of walking the
  whole index namespace, and ANALYZE statistics count entries/distinct keys
  from the durable keyspace instead of reporting zero keys to the planner.
  Exclusion-constraint indexes and non-registry collections keep the
  resident load; pre-4a databases keep the rebuild fallback. Reverse durable
  cursor shipped 1.0.93-beta. Unique enforcement (1.0.95-beta): the atomic
  check-and-insert now exists inside the batch transaction —
  `apply_paged_index_upsert` probes the exact key under the store write
  guard at the batch's own-writes snapshot before inserting a unique entry
  (always on in debug builds, `BICDB_PAGED_UNIQUE_APPLY_PROBE=1` in
  release). Enforcement AUTHORITY still sits pre-WAL (validation replay +
  per-key claims): moving it fully in-transaction means the paged
  transaction must open before WAL sequencing so a duplicate can abort
  cleanly, which extends the store write guard across the commit's serial
  section — a committer-throughput change that must not ship without a
  TPC-C A/B. That hoist (and retiring `claim_unique_keys` for paged
  collections with it) is the remaining piece of this item.
  Separately observed while measuring, then fixed in 1.0.94-beta:
  `create_index` on an already-populated paged collection was quadratic in
  rows (50k rows 16 s, 100k rows 72 s) — the build's VerifyingRows phase
  scanned each row's whole key run instead of point-probing the exact
  (key, pk) entry, O(rows-per-key) per row on a low-cardinality index.
  With the point probe: 100k rows 1.3 s, 500k rows 23 s end to end.
- [ ] Online, resumable index build with bounded batches (today's CREATE INDEX
  backfill is one transaction — atomic but O(n) memory during the build).
- [x] Persistent FULL-TEXT postings (0.9.40-beta): term entries live in the
  reserved index keyspace, one entry per term, maintained in the same paged
  transaction as the row with old/new term-set diffing; loaded at open like
  B-tree entries; FTS-indexed collections are registry-mode eligible. 200k
  rows x 4 terms: reopen 0.85 s / 423 MiB, point lookup ~11 ms, verify valid.
- [ ] Persistent spatial/array/jsonb variants (these still materialize
  stubs at open, and gate a collection out of registry mode).
- [ ] Index cache/split/height/bloat telemetry.

## Phase 5 — Bounded query execution

**This is now the binding constraint, measured 2026-07-25.** Storage is bounded;
execution is not. On the 2,000,000-row paged database (`server_paged`,
`BICDB_SYNC_OUTBOX=off`, 256 MB buffer pool), via
`crates/bicdb-sql/examples/paged_query_probe.rs`:

| step | time | RSS |
|---|---|---|
| open | — | **269 MiB** |
| `SELECT COUNT(*) FROM articles` (returns 2000000, correct) | 16.6 s | **4096 MiB** |
| `SELECT id FROM articles LIMIT 5` | 5.4 s | **3760 MiB** |

A `LIMIT 5` materializes two million rows. Extrapolated to 40M PubMed records,
either query wants roughly 80 GB — so a database that *stores* 40M rows in
263 MiB still cannot be *queried* at that size. Storage work past this point
buys nothing until execution streams.

The choke point is one function: `SqlEngine::scan_records` returns
`Vec<Arc<Record>>`, and `PlanKind::FullScan` calls it, so every plan begins by
materializing its whole input. `bicdb-core`'s `scan_collection` collects the
already-lazy `PagedRecords::scan` cursor into a `Vec` for the same reason.
Fixing it means threading an iterator through `select_exec` (~6,700 lines) —
a real project, deliberately not started as a side effect of the storage work.

- [~] Change table and index scans to lazy cursors that pin a bounded number of
  pages and release them promptly on cancellation, error, or client disconnect.
  — `PagedScan`, `HeapScan`, `BTreeRange` and `KeyScan` are bounded cursors over
  the paged engine, and `PagedRecords::{scan, scan_identities, scan_ids,
  count_live, for_each_batch}` expose them.
  `BicDb::for_each_record_batch` streams a lazy paged collection in bounded
  batches (1024 rows, each batch a fresh key-range scan resumed just past the
  previous batch's last key), returning `false` when it cannot stream so the
  caller falls back rather than silently seeing nothing. SQL batch scans and
  exact row counts now check cancellation before every page-backed row and
  release all page pins before returning the error. Adding cancellation-aware
  adapters to every lower-level maintenance cursor remains.
- [~] **One-pass projection streams (first slice, 2026-07-25).**
  `SqlEngine::try_streaming_projection` takes the `FullScan` plan when the
  result needs no global view of its input — no ORDER BY, no aggregate, no
  DISTINCT, no RLS context, no open transaction — and emits each row without
  retaining it, stopping at `LIMIT`:

  | query (2,000,000 rows) | before | after |
  |---|---|---|
  | `SELECT id FROM articles LIMIT 5` | 3760 MiB / 5.4 s | **285 MiB / 1.3 s** |

  Extended 2026-07-25 to the two shapes that dominate real use:

  | query (2,000,000 rows) | before | after |
  |---|---|---|
  | `SELECT id FROM articles LIMIT 5` | 3760 MiB / 5.4 s | **285 MiB / 1.3 s** |
  | `SELECT id, year … WHERE year = 2011 LIMIT 3` | 4096 MiB / 17.5 s | **286 MiB / 2.2 s** |
  | `SELECT COUNT(*) FROM articles` | 4096 MiB / 16.6 s | **283 MiB / 1.4 s** |

  The `WHERE` case needed no new evaluator: the streaming path already filters
  through the same `row_from_record` + `eval_row_truth_typed` machinery
  `execute_row_query` uses, and the query simply never reached it because
  routing ran first. The attempt now happens inside that routing branch.

  `COUNT(*)` with nothing that could change which rows count is answered from
  `collection_record_count` without reading a row
  (`try_count_star_fast_path`).

  **The streaming attempt is best-effort by construction.** The first version
  guarded on an enumeration of shapes it believed it could not serve; the
  enumeration was wrong in thirteen ways (subqueries, correlated references,
  LATERAL, RLS), each a query that took the streaming path and failed. Any
  error — including one raised during *planning*, as `WHERE pk = (SELECT ...)`
  does — now abandons the attempt and falls through to the general path, which
  answers correctly or raises the same error itself. A capability that reports
  itself cannot go out of date the way a list does.

  BicDB 1.0.88-beta folds `AVG` through the same streaming simple-aggregate
  scan as COUNT/SUM/MIN/MAX: the accumulator is an incremental numeric/float
  sum plus count shared with the materializing path, so a whole-table or
  filtered `AVG(col)` no longer materializes the input and the two strategies
  cannot diverge.

  **Still materializing:** joins, window functions, grouped aggregate state,
  final result assembly, and predicates the streaming evaluator cannot handle.
  ORDER BY, GROUP BY, and DISTINCT now use the bounded external-sort path, but
  surrounding operators can still materialize. Those gaps remain capable of
  reaching ~4 GB on 2M rows.

  Tests: `paged_streaming_scan.rs` pins the batch-boundary, OFFSET and
  duplicate cases the ordinary suite is too small to reach (verified by
  mutation); `count_star_fast_path.rs` pins both that the count is right and
  that **every guard fires**, each asserting a value the fast path would get
  wrong so a rotted guard fails loudly instead of returning the row total.
- [x] `LIMIT`/`OFFSET` apply to aggregate results. Both aggregate return points
  (`execute_query` and `execute_row_query`) returned before `apply_limit` ran,
  so `SELECT COUNT(*) FROM t OFFSET 1` returned the count where PostgreSQL
  returns no rows. Pre-existing, found while testing the fast path above.
- [x] Row counts are correct in paged mode. `collection_record_count` counted
  *resident* rows, which is zero for a lazy collection — so every paged table
  reported empty, and the planner costed a 40M-row table as if a full scan were
  free. It now counts visibility-checked keys from the page store (no record
  decode); `estimated_record_count` is the new cheap planning path, answered
  from the registry with no I/O where one exists.
- [~] Give each query and the whole server explicit memory budgets. External
  sort has an explicit per-operator byte budget; a shared whole-query and
  whole-server allocator remains.
- [x] Add external sort with byte-bounded run generation and bounded-fan-in
  multiway merge. `DbConfig` and environment settings cap work memory, peak
  temporary bytes, and files opened per pass. Runs carry CRC32 checksums, use
  per-query ownership directories, and are removed on success or failure.
  Quota errors use resource-limit SQLSTATEs and may not fall back to an
  unbounded materializing implementation.
- [ ] Add spillable hash aggregate and hash join with partitioning.
- [ ] Preserve streaming through `LIMIT`, `EXISTS`, portal fetch, and
  `COPY TO STDOUT`; avoid materializing rows that the client will not consume.
- [~] Add a bounded temporary-file manager with per-query ownership, checksums
  where needed, startup cleanup, cancellation cleanup, and a hard disk quota.
  External sort has ownership directories, checksums, RAII cancellation/error
  cleanup, startup scavenging, and both per-operator and shared process-local
  hard quotas. The manager removes only BicDB-owned paths and does not follow
  symlinks. Run generation and multiway merge now have cancellation checkpoints
  and return all shared reservations after removing their workspace.
  Generalizing it to hash/join operators and cross-process filesystem
  reservations remains.
- [ ] Make the planner cost cached versus uncached page access, sequential
  versus random I/O, index correlation, and expected spill.
- [ ] Prevent a large analytical scan from flooding the OLTP buffer-pool hot
  set.
- [ ] Gate: adversarial sort/join/group queries over data larger than RAM stay
  within both memory and temp-space limits and fail cleanly when a quota is hit.

## Phase 6 — Disk-resident embeddings and vector indexes

- [ ] Separate vector ownership from row residency: rows retain vector
  provenance, while vectors live in bounded, checksummed vector pages keyed by
  stable document/record identity.
- [ ] Implement a batched exact-scan fallback that streams vector pages and
  maintains only a top-k heap. Apply authorization/filter candidate constraints
  before returning vector-associated content.
- [ ] Stop duplicating every vector in both `StoredRecord`, `VectorStore`, and
  HNSW nodes in server-paged mode.
- [ ] Define a persistent ANN access trait whose result contract matches current
  vector search and whose implementation can be shadow-evaluated against exact
  search.
- [ ] Prototype a first-party page-backed HNSW graph with bounded node/vector
  caches. Measure random-I/O amplification, recall, build throughput, update
  behavior, and recovery.
- [ ] Separately evaluate a first-party partitioned/quantized index for cold,
  very large corpora. Do not select an algorithm on memory savings alone;
  measure recall and medical-term retrieval quality.
- [ ] Keep index generations, model/version/dimension, vector content hash, and
  tombstones durable and version-safe.
- [ ] Add online/resumable build, pause/resume, stale-vector accounting, and
  atomic generation promotion.
- [ ] Apply the same proprietary-license and transitive-dependency gate to
  model runtimes, quantizers, SIMD/native code, and benchmark datasets.
- [ ] Gate: semantic search over a corpus larger than RAM has bounded RSS,
  measured recall against exact search, deterministic authorization behavior,
  and restart without rebuilding from source embeddings.

## Phase 7 — Operator-controlled residency

- [~] Implement buffer-pool observability before adding tuning knobs. Operators
  need evidence of hot tables/indexes and cache churn.
  — BicDB 1.0.68-beta adds a strict, versioned, scrape-safe snapshot for page
  geometry, free space, aggregate I/O, WAL, and every existing bounded
  buffer-pool counter. It is wired through `BicDb`, operational metrics,
  Prometheus, doctor, sanitized bundles, CLI, and TUI without dynamic labels.
  Cache churn is now visible; per-table/index residency and I/O latency remain.
- [ ] Add `warm` operations for a table, index, primary-key range, or declared
  workload. Warming must be cancellable, rate-limited, and lower priority than
  foreground reads.
- [ ] Add `hot` priority with a bounded share/weight rather than an absolute
  guarantee.
- [ ] Add `pinned` reservations only with preflight sizing, a named memory
  reservation, permission checks, audit output, and a hard failure on
  overcommit.
- [ ] Persist desired warm/pin policy but rebuild actual cache contents safely
  after restart; cache pages are never canonical state.
- [ ] Expose policy and residency through SQL/server stats and `bicdb doctor`.
- [ ] Verify that one tenant/table cannot consume the entire shared pool unless
  explicitly authorized.
- [ ] Gate: a pinned hot set remains resident under a bounded competing scan,
  and removing the reservation returns pages to normal eviction without
  affecting correctness.

## Phase 8 — Migration and coexistence

- [ ] Add `bicdb storage plan` to report current mode, logical/data/index/vector
  sizes, required temporary space, incompatible features, expected copy steps,
  and backup requirements without writing.
- [ ] Add an explicit offline migration from `embedded_memory` segments to
  `server_paged` page files using bounded batches and durable checkpoints.
- [ ] Verify every migrated row, version, index definition, vector, and logical
  checksum before atomic mode promotion.
- [ ] Keep the old files read-only until the operator accepts verification and a
  post-migration backup drill succeeds.
- [ ] Add logical bulk import/export compatible with PostgreSQL `COPY` so a
  PostgreSQL 18 deployment can be the interim production system.
- [ ] Add a change-capture/shadow-read migration path only after the offline
  path is proven. Do not make dual-write correctness an initial dependency.
- [ ] For PostgreSQL-to-BicDB cutover: bulk load, catch up changes, compare
  counts/checksums/query samples, run restore drills, shadow reads, canary
  traffic, then explicit cutover with a rehearsed rollback.
- [ ] Never auto-convert a database merely because a new BicDB binary supports
  server paging.

## Phase 9 — Multi-terabyte qualification

- [ ] Add generated fixtures whose logical size exceeds RAM by at least 10x and
  whose access patterns cover OLTP, sequential reporting, PubMed-style citation
  search metadata, broker logs, and embeddings.
- [ ] Run a physical 100 GB gate before 1 TB; run the 1 TB gate on declared
  hardware rather than extrapolating from a tiny fixture.
- [ ] Define the memory envelope as:
  `buffer_pool + active_query_budgets + bounded_engine_overhead`; fail the gate
  if RSS grows with total database size after caches reach steady state.
- [ ] Measure cold/warm P50/P95/P99 point reads, range scans, writes, checkpoint
  latency, recovery, vacuum, index build, exact vector search, ANN recall, and
  search latency during ingestion.
- [ ] Measure IOPS, bandwidth, dirty-page backlog, cache hit ratio, write
  amplification, temporary spill, CPU, and peak RSS.
- [ ] Test disk-full, temp-space-full, checksum corruption, torn page, torn WAL,
  killed checkpoint, killed vacuum, killed index split, killed migration,
  standby lag, backup restore, and old-binary rejection.
- [ ] Run database sizes and file offsets above 4 GB continuously in CI using
  sparse fixtures; schedule physical large-capacity tests outside normal CI.
- [ ] Publish only measured numbers with hardware, filesystem, durability,
  dataset, cache size, and concurrency. Label all extrapolations.
- [ ] Gate: the release candidate passes workload shadowing and recovery drills
  for at least 30 days before it is considered for a multi-terabyte primary.

## Required telemetry

- [x] Buffer-pool bytes, page count, hit/miss ratio, evictions, dirty pages,
  pinned pages, admission failures, waits, read-ahead usefulness, and writeback
  lag.
  — 1.0.68-beta exports the initial pool gauges and counters. 1.0.77-beta adds
  bounded read-ahead queue depth, load/use/waste, cancellation, admission,
  error, and stop-reason counters. 1.0.78-beta completes the set with
  metadata-shard and page-latch contention counts plus total/maximum wait time,
  conservative oldest dirty-page age, and exact resident writeback backlog in
  pages and bytes. All series remain fixed-cardinality.
- [~] Heap/index/vector page reads and writes, logical versus physical bytes,
  checksums, short reads, and I/O latency histograms.
  — 1.0.68-beta exports aggregate page reads/writes/bytes, file geometry, and
  checksum/torn/short-read counters. In 1.0.70-beta, checkpoint tail-reclamation
  attempts, bounded deferrals, and truncated pages are exported as well.
  1.0.79-beta adds fixed physical superblock/heap/overflow/B+ tree/free/
  unclassified read, write, and transferred-byte counters plus fixed cumulative
  read/write/page-file-sync latency histograms. Logical catalog/index/FTS/vector
  namespace attribution and sparse physical allocation remain.
- [x] WAL bytes, group size, fsync latency, checkpoint age/duration/bytes, redo
  start/end, and recovery duration.
  — 1.0.68-beta exports live/trigger bytes, append bytes/records, syncs,
  group-commit savings, checkpoints, and durable/next/redo LSNs. 1.0.80-beta
  adds exact record counts per successful sync, fixed successful-sync latency,
  sync failures, monotonic process-lifetime checkpoint age, wall completion
  time, durable supervisor active/wall duration and WAL bytes, plus retained
  startup-recovery bytes, duration, replay outcomes, and redo/end LSNs. Durable
  cross-restart wall-time history and commit-count batch distributions are
  useful future refinements, not gaps in this fixed-cardinality pressure gate.
  1.0.81-beta adds exact scan passes, peak validated record bytes, and recent
  outcome cardinality for the streaming recovery path.
- [ ] MVCC live/dead versions, oldest snapshot, vacuum lag, reclaimed pages, and
  blocked reclamation.
- [ ] Per-query granted/used memory, spill bytes, temp quota failures, rows
  scanned before limit, and cancellation cleanup.
- [ ] Index height, page fill, splits/merges, bloat, lookup page count, rebuild
  progress, active generation, and verification state.
- [ ] Vector bytes by model/index generation, hot-cache bytes, pages visited,
  candidates, exact-versus-ANN latency, recall evaluation, tombstones, and
  coverage.
- [ ] Bounded-cardinality labels only. Record IDs, SQL text, embeddings, and row
  content must not become metric labels.

## Definition of done

`server_paged` is suitable for a multi-terabyte production evaluation only
when all of the following are true:

- [ ] Opening a database does not load or scan every row, index entry, or vector.
- [ ] Steady RSS stays inside the declared buffer/query/engine envelope as the
  database grows.
- [ ] Point, range, transactional, SQL, RLS, constraint, and vector semantics
  pass the shared conformance suite.
- [ ] Valid persisted indexes survive restart without a corpus-wide rebuild.
- [ ] Sorts, joins, aggregates, exports, and maintenance have bounded memory and
  disk quotas.
- [ ] Recovery replays from the last durable checkpoint and survives every
  injected crash boundary.
- [ ] Backup, PITR, replication, migration, verification, and rollback drills
  pass on the selected production topology.
- [ ] Hot/pinned working-set controls cannot overcommit memory or become a second
  source of truth.
- [ ] The dependency graph passes the proprietary-license policy, produces an
  auditable notice/SBOM set, and contains no unapproved reciprocal storage
  component.
- [ ] A representative workload larger than RAM meets published latency,
  throughput, availability, and recall targets under concurrent ingestion.
- [ ] A multi-terabyte primary is approved from measured canary/shadow evidence,
  not from architecture alone.

## Bounded-memory measurements (2026-07-25)

The mode now delivers a database larger than RAM, measured through the full
`BicDb` API (`crates/bicdb-core/examples/paged_scale_probe.rs` and
`paged_reopen_probe.rs`, PubMed-shaped ~600-byte records, fsync off,
AMD Ryzen 9 7900X):

| corpus | ingest rate | reopen time | reopen RSS |
|---|---|---|---|
| 2,000,000 records (~1.3 GB logical) | ~100k rec/s | **0.10 s** | **263 MiB** |

Reopen RSS is the 256 MiB buffer-pool budget plus ~7 MiB of fixed overhead,
flat in corpus size. Three mechanisms, layered in this order:

1. **Eviction stubs** — shards keep identity fields; row bytes live in pages,
   fetched on demand pinned to each version's own snapshot.
2. **Lazy shards** — open loads nothing; shards cache only rows touched this
   session, and reads that miss them fall back to the page store at the
   transaction's paged snapshot (with baseline seeding on first touch, so old
   snapshots keep seeing pre-touch values). Collections with secondary indexes
   or vectors are materialized as stubs instead — defining an index is how a
   caller opts into O(n) residency, until Phase 4 makes indexes page-backed.
3. **Transaction-log truncation at open/close** — in paged mode the core log's
   recovery value is spent once its commits are re-applied to the page store;
   before this, a 500k-row session left a 411 MB log whose full replay cost
   89 s and 2.2 GiB at *every* open, growing without bound.

Operational notes for large imports:

- `BICDB_SYNC_OUTBOX=off` — the CDC outbox keeps every unsynced op resident
  (both modes, by design); an import that nobody will sync from should not pay
  for it.
- The writing session's shard cache grows ~800 bytes/row; a long import should
  close and reopen the database periodically (for PubMed, at source-file
  boundaries) to reset it. Session-cache eviction under a budget is the
  follow-up lever.
- The ingest write path currently logs rows to both the core log and the page
  WAL during the session (bounded by the truncation at close); logical logging
  is the known lever if import disk churn matters.

## Cross-mode suite results (2026-07-25)

Setting `BICDB_STORAGE_MODE=server_paged` runs the entire established test suite
against the paged engine without editing a single test. This is the most
productive check performed on the mode so far, and it should be run on every
change to either engine.

Hand-written cross-mode tests only cover paths someone thought to write down.
Running the existing suite in the other mode covers the paths nobody thought
about — which is where all four findings below came from.

    cargo test -p bicdb-sql                              # 795 pass
    BICDB_STORAGE_MODE=server_paged cargo test -p bicdb-sql   # 795 pass

What it found, in order of severity:

1. **Encryption at rest was silently defeated.** `bicdb-page` has no dependency
   on `bicdb-core` and therefore no access to the database key, so an encrypted
   database in paged mode encrypted its segments while writing the same rows as
   plaintext into `paged/`. Caught by
   `encrypted_database_round_trips_and_hides_record_plaintext`, which scans every
   file under the database directory for known plaintext. Now refused at open
   (`encryption::ensure_compatible_with_storage_mode`). Lifting the fence means
   page-level AEAD with the page id as associated data — its own phase, not a
   footnote.

2. **Two stores could own one directory.** `PagedStore::open` took no lock, so a
   second open — trivially reachable, and legal against the in-memory engine —
   gave two buffer pools, two WALs and two meta pages over the same files, and
   the last to flush discarded the other's committed transactions. Now an
   advisory `flock` (`bicdb-page/src/lock.rs`), refused rather than waited on.
   An OS lock rather than a lock file, so a `SIGKILL`ed owner does not leave a
   database that needs manual repair.

3. **Replicated and offline-sync writes never reached the page store.**
   `apply_replicated_upsert`/`apply_replicated_delete` bypass the transaction
   path, so imported rows were durable in segments but invisible in paged mode.
   Every paged write now goes through `Db::in_paged_transaction`, which states
   the commit/abort discipline once.

4. **Several core tests were mode-sensitive by accident** — they asserted
   embedded-mode specifics (`storage_mode` defaults, exact feature-flag sets)
   while opening with `DbConfig::default()`. Those pin their mode explicitly now,
   which is correct regardless of the ambient default.

## Recommended implementation order

The narrowest credible vertical slice is:

1. page file + buffer pool;
2. one disk-resident collection with primary-key point get/upsert/delete;
3. WAL/checkpoint/recovery;
4. one persistent secondary B+ tree;
5. lazy range scans and bounded sort spill;
6. migration tool and dual-mode conformance;
7. disk-resident vector exact scan, then persistent ANN;
8. operator residency controls; and
9. 100 GB, then 1 TB qualification.

That order produces a recoverable, testable database at every milestone and
does not put the existing browser/embedded product or proprietary licensing at
risk.
