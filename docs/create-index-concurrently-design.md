# CREATE INDEX CONCURRENTLY — design

Status: DESIGN (2026-08-09). The keyword is currently **rejected** with a
pointer to this document (`execute_create_index`, and the raw
`CREATE INDEX … ON ONLY` parser). It used to be parsed and silently
stripped, which handed a client that asked for a non-blocking build a
build that holds the database's exclusive write lock for its whole
duration — at 100M+ rows, hours of stalled writers behind a keyword that
promises the opposite. `DROP INDEX CONCURRENTLY` still maps to plain
`DROP INDEX` deliberately: a drop is a metadata unpublish plus bounded
namespace purge, so the semantic gap is a brief lock acquisition, not an
hours-long stall.

## Where the blocking comes from today

Not the build itself. The paged B-tree and FTS builds are already bounded,
crash-resumable, and resource-governed (`begin_paged_btree_build` /
`advance_paged_btree_build_governed`; the external FTS block build), and
the old index stays queryable until an atomic catalog swap. The blocking
is the ACCESS PATH: `create_index` takes `&mut BicDb`, and over pgwire
that means the session-layer `Arc<RwLock<BicDb>>` write lock is held from
the first backfill batch to the swap. Every other session — readers
included — queues behind it.

## What PostgreSQL does (the shape to match, not the letter)

1. Publish the index as *invalid* so concurrent writers start maintaining
   it immediately.
2. Build from an MVCC snapshot while writes continue.
3. Catch up: index everything written between the snapshot and now.
4. Wait out transactions that predate the index, then mark it valid.

BicDB already owns every primitive this needs:

- **Snapshot build**: `PagedStore::begin_transaction() → (Xid, Snapshot)`
  gives a stable view; the existing chunked, resumable backfill can run
  against it from a `&self` context. Checkpoints already refuse to freeze
  past the oldest active snapshot, so the build's snapshot pins exactly
  what it must and nothing more.
- **Concurrent maintenance**: the write path already applies index
  mutations per commit (`apply_paged_index_upsert`); an index in
  *building* state needs those mutations applied from its publish point
  onward, exactly like a valid index, just not consulted by the planner.
- **Catch-up**: the range-relocation machinery
  (`checkpoint_relocation_snapshot` → `checkpoint_relocation_catch_up`)
  is precisely a snapshot-then-delta loop with a bounded cursor; the
  same pattern re-walks pks written after the build snapshot (their
  commits carry xids > the snapshot's horizon) and indexes them.
- **Atomic activation**: the catalog swap that ends today's rebuild
  (`catalog-checkpoint → snapshot-scan → catalog-swap`) is the "mark
  valid" step.

## Proposed phases

```
Phase 0  catalog: publish IndexState { building: true } under a SHORT
         exclusive acquisition (milliseconds — a catalog write, no scan).
         From here every committed write maintains the index; the planner
         ignores it.
Phase 1  snapshot build: pin Snapshot S; run the existing resumable
         backfill against S from a shared-access context. Unique
         violations abort the build (drop the invalid index) exactly as
         PG does.
Phase 2  catch-up: bounded cursor over commits with xid > S's horizon
         (or, simpler and still correct: a second pk-ordered sweep that
         upserts idempotently — entries are keyed, re-put is a no-op).
         Repeat until a pass finds nothing new.
Phase 3  validate: short exclusive acquisition — final micro-catch-up of
         anything committed since the last pass, flip building → valid,
         catalog swap. Bounded by one pass over a bounded delta.
```

Uniqueness is the hard case, as in PG: a unique index must reject
concurrent duplicate inserts from Phase 0 onward, which means the
maintenance path must consult the *building* index for unique checks
while the planner still refuses to read it. That is the same
authority-vs-visibility split the paged unique probe already implements
for the read-through path.

## What must NOT be attempted

- No new locking primitive at the session layer. Phases 0 and 3 use the
  existing write lock briefly; phases 1–2 hold only what a reader holds
  plus the build snapshot.
- No planner consultation of a building index — including uniqueness
  short-circuits in the row evaluator.
- No silent fallback: if any phase cannot make progress (snapshot pinned
  too long, unique violation), the index is dropped as invalid and the
  statement errors. PG leaves an INVALID index behind; we should match
  that observable state so operators can `DROP INDEX` and retry.

## Effort and sequencing

The build/maintain/catch-up machinery exists; the work is the access-path
refactor (backfill callable from `&self` with the session layer letting
writers through) and the building-state catalog flag with its uniqueness
semantics. That refactor touches the session locking model, which is why
this is a design-first project: implement behind an opt-in
(`BICDB_CREATE_INDEX_CONCURRENTLY=1`) with the TPC-C suite running
during builds as the acceptance gate, then flip the SQL keyword from
rejection to the real path.
