# Incremental OLAP: aggregate projections (and, eventually, cubes)

Status: design. No `CREATE CUBE` surface is exposed, deliberately — see §10.

## 1. Position

A cube in BicDB is not a storage silo and not an overnight batch build. It is:

> a **versioned, incrementally maintained aggregate projection** over a
> collection, with declared **grain** and known **dimensional hierarchies**.

That makes it another projection type on machinery that already exists —
`stream → projection → aggregate` — rather than a second engine. The base
table stays authoritative; the projection is a derived, rebuildable artifact.
Nothing below is allowed to make the base table's correctness depend on a
projection.

## 2. The load-bearing fact: there is no before-image

`record_updated_event` / `record_deleted_event` (`db.rs`) put only the **new**
record on `bicdb.records`:

```json
{ "collection": "...", "record_id": "...", "record": { ...after... } }
```

Incremental aggregation needs the **old** values to retract them. An update
that moves `hosting_provider` Hostinger → Cloudflare must decrement the
Hostinger cell and increment the Cloudflare one; with only the after-image the
projection cannot know which cell to decrement. Three ways out:

| Option | Cost | Verdict |
| --- | --- | --- |
| A. Add before-images to the audit stream | ~doubles audit volume on a stream that is already a growth problem (see the 2026-08-16 crawl-ingestion incident) | **No** |
| B. Projection owns a per-record *input state*: the projected columns it last applied | bounded by (projected columns × rows), not (full row × rows) | **Yes** |
| C. Read the previous MVCC version at apply time | the prior version may already be vacuumed; correctness would depend on retention | **No** |

**Decision: B.** Each projection persists, per source record, the exact tuple it
last contributed:

```text
input_state[record_id] = { dims: [...], measures: [...], version }
```

Retract uses that, never the base table. This has a second payoff: the stored
`version` is also the **idempotency key** (§4), so one structure solves both
problems. The cost is honest and must be documented per projection: a
projection over 4 small columns of 70M rows is a real, sizeable structure — it
is *not* free, and it is the main reason a projection's grain is a deliberate
choice rather than "add every dimension".

## 2b. Projection identity: NOT `RowId`

> **Physical `RowId` MUST NOT be used as projection identity.** It is
> process-local, never persisted, and re-derived on every `open`. Projection
> input state is keyed by a digest of the persistent logical `Record.id`.

This is written down because it is a *tempting* optimization: `RowId(u64)`
exists, is `Copy`, would halve the 16-byte identity, and — if ids were dense —
would allow a paged state array instead of a hash table, removing the hash
overhead entirely. All of that is real, and all of it is wrong here. The
type's own contract says it is a "physical record locator — a PostgreSQL-TID
analog… assigned on insert/recovery and re-derived fresh on every `open`".

A projection persisted against such an identity would silently attribute every
row's state to the wrong record after a restart — the failure mode this whole
design exists to prevent. A **true logical row identity** (persisted, never
reused) would unlock the dense-array representation and is worth its own
engine campaign; it must not be smuggled in as a projection optimization.

## 3. Measure algebra

Measures are not arbitrary closures. Each declares a **state type** and what it
supports; the engine refuses combinations it cannot maintain.

| Measure | State | Retractable | Mergeable | Class |
| --- | --- | --- | --- | --- |
| `COUNT(*)` | `count` | yes | yes | additive |
| `SUM(x)` | `sum` | yes | yes | additive |
| `AVG(x)` | `sum, count` | yes | yes | algebraic |
| `MIN/MAX(x)` | value + support | **no** (see below) | yes | algebraic-ish |
| `COUNT(DISTINCT x)` | HLL sketch | approximate | yes | holistic → sketch |
| `P50/P95/P99(x)` | KLL / t-digest | approximate | yes | holistic → sketch |

**MIN/MAX are the trap.** Increments are trivial; *retracting the current
extreme* is not — you cannot un-min without the multiset. Options are keeping a
bounded top-k support set per cell (correct until the support empties, then a
cell-local recompute) or declaring MIN/MAX rebuild-only. Do not ship MIN/MAX as
if it were SUM.

**Sketches are labelled approximate at the query surface.** A `P95` column that
silently returns an estimate is a correctness bug in reporting even when the
sketch is behaving.

**Mergeability is the property that makes distribution nearly free** (§8): if a
cell's state can be merged associatively, per-shard partial states merge at a
coordinator with no re-scan. COUNT/SUM/AVG/HLL/KLL all merge. That is a more
valuable architectural property than "we have cubes".

## 4. Effectively-once, not exactly-once

The audit stream can replay: a duplicated retract/apply drifts counts silently,
which is the worst failure mode here (wrong numbers that never announce
themselves). We do not need exactly-once delivery infrastructure; we need
**effectively-once application**:

- every source event carries a monotonic identity (its stream position, plus
  the record's own version/order for per-record ordering);
- each projection persists a **watermark**: the highest source position durably
  applied;
- an event at or below the watermark is **ignored**;
- an event whose per-record version is **older** than the record's recorded
  `input_state.version` is ignored (late/out-of-order delivery);
- the watermark advances only after the cell mutations are durable, in the same
  atomic step, so a crash between them cannot double-apply.

Defined behavior, explicitly:

```text
event 9182 applied once      -> applied
event 9182 replayed          -> ignored (<= watermark)
event 9184 arrives before 83 -> applied; 9183 later ignored if its record
                                version is older than what was applied
```

## 5. Consistency model (stated, not implied)

An aggregate projection is **not** transactionally instantaneous with the base
table, and pretending otherwise is how dashboards lie. The contract is:

> A read of projection P reflects all committed base changes through source
> position `P.projection_position`.

and the engine exposes the lag rather than hiding it:

```text
projection_position: 128,811,992
source_position:     128,812,041
lag_events:                   49
```

A query surface may then offer a freshness bound ("fail/warn if lag >
N events or T seconds") instead of silent staleness. Strong-consistency reads,
if ever wanted, are a later opt-in (block until caught up) — not the default.

## 6. Grain, not 2^N cuboids

A full cube materializes the powerset of dimensions. That is the classic
mistake and it is worse here: `city × category × platform × hosting × H3` at
fine H3 resolution approaches the row count, so the "cube" costs more than the
table it summarizes.

Therefore a projection **declares its grain explicitly**:

```sql
CREATE MATERIALIZED AGGREGATE website_market
ON businesses
GROUP BY (country, admin1, category, hosting_provider)
MEASURES (COUNT(*), SUM(website_score), AVG(website_score));
```

Coarser slices are answered by **rolling up on query** over declared
hierarchies (admin boundaries, category trees, time buckets, H3 parents —
BicDB already has all four as primitives), not by materializing every cuboid.
High-cardinality dimensions (H3 at fine resolution, domain, business id) belong
in the *base table with an index*, not in a projection's grain.

## 7. What a cube is **not**

> "Show me the 50,000 businesses most likely to buy Wewobo this month."

That is **not** an aggregate query. A projection tells you a *segment is worth
~50k* (a cell measure). Producing the 50,000 **rows** is filtered top-K over
per-business features, which BicDB already does better with indexes, BM25
top-k, and ordered scans. Keep the two phases separate:

1. projection → *which segments are worth pursuing* (cheap, precomputed);
2. indexed/ranked scan within those segments → *the actual list*.

Conflating them is where this design would get muddy.

## 8. Distribution falls out of mergeability

Because cell states merge associatively, distributed aggregation needs no new
theory: each shard maintains partial states over its own rows, the coordinator
merges them.

```text
shard A: SUM=12,381 COUNT=194
shard B: SUM=19,921 COUNT=306
merged : SUM=32,302 COUNT=500  -> AVG=64.6
```

The same holds for HLL and KLL. This is the property to protect in every design
decision: **do not introduce a measure whose state cannot be merged**, or
distributed OLAP stops being nearly free.

## 9. Drift is the real risk; reconciliation is not optional

An incremental aggregate that silently disagrees with its base table is worse
than no aggregate, because decisions get made on wrong numbers and nobody
notices. So reconciliation is a first-class, scheduled operation, not a
debugging tool:

- recompute one cuboid from the base table;
- diff against the incrementally maintained cells;
- emit **drift metrics** (cells compared, cells differing, max absolute and
  relative delta) and alarm on any nonzero drift;
- provide targeted repair (recompute the affected cells) and full rebuild.

The acceptance bar for the whole subsystem is: after millions of mutations,
forced crashes, replays and out-of-order delivery,
`incremental == authoritative recomputation`, exactly, for additive measures.

## 10. Evolution: rebuild and swap, never mutate in place

When a dimension is renormalized, an H3 resolution changes, or the category
tree is revised, do **not** attempt clever in-place mutation of every cell:

```text
projection v1 (serving)
projection v2 rebuilding from base
projection v2 catches up with the stream
atomic swap
drop v1 later
```

This is the same shape as the FTS generation swap already in the tree, and it
is why projections are versioned from day one.

## 11. Staged campaign

Each item is one PR, bumps 0.0.1, merges to main, and is ticked here.

- [ ] **G1. Aggregate algebra** — COUNT/SUM/AVG state types, retract/apply/merge
      semantics, refusal of unmaintainable combinations.
- [ ] **G2. Incremental projection runtime** — audit stream → retract(before
      from input state) → apply(after); per-record input state.
- [ ] **G3. Checkpoint & effectively-once** — watermark, replay/duplicate
      suppression, out-of-order rules, crash-atomic advance.
- [ ] **G4. Reconciliation** — base recomputation, diff, drift metrics, repair.
- [x] **G5. Durable projections** — dictionaries, input state, cells and
      watermark persist as ONE atomic unit; crash-tested at every durability
      boundary and under a random crash storm (§12e). Query surface still to
      come.
- [x] **G5.1. Declared-width `CellState`** (§12f) — 72 B struct → 24 B packed
      for one measure, 8 B for `COUNT`-only.
- [x] **G5.2. Query surface** (§12g) — projections are ordinary relations
      under `bicdb_projection.<name>`; `bicdb_projections` reports freshness.
- [x] **G5.3. Multi-projection corpus run** (§12h) — 1,000,000 rows, four
      projections, reconciled clean at every checkpoint. The **real** India
      corpus run additionally needs access to that database; the harness is
      unchanged for it, only the loader. — ordinary SQL against a projection, with
      `projection_position` / lag exposed. Interning **done** (§12c: 273 → 91
      B/row). G5 persists three compact structures — input state, cell state,
      watermark — under one atomic boundary.
- [x] **G5.4. Chaos harness** (§12i) — real `SIGKILL`, corruption,
      truncation, orphan temp files, generations, and a reusable durability
      oracle.
- [x] **G6. Incremental persistence** (§12k)
- [x] **G8. Hierarchical rollups** — admin boundaries, category trees, time
      buckets, H3 parents.
- [x] **G10. `EXPLAIN MATERIALIZED AGGREGATE`** — cost a grain before
      materializing it; warns on poor reduction and names the dominating
      dimension.
- [x] **G11. `RECONCILE MATERIALIZED AGGREGATE`** — the correctness check as
      an operator command, reporting `EXACT`/`DRIFT`.
- [x] **G7. Sketch measures** (§12j) — HLL distinct counts and bottom-k
      quantiles, both associatively mergeable. **Not** yet wired as projection
      measures: sketches are not retractable, so a projection carrying one
      must rebuild affected cells rather than retract — that integration is
      the remaining half.
- [x] **G7b. Wire sketches as projection measures** — HLL for distinct, KLL/t-digest for quantiles,
      labelled approximate.
- [ ] **G8. Rebuild & versioning** — online rebuild, catch-up, atomic swap.
- [x] **G9. Distributed aggregation** — per-shard partial states, coordinator
      merge.
- [ ] **G10. `CREATE CUBE` sugar** — only once every semantic above is boring.

## 12. Non-goals and known risks

- **Not** a replacement for indexed row retrieval (§7).
- **Not** transactionally instantaneous with the base table (§5).
- MIN/MAX and holistic measures are explicitly staged later (§3); shipping them
  early with additive-looking semantics would be a correctness bug.
- The per-record input state (§2) is real memory/disk. A projection's grain and
  measure set must be justified against that cost, and the cost must be
  reportable (`bicdb_memory_report()` already attributes per-collection and
  per-index residency; projections must appear there too).
- This subsystem multiplies work on the **write path**. The crawl-ingestion
  incident (2026-08-16) was a write-path memory and latency failure; projection
  maintenance must be measured against that same workload before it is enabled
  by default anywhere.

## 12b. Measured, not assumed (Tier-1 harness, 2026-08-16)

`crates/bicdb-core/examples/projection_scale.rs` runs a value-free corpus at
production cardinality. Release build, `embedded_memory`:

| rows | rebuild | reconcile | cells | cells/row | input state |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 50k | 0.2s | 0.1s | 43,278 | 0.87 | 273 B/row |
| 100k | 0.2s | 0.3s | 79,323 | 0.79 | 273 B/row |
| 200k | 0.5s | 0.8s | 139,563 | 0.70 | 273 B/row |

`incremental == authoritative` was clean at every round, under host-moves,
deletes and repeated replays.

**Finding 1 — input state must be interned before G5.** The cost is flat at
~273 B/row, so it extrapolates honestly: ~250 MiB at 918k rows, **~19 GiB at
70M**. The dimension data itself is ~23 bytes; the rest is `String`/`Vec`/enum
and `BTreeMap` overhead, plus storing the cell key twice (once in `cells`,
once per record in `inputs`). Interning dimension values into a dictionary and
holding `u32` ids should take this to roughly 40 B/row. This is a
**prerequisite for G5**, not an optimization — §12 said the grain must be
justified against this cost, and at 70M the current representation does not
justify.

**Finding 2 — the grain trap is real and measurable.** `cells/row` is 0.70–0.87
at `state × category × host`: the projection is ~70% the size of the table it
summarizes, which is §6 in numbers. Default grains should be coarse, with
fine dimensions reached by rollup or left to the base table's indexes.

**Adjacent observation, not projection code.** `bulk_load_insert` in
`embedded_memory` is superlinear: 118 → 242 → **1055 µs/row** across those
three sizes. That is why the table stops at 200k and extrapolates rather than
measuring 918k, and it deserves its own investigation — it matters to any
bulk ingest, not just to this harness.

## 12c. G4.5 result — compact input state (2026-08-16)

Three changes, measured with the same harness: projection-local interning
dictionaries (monotonic, never-reused ids, compacted only at generation
rebuild), fixed-width `Copy` state with no `Vec`s, and a 128-bit record digest
as the input-state key instead of the primary-key string.

| rows | before | after |
| ---: | ---: | ---: |
| 50k | 273 B/row | **91 B/row** |
| 100k | 273 B/row | **91 B/row** |
| 200k | 273 B/row | **91 B/row** |

Flat, as before. **3.0× reduction; at 70M rows ~19 GiB → ~6.3 GiB.**
All nine correctness tests pass **unchanged** — the representation was swapped
underneath the contract, which is what made the swap safe to attempt.

**The <50 B/row target was not met, and the arithmetic says why.** The entry is
`u128 key (16) + InputState (64)` = 80, × ~8/7 hash-table slack = 91:

| component | bytes | why it is that size |
| --- | ---: | --- |
| record digest | 16 | audit events carry no integer row identity, only the PK string; 128 bits keeps collisions at ~1e-23 at 70M |
| dimension ids | 16 | `[u32; 4]` — the interning win (was ~150) |
| measure slots | 32 | `[f64; 4]` — fixed at 4 because the design's own example is `COUNT(*)` + three `AVG`s |
| version | 8 | per-record ordering guard for out-of-order delivery |

Measured lever: `MAX_MEASURES = 2` yields **73 B/row**. Not taken — three
measures is a stated requirement, and shrinking a capability to hit a number is
the wrong trade.

**Two real levers remain, both engine changes rather than tuning:**

1. **Size measure storage to what the projection declares** (generics or a
   slab) — recovers up to 24 B/row for the common one-measure projection.
2. **Plumb `RowId` into audit events.** `RowId(u64)` exists in the engine but
   `Record` carries only `id: String`, so the projection cannot key on it. That
   would take the digest 16 B → 8 B *and* remove the collision argument
   entirely.

Together those land ≈ **51 B/row**, and they are the honest prerequisite for
70M-scale — not further tuning of the current shape.

## 12d. G4.6a result — projection-declared layout, format v1 (2026-08-16)

Input state is now a flat slab strided by a width the projection *derives from
its own grain*, not a struct sized for the maximum possible projection.

```text
offset 0        version    u64
offset 8        present    u8
offset 9        dimensions u32 x n
offset 9+4n     measures   f64 x m
width  = 9 + 4n + 8m
```

| projection (3 dimensions) | state width | logical B/row |
| --- | ---: | ---: |
| `COUNT(*)` | 21 | 37 |
| `COUNT(*)`, one `AVG` | 29 | 45 |
| `COUNT(*)`, three `AVG` | 45 | 61 |

Measured for the one-measure shape at 50k/100k/200k, flat at every size:

| metric | B/row |
| --- | ---: |
| logical (identity + state) | **45** |
| packed slab alone | 38 |
| resident (slab + index + slack) | **60** |
| container overhead | 15 |

**91 → 60 B/row resident.** Cumulatively **273 → 60, a 4.6× reduction**, with
all correctness contracts unchanged. At 70M rows: ~19 GiB → ~3.9 GiB.

Logical and resident are reported separately on purpose. Now that the state is
compact the **container is the next enemy**: 15 of the 60 bytes are the
digest→slot index and its hash slack, and the 16-byte identity is a further
third of the logical cost. Both are addressable only by a true logical row
identity (§2b), which is deliberately out of scope.

### Format v1 is frozen here

`ProjectionLayout` byte order is fixed and little-endian so G5 can persist the
slab as-is. **Once G5 makes it durable, changes are a generation upgrade**
(rebuild as v2 → catch up → atomic swap, §10) rather than a struct edit.

### Known remaining: `CellState` is still MAX_EVERYTHING

Cells still carry `[f64; MAX_MEASURES]` sums and counts (72 B) regardless of
the declared measures, and at 0.70–0.87 cells/row (§12b) that is nearly a
per-row cost. Applying the same layout treatment to cells is the obvious next
compaction — it was left out of G4.6a to keep the change reviewable, and it
does **not** block the freeze, since cell state is derived and can be rebuilt.

## 12e. G5 result — durable projections (2026-08-16)

**Dictionaries are durable state, not derived.** The earlier claim that they
were rebuildable was wrong: a persisted slab byte reading `host_id = 174` is
meaningless without the exact dictionary that minted 174, and regenerating
dictionaries independently would silently reinterpret every stored row. They
therefore live inside the same atomic unit as the state referencing them,
which also satisfies the rule that *a dictionary mapping must be durable
before anything references its id*.

The durable unit is: **layout + dictionaries + input state + cells +
watermark**, published by an atomic temp→fsync→rename→sync-parent. A crash at
any instant leaves either the previous complete snapshot or the new complete
snapshot — never a mixture — which is what forbids the dangerous
combinations (watermark ahead of state; cells updated without their input
state; state referencing unpublished dictionary ids).

Restart replays from the persisted watermark; duplicate events are inert
(§4), so resuming from a slightly stale watermark is always safe. That is why
per-event atomicity is unnecessary: the projection is a deterministic function
of the stream from its watermark.

**Format versioning is split deliberately.** `INPUT_STATE_FORMAT` is frozen —
persisted slab bytes are interpreted by it, so changes are a generation
upgrade. `CELL_STATE_FORMAT` is versioned separately and may change freely,
because cell state is derived and rebuildable. Both fail closed on an
unrecognised version rather than misinterpreting bytes.

### Acceptance

| criterion | result |
| --- | --- |
| reopen preserves dictionary ID semantics | ✅ |
| cells / input state / watermark commit atomically | ✅ atomic rename |
| dictionary IDs durable before reference | ✅ same unit |
| duplicate replay after restart inert | ✅ (5 restarts, cells identical) |
| dimension moves survive crashes | ✅ every boundary, incl. a brand-new dictionary value |
| deleting the last contribution removes the cell | ✅ durably |
| no negative cell count can become durable | ✅ asserted after every recovery |
| watermark never ahead of state | ✅ asserted |
| deterministic crash at every boundary reconciles | ✅ 4 boundaries |
| random crash/restart storm reconciles | ✅ 60 rounds |
| the original nine tests, unchanged | ✅ 12/12 |

> **Incremental aggregate projections provide crash-durable, effectively-once
> processing with exact reconciliation against authoritative base state.**

Known limitation: the snapshot is a whole-state document (hex-encoded slab),
so a save is O(state). Fine at present scale and correctness-first by design;
incremental persistence is a later optimization that must preserve the atomic
boundary above.

## 12f. G5.1 result — declared-width cells (2026-08-16)

The same physical principle as G4.6a, applied to the other half. Cells were a
fixed 72-byte struct (`count` + `[f64; MAX_MEASURES]` sums + `[i64;
MAX_MEASURES]` counts) inside a `BTreeMap`, regardless of declared measures.
They are now a layout-strided slab:

```text
offset 0        count       i64
offset 8        sums        f64 x m
offset 8+8m     sum_counts  i64 x m
width = 8 + 16m
```

| projection | cell width |
| --- | ---: |
| `COUNT(*)` only | **8 B** |
| `COUNT(*)` + one `AVG` | **24 B** |
| `COUNT(*)` + three `AVG` | 56 B |

Measured (one measure, 3 dimensions):

| | before | after |
| --- | ---: | ---: |
| cell packed | 72 B | **24 B** |
| cell resident | ~112 B | **59–67 B** |
| total projection @200k rows | 32.3 MiB | **20.4 MiB** |

At 70M rows and ~0.70 cells/row the whole projection lands near **7.3 GiB**
(input ~4.2, cells ~3.1), down from ~9.7 GiB before this change and ~19 GiB
before G4.5.

Safe precisely because **cell state is derived**: it carries its own
`CELL_STATE_FORMAT`, separate from the frozen `INPUT_STATE_FORMAT`, and can be
rebuilt from the base table. The snapshot still serializes cells in the stable
public shape, so the durable wire form did not change even though the
in-memory representation did.

`cell()` now returns `CellState` **by value** rather than a reference, since
the durable form is a packed slab row rather than a struct in a map. That is
the only API change; all seventeen correctness and durability tests pass.

Remaining cell cost is now the container, as with input state: ~23 B of
hash entry per cell against 24 B of payload.

## 12g. G5.2 — query surface (2026-08-16)

Projections are exposed as **ordinary relations**, so the whole SELECT
machinery applies with no cube-specific syntax:

```sql
SELECT state, category, count, avg_score
FROM bicdb_projection.business_market
WHERE state = 'MH'
ORDER BY count DESC;
```

Columns are one per dimension, then `count`, then `sum_<m>`/`avg_<m>` per
measure. Because it is a real relation, **rollup comes for free**:

```sql
SELECT state, sum(count) FROM bicdb_projection.business_market GROUP BY state;
```

A namespace rather than a bare name so a projection can never shadow a real
table; `bicdb_projection` joins `pg_catalog`/`information_schema` as a
qualifier that survives physical-name encoding.

**Freshness is visible, not implied** — `SELECT * FROM bicdb_projections`
reports `projection_position`, `source_position`, `lag_events`, `cells`,
`source_rows`, `resident_bytes` and `input_logical_bytes` per projection.
Reading a projection relation catches it up first, so a query never serves
state older than what is already committed, while the status relation still
shows how far behind the last durable snapshot was.

## 12h. G5.3 — the million-row multi-projection run (2026-08-16)

Four projections of deliberately different shapes over one 1,000,000-row
business corpus, then four recrawl rounds of 50,000 mutations each (dimension
moves, score changes, deletes), **reconciled against authoritative
recomputation after every round** — not only at the end.

| projection | cells | cells/row | B/cell | B/row | rebuild | resident |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `state × category` | 18,000 | 0.02 | 68 | 48 | 3.0s | 47 MiB |
| `state × category × host` | 428,433 | 0.43 | 51 | 52 | 3.1s | 71 MiB |
| `h3 × category` | 850,391 | 0.85 | 73 | 57 | 3.2s | 113 MiB |
| `cms × ecommerce × score_band` | **130** | 0.00 | 113 | 61 | 2.9s | 58 MiB |

```text
load                1,000,000 rows   25.0s
catch_up            ~50,000 events   ~0.9s   (43,000-56,000 events/sec)
reconcile           4 projections    ~17s
all 4 projections   289 MiB resident (306 B/row combined)
every round         incremental == authoritative: CLEAN
```

**Grain dominates everything.** `cms × ecommerce × score_band` reduces a
million rows to **130 cells** — a 7,700× reduction — while `h3 × category`
produces 850,391 cells, 0.85 per row, which is barely an aggregate at all.
Same corpus, same engine, same code path: a **6,500× difference in reduction
ratio purely from grain selection**. That is the empirical case for G10's
`EXPLAIN MATERIALIZED AGGREGATE` warning, and it is why `CREATE CUBE` must
never materialize cuboids automatically.

### What this run cost to make possible

It could not finish before. Bulk ingest was superlinear (100 → 195 → 813
µs/row across 50k/100k/200k), which put a million rows out of reach. Feature
elimination — audit off, no indexes, no FTS, no projections, both storage
modes — ruled out every subsystem and pointed at the transaction itself: row
locks were kept in a `Vec` and membership-checked with `Vec::contains` per
written record, making a transaction's own locking quadratic in its rows.
Fixed, the curve is flat at 6–7 µs/row (**116× at 200k**), and a million rows
load in 25 seconds.

**This is the second general engine defect the OLAP campaign has surfaced**
(the first: catch-up reading the whole event stream per call). Neither is an
OLAP bug; both were found because a projection harness put the engine under a
shape nothing else did.

### Status distinction

- **G5.2:** complete.
- **G5.3 correctness at multi-projection million-row scale:** passing.
- **G5.3 against the real India corpus:** still outstanding — it needs access
  to that database. Nothing in the harness changes for it except the loader.

Remaining known cost in this run: per-row `insert` churn is ~4–5 ms/row
(200–255s for 50,000), because that path is one transaction per record rather
than the batched loader. Bulk ingest is fixed; single-row write throughput is
a separate lever.

## 12i. G5.4 — the hostile harness (2026-08-16)

A **separate child process** opens the database, mutates rows, keeps a durable
projection caught up and checkpoints — in a loop, with no idea when it will
die. A parent murders it with `SIGKILL` at arbitrary instants spanning
startup, mutation, catch-up and snapshot publication. After every kill the
parent reopens, follows the stream from the surviving watermark, and demands
exactness.

Acceptance is one sentence: **no silent wrong result, ever.** Either the
projection reconciles exactly against authoritative recomputation, or BicDB
refuses to load the snapshot.

### What this forced into the design

**A checksum.** The snapshot was JSON with a magic and format versions but no
integrity check. JSON catches structural damage; it does *not* catch a flipped
digit inside a number, which parses cleanly and yields plausible, wrong
aggregates — precisely the failure this subsystem exists to prevent. Snapshots
are now framed in an envelope:

```text
magic · envelope_version · generation · payload_sha256 · payload
```

Any single-bit change is refused rather than interpreted, and `generation`
advances on every publish so recovery can prefer the newest **valid**
generation rather than trusting filesystem timestamps.

**A reusable durability oracle.** `verify_durable_invariants` checks, after
every recovery: exact reconciliation against the base table; the watermark
never ahead of the source; no negative durable cell count; and **no slab row
referencing a dictionary id that was never published** — the exact corruption
a non-atomic dictionary write would cause. It is deliberately reusable: G6
replaces the persistence mechanism underneath it without changing what it
checks.

### Coverage

| hostility | result |
| --- | --- |
| `SIGKILL` storm, child unaware of timing | ✅ 12 kills, 12 exact recoveries |
| snapshot truncated to 99/95/80/50/1% and 1 byte | ✅ refused |
| random bit flips in the payload | ✅ refused (or still exact) |
| orphan `.tmp` newer than the published snapshot | ✅ ignored; publish marker is `rename`, not mtime |
| repeated publishes | ✅ generations advance monotonically |

### A note on the harness itself

The first version **silently skipped** — it looked for `examples/chaos_child`
while cargo had emitted `chaos_child-<hash>`, so five tests "passed" in 0.17s
having done nothing. It now matches on the stem and **fails loudly** if the
child is missing. A chaos test that skips is worse than one that fails.

## 12j. G7 — approximate measures (2026-08-16)

`COUNT`/`SUM`/`AVG` are exact and retractable. Distinct counts and percentiles
are *holistic* — not maintainable exactly in a fixed-width cell — so they are
sketched, and **labelled approximate at the query surface**. A `P95` that
silently returns an estimate is a reporting bug even when the sketch behaves.

**HyperLogLog** (2^14 registers, ~1.6% error), stored sparsely so a cell with
a handful of distinct values costs bytes rather than 16 KiB. Measured within
5% at 10 / 500 / 10,000 / 200,000 distinct values, unaffected by 50× duplicate
insertion, and merge-order independent.

**Quantiles by bottom-k sampling.** Each observation gets a hash-derived
priority; the sketch keeps the k smallest. Deterministic (same stream, same
sample) and associatively mergeable (union, keep bottom k). Exact while a cell
holds ≤ k values, and `is_exact()` says which regime it is in.

> The first quantile implementation kept both tails and thinned the middle —
> deterministic, bounded, and **biased**: on ascending input the median of
> 1..=50,000 came out at **49,871**. It was discarded rather than tuned.
> Confidently-wrong percentiles are worse than absent ones, and a test that
> checked accuracy rather than merely "it returns a number" is what caught it.

Both sketches are **mergeable**, preserving the invariant distribution depends
on (§8), and **neither is retractable** — removing a value from an HLL
register or a sample set is undefined. A projection carrying sketch measures
must therefore rebuild affected cells rather than retract them, which is why
wiring them into `AggregateProjection` is tracked separately (G7b) rather than
bolted on here.

## 12k. G6 — incremental persistence (2026-08-16)

A checkpoint was O(total state): serialize everything, fsync, rename. Now:

```text
<name>.projection/
  manifest.json          <- the atomicity boundary
  pages/
    input-<page>-<gen>
    cell-<page>-<gen>
    dictionary-<dim>-<gen>
```

Only pages mutated since the last checkpoint are written. Pages are
**immutable and generation-stamped**, so they can be written freely before
publication — a half-written page can never be referenced by a published
manifest. Publishing is one small atomic rename. A crash before it leaves
orphan pages that nothing references: inert.

**The design problem was the maps, not the pages.** The digest→slot index and
the cell key→slot map are O(state) to serialize even when the slabs are
paged, which would have defeated the whole exercise. So rows became
**self-describing**: an input row carries its own 16-byte digest, a cell row
carries its key, and both carry a free-slot tombstone. The index, the cell
map and both free lists are now rebuilt by **scanning the pages** at load, so
the manifest stays tiny. That is format v2 (`INPUT_STATE_FORMAT = 2`); v1
snapshots are refused with "rebuild the projection", which is exactly the
documented upgrade path from §12c.

Measured: after a full checkpoint of a 40,000-row projection, mutating five
rows and checkpointing again writes **less than a quarter** of the full state
— asserted, not asserted-to-be-plausible.

### The bug the page design introduced

Rows are not page-aligned, so a row can straddle a page boundary. Marking
only the row's *starting* page dirty would leave the tail of a straddling row
unwritten and silently wrong on reload. Every write now marks every page the
row touches.

### The harness did not change

`verify_durable_invariants` and the SIGKILL storm are **untouched** and pass
against an entirely different persistence mechanism — which is precisely why
G5.4 came first. Only the corruption *injection points* moved (the harness
must know where to damage things), and pages became a new corruption surface:
the manifest can be pristine while a page it references is damaged, so each
page carries its own checksum and a corrupted page is refused.

### Still outstanding

`ENOSPC`/`EIO` injection still needs a filesystem seam around the publisher;
the page writer is the natural place for it.

**Orphan pages are reclaimed as of 1.0.213-beta.** A publish that failed after
writing its pages left them on disk forever — inert, because nothing
referenced them, but a projection that failed to publish repeatedly grew
without bound. A successful publish now also deletes unreferenced page files.

The sweep is bounded to generations **strictly older than the published one**.
A save in flight writes at `published + 1` and has not published yet, so its
pages are unreferenced too; bounding below the published generation makes it
impossible to delete work another writer is still doing.

## 12l. G7b — non-retractable measures (2026-08-16)

Every measure until now was an abelian group: apply adds, retract subtracts,
they cancel exactly. **Sketches are not.** Removing a row from a
distinct-count cannot decrement anything — the sketch does not know whether
another row carried the same value — and removing a sample from a bottom-k
sketch cannot recover the sample that should replace it.

So sketch measures get a *different, stated* contract:

> **Retract marks the cell stale; a stale cell is recomputed from its own
> rows.**

That is `O(cell)`, not `O(table)`, because each cell threads its input rows on
a doubly-linked list. `COUNT(DISTINCT host)` and `p95(price)` are now
declarable measures, exposed as `distinct_<name>` / `p50_<name>` /
`p95_<name>` columns on the projection relation.

### Two decisions that made it cheap

**Sketch state is derived, so it is never persisted.** It is rebuilt in the
page scan `load` already performs. Adding sketches therefore costs the G6
manifest nothing and adds no new way for a checkpoint to be corrupt. The row
chains are derived the same way.

**The quantile sketch is keyed on record identity, not arrival order.** The
old `Quantiles::add` drew its sample priority from an insertion counter, which
makes "rebuilt" and "incrementally maintained" two different samples of the
same data — and then the reconcile oracle cannot check sketches at all. The
new `add_keyed(value, identity)` derives priority from the record digest, so
bottom-k of the union equals the union of bottom-ks. That buys **rebuild ==
incremental** (this feature) and **merge == whole** (G9, for free).

### The one that would have shipped silently

`apply_input` seeded quantile priorities from the *event position* while
rebuild used the *record digest*. Distinct-counts matched perfectly; only the
percentiles diverged, and only after a retract. A test asserting "reasonable
percentiles" would have passed. The test asserting *rebuild equals
incremental* failed immediately — which is the entire argument for making
that the acceptance property rather than a plausibility check.

### Fast path

A sketch only goes stale when a row actually **leaves** its cell or its sketch
input **changes**. A recrawl where prices move but hosts do not leaves the
underlying set identical, so nothing is rebuilt.

### A G6 regression this campaign caught

`list_projections` still matched `.projection.json`, the pre-G6 single-file
name. G6 made a projection a *directory*, so `bicdb_projections` listed
**nothing** while every projection kept working — visible only through the SQL
catalog, which the G6 tests did not touch. Fixed here.

## 12m. G8 — hierarchical rollups (2026-08-16)

`ROLLUP MATERIALIZED AGGREGATE <name> TO (day PREFIX 7, category KEEP)`.

Levels: `KEEP`, `WHOLE` (drop the dimension), `PREFIX n`, `SEGMENT <sep> n`,
`BUCKET n`, and `H3 <resolution>`.

### Why this is not a GROUP BY

Rolling up by **dropping** a dimension was already free — `GROUP BY` over the
projection relation does it. Two things needed engine support:

1. **Coarsening a value**: `2026-08-16` → `2026-08`, h3 r8 → r6,
   `food/pizza/napoli` → `food/pizza`.
2. **Measures that do not add.** `SUM` and `COUNT` roll up by addition, so a
   `GROUP BY` gets them right by accident. `COUNT(DISTINCT)` and percentiles
   do **not**: adding two cells' distinct-counts double-counts every value
   they share. Rollup merges their sketches instead, which a `GROUP BY` over
   materialized numbers structurally cannot do.

The second point is the whole justification. `rolled_distinct_counts_do_not_add_up`
rolls a corpus whose days deliberately share hosts and asserts the answer is
the true 12, not the inflated sum.

### Layering

`bicdb-core` owns the merge arithmetic and the dependency-free levels;
`rollup_with` takes a caller-supplied coarsening function so vocabularies
needing crates core does not carry — H3 parents need `h3o`, which lives in
`bicdb-sql` — plug in without inverting the dependency.

### What G7b paid for here

Rolled-up percentiles match a natively-coarse build **exactly**, not
approximately, because bottom-k of a union is the union of bottom-ks. That
only holds because G7b keyed sample priorities on record identity. Had the
sketch stayed order-dependent, this section would have had to say "close
enough".

## 12n. G9 — distributed partial aggregation (2026-08-16)

`MERGE MATERIALIZED AGGREGATE shard_north, shard_south`, plus a
`PartialAggregate` wire format that shards export and a coordinator combines.

### The landmine: dictionary reconciliation

Every projection interns its dimension values into a **local** id space, minted
on first sight. Shard A's id 7 and shard B's id 7 are different strings.
Merging over interned keys would produce a result with every cell present and
every number wrong — the worst failure mode available, because nothing looks
broken.

So a partial is exported in **resolved value space**. That is not a compromise
on compactness: compactness matters for RESIDENT state, and a partial is a
one-time export, so it can afford to pay for its own safety.
`shards_disagree_about_interned_ids` builds two shards over identical data in
opposite insertion order to show the disagreement is real, not theoretical.

### Sketches merge; they do not add

Two shards that both saw `host3` must count it **once**. Additive measures
add, sketches merge — and merged percentiles match a whole-corpus build
**exactly**, again because G7b keyed sample priorities on record identity.

### Two decisions worth recording

**Freshness is the MINIMUM watermark, not the maximum.** A merged answer is
complete only through the point every shard has reached. Reporting the max
would claim a freshness the result does not have.

**Collection names are not compared; the grain is.** Shards live in separate
databases under one name in one topology, and in one database under different
names in another. Neither is more correct, and the name is a poor proxy for
the real precondition anyway.

### The precondition that cannot be checked

**The shards must partition the data.** A record counted by two shards is
counted twice, and nothing in a partial carries the record identity that would
let this be detected — that identity is exactly what aggregation discarded.
This is a deployment property, stated in the API docs, not a merge-time
condition. Do not let a future change pretend otherwise.

## 12o. G12 — `CREATE CUBE` (2026-08-16)

```sql
CREATE CUBE sales_cube ON sales
  DIMENSIONS (region, category)
  MEASURES (SUM(price), COUNT(DISTINCT host), PERCENTILES(price));

REFRESH CUBE sales_cube;
DROP CUBE IF EXISTS sales_cube;
```

The DDL this campaign deliberately withheld at §1, so the syntax describes
what the engine can actually **maintain** — additive measures it can retract,
sketches it can rebuild and merge — rather than promising an aggregate
vocabulary and discovering later that half of it cannot be kept current. Every
measure form maps one-to-one onto an existing engine capability.

### The estimator refuses, it does not warn

`CREATE CUBE` runs the G10 grain estimator **before materializing anything**.
A cube whose grain is nearly as fine as the table is a second copy of the
table; building it and then reporting the memory it consumed is not a warning,
it is an incident. `WITH FORCE` overrides — a refusal that cannot be
overridden is a policy, not an estimate.

### Cubes stay in their own namespace

A cube is queried as `bicdb_projection.<name>`, not as a bare name. The
temptation was to let `CREATE CUBE x` be followed by `SELECT * FROM x`, but
the namespace exists precisely so a projection can never shadow a real table.
`CREATE CUBE` returns the relation name in its result instead — closing the
ergonomic gap with information rather than by relaxing the rule.

### One inconsistency this shook out

The relation surface reported `distinct_host` as `15.0068…` while ROLLUP and
MERGE reported `15` — the same measure typed two different ways depending on
which path you took. Distinct-counts are approximate but they are still
counts, and `15.0068` advertises a precision the estimate does not have while
reading as a bug. All three surfaces now type them as integers.

## 12p. Checkpoint hygiene — three bugs in the G6 writer (2026-08-16)

G6's crash-safety argument rests on page files being **immutable and
generation-stamped**. Repeated checkpoints from one live projection broke all
three parts of it, and one root cause explains everything: `save` took
`&self`, so it could not record anything about what it had just done.

| Symptom | Consequence |
|---|---|
| `generation` never advanced | Every checkpoint published generation 1 |
| Published pages rewritten in place | A crash mid-write corrupts the **already-published** checkpoint |
| Dirty set never cleared | Every checkpoint re-wrote everything dirtied since the projection was built |

Measured before the fix: the second checkpoint wrote **151,745 bytes against a
full state of 151,745** — G6's incrementality was entirely absent.

### Why the G6 tests missed it

`projection_incremental_persistence` went through a `load()` between its two
checkpoints, and loading resets the dirty set and the persisted-page map from
the manifest. That is the *reload* path, which was always correct. The
*live* path — checkpoint, keep mutating, checkpoint again — was never
exercised, and it is the path a long-running server actually uses.

### The fix

`save` now takes `&mut self` and, only after `write_atomic` returns:

1. adopts the published generation,
2. clears the dirty sets,
3. records the manifest's page map as the new durable baseline,
4. deletes page files the **previous** manifest referenced and this one does
   not.

Pruning is scoped to the previous manifest's set on purpose. Files written by
a failed publish are left alone rather than raced against, and a reader midway
through the previous checkpoint keeps the files it is reading until a publish
supersedes them. Removal failures are non-fatal: the checkpoint is already
durable, and refusing to report success because cleanup failed would turn a
full disk into a lost checkpoint.

## 12q. Test targets that were never compiling (2026-08-16)

Running `cargo test --workspace --examples` selects **only example targets**.
It reports a clean run without building the test suites at all — which is how
the following went unnoticed:

- `crates/bicdb-sql/Cargo.toml` declared an example, `tpcc_direct_probe`,
  whose source file is not in the repository. `cargo check --all-targets`
  fails on a clean checkout. Left over from the TPC-C campaign.
- `bicdb-core/tests/core.rs` did not compile: two `DbConfig` literals predated
  `paged_wal_segment_bytes`.
- `bicdb-sql/src/tests.rs` did not compile: two `RoutineSchema` literals
  predated the `schema` field added by the pg_dump trigger fix.

Both test files are tracked and had simply stopped building, so their
assertions had not run since those fields landed. Verify with
`cargo test --workspace --all-targets`; `--examples` is a filter, not an
addition.

## 12r. Interrupted-publish temporaries, everywhere (2026-08-16)

The projection page leak turned out to be one instance of a repeated pattern:
a subsystem writes to a temporary, renames it into place, and **never reclaims
the temporaries left by publishes that were interrupted before the rename.**

| Subsystem | Leaked | Effect |
|---|---|---|
| Aggregate projections | superseded `pages/*` | Unbounded growth per checkpoint |
| Backup | `<archive>.tmp`, `.<archive>.*.tmp` | A half-archive per interrupted backup, forever |
| HA standby | `*.ha-tmp` | A full-sized partial copy per interrupted apply, forever |

The HA case was the most durable-looking: `collect_ha_files` **skips**
`.ha-tmp` when enumerating, so the leftovers were invisible to every later
apply — correct for correctness, and precisely why nothing ever removed them.

All three now reclaim on the same principle: sweep **after** the publish is
durable, scope the sweep to names derived from the target, and treat removal
failure as non-fatal. Refusing to report success because cleanup failed would
turn a full disk into a lost backup, a failed standby sync, or a lost
checkpoint — strictly worse than the leak.

## 12s. What the restored targets were hiding (2026-08-16)

Making the un-compiling targets build again surfaced seven failures. Six were
real and are fixed; none were flaky.

**Three were assertions that outlived the behaviour they described.** Each
asserted an *absence* that a later campaign had filled in:

- The Postgres-compat scorecard asserted `CALL` returns SQLSTATE `0A000`.
  CALL execution landed during the TPC-C work, so the scorecard was reporting
  a capability BicDB **has** as one it lacks. A compat report that understates
  the engine is as wrong as one that overstates it.
- A geo test asserted `MULTIPOINT` was an unsupported WKT type. The geo
  campaign added `Multi*`, so the assertion was the wrong way round. Every
  type WKT can express is now supported, so the remaining negative case moved
  to a GeoJSON `type` that is not an OGC geometry.

**Three were security gates the callers never opted into.** Mesh replication
became opt-in per collection and imports became TOFU-pinned. The export path
**silently skips** un-authorized collections — correct, because a mixed
database must not leak un-authorized data into a bundle, but it means a caller
that forgets to opt in measures *exporting nothing*. The sync bench reported
`converged: false` rather than failing. Fixed by authorizing collections and
exchanging verifying keys the way a deployment does, rather than disabling the
checks.

**One was a genuine metric bug.** The `pg_sleep` cancellation path incremented
`canceled_queries` *and* returned `QueryCanceled`, which the classification
site then counted again. One cancellation, charged twice — which is exactly
what server certification saw: two cancelled queries against one failed query.
Deterministic, not racy. The detection site no longer counts; the single
classification site does.

### Still open: bounded vacuum cannot finish a large page

`public_supervisor_restarts_every_step_and_publishes_completion_atomically`
still fails, and it is a real engine liveness bug rather than a stale test.

Traced: every step starts at page 5, enters it, exceeds the byte envelope
while walking that page's version chains, and reports `next_cursor =
Some(5)` — the same page. `VacuumCursor` carries only `next_page_id`, so
there is **no way to express partial progress within a page**. The sweep
therefore restarts the page from the beginning on every step, forever:
`versions_reclaimed` climbed past 61,000 from a 600-row table while
`bytes_reclaimed` and `pages_freed` stayed at **0**.

`paged_maintenance.rs` already has a stall guard for this exact class, but it
requires `pages_scanned == 0` — and here the page *is* entered, so it never
fires. Two candidate fixes, neither attempted here because both change the
sweep contract:

1. Give `VacuumCursor` an intra-page position so a large page resumes.
2. Make a page the atomic unit — check the byte envelope only *between*
   pages, so a started page always completes. This matches the cursor's
   existing page granularity: a limit that can stop mid-page produces a state
   the cursor cannot represent.

Whichever is chosen, the stall guard should also be broadened to a true
non-progress predicate (cursor unchanged **and** nothing freed), so this fails
loudly instead of burning I/O and checkpoints indefinitely.

## 12t. Bounded vacuum: the page is the atomic unit (2026-08-16)

The liveness bug in §12s was not a tuning problem. It was a **contract
mismatch made visible**: `VacuumCursor` is page-granular — it can say "resume
at page N" and nothing finer — while the byte envelope could interrupt work
*inside* a page. The only way to encode "stopped halfway through page 5" was
to point back at page 5, so the next step redid it. Forever.

### The new contract

> The byte envelope is checked **between pages**. Once vacuum starts a page it
> may exceed the requested budget by enough to finish that page and advance
> the cursor.

The trade is explicit: **the byte limit is a soft bound whose maximum
overshoot is one page's vacuum work.** For a maintenance API that is a far
healthier promise than a hard limit the cursor cannot resume from.
`max_pages` stays hard, because a page count *is* expressible in the cursor.
The time limit is soft for the same reason and with the same bound — a step
that has entered a page finishes it before checking the clock again.

All three mid-page bailouts (header bytes, overflow release, whole-page free)
were removed; the loop now decides only between pages, and only once at least
one page has completed.

### The invariant that makes the bug unrepresentable

> If a vacuum step enters a page, it must advance beyond that page or return
> an error.

`start cursor 5 -> end cursor 5, success` is now impossible: the engine
returns an error, and `VacuumReport` carries `start_page`/`end_page` so the
symptom is legible directly. That is the metric that would have made this
obvious on day one — `versions_reclaimed = 61,000` on a 600-row table merely
looks odd, but **`5 -> 5` repeated a thousand times names the bug**.

### Progress is durable movement, not counters

The old stall guard keyed on `pages_scanned == 0`, which is precisely the case
that did *not* happen. Progress is now:

```text
next_cursor != previous_cursor || pages_freed > 0 || bytes_reclaimed > 0
```

Counters that can re-count the same work cannot define progress. The unit test
`a_sweep_that_reenters_the_same_page_pauses_despite_climbing_counters` feeds
the supervisor a target whose `versions_reclaimed` rises every step while the
cursor stands still, and requires a pause.

### Why not an intra-page cursor

Rejected deliberately. It would push partial-page state into recovery, cursor
serialization, format compatibility and every test that touches them, to buy
something only needed if a *single page* could violate an operational latency
requirement. If that day comes the option is still open; today it is cost
without benefit.

### Tests

`paged_vacuum_progress.rs` (8) pins the contract: a budget smaller than the
first page still advances; the cursor advances monotonically and never
repeats; no page is reclaimed twice; a pathological long-chain page finishes
and advances; a tiny budget terminates; a full sweep reaches `None`; and a
completed sweep is a **fixed point** — a second sweep reclaims nothing, which
proves the first skipped no work without encoding assumptions about heap
layout. The original 600-row supervisor reproducer is unchanged and passes.

## 13. Test corpus

The million-business India crawl is the corpus: real, mutating, and
independently verifiable. Every claim in §9 can be checked by asking the base
table the same question:

```text
COUNT by state/category      AVG score by state/category
COUNT by CMS                 AVG performance by hosting provider
COUNT ecommerce by district
```

Run incremental maintenance under recrawls and forced crashes, then recompute
from source. If they match exactly for additive measures, the subsystem is
trustworthy; if they do not, nothing else about it matters.
