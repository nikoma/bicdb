# Integer RowId migration (PostgreSQL-TID-style record identity)

Status: in progress on branch `perf/integer-rowids`.

## Why

Fresh on-CPU profile at vu16 (memory: `bicdb-vs-pg-5x-gap-cpu-profile`) shows the
single largest identifiable difference vs PostgreSQL is record identity:

- Every index lookup returns `Vec<String>` record ids. The read path
  (`indexed_record_ids_for_table_selection`) **clones** those ids into a `Vec<String>`,
  then fetches each record by hashing the String again. `String::clone` is **7.45%**
  of CPU and drives much of the **~12%** allocator cost.
- PostgreSQL indexes return 6-byte **TIDs** (block, offset): no allocation, direct
  heap access.

The fix: give every physical record a `Copy` integer locator (`RowId(u64)`), store
that as the index payload, and key the record heap by it — so an index lookup yields
`Copy` rowids and a fetch is a `u64` hash with **zero String allocation**.

## What does NOT change (key insight that bounds the blast radius)

Indexes in bicdb are **derived**: `build_index_state_with_options` rebuilds every
index from the record heap on `open`/recovery. They are never persisted. Therefore:

- **WAL** (`RecordFrame`) — unchanged. Still serializes the whole `Record` (whose
  `id: String` field is the logical PK).
- **Snapshots / on-disk segments** — unchanged. Existing seed snapshots stay
  compatible; no migration of on-disk data.
- **`Record.id`** — unchanged. It remains the user-facing logical primary key.

RowId is a **purely in-memory physical locator**, assigned on insert/recovery and
re-derived fresh on every `open` (like a TID is physical, not logical identity). It is
never written to disk, so there is no format-compatibility surface.

## The RowId encoding (TID analog)

```
RowId(u64):
  bits 63..58 (top 6)  = shard index (0..SHARD_COUNT-1, SHARD_COUNT=64)
  bits 57..0  (low 58) = per-shard monotonic local sequence (process-lifetime unique)
```

- A NEW record's shard is `shard_index(pk)` (hash of the String PK) — **identical to
  today**, so commit concurrency to disjoint keys is unchanged and a record's PK,
  version chain, and rowid all live in the SAME shard (single-lock atomic commit per
  key preserved).
- `rowid.shard()` = `(rowid >> 58) & 63` → O(1), no String. The read path routes an
  index-supplied rowid straight to its shard.
- Low 58 bits = ~2.9e17 rows per shard per process lifetime; never reused within a
  run (monotonic). Re-derived on open, so no persistence/uniqueness-across-restart
  requirement.

## Heap re-key (per `Shard`)

| field | before | after |
|-------|--------|-------|
| `records` | `FxHashMap<String, RecordEntry>` | `FxHashMap<RowId, RecordEntry>` |
| `versions` | `FxHashMap<String, Vec<VersionedRecord>>` | `FxHashMap<RowId, Vec<VersionedRecord>>` |
| `version_max_tx` | `FxHashMap<String, u64>` | `FxHashMap<RowId, u64>` |
| `dirty_since_checkpoint` | `FxHashSet<String>` | `FxHashSet<RowId>` |
| `pk_to_rowid` | — (new) | `FxHashMap<String, RowId>` |
| `next_local` | — (new) | `u64` (per-shard rowid allocator) |

`Record.id` (the String PK) still lives inside each `RecordEntry`/`Record`, so the
checkpoint/dirty path materializes a frame by reading `record.id`; nothing on disk
needs the rowid.

## Index payload

`OrderedIndexStore` value type `BTreeSet<String>` → `BTreeSet<RowId>` (Copy, 8 bytes,
ordered, dedup). Within one index key, rowid order vs PK order is irrelevant to
correctness (ORDER BY is on the key fields; intra-key tiebreak was already arbitrary).

## Public API

- New, hot-path: `lookup_index_rowids`, `…_exact`, `…_extreme*`, ordered-scan, and a
  `get_record_by_rowid(rowid)` fetch. These yield/consume `Copy` rowids — no String.
- Existing String-returning APIs (`lookup_index`, etc.) are **kept** and reimplemented
  on top (rowid → `record.id` clone) so all non-hot callers compile unchanged.

## Phases (each compiles + passes tests; commit per phase)

1. **Storage migration (correctness-preserving, no perf win yet).** Add `RowId`; re-key
   the heap; index payload = RowId; add `pk_to_rowid` + per-shard allocator; add rowid
   APIs; keep every String API working on top. bicdb-sql untouched → still clones
   Strings, still correct. All bicdb-core + bicdb-sql tests green. This quarantines the
   risky storage change behind a zero-behavior-change build.
2. **Flip the hot read path.** Switch `indexed_record_ids_for_table_selection` /
   `records_for_ids` to the rowid APIs. **This delivers the win.** A/B on benchmark-primary.
3. **Cleanup.** Vector store record ids, any remaining String-id hot spots, drop dead
   String APIs if unused.

## Invariants / risks

- `pk_to_rowid` and the rowid-keyed maps must stay consistent under concurrent
  per-shard commits — they're mutated together under the same shard write lock.
- Re-key must preserve MVCC version-chain semantics and `version_max_tx` conflict
  detection (now keyed by rowid; committer resolves rowid via `pk_to_rowid`).
- Validate with: full test suites, `examples/tpcc_decay_probe.rs`, and a benchmark-primary A/B
  (memory: `bicdb-vs-pg-5x-gap-cpu-profile` for the baseline numbers).
