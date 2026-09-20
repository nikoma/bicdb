# Durable rowid index entries (v3 entry format)

## Problem

A durable ordered-index entry's key is `(escaped encoded_key, 0x00 0x00, pk)`.
The pk string is both the duplicate-key tiebreaker and the row locator. With
UUID pks that is ~36 bytes of every entry — usually more than the indexed
value itself. Consequences: fat leaves (fewer entries per page), deeper trees,
more pages touched per prefix scan, more WAL bytes per index write. Postgres
pays 6 bytes for the same job (TID).

## Why not physical locators as entry identity

PG can key on TIDs because live heap tuples never move. Our heap relocates
tuples (vacuum) and every update produces a new chain head, so a physical
locator in the entry KEY would need index rewrites on unrelated maintenance.
Physical locators therefore stay what PR #434 made them: advisory VALUE-side
hints, validated and fallback-protected.

## Design: per-collection intern ids

An **intern id** is an 8-byte, per-collection, monotonically allocated id,
assigned to a pk the first time an ordered-index entry is written for it, and
never reused or reassigned for the lifetime of that pk (delete may retire it;
a re-inserted identical pk MAY receive a fresh id — equality of ids is never
used across the delete boundary).

Durable keyspace additions (all inside the existing paged store, MVCC rows
like everything else):

- `intern_fwd(collection, id)  -> pk bytes` — resolution on the read path.
- `intern_rev(collection, pk)  -> id BE bytes` — dedup on the write path.
- `intern_ctr(collection)      -> next id` — allocation counter, updated in
  the same paged transaction as the allocation itself (crash-atomic with it).

Entry format **v3**: `(escaped encoded_key, 0x00 0x01, id BE8)`; the value
carries the same 22-byte TID hint as v2. The `0x01` terminator byte
distinguishes v3 from v2 (`0x00 0x00`) in the same keyspace, so one index is
always homogeneous but readers can decode either.

Catalog: `PagedDurableIndexDef.entry_format` (v2 default for existing
indexes; v3 for newly created ones). The maintenance record persists it.

## Read path

Scans yield `(encoded_key, EntryRef)` where `EntryRef = Pk(String) |
Intern(u64)`. Fetch order for `Intern`:

1. TID hint from the entry value (today's machinery, unchanged) — hit rate is
   ~100% read-mostly, and the hint validates against slot reuse via xmin.
2. Miss: `intern_fwd` point lookup (dense 8-byte-suffix subtree — shallow and
   hot) → pk → existing pk fetch path.

The pk row tree and all public String APIs are unchanged.

## Write path

`fill`: on upsert of an indexed row, resolve-or-allocate the intern id once
per row per transaction (rev lookup; miss → counter bump + fwd/rev insert in
the same paged transaction as the entry). Delete of an entry uses the rev
mapping; deleting the ROW retires fwd/rev rows (normal MVCC deletes — old
snapshots still resolve).

## Migration

`bicdb index rekey <path> <index>`: resumable, bounded-memory rewrite of a
v2 index's entries to v3 (scan entries in key order, allocate/resolve intern
ids, write v3 entry + delete v2 entry in bounded batches, checkpoint after
each batch; flips `entry_format` in the catalog last). Old stores work
indefinitely without migrating — v2 stays a first-class read/write format.

## Expected wins (to be measured in S4)

- Entry key: 36+ bytes pk suffix → 9 bytes. UUID-keyed index entries shrink
  ~2–3×; leaves hold proportionally more entries.
- Prefix scans touch proportionally fewer pages.
- WAL bytes per indexed write drop by the same entry-size delta.

## Non-goals

- No change to record storage keys (rows stay pk-keyed; that migration, if
  ever, is a separate campaign).
- No cross-collection global rowid.
- Resident-mode indexes (in-memory RowId) unchanged.
