# Typed resident rows: replacing JSON text as the row body

Status: design (2026-09-03). Owner: performance campaign (road to 500k / PG parity).

## Why this and not the earlier "typed rows"

Two earlier attempts cached a *typed view next to* the JSON text (`TypedRow` in
`StoredRecord::typed`, `BICDB_TYPED_ROWS`): both regressed (−2.1%, −7.9%). They
added memory and a second representation to keep coherent while every write
still produced and parsed JSON. The cost that actually matters on TPC-C is the
JSON tokenizer on every touch:

| where (benchmark-primary profile, main@3a741ebc)         | share |
|----------------------------------------------|-------|
| `cells_into` (reads: slot rows, UPDATE, join) | 7.8%  |
| `to_record` / `metadata_value` (write side)   | 2.8%  |
| `StoredRecord::from_record` (serialize)       | ~1%   |
| checkpoint re-serialization (bg thread)       | 2.2%  |
| `format_escaped_str` / `skip_to_escape`       | 3.4%  |

Replacing the body removes all of it; caching a view on top of it does not.

## Target format (v3 row body)

One resident body per record, also the WAL/page payload:

```
header  : u8 version(=3) | u16 column_count | u32 body_len
dir     : column_count × { u32 name_id, u8 kind, u32 offset }   (kinds below)
payload : cells back to back
```

- `name_id` indexes a per-collection interned column-name table (already
  exists for cubes: reuse the interning dictionary); unknown/dynamic keys use
  an inline-name kind.
- kinds: Null, Bool, Int(i64 varint), Float(f64), Str(len+utf8), Numeric
  (scale u8 + i128 or decimal text when it does not fit), Timestamp(i64 µs) /
  Date / Time, Envelope(pg_type_id + text + index_key bytes) for the remaining
  typed storage, Raw(JSON text) for nested objects/arrays and anything the
  encoder does not understand.
- Cells are self-describing so a reader never needs the schema; the SQL
  layer's `CellRef` decoder becomes a directory walk (O(1) per referenced
  column) instead of tokenizing.

## What changes, by layer

1. `bicdb-core::record` — `StoredRecord { body: RowBody }` where
   `RowBody::Json(RawValue) | RowBody::Cells(Bytes)`. `cells_into`,
   `to_record`, `metadata_value`, `from_record`, `record_from_cells` get a
   Cells arm; the JSON arm stays for v1/v2 rows. `to_record` on Cells builds
   the `Value` tree only for callers that still need it (predicates, FTS,
   spatial, events).
2. Write side — `StoredRecord::from_record` encodes Cells directly from the
   `Record` value tree (no JSON text); UPDATE's write side (after #760) builds
   the new body from the old Cells + assigned values: copy unchanged cells,
   re-encode changed ones. No document re-serialization.
3. WAL — `TxFrameRef::WriteStored` carries the body bytes as-is with a body
   version tag; replay accepts both. Page segments store the body verbatim.
4. Indexes — `stored_index_key` reads cells by name_id; `OldImage`/new-record
   key memos stay.
5. Wire/SQL — `slot_row_from_cells` and `CellRef` unchanged in shape; the
   Envelope/Numeric/Temporal cells map to `SqlValue` without text parsing
   (Numeric → the (i128, scale) fast path; Timestamp → `PgTimestamp` then the
   canonical text only when displayed).
6. Migration — `format.json` body_version; new writes produce v3 once every
   reader in the process understands it (feature flag `BICDB_ROW_BODY=cells`,
   default on after the A/B); old rows convert lazily on rewrite; a
   `bicdb store rewrite-rows` CLI converts in bulk. Backups/replication carry
   the tagged bytes.

## Phases

- P1 (core, no behavior change): `RowBody` enum + Cells encoder/decoder +
  differential tests (encode(decode(x)) == x; cells_into equality vs JSON on
  every shape in `record_from_cells_equals_the_parsed_record_or_declines`).
- P2: readers (`cells_into` Cells arm) + writers behind the flag; WAL tag;
  A/B on the VMs (expected: −7% cells_into, −3% escaping, −1% from_record).
- P3: UPDATE patching from cells (write side never builds `Value`); INSERT
  encodes from evaluated values via the per-node layout.
- P4: checkpoint/page path stores bodies verbatim (removes the background
  re-serialization); CLI rewrite; flip default.

## Risks

- Every consumer of `metadata_value()`/`to_record()` keeps working through the
  JSON arm, but any that parses `StoredRecord.metadata.get()` text directly
  must be found: grep count today is small (see campaign notes) and all in
  core.
- Ordering/equality semantics of JSON objects (duplicate keys, key order) —
  the encoder must reproduce the parser's last-wins and BTreeMap ordering
  when converting back to `Value`.
- Memory: v3 bodies are ~30-40% smaller than JSON text for TPC-C rows
  (numeric/temporal envelopes shrink most); no extra cache.
