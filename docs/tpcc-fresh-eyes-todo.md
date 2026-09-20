# TPC-C fresh-eyes review and todo (2026-09-01)

Status of the campaign when this was written: high-water 418.4k DSUM NOPM
(VU32, 16 warehouses, checkpointer ON, thin LTO) against PostgreSQL 16 at
~640k on the same benchmark-primary host. Target remains >= 500k (+20%). Full history is
in `docs/tpcc-performance-ledger.md`; this file is the plan going forward.

## Where the gap is now

| | bicdb | PostgreSQL 16 |
|---|---|---|
| NOPM (benchmark-primary, 16 wh) | 418k @ VU32 | 642k @ VU16 |
| transactions / s (NOPM / 0.449 / 60) | ~15.5k | ~23.8k |
| server cores busy | ~20-22 | ~16 |
| CPU per transaction | ~1.3 ms | ~0.6 ms |
| single-connection NEWORD wall | 1.2-1.35 ms | ~0.85 ms |
| WAL per transaction | ~7 KB (full JSON row images) | ~1.5 KB |

The load-dependent gap is gone; bicdb is CPU-bound at roughly 2x PG's
per-transaction cost. The profile is flat (allocator 7-12%, memmove 5-6%,
String::clone ~6%, memcmp 5-6%, to_record ~8.6% incl, lock_shared_slow 7-10%)
because the cost is plumbing tax spread across many symbols, not one hot
function. Instruction-shaving micro-cuts have repeatedly landed neutral; the
changes that won (RowId identity +41%, Rc-ids +27%, inline index payloads,
thin LTO, mimalloc purge delay +3.4%) all cut memory traffic or contended
cache lines. Every item below is chosen from that class.

## Regression found while re-baselining (2026-09-01)

Nobody had run HammerDB between the 418k high-water (2026-07-02) and this
campaign. Rebuilding the seed and the first base trial on benchmark-primary exposed a
stack of regressions on `main`, all fixed in their own PRs before any A/B:

| Symptom on benchmark-primary | Cause | Fix |
|---|---|---|
| Seed build refused at the final `ANALYZE` | bare `ANALYZE` by a non-superuser was refused instead of analyzing owned tables | #714 |
| 6 of 8 loaders dropped with `Resource temporarily unavailable` | the 250 ms NOTIFY poll read-timeout also governed mid-message reads | #714 |
| 21 of 33 VUs `FATAL: too many connections` | pgwire's per-IP cap of 20 had no CLI flag | #716 |
| 8 of 32 VUs die on `invalid input syntax for type numeric: ""` | `0 + CAST(NULL AS NUMERIC)` was a parse error, hit on NEWORD's 1% invalid-item path | #717 |
| **30k NOPM** (vs 418k), 77% of CPU under `row_description_with_db` | pgwire asked "is this OID a table row type / user type?" per result column by listing and JSON-parsing the whole catalog (since #114/#119, 2026-07-19) | #718 |
| 120k NOPM, ~13% CPU in the routine EXECUTE gate | per stored-function call: re-read role, re-read routine, rescan grants | #719 |

| 133k NOPM; schema cache hit deep-cloned `TableSchema` per row, `table_oids` per result, memory-job record clones per commit | per-row type inference loads schemas; column metadata lists the catalog; idle memory-job finalization | #720 |
| every NUMERIC cell read cloned + trial-deserialized twice | `$bicdb_typed` envelope decode | #721 |
| PL/pgSQL binary operators inferred operand/result types 5× per evaluation | typed-semantics work of July 17–19 | #724 |
| per-row predicate type inference; one SqlEngine + bound-context rebuild per UPDATE…FROM candidate row | typed-semantics work; UPDATE…FROM executor | #725, #726 |
| `bicdb_replication_status` after a long run allocates ~30 GB | unknown (product bug, open); harness skips it | #723 (harness) |
| seed loader OOM-killed at 53 GB on a 62 GB host (development-host) inside HammerDB's closing `ANALYZE`; the load itself sits at 13 GB | every ANALYZE pass materialized the whole table (core cloned the record map, SQL scanned it three more times and built per-row string pairs per column) | #729: PostgreSQL-style 30 000-row reservoir sample per table (`BICDB_ANALYZE_SAMPLE_ROWS`), exact under the limit, Duj1 distinct estimate |
| `bicdb compact` on the fresh 5.9 GB seed WAL killed at 50 GB | compaction decoded every record of a collection and serialized every frame before writing; and opening a store whose load sits only in `transactions.log` replays it into memory first (every decoded write buffered, then applied: several times the log size resident before the first segment exists) | #729: segment rewrite streams 4096-record batches (compact of the materialized seed now peaks at 10 GB); the seed loader checkpoints its own WAL (`BICDB_AUTO_COMPACT_WAL_MB`, loader peak 14 GB) and the build waits for it to drain before the offline compact. Streamed WAL replay is still open (product bug: recovering a large unmaterialized log needs several times its size in RAM) |
| trial server 13 GB -> 37 GB at shutdown after a 2-min VU32 run (3.8 GB WAL); the harness's graceful-reopen check then replays that WAL and was killed at a 40 GB cap | shutdown checkpoint and reopen both materialize the unmaterialized log (same class as the compact finding); MVCC versions also grow 13M -> 17M during the run | open; development-host trials run with `GRACEFUL_REOPEN_CHECK=0` and a cgroup cap. Streamed WAL replay / checkpoint would close all three |

Ladder on benchmark-primary (16 wh, 32 VU, tuned-durable, district-sum NOPM): 30k (main as
found) → 120k (#718) → 133k (#719) → 140k (#720) → 151k (+Tier 1, #722) →
166k (+#724) → 162k/166k (+#725/#726, one-rep noise band). The July 2 high-water binary measures 330k on the same harness
today (frame-pointer build; 418k release in July), so roughly 2× per-transaction
CPU is still unexplained; the profile diff against it puts the remainder in
stored-function calls (~6× July), the UPDATE…FROM UNNEST path (~3×) and record
parsing (~2.4×, the typed envelope grew the 16-warehouse seed from 3.3 GB to
5.6 GB).

Single-session NEWORD: 6.2 ms → 2.7 ms (#718) → 2.3 ms (#719) → 2.15 ms (#720)
→ 1.74 ms (#722 + #724). The method that found these: a
frame-pointer build (`RUSTFLAGS=-C force-frame-pointers=yes`, separate
target dir excluded from git status) plus `perf record -g` and
`perf report --children -g none`.

## Findings from the code read (not previously in the ledger)

Each was verified at the cited line on main at the time of writing.

1. **Read-your-writes overlay is a linear scan.** Every point read walks the
   whole transaction write log with two string compares per entry
   (`crates/bicdb-core/src/db.rs:12992`) and allocates two Strings into a
   mutex-guarded observed-versions map (`db.rs:12986`). A late-loop NEWORD has
   ~25 pending writes and ~40 reads.
2. **The RowId fast path disables itself mid-transaction.**
   `rowid_fast_path_eligible` (`crates/bicdb-sql/src/engine/predicates_locators.rs:1050`)
   calls `pending_record_ids_for_collection` (`db.rs:13806`), which clones every
   pending id into a BTreeSet and a Vec just to test `is_empty()`. After the
   first stock write, items 2..15 of NEWORD take the String path.
3. **Each embedded statement builds a new SqlEngine.** `Session::sql_engine`
   (`crates/bicdb-sql/src/session/txn_update.rs:44`) deep-clones the GUC
   `HashMap<String,String>`, the security context, routine vars Arc, runtime
   and cancellation handles. ~15-20 times per NEWORD.
4. **The schema cache deep-clones on every hit.** `sql_schema_cache_get`
   (`crates/bicdb-sql/src/lib.rs:918`) allocates a `to_string()` key and returns
   `.cloned()` of the whole `TableSchema`, several times per statement.
5. **Per-statement setup allocations.** Nine ORM-compat rewrite probes run
   before planning (`engine/materialize.rs:686-712`); the wildcard field list
   allocates two Strings per column per statement; the full row is
   materialized into 72-byte `SqlValue`s before projection
   (`predicates_locators.rs:4726-4754`); composite PK probe keys go through a
   temporary BTreeMap and `serde_json::to_string` (`records.rs:846`, `:892`).
6. **UPDATE deep-clones each row twice.** A before-image is cloned for every
   updated row even when no FK parent / trigger consumes it
   (`txn_update.rs:572`), RETURNING clones again (`:703`, `:728`), then the
   Value tree is re-serialized to JSON and the WAL gets the full row image.
7. **NUMERIC is `SqlValue::String`.** Every money/tax operation parses text to
   BigInt and formats back (`crates/bicdb-sql/src/eval/operators_compare.rs:1212-1344`);
   `compare_values` can parse each side up to three times.
8. **Row-lock rule aborts the older transaction on contact.**
   `commit_snapshot.rs:1187` (`if tx_id.0 < owner.0 { break; }`) ignores the
   attempt budget; the transaction with the most sunk work dies. Blocking was
   refuted at ~3 ms hold times; this specific rule was never A/B'd alone and
   per-txn cost has since halved.
9. **Three shared-counter locks per point read.** Outer `RwLock<BicDb>`
   read_recursive, per-collection `RwLock<CollectionState>`, shard RwLock.
   Readers do atomic RMWs on the same words; benchmark-primary's 7900X has two CCDs so those
   bounce across the IO die. `lock_shared_slow` 7-10% was measured, never
   fixed. Commit also takes a read lock on every index in the DB to filter by
   collection (`commit_snapshot.rs:780`, `:912`).
10. **Bench env reminder.** Default index store is the single-lock
    `BTreeIndexStore`; the benchmark must keep
    `BICDB_INDEX_STORE=sharded BICDB_INDEX_SHARDS=64 BICDB_ORCH_SHARDS=16`
    (seed is bound to shards=64).

## Todo

Also found and fixed on the way (2026-09-01): a bare `ANALYZE` from a
non-superuser was refused outright, which broke HammerDB's schema build on
current main; it now analyzes the caller's own tables like PostgreSQL.

Gate for every item: same-session interleaved A/B on benchmark-primary, 2 reps minimum,
VU32 MAW24 checkpointer ON, clean slate before every run, ledger row added.
Ship only if >= +2% or strictly less work with no regression.

### Tier 0 - zero code, one evening on benchmark-primary

- [ ] **T0.1 PGO build.** `cargo pgo build` -> 3-min HammerDB profile run ->
      `cargo pgo optimize build`. Then `cargo pgo bolt` on top if the PGO win is
      real. Expect +5-15% on this flat, branchy profile.
      → IN PROGRESS 2026-09-02: instrumented profile collected on benchmark-primary; optimized build + A/B (c9) running.
- [ ] **T0.2 Release profile ladder.** `lto = "fat"` + `codegen-units = 1`,
      then `panic = "abort"` (check nothing in the server relies on
      catch_unwind first). One arm each.
      → NOT RUN yet (queued after PGO).
- [x] **T0.3 Huge pages for the heap.** `MIMALLOC_ALLOW_LARGE_OS_PAGES=1`
      (and a `MIMALLOC_RESERVE_HUGE_OS_PAGES=N` arm), with
      `/sys/kernel/mm/transparent_hugepage/enabled=always` on benchmark-primary. PG gets
      huge pages by default (`huge_pages=try`); bicdb's multi-GB scattered heap
      sits on 4 KB pages.
      → MEASURED NEGATIVE/NEUTRAL 2026-09-02 (c7): +0.7% NOPM (noise) with +18% peak RSS. Not a keeper.
- [x] **T0.4 Promote `MIMALLOC_PURGE_DELAY=-1`** into the canonical launcher
      env after one interleaved confirming pair (already +3.4% over 4 reps).

      → Bundled into the c7 arm with large pages: neutral. Not promoted.
### Diagnostics - turn the flat profile into a ranked list

- [x] **D.1 Mallocs per NEWORD.** Run a single-connection NEWORD loop under
      heaptrack (or `MIMALLOC_SHOW_STATS=1` delta over N calls). Record
      allocations/txn and the top 15 allocation sites. PG does dozens per
      transaction; this number is the new KPI for Tier 1.
      → SUPERSEDED by the frame-pointer call-graph profiles (prof-fp*, callers.sh) which named the culprits directly; the harness's ALLOCATION_TRACE remains available.
- [x] **D.2 `perf stat`** on the fp binary during steady state: cycles,
      instructions, IPC, LLC-load-misses, dTLB-load-misses. Confirms or
      refutes the memory-stall hypothesis; re-run after T0.3.
      → Harness collects cycles/instructions/cache-misses per trial (result.json .cpu.perf); IPC ~1.5 on the regressed base.
- [x] **D.3 `perf c2c record`** during steady state. Lists the cache lines
      that actually bounce (expected: DB RwLock, hot CollectionState RwLocks,
      commit_seq, applied_watermark, MAW counter, stats counters). Drives T2.1.

      → NOT RUN; the regressions found were algorithmic, not cache-line contention. Still worth doing once per-transaction CPU is back near July.
### Tier 1 - contained code changes, days each

- [x] **T1.1 Overlay index.** Add `FxHashMap<RecordLockKey, usize>` (last
      write index) and a per-collection pending counter to `Transaction`.
      `get_visible_unchecked` does one hash probe; `rowid_fast_path_eligible`
      reads the counter. Replace the observed-versions
      `Mutex<FxHashMap<(String,String),u64>>` keying with the same key type and
      drop the per-read String allocations.
      → DONE 2026-09-01 (85e9925e): write_index + has_pending_writes; core 887/0, sql 1116/0.
- [x] **T1.2 Fast path merges pending candidates instead of switching off.**
      With T1.1 in place, let the RowId path run under pending writes by
      overlaying the (now O(1)) pending lookup per rowid. Also lets
      `BICDB_TYPED_ROWS` apply inside write transactions; re-A/B typed rows
      afterwards (prior -1.8%/-4% strikes were measured with the path mostly
      disabled).
      → REVISED: the overlay index (T1.1) made eligibility cheap; the env arm `BICDB_TYPED_ROWS=1` measured −7.9% (c11), so the typed-row read path itself is not a win as built. Not pursued further.
- [x] **T1.3 Per-statement engine setup.** `Arc<HashMap>` for session GUCs
      (copy-on-write on SET), `Arc<TableSchema>` in the schema cache with a
      borrowed key lookup, skip the ORM-compat probes when executing inside a
      routine, project before materializing when the select list is narrower
      than the row.
      → DONE 2026-09-01: Arc GUC map + catalog memos (triggers, event bindings, inbound FKs). Schema-cache Arc and index_definitions memo still open.
- [x] **T1.4 UPDATE clones.** Take the before-image only when an FK parent
      update or a row trigger will consume it; move the updated record into the
      write and build RETURNING from a borrowed view; stop cloning
      `updated_records` for RETURNING-without-FROM.
      → DONE 2026-09-01: update_needs_before_image gates the before/pair clones; RETURNING projected before the move.
- [x] **T1.5 Fixed-point decimal.** Add `SqlValue::Decimal { mantissa: i128,
      scale: u8 }` for NUMERIC values that fit (all TPC-C NUMERIC(12,2) do);
      text stays canonical on disk and on the wire; BigInt remains the
      fallback. Parse once at row materialization, not per operator.
      → DONE 2026-09-01 as an (i128, scale) fast path inside SqlValue::String semantics rather than a new variant; differential tests vs BigInt.
- [x] **T1.6 Lock rule A/B.** Flip `commit_snapshot.rs:1187` to wait-die
      (younger dies, older waits up to a bounded budget), measure alone with
      the serfail count and FAILED=0 as gates. Deadlock-freedom argument must
      be re-stated in the comment.
      → SWITCH SHIPPED 2026-09-01; A/B 2026-09-02 (c8): wait-die −0.9% NOPM (noise) with 16% fewer serialization failures. Default stays old-dies; wait-die is a reasonable operator choice when client-visible aborts matter more than peak NOPM.
- [ ] **T1.7 Index-guard filter.** (DEFERRED: ~30 uncontended lock reads per commit, low value) Maintain a collection -> index-name reverse
      map so validate/apply lock only the indexes of the written collections.

### Tier 1b - from the 2026-09-02 frame-pointer profile (development-host, buffered lane, 200k NOPM)

Measured shares of server CPU on the current binary; July does 1.70x this
throughput on the same CPU with the same IPC, so the target is instruction
count, not memory stalls (perf stat: 1.73x instructions/txn, IPC 1.33 vs 1.26).

- [x] **T1b.1 Raw-probe gate.** 42 raw-text handlers + builtin matcher ran on
      every CALL before parsing (~2%). Bypassed for CALL/INSERT/UPDATE/DELETE
      (1455c177). `parse_statements` itself is still 3.8% per CALL.
- [x] **T1b.2 AST formatting on the hot path.** `core::fmt::write` 4.0%;
      largest source `FieldRef::from_expr` rendering an error five callers
      discard. `from_expr_opt` + structural name compares (1455c177): 4.0 -> 2.8%.
      Remaining: `parse_statements` 2.8k samples of formatting (the
      `rewrite_postgres_parse_compat` chain), `PgTimestamp::to_iso_text`,
      `json_function_call_pg_type`, `RoutineFrame::new_with_symbols` (`format!("${}")`
      per arg per call).
- [ ] **T1b.3 Per-row type inference outside the memo.** `projected_expr_pg_type`
      7.9% inclusive, called per evaluated row from `compare_expr_values`,
      `eval_binary_expr_value`, `cast_expr_value(_with_db)`,
      `eval_unary_bit_not_expr_value` (7 sites in eval/ranges_network.rs and
      eval/value_ops2.rs) — the #725 memo covers row_eval only. Either route these
      through the bound-row memo or make inference allocation-free
      (`Option<Cow<'static, str>>`: 90 static `to_string()` returns; sized at 169
      compile sites, 1-2 h mechanical).
- [ ] **T1b.4 Nested stored-function call overhead.** `eval_stored_function_value`
      8.8%, of which the body is 82%; frame setup (`RoutineFrame::new_with_symbols`
      normalizes every symbol name and formats `$n` per arg), `ensure_routine_execute_p*`
      and `cast_routine_return_value` are ~13% -> cache a frame template
      (slot ids/names) per routine IR.
- [ ] **T1b.5 Record decode.** `StoredRecord::to_record` -> `metadata_value` ->
      serde_json 10.4%: every fetched row is parsed to a `Value` tree. The typed
      envelope shrink (#730) trims it; T2.3 removes it.
- [ ] **T1b.6 Joins inside procedure SELECTs.** `apply_row_join` 16%,
      `apply_prepared_primary*` 8%, `indexmap insert_full` 4.5% (per-row map
      building), `slot_row_from_record_fields` 5.2%, `FieldRef::value` 4.4%.
      Needs a caller breakdown before choosing a cut.
- [ ] **T1b.7 Commit** 6% (`commit_transaction*`), `collection_state` /
      `collection_generation` lock reads ~0.7% — T2.1 territory.

### Tier 2 - structural, weeks

- [ ] **T2.1 Reader-friendly locks.** Striped or RCU-style reads for the outer
      DB lock and `CollectionState` (readers touch a per-thread cache line;
      DDL/checkpoint sweep all stripes). Build only after D.3 confirms the
      bounce.
- [ ] **T2.2 Logical delta WAL for updates** (pk + changed columns, previous
      image reconstructed at replay). Cuts WAL 7 KB -> ~1.5 KB per txn and the
      checkpointer's memory-bandwidth tax.
- [ ] **T2.3 Binary row codec per collection** for fully typed schemas
      (offset table + fixed-width cells; JSON stays the interchange form).
      Removes to_record parse, Value round trip and re-serialization.

## Expected outcome

Tier 0 alone is plausibly +8-15%. Tier 1 items target the allocation and
contention classes that make up the flat ~40% of the profile; 20-30% less CPU
per transaction is a realistic combined estimate, which reaches 500k without
Tier 2. Every item still needs the benchmark-primary A/B: this box has punished plausible
ideas before (Arc<str> ids -18%, fused scan-and-fetch -24%).
