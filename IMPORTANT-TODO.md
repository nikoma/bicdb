# IMPORTANT — open findings from the 2026-07-25 performance & bug audit

This file exists so these findings cannot get lost between sessions. Every
item was verified against the code, not speculated. Update statuses here in
the same commit that changes them; delete items only when a merged PR makes
them false.

Measurement baseline throughout: the 2,000,000-row `server_paged` database
(256 MB buffer pool, `BICDB_SYNC_OUTBOX=off`), probes in
`crates/bicdb-sql/examples/paged_query_probe.rs` and
`crates/bicdb-core/examples/paged_*_probe.rs`.

---

## Bugs

### B0 — WAL acknowledgement race can lose an acknowledged commit
**Status: FIXED 2026-08-19, mutation-verified. Deterministic test:
`crates/bicdb-core/tests/wal_acknowledgement_race.rs`.**
Fix: (1) the reservation-to-enqueue window is now gap-free — if the fallible
WAL encoding fails after the sequence is public, the queue slot is occupied
with empty bytes so the contiguous prefix can still advance; (2)
`TxLog::write_durable(seq)` no longer returns `Ok(())` on an empty contiguous
drain. It verifies `written_seq >= seq` and waits for the missing committer to
enqueue, which (1) guarantees will happen. Failpoint:
`BICDB_TEST_SEQ_RESERVED_PAUSE_MS`. Reverting either half fails the test.

Original analysis, retained because it explains the shape:
Surfaced here from `docs/performance-campaign-handoff-2026-07-09.md`, which was
its only record — a benchmark handoff note is the wrong home for the most
serious open correctness item in the tree.

`commit_transaction` reserves and publishes a commit sequence, then performs
fallible WAL encoding, then enqueues the bytes; `write_durable(seq)` is called
after the database guard is released. Two committers can interleave so that N
reserves and pauses before enqueue while N+1 reserves, enqueues, and calls
`write_durable(N+1)`. `drain_contiguous` can only drain a contiguous prefix, so
N is missing — and `TxLog::write_durable` (`crates/bicdb-core/src/db.rs`,
`if last == state.written_seq { return Ok(()) }`) returns success without
verifying `written_seq >= seq`. N+1 is acknowledged before its WAL is durable;
a crash in that window loses an acknowledged commit. A fallible encoding error
after reservation can also leave a permanent sequence gap while later durability
calls still return success.

Existing comments claiming the queue is assigned and enqueued gap-free are not
true under this interleaving. Existing concurrent recovery tests wait for all
committers and so miss the acknowledgement window entirely.

Fix order (from the original analysis): add a deterministic hook between
sequence reservation and enqueue, prove N+1 can return while
`stats.written_seq < N+1`, then introduce a sequencing critical section that
finishes fallible encoding and enqueues before publishing the sequence — and
make `write_durable` verify it actually reached `seq` rather than returning
`Ok(())` on an empty drain.

NOT attempted as part of the source-release audit: this is a change to the
commit and durability path, and it needs the deterministic test first.

### B1 — Failed commit apply wedged the DB and resurrected on restart
**Status: FIXED (pending merge), mutation-verified both ways.**
`commit_transaction` enqueues WAL frames *before* applying; a concurrent
committer's durable write can flush them. A failed apply then (a) stalled the
contiguous `applied_watermark` forever — every later commit invisible to new
snapshots — and (b) recovery replayed the durable Commit frames of a
transaction whose client saw an error. Fix: `revoke_enqueued_commit` — an
`Abort` frame at a fresh seq (recovery's fold lets later Abort override
earlier Commit), both seqs marked in the watermark, revoke flushed durably
before the error returns. Failpoint: `BICDB_TEST_FAIL_COMMIT_APPLY`. Test:
`crates/bicdb-core/tests/commit_apply_failure.rs` — each half fails
independently under mutation (the wedge assertion needs a fresh-thread
transactional read; the per-thread commit floor masks it on the same thread).
Accepted cost, documented at the fix: a mid-batch failure after partial apply
leaves the applied collections visible (nothing can un-apply resident state);
restart converges to "transaction absent".

### B2 — Streaming SQL paths bypass RLS when no security context is set
**Status: FIXED (2026-07-25 late), mutation-verified.**
The streaming paths now decline any table with `rls_enabled || rls_forced`.
Diagnosis refined during testing: with plain ENABLE, the no-context session
runs as the table owner and PostgreSQL's owner bypass exempts it — no
divergence. The real leak was FORCE ROW LEVEL SECURITY (and GUC-switched
non-owner users): the streamed shape returned all 1,500 rows where the
policy allows 150. Test `streaming_rls_guard.rs` needed FORCE to bite, and
asserts the oracle actually filtered so it can never pass vacuously.

### B3 — Every query against a non-indexed lazy paged table plans in O(n)
**Status: FIXED (2026-07-25 late).**
`estimated_record_count` now seeds once from a key-only walk and is nudged by
commits: +1 for a first-touch upsert whose pre-image was absent (the baseline
seed already knew), −1 for deletes of live rows. Measured on 2M rows: first
query pays a 3.3 s seed, every later simple query is ~0 s (was 0.9–2.2 s
each). Found en route: the `registered > 0` shortcut fired for plain-lazy
collections whose registries hold only touched rows — after 15 deletes the
planner thought a 500-row table had 15 rows; now gated on `track_reverse`.
Drift sources (accepted, planning-only): dead keys at seed, replication
imports don't nudge.

### B4 — Scans descended the B-tree twice per row
**Status: FIXED (pending merge).**
`BTreeRange` already yields `(key, locator)`; `PagedScan` discarded the
locator and re-looked the key up per row. Now `walk_chain_as_of` walks from
the yielded locator under the same structure-read acquisition
(`crates/bicdb-page/src/paged.rs`). Measured: streamed `WHERE … LIMIT 3` on
2M rows **2.2 s → 0.9 s**.

### B5 — Short reads/writes unverified in the page store
**Status: OPEN (robustness).** `write_page_raw` never checks
`written == len`; a legal short write is detected later by the torn-page
trailer and mislabeled as corruption instead of retried. Same for short reads
→ `ChecksumMismatch`. Loop-until-complete in `crates/bicdb-page/src/pio.rs`
callers.

### B6 — Hot-key version-chain bloat, two sources
**Status: OPEN.** (a) `put_index_entry` writes a new version even when
`old_key == new_key` — skip when the entry already exists; (b) **vacuum never
runs automatically**, so hot rows (TPC-C district) grow unbounded chains that
every historical read walks. Piggyback a page-budgeted vacuum on checkpoint.

### B7 — Time-series/spatial/graph paths iterate `shard.records` directly
**Status: OPEN (correctness risk).** Silently partial on lazy paged
collections; the SQL suite does not exercise them cross-reopen. Either route
through the paged scan or force those collections out of lazy mode at open.

### B8 — Two unbounded logs in long server sessions
**Status: OPEN (operational).** `transactions.log` is truncated only at
open/close (a multi-day server session grows it without bound — piggyback
truncation on the existing checkpoint machinery); the CDC outbox (`sync.log`)
is fully resident by design and on by default — imports must set
`BICDB_SYNC_OUTBOX=off`.

### Fixed earlier the same day (context)
- LIMIT/OFFSET silently ignored on bare aggregates at BOTH return points
  (PR #205).
- Planner saw 0 rows for every paged table (`collection_record_count` counted
  resident rows) (PR #205).
- 13-way wrong enumeration guarding the streaming path → best-effort fallback
  (PR #205). Lesson: don't enumerate what a fast path can't do; let any error
  fall through to the general path.

---

### B9 — vector_dim never persisted by the transactional apply path
**Status: FIXED (with speedup #3), found by its removal of accidental cover.**
`apply_committed_record_writes`' dim-transition branch set `meta.vector_dim`
in memory but never persisted the catalog (the non-transactional insert path
does). A paged vector collection created via `batch_insert`/transactions then
reopened with `vector_dim: null`, took the LAZY branch (zero resident rows),
and `load_hnsw_indexes` tombstoned every persisted HNSW node against the
empty resident set — ANN search silently returned nothing. Full Write-frame
replay used to rematerialize the records at open and mask this whenever the
session ended without a clean close. Fix: persist the catalog after commit
apply when a None→Some dimension transition happened (once per collection
lifetime). Test: `paged_concurrency.rs::hnsw_search_survives_reopen_without_close_in_paged_mode`.

## Top 10 speed-ups (impact-ranked)

| # | Idea | Status / expected win |
|---|---|---|
| 1 | Locator passthrough in scans (B4) | **DONE** — streamed scan 2.2→0.9 s |
| 2 | O(1) planner row estimate (B3) | **DONE** — simple queries 0.9–2.2 s → ~0 s after one-time seed |
| 3 | Logical logging in paged mode | **DONE** — core log per commit is now a ~100-byte `CommitMaterialized` marker (with `commit_seq` for recovery continuity) instead of full Begin+Write…+Commit frames; 500k-row ingest writes 0 B of record data to `transactions.log` (was ~98 MiB, ~205 B/row — ~20% of total write volume) and skips per-write pgwire pre-serialization. Enqueue-before-apply position kept (durability race — see `write_durable` gap note); failed applies still revoked via B1's Abort override. **Exception: `replication.enabled` keeps full record logging** — standbys are fed Write frames from this log (`replication_commits_from_retained_wal`). Tests: `paged_concurrency.rs` (`the_core_log_carries_markers…`, `a_replicating_paged_primary…`, both fsync-on so sizes have teeth). Pre-existing hole noted: materialized commits were ALWAYS invisible to `export_replication_frames_since`, so paged+replication had a silent-lag hazard before this change too. |
| 4 | Skip no-op index-entry puts (B6a) | halves index write traffic on update-heavy loads |
| 5 | Auto-vacuum on checkpoint (B6b) | bounds chain walks on hot keys |
| 6 | Header-only `count_live` (visibility needs headers, not values) | exact COUNT 1.4 s → few hundred ms |
| 7 | One paged txn per core commit (not per collection) + shared fsync | fewer WAL syncs under fsync |
| 8 | Binary row codec for the page store (segments keep JSON) | **DONE (0.9.46-beta)** — version-tagged binary envelope (tag 0x01; legacy JSON rows start with `{` and decode through the old path forever, no migration): raw-f32 vectors, length-prefixed fields, metadata embedded as JSON bytes that identity decodes SKIP by prefix instead of lexing. 500k-row probe: ingest 91.5k/s → **111.5k/s (+22%)**, paged dir 406 → 367 MiB (−10%); a real pre-codec DB opens and point-reads identically (mixed formats coexist). Codec unit tests: round-trip all field combos, legacy decode, truncation errors. |
| 9 | B-link / latch-coupled index | lifts the global writer mutex + structure RwLock (write scaling) |
| 10 | `Arc<str>` pk sharing in the registry + generation-cached `stats()` | registry 134 → ~90 B/row |

### B10 — spatial k-nearest is unreliable for k < total; `<->` ORDER BY keys silently Null
**Status: RESOLVED (2026-08-09, 1.0.110-beta).** (a) was fixed by the bounded
best-first k-NN rewrite of the nearest index paths (#437, 1.0.107-beta):
`spatial_nearest_envelope_index` now returns true nearest neighbors for any
k, oracle-tested against brute force. (b) untyped `<->` eval now sniff-parses
geometric operands (`eval_untyped_geometric_distance`) before falling through
to the vector metric, so `ORDER BY spot <-> point(…)` sorts by real
distances. The LIMIT pushdown is REINSTATED for static LIMIT+OFFSET on
point-typed indexes when no WHERE/RLS/pending-tx/DISTINCT/GROUP BY/HAVING/
window function can invalidate the bound; everything else stays exhaustive
(box/polygon k-NN orders by MBR distance, a lower bound of the shape
distance `<->` sorts by, so those always stay exhaustive). Pinned in
`geometric_indexes.rs::knn_limit_pushdown_is_bounded_correct_and_declines_when_unsound`.
Original finding (2026-07-26):
Two independent problems, discovered by a probe test that pushed the query's
real LIMIT into `GeometricKnnIndexScan`: (a) `spatial_nearest_envelope_index`
asked for k=4 around (0,0) on 20 diagonal points returned a point at distance
~11 while dropping the two nearest — the envelope heap is not returning true
nearest neighbors for small k; (b) on the row path, `ORDER BY spot <->
point(0,0)` evaluates to an error per row which the ORDER BY machinery
silently degrades to Null (all-equal keys), so the "order" is just stable-sort
insertion order. Both were invisible because the KNN plan always fetches
`limit: total_rows` and the stable sort preserves scan order. The pushdown is
REVERTED (comment at the plan site) until both are fixed; the top-K sort uses
an original-index tiebreak so all-Null keys keep today's deterministic
behavior instead of becoming nondeterministic.

## Search-core (drop-tantivy gap) — PR sequence in flight

- **1. Probe-driven AND intersection (0.9.50-beta, DONE)**: conjunctive
  ranked queries pick the rarest positive operand (key-only capped count —
  no heap reads, dead versions tolerated as a heuristic), fetch only its
  postings, and resolve every other term per driver document via
  `get_index_entry` point probes (one descent each; payload returned for
  scoring). Single-term queries skip it. engine_bench AND pair 552 -> 78.5 ms
  (probes ~3.9 us each; the 20k-doc driver dominates). Prefix operands and
  OR trees decline to full fetch.
- **2. Impact-ordered postings + early termination (0.9.51-beta, DONE)**:
  v2 namespace [0,0,2], key = term + inverted u16 impact bucket + pk, value =
  the posting payload (per-posting point probes back into v1 are cold random
  reads at scale — the ordering scan must carry what ranking needs). Bucket =
  EXACT rank_or single-operand term (same accumulation order and constant),
  sqrt-spread over u16 (u8 collided adjacent tf classes into one bucket and
  the top bucket outgrew the bail cap). Termination: kth >= ((bucket+1)/65535)^2
  once k are held — sound because floor quantization makes every remaining
  score strictly below its bucket's edge, so even ties lose. Completeness
  sentinel written by full backfill gates the path (incremental v2 twins on a
  pre-upgrade index must not enable it); tie-plateau bail (first bucket only,
  cap max(4k, 8192)) falls back to the bulk path for uniform-tf terms.
  Measured: Zipf-tf broad term (50% of 200k docs, tf 1..16) **369 ms ->
  4.1 ms**; scan stops at exactly the top class boundary (6,251/100k).
  HONEST COSTS: v2 payload twins double the index dir (2.2 -> 4.4 GiB at
  200k), and uniform-tf terms (no discrimination possible) pay bail + pool
  pressure: medium 28 -> 73 ms, uniform-broad 518 -> 632 ms. Natural-language
  tf is Zipfian, so the win is the realistic case; the size tax is PR-3's
  target (posting compression / drop v1 twins for v2-complete indexes).
- **3. Size-tax experiment (0.9.52-beta): NEGATIVE RESULT, documented.**
  Emptying v1 payload values saved ~1% (4,401 -> 4,364 MiB at 200k): the
  doubling is B-tree ENTRY overhead (key + version header + slot) of the
  second keyspace, not payload duplication. Scoring AND candidates from row
  projections instead of probe payloads regressed AND 94 -> 529 ms and
  phrase 24 -> 264 ms (row heap pages are scattered; term-clustered probe
  pages are not). Both reverted — 0.9.51's twin-with-payload layout stands.
  The real size fix is BLOCK postings (one entry per term-block, delta/varint
  doc lists — a per-posting-entry layout cannot get there), which is also the
  prerequisite for true block-max WAND. That is the next deliberate project
  if broad-uniform parity with tantivy is required; current verdict
  unchanged: bicdb serves selective/phrase/AND/Zipf-broad ranked search well
  (0.9-80 ms at 200k), tantivy keeps uniform-broad and index size/build.
- WAND-lite for OR (per-term max-bucket pruning) remains open (task #22) —
  moderate value while OR shapes are rare in the builder's output.

## Block postings (0.9.53-beta) — EXPERIMENTAL fold, correctness complete

`compact_full_text_index(name)` folds a GIN index's per-posting tail into
front-coded varint posting blocks ([0,0,3] namespace, tombstones in [0,0,4];
tail > tombstone > block precedence, atomic per-term swap, block-aware
verify, fully-folded indexes open read-through). All reads merge correctly
(oracle + stale-posting-after-update mutation-verified); crash reopen safe.
**Do not compact production indexes yet**: (a) pk-ordered blocks make
block-max termination degenerate when impact classes interleave — every
block's max ties, ranked broad terms fall back to bulk (~670 ms vs 3.7 ms
unfolded impact path); (b) the file does not shrink in place (dead pages go
to the free list, like PostgreSQL) — live posting bytes drop ~400 MB -> ~27 MB
at 200k docs, file-level reclamation needs page-file truncation; (c) eager
vacuum inside the fold is unsafe (reclaims slots visible to snapshots).
**PR-B landed (0.9.54-beta)**: the fold now writes a SECOND, impact-ordered
copy of each term's blocks ([0,0,5], sequence-keyed so ascending scan =
descending max impact; block bytes are compressed so the duplicate costs
~block size, trivial next to what folding saves). The ranked block scan
streams that copy and terminates at the top impact classes: skewed broad
term on a folded 200k index scans 6,266 of 100k postings. Measured folded
ranked latency: ~290 ms — of which ~285 ms is walking the fold's MVCC-DEAD
v1 entries in the "empty" tail scan (visibility checks per corpse); after
dead-version reclamation this is ms-class. **B11a FIXED
(0.9.55)**: `mark_deleted` (the first write after a vacuum) and
`walk_chain_as_of` now treat StaleLocator/DeadSlot/NoSuchSlot on a chain
link as end-of-chain per vacuum's contract — reproduced and pinned by
`bicdb-page/tests/vacuum_dangling_links.rs` (insert/delete/update histories,
vacuum, then reads AND writes over the reclaimed chains). **B11b FIXED (0.9.56)**: page
compaction no longer trims trailing dead slots — dropping them discarded
generation history, letting a stale locator from a slot's first occupancy
validate against a new tenant (silent wrong-row read; produced
stale-positive postings under fold+vacuum churn). Dead slots persist and
insert's monotonic-generation reuse is the defense; regression test pins the
alias case. Fold now checkpoints+vacuums safely (churn shadow green);
folded ranked scan 290 -> 185 ms. **Index-entry GC landed (0.9.57)**:
`sweep_dead_index_entries` removes keys whose HEAD locator no longer
resolves (head = newest version; vacuum only reclaims everyone-dead
versions, so a gone head proves the whole chain is unreadable forever) —
two-phase collect-then-remove under the writer+structure locks; the fold
runs checkpoint -> vacuum -> sweep. Folded ranked broad term: 185 -> 31 ms
(remaining ~5 us/posting is block decode + rank; unfolded impact path is
still 3.7 ms — the fold trades that for ~15x smaller postings).
**Direct-to-blocks build landed (0.9.58)**: CREATE INDEX over an existing
corpus now writes v3/v5 blocks straight from a chunked pk-ordered scan
(per-term carry between chunks, append-only because chunk pk ranges are
disjoint) — no per-posting tail is ever written, so there is nothing to
fold or GC afterwards. Bulk flow is rows-first-then-CREATE-INDEX.
Measured @200k docs: build 408 s -> 101 s, dir 4.4 GB -> 890 MiB
(tantivy: 19 s / 113 MiB — gap now ~5x/~8x, from ~21x/~39x); ranked
AND 224 -> 34 ms and phrase 122 -> 8.7 ms once the bulk sorted probe
landed; selective 0.97 ms, skewed broad 2.6 ms; uniform broad 179 ms
stays the WAND item. Along the
way, three latent bugs fixed: prefix (`term:*`) reads ignored posting
blocks (folded terms were invisible to prefix queries); DROP INDEX leaked
v2/v3/v4/v5 + sentinel (poisoning same-name re-creates); and a crashed
chunked backfill could later be served read-through (now vetoed at open —
blocks without the sentinel force the resident fallback). Probe-driven
AND intersection got a bulk sorted-probe API (each block decoded once,
not once per driver pk). Legacy array projections fall back to the
per-posting build. **Page reclamation landed (0.9.59, GC 3/3)**: vacuum
now FREES fully-dead heap pages to the free list (safe against stale
locators via a per-page generation FLOOR kept in the durable catalog —
the B11b defense extended to page reuse; alias case mutation-pinned),
checkpoints with an empty WAL truncate the trailing free run off the
file, and the direct build runs checkpoint->vacuum->sweep first so its
blocks are absorbed by the holes the projection rewrite left. @200k:
dir 890 -> 778 MiB, build/queries flat. The remaining bulk is row
metadata carrying the JSON lex projections (body + lexeme map stored
per row) — compacting that representation is the next size lever, not
a GC item. Fold + direct build remain OPT-IN/experimental for prod;
nothing regresses by default. **Ranked-read catch-up (0.9.60-0.9.61)**:
zero-alloc block scans + closed-form ts_rank (single-term AND
conjunctive rank_and, both property-pinned bit-exact) + streaming
candidate arena for ranked AND/phrase (driver stream + lockstep block
probes, matches() only for phrase trees) + plateau bail removed from
the block branch (full scan at ~40 ns/posting beats bailing to bulk).
@200k vs tantivy: medium 1.5 vs 0.18 ms, uniform broad 6.5 vs 0.83,
AND 7.0 vs 1.0, phrase 3.6 vs 0.35, skewed 1.2 vs 0.5, selective 0.87
vs 0.13 — every shape within one order of magnitude (was 30-230x on
the worst). **Doc-terms overhaul (0.9.63)**: bulk CREATE INDEX writes one compact
blob per row into the index keyspace (ns [0,0,6]) instead of rewriting
every row with a JSON lexeme projection; the direct build scans blobs,
not rows; UPDATE/DELETE diffs resolve from the blob and retire it once
the row is rewritten. @200k: build 104 -> 64.5 s (tantivy 22 s — 2.9x),
store 778 -> 668 MiB, every query faster (leaner rows: selective
0.67 ms, medium 1.13, broad 5.0, AND 6.4, phrase 3.4, skewed 0.99).
CAVEAT: record-level sync/import of bulk-indexed rows carries no
projection — a synced replica must run CREATE INDEX itself (documented;
same silent-skip semantics core-API writers always had). **Fixed-cost cut (0.9.64)**: every execute() was COMPILING four
raw-dispatch regexes (52% of a selective query, perf-measured) and
re-parsing/re-rendering stored index expressions (~14%) — both memoized.
@200k vs tantivy: selective 0.17/0.11 ms, skewed broad 0.62/0.53,
medium 0.68/0.18, broad 4.8/0.89, AND 6.3/1.0, phrase 3.0/0.37;
build 62/19 s, store 668/113 MiB. Remaining in-process fixed cost is
sqlparser (~30% — prod pgwire prepared statements bypass it). **Block-max WAND (0.9.65)**: block format v2 stores the EXACT best
single-term rank per block (the u16 bucket cannot retire a tie plateau);
the ranked scan gates whole blocks from the header (Stop on bucket
bound, Skip on exact bound — ties provably lose by within-bucket pk
order). Uniform broad 4.8 -> 0.89 ms (tantivy 0.83 — PARITY; cost now
bounded by the top-k, not the posting count = scale-proof at 35M docs);
medium 0.25 ms; skewed 0.23 ms (FASTER than tantivy's 0.50). v1 blocks
decode fine (max_rank None -> bucket bound); the boundary regression
pin finds a real bucket-colliding tf pair at runtime. Remaining for GA:
fold-triggered-by-checkpoint, conjunctive WAND for ranked AND (6.2 ms)
+ phrase (2.9 ms) — the last shapes above 2x, SIZE AUTOPSY (page-type
histogram + fill factors; also the sync outbox JSON log that ships with
every store), ts_rank_cd + normalization!=0 + weighted/prefix operands
still take the docs-map path.

## Vectors (server_paged) — lazy open path (0.9.49-beta, Slice 3)

Vector collections no longer force stub materialization at open (the prod
cold-start: `passages` with 768-dim vectors). The registry gate admits them;
`load_hnsw_indexes` streams vectors from the page store's identity projection
for lazy collections (raw f32s under the binary codec) instead of resident
shards — validating against resident shards would tombstone every node, the
B9 shape; exact search streams pages in batches through the SAME
vector::search machinery with a top-k merge; ANN hit resolution falls back to
the lazy read path. Zero resident rows after reopen; ANN+exact parity vs the
embedded oracle with a deleted row excluded (paged_concurrency test). HNSW
node vectors themselves remain resident (graph + vectors = the ANN index
cost) — true read-through node vectors are the next wall, noted, not claimed.

## Full-text (server_paged) — postings now READ-THROUGH (0.9.47-beta)

- **Slice 1 of the drop-tantivy plan, DONE**: paged FTS indexes no longer
  bulk-load postings into the resident store at open. `IndexState.
  paged_read_through`: the store stays EMPTY; term lookups are bounded range
  scans over the durable entry keyspace (`scan_index_exact` /
  `scan_index_encoded_prefix` — one B-tree descent + leaf run through the
  buffer pool; both escape layers are byte-prefix-preserving so bounds are
  exact). Resident maintenance is skipped (durable entries are maintained by
  the commit's paged apply); verify materializes durable entries on demand;
  pre-upgrade DBs (no durable entries but rows) still rebuild resident.
  Measured, 1M docs x 20 terms (50k-term vocabulary, 20M postings): reopen
  1.31 s / 1,001 MiB — IDENTICAL RSS to a 4M-posting corpus, i.e. resident
  memory is now independent of index size (bulk-load would have been ~2.7+
  GB and walked all 20M entries at open). Point lookup 400 pks in 1.2 ms;
  degenerate hot terms (53k pks) 28 ms vs 11 ms resident — the page-walk
  tradeoff, cached by the pool. Remaining zeros to know about: planner
  `table_statistics.indexed_rows/distinct_keys` and residency `store_bytes`
  report 0 for read-through indexes (honest for residency; statistics are
  unused by FTS planning, which counts real candidates).
- **Slice 2 DONE (0.9.48-beta): positions in posting VALUES + rank-from-index.**
  Posting payload v1 `[tag][doc_len u32][distinct u32][count u16][packed u16*]`
  (packed = position | weight<<14); projection object
  `{"v":1,"len","distinct","lex":{term:[packed…]}}` written at materialization
  (legacy term arrays still read; their postings carry empty payloads and the
  ranked path declines to the text fallback). `WHERE tsv @@ q ORDER BY
  ts_rank[_cd](...) DESC LIMIT k` (try_ranked_fts_topk) fetches postings for
  every operand of both queries (NOT included, prefixes expanded over the
  term keyspace), rechecks `@@` against a SPARSE tsvector reconstructed from
  postings (positions+weights → phrases/weights/negations recheck exactly),
  ranks with the SAME ts_rank code via injected doc scalars
  (ts_rank_with_scalars / ts_rank_cd_with_scalars), top-k, THEN fetches only
  k rows. Parity oracle across 13 shapes (both rank fns, AND/OR/NOT/phrase/
  prefix, flags 1/2/8/16/32, float4[] weights, OFFSET); sign-flip mutation
  kills it. Term-diff now compares payloads (positions can change while the
  term set does not). **Measured, 500k rows, every row matching: 34.4 s
  (text-ranked) -> 0.87 s (rank-from-index), ~38x; selective terms are
  millisecond-class.** **Head-to-head measured** (comparison corpus
  tests/engine_bench.rs, 200k abstracts, top-10 ranked, warm): build 19 s /
  112 MiB (tantivy) vs 193 s / 2,289 MiB (bicdb); selective 0.1% term 0.11 ms
  vs 0.96 ms; medium 10% 0.18 ms vs 59 ms; broad 90% 0.85 ms vs 518 ms; AND
  1.0 ms vs 552 ms; phrase 0.34 ms vs 13 ms. Diagnosis: bicdb ranks EVERY
  candidate (no block-max WAND/impact-ordered pruning) and stores one
  MVCC B-tree entry per (term,doc) (no delta-compressed posting blocks) —
  closing that is a deliberate build-a-search-core project (compressed
  posting blocks + skip data + max-score pruning), not a slice. VERDICT for
  the 900GB corpus today: bicdb FTS is now viable for selective/filtered and
  phrase search with bounded memory; keep tantivy for broad-term ranked
  retrieval. Also remaining: score-in-projection (SELECT ts_rank declines),
  ranked shapes beyond single-@@ WHERE.

## Full-text (server_paged) — persist postings DONE, top-K sort DONE

- **Durable FTS postings (0.9.40-beta, DONE)**: `IndexKind::FullText` entries
  are now page-backed like B-tree entries — one durable entry per TERM,
  written in the same paged transaction as the row (`apply_paged_index_upsert`
  diffs old/new term sets, so unchanged terms cost zero WAL traffic — that's
  speedup #4's shape applied to FTS from day one). Loaded at open from the
  reserved keyspace; FTS-indexed collections are REGISTRY-MODE eligible (an
  FTS index no longer forces O(n) residency). The `NoDurableEntries` fallback
  now rebuilds from PAGE-STORE rows, not resident shards (which are empty in
  registry mode — the old fallback would have produced a silently empty index
  for any durable-kind index on upgrade). Verified by
  `tests/paged_fts_postings.rs` (incl. an embedded-vs-paged shadow churn test
  and mutation-verified loader/diff paths) and `bicdb-sql/tests/paged_fts_scan.rs`
  (FullTextIndexScan fires on the loaded index after reopen). 200k rows x 4
  terms: reopen 0.85 s / 423 MiB, point lookup ~11 ms.
- **Top-K sort (0.9.41-beta, DONE — and honestly marginal)**: `ORDER BY <expr>
  LIMIT k` on the row path now selects the k best in O(n) (original-index
  tiebreak keeps it bit-identical to the stable sort + truncate it replaces)
  and truncates BEFORE projection; the FTS candidate id set is computed once
  at plan time and carried on the plan (was walked twice: costing +
  execution). Measured on 500k rows, `WHERE @@ ... ORDER BY ts_rank(...) DESC
  LIMIT 10`: **~31 s before AND after** — the sort was never the bottleneck.
- **THE actual ts_rank bottleneck (next, measured)**: each candidate row pays
  `to_tsvector` re-tokenization TWICE — once for the WHERE `@@` recheck, once
  for the ORDER BY key — ~60 µs/row × 500k ≈ the whole 31 s. Levers, in
  order: (1) skip the `@@` recheck when the index candidate is EXACT (plain
  AND/OR of unweighted non-prefix terms mirrors `@@` semantics precisely; NOT
  / phrase / weights / prefix make it a superset — track exactness in
  `fts_index_candidate_node`); (2) cache the computed tsvector per row within
  a statement so WHERE and ORDER BY share it; (3) store positions+frequencies
  in the durable posting VALUES (empty today) so ts_rank reads the index
  instead of re-parsing text — that is the real "rank from the index" endgame.
- Remaining non-durable kinds: spatial, array, jsonb (still gate collections
  out of registry mode).

## Phase 5 remainder (execution memory)

Bounded now: plain projection+LIMIT (285 MiB), WHERE+LIMIT (286 MiB), bare
`COUNT(*)` (283 MiB), `ORDER BY <expr> LIMIT k` (top-K, 0.9.41-beta), and —
0.9.42-beta — **simple aggregates with WHERE**: COUNT(*)/COUNT(x)/SUM/MIN/
MAX/BOOL_AND/BOOL_OR over one plain table fold per batch while the scan
streams (`try_streaming_simple_aggregates`), reusing the SAME per-slice
aggregate primitives as the materializing path so the two cannot drift.
Measured, 500k-row paged DB, per query (identical results): streaming
**+13 MiB / ~2.5 s** vs materializing **+1,020 MiB / ~4.1 s**. AVG declines
(its final division has type rules a (sum,count) partial pair does not
reproduce); DISTINCT-inside-aggregate, FILTER, GROUP BY, HAVING all decline
to the correct fallback. Engagement requires an untouched lazy collection
(same rule as all streaming paths). Tests: `streaming_aggregates.rs` —
transaction-forced materializing oracle + closed-form constants, WHERE and
SUM folds mutation-verified.

**Full ORDER BY is bounded too (0.9.43-beta, Phase 5b)**: an external merge
sort (`external_sort.rs` — memcmp-normalized keys: escape-encoded components,
byte inversion for DESC, null markers per NULLS FIRST/LAST, sequence-number
stability tail; tagged spill codec because SqlValue's untagged serde cannot
round-trip) streams the scan into budgeted sorted runs
(`BICDB_SORT_SPILL_ROWS`, default 65,536) and k-way merges. Engaged from both
the general path and the row-evaluator branch (a `WHERE n % 2 = 1` selection
routes there and previously bypassed it). Declines on unprovable fidelity:
non-field order expressions, untyped keys, collations, user types. Measured,
500k rows: `ORDER BY n DESC` 816 MiB/1.1 s vs 1,834 MiB/2.0 s materializing;
`WHERE n % 2 = 1 ORDER BY n` 814 MiB/3.1 s vs 1,908 MiB/4.0 s. Oracle-equality
tests across 8 shapes under a 64-row budget (`external_sort_stream.rs`),
DESC-inversion mutation-verified; run files cleaned up.

**GROUP BY and DISTINCT are bounded too (0.9.45-beta, Phase 5c)**: both spill
(group key, values) through the external sorter and dedup/fold ADJACENT keys
during the merge (`finish_each` — memory is the spill budget plus one row per
GROUP, never per input row). Aggregate folds use the same value-level
primitives as the materializing path. Group keys must be plain typed fields
("default" collation is the byte order the typed keys already encode — a
non-default collation declines; so do HAVING, ORDER BY, AVG, expression
group keys). Measured, 500k rows: GROUP BY 814 MiB / 1.4 s vs 1,517 MiB /
3.1 s; DISTINCT 813 MiB / 1.4 s vs 1,449 MiB / **29.0 s** (the materializing
DISTINCT dedup is the slow part, not just the memory). Tests:
`streaming_grouping.rs` — order-insensitive tx-oracle across 11 shapes +
closed-form pins (24 groups incl. the single NULL group); dedup and
group-boundary folds mutation-verified. Gotcha fixed along the way: sqlparser
`Expr` equality includes source spans, so projected `dept` != GROUP BY `dept`
as ASTs — compare rendered SQL.

Still materializing (~4 GB on 2M rows): joins, window functions, AVG,
HAVING, expression group keys, non-default collations. Next big lever:
binary row codec for the page store (ingest CPU ceiling).
