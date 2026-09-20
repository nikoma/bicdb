# FTS Lean Storage Campaign

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

Tantivy indexes our Common Crawl corpus at **0.70 GiB in 6.5 s**. BicDB's
index is **~9.5 GB built in 136 s** — roughly **13x the size, 21x the time**,
for the same 123,187 documents with positions on both sides.

Before redesigning anything: where are those bytes?

## The decomposition

`fts_workload <db> storage`, on the real corpus:

| namespace | entries | keys MiB | values MiB | share |
|---|---|---|---|---|
| row data | 123,187 | 2.6 | 1035.3 | 19.6% |
| document terms | 123,187 | 7.8 | 816.1 | 15.6% |
| **term dictionary** | **5,406,839** | **408.2** | **1071.1** | **28.0%** |
| document ids | 246,374 | 15.3 | 2.3 | 0.3% |
| **postings** | **6,284,643** | **507.2** | 760.4 | **24.0%** |
| **impact metadata** | **6,056,356** | **467.8** | 174.2 | **12.2%** |
| document statistics | 123,187 | 7.3 | 8.5 | 0.3% |

```text
logical total        5.16 GiB   across 18,363,773 entries
of which KEYS        1.38 GiB   (26.8%)
on disk              ~9.5 GB
page overhead        ~4.3 GB    (~45% ON TOP of logical)
```

## Five findings, in order of size

### 1. 18.4 million keyed entries where a search engine has none

Every posting block, dictionary entry and impact record is a row in a paged
B-tree keyspace, carrying its own key. **Keys alone are 1.38 GiB — 26.8% of
the logical total.** A packed posting block pays zero bytes of key for the
same data.

This is the structural mistake. The index is being stored as a general-purpose
transactional keyspace when it wants to be packed immutable segments.

### 2. Page overhead is ~45% ON TOP of the logical bytes

5.16 GiB logical becomes ~9.5 GB on disk. Page headers, checksums, slot
directories, MVCC version headers and intra-page slack roughly **double** the
data. Tantivy writes append-only segment files with none of that.

### 3. The term dictionary costs 287 bytes per entry

5,406,839 entries for 1.48 GiB. A Tantivy FST stores a term in roughly
10–20 bytes. **This is ~20x off** and is the single largest namespace.

### 4. `document terms` is a second copy of the corpus

816 MiB for 123,187 documents is **6.6 KB per document**, against an 8.5 KB
mean document. This is the per-document term blob that makes incremental
retract possible — and it is nearly as large as the text it describes.

Worth asking whether it must be durable at all: the FTS index is **derived
state**, rebuildable from authoritative records.

### 5. Impact metadata has as many entries as postings

6,056,356 impact records against 6,284,643 posting blocks, 642 MiB total, of
which **468 MiB is keys**. Block-max metadata should be a small summary
*attached to* a block, not a separately keyed row per block.

## What NOT to do

**Do not drop positions.** Tantivy stores positions and still fits in
0.70 GiB. Positions are not the problem; the representation is. Dropping them
would treat a symptom and give up phrase search for it.

**Do not tune query execution further** until this is fixed. Everything tuned
so far — partition width, cache classes, adaptive scheduling — was making a
13x-oversized index traverse faster.

## The target

Keep this corpus and this table. For every storage change:

| Metric | BicDB now | Tantivy | First milestone |
|---|---|---|---|
| index | ~9.5 GB | 0.70 GiB | **< 2 GB** |
| build | 136 s | 6.5 s | **< 20 s** |
| positions | yes | yes | yes |
| documents | 123,187 | 123,187 | identical |

Within 2x first. Parity second. Speed third.

## G1a: the database was repeatedly saying its own name

The first strike, one line, before any packed-format work.

Every key in the FTS keyspace embeds the **physical index name**, which was
generated as `$bicdb_fts_build_<uuid-simple>` — **49 bytes**, in every one of
18.4 million keys. That is ~858 MiB of one repeated constant: **62% of all key
bytes**.

Shortened to `$f<8 hex>` — 10 bytes. These names only need to be unique among
the live generations of one database, not globally.

| | Before | After | |
|---|---|---|---|
| **keys** | 1.38 GiB | **0.73 GiB** | **-47%** |
| logical total | 5.16 GiB | 4.50 GiB | -0.66 GiB |
| **on disk** | 11.0 GB | **8.8 GB** | **-2.2 GB (-20%)** |
| build | 136 s | 128 s | -6% |

### The finding that matters more than the 2.2 GB

**A 0.66 GiB logical saving became 2.2 GB on disk — 3.4x amplification.**

Key width is therefore a first-class FTS performance variable, not a tidiness
concern. Narrower keys mean more entries per page, fewer pages, shallower
B-tree descents, fewer splits, less I/O and better cache residency. When
18 million keyed objects eventually collapse into packed streams, the query
benefit should exceed what the byte count alone predicts.

### And build time barely moved

136 s -> 128 s, only 6%, against a 20% reduction in bytes written. **The build
is not bottlenecked on bytes.** It is bottlenecked on the per-object work:
millions of keyed insertions, B-tree maintenance, comparisons, and impact and
dictionary object construction. That is the case for the packed-segment
redesign, stated in measurement rather than intuition.

### Next: forensics on the remaining 0.73 GiB

Before writing the packed subsystem, account for every remaining key byte:

```text
namespace tag            ?
physical index id        ?
term bytes               ?
doc / block id           ?
separator + escaping     ?
length prefixes          ?
```

The question to ask of each: **is this byte already implied by the tree it
lives in?** Every implied byte should disappear. At the amplification measured
above, another 300 MiB of logical key waste would be roughly another 1 GB on
disk — still without touching the index algorithm.

## G1b: forensics on the remaining 0.72 GiB of keys

`fts_workload <db> keys`. Every FTS key is
`[3 ns][2 len][index name][escaped term][0x00 0x00][8 suffix]`, so attributing
the bytes is arithmetic once the layout is known.

| namespace | entries | total | ns+len | idx name | framing | term | id |
|---|---|---|---|---|---|---|---|
| postings (numeric) | 6,277,186 | 275.8M | 29.9M | 59.9M | 12.0M | 126.2M | 47.9M |
| impact blocks | 6,049,674 | 245.1M | 28.8M | 57.7M | 11.5M | 100.8M | 46.2M |
| term dictionary | 5,401,274 | 209.7M | 25.8M | 51.5M | — | 132.4M | — |
| document ids | 246,374 | 6.1M | 1.2M | 2.3M | — | 2.6M | — |
| document terms | 123,187 | 3.2M | 0.6M | 1.2M | — | 1.4M | — |

```text
TOTAL   0.72 GiB across 18,097,695 entries   (42.9 B/key)

REQUIRED  term + id                457.4 MiB   61.8%
IMPLIED   ns + len + name + framing 282.4 MiB   38.2%
            index name              172.6 MiB
            ns + length prefix       86.3 MiB
            framing                  23.5 MiB
          escapes inside terms       22.3 MiB
```

### Tagged

| Component | MiB | Tag | Projected |
|---|---|---|---|
| term bytes | 359.4 | **CAN BECOME ORDINAL** | term -> u32: **-291 MiB** |
| index name | 172.6 | **REPEATED CONSTANT** | name -> ordinal: **-155 MiB** |
| doc / block id | 94.1 | **REQUIRED** | delta-codable within a block |
| ns + length prefix | 86.3 | **IMPLIED** | 1-byte tag, no length: **-58 MiB** |
| framing | 23.5 | **DISAPPEARS WITH ORDINALS** | **-23 MiB** |
| escapes | 22.3 | **DERIVABLE** | vanishes with fixed width |

**Only 61.8% of key bytes carry information the subtree does not already
imply — and most of that is term strings that want to be ordinals.**

Projected total if every implied and ordinal-isable byte went away: ~530 MiB
logical, which at the 3.4x amplification measured in G1a is roughly **1.8 GB
on disk**. That would take 8.8 GB to about 7 GB.

### Why that is the argument for stopping here

7 GB is still **10x Tantivy**. Shrinking keys cannot close this gap, because
the gap is not key width — it is that **18.1 million objects exist at all**.

Each carries a B-tree slot, a page-directory entry, MVCC headers and its share
of page slack. G1a proved the multiplier: 0.66 GiB logical became 2.2 GB
physical. The transformative move is not making 18 million keys smaller, it is
having thousands of packed blocks instead of 18 million keyed rows.

**No further key-shortening is worth another rebuild.** The remaining wins are
all inside the packed-segment format, where they come for free — a packed
block has no key, no namespace tag, no length prefix, no framing and no
escaping, and its term is an offset in a dictionary.

### Entry counts, which is the real target

```text
postings           6.28M  ->  thousands of packed blocks
impact metadata    6.05M  ->  block headers, not rows
term dictionary    5.40M  ->  one FST
document ids       0.25M  ->  a dense array
document terms     0.12M  ->  possibly nothing (derived state)
-----------------------------------------------------------
                  18.10M  ->  O(segments)
```

## Scoreboard

| | index | build |
|---|---|---|
| v1 | 11.0 GB | 136 s |
| G1a (short index name) | 8.8 GB | 128 s |
| G1b (forensics only, no change) | 8.8 GB | 128 s |
| packed segments, format 1 (1.0.224) | 1.15 GB | 39.8 s |
| **packed segments, format 2 (1.0.225)** | **0.66 GB** | **38.0 s** |
| **Tantivy** | **0.70 GiB** | **6.5 s** |
| first milestone | < 2 GB | < 20 s |

**The line is drawn here.** Packed FTS segments are physical-format v2, not
another optimisation, and they need builder, reader, snapshot visibility,
generation publication, tombstones, merging, crash recovery, old-format
compatibility and correctness equivalence considered together. That deserves a
fresh campaign with this report beside it.

## Packed segments shipped (1.0.224-beta): 8.8 GB -> 3.8 GB, 128 s -> 40 s

Physical-format v2. The build's merge streams its blocks into packed
immutable files instead of committing 17.7 million keyed rows:

```text
<paged>/fts-segments/<physical>/
  postings.dat    759 MB   pk-ordered blocks, byte-identical to v1
  impacts.dat     174 MB   impact-ordered blocks, byte-identical to v1
  terms.dat       221 MB   front-coded term directory + block metadata
  manifest.json            format, byte counts, SHA-256 per file
```

**The block bytes are deliberately identical to v1** — only where they live
changed. That is what made the equivalence gate airtight: bit-exact scores at
unit level, and on the retained Common Crawl corpus identical hits with
identical postings-bytes-read across all 36 shape/k rows.

| | keyed (G1a) | **packed v2** | Tantivy |
|---|---|---|---|
| total on disk | 8.8 GB | **3.8 GB** | — |
| index alone | ~9.5 GB | **1.2 GB** | 0.70 GiB |
| build | 128 s | **39.8 s** | 6.5 s |
| key bytes | 0.73 GiB | **13 MiB** | — |
| extreme@10 | 8.1 ms | **4.4 ms** | — |
| extreme@1000 | 15.9 ms | **10.6 ms** | — |

**First milestone (<2 GB) passed. Queries got ~2x faster as a side effect** —
term boundaries are served from resident metadata instead of 64-byte value
reads, batches are one contiguous pread, and no B-tree descent happens per
block. The physical-amplification argument ran in reverse, as predicted.

### How mutability still works

The segment is the immutable base. Post-build writes land in the transactional
tail exactly as before, and a FOLD writes the merged term into the paged
keyspace — which then **supersedes the segment for that term**, decided
per-term, snapshot-consistently, inside `PagedRecords`. No query kernel
changed at all; the ~30 read call sites in `db.rs` are untouched.

Aborted builds got STRICTLY safer: the keyed format committed partial blocks
that fallback logic then had to ignore; a segment build streams into
unpublished files and an abort leaves nothing visible.

### P-status

P1 segment+manifest ✓ · P2 packed representation ✓ · P3 identical reader ✓ ·
P4 block-max inline ✓ · P5 positions ✓ (inside block bytes) ·
P6 dictionary: front-coded + restarts, **not yet FST** ·
P7 tombstones/updates: inherited tail + fold overlay (works; not
segment-native) · P8 merge: not built — one segment per generation, fold
covers increments · P9 publish/recovery ✓ (phase restart, idempotent publish,
torn files refused with REINDEX guidance) · P10 ✓ for new builds — **default
ON**, `BICDB_FTS_PACKED_SEGMENTS=0` opts out, legacy generations stay
readable forever.

### What remains between 3.8 GB and Tantivy's shape

```text
row data          1.04 GB   the database being a database (fair cost)
document terms    0.82 GB   G3: the retract copy — candidate for death
segment           1.15 GB   G2: FST dictionary; re-encode block payloads
                            (per-posting doc_length/doc_distinct are
                            document properties, stored ~50x each)
```

Build floor is now tokenize + external sort (~40 s), not storage.

## Format 2 (1.0.225-beta): segment 1.15 GB -> 0.66 GB — smaller than Tantivy

G2 executed. The rule stayed the same — **store only what cannot be
recomputed** — but it now applies inside the segment files themselves. Block
bytes on disk are no longer v1; block bytes handed to kernels still are,
regenerated bit-identically at read. The equivalence gate did not move an
inch: v1's entire block header (max_impact, max_rank, max_tf, weight_mask)
is a deterministic function of the postings, so re-encoding a decoded slim
block reproduces the stored-in-v1 bytes exactly.

| file | format 1 | format 2 | what changed |
|---|---|---|---|
| postings.dat | 759 MB | **409 MB** | slim blocks: BP128 doc deltas + zigzag-delta positions; per-posting `doc_length`/`doc_distinct` (document properties stored once per *posting*, ~50x each) moved to docs.dat; v1 header dropped, regenerated at read |
| impacts.dat | 182 MB | **82 MB** | sidecar elision: 5.30M of 5.40M terms fit one pk block, and their impact copy is a pure function of it — the writer drops it, the reader resynthesizes through the build's own comparator and encoder, byte-identically by construction |
| terms.dat | 221 MB | **165 MB** | restart-anchored bases (9-10 redundant bytes/term gone), `last_doc` derived from the delta chain, `first_doc` span-encoded (one zero byte for df=1 terms), elided sidecars store no lens; multi-block terms keep the v1 header pair (+7 MB) so scattered probes skip the per-posting rank loop |
| docs.dat | — | **1 MB** | `doc_id -> (doc_length, doc_distinct)`, once per document |
| **segment** | **1.15 GB** | **657 MB = 0.61 GiB** | |

**Tantivy's index is 0.70 GiB. BicDB's is now 0.61 GiB — 13% smaller —
holding strictly more recomputable state (the impact sidecar, which Tantivy
does not have).** Whole database: 3.53 GB (row data + doc terms + segment).
Build: 38.0 s.

Equivalence held at every step, and grew teeth:

- bit-exact scores and identical hits + identical postings-bytes-read on all
  36 frozen shape/k rows of the retained corpus;
- a cross-mode impact-stream fingerprint test (gate sequence, block bounds,
  posting order, rehydrated document fields) covering stored AND
  resynthesized sidecars, with a structural assertion that elision actually
  fired;
- publish verifies the delta-chain reconstruction of `last_doc` and the base
  accumulation against the absolutes before dropping them ("torn" refusal);
- debug builds assert the stored header pair equals the recompute on every
  multi-block fetch.

Honest costs, both scheduled as the next PR (slim-native seeking kernels +
mmap the term directory):

- the extreme term's first touch re-encodes ~740 blocks: extreme@10
  mean-of-5 rounds 19.0 ms vs 8.8 ms legacy (format 1, raw stored bytes, was
  ~4.4 ms); warm rounds are block-cache-served at parity;
- first query per process loads terms.dat resident: ~38 ms one-off on this
  corpus (was 165 MB read; mmap removes it).

### The elision argument, because it generalizes

A df=1 term's impact block stores the SAME posting the pk block stores,
reordered. At 3.5M such terms that was 55 MB of pure restatement — and
another 45 MB for df 2..128. The lockstep writer proves "one pk block, one
impact block" per term *at build time* (it reads the pk pass's metadata in
term order while the impact pass streams), so the drop rule never guesses.
The synthesis path reuses `posting_cmp`'s exact ordering (impact bucket
descending, document id ascending) and the exact shared encoder. Where v1
paid 18.1M keyed rows to make everything independently addressable, format 2
pays bytes ONLY for information that cannot be derived — which turned out to
be 0.61 GiB of the original 9.5.

## Slim-native kernels + mmap (1.0.226-beta): faster than the keyed format

The re-encode bridge came out of the hot path. Fetches now hand the seeking
kernels the stored slim bytes plus the v1 header triple served from the term
directory (format 3 adds `weight_mask` there, +1 MB); the kernels decode slim
directly, with `docs.dat` rehydration inline. Single-block terms compute
their header once during decode and live in the block cache. terms.dat and
docs.dat are memory-mapped — the 165 MB resident load at first query is
gone. The v1 re-encode bridge remains on the non-hot scan paths and as the
equivalence oracle.

| query | legacy keyed | fmt 2 (re-encode) | **fmt 3 slim-native** |
|---|---|---|---|
| rare@10 (cold) | 0.38 ms | 38.3 ms | **0.21 ms** |
| extreme@10 | 8.8 ms | 19.0 ms | **7.2 ms** |
| extreme@1000 | 26.8 ms | 26.9 ms | **13.1 ms** |
| extreme+common@10 | 3.1 ms | 3.0 ms | **1.5 ms** |
| all 36 rows total | 64.5 ms | 125.5 ms | **39.3 ms** |

Hits identical on all 36 rows. The postings-bytes metric now reports slim
bytes — extreme@10 reads 2.50 MB where the keyed format read 5.97 MB, which
is the same physical truth the storage numbers tell. Segment: 658 MB
(0.613 GiB, still under Tantivy's 0.70). Existing format-2 segments are
refused with REINDEX guidance.

## G3 (1.0.227-beta): the 0.82 GB retract copy was already dead code

The plan was doc-level tombstones and segment merge. Reading the retraction
path first found something better: `full_text_diff_postings` derives the
old postings **from the row itself** — projection object parsed,
pre-tokenized array taken as given, raw text through the same tokenizer the
build used — and consults the doc-terms blob only when the row yields
nothing, which for a present field never happens and for an absent field
means nothing was indexed. The blob was unreachable on the very path it
existed to serve. Row-backed builds now simply do not write it.

| | before | after |
|---|---|---|
| whole database | 3.53 GB | **2.24 GB** |
| document terms namespace | 0.82 GB logical (1.29 GB physical) | **0** |
| build | 38.0 s | **35.4 s** |
| queries (36 frozen rows) | 39.3 ms | 40.2 ms (hits+bytes identical) |

Fully compatible in both directions: old binaries take the same row-derived
arm, old generations keep their blobs and stay readable, REINDEX reclaims
them. Direct-document ingestion keeps writing blobs — those terms are
caller-tokenized and no row exists to rederive them from; that namespace is
the honest cost of external tokenization, not a retract copy.

P7/P8 as originally imagined (doc tombstones, stale statistics, merge) are
NOT needed for this: retraction stays per-term and exact, folded by the
existing machinery. The equivalence gate gained a both-modes test that a
row-backed build holds zero blobs while delete/update still retract to the
last phantom posting.

## Single-pass build (1.0.228-beta): 35.4 s -> 26.1 s, byte-identical output

The build wrote every posting into TWO sorted spills and merged each with
its own full pass — but the impact-ordered pass only ever needed (bucket,
rank, id) per posting, and only term-locally. The merge now derives each
term's sidecar at term end from triples accumulated during the pk pass
(sorted by the impact-run comparator, cut by the block rule with the
impact-run byte estimator, encoded by a byte-identical ids-only sibling of
the encoder). The impact spill, its sort, its run files and its merge pass
no longer exist; a monster term past the memory budget spills sorted triple
chunks and k-way merges them, so 74M-document corpora cannot pin gigabytes.

| phase | two-pass | single-pass |
|---|---|---|
| scan+tokenize | 6.7 s | 5.6 s (impact spill gone) |
| merge | 15.3 + 9.8 s | **17.3 s** |
| publish | 1.5 s | 1.3 s |
| **total** | **35.4 s** | **26.1 s** |

**Every segment file — postings, impacts, docs, terms — is byte-identical
to the two-pass build.** Two-pass-era workspaces checkpointed at the
removed phase resume by redoing the single-pass merge. Campaign arc:
136 s (v1) → 26.1 s, against Tantivy's 6.5 s. The remaining floor is the
sequential merge (17.3 s, profile spread across sort, allocator, run I/O
and heap); the identified next lever is a parallel term-range merge or an
in-merge encode pipeline — either is its own campaign step.

## Reading Tantivy's source: why its build is "fast" — and the correction

Verified in `tantivy-0.22.1` + `tantivy-stacker-0.3.0` (cargo registry), plus
a re-run of the harness with merge instrumentation.

**Mechanism.** Tantivy never sorts postings. Each of up to 8 worker threads
(`index_writer.rs`, `MAX_NUM_THREAD = 8`) indexes into a bump-arena hashmap
(`stacker::ArenaHashMap`) mapping term -> `ExpUnrolledLinkedList` — an
arena-allocated unrolled list (blocks doubling 8 B -> 32 KB) that postings
APPEND to in document order. Sorting happens once per flushed segment over
unique TERMS only (`postings_writer.rs:74`), never over the 58M postings.
When a thread's memory budget fills, it flushes an independent segment;
commit just fsyncs a manifest. There is no durable intermediate state — no
run files, no checksums, no resume; a crash loses everything since the last
commit.

**The correction.** The 6.5 s figure produces **28 segments** (~4,400 docs
each) — a fan-out, not an index. Every query pays 28 term lookups and a
28-way merge until background merges consolidate. Forcing consolidation to
the single-segment shape OUR build emits directly:

| to ONE queryable segment | Tantivy 0.22 | BicDB 1.0.228 |
|---|---|---|
| build + merge | 6.2 s + **25.4 s** = **31.6 s** | **26.1 s** |
| index size | **0.402 GiB** | 0.613 GiB |

**BicDB builds the single-segment shape faster than Tantivy does.** The
earlier scoreboard rows compare our one segment against their 28-segment
layout (0.70 GiB): honest on disk-as-committed, flattering on build time —
their 6.5 s and our 26.1 s were never the same artifact.

Their merged segment is 1.5x smaller than ours, and the gap decomposes into
known, chosen items: an FST dictionary (.term 91 MB vs our front-coded
166 MB — P6, deliberately deferred), bit-packed positions (.pos 217 MB vs
our varint streams), quantized 1-byte fieldnorms (0.25 MB vs our exact 8-byte
docs.dat), and 82 MB of impact sidecar they simply do not have — it is what
buys our score-first seeking.

**The adoptable idea** (supersedes the parallel-range-merge plan): per-worker
arena term-hash accumulation, flushing term-sorted PARTIAL segments — sort
terms once per flush, never postings — then k-way merge over sorted term
streams. Our doc ids are assigned in pk order per batch, so each partial
covers a contiguous doc-id range and per-term posting lists merge by
CONCATENATION. Keeps the single output segment, resumable checkpoints,
checksummed spills and byte-stable block cuts, while deleting the 58M-record
external sort, the posting heap and most allocator traffic — the realistic
path to ~10-12 s.

## Term-grouped runs (1.0.229-beta): 26.1 s -> 22.5 s, still byte-identical

The first slice of the Tantivy adoption. Spill runs are now TERM-GROUPED —
the term written once per group instead of once per posting, bucket/rank
precomputed by the parallel tokenize workers — and the merge advances per
TERM: equal terms across runs concatenate (each run covers a contiguous,
ascending document-id range by construction), so the per-posting heap, its
58M term comparisons and its 58M per-posting term allocations no longer
exist. The heap now holds one entry per (run, term) switch. Level
collapsing for large corpora is preserved — a collapse output is itself a
grouped run. Checksummed trailers, truncation refusal and phase resume are
unchanged; the checkpoint version bump restarts in-flight old-format
builds cleanly.

Merge 17.3 -> 13.7 s, total 26.1 -> 22.5 s, output byte-identical
(cmp-verified, all four files). The remaining 13.7 s merge is sequential
read+decode+encode; the second slice is range-partitioning it across
workers over the grouped runs (they make term ranges seekable), which is
where the ~12 s target lives.

## Parallel range merge (1.0.230-beta): 22.5 s -> 13.2 s — 2.4x Tantivy

The second slice. Grouped runs carry a byte-length per group, so a reader
can skim to any term without decoding a posting — which makes the term
space partitionable. The merge now: verifies every run's digest up front
(no range worker reads a file end to end), samples split terms from one
run's group headers so ranges hold roughly equal payload bytes, and runs
one worker per range — each a pure file writer producing its part's slice
of postings.dat/impacts.dat and its own term-metadata tmps with the exact
sequential per-term logic. Assembly concatenates parts in term order
(hashing while copying), merges the per-part docs tables, and publish
rebases the part-local offsets while zipping the tmp pairs — every
verification it performed before still holds, now per part.

| phase | 1.0.229 | 1.0.230 |
|---|---|---|
| scan+tokenize | 5.8 s | 5.8 s |
| merge | 13.7 s | **4.6 s** (16 ranges) |
| publish | 1.3 s | 1.2 s |
| **total** | **22.5 s** | **13.2 s** |

**Output byte-identical to the sequential merge, cmp-verified across all
four files** — the range workers never see the paged store, and elision,
block cuts and sidecar generation replicate the sequential decisions
exactly. `BICDB_FTS_PARALLEL_MERGE=0` opts out; legacy keyed mode keeps
the sequential path.

Campaign arc: **136 s -> 13.2 s (10.3x)**. Tantivy needs 31.6 s to reach
this same one-segment shape (6.2 s fan-out + 25.4 s merge) — BicDB now
builds it **2.4x faster**, with checksummed spills, resumable phases and
crash-refusing runs Tantivy does not attempt. The remaining floor is
tokenize (5.8 s, already parallel) and publish; at-scale note: per-part
docs tables cost parts x 8 B per document at assembly, worth moving to the
ingest at 74M-document scale.

## Size lever 1 (1.0.231-beta): columnar slim blocks, 657 -> 611 MB

Format 4. The slim block's four value families live in separate frame-packed
streams — BP128 doc-id deltas, bit-packed position counts (~6 bits vs a
varint byte), ABSOLUTE 16-bit-bounded first positions (measurement killed
the delta-chain idea: chaining across unrelated documents cost 19.3
bits/value where absolutes frame at ~16), and zigzag position gaps — and
single-posting blocks (every df=1 term, 3.5M of them) skip stream framing
entirely for a 7-9 byte record. `BICDB_FTS_SLIM_STATS=1` prints the
per-stream accounting that guided this.

postings.dat 409.4 -> 362.7 MB; segment 657.3 -> 611.5 MB. And because the
position streams are now a length-prefixed tail, the BM25 leader's
score-only decode skips them without touching a byte: **extreme@10 7.0 ->
3.9 ms — the fastest this query has ever run**, all 36 frozen rows
identical hits. What measurement refused to give: doc ids run at 17.7
bits/value (rare-term gaps are near-entropy — Tantivy pays the same in
.idx) and position gaps at 12.5 bits are close to their information
content; the original -100 MB estimate for this lever was wrong and -47 MB
is what exists.

## Size levers 2+3 (1.0.232-beta): FST refuted by measurement, f32 bound to one byte

**The FST dictionary — the planned P6 — is a measured NEGATIVE on this
dictionary and is not shipping.** Built with `fst` 0.4 mapping term ->
ordinal, the automaton came out at ~105 MB where front-coding stores the
same keys in 72 MB: a real-web dictionary is dominated by df=1 hapaxes —
URLs, numbers, multilingual fragments — whose unique tails share no
suffixes, so automaton structure costs more than the bytes it deduplicates.
terms.dat grew 165.6 -> 198.9 MB. Reverted; front-coding stays. (Tantivy's
91 MB .term on this corpus is mostly its compact TermInfo, not FST magic.)

What did ship: `maximum_contribution` is a pruning UPPER bound, so it
tolerates lossy encoding in exactly one direction. One log-scale byte
(3.7% steps, always rounded up) replaces the f32: terms.dat 165.6 ->
149.4 MB, segment **611.5 -> 595.3 MB**. On the frozen query set: hits
identical AND zero postings-bytes drift — the looser bound did not change
a single block visit.

## Size lever 4 (1.0.233-beta): run-split sidecars, 595 -> 575 MB

The offline experiment on 4,000 real sidecar blocks overturned the earlier
back-of-envelope: whole-sequence zigzag paid ~18-bit BP128 frames because
every impact-bucket boundary injects one large negative jump, while
splitting into descending-bucket runs (ascending ids within each) measures
109 -> 79 bytes per block. Stored form: count, run count, both header
bounds, run lengths, a zigzag chain of run-first ids, and one ascending
in-run delta stream. The reader decodes and regenerates exact v1 compact
bytes through the shared ids encoder — same bridge discipline as slim pk
blocks. impacts.dat 82.2 -> 61.7 MB.

**Size campaign result: 657.3 -> 574.9 MB (0.535 GiB), hits identical and
queries flat on every step.** Against Tantivy's merged 0.402 GiB: 61.7 MB
of the difference is the impact sidecar it does not have; on strictly
comparable content BicDB stands at ~0.478 GiB. The measured floors — doc
ids and position gaps near entropy, FST worse than front-coding on a
hapax-heavy dictionary — are documented above; what remains beyond this is
positions-optional indexes (a product decision), not codec work.

## Progressive builds (1.0.237-beta): searchable in minutes, not hours

Adopted from reading Tantivy's source with our contract kept: its 6-second
"build" is searchable immediately because each flushed segment is a real
index and the merge is background optimization. BicDB now offers the same
online property behind `BICDB_FTS_PROGRESSIVE=1` /
`with_fts_progressive(true)` without giving up the single final segment:

- **Sub-segments during ingestion.** Every `fts_progressive_interval_docs`
  (default 1M), the build merges its new runs into a published sub-segment
  under `<physical>/sub-NNNN/`. Sub-segments cover disjoint ascending
  document ranges, so the read side composes them by CONCATENATION — the
  same invariant the parallel range merge stands on. The first sub flips
  the index visible (statistics first, so BM25 works from the first query).
- **Fan-out reads.** The segment registry holds a SegmentSet; per-term
  postings, boundaries and statistics concatenate/sum across subs, impact
  sidecars k-way merge on their header bounds so the score-first gate keeps
  its non-increasing order. A finished build is a set of one — the fast
  path is unchanged.
- **The stepped build.** `full_text_build_step(definition, budget_docs)`
  runs one bounded slice and returns `InProgress`/`Complete`; a server or
  embedding loops it, answering queries between steps over everything
  already published. The final step merges, publishes the ordinary single
  segment, atomically swaps, and deletes the disposable subs — the finished
  index is byte-for-byte the CREATE INDEX artifact.

Deliberate semantics, opt-in for exactly this reason: partial results are
VISIBLE mid-build, and a crashed progressive build stays visible-partial
after `create_index`/steps resume (which re-flip immediately). The
equivalence suite gained the full lifecycle: crash before the final merge
-> partial index answers; resume -> fingerprint identical to a clean
build; sub directories gone. The interleaved-steps test also caught the
one subtle hazard before it could ship: the FTS block/dictionary cache is
keyed by generation NAME, and a progressive generation changes content
under a constant name — every registry change now purges that
generation's cache entries, block and statistics alike.

Cost: each run is merged twice (sub + final), ~+25-35% total build work.
For the Common Crawl deployment that trades a day of unsearchable
building for results within the first interval.

## Reproducing

```bash
fts_workload <db> seed-wet /home/benchmark/cc-wet 400000
fts_workload <db> build
fts_workload <db> storage
# reference
/home/benchmark/tantivy-bench/target/release/tantivy-bench /home/benchmark/cc-wet <out> 400000 stored
```
