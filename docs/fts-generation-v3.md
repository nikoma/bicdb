# FTS Generation Format v3

Status: complete in `0.9.115-beta`.

Update `1.0.128-beta`: posting blocks are now written as **v4**. The layout
splits the per-posting position stream out of the metadata stream, so the
ranked BM25 score decoder reads document IDs, lengths and term frequencies
without touching a single position byte; positions are decoded only for
candidates that reach the retained top-k. Bit-packed frames are also decoded
word-at-a-time instead of bit-at-a-time. Measured on the 128-document/tf-48
decoder microbenchmark this makes score-only block decode 9.3x faster
(65 -> 7 ns per posting). v1-v3 blocks remain fully readable (v2/v3 score
decodes skip interleaved positions with a word-scan instead of per-varint
decode); binaries older than `1.0.128-beta` cannot read v4 blocks, so
rebuild or refold indexes only after the fleet is upgraded.

Update `1.0.129-beta`: conjunctive ranked BM25 navigates secondary terms
through a **key-only shallow seek**. Block boundaries come from the posting
B-tree keys and the rank ceiling from a bounded 64-byte value prefix
(`read_prefix` — no full block value is resolved through the MVCC heap);
payloads are fetched only for windows that survive the block-max gate.
**This requires no index rebuild**: it works unchanged on existing v1-v4
blocks, which matters for TB-scale corpora where a rebuild takes a day or
more. Instrumented as `posting_block_boundaries_read`.

Update `1.0.131-beta`: `SELECT count(*) ... WHERE tsv @@ q` is answered by
**dense posting-block intersection** when `q` is a constant conjunction of
plain lexemes — previously every unranked `@@` predicate fell to a full
table scan that re-tokenized each row's text (measured 7.8 s for a broad
two-term count at 200k docs; now 6 ms, and unlike the scan it does not grow
with corpus text size). OR/NOT/phrase/prefix/weighted queries, extra WHERE
conjuncts, transactional tails, tombstones, RLS and in-transaction reads
all decline to the ordinary scan. No rebuild required. Instrumented as
`boolean_block_counts`.

Update `1.0.132-beta`: the two shapes 1.0.131 left on the re-tokenizing
scan now come from posting blocks as well. **Phrase counts** enumerate the
conjunction of the query's lexemes and decide distance with the standard
`matches()` recheck over each candidate's packed positions (7.5 s -> 6.5 ms
at 200k). **Unranked row-returning `@@` SELECTs** (no ORDER BY) resolve the
same candidates to primary keys and point row fetches, with LIMIT/OFFSET
bounding the scan (broad two-term SELECT LIMIT 10: 7.5 s -> 8 ms; phrase
SELECT LIMIT 10: 1.8 ms). The conjunctive position scan is two-pass —
document ids intersect through the position-free decoder first, positions
decode only for blocks holding intersection members — and declines above a
32M-posting budget. Same fallback gates as the count path; instrumented as
`conjunctive_block_scans`. No rebuild required. Still on the ordinary scan
by design: OR/NOT/prefix/weighted queries, extra WHERE conjuncts, ORDER BY
(non-ts_rank), RLS, and in-transaction reads.

Update `1.0.133-beta`: two ranked-path scale fixes measured against the
40M-document PubMed workload profile. **Ranked phrase queries** no longer
resolve a primary key per conjunctive candidate before ranking — the
candidate arena fills from the two-pass position scan on document ids and
pks resolve lazily, only for candidates that actually contend for the
top-k (tie-break order, by pk, is unchanged). **Broad two-term ranked AND**
("diabetes treatment": every operand over 16k documents) routes to the
same intersection arena instead of block-max WAND, whose noisy-OR bound
saturates near 1.0 on broad pairs and degenerates into a full positions
decode of both posting lists (measured 2.9 s at 40M docs). Selective
conjunctions and 3+-term queries keep WAND, which their bounds reward.

This is the implementation checklist for BicDB's search-oriented full-text
generation. The format is rebuilt as a shadow physical generation and is
published only after all posting orders, dictionaries, statistics, document-id
maps, and the completeness sentinel are durable.

## Completed checklist

- [x] **Compact term dictionary.** One bounded lookup returns document and
  collection frequency, posting locations and byte counts, block counts,
  document-id bounds, and maximum contribution. Query planning does not count
  posting keys.
- [x] **Numeric internal document IDs.** Each immutable generation assigns
  dense `u64` IDs and persists both primary-key mappings. Posting blocks contain
  numeric IDs rather than repeated front-coded UUID strings.
- [x] **Compressed postings and SIMD intersection.** Document-ID deltas are
  bit-packed in 128-value groups. Conjunction uses architecture-specific AVX2
  or NEON equality probes where available and a portable galloping fallback.
- [x] **Multi-term Block-Max WAND/MaxScore.** Ranked OR uses document-at-a-time
  WAND pivots with global and per-block bounds. Ranked AND uses the rarest
  driver plus block-max pruning. Both return exact top-k results.
- [x] **BM25 and BM25F.** Generation statistics include corpus, document, and
  four weighted-field lengths. Public APIs expose configurable BM25 and BM25F
  ranking with IDF, length normalization, and field boosts.
- [x] **Native filter pushdown.** B-tree equality results and sealed exact-value
  postings can be translated to adaptive sparse/dense generation-qualified
  filters and intersected inside WAND, ranked AND, BM25, and BM25F before
  scoring. SQL pushes indexed equality predicates into ranked FTS retrieval.
- [x] **Concurrent read sessions.** `FullTextReadSession` pins an independent
  MVCC snapshot and one physical FTS generation. Query workers no longer need
  to serialize lexical reads through one application database mutex.
- [x] **I/O prefetch and hot-block caching.** A shared generation-qualified LRU
  caches compact dictionary records, encoded bytes, decoded postings, and block
  headers under an explicit byte ceiling. Each term cursor can predecode a
  bounded number of upcoming blocks.
- [x] **O(1), bounded-memory FTS opening.** A completed generation is opened
  with a constant number of metadata lookups per index. Postings and per-row
  FTS registries are not scanned or retained. `FullTextOpenMetrics` makes the
  behavior testable; legacy incomplete indexes retain a correct rebuild
  fallback.

## Build and memory configuration

The external builder tokenizes bounded primary-key batches in parallel, sorts
and spills posting runs, releases batch memory, and performs bounded fan-in
merges. These settings can be supplied through `DbConfig` builder methods or
environment variables:

| Setting | Default | Meaning |
|---|---:|---|
| `BICDB_FTS_BUILD_MEMORY_BYTES` | 256 MiB | Global run-generation memory ceiling, divided among workers |
| `BICDB_FTS_BUILD_WORKERS` | available CPU, max 64 | Tokenization and run-generation workers |
| `BICDB_FTS_BLOCK_CACHE_BYTES` | 64 MiB | Combined posting/dictionary hot-cache ceiling; `0` disables it |
| `BICDB_FTS_PREFETCH_BLOCKS` | 2, max 32 | Upcoming blocks decoded per active term cursor; `0` disables it |

Merge fan-in and concurrent merge workers are separately bounded so increasing
tokenization parallelism cannot multiply memory or file-descriptor use without
limit.

## Direct ingestion and native fields

Applications that already have an analyzed corpus can build a generation
without inserting a duplicate `search_text` row. Create the empty logical FTS
index, open its normal resumable builder, submit primary-key-sorted analyzed
documents, finish tokenization, and publish:

```rust
use bicdb_core::{
    FullTextDocumentInput, FullTextField, FullTextFieldInput, FullTextFilterInput,
    FullTextTermInput,
};

let progress = db.prepare_full_text_build(
    "documents_fts",
    "documents",
    "example-analyzer-v1",
)?;

if progress.needs_tokenization {
    db.append_full_text_documents("documents_fts", &[
        FullTextDocumentInput {
            primary_key: "patient-42".into(),
            fields: vec![
                FullTextFieldInput {
                    field: FullTextField::Title,
                    terms: vec![FullTextTermInput {
                        term: "cardiology".into(),
                        positions: vec![1],
                    }],
                },
                FullTextFieldInput {
                    field: FullTextField::Body,
                    terms: vec![FullTextTermInput {
                        term: "cardiology".into(),
                        positions: vec![18],
                    }],
                },
            ],
            filters: vec![FullTextFilterInput {
                name: "domain".into(),
                value: "example.com".into(),
            }],
            stored_text: Some(b"optional source or retrieval text".to_vec()),
        },
    ])?;
    db.finish_full_text_tokenization("documents_fts")?;
}
db.complete_prepared_full_text_build("documents_fts")?;
```

`filters` are exact-match, generation-local posting sidecars. They are not
tokenized, do not contribute to document length, term frequency, or BM25
statistics, and are read with
`full_text_document_filter_from_sealed_value`. The resulting filter uses sparse
document IDs for selective values and a dense bitset only when that is smaller.
This makes values such as registrable domain, host, language, content type, or
capture month practical on sealed corpora without duplicating collection rows.

`Body`, `Auxiliary`, `Heading`, and `Title` map to BM25F slots D, C, B, and A.
Field lengths are persisted once per document and field term frequencies come
from the native posting positions. Important text therefore receives a field
boost without being repeated in a synthetic input string.

The caller owns analysis so query and index normalization remain identical.
Terms must already be normalized/stemmed and documents must be submitted in
strictly ascending primary-key order. `progress.resume_after` identifies the
first key to skip after an interruption.

Optional `stored_text` is kept in a dedicated generation namespace. Values of
256 bytes or more use zstd level 3 when compression produces a smaller result.
The envelope records and validates the decoded length, has a 64 MiB limit, and
remains readable by builds without native compression only when the stored
value used the plain codec. Retrieve it with
`BicDb::full_text_stored_text(index, primary_key)`.

## Compact impacts and storage accounting

Generation v3 impact blocks contain only bit-packed document IDs plus the
block's maximum impact and exact maximum rank. Document lengths, distinct-term
counts, field positions, and phrase positions exist only in document-order
posting blocks. An impact scan first applies the header-only block-max gate and
probes canonical postings only for surviving IDs. V1–v3 numeric posting blocks
and the former full-copy impact blocks remain readable.

Temporary impact-order runs use the same principle: each entry retains only
the term, document ID, impact bucket, and exact rank. A build resumed from a
v2 checkpoint can still read the former full-posting run envelope and rewrites
it compactly at the next merge level.

`BicDb::full_text_storage_accounting(index)` returns live logical key and value
bytes independently for:

- source collection rows;
- document-to-term reverse data;
- compact term dictionary;
- dense document-ID maps;
- canonical postings (including a transactional tail);
- compact impact metadata;
- per-document BM25F statistics;
- compressed stored text.

The report intentionally excludes page headers, free space, dead MVCC versions,
WAL, and temporary build runs. Use page-store checkpoint/vacuum and filesystem
metrics for those physical costs.

## Checkpoint, publication, and failure contract

Build manifests checkpoint tokenization, document-order merge, impact-order
merge, and publication. Completed `.run` files are reused on an identical
retry. Torn `.tmp` files and abandoned unpublished generations are removed
without touching the active generation.

The logical index name continues to resolve to the previous valid generation
throughout a rebuild. The alias catalog is atomically replaced only after the
new generation and completeness sentinel are durable. Readers that already
hold `FullTextReadSession` continue against their pinned old generation.

Terms longer than 2,046 UTF-8 bytes are skipped consistently during indexing,
resume, incremental maintenance, and querying. Their content is not logged;
the process exposes a bounded warning and
`full_text_oversized_terms_skipped()` metric.

## Migration

Existing indexes remain readable. Recreate an FTS index after upgrading to
write generation format v3 and enable the dictionary, numeric postings,
compressed/WAND/BM25 paths, filter bitsets, and constant-time opening together:

```sql
DROP INDEX idx_docs_fts;
CREATE INDEX idx_docs_fts ON docs
  USING GIN (to_tsvector('english', COALESCE(body, '')));
```

Load rows before `CREATE INDEX` for the bounded parallel bulk path. Keep the
previous production index or search service active until the atomic generation
promotion completes.

## Instrumentation

`full_text_query_instrumentation()` reports dictionary lookups/misses, posting
blocks and bytes read, postings decoded, scalar/SIMD intersections, WAND
candidates and skips, block-max prunes, filter rejections, cache hits/misses,
and prefetched blocks. `BicDb::full_text_open_metrics()` reports FTS definition
count, bounded metadata probes, and any row IDs registered solely for a legacy
or incomplete FTS fallback.


Update `1.0.136-beta`: **plain BM25 conjunctions no longer decline on
unfolded writes.** The per-term write layer — tail postings and
tombstones, capped at 65,536 entries — is layered over the sealed
blocks: documents it touches are excluded from the block-max scan and
re-scored from their current tail truth, so fresh inserts compete for
the top-k, updates rank by their new term frequencies, and deletes
disappear. The sealed corpus keeps the full seeking fast path. Ranking
uses the sealed generation's statistics (a bounded-layer IDF
perturbation). Folding (`bicdb_fts_fold`) remains worthwhile — it
shrinks the layer and reactivates the ts_rank/SQL block paths, which
still gate on a clean index — but is no longer required for ranked BM25
freshness. Instrumented as `tail_merged_bm25_queries`; `bicdb_fts_route`
reports the tail-merged verdict. Measured: a 200k corpus with 500 fresh
writes serves the ranked conjunction in 8.5 ms where it previously
declined to the caller's fallback entirely.
## Query budgets and cancellation (1.0.156-beta)

Every materializing full-text route is bounded and cancellable.
`FtsQueryLimits` carries immutable per-query ceilings (posting blocks
decoded, postings materialized, candidate documents, hydrated records);
wall-clock deadlines ride on the `CancellationToken` itself. Exhaustion
returns `query_budget_exceeded` — never a silent fallback to a wider scan
(`fts_budget_abort` in the SQL engine aborts the query instead of
declining to another plan).

Entry points: `full_text_term_postings_budgeted` (charges every block and
posting; the plain variant delegates with an unlimited budget),
`full_text_posting_probe_many_budgeted`, and
`full_text_bm25_top_k_budgeted` — which pre-flights the merged/exhaustive
route against term document frequencies (rarest for conjunctions, summed
for disjunctions) so an over-budget corpus scan is refused before it
starts. The block-max seeking paths are bounded by construction and carry
no per-posting overhead. SQL sessions opt in via
`SqlSession::with_fts_limits`; the default stays unlimited.
