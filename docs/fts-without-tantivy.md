# Full-text search without tantivy

The measured comparisons below are the **0.9.56-beta baseline**
(2026-07-27). BicDB `0.9.77-beta` adds the completed search-oriented
[FTS Generation Format v3](fts-generation-v3.md), including numeric compressed
postings, multi-term Block-Max retrieval, BM25/BM25F, native filter pushdown,
concurrent snapshots, bounded caching/prefetch, and constant-time FTS opening.
Re-benchmark against the target corpus before using the historical routing
thresholds below. Every number
in this document was measured on a 200,000-document synthetic abstract
corpus (`crates/bicdb-sql/examples/impact_probe.rs`); scaling notes call out where
behavior is linear in matches.

## The routing decision

| Query shape | Route to | Measured (200k docs, top-10, warm) |
|---|---|---|
| Boolean / filtered search, counts | **BicDB** | ~ms, bounded memory |
| Phrase search | **BicDB** | 10–25 ms |
| Ranked top-k, selective terms (<1%) | **BicDB** | ~1 ms |
| Ranked top-k, broad terms with natural (Zipfian) tf | **BicDB** | 3.7–4.3 ms |
| Ranked AND queries | **BicDB** | ~ms–80 ms (driver-size bound) |
| Ranked *uniform-tf* broad terms | **tantivy** | 620 ms vs 0.8 ms |
| Anything where BM25 ordering is the product requirement | **tantivy** until relevance is signed off | `ts_rank` ≠ BM25 |

Why the last two rows: BicDB ranks candidates it cannot prune when every
posting has identical impact (no tf variance = nothing to skip), and its
per-posting index was ~20× tantivy's size / ~10× slower to build; the
0.9.58 direct-to-blocks build plus 0.9.59 page reclamation bring a 200k-doc
corpus to ~5× build time and ~7× store size (778 MiB vs 113 MiB — most of
the remainder is row metadata carrying the JSON lexeme projections, not
postings). Closing the rest is the block-postings GA track (below). Natural-language tf is
Zipfian, so the uniform-tf row is rare in real corpora — but "study"-class
stopword-ish terms approximate it.

## Setup

```sql
-- In a server_paged database:
CREATE INDEX idx_docs_fts ON docs
  USING GIN (to_tsvector('english', COALESCE(body, '')));
```

1. **Bulk imports: insert the rows FIRST, then CREATE INDEX** (0.9.58+).
   The backfill direct-builds compressed posting blocks from a chunked
   pk-ordered scan — measured @200k docs it cuts build time ~3.5x and
   index disk ~5x vs the index-first flow, and there is no per-posting
   tail to fold afterwards. Index-first still works; every row then pays
   the per-posting tail write and the index stays ~20x bigger until you
   fold it.
2. **Create or recreate the index on ≥ 0.9.51 (≥ 0.9.58 for the direct
   build).** Postings carry positions from 0.9.48 and the impact ordering
   plus its completeness sentinel are planted by a full index build.
   Older indexes keep working but silently take slower paths — recreate
   to get the fast ones.
3. **Reopen the database after CREATE INDEX.** The creating session serves
   the index residently; after a reopen it is read-through: postings live
   on pages and resident memory is independent of index size (measured:
   20M postings reopen at the same RSS as 4M).
4. **`compact_full_text_index()` is for streaming-ingest indexes.** A
   direct-built index starts fully block-compact; call the fold
   periodically only on indexes that accumulate a per-posting tail from
   ongoing writes. Since 0.9.57 the fold runs its own
   checkpoint -> vacuum -> key-sweep chain (folded ranked broad term:
   31 ms). If a CREATE INDEX crashes mid-backfill the index serves from
   the resident fallback (correct, slower open) until you DROP + CREATE
   it again — partial blocks are never trusted.

## Query patterns

```sql
-- Boolean / filtered:
SELECT count(*) FROM docs
WHERE to_tsvector('english', COALESCE(body,''))
      @@ to_tsquery('english', 'ashwagandha & anxieti');

-- Phrase (positions are in the postings):
... @@ phraseto_tsquery('english', 'sleep quality');

-- Ranked top-k — the canonical shape, served from the impact-ordered
-- posting scan with early termination:
SELECT id FROM docs
WHERE to_tsvector('english', COALESCE(body,''))
      @@ to_tsquery('english', 'ashwagandha')
ORDER BY ts_rank(to_tsvector('english', COALESCE(body,'')),
                 to_tsquery('english', 'ashwagandha')) DESC
LIMIT 10;
```

### Fast-path checklist (ranked)

All ranked shapes are **correct**; these conditions decide which get the
millisecond paths. The impact-ordered early-termination path fires when:

- the WHERE clause is exactly one `tsvector @@ tsquery` predicate on the
  indexed expression;
- the ORDER BY is `ts_rank` (not `ts_rank_cd`) over the same expression
  and the same-shaped query, `DESC`, with a constant `LIMIT`;
- default weights and normalization `0`;
- no row-level security, not inside an open transaction.

AND queries (`'a & b'`) use probe-driven intersection automatically: the
rarest positive operand drives, other operands resolve via single-descent
point probes. Negations, prefixes (`term:*`), weights, and normalization
flags are all parity-tested against the text-ranked oracle — they fall to
the bulk path, which is linear in matching documents.

Known decline worth planning around: `SELECT ts_rank(...) AS score` in the
projection routes the whole query to the fallback. Select ids in the fast
query and compute display scores on the fetched rows.

## Adopting in an existing search service

The clean migration is a routing flag in the lexical channel, not a
rip-out:

1. Branch at the `LexicalIndex::search()` call sites: builder-generated
   queries (boolean, phrase, selective ranked — everything
   `requiresLexicalMatch` catches) → BicDB SQL; uniform-broad single-term
   ranked → tantivy.
2. Keep the RRF fusion untouched — it consumes `(doc_id, score)` pairs and
   does not care which engine produced the lexical leg.
3. Run both engines side by side for a while and compare latency **and** top-10
   overlap on representative production queries before removing the tantivy
   dependency. BM25 vs
   `ts_rank` is a relevance decision no benchmark settles.

## Operational caveats

- **Disk/build**: per-posting index ≈ 20× tantivy's bytes, ~10× build
  time. Budget accordingly until block postings reach GA.
- **Replication**: open a replicating paged primary with
  `replication.enabled` — otherwise commits log materialized markers that
  standbys cannot replay, and frame export refuses loudly.
- **Memory**: postings are read-through; the buffer pool caches hot terms.
  A degenerate hot term (50k+ docs, one page walk per posting) costs
  ~28 ms vs ~11 ms resident — the pool absorbs repeats.

## The GA track for dropping tantivy entirely

In dependency order, all specified in `IMPORTANT-TODO.md`:

1. **Index-entry GC** — MVCC `delete` keeps B-tree keys by design; folded
   terms pay ~180 ms/100k dead keys in tolerated chain walks. This is the
   gate on recommending `compact_full_text_index` in production.
2. **Page-file truncation** — freed pages recycle (PostgreSQL-style) but
   the file never shrinks in place; live posting bytes already drop
   ~400 MB → ~27 MB at 200k docs after a fold.
3. **Block-max WAND over compressed blocks** — uniform-tf broad terms and
   OR queries at tantivy speed. The impact-ordered block copy (0.9.54)
   is the substrate; the fold's dual-order layout was built for exactly
   this.
4. **Relevance sign-off** — product decision, human eyes on top-10s.

Until (1)–(3): BicDB carries boolean, phrase, AND, and Zipf-ranked search
with bounded memory; tantivy carries uniform-broad ranked retrieval and
wins on index footprint.
