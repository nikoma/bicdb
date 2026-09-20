# Common Crawl, first contact

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

123,187 documents from six WET files of **CC-MAIN-2026-30** (July 2026),
0.97 GiB of extracted text, ingested and indexed on 24 cores.

Every number in `fts-search-node-workload.md` before this was measured against
a 40-word synthetic vocabulary. This is what changed when real text arrived.

## 1. Document size: the estimate held

**8,487 B/doc mean** against the 8.0 KB working estimate. Sizing based on it
stands.

But the *distribution* does not:

| | Synthetic | Real WET |
|---|---|---|
| median | 6,902 B | 4,668 B |
| p99 | 7,061 B | **63,486 B** |
| spread | 1.02x | **13.6x** |

The synthetic corpus was effectively uniform. Real text has a median well
below the mean and a p99 an order of magnitude above it — so per-document cost
models built on the mean are wrong at both ends.

## 2. Vocabulary: wrong by four orders of magnitude

| | Synthetic | Real WET |
|---|---|---|
| terms with df>=3 | 40 | **487,735** (from 40,000 documents) |

Sampled strata, by true document-frequency percentile:

| Stratum | Example | df (of 40,000) |
|---|---|---|
| extreme | `the` | **30,524** (76% of documents) |
| common (97th) | `doble` | 124 (0.31%) |
| medium (50th) | `6175` | 5 |
| rare (3rd) | Thai fragment | 3 |

**A 246x gap between the extreme tier and the 97th percentile.** The synthetic
corpus spanned maybe 3x. The real curve is a tiny head of enormous posting
lists above a vast tail of near-empty ones, and the median term appears in
**five** documents.

The tail is also multilingual and full of junk: Vietnamese, Cyrillic, Arabic,
Thai, German, Catalan, and bare numbers like `8329337`. Any vocabulary
estimate that assumed English words is wrong.

## 3. THE INDEX IS ~9x THE TEXT, not ~10%

| | |
|---|---|
| text ingested | 0.97 GiB |
| on disk, no compression | **11.0 GB** (10.5x) |
| on disk, `paged_value_compression` | **9.5 GB** (9.8x) |
| per document | ~77 KB for an 8.5 KB document |

Earlier this document estimated postings at **~10% of text size**. The
measured figure is **~900%** — off by roughly ninety times.

Compression removed only 14%, not the 50% claimed earlier, which localises the
cost: **the bulk is the index, not the stored text.** Positional postings over
a 487,735-term vocabulary, where an 8.5 KB document contributes on the order of
a thousand positioned occurrences.

### What this does to the 16 TiB plan

```text
16 TiB of extracted text  ->  ~157 TiB on disk
```

against the ~20 TiB previously assumed. That is a different machine, a
different budget, and a different architecture conversation. **Non-positional
postings with a positional re-check on the top-k — previously framed as a
nice-to-have — is now the load-bearing decision**, because positions are most
of the 9x.

## 4. The parallelism work is UNTESTED on real data

Every ranked query in the run reported `workers 0/0`.

`PARALLEL_RAREST_DF_FLOOR` is 262,144. The most common term in this corpus has
df ~94,000, so **nothing qualifies and the parallel path never engages**. The
partition-width rule, the top-k budget of 800, the adaptive ceiling of 8 — all
were measured on a synthetic corpus where the floor *was* cleared, and none of
them have run against real text.

They are not disproven. They are unexercised, which is worse than it sounds:
the tuning is currently justified by a corpus whose shape does not exist.

## 5. A methodological failure worth keeping

Most conjunctive shapes returned **zero hits**: `common+common`,
`medium+common`, `rare+common`, `three_term`, `five_term`.

Sampling terms independently from the df distribution produces pairs that
never co-occur. Real queries are *correlated* — people search for terms that
appear together. A percentile-sampled query set measures the empty-result path,
not search.

Only three shapes did real work:

| Shape | k | wall | postings read |
|---|---|---|---|
| `extreme` (`the` AND `2026`) | 10 | 6.2 ms | **6.0 MB** |
| `extreme` | 1000 | 13.5 ms | 6.4 MB |
| `extreme+common` | 100 | 2.8 ms | 350 KB |

Three orders of magnitude between the extreme tier and everything else. **That
is the only stratum that exercises the engine**, and it is the one a synthetic
corpus cannot produce.

The next version of the query generator must build conjunctions from terms
**observed co-occurring in sampled documents**, not from independent
percentile draws.

## 6. A tokenizer defect, narrower than first claimed (1.0.218-beta)

The rare stratum first came back as Thai *fragments* — `องช`, `องทางต` — which
looked like the indexer shredding Thai at its vowel signs.

**That diagnosis was wrong, and the empirical test is what caught it.** Rust's
`char::is_alphanumeric()` uses the Unicode `Alphabetic` property, which
includes `Other_Alphabetic` and therefore already covers Thai, Lao and Indic
vowel signs. Thai and Devanagari indexed and matched correctly all along. The
fragments were an artifact of the *analysis script*, whose regex class was not
the same set.

The real defect was narrower and genuine: **Latin combining marks in
U+0300..U+036F are marks but not `Other_Alphabetic`**, so `cafe` + U+0301 —
the decomposed spelling of `café`, which macOS-originated text and some HTML
pipelines produce — split into `cafe` plus a stray mark and never matched the
precomposed form.

It also meant **two tokenizers disagreed**: the SQL layer's
`is_text_token_character` already admitted marks, while the core indexer did
not. A document indexed by one and queried through the other is indexed under
terms no query can produce.

Both now use `is_alphanumeric() || '_' || is_mark()`.

### What the fix changed, and what it did not

| | Before | After |
|---|---|---|
| terms with df>=3 | 487,735 | 484,191 (**-0.7%**) |
| rare stratum | `องช`, `องทางต` (fragments) | `ของบริษัท`, `ขั้นตอน`, `ข่าวสาร` (words) |

The stratum went from shards to meaningful Thai words — but the vocabulary
count barely moved. **The tokenizer was not the cause of the vocabulary
explosion.** 484,191 terms from 40,000 documents is simply what multilingual
web text plus numeric junk looks like, and the storage conclusion in section 3
stands unchanged.

## 7. Tantivy on the same corpus: BicDB's index is ~13x larger

Same 123,187 documents, same WET files, same 200-byte filter. Tantivy 0.22
with `TEXT` (positions and norms, matching what BicDB stores) and `STORED`
for the like-for-like case, 512 MB writer heap.

| | BicDB | Tantivy |
|---|---|---|
| text | 0.97 GiB | 0.97 GiB |
| **index + stored text** | **9.5-11.0 GB** | **1.18 GiB** |
| index only | ~9.5 GB | **0.70 GiB** |
| ratio to text | 9.8-10.5x | **1.21x** |
| build time | 136 s | **6.5 s** |

Splitting BicDB's total: the record store was **1.5 GB** before the index
build, so the index is **~9.5 GB**. Against Tantivy's 0.70 GiB inverted index,
that is **roughly 13x larger, built 21x slower**.

### What is legitimate and what is not

BicDB is a transactional database and Tantivy is a search index. Some of the
gap is real work Tantivy does not do — MVCC version headers, page checksums,
WAL-backed durability, and a record store that supports updates. The record
store at 1.55x of text against Tantivy's ~0.5x stored is roughly that cost,
and it is defensible.

**The index is not.** 13x is far beyond transactional bookkeeping, and it is
where essentially all the excess lives.

### Where to look

- **Posting encoding.** A BicDB posting carries `document_id: u64`,
  `doc_length: u32`, `doc_distinct: u32` and positions as raw `u16`. Tantivy
  delta-encodes and bit-packs document ids in SIMD blocks and VByte-encodes
  positions. Per-posting fixed overhead is the first suspect.
- **Per-document fields repeated per posting.** `doc_length` and
  `doc_distinct` are document properties, not posting properties. Storing them
  on every posting multiplies them by the document's term count.
- **Stored-text compression.** `paged_value_compression` removed only 14% here;
  Tantivy's store gets roughly 50%. Worth checking what is being compressed and
  at what granularity.

### What this changes

Section 3 extrapolated **~157 TiB** for 16 TiB of text and called
non-positional postings load-bearing. Tantivy indexes the same text at 1.21x
INCLUDING positions and stored text, which puts the same corpus near **19 TiB**.

So the conclusion flips: **the problem is not that positions are expensive, it
is that this encoding is expensive.** Dropping positions would be treating a
symptom. A posting format closer to the state of the art is worth more than any
query-side optimisation in this document, and it is worth more than
segmentation.

### Reproducing

`/home/benchmark/tantivy-bench` — `tantivy-bench <wet-dir> <out-dir> [max-docs]
[stored|nostored]`.

## 8. Neither engine can search Thai, and my earlier test hid it

`a_thai_word_matches_the_document_containing_it` passes — but only because the
Common Crawl page it was taken from happened to put spaces between those Thai
words. **Thai is normally written without spaces.**

Tantivy on the same input, default analyzer:

| Input | Tokens produced | Search for a word inside |
|---|---|---|
| `ศึกษาวิจัยและเสนอแนะ` (as written) | **`[]` — none** | **no match** |
| `ศึกษา วิจัย และ เสนอแนะ` (spaced) | 4 tokens | match |
| as written, n-gram analyzer | 37 n-grams | match |

Tantivy produces **zero** tokens for the unbroken run: its default chain ends
with `RemoveLongFilter(40)`, and 20 Thai characters is 60 bytes.

BicDB keeps the run as **one term**. Searching for a word inside it fails;
searching for the entire run succeeds. Different failure, same outcome.

This is **not** the combining-mark defect from section 6 and is not fixed by
it. There is no boundary character to split on. Correct support needs
dictionary-based word segmentation, and the same gap applies to Lao, Khmer,
Burmese, Chinese and Japanese — a large share of the web.

`thai_written_without_spaces_does_not_match_a_word_inside_it` pins the current
behaviour and says in its assertion that it should become a positive
assertion once segmentation exists.

The honest summary: **BicDB's CJK/SEA support is no worse than Tantivy's
default, and both are inadequate.** Tantivy at least ships alternative
analyzers.

## What to do with this

1. **Fix the posting encoding.** Tantivy indexes this corpus at 1.21x
   including positions and stored text; BicDB's index alone is ~13x larger.
   This is now the single highest-value item in the FTS stack.
2. **Do not drop positions as a first move.** Section 7 shows positions are not
   the cause — the encoding is. Dropping them would treat a symptom and give up
   phrase search for it.
3. **Fix the query generator** to sample co-occurring terms.
4. **Re-derive every tuning constant** once queries reach the parallel path —
   ideally on a corpus large enough to clear the DF floor.

## Reproducing

```bash
curl -O https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-30/wet.paths.gz
# fetch N paths, gunzip into <dir>
fts_workload <db> seed-wet <dir> [max-docs]
fts_workload <db> build
fts_workload <db> queryset <out.json> [sample]
fts_workload <db> bench <out.json>
```

`BICDB_WORKLOAD_COMPRESS=1` enables value compression;
`BICDB_WORKLOAD_POOL_MB` sizes the buffer pool.
