# Search-node workload measurements

`cargo run --release -p bicdb-core --example fts_workload -- <dir> seed|seed-wet|build|phases|matrix`

Two questions on one corpus: does cache isolation hold under a real workload,
and where do the ranked-parallelism gates bind. `seed-wet` ingests a directory
of extracted-text files so the same measurements run against a real corpus.

Corpus below: 400,000 synthetic documents (~1,100 Zipf-skewed words each),
8.4 GiB on disk, 96 MiB buffer pool, 24 cores.

## 1. The three-phase test does not discriminate, and that is the finding

Popular queries -> stream every document -> the same queries again:

```
phase 1   p50 140.66ms   cache 40.6%   workers 700/700
phase 2   400,000 documents (2.57 GiB) scanned in 2.71s
phase 3   p50 142.38ms   cache 41.5%   workers 700/700
VERDICT   1.03x  PASS
```

**It passes identically with cache classes disabled.** The control was run and
gave 1.01x either way, so this PASS proves nothing on its own.

The reason is architectural and worth knowing: **posting blocks and document
pages already live in different caches.** `fts_block_cache_bytes` holds decoded
posting blocks; `paged_buffer_pool_bytes` holds pages. A document scan cannot
evict postings at that boundary however large it is, because it never touches
that cache. Note phase 2 reports `postings touched 0 B`.

The buffer-pool cache classes shipped in 1.0.205-beta are still doing real
work — they protect catalog, B-tree interior and non-FTS index pages — but for
the *ranked FTS path specifically* the separation already existed one level up.

## 2. The ranked path is CPU-bound on posting decode, not cache-bound

Sweeping `fts_block_cache_bytes` on the 120k corpus:

| Cache | Hit rate | Mean latency |
|---|---|---|
| 1 MiB | 0.0% | 35.29 ms |
| 8 MiB | 27.0% | 37.02 ms |
| 64 MiB | 96.1% | 35.70 ms |
| 512 MiB | 96.1% | 35.62 ms |

**Hit rate moves 0% to 96% and latency does not move.** Buying RAM for the
block cache is not the lever it looks like; the time is in decoding ~35 MB of
postings per query, not in fetching them.

## 3. Parallelism is unreachable below the DF floor

`workers 0/0` for every query on the 120,000-document corpus.
`PARALLEL_RAREST_DF_FLOOR` is 262,144, so no term in a corpus that size can
qualify. At 400,000 documents the same queries report `700/700`.

This is worth stating plainly because it means **small-corpus benchmarks
cannot measure ranked parallelism at all**, and a test that does not check
`workers` will silently report single-threaded numbers as if they were
parallel ones.

## 4. Partition count and cache hit rate are coupled — and 24 is harmful

400,000 documents, wall-clock per query:

| Shape | top-k | p=1 | p=8 | p=24 |
|---|---|---|---|---|
| rare+common | 10 | 74.6 ms | **24.5 ms** | 32.1 ms |
| rare+common | 1000 | **122.6 ms** | 115.4 ms | 234.4 ms |
| medium+common | 10 | 118.2 ms | **55.7 ms** | 74.5 ms |
| medium+common | 100 | 121.8 ms | **118.7 ms** | 312.0 ms |
| medium+common | 1000 | **171.3 ms** | 181.1 ms | 329.5 ms |
| common+common | 10 | 140.5 ms | **73.4 ms** | 88.9 ms |
| common+common | 100 | 146.9 ms | **142.1 ms** | 320.6 ms |

The mechanism shows up in the instrumentation rather than having to be
guessed. For `medium+common` at top-k 1000:

| Partitions | Cache hit rate | Posting bytes read |
|---|---|---|
| 1 | 85.4% | 107.8 MB |
| 8 | 3.3% | 147.4 MB |
| 24 | 3.1% | 178.4 MB |

Partitioning weakens WAND pruning, so each partition reads more postings; the
extra bytes blow the shared block cache, and the hit rate collapses from 85%
to 3%. **The cost of parallelism is not just CPU — it is cache residency**,
and past a point the extra reads cost more than the extra cores save.

### Fixed: the partition count now falls as top-k rises (1.0.211-beta)

`effective_fts_partitions(configured, keep)` narrows the configured width for
large-k queries and never widens it. The budget is 800 retained hits, which
puts `k<=100` at full width and drives `k>=800` to serial — where the
measurements say the crossover is.

Re-measured at **configured = 24**, the setting a search-node operator would
choose:

| Shape | top-k | Before | After | |
|---|---|---|---|---|
| rare+common | 100 | 66.9 ms | **41.2 ms** | 1.6x |
| rare+common | 1000 | 234.4 ms | **139.7 ms** | 1.7x |
| medium+common | 100 | 312.0 ms | **109.2 ms** | 2.9x |
| medium+common | 1000 | 329.5 ms | **227.9 ms** | 1.4x |
| common+common | 100 | 320.6 ms | **158.2 ms** | 2.0x |

Block-cache hit rate at `k=1000` recovered from **3-4% to 89-91%**, which is
the mechanism rather than a side effect: fewer partitions read fewer postings,
and the cache stops thrashing.

The `workers` column shows the rule firing — `115/115` at k=10 (24
partitions), `35/35` at k=100 (8), `0/0` at k=1000 (serial).

At the default `min(8, cores)` the rule is neutral for `k<=100` and sends
`k=1000` serial. One honest cell: `medium+common` at `k=1000` measured 181 ms
at 8 partitions against 196 ms serial here, so the rule costs a little there
while winning 1.4-2.9x at the wide configurations it exists to protect.

## 5. Concurrency: permit starvation is real, and it inverts the tuning advice

`fts_workload <dir> concurrent <clients> <seconds>`. 400,000 documents,
24 cores, `fts_query_partitions=8`:

| Clients | q/s | p50 | granted/wanted | Starvation |
|---|---|---|---|---|
| 1 | 12 | 87 ms | 665/665 | 0.0% |
| 4 | 15 | 281 ms | 698/833 | 16.2% |
| 16 | 27 | 610 ms | 454/1498 | **69.7%** |

The counters added in 1.0.204-beta do exactly what they were built for: at 16
clients **70% of requested workers are denied**, which is the global permit
pool doing its job rather than a defect.

### The finding that inverts earlier advice

Sweeping the partition count at 16 concurrent clients:

| Partitions | q/s | p50 |
|---|---|---|
| **1** | **48** | **296 ms** |
| 2 | 22 | 728 ms |
| 8 | 26 | 613 ms |
| 24 | 26 | 622 ms |

**Serial per-query wins by 1.85x on throughput and 2x on p50.** Concurrency
already saturates the cores, so splitting each query buys no parallelism the
machine has to give — it only pays WAND pruning loss and re-reads postings
that pruning would have skipped.

This **contradicts the search-node profile recommended earlier in this
document**, which said to raise `fts_query_partitions` toward the core count.
That advice holds only for a node serving few concurrent queries. Corrected:

```text
interactive / low QPS   fts_query_partitions = cores      (latency)
serving load / high QPS fts_query_partitions = 1          (throughput)
```

**Superseded by section 6**: the engine now picks this per query.

Intra-query parallelism and concurrency compete for the same cores. Only one
of them should be spending them.

### All of the plan or none of it (1.0.212-beta)

A partial permit grant pays pruning cost for partial parallelism, and is
itself the signal that the machine is busy. A query that cannot get its whole
plan now hands the permits straight back and runs serial.

Worth 26 -> 29 q/s and p50 613 -> 548 ms at 16 clients, and neutral at 1
client, where the full plan is always granted.

**It does not close the gap to serial's 48 q/s.** A first attempt — refusing
only *narrow* grants, below 3 extra workers — changed nothing at all, because
under this load the pool grants either the full 7 or 0 and the narrow case
barely occurs. The dominant lever here is configuration, not code.

## 6. Adaptive partition width (1.0.214-beta)

The section above shows the optimum **moving with workload shape**, which a
static knob cannot express. `fts_query_partitions = 0` — now the default —
chooses per query from current load:

```text
width = clamp(cores / in-flight ranked queries, 1, 8)
```

then narrowed by the top-k rule from section 4.

One query gets the machine. Sixteen concurrent queries get one core each and
run serial. Five get four or five ways. The ceiling of 8 exists because width
has negative returns even when idle: a single query measured 24.5 ms at 8
partitions against 32.1 ms at 24.

| Clients | **Adaptive** | fixed = 8 | fixed = 1 |
|---|---|---|---|
| 1 | **12 q/s, p50 91 ms** | 12 q/s, p50 88 ms | 8 q/s, p50 110 ms |
| 4 | **22 q/s, p50 194 ms** | 15 q/s, p50 281 ms | — |
| 16 | **45 q/s, p50 319 ms** | 26 q/s, p50 613 ms | 48 q/s, p50 296 ms |

Adaptive lands within 6% of the best fixed choice at both extremes and is
**1.4x better than any fixed choice in the middle**, where neither static
setting is right. An explicit non-zero width still pins the plan — adaptive is
a default, not a takeover.

### What this replaces

The two-line operator table from section 5 is now the engine's job:

```text
interactive / low QPS   -> partitions ~= cores     was manual
serving / high QPS      -> partitions = 1          was manual
```

### Honest limits

- In-flight count is a **coarse** signal. It does not know whether those
  queries are CPU-bound or blocked on I/O, so a workload where most queries
  are waiting on disk will narrow more than it needs to.
- The ceiling of 8 and the top-k budget of 800 are both fitted to a 40-word
  synthetic vocabulary. Real Common Crawl text has different posting sizes and
  different pruning behaviour, and **both constants should be re-derived
  against a real corpus** before they are trusted.

## 7. The real-corpus benchmark suite (1.0.215-beta)

Everything above was measured against a **40-word synthetic vocabulary**. It
did its job — it exposed mechanics: pruning loss, cache thrash, permit
starvation, the concurrency inversion. It cannot validate the constants those
mechanics produced, because FTS is unusually sensitive to real distributions:
Zipfian frequency, enormous stopword postings, HTML garbage, duplicate
content, document-length variance, and every language on earth.

```
fts_workload <dir> seed-wet <dir-of-text>     ingest real extracted text
fts_workload <dir> build
fts_workload <dir> queryset <out.json> [n]    derive strata FROM the corpus
fts_workload <dir> bench <queryset.json>      run the retained set
fts_workload <dir> concurrent <clients> <s>   serving behaviour
```

### The strata are derived, not written down

"Common" means whatever is common in **this** text. The generator samples
documents, counts **document** frequency (a per-document term set, because df
is what drives BM25 pruning), drops the `df < 3` tail the way a real index
would, and splits the survivors **by rank**: top 1% common, next quartile
medium, remainder rare.

Shapes: `rare`, `medium`, `common`, `rare+common`, `medium+common`,
`common+common`, `three_term`, `five_term`, each at k = 10 / 100 / 1000.

### Keep the file

The query set is written to disk and meant to be **retained forever**. A
benchmark whose queries drift is a benchmark whose numbers cannot be compared
across releases. It also records what it sampled — document count, surviving
vocabulary, median and p99 document length — so a later reader can tell
whether two runs are comparable at all.

### What this run proves, and what it does not

Exercised end to end on the synthetic corpus: 20,000 documents sampled, 40
terms surviving `df>=3`, median document 6,902 B, and the full 8-shape x 3-k
matrix executes.

**The strata it produced are meaningless there, and visibly so** — "rare"
came back as `that`, `this`, `will`, because a 40-word vocabulary has no tail.
That is the demonstration, not a defect: the machinery is right and the corpus
is wrong. Two constants in particular are still fitted to synthetic text and
should be re-derived before they are trusted:

- `ADAPTIVE_PARTITION_CEILING = 8`
- `PARTITION_KEEP_BUDGET = 800`

### Three truths, not one

Every future FTS change should report all three, because each hides what the
others show:

```text
matrix        single-query latency by shape and k   (synthetic, fast, stable)
bench         the same, against real text           (retained query set)
concurrent    serving throughput and starvation     (where tuning inverts)
```

## What is still not measured

- **No real corpus yet.** The suite in section 7 is built and exercised, but
  has never been run against Common Crawl WET output. Until it is, every
  constant here is fitted to a 40-word vocabulary.
- The buffer pool was never made the bottleneck, because the FTS block cache
  absorbs the ranked path. A workload that stresses non-FTS index pages would
  be needed to exercise the 1.0.205-beta cache classes end to end.
