# Intra-query parallel BM25 search

## The problem

One BM25 query used one core. Adding threads only helped *concurrent distinct*
queries, and those then collided on the per-collection `RwLock`. For a corpus
in the tens of TiB that is the binding constraint: query latency is fixed at
single-core throughput no matter how large the machine.

## What changed

Block-Max WAND now accepts a document range, and a query is split across N
disjoint ranges traversed concurrently, with the per-range top-k lists merged
at the end.

This parallelises without touching storage, MVCC or the on-disk format,
because the traversal is a **pure function of already-loaded blocks** — no
database handle, no lock, no I/O. Blocks are shared between partitions rather
than copied: the payload is an `Arc<[u8]>`, and the range guard is what keeps
a document straddling a boundary from being emitted twice.

## Measured

4,000,000 documents, 3 terms, 12,998 blocks, 24 cores:

| Partitions | Wall clock | Speedup |
|---|---|---|
| serial | 182.32 ms | baseline |
| 2 | 119.97 ms | 1.52x |
| 4 | 53.76 ms | 3.39x |
| 8 | 33.56 ms | 5.43x |
| **16** | **25.82 ms** | **7.06x** |
| 32 | 26.37 ms | 6.92x |

Reproduce with:

```
cargo test -p bicdb-core --lib --release parallel_wand_scaling -- --ignored --nocapture
```

## The cost, stated plainly

Speedup is **sub-linear on purpose**. Partitioning weakens WAND pruning: each
partition must establish its own top-k threshold instead of sharing one global
threshold, so *total CPU rises* even as wall-clock falls. The plateau at 32
partitions on 24 cores is that effect meeting oversubscription.

This is a latency-for-throughput trade, which is why it is a knob rather than
a constant. A read-serving deployment with few concurrent queries wants it
near the core count. A high-concurrency mixed workload wants it low.

```rust
DbConfig::default().with_fts_query_partitions(1)   // 1 under serving load; see fts-search-node-workload.md section 5
```

The default is `min(4, cores/2)` — enough to matter without letting a
high-concurrency workload spawn threads it cannot use.

Queries below 64 blocks stay serial: the hand-off costs more than it saves.

## The property that makes it safe

**A parallel search returns exactly what a serial search returns.** Not
approximately, not modulo ordering — the same documents in the same order with
the same scores. `parallel_wand_returns_exactly_what_serial_wand_returns`
checks this across 3 term counts x 3 top-k sizes x 4 partition counts, and the
scaling benchmark re-asserts it at every partition count it measures.

Partitioning changes how work is scheduled. It must never change the answer.

## Why this was built before index segmentation

Segmenting the index is the larger, more valuable change: it makes ingest
incremental and lets each segment carry its own dictionary. But it is a format
change, and it is only worth building if a single query can actually use N
cores.

This measures that on the existing format. **7x on 24 cores says the axis is
real**, so segmentation inherits a proven parallel execution path rather than
betting on one.

## Correction: the seeking path was already parallel (1.0.204-beta)

The first version of this document, and PR #547's description, said the
seeking kernels were "still serial". **That was wrong.** The production BM25
path has partitioned by document range and run partitions on
`std::thread::scope` since before either PR. The claim came from observing
that `block_max_bm25_and_top_k_seeking` is *called* serially, without checking
its caller — which already splits the work above it.

What was true: the **pre-loaded** WAND kernel was serial, and #547 fixed that.

What the mistake nearly cost: a rewrite of infrastructure that exists.

### What the seeking path is actually gated by

| Gate | Value | Effect |
|---|---|---|
| `PARALLEL_RAREST_DF_FLOOR` | 262,144 | Only parallelise when the *rarest* term matches 262k+ documents |
| worker cap | was hard-coded `min(8)` | A 24-core node left 16 cores idle |
| permit pool | `cores - 1`, **global** | A query issued while the node is busy gets fewer workers, or none |
| block floor | `boundaries >= workers * 4` | Small queries stay serial |

The DF floor is *correct* and worth keeping: a conjunction is bounded by its
rarest term, so `common + rare` visits few candidates and finishes fast
serially. It is not a bug that `common + medium + rare` does not parallelise.

### What changed here

**One knob now governs both ranked paths.** `fts_query_partitions` replaced
the hard-coded `min(8)`. Its default is `min(8, cores)` — deliberately
identical to the constant it replaced, so unifying the controls changes no
existing deployment's behaviour. It only makes the behaviour configurable, and
makes one setting mean one thing.

**Requested versus granted parallelism is now reported.** Extra workers come
from a global permit pool, so a query on a busy node silently runs with fewer
workers. Previously an operator saw only "slow query" and had to infer
starvation; `ranked_workers_wanted` against `ranked_workers_granted` shows it:

```
ranked_queries_planned        queries that reached the parallel planner
ranked_queries_run_parallel   queries that got at least one extra worker
ranked_workers_wanted         extra workers requested
ranked_workers_granted        extra workers the pool actually gave
```

A large wanted/granted gap is contention, not slow code. That distinction is
the whole reason to measure it rather than infer it.

## Search-node profile

The defaults are tuned for a mixed OLTP database. A node serving a large
corpus wants different ones, and they should be written down rather than
rediscovered:

```rust
DbConfig::default()
    // Halves page bytes on text, which doubles effective cache. At corpus
    // scale this is the largest single lever available.
    .with_paged_value_compression(true)
    // Toward the core count on a dedicated search node; lower it for a
    // mixed workload, because extra workers come from a shared pool.
    .with_fts_query_partitions(16)
    // Never leave host-sized. Size against the working set, not the corpus.
    .with_paged_buffer_pool_bytes(192 * 1024 * 1024 * 1024)
```

And explicitly set the pgwire active-query and connection limits — the
host-sized defaults (24 active queries, 100 connections) have already cost one
production incident.

## What this does not fix

- The per-collection `RwLock` is still taken per access on the read path. For
  an immutable corpus the right shape is a snapshot handle acquired once per
  query.
- Document text and postings still share one buffer pool, so a scan can evict
  the term dictionary.
- BM25F still has no seeking path and is always exhaustive. Parallelising an
  exhaustive scan makes it faster, not bounded.
