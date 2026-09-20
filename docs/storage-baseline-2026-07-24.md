# Storage baseline — `embedded_memory`, 2026-07-24

Phase 0 evidence for [`server-paged-storage-todo.md`](server-paged-storage-todo.md).
Every later gate in that roadmap is a bound on memory or open time, so these are
the numbers those gates are stated against.

Reproduce with:

```
cargo build --release -p bicdb-cli
./target/release/bicdb bench storage-baseline \
    --records 1000000 --metadata-bytes 1024 --batch-size 5000 \
    --json-out reports/storage-baseline/1gb.json
```

## Measurement conditions

| | |
| --- | --- |
| Commit | `fda7b3d` |
| CPU | AMD Ryzen 9 7900X, 12 cores / 24 threads |
| RAM | 62 GB (≈19 GB held by an unrelated tenant during the runs) |
| Filesystem | ext4 on RAID (`/dev/md0`) |
| Kernel | 6.8.0-136-generic |
| Toolchain | rustc 1.96.0, `--release` |
| Durability | `fsync = false` — these measure memory and open cost, not the durability path |
| Storage mode | `embedded_memory` |

Records are `record-{i:012}` with JSON metadata padded to the stated size. All
figures are single runs on a shared host; treat times as ±10% and the memory
figures as the reliable part.

**Not measured: 10 GB and the largest available fixture.** The host had another
process holding ~19 GB and the runs would have competed with it. The harness
takes those sizes unchanged — this is a scheduling gap, not a missing capability.

## Results

| Run | Records | Logical | Open | Accounted resident | Accounted/logical | Steady RSS | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 MB | 100 k × 1 KB | 98.4 MB | 0.40 s | 152.1 MB | 1.55× | 542 MB | 542 MB |
| 400 MB | 400 k × 1 KB | 393.8 MB | 1.67 s | 608.8 MB | 1.55× | 1.15 GB | 1.92 GB |
| 1 GB | 1 M × 1 KB | 984.7 MB | 4.93 s | 1.61 GB | 1.64× | 2.40 GB | 4.70 GB |
| 400 MB + 2 indexes | 400 k × 1 KB | 393.8 MB | 1.81 s | 617.4 MB | 1.57× | 973 MB | 1.95 GB |
| 200 k × 384-dim vectors | 200 k × 256 B | 350.4 MB | 2.48 s | 871.7 MB | 2.49× | 1.01 GB | 1.99 GB |

Category breakdown at 1 GB:

| Category | Bytes | Share of accounted |
| --- | ---: | ---: |
| rows | 1.218 GB | 75.5% |
| version chains | 220.6 MB | 13.7% |
| primary-key maps | 175.6 MB | 10.9% |
| secondary indexes | 0 | — |
| exact vectors | 0 | — |

## Findings

### 1. Open time is linear in total data — ~5 s per GB

0.40 s → 1.67 s → 4.93 s for 100 MB → 400 MB → 1 GB. That is 4.2× and 3.0× for
4× and 2.5× the data: linear, as expected from an open path that decodes every
current record and rebuilds every index.

Extrapolated (and labelled as extrapolation, not measurement): **1 TB would take
roughly 85 minutes to open**, before accounting for the fact that it would not
fit in memory at all. This is the number Phase 3's gate — "recovery time
correlates with the post-checkpoint WAL suffix, not total database bytes" —
exists to eliminate.

### 2. Resident memory is ~1.55× logical, and rows dominate

The accounted/logical ratio is strikingly stable at 1.55× across sizes, rising to
1.64× at 1 GB. Rows are three-quarters of it. This is the compact-JSON
representation working as designed — a `serde_json::Value` tree would be roughly
ten times the raw size — but 1.55× is still *above 1.0*, which is the only
threshold that matters for the roadmap's premise: while resident bytes exceed
logical bytes, the engine cannot hold a database larger than RAM by any margin.

Version chains at 13.7% and primary-key maps at 10.9% are both larger than they
look. Together they are a quarter of resident memory and neither is user data:
history that vacuum would reclaim, and an index from primary key to locator that
a page-backed engine would keep on disk.

### 3. Peak RSS is ~2× steady, and ~4.8× logical

At 1 GB: 4.70 GB peak against 2.40 GB steady. The build path roughly doubles the
footprint transiently. Any memory envelope derived only from steady-state
measurements will be wrong by a factor of two during ingestion — which is
precisely the condition Phase 9 requires testing under ("search latency during
ingestion", "concurrent ingestion").

### 4. Vectors are stored twice, and it is measurable

The 384-dimension run stores 200 k × 384 × 4 B = **307 MB** of raw vector data.
It appears twice:

- `exact vectors` = 414 MB (the denormalized `VectorStore`: vectors, norms, and
  record-id strings), and
- inside `rows` = 394 MB total, of which ~307 MB is the same vector data held on
  each `StoredRecord`.

Accounted/logical accordingly jumps from 1.55× to 2.49× as soon as vectors are
involved. This confirms the roadmap's Phase 6 item ("Stop duplicating every
vector in both `StoredRecord`, `VectorStore`, and HNSW nodes") with a number: on
a vector-heavy corpus, deduplicating alone would return roughly a third of
resident memory — before any paging. With an HNSW index built, the same vectors
would be resident a *third* time.

### 5. Index memory is cheap; do not over-prioritize Phase 4 for memory reasons

Two B-tree indexes over 400 k records cost 8.6 MB — 1.4% of accounted memory, or
about 10.8 bytes per entry per index. Phase 4 (persistent secondary indexes) is
worth doing for restart cost and for not rebuilding indexes from a corpus-wide
scan, **not** for memory. The memory argument for moving indexes to disk is
weak at this row width; it would strengthen with many indexes or narrow rows.

### 6. On-disk amplification is 1.54×, and it is all transaction log

`segment_bytes` is **zero** in every run. Freshly written records live in the
transaction log until a checkpoint folds them into segments, so a database that
has been built but not checkpointed carries its entire contents as WAL. A cold
open reads 1.54 bytes per logical byte, all of it log replay.

This matters for Phase 3 more than it first appears: the write-ahead log is not
a small suffix in this engine's normal state, it is the whole database until
maintenance runs. "Recovery proportional to the WAL suffix" presupposes
checkpointing that actually bounds the suffix.

## What these numbers do not cover

- **10 GB and above.** Not run here; see the conditions section.
- **Broker projections and query intermediates.** Not instrumented — the
  residency report names them in `not_instrumented` rather than reporting zero.
  Query intermediates in particular are the subject of Phase 5, and the memory
  envelope cannot be closed without them.
- **HNSW.** No HNSW index was built in these runs, so the third copy of each
  vector is inferred from the code, not measured here.
- **Concurrent read/write load.** Every run is a quiet build followed by a quiet
  reopen. Steady RSS under traffic is a different measurement.
- **The unaccounted gap.** Steady RSS exceeds accounted bytes by ~400 MB at
  100 MB of data and ~790 MB at 1 GB. Some is the 858 MB release binary's mapped
  pages and allocator arenas; how much is genuinely unexplained has not been
  chased down. Narrowing this is the honest remaining work in Phase 0's
  measurement item.
