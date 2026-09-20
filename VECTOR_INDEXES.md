# BicDB Vector Indexes

BicDB supports two vector search paths:

- Exact search scans the canonical vector store and returns exact top-k results.
- ANN search uses an optional persistent HNSW index and trades recall for lower
  latency on larger vector collections.

Exact search remains the default. HNSW is opt-in per collection.

## Rust API

```rust
use bicdb_core::{BicDb, HnswIndexConfig, VectorMetric};

let mut db = BicDb::open("./agentdb")?;

db.create_vector_index(
    "memories",
    HnswIndexConfig {
        m: 16,
        ef_construction: 100,
        ef_search: 50,
        distance: VectorMetric::Cosine,
    },
)?;

let exact = db.search_vector_exact("memories", &[0.1, 0.2, 0.3], 10)?;
let ann = db.search_vector_ann("memories", &[0.1, 0.2, 0.3], 10, 100)?;

# Ok::<(), bicdb_core::BicDbError>(())
```

`HnswIndexConfig::default()` uses cosine distance, `m = 16`,
`ef_construction = 100`, and `ef_search = 50`.

Supported metrics:

- `VectorMetric::Cosine`
- `VectorMetric::Dot`
- `VectorMetric::L2`

The persisted HNSW sidecar stores graph links, configuration, record IDs, and
tombstones. Vector payloads remain canonical BicDB records and are rehydrated
from the database on open.

## CLI

Build or rebuild an index:

```bash
bicdb vector index build ./db --collection memories
bicdb vector index build ./db --collection memories --metric cosine --m 16 --ef-construction 100 --ef-search 50
bicdb vector index rebuild ./db --collection memories
```

Verify an index:

```bash
bicdb vector index verify ./db --collection memories
```

Run the ANN benchmark:

```bash
bicdb bench ann --records 1000000 --dim 768 --top-k 10
```

The benchmark compares exact search with HNSW `ef_search = 20`, `50`, and
`100`, and reports p50/p95/p99 latency, recall@k, index build time, sidecar
size, and memory estimate. Use `--json-out` and `--csv-out` for export.

## SQL

The SQL engine supports pgvector-style nearest-neighbor ordering:

```sql
SELECT *
FROM memories
ORDER BY embedding <=> '[0.1,0.2,0.3]'
LIMIT 10;
```

Operators:

- `<=>` cosine distance
- `<#>` negative inner product
- `<->` L2 distance

SQL uses exact vector ordering by default. In session-based SQL and pgwire
connections, opt into ANN:

```sql
SET bicdb.vector_search = 'ann';
SET bicdb.ef_search = 100;

SELECT *
FROM memories
ORDER BY embedding <=> '[0.1,0.2,0.3]'
LIMIT 10;
```

ANN routing is intentionally conservative in v0.1. It activates only when:

- `bicdb.vector_search = 'ann'`
- the collection has a vector index
- the query has one vector `ORDER BY`
- the order direction is ascending or omitted
- the query has a `LIMIT`
- there is no `WHERE` filter or aggregate

Other vector queries fall back to the exact SQL path.

## Updates

For v0.1:

- New vector inserts are indexed when a vector index exists.
- Vector upserts rebuild the collection HNSW index conservatively.
- Deletes tombstone HNSW nodes and exclude them from search.
- `rebuild_vector_index` compacts away tombstones and repairs graph state.
- Sync imports and transaction commits refresh/tombstone affected indexes.

## Tradeoffs

Higher `ef_search` usually improves recall and increases latency. Higher
`ef_construction` usually improves graph quality and increases build time.
Higher `m` increases graph connectivity, memory usage, and index size.

Use exact search when:

- the collection is small
- recall must be exact
- queries need filters that ANN does not support yet
- the vector index metric does not match the query metric

Use ANN when:

- the collection is large
- low latency matters more than perfect recall
- the query is a straightforward nearest-neighbor lookup
- recall can be measured against exact search for the workload

## Local Benchmark Sample

Development-mode sample on this machine, run with:

```bash
cargo run -p bicdb-cli --features bench -- bench ann --records 1000 --dim 32 --top-k 10 --path /tmp/bicdb-ann-doc-bench
```

Results:

| Search path | p50 | p95 | p99 | recall@10 |
| --- | ---: | ---: | ---: | ---: |
| Exact | 1.557 ms | 1.649 ms | 1.649 ms | 1.0000 |
| HNSW ef=20 | 0.327 ms | 0.381 ms | 0.381 ms | 0.7800 |
| HNSW ef=50 | 0.621 ms | 0.681 ms | 0.681 ms | 0.7800 |
| HNSW ef=100 | 1.306 ms | 1.343 ms | 1.343 ms | 0.7800 |

Index build time was 1443.603 ms for 1,000 vectors, sidecar size was
533.34 KiB, and the memory estimate was 330.83 KiB. Release-mode numbers should
be captured for real regressions.

## Limitations

- Filtered ANN is not implemented yet.
- Incremental HNSW update quality is conservative; rebuild after heavy churn.
- Deleted vectors remain as tombstoned nodes until rebuild.
- Sidecars are derived and can be rebuilt from canonical BicDB records.
- Exact search remains the correctness oracle for recall benchmarking.
