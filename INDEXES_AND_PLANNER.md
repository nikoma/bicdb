# Indexes And Planner

BicDB Phase 10 adds practical secondary indexes and a small cost-estimated SQL
planner. The goal is fast common local-app queries without turning BicDB into a
full PostgreSQL optimizer.

## Storage Model

Canonical BicDB records remain append-only segment data. Index metadata is
persisted in `indexes.json`; index contents are rebuildable in-memory B-tree
maps derived from canonical records at open time.

This keeps recovery simple:

- `indexes.json` records index definitions.
- `planner-stats.json` records ANALYZE output for row-count and selectivity
  estimates.
- Segment files remain authoritative.
- `db.rebuild_index(name)` rebuilds one index from records.
- `db.verify_index(name)` compares the current index state against canonical
  records.

## Supported Indexes

SQL:

```sql
CREATE INDEX idx_patients_age ON patients(age);
CREATE INDEX idx_patients_email ON patients(email);
CREATE INDEX idx_wearable_device_metric ON wearable(device_id, metric);
CREATE INDEX idx_wearable_timestamp ON wearable(timestamp);
CREATE INDEX idx_metadata_clinic ON patients((metadata->>'clinic'));
CREATE SPATIAL INDEX idx_places_geometry ON places(geometry);
DROP INDEX idx_patients_age;
```

Rust:

```rust
use bicdb_core::{BicDb, IndexDefinition, IndexField, IndexKind};

let mut db = BicDb::open("./testdb")?;
db.create_index(IndexDefinition {
    name: "idx_wearable_device_metric".to_string(),
    collection: "wearable".to_string(),
    fields: vec![
        IndexField::MetadataPath(vec!["device_id".to_string()]),
        IndexField::MetadataPath(vec!["metric".to_string()]),
    ],
    unique: false,
    kind: IndexKind::BTree,
})?;

let report = db.verify_index("idx_wearable_device_metric")?;
assert!(report.valid);

# Ok::<(), bicdb_core::BicDbError>(())
```

Indexable fields:

- `id`
- `timestamp`
- metadata columns such as `age`, `email`, `device_id`, `metric`
- metadata paths such as `metadata.clinic`
- JSON text/path expressions rooted at metadata, such as `metadata->>'clinic'`
- spatial indexes accept one geometry field, either canonical `geometry` or a
  metadata field containing WKT text or a GeoJSON Geometry/Feature object

## Planner

`EXPLAIN` returns the chosen physical plan plus the planner's local estimates:

```sql
EXPLAIN SELECT *
FROM wearable
WHERE metadata.device_id = 'band-1' AND metadata.metric = 'hrv';
```

Example output:

```text
IndexScan idx_wearable_device_metric
PrefixKeys 2
EstimatedRows 128
Cost 13.800
```

Current candidate plans:

- `PrimaryKeyLookup` for `WHERE id = '...'`
- `IndexScan` for indexed equality predicates
- `IndexRangeScan` for single-field range predicates
- `IndexScan` with prefix keys for composite indexes
- `OrderedIndexScan` for indexed `ORDER BY timestamp DESC LIMIT n`
- `SpatialIndexScan` for supported `ST_DWithin(field, ST_Point(...), meters)`
  and `ST_Intersects(field, ST_Envelope(...))` predicates
- `FullScan` fallback when no supported index rule applies

The planner enumerates supported candidates, estimates matching rows from the
current collection/index cardinality and persisted `ANALYZE` statistics,
assigns a small local cost, and chooses the lowest-cost candidate. Indexed
equality and range plans can lose to a full scan when statistics show the
predicate is nonselective.

Statistics include table row counts, per-field null counts, distinct counts,
min/max values, and a small most-common-value distribution for core fields,
top-level metadata columns, metadata/json paths used by indexes, and timestamp
ranges. B-tree and spatial index summaries record indexed rows and distinct
key/envelope counts. This is intentionally conservative; it is not a
PostgreSQL-style optimizer with arbitrary-expression histograms.

The planner always applies the original SQL predicate after loading candidate
records. This keeps results correct when an index range or R-tree envelope is
broader than the exact predicate, for example `>` versus `>=` or an intersecting
bounding box candidate that the exact spatial predicate rejects.

Two-table inner joins are reordered deterministically when persisted statistics
show the right-hand table is smaller than the left-hand table. Outer joins and
larger join graphs keep SQL order to preserve semantics.

## ANALYZE And EXPLAIN ANALYZE

Refresh statistics after a bulk load, large delete, major tenant import, or
data skew change:

```sql
ANALYZE TABLE invoice_lines;
```

The refreshed stats persist in `planner-stats.json` and are loaded on reopen.
Until `ANALYZE TABLE ...` is run again, plans remain based on the previous
distribution; this gives ERP report plans stable behavior across data skew
changes and makes refresh timing explicit.

`EXPLAIN ANALYZE` runs supported SELECT plans and reports estimated rows,
actual rows, and elapsed node time:

```sql
EXPLAIN ANALYZE
SELECT id FROM invoice_lines WHERE cost_center = 'ops';
```

Example output:

```text
IndexScan idx_invoice_lines_cost_center
PrefixKeys 1
EstimatedRows 42
Cost 70.400
ActualRows 42
ActualTimeMs 0.381
```

## Maintenance

Indexes are updated or rebuilt after:

- direct `insert`, `batch_insert`, and `delete`
- committed transaction writes
- rollback, by doing nothing because uncommitted writes never enter indexes
- sync import upserts/deletes
- backup restore and database reopen, by rebuilding from canonical records

Spatial R-tree contents are rebuilt from canonical records — except when a
packed base has been published via `pack_spatial_index` (`server_paged` only):
then the immutable Hilbert/STR-packed node tree lives in the durable index
keyspace, reopen loads its meta plus a durable delta tail, and only the delta
is resident (see `docs/SPATIAL.md`, "Packed Spatial Indexes").

MVCC snapshots remain record-version based. Indexes accelerate current committed
state queries; snapshot scans still use the snapshot's visible record map.

Operational maintenance commands are available for large databases:

```bash
bicdb index verify <db> --all --json
bicdb index rebuild <db> --all --json
```

`index verify --all` emits machine-readable reports for secondary B-tree,
metadata/JSON, timestamp, spatial R-tree, and HNSW vector indexes that exist in
the database. Verification compares exact canonical entries and reports missing,
stale, duplicate, and wrong entries.

`index rebuild --all` builds replacement index state from committed canonical
records, leaves the previous valid index live until the bounded catalog swap
phase, persists restart status in `index-maintenance.json`, and verifies the
replacement before reporting success. Interrupted rebuilds safely restart from
canonical records; partial derived state is not trusted.

See [docs/index-maintenance.md](docs/index-maintenance.md) for the operational
contract and ERP guidance.

## Benchmarks

Run:

```bash
cargo run -p bicdb-cli --features bench -- bench indexes --records 1000000 \
  --json-out reports/indexes.json \
  --csv-out reports/indexes.csv
```

The benchmark reports:

- point lookup by indexed field
- timestamp range scan
- metadata equality
- composite lookup
- indexed `ORDER BY timestamp DESC LIMIT 10`
- index build time
- index rebuild time
- index catalog size
- peak derived-index memory estimate
- write overhead with indexes
- full scan versus index scan timing

Development-machine 100k smoke run:

```text
Records: 100000
Index build: 685.233ms
Point lookup: scan 349.868ms / index 1.781ms
Timestamp range: scan 287.086ms / index 45.548ms
Metadata equality: scan 322.936ms / index 6.361ms
Composite lookup: scan 325.892ms / index 1.972ms
ORDER BY timestamp DESC LIMIT 10: scan 312.692ms / index 0.085ms
```

Artifacts from that run are in `reports/indexes-100k.json` and
`reports/indexes-100k.csv`.

## Limitations

- The planner has basic local cost estimates, not a full PostgreSQL optimizer.
- Join reordering is limited to deterministic two-table inner joins with
  persisted table statistics.
- Statistics refresh is explicit; write paths do not auto-refresh
  `planner-stats.json`.
- No partial indexes.
- No executable expression indexes beyond metadata/JSON path forms listed
  above.
- WalkNorth-style PostgreSQL full-text `GIN` expression indexes using
  `setweight(to_tsvector('english', COALESCE(column, '')), weight)` terms joined
  with `||` are accepted as metadata-only compatibility entries. They are
  visible in practical `pg_catalog` index introspection, but they are not used
  for PostgreSQL full-text search, ranking, stemming, or `tsquery` execution.
- Index contents are in-memory and rebuilt from persisted definitions and
  records on open.
- Unique indexes are validated during build, but SQL does not yet provide full
  PostgreSQL-style constraint behavior.
- Index updates are currently coarse-grained rebuilds for affected collections;
  incremental per-key maintenance is a v0.2 optimization target.
