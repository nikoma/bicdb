# Index Maintenance

BicDB supports operational index maintenance through:

```bash
bicdb index verify <db> --all --json
bicdb index rebuild <db> --all --json
```

Both commands also accept `--csv`; without `--json` or `--csv`, they print a
human-readable report.

## Online Rebuild Contract

`bicdb index rebuild --all` rebuilds secondary B-tree and spatial R-tree
indexes from committed canonical records and verifies the replacement before it
reports success. Existing valid indexes remain available until the catalog swap
phase completes.

The rebuild report marks these bounded phases:

- `catalog-checkpoint`: writes durable maintenance status.
- `snapshot-scan`: builds replacement index state from committed records.
- `catalog-swap`: swaps the rebuilt index into the live catalog and persists the
  index definition catalog.

The current embedded API exposes `&mut BicDb` write access for index rebuilds,
so writes are serialized by the caller during the rebuild operation. Reads that
use an existing handle or snapshot can continue against the old committed record
state until the swap completes. Server deployments should document this as
write serialization during maintenance and bounded catalog phases for readers.

## Restart And Crash Safety

Maintenance status is persisted atomically in `index-maintenance.json`.
Interrupted rebuilds leave the old valid index untouched until a replacement is
fully built and swapped. A later rebuild sees the prior `building` status and
reports `interrupted_previous: true`; it safely restarts from canonical segment
records rather than trusting partial derived state.

Canonical segments remain authoritative. Index contents are derived in memory,
and `indexes.json` plus `index-maintenance.json` are metadata sidecars. If a
process crashes before the swap, reopening the database rebuilds indexes from
canonical records and the next maintenance run can repair or refresh the
sidecar status.

### Bulk GIN builds in `server_paged`

`CREATE INDEX ... USING GIN` over an existing full-text corpus uses a bounded,
external-memory build:

- rows are read and tokenized in bounded primary-key batches;
- tokenization and posting-run generation use
  `BICDB_FTS_BUILD_WORKERS` workers (the default is available CPU
  parallelism, capped at 64);
- each worker spills both primary-key and impact-ordered posting runs under
  `fts-index-builds/`;
- the global in-process posting budget is
  `BICDB_FTS_BUILD_MEMORY_BYTES` (default 256 MiB), divided across workers;
- fan-in merge levels use at most eight concurrent merge workers to avoid
  unbounded file descriptors and I/O queue depth;
- checkpoints advance through tokenization, primary-key merge, impact merge,
  and publication.

Completed `.run` files survive interruption and a repeated identical
`CREATE INDEX` resumes from the checkpoint. Torn `.tmp` files are removed when
the workspace is reopened. `DROP INDEX` explicitly abandons any unpublished
build or replacement generation and reclaims its physical generation.

Final posting blocks are written under an immutable physical generation. The
logical index name continues resolving to the prior valid generation until both
posting orders and the completeness sentinel are durable. Publication is the
atomic rewrite of `fts-index-generations.json`; only after that swap can old
generation data be reclaimed.

Full-text lexemes are limited to 2,046 UTF-8 bytes. Tokenization, query
construction, incremental index maintenance, and resumed external builds all
skip longer pathological tokens without truncating or exposing their contents
in logs. Normal identifiers at or below the boundary remain indexed. BicDB
emits at most four per-process warnings plus one suppression notice, and
`full_text_oversized_terms_skipped()` exposes the monotonic skipped-term count
for metrics.

The complete generation-format implementation checklist, query-time memory
controls, compatibility behavior, and migration procedure are documented in
[FTS Generation Format v3](fts-generation-v3.md).

## Verification Report

`bicdb index verify --all --json` reports, per index:

- `indexed_records` and `expected_records`
- `missing_entries`, `stale_entries`, `duplicate_entries`, and `wrong_entries`
- `size_bytes`
- `last_verified_unix_ms`
- `stale`, `corrupt`, and `valid` status

The command covers B-tree indexes over primary ids, timestamps, composite
metadata/JSON paths, and spatial R-tree indexes where those index definitions
exist. It also includes HNSW vector ANN reports in `vector_reports`.

## Large-database operations

For large transactional databases, run verify after bulk imports, sync imports, backup
restore drills, and planned rebuild windows:

```bash
bicdb index verify /data/application.bicdb --all --json > reports/index-verify.json
bicdb index rebuild /data/application.bicdb --all --json > reports/index-rebuild.json
```

The large fixture benchmark is `bicdb bench indexes` in a CLI built with
`--features bench`; use its JSON/CSV
outputs to publish build/rebuild time and memory evidence for the certification
gate.
