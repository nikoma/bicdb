# BicDB Compaction

BicDB supports crash-safe online compaction checkpoints for record segments,
`transactions.log`, the event stream, the sync log, index metadata, and derived
sidecar accounting.

## Run

```bash
cargo run -p bicdb-cli -- compact ./testdb
cargo run -p bicdb-cli -- compact ./testdb --collection patients
```

For encrypted databases, pass the same key options used by `inspect` and
`verify`:

```bash
cargo run -p bicdb-cli -- compact ./encrypted-db --key "$BICDB_KEY"
```

Policy knobs:

```bash
cargo run -p bicdb-cli -- compact ./testdb \
  --reclaim-threshold-percent 20 \
  --max-io-bytes-per-sec 104857600 \
  --max-pause-ms 5000 \
  --schedule "daily 02:00" \
  --force
```

`--collection` rewrites only that collection segment and leaves transaction
history intact. Full database compaction coordinates all logs and writes
`compaction-checkpoint.json` with the checkpoint id and phase.

## Checkpoint Protocol

Full compaction runs under the `BicDb` mutable database handle. Server mode
therefore continues to serve reads while queued writes wait for the database
write lock; no live transaction may be pending when the checkpoint starts.

Phases:

- `started`: write a durable checkpoint manifest with selected collections and
  pre-compaction log sizes.
- `records_rewritten`: atomically replace each selected record segment with the
  current live records, then rebuild in-memory indexes, HNSW indexes, and graph
  projections.
- `logs_checkpointed`: after all selected record segments are durable, truncate
  `transactions.log`, rewrite the event stream with unique live events, and
  rewrite `sync.log` with pending sync operations only. The local sync export
  offset is reset to zero because event byte offsets may change; duplicate
  exports remain idempotent through event IDs.
- `committed`: persist catalog, index metadata, sync state, and final manifest.

If a process stops before `logs_checkpointed`, recovery opens from whichever
record segments were replaced and replays the old transaction log. Transaction
upserts and deletes are idempotent at the record-id level, so this returns to a
valid old-or-new state without duplicate records. If the stop happens after
`logs_checkpointed`, recovery opens from the compacted record snapshot and the
checkpointed auxiliary logs.

Temporary files use atomic rename. Startup ignores abandoned compact temporary
files because canonical paths are changed only by rename.

## Metrics

`compact` reports:

- bytes scanned, bytes before/after, and bytes reclaimed;
- live record count and dead record estimate;
- duration and pause time in milliseconds;
- checkpoint id and last error field;
- transaction, event, and sync log bytes before/after;
- index metadata bytes and derived sidecar bytes.

## 100GB Operations

For a 100GB ERP database, run compaction during a low-write window with an IO
cap sized below the storage device's sustained write bandwidth. Keep free disk
space above the largest collection segment plus event/sync log size because the
checkpoint writes replacement files before renaming them.

Use the ERP benchmark to capture amplification evidence:

```bash
cargo run --release -p bicdb-cli --features bench -- bench erp --profile 100gb \
  --path /data/bicdb-erp-100gb \
  --json-out reports/erp-100gb.json \
  --csv-out reports/erp-100gb.csv \
  --markdown-out reports/erp-100gb.md
```

The report includes database size before compaction, compacted database size,
disk amplification before/after compaction, bytes reclaimed, and compaction
duration.
