# Production Observability

BicDB exposes an operator surface for transactional deployments through structured JSONL
logs, machine-readable metrics, health checks, slow-query logging, and sanitized
doctor bundles. These commands are intended for systemd units, container
probes, backup jobs, and incident collection.

## Metrics

Use JSON for automation:

```bash
cargo run -p bicdb-cli -- metrics ./erpdb --json
```

Use Prometheus text format when a sidecar or node exporter textfile collector
scrapes command output:

```bash
cargo run -p bicdb-cli -- metrics ./erpdb --prometheus > /var/lib/node_exporter/textfile_collector/bicdb.prom
```

Metrics are grouped by server, storage, planner, transaction, backup,
compaction, security, and replication. Live pgwire servers also expose
connection, queue, lock, write-admission, streaming, cancellation, and error
metrics through the `bicdb_server_metrics` SQL virtual table.

## Slow Queries

CLI SQL can write slow queries as JSONL:

```bash
cargo run -p bicdb-cli -- sql ./erpdb "SELECT 1" \
  --slow-query-log ./logs/slow-query.jsonl \
  --slow-query-threshold-ms 1000 \
  --redact-field ssn \
  --redact-field patient_name
```

Pgwire serving accepts the same controls:

```bash
cargo run -p bicdb-cli -- serve-pg ./erpdb \
  --slow-query-log ./logs/slow-query.jsonl \
  --slow-query-threshold-ms 1000 \
  --redact-field ssn
```

Bind parameters are redacted by default. Use `--redact-query-text` for
environments where SQL text can contain sensitive data or secrets and should never be
persisted.

## Health Checks

Use liveness when orchestration only needs to know that the database path is
present:

```bash
cargo run -p bicdb-cli -- health liveness ./erpdb --json
```

Use readiness before serving traffic:

```bash
cargo run -p bicdb-cli -- health readiness ./erpdb --json --max-size-bytes 107374182400
```

Readiness opens the database, runs integrity checks, checks HA readiness, and
fails when a compaction checkpoint is still present or the optional size limit
is exceeded. Non-ready status exits non-zero for systemd and container probes.

## Doctor Bundles

Collect a sanitized incident bundle:

```bash
cargo run -p bicdb-cli -- doctor ./erpdb --out ./bicdb-doctor.json
```

The bundle includes stats, metrics, health checks, manifest file presence, and
recommendations. It intentionally excludes record payloads, metadata values,
query text, secrets, encryption keys, and sensitive data. The database path is represented
by a stable hash.

## Dashboards

Production dashboards should include:

- Storage: database size, logical bytes, overhead bytes, collection count, row
  count, and growth rate.
- Query latency: p50/p95/p99 from client or pgwire measurements, slow-query log
  count, failed query count, timeout count, and cancellation count.
- Admission and locks: active and queued queries, read/write queue depth, queue
  wait p95/p99, DB lock wait and hold maxima.
- Backup: newest successful backup age, verify/drill status, artifact count,
  and restore drill RTO/RPO.
- Compaction: checkpoint presence, reclaimed bytes, duration, transaction log
  bytes, event log bytes, and sync log bytes.
- Replication: HA role, readiness, lag bytes, last apply time, and last apply
  error.
- Security: authentication failures, TLS/auth configuration, protected-data release-gate
  status, and slow-query redaction mode.

## Alert Thresholds

Start with these transactional-workload thresholds and tune after baseline load tests:

- Disk growth: warn at 75% filesystem usage or 20% day-over-day database growth;
  page at 85% usage or projected exhaustion within 24 hours.
- Compaction lag: warn when a compaction checkpoint remains for 30 minutes; page
  at 2 hours or when storage overhead exceeds 30% of database size.
- Backup failure: page when the last verified backup is older than the RPO
  target; warn when a drill has not completed in 24 hours.
- Replication lag: warn above 64 MiB or 5 minutes of apply lag; page above
  1 GiB or 15 minutes.
- Error rate: warn when failed queries exceed 1% over 5 minutes; page above 5%
  or any repeated corruption/integrity failure.
- Tail latency: warn when p95 query latency exceeds 2x baseline for 10 minutes;
  page when p99 exceeds the application SLO or slow-query volume doubles for 15 minutes.
- Queue depth: warn when query or write queues stay above 50% capacity for
  5 minutes; page when admission rejects or write timeouts occur.
