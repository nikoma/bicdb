# Continuous verification, regional reads and workload protection

These services are opt-in except for the live-query circuit breaker. Existing
replication TLS authentication, durable resume positions, snapshot bootstrap,
election fencing, and server admission continue to apply.

## Scheduled verification

```sh
export BICDB_BACKUP_KEY='your existing archive backup key'
bicdb serve ./data --log-archive ./archive \
  --log-archive-key-env BICDB_BACKUP_KEY \
  --verify-interval-secs 3600 --verify-scratch /scratch/bicdb \
  --metrics-listen 127.0.0.1:9091
```

Each primary drill pins an MVCC snapshot, streams a SHA-256 hash of every stored
row into a per-table checksum, verifies each executable index against its table,
archives through that snapshot, restores the appropriate base plus the exact
commit suffix into a unique scratch directory, and compares every table. It
then exercises persisted native standby promotion, writes a probe, closes and
reopens the promoted scratch database, and checks the acknowledged probe.
Scratch directories are removed on success and on ordinary error returns.
An abruptly killed verifier can leave a `bicdb-verify-*` directory for inspection.

The promotion drill tests local promotion and recovery mechanics. The automated
three-process HA tests separately exercise elections, primary death, planned
handoff, and rejoin; the scheduled drill does not kill production nodes or test
your provider's routing and network partitions.

The archive worker runs its own drill, excluding archive append and pruning
while restore reads those files. Writes continue and the WAL remains retained.
The next interval starts after the previous run finishes. Without an archive,
verification checks local tables and indexes and reports `restore_and_failover =
false`. With an archive configured, scheduled verification requires its base
backup key. Standbys run local integrity checks and consume the primary's
verification manifests; they do not attempt to restore their own archive.

Configure verification on both the primary and standbys to enable:

- Actual replica post-image checks against each imported commit before ACK.
- Periodic full-table checksum comparisons at the same applied commit, including
  empty tables, internal catalogs, payloads, vectors, geometries, and deletes.
  A manifest waits for its matching commit even when a replica is behind.
  A divergent replica retains a durable quarantine across restart and cannot
  resume replication or promote until a fresh snapshot is installed.

Table hashing verifies referenced attachment blobs as well as row values. The
current row commit stream does not transport external attachment files:
attachment mutations are refused while native replication or log archiving is
enabled, and a replica refuses a row referencing a missing blob. Seed existing
attachments with a full snapshot. This prevents acknowledging a row whose blob
would be absent after failover or PITR; streaming new attachment files is a
separate capability, not silently assumed to work.

Table hashing streams one row at a time. Commits pause briefly while the MVCC
snapshot is pinned. Index checks hold commit admission for one index at a time
so a table and its index cannot describe different commits; existing index
verification can allocate an expected index. Size and schedule this work for
your corpus. Initial verification and full comparisons are proportional to data
size; commit post-image verification is proportional to changed rows. These are
integrity checks, not a repair mechanism or a cryptographic proof of absence of
all storage or network faults.

A verification quarantine persists across restart and blocks candidacy and
promotion. Stop that standby and stage a known-good base backup from the same
primary cluster, preserving its voting identity:

```sh
bicdb ha reseed ./standby --backup ./archive/base-00000000000000123456.bicbackup \
  --key-env BICDB_BACKUP_KEY
```

Restart with that node's existing replication settings. Bootstrap installs the
verified archive, clears the quarantine, and resumes from its durable prefix.
Keep enough source WAL for catch-up, or use a newer base. The reseed command
refuses an open target; it does not clear quarantine on the old data in place.

For an offline one-shot check against a source directory:

```sh
bicdb verify ./data --archive ./archive \
  --archive-key-env BICDB_BACKUP_KEY --scratch /scratch/bicdb --json
```

Use the server to query `SELECT * FROM bicdb_verify`. It retains the last 64
completed local results in memory; restart clears them. Error details are
visible only to administrators. Prometheus exposes:

- `bicdb_verify_enabled`
- `bicdb_verify_last_success` (zero before the first completed run; success
  requires the latest result for each check kind and a healthy replica)
- `bicdb_verify_last_completed_timestamp_seconds`
- `bicdb_verify_last_restore_and_failover`

Failures also produce a structured `verification.failed` event. Connect the
metrics to your existing pager, for example:

```yaml
- alert: BicDBVerificationFailedOrStale
  expr: >-
    bicdb_verify_enabled == 1 and
    (bicdb_verify_last_success == 0 or
     time() - bicdb_verify_last_completed_timestamp_seconds > 7200)
  for: 5m
  labels:
    severity: page
```

Choose the stale threshold from your configured cadence plus measured drill
runtime. A separate alert on `last_restore_and_failover == 0` should apply only
to primary nodes expected to run archive drills. This repository supplies the
metrics and alert rule; configure the notification receiver in your monitoring
system.

## Region-local reads and planned handoff

Run an asynchronous standby in the remote region using the existing mTLS
replication settings (`--standby-of`, certificate identity pinning, cluster/node
IDs, and the failover peer list). Label nodes with `--region eu-west`, and connect
read clients to the SQL address in their own region:

```sql
SET bicdb.max_lag = '2s';
SELECT * FROM posts WHERE id = 123;
SELECT * FROM bicdb_region;
```

A standby allows a bounded read only after a complete, caught-up heartbeat. The
proof uses the standby's monotonic time from the start of that replication
request cycle; WAN transit and apply time count against the bound. It does not
compare machine clocks or use the age of the last write, so an idle primary is
not mistaken for a stale replica. Unknown, lagging or disconnected histories
fail with SQLSTATE `53300` when the bound cannot be proved. A missing setting
preserves unrestricted standby reads; `RESET bicdb.max_lag` removes the bound.
Values accept seconds, `ms`, `s`, and `min`.

Bounds apply at statement start. Fixed-snapshot transactions and suspended
extended-protocol portals retain their original freshness watermark, so fresh
heartbeats cannot refresh an old result. Rollback remains available when stale.
Use a new transaction/query when a retained result exceeds its bound.

Ordinary replication reconnects from the standby's durable applied prefix,
including after a process restart. Duplicate frames remain idempotent; gaps and
wrong epochs are refused. WAN batches are bounded by commits and bytes. A node
behind retained WAL uses snapshot bootstrap. Snapshot transfer itself restarts
on disconnect; byte-offset resume of a partially transferred snapshot is not
implemented.

A planned zero-loss handoff requires a failover group, an allowed standby target,
and fsync enabled. Through authenticated SQL endpoints (or local SSH tunnels for
the CLI's existing PostgreSQL connection transport):

```sh
bicdb ha switchover --primary 127.0.0.1:15433 \
  --target 127.0.0.1:25433 --target-node eu-standby \
  --user postgres --password-env BICDB_OPERATOR_PASSWORD --timeout-secs 120 --json
```

The primary drains in-flight commit application, flushes its final prefix, and
durably refuses subsequent writes before sending an authenticated handoff to
the named standby. The target accepts only its exact final commit, source
history and reserved newer term. Promotion persists across restart. The CLI
waits until the target reports itself primary with that epoch and prefix; the
normal group machinery fences and rejoins the old primary. The standalone SQL
control is `SELECT bicdb_ha_switchover('eu-standby')`, restricted to administrators
on authenticated servers and refused inside transactions and delegated sessions.

If connectivity fails, the source stays drained across restart. The command
reports failure rather than silently undoing the drain; restore connectivity
and retry the same target. A forced promotion after an unplanned regional loss
retains the asynchronous group's ordinary RPO: planned zero-loss handoff does
not make asynchronous disaster failover zero-loss.

New verification/handoff frames require matching server versions. Older senders
cannot attest bounded-read freshness. Mixed-version rolling upgrade validation
is outside this change.

## Role and tenant budgets

Pass a JSON file with `--workload-limits ./workload.json`:

```json
{
  "roles": {
    "app": {
      "max_concurrent_queries": 8,
      "memory_bytes": 268435456,
      "query_timeout_ms": 2000,
      "max_estimated_rows": 1000000,
      "cost_gate_active_queries": 16
    }
  },
  "tenants": {
    "tenant-a": { "max_concurrent_queries": 4, "memory_bytes": 134217728 }
  },
  "live_evaluation_timeout_ms": 250,
  "live_max_changes_per_round": 5000
}
```

`roles` use the authenticated login identity; `SET ROLE` does not evade the cap.
`tenants` use the host-verified security context, including verified delegation.
A `"*"` policy supplies a default for identities without an exact policy.
Both role and tenant policies must admit a query. Limits of zero disable the
corresponding cap; live evaluation duration always has a positive minimum.
Unknown JSON keys are rejected to avoid silently ignored operator typos.

Concurrency admission precedes the global query queue. Memory is an aggregate
cooperative envelope: active query reservations plus retained named-subscription
results are charged to the role/tenant. Each query receives a share of the
remaining envelope, bounded by configured concurrency. Scans check retained-row
estimates while materializing, SQL materialization/join checkpoints check their
working sets, and existing bounded sort operators adopt the tighter work-memory
limit. Oversized results fail with SQLSTATE `53200`. This estimates instrumented
working memory; it is not a process allocator or RSS limit, and external/native
allocations are not all instrumented. Use process/cgroup limits for a hard host
memory boundary.

The cost gate rejects with SQLSTATE `53300` before execution. Its conservative
estimate multiplies referenced relation cardinalities, including join fan-out;
it is not a selectivity-aware optimizer estimate and can reject indexed queries
on large tables under load. `cost_gate_active_queries` controls when it applies.
Time limits are cooperative statement deadlines and also apply to Describe
paths that must evaluate SQL. Rollback bypasses role admission for cleanup.

The live dispatcher never waits in the foreground queue. Evaluations have a
per-round deadline (default 1000 ms), a change-count budget (default 10,000),
and retained-result memory accounting. Breaching a budget stops that
subscription and sends an explicit resubscribe error. A full notification
backlog invalidates that connection's subscriptions with `resubscribe_all`,
rather than dropping result deltas silently. Notifications for named
subscriptions go only to their owning connection, even if another session uses
the same subscription name or issues `LISTEN` on that name.

`SELECT * FROM bicdb_activity` exposes the existing connection activity view,
with its existing cross-user redaction. `SELECT * FROM bicdb_workload` adds
per-identity active queries, reserved/retained memory, admissions, refusals and
observed query memory. Non-administrators see their own role only. Prometheus
adds per-identity admission/memory/refusal metrics and
`bicdb_live_breaker_trips_total`.

SQL catalog verification also checks type, relation, namespace and constraint
identifier uniqueness. This catches legacy logical aliases even when the same
metadata bytes exist on a standby or restore. New table/view type identities
are durably reserved. A reported old collision requires explicit repair; the
verifier never guesses a reassignment or executes user-defined SQL.
