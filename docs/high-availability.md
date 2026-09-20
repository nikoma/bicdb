# BicDB High Availability

> This document begins with the general/legacy server HA path. The hardened,
> one-Cell Phase-5 path is a separate capability boundary documented in
> [`cell-runtime-phase5.md`](cell-runtime-phase5.md). Do not use the commands
> below as substitutes for Cell writer epochs, replica leases, Cell-bound
> encryption, Recovery quorum, or regulated-workload admission.

BicDB uses a single-primary high availability model. One primary accepts writes.
That primary can execute and commit concurrent transactions; this topology does
not impose a single transaction writer. See [write concurrency](../TRANSACTIONS.md#write-concurrency).
Native ordered commit-stream replication is the preferred production direction;
the original whole-file shipping path remains supported as a legacy/fallback
bootstrap and repair mechanism.

Native Raft-style consensus support exists for ordered commit coordination.
It is separate from this legacy file-shipping HA path and does not make BicDB
multi-writer. Operators must still fence the old primary before promoting a
standby until the server daemon owns automatic failover end to end.

The 1.0.322-beta Cell runtime adds a separate, callable HA path: threshold
writer/replica authority is verified before Cell key release, primary commits
cross a locally revocable database fence, native commit objects are signed and
Cell-encrypted for one destination, and backup/restore authority is
Recovery-quorum scoped. Its keyless failover supervisor forbids route
publication before old-writer fencing and new-epoch activation. It currently
supports asynchronous replication with a declared commit RPO and
Auditor-measured drill evidence. It does not claim continuous RPO enforcement
at commit admission. A `SynchronousQuorum` policy is explicitly rejected
because zero-RPO quorum commit replication is not yet implemented.
Regulated-data admission remains denied.

Distributed range placement uses a different data-plane protocol: starting in
1.0.9-beta, row transactions are epoch-fenced and majority committed through
the per-range command log described in
`docs/distributed-cluster-operations.md`. Do not confuse that synchronous
range quorum with the legacy whole-database streaming/HA commands below.

## Architecture

- Primary: normal BicDB server or local database path with `role=primary`.
- Standby/read replica: copied BicDB path with `role=standby`; server writes
  are refused and local BicDB write APIs return a read-only standby error.
- Native streaming path: export committed transaction frames by `commit_seq` and
  apply them to a standby in order. See `docs/replication-streaming.md`.
- Shipping/apply fallback: `bicdb ha ship <primary> <standby>` opens and flushes
  the primary, copies durable catalog, segment, transaction, event, sync, index,
  planner-statistics, graph, vector-index, encryption, and server-user files,
  then writes standby HA metadata last.
- Replication mode: asynchronous. Re-run `bicdb ha ship` after primary writes
  or from an external supervisor/timer.
- Read replica: start pgwire on the standby path. `SELECT`/`SHOW` queries are
  served, while `INSERT`, `UPDATE`, `DELETE`, DDL, `COPY FROM`, and `COMMIT` of
  buffered write transactions are rejected.

## File-Shipping Commands

Create or refresh a standby:

```bash
cargo run -p bicdb-cli -- ha ship /srv/bicdb/primary /srv/bicdb/standby
```

Inspect lag and readiness:

```bash
cargo run -p bicdb-cli -- ha status /srv/bicdb/standby
cargo run -p bicdb-cli -- ha status /srv/bicdb/standby --json
```

Promote after fencing the old primary:

```bash
cargo run -p bicdb-cli -- ha promote /srv/bicdb/standby
```

If the standby reports non-zero lag or an apply error, promotion fails unless
`--force` is passed. Use `--force` only after manually verifying the old primary
cannot accept writes and the data loss window is acceptable.

## Health and Readiness

Offline status is available from `bicdb ha status`. A running pgwire server also
exposes:

```sql
SELECT * FROM bicdb_ha_status;
```

The status row includes `role`, `read_only`, `ready`,
`source_checkpoint_bytes`, `applied_checkpoint_bytes`, `lag_bytes`,
`last_apply_at`, `last_apply_error`, `promoted_at`, and
`last_durable_checkpoint_bytes`.

`bicdb server status` also prints the HA role, read-only state, lag bytes, and
durable checkpoint bytes for offline inspection.

## Failover Runbook

1. Fence the old primary at the process, host, volume, or load-balancer layer.
2. Run `bicdb ha status <standby>` and verify `ready=true`, `lag_bytes=0`, and
   no `last_apply_error`.
3. Promote with `bicdb ha promote <standby>`.
4. Start the pgwire server on the promoted path.
5. Repoint clients or the supervisor endpoint to the promoted server.
6. Rebuild the old primary only from the new primary; do not restart it as a
   writer with stale data.

For planned maintenance, run one final `bicdb ha ship` after stopping writes,
verify zero lag, promote the standby, and then move clients.

## Limitations

- Streaming replication is asynchronous whole-database commit-stream
  replication. The `consensus run` interface is retained for compatibility;
  distributed `bicdb serve` data safety comes from the synchronous per-range
  command log, not from asynchronous post-commit frame export.
- File shipping is async fallback replication, not streaming consensus.
- BicDB has a durable consensus state machine for leader election, but no
  automatic failover supervisor in `bicdb serve` yet.
- BicDB has no multi-writer mode.
- Operators are responsible for fencing to prevent split brain.
- The lag metric is byte/checkpoint based for the last completed ship, not a
  transaction LSN compatible with PostgreSQL.
- Shipping should run when no external process is writing directly to the
  standby path.
