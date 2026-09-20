# Bounded server-paged checkpoint maintenance

Status: bounded state machine and supervisor shipped in BicDB 1.0.75-beta;
automatic server hosting shipped in BicDB 1.0.76-beta; duration and WAL-byte
accounting shipped in BicDB 1.0.80-beta.

Server-paged checkpoints no longer require a threshold-crossing commit to flush
every dirty frame. `PagedStore::checkpoint_step` advances one strict phase, and
the BicDB checkpoint supervisor durably schedules those phases through the
background compaction resource lane.

## State machine

The inclusive `PagedCheckpointCursor` has four phases:

1. `drain` writes one cursor-bounded dirty-page window without taking the
   store's writer gate;
2. `freeze` advances through a bounded number of contiguous committed
   transaction statuses without allocating a complete status list;
3. `finalize` takes the writer gate, rechecks freeze progress, drains at most one
   bounded window, stages the current roots and visibility watermark, and
   publishes only after observing zero dirty pages; and
4. `complete` is terminal and cannot be advanced.

The final meta-page write creates exactly one new dirty page under the writer
gate. That page crosses the normal WAL-before-page barrier and is synced before
the durable checkpoint record. WAL truncation occurs only when no committed
transaction remains above the persisted freeze watermark. Tail reclamation is
attempted only after successful WAL truncation and retains its independent page
and logical-I/O bounds.

The transaction table maintains an active-XID set separately from retained
commit status. Finding the oldest active snapshot is therefore allocation-free
and logarithmic instead of materializing or scanning the complete status table.

A pinned dirty page is skipped and retained in the dirty index. Reaching the end
of a pass with dirty pages remaining resets the inclusive writeback cursor for a
later pass. No phase forgets a pinned or newly dirtied page.

## Resource envelope

`PagedCheckpointLimits` fixes, before work begins:

- the writeback candidate, logical-I/O, and cooperative-duration limits;
- the maximum transaction statuses frozen by one call; and
- the tail-reclamation page and logical-I/O limits.

`PagedCheckpointScheduleLimits` adds step intervals, saturation delay,
exponential failure backoff, failure and state-size ceilings, and the complete
memory/I/O/CPU reservation. One admitted step cannot exceed the declared
writeback candidate memory or combined finalization I/O envelope.

The WAL threshold path uses the same default bounds and advances exactly one
phase per threshold-crossing commit. It no longer calls an all-buffer flush.
The durable supervisor is the production control plane that continues work
without relying on subsequent foreground commits.

## Durable supervision

The state lives at:

```text
maintenance/paged/store-identity.json
maintenance/paged/checkpoint.json
```

The schedule contains the immutable store ID, operation ID, page size, limits,
inclusive cursor, bounded last report, cumulative totals, due time, retry state,
pause state, monotonic state sequence, and SHA-256 checksum. It is size-bounded,
opened without following symlinks, verified completely, and replaced atomically.

Schedule format v2 adds cumulative active-step nanoseconds and terminal WAL
input bytes. A format-v1 schedule is parsed through its exact strict legacy
schema, verified against its original checksum, and migrated in memory without
changing its cursor, operation identity, accepted counters, retry state, or
pause state. The next state publication writes format v2 atomically. Timing and
WAL-byte counters start at zero for pre-upgrade work because those facts were
not recorded by format v1; BicDB never fabricates historical telemetry.

A crash after page work but before schedule publication repeats the previous
inclusive phase. Page writes, freeze advancement, checkpoint publication, WAL
reset, and tail reclamation are safe to repeat. A stale worker cannot advance a
replacement operation because every mutating API requires the exact operation
UUID.

Public APIs are:

- `start_paged_checkpoint_maintenance`;
- `paged_checkpoint_maintenance_status`;
- `tick_paged_checkpoint_maintenance`;
- `pause_paged_checkpoint_maintenance`; and
- `resume_paged_checkpoint_maintenance`.

Starting a second operation while one is active fails. Saturation publishes a
bounded retry time before touching a page. Repeated failures use capped
exponential backoff and eventually pause rather than hot-looping.

## Automatic server hosting

`bicdb serve` and every database opened by the cluster server now start one
automatic checkpoint driver in `server_paged` mode. The driver:

1. loads and fully verifies an existing durable schedule;
2. resumes the exact active operation ID and its persisted limits before
   considering new work;
3. preserves operator and automatic pauses without replacing the operation;
4. starts a new schedule when WAL exceeds its configured trigger or when the
   periodic checkpoint cadence finds WAL or dirty pages;
5. advances only one due supervisor step per host tick; and
6. stops when the server shutdown flag is set.

The host retains only periodic and pressure-retry deadlines. Losing those two
deadlines can cause an earlier check after process restart, but the durable
operation cursor, accepted totals, retry state, and pause state remain the
authority. A completed checkpoint that must retain WAL is rate-limited before a
new generation can start, preventing a permanently blocked visibility
watermark from causing a hot loop.

`PgWireConfig` makes the hosting policy explicit:

- `automatic_paged_checkpoint` enables or disables the host driver;
- `automatic_paged_checkpoint_poll_interval` is bounded to 1 ms through 60 s;
- `checkpoint_interval` is the new-operation cadence and is bounded to 1 ms
  through 24 hours;
- `automatic_paged_checkpoint_limits` fixes all page work and declared demand;
  and
- `resource_governor` defines the node envelope and lane limits.

Every database hosted by one cluster server shares the same node-owned resource
governor. Checkpoint and range-repair workers therefore cannot multiply the
background allowance either by task type or by opening more databases. New
checkpoint demand is validated against the compaction lane before the server
opens. A resumed schedule is validated against the current hard lane before it
can touch pages; an impossible restored demand remains intact and fails closed
instead of being misclassified as transient saturation forever.

Host-side failures are logged at power-of-two intervals and receive capped
exponential backoff. Durable page-step failures continue to use the schedule's
own persisted retry and automatic-pause policy. Embedded-memory databases do
not create maintenance state.

## Observability

Scrape metrics expose only fixed-cardinality state:

```text
bicdb_compaction_paged_checkpoint_phase
bicdb_compaction_paged_checkpoint_active
bicdb_compaction_paged_checkpoint_paused
bicdb_compaction_paged_checkpoint_completed
bicdb_compaction_paged_checkpoint_state_sequence
bicdb_compaction_paged_checkpoint_steps_total
bicdb_compaction_paged_checkpoint_pages_written_total
bicdb_compaction_paged_checkpoint_logical_writeback_bytes_total
bicdb_compaction_paged_checkpoint_active_duration_nanos_total
bicdb_compaction_paged_checkpoint_wall_duration_millis
bicdb_compaction_paged_checkpoint_transactions_frozen_total
bicdb_compaction_paged_checkpoint_wal_truncations_total
bicdb_compaction_paged_checkpoint_wal_bytes_checkpointed_total
bicdb_compaction_paged_checkpoint_tail_pages_truncated_total
bicdb_compaction_paged_checkpoint_failures_total
bicdb_compaction_paged_checkpoint_resource_deferrals_total
```

Operation IDs, paths, errors, and operator reasons are intentionally excluded
from metric names and labels.

Active duration includes time inside admitted page-engine steps, including a
step that returns an error. It excludes queueing, resource deferral, operator
pause, scheduler delay, and durable schedule publication. Wall duration is the
schedule's last accepted observation time minus its start time and therefore
includes those operational delays. WAL bytes are the logical bytes present at
the terminal checkpoint attempt, recorded once in a completed schedule; they
are not physical write amplification.

BicDB 1.0.77-beta completes the Phase 1 worker set with separately governed
bounded read-ahead. Explicit synchronous `checkpoint()` remains a compatibility
API; production maintenance uses the resumable supervisor through the automatic
server driver.
