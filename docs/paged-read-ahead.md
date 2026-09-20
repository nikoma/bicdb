# Bounded server-paged read-ahead

Status: production path shipped in BicDB 1.0.77-beta.

BicDB read-ahead is an asynchronous optimization over durable page authority.
Foreground reads remain correct when it is disabled, saturated, interrupted, or
unable to read a speculative page.

## Data path

Only exact physical dependencies are submitted today:

- the right-sibling page recorded in a B+ tree leaf; and
- the next page recorded in an overflow-chain header.

Submitting a hint performs no I/O. It enters a deduplicated FIFO queue with an
explicit construction-time page-count ceiling. The queue uses a monotonic
sequence index, so foreground cancellation and worker removal are logarithmic
in the bounded queue size rather than linear in database size.

The host worker clones a narrow page-only capability and releases the database
guard before doing I/O. Every step is admitted through the node's background
compaction lane and has immutable candidate, logical-I/O, and cooperative-time
bounds. A busy node rejects the permit before the first hint is removed, so
foreground and consensus reservations win without losing queued work.

## Configuration

The database queue ceiling is configured with:

| Setting | Default | Builder | Environment |
| --- | ---: | --- | --- |
| queued page hints | 1,024 | `with_paged_read_ahead_queue_pages` | `BICDB_PAGED_READ_AHEAD_QUEUE_PAGES` |

Zero disables speculative queueing. The hard implementation maximum is 65,536
pages; a larger value fails before page storage is created.

`PgWireConfig` controls host execution:

| Field | Default |
| --- | ---: |
| `automatic_paged_read_ahead` | `true` |
| `automatic_paged_read_ahead_poll_interval` | 2 ms |
| `automatic_paged_read_ahead_limits.max_candidates` | 128 |
| `automatic_paged_read_ahead_limits.max_io_bytes` | 1 MiB |
| `automatic_paged_read_ahead_limits.max_duration_millis` | 25 ms |
| worker memory reservation | 64 KiB |
| worker I/O reservation and charge | 1 MiB |
| worker CPU reservation | 1 slot |

The host validates the limits against the opened store's durable page size and
against the current node governor before accepting traffic. Invalid or
underdeclared envelopes fail during server construction.

## Cache admission

Speculation may consume a free frame or replace an unpinned, clean
probationary frame. It never:

- grows the preallocated buffer pool;
- evicts a protected hot page;
- evicts or writes back a dirty page;
- waits for a pinned victim; or
- counts a speculative load as a foreground cache touch.

The first real use of a prefetched page records usefulness but leaves the page
probationary. A second real use is required for protected-queue promotion. An
unused prefetched page replaced by later work is counted as wasted.

Foreground demand cancels a still-queued hint before acquiring the page shard.
If the worker already loaded it, the normal read consumes the resident frame.
If a speculative read fails, the fixed frame is returned, the error is counted,
and a later foreground read reports the authoritative storage error.

## Failure and lifecycle semantics

Hints are deliberately not durable: B+ tree and overflow cursors regenerate
them from durable page links. A crash or shutdown may discard queued hints but
cannot discard data, transaction state, or maintenance progress.

Worker failures use capped exponential backoff and power-of-two operational
logging. Per-page speculative failures use bounded aggregate telemetry without
page identifiers. Shutdown sleep is interruptible in 100 ms slices.
Non-paged databases do not retain an idle read-ahead worker.

## Fixed-cardinality telemetry

Snapshot format v4 and operational metrics expose:

- queue capacity and depth;
- submitted, enqueued, duplicate/resident, and full-queue outcomes;
- worker steps and candidate/I/O/duration stops;
- pages loaded, used, and wasted;
- admission declines and read errors; and
- foreground cancellations.

No page, path, collection, index, record, tenant, query, or plugin identity is
used as a metric name or label.
