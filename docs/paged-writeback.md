# Bounded server-paged writeback

Status: shipped in BicDB 1.0.74-beta.

`BufferPool::writeback_step` and
`BicDb::paged_writeback_step_governed` write dirty resident pages without an
all-buffer flush. One call has immutable candidate, logical-I/O, and
cooperative-duration limits and returns an inclusive restart cursor.

## Bounded dirty index

The buffer pool maintains one ordered dirty-page entry per dirty resident
frame. A writable guard publishes its entry when the guard is dropped. A page
write or eviction removes the entry only after the page write succeeds.

The index is therefore bounded by `buffer_pool_bytes / page_size`, independent
of database size and lifetime write volume. Cursor discovery reads at most the
configured candidate count from this index; it never scans or sorts the whole
buffer pool.

## Write ordering and concurrency

For every selected unpinned page, the step:

1. holds the responsible buffer-pool shard against concurrent pinning;
2. if the page still owes a log record, invokes the installed writeback barrier
   with the exact page after-image;
3. waits for that barrier, or relies on the already-durable committed image;
4. writes the page;
5. clears dirty and pending-log state and removes the dirty-index entry; and
6. syncs the page file once after the bounded batch.

A barrier or page-write failure leaves the page dirty and retryable. Pinned
pages are never forced or blocked on; they remain dirty and indexed for the
next sweep. Pages dirtied behind a published cursor likewise belong to the next
online sweep.

`complete` means that the cursor reached the end of its current ordered pass.
`dirty_pages_remaining` must also be zero before a scheduler considers the
pool drained.

## Limits and reports

`WritebackLimits` contains:

- `max_candidates`, capped at 8,192;
- `max_io_bytes`, capped at 128 MiB; and
- `max_duration_millis`, capped at 60 seconds.

Logical I/O conservatively charges one page after-image plus one page-file
write per flushed page even when the image was already durable. Duration is
cooperative: after at least one candidate, the next
candidate is returned unchanged when the deadline has elapsed.

`WritebackStepReport` accounts for candidates examined, pages and bytes
written, logical I/O, pinned skips, stale-index cleanup, remaining dirty pages,
the exact next cursor, and a candidate, I/O, duration, or completion stop
reason. Its validator recomputes byte accounting and rejects internally
inconsistent serialized evidence.

## Resource governance

The `BicDb` entrypoint validates the caller's memory and I/O reservation before
looking at a candidate, then admits the step through the background compaction
lane. The declaration must cover the bounded candidate vector and the complete
logical-I/O envelope. Saturation rejects the step before page work, preserving
capacity for foreground and consensus traffic.

Snapshot format v3 and fixed-cardinality metrics expose step count, background
pages written, candidate/I/O/duration stops, and pinned skips. Embedded-memory
databases reject the paged-only API.

Snapshot format v5 also reports the oldest dirty page's conservative age and
the exact resident writeback backlog in pages and bytes. Dirty age starts at
writable-guard acquisition and survives repeated mutations until a successful
page write. WAL-barrier and page-write failures therefore remain visible as
lag instead of resetting the clock.

This is the worker-safe storage primitive. BicDB 1.0.75-beta composes it into a
durable bounded checkpoint state machine and governed supervisor, 1.0.76-beta
hosts that supervisor automatically, and 1.0.77-beta adds separately governed
bounded read-ahead.
