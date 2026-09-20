# Server-paged storage configuration and observability

Status: initial contract shipped in BicDB 1.0.68-beta; snapshot format v2 in
BicDB 1.0.70-beta adds bounded tail-reclamation counters; snapshot format v3
in BicDB 1.0.74-beta adds bounded background-writeback counters; snapshot
format v4 in BicDB 1.0.77-beta adds bounded read-ahead gauges and counters;
snapshot format v5 in BicDB 1.0.78-beta adds contention, dirty-age, and
writeback-backlog telemetry; snapshot format v6 in BicDB 1.0.79-beta adds
physical page-class counters and fixed I/O-latency histograms; snapshot format
v7 in BicDB 1.0.80-beta adds WAL sync batches and latency, checkpoint
freshness, and retained startup-recovery evidence; snapshot format v8 in BicDB
1.0.81-beta proves bounded two-pass recovery with scan count, peak record bytes,
and final transaction-outcome cardinality.

`storage_mode = server_paged` has an explicit resident-memory and recovery-log
envelope. The same effective values and live pressure counters are available
through the Rust API, operational metrics, Prometheus text, `bicdb doctor`, and
the terminal dashboard.

## Configuration

The Rust configuration fields and builders are:

| Setting | Default | Builder | Environment |
| --- | ---: | --- | --- |
| page size | 8 KiB | `with_paged_page_size` | `BICDB_PAGED_PAGE_SIZE` |
| buffer-pool bytes | 256 MiB | `with_paged_buffer_pool_bytes` | `BICDB_PAGED_BUFFER_POOL_BYTES` |
| read-ahead queue pages | 1,024 | `with_paged_read_ahead_queue_pages` | `BICDB_PAGED_READ_AHEAD_QUEUE_PAGES` |
| WAL checkpoint trigger | 256 MiB | `with_paged_wal_max_bytes` | `BICDB_PAGED_WAL_MAX_BYTES` |

The buffer-pool budget is a hard resident frame ceiling. It is rounded down to
whole pages, allocated once, and never grows on admission pressure. If all
frames in the responsible shard are pinned, the operation fails with a bounded
`PoolExhausted` error and increments the admission-failure counter.

The page size is durable. Reopening an existing store with a different value
fails before mutating it. Invalid page sizes, a buffer budget smaller than one
page, and a WAL trigger smaller than one page fail before page storage is
created.

The WAL value is a checkpoint trigger, not a per-transaction byte ceiling. One
large transaction, or an active transaction preventing the committed prefix
from freezing, may temporarily take the log above the trigger. The live and
configured values are both exported so this condition is visible.

## Typed snapshot

`BicDb::paged_storage_snapshot()` returns `None` for `embedded_memory` and a
versioned `PagedStoreSnapshot` for `server_paged`. The snapshot includes:

- page size, logical page count, used and free pages;
- logical page bytes and current file length;
- WAL bytes, trigger, LSNs, append/sync/checkpoint counters, exact records
  covered by successful sync batches, last and maximum batch size, sync
  failures, fixed sync-latency histograms, process-lifetime checkpoint
  freshness, and group-commit savings;
- bounded evidence from the current process's startup recovery: logical WAL
  input bytes, duration, records scanned, committed transactions, pages
  replayed, uncommitted images skipped, torn-tail bytes, and redo/end LSNs;
  the report also exposes its exact scan-pass count, peak validated record
  bytes, and recent transaction-outcome count;
- buffer-pool budget, frames, resident/dirty/pinned/evictable pages, queue
  occupancy, hit/miss/eviction/writeback/admission counters, bounded
  background-writeback stop and pin-pressure counters, bounded read-ahead
  queue/usefulness/failure counters, metadata-shard and page-latch contention,
  oldest dirty-page age, pending writeback pages/bytes, and hit ratio;
- aggregate page reads, writes, transferred bytes, allocations, frees, syncs,
  checksum failures, torn pages, and short reads;
- physical page-type reads, writes, and transferred bytes for superblock, heap,
  overflow, B+ tree interior, B+ tree leaf, free, and unclassified pages;
- read, write, and page-file sync latency as count, total nanoseconds, maximum
  nanoseconds, and nine fixed cumulative buckets;
- effective fsync behavior.

Taking a snapshot performs no heap, index, record, or WAL-content scan. It is a
fixed-size walk over the explicitly bounded buffer pool plus atomic counters and
one metadata query on the open page file. Its cost is independent of database
size.

Snapshot JSON rejects unknown fields. The `format_version` field allows future
consumers to bind alerts and diagnostic parsers to an exact contract.

## Metrics

`OperationalMetrics::from_db` folds the snapshot into fixed names under the
existing `storage`, `transaction`, and `memory` sections. Examples include:

```text
bicdb_storage_paged_page_count
bicdb_storage_paged_free_bytes
bicdb_storage_paged_page_checksum_failures_total
bicdb_storage_paged_page_heap_reads_total
bicdb_storage_paged_page_btree_leaf_writes_total
bicdb_storage_paged_page_unclassified_bytes_read_total
bicdb_storage_paged_page_read_latency_le_1us_total
bicdb_storage_paged_page_read_latency_le_10s_total
bicdb_storage_paged_page_read_latency_inf_total
bicdb_storage_paged_page_write_latency_nanos_total
bicdb_storage_paged_page_sync_latency_max_nanos
bicdb_storage_paged_tail_reclaim_attempts_total
bicdb_storage_paged_tail_reclaim_deferrals_total
bicdb_storage_paged_tail_reclaim_pages_truncated_total
bicdb_transaction_paged_wal_bytes
bicdb_transaction_paged_wal_max_bytes
bicdb_transaction_paged_wal_sync_records_total
bicdb_transaction_paged_wal_last_sync_records
bicdb_transaction_paged_wal_max_sync_records
bicdb_transaction_paged_wal_sync_latency_count
bicdb_transaction_paged_wal_sync_latency_le_1ms_total
bicdb_transaction_paged_wal_sync_latency_inf_total
bicdb_transaction_paged_wal_last_checkpoint_completed_at_millis
bicdb_transaction_paged_wal_checkpoint_age_millis
bicdb_transaction_paged_recovery_wal_bytes_scanned
bicdb_transaction_paged_recovery_duration_nanos
bicdb_transaction_paged_recovery_scan_passes
bicdb_transaction_paged_recovery_peak_record_bytes
bicdb_transaction_paged_recovery_transaction_outcomes
bicdb_transaction_paged_recovery_pages_replayed
bicdb_memory_paged_buffer_pool_resident_bytes
bicdb_memory_paged_buffer_pool_dirty_pages
bicdb_memory_paged_buffer_pool_hit_ratio_basis_points
bicdb_memory_paged_buffer_pool_admission_failures_total
bicdb_memory_paged_buffer_pool_writeback_steps_total
bicdb_memory_paged_buffer_pool_background_writebacks_total
bicdb_memory_paged_buffer_pool_writeback_candidate_limit_stops_total
bicdb_memory_paged_buffer_pool_writeback_io_limit_stops_total
bicdb_memory_paged_buffer_pool_writeback_duration_limit_stops_total
bicdb_memory_paged_buffer_pool_writeback_pinned_skips_total
bicdb_memory_paged_buffer_pool_read_ahead_queue_capacity
bicdb_memory_paged_buffer_pool_read_ahead_queue_depth
bicdb_memory_paged_buffer_pool_read_ahead_pages_loaded_total
bicdb_memory_paged_buffer_pool_read_ahead_pages_used_total
bicdb_memory_paged_buffer_pool_read_ahead_pages_wasted_total
bicdb_memory_paged_buffer_pool_read_ahead_read_errors_total
bicdb_memory_paged_buffer_pool_shard_lock_waits_total
bicdb_memory_paged_buffer_pool_shard_lock_wait_nanos_total
bicdb_memory_paged_buffer_pool_shard_lock_max_wait_nanos
bicdb_memory_paged_buffer_pool_page_latch_waits_total
bicdb_memory_paged_buffer_pool_page_latch_wait_nanos_total
bicdb_memory_paged_buffer_pool_page_latch_max_wait_nanos
bicdb_memory_paged_buffer_pool_oldest_dirty_page_age_millis
bicdb_memory_paged_buffer_pool_writeback_lag_pages
bicdb_memory_paged_buffer_pool_writeback_lag_bytes
```

There are no page, path, collection, index, record, tenant, query, or plugin
labels. Metric cardinality is constant for one process.

Page classes are the trusted physical `PageType` decoded after checksum and
torn-page verification. A short read, failed verification, or invalid header is
charged to `unclassified`; BicDB never trusts the damaged bytes merely to make
the metric more specific. Successful positioned system calls are counted even
when they return a short transfer. Calls that return an I/O error, writes
rejected before the system call, and failed syncs are not counted as completed
I/O. An unexpected short operating-system write records its actual transfer and
then fails closed with `ShortWrite`; the explicit shorter fault-injection write
remains available to simulate process loss after a torn page. The small
page-size probe used while opening an existing file is metadata discovery
rather than a page read and is excluded.

The cumulative latency boundaries are 1 microsecond, 10 microseconds, 100
microseconds, 1 millisecond, 10 milliseconds, 100 milliseconds, 1 second, 10
seconds, and infinity. They are deliberately non-configurable, keeping the
snapshot shape and exporter cardinality stable. Timing covers the positioned
read, positioned write, or page-file `sync_data` call, not subsequent checksum
verification or decoding.

WAL sync latency uses the same fixed buckets. It measures the physical
`sync_data` call when durability is enabled and the explicit `flush` call in
unsafe no-fsync test mode; it excludes append and file-lock wait. Batch sizes
count newly covered WAL records rather than bytes or caller identities. A
successful redundant request that was already durable does not create a sync
observation. Failed syncs increment their own counter and do not enter the
successful latency histogram.

The checkpoint completion timestamp is wall time for correlation. Its age is
measured from a monotonic process clock, so wall-clock adjustment cannot make
an old checkpoint appear fresh. Both are zero until a checkpoint completes in
the current process. They intentionally reset after restart; durable
checkpoint progress remains represented by the WAL redo LSN and the checksummed
checkpoint-maintenance schedule.

Startup recovery evidence is retained in the open store and is therefore
stable across scrapes. `wal_bytes_scanned` is the logical WAL input present
before torn-tail truncation, not a claim about aggregate device bytes across
the two outcome/replay passes. Duration covers tail validation, transaction
outcome reconstruction, and page replay performed before the database becomes
observable. `peak_record_bytes` is the largest validated 24-byte header plus
body held by the reusable scanner; it is bounded by the database page size plus
40 bytes and by `MAX_WAL_RECORD_BYTES` globally. The report contains counters
and LSNs only, never record payloads, page contents, paths, or row identities.

I/O counters are lock-free process-lifetime scrape samples. A concurrent scrape
may observe an aggregate counter and its page-class or latency counterpart at
adjacent instants. Their totals are exact once I/O is quiescent; monitoring
should not treat a transient one-operation difference as corruption.

Wait counters advance only when a nonblocking lock attempt fails. Uncontended
page access therefore avoids clock sampling. Total and maximum wait nanoseconds
describe process-lifetime pressure; they are not durable across restart.

Dirty age begins when a writable page guard is acquired, conservatively
capturing the earliest possible modification. The first write keeps that
timestamp while repeated writes remain dirty. Age and writeback backlog clear
only after the page write succeeds; a failed WAL barrier or page write leaves
both visible and retryable. These gauges describe live cache pressure, not a
durable correctness checkpoint.

Counters are process-lifetime values and reset on reopen. Durable gauges such
as page size, page count, free pages, file length, and configured limits survive
reopen. Monitoring should use rates for `_total` counters and direct thresholds
for gauges.

## Doctor findings

The sanitized doctor bundle contains the typed `paged_storage` snapshot. The
human-readable command prints the effective envelope and live pressure. Doctor
adds recommendations when:

- the buffer pool has refused an admission;
- at least 75% of frames are dirty;
- WAL bytes reach at least 80% of the checkpoint trigger; or
- the physical file length exceeds the logical superblock length after the
  safe crash window between metadata publication and tail truncation.
- bounded checkpoint tail reclamation has deferred a rewrite that requires the
  resumable file/extent maintenance path.

The last condition is not data corruption. The extra tail is outside the
published page count and a later safe reclamation pass may remove it.

## Current limits

This release exports physical superblock, heap, overflow, B+ tree, free, and
unclassified I/O. It cannot yet attribute a shared B+ tree or overflow page to
a logical catalog, secondary index, FTS, or vector namespace. It also does not
export physical allocated filesystem blocks for sparse files. Durable
cross-restart checkpoint wall-time history, commit-count batch distributions,
and logical-namespace residency remain explicit telemetry roadmap items.
