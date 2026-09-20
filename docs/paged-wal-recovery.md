# Bounded server-paged WAL recovery

Status: streaming two-pass startup recovery shipped in BicDB 1.0.81-beta.

Server-paged startup no longer reads the complete WAL into a `Vec<u8>` or
materializes every decoded page image. Recovery performs exactly two ordered
forward scans over the post-checkpoint WAL suffix.

## Recovery algorithm

The first pass:

1. reads one fixed header;
2. rejects invalid magic, reserved bytes, record kinds, lengths, checksums, and
   non-contiguous LSNs at the first bad record;
3. retains one reusable body buffer bounded by the durable page size plus the
   16-byte record prefix;
4. records only final commit/abort decisions and the newest redo point; and
5. truncates an incomplete tail while still holding the WAL file lock.

Holding the same lock across validation and truncation prevents an append from
landing between the scan and `set_len`. An incomplete or invalid suffix cannot
cause an allocation based on its claimed length: the length is checked against
the database page size and the global one-record ceiling before the buffer is
resized.

The second pass applies only page images at or after the redo point whose
transaction committed. Transaction zero remains structural and unconditional.
The scanner lends its mutable page payload directly to the page writer, which
stamps the new physical generation and checksum in place; recovery does not
allocate a second page-sized image. A change in valid record count or final LSN
between passes fails the open.

Since 1.0.84-beta, the first pass encodes each terminal decision as an exactly
9-byte sortable value: eight big-endian transaction-ID bytes and one status
byte. It sorts and deduplicates that vector in place, rejects a transaction that
has both commit and abort records, and moves the same allocation directly into
the recent MVCC status table. It does not build one allocation-heavy tree node
per recovered transaction or copy outcomes through another startup container.
Status below the durable frozen watermark remains represented by that watermark
rather than reconstructed from table pages.

Since 1.0.85-beta, the watermark also carries **durable abort exceptions**.
Nothing is in flight at open, so after the recovered outcomes are installed the
store advances the watermark across every representable transaction: committed
outcomes freeze outright, and aborted or crash-orphaned ones freeze as
exceptions recorded in a bounded region of the checkpoint meta page. A
transaction the watermark stepped over keeps reading as aborted even after the
WAL holding its abort record is truncated, because the exception list is
flushed before the checkpoint record that permits truncation. When the
meta-page region is full, freezing degrades to its pre-1.0.85-beta stop
behavior and reports it through checkpoint step reports, snapshots, and
metrics.

## Memory contract

`MAX_WAL_RECORD_BYTES` is the hard maximum for a validated record: the 24-byte
header, 16-byte page/transaction prefix, and the maximum supported 1 MiB page.
For a particular database, startup uses its durable page size rather than the
global maximum.

Therefore page-image memory is independent of WAL length:

```text
one reusable WAL body <= page_size + 16 bytes
one fixed header       = 24 bytes
compact outcomes       = 9 bytes per retained terminal record plus Vec capacity
abort exceptions       = 8 bytes per entry, bounded by the meta-page region
status spill           = 9 bytes per evicted outcome, disk-backed, one
                          reusable read buffer on demand
```

The remaining outcome vector is intentional recent MVCC state, not duplicated
WAL payload. Its exact peak allocated capacity is reported, as is the outcome
count the open-time freeze absorbs into the watermark. Automatic bounded
checkpoints advance the frozen transaction watermark and truncate the WAL when
safe; since 1.0.85-beta a single aborted transaction can no longer pin the
watermark or block truncation, because it freezes as a durable exception.

Since 1.0.86-beta a **disk-backed status spill** closes the long-running
transaction case. When a live transaction pins the watermark, the commits above
it cannot freeze; each checkpoint persists those terminal outcomes to chained
spill pages, syncs them, evicts them from the resident table, and then
truncates the WAL. Visibility resolves an evicted outcome with one demand read
of its spill page (a single reusable page-sized buffer), so resident
transaction memory stays bounded by in-flight transactions plus one
checkpoint's worth of commits rather than by the whole pinned suffix. The
spill descriptor lives in the meta page and is written only after the spill
pages are durable, so a crash never names a page that does not exist.

One residual case keeps this from being a constant-memory claim:

- **exception capacity overflow** — more aborted transactions below the
  watermark than fit the meta-page region — degrades freezing to the old stop
  behavior for the excess, which is reported rather than hidden. The affected
  excess is then carried by the status spill like any other unfrozen outcome.

`Wal::read_all` remains a compatibility and diagnostic helper that explicitly
materializes its return value. The database-open path does not call it.

## Interruption and corruption behavior

- A torn tail is removed before replay and its exact byte count remains in the
  recovery report.
- A forged huge body length is rejected before allocation and the invalid
  suffix is removed.
- Checksum failure and out-of-order LSNs stop at the last validated boundary.
- Transaction or page identifiers that would overflow runtime state are
  classified as WAL corruption and fail the open.
- Contradictory commit and abort records for one transaction fail the open as
  WAL corruption rather than silently taking the last record.
- Replay is idempotent because records carry complete page after-images.
- A second-pass mismatch fails closed rather than applying a moving log.
- No page, transaction payload, path, or record identity enters metrics.

## Observability

The strict startup report exposes:

- logical input WAL bytes;
- validated record count;
- exactly two scan passes;
- peak logical record-buffer bytes;
- final transaction-outcome count;
- terminal outcome-record count before deduplication;
- peak bytes allocated for compact transaction outcomes;
- outcomes absorbed into the watermark by the open-time freeze;
- durable abort exceptions resident after open;
- status-spill entries and pages loaded at open;
- committed transaction count;
- pages replayed and uncommitted images skipped;
- torn-tail bytes;
- redo and end LSNs; and
- total recovery duration.

The report is retained by the open store and exported through the typed
snapshot, operational metrics, Prometheus, doctor, and diagnostic bundles.

## Verification

Tests cover committed and uncommitted replay, aborted transactions, idempotent
replay, redo-point skipping, torn tails, checksum damage, out-of-order LSNs,
oversized forged lengths, strict serialization, and a suffix larger than 10 MiB
whose reported peak record buffer remains one 552-byte record at a 512-byte
page size. Semantically valid-checksum records with overflowing transaction or
page identifiers fail as classified corruption instead of wrapping or panicking.

BicDB 1.0.82-beta adds the clean-process
[`bicdb bench paged-recovery`](paged-wal-recovery-benchmark.md) harness. It can
vary checkpointed database bytes independently from WAL suffix bytes, samples
startup RSS, verifies rows on both sides of the checkpoint, emits versioned
JSON/CSV evidence, and fails declared time or memory ceilings. BicDB
1.0.83-beta binds that evidence to the exact executable, source declaration,
host and storage environment, and cache preparation; checksums the normalized
artifact; and independently recomputes every gate through
`bicdb bench paged-recovery-verify`.

BicDB 1.0.84-beta reduces the outcome side of recovery from a node-allocated
ordered map to the compact representation above and exports its peak allocation
through snapshots, operational metrics, Prometheus, doctor, diagnostic bundles,
and benchmark JSON/CSV. Tests cover 20,000 reverse-ordered terminal records,
in-place ordering/deduplication, exact MVCC visibility, prefix reclamation, and
contradictory terminal decisions.

BicDB 1.0.85-beta adds durable abort exceptions and the open-time freeze, and
bumps the benchmark evidence format to version 4. The verifier now fails an
artifact whose retained abort exceptions exceed the meta-page capacity for its
page size, or whose open-time freeze did not absorb the complete terminal
outcome set of a fully closed fixture. Tests cover an abort no longer blocking
WAL truncation, exceptions surviving restart with an empty WAL, crash-before-
checkpoint rederivation, capacity-overflow degradation, and a 500-transaction
retained suffix collapsing to zero resident entries at open.

BicDB 1.0.86-beta adds the disk-backed status spill and bumps the benchmark
evidence format to version 5. Checkpoints persist watermark-pinned terminal
outcomes to chained spill pages, evict them from the resident table, and
truncate the WAL; visibility resolves an evicted outcome with one demand page
read. The verifier fails an artifact whose spill entries exceed the reported
page capacity or that retains any spill entry for a fully closed fixture.
Tests cover spilled outcomes surviving restart with a pinned watermark,
repeated spill-and-merge checkpoints, the watermark advancing past spilled
xids, and a concurrent-committer checkpoint storm that previously lost rows to
a spill merge-ordering defect.
