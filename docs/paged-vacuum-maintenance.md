# Server-paged vacuum maintenance

BicDB 1.0.67-beta turns the bounded page primitive introduced in 1.0.66-beta
into a durable, host-driven maintenance state machine. It is deliberately a
tick API rather than an unbounded loop or a hidden thread: the application host
decides when to tick, while BicDB owns identity, resource admission, cursor
correctness, retry policy, and atomic publication.

## Durable files

The database root contains two small files:

```text
maintenance/paged/store-identity.json
maintenance/paged/vacuum.json
```

Both formats are versioned, strict JSON. The immutable store identity and every
schedule state carry canonical SHA-256 checksums. Loads reject symlinks, empty
files, non-regular files, oversized files, unknown fields, checksum mismatch,
and a schedule copied without its matching identity. State is written through
a temporary file, synchronized when database durability is enabled, renamed
atomically, and followed by a parent-directory sync.

A backup or exact clone may copy both files together. The clone resumes the
same physical lineage safely because its pages and cursor snapshot are copied
together. Moving only `vacuum.json` to another database fails store-identity
validation before any page is touched.

## Host lifecycle

```rust
let limits = PagedVacuumScheduleLimits::default();
let schedule = db.start_paged_vacuum_maintenance(now_ms, limits)?;

loop {
    let state = db.paged_vacuum_maintenance_status()?.unwrap();
    let due = state.next_attempt_at_ms.unwrap_or(now_ms);
    let outcome = db.tick_paged_vacuum_maintenance(
        schedule.operation_id,
        &node_resource_governor,
        due,
    )?;
    if matches!(outcome, PagedVacuumScheduleAdvance::Complete { .. }) {
        break;
    }
}
```

An active operation cannot be replaced. Every tick must present its operation
UUID, so a stale worker cannot advance a newer sweep. `BicDb` serializes local
start, tick, pause, resume, and status transitions; the paged directory lock
excludes another process from opening the same store concurrently.

Operators may pause an active sweep with a bounded reason and later resume it
at a monotonic due time. A completed or paused sweep may be superseded by a new
operation. Starting again walks from the beginning, which is safe because page
vacuum is idempotent.

## Resource and progress contract

`PagedVacuumScheduleLimits` is immutable for one operation and contains:

- page, logical-byte, and cooperative-duration limits for one page step;
- delay between successful steps;
- resource-saturation retry delay;
- bounded exponential failure backoff and a failure ceiling;
- maximum schedule file bytes; and
- explicit peak memory, in-flight I/O, CPU slot, and token-bucket I/O demand.

The declared I/O reservation and rate charge must cover the step's byte limit.
The memory reservation must cover the bounded page-ID staging list plus engine
overhead. A tick obtains a `Compaction` lane permit before calling the page
engine. Saturation performs no page I/O, records a deferral, publishes the next
due time, and returns. The RAII permit is released before schedule publication
on both success and failure.

The schedule exposes the exact inclusive cursor, last page report, successful
step count, scanned and reclaimed page/version/byte totals, failure count,
resource deferrals, next due time, pause reason, completion, and a monotonic
state sequence. Totals include reports that reached the atomic schedule
publication boundary; an attempt lost to a crash is replayed rather than
counted speculatively.

## Crash and failure semantics

Page changes and the supervisor file are separate durability domains. This is
safe because the cursor is inclusive and the page operation is idempotent:

1. If the process dies before a page step finishes, the old cursor remains.
2. If it dies after page mutation but before schedule rename, the old cursor
   remains and the page is revisited.
3. If it dies after rename, the new cursor is authoritative.

No crash ordering can make BicDB skip an unacknowledged page. Step failures are
checkpointed with bounded error text and exponential retry. Reaching the
failure ceiling pauses the operation. A byte envelope that repeatedly cannot
process the page at the current cursor is paused rather than placed in a hot
retry loop; start a new operation with a larger explicit envelope.

Clock regression, a stale operation ID, invalid limits, corrupt state, or a
store-identity mismatch fails before page work. Embedded-memory databases
reject this API and do not create maintenance state.

## Remaining boundary

This state machine makes paged vacuum durable and governed. It does not claim
that every BicDB maintenance operation has been migrated. Whole-file or extent
rewrite, a few legacy cleanup paths, and production scale/failure campaigns
remain tracked in `server-paged-storage-todo.md` and
`automatic-distributed-sharding-todo.md`.
