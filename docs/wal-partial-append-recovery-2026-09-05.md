# WAL append failure and retry — 2026-09-05

This is a correctness repair, not a throughput result or proof of universal
durability. The 600k performance target remains open.

## Reproduced failure

On main `f5276d61`, the cached transaction-log writer retained a failed batch
for retry, but did not remove bytes already appended before `write_all`
returned an error. Retrying appended a complete copy after the incomplete
frame, then advanced the durable commit watermark. Recovery stopped at that
damaged frame and discarded the newly acknowledged batch.

The Linux regression uses a child test process, ignores SIGXFSZ and lowers
RLIMIT_FSIZE so the kernel actually performs a short append before EFBIG.
It restores the process limit before retrying. Before the fix, the original
two-frame test acknowledged both frames but recovery discarded 4,144 bytes
and lost the second acknowledged frame. This is not an inferred business
counter mismatch or an injected error before any write took place.

## Repair and failure boundary

The cached writer captures the original file length under its writer mutex.
After a write or explicit-sync error it truncates to that boundary, and, in
durable mode, syncs the repair before permitting another append. O_DSYNC does
not replace the explicit sync required for the truncation itself.

If either truncation or its sync fails, the handle latches an error. Later
commits, including empty sequence entries, and auxiliary outbox frames fail
closed. Checkpoint preparation checks this state even with no pending commits;
descriptor invalidation cannot clear it. Reopening through database recovery
is required. Pending commits and the acknowledged watermark are not advanced
by a failed append.

All live cached-WAL appends use the shared transaction-log writer mutex.
The backup I/O gate is shared and is not a substitute for that mutex.
This change specifically protects the cached transaction-log writer. Other
storage append paths, host/power-loss behavior, and failed-checkpoint recovery
need their own evidence; this patch does not claim to cover all of them.

## Regression coverage

- Real kernel short appends both inside the first pending frame and after one
  complete frame of a grouped batch, in durable and buffered modes.
- Recovery after retry and a subsequent group commit: every acknowledged
  frame exactly once, no discarded bytes; empty sequence entries also drain.
- Explicit-sync failure after writing a complete batch, repaired before retry.
- Failed truncation and failed repair sync, refusing later writes.
- Failed auxiliary repair, empty pending queue, checkpoint refusal,
  invalidation, and empty-commit refusal.
- Existing transient open failure remains retryable.

Focused logs: `/tmp/bicdb-partial-append-before.log`,
`/tmp/bicdb-append-focused.log`, and
`/tmp/bicdb-wal-repair-focused-final.log`. Broader core/SQL validation is
recorded in `/tmp/bicdb-wal-repair-core-sql.log`.

The complete combined core/SQL run exited successfully: 2,153 passing tests,
zero failures, nine ignored tests, 190 test binaries plus both doctest targets.
The child fault test also reports its own nested one-test success; it is not
counted twice in that total. The expanded four-case kernel fault matrix was
rerun separately after the broad run compiled, and all three WAL failure tests
passed. Formatting, whitespace, licensing invariants, cargo-deny, dependency
boundary, and changed-production-line workload-neutrality checks passed.

For comparison, PostgreSQL 18.6 raises PANIC for WAL write and WAL sync
failures rather than continuing normal operation beyond an unsafe boundary:
[WAL write](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/xlog.c#L2286),
[WAL sync](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/transam/xlog.c#L8218).
That source comparison is not a claim that PostgreSQL never aborts a
transaction, and is not a same-fault runtime test of both engines.
