# Snapshot count without row materialization — 2026-09-05

The pgwire shared execution path seeds SELECT statements with a transaction,
including autocommit SELECTs. Both the scalar count shortcut and streaming
aggregate path previously refused every transaction-bearing execution. An
unfiltered count consequently materialized every row and its JSON payload.
The retained VM1 audit's final whole-table count hit the memory guard at
91,296 MiB RSS and aborted the server.

The new resident-collection count walks version visibility at the calling
transaction's snapshot and overlays each key's final pending write. It does
not parse row payloads or substitute the latest collection length for a pinned
snapshot. Cancellation is checked in bounded intervals. Lazy paged collections
explicitly decline: merging their page-store base with resident chains is not
implemented by this change. Security-context and RLS guards remain in place.

The SQL regression uses the server-style transaction-bearing shared session.
Before the fix it returns the correct count but materializes 1,024 payload rows;
after the fix it returns the same count with zero rows materialized. Core tests
compare against full snapshot scans after a later committed insert, repeated
updates, pending insert/delete/reinsert, and cancellation. A paged-mode test
verifies explicit fallback rather than an incorrect resident-only count.

- Before: `/tmp/bicdb-snapshot-count-before-r2.log` (expected assertion failure).
- Focused: `/tmp/bicdb-snapshot-count-focused.log`, all three tests pass.
- Complete core/SQL: `/tmp/bicdb-snapshot-count-core-sql.log`, exit 0,
  2,156 tests passed, zero failed, nine ignored; 190 binaries and two doctest
  targets. The isolated WAL child reports an additional nested success, not
  counted twice in that total.

Optimized diagnostic build uses the existing integrated PGO profile,
fat LTO, one codegen unit, and x86-64-v3. It reports profile hash mismatches,
including changed row-projection/count functions; it is not freshly trained
or a qualified throughput keeper. Build log:
`/tmp/bicdb-correctness-pgo-20260905.log`.

- Parent source: `542d910adfcae9c4f97880a6de3ac011a1cb9c6f` plus this count patch.
- Compiled source diff SHA-256 before documentation:
  `71ba8ad79f22b5ab36fadc7435211caf13fce8485b8390c8b660827f09611932`.
- Full binary SHA-256:
  `62bc6be3535bd6fa7277cd6bbe60ddf192c1661ce89ddc2ca42a58b8277934af`.
- Debug-stripped diagnostic `/tmp/bicdb-correctness-count-pgo` SHA-256:
  `4938714482eb40b52ba5e45e0f94b26401dfb701e869d02d799624ce2e4f04d5`.
- Old unqualified PGO artifact preserved on VM2 at
  `/tmp/bicdb-pre-wal-repair-pgo-20260904-unqualified`, SHA-256
  `4c2ba6c9a848a204e858f904f648b9c56106ff3915d37fe7e0355a0306c7efff`.

This repairs one aggregate path. General transaction-aware streaming and the
larger correctness/performance goal remain open.

## Live retained-dataset validation

The corrected binary recovered the original VM1 dataset and completed the
previously crashing network counts on 2026-09-05:

| Table | Global count | Query seconds |
| --- | ---: | ---: |
| orders | 2,148,731 | 0.0269 |
| order_line | 21,484,602 | 0.1387 |
| new_order | 770,797 | 0.0152 |

All counts exactly match the sums from the 160-district per-order audit.
Process RSS rose from 32,579,056 KiB to 32,582,596 KiB across these queries;
the recovery-time high-water mark remained 45,486,272 KiB. No memory-guard
abort occurred. This closes that audit's missing global count cross-check,
not its separate financial-correctness qualification. Evidence:
`/tmp/bicdb-count-live-validation-20260905.jsonl`, produced by
`/tmp/bicdb-count-live-validation.py`. Timings exclude the SSH memory probes.

## Concurrency test synchronization

PR #831's complete CI passed, but the subsequent main run 33956618558 failed
one pgwire assertion: twenty-row counts finished too quickly to guarantee
overlapping active queries. Its read/write lock-acquisition assertions passed.
The test now holds an advisory lock, observes two admitted readers waiting,
then releases the holder by disconnecting (which needs no query-admission
slot). It retains the count results, shared-read/write-lock counters and
peak-concurrency assertions. No production query delay was added.
The focused test and complete pgwire suite pass: 75 unit tests and 158 protocol
tests, zero failures, one ignored test. Logs:
`/tmp/bicdb-count-concurrency-focused.log` and
`/tmp/bicdb-count-concurrency-pgwire-suite.log`.
