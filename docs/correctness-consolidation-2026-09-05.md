# Correctness and repository consolidation — 2026-09-05

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

The 600k target is not achieved. Neither a successful CALL nor a high NOPM
number establishes transaction completion or durability.

The latest [compact-recovery follow-up](recovery-compact-validation-2026-09-05.md)
closes the retained corrected-Payment dataset's full financial audit and
per-order coverage, and passes a fresh persistent-filesystem process-crash
acknowledgement audit. Historical gaps below record the order of discovery;
the older stock-procedure results remain financially unqualified. The shared
workload field omissions, recovery version-chain cost, durable throughput
qualification and 600k target remain open.

## Repository decisions

Started from GitHub main `e438170a`, including the user's merges of the
integrated HammerDB work (#828), replication-status memory fix (#728), Cell
admission fixes (#826), owner's guide (#827), and typed-row design (#766).
Do not replay their older branch histories over the newer main tree.

Closed the four remaining stale PRs with evidence and retained their branches:

- #736: typed-row default had a documented 2.1% regression and six failing SQL
  tests. Current main keeps it opt-in and uses the newer borrowed-cell path.
- #767/#768/#770: superseded by the rebased #816, which was itself closed
  after three VM1 pairs averaged a 3.4% regression. These are archived
  experiments, **not merged implementations**.

The uncommitted checkpoint-precopy experiment is backed up in commit
`c0649da3` on `perf/hammerdb-checkpoint-precopy`. Its predecessor is the opt-in
WAL-reclamation experiment `11fa41e4`. Neither is qualified as a throughput
keeper; neither is silently enabled or merged by this consolidation. The
negative inline-cell screen `3b962613` likewise remains on its branch.
No worktrees, benchmark data, source branches, or release artifacts were deleted.

## Persistent acknowledgement audit

VM2 (`192.0.2.12`), existing managed OS filesystem, canonical Linux
binary SHA-256 `86227835b6112fbe9d31047f5d32d54dbf2df1edafe89a253f3b88a097deed17`.
Sixteen clients execute atomic transfers with unique operation IDs. A local,
externally fsynced ledger records an acknowledgement only after COMMIT returns.
Confirmed serialization/deadlock aborts retry the complete transaction;
connection losses are uncertain and are not blindly retried.

After SIGKILL during active transactions and restart:

| Check | Result |
| --- | ---: |
| Attempted unique operations | 10,067 |
| Acknowledged operations | 10,060 |
| Recovered operations | 10,060 |
| Missing acknowledged operations | 0 |
| Duplicate/unexpected operations | 0 |
| Uncertain operations found committed | 0 |
| Account balances | 989,940 / 1,010,060 |

Every recovered operation had amount 1; both balances exactly reconcile with
the operation set. This is **process-crash recovery on persistent storage**,
not a host/power-loss test, not a performance result, and not proof covering
all failures. The seven attempted-but-unacknowledged operations are not lost
acknowledged commits.

Original reproduction script: `a664de86` on `fix/transaction-outcome-audit`.
The main-tree `bench/ack_crash_audit.py` generalizes its explicit host/data/PID
and restart parameters and checks whole process arguments before SIGKILL.
The original run predates that generalization and the non-Unix fix below.

Local artifacts: `/tmp/bicdb-persistent-ack-audit-20260905-r1`.

- Ledger SHA-256: `f19a0e05a1828daffe06196fbe4b8c3d17f2f12cf36501fe3c3623345eed2dfc`
- Result SHA-256: `5a33b02169479cb8ea2908330920febec289de25d0b06c26521efaa5e5d43cc9`

## Changes and remaining correctness work

The cached WAL append handle discarded `sync_after_write`. Unix uses O_DSYNC,
but non-Unix relies on that flag to request explicit `sync_data`. The fix
retains the flag and propagates sync failure before advancing the durable
watermark. Tests exercise write failure, sync failure, and the explicit-sync
fallback on Linux. This is not the cause of the Linux Payment deficit.

New benchmark runs with `BUSINESS_COUNTERS=1` require exact Payment/history
and New-Order/district reconciliation by default. Missing evidence, negative
deltas, zero calls, and count mismatches fail that qualification. Diagnostics
can explicitly set `REQUIRE_BUSINESS_COMPLETION=0`; old metadata/results are
not retroactively rewritten. This strict call-to-commit gate describes the
current no-client-retry workload, not a universal rule equating retry attempts
with unique business operations. Delivery requires a separate orders/backlog
audit because one call can deliver zero through ten orders.

The traced VM1 diagnostic finished its workload, but its subsequent broad
order-line audit query caused a confirmed Linux OOM kill (PID 351284,
2026-09-05 06:50:56 UTC, approximately 98.8 GiB anonymous RSS). The dataset
is retained and recovery/reconciliation continues; the audit is incomplete.
The final trace records 232 Payment conflict-handler returns, each discarding
two buffered writes, and 60,586 Delivery conflict-handler returns before
buffered writes. These are not evidence of acknowledged durable-commit loss.
The generated HammerDB driver clears a normally returned Payment result and
continues its workload loop; it does not retry a procedure's swallowed abort.

Recovery of that retained dataset now reconciles the two primary counters:
history has 2,145,417 rows minus the 480,000-row seed = 1,665,417 committed
Payments. This exactly equals the clean Payment trace count. Total Payment
calls were 1,665,649, so the 232 missing business operations exactly match
the 232 swallowed conflict rollbacks; they were not retried. District sum is
2,148,891 minus 480,160 = 1,668,731 New Orders, matching every New-Order call.
The wider Delivery/order-line atomicity audit remains incomplete.

Still open: full recovered business-state reconciliation, eliminating abandoned
Payments without changing SQL exception semantics, Delivery atomicity/backlog,
PostgreSQL Stock-Level plan variability, equivalent persistent-storage
throughput, and host/power-loss evidence. The subsequent WAL fault injection
and repair are recorded in `wal-partial-append-recovery-2026-09-05.md`.

## Subsequent per-order Delivery audit

A read-only, district-bounded audit of the recovered VM1 diagnostic examined
2,148,731 orders, 21,484,602 order lines and 770,797 pending-order rows across
all 160 configured warehouse/district pairs. It found zero orphan order IDs,
duplicate order IDs, missing lines, line-count/number disagreements,
carrier-versus-pending-membership disagreements, or partial delivery states.
Counts and consecutive line numbers agree for each order, not just in total.

Twenty-four already-delivered orders had two distinct line timestamps. An
isolated clone of the original, hash-matched PostgreSQL seed has exactly the
same 24 exceptional order IDs; all are in the initial delivered range. For
example, warehouse 1 / district 5 / order 678 already spans
`2026-09-05 02:15:46+00` and `02:15:47+00` in the seed. These exceptions are
not new to the diagnostic run. This check compares the exception sets; it
does not assert byte-for-byte equality of every timestamp in the database.

The per-district audit finished in 124 seconds. Its subsequent unbounded
whole-table coverage count triggered BicDB's memory guard at approximately
91,296 MiB RSS with host MemAvailable 7,033 MiB. The guard aborted the server;
the client received a connection failure. This is an unresolved availability
defect. The audit therefore has no successful final global-coverage result,
and is not a full financial reconciliation or universal correctness pass.
Original data, partial outputs, and server logs are retained.

- Per-district JSONL: `/tmp/bicdb-delivery-per-order-vm1-20260905-r3.jsonl`,
  SHA-256 `9f804517698754da7b99c03f4544bacc8e69385cc291172d3f5b1536111a8098`.
- Original seed timestamp exception CSV:
  `/tmp/bicdb-delivery-seed-timestamp-exceptions.csv`,
  SHA-256 `9f12da5ba00c4c66c89a4ccd41a79f4f0deff2e50c15f4e7504ecc422b3e62d0`.
- Seed audit container: `codex-seed-audit-20260905` on VM1, using only a
  disposable clone `/dev/shm/codex-seed-audit.gm0Odu/data`. Original seed
  `/dev/shm/codex-pg186.zZrxCM/data` was not opened or modified.

Combined core/SQL testing also exposed four fixture assumptions about sorted
JSON maps when SQL enables serde's insertion-order feature. Test fixtures now
explicitly canonicalize where required, or verify actual source-key order;
production JSON behavior is unchanged.

## Longer-wait correctness trial

VM2 trial `codex-correctness-wait-20260905/wait20000-d1`, same canonical
Linux binary, 16 warehouses, fixed layout A, 32 active users, MAW24, one
minute ramp plus two measured minutes. `BICDB_RC_UPDATE_LOCK_ATTEMPTS=20000`
replaces the previous two-attempt budget; repair/wait-graph/key notification
remain enabled. Fresh seed copy, strict business-completion gate enabled.
The harness now defaults to that tested 20,000-attempt budget. BicDB's
production default was already 240,000; the two-attempt override was specific
to the benchmark configuration, not the database default. Historical controls
must retain explicit `RC_UPDATE_LOCK_ATTEMPTS=2` and remain separately labelled.

- Payments: **1,675,934 committed / 1,675,934 calls**.
- New Orders: **1,673,691 committed / 1,673,691 calls**.
- Zero failed queries, zero handled serialization/deadlock exceptions,
  zero WAL watermark gap, all 33 VUs successful.
- 20,631 handled no-data exceptions remain separately reported.
- The raw district-sum score is 557,897 NOPM. This run is classified as a
  diagnostic (`profiling.kind=latency`, excluded from throughput), is on VM2,
  and uses tmpfs. It is **not** a canonical throughput keeper, a persistent
  storage performance result, a multi-run reliability proof, or 600k.
- Subsequent financial reconciliation found 80,842 customer balance
  discrepancies despite exact call completion. The captured procedure also
  omits customer YTD/payment-count updates. See
  [the financial audit](payment-financial-audit-2026-09-05.md) for the shared
  procedure defect reproduced on both engines. This run is not a financial
  correctness pass.

## CI guard repair

The first #829 dependency-boundary check was green even though its job log
contained six `rg: command not found` errors. Missing scanners inside shell
`if` conditions had silently skipped enforcement. CI now installs ripgrep;
the script rejects missing tools and scanner errors. A narrow exception for
the exact README trademark-ownership sentence preserves legal attribution
without allowing product-specific API names. The actual local scan passes.

The SQL CI job had the same missing-scanner defect: its log showed 279 unit
tests and doctests passing, then `rg: command not found` during discovery.
It had silently skipped all 118 integration targets. The bounded runner now
requires its discovery tools and propagates discovery errors; CI installs
the scanner. A dry-run with Cargo stubbed verifies 120 invocations (unit,
doctest, 118 integrations), and injected scanner failure returns exit 2.

The actual full local integration run exposed a JSONB primary-key regression:
the borrowed-cell path projected `1.0` as `1`, whereas the record path and
PostgreSQL preserve `1.0`. The fast path now declines JSON/JSONB scalar columns
and uses the existing lossless decoder, preserving JSON type, numeric scale
and precision, and JSON-null semantics. Existing primary-key regressions plus
a new scalar-type/precision test pass, as does the cell-row suite.

Final complete local SQL rerun: **1,214 tests passed, zero failed**, across
120 targets (unit, doctest, all 118 integration targets).

The broad pgwire CI run exposed a pre-existing test scheduling race:
`graceful_shutdown_drains_many_idle_and_active_clients` could miss its workers'
100 ms sleep entirely. The test now holds an advisory lock until shutdown
starts and observes all four admitted workers blocked on it, then closes the
idle lock holder. This preserves the completion/drain assertions without
depending on catching a short timing window.

## Bounded Delivery follow-up

The retained first diagnostic was audited with 160 separate warehouse/district
prefix queries. EXPLAIN confirmed `IndexScan order_line_i1`, `PrefixKeys 2`.
All 160 undelivered-line counts exactly match the corresponding undelivered
orders' declared line counts: **7,711,905 lines**, with no district mismatch.
The output SHA-256 is
`b466b7e0efd59e7c59b9850767467be1d71db8718cd6608155388b9be8690c96`
(`/tmp/bicdb-bounded-delivery-audit.csv`). This closes the previously OOM-killed
district-count comparison, not the entire per-order/financial atomicity audit.

The longer-wait trial recovered with 733,842 remaining pending orders, versus
770,797 in the original diagnostic. Thus eliminating handled conflicts does
not by itself explain or close the PostgreSQL orders-per-Delivery-call gap.
Scheduling/rechecks and the remaining detailed atomicity checks still matter.
