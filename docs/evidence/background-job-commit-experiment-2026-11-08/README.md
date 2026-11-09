# Background-job COMMIT experiment, reconciled 2026-11-08

All 70 full-batch diagnostic runs completed. Both target background-job tests
failed in every variant. None of these runs is a complete official 1,464-test
qualification, storage qualification, or approval to promote an experimental mode.
No production setting or upstream assertion was changed.

This report recovers the completed background work from the interrupted session.
`reconcile.py` independently cross-checks every retained JUnit report against its
analysis record: 70 target failures, 6,615 other passing test invocations, no
errors or skips. These are repeated invocations, not 6,615 distinct tests.
The runner's own successful execution is not interpreted as an application pass.

## Protocol and provenance

Host: ny8. Each run used its own disposable copy of
`pristine-paged-current-schema-run.json`, the Oracle preview profile, and the
unchanged upstream full batches `remaining-08` (97 tests) and `remaining-14`
(94 tests). The upstream job wait remained 10 seconds. Each run record confirms
the selected class origins and disposable database removal.

A and B alternate five repetitions of each batch. A2 and E likewise alternate
five; C, D and a new A2 control alternate another five. The table pools the ten
A2 controls; per-run records retain the repetition numbers (1–5 versus 6–10)
so that phase-matched comparisons can be made. Binaries were built on s3ny;
`runs.json` contains full source commits, binary SHA-256, runner/adapter hashes,
configured gates, observed process gates, host samples, and per-job metrics.
Original analysis files are identified by name and SHA-256.

| Variant | Source | Server-only experimental settings |
|---|---|---|
| A | `decb9d02dbd753b6d13163dd8a2cf99d57c5c8f1` | none |
| A2 | `3f3f9a781030b895e068cfdfdbf7a19fb507aa94` | none; includes fail-closed fix #55 |
| B | `49d0464fbce5c1f4b4e4a9055920aabee6d39fae` | unlocked page-log sync (#43) |
| C | `e505e80f27a2f23e6738338793f8182d563d7112` | commit sync outside store lock (P2b) |
| D | `88c2d473397114fe151bd64246f95f9ba75344fe` | both B and C |
| E | same binary as A2 | transaction-log-only durability |

`BICDB_SYNC_OUTBOX=off` matches the official harness in every variant. Recorded
live process environments confirm that all experimental settings were restricted
to disposable database servers and absent from JVMs in 69 runs. A/remaining-08/r1
has no saved live environment observation; its configured settings are retained,
but live isolation for that one run is not independently evidenced by its record.

## Results

Values below are medians of per-run metrics. COMMIT percentiles come from the
server statement trace (integer milliseconds), not client end-to-end times.
The job window starts at its scheduling insert and ends when the test cleans up
or gives up. Failed jobs are censored: their final completion time is unknown.
| Variant | Batch | Runs | Passes | COMMIT p50/p95/p99 ms | COMMIT ms in job window | Total DB ms in job window |
|---|---|---:|---:|---|---:|---:|
| A | remaining-08 | 5 | 0 | 31/38/48 | 4497 | 7074 |
| A | remaining-14 | 5 | 0 | 32/60/66 | 4789 | 6845 |
| A2 | remaining-08 | 10 | 0 | 31.0/38.5/49.5 | 4370.0 | 6617.0 |
| A2 | remaining-14 | 10 | 0 | 31.0/49.5/62.0 | 4583.0 | 6622.0 |
| B | remaining-08 | 5 | 0 | 32/43/59 | 4463 | 6655 |
| B | remaining-14 | 5 | 0 | 31/59/67 | 4739 | 6810 |
| C | remaining-08 | 5 | 0 | 31/39/51 | 4353 | 6541 |
| C | remaining-14 | 5 | 0 | 30/52/59 | 4579 | 6651 |
| D | remaining-08 | 5 | 0 | 31/39/44 | 4366 | 6603 |
| D | remaining-14 | 5 | 0 | 31/51/67 | 4599 | 6611 |
| E | remaining-08 | 5 | 0 | 14/20/25 | 2806 | 5291 |
| E | remaining-14 | 5 | 0 | 14/20/27 | 2830 | 5254 |

B, C and D show no material benefit for these serial commit streams. E reduces
COMMIT latency substantially but still fails both tests. This evidence does not
justify removing a durability barrier or enabling an experimental mode.

The separate serial-commit profile attributes approximately 28–31 ms to three
serial synchronous writes, each approximately 9 ms: transaction-log record,
page-log flush, then transaction-log commit marker. #55 adds no sync syscall in
that profile. See [PROFILE.md](PROFILE.md) and `profiles/*.json`. The copied
profile narrative predates completion of C/D; the C/D JSON results are final.

The prior handoff's claim of only two synchronous operations was incomplete.
Lock optimizations can improve concurrent workloads without helping this serial
workload. Hardware, host load, JVM work, and remaining statement execution all
limit conclusions; these runs do not establish performance on another host.
No hard VM reset, power-loss test, or storage promotion was performed here.

[UPSTREAM-FAILURES.md](UPSTREAM-FAILURES.md) preserves the separate analysis of the
Oracle-provider savepoint branch and ClassGraph callout startup race. Those are
not counted as passes in this experiment or in the official inventory.

## Reproduce the reconciliation

On the host retaining the original records and JUnit files:

```sh
python3 reconcile.py /root/.local/state/bicdb/idempiere-reference/bgjob-experiment /tmp/bgjob-reconciled
```

This reads evidence and writes summaries only; it does not start a database or
run tests. The original records and JUnit reports remain in the state directory.
`summary.json` and `runs.json` provide portable, allowlisted metrics without
private connection configuration. No earlier source revision's application
results are represented as tests of current `main`.
