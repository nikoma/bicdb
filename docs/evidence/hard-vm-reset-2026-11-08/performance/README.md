# Paired commit timing: actual main and default structural cuts

All 18 runs and 36 planned-abort row-count recoveries completed. Timing is
inconclusive about a consistent improvement or regression: ranges are wide and
paired directions differ. No outliers were removed. The original SQL performance
assertions and the separate hard-VM data-safety campaigns remain independent.

Both exact binaries use Rust 1.96.0, the standard Cargo release profile with thin
LTO and line-table debug information, identical benchmark source SHA-256
`cbfa2649c49923e2064f256b33c37c9950e19de2f2ad6fc870e991b98882c661`,
and no experimental features. Actual-main control is `0af1b55e3d586d4a6933098419e7ee46a7ae5a79`;
default candidate is `6d225c1940cf13ffbb7693f8ebc2917e5abaad82`.
`artifact-manifests.json` records source, input binary hashes and build provenance.
The candidate includes the no-steal/structural-cut implementation and bounded
maintenance batching plus its pressure-report counters; this comparison is not
one old feature flag toggled on an otherwise identical build.

The workload uses explicit server_paged storage, fsync on, all-logs commit
barriers, a 256 MiB normal pool and sync outbox off on both sides. Each transaction
inserts an indexed unique row and updates a hot row. Three alternating pairs run
for 15 seconds at each of 1, 16 and 64 writers. The harness verifies matching
source/example hashes before starting. Benchmark stdout is flushed before its
planned SIGABRT; every resulting store opens twice and reconciles row count to
reported commits plus writer seed rows. This is a recovery timing/count check,
not an independent external acknowledgment ledger or uncertain-commit proof.

| Writers | Main commits/s median (range) | Default commits/s median (range) | Median change |
|---:|---:|---:|---:|
| 1 | 366 (167–501) | 299 (165–659) | -18.5% |
| 16 | 320 (310–1089) | 785 (740–1079) | +145.1% |
| 64 | 781 (556–960) | 932 (421–951) | +19.3% |

Single-writer median-of-run commit p50 was 1.640 ms on main and 1.651 ms on the
default candidate. The three paired throughput ratios vary in both directions;
`summary.json` retains each ratio and their geometric mean as well as the run
medians. These descriptive summaries are not causal effect estimates.

All agents held heavy work during this coordinated window. The shared host still
recorded 2.6–7.5 seconds of system-wide IO `some` pressure per 15-second run;
this includes pressure from the benchmark itself and is not evidence of a
particular competing process. Available RAM stayed above the active 10 GiB
headroom guard. Pressure/RAM samples and process output-block counts are retained.
End-of-run WAL sizes divided by commits are footprints, not total write
amplification: truncation/rotation and allocation can affect the file size.
Process output blocks also include startup and filesystem effects.

Run `python3 audit.py` to verify hashes, all 18 records, exact stdout, matching
source/example provenance and all 36 row-count recoveries. `configuration.json`,
`raw/` and `summary.json` are allowlisted exports; only service counts are retained,
not private service descriptions. No application databases, signing keys,
production data or external credentials are included. Raw workload directories
and the complete original capture remain on the task-owned host. This evidence
qualifies the named microbenchmark sources; final integrated-main application,
performance and postmerge safety qualification remain separate.
