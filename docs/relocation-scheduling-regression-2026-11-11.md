# Relocation scheduling regression — November 11, 2026

The supervisor could indefinitely postpone healthy ranges while an earlier move
returned `Deferred`. This is a confirmed scheduling defect in source
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b` (1.0.463-beta). Its relationship to
the original Azure run's 300-second nonconvergence remains **inconclusive**.
That run did not retain the per-move scheduling history needed to establish causality.

With four active moves and one step per tick, the old controller attempted
`[1, 1, 1, 1]`; healthy moves 2, 3 and 4 remained unattempted. The regression was
compiled against the immutable actual 1.0.463-beta core library. The
[raw failure](evidence/relocation-scheduling-2026-11-11/main463-test.stderr),
[original test](evidence/relocation-scheduling-2026-11-11/original-regression.rs)
and [machine-readable findings](evidence/relocation-scheduling-2026-11-11/summary.json)
are retained.

The fix keeps an in-memory queue of eligible relocation IDs. Existing work keeps
its turn, new work joins the end, and the configured number of moves rotates to
the back before driver calls. Completed or failed moves leave the queue. Driver
I/O runs outside the queue mutex. Durable relocation phases remain in the store;
restarting the supervisor reconstructs scheduling order from active moves.
For a stable set of N moves and a budget S, each move gets a turn within
`ceil(N / S)` completed ticks. This bounds scheduling opportunities, not elapsed
time or completion: a blocking synchronous driver call can still delay the tick.
Repeated supervisor restarts can reset the order.

| Finding | Result | Evidence and limit |
| --- | --- | --- |
| Old selection starves independent ranges | Failed | Actual 463 core, deterministic mock-driver regression |
| Deferred work and newly arriving work receive turns | Passed | Two integration regressions |
| Existing distribution unit regressions | Passed | 125 tests |
| Cause of Azure's original stall | Inconclusive | Missing original per-move attempt history |
| Bounded real I/O, controller restart and physical convergence | Not tested | Require independent acceptance experiments |
| Strong-read correctness and production qualification | Not tested | Unchanged gates |

Candidate checks used parent `925d5f3e7030d4211739b75813baa1e0e13d5df9`
plus the retained [exact patch](evidence/relocation-scheduling-2026-11-11/candidate.patch),
SHA-256 `365eab4e0b43cd71b650e1172801755c70858b9cfd2d690cf1c6f912ebeeea9b`.
The first candidate run had one fixture failure because the planner allocated no
new work. Its logs are preserved; the final fixture allocates a valid public
operator plan and keeps the original assertions.

Reproduce the candidate with:

```console
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test --release --locked -p bicdb-core --config profile.release.package.bicdb-core.opt-level=0 --config profile.release.package.bicdb-core.debug=0 --test distribution_supervisor_fairness -- --nocapture
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test --release --locked -p bicdb-core --config profile.release.package.bicdb-core.opt-level=0 --config profile.release.package.bicdb-core.debug=0 --lib distribution_ -- --nocapture
```

The core used opt-level 0/debug 0 with optimized dependencies. This was local
controller validation: RF1, fsync off, metadata only, mock driver, no TCP or
physical relocation. It produced no new runnable database binary, release or
deployment. Memory monitoring recorded minimum available RAM of 26.59 GB for
the integration command and 24.22 GB for the unit command; neither was interrupted.

The next smallest test must hold one real relocation RPC past its deadline while
an independent range continues. Then interrupt and restart the controller during
snapshot, catch-up and cleanup, checking persisted progress. Physical expansion
must independently demonstrate convergence, exact acknowledged data reconciliation
and the requested read consistency before qualification changes.
