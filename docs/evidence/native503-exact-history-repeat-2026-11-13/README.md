# Exact-source native 503 serial-history repeat

The owner-authorized repeat of the failed required native job passed:
**212 passed, 0 failed, 1 ignored, 877 filtered out**, in 913.06 seconds.
Run [43529104485](https://github.com/nikoma/bicdb/actions/runs/43529104485),
attempt 2, newly executed native job
[132276661454](https://github.com/nikoma/bicdb/actions/runs/43529104485/job/132276661454)
used exactly the original 213 selected test names in the original order.
The same internal child entry remained ignored; it did not execute.

This clears that CI check. It is **not a runtime fix or an explanation of
the original failures**. Their causes remain inconclusive. The original
208-pass/4-fail/1-ignored observation and its evidence are unchanged.

| Observation | Original attempt 1 | Controlled attempt 2 |
| --- | --- | --- |
| Native job | 132242099377 | 132276661454 |
| Selected names, including ignored child | 213 | Same 213, exact same order |
| Result | 208 passed, 4 failed, 1 ignored | 212 passed, 0 failed, 1 ignored |
| Rust test duration | 1197.50 seconds | 913.06 seconds |
| Original four failing cases | Failed | All four passed |
| Runner ID | 1000024718 | 1000024937 |

The four cases cover normal-Serving current-damage refusal, finite-open
worker refusal, retained-Store replacement/restoration, and drop-without-stop
with same-root reopen. Their original assertions were not changed.
Both durations describe these test suites, not RTO or performance
qualification. The same declared runner image and agent do not establish
identical load, memory, cache state, scheduling or physical hosts.

## Actual source and execution identity

Both attempts have run head `c6a081a6026f4efd00d80f392c94b67f2f2f4e12`
(1.0.503-beta). Both raw logs show actions/checkout actually checking PR
merge commit `de15f8f9836e0e93fb2e912ad230437bc975aa6a`. Retained raw Git
commit objects prove that these different commits have the same complete
tree, `a4712298110429f1c0d7ce63876b200e42a874b3`.
The run-head SHA is not substituted for actual checkout evidence.

The workflow, selection, order, flags, assertions, budgets and retry policy
were unchanged. This repeat contains no passive diagnostic instrumentation
or build memory guard. Its command was:

```sh
cargo test --config 'profile.dev.package."*".opt-level=2' -p bicdb-core --lib --locked -- \
  distribution_native_host:: range_image_native::rf3_fixture:: --test-threads=1
```

The retained [single-POST request record](single-post-request.json)
documents the one owner-authorized rerun. This capture made no dispatches,
retries or source changes. The request record is an operational provenance
record, not an independent exhaustive API audit.

Other passing checks displayed in attempt 2 are **reused results**, not
new executions. Their IDs were copied by GitHub; the checker compares
their original and displayed start times, completion times and conclusions
by job name. Only the newly executed native job is counted as fresh
validation in this packet. Skipped jobs remain unexecuted.

## Retained evidence and limits

The packet retains the complete lossless compressed log of the new job,
a byte-exact copy of the original failure packet's complete compressed log,
terminal metadata, both attempts' job listings, actual source proof,
ordered test observations and the source-pinned comparison. Neither prior
packet is edited. The checker derives both results, complete 213-name
orders, ignored child, actual checkout and four case outcomes directly
from the raw logs.

This uninstrumented repeat has no runtime RAM/PSI measurements, raw SDK
frame ledger or new per-call APP/native outcome reconciliation. A full
test pass includes the original assertions; it does not turn the
separate four-case diagnostic's three APP acknowledgments into measured
call counts for this repeat. Do not infer rollback, zero loss, an RPO or
a root cause from a pass or timeout. The original failed lifecycle mode
and any earlier acknowledged modes remain unmeasured in the old log.

The [original failed packet at 3218ee4b6](https://github.com/nikoma/bicdb/tree/3218ee4b635c9710f69889e4294445a6526db680/docs/evidence/native503-four-failure-2026-11-13)
and [separate four-case diagnostic](../native503-passive-four-diagnostic-2026-11-13/README.md)
have distinct source and execution scopes. No physical HA, VM fault,
scale-out, online relocation, capacity gain or production admission is
qualified here. See [issues and next tests](issues.md).

```sh
python3 path/to/this/packet/verify.py
python3 path/to/this/packet/verify.py --self-test
python3 path/to/this/packet/verify.py --repo /path/to/bicdb
```

The portable checker uses only retained bytes and Python's standard library.
It does not need Git, network access, BicDB, database opening or original
absolute paths. `--repo` additionally checks repository source objects.
Manifest consistency and commit pinning are not independent origin
authentication. Fresh Git-export verification is recorded at delivery.

This is a public repository. Upstream Actions logs follow GitHub's access
and retention rules; a locally restricted copy does not make them
confidential. No database, private-key or custody archive is packaged.
The literal private-key-marker scan is limited, not exhaustive secret
detection. Public Git signatures are not private signing keys.
