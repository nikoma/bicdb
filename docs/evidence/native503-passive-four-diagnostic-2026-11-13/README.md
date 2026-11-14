# Native 503 four-case passive diagnostic

Run [43537820765](https://github.com/nikoma/bicdb/actions/runs/43537820765)
passed four focused tests: **4 passed, 0 failed, 0 ignored, 1086 filtered
out**, in 15.52 seconds. The original failed required suite remains a
separate failed observation. This diagnostic did not reproduce its failures
and does not establish their cause or deliver an HA fix.

The qualifier is `2b8c2b2de400038729a226ff61b9b275df846d18`.
Its Core tree is exactly the Core tree of the test-only diagnostic source
`7ab56348a8309d4d396b1af87467232bfed1ac96`; both inherit source version
1.0.503-beta. Retained raw Git objects prove the path from each commit
through its root and crates trees to Core tree
`3e65798272bcc9bd050c505556de481ac2b185a6`. Only the qualifier workflow and
build guard differ. This packet adds documentation and evidence only.

| Original case | Diagnostic result | Reached boundary |
| --- | --- | --- |
| Normal Serving current-damage refusal | Passed | Fresh metadata acquisition succeeded; actual current-record damage injection and original refusal assertions executed |
| Finite open does not start process worker | Passed | All three initial preparation calls returned successfully; original worker assertions executed |
| Retained Store replacement/restoration | Passed | Preparation completed; original replacement/restoration and held-reservation assertions executed |
| Drop without stop and same-root reopen | Passed | All three lifecycle modes completed, with three acknowledged APP returns, three full-value Strong returns and six native same-root reopens |

The lifecycle calls belong to **three separate fixture clusters**, one per
mode. They are not one cluster's failover, scale-out or throughput campaign.
Mode 2 exercises durability fencing and intentionally does not request a
native reopen. The normal-Serving fixture's internal transactions are not
included in the public APP acknowledgment count. Expected full values,
returned values, hashes and diagnostic call identities are retained; these
test-only memory observations are not an external durable acknowledgment
ledger. SDK raw frames and internal retries were not captured.

## Capture limits

Four snapshots retain 764, 162, 240 and 756 records respectively. They
report **18 busy-latch drops**: 11, 1, 0 and 6. No bound or serialization
drops are reported at snapshot time. Every snapshot explicitly states
`global_capture_complete=false` and `background_quiescence_proven=false`.
Zero snapshot drops for one case does not establish a complete global
probe history. No extra worker joins or drains were added.

There are 60 retained per-voter cancellation errors. Each was recorded
after a matching successful metadata acquisition terminal: 20 in the
normal-damage case, 10 in the Store replacement case and 30 in the
lifecycle case. All 157 retained acquisition terminals returned success.
These are observations of this diagnostic; they do not explain the
original failed acquisition. The checker preserves the distinction
between queue intent, worker invocation, transport result and caller
terminal result.

## Build checks and pressure

The focused job is [132271009934](https://github.com/nikoma/bicdb/actions/runs/43537820765/job/132271009934).
Its helper build, exact four-name listing assertion and serial test
execution passed. Workspace job
[132270985379](https://github.com/nikoma/bicdb/actions/runs/43537820765/job/132270985379)
passed `cargo check --workspace --all-targets --locked` and the workflow
consumer check without Core defaults.

The four retained guard logs cover helper compilation, test listing,
workspace checking and consumer checking. Each ends with child/guard exit
0, no monitor failure (74), no resource interruption (75), and no omitted
pressure samples. The minimum recorded host `MemAvailable` is
11,250,642,944 bytes, from the list step. These are sampled **build/check/list**
observations. They do not measure runtime pressure, cgroup headroom or
exclude sudden OOM, and are not the cause of the original failure.

## Portable evidence

The packet retains both original ZIPs byte for byte, complete compressed
Core and workspace job logs, their raw hashes, terminal steps, source and
dispatch identities, parsed snapshots, logical analysis and per-member
digests. ZIP member CRC checks are performed without extracting or opening
a database. The ZIPs contain logs, source/listing information and pressure
records; no database or key custody archive is included.

```sh
python3 path/to/this/packet/verify.py
python3 path/to/this/packet/verify.py --self-test
python3 path/to/this/packet/verify.py --repo /path/to/bicdb
```

The default checker uses only Python's standard library and retained
bytes, without network, Git, BicDB or original absolute paths. It checks
the exact test selection, results, complete snapshot content, lifecycle
value hashes, cancellation/terminal ordering, guard scope and raw Git
object path. `--repo` additionally compares source pins with repository
objects. Integrity against the manifest and pinned commit is not
independent origin authentication. See [issues and next tests](issues.md).

The [original failed packet at 3218ee4b6](https://github.com/nikoma/bicdb/tree/3218ee4b635c9710f69889e4294445a6526db680/docs/evidence/native503-four-failure-2026-11-13)
retains 208 passed, 4 failed and 1 ignored at source `c6a081a6`.
That failure archive is unchanged. The later exact-source broad repeat is
outside this immutable diagnostic corpus, regardless of its outcome.
No RPO, RTO, physical HA, online relocation, post-convergence throughput,
general distributed SQL or production qualification follows from this run.

This is a public repository. Upstream Actions artifacts and logs follow
GitHub's access and retention rules; local restricted copies do not make
upstream evidence confidential. Retained artifact metadata records the
actual expiration dates. The literal private-key-marker scan is a narrow
check, not a guarantee against every secret format. Public Git commit
signatures are not private keys.
