# Local physical namespace identity — validation evidence

Exact tested source: `24aa180cdce2704fe313852c11177c238b403658`, package
`1.0.475-beta`. The raw results all bind this clean source. They do not establish
that later commits ran the same tests. A complete engine patch from public
`149a09b812db56b646d25104c5aebfac9e00ed0c` and hashes of all 1,282 engine input
files support offline reconstruction without modifying a supplied repository.

| Scope | Retained result |
| --- | --- |
| Namespace-focused Core units | 11 passed, 1 ignored; overlaps the broad suite |
| Explicit pinned source-463 old-reader negative | 1 passed, separately requested with `--ignored` |
| Broad Core unit suite | 534 passed, 4 ignored; `BICDB_PAGED_UNIQUE_APPLY_PROBE=1` explicitly enabled |
| Rename, snapshot, distribution snapshot, HA state durability/crash and archive/PITR integrations | 44 passed across eight targets |
| Downstream SQL version identity | 2 passed; not SQL/application acceptance |
| Initial HA crash invocation before child build | 0 passed, 7 failed; setup failures retained |

The broad output contains 535 successful executions because a passing test invokes
a nested child test already among its 534 unique successful IDs. Its outer Rust
summary is 534 passed / 4 ignored. The focused eleven are a subset of that broad
suite; no summed independent-test total is claimed. The compatibility negative
runs the previously frozen `bicdb 1.0.463-beta` CLI and checks its refusal of the
activated format. Its exercised binary SHA-256 is
`840f0124f27a1a7695c4b2b0c72b05a262456802af65e063cff12242559e2f7d`.
This bundle records/rechecks that existing copy's identity; it distributes no
binary and does not independently attest the original launch.

The first HA crash target failed seven tests because the required `ha_state_child`
executable had not been built. Complete failed stdout/stderr are retained in v1.
The v2 runner builds the child successfully, then the unchanged seven-test target
passes; archive/PITR also passes. No failed result is relabeled as success and no
product assertion is weakened. Source and dirty-patch identities remain equal
across v1, v2 and v3. The v3 downstream version check builds against that same
source. Raw bytes are compressed losslessly and have separate original/export
hashes; warnings and all unsuccessful setup output remain intact.

The commands use release dependencies, Core `opt-level=0/debug=0`, explicit
`tls-replication`, one build job, incremental compilation disabled and two test
threads. The SQL version check also uses SQL `opt-level=0/debug=0`. The complete
three executed guard scripts and their environment overrides are retained. The
first Core compile names the intended worktree; resource samples track available
RAM, swap, pressure and disk throughout. Minimum available RAM across all fourteen
commands was **18,252,296,192 bytes**; no command was interrupted or paused by the
guard. These are validation timings/resources, not database throughput or RTO.

This validates the opt-in local UUID/reader-fence implementation and the listed
regressions. It does not enable the separately experimental native image format,
prove range history, serving authority, StrongGet, automatic membership recovery,
physical HA, capacity gain, original audit/broker effects or production admission.
No CI result, current-source operator binary, release or deployment is claimed.
The old CLI is an input only. The test databases were temporary; no post-test
physical database reconciliation is supplied here.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/local-namespace-identity-2026-11-11/verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/local-namespace-identity-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/local-namespace-identity-2026-11-11/test_verify.py
```

The verifier checks exact manifest coverage, raw-original hashes, actual test IDs,
ignored/failed counts, environment overrides, source identities, compiler paths
and sampled headroom. Optional reconstruction applies the retained patch to the
pinned public engine tree in a temporary directory and checks every engine file.
It reruns no Rust or compatibility executable and does not independently attest
execution authenticity. The integrity negatives reject manifest omissions,
inflated results, erased setup failures, missing broad-suite environment and a
wrong compiler worktree even after exported hashes are recomputed.
