# Metadata Append frontier comparison — 2026-11-12 UTC

These original CI artifact ZIPs preserve the failing reproduction and the fixed
comparison, including all six checksummed pre/post-recovery store envelopes per
case, real message traces, result reports, source identity, compiler identity
and complete focused test output. They contain generated test identities and
loopback addresses; there are no production records or TLS private keys.

| Case | Exact source | Actual matched / returned index | Premature metadata commit | Classification |
| --- | --- | --- | --- | --- |
| [Baseline run 43173136830](https://github.com/nikoma/bicdb/actions/runs/43173136830) | `c295916d0030f3fbd37962150cfd1f65dc70e752` | 1 / 3 | Yes: C commit index 3 | Failed |
| [Fixed run 43173148741](https://github.com/nikoma/bicdb/actions/runs/43173148741) | `e0bb370fb1bad2cddc783b1b8f98775735281c05` | 1 / 1 | No: C commit index 0 | Passed |

Both runs complete the subsequent genuine majority-recovery sequence and strict
reopen of all three actual stores before their final safety assertions. Both
finish at committed index 4 with the original topology. In the baseline this
replaces C's earlier falsely committed topology; in the fixed case that proposal
remained uncommitted. The baseline regression fails as expected; do not hide it
or classify its complete workflow as green. Other still-live jobs are reported
separately in the [finding](../../design/metadata-append-frontier-safety-2026-11-12.md).

`manifest.json` records original GitHub artifact IDs, source/run IDs, verified
archive SHA-256 values and every archived member's size/hash. The ZIP bytes are
unchanged from the downloaded artifacts. The following offline check verifies
that custody and the recorded state/result comparison without extracting files:

```sh
python3 docs/evidence/metadata-append-frontier-2026-11-12/verify.py
```

This check does not execute BicDB or repeat its strict reopen. To repeat the real
regression, use the source-pinned command in the finding. The evidence covers a
direct fsynced metadata FSM schedule, not network/TLS faults, physical power loss,
application commit loss, original Azure causality or production qualification.
The broader full-suite and final-version gates remain independent.
