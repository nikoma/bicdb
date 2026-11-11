# Relocation handoff: acknowledged visibility fails after cleanup-first ordering

**Preservation target: FAILED_CORRECTNESS. Healthy control: PASS.**
Runtime is frozen **1.0.463-beta**, source
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, core library SHA-256
`dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356`.
No engine source was changed or recompiled. This deterministic local fixture
uses real range commands, actual snapshot/catch-up/cleanup, five actual metadata
consensus stores, strict schema fencing and fsync=true. Delivery and scheduling
are in-process and scripted; TLS and host/VM faults are not exercised. It proves
neither an Azure cause nor irretrievable data loss or production qualification.

## Observed comparison

Both cases commit a seed on RF3 `{n0,n1,n2}`, copy/catch up source n0 to learner
n3, and reach real `ReadyToPromote`. A later transaction is acknowledged by n0
after real quorum admission/certification/application on n0 and n1 while n2 is
unavailable. The captured source DB watermark moves from 1 to 2 after the last
copy; the actual late command has index 2. These are distinct sequences whose
numbers happen to coincide in this fixture; the proof uses causal ordering,
actual command identity/checksum and exact payload comparison.

| Cut | Actual data voters | Result |
| --- | --- | --- |
| After final copy and late ACK | n0,n1,n2; n3 learner | Late value applied on n0/n1; absent n2/n3 |
| Committed promotion | n0,n1,n2,n3 | Source retained; quorum is 3; recovery with only n2/n3 correctly refuses |
| Control: recover with source before cleanup | n0,n1,n2,n3 | n0/n2/n3 satisfy quorum; late value recovered and survives cleanup |
| Cleanup first, then source removed | n1,n2,n3 | With n1 unavailable, fresh n3 recovery using n2/n3 returns `Ok(1)` but both retain resolved prefix 1 and return no late value |

The initial hypothesis that promotion immediately creates the final RF3 set was
incorrect. Promotion retains the source as a fourth voter. The reproduced gap
requires actual subsequent source cleanup/removal before new-leader recovery.

Metadata leader n2 commits promotion using actual Append responses from n3 and
n4 plus itself, a majority of all five metadata voters. The stores on n0/n1
remain at commit index 6 while n2/n3/n4 reach 7. Source n0 then learns committed
promotion/cleanup metadata, making its actual cleanup request valid. Subsequent
transitions commit on n2/n3/n4 before source n0 receives them. Final current
metadata is commit index 11/range epoch 10; n1 retains commit index 6/epoch 8.
The captured transitional data authority is epoch 9/four voters. No progress,
catalog entry or snapshot completion counter was fabricated.

The control recovers the exact late payload before cleanup. In cleanup-first
ordering, source n0's row is physically deleted, n1 still holds the exact late
value, and n0's checksummed catalog still retains the applied late command.
The new authority reports recovery complete while missing that acknowledged
value. This is a recovery/readiness correctness failure against acknowledged
preservation during handoff. Legacy Get remains a local/eventual read; no new
strong-read contract was exercised. Durable old copies remain available for
investigation/salvage; permanent loss of all copies is not demonstrated.

## Reproduction and retained evidence

The fixture is
`scripts/experiments/relocation-handoff-characterization.rs`, executed from
fixture revision `34084933` with the frozen463 core. The source's draft header
records its pre-execution state; [manifest.json](manifest.json) binds its exact
executed bytes. Helper SHA-256 is
`f34c50630f4db5f52a723bd84f0083673214eee96586e2c8b565738a99c97a73`.
The helper linked in 12.52 seconds and both cases ran in 43.66 seconds. Those
durations are fixture timings, not database RTO or performance results.

Full [control](control.json) and [cleanup-first](cleanup-first.json) reports retain
all captured cuts, exact command/certificate states, metadata requests/replies,
actual per-store commit indexes/voter configurations and payloads. External
ACK/cut ledgers were fsynced. Complete [five-store topologies](five-store-topologies.json)
are explicitly reconstructed from the retained actual metadata entries/snapshots
and each store's observed committed generation; directly observed range
descriptors are independently checked for equality. They are not mislabeled as
additional live snapshot queries. Report copies differ only in JSON whitespace,
with raw and portable hashes recorded.

The original local DB roots/reports were retained, all files/directories fsynced,
and frozen read-only; [runtime-custody-manifest.json](runtime-custody-manifest.json)
indexes 171 files. No subsequent mutation or collision experiment touched them.
Two fixture compilation failures and their corrections remain in
[compile-attempt-history.json](compile-attempt-history.json). They were invocation
and shared-handle ownership errors, not engine failures. The successful build
and run receipts identify their own source/command scope.

Replay verification requires only the retained JSON:

```sh
python3 scripts/verify-relocation-handoff-characterization.py \
  docs/evidence/relocation-handoff-witness-2026-11-11
python3 scripts/verify-relocation-handoff-characterization.py \
  docs/evidence/relocation-handoff-witness-2026-11-11 --require-preserved
```

The first exits 0 to validate this historical witness and healthy control. The
second deliberately exits 1 because the preservation target fails. Both results
are retained in [verification-results.json](verification-results.json); a valid
witness is not a passing database correctness gate. RAM, swap, disk allocation,
I/O pressure and owned process-group limits were actively monitored, with low
CPU/I/O priority. [Resource statistics](resource-summary.json) record two I/O
pressure pauses totaling approximately 26 seconds for the owned process group,
followed by successful resumes; no guard terminated the run. Wall time includes that
pause. The successful runtime used at least 25.02 GiB available RAM.

## Unrepaired blocker and next test

An image-floor field describing the earlier copy does not capture this later
acknowledged transaction. Durable certificates on the old quorum alone do not
establish safe handoff into the final new quorum. Final catch-up/fencing and a
verified new-leader readiness/handoff boundary need their own implementation and
proof before automatic movement can claim acknowledged preservation. This
fixture is evidence of the blocker, not its repair.

The next smallest deterministic test is **proposed, not run**: in a fresh
cleanup-first instance, acknowledge a different new command on n3/n2 at the
derived next index, then return n1 with its retained old command at that index.
Compare command identities/checksums, payloads and equal-watermark repair,
followed by bounded recovery attempts. This would determine collision,
divergence detection and liveness behavior; none is assumed from the current
witness. Preserve the sealed witness roots and use new roots for that test.
