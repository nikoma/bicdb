# An acknowledged RF3 write can require its unavailable old leader

**FAILED:** the deterministic one-leader-loss recovery target on unmodified
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b`, **1.0.463-beta**.
This finding is independent of the original Azure experiment. It establishes a
specific recovery-availability defect; it does not demonstrate lost data or
identify the cause of any Azure symptom.

The standalone [fixture](../../../scripts/experiments/range-certificate-cut.rs)
links the frozen optimized core (`dc714b001a0fe8758473c2c4ef80158474d7426e16630926edb65b6cc35f0356`).
It runs actual transaction admission, replica handlers and leadership recovery.
Transport and authority publication are scripted; three local paged databases
use 8 MiB pools and **fsync=false**. This is a source-level fault cut, not physical
HA qualification. The helper links at opt0, without rebuilding the engine;
[build.json](build.json) records settings, hashes and measured RAM headroom.

1. All three voters prepare one exact command.
2. `n0` and `n1` persist provisional decisions; the `n2` commit RPC fails.
3. `n0` persists its certificate; the certificate RPC to `n1` fails.
4. The real transaction nevertheless returns success. Probes show the same
   command/checksum at `n0=Applied/resolved1`, `n1=Committed/resolved0`, and
   `n2=Prepared/resolved0`.
5. Only `n0` becomes unavailable. Both surviving voters are reachable, prior
   phase faults are cleared, and scripted epoch2 authority names `n1`.
6. Three recovery attempts each refuse the ambiguous index1 tail. The target
   assertion fails with exit101; exact probes, command/payload, replication calls,
   published topology and errors remain in [red.stdout](red.stdout).

The exact refusal is `ambiguous provisional decision for r1 index 1: 1 durable
decisions, 1 unavailable or compacted voters`. It is correct to refuse ambiguity:
this surviving history is also compatible with an uncertified provisional
outcome. The original command still exists on the unavailable leader. No
rollback or data-loss conclusion follows from this test.

| Finding | Status |
| --- | --- |
| Every prior ACK recoverable after only one RF3 leader loss in this cut | FAILED |
| Ambiguous surviving history refuses unsafe apply/abort | PASSED |
| Data actually lost | INCONCLUSIVE; not demonstrated |
| Physical fault behavior, metadata election and Azure causation | NOT TESTED |

The relevant [admission path](../../../crates/bicdb-core/src/distribution_range_consensus.rs)
counts a provisional decision quorum, persists the leader certificate, then
best-effort certifies other voters before returning a ticket. Recovery applies a
certified tail or a decision quorum and otherwise refuses ambiguity. Merely
adding a read barrier cannot repair this missing acknowledged-write recovery
proof.

The smallest correction to investigate is requiring durable certificate evidence
on a voter quorum before an application ACK. A failure after an irrevocable
certificate must remain an **unknown outcome**, preserve recovery fencing and
never masquerade as a definitive rollback. Required follow-up regressions include
this cut, lost certificate replies, one-leader-loss recovery with a healthy
surviving quorum, and continued write progress after recovery. Range membership
replacement/fencing still needs its own proof.

Run `python3 scripts/verify-range-certificate-cut-evidence.py` to validate the
retained failed evidence and source linkage. It intentionally does not execute a
new database experiment. `<artifact-target>` paths in the portable build receipt
must be replaced with retained dependency locations to relink. Raw local files
remain under `/tmp/bicdb-p0-range-certificate-cut-evidence`; the export replaces
local task paths only and retains all measurements.
