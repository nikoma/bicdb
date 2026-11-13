# HOT original-origin repair: first passing component, November 12, 2026

**Scoped component passed; production qualification remains incomplete.**
Three actual NativeHosts/RF3 use pinned TLS in **one Core test process**. The
original origin survives a real post-admission/pre-local-WAL failure; its actual
Core result is Unknown without a sequence, its transaction cleanup and normal
APP-handler exit finish, and two peers have materialized command 2. Autonomous
process-owned maintenance subsequently materializes the origin's committed tail,
opens recovery, and permits the next actual ACK and exact Strong values.
This is not three CLI processes, five-node movement, VM qualification or a
claim about the original Azure cause. The implementation is an unmerged candidate.

| Exact source and run | Actual result | Classification |
| --- | --- | --- |
| `a96bbf958b8f366d41bbe31b2bc776f1fb6bcbe4`, [controls 43334746500](https://github.com/nikoma/bicdb/actions/runs/43334746500) and [baseline 43334749077](https://github.com/nikoma/bicdb/actions/runs/43334749077) | Both all-target and Core helper builds fail E0599: `RecoveryHistoryObservation.require_live` does not exist. Listing/execution never runs; consumer checks skip. | **FAILED COMPILATION**, zero selected-test executions. |
| `8334a99e666970263c2d5a4192b5c9ce61d26677`, [controls 43335418131](https://github.com/nikoma/bicdb/actions/runs/43335418131), Core 131590289083 | 5 passed, 1 failed, 965 filtered, 5.23s. | **FAILED COHORT**. The writer-replacement case stops at an early diagnostic assertion: an ended Weak registration produces JSON null, not false. Its later refusal/cleanup assertions are **NOT EXECUTED**. |
| Same `8334…`, [unchanged baseline 43335421681](https://github.com/nikoma/bicdb/actions/runs/43335421681), Core 131590303747 | 2 passed, zero failed, 969 filtered, 14.38s. | **PASSED scoped original-origin fault and arm-lifetime controls**. |
| `60c47ae29d84371f6815cdedf1fb0b8ffa866d75`, [whole corrected controls 43336846793](https://github.com/nikoma/bicdb/actions/runs/43336846793), Core 131595098465 | 6 passed, zero failed, 965 filtered, 6.74s. | **PASSED whole control cohort**. Original writer/Store-slot, queued read handoff, cancellation, process ownership and Activate-denial assertions execute. |

The last three runtime cohorts' all-target and isolated no-default consumer
checks pass at their own exact sources. `8334…` to `60c…` changes **only** the
control test file: production bytes are identical. The original fault test is
byte-identical to source `721c329de4196aeef96604642c439fe7d8ddb0bf`, whose
required terminal repair gate failed. The compile correction adds `.fence()`
before the existing history observation's lifetime check. The subsequent control
correction proves actual registration-ended, directory-alive and lifetime-refused
facts, then retains the original reservation/native-state/replacement assertions.
Neither correction raises a deadline or changes a production predicate.

Coverage is **eight distinct tests**, across **14 runtime executions: 13 pass,
one preserved failure**. The earlier compile failures add no runtime executions.
The six controls' binary digest was not separately recorded; the baseline's
actual self-hashed executable is 137,089,984 bytes, SHA-256
`eed6f9e49f7eb9f44f8310cb6194349abe898613230376c7e3b777cbb82d1e3d`.
The executable itself is not retained in this packet.

## Actual fault and complete workload ledger

The original 339-row journal is retained, byte-for-byte, inside
[the baseline artifact](artifacts/baseline-8334.zip). It contains nine API intents
and nine outcomes, plus 310 observer records covering **77 physical attempts**:
six APP, 20 Strong and 51 successful full topology discoveries. Complete frames
and request-ID parity are checked for 76 replies; one Strong attempt has a
partial/error path, never a successful absence. Every discovery must actually
contain `Ok.BootstrapSnapshot`, generation 1, and a source birth matching that
snapshot. An error reply cannot be counted as a successful snapshot.

- Old command 1 is externally ACKed and read exactly at native R1 / typed local
  B1. Its full object, nested null and boolean values are retained.
- The first fault is actual transaction 11 / command
  `01a1d246-41e4-76f0-81c9-a563d5aa3e76`, checksum
  `a101183ec08c06342b11ddac14d7f516fdd552c60ef1aa11dc14e7c903bfb9c4`.
  The immutable first probe records CoreUnknown, sequence **None**, matching
  transaction/ticket, completed release callback and normal handler exit.
- The origin is native R1/Pending2, not invalidated, with actual raw CQ2. Both
  peers separately prove native R2 / local B2, the exact command and full old/new
  rows, common H0 and H2. Raw CQ alone is not treated as native materialization.
- The fault API makes four actual APP attempts: first CoreUnknown, then three
  recovery-incomplete errors. Its aggregate result is **NO USER ACK, DURABLE
  OUTCOME UNKNOWN**, with `no event cluster member is reachable` text. Those
  replies do not establish all hosts offline or rollback. No later SDK retry
  ACKs this fault call.
- Two immediate Strong calls time out, with no successful absence. In the first
  subsequent health round, old and committed values return exactly at R2/B2,
  a new APP is ACKed, and its full value returns at R3/B3. The first Unknown
  remains unchanged; the committed Strong result is not a retroactive user ACK.

The raw Core error includes generic advice to “Reopen the database.” That text
is retained unchanged. It is an **operating-guidance gap** for this live-original
repair: cold/replaced-origin recovery is not qualified here. It is not new
behavior or a tested instruction to reopen this native root.

The workload observer is attached before workload RPCs. The first direct
Bootstrap call records its logical request/reply but explicitly has
`bootstrap_raw_frame_covered=false`. The controller's internal recovery traffic
has phase/producer traces, not a complete independent internal wire ledger.

## Autonomy, clocks and storage scope

The unchanged test starts all three real automatic-metadata supervisors after
DataOpen and **before** the old ACK and injected fault. Those retained process
owners start the separate HOT worker. There are **zero** supported-registry or
supported-metadata manual-maintenance records before success: the first health
round breaks before that fallback code. The trace has three actual calls for
each Fence, Certify, Apply, Prepare, Complete and Activate phase, under the same
command/session, followed by one successful Publish with 560ms left in its
original five-second attempt. This is source/trace correlation, not a grant
from a phase label.

The health receipt is **1,523,718,116ns**; the terminal sample is
**1,523,989,554ns**, inside the unchanged 20s gate. That window starts **after**
the two unsuccessful post-fault Strong calls, each about 2s. The 14.38s suite
includes setup/cleanup and is **not RTO**. Journal wall-clock samples place the
healthy receipt about 7.546s after arming and 5.545s after handler-exit observation;
those are observed journal intervals, not a physical recovery bound. The client
2s setting is per RPC/attempt, not a universal hard logical-call bound.
Cleanup reports no errors. There is no manual recovery, genesis reset or
counter seeding in the successful path. The immutable raw fixture flag
`no_engine_reopen_or_repair=true` predates this controller: it records no direct
test-driven reopen/repair call. The autonomous controller did perform native
engine recovery; that raw flag must not be read as no engine mutation.

The three original artifact ZIPs are **logs, compiler/source records and test
JSON only**. No native database, WAL, range catalogue, executable or private TLS
key is retained here. The runner's original DB root is a path-only record; it
was not portably copied or replayed. The earlier Unknown is not reclassified
from a stopped metadata file or row-presence inference.

All three recipients are reachable. One-peer-loss, cold/replaced origin, changed
epoch/domain, broader Strong prerequisites, genuine fifth-node assigned-image
and automatic relocation, public process acceptance of this repair, sustained
load/capacity and physical RPO/RTO remain open. Source, a binary, a release and a
deployment are separate states. Production admission and the original Azure
FAILED classification are unchanged.

## Offline verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-hot-origin-recovery-2026-11-12/verify.py --repo /root/bicdb --tamper-tests
```

Without `--repo`, byte/manifest/raw-log/ZIP/full-journal checks still run;
repository-wide source parity is explicitly **NOT RUN**. With it, the verifier
reads the pinned Git objects, compares the exact whole-tree delta, original
fault body and supplied control patch. It never builds, opens an engine or
accesses the network. The eight semantic mutations reject dropped Unknown,
retroactive ACK, manual tick, false absence, missing attempt, incorrect executed
source, an error counted as a snapshot, and a wrong native peer prefix.
The manifest establishes retained-byte integrity, not execution authenticity.
The [producer scope and remaining gates](../../design/native-initial-committed-tail-56f-source-audit.md)
remain separate from this execution receipt.

## Retained byte identities

The manifest covers 10 original full job logs and their capture indices, three
original ZIPs, exact source bodies/patch, the complete derived outcomes, and
this verifier. All four passing-source Core/compile raw logs are retained;
the other six preserve the earlier compile and control failures.

| Raw passing-source job | Bytes before gzip | SHA-256 of full raw log |
| --- | ---: | --- |
| 131590303747 | 2,726,767 | `783ed47a38fd58c49861eb9efce8c3cf1de6d233224e86cc9a6ad45b617e6bbf` |
| 131590268686 | 2,341,062 | `eae6ed8f43cdb8734878929b1e48ef8445f84a3aecd5341768e28bcfcf52692e` |
| 131595098465 | 2,818,112 | `02734a19dddea338cca8ffec6916227d07b53ec521daa3a340cbd38fd7217715` |
| 131595062512 | 2,341,141 | `1d0cc6cef4807d673c67d92d9d9337104e560f1665bd83a2d9e8db22aea143b5` |

| Original GitHub artifact | Bytes | SHA-256 of ZIP |
| --- | ---: | --- |
| [13996624381](artifacts/baseline-8334.zip) | 156,798 | `7a56b40fd316fcbf5f3b94f43159bf123672fbc8b1ef57c7f8fb9992bc241120` |
| [13998551075](artifacts/corrected-controls-60c.zip) | 109,735 | `519d375efcd908753662f31acb2ac2f102e1ea212f6d6f5deacf27edf93ef369` |
| [13996644806](artifacts/first-controls-failed-8334.zip) | 108,987 | `ff2dfb87b589b4a79ce7665ee193b39f58a74f7009d82be3aa259860703b33e0` |

The unchanged full journal is 1,353,902 bytes, SHA-256
`b5c075a797403821d5dab865a396115c86e2ffd0a164488333c5fd9d5cdb073d`. ZIP member allowlists contain
no native storage files or private TLS key material. Public test values and
certificate fingerprints are evidence data, not authority.
