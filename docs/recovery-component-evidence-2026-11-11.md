# Native recovery component evidence — November 11, 2026

These are retained local tests of experimental source compositions. Publishing
this evidence does not merge their engine patches, deploy a binary or qualify
HA. The source version of this documentation update is separate from every
historical tested version below.

| Bundle | Exact experimental source | Retained result | Boundary |
| --- | --- | --- | --- |
| [Fresh recovery receiver foundation](evidence/fresh-recovery-receiver-2026-11-11/README.md) | `bcddd35a` equivalent to pinned `9979638e` plus retained patch; package 1.0.466-beta | 44 distinct focused tests, 45 executions | Fresh roles, fences, quorum observations, native local physical proof and mutation ownership; later historical Apply/RPC collection excluded |
| [Native catalogue adoption and historical replay](evidence/native-catalogue-historical-recovery-2026-11-11/README.md) | `9e37d812369de61bb9489ce5415d73d47fdcef35`; package 1.0.471-beta | 38 distinct focused tests | Actual image catalogue adoption and fresh local WAL replay after checkpoint/reopen; recovery still blocks ordinary new writes |
| [Coherent participant source composition](evidence/recovery-participant-compose-471-2026-11-11/README.md) | `5d70669eec9fcea8f9963894ef73d243ef9d208f`; package 1.0.471-beta | 55 distinct focused tests | New recipient RPC collector and recovery-to-installer lease compile; neither new path was exercised in this bundle |
| [Local preparation and authenticated receiver](evidence/native-prepared-receiver-2026-11-11/README.md) | `d440c58ebc962f1e62374d86faecbfcc051e2bbf`; package 1.0.471-beta | 40 distinct focused tests | Actual mTLS historical receiver and owned lease components; durable local preparation still grants no NativeReadyQQ or RecoveryComplete |
| [Nonempty current-voter repair](evidence/native-rf3-current-repair-2026-11-11/README.md) | `33ffcf88a470a7f267838561d1b8f04d9b1f5820`; package 1.0.471-beta | 10 distinct focused tests | Actual local RF3 source/copy/history/catalogue repair and cold MVCC read; recovery and image refusals remain |
| [Participant and readiness integration](evidence/native-participant-integration-2026-11-11/README.md) | `1c91526c`, `fd9dea6d`, `c8859805`; full IDs in receipt, each package 1.0.471-beta | Separate cohorts of 10, 38 and 8 tests | Historical TLS and owned lease; local completed-frozen producer; then actual Ready TLS with a trusted local quorum collector |
| [Recovery composition](evidence/native-recovery-compose-72-2026-11-11/README.md) | `d52e73726edaedc90d1a5d134f205e99c7c46d0a`; package 1.0.471-beta | 72 distinct focused tests | One exact source cohort; incoming Complete and activation excluded |
| [Serving/image failure baseline](evidence/native-serving-image-baseline-2026-11-11/README.md) | `1978a0ea8eec099ab5285c7ff5d30118ae5c1825`; package 1.0.471-beta | Expected failure: 0 passed, 1 failed | Retained serving record A blocks physical readiness after compatible image B; separate earlier wrong-recipient fixture also preserved |
| [Authenticated frozen completion](evidence/native-completion-participants-2026-11-11/README.md) | `cad84207e8a4329db47449e43f658325d8f8cf55`; package 1.0.471-beta | 66 distinct focused tests, 67 executions | Actual incoming Complete/readonly TLS, post-persistence canceled socket and floor-preserving image controls; state remains NativeCompletedFrozen |

The bundles overlap; their counts must not be added. Each retains commands,
resource samples, warnings, prior failed attempts and source bytes or a complete
patch against a pinned public source. The 44-test bundle includes its exact
source archive; the 55-test verifier can independently reconstruct its source
from public base `b0e2498842be7b01e355bb8da1cc249ff16be056` without modifying the
supplied repository. The source archive includes existing public TLS test
fixtures, byte-for-byte checked against that public base; it contains no live
member keys or cloud credentials. Verification checks retained evidence and does
not rerun Rust or independently attest execution.

Transport coverage is specific to each cohort. The earlier local historical
Apply scenarios use actual fsynced native stores, quorum observations and local
WAL callbacks; separate provider TLS tests do not make those scenarios remote
receiver tests. The 40-test and later participant receipts execute actual
authenticated historical receiver collection on their own sources. The Ready
TLS cases use actual peer messages with a trusted local collector. The 66-test
receipt additionally executes incoming Complete and readonly completion TLS.
A declared metadata epoch/leader is not an observed automatic election. These
later receipts do not extend the earlier 55-test compiled-only boundary.

The [72-test progress snapshot](native-recovery-progress-2026-11-11.md) retains
its observed public-main timestamp and subsequent failed-image finding. The
separate failed baseline and later 66-test source preserve both sides of that
counterexample: the original failure remains failed, while the later source
keeps A as a historical floor and obtains a new closed quorum for B. The GREEN
witness retains assertions and logs but no post-test database archive. Neither
source establishes a successful ordinary write after recovery.

No bundle establishes physical scale-out, sustained throughput gain, hardware
RPO/RTO or production p50/p95/p99. Actual incoming RecoveryComplete is exercised
only to the frozen state in the 66-test receipt. Ordinary new writes after
recovery, serving configuration activation, ordinary remote native Apply,
complete audit/sync/broker effects, StrongGet and fault-safe four-to-five-node
relocation remain outside these claims. General distributed SQL and atomic
multi-range transactions are not supplied by this component evidence. The
[HA/scaling audit](ha-horizontal-scaling-audit-2026-11-10.md) and
[Azure results](azure-scale-out-results-2026-11-11.md) remain the operational
qualification record.

Recheck the self-contained bundles from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/fresh-recovery-receiver-2026-11-11/verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/fresh-recovery-receiver-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/recovery-participant-compose-471-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/recovery-participant-compose-471-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-prepared-receiver-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-prepared-receiver-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-participant-integration-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-participant-integration-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify-native-rf3-current-repair-evidence.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p test_native_rf3_current_repair_evidence.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-recovery-compose-72-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-recovery-compose-72-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify-native-serving-image-baseline.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p test_native_serving_image_baseline.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-completion-participants-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-completion-participants-2026-11-11/test_verify.py
```

The catalogue/replay bundle retains its exact manifest, compressed source
snapshots, public-base patch, prior-failure reverse patch and reconstruction
receipt. Its README describes the source reconstruction and the original fixture
budget failure; both the failure and correction remain preserved.
