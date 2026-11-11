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


| [Certification preflight and recovery order](evidence/native-recovery-order-2026-11-11/README.md) | `9eace333`, `021ba5c4`, `7d2f84a5`, `e68fa21f`; full IDs in receipt | **Failed**, then separate **1 PASS**, **22 PASS**, and large-case **failure** | Pure gap refusal formerly poisoned durable observations; larger fixture first exceeded complete-obligation reply bound |
| [Complete obligation capacity](evidence/native-complete-obligation-capacity-2026-11-11/README.md) | `d2c202202943f047618784994b929a49738cd627`; package 1.0.471-beta | **21 distinct PASS** | Actual copied 1025-ACK fixture retains every obligation and all ACK digests; compacted gap, native catch-up and activation remain unproved |
| [Generic repair failure baseline](evidence/native-generic-repair-baseline-2026-11-11/README.md) | `035fd22f40028a4d7c23e4a086af19791674677f` | **Failed** positive catch-up test | Generic repair reports success/raw Applied while native history becomes unknown; not an Azure attribution |
| [Late Prepared failure baseline](evidence/native-late-prepared-baseline-2026-11-11/README.md) | `e3572fc655fe0dff9e634f56c5c01019ab8de173` | **Failed** positive completion test | Activated+Prepared available quorum cannot complete in that earlier source; ACK remains readable |
| [Native abort failure baseline](evidence/native-origin-abort-baseline-2026-11-11/README.md) | `eafa6949a78ea4e97f1ea3271ff97f2de27d20c6` | **Failed** positive continuation test | Failed prepare leaves raw Aborted1/native0; restored quorum still cannot ACK the next transaction |
| [Protected activation decision](evidence/native-activation-decision-2026-11-11/README.md) | `7b36642b444dc57051dde1fbb369f15ddb3cc3e1`, then separate `1cd28523c2f8a3970635dcae4ca43977727b1afe` | **10 distinct PASS**, then **1 separate PASS** | Protected prior decision repairs original late-follower test; later genuine ACK2 and late activation of old R1 tested separately. Late R2 catch-up and strong reads not tested |

| [Sparse committed-decision source recovery](evidence/native-sparse-source-tail-2026-11-11/README.md) | `0d58359b07c31b38ed432e0a72ef7996ee2544b7`; package 1.0.471-beta | **3 distinct PASS**; separate codec pass at `1e6e3a0a`; two prior failures retained | Actual lagging voter stores full committed1026 without data/raw advancement; current quorum lets source reach native/Ready1026 and 1025 ACK digests match. Behind image repair/cold sparse reissue not tested by this cohort |

| [Native ordinary Apply and catch-up](evidence/native-normal-apply-2026-11-11/README.md) | `18723cf4f1ddbb01e37f3762067b30000707cadb`; separate prior `87f1da00` cohort; package 1.0.471-beta | **5 distinct PASS**, with **7 earlier origin PASS** kept separate | Actual mTLS retained-command catch-up, new writes, duplicate preservation and wrong-actor/domain refusals. Original exact before-Prepare2 regression not rerun here; successful DB archives/external ACK ledgers not retained |
| [Local native read observations](evidence/native-read-observation-foundation-2026-11-11/README.md) | `6d07ded1ec9e44d688f2263a7804c236b3aa9f14`; package 1.0.471-beta | **9 distinct PASS**; earlier compile and fixture failures retained | Actual local native R/H and target-local B, current-open activation, raw-ahead refusal, cancellation and gate invalidation. No authenticated read RPC, read quorum, admitted host/store identity, MVCC read barrier or StrongGet in this cohort |

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
of those older cohorts establishes a successful ordinary write after recovery.
The later, separately pinned 1cd follow-up does: actual nodes 0 and 2 ACK write 2,
then nodes 0 and 1 complete/activate the old R1 decision. Node 1 remains honestly
at R1, without row 2 and without current Ready; this is not an R2 catch-up result.

No bundle establishes physical scale-out, sustained throughput gain, hardware
RPO/RTO or production p50/p95/p99. The activation receipt exercises a trusted
local Runtime activation API after actual authenticated completion observations;
it does not establish an incoming Activate endpoint or operator/controller
integration. Ordinary native Apply and retained-command catch-up have the separate local/mTLS
component evidence above; this does not establish the complete recovery/serving
lifecycle. Complete audit/sync/broker effects, StrongGet and fault-safe
four-to-five-node relocation remain outside these claims. The late follower's current R2 catch-up remains **not tested**;
the original Azure visibility cause and convergence cause remain **inconclusive**.
All failures above are private-prototype findings unless a receipt explicitly
identifies a public runtime; no source-level fix alone establishes qualification. General distributed SQL and atomic
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

The [native no-data abort-history proposal](design/native-abort-history-recovery.md)
is design only. It requires actual completed transaction cleanup, permanent
no-commit promises and a real abort quorum before publishing canonical abort
history with unchanged local data visibility. Raw Aborted alone is insufficient;
no implementation or executed acceptance follows from that document.

The [interrupted current-image resume review](design/native-interrupted-current-image-resume-review-2026-11-11.md)
is a separate **source review**, not an executed recovery result. It identifies
an admission cycle after all owners reopen a partially installed image: the
ordinary target fence refuses the durable install intent, while obtaining a new
source cut needs the currently unavailable data quorum. A persisted intent is
a refusal fence, not evidence of fresh quorum authority. The review preserves
the requirement for fresh InstallingFence observations and revalidation of all
target obligations before any destructive continuation. It does not establish
durable-data loss or explain the original Azure run.

Local native birth is likewise a fact rather than voter-continuation authority.
A future strong coordinator must compare every counted voter's actual birth
against immutable, quorum-committed enrollment for that node incarnation. A
recreated empty database cannot become an old voter merely because its node
name, epoch, certificate and bootstrap specification match. Missing enrollment
must refuse admission; replacement requires a new incarnation and learner
repair. This host admission protocol and its physical replacement tests remain
**not implemented/qualified by the local observation receipt**.
