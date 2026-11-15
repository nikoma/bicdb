# Native runtime corrections after the failed source7e99 run

This is a source-composition checkpoint before validation of the assembled candidate. It starts from **1.0.520-beta / `7e99c9c96970b78a44402c975627c64e001adffa`**, whose complete CI run failed **396 passed / 55 failed / 1 ignored** in the serial native job. Its other component and application passes remain scoped to that exact source. The [complete baseline evidence](native-520-regression-findings.md) and original failed Azure qualification are preserved unchanged.

Subsequent assembled source **`e2adb410befd41bd475f015fd6590c33d1afaa4f`**
failed its completed normal CI run on two Core library diagnostics and one
child-module-resolution error. Core/native tests did not execute; remaining
workspace passed 564 component cases before dependency compilation failed.
The [exact successor findings and three-file compiler correction](native-520-regression-findings.md)
are recorded separately. Neither the corrections nor baseline source7e99's
passing component jobs establish a passing assembled runtime result.

Later source **`d3af39c107951802a612d31b42e70fd4bc90e71c`** completed with
formatting/SQL/PGwire/Rails passes but a Core/native test-compilation error and
a real CLI availability failure after metadata-only fifth-voter addition.
The first-allocation unit's strict pending getters also affect the existing
v1 registry publisher: its origin preflight calls that getter while its own
publication is pending. Fresh retries self-refuse too. No DATA move was
attempted; Strong calls returned Unavailable rather than absence. The separate
validator/test-cut corrections do not fix this production availability defect.
An original-owner, actual-outcome-safe registry repair must preserve strict
ordinary APP/Strong fences. See the [complete d3 findings](native-520-regression-findings.md).
Neither predecessor component passes nor source-only composition establish a
successful assembled HA result.

At the original pre-e2 composition checkpoint, the changes below were **implemented or defined in source and independently source-reviewed; assembled compilation and execution had not yet been attempted**. The subsequent failed e2 attempt is recorded above. No successful assembled compiler/runtime result is established. Formatting, source-preservation, version identity and documentation links are separate checks. No source-only correction is labelled a passing regression, physical qualification, release or deployment.

| Source unit | Exact unit commits | Implementation and evidence boundary |
| --- | --- | --- |
| Final-WAL control | `2e85dac09b1b56cc7ccc5da183804910354815d3` | [Numeric committed-XID assertion](native-7e99-first-final-xid-control.md); producer and all remaining assertions unchanged |
| Fresh local catalogue Open | `c069309d936617a13623ff401f462b91773e3e3a` | [One full same-owner/domain validation](native-hot-open-combined-domain-7e99.md), preserving later foreign/native/gate/currentness checks and original 1500 ms window; two new native component definitions |
| Metadata socket-probe pacing | `bf8908848d14c128b3d0cc865faaf37765eac72a`, `b22c2111a3c68ad3e4cffe5fdbaa907e36925e9b` | [Paced existing reset/refused retries](native-metadata-probe-exhaustion-7e99.md) within the same context, deadline and four-attempt cap; two real TLS provider controls selected by Core, not the serial native-host selector |
| FIRST request waits | `48d4fbc68d456ea41027a091a206eacbae655cb0`, `a9ee09823820ebd10ef71f7e12b454dc735af299` | [Guard-free finite service/State/OLD waits](native-first-original-request-waits-7e99.md); same actual owners and full predicates, original OWN cancellation linked, ten component definitions; generic instantaneous APIs remain strict |
| First allocation epoch | `b34e60bbc984ce5ab1f9c0fadb88b3fa1029fd2f`, `96f811e664a3ad21c85b7852691dba6bb84325bd`, `68c9225ccd8f8cff7cb1d7ef56a4580478be78d2`, `9e5f1be019529e6015c7840b5c0ae97c5252d351` | [Same live writer audit and recovery](native-live-allocation-first-lane-source-unit.md), exact local native predecessor and actual native serving configuration; status/cancellation/error-format corrections retained; six component definitions with separately declared scopes |
| Allocation publication refusal | `0a691da6e4cb00322a6836e7ffb33da85c9b8da2` | [Distinct actual allocation publication guard](native-allocation-publication-atomic-refusal-source-unit.md), preserving full checks outside the short guard and atomic health refusal inside; one injected same-health component, not a storage IO fault |
| Uncertain catalogue test arming | `20d77f2c87f7ea77f5f07066df84b3dd6ae69eb5` | [Test-only retained uncertain custody](native-7e99-catalogue-uncertain-control-arming.md) for fault codes 7/8; no production getter, persistence, uncertainty or assertion changes |
| Original bridge budget diagnostics | `e076921b4a9add32cff55d5e395bb4c8e79be655`, `9f2695bf2fa613f19201fab3f0dd6e768bee8907` | [Passive original phase clocks and retained errors](native-remote-bridge-budget-evidence-7e99.md); ten original bridge RPC deadlines remain unresolved; fallible diagnostic output preserves original errors |

The source units distinguish three facts: a current range descriptor can be admitted by the exact audited writer policy; a leader can have a generic recovered-epoch marker; and the actual native serving configuration can satisfy that descriptor. A changed allocation's APP/Strong paths and status must require the latter checks rather than treating a policy publication or generic marker as readiness. Strong reads still need their own fresh metadata/data proof and actual view; a status response is not a read barrier.

## Composition and preserved boundaries

The request-wait unit factors the original hot maintenance State body into a helper that checks the actual retained State pointer. The allocation unit adds real work to that same body. Their two manual conflict resolutions retain both modules and the complete allocation body inside the same-State helper, with explicit immutable/mutable dereferences of the actual MutexGuard. Generic admission still uses instantaneous `try_lock`; only the distinct finite request entry waits. Manual registry and maintenance share one child bounded by the original caller and five seconds, anchored before either operation. A wait cannot obtain a new job, reset a cursor, replay source effects or renew a Role/OWN window.

The later atomic-refusal unit and formatter correction meet in one test-evidence JSON hunk. The composition retains both new health-refusal facts and `first_error_kind`. The production original Result, candidate, Known/Unknown outcome and XID remain unchanged. Static error classes are bounded diagnostic DATA, not complete original error messages or a full outcome ledger.

Source-preservation checks compare each independent unit's unchanged files and invert the declared composition-only edits in `initial_data.rs`, `hot_recovery.rs` and the FIRST parent module. The only existing assertion correction is the separately reviewed numeric-XID contract. Original fault counts, timeouts, unavailable/error assertions, SDK two-second windows and full later SDK/Strong tails remain. New unit controls do not substitute for those tails or independent application acceptance.

## Finding classification and outstanding gates

| Finding or claim | Classification at the original pre-e2 source checkpoint |
| --- | --- |
| Exact source7e99 native acceptance | **Failed**; immutable original results retained |
| Source formatting, preservation and relevant documentation checks | **Passed within their recorded source scope**; these do not compile or execute Rust |
| Corrected FIRST/native/provider/health controls | **Not tested** as assembled source |
| Ten original bridge RPC deadline failures | **Failed** in baseline; exact phase cause **inconclusive**, new diagnostics not yet executed |
| Original Azure unreadable acknowledged records' cause | **Inconclusive**; these source changes do not reconstruct missing original request/route/watermark evidence |
| Source7e99 failures' acknowledged-data-loss implication | **Inconclusive**; failures do not by themselves establish durable acknowledged-data loss |
| Automatic four-to-five-node convergence, read correctness during all relocation faults and capacity gain | **Not tested** by this composition; the original Azure qualification remains failed |
| Current artifact physical RPO/RTO and p50/p95/p99 | **Not tested**; no certified numbers inferred from component timing |
| Production qualification or verified deployment | **Not tested**; admission gate unchanged |

The first-allocation lane is not a repeatable scale-out product. RegistryAfterAllocation, changed-cohort continuation, retained original jobs/debts, terminal warm-custody reclamation, shared history/capacity bounds, source loss, coordinator replacement and populated cold restart remain open. No automatic controller/source-completion integration or general cross-range SQL/coordinator is delivered by this batch. The independently reviewed future Source/ALLPolicy/cold protocol units are excluded.

After that separate compiler correction, the next validation is a normal exact-head PR CI run, preserving every useful job to natural completion and retaining complete original logs/artifacts. A passing subset, an intended fault not reached, or a timeout relabelled as a successful refusal does not clear acceptance. Physical requalification requires a separate pinned binary, complete external attempt/ACK/uncertainty/retry ledger, converged placement, correct read consistency and fault recovery, then sustained throughput measured after convergence at the same latency/error budget. No Azure resources are provisioned by this source checkpoint.
