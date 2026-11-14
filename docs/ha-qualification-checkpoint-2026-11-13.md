# HA qualification checkpoint — November 13, 2026

Evidence cutoff: November 14, 2026, 03:51 UTC (November 13 in Los Angeles).
This is a source-pinned development checkpoint, not a production certificate.
The production gate is unchanged: zero lost acknowledged commits, correct read
consistency/routing, bounded fault-safe rebalance, and a sustained throughput
gain measured after complete convergence.

## Delivered main and qualification identity

GitHub main at this cutoff is **1.0.505-beta**,
`e25b3ba194c6f3ad73e2f393b68b175fbf1edd92`.
[PR183](https://github.com/nikoma/bicdb/pull/183) delivered client deadline,
uncertain-outcome and original evidence-cause fixes, plus narrow broad-CLI
evidence retention. All required checks passed. Its complete tree
`6e1547a07a2749be2946ba50f2028b74ac080b90` equals tested head
`99f1a05bce08ba0ad5f8a777a844317d6c7a6d2b`; the serial native job reports
212 passed/0 failed/1 ignored in961.23 seconds. The remaining-workspace job
passed127 reported test-suite results with no failed suite; that number counts
suite summaries, not cases. Both are existing hosted regression coverage,
not qualification of the separate prototype branches below. The merge produced
no release binary, published release or verified deployment.

The preceding504 merge, `3dc753edf9be3865ab8376657f99b14daceaf534`,
delivered documentation/evidence. Its complete tree
`197b2afa4f5431370be89dce1079037321934add` equals tested PR178 head
`54b325f69693b34b00c3c92325ad11ecd7dbffeb`.

The preceding main503 merge, `15e1dc84b9cad4cc8c70990ea2a919621d564f67`,
has complete tree `a4712298110429f1c0d7ce63876b200e42a874b3`, exactly equal
to candidate `c6a081a6026f4efd00d80f392c94b67f2f4e12`. That identity supports
attribution of the exact-source native repeat to that source. It does not turn
reused checks into new executions or explain the earlier four failures.

## Findings by evidence disposition

**Passed**, **failed**, **inconclusive** and **not tested** describe the stated
scope. An implemented path, passing hosted test, physical experiment, production
qualification and verified deployment remain separate states.

| Scope and source | Disposition | What the evidence establishes | Remaining limitation |
| --- | --- | --- | --- |
| Native main502 fifth-member positive data path, retained by main504 | Passed, bounded hosted tests | Real mTLS, one-process fixture orchestration; one range can reach a fifth-member leader, preserve earlier values and accept/read later writes. The tested native suite passed212/0 with one ignored. | No automatic cross-process relocation/recovery or physical capacity qualification. Interrupted transfer can remain safely unwritable. |
| Original c6 native suite, run43529104485 attempt1 | Failed | 208 passed, four failed, one ignored. Each original case, failing phase, raw log and source proof is retained. | Metadata acquisition errors, two initial-configuration timeouts and a lifecycle append timeout are distinct; not all occur after an acknowledgment. |
| Exact same c6 suite, attempt2 | Passed, CI gate only | 212 passed, zero failed, one ignored; same213 selected names/order and same ignored child. All four previously failing cases passed. | Different runner instance; contemporaneous load unknown. Other green checks were reused. No RAM measurements, external commit ledger or physical RPO/RTO. Original causes remain inconclusive. |
| Instrumented four-case diagnostic, runtime7ab through qualifier2b8 | Passed, diagnostic only | Four selected/executed cases passed. Lifecycle trace links three actual APP acknowledgments to three exact full-value Strong reads and six same-root reopens. |18 busy-latch record drops; global capture completeness explicitly false. Successful acquisition followed by surplus-worker cancellation is not proof of the original failure cause. |
| Four-thread CLI diagnostic, qualifier034 | Failed | Five cases passed and one failed at receiver PREPARE before the intended APP phase. The original first-ACK scenario passed in this diagnostic. | Five ACKs/eight Strong reads aggregate two fixtures; they are not one five-node rebalance or an explanation of the older first-APP failure. |
| Paired caller-budget controls, b18 versus505 baseline | Failed b18 / passed505, TCP controls | Identical expired-caller tests show one actual TCP/APP dispatch in b18 and zero in the synchronous505 baseline; healthy controls succeed. | Mock protocol exchange, no native database commit or HA qualification. |
| Inflight client integration1ce, run43538783755 | Failed |33 of34 controls passed. The sibling-cleanup control returned dispatched uncertainty but lost the original evidence-callback detail; later assertions were not reached. | Not an acknowledged-data-loss finding. Frozen failed evidence is separate from its source fix. |
| First-cause successor `cd366`, qualifier `521` | Failed compilation; runtime not tested | Both jobs report two E0308 errors in the new concurrent test's heterogeneous Result arrays; zero tests executed. | Original request body was not archived; the recorded Root command is explicitly reconstructed, not an original request artifact. |
| Corrected first-cause runtime `0c960`, qualifier `6dd` | Passed, component controls |37 compiler-listed cases equal37 actual passing results, zero failures/ignored,12.48 seconds; workspace all-target and consumer compilation passed. | Mock TCP/local mTLS and component checks, not native business commits. Tested PR183 head `99f1a05b` has the same Core tree. Its required broad checks passed separately and the exact whole tree merged as505; neither test scope is native scale-out qualification. |
| PR183 baseline01308, broad REST native CLI case | Failed; third write outcome inconclusive | Two positive writes returned ACKs and their full Strong reads succeeded; the third positive append, an empty object, returned QueryTimedOut. | No ACK or subsequent Strong read for the third operation. Its RPC journal was not retained by the original broad workflow. No durable-loss inference or reconciliation claim. |
| Native composition `3b`, qualifier `29ae` | Failed compilation; runtime not tested | Four concrete import/privacy compiler errors precede CI's90-minute cancellation. No test list or runtime execution was reached. | The old71 count was an unverified text inventory, not an executed cohort. |
| Compile successor `5a859`, qualifier `244` | Passed compilation; failed inventory guard; runtime not tested | Workspace all-target/consumer checks and the actual library test build passed. Rust listed82 tests; the guard stopped before runtime because its static71 expectation omitted11 existing macro-generated controls. | Zero runtime cases executed. All82 cases, including the omitted macros, remain required; no case or deadline was removed. Workflow-only successor `88ffe19f` executed that full exact compiler list separately and failed as recorded below. |
| Full82 runtime at `5a859`, qualifier `88ffe19f` | Failed |70 passed,12 failed, zero ignored,941.30 seconds. Eleven hot/data/term cases supplied a donor that was neither allowed leader as their initiator; the twelfth reached actual term mutation and received the newer version-fence refusal before an older error-message assertion. | The intended later cuts of the eleven cases were not reached. This is not an acknowledged-loss finding. Corrected fixture `dd8870bd` preserves production authority rules and business assertions; its new qualification is separate. |
| Corrected82-case fixture `dd8870bd`, qualifier `7dd8321e` | Failed | All82 executed:72 passed,10 failed, zero ignored,990.31 seconds. Workspace all-target/consumer compilation passed. The corrected actor and version-fence controls now pass; the remaining cases reach Registered NEW and fail its complete remote OLD/native/configuration image comparison. | Later named decision/policy/Joined APP/Strong/term cuts are not tested in those ten cases because their NEW prerequisite fails. Receipt-producer investigation is separate; no acknowledged-data-loss or completed relocation claim. |
| Two-populated-range component `c968`, qualifier `21669a25` | Passed, one component only | Exact1 executed/passed; full83-name inventory compiled/listed separately. Three APP ACKs across the fixture, four full Strong reads; an old Strong, fresh APP and new Strong on the unaffected range all completed while the selected range's actual exported Rows response remained held. | Transport hold occurs after export/DB guards release. Original request resumes unexpired; Rows.complete is false, no Finish/target install/second Joined move/controller/disk/physical/capacity or RPO qualification. |
| First S2 export-reference retirement `913bd4b`, qualifier `3d7b6a74` | Failed qualification; bounded partial passes | Exact15 executed:14 passed, one failed; whole97-name inventory compiled/listed. The OWN-expiry case panicked on an immediate busy diagnostic after its RPC timed out, before its post-state assertions. | That case's capture/terminal/pin invariant remains inconclusive. Whole-listener cleanup drained, but no tagged request completion or post-expiry state was retained. Other passes do not qualify the complete path. Shared OLD/target pins remain; no NEW quorum/decision/Open/Serving claim. |
| S2 discovery through complete Joined Serving | Not tested; source integration incomplete | The first target/export-reference wire slice has partial14/15 results above; full remote NEW quorum/decision/activation/Open producers and OLD-custody retirement remain absent. | Separate discovery, export-reference and hot-lifecycle fixtures do not establish the complete path. A private getter or manifest DTO cannot replace authority. |
| Second populated range onto the same Serving Joined target | Not tested; confirmed implementation gap | Source-pinned FIRST policy accepts exactly one local-voter range; original-image and dispatch paths do not provide a protected additional-range receiver on Joined. | Requires a genuine per-range receive/activation producer preserving the first range and native WAL/index history. A successful transport-stall component does not exercise this path. |
| Independent-machine four-to-five-node expansion | Failed original Azure qualification | Original failure, partial results and teardown remain the baseline. | No new Azure campaign, stable converged five-node throughput comparison or qualified physical RPO/RTO/percentiles. |
| Transparent PostgreSQL cross-range business transactions | Not tested as a delivered capability; implementation incomplete | Current SQL source review retains local gateway execution and an abstract multi-range protocol without a production coordinator. | No general transparent cross-range joins/snapshots or durable atomic multi-range SQL execution. |

## Raw custody and public evidence

The complete original native82 artifact is retained privately outside Git, with
ZIP SHA256 `bdf53f0eb21bf88397a14099f6a971460db1497104bcb799ae078b3d85c03561`.
Its 9,361 members include native DB/pages/WAL and per-fixture mesh-signing keys.
Those bytes are not suitable for a public documentation upload. The separate
logical projection and omission inventory must identify the excluded custody;
file hashes and copied metadata do not establish fsync, cold recovery or the
absence of secrets in arbitrary output. The two-range component's complete
original archive is likewise retained privately. Neither archive was opened
with a database engine or repaired during verification.

## Reproduction and next bounded tests

The [four-failure report](evidence/native503-four-failure-2026-11-13/issues.md),
[exact-history repeat](evidence/native503-exact-history-repeat-2026-11-13/issues.md),
[passive diagnostic](evidence/native503-passive-four-diagnostic-2026-11-13/README.md),
[CLI diagnostic](evidence/native-cli-four-thread-diagnostic-2026-11-13/issues.json),
[paired caller-budget proof](evidence/event-client-caller-query-pair-2026-11-13/README.md),
[original third-append failure](evidence/sdk505-rest-third-append-2026-11-13/issues.md),
[original full82 failure](evidence/native82-hot-initiator-failure-2026-11-13/issues.md),
[two-range transport component](evidence/two-range-transport-stall-component-2026-11-13/issues.md),
[corrected first-cause qualification](evidence/event-first-cause-37-2026-11-13/README.md),
and [failed34 cohort](evidence/event-inflight-34-failed-2026-11-13/README.md)
retain exact sources, requests, outcomes and portable verifiers.

1. Preserve the original213-case history when investigating the c6 failures.
   The successful exact repeat clears its CI gate but leaves the causes
   inconclusive. A future reproduction needs actual per-voter error/phase
   evidence; absence of a failed observation in a bounded diagnostic is not
   evidence that the failure cannot recur.
2. Retain the passing37 client controls as regressions for original callback
   details and monotone dispatch uncertainty even when a sibling or cancellation
   finishes first. Keep actual successes subject to their original deadline and
   identity checks. The exact tested505 merge is delivered; retain the failed earlier source evidence separately.
3. Preserve both complete82-case failures. The corrected fixture moved ten
   cases past witness admission to an actual Registered NEW image mismatch.
   Compare the original OLD receipt, exported image receipt and Finished reply
   at the same source/attempt before changing producers or validation. Keep
   authority checks, runtime deadlines, SDK endpoints and business assertions.
   For the separate15-case protocol timeout, retain the original RPC result
   before waiting for tagged original-handler completion under a diagnostic-only
   budget; no business retry or renewed expired authority is permitted.
4. Preserve the original broad CLI command/order/four-thread setting while
   retaining its existing RPC journals, operation ledger and narrow stopped-file
   diagnostics. Classify every APP separately. Require full-value Strong reads
   and reconciliation of uncertain outcomes; a later successful run alone cannot
   settle the original third append.
5. Implement the missing genuine protocol producers before claiming discovery
   to Serving. The [source-pinned gap](design/native-source-discovery-serving-integration-gap-3b4.json)
   identifies the boundaries. The [wire-producer contract](design/native-source-retirement-wire-contract.md)
   additionally identifies that current NEW and release producers consume local
   recipient owners, not authenticated remote phase observations. Normal and
   lost-Start-reply cases must preserve the original target attempt/session,
   genuine NEW/Open evidence and fixed-gateway SDK budget. All-member activated/
   Open continuity can still hold Frozen owners while obsolete OLD references
   drain; later released/armed Open gates FIRST Serving. Requiring Serving for
   the earlier retirement would create a dependency cycle. In-process owner
   references are not RPC proof. The [second-range admission report](design/native-joined-second-populated-range-admission-gap.md)
   pins the separate receiver/dispatch/policy boundary. Terminal registry pruning,
   Joined-as-future-donor admission, cold reconstruction and the automatic
   controller remain open.
6. After the supported automatic path is qualified, rerun the independent
   four-to-five-node physical experiment with its complete external attempt/ACK/
   uncertainty ledger. Separate relocation throughput from sustained throughput
   after convergence. No capacity-gain claim is supported by this checkpoint.

The [main audit](ha-horizontal-scaling-audit-2026-11-10.md) separates legacy/native
single-primary HA, distributed ranges, Cell HA and application fleets. None of
these development results broadens the other modes' contracts, establishes
production admission, or qualifies index/FTS/queue/audit parity during movement.

## Latest unmerged qualifier results

The corrected82-case failure is [run43550677120](https://github.com/nikoma/bicdb/actions/runs/43550677120),
Core job132312183515, qualifier `7dd8321e9a4acc27d74ee9477e53ce6a8b574666`,
whose Core tree `7bae84784dc10a5c54bb510ce92140be3117be4e` equals fixture
runtime `dd8870bd52f8e364f8c8e4bf74e58ee49880c239`. The original70/12 cohort
and this72/10 successor are different executions and remain separate.

The first15-case source-retirement failure is
[run43550673003](https://github.com/nikoma/bicdb/actions/runs/43550673003),
Core job132312172283, qualifier `3d7b6a74340c7068136f9f00b98573ad8e5df349`,
whose Core tree `22ed9d7f3f32852f921e22fccbeadfedc87661d0` equals runtime
`913bd4b4427711d542504d25d8d94c085a17a4c4`. Actual14/1 results took223.38
seconds. Its OWN-expiry case returned an RPC deadline error, then its immediate
state diagnostic failed with `range image: discovery source busy`. The captured
client parent had5000ms; the actual server parent was capped by the1500ms
network I/O timeout. The client-parent observation does not prove server-parent
liveness. There is no tagged request-completion/post-timeout state evidence.
Two distinct callback-expiry/cancellation controls passed, but do not settle this
failed case. Whole-listener cleanup completed; it is not per-request proof.

The original full S2 artifact14096837323 is preserved privately outside Git,
SHA256 `7bbdcb64911cb71c6923a64e9fd50dddcd2be00956d67b04d5a627abea5981d7`.
These linked latest results are checkpoint summaries; their complete public
logical projections are not part of this506 packet. The original full82 and
transport-component public projections above are included and portable.
