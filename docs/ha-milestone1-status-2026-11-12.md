# HA Milestone 1: revision-scoped status, November 12, 2026

**Production qualification remains incomplete.** The metadata repair merged into main as **1.0.485-beta**,
`e3fa39974d23c13d6212d5b94ab84437271c50bf`, merged the metadata Append
frontier repair in [PR 162](https://github.com/nikoma/bicdb/pull/162). The exact
tested candidate was `ce02303676fe5fe730ffd7fc822a3a27ee8189aa`. The merge has
the same tree. A source version is not a published binary or verified deployment.
Evidence updates do not establish a new release binary or deployment. The
larger recovery/relocation candidate remains unmerged; its source-scoped results follow.

## Current operational verdict

Remote main is source **1.0.490-beta**, `702f0435a12944563c7751525f8429d80374a3d0`,
at this documentation checkpoint. The read-safety foundation below is merged;
the larger candidate is not. The proposed 1.0.491-beta delivery contains only
documentation/evidence and synchronized version metadata, not these candidate
implementation changes, a new binary or a deployment.

The newest executed autonomous-recovery baseline is **failed**, exact candidate
`721c329de4196aeef96604642c439fe7d8ddb0bf`,
[run 43325316433](https://github.com/nikoma/bicdb/actions/runs/43325316433):
one control passed, one required recovery test failed, 957 filtered, 26.04s.
All-target and isolated-consumer compilation passed. After actual quorum
admission and a pre-local-WAL fault, the original leader's normal APP handler
has exited and release callback has returned. Two actual native replicas
contain the complete committed command at R2/B2, while the original leader
retains R1/Pending2. The original call remains Unknown with no user ACK; it is
not rollback. Subsequent Strong calls return unavailable, and neither next-write
API call ACKs. The two completed health-round receipts are 7.650s and 15.333s
under a declared 20-second success limit; sampling ends at 15.333s, not second20.
This confirms a missing bounded autonomous committed-tail recovery path in the
candidate, not acknowledged data loss or the cause of the old Azure incident.
[Complete attempt ledger, actual native observations and offline verifier](evidence/native-origin-pre-wal-2026-11-12/README.md).

The no-reinstall handoff baseline separately fails at image admission before
rows are cleared. A [read-only source audit](design/native-closed-handoff-image-admission-audit.md)
identifies missing closed-purpose admission for the retained original writer
and for a genuine Registered receiver; the latter cannot borrow INITIAL-genesis
authority. These are proposed contracts, not implemented fixes.

The passing post-joint component tests below establish useful protocol paths,
but require explicit fixture orchestration and writer re-enrollment. Automatic
DATA movement to a genuine fifth node, crash-safe convergence, current physical
RPO/RTO and stable-five throughput gain remain unqualified. Original Azure
qualification remains **FAILED**. Historical sections retain their own
revision-scoped outcomes; their old pending/open labels do not override this
current verdict or the later executed results.

## Read-safety foundation delivered: 1.0.489-beta

[PR #157](https://github.com/nikoma/bicdb/pull/157) merged on November 12 at
18:05 UTC as GitHub main commit
[`b9394418bdf9d87864c2c12d6fe81bb7d4039d53`](https://github.com/nikoma/bicdb/commit/b9394418bdf9d87864c2c12d6fe81bb7d4039d53),
source **1.0.489-beta**. The merge tree exactly matches tested candidate
`2ae800454d57b5f5660a36d27a8f309d0ebb4e9a`; all 29 source-version identities
were checked. The temporary delivery branches were removed after verification.
This records the HA foundation's delivery revision, not an assertion that489
will remain the newest main version.

The complete [required test run 43260701749](https://github.com/nikoma/bicdb/actions/runs/43260701749)
and [stock-adapter Rails 43260701703](https://github.com/nikoma/bicdb/actions/runs/43260701703)
passed on exact 2ae. Library counts: Core 847 passed/zero failed/nine ignored;
SQL 866/zero failed/four ignored; PostgreSQL-wire 151/zero failed. The remaining
workspace job passes, with 126 test-invocation summaries, not 126 unique tests.
All-target and isolated no-default-feature consumer compilation, formatting,
licensing, selector and repository-boundary checks pass. Earlier failed
compositions remain preserved with their diagnoses; none was waived.

Delivered scope is fail-closed missing-catalogue custody and scoped native
point-read/recovery identity, writer and visibility fences. The larger native
Abort/joint/DATA/relocation candidate remains **unmerged and unqualified**.
No 1.0.489-beta release binary, published download, deployment, physical RPO/RTO,
automatic five-node convergence or sustained capacity increase was produced.
The original Azure qualification remains **FAILED**, and production admission
remains unchanged. The following checkpoints retain their original revisions
and pre-merge status; they are not newer claims that read489 is still unmerged.

## Latest executed component result: ACK4/Strong and historical retry pass

Exact unmerged candidate `eedf23d58e236cf62d61366349ad4de3bd52373b` passes
three distinct component tests. [Run 43319425823](https://github.com/nikoma/bicdb/actions/runs/43319425823),
Core `131535932600`, records **two passed, zero failed, 955 filtered, 100.89s**:
the unchanged post-joint PrepareOnly → canonical Abort3 → all-member activation
→ actual ACK4/Strong R4 positive, plus its separate post-ACK idempotence test.
[Run 43319432266](https://github.com/nikoma/bicdb/actions/runs/43319432266), Core
`131535951702`, records **one passed, zero failed, 956 filtered, 98.32s**: the
actual copied-image target returns historical AnchorR3 at current R4, then
refuses a deliberately removed immutable serving archive with the caller live.
Both exact-source all-target and isolated-consumer jobs pass.

The actual diagnostic confirms n3's physical members `[n0,n1,n2]` and protected
effective/observer voters `[n1,n2,n3]`. The correction binds the NEW domain to
actual native joint/serving records and archives. Historical AnchorR3 remains
distinct from a current-prefix or Strong-read grant. Exact prior ACK payloads,
actual R/H/B/UID and inherited commitments survive the tested paths.

**This is not a passing whole-candidate qualification.** The sibling
[run 43319438217](https://github.com/nikoma/bicdb/actions/runs/43319438217), Core
`131535993682`, records **zero passed, four failed, 953 filtered, 81.00s**.
All four cases reach target ACK2 but fail their StrongR2 prerequisite before
CurrentJoint preparation, fault injection or the new timing spans. Their
structured unavailable/timeout results do not establish false absence or durable
loss. All-target/consumer checks pass; zero observer timing contexts were reached.
[Exact failed prerequisite and custody scope](design/native-ready-observer-inner-result-eedf.md).

The passed component uses fixture-level opaque protocol orchestration, writer
re-enrollment prerequisites and explicit coordinator recovery. It does not prove
automatic NativeHost repair, original-writer operational transition, a genuine
fifth-node DATA move, online or 8 GiB transfer, crash/cold recovery, throughput,
physical RPO/RTO, general SQL transparency or deployment. The independent
no-reinstall image-policy baseline remains **failed**. Successful CI artifacts
retain complete logs/source/compiler facts but **no database or WAL custody**.
[Complete source-pinned receipt, raw logs, artifacts and offline verifier](evidence/native-current-joint-first-success-2026-11-12/README.md).
Original Azure qualification and production admission remain unchanged.

A separate one-selected-test experiment, exact same eedf source and unchanged
worker/deadline settings, [run 43322376572](https://github.com/nikoma/bicdb/actions/runs/43322376572),
passes the missing-decision-archive control: **one passed, zero failed, 956
filtered, 71.83s**; its compilation/consumer checks pass. It reaches the actual
observer spans. In the representative boundary, tagged full-writer checks account
for about 7.1%, so repeated writer validation is not established as the dominant
cost. This isolated result does not replace or explain the four-selected-case
StrongR2 failures, prove concurrency causation, or qualify the whole candidate.
[Exact isolated source, disjoint timing accounting and limits](design/native-ready-observer-isolated-eedf-result.md).

## Executed 68b results: activation releases, then anchor projection refuses

Exact source `68be040ebb00a4c606d519697f913e310c496812` has **failed full
acceptance**. All four workflow all-target and isolated-consumer checks pass;
this does not qualify their failed runtime cases.

| Finding | Classification | Exact evidence and limit |
| --- | --- | --- |
| New post-catalogue controls | **Failed cohort; three controls passed** | [Run 43315433251](https://github.com/nikoma/bicdb/actions/runs/43315433251), Core `131522161919`: three passed, one failed, 952 filtered, 90.87s. Foreign actual DB/Store, original cancellation with independently fresh exact recovery, and actual writer-hook replacement cuts pass. The missing-decision-archive case fails its third Complete prerequisite before injection; no archive-cut success follows. |
| Full post-joint recovery positive | **Failed; all three activations released** | [Run 43315439379](https://github.com/nikoma/bicdb/actions/runs/43315439379), Core `131522178395`: zero passed, one failed, 955 filtered, 83.58s. All three actual native decisions, catalogue activations and arm/release steps finish. The n3 result observer rejects `activated completion anchor differs from actual current same-voter native domain` with 2594ms remaining. This is a semantic refusal, not the previous deadline failure. ACK4 and Strong R4 are not reached. |
| Activation/idempotence cohort | **Failed; seven legacy controls passed** | [Run 43315446340](https://github.com/nikoma/bicdb/actions/runs/43315446340), Core `131522192672`: seven passed, one failed, 948 filtered, 66.97s. The heavy case likewise releases all three activations, then n3's result observer refuses with 2914ms remaining. Later ACK4/Strong/idempotence remain unexecuted. |
| Original-writer image transition | **Failed required baseline** | [Run 43315453055](https://github.com/nikoma/bicdb/actions/runs/43315453055), Core `131522237759`: zero passed, one failed, 955 filtered, 2.31s. Both fixture writer reinstalls are omitted. Actual OLD closure and initial ACK rows survive on original writers; n3 remains correctly untrusted before image installation. Actual image clear then refuses the changed installed-writer range policy before its row mutation grant. No adoption or global joint decision is reached. [Complete original log/report and custody scope](evidence/native-original-writer-image-policy-refusal-2026-11-12/README.md). |
| Missing-archive case's earlier Complete failure | **Inconclusive detailed cost attribution** | Actual peer service/DB queueing is negligible in the retained trace. First remote observer validation takes 774/795ms under an 801ms child allowance; retry construction takes 511/518ms under shorter remaining allowances. Entry/exit spans do not attribute cost among full writer, native admission, lineage or joint binding. Nested instrumentation is required before claiming which checks dominate. [Exact source-specific trace/profile](design/native-current-joint-activation-scope-68-result.md). |

The installed image preserves OLD physical voter credentials; the current joint
bridge protects the NEW voter set. Source review identifies the historical
completion anchor's use of physical `base_members` as a mismatch for n3.
Correction `ebcb23b92e66911d3b74fe96d21e8612bf2100ff` selects protected effective
credentials **only for CurrentJoint**, binds the exact joint decision and serving
archive, and preserves the NormalRecovery predicate. Test import
`1ecb621d43762c9cfe772cf43ba1ff0d43c989ce` adds a real n3 AnchorR3-at-R4 positive
and missing-serving-archive refusal after the unchanged ACK4/Strong prerequisite.
**Both changes remain source-only, uncompiled and unexecuted at this checkpoint.**
The exact differing runtime predicate still needs the new membership diagnostics;
source inference is not a replacement for its passing execution.

The following section preserves the earlier launch-time checkpoint. Its pending
labels are superseded by the terminal results above, without changing source IDs.

## Historical launch checkpoint: 68b correction and new executions

Candidate `68be040ebb00a4c606d519697f913e310c496812` contains the independently
reviewed [activation validation scope batch](design/native-current-joint-activation-validation-scopes.md),
author commit `a4345eda607a108cbe05f72852dc333d9b656371`, imported as
`3cda3fc73b5e6a563bceacdc0f9d40a136ba6881`. It uses separate actual pre-catalogue,
post-catalogue and late-arm scopes. Current authority, original writer/caller,
current/archive records and physical ownership remain required; no deadline,
retry or worker count is changed. Formatting, diff and all 29 source-version
identity checks pass locally. **Runtime and compilation results for this new
batch are pending**, not inherited from f58. Source remains unmerged.

Actual focused executions at exact 68b are:

| Selection | Run | Status at this checkpoint |
| --- | --- | --- |
| Four new post-catalogue fault controls | [43315433251](https://github.com/nikoma/bicdb/actions/runs/43315433251) | **Not yet established**; execution/compilation in progress |
| Unchanged full Abort3 → activation → ACK4/Strong positive | [43315439379](https://github.com/nikoma/bicdb/actions/runs/43315439379) | **Not yet established**; execution/compilation in progress |
| Existing activation controls and full idempotence continuation | [43315446340](https://github.com/nikoma/bicdb/actions/runs/43315446340) | **Not yet established**; execution/compilation in progress |
| Original writers retained through both fixture reinstall boundaries | [43315453055](https://github.com/nikoma/bicdb/actions/runs/43315453055) | **Not yet established**; separate required diagnostic baseline |

The preceding diagnostic at source
`11f5b4b222f4bd84d1f3870dbd59da11ee8cd17c`,
[run 43314169894](https://github.com/nikoma/bicdb/actions/runs/43314169894),
Core job `131517781459`, **failed**: zero passed, one failed, 951 filtered,
2.12s. Its observation helper incorrectly required the not-yet-installed learner
to have a trusted native anchor. The actual error is `no trusted native anchor;
image repair required`. This is a **test observation defect**, not execution of
the expected image-policy refusal or proof of an engine defect. Original raw log,
source and complete 75-file custody are preserved. Correction 68b records the
learner's actual refusal as data, asserts actual OLD-member ACK rows and continues
toward the unchanged image operation; it does not invent a trusted zero floor.
The new run must establish the earliest real operational boundary independently.

## Historical f58 acceptance: completion progresses, activation fails

**Milestone 1 remains incomplete.** These results belong to the unmerged candidate
revisions below. They do not establish release admission, a deployed binary,
physical failover, automatic four-to-five-node convergence or throughput gain.
Earlier failures remain retained; passing controls do not waive full acceptance.

| Finding | Classification | Exact executed evidence and limit |
| --- | --- | --- |
| Original-writer registry replacement controls | **Passed focused controls** | Source `29bfd6c9af7652a8e40a97e46b23d4629777ece9`, [run 43308056358](https://github.com/nikoma/bicdb/actions/runs/43308056358), Core job `131496514002`: five passed, zero failed, 945 filtered, 7.61s. This includes the corrected test of actual old/new writer-generation custody. Workspace all-target and isolated consumer compilation pass at this revision. It is not DATA relocation. |
| Discovery under actual held DATA-service locks | **Passed focused controls** | Same source 29b, [run 43308063984](https://github.com/nikoma/bicdb/actions/runs/43308063984), Core job `131496564730`: three passed, zero failed, 947 filtered, 5.71s. Bounded fixture-holder startup retains the original actual three-lock hold and quorum/read assertions. All-target and isolated consumer compilation pass. |
| Original owned-validation fixtures | **Inconclusive suites; cancelled after a confirmed fixture deadlock** | Source `ded3c412cf25c12f3c6fe524d22aac02310a1c0b`, [run 43303070364](https://github.com/nikoma/bicdb/actions/runs/43303070364), Core job `131479132858`, records 57 individual passes and one ignored helper, but no final suite result. Source 29b, [run 43308048131](https://github.com/nikoma/bicdb/actions/runs/43308048131), Core job `131496490839`, selects four controls and completes none. The fixture calls `config(&f)` while already holding the same non-reentrant metadata mutex; `config` locks it again. Both task-owned runs were explicitly cancelled for this source-proven setup deadlock, with partial logs retained. This is not evidence of a production relocation deadlock or a passed broad suite. |
| Corrected owned-validation execution | **Passed focused controls and compilation** | Source `6e00ba519715ee0ca392357394f2b0a9749a1358`, [run 43310478213](https://github.com/nikoma/bicdb/actions/runs/43310478213), Core job `131505005808`: four passed, zero failed, 947 filtered, 2.30s. Configuration is captured before acquiring the fixture mutex. Both strict-owned and both HOT registered-catch-up contention/release/cancellation cases execute. No production condition or authority deadline is relaxed. Workspace job `131504948931` subsequently passed both all-target and isolated no-default consumer compilation at exact 6e; the complete focused workflow passed. No COLD fault-control or whole-CLI pass follows. |
| CurrentJoint observer-scope controls | **Passed focused controls** | Source `f58d14fc8836e5cb14ba5251d4415d5ac47fd5b0`, [run 43309668194](https://github.com/nikoma/bicdb/actions/runs/43309668194), Core job `131502198055`: five passed, zero failed, 946 filtered, 75.06s. Includes actual full-handoff R2 Completed preparation followed by foreign DB/Store, original-parent cancellation and real writer-hook replacement refusals, alongside existing codec/authority controls. This does not establish activation or a post-transfer ACK. All-target and isolated consumer compilation also pass at this revision. |
| Native producer controls | **Passed focused controls** | Same f58 source, [run 43309675405](https://github.com/nikoma/bicdb/actions/runs/43309675405), Core job `131502195020`: nine passed, zero failed, 942 filtered, 0.79s. This selector does not substitute for the full handoff. All-target and isolated consumer compilation also pass at this revision. |
| Activation continuation | **Failed; useful later durable cut** | Same f58 source, [run 43309691294](https://github.com/nikoma/bicdb/actions/runs/43309691294), Core job `131502250718`: seven passed, one failed, 62.07s. All three Complete calls return within their original deadlines. First activation persists the actual native decision and `NativeActivated` catalogue, then its final catalogue-receipt validation consumes 986.139ms with approximately 746ms remaining. The runtime refuses to arm after expiry. No successful activation reply, ACK4, Strong R4 or idempotent continuation is established. Retained catalogues are n1 Activated, n2/n3 Completed; this is observed custody, not cold-replay qualification. |
| Unchanged full positive handoff | **Failed at activation preflight** | Same f58 source, [run 43309683056](https://github.com/nikoma/bicdb/actions/runs/43309683056), Core job `131502214629`: zero passed, one failed, 950 filtered, 102.51s. All three Complete calls return; activation expires during native preflight before the new decision WAL. All retained catalogues remain Completed. This is a different cut from the activation continuation above. Workspace all-target and isolated consumer compilation pass at exact f58; the complete workflow fails its acceptance test. Nested debug timings are not summed or extrapolated to physical performance. |
| Initial native committed-tail recovery | **Open source-level gap; automatic recovery not tested by this audit** | The retained 56f CLI unknown operation is not established as lost or rolled back. A source audit distinguishes a still-running original APP worker from genuine post-admission error/cleanup. Fresh Strong reads must refuse unresolved history; no automatic NativeHost committed-tail controller is established. The next deterministic test must prove actual handler exit before requiring autonomous recovery and the next ACK/Strong. See [source audit and exact test specification](design/native-initial-committed-tail-56f-source-audit.md). |

All four f58 workspace/all-target and isolated-consumer checks subsequently
passed. Authority and producer workflows pass; both recovery acceptance workflows
remain failed on their runtime tests. See the [complete f58 receipt](design/native-current-joint-observer-f58-result.md)
and [source-separated isolation controls](evidence/native-registry-isolation-controls-2026-11-12/README.md).

The next activation correction must independently establish current native facts
after catalogue fsync. Reusing a pre-fsync currentness check is insufficient.
Repeated full lineage/materialization checks are source-proven; the trace does
not attribute all elapsed time to individual repeated calls. Removing repetition
must preserve actual current/archive proofs, original writer and caller fences,
fresh quorum/role authority and entry/exit checks for mutable facts. The original
full acceptance, time limits, terminal ACK4 and Strong R4 remain mandatory.

## Historical native candidate checkpoint: discovery and registry admission

The working native candidate remains **unmerged and unqualified**. The results
below identify their actual tested revisions; they do not qualify the later
composition or main merely because each carries source version `1.0.489-beta`.
No new release binary, deployment or physical experiment is established.

| Finding | Classification | Exact executed evidence and limit |
| --- | --- | --- |
| Discovery with DATA-service contention | **Failed baseline** | Source `05979463ef312fc1128a42903f908de0189c2a63`, [run 43278532377](https://github.com/nikoma/bicdb/actions/runs/43278532377): zero passed/one failed, 5.07s. Two genuine metadata-quorum acquisitions succeed with all three DATA-service mutexes held, while public discovery receives three bootstrap-busy and three unsupported-fallback replies. Releasing the locks restores the same client's original ACK/value and a fresh null ACK/Strong result. [Portable original receipt and precise scope](evidence/native-initial-data-discovery-held-2026-11-12/README.md). This is a same-process TLS component witness, not an Azure causal finding. |
| INITIAL-only routing correction | **Passed focused runtime and compilation controls** | Source `26cdaa87c446ca35542ec816b29f069d57f47242`, [run 43284689289](https://github.com/nikoma/bicdb/actions/runs/43284689289), Core job `131414888059`: three passed/zero failed/933 filtered, 5.93s. All three actual bootstrap replies contain the full committed topology while the DATA locks remain held; both genuine held metadata quorums and the original full-value/present-null reads pass. Separate controls reject wrong service, stopped owner and a replaced Store even while the original Store/writer remain alive. The actual slot-generation fence also rejects restoration of that old Store. Generic/control bootstrap and listener allowlists remain unchanged. Workspace all-target and isolated no-default consumer compilation also pass on exact 26c. This does not establish a broader regression suite or qualify the later composition. [Original complete passing receipt](evidence/native-initial-routing-discovery-2026-11-12/README.md). |
| Healthy read/write continuity after fifth metadata-voter admission | **Failed** | Source `1d3976f9f8011f3ac7411494aa0dfb432763ce78`, [run 43282887768](https://github.com/nikoma/bicdb/actions/runs/43282887768), CLI job `131408621456`: five passed/one failed, 84.04s. The original five cases pass; the additive strict case fails its final health assertion after attempting BOTH old-ACK Strong reads and fresh Appends in two rounds. Reads receive actual typed `WriterAdmission` unavailable replies at generation 4; writes receive actual registry-changed RPC errors. Successful routing snapshots remain distinct from fresh metadata-quorum authority. All five original child processes are running after the probes; no restart is inferred or performed. All-target and isolated-consumer checks pass. The fifth node is still an unassigned receiver, not a relocated DATA owner. [Complete wire, ledger and stopped-custody receipt](evidence/native-writer-registry-strict-baseline-2026-11-12/README.md). |
| Strict-case ACK and uncertain-write custody | **Inconclusive for independent native-page reconciliation** | Complete journals and stopped owned database roots are retained for the failing strict fixture. It has one prior external ACK and two uncertain fresh-write API calls with physical retries; neither an RPC error nor a catalogue entry alone proves rollback or native-row visibility. Earlier passing fixtures' four ACKs are separate and their database roots are not in this failed fixture's capture. This is process-stop custody, not a physical power-loss or cold-replay result. |
| CurrentJoint completion under original deadlines | **Failed full recovery; useful partial completion** | Source `fd85a24392abf8a273479f8637e040523c053109`, [run 43278537440](https://github.com/nikoma/bicdb/actions/runs/43278537440): zero passed/one failed, 83.78s; all-target and isolated-consumer checks pass. Node n1 actually returns completion with 226ms left. Node n2 returns from native-WAL and catalogue fsync before the deadline, then expires during refresh; its reply remains uncertain. Node n3 completion, activation, ACK4 and Strong R4 are not reached. [Exact profile, source reconstruction and custody limits](evidence/native-current-joint-refresh-fd85-2026-11-12/README.md). Nested timing spans overlap and are not additive throughput measurements. |
| Two-scope publication/cancellation/ownership controls | **Passed focused controls and compilation** | Exact source `883f5f6e1ec30d96b14cb55ab346cf769c9ea693`, [run 43288464580](https://github.com/nikoma/bicdb/actions/runs/43288464580), Core job `131428110439`: nine passed/zero failed/929 filtered, 0.61s. Includes genuine foreign Store/fresh-role refusal, original QQ/caller cancellation, persistence ambiguity and actual second-range prompt busy refusal followed by observer progress after release. All three 883 workspace/all-target and isolated-consumer compilation jobs subsequently passed. This does not demonstrate concurrent ACK progress during a stalled global journal or uniform Normal born-v2 original-writer safety. |
| Full recovery after two-scope correction | **Failed, at a later stage** | Same exact source 883f, [run 43288472362](https://github.com/nikoma/bicdb/actions/runs/43288472362), Core job `131428140992`: zero passed/one failed/937 filtered, 65.45s. All THREE Complete RPCs actually return within their original cap: n1 1571.750ms, n2 2116.626ms, n3 1386.843ms. First recovery activation then reports `native image journal busy`; ACK4/Strong R4 are not reached. Source tracing finds an inherited journal reentry in both fresh and same-session native activation-decision paths. Exact runtime persistence stage remains subject to retained-state verification. These three CI phase timings are not physical RTO or latency percentiles. |
| CurrentJoint authority/original-writer lane after scope correction | **Focused Core job passed; limited selector** | Same exact source 883f, [run 43288468790](https://github.com/nikoma/bicdb/actions/runs/43288468790), Core job `131428118476` passes. The module selector matches one heavy authority/original-writer test; it does NOT execute the earlier four-test `current_joint_` group. The other producer/codec controls still require the final coherent Core suite. No earlier revision's four-test result is transferred to this source. |
| Activation journal correction, full recovery prerequisite | **Failed; new activation path not reached** | Source `432fc9bc5e9f681bb40e3bfe6b07fd043bcdf061`, [run 43294492826](https://github.com/nikoma/bicdb/actions/runs/43294492826), Core job `131449108302`: zero passed/one failed/939 filtered, 101.66s. The unchanged full positive returns n1/n2 Complete in 2894.045/3949.512ms with 1104/48ms remaining. The third Complete fails closed-Ready quorum collection with a peer n2 query timeout while its parent still has 1056ms remaining; no activation, ACK4 or Strong R4 follows. Aggregate spans do not record the peer's exact allowance, local contention or the timeout's physical cause. This failure does not validate or refute the newly changed activation branch, which was not executed. |
| Activation controls and added full-path idempotence continuation | **Seven controls passed; new continuation failed before activation** | Same source 432fc, [run 43294496599](https://github.com/nikoma/bicdb/actions/runs/43294496599), Core job `131449126336`: seven passed/one failed/932 filtered, 89.94s. The separate continuation first requires the original fresh activation/ACK4/Strong path. It fails at the same third Complete prerequisite, with peer n1 query timeout and 1067ms parent remaining. Neither its fresh activation nor later Runtime idempotence assertions execute. Workspace/all-target and isolated no-default consumer compilation pass at this source. No existing control's pass is transferred to the unexecuted CurrentJoint activation branch. |
| Retained-writer metadata-registry correction, first focused cohort | **Failed cohort; four controls passed** | Reviewed batch `6c5074442d73f8e9a4398eaa5c4d07f6fd235c9b` + `37ee4db501c81e90408aad659519a72f04e8f85f` + `9771fcfeb7fd79f2f4818351b644d9e3e2c68b9a`, integrated as `aac6192c2b034e1f2e4c88a16fd84bcd94e0bf6b`. [Run 43296887408](https://github.com/nikoma/bicdb/actions/runs/43296887408), Core job `131457525202`: four passed/one failed/940 filtered, 6.47s. Passing controls cover actual post-native-readback error and independently fresh exact retry/cold no-grant, original cancellation and replaced Store-slot refusal, actual fifth-voter extension followed by committed label mutation, and legacy no-baseline refusal. The new replacement test expects a later object-fence message but receives an earlier real hook-lifetime refusal; its subsequent audit/point/cleanup assertions did not execute. Test-only successor `794fd5ad3f16e21e07c641bb9635910ffb6d274a` separately checks fresh-ingress refusal and replay of a genuinely captured pre-replacement preparation, with original-token, full ACK-row and actual replacement-hook assertions. That successor is not tested by this result. No production fence was relaxed. |
| Learner's actual schema identity publication | **Passed focused control** | Same exact aac source, [run 43296897267](https://github.com/nikoma/bicdb/actions/runs/43296897267), Core job `131457557639`: one passed/zero failed/944 filtered, 2.68s. Actual native/TLS enrollment commits the sealed observed schema with the learner birth through the real pending proposal and old-voter quorum. Contradictory and previously bound-but-missing labels refuse. Metadata enrollment is not DATA Ready or relocation. |
| Strict fifth metadata-voter addition with existing DATA continuity | **Passed strict case; complete CLI cohort failed elsewhere** | Same exact aac source, [run 43296905362](https://github.com/nikoma/bicdb/actions/runs/43296905362), CLI job `131457578487`: five passed/one failed, 65.89s. The additive strict case passes on its first health round: the same client receives the prior ACK's exact Strong value, a fresh ACK and its exact Strong value at monotonic receipt 1,015,797,325ns, inside the unchanged 10,000,000,000ns gate and original two-second API budgets. It records no false absence or wrong value. The four retained writers publish registry extensions; the fifth remains non-serving and DATA assignments do not move. This is a bounded metadata-addition continuity result, not physical relocation, RTO or capacity qualification. |
| Existing automatic join/election CLI case at aac | **Failed** | The same complete CLI target fails `native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter` on the actual n4 `elect` command, after n4 becomes an owned metadata voter and before the sixth process is spawned. The error is `owned metadata member changed/busy`; all five processes are still running before custody teardown. Source `learner_birth/metadata_transition.rs::validate_owned_metadata_member` emits that exact message only when its second `try_lock` fails, after factory validation; actual proof-state change has a different error. This identifies a local acquisition refusal, not a child crash or an Azure causal finding. A bounded wait must preserve the original caller/MQ expiry and the unchanged final proof-current check. No failing assertion is waived. |
| Automatic DATA-policy transition and retained ingress across handoff | **Not tested operationally; missing integration identified by source review** | [Exact source audit](design/native-data-policy-transition-gap.md) at aac distinguishes the component fixture's explicit `refresh_hosts` re-enrollment after actual metadata transition from a production retained-host transition. Later original-writer checks preserve the post-refresh preparation's writer; they do not prove automatic lifecycle retargeting. Metadata-registry refresh deliberately refuses DATA range/epoch/leader changes. An authorized internal policy rotation or extension may be valid, but the actual controller must consume the handoff decision, keep old ingress fenced, reconcile uncertain publication, and preserve all native/ACK obligations. No existing fixture-map replacement proves this gate. |
| New physical four-to-five expansion, fault-safe convergence and capacity | **Not tested** | No new VMs, physical RPO/RTO, stable-five latency window or sustained throughput comparison. The original Azure qualification remains **FAILED**. These component results do not close its unknown restoration event or prove its original unreadable-ACK/nonconvergence causes. |

All three aac workspace/all-target and isolated no-default consumer compilation
jobs passed. They do not change either failed cohort's classification, and do
not validate the later test-only successor or subsequent composition.

The [first aac receipt](evidence/native-writer-registry-first-aac-2026-11-12/README.md)
retains all 54 strict journal records, distinct failed-command/stopped-state
chronology, actual binary identity and separate fixture custody. Its portable
and private checks, including four semantic tamper cases, passed independently
at integration. They verify retained bytes and consistency, not engine replay
or independent physical qualification.

Hot owned-member correction `f5b419100beadaece29c3b3dcb85f88f6fd226a4`
(integration `56f9a3c999fba329c349d3e6dda664dcfb64e4c6`) waits for the two short
metadata borrows under the minimum of the original MQ expiry and caller
deadline/cancellation, with the original final proof-current check intact.
Cold-member and other service-lock paths are unchanged. Its two genuine mutex
controls and the separate actual-TLS expired-delivery control **did not execute**
in the first 56f Core jobs: three snapshot calls used a nonexistent `BicDb::scan`
API and failed compilation. [Exact failed-build receipt](evidence/native-writer-registry-first-aac-2026-11-12/FAILED-BUILD-56f.md).
Test-only correction `626736598093281a440ca500794846f9373bcb4c` uses the real
`scan_collection` API without changing assertions. Earlier passing controls
cannot validate this corrected source.

The [correlated observer diagnostic](design/native-ready-peer-deadline-diagnostic.md)
adds test-only peer budgets, receiver validation phases and actual operation-exit
traces. Its first composition `be85db58c1c4f54b13372a9a966794d146d31722`
also failed test compilation on those same three snapshot calls, before any
observer telemetry or recovery test ran. No receiver-cost attribution or
activation success follows. Corrected composition and final versioned-head
regressions remain required; the original full positive's acceptance child is
byte-identical to 883.

The registry repair has a [retained-writer design contract](design/native-retained-writer-registry-refresh-contract.md)
and a source-reviewed implementation in the unmerged candidate. Its companion
learner change publishes the actual observed schema identity with the registered
birth. Runtime evidence must still establish automatic refresh under fresh actual
metadata authority while preserving the original writer/registration/coordinator
and recovery lineage. No initial-genesis reset or cached-topology grant is allowed.
The [completion borrow-scope contract](design/native-current-joint-refresh-borrow-scope-contract.md)
has focused passing two-scope controls and the failed full-path results above.
The later activation correction remains unvalidated because its prerequisites
failed; [its complete failed receipt](design/native-current-joint-432fc-closed-ready-timeout.md)
and the [observer timing/scope analysis](design/native-ready-observer-read-scope-analysis.md)
separate measured aggregate durations from unmeasured receiver costs.
Its original-writer operation proof is specific
to CurrentJoint, and does not newly qualify Normal born-v2 hook replacement.
No measured performance or recovery success follows from this source review.
Production admission and the complete Milestone 1 gates remain unchanged.

## Earlier verified checkpoint (before the read489 merge)

Verified GitHub main is now **1.0.488-beta**,
`d1d816afe0446b2a614c38004738d3d1c8bbc547`, containing tested runtime
`d8fc122db55300fd019fffe95321403c30ef45be`. Its complete standard CI
[43238398103](https://github.com/nikoma/bicdb/actions/runs/43238398103) and
[Rails43238398124](https://github.com/nikoma/bicdb/actions/runs/43238398124)
passed. These general/application regressions do not qualify distributed HA.
The metadata repair entered main485; the larger native candidate remains
unmerged. Read489 integrates actual main488 at `d9b8f3472`; its tree exactly
matches focused/final-regression source `eec5e3b206f029005826e1406fe366baab4c7441`.
The later status-document update changes no runtime code.

| Gate | Current classification and exact limit |
| --- | --- |
| Missing-catalog fail-closed read repair | Passed on read candidate `25710b785` (paired witness plus thirteen enrollment controls) and native regression `b08556c3f` (231 passed, zero failed, two ignored). Final versioned-main validation is still required. |
| Complete same-open handoff and next ACK/Strong | Passed in the complete nine-test cohort at `88082c179`, run 43241594839: nine passed, zero failed, zero ignored, 75.50 seconds, original four-second budgets unchanged. The earlier same-cohort `eb5b454a5` result remains failed (five passed/four failed); [exact corrected scope and immutable evidence](evidence/native-joint-bounded-nine-2026-11-12/README.md). Initial metadata election is a declared fixture precondition. |
| Stale-seed discovery | Passed on exact `cf91eda30`: two actual-TLS/current-quorum tests. Enrollment remains metadata-only. |
| Concurrent opening faults and original writer continuity | Passed on `ab2615524`, run 43230536293: three passed, zero failed, 20.28 seconds; compilation passed. All intended cuts were reached, including fresh collection refusal after actual foreign writer replacement. The broader nine-test pass above includes the original-image and survivor-release controls. Earlier two-pass/one-fail `e8d263bac` evidence remains failed. |
| Full custody composition | `ab2615524` passed sixteen enrollment controls at run 43232582536 and the actual acknowledged-value/missing-catalog paired control at run 43232590513. The earlier `2c1b7d8be` workflows failed compilation before runtime; a test-only genuine-election helper corrected that composition. These targeted passes do not establish full release or physical qualification. |
| Post-joint recovery after the next failed Prepare | **Positive acceptance still failed**, with useful executed substages. At unmerged `d6331d1d6`, run43253229007, all three actual CurrentJoint fences, no-commit promises, canonical Abort3/native R3 and native Ready preparations completed. The first completion RPC timed out (0/1,61.11s); afterward the retained snapshot shows n1 NativeCompleted R3 and n2/n3 NativePrepared R3. Snapshot timing alone does not prove whether n1 persistence preceded the client deadline. Activate/next ACK4/Strong R4 were not reached. Diagnostic sourcea0ed at run43255635176 expired earlier while recording the last fence; it collected no completion timing and remains inconclusive for that cost. |
| CurrentJoint authority/custody controls | Unmergedd633, run43253237656: four passed/zero failed/zero ignored,48.83s. Includes real metadata-only empty-round refusal, codec controls and the actual complete-handoff prerequisite followed by wrong-purpose/domain/original-nonce, original client-expiry and actual writer-replacement refusals. This does not establish successful completion/recovery or a physical fault result. |
| Read foundation release delivery | Candidate489 remains unmerged. Original2f failed primary-only export on its superseded488 base. Corrected0a75 passed Core/SQL/Rails but its PGwire library failed150/1 after an observed listener bind collision, with n2 identity refusals. Sourceeec retains real bound sockets, preserves receiver identity checks and adds exact diagnostics; focused actual wrong-route and unchanged metadata-failover controls both pass (one each). Full final-source validation is running; no failed check is waived. |
| Public data-open and local cleanup | Unmerged4b passed the entire public CLI target at run43249496779: four passed/zero failed in45.81s. The DATA case uses four actual processes, one unchanged client and three ACK/full Strong values including null/empty/absence distinctions. Metadata joins and promoted-voter kill/reopen use separate fixtures. Final nonwaiting cleanup/full-record+R/H controls at run43249513728 passed four/zero in8.79s. These are single-CI-host results, not independent VM qualification. |
| Metadata leader self-maintenance | Reproduced actual failure on ec20 (run43251524358): one automatic maintenance call made the same Store's leader become follower without a term/commit change. Fixedcd8 (run43251543496) passes the same intended control, preserving fresh authority. Source excludes actual self from selection/catch-up and leaves Raft receiver semantics intact. The earlier sixth-process CLI failure has consistent stopped-state/error evidence; it lacks an exact self-Append request trace. No original Azure causal claim follows. |

| Operator-driven expansion, cold recovery and physical capacity | Open. Registered fifth-node replica receiver, authenticated native image/joint wire consumers and native placement-controller integration remain unfinished. Initial-only data-open does not supply general post-joint/cold recovery. No automatic four-to-five relocation/controller-restart result, new VM trial, physical RPO/RTO or stable-five throughput result exists. |


## Additional candidate checkpoint: November 12, 17:14 UTC

The earlier rows retain their exact source histories. The larger working
candidate is still unmerged; its code anchor is
`2a7a587fcfd392e851dc42eccc8762837b0c7fb6`, source `1.0.489-beta`.
It includes the separate read489 candidate; it is not GitHub main489, a
release binary or a deployed cluster. Main remains verified488 at this checkpoint.

| Finding | Classification and exact evidence |
| --- | --- |
| Full read/Abort/joint composition | Passed focused checks only. At `f8a68673b`, run43264294037, native-writer controls pass10/0/907filtered in1.73s; all-target and isolated-consumer compilation pass. This does not validate later recovery code. |
| Registered fifth-node receiver | Passed limited controls on `3ee40c19b`: Core3/0/913filtered in2.28s. The real CLI receiver case obtained an ACK/full Strong result before joining the fifth metadata voter and opening its replica-only receiver. APP/Get/Scan refused correctly. The same gateway then timed out: healthy post-join continuity, native image/joint transfer and automatic movement remain unproved. |
| Expanded CLI cohort | **Failed4passed/1failed**, source3ee, run43262683104. The existing initial-DATA case obtained one object ACK/full Strong value, then its JSON-null append returned generic no-member-reachable. All four processes were alive, and retained metadata shows no leader change. Null durable outcome and post-failure ACK reconciliation are unknown because full DB/page/WAL/range-catalogue custody was not retained. [Original full logs, diagnostics, hashes, source excerpts, verifier and precise next-test limits](evidence/native-registered-receiver-cli-2026-11-12/README.md). No rerun or durable-loss conclusion follows. |
| Ready-preparation timeout | **Failed positive acceptance** at `6ce80ad32`, run43263149425. Actual preparation fsync returned with662ms of the original budget left; full post-persistence validation left80ms, then repeated response validation expired. Service-lock waits were microseconds. This directly proves preparation persistence before the deadline in this cohort, not completion/activation/next ACK. |
| Scoped produced-Ready correction | Source-reviewed; full positive result pending. Code2a7 consumes actual physical evidence, retains full validation before and after fsync, and immediately projects within the same exclusive Store/DB/freeze/pin ownership. Original deadline, caller, writer and session checks remain. RF1 controls pass2/0 in0.15s at run43265974939; CurrentJoint authority controls pass4/0 in47.80s at run43265975524. Full recovery acceptance43265975417 is still running. Predicted latency savings are not measured success. |

The capability catalog now explicitly scopes automatic replica rebalancing to
the legacy range/event controller. It does not assert that the unfinished native
DATA image/joint/controller path is implemented. Manifest tested, qualified and
deployed statuses remain not-established. No physical qualification, capacity
increase, general distributed SQL or critical-workload admission is established.

## Deployment modes

| Mode | Implemented and tested scope | Remaining qualification / deployment evidence |
| --- | --- | --- |
| Legacy file shipping; native single-primary synchronous/asynchronous HA | Existing copy/promotion and ordered streaming/election paths. Current automatic-failover configuration validates synchronous voting acknowledgments against election quorum requirements and excludes observer acknowledgments. Asynchronous and timeout-degraded operation retain a positive-loss-window contract. | No new physical RPO/RTO or current-release production qualification. The original audit's unsafe-K configuration concern was source-repaired; it is not still an absent validation feature. See [the audit correction](ha-horizontal-scaling-audit-2026-11-10.md#current-single-primary-configuration-correction--2026-11-12-utc). |
| Distributed virtual ranges | Metadata consensus, range quorum and automatic placement/repair have existing code and CI evidence. The reproduced bounded-Append safety defect is repaired on main. The larger native read/recovery/relocation candidate remains separate and unmerged. | The original Azure four-to-five trial failed. Fault-safe convergence, correct live reads throughout movement, stable five-node capacity gain and production admission remain unqualified. No verified new deployment. |
| Cell-scoped HA/encryption/fencing/recovery | Separate signed authority, lease, fencing and asynchronous-recovery contract; synthetic drill evidence is not a live recovery drill. | No new operational drill, measured physical recovery or production admission evidence. This native-range repair does not qualify Cell HA. |
| Application fleet placement/scaling | Separate worker placement, leases and reassignment tests. | Worker assignment is not database shard scaling. No new independent-machine sustained application-throughput or state-group HA qualification. |

The detailed [four-mode audit](ha-horizontal-scaling-audit-2026-11-10.md)
and [failed Azure report](evidence/azure-long-growth-failure-2026-11-11/README.md)
retain their historical revisions and measurements.

## Findings and exact evidence

Labels apply to the stated scope only. A passing control in a failed cohort
does not turn that cohort, the final candidate or a physical deployment green.

| Finding / check | Classification | Evidence and limit |
| --- | --- | --- |
| Bounded metadata Append could count an untouched divergent suffix | Failed on baseline; source-fixed and CI-tested on main | Genuine fsynced three-store election/message execution: baseline ACK 3 for matched index 1 permits a false topology commit; genuine majority recovery later replaces it. Fixed receiver ACKs 1 and cannot make that premature commit. [Original failing/fixed artifacts, twelve state checksums and offline verifier](evidence/metadata-append-frontier-2026-11-12/README.md); [source/report and controls](design/metadata-append-frontier-safety-2026-11-12.md). This is not a business-row-loss demonstration or an Azure causal finding. |
| Final versioned consensus candidate | Passed | At `ce023036`, [full run 43179263914](https://github.com/nikoma/bicdb/actions/runs/43179263914): Core library 536 passed / zero failed / four ignored plus passing integration invocations; SQL, PostgreSQL-wire and remaining-workspace jobs pass. [Rails run 43179264201](https://github.com/nikoma/bicdb/actions/runs/43179264201), all-target/isolated-consumer compilation, formatting, licensing and repository-boundary checks pass. Remaining workspace has 126 passing test-invocation summaries; that is not 126 individual tests. |
| Original Azure metadata supplies a causal witness of that defect | Inconclusive | All five original archive hashes and metadata-envelope checksums verify. The only equal positive committed-index pair agrees; sequential stopped captures have neither complete intermediate history nor request-time Append traces. [Read-only derivative](evidence/azure-original-metadata-correlation-2026-11-12/README.md). Absence of a final conflict cannot exclude an earlier false commit. |
| Original temporary unreadability of two ACK records | Inconclusive cause; failed live verification | Later routed reads matched both records. Retained committed-command contents and later visibility do not establish request-time strong-read correctness, the first restoring event or a complete live-read reconciliation. Stale routing, incomplete recovery/catch-up and verifier collapse remain distinct hypotheses; durable data loss is not established. |
| Original growth's 404 operation failures | Failed operations; incomplete classification | **404 is the count of failed operations, not an HTTP status.** Only 20 bounded error details survive: eight connection-reset, seven temporarily-unavailable, four recovery-not-complete, one no-reachable-member. The remaining 384 lack detailed responses. A full per-attempt gateway/range/generation classification cannot be reconstructed from this evidence. |
| Original 300-second convergence observation | Failed convergence; mechanism inconclusive | 68 status samples retained ongoing topology/relocation changes and repeated failed moves; no completed rebalance. Activity alone proves neither bounded forward progress nor oscillation. Cleanup-confounded stopped states cannot establish the runtime cause. No steady-five capacity comparison ran. |
| Native metadata heartbeat catch-up and mid-round membership retirement | Passed, focused unmerged candidate | `f1f7ebdf96b737c8f9c192b2e7831ee108abe6f6`, [run 43181969102](https://github.com/nikoma/bicdb/actions/runs/43181969102): three passed / zero failed / 860 filtered out. Actual TLS 17/65-barrier backlog and retirement controls; full matrix skipped. Not operator scheduling, physical failover or full-candidate qualification. |
| Native learner enrollment positive/cancellation/pending retry | Passed, focused unmerged candidate | Same source, [run 43181974286](https://github.com/nikoma/bicdb/actions/runs/43181974286): three passed / zero failed / 860 filtered out, actual TLS. Registered identity/birth lifecycle is not data Serving or completed expansion. |
| Current-quorum image substitution | Failed candidate; correction not yet qualified | `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`, [run 43183211113](https://github.com/nikoma/bicdb/actions/runs/43183211113): zero passed / six failed before substitution assertions because quiescent preflight ran under its own counted write lease. A source correction retains all guards; its integrated runtime coverage is still required. |
| Complete joint handoff, new-leader ACK and Strong read fixture | Not tested at newest correction | `a96d406596991057e6f4167b7185783f06f3c8fd`, [native run 43190505814](https://github.com/nikoma/bicdb/actions/runs/43190505814) and [substitution run 43190512897](https://github.com/nikoma/bicdb/actions/runs/43190512897) failed compilation at three new `Record`/`Arc<Record>` assertion mismatches. No runtime result follows. Corrected source `dc6a13d4c5823c39f16fdfb8e2f135f3d2987a6b` is under fresh focused CI; subsequent fixture corrections also need execution. |
| Operator-driven native four-to-five data expansion, all-owner cold recovery, post-joint NoData and cleanup | Not tested / implementation incomplete | Private lifecycle and proof components do not supply a public Init/Join/Serve chain, remote handoff, restart scheduler or completed cleanup. See the command delta and recovery designs below. |
| New physical RPO/RTO, latency or capacity qualification | Not tested | No new Azure resources or VM experiment. Original trial remains failed. Source/CI work does not reopen its expired spending window. |

## Subsequent revision-scoped CI results

These results supersede the older candidate results only for the named checks.
The larger native candidate is still unmerged. They do not establish complete
data relocation, a physical experiment, production qualification or deployment.

| Check | Classification | Exact source and evidence |
| --- | --- | --- |
| Same gateway retained across same-voter range leadership transfer | Passed, focused isolated read candidate | `b8b6e9bdc` at [run 43193116509](https://github.com/nikoma/bicdb/actions/runs/43193116509): 226 passed, zero failed, two ignored, 618 filtered. The new control retains an actual gateway and acknowledged full value across the transfer, checks structured redirect/unavailable responses and then reads the exact value through that same gateway after recovery. This is not replica relocation or automatic failure detection. Final versioned-main validation remains required. |
| Current-quorum image replacement and recovery | Passed, focused unmerged candidate | `c96f27427b855ab20d533eb30dfd3aa11f9fe194` at [run 43200324976](https://github.com/nikoma/bicdb/actions/runs/43200324976): nine passed, zero failed. Includes nonzero target-native history, source failure, higher replacement cut and stopped-owner copy/reopen controls. Local durable-store fixtures are not independent-host fault qualification. |
| Strict all-owner learner cold restart | Passed, focused unmerged candidate | `80a2ff0b62867c8feeac490ce6534c8786be2bae` at [run 43198801014](https://github.com/nikoma/bicdb/actions/runs/43198801014): four passed, zero failed. Same protected StoreUUID, actual persisted metadata and refusal controls; metadata learner without assigned data. This does not establish promoted-voter cold recovery. |
| Actual fifth CLI process killed and reopened | Passed, focused unmerged candidate | `2cc5debdb9cc180ad8a68013fac7e794608f875a` at [run 43201140398](https://github.com/nikoma/bicdb/actions/runs/43201140398): one passed, zero failed. State captured after actual kill/wait; strict same-root reopen and new bound acknowledgment preserve durable identity/history. The other four voters remain live; all five refuse application data. |
| Automatic metadata elections and catch-up | Passed, focused unmerged candidate | `d194594b138fced00b611bef2b00e70f950e1045` at [run 43207170874](https://github.com/nikoma/bicdb/actions/runs/43207170874): three passed, zero failed. Actual TLS votes, current-term barrier, valid-contact checks and malformed/stale rejection. Stop/drain is not a process kill; metadata-store reopen is not an all-database-owner restart. |
| Metadata promotion authority | Passed, focused unmerged candidate | Same source at [run 43207174755](https://github.com/nikoma/bicdb/actions/runs/43207174755): two passed, zero failed. Actual owned acknowledgment and original live budget required; pending promotion and held acknowledgment cannot cross an actual leader transfer. |
| Registered metadata catch-up, cold owner and promoted control batch | Passed, focused unmerged candidate | `67aff6f566c62117999e8625696cb327b3afaba1` at [run 43211866859](https://github.com/nikoma/bicdb/actions/runs/43211866859), job 131161252327: 40 passed, zero failed, one ignored, 844 filtered, 47.53 seconds. All-target and isolated-consumer compilation passed. Includes actual Registered catch-up after withheld promotion delivery, all-target-owner cold voter reopen, exact elected-fifth-node registration/schema RPCs, stale metadata authority refusal, and metadata supervisor controls. Lower catch-up custody does not grant readiness; strict current owned-member evidence remains required. This does not test the later CLI process-kill successor or complete data relocation. Raw job-log SHA-256: `0bbcdd6e7d37b81fb9b8cdcf5a076eb56d41312200146b8ec92e1df40ce8a4f8`. |
| Default automatic fifth-node enrollment | Passed, focused unmerged candidate | Source `d194594b138fced00b611bef2b00e70f950e1045` at [run 43207178489](https://github.com/nikoma/bicdb/actions/runs/43207178489), job 131145204507: two passed, zero failed, 8.92 seconds for the complete CLI target. One test is the manual/cold control; the other uses default automatic scheduling to make the actual fifth process a locally ready metadata voter. All application-data requests remain refused. Raw job-log SHA-256: `558fb080e4f9315d10892b5114d5477eafdba7c4546e50486346ea1ea58bff89`. |
| Bounded joint publication and lost-reply recovery | Passed, focused unmerged candidate | Source `d194594b138fced00b611bef2b00e70f950e1045` at [run 43207167250](https://github.com/nikoma/bicdb/actions/runs/43207167250): two passed, zero failed. Fresh bounded authority for each publication/recovery preserves the expired original quorum and original caller cancellation. Covers all three durable publications and uncertain replies, not opening/releasing replicas or the next write/read. |
| Complete joint handoff | Failed in retained complete-fixture runs; newer phase corrections not yet qualified | `c96f27427b855ab20d533eb30dfd3aa11f9fe194` at [run 43200329282](https://github.com/nikoma/bicdb/actions/runs/43200329282): zero passed, one failed. Component traces identify exhaustion of the retained quorum's deadline during repeated authority validation. The required target-as-new-leader, next acknowledged write and Strong read remain mandatory. A passing publication-only check does not close this failure. |
| Fresh phase opening while all three owners remain frozen | Failed, focused unmerged candidate | `5056d857cc44d9f405d60a40a76323a1f2d5b45d` at [run 43209913124](https://github.com/nikoma/bicdb/actions/runs/43209913124), job 131154586387: zero passed, one failed, 883 filtered, 37.31 seconds. All-target and isolated-consumer compilation passed. All three publications completed; two fresh-open attempts on target n3 then exhausted their bounded authority before the required open result. Exact failing assertion is `complete_joint_tests.rs:1005`. Opening, release and subsequent write/read are not established by this run. Preserve the failed execution; do not infer the original Azure mechanism from this CI timeout. |
| Instrumented all-Frozen opening with actual lost-reply reconciliation | Passed once, focused unmerged candidate; earlier timeout cause and stability unresolved | `656a8e6a44d324acd94ab36a7f06ac83497ff08a` at [run 43212523246](https://github.com/nikoma/bicdb/actions/runs/43212523246), job 131163486234: one passed, zero failed, 884 filtered, 32.96 seconds. All-target and isolated-consumer compilation passed. The successor adds diagnostic spans, fault-state capture and a test correction that injects lost reply after the first actual returned durable target receipt, reserving a bounded reconciliation attempt. No authority timeout was extended. Successful Rust output was suppressed, so this run supplies no retained component timings; the capture-on-failure hook was not exercised. Raw job-log SHA-256: `484083abf63a9d3ebcf43cbfccadbf74962f18768f69db07fe1e0358bc26f6f2`. Full release, new-leader ACK and Strong read remain open. |
| Opening repeat with retained component traces and captured files | Failed; measured validation-budget exhaustion, physical cause not inferred | `399529dedeaba5d404098d091f733491c690067d` at [run 43213581356](https://github.com/nikoma/bicdb/actions/runs/43213581356), job 131167066109: zero passed, one failed, 884 filtered, 36 seconds; compilation passed. All three target attempts exhausted their four-second opening phases. Collection, round closure and service/kernel entry repeatedly validated the full participant set; the last kernel entry had 438 ms remaining and expired rechecking participants. These are debug CI component timings, not physical RTO or attribution to the Azure slowdown. Captured catalogs have valid envelope checksums, global decisions and no serving activation at the observed cut. Artifact 13941814654 contains 71 of 75 manifest-listed files with matching hashes/lengths: four zero-byte `.backup-write.lock` files were omitted by upload filtering. Archive completeness is therefore partial despite successful runner-side copying. No engine replay or atomic power-loss snapshot was established. Raw job-log SHA-256: `11ff4f55a212003d83aeaf168ca46a3755b7a67b0c92803875b27eaeefa7f8bf`. |
| Scoped opening execution after retained timeout diagnosis | Passed, focused unmerged candidate; additional fault controls and full handoff still open | `3008f6acd0e4c55b2c95072e5e9eac851ad9d019` at [run 43215307947](https://github.com/nikoma/bicdb/actions/runs/43215307947), job 131172932586: one passed, zero failed, 884 filtered, 29.76 seconds; compilation passed. The final kernel boundary retains all foreign-member checks, then validates the complete local proof and retained history under one actual borrowed native-journal scope. No timeout or quorum was reduced/extended. Retained traces show actual target catalog fsync, injected lost reply, fresh exact-activation recovery without rewrite, and opening the other two replicas. Original caller and ABA/refusal assertions remain. The first target result's 6,269 ms label includes the deliberately injected 4.01-second expiry sleep; it is not an unbounded successful phase. Raw job-log SHA-256: `a63df58274fd6cf07299bfb5ce61bc1c0e9dc8ceb69cb4afd14e3895de86cffb`. This is all-Frozen opening, not releasing replicas, accepting the next write, physical recovery or production qualification. |
| Isolated read candidate broad regression and stock application compatibility | Passed for Core, SQL, PostgreSQL-wire, Rails and remaining workspace | `480db3b52820919b0c18a48e15cccda38fe7c1c1`: [Rails run 43212218313](https://github.com/nikoma/bicdb/actions/runs/43212218313) passed. Core library 838 passed / zero failed / eight ignored, its integration invocations, SQL, PostgreSQL-wire and all-target/isolated-consumer compilation in [run 43212214924](https://github.com/nikoma/bicdb/actions/runs/43212214924) and remaining workspace passed at this source. Child/repeated test invocations are not summed into unique coverage. The ignored native-library cases include a frozen-reader artifact control and three retained-1025-ACK recovery cases; these are not newly qualified by this run. Stock Rails compatibility is not transparent cross-range SQL qualification. These results do not qualify later changed or versioned sources. |
| Promoted-node CLI integration successor | Failed: one passed, two failed; downstream cold reopen not established | `a4aaec61a68cb7c6a7468bd3ffa4de0568c03a4b` at [run 43212556847](https://github.com/nikoma/bicdb/actions/runs/43212556847), job 131163595260: one passed, two failed, 15.75 seconds; compilation passed. The earlier manual learner/cold control passed. The sixth actual process exited during the expanded automatic join test, but child stderr/status files were not archived, so the cause is inconclusive. The promoted-voter kill/reopen test failed before cold reopen because it expected an explicitly serialized Voter field; Voter is an omitted schema default. A test correction decodes the actual persisted topology. It does not fix or explain the sixth process exit. Bounded child diagnostic custody and corrected runtime execution remain required. |
| Missing-catalog genuine-ACK baseline, first submission | Failed compilation; recovery hypothesis not runtime-tested | `ecbdb2e5ec8c0ae2ed18c60586b41c481d9e51a4` at [run 43216460107](https://github.com/nikoma/bicdb/actions/runs/43216460107) failed E0308 at `missing_catalogue_tests.rs:185`: a copied root `PathBuf` was passed where strict reopen requires `&Path`. No intact/missing recovery result follows. A test-only borrow correction and stopped-cut evidence retention are being prepared; no production repair or observed acknowledged-data loss is claimed. |


| Missing-catalog custody repair and original paired ACK witness | Passed, focused unmerged candidate | Source `25710b785f6b468737015888d4db0c3b5d19259c`: [paired acceptance 43221341175](https://github.com/nikoma/bicdb/actions/runs/43221341175) passed one test; [enrollment controls 43221345069](https://github.com/nikoma/bicdb/actions/runs/43221345069) passed thirteen. Original intact twin recovers the acknowledged value; three missing-catalog reopen attempts return typed `NativeRangeCatalogueCustodyMissing` without recreating the catalog or installing policy. Creation-only durable initialization, interruption and old-reader fence controls are included. This fixes the demonstrated candidate path, not arbitrary old-copy rollback or every possible recovery defect. |
| Custody-repaired native regression batch | Passed, focused unmerged candidate | Doc-only successor `b08556c3f8ef5422833164d7efef90954846d93c`, [run 43222639169](https://github.com/nikoma/bicdb/actions/runs/43222639169), job 131197782760: 231 passed, zero failed, two ignored, 618 filtered, 240.87 seconds. All-target and isolated-consumer compilation passed. Final versioned head still requires release checks and merge; this is not physical or production qualification. |
| Fresh learner through stale seed | Failed, deterministic unmerged candidate | `5b079a1e412afd1274748f61a55cbb260125132e`, [run 43223785342](https://github.com/nikoma/bicdb/actions/runs/43223785342), job 131201673568: zero passed, one failed, 2.97 seconds. Actual current leader n4 has quorum index 6 and serves the required schema; seed n0 still has index 5. Fresh n5 constructor rejects membership after 9 ms before discovering current authority; its root remains absent. Next test: bounded current-quorum discovery from stale seed routing hints, followed by unchanged identity/schema checks before root creation. This does not establish the cause of the earlier CLI process exit. |
| Full-handoff fixture dispatch correction | Inconclusive for full handoff; corrected execution pending | `9f85f6eea4ccd17985c81b336026bdf70ff01626`, [run 43222492743](https://github.com/nikoma/bicdb/actions/runs/43222492743), passed a step-cancellation branch under the full-handoff test name because integration reused mode 4. It did not execute full release, next ACK or Strong-read assertions. Preserve only the cancellation control result. Source `b1ba88c57d99d3601946bd02ef4aee1a4cf070b4` uses disjoint dispatch and an asserted typed full-terminal outcome; [run 43224600329](https://github.com/nikoma/bicdb/actions/runs/43224600329) is pending. No full-handoff success is established. |

| Corrected full same-open handoff terminal | Passed, focused exact unmerged candidate; operator/physical gates remain open | `b1ba88c57d99d3601946bd02ef4aee1a4cf070b4`, [run 43224600329](https://github.com/nikoma/bicdb/actions/runs/43224600329), job 131204452860: one passed, zero failed, 888 filtered, 59.33 seconds; compilation passed. Typed full terminal and retained output confirm three final releases, actual target ACK2, Strong R2, idempotent R2 preservation, stale-writer/generation and original-caller refusal assertions. [Unchanged artifact, hashes and limits](evidence/native-joint-full-handoff-2026-11-12/README.md). This result does not validate later publication/custody composition, cold restart, automatic operator relocation, physical convergence or capacity. |


| Current-authority discovery through stale seed | Passed, focused exact unmerged candidate | `cf91eda3011f4bb441124a9707d2418d9e362430`, [run 43226133271](https://github.com/nikoma/bicdb/actions/runs/43226133271), job 131209693383: two passed, zero failed, 888 filtered, 4.05 seconds; all-target and isolated-consumer compilation passed. Actual stale n0/index5 now discovers n4/current quorum index6 and creates the legitimate fresh learner in 149 ms. Endpoint/incarnation negatives, held real TLS bootstrap deadline and canceled/expired-before-root controls pass. This is metadata-only enrollment (`data_serving=false`), not a data-movement or old CLI process-exit repair claim. Raw job-log SHA-256: `9027df9146e17b4dd983cf8237e80bf8c789ac64f240169560caac0da8931d42`. |
| Concurrent fault tests after scoped publication | Failed opening; publication substage completed, intended fault cuts not tested | `61eb4d92b5938349cf18a283e936eee45fa97e9d`, [run 43225435854](https://github.com/nikoma/bicdb/actions/runs/43225435854): zero passed, three failed, 38.19 seconds; compilation passed. Every fixture completed all three publications and exact lost-reply reconciliation, then exhausted the four-second open phase before its fsync/cancel or foreign-writer cut. [Exact errors, timing diagnosis, complete three-cut custody and smallest next test](evidence/native-joint-open-faults-2026-11-12/README.md). Independent source review of closure correction `674549a4c` passed; runtime successor `e8d263bac` at run 43227153532 is pending. |

Counts are overlapping test invocations at different revisions, not a sum of
unique tests. Debug CI component timings are not physical latency or RPO/RTO.
Actual default joining is metadata-only evidence; it is not proof that physical
data moves automatically or that five nodes provide greater sustained capacity.

## Smallest next tests and open blockers

| Invariant / blocker | Next deterministic evidence required |
| --- | --- |
| Strong-read authority and visibility after leadership movement | Actual acknowledged value immediately after transfer: latest value or bounded retryable unavailable. No successful false absence. Independently observe replica-ready, leader-ready and strong-read-ready; none is inferred solely from election or topology. |
| Stale gateway route | Hold an actual stale route across promotion, record its structured refusal, bounded refresh and final read against the same ACK ledger. Preserve request/response bytes, topology generation, range epoch, leader and applied/commit positions. |
| Same-birth reopen after range-catalog loss | **Baseline failed; scoped repair delivered in main489.** Source 3d0041e09 at run 43218099779 admitted successful absence after only a twin's catalogue was removed. [Retained paired witness](evidence/native-read-catalogue-loss-2026-11-12/README.md). The subsequent fail-closed custody and reopen controls passed and were included in the read489 delivery above. This does not qualify arbitrary snapshot rollback, every recovery defect or Azure causality. Compose later recovery/relocation changes with current main and rerun their relevant final-head checks. |
| Source fails during image installation | Complete actual current-quorum replacement/resume while target is Installing. Cover nonzero target-native prefix and a higher replacement cut, original cancellation/authority, cold restart, retained archived stage ancestry and all inherited command obligations. No historical receipt may become fresh quorum authority. |
| Joint handoff and post-joint recovery | Exact eedf passes full PrepareOnly/Abort3/activation/ACK4/Strong R4 and its separate idempotence/archive controls. Its four-case sibling cohort fails the earlier Strong R2 prerequisite, while an isolated archive case passes. Resolve and rerun the failed cohort; the isolated pass is not a waiver. Replace fixture re-enrollment/orchestration with actual operator-owned producers and qualify the genuine fifth-node path. See the executed results above and [post-joint audit](design/native-post-joint-no-data-audit.md). |
| Hot committed-tail autonomous repair | Exact721 reaches the intended fault and fails recovery after the normal origin handler has exited. Recover the actual quorum-committed command into the same original writer, then obtain exact old/new Strong values and the next ACK/Strong without a manual reset or coordinator replacement. Preserve the complete attempt ledger; do not convert a committed tail into NoData/Abort. See the [failed baseline](evidence/native-origin-pre-wal-2026-11-12/README.md). |
| Original writer and genuine receiver image admission | Both image row-scope construction and transaction authorization require the actual native writer descriptor. Pending's epoch differs from original C1. Supply closed-purpose, actual-proof-bound admission for the original writer and a distinct genuine Registered receiver, without weakening ordinary APP/Strong guards. Prove first and second range images with the same owners; see the [source audit](design/native-closed-handoff-image-admission-audit.md). |
| One stalled move versus independent ranges | Stall actual disk/catch-up work, prove other ranges continue, persist bounded progress and explicit refusal reason; restart controller at promotion and cleanup cuts. Distinguish progress, blocked condition, oscillation and controller error from request-time traces. |
| All-owner restart and command lifecycle | Strictly reopen actual birth/namespace/store UUID, term/vote/log and exact original configuration. Drive registration, catch-up, promotion and data activation through operator commands. See [command-path delta](design/native-command-driven-enrollment-delta.md) and [cold-recovery design](design/native-learner-all-owner-cold-recovery-design.md). |
| Long-running availability | Resolve bounded permanent-slot/inventory and archived-history limits with crash-safe reader and persistence contracts. Small tests do not qualify an 8 GiB/long-running relocation workload by avoiding those limits. |
| Physical repeat and capacity | Reviewed, source/binary-pinned candidate; isolated four-to-five VMs; complete independent attempt/ACK/uncertain/failed/retry ledger; explicit pre/during/post-convergence windows; zero lost ACKs or false strong-read absence; bounded convergence. Only a stable five-node window at equal latency/error budget can establish capacity gain. Separate authorization is required before new spending beyond the expired experiment window. |
| Ordinary PostgreSQL application transparency | Implement and qualify gateway routing, same-view cross-range scans/joins and durable recoverable multi-range SQL decisions. [Minimum execution/recovery design](design/distributed-sql-minimum-execution-contract.md) is design only. Tenant locality remains an optional optimization, never a correctness prerequisite. |

GAP-1's structural repair remains merged source, not integrated physical crash
qualification. WAL/index/FTS, queues/audits, backup/restore and interrupted DDL
must retain their separate evidence gates. The production gate is unchanged:
zero lost acknowledged commits, correct read consistency and routing, bounded
fault-safe rebalance, and sustained throughput gain after complete convergence.
