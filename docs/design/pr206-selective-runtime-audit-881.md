# PR206 runtime audit and selective private port

This records a read-only audit of external PR206 at `23e2b7682ccbfd5db8fbca93e9fc2740f842aefc`, followed by a private source-only port onto `881087806b3a5a9792b3e0ba46ba1ba1f2dd0617`. It changes no source version or workflow. The prior audit JSON remains immutable: `pr206-23e2-independent-source-evidence-audit.json`, SHA256 `9901ed1893dccbea79a76f5bddf456bd837fcab916205eff5c94e4adc37b9cc8`.

The source versions and test results below refer to those exact revisions. The audit does not establish the current state of a later PR head, main push, build, release or deployment.

## Evidence and comparability

| Check and scope | Outcome | Evidence or limit |
| --- | --- | --- |
| External hot fixture uses a different OWN window | Passed | Its [fixture lines 208–224](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline.rs#L208-L224) change the connection/OWN ceiling from 1.5s to 4s. The existing requested 4s phase is consequently less tightly bounded. This is a test-only change, not an identified production deadline increase. |
| External workflow differs | Passed | [Native line 244](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/.github/workflows/tests.yml#L244) adds workspace `profile.dev.opt-level=1`; [focused lines 335–350](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/.github/workflows/tests.yml#L335-L350) select serial execution and optimization for named multi-node filters. Broad native execution was already serial. Performance settings can be assessed separately from unchanged-budget acceptance. |
| Claimed shared 39/0 establishes the original 881/c05 acceptance contract | Failed | Both the source and validation configuration differ. The external branch descends the older `503964ae9` native line; it does not include eight later negative cases, bounded supervisor admission/reader control, RF3 bounded actual-point acquisition or the later scoped identity/countercontrols in 881. An ancestry difference is not evidence of intentional deletion. |
| Exact shared-run command, head, runner and lossless result reconciliation | Not tested | No such links were supplied in the inspected PR body, comments, reviews or release note. The [release lines 54–61](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/docs/releases/1.0.517-beta.md#L54-L61) claim 30 hot + 3 custody + 6 fifth tests passed, while the inspected PR body still said 29/30 and one follower failure. Required run `43659430043` was live, with no artifacts yet, at the audit GET. These are statements about inspected evidence, not a claim that private logs cannot exist. |
| CPU/I/O explanation or original Azure cause | Inconclusive | Changed optimization, time windows and source do not isolate a cause. No physical reproduction was performed. |

This private port keeps the original 881 1.5s fixture I/O/OWN ceiling, 5s requests, 2s SDK timeout, all old assertions, eight later negative cases, RF3 correction, supervisor checks, and workflows. Final acceptance still needs those original conditions and retained negative cases on the final composed source. A different optimized profile must be disclosed independently, never reported as an unchanged configuration.

## Runtime changes selected

The following external changes are ported as source, with their original provenance preserved in the private freeze receipt:

- Verified Serving range selection at APP preflight, from `5d0c60f6ccf66eb109d18dbe94f1da1a632b278a`. It still checks the actual writer/database/registry and refuses unresolved publication.
- The private actual-pending callback read, from `90ebf348d2178c3d963210b72619d7bed3fd35e0`. It requires the actual database open, exact stored pending command (ignoring only its materialization mark), source birth, previous index and previous receipt. All ordinary lineage readers still reject a pending command. The existing callback continues to validate the actual WAL apply receipt. No command/token is constructed from test DATA.
- Actual retained INITIAL coordinator leadership recovery, from `04f0a9b231713d74a2c36df767406e87cbbfdd3f`. It uses the original live owner/coordinator and quorum. The follower acceptance invokes that real producer on the surviving leader before demanding a new ACK; the target's follower-readiness assertions remain intact. This still has no automatic controller caller. A zero recovery result is not converted into readiness.
- Retirement-only validation of the exact own published successor or strict predecessor, from `e12dacf831abb18c55f50938aef9c8516bb52325`, with the previous-handoff predecessor correction from `23e2b7682ccbfd5db8fbca93e9fc2740f842aefc`. The original native close/promise, physical prefix, owner and Frozen checks remain. Ordinary `require_current` does not use this retirement exception.

These are **not tested** compilation/runtime changes. The normal native persistence body, strict ordinary APIs, final scoped publisher, foreign sweeps, Controlled/quiescence checks and native recovery gates are preserved. This port does not import PR206's generic quiescence relaxation, removed pin sweeps, duplicate-read optimizations, changed foreign matcher, teardown acceptance change, increased fixture window or workflow changes.

Two additional regression definitions retain the real fixture effects and original windows:

1. A surviving-leader SDK write reaches the actual callback with its genuine private pending owner. An armed DATA-only probe then checks that the ordinary reader still refuses the pending record and a real different DB/open refuses that same owner. The probe returns no owner, Role, permit, writer or Frozen; registry retention is bounded, and its guard removes only its own entry. The original ACK/full-Strong assertions remain.
2. After genuine native Open on actual original survivors, the strict predecessor check must still refuse their own published successor, while the separate retirement-only check succeeds. Actual outputs are saved before assertions. This does not test a full second handoff or registry reclamation.

Both definitions are **not tested**. Failing an earlier finite phase means the later intended cut was not reached; it must not be reported as a passed negative control.

## Activation and unknown persistence boundary

The Registered activation/re-fsync changes are **not ported in this safe leaf**. External [joint identity persistence](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/crates/bicdb-core/src/distribution_range_consensus/registered_joint_identity.rs#L64-L104) temporarily sets aside a global uncertainty boolean. Matching a selected activation does not by itself identify the owner of every whole-catalogue effect or prove an exact current head. This audit does not claim a reachable unrelated-range loss schedule merely from that boolean; the actual producer/lawful schedule must be shown.

The next separate child needs an actual private retained original/candidate whole-head intent captured while the catalogue is durable, before memory/IO; exact Store/open/native bridge/phase-owner binding; current disk and memory comparisons; actual IO return retained before postchecks; and a narrow fresh-admission retry that can reach its own retained candidate without weakening generic currentness. It must refuse absent owner, changed/unrelated head and unknown native provenance. A successful new re-fsync can prove new durability of the same lawful candidate, never historical success of a failed old fsync. Cold reconstruction and whole-head rebasing remain separate gates. Genuine interrupted/changed-head/wrong-owner controls are required. That producer is **not implemented or tested by this leaf**.

## Remaining delivery boundary

There is no automatic range relocation/controller or remote Serving backend in this port. The separate normal-host FIRST/materialization/native-D work remains unmerged and source-only. PR206 also leaves the void five-second visibility wait in [commit_snapshot.rs](https://github.com/nikoma/bicdb/blob/23e2b7682ccbfd5db8fbca93e9fc2740f842aefc/crates/bicdb-core/src/db/commit_snapshot.rs#L48-L75); it does not fix issue205. Later integration must preserve its independent visibility correction and actual native custody.

Full remote catalogue quorum, Open/ALL release, cold recovery, permanently unavailable OLD replacement and source/coordinator/receiver registry reclamation remain required. Passing a finite fixture would not establish production convergence, RPO, RTO, physical four-to-five scale-out or recovery after process loss.
