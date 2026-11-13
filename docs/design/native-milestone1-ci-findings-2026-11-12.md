# Native Milestone 1 CI findings, November 12, 2026

This is a candidate audit, not a release or production qualification. Public
main at observation was **1.0.481-beta**, `3cce710c689ba8ec7a820538113e389481e04669`.
The tested unmerged head is `f30e5a4b1e4f1ab46b06cc4720f23275d61f64a4`,
tree `e1b28b05897cf28842cef24d34fd595c690eaa1f`, working version 1.0.481-beta.
[Candidate CI](https://github.com/nikoma/bicdb/actions/runs/43150979082) is separate
from the draft PR's older source. A later source fix does not change this result.

| Check or control at that exact source | Classification | Evidence and bounded meaning |
| --- | --- | --- |
| Workspace compilation with all test targets | Passed | `cargo check --workspace --all-targets --locked`; compilation, not execution |
| Formatting, licensing, public repository boundary | Passed | Corresponding candidate workflow jobs |
| PostgreSQL-wire regression job | Passed | Candidate workflow job; does not establish distributed SQL execution |
| Core unit run | Failed | 813 passed, four failed, eight ignored; 219.74 seconds. A separate smoke/child invocation is not added to these totals. Later Core targets were not qualified by this failed job. |
| Actual-open and per-use database fences | Passed | Three controls: retained original directory with replacement database; guard while both latches are held; actual authenticated TLS refusal after original-open expiry with replacement unchanged |
| Native metadata ownership | Passed | Eight controls including actual cold reopen, retained vote/term, missing-state refusal, incomplete initialization cuts and inner-database replacement. These are owned local stores, not physical-host power-loss tests. |
| Commit uncertainty | Passed | Three actual-service controls retain uncertain durable decisions; the fixture's older native mode is not promoted to the newer enforced-mode contract |
| Explicit broker effects | Passed | Native broker-only/mixed transactions refuse before WAL; local legacy transactional-outbox positive remains valid. Automatic audits, sync effects and receipt migration remain open. |
| Four-process initial birth/restart | Passed | Four actual OS processes and cold restart of one process preserve initial configuration and birth identity. No user ACK, automatic expansion, relocation or physical-host claim. |
| Stable five-node capacity and production gate | Not tested | No new Azure cluster, converged throughput comparison or current physical RPO/RTO/latency result |

These selected passing controls are contained in the Core run; they must not be
added to its 813 passes as independent extra coverage. Full decoded logs,
compressed hashes and exact test-line observations are retained in restricted
experiment custody. The CI run identifies the public source and job output.
Temporary database/TLS directories named in the Core log were on the ephemeral
runner; their complete contents were not archived by this CI job. Log custody
must not be described as a retained database image or independent ACK-ledger
reconciliation. Future fault qualification requires explicit artifact collection
before runner teardown.
The earlier compilation failures at `058195602092` and `f53ed5337865` are retained
separately; neither is relabeled as a failed test execution.

## Four failed controls and the next deterministic checks

| Control | Finding at the tested source | Repair/control status and next smallest check |
| --- | --- | --- |
| `native_strong_point_initial_four_metadata_members_rf3_keeps_exact_data_identity` | **Failed, confirmed runtime omission.** Ordinary replica status compared four immutable birth members with three actual data voters; n1/n2 refused Prepare's status proof. | Source fix `f311b8c7e2d` uses the actual range voter projection without weakening credential/range checks or changing the original control. Rerun the unchanged four-member control and its identity negatives; then exercise activation with the same membership. Not yet tested on the fix. |
| `native_metadata_actor_reused_pin_cannot_rebind_old_root_but_retirement_persists` | **Failed assertion.** Decoded envelope differences were only authenticated `leader_id` and its checksum. Term, vote, topology, history and physical store ID did not change. This is not evidence of old-actor promotion or lost data. | The successor control validates checksums and exact state equality except this explicit leader hint, using a metadata-only actor to avoid bypassing data-membership handoff. Rerun retirement, same-pin replacement, actual replacement promotion and strict old-root reopen refusal. Not yet tested on the successor. |
| `native_initial_birth_tls_before_latch_but_busy_actual_factory_is_bounded_unavailable` | **Failed classification assertion.** Actual protected-metadata preflight returned a busy error with different wording from the established `factory is busy` classification. | Preserve the original bounded-before-latch assertion and align the producer's busy classification. Rerun with actual service and database latches held. Source fix is not yet tested. |
| `actual_gap_refusal_preserves_durable_observations_and_retained_prefix_recovers` | **Failed with busy recovery context.** Quorum observer callbacks retain owned operations until cancellation drains; an immediate following exclusive control round can still encounter that refusal. Permanent leakage or data loss is not established. | The sequential harness now waits for bounded actual callback drain without clearing a fence, re-fsyncing or changing original history/value assertions. Rerun the complete refusal-to-prefix-recovery control. Production caller retry and bounded operational recovery remain separate gates. |

An additional source-only dispatcher characterization retains an older cached
view while replaying an already durable prefix. It is component evidence only:
the existing pgwire driver republishes committed metadata each follower loop
and leader heartbeat (`checkpoint_driver.rs::publish_committed_metadata`). It
does not establish an indefinitely stale operational cache or explain the Azure
run. The core native-host prototype still lacks the complete operational loop.

The original Azure campaign remains a **failed scale-out qualification**.
No component result above changes its external-ledger findings, convergence
failure, teardown record or lack of a stable five-node capacity measurement.

## Subsequent candidate results

These are separate source revisions; their results do not replace the failed
`f30e5a4b1e4f` run or qualify newer source.

| Exact source and check | Classification | Result and next smallest check |
| --- | --- | --- |
| `f30e5a4b1e4f1ab46b06cc4720f23275d61f64a4`, remaining jobs in run `43150979082` | Passed | SQL and the rest-of-workspace jobs subsequently completed successfully. The overall run remains failed because Core failed. |
| `3c3b4c9f2ff49fa45db7216b497974da97d8156d`, [run `43152793728`](https://github.com/nikoma/bicdb/actions/runs/43152793728) | Failed | Core: 818 passed, three failed, eight ignored, 227.02 seconds. Compilation of all test targets, PostgreSQL-wire, SQL, formatting, licensing and repository-boundary checks passed. The rest-of-workspace job was still running at this observation. |
| Same `3c3b4c9f2ff4` Core run, ordinary four-metadata-member/RF3 control and actual RF3 activation | Passed | Exact data-voter projection now passes both controls. This is component coverage, not automatic five-node expansion. |
| Same `3c3b4c9f2ff4` Core run, two actor-retirement controls | Failed | Both stopped during fixture creation: data voter n3 remained a metadata learner. No retirement operation ran. Source `d6d30dca1` makes all four actual metadata participants voters; rerun both controls without weakening retirement assertions. |
| Same `3c3b4c9f2ff4` Core run, sparse cold-reopen control | Failed | The expected semantic-error assertion failed without recording the actual error. Cause remains inconclusive. Source `ce71f764c` adds bounded callback drain and exact error diagnostics while preserving the original assertion; rerun the unchanged semantic condition. |
| `26856d0432105d72c520004cdd3d0f9dfa78eebd`, [run `43156562484`](https://github.com/nikoma/bicdb/actions/runs/43156562484) | Failed | Production compilation stopped with E0425: missing `Mutex` import at `distribution_data.rs:933`. Core and all-target logs contain no test execution. Source `b9d7be8e0a78204f2e557257c47f0f1fc4beab8a` imports the required `parking_lot::Mutex`; a new compile/run is required. |
| Same `26856d043210` source, [Rails run `43156562541`](https://github.com/nikoma/bicdb/actions/runs/43156562541) | Failed | Build BicDB failed; Rails application assertions were skipped. This is a compile failure, not a failing Rails behavior test. |
| Actual startup, destination-birth fencing and unassigned-learner factory at integration source `9f7f0024eddc74694ba4bea066d30cacb1f64ca7` | Not tested | These newer sources have not compiled or executed together. Formatting and source-version consistency alone do not prove their behavior. |

Complete compressed Core/all-target logs for `26856d043210`, including SHA-256
indexes, are retained in restricted experiment custody. They show the same
missing import; they do not establish that subsequent compilation is clean.
The native-host operator loop, automatic recovery, joint data-membership
handoff and independently reconciled physical expansion remain open gates.

## Integrated run at `25aebf446105`

[Tests run `43161126843`](https://github.com/nikoma/bicdb/actions/runs/43161126843)
compiled all workspace test targets successfully. Core job `130992295118`
then recorded **827 passed, four failed, eight ignored**, 178.63 seconds.
The separate one-test child invocation is not added to these totals.
[Rails run `43161126886`](https://github.com/nikoma/bicdb/actions/runs/43161126886)
completed successfully, including its application assertions. These results
are for `25aebf446105da552b9ba65205811271e00898b7`, working version
1.0.481-beta, not for subsequent source.

| Control at this source | Classification | Evidence and next check |
| --- | --- | --- |
| Retired actor/reused-pin control, stale same-cert destination before held latches, genuine native learner catch-up/promotion | Passed | Actual owned stores and authenticated TLS where specified by the individual control; not physical-host qualification |
| Four actual processes: initial metadata election, host quorum and writer policies | Passed | The listener remains metadata-only. No data Ready, first ACK, expansion or automatic placement was tested. |
| Original four-member/RF3 identity, retained-prefix recovery and sparse cold-reopen controls | Passed | Previously failed controls now execute successfully at this source; no Azure causal attribution follows |
| `native_service_actual_open_cannot_be_revived_by_outer_arc_or_retained_directory` | Failed | Fixture created a new enforced store without completing its planned native genesis. Listener birth capture refused with `native recovery requires completed fsync image-aware factory`. Source `72d5a13a4` completes actual persisted-schema/genesis production before the original lifetime assertions; rerun required. |
| Prepared cold copy and protected-proposal/metadata-cut cold copies | Failed | Both refused with `bootstrap enrollment requires actual unwritten native genesis`. Cold-loader investigation remains open; no reset or bypass is an acceptable repair. |
| Exposed cold copy refusing pre-vote entry | Failed | The expected `MAY HAVE BEEN EXPOSED` classification assertion failed without recording the actual error. Keep the original no-reset/term assertions and capture the exact earlier refusal on rerun. |
| New outer factory and original-cancellation controls at `1865e6b98` | Not tested | Actual four-member TLS metadata setup plus fifth registered learner source calls create, strict reopen and proposal APIs. It is not an executed fifth-node join or data relocation. |

The entire Core job remains **failed**. Full compressed Core and Rails logs
and their SHA-256 indexes are retained in restricted custody. Other jobs still
running at this observation must be allowed to finish and recorded separately.
Temporary Core database/TLS roots were not uploaded by the workflow.

Separately, abort candidate `51be8f5ef88de8966b4502727f60e0931cf8f320`
failed production compilation in
[run `43161562507`](https://github.com/nikoma/bicdb/actions/runs/43161562507):
missing `actual_transaction_mutations_sha256` imports and an import through
private `db::range_image_native`. The reported unsized-`str` error followed
the unresolved function. No abort test execution is claimed. Narrow imports
and a single-record crate-visible reexport were corrected at
`1ba3a30b18a2270cea0ecd3751a067baf15ed8b5`; its independent
[run `43161948968`](https://github.com/nikoma/bicdb/actions/runs/43161948968)
was still live at this observation. A compile fix does not establish
canonical abort recovery, automatic recovery or post-handoff recovery.

## Abort-enabled runtime at `be1345d3cfee`

[Run `43162977062`](https://github.com/nikoma/bicdb/actions/runs/43162977062)
passed all-target compilation and PostgreSQL-wire. Its Core job
`130998239851` completed with **820 passed, 18 failed, eight ignored**,
305.61 seconds. The separate one-test child invocation is not added to that
total. The tested source is `be1345d3cfeed7b416a9e60af6547633b9b30114`;
it enables the new abort1 contract in the enforced fixtures. Earlier passing
results do not qualify this changed contract.

| Finding/control | Classification | Evidence, limitation and next deterministic action |
| --- | --- | --- |
| All three previously failed cold-preparation controls and the actual-open lifetime control | Passed | Original controls now pass, including unchanged native genesis receipt checks. No reset or fresh genesis was substituted for cold recovery. |
| Actual fifth registered learner: factory create, strict reopen and configuration-only proposal | Passed | Four real metadata TLS listeners plus a fifth requester, on one host. No fifth metadata backend, enrollment, data relocation, Serving or user ACK is claimed. |
| Twelve strong-read/activation controls | Failed | Most reject n1/n2 with `native read/ready has unresolved or mismatched protected slot control`; one final-metadata control times out. Source investigation identifies a peer Commit path updating the raw mirror without publishing the protected ever-Commit fact first. Repair the actual producer ordering and rerun unchanged read/ACK assertions; never remove the inventory guard. These unavailable results do not establish durable loss or false absence. |
| Three canonical abort lifecycle controls | Failed | Prepare-failure, E2 and cold-publication recovery stop at `actual permanent AbortPromise quorum unavailable`. Participant errors need explicit bounded diagnostics. Follower Resolve currently fans out mutating requests under its own transport identity while NormalRecovery requires the current data leader; preserve that authority check and separately observe actual existing permanent promises. |
| Lost durable-Commit negative | Failed | The raw JSON lookup returned null where the assertion expected `committed`. The test uses displayed `rN` (for example `r1`) as a JSON map key while transparent numeric RangeId keys serialize differently. Correct the verifier lookup, retaining all protected ever-Commit/no-effect/row checks. No data-loss finding follows from a null verifier path. |
| Two uninstalled-writer controls | Failed | They assert the range-catalogue file is absent, while the new from-birth protocol requires an empty registered catalogue before an operation. Preserve exact pre/post bytes, zero Prepare/entries, no rows and no application-WAL mutation; do not disable abort1 to retain an obsolete file-absence expectation. |
| Missing/foreign registered-catalogue refusal and birth serialization shape | Passed | Two of the six `native_abort_` controls passed. Four lifecycle controls failed as above; there is no six-test passing abort result. |

Full compressed logs, SHA-256 indexes and all 18 failed test names are retained
in restricted custody. Temporary test database/TLS roots were not uploaded;
log paths are not retained database images. Remaining SQL/workspace jobs were
still live at this observation. The original Azure result remains failed.

The later joint/learner-wire candidate `0934b108a5a8f06acd9ac8582845a45a59d18e79`
failed test compilation in
[run `43163698294`](https://github.com/nikoma/bicdb/actions/runs/43163698294).
Two replacement controls indexed a `BTreeMap<u64, _>` with `RangeId`
(`installing_replacement_case.rs:302,483`, E0308). Source `57cd5c98c`
uses the actual numeric map keys, retaining the same epoch/value assertions.
No joint or new learner-birth RPC test execution is claimed for that run.
Subsequent startup-cancellation and same-open joint-release retry source
changes also require independent validation; no complete activated handoff
fixture or physical convergence result exists yet.


## Focused native candidates and independent metadata defect

These manual runs select Core library tests by substring. The complete suite
matrix is visibly skipped; these are **focused results**, not full regression
or physical qualification. Separate all-target and isolated no-default-Core
consumer compilation checks passed in the runs below.

| Exact source and executed run | Classification | Result and next action |
| --- | --- | --- |
| `cdc5203854bab7f5db194d44e8085977acef5b3c`, [43166739631](https://github.com/nikoma/bicdb/actions/runs/43166739631) | Failed | 227 passed, four failed, two ignored, 619 filtered out, 204.60 s. The genuine delayed unbound metadata ACK could promote after enrollment. Other failures were two fixture expectations and an orphan-replacement fixture permitting an additional native apply before its intended cut. Six native abort controls passed; this does not supply a remote operational recovery loop. |
| `983addf4e5820da245e58a73adeaed0884c38bbf`, [43168817748](https://github.com/nikoma/bicdb/actions/runs/43168817748) | Failed | 230 passed, one failed, two ignored, 619 filtered out, 144.71 s. Native append provenance, the delayed-ACK regression, actual TLS actor-retirement and learner-birth observation controls passed. Orphan replacement stopped on an incorrect topology map-key lookup before its later assertions. `bb725beb11d698f9f4a4a6955196b875335cd89d` corrects that lookup using the actual range ID; that correction is not covered by this run. |
| Independent read foundation `29f9e80c572638b8ff4fcefa01f70e8448535be9`, [43169260087](https://github.com/nikoma/bicdb/actions/runs/43169260087) | Passed | 222 passed, zero failed, two ignored, 616 filtered out, 228.17 s. This branch intentionally excludes Abort/Joint/new learner-birth RPC work. It remains a separate unmerged candidate and does not establish complete Milestone 1. |
| Tests-only public-main baseline `e5abb9f9b842081385d0f93c5ab7abd75b6819df`, [43171290447](https://github.com/nikoma/bicdb/actions/runs/43171290447) | Failed | Zero passed, one failed, 538 filtered out, 0.07 s. A real bounded Append matching index 1 returns ACK 3 from an untouched divergent tail and can prematurely commit a distinct leader topology. Genuine majority recovery and strict reopen controls complete before the failing assertion. This is an independent metadata-consensus defect; no business-row loss or original Azure cause is established. |

The first metadata baseline archives logs, not its temporary state directory.
A separate retained-state rerun and fixed candidate are in flight. The narrow
public-main fix and its reproducible report are being delivered separately from
the larger native engine batch. The native ACK digest must be produced from the
correct matched frontier; a valid physical birth alone cannot make a divergent
suffix replication evidence. No run above qualifies operator join, remote joint
data handoff, complete relocation, current physical RPO/RTO or capacity gain.


## Subsequent executed failures and separate safety delivery

| Exact source and executed check | Classification | Result and next smallest check |
| --- | --- | --- |
| `e3bb1204a0c0d35e091675d631b05881bdac4feb`, [43173550574](https://github.com/nikoma/bicdb/actions/runs/43173550574) | Failed | 232 passed, two failed, two ignored, 620 filtered out, 297.11 s. Compilation and isolated consumer checks passed. The newly added cancellation/expiry and persisted exact-entry retry controls passed. A positive enrollment fixture's old fact expired before its intended identity assertion; the separate orphan-replacement control reached a catalog format validation failure. |
| Same source, ordinary source certification | Failed | `sparse decision field lacks its required reader fence`: the candidate inserted sparse history before advancing its catalog format gate. `e5c4a92b1` moves candidate validation after the existing format upgrade and mandatory-field preparation, preserving validation before mutation/publication. This concerns persistent-format reader compatibility, not a missing strong-read barrier. |
| `71737b29cb8c126cecb73d9966f166ec7145f512`, [43175510487](https://github.com/nikoma/bicdb/actions/runs/43175510487) | Failed | Two replacement controls executed: zero passed, two failed, 856 filtered out, 1.70 s. Both pass the former sparse-format failure and later stop on an assertion expecting the word `duplicate` from a generic current-domain refusal. `965714909` diagnoses genuine duplicate receivers separately after all authenticated-domain checks; the original distinct-vote/duplicate refusal assertions remain. This correction is not covered by the run. |
| `d9fdffd32741df4a216532c4a5d94ce22bb56e1e`, [43174437808](https://github.com/nikoma/bicdb/actions/runs/43174437808) | Failed | Zero passed, one failed, 857 filtered out, 6.17 s. Actual TLS votes generate 17 uncommitted leadership barriers. Two real heartbeat calls leave healthy followers at 16, leader commit at zero; a genuine subsequent election still lacks durable quorum. The current-term Host MQ correctly refuses. This is a native-prototype recovery stall, not evidence about public-main operator behavior or the original Azure cause. |
| Progress fix `232258bd1` and 65-barrier control `2c73af35401a3894e18b69fe5cdaaa746f51baf8` | Not tested at this checkpoint | Resume from accepted per-peer metadata progress across calls, retaining three rounds and 16-entry batches, term/configuration resets and current-term read fencing. The new genuine TLS control checks multi-call progress and conservative reply/NACK/reset handling. [43176666917](https://github.com/nikoma/bicdb/actions/runs/43176666917) was executing; inspect its terminal result before claiming a pass. |
| Retained learner transition `45951aabf`, included in the same `2c73af354` run | Not tested at this checkpoint | Same-service/store drain, actual protected StoreUUID, immutable replacement metadata-only listener and three transition controls are source additions. Prepared cuts quarantine; complete cold host recovery remains open. Compilation alone does not establish catch-up, promotion or data serving. |

Enrollment fixture correction `71737b29c` completes both actual physical factories
before holding an old fact and obtains each NEW source observation before its
identity change. It retains original five-second scope checks; it never refreshes
an old fact after the change or accepts expiry as the intended identity rejection.
The original expiry failure remains recorded above.

The independent public-main metadata frontier repair has its own exact
failing/fixed artifact pair and two passing direct-FSM controls. It is proposed
as **1.0.484-beta**, PR [160](https://github.com/nikoma/bicdb/pull/160), source
`4de8399579d2607aa05a7950d33c09e5b4710bc8`, incorporating the queued
1.0.483 source. Its complete final-version matrix is separate from these focused
native results. None of the larger native feature batch enters that safety PR.
Main merge, binary publication, verified deployment and physical qualification
remain separate states.

## Main 485 repair and later isolated native receipts

Public main **1.0.485-beta**, `e3fa39974d23c13d6212d5b94ab84437271c50bf`,
now contains the separate metadata frontier repair from PR 162. Its tested
`ce02303676fe5fe730ffd7fc822a3a27ee8189aa` tree equals the merge tree.
Full Core (library 536 passed / zero failed / four ignored plus integration
invocations), SQL, PostgreSQL-wire and workspace [run 43179263914](https://github.com/nikoma/bicdb/actions/runs/43179263914),
and [Rails run 43179264201](https://github.com/nikoma/bicdb/actions/runs/43179264201)
pass, with all-target/isolated-consumer compilation and static jobs. The larger
native read/abort/joint/registration candidate below is not merged by that PR.

| Exact source / executed check | Classification | Result and bounded conclusion |
| --- | --- | --- |
| `2c73af354`, [43176666917](https://github.com/nikoma/bicdb/actions/runs/43176666917) | Failed | Focused native cohort: 237 passed / two failed / two ignored / 621 filtered, 226.71 s. Actual 17/65-barrier heartbeat and all three retained-owner learner-transition controls pass. Positive enrollment exposes retirement during a captured heartbeat peer list; replacement controls also have an obsolete generic error expectation. No new physical result. |
| `f1d1398f738868accc20e3757a33752d5e62b726`, [43177871485](https://github.com/nikoma/bicdb/actions/runs/43177871485) | Failed | Two replacement cases fail at the kernel's explicit current-quorum substitution-proof requirement. The source verified a replacement but discarded the consumed live verifier. Retained P1/stage facts alone cannot supply current authority. |
| `f1f7ebdf96b737c8f9c192b2e7831ee108abe6f6`, [43181969102](https://github.com/nikoma/bicdb/actions/runs/43181969102) | Passed, focused | Three real TLS heartbeat controls, zero failures, 860 filtered, 3.49 s: 17/65 genuine barriers and peer retirement during one heartbeat. Full matrix skipped; this is not scheduler or failover qualification. |
| Same `f1f7ebdf`, [43181974286](https://github.com/nikoma/bicdb/actions/runs/43181974286) | Passed, focused | Three real TLS enrollment/cancellation/pending retry controls, zero failures, 860 filtered, 4.86 s. All-target and isolated-consumer compile checks pass in both runs. |
| `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`, [43183211113](https://github.com/nikoma/bicdb/actions/runs/43183211113) | Failed | Zero passed / six failed before substitution assertions. Controlled-WAL preflight ran under its own counted write lease. Corrective source performs quiescent preflight before acquiring that lease and retains the lease only through the actual synchronous publication; it does not relax the guard. |
| `72964ed7`, [43189691020](https://github.com/nikoma/bicdb/actions/runs/43189691020) and [43189697430](https://github.com/nikoma/bicdb/actions/runs/43189697430) | Failed compilation | New complete-joint module needed an explicit path under its explicitly located parent. No runtime assertion result follows. The path was corrected separately. |
| `a96d406596991057e6f4167b7185783f06f3c8fd`, [43190505814](https://github.com/nikoma/bicdb/actions/runs/43190505814) and [43190512897](https://github.com/nikoma/bicdb/actions/runs/43190512897) | Failed compilation | Three new complete-joint assertions mixed `Record` and `Arc<Record>`. Corrected full-record comparisons preserve ledger and read assertions. Neither new test set executed at this source. |
| `dc6a13d4c5823c39f16fdfb8e2f135f3d2987a6b`, [43191731082](https://github.com/nikoma/bicdb/actions/runs/43191731082), job 131092478826 | Passed, focused | Seven actual Installing replacement/resume tests pass, zero failed/ignored, 862 filtered, 10.89 s. Includes dead source/higher epoch, native-proposal orphan/durable P2 cold resume, partial received/audited history, wrong-proof rejection and unmatched actual R0 target point. All-target and isolated-consumer compilation pass. This does not prove a nonzero target point, higher accepted cut, learner repair or general operational recovery. |
| Same `dc6a13d4`, [43191727563](https://github.com/nikoma/bicdb/actions/runs/43191727563), job 131092467880 | Failed | Focused native cohort: 241 passed / one failed / two ignored / 625 filtered, 277.39 s. Four-process CONFIG-registration and all three same-service learner-transition tests pass. Complete-joint fixture fails before its closure assertion because it initialized a planning cache with an already known unbound native voter, then attempted to install born2 C. The existing native-enrollment guard correctly refuses. Correction uses the actual fresh MQ's C to provision an EMPTY planning cache; no native authority is created by that cache. |

Both `dc6` full logs and checksummed indexes are retained privately. Recovery
log raw SHA-256: `1d7b0c3827f98fd3f043b151f3878ed5d3b250fd48adaadefa13c29c657e2e0d`;
native-cohort raw SHA-256: `f1d6d3cd21f8632c3a9016c03ec4c11f4a8174a4d25cf2d65b7739e6c906628e`.
These are log-custody hashes, not retained database images. The fixture's
RETAINED path messages do not mean its private TLS/database directories were
uploaded. No new VM or physical disk-failure experiment ran.

The later `c29227e0e` batch composes the empty planning-cache correction, main
485 (native consensus implementation remains byte-identical across that merge),
a genuine acknowledged R1-target/source-R2 cold-substitution test and the
independent long-lived stale-gateway test source. Those new source controls
require their own executed CI results; the seven `dc6` passes are not attributed
to changed tests. Higher accepted-cut, complete handoff/new-leader ACK/Strong,
all-owner cold host, post-joint NoData, public Init/Join/Serve, protected cleanup,
long-run slot/history capacity and physical convergence remain open.

## Additional actual read and recovery controls

| Exact source / run | Classification | Executed result and limit |
| --- | --- | --- |
| Isolated read `b8b6e9bdc`, [43193116509](https://github.com/nikoma/bicdb/actions/runs/43193116509), job 131097166317 | Passed, focused | 226 passed / zero failed / two ignored / 618 filtered, 235.57 s. Includes the SAME successful gateway retained across genuine fsynced same-voter metadata transfer, actual authenticated Redirect and unready Unavailable replies, then exact full >16 KiB ACKed value after real recovery and local activation. This is not replica/placement movement or a physical/election qualification. All-target and isolated-consumer compilation pass. |
| `c29227e0e`, [43193107973](https://github.com/nikoma/bicdb/actions/runs/43193107973), job 131097138776 | Failed | Seven passed / one failed / 863 filtered, 11.17 s. New acknowledged R1-target case correctly retains original command1 AND uncertain command2, while the reused test expected only command2. No runtime invariant was weakened: the corrected test reads original ACK1's actual source command hash and requires exact obligations `{1,2}` only for that mode; existing R0 modes still require exact `{2}`. |
| Corrected `d42de9fc126d860e4c00d876a31831bb3f007213`, [43194375563](https://github.com/nikoma/bicdb/actions/runs/43194375563), job 131101422512 | Passed, focused | Eight passed / zero failed / zero ignored / 863 filtered, 11.96 s. New nonzero target ACK R1/source R2 cold-substitution case passes with original H1/row/digest, full retained CQ2 obligations, local B and UID. Source cut accepted by P1 and P2 is still R2; this does NOT prove a higher accepted P1R1 to P2R2 transition. All-target and isolated-consumer compilation pass. |
| `c29227e0e`, [43193385516](https://github.com/nikoma/bicdb/actions/runs/43193385516), job 131098078389 | Failed | Zero passed / one failed / 870 filtered, 9.40 s. Complete actual joint fixture passes the planning-cache seam and prepares all NEW members, then `acquire_local_activation` returns QueryTimedOut. ACK2/Strong/actual owner release were not reached. |
| Diagnostic `39d1208f606a237281fda019b82330efd3492560`, [43195089670](https://github.com/nikoma/bicdb/actions/runs/43195089670), job 131103877371 | Failed | Zero passed / one failed / 870 filtered, 7.68 s. All three NEW preparations complete by 673 ms; activation preflights pass at 920, 1994 and 4260 ms. First two fresh acquisitions take 384 and 1024 ms; third fails immediately (0 ms). All four actual metadata locks are available at failure, term1/commit5/last5/generation5. This run points to retained authority/readiness budget exhaustion rather than a five-second blocking RPC, but does not identify the exact expired ancestor. Original budgets remain unchanged. |

Read raw-log SHA-256: `269295ab0bc4d071f29d591183c3ac067f6eb60f55d9457c57ade9db275fa647`.
Eight-case recovery raw-log SHA-256:
`6977646463524d213f25a2211671bf6811358630edcda5e7ea0d41f29e0b5b3a`.
Full compressed logs and indexes remain in restricted custody. Temporary DB/TLS
directories printed by these fixtures are not claimed as archived images.

The installed `NativeWriterHostState` is historical registry policy/lifetime
binding, not a lease: its record/state contains no deadline or cancellation.
Its fresh sealed enrollment observation expires during installation, but an
installed-policy TTL is not an explanation of this activation failure. The next
control reports the ACTUAL prepared-round cancellation deadline and each
retained committed-MQ deadline separately. It adds no deadline extension, proof
refresh, cancellation replacement or authority constructor. A bounded retry with
fresh actual phase authority would need its own durable-history/recovery proof;
it cannot revive an expired old quorum object.

## Main 486 documentation delivery and later component execution

Main **1.0.486-beta**, `8fa5fd02474bdf52a6d9b8e182bcdd70061b490c`,
merged documentation/evidence PR 163. Exact candidate `dda357ae2bb0ada4b830c4e5beca28bbfa1ebbc3`
passed [full run 43192391620](https://github.com/nikoma/bicdb/actions/runs/43192391620),
[Rails run 43192389304](https://github.com/nikoma/bicdb/actions/runs/43192389304),
all-target/isolated-consumer compilation and static checks. Runtime features are
unchanged from 485; no binary publication or deployment was produced. The merged
[status](../ha-milestone1-status-2026-11-12.md) is revision-scoped, not a claim
that the following unmerged native candidate runs on public main.

| Exact source / executed run | Classification | Result and scope |
| --- | --- | --- |
| `80a2ff0b62867c8feeac490ce6534c8786be2bae`, [43198801014](https://github.com/nikoma/bicdb/actions/runs/43198801014), job 131116521452 | Passed, focused | Four cold learner controls / zero failed / zero ignored / 872 filtered, 8.60 s. Actual all-target-owner closure, copied stopped state, original sidecar/both Prepared cuts, same protected UUID, namespace IDs, real TLS bound ACK, Registered loss refusal, bind/cancel retry and strict old-codec rejection. Same-host Core tests, not independent-host or physical-power-loss qualification. All-target/isolated-consumer compilation passes. |
| Same `80a2ff0b`, [43198805159](https://github.com/nikoma/bicdb/actions/runs/43198805159), job 131116534444 | Passed, focused | Higher accepted-cut test: one passed / zero failed / 875 filtered, 2.66 s. P1/R1, genuine 0+2 data recovery/ACK2, then P2/R2 with explicit H1 coverage and both-ACK reconciliation. Data-service delivery uses the declared local trusted-caller fixture, not mTLS or independent hosts. The first `df8f9676` run failed because its test committed a new topology in another metadata directory; the original retained metadata correctly refused the mismatched recovery role. The correction uses the SAME retained metadata directory; no admission guard changed. |
| `c96f27427b855ab20d533eb30dfd3aa11f9fe194`, [43200324976](https://github.com/nikoma/bicdb/actions/runs/43200324976), job 131121746788 | Passed, focused | Nine Installing/replacement controls / zero failed / zero ignored / 867 filtered, 12.04 s, with actual durable vote replies in the shared metadata test-election helper. Includes earlier overlapping controls and the higher-cut case; do not add these totals as unique independent coverage. All-target/isolated-consumer compilation passes. |
| `d46a12a466f703771f5aa1c94338ea8440b394c8`, [43195671022](https://github.com/nikoma/bicdb/actions/runs/43195671022), job 131105880933 | Passed, CLI | One real CLI process test / zero failed, 3.34 s: four actual executable processes, common startup/control commands, real authenticated registration of a fifth CONFIG identity and data refusal. No fifth backend at this source. All-target/isolated-consumer compilation passes. |
| `c96f2742`, [43200333259](https://github.com/nikoma/bicdb/actions/runs/43200333259), job 131121776816 | Passed, CLI | One test / zero failed, 3.46 s. Adds an actual fifth factory/backend process, verified schema transfer, observed birth binding, owned metadata initialization and same-StoreUUID bound ACK catch-up. Explicit wrong own address refuses before root/control-file creation. All five data paths refuse. Manual protocol commands are not automatic join or data rebalance. All-target/isolated-consumer compilation passes. |
| `2cc5debdb9cc180ad8a68013fac7e794608f875a`, [43201140398](https://github.com/nikoma/bicdb/actions/runs/43201140398), job 131124577734 | Passed, CLI | One test / zero failed, 3.62 s. Kills fifth process only after real bound ACK; captures actual stopped metadata AFTER kill/wait; same-root explicit cold command preserves protected StoreUUID, birth, schema, all namespace IDs, durable term/vote/base/log/commit/config and original-C fact, then gets a new actual bound ACK. Role/leader hints are not asserted as durable authority. Bound Learner/no-data scope only; no application commits or physical RPO/RTO claim. All-target/isolated-consumer compilation passes. |
| `a8d36ca1ec944e2d60158154adc504b093c8f330`, [43203415424](https://github.com/nikoma/bicdb/actions/runs/43203415424), job 131132398365 | Failed | Fresh-phase controls: one passed / one failed / 876 filtered, 21.42 s. Original-caller expiry cannot be replaced by a later longer token (passing negative). Positive all-member publication expires at n2 after an actual native WAL publication, before final revalidation returns its receipt. This is uncertain completion, not proof of no write. Correction retains pending attempts, recovers exact protected receipts without changing original audits, and bounds each fresh phase to one local attempt. New correction requires execution; no longer timeout was substituted. |
| `f74fbf4d8e30cba3d07166614ae4785474c5217f`, [43204503368](https://github.com/nikoma/bicdb/actions/runs/43204503368), job 131136135028 | Failed | Supervisor cohort: two passed / one failed / 878 filtered, 9.98 s. Real TLS election/current-term anchor/stability/healing and strict owned-MStore reopen controls pass. Invalid/stale test stops at an obsolete assumption that a committed compacted barrier still has a log entry. Corrected source tests actual malformed snapshot AND genuine pending-entry requests, retaining no-contact-renewal, stale and duplicate-worker assertions. Correction remains unexecuted. Core endpoint stop/drain is not a process kill; reopening MStore while retaining DB is not all-owner DB recovery. All-target/isolated-consumer compilation passes. |

Raw-log SHA-256 values, respectively: cold Core
`f3ef97856f2e9900d067c6a2816caf2df045fa72213a847cc708e70dcf361c21`;
higher-cut `6fe291e204dfb96c2c0fa56994299d2fb8fd8e61488eca1f216e9a80dfad2d3d`;
nine-case recovery `73763440c46d22b8295a78620daff6d311281c681d2e19ba4cdbcabbcbc9f19e`;
four-process CLI `d7b5db0765bd3bd2b59c6971ece29aa280050c6d4785d99cbe955ddac80cbe7b`;
fifth hot CLI `23c455b756e851183d7b5a2c0d40a9d58204970d72ee9459e8a0f578c7e09e33`;
fifth cold CLI `6acfb3b9ad230d848de638ed85523125879be31d58c1d78a74908c12392ca646`.

New automatic metadata joining/promotion and runtime wiring are SOURCE ONLY
until their fresh Core and real CLI runs execute. Global Voter configuration
alone does not establish local voting readiness; actual local committed C,
protected identity and promotion-entry ACK must agree. Metadata voting or cold
metadata continuity does not establish data Serving, automatic physical data
relocation, all-replica activation/release, operational fault recovery, transparent
SQL, stable five-node throughput or production admission. Those gates stay open.
