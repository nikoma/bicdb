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
