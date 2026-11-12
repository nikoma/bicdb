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
