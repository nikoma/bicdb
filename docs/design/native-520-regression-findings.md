# Native 520 candidate regression findings

This record begins with candidate **1.0.520-beta / `326c363477477950f76bdb51fe6425cab85dc362`**, tree `66972d1997cc2c4626aa1a02ae94dd13d880329b`, and retains later exact-source checkpoints separately. Source326 is a failed CI candidate, not a qualified release. Its tested pull-request checkout was `44d50d087cd1e4fefdba93e3f539b1806f232f83`, with that same tree. Corrections retain their individual compilation and execution status below; an earlier passing result is not inherited by a successor. The original Azure qualification remains failed and its evidence is unchanged.

## Actual execution

### Current head2ce: application passes, HA compilation and Cell rotation failures

This is a **completed, failed CI checkpoint**, not a qualified release.
Source **`2ce4c48202d4d1bb1ef759b08a41dc36f431af6d`**, tree
`f4d72164c2a2a906857b179d44df216d4457b9f8`, is the public PR207 head. Actual
checkout `e1ef338cb41d51d4d9dfbc8660502e4afbc3f366` has that identical tree.
[Tests43696050976](https://github.com/nikoma/bicdb/actions/runs/43696050976)
failed; Rails completed successfully. All logs and original artifacts are
frozen and independently verified. No useful job was cancelled or restarted.

| Scope | Classification | Exact result and limit |
| --- | --- | --- |
| All-target, Core and Native | **Failed compilation** | Core test `storage.rs:2910` has E0282 for an unconstrained `mpsc::channel()` payload. Core/Native helper builds passed, but library-test compilation failed with zero actual Core/Native cases. The nondefault-consumer check was skipped. |
| PGwire | **Passed, CI scope** | 511 passed / 0 failed / 2 ignored across target summaries. Separate Oracle HR: 50/0; embedded HA: 11/0; verification: 3/0. These do not exercise the complete distributed handoff or physical expansion. |
| Rails | **Passed, application CI scope** | [Rails43696050973](https://github.com/nikoma/bicdb/actions/runs/43696050973): 20 runs, 100 assertions, zero failures/errors/skips; real queue/cache process-kill recovery, 24 completed reports and 24 cable messages. This does not qualify distributed HA. |
| SQL | **Passed, CI scope** | 2,682 passed / 0 failed / 4 ignored across 282 summaries. The actual committed-finalization error/DDL cleanup and SQLSTATE mapping controls passed. This is not timed visibility-Unknown or distributed SQL acceptance. |
| Remaining workspace | **Failed** | 851 passed / 1 failed / 1 ignored across 61 summaries and 853 unique case names. Cell signed key rotation failed at `bicdb-cell/src/lib.rs:7589` with `Database(Backup("enrolled physical backup gate is missing"))`; Cell target 39/1/0. Activated/reopen/canary tails and later CLI/AppHost targets were not reached. |
| Page components | **Passed, CI scope** | Checksum/no-repair at raw1396, paired segmented/monolithic gated profiles2649, tracked relayout3855, retained claims4350/4351 and moved-claim4512. Page unit346/0 plus integration152/0. This does not execute Core's backup/bootstrap or HA controls. |
| Physical expansion and release qualification | **Not tested** | No new Azure experiment, stable five-node throughput comparison or production admission. |

The completed raw PGwire log is 1,323,097 bytes, SHA-256
`6ad1a62b5f5256fbbf176f7e04061ab74cec6592889a835f2c88f0a923d3f099`;
Rails is 1,185,534 bytes, SHA-256
`3978cc0707f57abac7640839e1b0b20f9bce7c88bb941d94167f40246a4a691c`.
SQL is 295,664,542 bytes, SHA-256
`59c5698e2bb6ea488349669edee4ec3488d9f9dcd70eebb6c39e1abcf8976bf6`;
its complete gzip is 35,058,855 bytes, SHA-256
`29d2eb64d5706bb9f71f54b9cfd3a2670645e489e2ca460706887cfd4cbc3d74`.
Independent raw-hash/case/summary receipt SHA-256:
`3aa6a420f16ac32430ab6c0e02d0369af98865008be70e684e2b316dc431ee54`.
The committed-error case passed at raw line12410; SQLSTATE mapping at11867.
The former uses real public transaction/model APIs and checks the original
`BicDbError::Memory` containing the synchronous indexing DimensionMismatch
detail, detached committed transaction/DDL undo, preserved row/DDL/GUC after
later ROLLBACK and a fresh SQL session, and healthy recovery with a failed
derived job. It does not invoke the separate Core timed-Unknown cut or establish
HA/failover behavior. Core's eight visibility controls remain unexecuted here.
Remaining-workspace raw log is 39,194,032 bytes, SHA-256
`2c5178564320d017b562fb54dad65516f9c7b20edc31214c45577d470dccb535`;
gzip SHA-256 `a6fd268fc6662bac8629ae8cca9b5b1558844b0f41a9e4fb25c57eafd6b7329f`.
Independent full-log/case/cut receipt SHA-256:
`5065ceb6c7b6a55740e36d475e4ffd9496df20f5526280c3c628a1de6735aa74`.
The failed Cell unwrap/error is at raw307795–307796; the raw error does not name
the internal rotation path or checkpoint. Source inspection finds that rotation
deliberately omits the source gate while copying enrolled FORMAT into a fresh
stage; later guarded stage writes then require a gate that was not initialized.
This is an integration defect to repair through actual stage ownership and a
new stable gate, not by bypassing the mandatory check. A fix is under source
review and remains uncompiled/unexecuted. Core visibility/Standard/bootstrap
and the later CLI/application-port acceptance remain **not tested** here.

The complete archive contains 89 premanifest files, 92 total, 11 complete gzip
logs and seven original ZIPs/24 members, with no missing or partial captures.
Manifest SHA-256:
`eaf355e7d77c1b41813a16547d4bc1848be9bf0fe7085a6e65939aa68b1b36e5`;
independent full-archive verification receipt SHA-256:
`74af72b27b7d31c39c3d9f4e67dbef702d7b79ef67ee4d8566c2afe6c794893b`.
Verification covers exact source/tree/API inventories, every stored/raw hash,
original ZIP API digest/size, CRC and member hashes. The original collector
exited naturally; no replacement collector was started. Archive integrity does
not qualify database behavior or the local successor fixes.

Local correction **`781b4e8029f0e8c747fee69252c94a9f04992103`** specifies
`mpsc::channel::<()>()` in the failed test. Only dropping the sender releases the
existing receive; no payload is sent. The actual thread-held shared guard,
timeouts, ordering and assertions remain unchanged. This correction has source
review and standalone formatting only; it has not been compiled or executed.
The smallest next check is compilation of the actual Core test target followed
by the unchanged backup exclusion control and required broad suites. The
original FIRST/Strong/ACK, cold recovery and controller gates remain open.

An independent source review also confirms the existing
[cross-call bootstrap association gap](standard-live-backup-physical-gate.md):
an originally absent target can be created and acknowledged by a normal opener,
survive the first install's no-replace refusal, then be claimed and replaced by
a distinct finish call after that owner closes. The original live-owner race
control stops before this fresh call. Classification: **failed source safety
review; runtime reproduction not tested**. The next deterministic regression
must retain both real acknowledged records and the first error, close the real
owner, invoke a distinct finish and require target/stage preservation plus safe
subsequent recovery. Original-target and renamed-predecessor association must
survive restart; a RAM flag or newly acquired DirectoryLock does not supply it.
This is not an established cause of the historical Azure visibility failures.

### Earlier head0a: Core guard compilation and Page profile assertion failures

Source **`0a04ccc06db33a04d254a5e00776248d7ea1772a`**, tree `06a13e5accafa331271049a9ae44f1bf3628661a`, failed [Tests 43694564959](https://github.com/nikoma/bicdb/actions/runs/43694564959) and [Rails 43694565004](https://github.com/nikoma/bicdb/actions/runs/43694565004). Actual checkout `8295fbbb5e0c724ddbfe2d6325c30805b847a568` has the identical tree. Six jobs failed Core `E0277` at `paged_collection.rs:494`: the facade promises `Result<impl Drop>`, but the new owning Page guard has field cleanup without an explicit `Drop` implementation. Core and Native stopped in projection-helper compilation with zero actual cases; SQL, PGwire/Oracle, Rails and nondefault runtime acceptance were not reached.

Remaining-workspace reached **384 case-result lines: 383 passed, 1 failed**, over 11 target summaries. Page unit tests passed **346/0**, including the new `physical_gate_profile_requires_actual_page_checksum_and_never_repairs_it` at raw line1358. This is an executed checksum/read-only component result. The later `extent_segments` target failed **6/1** at its original `segmented_superblock_uses_v2_magic_and_legacy_stays_v1` assertion137: actual `BICDBPG6`, expected `BICDBPG2`. The paired monolithic assertion142 was not reached. Later retained-claim, CLI and application-port acceptance was not reached. Do not classify this workspace result as another compile failure or infer a data-loss/transaction outcome from the format assertion.

The complete archive is frozen: 86 premanifest files, 89 total, 11 full gzip logs and six original ZIPs/six members. Manifest SHA-256: `8584a08b5c072b0c2b5f82490e18e02949c3973f5553d2da40960a56b1f10d3b`; independent full-archive receipt SHA-256: `7536d815a45611a893c724dd03a3a29f1f2ebafaecc42c7ade97a2953449a7d7`; independent seven-job raw execution classification SHA-256: `95a8a1e5aca4120836e7518e1e2ac89f4955e90138d9bbc00b2c2b07cee11a27`. Workspace raw SHA-256: `d2d755c06f12a42d2bbcfb6c5bc1dafa9da3978e24d313aad36c1af250de4902`, 750,532 bytes. Native raw SHA-256: `952d8b089c42d2fb54bcdc54026df439559d1e35b7a799a6c7ba55915f5ff73e`, 157,798 bytes. The collector exited naturally after complete capture; no jobs were restarted or cancelled.

Source correction `5e292cf7c1a58d92e72b3cb1523ef7356e9c43ef` preserves the facade's opaque public API and adds explicit `Drop` to its real owning guard. Its original writer/physical fields and automatic release order remain unchanged. The paired [profile test correction](page-gated-profile-test-correction.md), unit `e8acc74054aa69c5e43eb395bae199fb03d7ddba`, requires exact mandatory gated profiles5/6/7/8 while retaining original layout/relayout/reopen assertions and historical input fixtures. The tracked-output mismatch was found by source inspection after the actual first failure; it was not executed in this run. Those successors are uncompiled/unexecuted at this checkpoint. Source-byte comparisons with the historical accepted set are not execution of an old binary. Complete exact-source CI and original HA acceptance remain required.

### Later composed head8bc: compilation failed before HA acceptance

Composed source **`8bc2df66361a3d4307b2943439a8a9d2b4fa0b92`**, tree `ce2dcceb3f8c0fddb34ce64dfc7566018c848637`, failed [Tests43693587297](https://github.com/nikoma/bicdb/actions/runs/43693587297) and [Rails43693587277](https://github.com/nikoma/bicdb/actions/runs/43693587277). Actual checkout `6b96f5507e0619bef1c7284282885dd2372eb9f2` has that exact tree. Seven jobs failed the same production Page `E0308`: `manager.rs:2841` passed `decode_superblock(path, &bytes, size)` while its actual signature at line2748 requires bytes, path, size. This is an executed compiler failure, not failed HA assertions or unavailable CI.

Core and Native failed while building their projection helper, with **zero actual case or summary lines** and their test steps skipped. PGwire/Oracle, SQL, Rails runtime and the nondefault consumer check were not reached. Remaining-workspace retained five earlier successful Lua runtime/workflow cases before hitting the same Page compile error; CLI election, original app-port and new port-owner diagnostics were not reached. Older326 passes below cannot be inherited by changed8bc source.

The complete8bc archive is frozen: 86 premanifest files, 89 total, 11 full gzip logs, six original ZIPs/six members, no missing/partial captures. Manifest SHA-256: `e2db464029f39830458c18f96199a4be8f4f130d7f68c5623efbc12fb64bf18f`; independent full-archive verification receipt SHA-256: `0f0b0020eb55fcbb7d72f5cb96050c60da37825632277977cbecd613e40db575`. Raw Core log: 109,705 bytes, SHA-256 `1a008f0b229143caeb218b50e179f2632de1c916432450eca81e28f0a804df0a`; Native: 110,193 bytes, SHA-256 `d684e10250e6ee87e75296c26ea9330cc1f04ebb3a64bbc0d7b1ba801285ab05`. The collector exited naturally; no useful job was cancelled or restarted.

Local successor **`327a8935f15dc3a3deba018ed1722a5321a17680`** corrects the argument order. Source review also found a separate missing precondition: the shared decoder assumes a page whose checksum/trailer was already verified. The reader-fence helper now calls the actual page verifier before decoding. A genuine fsynced Page file under actual EX is checked, damaged in its body while preserving profile/size, required to return `ChecksumMismatch` without repair, restored and reopened. This corruption control is **not executed**, and the missing check is a source finding rather than observed corruption. The successor has formatting/preservation review only; compilation and all original/new acceptance remain pending.

### Earlier source326: executed runtime results

[Tests run 43682851011](https://github.com/nikoma/bicdb/actions/runs/43682851011) passed all-target and nondefault-consumer compilation, Core, SQL and PGwire/Oracle checks. [Rails run 43682850892](https://github.com/nikoma/bicdb/actions/runs/43682850892) passed its actual application checks. These are separate scopes.

[Native job 132717026027](https://github.com/nikoma/bicdb/actions/runs/43682851011/job/132717026027) failed: **372 passed, 27 failed, 1 ignored**, with 956 filtered out, in 2,334.11 seconds. All 400 individual result lines and the complete raw log were independently verified. The raw log is 35,508,424 bytes, SHA-256 `3deb1461fa4fb269c54e3a6adacafa91623e8f6c69ba221fae7777ca06e38046`; its retained gzip is 3,239,240 bytes, SHA-256 `4d1f868fec4fc19118f3b236d34d75572ba261359a58ab97c2e4fa2a7ad2f4f2`. Independent result receipt SHA-256: `9a214208afd00ddf37d714e1f6a17428d99408d41bd5d97f0db0405238c34942`.

The [remaining-workspace job 132717026000](https://github.com/nikoma/bicdb/actions/runs/43682851011/job/132717026000) also failed, independently of the native suite. It failed `native_metadata_operator_actual_cli_single_join_fresh_becomes_local_metadata_voter` at `crates/bicdb-cli/tests/native_metadata_operator_cli.rs:76`: `operator elect failed: Error: native metadata action refused: "cluster error: native host bootstrap: owned learner metadata status busy"`. The CLI target finished 5 passed / 1 failed. Its actual schedule control had passed earlier; the main519 application-state port-bind test was not reached and receives no candidate326 passing coverage.

The complete workspace raw log is 91,375,658 bytes, SHA-256 `71db759ba79963909dbc6f6edcd3fbf41d23271f6f60d0512329e1b79e2c05da`; retained gzip SHA-256 `118ddf7b4e851d0725c303d11fb3c6a105264a24ac2e8764e791fcb0c144457a`. Independent raw-log/failure-cut receipt SHA-256: `94b12a7d243b250b5c361bab6afaa3a131c4e783df41413b2df9eb908ac1c0d7`.

All jobs have terminated. The complete candidate archive is frozen: 98 premanifest files, 101 total files, 11 full gzip logs and eight original artifact ZIPs containing 61 members, with no missing or partial captures. Manifest SHA-256: `1d92548c8b49f6089eb32d3b03260a35f4721cab4aaeb34a05aee8e80828891e`; independent full-archive verification receipt SHA-256: `952619a06d69e84ce753c44576c2e27f4ff3f1638d1d8998f12938a705c97658`. Verification covers stored and decompressed hashes, exact API source/job/artifact inventories, ZIP API digests/sizes, CRC and member hashes. The collector exited naturally; no useful job was cancelled or restarted. Archive integrity does not qualify database behavior.

| Finding | Classification | What the evidence establishes |
|---|---|---|
| Entire native suite | **Failed** | Twenty-seven actual test failures; this candidate cannot pass its merge gate. |
| Remaining workspace | **Failed** | The CLI join test failed on a busy owned-learner metadata status read. Later application-state tests were not reached. |
| Real follower apply with another range's global visibility gap | **Passed, component scope** | `native_local_rf3_real_follower_apply_completes_before_other_range_global_gap_closes` passed, raw line 17254. This does not qualify the complete handoff or physical deployment. |
| Partial-setup cleanup phase check | **Failed test; source correction untested** | Actual JSON phase was `stopped`; the assertion expected `Stopped`. The existing enum has `serde(rename_all = "snake_case")`. The control did not establish its complete two-cut cleanup result. |
| Two surviving-leader hot controls | **Failed tests; downstream guarantees not tested in these invocations** | Their first genuine recovery call passed `recovered >= 1`. An immediately repeated recovery returned zero; the second assertion failed before the applying probe and subsequent acknowledged-payload/Strong checks. |
| Other FIRST/cross-protocol failures | **Failed tests; causes under investigation** | Actual errors include connection reset/refusal during initial-data opening, changed range configuration, changed source metadata, absent original native decision and changed metadata acquisition authority. An expected injected fault return must be distinguished from the eventual unexpected test failure. |
| Metadata-owner catalogue certification | **Failed path with a source authority mismatch** | The metadata owner asks for a range-leader-only metadata observation to construct its collection plan. A metadata leader distinct from the range leader cannot pass that request's authority check. This requires a separate read-only certification scope, not a weaker range-leader predicate. |
| Large unrelated-catalogue control | **Failed test with passed earlier components; timeout cause inconclusive** | The real directory-sync fault fired once; fresh recovery, same native/Frozen ownership and retained-intent size above the former 8 MiB aggregate cap passed. Large-entry preservation verification then passed. The test later ended with `QueryTimedOut` at `hot_policy_tests/data_tests.rs:105` during DATA-policy installation. This is not complete capacity/DATA-availability qualification, evidence of acknowledged-data loss or proof of a particular performance bottleneck. |
| Physical expansion, cold recovery and sustained capacity gain | **Not tested by this run** | These are in-process CI controls; they do not replace the independent four-to-five-node experiment. |

## Reproducible test corrections

### Follow-up source status at the source326 checkpoint

At this historical checkpoint the public candidate was `326c363477477950f76bdb51fe6425cab85dc362`, with the failed execution above. The following corrections were separate source units without compilation or runtime results at that checkpoint. Later exact-source execution is recorded above; it does not establish unexecuted acceptance cuts:

| Source unit | Source status | Remaining execution gate |
|---|---|---|
| Owned-learner status acquisition, `6c29102c617564fb3a87e73abc8959b03c7b1798`, composed locally as `e4edfe0790f2d699a58d3649a3d240f6fe183b34` | Bounded wait on the same actual metadata mutex, using the existing caller/OWN window. Release, cancellation and expiry controls defined. The original election is not repeated. | Compile and execute those controls and the unchanged failing CLI test. Cold status uses the helper but has no separate executed held-lock control. |
| Initial listener transition, `a179fce0f8b5304848af5ed4f50b2c8f1c1c57ea` | Locally composed source unit. Only the pre-enrollment read retries privately identified socket failures within the original action deadline and actual service open/slot. Accepted authenticated worker drain and an unaccepted backlog socket have distinct controls. | Compile, reach both actual socket cuts, then pass unchanged full initial-data/FIRST acceptance. The component fixture retains later genesis errors and does not assert full DATA readiness. |
| Backup/bootstrap ownership, `38d15efbb815e673859ebfd8b67a450acdba322d` plus `d6760a938ceb6983abcc28196239864a7764ad13` and `c81a26981ddc86c2215040e11041d3c91a7cad76` | Locally composed source units. Retained physical custody covers selected writers and both restore codecs' HA/READY tails. Namespace publication uses actual held owners and atomic no-replace renames; an interrupted predecessor remains until parent sync. | Compile and execute genuine contention, retained-child, both-codec interruption, namespace/preimage cuts and original backup/bootstrap regressions. Cross-call target intent and replaced-tree logical association remain open cold-protocol blockers. Windows held-handle rename and old pre-format bootstrap remain unqualified. |
| Final source-frame metadata acquisition, `62313619ddd7fa2625067e169c95218e65cd5e06`; controls `72740ea03f02370920dc8fb7d5510741b30fea93` plus `048be25abc9ef7679e31b4c78e1dc9fe23b94dc0` | Locally composed correction and three genuine final-frame held-lock control definitions. The child signals the actual server result only after releasing the session mutex, avoiding a false diagnostic-busy race. | Compile and execute real same-session release, caller cancellation with OWN live, and original earliest-scope expiry; retain separate wire/server outcomes and the full unchanged frame/payload assertions. Independent OWN-only expiry is not established by the coupled expiry definition. |
| Metadata-owner catalogue certification, `8ea6251fab97b913efdf807d30223ce8810135b4` | Locally composed source unit. Distinct read-only actual metadata-owner scope; strict range-leader/native-role factories preserved. Separate TLS guard and new-control expiry/guard-lifetime corrections are retained. | Compile and execute distinct-owner, foreign-owner, cancellation, coupled-expiry, exact ancestry and metadata-version controls plus full FIRST acceptance. Independent OWN-only expiry and bounded progress under unrelated metadata churn are not established. |

Independent source review is partial and does not substitute for these gates. No replacement CI candidate, new binary, merged correction or physical experiment is established by these source units.

The cleanup failure is in `crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/first_owned_tests.rs`, test `native_first_partial_normal_setup_error_and_panic_stop_returned_supervisors`. Raw lines 21756–21759 show the assertion and its two values. `distribution_native_host/operator.rs` declares the serialized phase convention; `operator/first_acceptance.rs::test_first_stop_state` serializes the actual enum. Local correction `378e62bb9acb2af4e21ae96e1f6a9e8594336c0f` changes only the expected string to `stopped`. Actual stop success, absent supervisor and stopped control-listener assertions remain required.

The two hot failures are `native_registered_fifth_remote_hot_data_follower_binds_without_fake_leader_ready` and `native_registered_fifth_remote_hot_surviving_leader_actual_pending_callback_rejects_foreign_open`. Both execute `hot_policy_tests/data_tests.rs::exercise`. At the tested source it called the same surviving leader's `recover_initial_data_leadership` twice, with no intervening epoch change. `distribution_range_consensus.rs::recover_local_leadership` selects only epochs absent from `recovered_epochs` and counts newly completed recoveries. Thus zero from the second call does not establish failure of the first recovery. Raw failure lines 89543–89544 and 117867–117868 precede the applying probe and original-payload read loop.

Local correction `b1f84e02e8deebef5c54f4e35c3207510effe9d8` invokes that real recovery once through the existing operation ledger, retains its actual `(leader, recovered)` result and requires both original positive-count assertions. It retains the original five-second recovery budget, follower-not-leader checks, foreign-open probe and complete subsequent ACK/Strong/write assertions. No production authority or readiness is synthesized. Passing source checks do not establish passing execution.

The smallest next controls are the existing exact cleanup test and the two exact hot tests on the corrected source. They must reach their intended fault/read/write cuts and complete all original assertions. A missing cut remains a failing or inconclusive prerequisite. Do not extend their deadlines, skip their assertions or treat a safe refusal as successful recovery.

For the initial-data connection failures, the next deterministic test should hold a real peer at the control-to-DATA listener transition while another host performs its original finite `OpenInitialData` action. Preserve the actual transport error, retained owner and original deadline; then prove bounded continuation with that owner after the listener resumes. The current logs alone do not identify the causal interleaving. For changed-metadata failures, independently advance unrelated metadata and selected range membership at explicit barriers; distinguish legitimate invalidation from unnecessary failure under unrelated progress before changing any freshness rule.

The FIRST failure inventory is 17 setup failures before the requested exercise, one cross-protocol exercise failure, five other exercise failures and one cleanup serialization mismatch. At raw line 20448 a genuine OLD-recipient native-decision request returned service contention; its subsequent known read at line 20451 failed because original D was absent. That sequence must be distinguished from a retained unknown attempt. The three passing FIRST cases were lost-Close read-only resolution, native-decision reply-loss read-only resolution and server-cancelled queued-request handling. These individual controls do not turn the failed suite into a passing handoff qualification.

The exact source of `source metadata changed before frame` is another important distinction: `distribution_range_image/remote_source.rs` returned that text when its final metadata `try_lock()` failed, before `proof.require_current` could check freshness. The matching runtime error therefore establishes mutex contention at that point, rather than an observed topology change. Local source correction `62313619ddd7fa2625067e169c95218e65cd5e06` waits on that same metadata owner within the minimum original caller, OWN and source-session windows. It rechecks cancellation/expiry after acquisition and retains the unchanged physical/native freshness predicates. It also applies those original-window checks to the earlier metadata acquisition in that same observation. Formatting passed; actual held-lock release/cancellation/expiry controls and compilation remain pending. This is not a blanket retry of source frames or native mutations.

The errors at raw lines 20124 and 20728 occurred in `actual_metadata_owner_independent_dual_durability_certification`. At the tested source, `distribution_native_host/provisional_listener/registration/first_catalogue_certification.rs:123` calls `acquire_metadata_read_observation` with the metadata owner's node ID and the selected capture range, then starts `NativeJointOldRound` to obtain a plan. `distribution_consensus.rs::validate_acquire_requester` requires that requester to be the range's leader. The FIRST fixture deliberately selects a range whose leader differs from the metadata leader. The certification path therefore applies the wrong authority requirement for that supported role separation. This is distinct from a busy mutex and must not receive a generic transport retry.

The correction needs a distinct read-only catalogue-certification scope produced from the actual metadata owner's HostOWN and protected metadata context. It must compare the complete pending manifest, birth/pin/schema/restore identities and original OLD-close ancestry, independently collect actual recipient native/catalogue durability, and retain fresh metadata-owner validation before proposing the candidate. It must not convert a HostOWN observation into a recovery role, local OLD quorum or native mutation plan. The composed [source contract](metadata-owner-catalogue-certification-scope.md), final unit `8ea6251fab97b913efdf807d30223ce8810135b4`, defines that path and genuine refusal controls while preserving the existing distinct-leader tests. Neither fixture co-location nor a weaker `validate_acquire_requester` is used. Compilation and execution remain **not tested**. Its original protected metadata version deliberately fails closed on unrelated changes; this is not bounded-convergence qualification.

## Follow-up bootstrap source gap

Review of the unexecuted `38d15efbb815e673859ebfd8b67a450acdba322d` unit confirmed that optional ownership captured for an absent old/replaced tree did not authorize later mutation of a newly appearing tree. `finish_standby_bootstrap` could subsequently use fresh path-presence checks to rename/remove it; its completion helper could also remove the current replaced path without an actual retained owner. A normal Core opener which passed its bootstrap preflight before staging appeared can create that root in the intervening window. An optional absent conventional Page child has a related standalone-Page-open race. These are confirmed source gaps; a runtime failure or historical data-loss cause has **not been tested**.

The corrective source units `d6760a938ceb6983abcc28196239864a7764ad13` and `c81a26981ddc86c2215040e11041d3c91a7cad76` retain actual owner/FD identity across moves, use atomic no-replace publication, refuse newly present unowned trees and reserve the conventional child through a genuine DirectoryLock. The separate moved-path observation preserves the strict writer-transfer validator and opens no extra Unix `store.lock` descriptor. Genuine normal Core/Page controls are defined at the preflight, custody and completion boundaries. They retain full locally committed payloads; they are not HA/SDK ACK or physical evidence. A repeated second-rename/parent-sync fault control also requires the original predecessor to remain until the durability cut. All these definitions remain **not tested**.

These changes close only the examined same-attempt source gaps. They do not persist the original target intent across calls: after the initial no-clobber error and a competing owner's later close, a fresh installer may claim that newly existing root while READY remains. A physical claim also does not prove the logical association of a replaced tree after a crash. Those explicit cold/recovery protocol blockers remain open; passing the new controls cannot establish their closure. See the [physical-gate contract](standard-live-backup-physical-gate.md).

## Separate main519 workspace failure

Main **1.0.519-beta / `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`** has a failed post-merge [Tests run 43677527718](https://github.com/nikoma/bicdb/actions/runs/43677527718). Its [Native job 132700843200](https://github.com/nikoma/bicdb/actions/runs/43677527718/job/132700843200) completed **348 passed, 0 failed, 1 ignored**, with 944 filtered out, in 2,183.25 seconds. The entire log and all 349 unique case results have now been independently reviewed and reconciled. Remaining-workspace job `132700843144` failed `unmigrated_legacy_jobs_are_rejected_before_any_state_listener_starts` at `crates/bicdb-app-host/tests/replicated_state.rs:313`, on the final replicated reopen: `Address already in use (os error 98)`.

All 349 main519 Native case names also ran in candidate326. Candidate326 added 51 cases: 25 passed and 26 failed. Of its 27 failures, 26 therefore have neither a test definition nor an actual execution in main519. Only `native_registered_fifth_remote_hot_data_follower_binds_without_fake_leader_ready` existed and passed in main519. That wrapper retained its name/body, but its candidate exercise added the second positive recovery-count assertion which failed before the original downstream probe/read checks. The common hot fixture budget also differs between the revisions (main519 4,000 ms; candidate326 1,500 ms). A matching name is not equivalent source/cut coverage; these facts do not by themselves identify a runtime regression or attribute a failure to that budget. The candidate's failed acceptance gate remains failed.

The separately frozen main519 Native scope contains 11 payload files, one manifest and one verification receipt; it does not claim to archive the other jobs or artifacts. Its raw Native log is 13,543,529 bytes, SHA-256 `da06ccedd111fc3d420c9ffb4f70691702f9ee1d7a31d2872af0dd9ce0305f10`; retained gzip SHA-256 `0aebda0d45fff90cb09f1530418ccd43a838997c2f646651bd60ffc3342f55ec`. Manifest SHA-256: `7eefde57b7e1023328b6b0ffbe107793f80319323d439444e54faeda42d42bfe`. The complete 27-case comparison matrix SHA-256 is `bff00b6060c94094de4bb6dfbb68ce0feb68c8d0d1319a4340a3f62331b4c832`; independent full-log/archive/case comparison receipt SHA-256 is `c99505e1ce75810637baf4eacd850dee47fe11ff3da4f0b925afb4c220c9e28a`. This is executed CI evidence, not physical or whole-release qualification.

The earlier no-listener bind assertion at line 306 and standalone migration/queued-job assertion at lines 310–311 had succeeded. The final prepare, consume and sync-policy assertions after line 313 were not reached. The test's port helper releases its reservation listeners before reuse; a port collision is plausible, but the competing owner was not recorded and the cause remains **inconclusive**. Do not relabel this as a schedule failure, unavailable CI or a demonstrated replication defect. The candidate326 test source is byte-identical and its actual result must be recorded separately.

The complete main519 workspace raw log is 134,690,258 bytes, SHA-256 `a080beeb01ecf60d3f360b44f48125a129dc4a131fd03039ef80336ee6358311`; retained gzip SHA-256 `59bed3b33e4c6e99e48b90b1d57c56f5e64a674afeeec59bc80a2ddd163770a0`. The smallest deterministic diagnostic is to introduce a known competing port owner at an explicit reservation/reopen barrier and capture both the bind owner and exact startup stage, while retaining the original migration, no-listener, binding and queued-job assertions. Merely rerunning until green would not explain the failure.

A new, uncompiled control is defined in `crates/bicdb-app-host/tests/explicit_port_owner/mod.rs`. It retains a genuine competing listener acquired after the actual standalone job migration, requires the exact typed `PgWireError::Io(AddrInUse)` from the original failed replicated open, and records the persisted replica binding. That binding is an important possible startup effect: its initializer runs before HA listener binding. The control then releases only its own listener and requires a distinct open with the exact same replica configuration, followed by the original job-consumption and sync-policy checks. It preserves the old error and does not replay a transaction or claim that the failed startup was effect-free. The original failing test body remains unchanged. This definition is **not tested**; even a passing control would establish this known-owner scenario, not identify the unrecorded owner in the historical main519 run.

The next smallest CLI status control should hold the actual learner metadata mutex during a finite status/election action, release it and require completion with the same original caller. Separate real cancellation and expiry cuts must return their exact errors without starting or repeating an election. A generic busy-string retry would not establish this contract.

Source tracing places this busy status refresh after `elect_metadata_control` returned successfully. The CLI's `operator elect failed` message therefore does not prove that no election happened. A correction must wait for the original status read within its original caller/OWN window and preserve the actual election result; it must not repeat the election to make the CLI command appear successful.

No new binary, deployment, Azure allocation, production admission, RPO/RTO result or post-convergence throughput measurement follows from these findings.
