# Visibility ec4 candidate: failed acceptance

Source **1.0.518-beta / `ec4c5a937e22e18d86b947d24d6813bc9fdf0ccb`**, tree
`d850d150352a46cbb29d732efa8cfeaf1329be4d`, failed the normal
[all-target compilation job](https://github.com/nikoma/bicdb/actions/runs/43659820277/job/132646388538)
at 2026-11-15 00:50:26 UTC. GitHub's actual checkout was
`f1fa20281c7519ecb7ac35af2e6ebe3c29f0a762` with that exact source tree.

The compiler reported eight `E0373` errors: scoped test workers borrowed local
result senders that could not live for the required thread scope. Seven sites
are in `db/commit_visibility_tests/visibility_wait_tests.rs` (baseline lines
200, 469, 637, 740, 770, 867 and 893); the eighth is in
`db/range_image_native/rf3_fixture/native_local_visibility_tests.rs:148`.

| Finding | Classification | Evidence boundary |
| --- | --- | --- |
| `cargo check --workspace --all-targets` | Failed | Eight test-closure lifetime errors; no all-target compile pass |
| Subsequent non-default Core consumer step | Not tested | Skipped after the failed preceding check |
| New Core/native regression execution in this job | Not tested | Compilation did not reach test execution |
| Separate Core library and native multi-node jobs | Failed compilation | Both reported the same eight `E0373` errors; neither started a test binary or reported a Rust test summary |
| Separate SQL job and new generic post-commit cleanup control | Passed on ec4 | The actual SQL control executed; this is not a timed-Unknown or native test result |
| Remaining-workspace job | Failed at runtime | The app-runtime schedule test reached its row/identity assertions, then its original disable call refused nonquiescent admission |
| Earlier formatting, version, legal and source reviews | Passed within their static scope | They did not detect or establish Rust lifetime correctness |
| Production runtime correctness or physical qualification | Not established | Absence of a production-code diagnostic does not prove a successful complete build or runtime behavior |

The complete original raw job log is 1,289,115 bytes, SHA-256
`1fb60d81224dd1a619c2d5519d20a48f00f8c5325c52e580e213b2dabf7f72bd`.
The retained gzip is 159,287 bytes, SHA-256
`ee6ed7a87d1f64970db89a1aee7255c165be7a151bfdc18fe8252e514af388ed`.
This document records that source and job only; independent jobs may compile
or execute different targets and require their own exact result attribution.

The separate [Core library job](https://github.com/nikoma/bicdb/actions/runs/43659820277/job/132646405955)
and [native multi-node job](https://github.com/nikoma/bicdb/actions/runs/43659820277/job/132646405972)
also failed compilation at this source. Their complete original raw-log SHA-256
values are, respectively,
`7cfc89360fee4d5a509ef95c671f3ccff34682c531dd3399eba3078c3b327c8f`
and `fd96fadd5e0681585070df8fbf211a066db222e772459507e5b473fe80e99678`.
These are compile failures, not runtime regression failures or zero-test passes.
The independent SQL and remaining-workspace jobs were still running at this
checkpoint; their outcomes are not inferred from the compilation failures.

Subsequently the [SQL job](https://github.com/nikoma/bicdb/actions/runs/43659820277/job/132646405961)
finished successfully at 2026-11-15 01:33:12 UTC. Its complete original log
records `actual_committed_sync_dimension_error_preserves_sql_ddl_row_guc_and_original_error`
as passed, at raw line 13,023. This invokes a real generic post-commit Sync
dimension error and checks retained committed DDL, rows, GUC state and the
original error. It does not inject a timed visibility Unknown or test PGwire,
distributed atomicity, Strong reads or cold recovery.

The SQL library reported 924 passed, zero failed and four ignored. Across 282
target summaries, the job reported 2,682 passed, zero failed and four ignored.
The full original raw log is 311,472,743 bytes, SHA-256
`15f00b9eed7f0e696c3d196363b448280c38c6536b12b0b936fb9312a2ba9b3c`;
the 36,432,309-byte gzip has SHA-256
`848125b528a0d174a59017cf7171ace236ebe05d88b3036266f7a07f8ac59b4c`.
Root independently checked both full hashes, all summaries and the exact
execution line. The relevant SQL implementation and control are unchanged by
the separate test-capture correction. This remains an ec4 result, not an
execution of the corrected candidate; the three compilation failures remain
failed and the remaining-workspace job was still running at this later cut.

## Separate remaining-workspace failure

The [remaining-workspace job](https://github.com/nikoma/bicdb/actions/runs/43659820277/job/132646406097)
subsequently failed at 2026-11-15 01:41:56 UTC. This was an actual runtime
failure, not another instance of the eight compilation errors.
`script_workflow::route_tests::script_schedules_fire_as_the_schedule_identity_and_write_declared_sql`
failed at `crates/bicdb-app-runtime/src/script_route_tests.rs:690:30` on its
original `runtime.disable("notes").unwrap()` call. The returned error was
`NotReady("catalog change requires quiescent application invocations; retry after draining traffic")`.
The target reported 158 passed, one failed and one ignored. The workspace
runner retained 57 completed target summaries before stopping; it did not
complete the remaining workspace.

The schedule had already satisfied the original nonempty-row, actor and run-ID
assertions. The actual `disable` producer checks lifecycle admission before
lifecycle/file/catalogue mutation. Its refusal condition is an active lifecycle
or nonzero invocation count. The original log does not identify that branch or
record the live counter. A visible SQL row is not proof that the schedule
thread has finished durable completion and released its invocation lease.
Relevant app-runtime sources were unchanged from source881 and main514; this
does not prove timing causality or execution equivalence across those builds.

The complete original REST raw log is 36,346,990 bytes, SHA-256
`63b5aa01a166a25861e1c4e20a9de1baab7c5f0d34c358dd7c46cb54e285f425`;
the 3,899,728-byte gzip has SHA-256
`03f28f3db65d328731647dbc3aa0a08a1d26744e3416070e995d8df91c101e39`.
The separate test correction stops and joins the actual background supervisors
after all original row/identity assertions and before the unchanged disable
assertion. It uses the existing stop/join teardown outside the supervisor-map
lock. It does not clear invocation counters, relax admission, retry or accept
`NotReady`, remove assertions, or change production behavior or timeouts.
This correction remains **not tested** until its exact source executes.

## Complete original capture

All ec4 jobs are terminal. The complete original capture verifies 85 manifest
files, 11 lossless original logs and seven original ZIPs containing 24 members,
with zero missing captures or partial files. API artifact digests and sizes,
stored/raw hashes and ZIP CRCs matched. Root independently repeated those
checks and hashed every member without extraction or opening a database.

The manifest SHA-256 is
`1cfd304f53f130d38929789b914694757b4f921d02fac9b8cd1d105d4430dbf6`;
complete-capture verification is
`25dc3a9b9f17ae1a172a7cb7766eb057990d3c70f1444ee8c25526faa32f796e`;
root independent verification is
`3318dcc32a948a72ad02127b97ac2339231ab1e8825e111fcf376065bfcd76c1`.
The original failed source and bytes remain unchanged by either correction.
Passing SQL, PGwire, Rails and static jobs do not repair failed acceptance.

## Separate correction and next deterministic checks

Test-only correction `779cecab20e61ef9d69c0e3ad572916a9af9a9d5` changes those
eight workers to move their original channel/hook inputs while explicitly
borrowing the same actual DB, fixture, node identities and source records.
Record-clone and map-index syntax preserves the original owned values and
borrowed keys. No transaction owner, native grant or fixture is duplicated.

Both changed test files recover byte-for-byte to ec4 under the exact inverse
capture/reference normalization. Original hooks, actual scheduling cuts,
assertions, RAII release guards, limits and timeouts are unchanged. Production
code, manifests, features and workflows are unchanged by that correction.
Formatting and independent source review passed; **the correction has not
compiled or executed** at this source checkpoint.

After retaining the terminal original results, allow automatic repository CI
on the corrected frozen head: all-target compilation and the
non-default consumer must pass, followed by actual execution of the eight
ordinary controls, native component, SQL cleanup control and broad regressions.
The app-runtime schedule case and the rest of the workspace must also complete
with their original assertions and deadlines.
No source-review receipt or unchanged assertion substitutes for those results.

No binary, release, deployment, five-node expansion, RPO/RTO or capacity gain
is established. Original failed Azure and earlier PR203/204 evidence remain
unchanged.
