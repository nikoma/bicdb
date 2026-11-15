# Visibility ec4 candidate: compilation failed

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
| Earlier formatting, version, legal and source reviews | Passed within their static scope | They did not detect or establish Rust lifetime correctness |
| Production runtime correctness or physical qualification | Not established | Absence of a production-code diagnostic does not prove a successful complete build or runtime behavior |

The complete original raw job log is 1,289,115 bytes, SHA-256
`1fb60d81224dd1a619c2d5519d20a48f00f8c5325c52e580e213b2dabf7f72bd`.
The retained gzip is 159,287 bytes, SHA-256
`ee6ed7a87d1f64970db89a1aee7255c165be7a151bfdc18fe8252e514af388ed`.
This document records that source and job only; independent jobs may compile
or execute different targets and require their own exact result attribution.

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

Let the current useful jobs finish and retain their exact results. Then allow
automatic repository CI on the corrected frozen head: all-target compilation and the
non-default consumer must pass, followed by actual execution of the eight
ordinary controls, native component, SQL cleanup control and broad regressions.
No source-review receipt or unchanged assertion substitutes for those results.

No binary, release, deployment, five-node expansion, RPO/RTO or capacity gain
is established. Original failed Azure and earlier PR203/204 evidence remain
unchanged.
