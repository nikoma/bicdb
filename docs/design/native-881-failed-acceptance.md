# Native source881: failed acceptance

Source **1.0.515-beta / `881087806b3a5a9792b3e0ba46ba1ba1f2dd0617`**,
tree `41268f7bbd0b206294ab82b8b5ad9e88084a8a40`, failed its normal
[native multi-node job](https://github.com/nikoma/bicdb/actions/runs/43657802254/job/132640172256).
The actual checkout was `962e751180f486207358b2a848c4a5c3e947ca91`, with that
exact tree. The job finished November 15, 2026, at 01:48:52 UTC.

The result was **325 passed, 21 failed and one ignored**, with 941 filtered
tests. The runner reported 4,394.18 seconds. This is not an RTO or throughput
measurement. All jobs are terminal: the other required jobs and Rails passed.
The failed native job blocks acceptance. This source is not merged or
production-qualified.

Source anchors are the
[hot fixture](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests.rs),
[collection controls](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests/collection_binding_tests.rs),
[fresh count controls](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests/fresh_collection_tests.rs),
[interior-history control](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests/publication_scope_tests.rs)
and [remote decision controls](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/source_export_retirement_tests/remote_new_tests/remote_decision_tests.rs).
The [machine-readable result](native-881-failed-acceptance.json) retains exact
failed test identities, source pins and original log offsets.

| Finding and scope | Classification | Actual evidence and limit |
| --- | --- | --- |
| Whole native acceptance | Failed | 325/21/1; no overall acceptance pass |
| Other required jobs and Rails | Passed on source881 | Compilation/nondefault consumer, Core, SQL, PGwire, remaining workspace and static jobs passed; they do not repair Native |
| Seven n4 preparation calls | Failed | `QueryTimedOut` at `hot_policy_tests.rs:756`; intended later cuts were not reached |
| Nine first n2 bridge publications | Failed | `QueryTimedOut` at `hot_policy_tests.rs:876`; all completed their recorded final foreign sweep and local preflight, but not the observed bridge-page-WAL marker |
| Foreign-archive publisher control | Failed diagnostic assertion | Actual after-collection fault and publisher refusal reached; line111 expected another immutable-archive detail |
| Fresh missing-archive count control | Failed diagnostic assertion | Actual candidate construction/count refused; old expected substring mismatch propagated the scope-pointer/archive error to line96. This was not a successful count or a pre-count refusal |
| Retained-middle-history control | Failed | Actual R3/interior-index1 fault completed. The API then returned `QueryTimedOut`, rather than the expected history error, at line142 |
| Normal remote native decision | Failed API completion | First RPC timed out at `remote_decision_tests.rs:118`; retained native evidence records actual XID22 and commit-returned/Healthy |
| Post-commit cancellation and same-attempt retry | Failed API completion | Actual cut2 fired. Same-native/gate assertions at lines240–245 passed before retry timeout at249; retained XID22/body/provenance stayed the same |
| Pre-put refusal and actual commit-sync Unknown controls | Passed in this job | These component passes do not qualify the failed positive path |
| Four prepared-candidate controls and five supervisor controls | Passed in this job | The two original c05 prepared-candidate diagnostic failures now pass |
| Six focused Strong-read/routing controls | Passed in this job | These are now actual source881 executions; the separate earlier source audit retained historical c05 results at its own checkpoint |
| Automatic Serving, physical expansion, cold recovery and capacity gain | Not tested by this job | Late intended cuts after earlier failures cannot be reported as exercised |
| CPU, IO, scheduling or original Azure cause | Inconclusive | Elapsed clocks alone do not isolate a cause |

The seven preparation entry observations had about 1.498 seconds of original
OWN lifetime. Preparation returned after 1.503–1.594 seconds, while the enclosing
caller still had 3.406–3.497 seconds remaining. In the nine publication failures,
the recorded foreign sweeps took 486,313–586,875 microseconds. No
`NATIVE_POST_WAL_FAILURE` marker was observed in any failed block. Marker
absence is **not** proof of rollback, no effect, an unstarted transaction or
acknowledged-data loss. In particular, the two remote cases separately retain
actual known native commits despite API errors.

The middle-history case differs from c05: the original c05 trial lacked an
interior entry, whereas source881 completed the requested actual fault.
Its scope observation had 122,705 microseconds of OWN remaining before the
fault; the post-cut observation was expired by 2,660 microseconds. It must not
be relabeled as either a passed corruption test or the old missing-prerequisite
failure.

## Exact comparison and original evidence

Root independently verified all 347 final case names/outcomes against the full
original log. The names match c05 exactly: eight earlier passes now fail and
two earlier failures now pass. That comparison does not establish identical
internal schedules or causal attribution. The internal process-child entry
remains ignored with its original reason; it is not a hidden failing test.

The complete original raw log is 13,733,863 bytes, SHA-256
`3695b6287ae9de98689a60b7f449cabdae2f444ff3083da7e25e3345ec16832e`.
Its 1,965,512-byte gzip has SHA-256
`c5b3457192c023b9e13ba07573b99adb9569db5a0d2c23b436a2a27d9ede2313`.
The exact classifier SHA-256 is
`0f3403c6f33e7673c4b7c9d9596ea60808b7e83c46bf873eca2a93399b8294c8`;
clock/control evidence is
`28979247d7f58b974276b721f287907e2a12b3d2c2072583cc33fc344ac0304a`;
verified compact remote-native evidence is
`702d725b83ade00a2b245db65ae610094b2d73a97fcef0213b6975379ee22bb2`.
Root's independent full-log and case comparison receipt is
`09a2e0370d7b175aa8a8ad4f5b600e84ed088e65dc9e308a59d2c9aeb9febaba`.
The full-run archive is now frozen and independently verified: 87 manifest
files, 11 lossless original logs, eight unique original ZIPs and 28 members,
with zero missing captures or partial files. API artifact digests/sizes,
stored/raw hashes and ZIP CRCs matched. Root hashed every member without
extraction or opening a database. Archive integrity does not establish database
correctness.

The canonical manifest SHA-256 is
`d368dafef3ecaa4f6bdf38454557a04c9ba6bc87bec0a24be7d72c3d28cb3644`;
complete archive verification is
`e8053fe60a2983cab25c0d64e46f52b6fc4489ff4fc4004b172708b754931db5`;
root independent whole-archive verification is
`55c1e2557a31a5029eccf8443c277eb70c3957ac8f92491d3fb8331ca574cc31`.

## Smallest next deterministic checks

Keep the original OWN, caller and phase windows. Re-exercise the real n4
preparation and first n2 publication with stage/cut evidence sufficient to
identify repeated work, without assigning a cause from elapsed time alone.
The archive controls need their original precise refusals under live authority;
weakening an assertion or treating timeout as the intended rejection is not
closure. Re-run the actual R3 interior fault with its original error and
currentness assertions.

For the two remote cases, distinguish original-call completion from recovery.
A genuine new readback producer may consume the original private
KnownCommitted attempt under fresh actual authority and exact physical
CURRENT/archive/provenance/XID checks. It must not replay native WAL, revive
the old request, or promote actual fsync Unknown from visible bytes. A later
readback success would not retroactively make the first timed-out RPC pass.
The original normal first-call success case still needs to meet its bound.

The [combined actual catch-up/transfer control](native-strong-catchup-transfer-881.md)
is separate, unexecuted source. Neither it nor the
[catalogue proof design](native-remote-catalogue-proof-boundary.md) repairs this
failed run. Original Azure evidence and the production admission gate remain
unchanged.
