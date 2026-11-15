# Retaining native remote-decision failure evidence

This diagnostic-only source leaf starts at candidate
`4efcfc2cccc51307721777ad7d53a251e67bbbf3` (1.0.512-beta). It has not
been compiled or executed. It changes no production decision protocol, request
deadline, metadata lifetime, retry policy or qualification gate.

## Original observations

[Required CI run 43590137719](https://github.com/nikoma/bicdb/actions/runs/43590137719)
finished with 307 native multi-node tests passed, 23 failed and one ignored.
Three failures are in `remote_decision_tests.rs` under
`crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/source_export_retirement_tests/remote_new_tests/`:

| Test suffix after `native_registered_fifth_remote_decision_` | Actual failing assertion | Classification |
| --- | --- | --- |
| `actual_donor_provenance_and_native_target_record` | The older-reader refusal was absent. | Failed assertion; whether the preceding native publication succeeded is inconclusive in retained CI output. |
| `actual_commit_sync_unknown_never_replaces_xid` | Expected `unknown_effect`, observed `actual_abort_returned`. | Failed assertion; reaching the intended commit-sync fault is not established by that observation. |
| `actual_postcommit_cancel_reconciles_same_xid` | The subsequent request returned `cluster RPC deadline expired`. | Failed retry; the preceding committed-outcome/xid assertions passed, but the retry's blocking stage is inconclusive. |

The fourth existing case, `actual_preput_abort_retains_original_attempt`, passed
in that exact run. This is a CI component result, not physical qualification.
The full native failure log and original per-case sections remain unchanged.
The test's detailed JSON files were written into its private temporary output,
but the current workflow did not upload those files. The retained log therefore
does not establish CPU, disk, scheduler or authority-expiration causation for
these three failures. Timeout observations are not measured production RTO.

## Diagnostic change

The test-only participant records whether its actual native cut callback fired.
The fixture checks that fact before asserting the fault's outcome. The normal
positive case checks the actual publication API result before the older-reader
control. This keeps a refusal before publication from being diagnosed solely as
a reader-fencing failure. Original commit, abort, unknown, same-xid and reader
assertions remain required.

A DATA-only guard captures the first and retry API error strings immediately
after each call returns. It holds no native owner, Session, request token,
read pin or authorization. Only on test panic does it export these observations
and the already-written `remote-decision-*.json` bytes to stderr with the prefix
`NATIVE_REMOTE_DECISION_FAILURE_EVIDENCE`. Successful tests emit nothing from
this guard. No workflow or artifact-selection change is required to retain the
failure output in the raw test log.

Export is bounded to 128 directory entries, 16 matching files, 1 MiB per file
and 4 MiB aggregate file bytes. Two API results are retained, with a 64 KiB bound
per error. An oversized error is explicitly omitted with its original byte count
and SHA-256. Each exported file has its original byte count and SHA-256; UTF-8
is preserved verbatim and other bytes are hex encoded. Enumeration errors,
truncation, skipped files and diagnostic I/O errors report incomplete capture.
Recorded API-result truncation and file-export completeness are separate claims;
both must be inspected. The API list covers returned calls recorded before the
panic; a panic inside an exchange may leave no returned result. This list does
not establish complete RPC coverage. File-name labels use safe lossy display conversion;
their labels are not evidence of exact non-UTF-8 path identity.

These are byte/count bounds, not an operating-system I/O deadline. Diagnostic
write errors are ignored so they do not replace the original assertion failure.
Neither a diagnostic artifact nor reaching a test cut proves a successful
recovery or durability result.

## Next execution and gates

Run the four unchanged actual TLS/native decision scenarios on the coherent
successor candidate, then inspect the real API result, cut-fired observation,
native outcome, commit-sync fault count and retained xid before classifying a
failure. The unknown case must reach its intended commit-sync fault and remain
unknown without replacement; postcommit cancellation must reconcile the same
known xid; preput cancellation must retain its original abort; and the normal
case must publish successfully and reject the older reader. Preserve every
failed attempt rather than accepting a later retry as its replacement.

Formatting, whitespace and synchronized-version checks are static checks only.
No Rust compilation, runtime regression, CI success, cold recovery, automatic
five-node expansion, throughput gain, release or deployment is claimed here.
The original Azure experiment remains a failed scale-out qualification.
