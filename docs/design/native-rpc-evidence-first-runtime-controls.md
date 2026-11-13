# First RPC evidence controls: exact failures and fixture correction

Executed source: `b32214b904211446c93e2aa7c280b5cf16285ab8`.
Run `43272227064`, Core job `131371509297`: **7 passed, 2 failed,
0 ignored, 922 filtered**, 0.11 seconds. All-target and isolated consumer job
`131371463179` passed. The earlier `db8` constructor-argument compile failure
is separate; none of these nine runtime results belongs to that failed build.
Raw Core log SHA-256:
`24425c9214eaba3b4a80773d77aa16612cb4e06ead6cdbf6b51fd91719d18798`.

`actual_rpc_evidence_mtls_canceled_before_dispatch_records_original_cancellation`
failed its record-count assertion: expected two, actually three. The actual
transport finishes the inner canceled result, then records the final caller
cancellation boundary. The successor checks exactly `invoked`,
`validated_transport_result_error`, `return_boundary_error_after_evidence`,
same attempt/contiguous sequence, both complete cancellation errors, no frame,
no possible dispatch, and no allocated request ID. Production behavior is
unchanged; the extra real diagnostic record must not be discarded.

`actual_rpc_evidence_pre_dispatch_sync_failure_fences_without_application_mutation`
failed BEFORE reaching its observer fault: first `BicDb::open_with_config`
refused at fixture construction line 129 with the generic already-open
message. Therefore its no-mutation/persistence-failure assertions were NOT
executed in this run. The Core opener maps all DirectoryLock acquisition
errors to that message, so the log does not identify the underlying PageError.

The fixture had a concrete ownership defect: TempDir was its first field,
and Drop only requested asynchronous server shutdown. Rust then deleted the
root before dropping DB/server fields; the actual background service could
still retain that database. `bicdb-page/src/lock.rs` correctly retains a
same-process registry keyed by directory device/inode while the owner lives.
Removing a live root can free that directory inode for reuse before the owner
ends. This is a source-backed possible explanation of the first-open refusal,
not a claim that the original log proved inode reuse or ruled out every IO
failure hidden by Core's error mapping.

The test-only successor drains the actual acceptor and counted handlers,
then waits within one finite cleanup window for all other actual service DB
owners to end. The DB field drops BEFORE the now-last TempDir field. If drain
fails, it fails cleanup and retains the root rather than deleting live state.
This is per-fixture ownership cleanup, not test serialization or a network
budget increase. A real Weak-owner/path control in the existing pre-dispatch
test verifies that successful teardown ends the actual DB owner and removes
its own root. Future first-open refusal also reports the read-only directory
lock probe and directory metadata, without retrying or weakening exclusion.

No production observer, directory-lock, routing, authority or deadline code
changes. The same nine tests, including real parallel TLS callbacks and
same-thread reentry, remain selected. The expected-red INITIAL discovery
baseline is a separate branch and is not included. Rustfmt/static checks are
not a Rust typecheck or runtime result; the successor requires new remote
execution before any passing-nine claim. These legacy TLS diagnostic controls
are not native DATA/HA/operator/physical qualification.
