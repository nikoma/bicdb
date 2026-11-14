# Failed third append and missing forensic retention

**Failed behavior:**
`initial_data_tests::native_initial_data_actual_cli_four_process_first_ack_and_strong_full_value`
panicked at `crates/bicdb-cli/tests/native_metadata_operator_cli/initial_data_tests.rs:271`
when the third positive `gateway.append` returned `QueryTimedOut` and the
original `unwrap` assertion executed. The third logical operation, expected
payload/hash and exact error were preserved before that assertion. No later
Strong reply exists for that key. Its effect is unresolved, not rolled back.

**Useful partial result:** two earlier positive append acknowledgments and two
exact Strong results were recorded, including present JSON null. These are
public SDK outcomes within this failed fixture. They are not internal fixture
transaction commits, a complete drop/reopen contract or an acknowledged-loss
finding. The expected pre-open rejection is a separate negative control.

**Evidence gap:** the existing journal was fsynced to a fixture TempDir, but
the broad REST workflow did not export the existing external journal/failure
roots and had no corresponding always-upload step. It also did not supply the
runtime source environment. The log therefore retains the measured binary hash
and an unavailable source declaration, but lacks the per-RPC record needed to
separate primary, refresh, routing and native outcome hypotheses. No cause is
established by the display error or by older first-APP failures.

**Smallest deterministic next capture:** run the original target and operation
sequence with the separate retention-only adapter (`cd4405867`), preserving the
original two-second SDK request budgets, four outer test threads, retries,
ordering and assertions. That adapter supplies the actual checkout SHA and
uses the already-fsynced RPC journal and stopped-file allowlist outside the
fixture TempDir, uploading only those exact two subtrees. It does not upload
database roots, TLS material or `/tmp` wholesale. No Strong warmup, manual route
refresh or inserted retry should hide the original failure boundary.

If the failure recurs, first correlate its logical operation with actual
primary/refresh attempt IDs, full errors, routing generation, destinations and
request budgets. A separately labeled diagnostic may reconcile the uncertain
key before teardown using genuine Strong proof and physical evidence, without
turning a failed call into an ACK. That diagnostic must preserve the original
failed assertion and may not assume timeout means rollback.

If the retention adapter is combined with a runtime fix, pin the new source and
results separately. A new pass is not proof of the original cause. Retention,
an API, a roadmap item, simulated replies or an older passing source are not
production qualification. All existing production gates remain open until
their actual current-artifact requirements are met.
