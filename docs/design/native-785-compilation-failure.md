# Combined candidate 785: failed compilation

Source **1.0.518-beta / `785ef07ad52fea1bca7f16143179558f606ea890`**
(tree `fb7758f89401804d756e50ba32bcb1c4ffd69d3c`) failed normal pull-request
CI. The actual checkout `73a298087187e6961a37406752141bc7584c112d` has that same
tree. [Tests run 43674157083](https://github.com/nikoma/bicdb/actions/runs/43674157083)
and [Rails run 43674157110](https://github.com/nikoma/bicdb/actions/runs/43674157110)
are terminal failures. Complete originals are retained separately from fixes.
The [machine-readable finding](native-785-compilation-failure.json) pins each
failed job, exact diagnostics, log hashes and affected source.

All seven failed jobs observed the same five `bicdb-core(lib)` errors: two
private activation-type reexports (E0365), two resulting private imports
(E0603), and direct access to private `NativeAbortProtocol.store_id` (E0616).
Core and Native stopped while building the projection-chaos prerequisite;
their actual tests and test-only compilation were not reached. SQL, PGwire and
Rails application acceptance likewise did not run. The nondefault consumer
step was skipped. Earlier independent workspace targets produced 32 passing
summaries aggregating 498 passed, zero failed and zero ignored before the
later dependency compilation failure; those are not native/application
acceptance results or a count of unique end-to-end scenarios.

The smallest correction aligns the activation alias with the declarations'
existing `pub(in crate::distribution_range_consensus)` scope and replaces
field access with the existing `store_id()` getter. It changes no authority,
value comparison, test assertion or timeout. Correction commit
`fb58b37bb748e167a3787723f43f3384ba846109` has source review and standalone
formatting checks only. Its compiler and runtime outcomes remain untested.
The next deterministic gate is normal CI on the corrected composed source,
first through library compilation and then the original native controls.
Additional test-only compiler or runtime failures remain possible.

This finding is **failed** compilation. Recovery correctness, controller
convergence and physical four-to-five-node expansion are **not tested** by
this run. No production qualification, acknowledged-data-loss conclusion or
capacity gain follows from these diagnostics. Historical passing and failing
runs retain their own source identities.
