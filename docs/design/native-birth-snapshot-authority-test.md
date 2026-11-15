# Native birth snapshot authority boundary regression

This test-only successor starts at candidate `d8115fedf13b0bd7107cfddd2664f2c6ea2f261f`
(1.0.510-beta). Required CI run `43572675143`, Core job `132380251018`,
retained an actual failure at `native_store_birth_tests.rs:304`: the private
`install_snapshot` helper refused to invalidate authority without a live original
operation. The original failure remains failed evidence.

The production caller is `handle_append_request`, which creates its actual
`authority_operation`, validates the incoming snapshot, calls that private
helper and persists the result before completing the operation. The unchanged
old unit called the inner helper directly, then persisted afterward. That call
is now correctly refused. This source finding does not establish a production
snapshot or durability defect, or that the generic error meant poisoned state.

The correction exercises the actual public append/snapshot receiver for both
the rejected missing-birth snapshot and valid added-binding snapshot. It keeps
the original cache, reopen, bootstrap and binding assertions, adds unchanged
invalid state/version and healthy completed-operation checks, and records actual
results plus bounded actual publication-version/durable-state diagnostics before
assertions. No authority constructor, poison reset, version bypass or runtime
predicate is added. No original timeout is changed.

This is metadata DATA validation in an existing unit, not an authenticated TCP
snapshot, native physical recovery, election or production qualification. No
new Rust job or local build ran for this leaf; coherent required CI must compile
and execute the corrected source. The passing parent did not yet have the new
publication-version fence. The private invalid-snapshot tests elsewhere remain
unchanged and still validate before any mutation.
