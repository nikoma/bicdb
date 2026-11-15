# Combined candidate fbb: failed test compilation

**1.0.518-beta / `fbbadef57c5532af6acbc0f3e2016b332a3d16ab`**, tree
`06fe10ab222543a91be665514626fb50c610c2a7`, corrected the earlier
[source785 library errors](native-785-compilation-failure.md). Actual CI checkout
`24c2e1987ac1596adb1fec42b7cff868540d02aa` has the same tree.

Three jobs in [normal Tests run 43675202613](https://github.com/nikoma/bicdb/actions/runs/43675202613)
failed `bicdb-core(lib test)` compilation on the same eight errors. Five calls
used a nonexistent `range` helper in two new activation identity tests; one
retirement diagnostic used `initial.service` instead of the retained host's
`initial.initial.service`; two FIRST diagnostics passed `String` to an `&str`
error helper. The [machine-readable finding](native-fbb-test-compilation-failure.json)
pins jobs, original log hashes, exact diagnostics and source.

All-target checking failed, and its subsequent no-default consumer check was
skipped. Core and Native each successfully built the projection-chaos helper
before attempting and failing test compilation. Neither job executed tests:
there is no test-result summary or native public-finalizer probe outcome.
This differs from source785, whose prerequisite library build failed before
test compilation. New metadata-wait, owned persistence, large-head and Strong
controls remain **not executed** by these jobs.

The same source's [Rails run 43675202479](https://github.com/nikoma/bicdb/actions/runs/43675202479)
passed its real build and application steps. PGwire job 132693725786 passed.
Those results do not substitute for Core/native acceptance. SQL and workspace
jobs were still live at this finding's capture; their final results must be
retained separately before the source is updated. No useful run is cancelled
or restarted for the correction.

The narrow correction `75ed90d49a73c50dce400e8020b5d2947d206f69` uses the existing
checked `RangeId::new(1/2)` constructors, accesses the actual retained field and
borrows the existing formatted diagnostic strings. Every edited site is test
code. Whole-file inverse comparisons, normalized only for formatting, preserve
all prior assertions, durations and production code. Independent source review
and standalone formatting passed; corrected compilation and execution remain
**not tested**. Normal exact-source CI through test compilation and then the
original controls is the next deterministic gate.

This is a **failed compilation** result. Native recovery, automatic convergence,
fault-safe physical expansion, RPO/RTO and sustained throughput remain
unqualified. Original failed-run evidence is preserved separately from fixes.
