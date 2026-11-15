# INITIAL opening failure evidence (diagnostics, not a recovery fix)

Source7c's Native job failed 363/45/1. Twenty-one FIRST cases returned a real
ConnectionReset/ConnectionRefused error during the normal parallel INITIAL DATA
opening prerequisite. Their later intended FIRST fault/mutation assertions were
not reached. This is not a durable-data-loss finding or a proven common cause
for the separate hot catalogue Open failures.

The direct `NATIVE_FIRST_ACTUAL` stderr events precede libtest's buffered failure
footers. Correlating the serial job's events with the next case outcome locates
these failures at `normal_initial_data_return`. The old helper returns on its
first error; remaining scoped threads finish, but their actual results are not
retained in that helper's report. Its `report.call` elapsed time measures retrieval
of an already completed result, not the actual DATA-opening duration. No missing
per-node results or timings can be reconstructed as new observations.

The diagnostic correction still invokes each original `OpenInitialData` once,
in parallel, with the unchanged five-second action window. It measures the call
inside that actual worker, joins every worker and records ordinal/node, status,
error or original panic payload before selecting the first original failure in
invocation order. No error is retried or relabeled successful. Scoped joins and
kernel work remain non-preemptible as before.

Four test-only failure boundaries in the actual native genesis-read path identify
first metadata acquisition, actual native read-candidate collection, genesis
comparison and final metadata acquisition. They log the same original error and
remaining caller allowance. Projection is local to the read module and stderr
writes are fallible, so diagnostic IO cannot replace the original error. Failures
before these calls (including remaining-budget checks) are not newly covered.
Production error values and protocol operations are
unchanged. A matching error at a boundary is diagnostic DATA, not proof of no
effect, rollback, readiness or a lawful mutation retry.

Status: source/format checks only; not compiled or executed. This correction does
not claim to fix the INITIAL socket failures, underlying listener-transition
race or read availability. The next deterministic test must use actual INITIAL
listeners, held/drained authenticated read paths and the original caller window
to identify which read loses its connection. Any resulting continuation must be
specific to a verified read-only operation and preserve actual identity, quorum,
original error attribution and deadlines. Original failed evidence remains intact.
