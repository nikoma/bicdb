# Temporal export preflight and current native candidate

The exporter now rejects unsupported timestamp precision and local-zone
declarations before creating catalog or row files. Its projection helper also
checks the same contract. New encoding metadata records the six-digit maximum,
with region identity and local-zone support explicitly false. This protects
against silent data shortening; full Oracle temporal support remains open.

The [validation record](validation-results.json) reports five preflight tests
and 14 existing migration unit tests passed. Tests cover all supported precisions
and offset forms, eight rejected declarations, refusal before any output files,
and exclusion of view-only declarations from base-table preflight. The old
exporter fails the new rejection regression as expected. Generated SQL is
unchanged for every one of the 915 exported application table projections.
These boundary checks were tested offline; no new live Oracle export is claimed
for this change. The preceding complete Oracle replay/export checkpoint retains
its own source and scope.

A native candidate was separately built from merged source b984572, including
routine-result and lateral row-lock changes. One compiler job and one CPU affinity
kept the build bounded under an active 14 GiB available-memory floor. Its native
migration-tool batch ran 85 tests: **83 passed and two Oracle-only fixtures were
skipped**, because the reference service was intentionally stopped to reserve
memory for target installation. The prior independent Oracle qualification is
separate; skips are not counted as passes. Candidate source and binary digest
are recorded independently of the Python preflight change.

The candidate is available for original application diagnostics; it was not
installed into the accepted application deployment. Completed source relationship
installation, target/source equality, full-schema business workflows and complete
application acceptance remain unqualified. No GitHub workflows were changed or
manually dispatched.
