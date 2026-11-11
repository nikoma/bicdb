# Final local P0 component validation: 1.0.466-beta

Exact clean source `226d9fd80ecd9dbf249fd4fee5984b61311fed47`: 17 integration
cases passed, followed by 503 core unit cases passed and three ignored.
[Commands and source identity](candidate-validation.json) retain the actual profile,
durations, resource minimums and unit-only environment override. Integration cases
include five native row-reservation regressions inherited from the 465 parent.
No resource interruption occurred. Tests use core opt-level 0/debug 0 with optimized
dependencies, not a fully optimized published engine binary.

The unit suite explicitly enables the documented optional release backstop with
`BICDB_PAGED_UNIQUE_APPLY_PROBE=1`. The earlier run without that opt-in retained
one backstop-fixture failure (502 passed, three ignored); it is documented in the
[P0 report](../../p0-range-recovery-progress-2026-11-11.md). Assertions were not
changed or suppressed. Integration tests have no such override.

These are local source checks. They do not prove strong row-read readiness,
correct data handoff, physical convergence, sustained capacity gain, production
qualification or deployment. The failing handoff and compacted-image witnesses
remain open. CI state belongs to the exact GitHub PR/head rather than these local
receipts. This later evidence-only commit does not turn earlier results into tests
of a changed engine revision.
