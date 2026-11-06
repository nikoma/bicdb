# Verified offline storage-mode copy — 2026-11-06

`bicdb store migrate-mode` now opens the source in its recorded mode, allowing
both embedded-to-paged and paged-to-embedded copies. It copies protected rows
under the existing exclusive offline administration boundary and preserves
logical collection metadata, including mutation grants, tenant policies, mesh
authorization and empty vector dimensions. Renamed collections receive the
new target's physical storage identity.

Before returning success, the command closes and reopens the target, compares
full stored-record checksums and counts with the source, compares logical
collection contracts and secondary-index definitions, and checks target index
contents. A missing record in the exclusively locked source fails the copy.
The operator `inspect`/`verify` commands now use the persisted engine rather
than assuming embedded storage. The default core open fence remains enforced.

The [exact qualification record](summary.json) reports five core tests and
fifteen CLI tests passing. Coverage includes both migration directions,
protected rows and policies, renamed collections, empty vector dimensions,
unsafe/live source refusals, target integrity/inspection, exports, backup,
cell-mode refusal and environment-password handling. Earlier completed CLI
suites were reused only on the unchanged implementation revision. Two broad
attempts were interrupted by the resource guard when host full memory PSI
exceeded one percent; their unfinished suites are not reported as passing or
failing. The initial protected-row test failure and one feature-gated command
rejection were preserved separately before the final checks completed.

Opening the source uses normal recovery and may checkpoint physical metadata;
it is not a byte-for-byte read-only operation. Use a cold offline clone when
the original must remain immutable. Active flashback history is explicitly
refused before target creation until an SCN/history transfer contract exists.
This is a logical records/collection/secondary-index copy, not full backup,
replication identity, external attachment transfer or historical-state
migration. Both directories must remain available after a verification error.
The original application schema still needs its own paged copy, source-value,
owner, FK/orphan, view and application qualification. No deployment or full
Oracle qualification is established by these tests.
