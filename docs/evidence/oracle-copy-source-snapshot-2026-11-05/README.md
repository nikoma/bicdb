# Frozen source COPY and per-table commit acceptance

The data importer now consumes the exact source bytes it validated, using one
private temporary file per table. It no longer hashes an exported path and then
reopens that path as COPY input. Validation of row shape, count, projection,
encodings and digest completes before target COPY or its transaction begins.
COPY requires an empty uncompleted target table and commits its row-count and
deferred-constraint checks before the migration journal advances.

The [validation record](validation-results.json) separates these checks:

- Four offline tests change the original file at COPY start, reject malformed
  rows/counts/projections/digests before target access, preserve nonempty targets,
  and reject row-count/commit failures without recording a commit.
- Two native tests use PostgreSQL transport with independent Oracle preview and
  PostgreSQL profiles. They retain 38-digit decimals, padded Unicode strings,
  empty/nonempty binary values and NULLs. They verify constraint failure rollback,
  connection reuse and persistence after clean server restart.
- A deferred foreign-key case observes both copied rows inside the transaction,
  then requires commit failure, zero persisted child rows, and a successful retry
  after inserting the parents. The Oracle control also invokes the actual `data`
  command and checks successful and failed table journals.
- Fourteen existing source-value and migration-policy tests passed.
- All 915 retained Oracle source files, containing 185,471 rows, pass the shared
  validator and produce frozen bytes with the manifest's original file digests.
  The largest individual temporary file was 19,241,566 bytes. This was an offline
  source check, not a fresh full-application target import.

Both native fixtures use explicitly quoted object names to make their intended
names identical across profiles. This batch does not qualify case-folding or
error mapping for mismatched quoted/unquoted identifiers. The native engine
candidate was already built from b984572; no engine change or accepted-runtime
upgrade was needed. Heavy compilation was not performed.

The [operating contract](../../oracle-source-value-verification.md) retains the
separate full-value verification phase, stopped-writer requirement and non-atomic
database-commit/filesystem-journal boundary. An interrupted nonempty unjournaled
table is preserved for investigation. Full Oracle-schema application integrity,
fresh source/target equality, recovery/cutover and whole-application qualification
remain separate gates. The running FK migration and queued application pipeline
continue using their frozen, previously validated tools.
