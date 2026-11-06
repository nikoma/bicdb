# Full application foreign-key installation checkpoint

The public resumable migration phase installed **all 3,826 exported iDempiere
foreign-key relationships** in a native Oracle-profile candidate database.
The [bounded installation record](installation-summary.json) records the source
manifest, complete private migration-report digest, candidate source and binary
digests, and explicit qualification limits. It includes no application row values
or connection credentials.

The completed report contains 3,826 distinct table/constraint pairs: **912
validated and 2,914 not validated**, referencing 3,823 primary-key and three
unique-key parents. Source validation state was retained. Before/after row
multiset fingerprints match for all **888 participating tables**. Definition
checks preserved **62 routines and 160 views**. The native database shut down
cleanly with exit status zero after verification.

This checkpoint used an already exercised target database. Matching pre/post
fingerprints establish that installing relationships preserved that target's
values; they do not establish equality with the original Oracle export. No rows
were repaired to obtain this installation result.

The subsequent [read-only inventory](orphan-inventory-summary.json) counted all
**3,826 relationships with zero orphan references**, repeated matching before/after
fingerprints for all **888 participating tables**, and preserved the 62 routines
and 160 views. The database again shut down cleanly. The offline
[comparison record](orphan-comparison.json) matches all 3,826 counts to the
independent [Oracle replay](../oracle-export-reference-2026-11-05/README.md),
with no mismatches. Reference and target reports are tied to the same source
manifest and retain their individual digests.

Matching orphan counts does not establish complete row-value equality. Pristine
source value equality, original application business tests with every
relationship installed, and recovery/cutover remain separate gates. In particular, this
checkpoint does not turn NOT VALIDATED relationships into validated ones or
qualify the entire application. The candidate is separate from the accepted
application deployment. No production qualification or Oracle certification is
claimed.

The [migration guide](../../oracle-foreign-key-migration.md) documents the public
commands and writer-quiescence contract. The earlier
[migration-tool validation](../oracle-foreign-key-migration-2026-11-05/README.md)
records the independent native fixtures and PostgreSQL regression coverage;
this installation checkpoint adds full relationship-count evidence rather than
claiming a new engine test run.

## Full-schema runtime diagnostic

A subsequent original application batch used a private copy of this completed
schema, explicitly isolated launcher installation and the previously built
application candidate. All 16 source BLOB columns were reconciled first. The
[partial runtime record](runtime-diagnostic.json) retains **27 reported tests:
22 passed and five failed**, with no reported errors or skips. The original
20-test insert class passed; four shipment assertions and one invoice assertion
failed after process-instance inserts returned transaction conflicts.

The run also reached a slow commit in an original change-log test. A JVM thread
dump showed the caller waiting for commit while the native process continued
consuming CPU. Source inspection identified exclusive reservations shared by
independent child references and a full child-table scan for each deferred
foreign-key hook. General concurrency and direct-child-lookup fixes require
native/profile regression validation and a new application run.

The operator interrupted this disposable attempt after identifying those
issues. The harness eventually killed its JVM and database after their graceful
shutdown deadlines; both returned -9. This is an interrupted validation with
real recorded assertion failures, not a completed 89-test batch, clean shutdown
or crash-recovery qualification. Logs and the disposable database remain for
diagnosis. No assertions were removed or relaxed. All **89 original tests**
remain required on a fresh completed-schema copy, followed by pristine data and
broader application/recovery gates.

## Runtime repetition after the shared-reference fix

The [qualified engine fixes](../oracle-foreign-key-reservations-2026-11-05/README.md)
were repeated on another isolated copy of the completed FK schema. The
[new interrupted-run record](shared-reference-runtime-interruption.json)
contains three completed original JUnit reports: **InOutTest 5/5, InvoiceTest
2/2 and POInsertSQLTest 20/20**, with zero failures, errors or skips. All 16 BLOB
columns were reconciled first, preserving table fingerprints and the 62 routine
and 160 view definitions. The class-load log matches the original pinned test
JAR, selected Oracle provider, BicDB adapter and PostgreSQL driver.

The host memory guard interrupted the run during POTest when available RAM fell
below its 14 GiB floor. The remaining 62 tests are unqualified; this is
**27 completed tests, not an 89-test pass**. Every owned process was subsequently
observed stopped, but clean shutdown and exit statuses are not established by
this forced interruption. Broader packaging, long-running resource behavior,
fresh source-value import and full application/recovery acceptance remain open.
