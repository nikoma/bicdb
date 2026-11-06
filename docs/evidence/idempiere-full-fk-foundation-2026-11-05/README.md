# Original foundation tests on the completed FK schema

All **89 selected original iDempiere tests pass**, with no failures, errors or
skips: InOutTest 5, InvoiceTest 2, POInsertSQLTest 20, POTest 53 and
InvoiceCustomerTest 9. The [validation record](validation.json) retains each
original JUnit report digest, exact source/binary identity, class-load provenance
and process shutdown results.

The database was an isolated copy of the completed source-declared FK baseline,
which contains all 3,826 relationships with their source validation/timing state.
The public migration reconciled all 16 source BLOB columns before testing,
preserving table fingerprints and the 62 routine and 160 view definitions at
that point. The test classes, selected Oracle provider, JDBC adapter and ordinary
PostgreSQL driver all loaded from their pinned artifacts. No application test
bytecode or assertion was changed.

The JVM and native database both exited cleanly with status zero. The memory
guard completed successfully. This compatibility-example server used the system
allocator with an explicit MALLOC_ARENA_MAX=2 setting, retained in the record.
It does not establish production CLI allocator behavior or general workload
memory bounds. The accepted running application deployment was not upgraded.

This closes the selected 89-test batch, including the earlier process-instance
conflicts and slow change-log commit path. It does not qualify the whole
application. Pristine source-value equality, complete pristine schema assembly,
remaining original tests and UI/business workflows, audit diagnostics, full
packaging, performance and recovery/cutover remain separate gates. The baseline
had already undergone legitimate business activity; this result does not assert
that its row values equal the original saved Oracle export.
