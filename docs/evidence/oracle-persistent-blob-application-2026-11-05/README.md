# Persistent BLOB application and JDBC metadata checkpoint

The original iDempiere `org.idempiere.test.base.POTest` class completed with
53 tests, zero failures, zero errors and zero skips on an isolated BicDB clone
using the application's Oracle SQL path and PostgreSQL transport. Its
`testBinaryData` case passed. That source test checks nonempty data and length;
it does not assert byte-array equality. Exact binary round-trip and rollback
checks are separately covered by the 8,193-byte native/protocol fixtures in the
[persistent execution checkpoint](../oracle-persistent-blobs-2026-11-05/README.md).

Before application execution, all 16 BLOB columns identified by the source export
manifest were explicitly reconciled from the clone's legacy binary storage.
All 1,481 column-row observations, including NULLs, and 32,504 non-NULL bytes
were preserved. Hash comparisons associate values with source primary keys
where available; four tables without primary keys use payload multisets.
The original 62 routines and 160 views remained present with their definitions
unchanged across reconciliation. These counts cover this export, not every
possible application version or Oracle object type.

The clone uses a previously stopped, clean database copy. The application test
JVM has only loopback networking, uses the original upstream test bundle, and
runs through the staged PostgreSQL JDBC adapter/provider. Its mutations do not
affect the accepted application database. The test JVM and clone database both
exited successfully. No new binary was deployed to the accepted application.

The first complete run passed 52 of 53 tests. Its remaining error occurred when
stock PostgreSQL JDBC requested base-column metadata: Oracle identifier lowering
rewrote qualified PostgreSQL system catalog names. The fix preserves native
semantics for parsed, read-only queries whose relation references are exclusively
qualified PostgreSQL catalog/information-schema relations. Mixed application
queries, locking reads and writes do not take this path. Ordinary PostgreSQL
execution is checked independently.

A real JDBC probe then passed 124 checks: 41 each in simple, extended and cached
extended modes, including base schema/table/column metadata, and one rejection
check on a default PostgreSQL-profile server. PostgreSQL JDBC's standard `getSchemaName`
continues to return its usual empty string; the base schema is obtained through
its `PGResultSetMetaData` extension. This is PostgreSQL-driver interoperability,
not unchanged Oracle JDBC/OCI support.

Initial harness failures were corrected rather than counted as application
passes: the export lives in its explicit application schema, and stock JDBC's
standard schema-name method does not return the base schema. One compilation
attempt was stopped by the memory guard; the successful SQL unit-test build used
512 codegen units while retaining the same memory floor. Resource interruptions
are separate from test failures.

Full application acceptance remains open. Earlier broader application runs
include failures outside this class; in particular, the original transaction
suite has a mock-based release-savepoint assertion although the application's
Oracle implementation returns before calling the mocked connection. That test
needs an independently justified Oracle acceptance treatment, not an engine
special case or an unreported skip. Broader business flows, complete LOB APIs,
DDL lifecycle, recovery, security and performance remain separate gates in the
[programme](../../oracle-compatibility-programme.md). No production qualification,
Oracle certification or programme completion follows from this checkpoint.


The subsequent combined source commit recorded in `validation-results.json`
passed 690 SQL unit tests (three pre-existing ignored benchmarks), 48 Oracle
protocol tests, 180 PostgreSQL protocol tests and eight locking tests. Workspace
all-target checking and the example server build passed. The memory guard
completed without interrupting that batch. Its subsequent application rerun passed both original classes: `POTest` 53/53
and `POInsertSQLTest` 20/20, with zero failures, errors or skips. The earlier
Oracle insert test that failed at persistent BLOB append now passes. All 16
source BLOB columns were reconciled again, values and definitions were preserved,
and both owned processes exited successfully. The reports and tested binary
hashes distinguish this 73-test run from the preceding candidate.
