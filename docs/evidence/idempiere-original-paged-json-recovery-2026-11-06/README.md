# Original JSON application test on the paged baseline

The unchanged original iDempiere `JsonFieldTest.testSavingJSONValue` passes its
one invocation on a fresh copy of the qualified paged Oracle-schema baseline.
The test rejects invalid JSON, then saves valid objects, arrays and NULL values,
and verifies generated migration-script files. Its earlier transaction-recovery
failure is resolved by general Oracle savepoint behavior across active and idle
boundaries; no application-specific engine rule or test assertion was added.

The [provenance record](summary.json) identifies backend source `20bfbc5`,
adapter source `a46572f`, artifact hashes, original source/class/JAR hashes, result
XML hash and four verified runtime class origins. The explicit PostgreSQL JDBC
transport adapter and versioned provider are recorded separately. Original test
source matches upstream `da0730a`; original class bytes and assertions match the
pinned test JAR. Private runtime resources come from the pinned source checkout.

The JVM and database exited zero and were confirmed stopped. The closed passed
disposable database was removed, reclaiming 1,030,189,056 allocated bytes. Logs,
XML, source/class/runtime-origin evidence and earlier failed results remain; the
protected original and qualified paged baseline are preserved.

This is a retest, excluded from distinct invocation totals. It establishes this
selected class on the recorded revisions and storage mode. Client setup, retained
business failures, remaining original classes, UI workflows and broader Oracle
programme acceptance remain open. There is no full application or production
qualification.
