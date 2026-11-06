# Original pristine foundation: 89/89 passed

All 89 unchanged selected original iDempiere foundation tests passed on a fresh
clone of the qualified Oracle-source, relation/routine-mapped baseline. No errors,
failures or skips were reported. The application used its Oracle SQL route over
the PostgreSQL driver and preview adapter, not its PostgreSQL schema/backend.
The engine source was `a4d766d3f7fd92e15ff22beb214b285a70f76d27`.

| Original class | Passed |
| --- | ---: |
| `org.idempiere.test.base.InOutTest` | 5 |
| `org.idempiere.test.base.InvoiceTest` | 2 |
| `org.idempiere.test.base.POInsertSQLTest` | 20 |
| `org.idempiere.test.base.POTest` | 53 |
| `org.idempiere.test.model.InvoiceCustomerTest` | 9 |

Original source/class hashes and the unmodified test JAR were verified. Runtime
class origins matched for all five test classes, the prepared Oracle provider,
the preview JDBC adapter and PostgreSQL JDBC. Both the test JVM and native
database exited zero. Before the JVM started, all 16 source BLOB columns were
reconciled while retaining stored definitions and complete table fingerprints.
The test network namespace contained loopback only.

After qualification, only the closed disposable test database was removed:
589,471,744 allocated bytes reclaimed. Original logs, XML reports, provenance and
the pristine baseline remain retained. [summary.json](summary.json) contains
sanitized counts, hashes, origin results and resource observations.

This is one foundation batch, not complete application acceptance. The next
64-test business batch and remaining original classes are separate gates.
A background monitoring query still fails on `DBA_FREE_SPACE`; the
[storage catalog contract](../../oracle-storage-catalog-contract.md) tracks that
unimplemented interface. Passing these assertions does not erase that gap or
qualify all background jobs, UI/reporting workflows, security, recovery,
production operation, or Oracle conformance.

The [routine-mapped pristine baseline](../idempiere-pristine-routine-owner-baseline-2026-11-06/README.md)
provides the preceding ownership/data checks. The earlier
[relation-only 89-test run](../idempiere-pristine-owner-baseline-2026-11-06/README.md)
remains recorded as unsuccessful. The new result uses a fresh disposable clone;
no old failed report or assertion was rewritten.
