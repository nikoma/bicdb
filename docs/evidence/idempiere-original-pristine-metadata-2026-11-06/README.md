# Original pristine accounting and grid tests: 100/100 passed

All 100 unchanged selected original iDempiere tests passed on a fresh clone of
the qualified Oracle-source, relation/routine-mapped baseline. There were no
errors, failures or skips. The application used the Oracle SQL route over the
PostgreSQL driver and preview adapter. Engine source:
`675973280789ef2b414c935d26054178c04a88b5`.

| Original class | Passed |
| --- | ---: |
| `org.idempiere.test.acct.AccountingSchemaValidRangeTest` | 2 |
| `org.idempiere.test.adwindow.GridFieldTest` | 26 |
| `org.idempiere.test.adwindow.GridFieldVOTest` | 5 |
| `org.idempiere.test.adwindow.GridTabTest` | 67 |

Original source/class hashes and the test JAR were verified. Runtime origins
matched all four test classes, the prepared Oracle provider, the preview JDBC
adapter and PostgreSQL JDBC. All 16 source BLOB columns were reconciled before
the JVM started, preserving stored definitions and complete table fingerprints.
The network namespace contained loopback only. Both the JVM and database exited
zero. After verification, the closed disposable database was removed; original
logs, XML reports, provenance and the protected pristine baseline remain.

Together with the earlier [foundation](../idempiere-original-pristine-foundation-2026-11-06/README.md)
and [business](../idempiere-original-pristine-business-2026-11-06/README.md)
batches, this is **252 passes and one failure across 253 reported invocations**.
The earlier batches used their separately recorded engine revision. This is
not a rerun of all 253 tests on this revision. The vendor-invoice failure remains
open; this independent diagnostic batch does not waive it. Another 1,211 of the
1,464 planned invocations remain unverified at this checkpoint. Complete UI,
background monitoring on a qualified paged baseline, security, recovery and the
broader Oracle programme remain separate gates.

[summary.json](summary.json) records sanitized counts, hashes, resource-guard
observations and cleanup evidence. This checkpoint does not qualify complete
application behavior or production operation.
