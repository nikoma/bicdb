# Original database and document tests: 88/88 passed

All 88 unchanged selected original iDempiere tests passed on a fresh clone of
the qualified Oracle-source baseline, using backend source
`a97545fc25234c89a4361e0dbfe8c98a02cb3e94` and the earlier JDBC adapter recorded
in [summary.json](summary.json). There were no errors, failures or skips.

| Original class | Passed |
| --- | ---: |
| `org.idempiere.test.base.DBTest` | 55 |
| `org.idempiere.test.base.DocumentSearchTest` | 2 |
| `org.idempiere.test.base.DocumentStatusTest` | 1 |
| `org.idempiere.test.base.DynamicServiceLocatorTest` | 6 |
| `org.idempiere.test.base.EmailFormatValidatorTest` | 24 |

Original source/class hashes, the unchanged test JAR and eight runtime origins
were verified. The JVM and database exited zero. The passed, closed disposable
database was removed, reclaiming 573,747,200 allocated bytes; logs, XML,
provenance and the pristine baseline remain.

Across the separately versioned, distinct original invocations, 402 have now
reported: 400 passes, one vendor-invoice assertion failure and one attachment
error. Another 1,062 planned invocations remain unverified. The separate
three-invocation [attachment retest](../idempiere-original-attachment-heap-2026-11-06/README.md)
is excluded from those distinct totals. These results do not qualify all tests
on one final revision, complete application/UI behavior, recovery or production.
