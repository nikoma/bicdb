# Next original application batch: 60 passes, one error

All 61 selected original invocations reported, with **60 passes, one error, no
assertion failures and no skips**. This independent diagnostic used a fresh
clone of the qualified Oracle-source, relation/routine-mapped baseline and
engine source `675973280789ef2b414c935d26054178c04a88b5`. The application used
its Oracle SQL route over the PostgreSQL driver and preview adapter.

| Original class | Tests | Passes | Errors |
| --- | ---: | ---: | ---: |
| `GridTabVOTest` | 1 | 1 | 0 |
| `GridTableTest` | 1 | 1 | 0 |
| `GridWindowTest` | 12 | 12 | 0 |
| `AcessSQLParserTest` | 15 | 15 | 0 |
| `AmtInWordsTest` | 2 | 2 | 0 |
| `AttachmentProviderTest` | 3 | 2 | 1 |
| `CLoggerTest` | 2 | 2 | 0 |
| `Convert_PostgreSQLTest` | 25 | 25 | 0 |

`AttachmentProviderTest.testDBLargeObjectProvider` reports `DeleteError` during
cleanup. Earlier in that same test, the BLOB-save path reports SQLSTATE `0A000`:
`row locking currently requires a primary key`. This establishes a missing
general locking capability for base tables without declared primary keys;
it does not establish that fixing locking will resolve every attachment error.
The failed disposable database is preserved for diagnosis.

Original source/class hashes and the unmodified test JAR were verified. Runtime
origins matched all eight test classes, the prepared Oracle provider, the
preview JDBC adapter and PostgreSQL JDBC. XML testcase counts matched the plan.
The JVM exited 255, the database exited zero, and both processes stopped.

Across this and the earlier separately versioned batches, **314 invocations
have reported: 312 passes, one vendor-invoice assertion failure and one
attachment error**. Another 1,150 of the 1,464 planned invocations remain
unverified. The previous vendor-invoice failure is retained; this diagnostic
does not waive application acceptance gates. The background storage-monitoring
gate, complete UI/workflows, security, recovery and the broader Oracle programme
also remain open.

[summary.json](summary.json) contains sanitized counts, hashes and provenance.
This result does not qualify complete application or production behavior.
