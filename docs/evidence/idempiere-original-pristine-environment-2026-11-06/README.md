# Original environment and integration tests — 2026-11-06

The next 90 unchanged original iDempiere invocations report **87 passes, two
failures and one error** on a fresh clone of the original Oracle schema over
PostgreSQL JDBC. All 70 `EnvTest` tests, payment export, BOM functions, nine
chart tests and six key/name tests passed. This is a failed diagnostic batch,
not application acceptance.

The [provenance record](summary.json) identifies exact backend and adapter
sources and artifact hashes, all original source/class hashes, eight original
XML reports and eleven verified runtime class origins. The test JVM exited 255,
the database exited zero, and both processes were confirmed stopped. The
failed database, logs and original assertions remain preserved.

Three issues require separate treatment:

- `GenericZoomProviderTest` encounters a parser error at explicit `LIKE ...
  ESCAPE` syntax. A minimal disposable control reproduces the Oracle lowering
  failure while PostgreSQL accepts the statement.
- `InitialClientSetupTest` cannot find the original accounting CSV at its
  expected application-home path. The file exists in the upstream source tree;
  this is a runtime packaging prerequisite, not evidence of an engine failure.
- `JsonFieldTest` rejects invalid JSON as intended, but application error
  handling then rolls back the whole transaction and later attempts to use a
  removed savepoint. The next valid save fails. A minimal constraint-error /
  rollback-to-savepoint control passes in both profiles; further application
  tracing and source comparison are required before attributing this failure
  to the transaction engine.

Across the original batches reported so far, 492 distinct invocations have
results on multiple backend revisions. The separate latest three-test
attachment retest passes, but is not added to that distinct denominator. These
numbers are coverage checkpoints, not an Oracle compatibility percentage. The
vendor-invoice ordering failure, remaining 972 planned original invocations,
paged deployment, complete UI/workflow acceptance and full Oracle programme
remain open.
