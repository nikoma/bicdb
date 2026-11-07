# Original iDempiere client setup on the Oracle-schema target

The unchanged upstream `InitialClientSetupTest` completes through the PostgreSQL
JDBC transport on a disposable copy of the qualified original Oracle-schema
paged baseline: **one test, zero failures, zero errors and zero skips**. The
original JVM and database both exit zero. Runtime class origins and original
test source/bytecode hashes match the pinned upstream and adapter artifacts.

The run completes 384 period-control INSERT statement events, all nine residual
print-format queries, and 736 print-item INSERT statement events without a
database-writer timeout. Individual event counts are diagnostic observations;
the original JUnit assertions provide this test's application acceptance.

See [the evidence summary](summary.json) for exact source, binary, driver and
report identities. [The extracted JUnit result](junit-result.xml) retains suite
and testcase result attributes, excluding environment properties and logs. The
summary also hashes the retained original XML. Extraction changes no test source,
bytecode, assertion, report outcome or application seed. Private runtime resources
are isolated copies; upstream files remain unchanged.

The backend is the recorded residual-EXISTS implementation revision, already
merged into main, rather than a claim that this application test exercised every
later main commit. The earlier [query-progress checkpoint](../sql-correlated-exists-residuals-2026-11-06/README.md)
records interrupted attempts separately. This run ends naturally and passes.
After class-origin, count, timeout and shutdown checks, only its closed disposable
database is removed; reports, logs and pristine baselines are retained.

This accepts one original client-setup test on the
[qualified paged schema baseline](../idempiere-paged-schema-qualification-2026-11-06/README.md).
It does not qualify the complete original test inventory, unresolved business
workflows, the web application, production deployment or the full Oracle
compatibility programme. Subsequent original test batches remain separate gates.
