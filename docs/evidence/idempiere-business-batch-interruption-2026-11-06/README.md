# Broader original application batch: interrupted run

An unchanged upstream five-class business batch planned 64 tests on a separate
copy of the exercised full-FK schema. Four classes finished: **35 passed and
7 failed out of 42 reported tests**. Payment tests passed 4/4; bank statements
passed 3/3; allocations passed 18/19; purchase orders passed 10/16. The remaining
22 sales-order tests are unreported, not qualified as passed or skipped.

The host memory guard interrupted its owned process group when available RAM
crossed the configured 14 GiB floor. Both test JVM and database PIDs are absent;
clean process exit is not established. [Validation](validation.json) records
actual resource observations and original class/driver provenance. The accepted
running deployment and preserved source baselines were not used for mutations.
This batch is distinct from the earlier completed
[89-test foundation batch](../idempiere-full-fk-foundation-2026-11-05/README.md).

The six purchase-order failures follow an earlier PostgreSQL-driver
`RELEASE SAVEPOINT` being rejected by Oracle identifier folding (SQLSTATE 42601),
then an aborted transaction (25P02) and failed workflow updates. A focused native
protocol reproduction fails at the same command in this Oracle candidate and
passes in its default PostgreSQL profile. Explicit driver-command support and
regression checks are under validation; this run does not qualify that fix.

The allocation failure compares a vendor's open balance before and after an
invoice. Read-only Oracle controls show the original seed's stored and
recomputed balance agree and its price-list/accounting currencies agree. Those
controls do not establish the target failure's cause. Original assertions remain
unchanged; isolation and a general semantic reproduction are still required.
