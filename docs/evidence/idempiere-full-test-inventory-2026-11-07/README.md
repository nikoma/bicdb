# Full original iDempiere test inventory on the compatibility schema profile

All 17 original iDempiere test batches ran against the release binary built from
merged main `f103a47`: **1,464 test invocations across 155 classes, 1,460 passed, 4 failed,
0 errors, 0 skipped**. No server aborted, and every selected class reported exactly
its expected test count. The `oracle-preview-v1` profile served the application
through the JDBC adapter.

This is the first run of the complete original inventory on one merged binary. It
does not show standards or vendor conformance, a qualified web application or
production deployment, or performance parity. The four failures below are open.

## What ran

| Item | Identity |
|---|---|
| Backend | release `oracle_server` from main `f103a4760dc3`, built on the benchmark host, sha256 `261448f7…8e3f3c` |
| Pre-run validation | CI green on the merged commit, release build, JDBC adapter probe against the binary |
| JDBC adapter | source `15f0634`, sha256 `c9b62554…9b9214` |
| Original test bundle | `org.idempiere.test` 13.0.0, sha256 `261afbb1…679e7`. Installed as a project directory, as the upstream build's test runner does. Test source, bytecode and assertions are unchanged |
| Schema baseline | Paged copy of the reference schema, plus the three upstream migrations it was missing (`202603261259_IDEMPIERE-6910`, `202604231822_IDEMPIERE-6910`, `202604282341_IDEMPIERE-6983`), applied without post-migration processes. No application seed was changed for any test |
| Isolation | Every batch ran on its own disposable copy of the baseline, with loopback networking only and private runtime resources. The copy was removed after its checks passed |
| Budgets | The original application timeouts, unchanged. No diagnostic tracing flag |

**Configuration note.** The server ran with `BICDB_SYNC_OUTBOX=off`, which disables the
change-data-capture outbox. That outbox has no consumer in this deployment. The
transaction log and the page WAL still sync before COMMIT returns, so durability is
unchanged.

## Results per batch

| Batch | Classes | Tests | Passed | Failed | Server aborts | Wall time (s) |
|---|---:|---:|---:|---:|---:|---:|
| foundation | 5 | 89 | 89 | 0 | 0 | 176 |
| business-day | 5 | 64 | 64 | 0 | 0 | 323 |
| remaining-01 | 4 | 100 | 100 | 0 | 0 | 168 |
| remaining-02 | 8 | 61 | 61 | 0 | 0 | 60 |
| remaining-03 | 5 | 88 | 88 | 0 | 0 | 60 |
| remaining-04 | 8 | 90 | 90 | 0 | 0 | 124 |
| remaining-05 | 5 | 89 | 89 | 0 | 0 | 258 |
| remaining-06 | 6 | 95 | 95 | 0 | 0 | 123 |
| remaining-07 | 11 | 100 | 100 | 0 | 0 | 450 |
| remaining-08 | 7 | 97 | 96 | 1 | 0 | 122 |
| remaining-09 | 6 | 84 | 83 | 1 | 0 | 675 |
| remaining-10 | 1 | 96 | 96 | 0 | 0 | 3566 |
| remaining-11 | 29 | 96 | 95 | 1 | 0 | 430 |
| remaining-12 | 22 | 98 | 98 | 0 | 0 | 141 |
| remaining-13 | 10 | 86 | 86 | 0 | 0 | 167 |
| remaining-14 | 16 | 94 | 93 | 1 | 0 | 378 |
| remaining-15 | 7 | 37 | 37 | 0 | 0 | 91 |
| **Total** | **155** | **1,464** | **1,460** | **4** | **0** | **7,312** |

## Remaining failures

| Test | Result | Classification | Cause |
|---|---|---|---|
| `ReportTest.testBackgroundJob` | failure after 11.5 s: "Error waiting for background job to complete" | BicDB performance limit | The test waits a fixed 10 s for a background job. The job's queries finish quickly. Measured in diagnostic rounds, about 4 s of database time on the job connection goes mostly to roughly 55 COMMITs at 24–50 ms each under concurrent load; the rest is application JVM work. |
| `AuditTraceContextTest.testBackgroundJob` | failure after 10.3 s, same message | BicDB performance limit | The same 10 s background-job wait. |
| `TrxTest.testReleaseSavepoint` | failure: "releaseSavepoint should throw SQLException" | application/profile difference | When iDempiere is configured for this database profile, `Trx.releaseSavepoint` returns before calling the driver, so no backend can raise the expected exception. Passing it would require changing upstream code. |
| `CalloutTest.testAnnotatedCallout` | failure: "CalloutInfoWindow not found for AD_InfoWindow.AD_Table_ID" | application harness race | The annotation scanner skips system bundles it has already read during concurrent startup, so the callout class is never registered. The test issues no database statement. |

## Performance limits

- **COMMIT latency** is the main BicDB limit in this workload. Each COMMIT waits for two
  serial durable flushes, the transaction log and the page WAL. Under the concurrent
  application load this costs about 24–50 ms per COMMIT. That accounts for both
  background-job timeouts. Shortening it means changing which log is the single
  synchronous point at COMMIT. That changes the recovery model, so it waits on a
  design decision and is not part of this run.
- **Long costing batch.** `BackDateAveragePOCostingTest` alone takes 3,545 s of the
  3,566 s in remaining-10, and all 96 of its tests pass. Classes this write-heavy take far
  longer than on the reference database. Total wall time for the inventory was 2 h 2 min.

## Files

- [summary.json](summary.json) records the binary, adapter, test-bundle and baseline
  identities, pre-run validation steps, totals, and every failure with its
  classification. For each batch it also has the per-class counts and the sha256 of
  each original JUnit report.
- [junit-results.xml](junit-results.xml) holds the suite and testcase attributes from
  all 155 original reports, with failure types and messages. Environment properties
  and logs are left out.
