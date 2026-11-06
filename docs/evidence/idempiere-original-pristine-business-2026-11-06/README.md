# Original pristine business test checkpoint

**Result: 63 passed, 1 failed, no errors or skips. Full application acceptance
remains open.** The earlier independent foundation batch passed 89/89 tests.

The unchanged original iDempiere 13 test bundle ran over the PostgreSQL driver
against the Oracle-preview profile on a fresh clone of the
[routine-mapped pristine baseline](../idempiere-pristine-routine-owner-baseline-2026-11-06/README.md).
The development server was built from
`a4d766d3f7fd92e15ff22beb214b285a70f76d27`.

| Original class | Passed | Failed | Errors | Skipped |
| --- | ---: | ---: | ---: | ---: |
| AllocationTest | 18 | 1 | 0 | 0 |
| BankStatementTest | 3 | 0 | 0 | 0 |
| PaymentTest | 4 | 0 | 0 | 0 |
| PurchaseOrderTest | 16 | 0 | 0 | 0 |
| SalesOrderTest | 22 | 0 | 0 | 0 |
| Total | 63 | 1 | 0 | 0 |

The failure is `AllocationTest.testAllocateVendorInvoice`, at the balance
assertion on original source line 361. Target traces show a currency conversion
after the application selects a price list. A read-only target control finds
multiple eligible price lists with equal values for the query's sole ordering
key. A subsequent read-only Oracle 23.26.3 control chooses the dollar list first
for the exact application query; the target chooses the euro list first. On
Oracle, removing the ordering clause reverses those tied records. An explicit
identifier tie-break selects the dollar list on both systems. These observations
support an ordering-portability dependency; they do not establish an arithmetic
defect or complete the application fix. No source data was modified, and the
Oracle container was returned to its original stopped state. No seed flags,
assertions or application bytecode were changed to obtain a passing result.

All five expected classes and their exact invocation counts reported. Original
source and class hashes, the complete test JAR hash, and eight runtime class
origins were independently checked, including the Oracle provider, BicDB adapter
and PostgreSQL driver. The test JVM exited 255 because the batch failed; the
database shut down cleanly with exit zero. The failed database and diagnostic
reports are retained privately. Public identifiers and hashes are in
[summary.json](summary.json).

This result does not qualify the remaining planned application batches,
background storage monitoring, full UI/workflows, source security, recovery or
production operations. A completed report inventory is not a successful test
batch, and the failing original result remains part of acceptance.
