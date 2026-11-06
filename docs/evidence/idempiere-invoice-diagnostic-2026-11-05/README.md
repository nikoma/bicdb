# Original invoice-class diagnostic on the current engine candidate

The [validation record](validation-results.json) reports the unmodified upstream
`org.idempiere.test.base.InvoiceTest`: **2 tests passed, no failures, errors or
skips**. Test-bundle and JUnit digests are recorded. The native candidate was
built from merged source b984572. The previous invoice attempt had not reached
a completed result, so no particular engine fix is credited for this outcome.

The discount case checks a two-percent discount on product plus freight, then
excludes freight and checks the product-only discount. The credit case checks
that preparation fails under credit stop, then changes to no credit check and
requires successful document completion with completed status. These are
original application assertions, not a substitute synthetic SQL model.

The JVM class-load trace confirms the staged `DB_Oracle` provider and the
PostgreSQL-backed Oracle adapter. Original Oracle schema/routines/views are used
on a disposable BicDB copy, with public BLOB reconciliation before JVM startup.
Both JVM and database exited cleanly. The accepted application database was not
used for mutations or upgraded; networking was isolated to loopback.

The first private attempt stopped before JVM startup because its batch path did
not use the BLOB harness's required namespace. The guard was retained and a
correctly named private batch was used. After clean shutdown, the failed data
copy was removed, reclaiming 564,254,951 bytes. That harness failure is not an
application test result.

## Remaining gates

This copy predates complete source foreign-key installation. Repeat the class
on the completed FK schema before final application acceptance. Broader invoice
posting/reversal and the entire application remain separate requirements.

Workflow audit `Update return 0` diagnostics require investigation/reference
comparison; green business assertions do not certify audit or rollback behavior.
The runtime also logs an unselected original provider singleton warning. Actual
selected provider classes were attested; clean packaging remains an operational
gate. Neither warning was suppressed. No GitHub workflows were changed or
manually dispatched.
