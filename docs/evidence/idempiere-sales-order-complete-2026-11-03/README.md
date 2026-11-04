# First committed Oracle-schema sales-order completion

The original iDempiere 13 application completed the selected public-sample sales
order through its browser UI on BicDB's Oracle preview profile, using the
PostgreSQL driver. All 62 original Oracle routine declarations were reloaded
without application-specific runtime exceptions. The checked binary contains
the [statement-clock correction](../oracle-current-time-2026-11-03/README.md).

An [independent connection](independent-read.json) observes order 50000 as
`DocStatus=CO`, `DocAction=CL`, `Processed=Y`, `Processing=N`. Its quantity-two
line reserves two units, with no delivered or invoiced quantity. TotalLines and
GrandTotal remain 123.50. These writes came from the UI Complete action, without
SQL repairs to status, quantities or amounts.

The [side-effect read](side-effects.json) observes the sales reservation as two,
the order tax base as 123.50 and tax as zero, and the new workflow process
1000002 plus all three activities as completed/processed. Earlier terminated
workflow records remain in the report as history. Standard Order now uses its
actual document type, 132. No shipment or invoice exists for this order yet.
The sample's SendEMail flag remains N.

The [original inventory functions](inventory-functions.json) return available
18, on hand 20, reserved 2 and ordered 0. The changed availability/reservation
correctly differs from the unchanged Oracle seed data; it is not counted as a
new seed-value parity comparison.

A [fresh browser session](fresh-reload.json) reads the document status input as
Completed and both totals as 123.50. The [post-action screenshot](completed.png)
shows the completion result. An initial login observation timed out, and an
earlier reload assertion searched body text rather than the status input value.
The corrected check reads the named status field directly; these observations
are recorded in the [validation record](validation.json).

After an intentional graceful shutdown, a standalone server reopened a copy of
the stopped database and [retained the completed order, reservation and inventory
values](graceful-reopen.json). The original stopped database is preserved. This
checks graceful reopen, not crash recovery, failover or continuous availability.

## Remaining application work

This is one completed Standard Order workflow, not the entire application.
Shipment generation, invoicing, payments, returns, purchasing, accounting,
nonzero/inclusive taxes, authorization, reports, concurrency, background work,
performance and operational/recovery qualification remain open.

Background matching/accounting logs contain ResultSet type-decoding errors.
A stopped-database clone also shows unqualified `SELECT *` join projections
putting the right relation's columns first, while a qualified wildcard and
explicit columns retain the requested relation. This is an engine investigation,
not a reason to alter the application's SQL or model classes.

The database process grew to approximately 10 GiB RSS during background work.
The isolated application was gracefully stopped after the completed-order and
fresh-reload checks, before the configured host memory floor was reached.
Available host RAM returned to approximately 22 GiB. Memory growth and background
decoding remain qualification gaps; the application is not left running or
described as production ready. The full Oracle migration programme stays active.
