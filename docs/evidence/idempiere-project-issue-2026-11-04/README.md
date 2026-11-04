# Project inventory issue and reversal

The original Oracle-schema iDempiere 13 fixture creates a work-order project,
issues one stocked product, completes and posts the issue, then reverses it
through the standard application UI. The application uses its PostgreSQL
driver against BicDB's `oracle-preview-v1` profile. No financial SQL repairs or
original object recreation were used.

The [qualification](qualification.json) records the exact source and runtime
binary. The [workflow verifier](../../../examples/oracle/idempiere/verify_workflow.py)
replays [77 selected observations](workflow-observations.json) in a read-only
transaction: all previous 67 controls plus ten project controls. The
[verification result](workflow-verification.json) records each outcome. These
are selected observations, not a percentage of application compatibility.

## Forward issue

The standard Set Project Type process assigns work-order type 103 to project
1000000. Issue to Project then creates issue 1000001 and project line 1000001
for one Oak Tree at HQ locator 101, dated November 4, 2026. The
[forward state](forward.json) records Completed and Posted status, stock
21 → 20 at HQ and project balance 53.2525. Accounting debits configured project
WIP account 568 and credits inventory account 742: 53.25 USD and 45.26 EUR.
The expected amounts were derived from the [pre-issue costs](before.json) and
configured account combinations before checking the posted rows.

An earlier attempt encountered an unsupported row-valued UPDATE in project
header aggregation. It rolled back without an issue, project line, movement or
ledger entry. The failed process records remain visible. The successful retry
uses the general [row-valued UPDATE implementation](../sql-row-update-2026-11-04/README.md),
which has separate Oracle and PostgreSQL regression references.

## Reverse Correct

The standard document-action dialog creates reversal issue 1000002. Both issues
are Reversed and Posted and point to each other. The reversal has line 20 and
quantity -1; the original line 10 and quantity 1 remain. Work-order stock
movements retain type `W+` with opposite signs. HQ stock returns to 21 and the
project balance returns to zero. The original project line remains as upstream
application history, with planned and committed quantities of one.

The [reversal state](reversal.json) retains all eight ledger rows. Reverse Correct
negates amounts in the same debit/credit columns; it does not exchange columns.
Each account, currency, product, locator and project group nets to zero.

Stock totals return to their original values while material-policy layers change:
the older HQ layer has 17 units and the November 4 layer has four. All storage
keys remain distinct. This is not a claim that reversal restores original layers.

The [costing rules](costing-rules.json) independently compare currency costing
precision with the Oracle reference. Both currencies use four places. Upstream
`MCost.getCost` rounds the returned cost to that precision, and
`MCost.setWeightedAverage` combines it with the remaining 24 units. Consequently
the EUR average changes from 45.26462500 to 45.26462400:

```
(24 × 45.26462500 + 45.2646) / 25 = 45.26462400
```

The USD average remains 53.25250000. Cost quantity returns to 25 in both schemas.
These expectations follow the application source and independently checked
configuration; this checkpoint does not run the complete application against
Oracle as a second execution target.

## Preserved state and limits

All prior 67 workflow observations and 27 settled business queries still match.
The 62 original routine and 160 view fingerprints are unchanged. All 141 saved
Trial Balance rows retain their fingerprints; report PDFs were not regenerated.
The browser displays Reversed and Posted for the original issue.

Other project categories, receipt/expense sourcing, project billing, adverse
concurrent schedules, full costing-method coverage and application-wide
performance remain unqualified. UI metadata lookups are still slow. This
checkpoint establishes the recorded issue/reversal workflow and preservation
checks, not full Oracle compatibility or production readiness.
