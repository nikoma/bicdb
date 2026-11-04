# Original Oracle-schema Trial Balance workflow

The standard Trial Balance UI now completes the selected HQ report on BicDB
through PostgreSQL drivers. It stores and renders all 47 expected rows: 19
opening balances and 28 current-period ledger entries. The 12-page PDF contains
all 19 opening-balance labels. The previous date-boundary omission is resolved
by the general [typed BETWEEN fix](../sql-typed-between-2026-11-04/README.md),
with no application source, routine-definition or schema-specific workaround.

The [qualification record](qualification.json) identifies the source/runtime
hashes, PDF hash/size, view metadata and preserved prior report. The new binary
reopened a copy made only after the parent database exited cleanly. No original
routines/views were recreated: all 62 routine and 160 view definition fingerprints match and
all 160 view labels/type families pass named Parse/Describe. Fresh login and
the 38 prior sales, purchasing, payment, reconciliation and inventory checks
still pass. This is clean local upgrade evidence, not crash/disaster recovery.

## Selected report and independent ledger expectations

The HQ request used schema 101, organization 11, November 1–30, 2026, actual posting,
no organization grouping, PDF, English (USA), and both Summary/Run as Job off.
The UI parameters were verified before its single explicit submission. Process
instance 1000039 is terminal (`N`, result 1, `Rows=47`).

| Ledger population | Rows | Debit | Credit |
| --- | ---: | ---: | ---: |
| HQ opening balances | 19 | 24,548.47 | 24,548.47 |
| HQ current-period entries | 28 | 1,910.35 | 1,910.35 |

Expected rows came from the source ledger before the intended submission,
independently of the reporting table. Every opening/detail row matches, all
47 generated identifiers have the expected UUID format and are unique, and
the PDF contains the previously omitted opening balances. The prior request's
47 persisted rows and the previous financial workflows remain intact.

An additional all-organization request (instance 1000038) was retained and
validated for its actual parameters. It also renders 47 expected rows, with
19 opening balances totaling 24,856.85 on each side and the same 28 current-period
entries. It is distinct from the explicitly verified HQ request; neither
historical request was repaired or overwritten.

## Read-only replay and limits

The [expected observations](workflow-observations.json) extend the previous
38 business checks with eight reporting checks. They include exact source-ledger
rows, request parameters, prior report preservation, identifier counts and the
fractional timestamp boundary that previously omitted the opening rows.
The [recorded replay](workflow-replay.json) passes 46/46.

```sh
PSYCOPG_IMPL=python python examples/oracle/idempiere/verify_workflow.py \
  --expected docs/evidence/idempiere-trial-balance-2026-11-04/workflow-observations.json \
  --report /tmp/trial-balance-replay.json
```

Supply `BICDB_DSN` for this exact fixture with the Oracle preview profile
selected. The verifier uses a read-only transaction and applies no repairs.
It does not provision arbitrary customer data or qualify another deployment.

This checkpoint does not qualify all reports/modules, other locales/currencies,
production throughput, backup/recovery, or the full Oracle programme. The
separate PostgreSQL volatile BETWEEN evaluation differences remain open and
visible in the operator reference fixture. Full application compatibility
remains unproven.
