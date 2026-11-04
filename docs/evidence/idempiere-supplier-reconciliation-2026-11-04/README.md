# Supplier payment bank reconciliation

The original Oracle-schema application reconciled offline AP payment
**TEST-AP-800003**, completed and posted bank statement **1000001**, named
**BicDB supplier settlement**, through its standard Bank/Cash Statement window.
This follows the [supplier payment/allocation](../idempiere-supplier-payment-2026-11-04/README.md)
and [supplier invoice](../idempiere-supplier-invoice-2026-11-04/README.md) checkpoints.

The [payment selection](payment-selection.png) selected exactly one eligible
payment for -181.54. Create lines from was confirmed once, followed by Complete
once (process 147 / instance 1000033, Result 1, Completed). All business mutations
were made through the application; verification connections only read data.

The [completed statement](completed-statement.png) and
[independent observations](workflow-observations.json) verify:

- Statement 1000001 is Completed, Processed and Posted, with Processing N,
  organization 11 and bank account 100. Beginning balance 269.03 plus statement
  difference -181.54 equals ending balance 87.49.
- Exactly one statement line references payment 1000001, invoice 1000001 and
  vendor 114. Statement/transaction amounts are -181.54, charge and interest zero.
- The payment remains Posted and Allocated, is now Reconciled, and retains
  IsReceipt N and IsOnline N. The bank's current balance is 87.49.
- Statement accounting has two rows per schema, balanced at 181.54 and 154.31;
  source totals balance at 181.54 in each schema.
- Nineteen preservation checks retain the paid supplier and sales invoices,
  both allocations and their accounting, purchase invoice/receipt matching,
  prior invoice/receipt/payment accounting, stock 21, unique receipt/shipment
  movements and the original purchase quantities. All prior statement records
  are unchanged, including the earlier customer-receipt statement.

The source is `c4da7cbde37770dc327cd1525329b1b6d91728b4`, on main at `13fa542`,
with binary SHA-256
`64a3be06c438851f3a59a55c396aedcfea812699299dc38e5686a2c5b1c26366`.
No engine change was required for this selected workflow.

## Repeat the fixture state checks

The [workflow verifier](../../../examples/oracle/idempiere/verify_workflow.py)
re-executes the six recorded observations and nineteen preservation queries in
a read-only transaction through PostgreSQL drivers. Supply `BICDB_DSN` privately
for the same qualified fixture and its Oracle-preview connection profile:

```sh
PSYCOPG_IMPL=python python3 examples/oracle/idempiere/verify_workflow.py \
  --expected docs/evidence/idempiere-supplier-reconciliation-2026-11-04/workflow-observations.json \
  --report /tmp/supplier-settlement-checks.json
```

The [live repeat](repeatable-checks.json) passed 25/25 checks. A deliberately
wrong expected bank balance returned failure; a multiple-statement observation
was rejected before connecting. Recorded queries must omit semicolons.

These checks use the recorded fixture IDs and expect this checkpoint's exact
state. They do not create the workflow, migrate data, prove UI behavior by
themselves, or qualify arbitrary customer data. Numeric values compare exactly
without treating decimal display scale as a different value. A mismatch or query
error returns failure; the report never claims full application qualification.

The selected purchase-to-payment-to-reconciliation path now completes locally.
Other ERP modules/rules, reversals and recovery, production operations and the
full Oracle compatibility programme remain open.
