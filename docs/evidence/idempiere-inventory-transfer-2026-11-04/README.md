# Inventory transfer, stock conservation and posting

The original Oracle-schema application completed and posted a two-unit transfer
of Oak Trees from **HQ Warehouse** to **HQ Transit** through the standard Inventory
Move window on the [upgraded runtime](../prepared-request-boundaries-2026-11-04/README.md#subsequent-application-upgrade-and-clean-restart).
The header and exactly one line were entered once through the UI. Locator fields
were selected using their normal locator dialogs, with Create New Record off.
Complete was confirmed once (process 122 / instance 1000034, Result 1, Completed).
No business quantity, document status, balance or accounting amount was repaired
through SQL.

The [completed transfer](completed-transfer.png) and independent
[read-only observations](workflow-observations.json) verify:

- Movement 1000000, document 10000000, is Completed, Processed and Posted;
  Processing and IsInTransit are N. Its organization is 11, document type 143,
  warehouse 103 and destination warehouse 50000.
- Its single line 1000000 references product 123, source locator 101 and target
  locator 50000, attribute-set instance zero and quantity two.
- Exactly two stock transactions reference that line: M- quantity -2 at source
  and M+ quantity +2 at destination. Their net quantity is zero.
- Source stock changes from 21 to 19, destination from zero to two. The existing
  store stock remains four; total product stock remains 25. No repeated shipment
  or receipt stock transaction was created.
- Accounting has two rows in each schema, balanced at 106.51 and 90.53, with
  source debit/credit totals equal to the respective accounting totals.
- All 27 settled sales/purchasing checks pass with the one intended change to
  the HQ stock expectation. Both invoices remain paid, both payments reconciled,
  allocations/matching and all earlier accounting retain their values, the bank
  remains 87.49 and order received/invoiced quantities are unchanged.
- Three additional checks preserve the original seed movement 100, its lines
  and accounting.

The [read-only Oracle seed comparison](source-numbering.json) explains the display
number: the seed already contains an older document 10000000 and its sequence
also starts at 10000000. BicDB consumed that next value exactly once and advanced
the counter to 10000001. Verification identifies the new document by movement ID
1000000 and separately preserves seed movement 100. This comparison inspects the
original source seed and counter; it does not execute the transfer on Oracle.

The binary uses source `7fccde8d2a9d21a78bf372092c5e213a14ebcfeb`, merged on main
at `c761bf7`, SHA-256
`553af9735efdd884a90b74f4cbab1182ea71fef6418913e0ff9d5c2dd13b4502`.
No engine change was required for this selected workflow.

The [workflow verifier](../../../examples/oracle/idempiere/verify_workflow.py) can
repeat this checkpoint's six transfer checks and thirty preservation queries:

```sh
PSYCOPG_IMPL=python python3 examples/oracle/idempiere/verify_workflow.py \
  --expected docs/evidence/idempiere-inventory-transfer-2026-11-04/workflow-observations.json \
  --report /tmp/inventory-transfer-checks.json
```

The [live replay](repeatable-checks.json) passes 36/36 checks.

Supply `BICDB_DSN` privately for the matching Oracle-preview fixture. Older
checkpoints expecting HQ stock 21 describe their historical state; this completed
transfer now expects HQ 19 and HQ Transit two. These fixture checks do not
qualify arbitrary customer data, reversals, multi-organization/in-transit rules,
other modules, production operation or the full Oracle compatibility programme.
