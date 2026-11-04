# Inventory Reverse Correct, linked documents and accounting reversal

After the [posted inventory transfer](../idempiere-inventory-transfer-2026-11-04/README.md),
the original Oracle-schema application ran **Reverse - Correct** once through its
standard Document Action dialog. The [selected action](reverse-correct-selection.png)
was verified before confirmation (process 122 / instance 1000035, Result 1,
Completed). No status, stock, document link or accounting amount was repaired
through SQL.

The [reversed original](reversed-transfer.png) and
[read-only observations](workflow-observations.json) verify:

- Original movement 1000000, document 10000000, and reversal 1000001, document
  10000000^, are Reversed, Processed and Posted, with Processing N. Their
  Reversal_ID values point to each other. Both movement dates are 2026-11-04.
- There are exactly two lines: the original quantity two and reversal quantity
  minus two, both for product 123 and locators 101/50000.
- There are exactly four stock transactions across the two lines. The original
  removes two at HQ and adds two at HQ Transit; the reversal adds two at HQ and
  removes two at HQ Transit. No repeated transaction is present.
- Stock is restored to HQ 21 and HQ Transit zero; the existing store remains
  four. Total product stock remains 25.
- Original and reversal each have two accounting rows per schema, balanced at
  106.51 and 90.53. Across both documents, accounting amount, source amount and
  quantity net to zero in every accounting-schema/organization/account/product/
  locator group, including each source and destination locator.
- All 27 original settled-business checks pass without changing their expected
  values: both paid invoices, reconciled payments, allocations/matches, earlier
  accounting, bank balance 87.49, orders and stock are preserved/restored.
- Three checks preserve the pre-existing seed movement 100 and its lines and
  accounting. The 62 stored routine and 160 view definition fingerprints remain
  unchanged.

The same binary is used: source `7fccde8d2a9d21a78bf372092c5e213a14ebcfeb`,
merged on main at `c761bf7`, SHA-256
`553af9735efdd884a90b74f4cbab1182ea71fef6418913e0ff9d5c2dd13b4502`.
No new engine change was required for this selected reversal.

The [workflow verifier](../../../examples/oracle/idempiere/verify_workflow.py)
repeats eight reversal checks and thirty preservation queries against the matching
fixture. Supply `BICDB_DSN` privately for its Oracle-preview connection:

```sh
PSYCOPG_IMPL=python python3 examples/oracle/idempiere/verify_workflow.py \
  --expected docs/evidence/idempiere-inventory-reversal-2026-11-04/workflow-observations.json \
  --report /tmp/inventory-reversal-checks.json
```

The [live replay](repeatable-checks.json) passes 38/38 checks.

The forward-transfer checkpoint's HQ19/transit2 values are historical; this
subsequent reversal restores HQ21/transit0. This qualifies the selected local
Reverse Correct path, not Reverse Accrual, other document/module reversals,
concurrent/retried/failure-injected reversals, production operations or the full
application/Oracle programme.
