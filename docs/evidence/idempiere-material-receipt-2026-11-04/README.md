# Purchase material receipt and posting

The original Oracle-schema application completed and posted material receipt
**10000511** against purchase order **800003** through the standard Material
Receipt window. The header was created once before the engine upgrade and
reopened afterward. The preceding
[set-operand resolver fix](../set-operand-builtin-types-2026-11-04/README.md)
removed the original line-selection view's unlowered NVL failure through a
general SQL rule; no application/table-specific engine branch was introduced.

The original selection screen showed one eligible line: three Oak Trees,
Default HQ Locator and order 800003. Only that row was selected. Its standard
Create lines from Shipment/Receipt action ran without another parameter dialog
and created one line (process 200141 / instance 1000028, Result 1, Created = 1).
Completion was then selected and confirmed once (process 109 / instance 1000029,
Result 1, Completed).

The [completed receipt](completed-receipt.png) and
[read-only checks](workflow-observations.json) verify:

- Receipt ID 1000001 is Completed, Processed and Posted, with Processing and
  SendEMail N. Its line references order line 1000001, product 123 and locator
  101, with entered/movement quantity three.
- Exactly one +3 stock movement exists for that line. Available/on-hand stock
  increased from 18 to 21, incoming quantity fell from three to zero, and
  reserved stock remains zero.
- The purchase order line is three ordered, three received and zero invoiced,
  with zero remaining on-order quantity.
- A processed/posted purchase-order match records quantity three and the intended
  receipt/order lines; the invoice-line reference remains null at this stage.
- Four receipt accounting entries balance in two schemas at 185.25 and 157.46.
  Source debit/credit totals are 185.25 in each schema.
- The original sales invoice remains Paid and Posted. Its receipt remains
  Allocated, Reconciled and Posted; the bank balance remains 269.03. All 22
  existing sales/shipment/invoice/payment/allocation/bank accounting entries
  retain their previous totals, and the original -2 shipment movement remains
  unique.

The binary uses source `c4da7cbde37770dc327cd1525329b1b6d91728b4`, on main at
`13fa542`, SHA-256
`64a3be06c438851f3a59a55c396aedcfea812699299dc38e5686a2c5b1c26366`.
No business line, status, quantity, balance or accounting amount was repaired
through SQL. This is local purchase-order/receipt qualification, not a published
release, production qualification or complete application acceptance. Supplier
invoicing, outbound payment, other ERP rules/modules and the full Oracle
programme remain open.
