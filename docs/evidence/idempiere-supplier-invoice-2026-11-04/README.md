# Supplier invoice, receipt matching and posting

The original Oracle-schema application, running on BicDB through PostgreSQL
drivers, created supplier invoice **TF-800003** for Tree Farm Inc. through the
standard Purchase Invoice and Credit/Debit Note window. The existing completed
[purchase order and receipt](../idempiere-material-receipt-2026-11-04/README.md)
provided the source documents.

The [receipt selection](receipt-selection.png) selected exactly one line from
receipt **10000511**, quantity three. The standard Create lines from Invoice
process ran once (process 200143 / instance 1000030, Result 1, Created = 1).
Completion ran once (process 111 / instance 1000031, Result 1, Completed).
No business values were inserted or repaired through SQL.

The [completed invoice](completed-invoice.png) and independent
[read-only observations](workflow-observations.json) verify:

- Invoice 1000001 is Completed, Processed and Posted; Processing and SendEMail
  are N. It is a purchase invoice for vendor 114, organization 11, order 1000001,
  payment term 106, with total lines and grand total 185.25.
- Exactly one invoice line links purchase order line 1000001 and receipt line
  1000001, product 123, quantity three, unit price 61.75 and net amount 185.25.
- The purchase order now has three ordered, received and invoiced, with zero
  remaining reserved quantity.
- The invoice/receipt match and purchase-order match both reference the intended
  invoice line, quantity three, and are Processed and Posted.
- Four invoice ledger entries balance in two schemas at 185.25 and 157.46.
  Source debit and credit totals are 185.25 in each schema.
- Available/on-hand stock remains 21, reserved/incoming quantities remain zero,
  and the receipt still has exactly one +3 movement. Invoicing did not repeat it.
- The earlier receipt ledger, 22 sales-side ledger entries, paid sales invoice,
  allocated/reconciled customer receipt and bank balance 269.03 are preserved.

The live binary uses source `c4da7cbde37770dc327cd1525329b1b6d91728b4`, merged
on main at `13fa542`, SHA-256
`64a3be06c438851f3a59a55c396aedcfea812699299dc38e5686a2c5b1c26366`.
No additional engine change was needed for this workflow.

This is a local supplier-invoice checkpoint, not complete application acceptance
or production qualification. The supplier invoice is unpaid here. Outbound
payment, its reconciliation, other ERP rules/modules and the broader Oracle
compatibility programme remain open.
