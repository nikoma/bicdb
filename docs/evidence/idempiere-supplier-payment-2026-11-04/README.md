# Supplier payment and allocation

The original Oracle-schema application completed and posted offline check payment
**TEST-AP-800003** against [supplier invoice TF-800003](../idempiere-supplier-invoice-2026-11-04/README.md)
through its standard Payment and Receipt window. It used AP Payment, vendor 114,
organization 11, bank account 100 and invoice 1000001. Online Access was disabled.
The application calculated payment 181.54 and discount 3.71 under the invoice's
2%10 Net 30 payment term; no amount was repaired through SQL.

The [saved draft](saved-payment.png) records the selected document and amounts.
The independent [read-only observations](workflow-observations.json) verify the
subsequent completion (process 149 / instance 1000032, Result 1, Completed):

- Payment 1000001 is Completed, Processed, Posted and Allocated, with Processing,
  IsReceipt and IsOnline N. Its amount 181.54 and discount 3.71 settle 185.25;
  write-off and over/under amounts are zero.
- Supplier invoice 1000001 is Paid and Posted and references that payment.
  SendEMail remains N.
- Exactly one allocation line links that invoice and payment. Allocation header
  1000001, document 1000003, is Completed, Processed and Posted. The AP allocation
  amounts are -181.54 and -3.71, with zero write-off and over/under amounts.
- InvoiceOpen and PaymentAvailable both return zero.
- Payment accounting has two rows per schema, balanced at 181.54 and 154.31.
  Allocation accounting has three rows per schema, balanced at 185.25 and 157.46.
  Source totals balance at 181.54 for payment and 185.25 for allocation.
- Eleven preservation checks retain stock 21, the unique +3 receipt and -2 sales
  movements, the purchase order's received/invoiced quantities, prior purchase
  receipt and invoice accounting, all 22 earlier sales-side accounting entries,
  the paid sales invoice, reconciled customer receipt and bank balance 269.03.

The source is `c4da7cbde37770dc327cd1525329b1b6d91728b4`, merged on main at
`13fa542`; binary SHA-256 is
`64a3be06c438851f3a59a55c396aedcfea812699299dc38e5686a2c5b1c26366`.
No engine change was needed for this selected workflow.

This checkpoint precedes supplier-payment bank reconciliation: IsReconciled is N
and the bank balance is still 269.03. It is not full application acceptance,
production qualification or completion of the Oracle compatibility programme.
