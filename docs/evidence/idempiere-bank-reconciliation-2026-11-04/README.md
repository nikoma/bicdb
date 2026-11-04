# Bank statement and receipt reconciliation

The original Oracle-schema application created and completed bank statement
**1000000**, named BicDB receipt settlement, through the standard **Bank/Cash
Statement** window. The original seed statements were left unchanged. The new
statement's beginning balance was calculated as 148.00 from its bank account.
After save, the empty statement's ending balance was also 148.00.

The standard Create lines from dialog found one eligible payment,
**#TEST-100001**, amount 121.03. Only that receipt was
[selected](selected-receipt.png), and line creation was submitted once. An
initial checkbox-label locator timed out before any creation request; the
corrected visible checkbox and selected-count/sum checks preceded confirmation.
The resulting single line recorded statement and transaction amounts 121.03,
zero charges and interest, and statement/account/effective dates 2026-11-04.
The draft ending balance became 269.03 while the bank balance remained 148.00
and the receipt remained unreconciled.

The Document Action dialog selected Complete and was confirmed once. The
[completed statement](completed-statement.png) and
[read-only observations](workflow-observations.json) independently verify:

- Process 147 / instance 1000026 finished with Result 1 and Completed.
- Statement 1000000 is Completed, Processed and Posted, with Processing N.
- Beginning balance 148.00 + statement difference 121.03 = ending balance 269.03.
  The bank account's stored current balance is now 269.03.
- Receipt #TEST-100001 is Posted, Allocated and Reconciled.
- Exactly one statement line exists for that receipt; charges and interest are zero.
- Four bank accounting entries balance in two schemas at 121.03 and 102.88;
  source debit and credit totals are 121.03 in each schema.
- Invoice 100001 remains Paid and Posted, total 123.50. The unchanged invoiceOpen
  and paymentAvailable functions still return zero.
- Stock remains 18 with one -2 shipment movement. The order line remains two
  delivered, two invoiced and zero reserved. Both older seed statement rows
  retain their original balances, processing and posting states.

No statement lines, balances, statuses or accounting amounts were repaired via
SQL. No additional engine change was required after the source-compared scalar
and numeric-format fixes. Binary source is
`c92dad68bc60184174902acdfedce2422a170420`, merged on main at `ab8dda7`, SHA-256
`32d2c0441d23b9a856e2cb45d2d85b263b3272b322b6c88acd2a9b6962eaa009`.

This extends the [invoice](../idempiere-invoice-posting-2026-11-04/README.md) and
[payment/allocation](../idempiere-payment-allocation-2026-11-04/README.md)
checkpoints through local bank reconciliation. It does not qualify the entire
application, purchasing/payables, reversals, other document rules, production
operations or the full Oracle programme.
