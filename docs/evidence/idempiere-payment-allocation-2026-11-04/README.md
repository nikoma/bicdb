# Receipt, discount and allocation through the application

The original Oracle-schema application completed receipt **#TEST-100001** against
invoice **100001** through its standard Payment and Receipt window. Selecting
the invoice calculated **121.03 payment + 2.47 discount = 123.50**, with zero
write-off and zero over/under-payment. The configured term is 2%10 Net 30; both
invoice and receipt dates are 2026-11-04. These amounts were calculated by the
application rather than entered as SQL repairs. The
[calculated-amount screen](calculated-amounts.png) records the values before save.

The draft was saved once for organization HQ and the seed bank account, tender
Check with test check number TEST-100001. Online Access was disabled. The
Document Action dialog selected Complete and was confirmed once. The
[completed receipt](completed-receipt.png) and
[read-only checks](workflow-observations.json) verify:

- Process 149 / instance 1000025 finished with Result 1 and Completed.
- Receipt ID 1000000 is Completed, Processed, Posted and Allocated, with Processing
  and IsOnline N. Its invoice reference is the intended invoice ID 1000000.
- Exactly one receipt and one allocation line exist for the invoice/payment pair.
- Allocation document 1000002 is Completed and Posted. Its line records payment
  121.03, discount 2.47, zero write-off and zero over/under-payment.
- Invoice 100001 is marked Paid and links to that receipt. The unchanged original
  invoiceOpen and paymentAvailable functions both return zero.
- Receipt accounting has two entries per schema, balanced at 121.03 and 102.88.
  Allocation accounting has three entries per schema, balanced at 123.50 and
  104.98. Source debit/credit totals balance as well.
- Stock remains 18, exactly one -2 shipment movement remains, and the order line
  remains two delivered, two invoiced and zero reserved.

The binary source is `c92dad68bc60184174902acdfedce2422a170420`, on main at
`ab8dda7`, SHA-256
`32d2c0441d23b9a856e2cb45d2d85b263b3272b322b6c88acd2a9b6962eaa009`.
No additional engine change was required after the independently qualified
scalar-subquery and numeric-format fixes. Earlier checkpoints record the
[posted shipment](../parenthesized-query-metadata-2026-11-04/README.md#subsequent-application-posting-recovery)
and [invoice generation/posting](../idempiere-invoice-posting-2026-11-04/README.md).

This is a local order-to-receipt/allocation checkpoint, not qualification of the
entire application. The receipt is not bank-reconciled at this stage. Bank
reconciliation, reversals, partial/over-payments, other document rules and ERP
modules, production operations and the broader Oracle programme remain open.


A fresh browser session opens **Sales Invoice and Credit/Debit Note** and verifies
the invoice's checked, read-only Paid control. Its
[Allocation tab](invoice-allocation.png) displays the single receipt allocation,
amount 121.03 and discount 2.47. An initial navigation attempt used the older
Invoice (Customer) menu label and returned no matching menu entry; the corrected
navigation uses the original exported menu metadata. No payment completion or
allocation action was repeated during this navigation correction.
