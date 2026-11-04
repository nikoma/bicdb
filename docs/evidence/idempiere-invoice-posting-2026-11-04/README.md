# Invoice generation and posting through the application

The original Oracle-schema application, running on BicDB through PostgreSQL
drivers, generated and completed invoice **100001** from sales order **50000**
using its standard **Generate Invoices (manual)** screen. The
[eligible-order screen](eligible-order.png) contained exactly one candidate,
total 123.50. Only that order was selected. Parameters were Complete, invoice
date 2026-11-04, consolidation enabled and Run as Job disabled. The process was
submitted once; no invoice, status, quantity or accounting value was written or
repaired directly through SQL.

The [UI result](process-success.png) reports success, Created = 1 and Invoice
Processed: 100001. [Read-only observations](workflow-observations.json) independently
verify:

- Process 134 / instance 1000024 finished with Result 1 and Created = 1.
- Exactly one invoice exists for the order, Completed, Processed and Posted;
  Processing and SendEMail are N. Total lines and grand total are 123.50.
- Its single line references the order line, product 123, quantity two and net
  amount 123.50. The order uses Immediate invoicing; the invoice line's shipment
  reference remains null in the recorded application result.
- Four ledger entries balance in two accounting schemas: 123.50 in schema 101
  and 104.98 in schema 200000. Source debits and credits are 123.50 in each.
- The order line is two delivered and two invoiced, with zero reserved.
- On-hand and available stock remain 18. There is still exactly one -2 movement
  for the shipment line; invoicing did not repeat the stock movement.

The binary is built from source `c92dad68bc60184174902acdfedce2422a170420`,
merged and verified on main at `ab8dda7`, SHA-256
`32d2c0441d23b9a856e2cb45d2d85b263b3272b322b6c88acd2a9b6962eaa009`.
Its [preceding upgrade](../numeric-format-pictures-2026-11-04/README.md#subsequent-application-upgrade)
loaded 62 original routines, verified 160 original view definitions/type families,
and preserved the already posted shipment and its balanced ledger.

This qualifies the selected local generation/completion/posting workflow. It does
not qualify payment or allocation, other invoice rules, cancellation/reversal,
other application modules, production operations or the broader Oracle programme.
The invoice is unpaid at this checkpoint. No new engine change was needed after
the preceding independently source-compared scalar-subquery and format fixes.
