# Purchase order completion and incoming stock

The original Oracle-schema application created and completed purchase order
**800003** for the seed vendor **Tree Farm Inc.** through the standard Purchase
Order window. The vendor, invoice location/contact, warehouse, price list and
payment term were resolved through normal application lookups/callouts.

The [completed order](completed-order.png) and
[read-only observations](workflow-observations.json) verify:

- Purchase order ID 1000001 is purchase-side (IsSOTrx N), Completed and Processed,
  with Processing and SendEMail N. Its vendor is 114, warehouse 103, price list
  101, currency 100, payment term 106 and target document type 126.
- The single saved line is product 123 (Oak Tree), quantity three at the
  application-calculated price 61.75. Line, order lines total and grand total
  are 185.25. No amount was repaired through SQL.
- Process 104 / instance 1000027 finished with Result 1 and Completed. Completion
  was selected and confirmed once in the original UI.
- The line is three ordered/on order, zero received and zero invoiced. The
  unchanged original stock functions return available 18, on hand 18, reserved
  zero and incoming/ordered three.

The application-calculated purchase price follows the selected price list; it
was not replaced with a value read directly from the supplier-product table.
Initial product lookup was slow in this unoptimized development build, and
metadata query timings were retained privately for subsequent performance work.
This checkpoint does not qualify production performance.

The binary source is `c92dad68bc60184174902acdfedce2422a170420`, SHA-256
`32d2c0441d23b9a856e2cb45d2d85b263b3272b322b6c88acd2a9b6962eaa009`.
The subsequent material receipt header was saved through the UI, but its original
line-selection view exposed an unlowered NVL in a later UNION operand. The
[general set-operand fix](../set-operand-builtin-types-2026-11-04/README.md)
has separate source comparisons; application receipt requalification is a
separate gate. Receipt, supplier invoice, outbound payment, the rest of the
application and the broader Oracle programme remain unqualified here.
