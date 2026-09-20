# Retained HammerDB NewOrder amount audit — 2026-09-05

The correctness-qualified >=600,000 NOPM goal remains open. Corrected Payment
v1 passes balance/history reconciliation, but a deeper NewOrder check finds
additional shared workload defects and a BicDB-specific rounding discrepancy.
These results do not invalidate the narrower recovery and ledger checks;
they do prevent calling the workload fully correct or its speed a keeper.

## Read-only comparison

Both databases are the retained, quiescent corrected-Payment-v1 runs. The
check covers warehouse 1, district 1, orders with `o_id > 3000`, not a random
or full-corpus sample. Each amount is compared with the retained procedure's
formula: item price * line quantity * (1 + warehouse tax + district tax) *
(1 - customer discount), rounded half up to two decimal places using Python
Decimal. This is a check of that procedure, not a claim of TPC-C compliance.

| Result | BicDB, pruning recovery | PostgreSQL 18.6 |
| --- | ---: | ---: |
| Post-seed lines examined | 498,097 | 374,804 |
| Non-null amount mismatches | 709 | 65 |
| Mismatches larger than one cent | 115 | 65 |
| Missing item or null amount | 545 | 418 |
| Missing item | 512 | 394 |
| Null amount | 545 | 418 |

The runs have different operation counts, so this is a comparison of failure
classes, not equal-row differential replay. The one-cent-only mismatch count
is 594 for BicDB and zero for PostgreSQL. Root cause is not yet established;
do not attribute those differences to WAL recovery without reproduction.

Captured examples include invalid item 100001 committed with a null amount.
Other examples show line amounts shifted to adjacent lines. Inspection of the
unchanged NewOrder procedure shows independently aggregated stock-returning
arrays later zipped with input line arrays; repeated item/warehouse keys can
produce fewer updated stock rows than input lines. These observations motivate
an explicit invalid-item rollback test and duplicate-key/line-alignment test.
They are not yet an exhaustive explanation of every mismatch.

The unchanged procedures also omit Delivery `c_delivery_cnt` and NewOrder
`s_ytd`, `s_order_cnt`, and `s_remote_cnt` maintenance. The exact seed has all
four counters zero: 480,000 customers and 1,600,000 stock rows. Seed stock
quantities span 10..100. The ordered seed quantity capture is
`/tmp/bicdb-financial-seed-stock-quantity.csv`, 1,600,000 rows, SHA-256
`1bf0e9d0b33823cc55562de345870fcd398c2872b23c8d989a5d16d736f4f272`.

## Reproduction and preserved evidence

`bench/tpcc/audit_neworder_amounts.py` preserves the executed diagnostic, with
an explanatory header. It performs only SELECTs, writes a new JSON result,
and intentionally returns zero on completed diagnostics even when counts are
nonzero. It is not wired into throughput qualification. Example:

```bash
python3 bench/tpcc/audit_neworder_amounts.py 55446 unique-label
```

Raw JSON and the original procedures are checked in under
`docs/evidence/neworder-amounts-2026-09-05/`. No retained dataset was modified
by the audit. The PostgreSQL container was stopped after the read-only check.

Next: reproduce invalid-item commit and duplicate-line alignment on fresh
small fixtures in both engines; isolate the BicDB one-cent numeric coercion
case; correct the workload explicitly and identically for both engines;
expand counter/amount reconciliation before qualifying any new speed result.
The queued instrumented training still uses Payment v1 and is explicitly
excluded from throughput results.
