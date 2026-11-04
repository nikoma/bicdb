# Single-product production and correction reversal

The original Oracle-schema application completes one BOM production through
its standard UI on BicDB, using the PostgreSQL driver and Oracle preview
profile. The workflow creates a header, generates four lines, completes and
posts the production, then uses Reverse - Correct. No engine or application
code change was required for this workflow; the runtime is the previously
qualified logical primary-key build.

The [acceptance record](acceptance.json) identifies source and binary. The
[recorded observations](workflow-observations.json) pass all 67 trusted SELECT
checks, including the prior 57 financial, inventory and reporting observations.
These are selected observations of a seeded fixture, not 67 complete UI flows
or full application qualification.

## Business result

Production 1000000, document 1000000, builds one Patio Furniture Set using BOM
145 at HQ locator 101. Process 1000044 generates the expected four lines;
process 1000045 completes the document. The correction reversal succeeds as
process 1000046 and creates production 1000001, document 1000001. Both headers
are Reversed, processed and posted, with reciprocal reversal links and four
processed lines each. The reversal document uses the application's normal next
sequence number.

| Product | Initial stock | After build | After reversal |
| --- | ---: | ---: | ---: |
| Patio Chair (133) | 30 | 26 | 30 |
| Patio Table (134) | 12 | 11 | 12 |
| Patio Sun Screen (135) | 15 | 14 | 15 |
| Patio Furniture Set (145) | 0 | 1 | 0 |

Exactly eight committed stock movements represent the build and reversal.
Movement types retain their source-document convention while reversal
quantities negate the original quantities. Stock totals are checked across
material-policy layers. Original layers date from 2003-01-22; returned
components occupy separate 2026-11-04 layers. Exact layer rows and the absence
of duplicate full layer keys are recorded. Broader FIFO and material-policy
behavior remain unqualified.

Each document has four accounting rows per schema. Debit and credit totals
are 225.00 in schema 101 and 191.26 in schema 200000. The latter follows
per-component currency rounding: 57.38 + 114.75 + 19.13 = 191.26. Forward
values were independently checked against those rounded component costs.
All eight product/account groups net to zero in accounting amounts, source
amounts and quantities after reversal.

## Preservation and replay

All 62 routine and 160 view fingerprints and all 141 stored report rows remain
unchanged. Earlier sales, purchasing, banking, movement and inventory-count
observations pass. Report PDFs were not regenerated. The preceding
[clean database upgrade](../sql-primary-key-extrema-2026-11-04/README.md)
records all 160 view descriptions and 11,983 column labels/type families.

Replay the fixture with the repository's
[read-only verifier](../../../examples/oracle/idempiere/verify_workflow.py),
providing this seeded database through `BICDB_DSN`. The verifier uses prepared
queries in a read-only transaction and compares numeric values independently
of formatting scale. Timestamp layer labels use an explicit SQL format.

The runtime's prior validation passes 613 SQL library tests and 364
driver/protocol tests, with independent Oracle and PostgreSQL references
recorded in the linked SQL checkpoint. No new binary or release was produced
for this application-only checkpoint. Production plans, disassembly, alternate
BOMs, broad concurrency, recovery and complete application/Oracle compatibility
require further qualification.
