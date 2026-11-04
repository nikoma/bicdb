# Original Oracle-schema physical inventory workflow

The actual iDempiere JVM now completes a selected physical inventory count,
adjustment, accounting posting and correction reversal through its standard web
UI, using the original Oracle-schema application on BicDB through PostgreSQL
drivers. All 57 selected observations pass, including all 46 earlier financial
and reporting observations restored after reversal. This is one application
workflow; full application and Oracle compatibility remain incomplete.

## Observed workflow

A new HQ Warehouse document used the normal Create Inventory Count List process,
filtered to the seeded Oak Tree product. It generated one line with book and
count quantities of 21. Entering a count of 20 through the quantity editor,
saving and completing the document produced one committed `I-` transaction of
minus one at the HQ locator. The application posted two balanced ledger rows in
each accounting schema: debit/credit 53.25 in schema 101 and 45.26 in schema
200000. HQ stock moved from 21 to 20; global stock moved from 25 to 24.

The first correction reversal exposed unsupported MERGE in cost reposting and
rolled back. Its failed process record, 1000042, remains preserved. General
[matched-update MERGE and row-binding fixes](../sql-merge-matched-update-2026-11-04/README.md)
were independently tested and merged. A clean runtime upgrade retained 52
forward-state observations, all original definitions/view metadata and saved
report rows before one fresh reversal submission.

The fresh process, 1000043, succeeds. Original header 1000000 and reversal header
1000002 are both reversed, processed and posted, with reciprocal references.
The reversal line has book 20 and count 21, producing one committed `I+`
transaction of plus one. Across the two documents, account/product/locator
balances net to zero in both accounting schemas. HQ stock returns to 21,
store stock remains four, transit remains zero and global stock returns to 25.

The new document uses its normal generated number; existing seed document
numbers were not changed. Sequence gaps from the failed attempt remain intact.
The three original inventory headers and their original line/date observations
are unchanged. Previously settled sales, purchasing and bank workflows still
match their recorded results.

## Evidence and replay

[Acceptance](acceptance.json) identifies source, binary, document/process IDs,
stock transitions, accounting and preservation hashes. All 62 routine and 160
view fingerprints remain unchanged; all 160 views retain original output labels
and type families. The three existing Trial Balance requests retain all 141 rows
with identical per-request hashes. Their PDFs were not regenerated in this
checkpoint. No original routines or views were recreated.

[Recorded observations](workflow-observations.json) contain the exact trusted
SELECT checks for this seeded fixture. The repository's
[read-only verifier](../../../examples/oracle/idempiere/verify_workflow.py) replays
all 57 checks with its connection supplied through `BICDB_DSN`. The historical
failed process is checked separately from the three successful workflow
processes; it is not relabeled as successful.

The implementation passes 612 SQL tests, 364 driver/protocol tests and 10
additional existing projection/quoted-join integration tests. Independent
references and explicit MERGE limits are recorded in the linked SQL checkpoint.
This is local clean-upgrade and selected workflow evidence, not crash-recovery,
distributed, release or production qualification.

An independent numeric MAX decoding gap was found by verification tooling and
remains open. It did not prevent this completed workflow; broader aggregates,
performance, naming, concurrency and the full compatibility programme require
further work.

The subsequent [logical primary-key checkpoint](../sql-primary-key-extrema-2026-11-04/README.md)
closes that numeric MIN/MAX decoding gap, with independent Oracle/PostgreSQL
controls and a clean database upgrade preserving all 57 observations. Broader
aggregation, performance and application coverage remain separate work.
