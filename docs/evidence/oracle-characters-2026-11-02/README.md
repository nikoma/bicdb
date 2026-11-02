# Oracle character expressions and completed temporal backfill

The unchanged PRODUCTATTRIBUTE source now matches the Oracle reference in all
six seed-data probes on the upgraded application database. It was first checked
against a disposable four-table copy with complete copied-value reconciliation,
then recreated in the application namespace under compiler version 10. Its body
is unchanged; the destination namespace is the only source adaptation. This is
bounded execution evidence, not coverage of every branch or the full application.
The original definition count remains **44/62**; 18 definitions remain blocked.

The engine changes are general semantic rules. Under the fixed AL32UTF8 preview,
CHR decodes packed bytes for one complete character, including supplementary
characters. It truncates fractional numeric input and checks numeric range.
Nested default TO_CHAR resolves character results in ordinary SQL, stored
expressions and embedded queries. Version 10 has language OID 999409; versions
1–9 keep their identities and earlier CHR behavior. Old readers reject the new
routine calls with 0A000 and recover for an ordinary query. Recreate Oracle source
explicitly to select the new contract.

Independent PostgreSQL controls retain Unicode scalar chr values. They also
exposed and corrected existing native NULL/error behavior: NULL remains NULL,
invalid positive Unicode values use 54000, and negative values use 22023. Native
PostgreSQL text results remain OID 25; Oracle-authored VARCHAR2 routine returns
retain native VARCHAR transport. A PostgreSQL-driver integration test checks
explicit integer/text bind types, NULL, failure recovery and persisted calls
after reopen under both profiles.

The initial character corpus exposed an unselected failing COALESCE argument.
A debugger trace located eager evaluation in the session evaluator. The fix
also covers database-aware constant projections, defaults, target-record
updates, generic records and specialized catalog evaluation. Borrowed arguments
preserve stable expression identity in routine caches. A regression exercises
the evaluator paths, including a row-dependent unused UPDATE argument; common
type validation still runs. SQL and procedural source comparisons distinguish
an unused failing argument from a selected failure.

The final corpus has **119/119** live Oracle character comparisons. The measured
source reports ORA-01426 for SQL CHR numeric overflow and wraps a procedural
call as ORA-06502. The fixture records both exact source codes; the preview maps
numeric overflow to PostgreSQL SQLSTATE 22003. This does not establish Oracle
SQLCODE/SQLERRM identity or all predefined exception distinctions. NUL is
explicitly unsupported in native PostgreSQL text; invalid packed characters,
whose Oracle results are indeterminate, are explicitly rejected. NCHAR_CS, other
database encodings and general globalization semantics remain unqualified.

Temporal reconciliation is now complete: **2,263 declarations across 906 base
tables**, with equal before/after target row multisets for every table. The
source catalog's view projections were excluded. An independent comparison
against a copy of the verified stopped-writer pre-upgrade snapshot then matched
**all 915 application tables and 185,537 rows**. The original rollback snapshot
was kept pristine. These are current target values, including earlier
application writes; the original Oracle export's pre-application-write baseline
was 185,471 rows. Eleven source row-count differences are recorded rather than
discarded. This comparison proves preservation through the temporal upgrade,
not fresh equality between a modified application database and the Oracle seed.

A second stopped-writer snapshot verified 991 files and 440,968,135 bytes before
the character runtime upgrade. Application source definitions and row values
remain private; public result records use row counts and canonical digests.

Validation on the new implementation:

- All 726 retained generated Oracle cases still match under compiler version 10.
  Together with the new character cases, this is 845 matching generated cases.
- The 37 selected original-routine calls, 21 configuration calls and 96
  business-date calls still match. PRODUCTATTRIBUTE adds six seed probes on the
  application database and the same six inputs on the independent subset.
- All 132 independent PostgreSQL controls match, including 22 character cases.
- 1,251 Rust tests passed, with three existing ignored tests. A focused runtime
  COALESCE regression passed again after making its UPDATE argument depend on a
  row value, keeping planning-time constant folding outside that assertion.
- All 22 Python reference tests and 22 migration tests passed. Formatting,
  licensing and open-core dependency checks passed. Earlier validation batches
  were interrupted while fixing observed failures; they are not counted as
  completed passes.

The development binary has SHA-256
`f91cc6dad93939a6d292b9fd8a9de92374ce3d638e2111a1e94f660754048ed9`,
built from engine commit `c512c89`; later source changes in this batch cover
tests, fixtures and documentation. The application development fixture runs
that binary. Memory/disk guards bounded compilation and snapshot comparison.
Cleanup reclaimed 2,834,368,224 bytes of completed test executables, retaining
about 5.5 GiB of reusable cache and required runtime/compatibility binaries.

The qualified UPDATE follow-up narrows an earlier observation: ordinary
non-primary updates succeed, while primary-key changes in a named schema fail
with reserved-internal-name SQLSTATE 42601 in both PostgreSQL and preview modes.
That remains a separate engine gap. A duplicate non-temporal catalog field
observed in the earlier batch also remains unqualified. Neither observation was
removed or treated as successful application coverage.

Stored expressions created before the new character contract may require source
recreation; temporal backfill alone does not rewrite defaults or views. Broader
temporal lineage, dependency recompilation, Oracle NVL/other scalar semantics,
remaining procedures/functions, foreign keys, all views and complete business
workflows remain outstanding. This batch does not establish a release,
production deployment, certification or complete iDempiere execution.
