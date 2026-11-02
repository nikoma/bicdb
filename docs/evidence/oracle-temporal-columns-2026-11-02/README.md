# Durable Oracle temporal column provenance

New temporal columns in the durable Oracle preview retain source DATE,
TIMESTAMP(n), or TIMESTAMP(n) WITH TIME ZONE declarations. DATE discards
fractional seconds; TIMESTAMP rounds to its declared precision. Their PostgreSQL
wire types remain timestamp/timestamptz. Native PostgreSQL DATE remains a calendar
date. This is a bounded development contract, not general Oracle compatibility.

The engine implements a general source-type rule, with no application-name
branches. `%TYPE`, base-column arithmetic, aliases, CTEs, derived tables and
cursor records use that provenance. It survives rename, reopen and native
backup/restore. Legacy physical timestamp(0) alone never implies Oracle DATE.
Single-column MODIFY can supply an explicit source declaration. Unchanged
physical metadata reconciliation preserves dependent views; an already declared
logical type change retains the dependency guard. Quoted column names now retain
their spelling through native ALTER TYPE as well.

Profile metadata format 2 fences old readers. The previous version-9 development
binary rejects a disposable upgraded database at startup, and the new reader
reopens it with correct values. The application database was never opened by
an old reader after promotion. A stopped-writer rollback snapshot verified 988
files and 415,981,036 bytes before its development runtime was upgraded.

Measured validation:

- 21 new live Oracle comparisons: COPY assignment, whole-second DATE versus
  timestamp precision, temporal `%TYPE`, quoted names, source metadata and native
  transport types. Timezone coverage is declaration/NULL metadata only.
- All 705 retained generated Oracle cases, 37 original-routine calls, 21
  configuration calls and 96 business-date calls still match.
- 110 independent native PostgreSQL controls, including six new temporal cases.
- 1,245 Rust tests passed, with three existing ignored tests; 22 Python reference
  and 22 Python migration tests passed. Formatting, license and open-core
  dependency checks passed.
- Disposable legacy-to-new migration and unchanged resume succeed while
  preserving complete target row multisets. A later inserted row is rejected by
  the resume fingerprint check. Source row-count differences are reported.

The first native temporal probe matched values and type IDs but found three
unnamed-expression label differences. A previous-binary baseline reproduces
them; its quoted-column ALTER bug required an ordinary lowercase column for that
baseline. Both initial reports remain here. The final six temporal controls use
explicit expression aliases and still compare selected/quoted column names.
Unnamed expression labels are not qualified by this change.

The original unchanged PRODUCTATTRIBUTE definition is now accepted after its
DATE column anchor was reconciled. This raises the original definition baseline
from 43/62 to **44/62**. All six initial execution probes still fail with SQLSTATE
0A000: default TO_CHAR cannot resolve a nested input in this source, which
contains CHR calls. Character-set CHR behavior is also outstanding. Definition
acceptance does not establish successful execution.

The application backfill is **in progress** at the recorded checkpoint: 127 of
2,263 source temporal declarations reconciled, across a source inventory of 906
base tables. View projections are excluded. The importer verifies complete
target row multisets before and after each table; it preserves application data
added since the original export. The checkpoint is not full source-data
reconciliation or completion of the backfill. Two source row-count differences
had been observed at that checkpoint. The earlier 915-table/185,471-row source
reconciliation remains the pre-application-write baseline.

Additional observations remain outside this qualification: a duplicate
non-temporal M_PRODUCT_BOM.M_PRODUCT_ID catalog entry and a reserved internal-name
error on a qualified UPDATE in a disposable preview fixture. The changed-target
resume control used INSERT. These are retained as follow-up catalog/name-resolution
gaps, not interpreted as successful UPDATE qualification.

The tested development runtime has SHA-256
`e807b88c08e3329a44ac59b8ea241708c7ec34c46d58ce499812540ed7bc5cab`,
built from engine commit `c97c58a`. The reconciler source is `b7f46d9`.
The build and full Rust validation used one compiler with active memory/disk
guards. Cleanup reclaimed 2,558,374,032 bytes of completed test executables and
left about 5.5 GiB of reusable cache, preserving runnable/compatibility binaries.

Remaining original definitions, broader temporal lineage, timezone regions,
dependency recompilation, foreign keys, complete view/routine execution and full
application workflows remain unqualified. No release, production deployment or
complete application acceptance is established by this batch. Application values
and SQL containing row identifiers are replaced by counts/digests in public
evidence; credentials, row exports and process metadata remain private.
