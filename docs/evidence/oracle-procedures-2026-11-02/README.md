# Stored procedure and CALL qualification

Compiler version 12 (`bicdb_oracle_preview_v12`, OID 999411) adds stored Oracle
procedure compilation, header normalization and namespace binding. Shared CALL
handling fixes OUT input-slot alignment, preserves IN OUT inputs, copies nested
outputs back to caller variables and returns declared PostgreSQL transport types.
The parser preserves BicDB's existing native header spellings and compact CALL
extension. No application names select engine behavior.

This remains a bounded qualification. **The original Oracle-provider iDempiere
application is not fully operational on BicDB.** The definition inventory advances
from **44/62 to 57/62**, but thirteen newly accepted procedures still fail the
separate diagnostic calls described below. A stored definition is not proof of a
compiled or functioning procedure.

## Oracle execution comparisons

[procedure-live-comparison.json](procedure-live-comparison.json) records **26/26**
comparisons against Oracle 23.26.3.0.0, under the fixed preview NLS configuration:

- Actual CALL effects on synthetic rows, followed by independent state queries.
- Parameterless declarations, defaults and replacement after a cached call.
- Qualified nested calls in IF branches and loops, and OUT/IN OUT copyback.
- DATE input/output fractional-second removal, including actual TIMESTAMP binds.
- An unhandled call's transaction rollback and a handled error's preservation of
  earlier successful statements in Oracle's exception model.
- Five actual-driver output comparisons: Oracle output binds versus PostgreSQL
  output rows, with NUMERIC OID 1700 and logical DATE as TIMESTAMP OID 1114.

The public procedure-reference.json freezes source observations. The runner
rechecks them against Oracle on every comparison. NUMBER outputs use text binds
to avoid a binary floating-point intermediary. The selected PostgreSQL transport
does not claim Oracle wire compatibility.

[postgres-procedure-controls.json](postgres-procedure-controls.json) records
**13/13** independent matches with PostgreSQL 18.4. It checks output placeholder
non-evaluation, named arguments, output metadata including NULL/BIGINT, nested
scalar/record copyback, defaults, native DATE, empty text, namespace rejection,
extra arguments and recovery. The same 13 cases pass with adaptive execution
flags enabled, recorded separately; that replay does not independently establish
which executor path ran.

Native BicDB also retains its existing positional compact CALL extension,
omitting pure OUT slots. The report explicitly records PostgreSQL rejecting that
form while BicDB accepts it. This intentional extension is verified separately
and is not counted as PostgreSQL equivalence. Oracle-authored procedures require
their declared output slots.

[reader-compatibility.json](reader-compatibility.json) verifies actual procedure
calls on the new reader, then opens only an isolated test database with the
previous version-11 binary. The old reader rejects version-12 calls with 0A000
and remains usable for a normal query. It never opens the upgraded application
database. This compiler marker introduces no storage-format transition.

## Application state and remaining procedure failures

[original-definition-inventory.json](original-definition-inventory.json) records
57 accepted definitions on the upgraded development database. Five remain
unaccepted: three package/autonomous-transaction functions, one hierarchical
query function and the procedure requiring Oracle dictionary column metadata.
Existing installed function bodies and compiler versions are preserved.

[original-procedure-call-diagnostics.json](original-procedure-call-diagnostics.json)
records a separate disposable copy of the application database. All thirteen
newly accepted procedures were called with NULL in every declared argument slot,
using argument metadata read from Oracle. **None of those diagnostic calls
succeeded**: eleven stop at remaining procedural grammar/compilation gaps and two
at dynamic SQL namespace binding. These are diagnostic inputs, not business
workflows. The original Oracle application procedures were not executed as part
of that rehearsal; it is not a source/target execution comparison. Application
procedure calls were confined to the disposable copy.

The retained 37 original-function, 21 configuration, 96 business-date and six
PRODUCTATTRIBUTE probes still match Oracle. A separate four-table import still
passes its six PRODUCTATTRIBUTE probes. Application values appear only as counts
and digests in the public evidence.

[complete-target-value-preservation.json](complete-target-value-preservation.json)
compares **915 tables and 185,537 current rows** against a verified stopped-writer
snapshot taken before the upgrade (997 files). Every row multiset matches, and
every table fingerprint equals the linked public vector from the previous
qualification. Current rows include earlier application writes; this is not a
claim that the live fixture still equals the original export's row count.

## Explicit existing semantic gaps

[existing-semantic-gap-observations.json](existing-semantic-gap-observations.json)
is a deliberately separate, **nonpassing** diagnostic artifact: **five of seven**
observations differ from Oracle. All seven outcomes also reproduce on the previous
version-11 runtime, so they are existing gaps rather than regressions introduced
by procedure support. They are excluded from the passing comparison counts.

The mismatches concern PL/SQL constant propagation around unused NVL conversions,
PLS_INTEGER resolution and formatless TO_NUMBER. For example, the observed
procedural TO_CHAR(NVL(1,'bad')) returns '1' on Oracle while the preview attempts
numeric conversion and raises 22P02. Runtime-parameter NVL error cases remain in
the passing corpus. Packages, autonomous transactions, transaction control,
dynamic SQL, broader overload/type rules and complete Oracle diagnostics remain
unqualified as well.

## Validation

- **1,259 Rust tests passed**, with three existing ignored tests.
- **25 reference-runner** and **22 migration-tool** Python tests passed.
- **938 retained generated Oracle cases** plus the 26 new procedure cases match,
  giving **964 passing comparisons** within their recorded scope.
- **145 native PostgreSQL cases** match; the adaptive-flag replay adds no unique
  cases to that count. The compact-call extension is explicitly separate.
- The broad checks initially caught header-mode and compact-call regressions;
  these were corrected, and the full checks rerun. The new namespace test also
  corrected its quoted-schema selection. No tests were disabled.
- Formatting, dependency license/source policy and the open-core boundary passed.
- Builds use one compiler job, disabled incremental compilation and active
  available-memory monitoring. Resource and artifact-cleanup measurements are
  recorded in validation.json.

The remaining original routine bodies, dictionary/package features, deferred
foreign-key validation, views, recovery operations and end-to-end application
workflows still require work. Neither 57 accepted definitions nor the passing
synthetic corpus establishes full Oracle compatibility or application readiness.
