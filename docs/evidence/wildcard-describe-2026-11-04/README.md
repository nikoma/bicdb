# Describe typed wildcard queries without executing sources

Preparing or describing a query should obtain its result metadata without running
its FROM clause or volatile expressions. Fully typed wildcard projections now use
the same static column inference path as explicit typed projections. Logical
wildcard ordering and duplicate labels are preserved. Unknown types and row-lock
queries retain their existing fallback; no application identity selects the rule.

The [reference fixture](../../../examples/oracle/reference/wildcard_describe.py)
uses actual Oracle cursor.parse and PostgreSQL protocol Parse/Describe. It checks
bare and qualified table wildcards, a joined wildcard with a sequence expression,
a stored view, exact quoted labels through tables, derived tables, CTEs and
declared view columns, and the first sequence value after all preparations. Both final
comparisons match **9/9**: [Oracle](oracle-controls.json) and
[PostgreSQL](postgres-controls.json). Both [Oracle](oracle-baseline.json) and
[PostgreSQL](postgres-baseline.json) prior-binary comparisons match **8/9**;
joined wildcard preparation raises an unsupported-function error. The source
expectations all qualify, and the final fixtures report zero cleanup errors.
The baselines do not demonstrate an advanced sequence: they retain the error and
its unchanged sequence observation. Separate native/Oracle wire regression tests
ensure preparation succeeds and does not advance a sequence.

Existing executed join controls retain **23/23 Oracle** and **23/23 PostgreSQL**
comparisons on this binary: [Oracle](join-oracle-controls.json),
[PostgreSQL](join-postgres-controls.json). SQL regression coverage includes a
volatile stored-view projection, qualified and unqualified wildcards, and typed
join output metadata without query execution.

An [application metadata read](application-view-metadata.json) on a disposable
copy of the stopped database compares **160 actual Oracle result descriptions**
against BicDB Parse/Describe. **127/160** match column names/order; 32 return
collection-not-found errors and one differs in column labels/order. All 33 remain
unqualified. A separate quoted catalog read confirms 128 stored views, with 32
source views absent from this checkpoint. The target descriptions take
7.077 seconds in total after database startup. This does not qualify view result
values, equivalent types, all view definitions or complete application operation.

An earlier probe incorrectly sorted exported numeric column-position strings as
text, producing a false mismatch for several views. Numeric sorting matches
actual Oracle result metadata for **all 160 views**. This was a probe error, not
an Oracle catalog defect; no database or migration column order was changed to
satisfy the invalid expectation. The final target comparison uses independently
read actual Oracle result descriptions.

The preceding full-application run loaded 62 original routines, but a complex-view
wildcard description stalled other work and the browser login timed out. That
run was stopped before this fix; no successful application rerun or memory-growth
resolution is claimed here. The original completed-order database is preserved.

The routine compiler remains 26 and durable metadata format remains 5. These
results qualify bounded development behavior, not a release, production deployment,
complete Describe validation, full application acceptance or the wider Oracle
compatibility programme.

The initial broader test run [failed a quoted-name regression](initial-quoted-label-regression.json).
The corrected implementation separates protocol result labels from the folded
names used for type binding. Dedicated SQL and both-profile wire regressions cover
quoted tables, derived tables, CTEs and declared view output names. The earlier
failure remains evidence, not a passing result.

A [missing-view DDL probe](missing-view-ddl-baseline.json) independently exposes
an ANSI character-type parsing gap. [Standalone Oracle controls](ansi-character-cast-baseline.json)
accept CHARACTER VARYING(6) and CHAR VARYING(6), while the prior target rejects
both with 42601. Twenty-four missing source definitions contain CHARACTER VARYING;
the original 32-view gap and all runtime workflows remain unqualified. No view SQL
was rewritten to evade the parser failure.

Final local validation passes **919 Rust test executions** in five batches, with
three existing ignores, plus 25 Python reference tests. Formatting, licenses and
the dependency boundary checks pass. The [validation record](validation.json)
records exact source/binary provenance and guarded resource measurements. The
build/test run retained more than 20 GiB host available RAM. Cleanup reclaimed
1.64 GiB of completed test executables; the task target remains approximately
7.5 GiB. Frozen binaries and reusable dependencies were preserved.
