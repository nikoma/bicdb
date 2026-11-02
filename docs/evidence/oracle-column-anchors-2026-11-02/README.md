# Column anchors and constrained numeric returns

The Oracle preview resolves table.column%TYPE and schema.table.column%TYPE in
the function definition's namespace, with quoted identifiers preserved. Session
schema selection is passed into source lowering before an unqualified function
is compiled. Qualified definitions retain their own namespace. Anchors use the
same physical relation encoding as SQL; a missing owner table does not resolve
to an unrelated public table.

For the tested mapped types, local NUMBER variables inherit precision/scale and
VARCHAR2 CHAR variables inherit character length. Column defaults and NOT NULL
do not transfer. Arguments remain unconstrained. Character return lengths are
unconstrained, while numeric return precision/scale is checked and rounded. A
function exception handler can catch numeric return overflow. The compiler
stores this conversion as an ordinary explicit CAST in the body, preserving
exact literal operands and existing reader behavior for that operation.

The [Oracle-only signature probe](oracle-signature-probe.json) established the
argument/return distinction independently of BicDB. It exposed an incorrect
initial assumption that numeric returns were unconstrained. The [public
fixture](../../../examples/oracle/reference/routine_anchors.py) now checks local
constraints, quoted names, UTF-8 character lengths, NULLs, unconstrained
arguments and character returns, exact wide numeric literals, numeric return
rounding, errors and exception handling. All [41 declared expectations passed
on Oracle](oracle-fixture-validation.json), including six explicit error cases.
The [BicDB/Oracle comparison](anchor-comparison.json) matches every assertion.

The engine regression then exposed missing live unary evaluation in the generic
procedural evaluator: a CASE branch using -parameter reached constant evaluation
and lost the binding. Unary signs now evaluate their live operand and consult
the frame's shared declared-type metadata, using existing typed operators. A
subsequent NULL regression exposed numeric unary minus trying to parse an empty
string; strict NULL propagation was restored before numeric parsing. All failing
assertions were retained. Independent PostgreSQL regressions cover exact wide
numeric values, NaN/infinity, NULLs, integer overflow and lazy CASE branches.
No application identity selects either rule.

[Driver regressions](../../../crates/bicdb-pgwire/tests/oracle_hr.rs) exercise
prepared calls, result metadata, selected schema versus definition owner,
precision errors, recovery and reopen for both profiles. Default PostgreSQL
keeps its signature-modifier behavior; Oracle constrained numeric returns have
a separate expected result. The [PostgreSQL declaration contract](https://www.postgresql.org/docs/18/sql-createfunction.html)
explains the native signature behavior.

The existing [102 week](week-recheck.json), [36 calendar](calendar-recheck.json),
[29 temporal](temporal-recheck.json) and [24 namespace](namespace-recheck.json)
assertions pass on the same binary. Together with anchors, all 232 public
reference assertions match Oracle Free 23.26.3.0.0. Generated fixture objects
were removed from both databases. The reference uses dot-decimal formatting;
alternate NLS configurations are not qualified.

The [original source retry](original-routines.json) accepted five of six
previously rejected definitions, increasing accepted original definitions from
30 to 35 of 62. Only destination namespaces were adapted; source bodies were
unchanged. ACCTBALANCE matches two complete 379-account seed sweeps;
BPARTNERREMITLOCATION matches all eighteen partners; PAYMENTAVAILABLE matches
both payments. Selected absent/NULL cases also match. Ten of thirteen read-only
comparisons match overall. DOCUMENTNO still fails on procedural CASE, and
GET_SYSCONFIG still fails on nested-block/ROWNUM handling. NEXTBUSINESSDAY's
anchor lookup succeeds but its parameterized cursor declaration remains
unsupported. Definition acceptance is not execution qualification, and the
three execution differences remain explicit in the report. Source rows are
published only as counts and hashes.

Two additional original views now match complete bounded row multisets on two
reads: C_PAYSELECTION_CHECK_VT (one row) and RV_PAYMENT (two rows). The newly
available functions initially executed through views whose stored result types
were still text. [Before refresh](views-before-refresh.json), full typed-row
comparisons failed. Replaying the [unchanged original view queries](view-refresh.json)
after their dependencies were installed refreshed the numeric result metadata;
[all repeated comparisons then matched](views-after-refresh.json). No comparator
coercion was added. This raises cumulative strict non-clock seed-view matches
from 90 to 92 (59 nonempty and 33 empty). It does not qualify automatic view
invalidation/recompilation or full application behavior.

The [original date functions](date-functions-recheck.json),
[six payment-term cases](paymentterm-recheck.json),
[two fixed-month branches](fixed-month-recheck.json) and
[nullable UNIQUE recheck](constraint-recheck.json) still pass. Temporary
payment-term input changes were rolled back and verified in both databases.

The [reader check](reader-compatibility.json) uses a separate database. Both
runtimes enforce the stored numeric CAST, including rounding, overflow and
NULLs. The older runtime rejects the new CASE evaluation path with SQLSTATE
0A000. The application database was never opened by the older runtime. This
checks selected execution capabilities, not a new durable storage format.

[Validation](validation.json) passed 1,106 Rust tests: 376 SQL unit tests (three
existing ignores), 417 SQL integration tests, 118 pgwire unit tests, 21
Oracle-profile driver tests, 168 protocol tests and six integrated application
compatibility tests. All nineteen reference-harness tests, formatting,
licensing and dependency-boundary checks passed.

Builds ran with one Cargo job and incremental compilation disabled. After host
memory headroom dropped during an earlier attempt, only the task's own build
was stopped. Subsequent runs used a monitor sampling available RAM every second
and stopping only its owned process group below eight GiB. The completed build
peaked at about 4.65 GiB aggregate process RSS, with at least 9.98 GiB host RAM
available. Resource interruptions and the two corrected regression failures were
not reported as completed passing runs.

The development binary SHA-256 is
`119164c75fb75ee7fafb1cfa8922baf257c43f651e490e0da3555e17b96ce8ab`;
its target is `debug/examples/oracle_server`. The isolated fixture was upgraded
to that binary. There is no published release or production deployment.

Types are materialized at source creation/replacement. Automatic dependency
invalidation after column DDL, variable/record anchors, temporal logical-type
provenance, general BYTE semantics, user-defined Oracle types and cross-schema
privileges remain outstanding. The application copy has earlier test writes;
row differences require input reconciliation before being attributed to engine
semantics. Foreign keys and complete business workflows remain unqualified.
