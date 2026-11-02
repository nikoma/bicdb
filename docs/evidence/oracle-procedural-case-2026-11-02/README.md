# Procedural CASE statement qualification

Simple and searched CASE statements use the shared routine interpreter. A scalar
CASE expression selects a statement-arm index once; its lazy conditions retain
selector evaluation and first-match behavior. Only the selected body executes.
An omitted ELSE raises SQLSTATE 20000 (CASE_NOT_FOUND), independently of scalar
SQL CASE's implicit NULL result. The named exception condition maps to that
state. Nested branch bodies retain RETURN, EXIT and CONTINUE behavior.

The parser distinguishes procedural END CASE from scalar CASE expressions and
tracks nested IF/loop/case boundaries. Branch WHEN tokens inside a handler's
nested case no longer split the enclosing exception-handler list. Namespace
binding, declared-type arithmetic, expression binding, symbol collection,
write classification and loop-control validation recurse through case arms.
The adaptive compiler falls back to the interpreter for these statements.
No application identity selects these semantics.

The [public fixture](../../../examples/oracle/reference/routine_case.py) has
[34 independently checked Oracle expectations](oracle-fixture-validation.json).
It covers both forms, multiple statements, lazy conditions, NULL selectors,
nested case/if/loop statements, scalar expressions, query statements, handler
bodies and a sequence selector evaluated once. Three expected-error assertions
check ORA-06592 (20000 over PostgreSQL transport) and ORA-01476 (22012).

The declaration contracts are described by [Oracle CASE statements](https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnpls/CASE-statement.html)
and [PostgreSQL control structures](https://www.postgresql.org/docs/18/plpgsql-control-structures.html).
Separate PostgreSQL tests preserve [integer division](https://www.postgresql.org/docs/18/functions-math.html):
1/2 is zero for INTEGER and selects a different branch from Oracle NUMBER's
fractional result. The first regression incorrectly copied the Oracle result
into that native test; the expectation was corrected independently. A second
regression checks invalid CONTINUE at invocation, matching BicDB's existing
native routine compilation timing; immediate DDL validation remains unqualified.

Labels, Oracle qualification of comma-separated simple-case alternatives,
newer selector predicates, general conversion rules, Oracle transaction/error-continuation semantics and
full PL/SQL remain outside this bounded evidence. New preview definitions use the version-5 routine language (OID 999404).
Version-4 declarations and their language identity remain readable. Version 5
fences new definitions from older runtimes whose CASE lowering could evaluate
a simple selector repeatedly. The native PostgreSQL comma-separated alternative
syntax is retained through scalar CASE dispatch. This is a routine capability
marker, not a new database storage format or proof of all-reader compatibility.

The [first live comparison](initial-case-comparison.json) matched 29 of 34
assertions. Four nested-loop calls exposed canonicalization splitting the `..`
range operator into decimal tokens. The sequence-selector call exposed missing
quoted/qualified NEXTVAL lowering. The ownership regression then exposed a
row-independent function call through DUAL running in a derived-row session
without writable sequence access. Those assertions are retained.

Range normalization now uses original token adjacency; whitespace-separated
decimals, comments and quoted literals retain their meaning. Qualified sequence
names retain quoted components and definition ownership. Additional DUAL elision uses the caller's session only for plain row-independent
scalar projections; it excludes aggregates, row references, query bounds and
CTE shadows. General mutable function
calls across arbitrary rowsets/CTEs and the complete Oracle sequence contract
remain unqualified. Recreate source definitions to obtain corrected range and
sequence source lowering; already translated invalid bodies are not silently
rewritten.

The broader regression run exposed an older cell-row test expecting a NULL
argument to the random-shaped helper to raise an error. An independent
[PostgreSQL 18.4 probe](native-null-reference.json) established NULL propagation
for both function shapes and numeric TRUNC, and SQLSTATE 22012 for real division
by zero. The regression now asserts NULLs through both call paths and retains
error parity using actual division by zero. No assertion was disabled. The
complete cell-row target then passed seventeen tests.

Scalar DUAL elision also exposed missing declared-function result metadata and
ignored FETCH clauses. Catalog-aware scalar typing now reads stored return
types; FETCH ONLY normalizes into the common LIMIT/OFFSET path, including
no-FROM queries and skipped scalar projections. Separate PostgreSQL regressions
check zero/default counts, offsets, tables, set-returning projections, typed
NULL results and a sequence that must not advance under a zero-row bound.
FETCH PERCENT and WITH TIES fail explicitly; general Oracle row-count coercion
and the complete row-limiting contract remain unqualified.

The final [CASE comparison](case-comparison.json) passes all 34 assertions.
The [integrated validation record](validation.json) records 1,141 passing Rust
tests, three existing ignored SQL unit tests, 19 reference-harness tests and
266 passing live Oracle comparisons across CASE, anchors, week, calendar,
temporal and namespace fixtures. Each comparison report is retained alongside
this document. Formatting, license and open-core dependency checks passed
locally; hosted CI has not been inspected. The original incorrect NULL-error
assertion and its correction are recorded separately from successful reruns.

The [selected older-reader check](reader-compatibility.json) uses a separate
disposable database. The new runtime executes three version-5 CASE functions;
the selected older runtime rejects all three with SQLSTATE 0A000 and remains
usable afterward. The application database was never opened with that older
runtime. This does not qualify every reader or upgrade path.

The [original-routine recheck](original-routine-rechecks.json) matches 12 of 13
selected comparisons, including DOCUMENTNO over all 56 seed rows and an absent
identifier. The remaining GET_SYSCONFIG missing-default call reports 42601;
nested exception blocks and ROWNUM remain blockers. Result rows are represented
only by counts and digests. The [additional recheck summary](application-recheck-summary.json)
records 23 original date-function cases, six existing payment-term cases, two
rolled-back fixed-month cases, three constraint-policy checks and repeated
reads of two manually refreshed views. These are bounded checks on a development
fixture, not full application qualification. Accepted original definitions remain
35 of 62; required foreign-key migration and business workflows remain outstanding.

The development example binary has SHA-256
`54179155d9b026dae1dbeb93efc5f77c69b2714da9437b133f1ae667660422b0`.
It was built from the source commit in the validation record. This evidence is
not a published release or a verified production deployment. Builds used one
job, disabled incremental compilation and monitored available host memory,
stopping only the owned build group if headroom fell below eight GiB. Superseded
test outputs were pruned while reusable dependencies and required binaries were
retained.
