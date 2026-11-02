# Original routine and view coverage

This development inventory uses the release-13 Oracle source clone and the
native BicDB fixture described in the [desktop probe](../idempiere-release13-desktop-2026-11-01/README.md).
The development binary and implementation validation are recorded in the
[week semantics evidence](../oracle-week-truncation-2026-11-01/README.md).
The application remains unqualified and its Java process was stopped during
this inventory. Foreign keys and end-to-end business workflows remain
outstanding.

Thirty of 62 original routine definitions were present or accepted. Four existing
preview definitions were preserved; the other accepted definitions were installed
with only their destination namespace specified. This is [catalog acceptance](routine-definitions.json),
not execution qualification. The separate date/payment-term evidence qualifies
only its selected calls. No application procedure was invoked by this inventory.
The rejected definitions expose general gaps in schema resolution for %TYPE,
procedural concatenation, parameterized cursors, autonomous transactions,
procedure DDL and other language constructs. A failed definition remains
outstanding rather than being replaced with a stub.

Two passes installed or refreshed views using their unchanged exported query
text inside CREATE OR REPLACE VIEW with an explicit destination namespace.
Inter-view dependencies and the two previously installed views were included.
The final definition pass accepted 128 of 160 definitions. Its first full read
pass observed 96 matching canonical row multisets. A second read pass exposed
unstable current-time outputs, including a match that depended on both queries
running in the same second. The [repeat diagnostics](repeat-read-diagnostics.json)
retain that failure. Nothing is normalized away to turn it into a match.

The conservative [coverage result](view-results.json) counts 90 views whose
complete seed-row multisets matched in both passes and whose definitions do not
explicitly reference clock functions: 57 nonempty and 33 empty results. Eleven
explicit-clock definitions remain unqualified pending controlled time inputs;
this lexical screen is not a full transitive dependency analysis. Empty results
do not establish behavior for nonempty workloads. Two larger views exceeded the
explicit 5,000-row acceptance limit and remain unqualified. Every error,
difference and exceeded limit remains in the report.

Both databases had a 15-second query limit. Row values, connection settings,
credentials and application source bodies remain outside the repository. The
public reports retain object names, source hashes, row counts and canonical
multiset hashes. Rejected view definitions include ANSI character casts and
UNION metadata support; execution failures include unresolved routine
dependencies and routine query parsing. View creation alone is not execution
acceptance.

The BicDB application copy contains earlier application/test writes. A row
mismatch is therefore an investigation item, not by itself proof of an engine
semantic defect. Source/target input drift must be resolved before attributing
those differences. These checks are bounded seed-data evidence, not complete
schema migration, full Oracle compatibility, production validation or a
published release.

After installation, the [existing routine smoke checks](routine-smoke-recheck.json)
and [23 date-function assertions](date-functions-recheck.json) still pass.
These rechecks cover selected GETDATE/CHARAT and payment-term behavior, plus
ADDDAYS, DAYSBETWEEN and FIRSTOF; they do not execute all thirty accepted
routines. No engine source changed in this inventory. The implementation's
1,099 passing Rust tests are recorded with the development binary evidence;
they are separate from these live reference checks.
