# Typed NVL2 and national literals

Oracle preview now lowers NVL2 to an eager selector with the existing NVL
result conversions. The first argument is the null test; the second and third
arguments determine result typing. All argument expressions and result
conversions run once before selection. An unused result can therefore raise a
division or conversion error. Native PostgreSQL CASE controls retain lazy branch
evaluation, and ordinary user-defined nvl2 functions retain native resolution.

The selected character/numeric conversion rules follow
[Oracle's NVL2 reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/NVL2.html).
The independent reference observations below establish the tested evaluation
counts and errors. Bare NULL and empty character constants retain their
separate typing identities. DATE results remove fractional timestamp precision;
TIMESTAMP results preserve the tested precision. Result metadata and date
provenance remain available to enclosing expressions.

Quoted uppercase NVL2 identifies the builtin. Other quoted spellings use normal
function resolution, including a lowercase user function protected by a shared
regression. Token recognition preserves DATE and empty-character identity inside
quoted builtin calls. Aggregate/window modifiers and invalid argument shapes are
rejected before argument evaluation.

## Shared national literal rule

The resolver previously lacked declared type information for N-prefixed
literals, affecting NVL as well as NVL2. Contract 27 gives these literals fixed
national character identity, lowers their runtime payload through the ordinary
text codec, and normalizes the empty literal to NULL. Existing UTF-16 length and
padding rules can then apply. The rule depends on syntax and compiler contract,
with no application, table or schema exceptions.

New routines use compiler contract 27. Contract 26 remains registered with its
original language OID 999425; contract 27 uses 999426. Reopen and catalog checks
protect the boundary. Earlier stored routines retain their previous literal
and builtin rules. Durable profile metadata is unchanged. The experimental HR
profile and its conservative offline scanner remain separate.

## Independent references and local validation

The [fixture](../../../examples/oracle/reference/nvl2.py) has 59 explicit
expectations per profile: result selection and conversion, NULL typing, empty
characters, padded text, national text, DATE/TIMESTAMP values and arithmetic,
quoted builtin calls, CTEs, scalar subqueries, prepared arguments and volatile
calls in SQL, procedural routines and embedded SQL. Shared NVL, national length,
padding and empty-literal controls qualify the general provenance fix.

The [previous binary](baseline.json) matches 13/59 Oracle controls and 59/59
PostgreSQL controls; some pre-existing error and side-effect controls already
match despite missing NVL2 result support. The [candidate](comparison.json)
matches 59/59 against Oracle 23.26.3.0.0 and 59/59 against PostgreSQL 18.4.
Both references independently satisfy their expected outcomes. Nonce objects,
temporary databases and disposable reference containers were removed.

Retained controls pass [20/20 extrema cases](retained-extrema-comparison.json),
[15/15 matched-update MERGE cases](retained-merge-comparison.json),
[19 Oracle / 20 PostgreSQL evaluation cases](retained-evaluation-comparison.json),
[16/16 typed boundaries](retained-typed-comparison.json) and
[13/13 RAW cases](retained-raw-comparison.json).

The [validation record](validation.json) identifies source and binary. All 617
SQL library tests and 364 driver/protocol tests pass, with three existing
benchmark ignores and two external interoperability ignores. Formatting and
whitespace checks pass. Builds use one Cargo job, disabled incremental
compilation and guards requiring 14 GiB available to start with an 8 GiB floor.

This qualifies selected NUMBER, ordinary/national character and DATE/TIMESTAMP
paths. LOBs, floating-point precedence, time zones, arbitrary NLS, exact Oracle
descriptor widths, all syntax/error variants and full NVL2 conformance remain
unqualified. No release or full application qualification follows from these
expression and routine checks; deployment acceptance is recorded separately.

## Subsequent application upgrade

The [clean runtime upgrade](application-upgrade.json) opens a copy of the stopped
current application database without recreating the original schema objects.
All 67 selected workflow observations and 27 settled business queries match
their pre-upgrade values. The 62 routine and 160 view fingerprints are unchanged;
all 11,983 projected view labels and type families still match the source.
All 141 rows of the three saved Trial Balance reports retain their fingerprints.
Their PDFs were not regenerated.

Prepared queries verify the new NVL2 result and unused-branch error behavior,
national literal length and empty-literal selection, numeric primary-key
extrema, and resolution of all stored routine language OIDs. The actual
iDempiere JVM serves HTTP 200 and the browser reaches the GardenAdmin desktop
without a browser script error. This qualifies the recorded upgrade and login;
it does not establish every application workflow or production readiness.
