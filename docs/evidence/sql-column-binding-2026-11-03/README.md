# Input columns and same-named routine values

The fast expression binder no longer captures a procedural value when its name
already denotes an input column but the binder cannot select a unique slot.
A schema-qualified physical relation can expose both a visible alias and a
storage qualifier for the same column. Previously that representation caused
the fast binder to fall back to a same-named local value. It now defers to normal
SQL resolution, which distinguishes physical aliases from actual ambiguity.
Qualified column binding and variables without any matching input column retain
their independent paths. This is a common execution fix with no application,
schema or table-name special case and no compiler/metadata version change.

A failing Oracle regression first reproduced `local` instead of `column` in a
non-public schema. The same public-schema test had passed, so it was strengthened
to cover the actual namespace condition. The fix passes that regression, binder
ambiguity checks and independent native SQL-function tests. Native PL/pgSQL
variable-conflict modes are a separate, unqualified contract.

## Live source comparisons

The [executable fixture](../../../examples/oracle/reference/column_scope.py)
checks projection, filters, explicit qualification, a scalar expression and a
variable with no matching column. The Oracle target uses an existing dedicated
non-public schema. It creates and drops only nonce-named tables and functions;
helper names avoid the reserved internal-function prefix. Query parameters use
prepared PostgreSQL transport, and every explicit expectation is independently
checked against the reference.

The exact development binary matches [5/5 Oracle cases](oracle-comparison.json)
and [5/5 PostgreSQL SQL-function controls](postgres-controls.json). Oracle is
23.26.3.0.0; PostgreSQL is 18.4. The retained prior-binary baselines are
[3/5 Oracle](oracle-baseline.json) and [5/5 PostgreSQL](postgres-baseline.json).
All fixture objects were removed. The [19 Oracle aggregate controls](aggregate-oracle-controls.json)
and [12 native aggregate controls](aggregate-postgres-controls.json) also pass
again on this binary, preserving eager Oracle NVL and conditional native behavior.

Local checks pass **1,436 Rust tests**, with three existing ignores, and **58
Python tests**. Formatting, diff, license and open-core checks pass. CI was not
run in this local qualification. The engine suite and built example are recorded
separately by source identity in [validation.json](validation.json). Optional
local query tracing through the existing logger was also observed at runtime;
it is disabled unless BICDB_COMPAT_QUERY_LOG is set.

## Original application functions

The isolated application database again accepted all **62 original definitions**.
All [four unchanged inventory calls](application-functions.json) now match Oracle:
available 20, on hand 20, reserved 0 and ordered 0 for the selected original
arguments. No routine body was rewritten to obtain those results. This closes
the two 99999-versus-20 value mismatches retained at the aggregate checkpoint.
These are selected calls, not execution qualification for all 62 definitions.

The [saved line](../idempiere-sales-order-line-2026-11-03/README.md) remains partial
application evidence: line values persist, but its parent totals are zero.
Header/tax propagation, processing, inventory effects, invoicing and accounting
remain open. The actual application runs with PostgreSQL transport and the
original Oracle provider; unchanged Oracle-driver protocol support is not added.

A separate [numeric scalar baseline](correlated-numeric-baseline.json) retains
another interface gap: an uncast correlated aggregate returns numeric text that
the PostgreSQL driver tries to decode as an integer. The source expectations
pass on Oracle; that target probe remains unsuccessful. An explicit numeric
cast allowed a [separate diagnostic control](correlated-cast-control.json) to read pending inserts and perform
the correlated update correctly. That control does not qualify the uncast query
or establish the cause of the application's header-total gap.

This checkpoint is source and development-binary qualification, not a published
release, complete application acceptance or production deployment. General
Oracle grouping legality, supplied packages and the remaining programme streams
remain open.
