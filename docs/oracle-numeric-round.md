# Oracle-preview scaled numeric ROUND

Oracle-preview SQL evaluates `ROUND(n, scale)` with an exact numeric result.
Oracle documents a
[NUMBER return when the scale is supplied](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ROUND-number.html),
including for floating-point input. This matters for expressions such as:

```sql
SELECT ROUND(SQRT(2), 2) FROM DUAL;
```

The PostgreSQL profile keeps its existing overload boundary: use an explicit
numeric cast for the corresponding two-argument call:

```sql
SELECT ROUND(CAST(SQRT(2) AS numeric), 2);
```

The preview lowerer preserves literal decimal tokens during coercion, avoiding
a conversion through binary floating point for large exact numeric literals.
Finite binary floating-point operands retain their actual binary value during
rounding: a binary64 parameter with the usual decimal spelling `2.675` rounds
to `2.67`, while the exact NUMBER literal `2.675` rounds to `2.68`. The source
Oracle controls use decimal fetching so large reference results do not pass
through Python float.

Metadata describes the scaled numeric result as numeric over the PostgreSQL
protocol. Positive/negative scales, nulls, finite floating-point parameters and
large exact literals have independent native controls. Explicitly quoted local calls in directly parsed application SQL bind to their
declared current schema before canonical lowering and native builtin dispatch;
An unqualified quoted call does not fall through to a same-named stored
function in another schema, and ordinary routine EXECUTE checks remain
authoritative. Unquoted builtin calls retain their own meaning.

The current helper admits precisions from -1000 through 1000 and rejects values
outside that range instead of silently clamping them.

The same precision policy applies when the operand is explicitly cast. Source
types still matter: bare Oracle `CAST(n AS NUMERIC)` and `CAST(n AS DECIMAL)`
default to precision 38 and scale 0, whereas bare `CAST(n AS NUMBER)` preserves
the fractional operand. The session frontend applies these source CAST defaults
before NUMBER becomes the native unconstrained numeric carrier and before
PostgreSQL bind values are rendered. Thus a numeric bind parameter retains its
fractional value. The PostgreSQL profile keeps its unconstrained bare numeric
and decimal casts. [Read-only source observations](evidence/oracle-numeric-cast-defaults-2026-11-06/source-control.json)
record the distinction for `15.193` and its scaled ROUND results.

This source CAST handling covers statements parsed at the session boundary.
Default modifiers in bare NUMERIC/DECIMAL table declarations, extended PL/SQL
source, native internal SQL and all descriptor typmods need separate
qualification; they are not established by these expression controls.

This checkpoint does not qualify datetime ROUND overloads, Oracle's complete
numeric range, arbitrary floating-point boundary equivalence, NLS conversion,
all PL/SQL or extended-grammar quote-provenance contexts, or a full
routine-overloading/namespace storage
model. The underlying routine store still keys by bare name. These remain part
of the broader compatibility programme; SQL-profile opt-in is not Oracle
conformance or production qualification.
