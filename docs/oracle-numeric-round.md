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

This checkpoint does not qualify datetime ROUND overloads, Oracle's complete
numeric range, arbitrary floating-point boundary equivalence, NLS conversion,
all PL/SQL or extended-grammar quote-provenance contexts, or a full
routine-overloading/namespace storage
model. The underlying routine store still keys by bare name. These remain part
of the broader compatibility programme; SQL-profile opt-in is not Oracle
conformance or production qualification.
