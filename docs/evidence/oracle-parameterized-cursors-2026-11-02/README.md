# Parameterized cursors and contextual NUMBER arithmetic

New Oracle preview definitions use compiler version 8, language OID 999407.
Bound cursors accept typed positional, named and mixed arguments, and Oracle
formal defaults. Actual arguments are evaluated once at OPEN. Formal values and
types exist only while the query runs; the outer frame is restored before the
loop body and after query errors. Nested cursor declarations retain their own
state. Logical DATE formals discard fractional seconds. Native PostgreSQL keeps
its separate scope/default rules. Versions 1–7 remain readable, and their new
parameterized Oracle declarations are rejected by the version gate.

All 34 [Oracle cursor comparisons](cursor-live-comparison.json) match Oracle Free
23.26.3.0.0 through PostgreSQL transport. The
[public fixture](../../../examples/oracle/reference/routine_cursors.py) covers
OPEN/FETCH/CLOSE, cursor FOR loops, repeated calls, snapshots, outer-variable
shadowing, named/mixed arguments, nested declarations, qualified formals, query
errors, preservation of an already-open cursor and logical DATE conversion.
The 28 [native controls](native-cursor-comparison.json) independently compare
values, type OIDs and SQLSTATEs against PostgreSQL 18.4. The
[public native runner](../../../examples/oracle/reference/postgres_cursors.py)
creates and drops its own uniquely named objects.

The [initial Oracle fixture](initial-cursor-reference.json) incorrectly expected
a formal parameter to be visible in another formal's default, despite an outer
variable with the same name. The [compile diagnostic](invalid-formal-default-reference.json)
is PLS-00227: the formal is not yet denotable. The parser now rejects that rule;
the valid default fixture uses a distinct enclosing variable. The
[initial PostgreSQL expectations](initial-postgres-reference.json) also assumed
Oracle's column precedence. PostgreSQL instead raises 42702 for the ambiguous
cursor parameter/column reference. Both corrected contracts have independent
[source records](cursor-reference.json), [PostgreSQL records](postgres-reference.json)
and Rust regressions. The relevant language rules are described by
[Oracle OPEN](https://docs.oracle.com/en/database/oracle/oracle-database/26/lnpls/OPEN-statement.html)
and [PostgreSQL bound cursors](https://www.postgresql.org/docs/18/plpgsql-cursors.html).

The DATE formal case exposed an arithmetic gap: multiplying a DATE day fraction
back into seconds retained a tiny decimal residue. Version 8 now rounds resolved
NUMBER arithmetic with the measured twenty-base-100 mantissa digits. Native
PostgreSQL and versions 1–7 retain their previous arithmetic behavior. All 218
[arithmetic comparisons](number-live-comparison.json) match, including finite
precision, cancellation, underflow, SQL/procedural overflow and NUMBER infinity
operations. The [public arithmetic fixture](../../../examples/oracle/reference/routine_number_arithmetic.py)
contains separate SQL and procedural functions with independent expected values.

The [boundary probe](number-boundary-reference.json) distinguishes a procedural
NUMBER overflow from SQL overflow. Oracle's NUMBER infinity has DUMP bytes
`255,101` or `0`, and default TO_CHAR returns `~` or `-~`. The reference driver
exposes the corresponding numeric sentinel as +/-1e126; treating this as an
ordinary finite value would conceal the difference. The comparison harness
normalizes that endpoint only for Oracle source results. Native PostgreSQL
finite numbers at those magnitudes remain exact. BicDB uses standard PostgreSQL
NUMERIC infinity transport and Oracle default character forms. Oracle's
[Number API](https://docs.oracle.com/en/database/oracle/oracle-database/26/lncpp/number-class.html)
documents infinity representations. The 128-case
[infinity reference](number-infinity-reference.json) records the tested operation
and context differences; these are not substituted with IEEE rules. Results on
other Oracle versions remain unqualified.

The previous [342 generated Oracle checks](previous-public-routine-rechecks.json)
still pass under compiler version 8, as do the
[62 previous native controls](native-prior-controls.json). The
[older-reader check](reader-compatibility.json) uses a separate disposable
database: the immediately preceding version-7 reader rejects four version-8
calls with 0A000 and remains usable afterward. It never opens the application
database. The storage format is unchanged; routine capability markers protect
new source semantics.

The [original inventory](original-definition-inventory.json) now accepts 43 of
62 unchanged source routine bodies, up from 42. NEXTBUSINESSDAY is newly accepted.
This is catalog acceptance: its [execution probe](new-original-execution-probe.json)
matches only one of three calls. Non-NULL weekday/weekend calls reach native
binary `decode` instead of Oracle DECODE and fail with 42601; matching the NULL
result alone does not qualify that semantic rule. The
[37 previous original calls](previous-original-routine-comparisons.json) and
[21 configuration/baseline calls](previous-configuration-routine-rechecks.json)
still match. Application rows are retained only as counts and digests.

The [validation record](validation.json) retains failed validation attempts and
subsequent checks. One old test expected parameterized declarations to fail; its
replacement verifies named calls and missing-argument rejection. A scalar
row-security init plan also lost a JSONB result type when folding a nested user
function result into a literal. The fix preserves the catalog-resolved declared
type, and the existing session-runtime/transaction regression remains enforced.

These results do not establish full PL/SQL, Oracle diagnostics, refcursors,
scrolling/portal behavior, all cursor attributes, NUMBER input/storage/constraints,
PLS_INTEGER overflow, arbitrary infinity migration, NLS or other Oracle versions.
The remaining 19 original definitions, all 3,826 required foreign keys, complete
views, application startup and business workflows remain migration gates. The
full [compatibility programme](../../oracle-compatibility-programme.md) is active.
