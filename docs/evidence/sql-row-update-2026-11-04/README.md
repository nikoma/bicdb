# Row-valued UPDATE assignments

UPDATE can assign multiple columns from a row constructor or one subquery:

```sql
UPDATE totals t
SET (amount, quantity) = (
    SELECT SUM(d.amount), SUM(d.quantity)
    FROM detail d WHERE d.parent_id = t.id
);
```

The executor keeps the source row together through evaluation. It does not
expand one query into a separate query for each result column. A missing source
row supplies NULLs; more than one source row raises a cardinality error. Each
assignment reads the original target candidate, including mixed ordinary and
row assignments. Existing constraint validation, triggers and statement rollback
remain in the ordinary UPDATE path. Primary-key row assignments use the existing
key-movement path and its restrictions.

The selected rules follow the
[Oracle UPDATE reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/UPDATE.html)
and [PostgreSQL UPDATE reference](https://www.postgresql.org/docs/18/sql-update.html).
The implementation uses SQL syntax, schema metadata and normal authorization;
no application, schema or table name selects behavior.

Every assigned column requires UPDATE permission. Source reads reuse the existing
read-privilege proof; nested query scopes conservatively require table SELECT
permission. Full correlated column-only SELECT authorization remains unqualified.
Prepared parameter inference follows each target column separately for selected
row constructors and subquery projections. This change leaves existing Oracle
compiler language registrations and durable profile metadata unchanged.

The reference workload also exposed shared predicate/candidate issues. Integer
range locators now decline probes whose representation cannot preserve SQL
ordering, including fractional and out-of-range numeric bounds. Typed binary
numeric comparisons choose a common numeric type instead of rounding the right
operand to the left column's integer type. Candidate cell decoding prefers an
exact column name before its legacy case-insensitive fallback, preserving
distinct quoted names. Flat independent row subqueries use the mutable session
before evaluation so a volatile routine source can advance its sequence once.
Single-column primary keys stored only in the record identifier, including
case-distinct quoted names, use logical key reads rather than an empty metadata
index. The physical lowercase `id` slot retains its existing readable path.
Non-NULL NUMERIC parameters retain their declared numeric type when text or
binary wire values become execution expressions. A driver regression checks
both comparison results and numeric result metadata, rather than only the
rendered parameter spelling. Broader parameter/null typing remains unqualified.
Nested binary values retain value text separately from standalone SQL casts,
including NUMERIC array elements, range bounds and composite fields. Existing
binary array and structured-type protocol matrices protect those paths.

## Independent references

The [fixture](../../../examples/oracle/reference/row_update.py) has 18 controls
per profile: constants, simultaneous swaps, correlated aggregates, zero/multiple
source rows, column-count errors, mixed assignments, primary-key movement and
collision, constraint rollback, distinct quoted columns, prepared parameters,
NULLs, a volatile function source, an unmatched target, caller rollback and an
empty aggregate. Both Oracle 23.26.3.0.0 and PostgreSQL 18.4 independently satisfy
their expectations. The volatile single-target control checks that the query
is evaluated once rather than once per destination column.

The [previous runtime](baseline.json) matches 1/18 controls in each profile.
The [candidate](comparison.json) and retained controls are identified by binary
and fixture hashes. Exact Oracle error numbers are outside this comparison:
Oracle may report ORA-01427 or an optimizer-transformed ORA-30926 for a repeated
source match; the fixture qualifies the cardinality category and unchanged rows.
Nonce objects, disposable target databases and reference containers are removed.

The separate [integer-bound fixture](../../../examples/oracle/reference/integer_bounds.py)
has 18 independent expectations per profile for primary and secondary indexes,
fractional/large bounds, reversed operands and prepared numeric parameters.
The [previous runtime](integer-bounds-baseline.json) matches 6/18 Oracle controls
and 8/18 PostgreSQL controls. Its [candidate comparison](integer-bounds-comparison.json)
records the same fixture and a separately qualified reference for each profile.

The [validation record](validation.json) records SQL library, type-consistency
integration and driver/protocol checks, formatting and memory limits. Six
shared regressions protect source cardinality/original values, authorization and
prepared parameter inference, numeric bounds, quoted candidate cells and volatile
sources independently of application data.

DEFAULT and generated-column row assignments, every constructor/type combination,
subquery shape validation with no matching targets, broader self-referential
statement snapshots, exact error identities and concurrent schedules remain
unqualified. Mutable routine calls in broader correlated/source-table subqueries
also remain unqualified. These checks do not establish full UPDATE, Oracle or application
conformance. Application retry and deployment acceptance are separate evidence.
