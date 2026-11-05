# Lookup bindings and mutation predicate coverage

Index and primary-key lookups must distinguish a SQL NULL from a column whose
join input is not available yet. Evaluating that missing input as NULL can
incorrectly prove that a query has no matching rows. CASE and COALESCE can also
turn a missing input into a non-NULL value and probe the wrong key.

Lookup execution now establishes that each value expression is bound before
evaluating it. This applies to primary keys, secondary-index prefixes, ranges
and optional trailing filters, including cached locator strategies and the
record-ID fallback used with pending writes. An unavailable optional filter is
left for residual predicate evaluation. Cached strategies retain structural
expressions rather than a previous execution's binding availability.

A mutation may skip its residual predicate only when the lookup could bind all
primary-key equalities. A structural equality alone does not establish that
the candidates came from a complete key. The existing empty scalar-subquery
UPDATE/DELETE regression protects against accidentally applying a mutation to
every row of a matching key prefix.

Repeated equalities on one key column also retain residual evaluation. A lookup
binds one value per column, so a second equality can impose an additional,
contradictory condition. For example, `a=1 AND b=1 AND b=2` cannot change any
rows. Repeating an agreeing condition still permits its one matching mutation.

Scalar queries over base relations can supply keys when their column references
resolve within the query or to available outer bindings. Complex query scopes
conservatively retain residual evaluation. The preliminary NULL shortcut does
not execute scalar queries before the actual lookup; the existing correlated
DELETE regression checks that two outer rows require at most two scalar-query
evaluations. These changes select behavior by SQL structure and runtime
bindings, independently of application or schema names.

## Independent references

The [view-join fixture](../../../examples/oracle/reference/view_pending_joins.py)
defines 23 expectations in each profile. It covers pending unrelated updates,
inserts and deletes; matching-row changes; explicit and comma joins; rollback
and commit; NULLs; changed view sources; scalar queries; COALESCE; and secondary
index CASE, range and trailing-filter expressions. Each reference database
independently satisfies its own expectations.

The separate [mutation fixture](../../../examples/oracle/reference/key_bound_mutations.py)
checks twelve composite-key UPDATE/DELETE cases per profile. Empty scalar
queries, empty aggregates and literal NULLs must not modify rows; a non-NULL
scalar key must modify exactly its matching row. Repeated and conflicting key
conditions are checked separately. Affected counts and complete
row contents are checked before rollback.

The previous runtime matches [13/23 join controls](baseline.json) and
[10/12 mutation controls](mutation-baseline.json) in each profile. The
[join comparison](comparison.json) and [mutation comparison](mutation-comparison.json)
record the final candidate against independently qualified Oracle 23.26.3.0.0
and PostgreSQL 18.4 references.

The comparison records identify the tested binary and fixtures by SHA-256.
Nonce reference objects, temporary target databases and reference containers
are cleaned up. PostgreSQL and Oracle behavior are qualified independently;
the application workload is a separate acceptance layer.

The [validation record](validation.json) includes the full SQL library,
type-consistency, query, scope/authorization and driver suites. Separate Oracle
and PostgreSQL regressions exercise pending view joins and repeated key
conditions. Existing scalar-subquery regressions protect empty-result mutation
semantics and correlated lookup evaluation counts. Four optimization switches
are disabled individually for the view-join controls.

## Application acceptance

The [original BOM test](upstream-bom-test.json) runs the upstream release-13
GetBOMFunctionsTest with its assertions unchanged. It uses the Oracle provider
and original Oracle-schema data on BicDB through PostgreSQL transport, in a
disposable database and loopback-only network namespace. Its product/price
changes roll back; 67 prior workflow observations, 27 settled business queries
and original routine/view fingerprints are preserved. Selected product and
price rows, including update timestamps and users, match the read-only Oracle
reference after rollback. The current primary application is not used for test
mutations. This qualifies one test class, not the full upstream suite.

This checkpoint does not establish full Oracle compatibility, complete
subquery optimization, or full application qualification. Broader procedural,
LOB, locking, error-identity and operational work remains tracked in the
[compatibility programme](../../oracle-compatibility-programme.md).

## Subsequent application upgrade

The [clean runtime upgrade](application-upgrade.json) opens a copy of the stopped
current application database without recreating original objects. All 77
selected workflow observations and 27 settled business queries retain their
pre-upgrade values, including the completed project issue and reversal. The 62
routine and 160 view fingerprints are unchanged; all 11,983 projected view labels
and type families match the source. All 141 rows of three saved Trial Balance
reports retain their fingerprints. Their PDFs were not regenerated.

The actual application JVM serves HTTP 200 and the browser reaches the GardenAdmin
desktop and standard Issue to Project process. This qualifies the recorded
upgrade and navigation; the remaining upstream failures, broader workflows and
full application acceptance remain open.
