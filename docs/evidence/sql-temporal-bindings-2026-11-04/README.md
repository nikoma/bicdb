# Temporal parameter and mutation semantics

This checkpoint addresses temporal snapshot comparisons over PostgreSQL transport.
A timestamp fetched by a client and rebound as a mutation guard must identify the
same stored value. Equivalent text spellings must not cause an optimistic update
to affect zero rows, and stale snapshots must not update or delete newer data.

## Implementation

Resolved built-in temporal parameter types survive text, binary and NULL Bind
messages, including temporal arrays. SQL casts are applied at the decoded
parameter boundary, outside nested array/range/composite value representations.
Unspecified parameter types continue to be inferred by the SQL context.

A shared temporal comparator canonicalizes date/time values before comparison.
Datetime operands retain the more precise common datetime type and use the
session timezone when promoting naive timestamps to timestamps with time zone.
Mutation predicates, row filters, scalar expressions and projected expressions
use the same rules. Projected binary expressions preserve the operand types
resolved from their row context. The bound evaluator defers temporal comparisons
with unresolved nonliteral operands to that context; numeric adaptive lowering
does not replace typed temporal comparison with numeric comparison.

The implementation contains no application, tenant or schema name branches.

## Qualification

The candidate satisfies 21/21 independently qualified Oracle controls and 21/21
PostgreSQL controls. The delivered baseline satisfied 12/21 and 8/21 respectively.
The same candidate retains all twelve independent reference families, including
23/23 join and 12/12 mutation-key controls in each profile. A stock PostgreSQL JDBC
42.7.8 probe confirms that a fetched timestamp permits the matching optimistic
update to affect one row.

SQL validation passes 632 unit tests, seven type-consistency tests, 42 query/write
regressions, 17 scope/authorization regressions and 14 temporal integration tests.
Three existing unit benchmarks are ignored. All 370 PostgreSQL-driver/protocol
tests pass; two existing external-reference tests are ignored. Formatting and
the open-core dependency boundary checks pass.

The unchanged original Oracle-provider application tests pass 52/53 persistence
tests and 1/1 BOM test on a disposable copy. The original optimistic-locking test
passes. The remaining persistence failure is `testValidateUniqueIndex`: duplicate
writes are rejected, but Oracle diagnostic identity/constraint-name translation
is not yet sufficient for the application's translated message lookup. This
failure is retained rather than represented as a passing class. Post-test checks
preserve all 67 prior workflow observations on that isolated copy, 27 settled
business queries and original object fingerprints. Seven price and four product
seed rows, including flags, timestamps and users, match the read-only Oracle
reference after rollback. The main application's separate 77-check baseline
also passes before upgrade.

[Validation](validation.json), [baseline controls](baseline-reference.json) and
[candidate controls](candidate-reference.json) record exact counts, source/file
fingerprints and the candidate binary SHA-256. These are bounded compatibility
results; the full application is not qualified.

The executable reference is
[`temporal_mutations.py`](../../../examples/oracle/reference/temporal_mutations.py).
It uses disposable neutral tables and independently checks Oracle 23.26.3 and
PostgreSQL 18.4. Its controls include fresh/stale UPDATE and DELETE guards,
fractional precision, reversed operands, NULLs, text/binary/unspecified temporal
parameters, Oracle DATE locking columns, timezone promotion in projection and
filtering, and caller rollback. Oracle DATE is compared with native PostgreSQL
TIMESTAMP(0), reflecting Oracle DATE's time-of-day component.

## Limits

This checkpoint does not establish full Oracle datetime or NLS conformance,
custom temporal domain transport, all cross-type operators, native Oracle-driver
support, or qualification of the complete application. The broader compatibility
and migration programme remains active. Existing LOB submission/locator and
Oracle error-translation gaps remain separate workstreams.

## Reproduction

Run the focused profile and wire tests with:

```sh
cargo test --locked -p bicdb-sql --test temporal_mutation_predicates
cargo test --locked -p bicdb-pgwire --test protocol temporal_bindings_keep_declared_types
```

For independent comparisons, configure `BICDB_PG_DSN` for a disposable target in
the matching SQL profile. Set `ORACLE_CONTROL_PG_DSN` for a PostgreSQL reference,
or provide a dedicated non-administrative Oracle reference configuration using
the existing [reference harness](../../../examples/oracle/reference/row_lock_waits.py)
password-file contract. Each invocation creates, rolls back and drops its own
neutral table:

```sh
python examples/oracle/reference/temporal_mutations.py --reference postgres --output temporal-postgres.json
python examples/oracle/reference/temporal_mutations.py --reference oracle --oracle-config reference.json --output temporal-oracle.json
```

Use bounded build concurrency and monitor available memory on shared hosts.
