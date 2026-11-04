# BETWEEN evaluation across language contexts

`BETWEEN` must preserve both operand types and the evaluation rules of the
language running the expression. SQL evaluates the tested expression again for
the upper comparison; Oracle PL/SQL retains its first value. A false lower
comparison skips the upper comparison and its operands. A NULL lower comparison
still evaluates the upper comparison and combines the results with three-valued
logic. `NOT BETWEEN` negates that combined result.

The evaluator now takes lazy expression callbacks. Routine language supplies
transient execution metadata, independently of the database's SQL profile.
SQL inside a procedural routine returns to SQL evaluation rules. The same
rules cover scalar and tuple SQL expressions, predicates and procedural calls;
no application, schema or table name selects behavior. Existing typed datetime
comparisons remain in use. No persisted object format changes.

The broader reference fixture also exposed two general parsing problems.
`RETURN nextval(...)` treated the beginning of an identifier as `RETURN NEXT`;
keyword parsing now respects identifier boundaries, including Unicode, dollar
signs and underscores. Prepared anonymous blocks passed their dollar-quoted
bodies to the ordinary SQL metadata parser. Inference now treats a single block
as a command with no outer bind parameters or result columns, without running
its body. Execution still parses the procedural body. Additional commands and
malformed trailing clauses are not silently swallowed.

## Reference comparison

The [fixture](../../../examples/oracle/reference/between_evaluation.py) uses
nonce sequences and functions against actual Oracle and PostgreSQL instances.
It observes results and exact sequence advances for SQL, stored procedural
expressions, SQL inside a routine and anonymous blocks. Runtime NULL locals
avoid confusing executor behavior with planner-time constant folding. The
PostgreSQL matrix also checks a tuple expression.

| Prepared-command cases | Previous binary | Candidate |
| --- | ---: | ---: |
| Oracle 23.26.3.0.0 | 0/19 | 19/19 |
| PostgreSQL 18.4 | 0/20 | 20/20 |

The [baseline](baseline.json) records all failures: the counter's plain
`RETURN nextval(...)` was rejected during setup, so these baseline counts do
not isolate evaluation defects. The [candidate comparison](comparison.json)
retains actual source expectations, results, exact sequence counts and binary
and fixture hashes. Every reference expectation is independently satisfied;
nonce objects and disposable containers were removed.

The [retained typed-boundary comparison](retained-typed-comparison.json) now
passes 16/16 in each profile. This closes the two PostgreSQL volatile-expression
mismatches recorded in the earlier [boundary checkpoint](../sql-typed-between-2026-11-04/README.md).
The [retained RAW controls](retained-raw-comparison.json) remain 13/13 in each
profile.

These expression checks do not qualify all optimizer evaluation orders, all
PL/SQL, the complete application or the full compatibility programme.

## Local validation

The [validation record](validation.json) identifies the source and binary.
All 604 SQL tests and 363 driver/protocol tests pass. Three existing benchmark
ignores and two existing external interoperability ignores remain explicit.
Formatting and whitespace checks pass. Builds use one Cargo job, no incremental
compilation and memory guards requiring 14 GiB available to start and preserving
an 8 GiB floor. Local checks do not establish a published release, deployed
application result or production qualification.
