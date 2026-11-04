# Scoped aggregate typing and conditional evaluation

Oracle NVL now resolves supported aggregate operands using the input query's
metadata, including inside parameterized stored routines. MIN/MAX preserve their
operand's logical type, including Oracle DATE. Metadata inference does not execute
WHERE predicates or substitute an application-specific schema rule.

The common aggregate evaluators prepare aggregate inputs before evaluating the
conditional scalar expression. COALESCE skips unused scalar fallbacks; CASE does
likewise for unselected scalar arms. Aggregate inputs in those arms still execute.
A nested scalar subquery has its own evaluation scope and is not executed merely
because its unselected parent expression contains an aggregate. Oracle NVL retains
its independent eager argument evaluation and conversion behavior.

The implementation caches typed values for original aggregate AST nodes within
a synchronous expression/group scope. It preserves AST identity and logical type
provenance, distinguishes nested queries, releases cached values on return or
error, and observes the query memory budget. This is bounded execution coverage,
not a claim to implement every optimizer evaluation-order rule. Compiler version
25, language OID 999424 and durable metadata format 5 are unchanged; existing
compiler-version fences remain in place.

## Measured results

The [executable fixture](../../../examples/oracle/reference/aggregate_conditionals.py)
passes **19/19 Oracle comparisons** and **12/12 independent PostgreSQL controls**
on the exact development binary identified in [validation.json](validation.json).
Every explicit expectation was checked against the actual reference database:
Oracle 23.26.3.0.0 and PostgreSQL 18.4. Query results use prepared PostgreSQL
transport; setup DDL uses simple queries. All nonce-named helper objects were
removed. Values and selected error codes are qualified; complete error identity
and every result metadata combination are not.

The same fixture on the prior binary matched [7/19 Oracle cases](oracle-baseline.json)
and [9/12 PostgreSQL cases](postgres-baseline.json). Those failing baselines are
retained rather than replaced by the passing reports. Regression tests independently
protect eager Oracle NVL, native conditional evaluation, aggregate execution counts,
subquery boundaries and cache lifetime. Local checks pass **1,434 Rust tests**,
with three existing ignores, and **58 Python tests**. Formatting, diff, license
and open-core checks pass. CI was not run in this local qualification. Memory
guards retained an 8 GiB host headroom floor throughout checks and the build.

## Application evidence and remaining blockers

All **62 original Oracle routine definitions** were again accepted on an isolated
application database, and the actual application reached its desktop. Definition
acceptance does not prove correct execution of those routines.

The [four unchanged inventory calls](application-functions.json) now execute
without the previous aggregate NVL errors, but only **2/4 match Oracle**. Available
and on-hand quantity return 99999 instead of 20; reserved and ordered quantities
match zero. These values do not qualify stock behavior or the complete application.

A separate [generic shadowing reproduction](sql-local-shadowing-baseline.json)
uses a table column and a local variable both named `label`. Oracle's embedded
`SELECT label INTO label` reads the column (`column`); the target reads the local
value (`local`). Direct product flags agree in both databases. The general SQL
column/local-variable name-resolution rule is the next implementation dependency;
no application-specific function or table handling has been added.

An initial fixture draft also exposed a separate grouping legality gap: Oracle
rejects a scalar subquery mixed with an ungrouped aggregate in that SELECT shape
with ORA-00937. The public conditional-evaluation case uses an explicit GROUP BY
so its SQL is valid on both references. General Oracle grouping legality remains
open; passing the conditional cases does not qualify that rule.

Saved order lines, order processing, accounting, recovery qualification and the
remaining compatibility programme are still open. The six GET_LINES acceptance
cases remain missing. This checkpoint is merged-source/development-binary evidence,
not a published release, complete application qualification or production deployment.
