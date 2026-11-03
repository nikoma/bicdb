# Preview hierarchy execution

The development candidate matches **19/19 Oracle reference cases** through
PostgreSQL drivers. This extends the earlier
[contract inventory](../oracle-hierarchy-contracts-2026-11-03/README.md)
with nested query scopes and wildcard projections.

The implementation materializes source rows before traversal, evaluates START
WITH roots at LEVEL 1, traverses depth first and applies output WHERE predicates
after expansion. PRIOR expression values identify cycles. NOCYCLE prunes cyclic
edges and marks their parents; ordinary traversal reports ORA-01436 with the
Oracle error stack carried over PostgreSQL transport. ORDER SIBLINGS BY preserves
tree order. Bounded generators retain an unexpanded root for nonpositive or
NULL upper bounds. Volatile SYS_GUID() keys are evaluated independently.

Nested hierarchy queries bind their own pseudocolumns. Wildcards expand against
source columns before private traversal state is added. Explicit numeric casts
preserve pseudocolumn types in stored definitions and empty results. An actual
saved-view reopen returns PostgreSQL int8, OID 20; a stored routine's cursor over
a hierarchy also executes after reopening.

Persisting a hierarchy view or routine advances the preview database marker to
format 4. It retains the earlier temporal and character capabilities. The prior
reader can open format 3 but refuses format 4; it also refuses a database written
by the new server with actual saved hierarchy definitions. Native PostgreSQL
databases reject CONNECT BY rather than silently ignoring its clauses.

Reproduce the comparison using a dedicated Oracle fixture schema and a matching
Oracle-preview target schema:

```sh
python examples/oracle/reference/hierarchical_queries.py \
  --oracle-config /private/reference.json --output /private/hierarchy.json
```

Set BICDB_PG_DSN privately. The target has a ten-second statement timeout and the
Oracle connection has a ten-second call timeout. The fixture creates and removes its own small table.
The [validation report](validation.json) records the candidate hash, individual
results and reader checks. The candidate is a development build.

Validation passes 1,346 Rust tests, with three existing ignores, and 58 Python
tests. All 46 retained reference-report families pass on the same candidate,
including separate native PostgreSQL controls, character buffers, reader
interoperability and migration checks. Formatting, whitespace, license and
open-core dependency checks pass. Builds use one compiler with incremental
compilation disabled; resource guards retain at least 8 GiB of available RAM.

This is an initial hierarchy subset. Path/root/leaf functions, broader join
placement rules, nondefault collations, sequence projections and the complete
Oracle hierarchy grammar remain unqualified. Traversal checks cancellation and
the configured aggregate-scan node budget; this is not a distributed or streaming
hierarchy executor. Collection methods and Oracle regular-expression semantics
remain separate requirements. The original routine definition baseline remains
61/62, with DETERMINISTIC now the first rejected declaration in the remaining
definition. Full schema constraints, application workflows and operational
qualification remain open.
