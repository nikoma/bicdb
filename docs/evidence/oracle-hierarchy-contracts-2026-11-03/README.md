# Hierarchy contract inventory

The public fixture establishes 13 executable Oracle hierarchy contracts before
the hierarchy implementation. They cover depth-first sibling ordering, subtree
root levels, both PRIOR directions, post-traversal WHERE filtering, recursive
predicate pruning, cycle errors, NOCYCLE flags and level generators.

All 13 source expectations qualify on Oracle. BicDB matches **0/13**, returning
an explicit unsupported-feature error for every case. The fixture exits nonzero;
this checkpoint does not implement hierarchy or advance the 61/62 routine
definition baseline.

```sh
python examples/oracle/reference/hierarchical_queries.py \
  --oracle-config /private/reference.json --output /private/hierarchy.json
```

Set BICDB_PG_DSN privately to an Oracle-preview target. Both databases need the
same existing dedicated fixture schema. The fixture creates and removes one
randomly named seven-row table; it invokes no application routines.

Oracle returns the DUAL root at LEVEL 1 even when the CONNECT BY upper bound is
zero, negative or NULL. START WITH can suppress that root. Two generator source
rows produce 2, 4 and 8 rows at levels 1, 2 and 3. Replacing the query with a
single generate_series call would change these results. A WHERE predicate that
removes a parent retains qualifying descendants and their original levels;
putting the same restriction into recursive expansion would also be incorrect.
NOCYCLE marks the parent whose next edge would revisit an ancestor and prunes
that edge; the ordinary traversal raises ORA-01436.

The native engine already has recursive-CTE acceptance tests. A hierarchy
implementation must preserve base-row identity, parent/child expression scopes,
root initialization, traversal order and cycle state, then apply output filters
at the correct stage. Joins, path functions, broader pseudocolumn behavior,
query limits and cancellation need additional contracts. These 13 cases are an
initial acceptance set, not complete hierarchy conformance. Collection methods
and regular-expression behavior remain separate requirements for the remaining
application routine.
