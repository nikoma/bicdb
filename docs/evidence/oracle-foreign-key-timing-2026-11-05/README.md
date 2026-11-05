# Selected foreign-key timing contracts

Oracle-preview lowering preserves foreign-key grammar while continuing to fold
ordinary Oracle identifiers, including columns named `DEFERRED` or `DEFERRABLE`.
Complete `SET CONSTRAINTS` statements bypass the unrelated session-variable
parser and resolve names in the selected schema. Standard
`information_schema.table_constraints` timing flags reflect stored foreign-key
metadata.

Named non-deferrable constraints have separate profile behavior: PostgreSQL
permits an explicit `IMMEDIATE` and rejects `DEFERRED` with SQLSTATE `42809`;
Oracle rejects both mode changes. The durable database profile selects this
rule, independently of mutable session settings. Immediate enforcement remains
active after PostgreSQL's successful no-op mode change.

The [public comparison](../../../examples/oracle/reference/foreign_key_timing.py)
creates nonce tables and checks 13 explicit expectations independently on each
reference and target. It compares PostgreSQL SQLSTATEs; Oracle numeric errors
are recorded without claiming error-number equivalence. See its
[invocation instructions](../../../examples/oracle/reference/README.md).

This checkpoint does not qualify full foreign-key conformance or migrate
application relationships. In particular, Oracle rechecks an explicitly assigned
unchanged foreign-key value on a legacy orphan under `ENABLE NOVALIDATE`;
PostgreSQL skips that check. The Oracle-preview implementation still needs that
assignment-sensitive rule, with a writer compatibility fence. Unrelated-column
updates must continue to tolerate pre-existing orphans.

Application acceptance still requires all 3,826 exported relationships with
their timing, validation state, actions and referenced columns, an orphan
inventory, and original business workflows with enforcement enabled. Composite
and nullable keys, concurrency, recovery and privilege behavior require broader
reference qualification. This is not Oracle certification or production
qualification.
