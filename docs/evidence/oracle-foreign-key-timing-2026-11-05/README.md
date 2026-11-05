# Selected foreign-key timing contracts

The [assignment follow-up](../oracle-foreign-key-assignments-2026-11-05/README.md)
resolves the Oracle mismatch recorded below and adds actual old-server rejection
evidence. This page retains the earlier candidate results.

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
PostgreSQL skips that check. At this checkpoint, the Oracle-preview implementation still needed that
assignment-sensitive rule, with a writer compatibility fence. Unrelated-column
updates must continue to tolerate pre-existing orphans.

Application acceptance still requires all 3,826 exported relationships with
their timing, validation state, actions and referenced columns, an orphan
inventory, and original business workflows with enforcement enabled. Composite
and nullable keys, concurrency, recovery and privilege behavior require broader
reference qualification. This is not Oracle certification or production
qualification.

## Recorded validation

The merged-source [reference results](reference-results.json) record Oracle 23.26.3 at 12/13
matching cases and PostgreSQL 18.4 at 13/13. Every reference case independently
satisfied its expected outcome. All nonce objects were cleaned up, both native
comparison servers stopped, and the disposable PostgreSQL container was removed.
The sole mismatch is `novalidate_unchanged_key` in Oracle mode.

The candidate SQL library passed 693 tests with three ignored. Integration
checks passed 20 deferred-key, two unchanged-key, eight authorization, four
rename and one RLS-depth tests. Protocol checks passed 181 PostgreSQL and
48 Oracle-profile tests. Workspace/all-target checks and the candidate server
build also passed. These counts describe this candidate, not all Oracle features
or the accepted application deployment.

After merging the latest upstream source, the SQL library passed 695 tests
with three ignored. The full integration/protocol/workspace/build batch passed
again, including the upstream routine-expression stack regression. The
[merged-source validation](validation-results.json) identifies the tested source
commit and frozen binary digest separately from the delivery commit.
