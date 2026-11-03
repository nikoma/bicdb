# Qualified primary-key updates and profile-specific uniqueness

Primary-key UPDATE previously reconstructed SELECT, DELETE and INSERT SQL.
For non-public schemas, the INSERT named an internal physical collection and
failed the reserved-name guard. That round trip also imposed INSERT/DELETE
privileges and policies on an UPDATE. The shared executor now constructs typed
records and moves them within the UPDATE transaction, with UPDATE grants,
UPDATE USING/WITH CHECK, applicable SELECT visibility and statement rollback.
The reserved-name guard remains enforced for SQL supplied by clients.

The original Oracle-provider iDempiere application remains **incomplete**.
This is a development candidate; the running application database is not
upgraded, and no production release or deployment is established.

## Independently observed uniqueness rules

Oracle 23.26.3.0.0 permits a statement to exchange distinct existing primary or
secondary unique keys when its final rows are unique. PostgreSQL 18.4 immediate
uniqueness rejects such cycles. The Oracle preview database profile validates
against the completed statement. The native PostgreSQL path checks each moved
row against prior moves and remaining old rows, permitting ordered shifts into
keys already vacated by this statement. Profile selection comes from durable
database metadata. The bounded HR profile retains its native contract.

These rules are covered by separate actual Oracle and PostgreSQL controls.
They are not implementations of deferred constraint modes or a qualification of
concurrent key changes. A duplicate final key, duplicate new keys within the
batch, NOT NULL, CHECK, unique and foreign-key errors leave the statement's
original data intact. Explicit caller transaction writes remain recoverable.

## Scope and executable checks

The Oracle comparison covers 41 cases: qualified names, aliases, simultaneous
assignments, defaults, composite keys, exact numeric and timestamp values,
prepared numeric parameters, the uniqueness rules above, errors and rollback.
The independent native PostgreSQL comparison covers 51 cases, including UPDATE
column grants without INSERT/DELETE, denied assignments, RLS checks and SELECT
visibility. Rust regressions also reopen the database, isolate identical table
names in different schemas, preserve non-SQL record attributes and reject
client-supplied internal physical names.

```sh
python examples/oracle/reference/primary_key_updates.py \
  --oracle-config /private/reference.json \
  --output /private/oracle-key-updates.json
python examples/oracle/reference/postgres_primary_key_updates.py \
  --schema reference_schema --output /private/postgres-key-updates.json
```

Set BICDB_PG_DSN privately to the appropriate dedicated BicDB profile. For the
native comparison also set ORACLE_CONTROL_PG_DSN. The same supplied schema must
already exist on source and target. The native role controls require role
management privileges. Runners remove their uniquely named tables, grants and
role, and preserve the externally supplied schema. Private qualification runs
use disposable targets and PostgreSQL containers.

Referenced parent keys, row triggers, UPDATE FROM/RETURNING remain explicitly
unsupported by this move path, preserving its existing capability boundary.
Concurrent moves, deferred constraints, exclusion/partition behavior and broad
Oracle transaction semantics remain unqualified. Column UPDATE controls use
table SELECT permission; general column SELECT grants, schema authorization,
Oracle VPD and definer/invoker security remain programme work. Nested-query
reads retain the table SELECT requirement until their column ACL scopes can be
proved by the planner.

The retained Oracle and PostgreSQL suites are rerun on the same candidate.
Original sequence procedure checks use both minimal dependencies and imported
application data, with source and target changes rolled back. Those are
separate from the generated comparison totals and do not establish complete
application execution. Existing NVL/type/conversion mismatches remain recorded
outside the passing corpus.

## Retained results and application boundary

[oracle-key-updates.json](oracle-key-updates.json) records **41/41** new Oracle
matches, and [postgres-key-updates.json](postgres-key-updates.json) records
**51/51** independent PostgreSQL matches. Errors compare Oracle numbers with the
selected PostgreSQL transport SQLSTATEs; this does not implement or qualify the
full ORA message catalog or all procedural SQLCODE/SQLERRM mappings.
[retained-oracle-controls.json](retained-oracle-controls.json) records
**1,086/1,086** earlier generated cases, for **1,127/1,127** total.
[retained-postgres-controls.json](retained-postgres-controls.json) records
**163/163** earlier native controls, for **214/214** total.

[original-sequence-comparisons.json](original-sequence-comparisons.json) retains
**9/9** minimal-dependency checks, and
[original-application-sequence-comparisons.json](original-application-sequence-comparisons.json)
retains **2/2** valid calls on imported application data. Both source and target
changes are rolled back. [application-copy-diagnostics.json](application-copy-diagnostics.json)
records definition acceptance **57/62** and **1/13** successful NULL-input probes;
those probes are not valid-input workflow acceptance. The five pre-existing
semantic mismatches remain in
[existing-semantic-gap-observations.json](existing-semantic-gap-observations.json).

[qualified-update-before.json](qualified-update-before.json) and
[qualified-update-after.json](qualified-update-after.json) show the original
reserved-name reproduction and successful updates under both profiles.
[validation.json](validation.json) records **1,280** passing Rust tests, **3**
ignored, Python **25+22** passing checks, source/binary identities, monitored
resources and cleanup. [running-application-unchanged.json](running-application-unchanged.json)
independently records the prior application binary; full live query values are
not requalified by this development milestone.
