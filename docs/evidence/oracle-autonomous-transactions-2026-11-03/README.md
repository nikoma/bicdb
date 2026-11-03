# Independent routine transactions and snapshot boundaries

New Oracle preview compiler version 15 implements standalone
`PRAGMA AUTONOMOUS_TRANSACTION` functions and procedures with a distinct core
transaction. Child COMMIT/ROLLBACK do not commit or roll back the suspended
caller. Committed child changes survive a later error, parent rollback and
restart. Returning with active child work raises ORA-06519 and rolls that work
back. The original Oracle-provider iDempiere application remains incomplete.
This milestone builds a development binary; it does not upgrade the running
application or publish a release.

## Independently checked behavior

[oracle-autonomous-transactions.json](oracle-autonomous-transactions.json)
records **38/38** matches against Oracle 23.26.3.0.0. Separate parent and observer
connections distinguish committed child data from pending parent data. Checks
include nested independent calls, commit followed by an application error,
active-return rollback, zero-row UPDATE/DELETE, savepoints, caught failed INSERT,
locking reads with no matching row, parent-lock deadlocks, SQL-invoked functions,
formal defaults evaluated in the caller, shared diagnostic context and
SERIALIZABLE parent snapshots.

Oracle's SERIALIZABLE snapshot stays fixed while child transactions see their
own current state. The implementation uses a pinned core snapshot for this
bounded contract. It does not enable PostgreSQL SERIALIZABLE or qualify all
Oracle concurrency/serialization-failure behavior. Native PostgreSQL and the
bounded HR profile retain separate validation rules.

Rust checks cover deferred Describe without independent side effects, exact
PG-driver error/rollback boundaries, successive child commits while a parent
write remains pending, lock preservation and reopen. Statements entering an
independent routine cannot be automatically replayed on optimistic retry or
exclusive-access fallback. An owning suspension guard tracks parent/child lock
dependencies without releasing parent writes or locks.

[reader-and-restart.json](reader-and-restart.json) records an older version-14
reader refusing a new version-15 routine with 0A000. The current reader then
executes it once, and its committed child row survives another restart. The
persisted original declaration and compiler identity prevent silent execution
with the old caller-transaction contract.

Run the generated comparison with matching pre-existing dedicated schemas:

```sh
python examples/oracle/reference/autonomous_transactions.py \
  --oracle-config /private/reference.json \
  --output /private/autonomous-transactions.json
```

Set BICDB_PG_DSN privately to the dedicated preview target. The runner creates
and removes uniquely named fixtures and opens separate observer connections.
Private qualification uses disposable target directories and native PostgreSQL
containers. Source application routines are not executed by this generated
fixture.

## Application boundary and remaining work

[application-copy-diagnostics.json](application-copy-diagnostics.json) records
recompilation of original definitions on an isolated imported application copy:
**60/62**, up from 57/62. Newly accepted definitions are NEXTIDFUNC,
REGISTER_MIGRATION_SCRIPT and TORECORDID. TOTABLEOFVARCHAR2 still requires
hierarchical-query/collection support; AD_COLUMN_SYNC still lacks its Oracle
dictionary %TYPE dependency. The existing NULL-input procedure diagnostics
remain **1/13** successful and do not establish valid business workflows.

[original-autonomous-call-diagnostic.json](original-autonomous-call-diagnostic.json)
records a separate **failing** valid-input call with unchanged GET_SYSCONFIG,
NEXTID and NEXTIDFUNC bodies in a generated schema with minimal tables. Oracle
returns 10. BicDB refuses the called NEXTID body at the existing dynamic-SQL
namespace guard with 0A000, including its static system-counter branch. This is
not part of the passing corpus. Definition acceptance does not establish that
the newly accepted routines execute. General namespace binding for dynamic SQL,
supplied package APIs and labelled procedural blocks remain application runtime
dependencies. No application-specific engine exceptions were added.

Child DDL and other operations requiring exclusive database access remain
unsupported because the child owns only shared core access. Independent
PostgreSQL notification delivery is rejected before commit. Packages, full
rights/session-state behavior, distributed child commits, read-only enforcement
and comprehensive commit/recovery failure injection remain programme work.

## Retained evidence and validation

[retained-oracle-controls.json](retained-oracle-controls.json) records
**1,127/1,127** earlier generated Oracle matches on the same candidate, giving
**1,165/1,165** in the passing generated corpus.
[retained-postgres-controls.json](retained-postgres-controls.json) records
**214/214** independent PostgreSQL controls.
[original-sequence-comparisons.json](original-sequence-comparisons.json) retains
**9/9** checks of the unchanged ordinary sequence procedure with minimal
dependencies; these are separate from the generated totals.

[existing-semantic-gap-observations.json](existing-semantic-gap-observations.json)
records the five pre-existing mismatches among seven additional NVL/type/
conversion probes. Their outcomes are unchanged. They and the original
autonomous-call failure above are explicitly outside the passing corpus.

[validation.json](validation.json) records **1,291** passing Rust tests, **3**
existing ignores and Python **25+22** passing checks, source/binary identities,
format/license/boundary checks, monitored resources and cleanup. An initial SQL
run failed its old autonomous-rejection assertion; after updating that contract
and retaining HR rejection, the full affected suite passed. Builds use one job,
no incremental cache and an active 8 GiB minimum available-memory guard. Rust
regression temporary files use task-owned tmpfs; separate reference and restart
fixtures use filesystem-backed directories. All are removed after checks.

Obsolete schema-cache libraries reclaimed about 32 GiB, retaining current
binaries and the newest two library generations per crate. The schema target is
53 GiB. Completed Oracle test harnesses were removed while retaining the current
development binary and immutable compatibility readers; its build cache is
5.8 GiB. [running-application-unchanged.json](running-application-unchanged.json)
verifies the prior live binary remains running; live application query values
are not requalified by this milestone.
