# Per-row Oracle SELECT lock-wait deadlines

The preview front end now supports literal nonnegative integer seconds in
`SELECT ... FOR UPDATE WAIT n`. `WAIT 0` lowers to `NOWAIT`; positive values
become a reserved query-local AST setting that survives SQL serialization,
parameter inference and static result metadata. The runtime applies a fresh
bounded timer for each row acquisition. No session timeout GUC is changed.

The core cancellation API adds independently cancelable children with local
deadlines. They retain earlier parent deadlines and cancellation sources while
leaving parents and siblings unchanged. A local lock expiry uses PostgreSQL
SQLSTATE `55P03`; an earlier statement deadline or explicit protocol cancel keeps
`57014`. Existing cycle detection, latest-row refresh and transaction lock
ownership remain in the same acquisition path. No table or application identity
selects this behavior.

The reserved metadata is validated against the durable preview profile before
statement execution and static parameter/result inference, including metadata in
unexecuted subqueries. Native source `WAIT` syntax and native attempts to inject
the preview policy return `42601`. The parser retains PostgreSQL feature switches;
there is no new parser dependency or default native grammar switch. Quoted tokens,
string literals and column locking lists are retained. The ordinary routine
compiler remains 25 and durable metadata format remains 5; full timed PL/SQL
cursor/routine qualification is separate work.

## Independent observations

The [five-case fixture](../../../examples/oracle/reference/row_lock_deadlines.py)
now [matches four cases](oracle-comparison.json): uncontended acquisition,
immediate contention, positive expiry and release followed by the latest row.
The prior [target baseline](oracle-baseline.json) matched zero. A positive
one-second expiry is approximately 1.001 seconds on Oracle 23.26.3.0.0 and 1.004
seconds on this BicDB development binary.

**The full five-case comparison still returns nonzero and `success=false`.**
The fifth source observation reaches its approximately five-second deadline
instead of completing explicit cancellation. It therefore remains unqualified.
The target completes cancellation in approximately 0.301 seconds and qualifies
all five target expectations. The discrepancy is retained, not counted as Oracle
cancellation parity. The PostgreSQL-wire regression independently verifies
prepared Describe without locking, Execute expiry, cancellation, rollback and
reuse of the same connection. Complete Oracle-protocol cancellation behavior is
not claimed.

The separate [multirow fixture](../../../examples/oracle/reference/row_lock_multiwait.py)
[matches 2/2 source cases](multirow-comparison.json), improving the prior
[0/2 target baseline](multirow-baseline.json). Oracle and BicDB both complete two
successive waits under `WAIT 1` in approximately 1.2 seconds. When the second row
stays locked, both expire at approximately 1.601 seconds from statement start.
This distinguishes a per-row budget from one whole-query deadline.

The unchanged native [PostgreSQL controls](postgres-controls.json) match **3/3**
for blocking/latest-row reads, cancellation and a two-row cycle. The
[pending-index Oracle](pending-index-oracle-controls.json) and
[PostgreSQL](pending-index-postgres-controls.json) controls retain **24/24** each;
[Oracle numeric metadata](numeric-oracle-controls.json) and its
[native counterpart](numeric-postgres-controls.json) retain **12/12** each.
All nonce fixture objects were removed, with no cleanup errors.

## Validation and remaining scope

Local validation passes **1,486 Rust test executions**, with three existing
ignores, across 19 batches. This includes repeated core lock tests under default,
wait-graph and key-notification configurations. **58 Python tests**, formatting,
diff, licenses and open-core checks pass. The [validation record](validation.json)
contains the source commit, exact binary digest and guarded memory observations.
Fourteen completed test executables were removed, reclaiming approximately
4.1 GiB; the build cache is approximately 7.5 GiB. Frozen/current binaries and
reusable dependency artifacts were preserved. CI was not run in these local
checks.

A [pre-existing batch-boundary observation](batch-boundary-baseline.json) remains
open. On the prior binary, DDL, INSERT, BEGIN and a locking SELECT in one request
end with an idle owner and a visible unlocked row in both target profiles.
[PostgreSQL 18.4](batch-boundary-postgres-reference.json) instead keeps that batch
in a transaction; another connection cannot yet see the newly created table
(`42P01`). With separate requests, the owner remains in a transaction and another
locking read returns `55P03`. The protocol deadline regression uses separate
requests, as the application's JDBC transaction path does. The batch issue is
not fixed or qualified by this change.

The application must be restarted on this exact binary and its Complete action
retested before claiming order-processing progress beyond the
[saved draft totals](../idempiere-sales-order-totals-2026-11-03/README.md).
Integer-second SELECT waits are a bounded checkpoint. Other wait units and forms,
timed reference cancellation, full routine/cursor coverage, all lock modes,
metadata/table waits, complete statement rollback/isolation, DDL batch semantics,
distributed transactions, and the remaining Oracle programme stay open.
This is development source/binary evidence, not a published release, production
deployment or complete iDempiere qualification.
