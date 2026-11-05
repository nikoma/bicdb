# Connection transaction isolation defaults

PostgreSQL clients can set connection defaults with
`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...` or
`SET default_transaction_isolation = ...`. A default applies to subsequent
transactions; it does not replace the mode of an active transaction. An explicit
BEGIN mode overrides that default for one transaction. SHOW and current_setting
report the active mode while a transaction is open and the connection default
outside one. Savepoint rollback, transaction rollback and RESET preserve their
respective setting scopes. COMMIT/ROLLBACK AND CHAIN retain the previous
transaction's mode rather than using a newly changed connection default.

The Oracle preview profile accepts SERIALIZABLE as its existing snapshot
isolation contract. Snapshot UPDATE and DELETE avoid READ COMMITTED row-refresh
paths. An opt-in core check rejects already committed conflicting writes before
buffering them, while commit validation remains mandatory. Native PostgreSQL
SERIALIZABLE still rejects with an unsupported-feature error; this change does
not supply PostgreSQL SSI. Session READ ONLY, deferrability and combined session
characteristics are explicitly unsupported by this bounded implementation.

Simple Query batches retain transaction boundaries even when a setup statement
precedes BEGIN. Independent autonomous transactions report their own mode rather
than copying a parent's active override. The implementation uses transaction
syntax, the selected SQL profile and core snapshot metadata. Application, schema
and table names do not select behavior. Durable compiler/profile metadata and
stored routine language registrations remain unchanged.

The setting scopes follow the
[PostgreSQL transaction-setting reference](https://www.postgresql.org/docs/18/sql-set-transaction.html).
The [Oracle session reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/ALTER-SESSION.html)
describes its READ COMMITTED/SERIALIZABLE defaults. The PostgreSQL transport
command above is not Oracle ALTER SESSION syntax: the Oracle reference fixture
uses ALTER SESSION outside an active transaction, while BicDB clients use the
PostgreSQL driver command. Oracle ALTER SESSION inside a write transaction has
different restrictions and is not qualified here.

## Independent references and limits

The [fixture](../../../examples/oracle/reference/session_isolation.py) qualifies
11 behavior controls against Oracle and 8 against PostgreSQL, including statement
refresh, uncommitted own writes, rollback, repeated transactions, explicit mode
override and independent connections. Oracle-only controls exercise stale-writer
rejection and fixed-snapshot own writes/rollback. The
[baseline](baseline.json) records the previous binary's transport parse failure
before reaching the Oracle controls; all 8 native PostgreSQL controls pass.
The [candidate comparison](comparison.json) pins both binary and fixture hashes.
Each source independently satisfies its expectations. Nonce objects, disposable
target databases and reference containers are removed after the comparison.

The [validation record](validation.json) records focused core/SQL/driver tests,
the broader regressions and memory limits. Driver tests cover changed defaults,
active overrides, savepoints, RESET ALL, chaining and statement-time stale-write
rejection. Retained reference comparisons protect previously qualified semantics.

Broader implicit transaction semantics for batches preceding BEGIN, arbitrary
concurrent schedules, read-only enforcement, complete isolation conformance and
full Oracle ALTER SESSION support remain unqualified. Session setting support
does not establish application completion, production readiness or certification.

## Application gaps that motivated the change

An isolated run of nine existing upstream test classes on the previous runtime
reported [163 passes, 5 failures and 2 errors out of 170 tests](upstream-foundation-baseline.json),
with no skips. Two errors came from the PostgreSQL driver's serializable-default
command. The remaining results concern BOM pricing after an uncommitted change,
binary LOB programs, optimistic timestamp updates, duplicate-error translation
and an Oracle-branch mock expectation. The last classification follows the
upstream source: its Oracle branch returns before invoking the mocked JDBC
method. It is retained as a failed upstream test, not counted as a passing test.

These are selected foundation classes, not the entire upstream suite. This
record preserves the failures and their current assessments; a subsequent
application retest must establish which failures this SQL change actually fixes.

The candidate's subsequent [DBTest retest](upstream-db-test.json) passes all 55
tests with zero failures, errors or skips, including both connection-default
cases. It runs the existing compiled upstream class with the Oracle application
provider through PostgreSQL transport, in a disposable database and a loopback-only
network namespace. After clean shutdown, reconciliation verifies that all 67
selected observations from its clean parent database, 27 business queries and
the original routine/view fingerprints remain unchanged. This copy predates
the primary application's completed project issue; the live 77-observation
database is not used for upstream test mutations. The other foundation classes
have not been rerun by this record, and their remaining failures stay open.
