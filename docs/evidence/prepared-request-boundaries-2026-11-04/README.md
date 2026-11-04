# One statement per prepared request

A PostgreSQL-driver qualification probe found that `execute(..., prepare=True)`
accepted `SELECT 1; SELECT 2` and returned the second result. A batch containing
an INSERT could also execute instead of being rejected. PostgreSQL 18.4 rejects
these prepared requests with SQLSTATE 42601 before execution. Simple Query
messages remain allowed to contain batches.

The Parse handler now rejects multiple top-level statements after SQL-profile
lowering and before parameter inference or statement registration. Applying the
check after lowering preserves Oracle routine and anonymous-block bodies in
their protected dollar-quoted representation. Existing statement splitting
preserves quoted semicolons, dollar-quoted bodies and trailing empty statements.
Parse errors now mark an active protocol transaction failed, so Sync restores
protocol progress while the transaction requires rollback, matching PostgreSQL.
No application, schema or table name selects this behavior.

The [reference fixture](../../../examples/oracle/reference/prepared_request_boundaries.py)
uses psycopg's PostgreSQL extended protocol against actual PostgreSQL 18.4 and
each BicDB profile independently. It covers batch rejection, absence of rejected
writes, literal/comment semicolons, routine creation and execution, failed
transaction state, rollback recovery and retained Simple Query batching.
The Oracle-preview target uses an original-style Oracle routine declaration;
the PostgreSQL reference uses its native declaration of the equivalent body.
This qualifies PostgreSQL transport behavior for both profiles, not Oracle Net
or Oracle-driver compatibility.

The [old binary](old-comparison.json) passed 6/11 cases in each profile. The
[candidate](candidate-comparison.json) passes 11/11 in each, with the source
qualified for every case and disposable object/container cleanup complete.

The source is `7fccde8d2a9d21a78bf372092c5e213a14ebcfeb`. Candidate SHA-256:
`553af9735efdd884a90b74f4cbab1182ea71fef6418913e0ff9d5c2dd13b4502`.
The retained [Oracle](retained-set-oracle.json) and
[PostgreSQL](retained-set-postgres.json) set-operand fixtures each pass 14/14
against their actual references on the candidate. These cover original-view
prerequisites including typed NVL/defaults, aggregates and logical DATE values.
There is no routine compiler, durable-format or language-marker change. A driver
regression separately prepares the original-style Oracle declaration and checks
its internal semicolons and execution, alongside the native PostgreSQL path.

The [local validation](validation.json) passes all 363 PostgreSQL-wire tests
with 2 existing environment-gated tests ignored, including all 36 Oracle-driver
tests. Formatting and the 30 Python migration tests also pass.

## Subsequent application upgrade and clean restart

The [upgrade observations](application-upgrade.json) now record a completed
local application upgrade to the candidate above. The existing JVM was stopped
with SIGTERM (exit 143), and the database then exited cleanly with code zero.
Only after both stopped was the current database copied to a separate directory.
The copied state includes both settled sales and purchasing flows; no older
shipment-only or unpaid-invoice database was substituted.

The upgraded database preserves 27 captured document, ledger and stock checks,
including both paid invoices, both reconciled payments, allocations, matching,
bank statements and their accounting. The repeatable workflow verifier passes
25/25 before and after the restart. Stock remains 21 at HQ and the bank balance
87.49. Fingerprints match for all 62 stored routine bodies/kinds and all 160 view
definitions. No routine or view was recreated to repair the copied database.
All 160 persisted views retain their source column names and Oracle type families
through named Parse/Describe; this does not qualify all view values or typmods.

A fresh browser login reached the admin desktop and
[Inventory Move](upgraded-navigation.png) on the upgraded runtime, without
browser page errors. That screen contains an unsaved new record; navigation is
not a completed inventory movement. Background processors were retained.

This is a clean local stop/copy/reopen and application-login check, not a crash,
backup/restore or disaster-recovery drill. Full application acceptance, other
business rules/modules and the full Oracle programme remain open.
