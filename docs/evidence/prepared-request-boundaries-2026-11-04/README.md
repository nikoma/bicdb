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

The live application still uses the preceding binary at this checkpoint. These
transport and reference results do not imply an application upgrade, full
application acceptance or completion of the Oracle compatibility programme.
