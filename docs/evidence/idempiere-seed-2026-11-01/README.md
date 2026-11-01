# iDempiere seed development evidence, 2026-11-01

The pinned iDempiere 13 Oracle seed was imported through Data Pump into an
isolated Oracle Free 23.26.3.0.0 reference schema, exported through a read-only
transaction and loaded into native BicDB using the experimental
`oracle-preview-v1` profile and a PostgreSQL driver. [summary.json](summary.json)
records measured counts and reconciliation digests.

All 913 table definitions and 185,292 source rows loaded. Independent comparison
of every table's complete row multiset passed, including exact decimal digits,
binary values, whitespace and timezone offsets. Physical Oracle storage was
adapted to native storage, LOB columns to text/bytea, and RAW byte-length checks
to an explicitly typed binary overload. Primary/unique keys and checks were
active during import. Foreign keys and other objects remain separate work.

The verified export preserves timezone offsets explicitly: the Oracle Python
driver's default timestamp-with-timezone fetch returned naive datetimes. Only
the AD_SCHEDULER row-file encoding changed between the initial and corrected
snapshots; its two non-null values both had source offset +00:00. Reconciliation
used the corrected snapshot. Source SCN observation is diagnostic, not CDC.

The import/verification run used psycopg's Python implementation. The binary
implementation crashed while inspecting a large number of table descriptions
on resume; its root cause remains under investigation. This report does not
claim qualification of all driver implementations. PostgreSQL JDBC 42.7.8
separately passed the explicit Oracle-provider transport probe.

The pinned application source and adapted provider built into a Linux server
product with Java 17 and the upstream Maven wrapper. The upstream default
skipped application tests. An actual product startup attempt reached BicDB over
PostgreSQL JDBC but failed resolving unqualified application table names in the
application schema. Startup, login, migrations and business workflows have not
passed. The application's Oracle provider remains selected; its existing
PostgreSQL SQL backend was not substituted.

Application acceptance is **not complete**. Foreign keys (including all 3,811
initially deferred definitions and 2,914 unvalidated existing-row sets), stored
objects, migrations, application execution and operational acceptance remain
outstanding. See the [target and reproduction tools](../../../examples/oracle/idempiere/README.md)
and [full programme](../../oracle-compatibility-programme.md).

Upstream application code, catalogs, seed data, database binaries, credentials
and local filesystem paths are excluded from this evidence directory.
