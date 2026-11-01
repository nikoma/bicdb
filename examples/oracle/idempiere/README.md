# iDempiere Oracle application target

iDempiere 13 is the selected larger application for Oracle migration testing.
It is an open-source ERP/CRM/SCM application with Oracle and PostgreSQL database
providers. The [pinned target](target.json) identifies the release-13 source at
`da0730aec42f3e9f5f671e81a70ff71e24d5e534`.

Source inspection found 59 SQL files under `db/oracle` and 1,085 Oracle migration
SQL files under `migration`. These are file counts, not live database object
counts. Database routines cover currency conversion, payment terms, stock and
bill-of-material quantities, document numbering and other business operations.
The full schema and seed must be imported before measuring object counts or
claiming application acceptance.

The [initial lexical inventory](initial-inventory-summary.json) scanned those
59 Oracle SQL files and identified 46 stored-function definitions, nine dynamic
SQL sites, three autonomous-transaction sites and other migration requirements.
These are static source leads, not a complete inventory of the seed or runtime
workload. The migration scripts, seed catalogs and application-generated SQL
still need assessment. No application or migration qualification is claimed.

The target is **selected, not yet running on BicDB**. HR acceptance and the
existing [reference query results](../../../docs/evidence/oracle-reference-2026-11-01/README.md)
do not establish iDempiere compatibility.

The intended acceptance route uses iDempiere's Oracle schema and Oracle SQL over
the PostgreSQL client transport. Running only its existing PostgreSQL backend
would test a different route. Its Oracle provider constructs Oracle JDBC
connections and uses Oracle-specific interfaces, so selecting a PostgreSQL JDBC
URL alone is insufficient. A transport/provider integration must preserve Oracle
SQL selection while implementing PostgreSQL binding and connection behavior.
The external provider preparation and transport probe now implement the explicit
PostgreSQL transport option. The preview's application-schema binding and
Oracle column-lock translation also pass focused tests. The actual product
serves `/webui/` over loopback HTTP, and its Oracle sequence-lock query executes
on BicDB. Session initialization and login remain unqualified: the next observed
startup blocker is Oracle date truncation in the session insert. A served HTML
page does not establish a usable application.

The pinned seed in [seed.json](seed.json) is an Oracle Data Pump export. The
reference installation must include Data Pump; Oracle Free Lite does not. A
full Oracle Free reference imported the seed successfully into an isolated
application user. Do not reuse a Lite data volume with the full image. The
upstream import script recreates its target user and prints connection arguments;
review it before use and keep credentials out of command lines and logs.

The measured seed contains 913 tables, 160 views, 48 functions, 14 procedures and
one type, with 185,292 table rows. All source objects were valid at export. Its
3,811 foreign keys are initially deferred; 2,914 are enabled without validation
of existing rows. Those states must be retained in the target. The March 2026
seed also requires the remaining release-13 migrations for the pinned October
application source. These counts describe this seed, not application completion.

## Reproduce schema/data development

Use a private working directory outside the repository. `prepare_seed.py`
verifies pinned archive and dump lengths and SHA-256 hashes, and reuses only
verified existing files. `export_reference.py` connects as a dedicated application
schema owner, opens a read-only transaction, exports complete logical catalogs
and row files, and hashes each row file. Its observed SCN is diagnostic information,
not a CDC or cutover guarantee. No passwords are emitted in the report.

The reference connection JSON contains `host`, `port`, `service_name`, `user` and
`password_file`; the password is read from that private file. Install python-oracledb
for export and psycopg for target import in a private Python environment.

```sh
python3 examples/oracle/idempiere/prepare_seed.py --directory /private/idempiere-seed
# Import the verified dump into an isolated Oracle schema using Data Pump first.
python3 examples/oracle/idempiere/export_reference.py \
  --oracle-config /private/reference-connection.json --directory /private/idempiere-snapshot
# Start a new BicDB database with --sql-profile oracle-preview-v1.
export PSYCOPG_IMPL=python
export BICDB_PG_DSN='host=127.0.0.1 port=5441 user=bicdb dbname=bicdb'
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase tables --report /private/tables.json
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase data --report /private/data.json
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase verify --report /private/values.json
```

The importer preserves logical column properties, checks and primary/unique
keys. It explicitly records foreign keys, other indexes, types, routines, views
and application execution as outstanding. COPY loads each table atomically.
`--resume` checks the journal's manifest digest, profile, endpoint and completed
prefix, plus completed table columns/counts. The journal is a development resume
mechanism; it does not certify an unmodified target or make import atomic across
all tables. Use the independent `verify` phase to compare complete row multisets,
preserving exact decimal digits, bytes, text whitespace and duplicate rows.
The reports always retain `application_qualified: false` at this stage.

The preview's [documented mappings](../../../docs/oracle-preview-profile.md) are
explicit adaptations. This tool is not a general-purpose Oracle migration tool,
and the application and database still need the acceptance stages below.

## Explicit JDBC transport preparation

`prepare_provider.py --checkout EXTERNAL_CHECKOUT --report PRIVATE_REPORT.json`
checks the pinned application revision and unmodified Oracle provider sources,
then adds the explicit JVM option `-Dbicdb.pgwire=true`. Without that property,
the original Oracle driver and URL construction remain selected. With it, the
Oracle SQL provider uses PostgreSQL JDBC and requests `oracle-preview-v1` and the
quoted application schema through startup parameters. The private report records
source/output hashes. The generated application sources retain upstream licensing;
this repository does not contain the application implementation or binaries.

Rebuild the application after preparation. The resulting provider needs the
PostgreSQL JDBC bundle as well as its existing dependencies. The local Linux
reference build used Java 17, the pinned Maven wrapper, a Linux-only target
configuration, and disabled only the unrelated Windows/macOS product installation
executions. Its default upstream test-skipping configuration is not evidence that
application tests passed. The release-13 product and adapted provider both built
successfully, but full application startup/workflow qualification remains pending.

`TransportProbe.java` is a small independent transport acceptance that must run
against a fresh loopback BicDB preview instance. Compile it with the rebuilt
product's `plugins/*` classpath and run with that classpath, the compiled probe,
`-Dbicdb.pgwire=true`, and the local BicDB port as its single argument. It creates
a dedicated schema/table; do not run it in a populated application schema. It
checks that the Oracle provider remains selected while PostgreSQL JDBC performs
an exact numeric prepared insert, commit and lookup. This probe passed against
the native BicDB development server; it does not establish application acceptance.

The application is maintained as an external checkout under its upstream GPL
license. This directory contains the target definition and test plan, not the
application, its GPL source, proprietary Oracle binaries or a database seed.
Use an exact upstream revision for reproducible testing:

```sh
git clone --filter=blob:none --sparse --branch release-13 \
  https://github.com/idempiere/idempiere.git /tmp/bicdb-idempiere-reference
git -C /tmp/bicdb-idempiere-reference checkout \
  da0730aec42f3e9f5f671e81a70ff71e24d5e534
git -C /tmp/bicdb-idempiere-reference sparse-checkout set \
  db/oracle db/postgresql org.compiere.db.oracle.provider \
  org.compiere.db.postgresql.provider \
  org.adempiere.server-feature/data/seed \
  org.adempiere.server-feature/utils.unix/oracle migration/iD13/oracle
```

Review upstream licensing and installation instructions in that checkout. The
seed is distributed separately; the checked-in `data/seed` directory contains
configuration examples, not an importable complete database.

Acceptance work must cover:

1. A pinned application build and Oracle seed, clean import and migration, no
   invalid objects, and measured schema/row/constraint counts.
2. A source/catalog/runtime inventory with dynamic SQL and driver-specific APIs,
   followed by explicit migration adaptations and independently verified data.
3. BicDB Oracle SQL semantics, the required stored routines, views, security and
   transaction behavior, using the PostgreSQL driver integration.
4. Application login and tenant isolation; customer/product setup; sales orders,
   stock/reservations, shipment, invoice/payment, posting and reconciliation.
5. Concurrent transactions, retries, cancellation and failures; migration resume,
   restart, recovery, backup/restore, cutover and rollback under a measured workload.

Each stage needs executable evidence. Oracle-reference success, BicDB schema
import, application startup and complete business-workflow acceptance are
separate states. This application target also does not replace the remaining
[Oracle compatibility programme](../../../docs/oracle-compatibility-programme.md).
