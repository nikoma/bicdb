# iDempiere Oracle application target

iDempiere 13 is the selected larger application for Oracle migration testing.
It is an open-source ERP/CRM/SCM application with Oracle and PostgreSQL database
providers. The [pinned target](target.json) identifies the release-13 source at
`da0730aec42f3e9f5f671e81a70ff71e24d5e534`.

The current local development checkpoint loads 62 original routines and verifies
160 original view definitions/type families. The selected
[sales workflow](../../../docs/evidence/idempiere-bank-reconciliation-2026-11-04/README.md)
completes through paid invoice, receipt allocation and bank reconciliation.
[Purchase order](../../../docs/evidence/idempiere-purchase-order-2026-11-04/README.md)
and [material receipt](../../../docs/evidence/idempiere-material-receipt-2026-11-04/README.md)
workflows also complete, including stock movement, purchase matching and receipt
accounting. The
[supplier invoice](../../../docs/evidence/idempiere-supplier-invoice-2026-11-04/README.md)
also completes/posts with receipt and purchase matching and balanced accounting.
[Supplier payment](../../../docs/evidence/idempiere-supplier-payment-2026-11-04/README.md)
and [bank reconciliation](../../../docs/evidence/idempiere-supplier-reconciliation-2026-11-04/README.md)
also complete, including allocation, zero open balances and balanced accounting.
The selected
[inventory transfer](../../../docs/evidence/idempiere-inventory-transfer-2026-11-04/README.md)
also completes/posts with paired stock movements, conserved quantity and balanced
accounting. Its
[Reverse Correct](../../../docs/evidence/idempiere-inventory-reversal-2026-11-04/README.md)
also completes/posts, restores stock and offsets accounting in each locator.
The selected [physical inventory adjustment and reversal](../../../docs/evidence/idempiere-physical-inventory-2026-11-04/README.md)
and [single-product production and reversal](../../../docs/evidence/idempiere-production-2026-11-04/README.md)
also complete through the original UI. The subsequent
[clean row UPDATE runtime upgrade](../../../docs/evidence/sql-row-update-2026-11-04/README.md#subsequent-application-upgrade)
preserves all 67 prior observations and reaches the browser desktop.
The selected [project inventory issue and reversal](../../../docs/evidence/idempiere-project-issue-2026-11-04/README.md)
now completes/posts through the standard UI. All 77 selected observations pass,
including the prior 67, with original routine/view fingerprints and 141 saved
report rows preserved. Stock totals and project balance are restored; accounting
offsets and average-cost rounding match the recorded upstream rules.
The rest of the application, broader production operations and complete Oracle
compatibility remain unqualified. These observations count assertions, not
application workflows or a percentage of compatibility.
The earlier inventory and initial observations below retain their historical
results; they do not replace this current checkpoint.

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

**Application qualification is incomplete.** HR acceptance and the
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
on BicDB. Session initialization and credential lookup now execute, and the actual UI
accepts the public seed credentials and reaches desktop initialization. The
observed desktop blocker is the missing `AD_UserDef_Form` table from an unapplied
release migration. Full desktop startup and business workflows remain unqualified. A served HTML page or an
accepted password does not establish a usable application.

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

A separate Oracle copy has now applied all 24 migrations missing from the pinned
seed's ledger, including five older than its latest recorded migration. It has
915 tables and 185,471 rows, with no invalid objects. A separate native BicDB
instance imported and reconciled every table/row multiset from that upgraded
snapshot. This does not qualify migration SQL execution on BicDB, its remaining
schema objects or application workflows. The original seed is preserved separately.

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
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase indexes --report /private/indexes.json
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

For a legacy import made before durable temporal provenance, take a stopped-writer
backup before upgrading the server, then reconcile explicit source declarations:

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase types --report /private/types.json
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase verify --report /private/values-after-types.json
```

The `types` phase filters the source catalog to exported base tables, validates
physical storage and complete target row multisets before and after conversion,
and journals each reconciled DATE/TIMESTAMP
column. `--resume` verifies its source snapshot fingerprint and completed logical
declarations. It does not infer DATE from timestamp(0), rewrite view projections,
or replace the full source value comparison. Target row-count differences from
the source export are reported separately; this phase can reconcile an existing
application database after application writes without discarding its data. Run
it without concurrent target DML or DDL. Resume also rejects changes to a
previously fingerprinted table. This phase activates profile metadata
format 2; the pre-upgrade backup is needed to return to an older binary.

For legacy CHAR/VARCHAR2 columns, qualify the source character set and SQL
string size explicitly, stop target writers and schema changes, and retain a
pre-upgrade backup. The preview currently accepts AL32UTF8 with STANDARD limits.
The source catalog must retain CHAR_USED, CHAR_LENGTH and DATA_LENGTH; native
VARCHAR/BPCHAR metadata cannot recover the lost length unit.

```sh
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase character-types \
  --source-character-set AL32UTF8 --source-string-size STANDARD \
  --check-only --report /private/character-preflight.json
python3 examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/idempiere-snapshot --phase character-types \
  --source-character-set AL32UTF8 --source-string-size STANDARD \
  --report /private/character-types.json
```

`--check-only` uses no probe DDL or ALTER. Before any mutation, the apply phase
also checks all affected base tables: physical types, known logical declarations,
complete row multisets, byte capacity and fixed padding. It refuses source/type
mismatches, oversized values, missing fixed padding and conversions that would
change saved values. Counts that differ from the source export are reported
separately, preserving valid target application writes.

Apply uses an ordinary source-declared ALTER and a transaction per table, with
complete before/after fingerprints checked before commit. Compatible provenance
attachment retains physical descriptors and dependent-view results. This is a
migration transaction contract; Oracle DDL implicit commits and general view
invalidation remain separate qualification. New provenance requires profile
metadata format 3. A capability probe may advance that format even if a later
apply transaction rolls back, and previously completed tables remain committed.
The pre-upgrade backup is needed to return to an older reader.

Use `--resume` with the same apply journal and endpoint. Resume validates the
source manifest, environment, completed table prefix, logical units and current
row fingerprints before proceeding. A check-only report is a separate journal,
and cannot be resumed as an apply report. Changes to a previously fingerprinted
table cause refusal; the tool never overwrites them to match its journal.
National table columns, configurable NLS/EXTENDED strings, casts, routine BYTE
capacity and full application execution remain separate workstreams. See the
[reconciliation evidence](../../../docs/evidence/oracle-character-reconciliation-2026-11-03/README.md).

The `indexes` phase separates constraint-backed indexes from additional logical
indexes and Oracle's internal LOB storage indexes. The measured seed has 267
additional logical indexes and 48 LOB storage indexes. It requires a ready, valid
native backing index before journaling completion; a metadata-only catalog entry
is insufficient. Unsupported expressions and nullable composite uniqueness are
reported as failures, and the phase remains incomplete while any are outstanding.
The index journal resumes an ordered subset because independent supported indexes
can be installed while another index is unsupported. Its format and recorded
index definitions/states are checked on resume. A negative control first rejects
older servers that incorrectly report placeholders as valid. The DDL-oriented
phase disables automatic prepared-statement caching.

The [initial index run](../../../docs/evidence/idempiere-indexes-2026-11-01/README.md)
verified 237 executable indexes. The
[nullable-index follow-up](../../../docs/evidence/idempiere-nullable-indexes-2026-11-01/README.md)
verified 266 of 267 against the original seed snapshot. Datetime expression
indexing remains unsupported in that run. The application dictionary
query returned the same 49 rows as Oracle using the original composite index,
without the temporary diagnostic lookup index.

[Oracle composite unique keys](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/constraint.html)
permit duplicate all-NULL keys but reject duplicate partially NULL keys with the
same non-NULL components. The importer models this with `NULLS NOT DISTINCT` and,
where necessary, a predicate excluding all-NULL keys. It first probes actual
duplicate rejection; syntax acceptance alone does not qualify that behavior.
The preview now passes this capability probe and the original snapshot's 24
nullable composite unique indexes were installed with that policy. New preview
UNIQUE constraints also preserve these NULL rules. The table/data importer checks
nullable composite constraint metadata and executable backing indexes, including
on resume. Older imports with ordinary PostgreSQL NULL semantics are rejected;
reopening does not silently change them. The
[constraint follow-up](../../../docs/evidence/idempiere-unique-constraints-2026-11-01/README.md)
records explicit replacement in the development application fixture. Foreign-key
deferment/validation and complete constraint equivalence remain unqualified.

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

For input-only Oracle anonymous blocks, build the
[reference JDBC adapter](../jdbc/README.md) and add `--jdbc-blocks` to the
preparation command. This selects `jdbc:bicdb:oracle:` and adds the adapter's
OSGi dependency while retaining the Oracle SQL provider and explicit profile.
Install the adapter alongside PostgreSQL JDBC 42.7.8. Build/install the changed
provider with a fresh OSGi bundle version: replacing a JAR while retaining an
already resolved version can leave the old provider class active. Verify actual
class origins when qualifying an installation.

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
an exact numeric prepared insert, commit and lookup. With the adapter installed,
add `-Dbicdb.jdbc.blocks=true` to also check an exact numeric anonymous input
block. This probe passed against
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

For legacy BLOB columns, the explicit `blob-types` phase supports read-only
preflight, target-value fingerprints and resumable conversion. See the
[BLOB migration guide](../../../docs/oracle-blob-migration.md) for commands,
writer-quiescence requirements and limits.


The [foreign-key migration guide](../../../docs/oracle-foreign-key-migration.md)
documents read-only preflight, quiesced apply, exact source state matching and
resumable target-value verification through `--phase foreign-keys`. Successful
relationship installation remains separate from orphan inventory, source value
equality and application qualification.
