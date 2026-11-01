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
That integration remains outstanding.

The upstream Oracle import script uses `impdp` and a Data Pump directory. The
installed Oracle Free Lite reference does not include Data Pump. Bootstrap a
separate full reference image or another appropriate Oracle installation before
performing the upstream seed import. Do not reuse a Lite data volume with the
full image. The upstream import recreates its target database user, so run it
only in a dedicated application reference environment after reviewing it.

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
