# Public source-faithful foreign-key migration phase

The [public guide](../../oracle-foreign-key-migration.md) documents
`import_bicdb.py --phase foreign-keys`, read-only preflight, quiesced apply and
resume. Explicit source declarations determine parent keys, ordered columns,
delete actions, deferrability, initial timing and validation state. A successful
schema phase remains separate from source data equality, orphan inventory and
application qualification.

## Verified behavior

All **12 migration tests passed**, including seven real-server integration cases;
none were skipped. They cover quoted names, composite unique references,
partially NULL keys, children without declared primary keys, restart/resume,
matching unjournaled DDL, changed completed relationships, disabled target checks,
changed projections/rows and refusal to repair an orphan under a validated source
relationship. Runtime checks exercise deferred child-before-parent writes,
CASCADE, SET NULL and immediate unique-parent reference enforcement.

The [validation record](validation-results.json) also includes 709 SQL library
tests passed with three ignored, seven BLOB migration checks, selected deferred
and authorization suites, 181 PostgreSQL and 48 Oracle-profile protocol tests,
workspace/all-target checks and the built server digest. These are local results;
no GitHub workflows were changed or manually dispatched for this checkpoint.

The [source plan](source-plan.json) resolves all 3,826 exported application
relationships, including 912 validated and 2,914 not validated, through the same
public declaration logic. That planning result does not count the relationships
as installed in an application database.

## General catalog and enforcement contracts

Filtered `pg_constraint` reads now resolve referenced column positions from the
parent schema even when only the child relation was selected. Foreign keys and
checks on tables without declared primary keys remain visible. PostgreSQL and
Oracle-profile unit regressions protect both rules independently. The native
migration tests had exposed zero parent-column positions under a relation filter;
the corrected catalog lets the importer verify actual metadata without inference
or application-specific substitutions.

The read-only `bicdb_referential_checks_enabled(text)` accessor exposes the native
table enforcement switch. It preserves PostgreSQL's separate declared
`conenforced` metadata, handles quoted namespaces/names and NULL/missing tables,
and retains disabled state across restart. The importer refuses disabled target
checks before probe or installation.

## Scope and remaining qualification

The initial shared native test database could not reuse explicit key/index names
across schemas. Final migration fixtures use independent databases. Schema-local
index-name namespaces remain a separate compatibility gap; this checkpoint does
not qualify multi-schema reuse of explicit index names.

Enabled same-schema source relationships are supported here. Disabled source
constraints, external references, unsupported parent states, complete orphan
inventory, source comparison, business workflows with every relationship
installed, rolling upgrades and production qualification remain broader gates.
The accepted application deployment is separate from the candidate used here.
