# Oracle foreign-key migration

The application importer supports `--phase foreign-keys` for enabled source
relationships exported in `manifest.json`. It uses the PostgreSQL driver with
`oracle-preview-v1`. It preserves ordered child/parent columns, referenced keys,
delete actions, deferrability, initial timing and validation state.

This phase installs schema relationships and verifies target-value preservation.
It does not certify an application, compare target values to the Oracle source,
repair existing orphans, or qualify every concurrency/recovery schedule. Run the
separate source-data verification and application acceptance stages as well.

## Supported declarations

- Enabled foreign keys referencing exported enabled, validated, non-deferrable
  primary or unique keys in the same source schema.
- Deferrable keys initially immediate or deferred, and non-deferrable keys.
- `NO ACTION`, `CASCADE` and `SET NULL` delete actions.
- `VALIDATED` and `NOT VALIDATED` source state. The latter becomes `NOT VALID`:
  existing rows are not retrospectively validated, while future writes remain
  subject to the constraint. It does not mean disabled enforcement.

Unresolved/external-schema references, disabled source relationships,
unsupported parent-key states or actions, duplicate declarations and missing
columns fail before migration DDL. These states require additional migration
policy and implementation; they are not silently omitted.

## Preparation and read-only preflight

Finish table/data and required logical type reconciliation first. Retain a
pre-upgrade backup and use an isolated migration target. Stop application
writers, other importers and schema changes during apply. The flag below is an
operator assertion; it does not acquire a distributed maintenance lease.

Keep the target DSN in the `BICDB_PG_DSN` environment variable. Do not put
credentials into the report or command history. From the repository root:

```sh
python examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/export \
  --phase foreign-keys \
  --report /private/foreign-keys-preflight.json \
  --check-only
```

Preflight checks parent keys, exact existing foreign-key definitions, target
columns, table enforcement switches and complete exported projections. It
fingerprints affected target tables. It issues no probe or schema DDL and does
not claim the server capability probe passed. A successful preflight is not a
completed migration.

The target must expose the read-only
`bicdb_referential_checks_enabled('"schema"."table"')` accessor. It returns the
table's native FK enforcement switch, or NULL for NULL/missing table names.
This is separate from PostgreSQL's declared `pg_constraint.conenforced` state;
`DISABLE TRIGGER ALL` does not change that declaration. The phase refuses a
disabled or unverifiable table switch rather than enabling triggers implicitly.

## Apply and resume

```sh
python examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/export \
  --phase foreign-keys \
  --report /private/foreign-keys.json \
  --writers-quiesced
```

Before source relationships are installed, a nonce-table probe checks deferred
failure on an explicitly reassigned unchanged invalid key. Its tables are
removed on normal success or failure. The probe activates the durable Oracle
foreign-key enforcement contract when needed; profile-aware older servers reject
the upgraded marker. See the
[assignment contract](evidence/oracle-foreign-key-assignments-2026-11-05/README.md).
A failed cleanup prevents migration success; inspect the reported native error
before continuing after a connection or process failure.

Each missing relationship is installed with ordinary native DDL and then read
back from the catalogs. Already-present relationships are adopted only if their
columns, referenced table, actions, matching mode, timing, validation and declared
enforcement exactly match. A validated source relationship validates target rows;
an invalid target row stops that installation instead of being deleted or repaired.

Console output identifies catalog checks, baseline table fingerprinting, the
capability probe, relationship installation and final table fingerprinting.
Fingerprint counters count affected tables; installation counters count source
relationships. Fingerprint progress is emitted every 32 tables and at the final
table, including during read-only preflight. Use the report's success and
completion flags for acceptance; console progress is not a completed journal.

The journal is saved in bounded batches and on normal completion/failure. Resume
rechecks the exact completed declaration prefix, target identity, catalogs,
enforcement switches and saved target row fingerprints:

```sh
python examples/oracle/idempiere/import_bicdb.py \
  --snapshot /private/export \
  --phase foreign-keys \
  --report /private/foreign-keys.json \
  --writers-quiesced --resume
```

All existing source relationships, including completed and unjournaled keys,
are validated against the complete initial catalog inventory before probes or
new DDL. The installation loop starts after the validated completed prefix and
reuses that inventory for pending-key presence checks. Every newly installed
relationship is read back before it can enter the completed journal. A missing
or mismatched post-install readback stops the migration without certifying that
installation. These rules require the same quiesced writers/schema changes and
unchanged row fingerprints as the rest of resume; the initial inventory is not
a replacement for a maintenance lease when other schema writers remain active.

A matching relationship installed just before a journal write can be adopted on
resume. A missing or changed relationship already recorded as completed is an
error. Source/endpoint journal mismatches leave the prior journal untouched.
Changed target values also stop resume; there is no silent baseline reset or
automatic orphan repair. Keep application workloads off the target until
migration verification finishes.

## Evidence and remaining gates

Reports contain declaration metadata and table multiset fingerprints, preserving
NULLs and duplicate multiplicity without storing row values or DSNs. They record
`data_compared_to_source: false`, `orphan_inventory_qualified: false` and
`application_qualified: false` independently of successful schema installation.
The pre/post fingerprints cover every column in the affected exported tables;
changed/missing/extra target projections are refused.

Before cutover, separately inventory legacy orphans, compare source data and
constraint catalogs, verify sequences/routines/views and run the original
application workflows with all relationships enabled. Decide how to handle
source `NOT VALIDATED` rows explicitly. Validation and any approved data repair
are separate operations. Online migration, mixed-version cluster upgrades and
complete production qualification remain broader programme gates.

The [read-only orphan inventory](oracle-foreign-key-orphan-inventory.md) uses the
completed migration journal and checks its target baseline before and after
counting. It does not repair legacy rows or qualify source-data equality.

See the [reference reservation contract](foreign-key-reservations.md) for the
separate child-reference and parent-key-mutation concurrency rules. Installation
and orphan-count success do not qualify all transaction schedules.

The [catalog-reuse qualification](evidence/oracle-fk-catalog-reuse-2026-11-06/README.md)
records traced fresh/resumed query counts, a negative counter control and a
post-install mismatch/resume control. All 15 pure/native migration checks pass;
these focused results do not establish production throughput or full application
acceptance.
