# Oracle foreign-key orphan inventory

After a successful [foreign-key migration](oracle-foreign-key-migration.md), the
read-only inventory counts unmatched child rows for every supported source
relationship. It retains duplicate row multiplicity and excludes a row if any
key component is NULL, following the supported MATCH SIMPLE contract. It uses
native SQL comparisons, including character and numeric semantics, rather than
Python stringification of keys.

This is an inventory, not data repair or source-data equality verification.
Source `NOT VALIDATED` relationships can have legacy orphans even while new
writes are enforced. The tool does not delete rows, rewrite keys, validate
constraints, disable enforcement or reset the migration's data baseline.

## Run on a quiesced migration target

Keep application, importer and schema writers stopped. `--writers-quiesced` is
an operator assertion, not a lock or distributed writer lease. The tool does not
claim a cross-table transaction snapshot under concurrent writers. Retain the
pre-migration backup and the completed migration report.

Use the same target connection endpoint and exported snapshot as migration.
Keep the PostgreSQL DSN in `BICDB_PG_DSN`; do not pass credentials on the command
line. From the repository root:

```sh
python examples/oracle/idempiere/foreign_key_orphans.py \
  --snapshot /private/export \
  --migration-report /private/foreign-keys.json \
  --report /private/foreign-key-orphans.json \
  --writers-quiesced
```

The server must confirm `oracle-preview-v1` and its time zone. The tool refuses
incomplete migration journals, source or endpoint mismatches, changed declared
relationships, missing/changed parent keys, disabled table enforcement and
changed projections. Before counting, it fingerprints all affected target
tables and compares them with the completed migration baseline. After counting,
it rechecks catalogs, enforcement and row fingerprints.

Enabled, same-source-schema relationships inherit the migration phase's support
limits. Unsupported or unresolved declarations fail; they are not silently
excluded from counts. Self references, ordered composite keys and nullable
references use the same counting query builder as ordinary relationships.

## Interpret the report

- `inventory_completed` means every declared relationship was counted and the
  final metadata/data checks passed. A partial report never sets this flag.
- `completed` retains declarations and unmatched row counts. It contains no row
  values, connection strings or repair statements. Counts across relationships
  are not a count of distinct affected rows: one child row can violate several
  relationships.
- `all_relationships_clean` means all counts are zero.
- `validated_relationships_clean` means every source-validated relationship has
  zero unmatched rows. A nonzero count there prevents `success` and causes a
  nonzero command exit. A successful inventory can still report orphans under
  source `NOT VALIDATED` relationships.
- `data_compared_to_source`, `orphan_inventory_qualified` and
  `application_qualified` remain false. Independent source inventory comparison,
  an explicit policy for legacy rows and complete application acceptance are
  separate qualification gates.

The output is saved after bounded batches and on ordinary failure. This command
does not resume a partial count inventory; rerunning starts a new read-only
inventory against the same completed migration baseline. It refuses to use the
source manifest or migration journal as its output path. If an explicit,
separately approved data repair occurs, rerun the migration verification phase
to establish its new baseline before inventorying that changed target.

## Bounded evidence

The [qualification record](evidence/oracle-foreign-key-orphans-2026-11-05/README.md)
separates native migration-target tests from independent Oracle/PostgreSQL
query fixtures. Small fixtures do not establish the source application's orphan
counts, whole-workload performance, online consistency or production readiness.
