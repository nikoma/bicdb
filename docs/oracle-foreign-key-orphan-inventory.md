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

For an older export whose live source has changed, use the
[isolated Oracle replay](oracle-export-reference-replay.md) to obtain independent
counts from that exact saved dataset. Matching counts alone still do not prove
that the complete BicDB target values equal the export.

## Compare with the saved-export Oracle reference

After both inventories complete, compare their reports offline:

```sh
python examples/oracle/idempiere/compare_orphan_inventories.py \
  --snapshot /private/export \
  --reference-report /private/oracle-reference.json \
  --target-report /private/foreign-key-orphans.json \
  --report /private/orphan-comparison.json
```

The comparator requires the same manifest digest and source schema, every
declared relationship exactly once, and identical complete relationship
definitions. Entry ordering can differ. Counts must be nonnegative integers;
booleans and fractional counts are rejected. The reference must attest complete
saved-export row verification for every table and successful temporary-object
cleanup. The target must attest a completed quiesced inventory with unchanged
fingerprints for every affected table. Validated relationships must have zero
orphans. Equal nonzero counts on unvalidated relationships can match, but still
require an operator's explicit legacy-data policy.

`comparison_completed` records that both reports passed these structural gates
and every count was compared. `counts_match` and `success` require all counts to
agree; differences are retained with their declarations and both counts, and
the command exits nonzero. Invalid or incomplete inputs leave an unsuccessful
report, including when replacing an older successful comparison. The output
cannot overwrite the manifest or either input report. Input SHA-256 digests are
recorded, and the source inventories remain unchanged.

This validates producer reports; it does not query either database, authenticate
report authors, establish their freshness, acquire a writer lease, or prove
full-row data equality. Keep both databases quiesced throughout the underlying
inventory procedure and retain report provenance. `data_compared_to_source`,
`live_source_compared`, and `application_qualified` stay false. An Oracle replay
is a reference for the saved export, not a comparison against today's live
source. Run the full [source-value verification](oracle-source-value-verification.md)
as a separate gate.

Eight offline regression tests cover matching legacy counts, mismatches,
incomplete/changed reports, duplicate declarations, invalid counts, missing
reference table verification, cleanup failures, malformed target fingerprints,
input overwrite protection, and stale-success invalidation. The retained real
Oracle replay report passes the reference-side checks for all 3,826
relationships. The full application-target comparison remains pending until
source foreign-key installation and target inventory finish.

## Resume an interrupted inventory

The inventory output is saved after bounded batches and on ordinary failure.
Without `--resume`, rerunning starts a fresh read-only inventory. To retain a
partial count prefix, use the same snapshot, completed migration journal,
endpoint and output path:

```sh
python examples/oracle/idempiere/foreign_key_orphans.py \
  --snapshot /private/export \
  --migration-report /private/foreign-keys.json \
  --report /private/foreign-key-orphans.json \
  --writers-quiesced --resume
```

Resume requires exact source/profile/endpoint identity and the same completed
migration-report digest. Counts must form an exact ordered declaration prefix,
use nonnegative integers, preserve complete original table fingerprints once
any count is retained, and contain zero orphans for validated relationships.
Changed/reordered declarations, malformed counts or a different migration
baseline are refused. A source/target identity rejection preserves the supplied
journal rather than overwriting another attempt's evidence.

After identity validation, completion flags are invalidated. The full current
catalog, enforcement switches, projections and row baseline are rechecked before
any prior counts can be reused. Only pending relationships are counted; the
final catalog and all row fingerprints are checked again. Even a previously
completed journal goes through these metadata/data checks. The
`resumed_from_relationships` field records the prefix considered for reuse;
`inventory_completed` and `success`, together with a zero command exit, remain
the completion gates. A failed command must not be treated as success because a
retained older report has completion flags.

Quiescence is required throughout interruption and resume; this does not acquire
a lease or authenticate journal authors. Preserve the private journal and its
provenance alongside the migration backup. Console fingerprint counters count
affected tables, while count counters refer to relationships.

The output cannot replace the source manifest or migration journal. If an
explicit, separately approved data repair occurs, rerun migration verification
to establish its new baseline and start a new inventory for that changed target.
Resume never repairs rows or silently resets the baseline.

## Bounded evidence

The [qualification record](evidence/oracle-foreign-key-orphans-2026-11-05/README.md)
separates native migration-target tests from independent Oracle/PostgreSQL
query fixtures. Small fixtures do not establish the source application's orphan
counts, whole-workload performance, online consistency or production readiness.
