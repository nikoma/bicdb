# BicDB On-Disk Format Lifecycle

BicDB stores database format metadata in `format.json` at the database root.
The file declares:

- `format_version`: the exact on-disk structure version.
- `min_reader_version`: the oldest reader that may open the database.
- `min_writer_version`: the oldest writer that may mutate the database.
- `feature_flags`: required format features for this database.

Current format version: `2`.

Current required feature flag: `format_metadata_v2`.

## Open-Time Compatibility

Opening a database fails before recovery or writes when:

- `format_version` is newer than the binary supports;
- `min_reader_version` or `min_writer_version` is newer than the binary
  supports;
- `feature_flags` contains an unknown required flag.

Databases without `format.json` are treated as legacy format `1`. The open path
upgrades them to format `2` by writing explicit format metadata. This migration
does not rewrite record segments.

## Migration Policy

Format migrations must be explicit, monotonic upgrades. Operators should run a
backup before any migration where the dry-run plan reports
`backup_recommended: true`.

The Rust API exposes:

```rust
let plan = BicDb::plan_format_migration("./erpdb")?;
```

The plan is a dry run and reports source version, target version, steps, and
whether a backup is recommended. Opening the database performs supported
upgrade steps.

Downgrades are not automatic. A binary that does not support the database
format fails clearly and must not mutate data. Rollback is restore-based: keep
the pre-upgrade backup and restore it with a binary that supports that source
format.

## Crash Recovery

During a format upgrade BicDB writes `format-migration.json` with progress
metadata before applying steps. On the next open, BicDB resumes or completes the
same migration and removes the progress file after the database reaches the
current format. Operators should retry the same binary first after a failed
upgrade. If retry fails, restore the pre-upgrade backup into a clean path.

## Backups And Restores

Backup archives declare the source database format version and required feature
flags in the encrypted manifest. `verify`, `restore`, and drill paths reject
archives whose source format is outside the binary's supported range.

Incremental backup chains inherit the same policy: verify the ordered chain
with the target binary before restoring into production.

## Release Checklist

For every format-changing release:

- increment `CURRENT_FORMAT_VERSION`;
- add any required feature flag to the known flag list;
- implement a dry-run migration plan and crash-recoverable progress marker;
- add compatibility tests for at least two historical fixture shapes;
- add backup/restore compatibility tests for the new policy;
- update this document and `docs/backup-recovery.md`;
- publish release notes with supported upgrade paths and rollback instructions.

## Failed Upgrade Runbook

1. Stop application traffic to the database path.
2. Retry opening with the same BicDB binary to allow migration recovery.
3. If retry succeeds, run integrity verification and a backup drill before
   serving traffic again.
4. If retry fails, restore the last pre-upgrade backup into a clean path.
5. Open the restored database with a binary that supports its source format and
   verify application smoke tests before resuming traffic.
