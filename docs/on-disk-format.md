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

## Online extent tracking compatibility

The first paged online backup durably enables
`online_backup_extent_tracking_v1` in database format metadata and upgrades the
page superblock magic to `BICDBPG3` (tracked segmented layout). A tracked store
converted offline to monolithic layout uses `BICDBPG4`. Earlier core binaries
reject the unknown feature flag, and earlier page tools reject these page
magics before writing. This prevents old writers from bypassing the extent
journal and making a later incremental reuse changed data.

Flush, reopen, checkpoint, historical WAL replay and offline relayout preserve
these guards. Relayout invalidates the journal identity before changing files.
There is no implicit downgrade after enabling tracking: use a compatible binary
for the database, its restored copies and branches. v1-v3 backup readers remain
available, but an artifact's source-format feature flags still apply.

## Mandatory physical backup gate profiles

Sources implementing `physical_backup_gate_v1` retain the four existing page
layout/tracking combinations with mandatory reader-fenced magics:

| Layout | Tracking | Current magic |
| --- | --- | --- |
| Monolithic | Untracked | `BICDBPG5` |
| Segmented | Untracked | `BICDBPG6` |
| Segmented | Tracked | `BICDBPG7` |
| Monolithic | Tracked | `BICDBPG8` |

The earlier `BICDBPG\0`/`BICDBPG2`/`BICDBPG3`/`BICDBPG4` values describe
pre-gate inputs. Current owning-engine opens validate those inputs and publish
the mandatory fence before later writes; tracking, extent routing and relayout
remain distinct. The new values are outside the prior decoder's accepted
magic set. Exact current-output tests therefore require 5/6/7/8, while tests
which intentionally read historical artifacts retain their original bytes.
The byte/source compatibility assertions in those tests do not execute or
qualify a historical reader binary. See the [physical gate contract](design/standard-live-backup-physical-gate.md)
and the [520 test-oracle correction](design/page-gated-profile-test-correction.md).

### Live native allocation-policy audit (source-only candidate)

The private first-allocation continuation adds the sticky local
`native_writer_allocation_policy_v1` reader requirement before publishing a
version2 `AllocationEpoch` writer audit. Version1 registry records keep their
original strict layout and registry-only meaning. The new audit preserves the
original registration, writer State, DB/Store and coordinator; it is not a
portable lease or permission to reconstruct them after restart. Native
CURRENT/archive/ancestry and the actual committed XID must match before the
live verified cache can use the exact new descriptor.

An attempted FORMAT flag or visible audit bytes do not establish Ready,
successful fsync, native Serving, APP or Strong. Unknown native IO remains
blocked; real closed native readiness, completion, activation and fresh
quorums are separate gates. The controlled old-feature-set validator definition
is not execution of an old binary. Backward-binary, mixed-artifact, restore and
cold continuation qualification have not been run for this profile. The
[implementation boundary](design/native-live-allocation-first-lane-source-unit.md)
also records the unimplemented RegistryAfterAllocation/repeated-cohort and
shared-history capacity gates; this profile does not establish scalable
add-node delivery.
