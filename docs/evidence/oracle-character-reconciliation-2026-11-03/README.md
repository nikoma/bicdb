# Legacy character declaration reconciliation

The migration tool now reconciles exported CHAR/VARCHAR2 declarations with an
existing Oracle-preview database. It requires an explicitly qualified
AL32UTF8/STANDARD source environment and checks exported length units and byte
widths. It does not infer units from legacy native VARCHAR/BPCHAR typmods.

Read-only preflight examines all affected base tables before probe DDL or ALTER:
physical type/length, known logical declarations, column shape, byte capacity,
fixed padding and a complete row multiset. Check-only mode issues no DDL. Apply
uses ordinary typed ALTER under the existing ownership, constraint and index
checks, and a transaction per table. Full fingerprints must match before commit.
The journal binds to the source manifest, source environment, mode and endpoint;
resume also rechecks completed provenance and current values.

A physical-compatible source attachment checks normal assignment for each
record and writes only metadata when all values remain identical. It preserves
view descriptors and results. A different known source unit remains an ordinary
type change. The native dependent-view guard now resolves qualified relation
names rather than searching SQL tokens for encoded storage collection names.
An independent PostgreSQL fixture checks explicit-column dependencies across two
schemas with identical quoted table names, including periods inside identifiers.
The prior candidate matches 3/5 cases and incorrectly accepts the protected
change. Wildcard dependencies, full dependency tracking and Oracle view
invalidation remain outside this qualification.

The live synthetic reconciliation fixture uses actual Oracle column descriptors
and saved values. It verifies old-reader access after read-only preflight and
a rejected oversized legacy value, real CLI apply and resume, unchanged view
results, rejection of changed target values, mapped byte overflow and restart.
Fault injection after ALTER verifies that the table transaction restores all
original declarations and values. Profile metadata format 3 may already have
been activated by the capability probe; table rollback does not promise access
by an older reader. Earlier table commits also remain in place. Retain a backup
before apply; this is not an atomic upgrade of the whole database or Oracle DDL
implicit-commit qualification.

The initial live fixture failed source setup because VIRTUAL_COLUMN was queried
through USER_TAB_COLUMNS; it now uses USER_TAB_COLS, as the exporter does. A later
control expected the prior candidate to reject the named-schema fixture, but it
accepted it; that result led to the independent native dependency comparison.
These attempts are separate from the final validation, and neither changed an
application database. The general engine regression also covers public-schema
source attachment, with exact stored-record equality and retained constraints.

National table types, EXTENDED/NLS configuration, routine-local BYTE capacity,
BYTE anchors, casts, source-default provenance, Oracle dictionary contents and
visibility, and full application behavior remain incomplete. The existing
running application database and binary are unchanged. No release is published.

The first full application-copy trial passed read-only checks for all **7,150**
character declarations across **910** tables, then completed 28 table migrations
before the resource guard stopped it. A transaction was retaining full-table
undo copies for every metadata-only column ALTER. The correction uses existing
schema-only undo when assignment validation proves every record unchanged; it
also avoids redundant row snapshots for apply-error recovery. A regression
performs eight attachments in one transaction, checks zero retained table-row
undo copies, and verifies complete column and record restoration on rollback.
The full application-copy retry and application-scale memory qualification are
separate, pending evidence at this checkpoint. The interrupted copy was removed;
the original snapshot and running application remain intact.

Final local validation passes **1,322 Rust tests**, with three existing ignores,
and **58 Python tests**. All nine source-based reconciliation checks pass on the
final candidate, together with **257/257** independent PostgreSQL controls. The
retained Oracle corpus has **1,475** passing positive/error contracts and one
separate negative alias boundary. Original minimal-dependency counter, record
and sequence fixtures retain 14/14, 8/8 and 9/9 matches. The isolated definition
inventory remains **60/62**; 1/13 NULL-input diagnostic calls complete, without
establishing Oracle execution equivalence or business-workflow acceptance.
Dictionary contracts remain 0/11, with separate character assignments at 2/2.
Five separately tracked NVL/conversion mismatches remain unchanged.

The initial application-copy and concurrent reference batches were stopped by
their own 8 GiB available-RAM guards. They are recorded as resource interruptions,
not passing qualification. The successful final batches retain that guard, one
compiler and disabled incremental compilation. A later validation wait resulted
from a full shared root filesystem. Cleanup removed inactive, verified old test
harnesses and an orphaned hashed CLI cache while preserving runnable paths,
required readers, hardlinks, active files and dependencies. Reusable library
inputs were temporarily moved to RAM with verified links. Formatting, licensing
and the public dependency boundary checks pass. These development checks do not
qualify unclean process termination, crash-atomic DDL, Oracle implicit commits or
an end-to-end application deployment.

The corrected candidate saved a complete persistent-copy baseline of **915
tables and 185,537 rows**. Its retry then reached the disk guard when shared-root
space fell below 2 GiB; the recorded process-group peak was 3,203,211,264 bytes.
The trial volume was only 400 MiB. A verified, byte-identical temporary RAM copy
allows semantic checks to continue while preserving the disk checkpoint and
resume journal. This does not qualify persistent storage: completion requires
copying the completed volume back, verifying it, and reopening it on disk.
Process restart on a RAM filesystem is not host-crash durability. The temporary
build-library archive has been fully restored; current cache inputs are on disk.
