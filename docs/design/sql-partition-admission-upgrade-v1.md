# SQL partition admission and upgrade v1

Status: implementation in progress on the private partition branch. This is a
logical SQL compatibility transition, not P2 storage-format promotion, a portable
namespace receipt, distributed placement, or HA qualification.

## Observed inputs

The genuine merged `1.0.482-beta` CLI at source
`f43583da4c34209e810c5e6a8f5925831b2949a1` created six immutable stores on S3:
`/root/bicdb-durability-s3/partition-old-store-fixtures-482-20261112`.
Both storage modes cover old parent-heap rows with native indexes, an acknowledged
out-of-bound child row, and independently stored child policies. The external
ledger has fourteen acknowledged rows. Candidate runs use copies and recheck
every original file's size and SHA256.

The unchanged partition runtime at `97aabad9`, with probe-only source `1f6a25ed`,
fails all six required upgrade cases. An upsert creates a second logical primary
key beside an old parent-heap row; invalid bounds and ambiguous policy state are
admitted. Both old-reader controls are accepted and mutate copied store files.
Those failures, old stdout, hashes and ledgers remain evidence, not passing tests.

Old policy storage is genuinely ambiguous. In the controlled policy fixture:

- `p` has forced RLS and `parent_guard USING (k < 10)`.
- `low` stores forced RLS with no policies; the old loader nevertheless applies
  its parent's policy and shows acknowledged key2 to `legacy_reader`.
- `high` stores both `parent_guard` and independently created `authored_child
  USING (k = 11)`; the old loader replaces that list with the parent's and hides
  key11. The new loader would allow key11 if those stored policies were silently
  activated.

The migration must not guess authorship from identical names, expressions or
flags. An explicit local operator decision is required where stored and old
effective child-policy states differ.

## Native capability component

`sql_partition_execution_v1` is a sticky required-reader capability. Activation
uses the existing durable replacement primitive with named write/sync/rename/
directory-sync fault points and permanently fences the affected handle on a
critical I/O failure. An activation retry always syncs, even when a prior failed
directory sync left the flag visible. Later format replacements also force sync
while the capability is present.

The bit alone does not prove SQL admission. Native only supplies activation,
its current local status, and a legacy-repair prerequisite check: fsync, loaded
secondary indexes, no pending transactions/row-lock owners, and healthy recovery.
SQL separately publishes a versioned completion certificate. Actual old-binary
refusal and unchanged bytes must be proved after the SQL path is implemented.

## SQL design to implement and qualify

1. Add optional execution-version fields to PostgreSQL `PartitioningSchema` and
   `PartitionOfSchema`; missing means legacy, unknown means refuse. Oracle
   partition metadata remains separate. New partition definitions must carry v1
   only after capability/certificate activation. Generic saves preserve markers.
2. Inspect raw schema/trigger records with exact stored identities and parent
   links. Reject cycles, wrong namespaces, incompatible columns, overlapping
   bounds, unknown versions and malformed binding/certificate state. Do not
   infer lineage from an unqualified name or donor identity.
3. A bounded read-only inspector reports a SHA256 snapshot fingerprint, legacy
   relations, policy-decision requirements and invalid child rows. Stream native
   batches where possible; collect only rows to move, with an explicit memory
   budget. Bind the fingerprint to relevant raw metadata, actual native
   collection metadata, definitions and acknowledged row content. This is a
   local operator snapshot binding, not authenticated portable provenance.
4. The public offline plan binds that exact snapshot. Policy choices are
   `keep_stored` or `preserve_historical_effective`; preserve exact old raw
   policy records in an immutable migration archive when a reviewed decision
   changes them. Out-of-bound child moves require explicit source/id/destination
   entries tied to the snapshot and the same exact partition tree. No row is
   dropped or merged to make a uniqueness check pass. Missing decisions, stale
   fingerprints, extra actions and conflicting acknowledged keys refuse.
5. Automatically route old parent-heap rows into the actual final leaves. This
   preserves their logical parent relation. Valid leaf rows stay in place.
   Validate all resulting bounds and constraints, destination identities and
   live index effects. Preserve original record content. A genuine duplicate or
   unroutable ACK requires explicit data/schema reconciliation, never deletion.
6. Preflight the entire migration. Persist the reader fence before any upgrade
   publication. Use one Native transaction for source deletions, destination
   inserts, schema-version/policy updates, required trigger clones, archived raw
   policy records and the completion certificate. The existing approved
   TransactionExceedsBufferPool safety policy still applies; do not weaken it.
   Native rollback/crash recovery must expose either the complete old set or
   the complete upgraded set with its certificate. A flag-only or empty-system-
   collection interruption is not a completion certificate and must resume.
7. Cache admission by actual database instance and schema/trigger/certificate
   generations. For ordinary schema changes, use the Native recent-change window
   to inspect only changed records; fall back to a complete bounded metadata
   scan if coverage is unavailable. Avoid a full catalog scan per statement or
   per ordinary CREATE during ERP baseline loading. Imported or directly changed
   legacy descriptors must invalidate admission too.
8. Cover SQL text, parsed AST, COPY, procedure entry and direct read-only engine
   entry points, plus host startup before a listener serves data. Read-only
   entry points never mint a certificate or modify a store. Shared SQL sessions
   request the existing exclusive-access fallback when actual repair is needed.
   A certified current store takes the fast path. No GUC can bypass admission.
9. Reader flag without a certificate is an incomplete activation; unknown or
   unfenced certificates refuse. New stores without PostgreSQL partitions should
   retain ordinary compatibility until the feature is actually used. Sticky
   fencing remains after partition DROP or rolled-back first use.
10. Default startup must refuse ambiguous policies and invalid child ACKs with
    SQLSTATE55000 and actionable inspection output. The explicit trusted offline
    plan is a real recovery path, not a fabricated replacement fixture.

## Required evidence and remaining gates

- Make all eight original old-store cases pass without editing old fixtures or
  weakening assertions. For valid migration, exercise upsert arbitration, all
  fourteen independent ACKs, native index verification and two reopens.
- Explicitly qualify each policy choice and child relocation, stale/wrong/extra
  plan controls, duplicate ACK refusal and exact raw policy preservation.
- Use the real old482 CLI to prove rejection before any copied store byte changes.
- Inject process cuts at fence, staging/publication and ACK boundaries; inject
  write/fsync/rename/directory-sync failures; reconcile independent ledgers.
- Complete Native-protected-collection authority, standby/replication capability
  propagation and process/fault proofs. Public unprotected Native APIs must
  refuse rather than bypass a required context or signed mutation grant.
- Finish trigger namespace/rename/mixed operator-type lifecycle, other partition
  constraints/FKs/hash/ONLY/concurrency/snapshot paths, then broad SQL/protocol.
- Integrate the next actual main, choose the next unused delivery version, run
  final checks/CI, merge/push and verify. This private source is not delivery.

The authenticated portable Native backing-birth/accepted-provenance producer for
replica rename recovery is still independently unimplemented. The original
Oracle/PostgreSQL inventories, unchanged10s waits, UI400/encrypted backup/restore,
physical HA, independent crash/VM and approved P1/P2 tracks remain in the full goal.
