# Incremental Online Backup Contract

## Purpose

BicDB must back up a live, multi-terabyte database without copying the entire
database to local staging on every run. The unit of reuse is an immutable
storage extent or index segment, not one monolithic archive file.

The backup must include records, regular indexes, database-native full-text
search (FTS), and every durable sidecar needed to reopen the database. A
successful restore must produce one transactionally consistent database, not a
collection of files captured at unrelated times.

This document defines the required behavior. It does not change the current
`BICBAK03` format by itself.

## Required outcomes

- The first backup is a full base.
- A later backup copies only new or changed immutable extents, index segments,
  metadata objects, and the WAL needed to reach its consistency point.
- Unchanged objects are referenced by content identity from an earlier
  retained backup; they are not read into a second full local archive.
- Backup runs do not stop readers or writers. Brief, bounded coordination is
  allowed to establish the cut, seal mutable files, and publish a manifest.
- Every published backup is encrypted, authenticated, independently
  verifiable, and restorable while its declared dependencies are retained.
- A failed or cancelled run never damages the live database or a previously
  published backup and never publishes a partially valid snapshot.

## What belongs in a backup

A BicDB backup is a database backup, not merely a page-store copy. Its manifest
must cover all durable components owned by that database:

- catalog, schema, transaction, sequence, and format metadata;
- record/page extents and large-value or blob sidecars;
- regular B-tree, spatial, vector, and other durable index structures;
- FTS definitions, dictionaries, posting blocks, field statistics, sealed
  generations, write tails, and tombstones;
- the WAL and other recovery logs required to make those components agree at
  the selected recovery point;
- checksums, encryption metadata, database identity, format/feature versions,
  and backup-chain identity.

An index may be marked `derived_rebuildable` only by an explicit operator
policy. The default is `required`. A restore that omits a rebuildable index
must open with that index unavailable, schedule or require its rebuild, and
must never silently serve incomplete results.

## Snapshot and consistency model

Every backup has one declared recovery point: a database UUID, backup ID,
parent backup ID, snapshot generation, and WAL start/end LSN. The manifest
binds every included or referenced object to that point.

The online capture sequence is:

1. Acquire a backup pin so checkpoint cleanup cannot remove required page or
   WAL history.
2. Establish a checkpoint and record the starting generation/LSN.
3. Seal mutable page, index, and FTS files into immutable objects where
   necessary. New writes continue into new active tails.
4. Compare immutable object identities with the parent manifest. Reuse
   unchanged objects and stream only changed/new objects.
5. Seal and capture the WAL tail that covers all writes observable in the
   streamed objects.
6. Record the ending LSN and publish the authenticated manifest atomically.
7. Release the backup pin only after every referenced object is durable in the
   configured repository, or after the run has failed and cleaned up its
   unpublished state.

The fuzzy page copy plus WAL-tail argument used by the existing online paged
backup remains valid: WAL page images must be durable before page writeback,
pages are captured before the final WAL cut, and WAL truncation is pinned for
the duration. FTS and other separately persisted indexes must either
participate in that same generation/LSN cut or be captured as derived data
with an explicit rebuild requirement.

## Incremental object model

The logical snapshot is described by a small manifest; it is not required to
exist as a second full local file. Each object entry contains at least:

- stable component and relative-path identity;
- object kind, including page extent, FTS segment, index segment, metadata, or
  WAL segment;
- plaintext length and cryptographic content digest;
- encrypted-object identity and encryption/key version;
- source generation and relevant LSN range;
- whether the object is included by this backup or referenced from an
  ancestor;
- required/derived-rebuildable restore policy.

The default extent size remains 1 GiB for segmented page stores. FTS segments
may use their natural immutable generation/block files; they do not need to be
padded to 1 GiB. Deduplication is content-addressed so a rename or manifest
reorganization does not force a payload upload when the content is unchanged.

An incremental snapshot may depend on ancestors, but restore time and
retention must not grow without bound. BicDB must support synthetic-full
manifest compaction: publish a new manifest that references the current live
object set without re-copying unchanged payloads. Chain-depth and age limits
are operator policy and must be reported before they are exceeded.

## FTS behavior

FTS is part of correctness. The manifest must prevent a restored database from
combining records from one recovery point with posting lists from another.

For one BicDB directory, its FTS metadata and segments are captured in the same
backup automatically. For an application that deliberately uses multiple
BicDB directories—for example, a canonical record store plus an immutable FTS
base and a mutable FTS delta—BicDB needs a backup-set manifest. That manifest
contains one member manifest per store plus a shared application snapshot ID
and the consistency relationship among the members.

Restore of a backup set is all-or-nothing:

- verify every member and dependency before activating any restored path;
- restore into inactive paths;
- validate database IDs, FTS definitions, document counts/generation bounds,
  and WAL coverage;
- atomically switch the application to the restored set only after validation;
- retain the previously active set until the post-restore health check passes.

If an FTS base is immutable and reproducible, policy may allow it to be shared
by many snapshots. The mutable FTS delta and its tombstones still require each
snapshot's exact generation or an explicit rebuild from the restored records.

## Repository and off-site behavior

BicDB should write directly to a repository abstraction backed by local disk,
an object store, or a transport such as Restic. The repository must provide:

- content-addressed immutable objects;
- atomic manifest publication;
- resumable multipart upload for large objects;
- bounded parallelism and configurable I/O, CPU, and bandwidth limits;
- server-side existence checks so unchanged payloads are not uploaded;
- encryption before untrusted transport and no secret material in logs or
  process arguments.

Restic deduplication remains useful as an off-site transport today, but it does
not replace the native behavior above: BicDB must avoid first materializing a
new full terabyte-scale local base merely so Restic can discover that most of
it is unchanged.

## Verification and restore

Verification has three levels:

1. **Manifest verification** authenticates the snapshot and resolves every
   ancestor/object dependency without reading all payload bytes.
2. **Payload verification** reads and authenticates every required object and
   validates its plaintext digest.
3. **Restore drill** restores to an isolated path, replays WAL, opens BicDB,
   runs strict storage/index integrity checks, and executes bounded record,
   regular-index, and FTS query smoke tests.

The normal backup job must complete manifest verification before declaring
success. Payload verification may be scheduled independently, but every object
must be verified at first upload and again according to retention policy. A
production restore must fail closed on a missing ancestor, missing object,
digest mismatch, unsupported format, WAL gap, database-identity mismatch, or
FTS generation mismatch.

Restore may materialize ordinary files or mount/lazily hydrate immutable
objects, but the database must not serve traffic until its required metadata
and recovery chain are local and verified. Lazy hydration must never turn a
missing backup object into silent data loss.

## Retention and deletion safety

Retention operates on manifests and object reachability, not filenames or
dates alone. Before deleting an object, BicDB must prove that no retained
snapshot, legal hold, in-progress backup, verification, or restore references
it. Garbage collection uses a mark-and-sweep pass over authenticated manifests
with a grace period and a dry-run report.

Deleting a backup manifest does not immediately delete shared extents. A full
base must never be deleted merely because newer incremental manifests exist.
Key rotation similarly requires a verified snapshot under the replacement key
and retention of old key material until no retained snapshot needs it.

## Cancellation and failure behavior

- Cancellation is checked between bounded copy/upload units.
- Temporary objects and manifests are uniquely named and remain unpublished.
- Retrying a run reuses already uploaded, verified immutable objects.
- Disk-full, network loss, process crash, WAL rotation, and concurrent writes
  must not invalidate the live store or older backups.
- A backup pin has observable age and retained-byte limits. If safety requires
  aborting, BicDB aborts the backup rather than exhausting production storage.
- Local staging cleanup is automatic after success or failure, subject to an
  explicit forensic-retention option.

## Operator surface and observability

Start/status/cancel/list/verify/restore/drill/prune operations must be available
through the core API and CLI, with SQL or service exposure where a second
process cannot open the live store. Status reports at least:

- backup and parent IDs, phase, start time, elapsed time, and recovery LSN;
- logical snapshot bytes, reused bytes, uploaded bytes, staging bytes, and
  remaining estimate;
- objects total/reused/uploaded/verified and current throughput;
- backup-pin age, retained WAL bytes, and free-space safety margin;
- each database/FTS member and its generation;
- repository target, encryption key version, cancellation state, and last
  error without secrets.

Success means manifest published, required objects durable, immediate
verification passed, repository snapshot visible, and the backup pin released.
An HTTP request returning `202 Accepted` or a worker merely exiting is not a
successful backup.

## Acceptance criteria

The feature is complete only when automated tests and a production-scale drill
demonstrate all of the following:

- after a full backup, a no-write incremental uploads metadata/WAL only and
  does not reread or restage every unchanged extent;
- changing one page extent and one FTS segment uploads only those objects plus
  required metadata/WAL;
- concurrent inserts, updates, deletes, checkpoints, FTS tail writes, folds,
  and WAL rotations preserve exact restored query results;
- base plus incrementals restore with record counts, regular-index lookups, and
  ranked FTS results matching the declared snapshot;
- a missing ancestor/object, mixed FTS generation, corrupted frame, or WAL gap
  fails before activation;
- cancellation, crash, disk-full, and network-loss injection leave production
  and the previous backup intact and permit a resumable retry;
- pruning cannot delete shared live objects;
- the recurring run uses space proportional to changed data, not total
  database size, and stays inside configured production I/O limits.

Until these criteria pass, off-site deduplication should be described as
incremental transport/storage, while the current online operation should be
described accurately as a full local base plus a WAL-tail consistency chain.
