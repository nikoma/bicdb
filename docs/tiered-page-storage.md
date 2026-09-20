# Tiered page storage

BicDB tiers immutable page extents, never the live mutable page file. A caller
must first establish a durable checkpoint and then seal a page-aligned range
from that checkpoint with `seal_page_extent`.

The sealing path:

1. refuses page zero (the mutable superblock), empty ranges, invalid page sizes,
   integer overflow, and an extent above the configured byte limit;
2. opens a regular source file without following its final symlink;
3. verifies every page checksum, generation trailer, type, and expected page ID;
4. derives a SHA-256 content identity in one bounded-memory pass;
5. streams the exact range through the provider in a second bounded-memory pass;
6. requires the provider to reproduce the expected byte count and hash; and
7. returns a versioned descriptor that binds range, page size, checkpoint LSN,
   tier, length, object key, and checksum.

`TieredStorageLimits` makes both the I/O buffer and maximum extent size
explicit. The default buffer is 1 MiB and does not grow with the database or
extent. Extents default to at most 64 GiB and may never be configured above
1 TiB.

## Local immutable provider

`LocalImmutableExtentStore` is the reference provider. Content addresses have
the only accepted form `sha256/xx/<64-lowercase-hex-digest>`; arbitrary paths
and aliases cannot be constructed through the public API. Provider roots,
namespaces, hash-prefix directories, source files, and stored objects reject
symlinks where they are opened or created.

Uploads use a unique `create_new` incomplete file in the destination directory.
The provider streams, hashes, optionally syncs, and atomically publishes with a
no-overwrite hard link. A concurrent publication of identical content is safe:
the existing object is fully verified and reused. Existing bytes are never
replaced. A failure at any point removes only the operation's own incomplete
file. Deletion accepts only an exact validated key and refuses non-regular
objects.

Reads validate descriptor bounds, object size, and the complete SHA-256 while
streaming. Callers that materialize a local cache file must keep that file
unpublished until `read_verified` succeeds; as with any streaming integrity
API, the destination may have received a prefix when an error is returned.

## Atomic generation publication

An uploaded extent is not by itself part of a database generation. A
`PageGenerationManifest` binds a database identity, monotonically increasing
generation, page size, checkpoint LSN, previous-manifest hash, and an ordered,
contiguous extent map covering every sealed data page from page one.

`TieredManifestCatalog` stores manifests and activation markers as immutable,
checksummed objects. Publication holds an operating-system ownership lock,
compares the expected active hash, requires exactly the next generation, and
verifies every new or changed extent, including each contained page's expected
ID and structural integrity. Unchanged objects are trusted through the previous
verified manifest rather than rereading PBs of content. Database identity and
page size cannot change, and checkpoint LSN or creation time cannot move
backward.

The activation marker is the commit point. It is atomically published without
overwriting anything; readers select the highest valid marker. A crash before
that marker leaves the prior generation active. A crash after publication makes
the complete new generation active. Retrying either outcome is idempotent.
Interrupted staging files are operation-owned and cleaned under a bounded
budget. Old manifests and activations remain immutable for rollback and later
reference-safe garbage collection.

Query and recovery code must continue using the prior local generation until
this publication succeeds.

## Bounded hydration cache

`TieredExtentCache` is a disposable local cache in front of an immutable extent
provider. Its byte budget, entry count, concurrent hydration count, admission
wait, and crash-staging cleanup count are all explicit hard limits. The cache
holds a kernel-released ownership lock so two processes cannot maintain
conflicting residency accounting over one directory.

A miss reserves bytes before I/O, coalesces concurrent requests for the same
content address, streams into an operation-owned staging file, reopens and
verifies the complete hash and every page, then hard-links the same inode into
its canonical cache location without overwrite. No second extent-sized copy is
needed. A crash leaves at most a recognizable staging file that the next owner
removes under a bounded cleanup budget.

Reads pin entries against eviction and verify the complete cached object.
Admission evicts the least-recently-used unpinned object; it fails instead of
exceeding the budget when all candidates are pinned. A corrupt hit is marked
invalid, unlinked after the last reader releases it, and rehydrated on the next
read. Startup discovers existing immutable objects while evicting anything that
does not fit a lower configured budget. Snapshot counters expose resident and
reserved bytes, entries, hydrations, hits, misses, evictions, and corruptions.

## Generation-pinned point reads

`TieredPageReader` turns an authenticated `ActivePageGeneration` into a bounded
point-read surface. Construction revalidates both the manifest checksum and its
outer content hash. The reader then owns that immutable snapshot: publishing a
new generation cannot mix its pages into an in-flight read view.

Extent selection uses the ordered manifest map and does not enumerate posting,
row, or page keys. A cache miss hydrates and verifies the owning immutable
extent; subsequent point reads use positioned I/O and copy only the requested
page into the caller's page-sized buffer. The selected page's ID, checksum, and
torn-write trailer are verified on every read. A corrupt local point read
invalidates and removes the cache object, allowing the next call to rehydrate
the provider's authoritative copy. Because restart discovery has not witnessed
the original hydration, the first post-restart use also revalidates the complete
content address and page layout before any point is served.

Page zero stays local because it contains mutable publication metadata. A
reader rejects page zero, pages beyond the sealed generation, a forged detached
manifest/hash pair, and non-page-sized destination buffers.

## Read-only buffer-pool integration

`PageReadSource` decouples buffer-pool misses from the mutable local page file.
`TieredPageReader` implements that interface, so a generation snapshot or
read-only follower can use the ordinary sharded, scan-resistant buffer pool
while its durable pages remain in immutable extents. The buffer pool still
preallocates every frame from its byte budget and admits exactly one page per
miss; extent hydration has its separate hard disk-cache and concurrency limits.

`BufferPool::new_read_only` fails construction on page-size disagreement or an
empty source and exposes the mode in its operational snapshot. It rejects every
writable guard before loading a page. This is an intentional correctness fence:
until BicDB has a durable mutable-overlay residency journal, a locally modified
page must never be evicted and then silently replaced by the older checkpoint
copy.

## Reference-safe provider garbage collection

Tiered extent GC never treats "not in the latest manifest" as sufficient proof
for deletion. A checksummed `TieredExtentGcState` pins the exact active
generation/hash, rollback retention count, protected-object identity, resource
limits, next content-hash prefix, and cumulative progress. The returned state
can be durably checkpointed between calls; replaying a completed or partially
completed prefix is idempotent.

Every step takes the same catalog lock as generation publication, reloads and
validates the retained manifest chain, and compares the active hash with the GC
checkpoint before deleting. An activation between steps therefore fences the
old run. Backup/snapshot owners supply a bounded protected-object set whose
canonical hash must remain identical for the run.

The local provider inventory is split into 256 deterministic hash prefixes. A
step loads at most one explicitly bounded prefix and deletes at most the
configured object count. If candidates remain, the same prefix is retried;
otherwise progress advances. Active, rollback-retained, and externally
protected objects are never candidates. Only after all prefixes finish does GC
prune generation markers and manifests older than the retained rollback window,
also under a hard file-count limit.

## Bounded provider-read retry

Hydration retries transient provider I/O failures—interruption, timeout,
backpressure, connection reset/abort/refusal, and disconnected transport—under
explicit attempt, initial-backoff, and maximum-backoff limits. Backoff doubles
only to its configured ceiling, and retry count is observable in the cache
snapshot.

Each attempt receives a new operation-owned staging file. Failed partial bytes
are deleted before waiting, so attempts cannot concatenate or publish a damaged
response. Content-integrity errors, missing objects, invalid descriptors,
authorization/policy failures, and other non-transient errors are never retried.
The provider interface remains synchronous; application hosts should run remote
hydration in their bounded blocking-I/O pool and enforce provider-side request
deadlines as well.

Remote credential/provider implementations and mutable buffer-pool fallback
remain subsequent increments. Version 1.0.29-beta can run bounded read-only
snapshot/follower pools over sealed pages, but live writable database pages may
not yet be evicted merely because an extent is active.
