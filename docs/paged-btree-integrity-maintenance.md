# Bounded server-paged B+ tree integrity verification

Status: shipped in BicDB 1.0.72-beta.

`BicDb::verify_paged_btree_step` verifies the primary server-paged B+ tree in
finite, restartable slices. It is the production-sized counterpart to the
older exhaustive `BTree::verify` diagnostic, which remains available for an
offline check but must not be used as a PB-scale maintenance primitive.

## Resource envelope

Every call receives strict `BTreeVerifyLimits` covering:

- entries inspected;
- leaf pages inspected;
- logical page bytes inspected, including the initial root descent;
- aggregate key bytes inspected;
- cooperative duration;
- the maximum serialized cursor-key size; and
- root-to-leaf height.

Every field has a finite hard ceiling. A new sweep must reserve enough page
bytes for its complete configured height guard before it starts. Resumed steps
need only enough page bytes for one page. Invalid combinations, an oversized
cursor key, page zero, and an out-of-file cursor page fail before any progress
is reported.

Working memory consists of one pinned page, page-bounded live-entry intervals,
one step-bounded leaf-cycle set, a height-bounded descent set, and one cursor
key. It does not grow with the database or total number of keys.

## Structural checks

The verifier does not trust node accessors until it has checked the page. It
classifies:

- non-tree pages reached through tree pointers;
- page-ID mismatches;
- invalid directory and free-space bounds;
- empty or out-of-page keys and values;
- overlapping live entries;
- duplicate or decreasing keys within one node;
- invalid or missing interior children;
- root-descent cycles and excessive height;
- leaf-sibling cycles and traversal beyond the file page-count bound; and
- non-increasing keys across leaf boundaries.

The report retains only page IDs, entry ordinals, counters, and bounded cursor
state—never damaged key or row payloads. Checksum, torn-page, short-read, and
other physical I/O errors remain their precise `PageError` values rather than
being relabeled as structural corruption.

## Cursor and restart behavior

`BTreeVerifyCursor.next_leaf` is the next physical leaf to inspect.
`after_key` is the last accounted key. `resume_within_leaf` distinguishes a
partially consumed leaf from a leaf boundary, so resumption neither recounts a
key nor hides cross-leaf ordering damage. Fully traversed leaf count and the
observed file page-count bound provide a finite cross-step traversal guard.

Leaves in this B+ tree generation split in place and are not merged or freed,
so a returned leaf page is a stable restart anchor across writes and process
restart. A future leaf-merge format must version or replace this cursor
contract before reclaiming such pages.

The cursor, limits, fault, stop reason, and report reject unknown JSON fields.
A host can atomically checkpoint `next_cursor` after accepting a valid step and
resume after reopening the database. The structure read lock is held for only
one bounded call. Writes may continue between calls; keys inserted behind the
published cursor belong to the next online sweep rather than being attributed
to a transactionally frozen snapshot.

## Result meaning

A step stops with `complete`, an entry/page/byte/time limit, or a classified
structural fault. `valid` means the portion inspected by that step contained no
structural fault. A store-wide structural claim requires one complete sweep,
every accepted step to be valid, and no discarded or substituted cursor.

`PagedStore`, `PagedRecords`, and `BicDb` expose the same contract. Embedded
mode rejects the paged-only operation explicitly.

## Durable supervision

BicDB 1.0.73-beta composes this cursor with bounded MVCC verification under the
checksummed, atomic supervisor documented in
[`paged-integrity-maintenance.md`](paged-integrity-maintenance.md). It owns
phase, immutable limits, store and operation identity, accepted totals,
resource admission, retry/pause state, and terminal evidence.
