# Durable structural cuts for the paged store

The ordinary paged engine uses complete structural cuts. There is no Cargo
feature or transaction class that permits eviction of an unlogged WAL-managed
frame. `begin_redo_backed` remains a compatibility spelling for beginning an
ordinary transaction; a logical redo decision does not exempt its page apply
from this rule.

At an operation boundary, pressure logs every unlogged frame under one fresh
internal transaction, then durably commits that transaction before clearing any
unlogged flag. A failed image append or commit leaves the store fenced. The cut
makes structure recoverable without committing the user transactions whose
versions it contains. Visibility still follows their own durable outcomes.
Recovery raises the xid allocator past all recovered image and outcome records,
so an invisible crash-lost version cannot become visible through xid reuse.

An operation cannot cut halfway through a B-tree mutation. Admission reserves
space; shards may transfer existing logged frames using nonblocking donor locks.
If necessary, each shard may allocate one emergency frame, and no more. The
normal frame budget is unchanged; the maximum extra resident page data is
`shard_count * page_size`, excluding ordinary frame/container metadata. A durable
boundary trims unpinned extras. If an operation cannot finish within this bound,
its failure after mutation fences the store before any later cut can persist a
partial structure.

Large overflow values do not acquire an exception to no-steal. A newly allocated
unreachable page has a private consumable capability. Its tail is durably sealed
before linking a predecessor; the durable xid high watermark precedes every
recoverable xid-zero image that may contain a live version. A higher empty
**committed** marker supplies that watermark without consuming an abort-exception
slot. The complete parent-reference cut precedes publication and later retirement.
Old chains are retired only after replacement references are durable and no
snapshot can still follow them. Free links become durable before free-head
adoption. A crash may leak unreachable pages, but must not free a reachable page.

The public pool's ordinary flush cannot independently log and write an unlogged
WAL-managed image: it returns `UnloggedWritebackNeedsCut` before writing that
image. Owned unreachable images have a distinct private path. Generic pool
barriers retain their existing trait behavior. Paged maintenance takes the
writer gate and makes a complete closure durable before disk-header
classification or adoption; this includes orphan-free reclamation with pending
writers. Checkpoint publishes the durable descriptor before retiring prior
status pages and resetting the WAL.

## Regression contracts

The historical interim refused large page-only transactions and still allowed
redo-backed steals. Those contracts are superseded: whole oversized bulk and
index builds must succeed atomically, and every writer must report zero unlogged
steals. The old GAP-1 integration file now uses finite inserts plus actual
**eviction-counter and WAL-growth** witnesses instead of waiting forever for a
refusal or positive steal counter. Its acknowledged-row, invisible-pending-row,
structural-integrity and repeated-recovery assertions remain. W1-L and free-page
reuse W8 are ordinary enabled tests.

Physical byte identity remains required for a refused isolated ordinary flush.
It is not required for a successfully completed internal structural cut: a cut
may persist complete invisible versions while leaving the logical acknowledged
state unchanged. This distinction replaces the interim's refusal-only raw-file
assertion. The original interim tests remain in PR81 history; the unpromoted
feature build and its source-scoped evidence remain in PR110 history.

The recovery-semantics document describes its explicitly named historical source
and interim policy. This document describes the replacement algorithm. Source
merging, optimized artifacts, crash qualification and production deployment are
separate states; no passing historical feature build qualifies a changed default
binary or a running deployment.
