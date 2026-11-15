# Standard live backup physical gate

This source batch starts at main 1.0.519-beta (`587dd6e4775e454cc9e8224d5fed76e8795c6ff6`). It has not been compiled or executed. It does not qualify a backup, HA deployment, cold recovery, or capacity. The original `backup_paged.rs` and `backup_repository.rs` assertions and Standard producer are unchanged.

The observed main517 failure was a strict refusal because `paged/store.wal` changed during streaming. That observation did not identify its original mutator. The source gap is broader: the Core append shared gate ended before Page apply; Page eviction, checkpoint, recovery and layout/WAL maintenance had independent locks. A lock obtained only at the final page write can invert owned capture's lock order.

## One actual lock domain

Core and Page use Page's stable `.backup-write.lock` inode. A live database retains its immutable gate binding. Process registry entries are weak references to those actual bindings; the registry never holds its mutex during IO. Shared scopes are thread confined and reenter only a still-owned actual locked descriptor with the same filesystem identity. No path existence, metadata flag, cached boolean or scalar grants a lock.

The outer exclusive backup guard remains Send + Sync. It publishes no TLS entry while moving between threads. Owned capture explicitly borrows that actual EX descriptor lexically before Page entry; the borrow duplicates the same locked open-file description rather than requesting SH under EX. Shared scopes and Page latches retain real ownership until their final drop. The optional observation marker follows File in field order, so the descriptor closes before observed liveness becomes false.

## Lock order

| Path | Required order and end |
|---|---|
| Ordinary Core commit | Existing serializable admission → root SH → sequence reservation/Core WAL → physical/resident/index apply → materialization marker enqueue/write. Release root SH before visibility waiting and external finalization/callbacks. |
| Branch / owned capture | Existing Core exclusive admission → root EX → actual Page writer / pool / WAL / layout locks. Borrow the actual EX for nested Page IO; drop that borrow before dropping or moving its outer owner. |
| Native Page transaction | Original higher native/service custody → root SH → Page reentrant writer → existing transaction/WAL work. The writer guard becomes fallible; all original callers propagate admission refusal before issuing their transaction. |
| Page reads / eviction / read-ahead | Root SH before structure/shard/frame/barrier locks. Returned read/write/upgradable Page guards keep the physical scope through latch release and dirty bookkeeping; upgrade transfers the same scope. |
| Maintenance / recovery | Root SH before Page writer, materializer pass, pool, WAL, migration/layout or page-file locks. Checkpoint/meta/status/root, WAL reset/rotation/archive metadata, torn-tail recovery, extent changes, direct physical writers and deferred materializer teardown use this order. |
| Closed-target restore / branch copy | Actual root and existing child DirectoryLocks plus stable backup-owner lock → initialize the new target coordination inode → target SH → payload installation. A verified archive omits coordination files; only this genuine private target owner initializes them. Force cleanup preserves stable coordination inodes. |

Acquiring ordinary commit SH *before* serializable admission would deadlock a branch holding Core admission while waiting for EX. The scope is deliberately after Core admission but before any sequence or durable decision. Pre-effect gate refusal remains distinct from actual durability failure; the original postdecision uncertainty and storage health rules remain in force.

## Enrollment and older writers

A successful Core open holds its actual DirectoryLock and SH while it installs mandatory `physical_backup_gate_v1` in FORMAT before recovery. Every enrollment attempt performs the actual file and directory durability barrier even if the same flag bytes are already visible from a previous failed attempt. Every later FORMAT replacement preserves the flag and fsync requirement. Older Core readers reject this flag before mutation.

The Page superblock also has mandatory reader-fenced profiles: `BICDBPG5` (monolithic), `BICDBPG6` (segmented), `BICDBPG7` (tracked segmented), and `BICDBPG8` (tracked monolithic). The existing payload/layout distinctions remain intact. New opens validate the original header, publish and sync the mandatory Page fence before tracker/recovery writes, and retain it across raw superblock/WAL images and later checkpoints. Older normal Page readers reject the new magic before recovery. Arbitrary raw-file or standalone WAL edits outside the owning engine contract are not fenced by a reader feature.

An already-open unenrolled legacy Core writer is explicitly refused for live Standard backup. A closed legacy root can enroll through its actual open before the cut. FORMAT alone is insufficient: at the actual EX cut, Standard rechecks Core enrollment and validates the actual full Page superblock gate profile before hashing the manifest. This catches a concurrent Core open that wrote FORMAT but failed against an old standalone Page owner.

An enrolled enclosing Core root takes precedence over a nearer standalone Page gate inode. A missing or replaced enrolled gate refuses ordinary open/write. Archives and materialized copies obtain their new, deliberately omitted coordination inode through the actual closed-target ownership path, rather than a generic missing-gate recovery waiver.

Unix uses device/inode identity and flock. Windows uses actual handle volume/file identity and the existing share-mode exclusion contract. Other platforms refuse unsupported physical locking. Actual main519 already refuses ordinary non-Unix/non-Windows open in DirectoryLock before this gate, including the current WASI path; the README browser advertisement does not establish a passing runtime baseline. No new host ownership bridge, uncoordinated fallback, or fake lock was added. These are source implementations, not executed platform qualification. Same-process weak binding storage is not a replacement for kernel cross-process exclusion.

## Evidence and remaining validation

The independent passive regression is a distinct dependency leaf: the whole frozen `cca1548cde6cc6445f498092bc5a5f88905fc506` Standard regression is retained byte for byte. Its actual transaction-state assertions require issue205's genuine `commit_with_sequence_in_place` API, which is absent on standalone main519 and present in the later coordinated composition. There is no cfg workaround or replacement API assertion. Its manifest and syscall/lifetime observations move to the common Page-owned descriptor, with a default-off test observation feature enabled through Core's dev dependency.

Source controls cover nested SH retention, real borrowed EX and cross-thread exclusion, inode replacement refusal, read/write/upgrade latch lifetime, sticky Page reader fencing, actual Core reader-capability refusal, repeated actual enrollment-dir-sync refusal despite visible same bytes, and an actual populated Core root with a different child gate inode. They are unexecuted definitions. The reader-capability controls simulate an older known set; they do not run an old executable.

The unchanged Standard regression must still execute real fsynced A at the Core-WAL/pre-Page cut and retain actual EX descriptor/lifetime checks, strict archive success, verify, restore, all 100 baseline values plus A and physical indexes. A missing reached cut is a failing/inconclusive control, never a skip or an expected-error success. Existing watchdog durations remain unchanged; kernel IO and joins are not hard preemptible. No performance, stalled-disk independence, old-binary execution or original-cause claim follows from source review.
