# Nonzero native image installation: composed software evidence

Exact private candidate `8a7ddf6ead2bce6e3a41e04fdf68af1a7df3cf57`
(package metadata `1.0.466-beta`) passed **34 distinct focused Rust tests**.
The source was compiled from its own worktree, with the actual Cargo compiling
path retained. Related controls reused that exact unchanged executable, whose
SHA-256 is `b11c62e01f769a25d78f0e0927dd784f874c9433427397a9d89daba3705a5747`.
This is an unpublished component candidate, not current-main release,
independent-node HA, a production binary, or qualification evidence.

The databases use actual `server_paged` storage, native page/WAL transactions,
fsync, real ordinary RF1 origin commits, and actual close/reopen. Metadata
fixtures use three durable stores, real election/barrier/probe logic and
registered test identities with in-process protocol transport. These tests
exercise no sockets, mTLS, separate processes, VMs or power loss. They do not
repair or repeat the failed Azure experiments.

| Exact-source phase | Passing tests | Scope |
| --- | ---: | --- |
| `attempt-5-image-install` | 10 | Target refusal, original error/reopen cases, complete data/history installation, R0/R1/R1025 |
| `native-controls` | 16 | Factory/birth/namespace protection, actual history, stored-spine staging and conflict refusal |
| `origin-controls` | 7 | Actual WAL/native origin callbacks, prefix/pending/Unknown and invalidation controls |
| `capture-controls` | 1 | Actual source capture, retained exclusive read pin and exact learner transition |

## What was observed

The R1 test uses real commits on another source range, so source-global DB
sequence differs from selected-range index R=1. Seventeen separate local
transactions on the target make its DB sequence larger than both. A stale
selected-range value is replaced; ordinary target MVCC `get` returns the exact
source value, the other range's rows remain unchanged, and the target keeps
its own namespace UUID. Copied rows are checked again after reopen. Source R,
source DB sequence and native page transaction ID are never substituted for
the actual target-local visibility/durability bound.

The R1025 test executes **1,025 genuine fsync-enabled RF1 source commits**
before the durable handoff promise. It asserts the actual source catalog has
resolved through 1,025, compacted the first raw command payload, and retains
1,024 irreversible command checksums. The actual complete native history has
1,026 entries including genesis, exported/staged/audited in steps of at most
256 entries. All 1,025 copied values are checked through ordinary target MVCC
reads and again after actual reopen. The actual target-local snapshot bound
is smaller than 1,025; the test would reject using source R as that bound.
This is not an RF3 fault experiment or a throughput measurement.

The copied source records in these positive fixtures are ordinary `Record::new`
rows with empty metadata. The R1 case additionally checks replacement of stale
metadata and retention of other-range metadata. There are no user secondary
indexes, FTS workloads, queues, application audit/event reconciliations, SQL
joins or multi-range business transactions in this bundle. A complete record
root plus matching schema definitions does not independently prove those
subsystems' query parity or failover behavior.

Before any target row removal, the installer persists catalog refusal and a
native Installing marker, then requires an opaque proof from a complete
ordered source-history transfer and reread of the actual stored inactive
spine. The proof checks the target's actual native history point, pending
command checksum and every retained raw CQ/Applied obligation. A deliberately
incompatible **lower local-store** command fixture takes actual
prepare/commit/certify transitions and is refused before data copy; that
fixture supplies the trusted local-catalog input and is explicitly not an
authenticated distributed fork schedule or proof that a learner can vote.

Data verification is owned bounded progress: each call scans a bounded batch
at the same actual target-local MVCC bound. Complete count/content/catalog,
namespace IDs, schema/readers and image generations must match. One actual
native page transaction publishes the target's local history-generation/base
receipt, deletes Installing, retires the exact old pending/prefix, and retains
immutable per-generation publication evidence. The adapter reads its own
transaction and checks all binding fields, including the actual target-local
bound, before journal retirement. Old immutable history generations remain.

The installed anchor is a distinct typed native anchor, never a fabricated
genesis. Its lower-bound integrity observation remains available when source
visibility is invalidated or Installing exists, so an irreversible floor
cannot silently become unknown/zero. Completed-window recovery rereads the
same actual installed history and complete target data in bounded steps; it
keeps the original native receipt and does not retire a second prefix.

**The catalog refusal remains active after these successes.** Native receipt
creation grants no membership activation, serving configuration, catalog
floor advancement, qualified leader, strong read or application SQL contract.
Current-voter repair still needs an actual all-fenced-quorum compatible source
producer; the declared purpose enum is not that implementation. Exact
activation/configuration ancestry, cold recovery-session rebinding,
source-substitution liveness, namespace recreation attempts and unrelated
Unknown transaction holes remain separate integration gates. Physical alias/
rename contracts remain explicitly unsupported. No GC of old history is proved.

## Failures, resources and replay

All four preceding failed attempts remain immutable and separately classified:

1. `8bc0e8d8`: compile errors from duplicated mirrored getters, stale fixture
   field access and typed map keys; no tests ran.
2. `96fdbe6f`: eight tests passed; a semantic-load restart fixture incorrectly
   indexed a token-keyed map by range ID and failed at that lookup.
3. `c8fd9076`: a new lower-store negative fixture called a private constructor;
   no tests ran. The correction uses a test-only wrapper around the original
   validating constructor; production visibility did not change.
4. `ff96c52d`: eight tests passed; final publication refused a legitimate load
   update because it compared raw source approximate load fields against the
   canonical fenced descriptor. The fix compares the already validated fenced
   descriptor while retaining epoch/span/leader/voters/placement and exact
   credential-map checks. The original load-change assertions remain.

The final installer phase took 276.04 seconds including compilation and
210.88 seconds of unit execution. These are test durations, not database
latencies or RTO. Minimum available RAM was 20.81 GiB, with no resource pause
or termination. The active monitor enforced 12 GiB pause, 14 GiB resume,
8 GiB hard RAM and 8 GiB disk limits, one build job, incremental compilation
disabled and two test threads. Only the owned process group could be stopped.

`summary.json`, all raw compressed logs, resource samples, source manifests,
exact relevant source snapshots and monitor bytes are retained. Snapshots do
not replace the full repository and locked dependencies needed to build.
The shared test-executable path is mutable; its retained SHA is an identity
observation, not a published artifact or retention guarantee. Temporary
fixture roots were removed by the Rust test harness. No production RPO/RTO,
p50/p95/p99, sustained aggregate capacity gain, CI status or deployment is
established here.

Run `python3 scripts/verify-native-nonzero-target-evidence.py` for read-only
manifest, exact-source, retained-failure and scope replay. It does not rerun
Rust or turn this component result into qualification.
