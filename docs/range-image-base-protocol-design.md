# Range image bases: proposed proof and implementation boundary

Status: **design, not implemented or qualified**. The
[actual463 relocation characterization](evidence/relocation-image-history-2026-11-11/README.md)
shows why this is required: after1,025 acknowledged range commands, a supported
relocation installs the correct latest row and completes, but target range index
remains0; healthy source voters have resolved1,025 and compacted1. Recovery asks
for another snapshot rather than recognizing a verified installed history base.
The8-command retained-suffix control recovers and serves its exact value.

A `RangeImageBase` must prove that a particular installed image represents one
certified **range-command prefix**, independently of source or destination DB
commit sequences. Assigning `resolved_through = snapshot_commit_sequence`, using
`last_index`, counting copied bytes, or trusting a caller's integer is invalid.
An image proof grants replica readiness at its cut. It grants no leadership or
strong read authority by itself.

## Typed identities and trusted construction

Keep public wire descriptions separate from opaque in-process admission types.
Only a trusted coordinator can construct a ready-to-install permit after actual
authenticated quorum observations; decoding a wire manifest is not that permit.
The receiver must validate the exact committed relocation and its own identity
before an image mutation. Existing member TLS/SAN/pin/cluster/protocol/request
checks remain mandatory. This is a crash-fault design, not a Byzantine protocol.

| Identity | Required binding |
| --- | --- |
| Session | Cluster, version, fresh session/nonce, exact relocationID and transfer state |
| Range | RangeID, token span/hash version, semantic routing/config identity, old/new voter identities and incarnations |
| Authority | Fresh metadata read-authority context/current-term anchor, designated range leader and persistent range-epoch promise |
| Source | Actual source voter identity/certificate/member incarnation and source DB incarnation |
| Cut | Quorum-certified range resolved index and retained-prefix/history digest; no provisional tail |
| DB position | Separately typed source DB snapshot sequence and catch-up cursor; never used as range index |
| Installed image | Ordered bounded-batch chain, complete record/image root, collection/namespace incarnations, schema and reader capabilities |
| Destination | Exact target member identity/incarnation, local DB installation incarnation/commit sequence and installed-image receipt |

The per-open DB incarnation is not a permanent lineage identifier. A persisted
installation receipt must verify across reopening against the native incarnation
journal/commit outcome evidence and the installed namespace/image identity. If
that evidence is unavailable, the node stays unready and reinstalls from an
authorized source. A successful local commit resolver does not itself certify
the range or the image contents.

## Smallest correct serving-path batch

Start with a bounded **quiesced range copy** if necessary; label its write pause
explicitly. That foundation can fix compacted-history relocation without claiming
online/no-pause relocation performance. Keeping writes online requires a later
proven base/suffix retention pipeline, rather than weakening the cut proof.

1. Bind the proposed copy to fresh metadata authority and exact committed
   relocation. Acquire range admission exclusion. Recover or refuse ambiguous
   tails; repair an actual voter quorum to one certified resolved cut `L`.
2. Install durable mutation fences on a leader-inclusive data quorum, confirming
   no unresolved tail and exact cut. Record the transfer intent so another
   authenticated current authority can resume/abort after coordinator failure.
   No clock-expiry shortcut may authorize conflicting configurations.
3. Export bounded physical batches from that fenced image, binding namespace,
   schema and reader capabilities before importing data. The batch checksum chain
   must end in a completion seal tied to `L`. Verify the actual source image root
   against a data quorum at the same fenced cut, using the existing digest
   machinery where its exact invariants apply.
4. Install through ordinary native durability/index/reader fences. Recompute or
   otherwise verify the actual destination root. Persist the resulting installed
   base only after the native image/reader state is durable; atomically install
   the base and its range-log checkpoint in the range catalog.
5. Before promotion, verify the old/new **data** configuration transition proof
   described below and the target installed base. Metadata snapshot parity alone
   cannot authorize cleanup. Revalidate semantic authority and release fences
   only through the committed transition/recovery protocol.
6. Leadership recovery starts from a valid installed base `L` and extends only a
   verified certified suffix after `L`. Strong reads still require fresh current
   authority and a quorum-certified applied floor through the requested read.

The normal relocation source is the recovered old range leader. A separate
recovery path may need a healthy current voter as physical source after leader
loss. That requires a narrowly authorized read/export permit bound to the exact
recovery plan. Do not relax the existing generic snapshot source check or create
an arbitrary administrator import/export bypass.

## Durable installation and failure ordering

`RangeLearnerApplyState` is temporary relocation progress and may disappear at
cleanup. Installed image authority belongs in a persistent range-owned catalog,
with version/checksum and bounded retention. It must survive learner cleanup,
node restart and command suffix compaction.

Use an explicit installation journal/state machine, for example
`Installing → NativeImageDurable → Installed`. Persist an installation intent
before mutation; keep replica/read admission closed while it is incomplete.
Reader capabilities and namespace incarnations must be installed/validated before
data. Couple the native image durability receipt to the final range checkpoint;
do not assert cross-file atomicity without a recovery rule. An interrupted
install resumes idempotently or discards/reinstalls its staging image. A crash
before the final verified installed record never grants readiness. A completed
installed record is trusted only after native recovery confirms its matching
commit/image/reader identity.

The image chain authenticates ordered transfer bytes but is not automatically a
quorum endorsement of their contents. The source root, cut and installed root
must agree through the trusted coordinator/digest protocol. Unknown mutation,
namespace change, schema/reader capability change, conflicting history, wrong
recipient, stale incarnation, missing chain segment or missing final seal all
refuse promotion/readiness.

## Data membership fencing is a separate prerequisite

A fresh metadata majority is not a range data majority. Adding a learner,
promoting it to a fourth voter and later deleting an old voter does not by itself
prove that an isolated old RF3 quorum has lost write authority. Every transition
needs durable old/new data-quorum evidence and a persistent epoch/config promise
which rejects old prepare/commit/certify/apply attempts even when no command has
yet been written in the new epoch.

The existing `current_epoch` often precedes routing authority while old commands
remain valid. Do not impose equality as a readiness shortcut. Some durable
promise mechanics already exist: `install_backup_fence` persists the requested
epoch, refuses unresolved tails, and `prepare` rejects a lower epoch even after
the fence is released. These may support a separately proved configuration
promise; a redundant new counter is not inherently required. The current
relocation characterization does not exercise such a fence quorum. Prove the
configuration binding, in-flight-tail exclusion and commit/certify/apply behavior
before reusing those mechanics. A temporary fence's TTL alone is not a permanent
configuration proof; expiry or learner cleanup must not revive old authority.

For joint activation, require old and new voter majorities to attest the bound
cut/image/readiness and persist the transition promise before publishing the new
writable configuration. Intersection with a durably fenced old majority prevents
an isolated old quorum from acknowledging later commands. The acknowledged-write
certificate-quorum correction is necessary evidence retention; it does not
replace this transition proof. New-voter physical catch-up, old-voter fencing and
metadata publication are distinct conditions.

Fences cannot become an indefinite deadlock when the coordinator fails. The
committed transfer intent and monotonic promises must let a healthy current
quorum finish or safely abort/reissue the transfer within bounded work/RPC
budgets. A manual-only or permanently unavailable recovery path fails the P0
healthy-positive requirement.

## Online extension and bounded work

For online copy, bind an initial certified range cut and a distinct DB snapshot
cursor; retain every needed logical suffix/physical WAL segment until the final
seal. A fuzzy multi-batch copy needs a proof that suffix replay reconstructs the
exact final image, including deletes and namespace/schema changes. Finish with a
bounded final mutation fence, catch up to certified `L_final`, verify image roots,
then install/activate. No retained history may be trimmed until a valid installed
base replaces its recovery role. If budgets expire, preserve explicit resumable
state; do not fabricate completion or silently switch to an unverified counter.

## Required deterministic gates before implementation completion

| Case | Required result |
| --- | --- |
| Actual supported1,025-command relocation | Exact target data, valid persisted base, successful healthy-quorum recovery and continued writes |
| Existing8-command retained suffix | Still succeeds and serves exact values |
| Snapshot batch/catch-up interrupted at each durable boundary | No premature readiness; bounded resume/abort with healthy quorum |
| Crash before native image durability, before final base, after final base and after learner cleanup | Correct installed-state recovery; no counter-only admission |
| Wrong range/span/relocation/namespace/schema/reader capability/source or destination incarnation | Refused before unsafe mutation/readiness |
| Uncertified tail, different quorum roots, missing chain/seal or truncated suffix | Fail closed; exact evidence retained |
| Old minority leader and skipped intermediate configuration | Cannot acknowledge a write; new caught-up quorum serves values |
| Leader loss during image sealing | Healthy current quorum completes/aborts without permanent fence |
| Post-base certified suffix, later compaction and another relocation | Base plus suffix reproduces exact current records/indexes |

The first implementation batch should introduce the durable installed-base type,
validation and crash-ordering tests without granting serving authority from an
unverified manifest. Actual relocation integration and healthy-positive tests
must follow before calling the availability defect fixed. Snapshot batch/native
reader-capability fields are owned by a concurrent change and require explicit
coordination before editing. No implementation or qualification follows from this
design document.
