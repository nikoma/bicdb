# Native joint-data handoff prototype

Status: unmerged, uncompiled joint implementation. No joint handoff, fifth-node
join, activation, cleanup, crash recovery, operator convergence or physical HA
success is established by this document. The first component control has been
written but has not run. The independent descriptor-voter correction was split
as `533b570b9`; that source checkpoint also has no test result here.

The intended first vertical case starts with four metadata members `n0..n3`,
RF3 data voters `{n0,n1,n2}`, and data leader `n0`. A genuinely new born-v2
`n4` starts with `initial_ranges=[]`. Its immutable birth is enrolled while it
is an unassigned metadata learner, before actual metadata promotion. Metadata
catch-up/promotion precedes adding its data learner. No existing born-v1 store
or missing old-voter identity is upgraded by this protocol.

## Independent domains

`R` is a command-history index. Source capture reservation `Bsource`, each
participant's native visibility bound, target import `Btarget`, native page-WAL
XID, metadata read index, and metadata topology generation are independent.

The actual completed source seal supplies `Bsource`; the old native prefix's
last materialization sequence is not used as a substitute. The actual target
MVCC imports and shippable barrier supply `Btarget`. Matching data points mean
matching logical genesis and full history commitment at `R`, not equal database
sequences or equal local native receipt checksums.

The immutable physical base, full birth-member ancestry, namespace UUIDs and
physical capture descriptor remain unchanged. Descriptor projections require
every actual Voter ID and its exact incarnation/pins; metadata-only birth
members do not gain data votes. A changed effective data domain needs a separate
protected native membership bridge. A metadata range descriptor alone is not
such a bridge.

## Required sequence

1. Commit the exact target learner allocation, source/target credentials,
   schema/restore/bootstrap domain, relocation ID and durable session/nonce.
   Refresh actual host policies before another data decision.
2. On each observed old data member, obtain an independently fresh exact-plan
   witness outside service/DB latches. Acquire and drain the actual range gate.
   Retain its owned freeze before a fallible durable operation. Expiration never
   releases a permanent promise.
3. Record every actual irreversible raw CQ/Applied, sparse decision, current
   certification and native pending checksum, including nonvoting peers that
   cannot close. A peer may close only when its actual raw tail is fully
   resolved, actual native history covers it, and every retained decision
   agrees with the immutable native spine. Persist its higher-epoch promise,
   protected native closed receipt and catalogue coupling.
4. Collect distinct old data majority observations. Select the greatest actual
   closed native point, verify every lower point's ancestry and EVERY observed
   irreversible checksum against the selected complete history. An observed
   higher/conflicting obligation blocks the cut. No missing entry is dropped.
5. Freeze the source for the initial copy. Export the actual full range image
   and complete `0..R` history in bounded steps. The target stages and verifies
   all history/compatibility obligations BEFORE destructive clear, uses its
   typed image application capability, imports with actual local MVCC commits,
   audits extras/full contents, and publishes the native target receipt and
   catalogue adoption with its own UUIDs and `Btarget`.
6. Collect a distinct intended-new data majority at the same `R/H0/H`, including
   the intended leader and actual installed target. Surviving members contribute
   actual closed materialization; the learner contributes its actual finalized
   image/native/catalogue proof. Generic Ready DTOs and old promises cannot vote.
7. Form one complete immutable decision body from these actual observations.
   Drain capture pins before counted Controlled native metadata writers. Publish
   the SAME full body and immutable archive in actual page-WAL transactions,
   read them back, and couple each local catalogue. Actual durable receipts must
   cover an OLD majority and a NEW majority before metadata can change.
8. Conditionally commit topology format 3 stages `Certified`, then union
   membership at E3 `PromotedFrozen`, then final `{n1,n2,n4}` membership at E4
   `AwaitingActivation`. These operations require the actual dual durable
   decision and independently fresh committed metadata. They construct only an
   ephemeral proposal view; a direct local topology-file mutation is refused.
   The real metadata consensus append/commit must publish each candidate. The E3 union is not
   allowed to acknowledge application data. Physical source deletion has NOT
   happened at E4; cleanup remains explicitly pending.
9. After final metadata commit, acquire fresh host observations and publish
   native membership bridges on the new members, tied to that exact protected
   decision and each actual local physical prefix. Collect actual NEW bridge
   readiness, persist activation, couple catalogue activation, and release
   freezes LAST. The old promise continues to reject old-epoch decisions.
10. Demonstrate a real new-leader application ACK and consuming Strong value
    read. Only a distinct actual new Activated quorum plus fresh final metadata
    may authorize source retirement. Persist its floor/history tombstone before
    typed bounded deletion; complete an actual full empty-range audit before
    metadata becomes `Cleaned`/Completed.

The first copy intentionally freezes the range. This gives bounded write
unavailability during the operation. Online copy/catch-up and automatic
controller availability remain separate acceptance gates.

## Current source boundary

The new `distribution_range_handoff/joint` modules draft scope binding, actual
old closure, complete-obligation collection, actual finalized-target votes,
actual old/new decision WAL publication, retained drain ownership and typed
conditional metadata stages. The native record writers live below
`db/range_image_native/serving_configuration/joint_handoff`.

Catalog 14 has an explicit optional joint-state map and a separate sticky local
reader fence. Exact format 14 requires the map and actual born-v2 enforcement;
a future format 15 can legitimately omit the joint map. A larger format number
never manufactures joint mode. Existing sparse/Installing/proposal fences and
the independent writer marker are preserved. Topology 3 retains decision and
cleanup tombstones. Unpublished protocol 23 is shared with the typed Strong
endpoint to fence older authors. These fences do not themselves grant readiness.

`native_joint_actual_old_closure_four_metadata_rf3_nonempty_complete_prefix_and_shared_pin`
is a SOURCE-ONLY component test. It declares an operator plan but commits it
through actual fsynced metadata stores, then acquires actual metadata witnesses.
It uses the existing metadata-only `n3` as the planned data target, not a fresh
fifth member. Its initial three genuine application ACKs comprise two unrelated
range writes and selected range ACK1. It checks the old closure and shared source
pin with actual source global B at least 3 versus selected R1. Its role-provider
call uses the trusted local initiator seam and real mTLS peer probes, not a new
incoming handoff RPC. It is not the full vertical acceptance test.

The later source batch adds the native serving bridge and a closed raw/open
lifecycle, described below. Historical suffix recovery, proof-authorized
cleanup, new fifth-member enrollment and the complete vertical tests remain
unfinished. None of this joint batch has been compiled or executed.
Legacy promotion/cleanup is deliberately refused for enforced native ranges;
that refusal is not an availability success.

## Current source-only serving lifecycle

`NativeJointDurableDecision::into_new_serving_owners` transfers the actual NEW
survivor/image owners after the typed metadata stages consume the durable
decision. It transfers exclusion, not activation. Each service consumes a fresh
`CommittedNativeJointDecisionObservation`, the actual retained owner, and the
complete local catalogue/native history to produce a native-WAL preparation.
Its recorded local B, namespace identities, physical anchor and history remain
local; the quorum compares the common decided R/H0/H and exact NEW membership.

The sealed NEW preparation quorum acquires post-quorum Host MQ internally,
outside service/DB latches, before publishing a complete native serving bridge.
A second actual publication round requires native bridge receipts on a NEW
majority including its leader. A further internally acquired Host MQ precedes
the local catalogue transition. No serialized audit projection constructs any
of these quorums or admissions. Original cancellation/deadlines remain bounded.

`persist_local_catalogue_activation` records a CLOSED activation and retires
only its exact adopted image intent in one fsynced catalogue publication. It
preserves old promises, all original history and the physical image descriptor.
Its installed open guard is initially unarmed. Every ordinary writer/read
path seeing this catalogue phase must obtain that guard; merely removing the
image refusal creates no legacy/Genesis fallback. Native effective members
come only from the actual protected bridge and immutable archive. Applied
suffixes bind its decision SHA and exact activated leader/epoch/member set.
The optional SHA is omitted from old prefix bytes; it grants no NoData right.

After all native/raw publications, the caller drops historical quorum/capture
owners. Survivor runtimes release only the exact S/N owner after validating
the actual local activation receipt and unique remaining owner. A standalone
target installer owner can then drain naturally. `arm_local_native_joint_open`
checks the actual DB-open lifetime, durable native/catalogue binding, serving
generation and a real gate observation proving no freeze/pin/callback remains.
It never accepts a caller-supplied released flag. A new freeze invalidates the
guard. The first intended live fixture uses a standalone target image freeze;
the separate recovery-installer lease return/exchange still needs its own
qualified joint completion producer and tests.

This is implementation source, not an exercised activation result. Raw-phase
re-fsync is conditional on the exact protected native activation and never
restores an old grant after uncertainty. Recovery after all original authority
budgets/owners are gone needs an independently fresh physical/quorum
reattestation; stored audits cannot renew a request. Native read/ACK3, target
lease drain, cleanup and crash-cut controls must run before claiming the
healthy fifth-node vertical. Later same-voter historical recovery needs the
effective protected joint ancestry rather than the old physical voter map;
ordinary recovery remains strict until that separate producer is composed.

## Required adversarial controls still open

- A lower native old voter may be part of an otherwise valid closed majority.
  Learning the selected higher committed cut needs a purpose-specific actual
  selected-old-cut repair proof and exact plan ancestry. Its lower local
  promise is not permission to discard higher obligations. Current generic
  repair/old physical revalidation intentionally does not establish this case.
- After final membership, a missing/behind new voter must be repairable from
  the protected decided domain under fresh current data quorum proof, even if
  an originally ready member is unavailable. Fresh MQ alone cannot cast
  membership or bypass the retained old promise.
- Kill the original source before/during copy and the coordinator before and
  after metadata finalization. Reconstruct the SAME full protected decision
  from actual stores and finish the SAME session/nonce. A new random nonce,
  deadline expiry or a decoded audit body cannot erase partial decisions.
- Release all temporary capture readers and historical quorum object owners
  before actual activation. Retained RAM proof references must not accidentally
  retain an otherwise activated freeze; neither may they be bypassed by an
  uncounted metadata writer.
- Recover a real new-domain postactivation pending suffix and then acknowledge
  another write. A retained old promise must reject old-epoch commands beyond
  its cut while allowing ONLY the actually certified new-domain suffix.
- Preserve generic raw-import/Dummy/wrong-hook refusals, namespace recreation,
  schema/restore/incarnation/pin changes, source/target sequence divergence,
  duplicate votes, partial publication, uncertain persistence, and full history
  beyond payload compaction. There is no eight-megabyte history truncation.

Only the complete vertical and these fault controls can establish a safe joint
candidate. Automatic four-to-five convergence, public host/controller delivery,
independent processes and physical qualification are later, distinct results.
