# Native joint preparation phase recovery: first bounded publication step

This is private experimental source design, not an operational handoff or a
passing runtime receipt. The complete goal still requires every final replica,
the new leader, protected catalogue activation, actual release, a new ACK, and
Strong value/absence checks. Five independent processes, cold recovery, online
copy, and physical qualification remain separate gates.

## Observed failure and unchanged limits

The original complete-joint schedule remains in
`native_joint_actual_complete_image_target_becomes_leader_ack_and_strong_read`.
Its original request authority is not renewed. Root's executed component trace
at source c96 (CI run 43200329282, job 131121762731) failed 0/1 after 9.03s:
second activation acquisition consumed 1,248,614 microseconds and repeated
all-pin release/revalidation consumed 903,194 microseconds. The original
remaining budget reached zero; cancellation was false and no ambient query
budget was active. Individual prepared-service checks were about 35ms, native
checks about 31ms, protected record readback about 12–15ms. Metadata stores
remained term 1, commit/last 5, generation 5. These measurements do not establish
a general performance claim or a successful handoff.

The new phase budget remains at most four seconds; HostMQ's existing five-second
maximum is unchanged. An original caller deadline or cancellation always wins.
A later long-lived caller is not an input to the new phase constructor.

## Durable fact and current permission are separate

`CommittedNativeJointDecisionObservation::require_durable_fact_current` checks
the actual protected decision and same database open, and rejects explicit
cancellation of its original metadata request. It does not validate expired
metadata as current and returns no new metadata capability. The existing
`require_current` still requires its original live metadata request.

`NativeJointPreparedServingQuorum` now retains the actual parent token supplied
to `NativeJointPreparedServingRound::begin`, separately from its shorter MQ
window. Starting a new phase checks both that original parent and explicit
cancellation of the old quorum. Old quorum/observation deadlines are unchanged;
old `require_current` continues to fail after expiry.

`NativeJointPreparationPhaseRound::acquire_actual_member` obtains a new real
recipient-bound HostMQ independently for EVERY original/final participant,
including the new leader. It never holds a service or database latch across
network collection. With that new authority, the SAME actual service checks:

- exact protected old/new global and local decision, same DB/birth/open;
- exact persisted preparation pointer/archive and immutable preparation audit;
- actual schema, restore, UID, native generations, history R/H and target-local B;
- unchanged complete catalogue obligations, no omitted higher pending/CQ state;
- the retained actual freeze and existing read pin, or actual quiescence after
  that pin was consumed;
- actual current installed writer registration, retaining its Arc identity so
  hook/generation replacement invalidates the phase.

A failed member acquisition poisons the round. The full participant keyset must
match, rather than choosing a smaller available majority. Fresh metadata must
not regress behind either original decision or persisted preparation anchors,
and its context must differ from their original contexts.

After the complete actual phase is collected and revalidated, the local
publisher acquires a DIFFERENT post-quorum HostMQ. Its context differs from all
counted phase contexts and its term/index must cover them. Native bridge WAL
publication still retains the counted controlled mutation lease to durable
completion. A retry reuses an existing immutable matching native record; it
does not rewrite the prior audit with a new metadata context.

All three actual native publications are required before resumed aggregation
can finish. Already persisted receipts can be reaffirmed only under another
complete current phase, with actual native record checks. This is an in-memory,
same-open component producer using real per-node metadata TLS; there is no
remote joint-phase DTO constructor or complete cross-process transport.

## Read-pin idempotence

Each preparation owns a private `Mutex<Option<SourceCaptureReadPin>>`, filled
once at construction and only consumed with `take`. No method puts a pin back
in that evidence. When all three are already absent, repeated release skips the
redundant per-pin release traversal but still revalidates ALL actual members
before and after. The underlying freezes and durable promises remain. This is
not a cached authority check and does not by itself solve phase expiry.

## Source-only regression scope

Two added genuine fixture cases have not been compiled or executed locally:

- `native_joint_fresh_preparation_phase_publishes_all_final_members_after_original_expiry`:
  expires the old MQ window, refuses a wrong actor/poisoned round and a missing
  final member, obtains each per-node HostMQ plus post-quorum local HostMQ,
  publishes all final native records, reaffirms them, and checks that every
  replica remains frozen. Retries are bounded to two NEW phases per native
  publication, only on timeout, under the unchanged original caller. Explicit
  old-quorum cancellation and original parent cancellation then refuse more
  phases. Old quorum current checks remain errors throughout.
- `native_joint_fresh_phase_refuses_expired_original_caller_with_later_long_budget`:
  uses a real short parent token at actual preparation-round construction,
  waits for that caller deadline, creates a later live token, and verifies the
  phase API still refuses with timeout and all actual freezes remain held.

The original complete-joint assertions and final participant set remain. No
old timing failure or passing unrelated recovery test is relabeled as coverage
of this new source. Existing changed-config/open/UID/catalogue/higher-CQ checks
are retained in the real revalidation chain; dedicated new dynamic negatives
for each mutation and writer-generation ABA remain to be executed/expanded.

## Explicit next boundary

A resumed publication quorum deliberately refuses `acquire_local_open`.
Publication is not catalogue activation, new serving permission or release.
This first step cannot turn the original full joint test green by itself.

The next producer must independently attest the exact monotone catalogue
transition. In particular the image target's capture epoch can change at
activation, making its old preparation catalogue digest stale; arbitrary digest
changes must never be ignored. Once a participant actually releases its freeze,
remaining phases cannot count its old Frozen owner. They need an actual current
activated/open receipt and writer generation, or must fail closed. The existing
image-release and survivor runtime owners also retain their old operation
lifetime. Cold or expired reattestation is not solved by this same-open phase.

Lower-R old-voter repair, missing new-voter post-finalization repair, learner
source replacement, origin-down recovery, post-joint NoData, remote joint RPCs,
and automatic operator/controller convergence all remain explicit open gates.
