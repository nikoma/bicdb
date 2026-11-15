# Native remote NEW materialization and read-pin handoff

This source slice follows the original S2 export retirement. It adds real
participant-owned INITIAL/Registered receivers and native-pinned mTLS calls.
It is **not yet compiled or runtime-tested**, and is not a durable decision,
Open, Serving, production, physical scale-out, or throughput qualification.
The inherited prototype source version is 1.0.502-beta; this is not a claim
about the newest release's integrated behavior.

The prerequisite exact OLD close/prefix provenance repair is the separate
source leaf `331c12a52fe3efa0d89021e33f8944c8acd4886a`. Its native record DATA
does not construct OLD/NEW owners or metadata authority.

## Actual operations and original custody

`distribution_transport/native_new.rs` performs an actual native-destination,
numeric-address, mTLS exchange. Only that code constructs the opaque remote
frame and independently fetched fact-set wrappers. Each destination is an
allowlisted OLD/NEW member with its current incarnation, certificate and
enrolled native birth. Frame/request sizes and member counts are bounded.

`registration/remote_new_metadata.rs` obtains the recipient's genuine current
CommittedHandoff Role from the actual initiating peer, plus an independent
OWN observation and the native current-MStore/member/version point. A
nonleader donor is never promoted into a metadata leader or given a fabricated
Role. Readonly callbacks obtain current OWN and prove their retained physical
owner; they cannot create a missing participant or issue mutation authority.

INITIAL operations require the original node-local OLD observation, writer,
service/open/slot and source process. Registered operations require the
original target A, completed actual native image/base/catalogue, original
hook, native birth and complete source Manifest. No receiver constructs a
missing OLD closure or replacement target attempt.

The original participant retains one immutable materialization snapshot before
postchecks or replies. NEW survivors retain their actual OLD native record;
the target's snapshot comes from its actual completed native image. Each
local database B stays in its own domain; range R/H0/H must match the fixed
source image. Retiring OLD-only participants are not NEW votes.

## Quorum before pin handoff

The distinct `NativeRemoteNewRound` begins with a genuine metadata-read plan,
the original collection request, and actual transport frames. Its exact NEW
predicate is the existing target + intended new leader + distinct majority.
It cannot construct `NativeJointNewRound`, local owner Arcs, a local QQ,
`NativeJointPreparedDecision`, or a writer grant.

Prepare requests carry immutable snapshot digests. **Every recipient fetches
the actual selected NEW snapshots independently** over pinned mTLS before
acquiring its final fresh Role/OWN/MStore point. Network calls occur outside
participant/table, service, DB, native and publication guards. The complete
exact fact-set intent, actual callback observations, fresh request ownership
and original native owners are retained before the first pin-reference take.
After uncertainty, another quorum, Manifest, A, phase or source session cannot
replace that intent.

The scope is bounded to 128 OLD/NEW members, 2 MiB per request and 4 MiB per
encoded reply, 64 exact request identities, eight prepare attempts, and 8 MiB
of retained encoded Manifest/request/frame evidence per participant. This is
an encoded-evidence bound, not an RSS guarantee. Exhaustion refuses without
evicting an earlier attempt. Metadata Role/OWN objects are additionally bounded
by the attempt count and their existing metadata limits.

## All actual read-pin ownership references

There is one exclusive native pin per range, not two simultaneously minted
pins on a target. Its real ownership references are accounted separately:

1. The S2 source capture shares the original OLD pin. Its actual terminal
   retirement removes the export reference, preserving the OLD owner/freeze.
2. The actual OLD observation then closes capture acquisition under its pin
   mutex and permits its take only with a unique original Arc. A still-live
   source export makes this refuse.
3. On the target, the readonly export owner and genuine NEW materialization
   share the **same** exclusive pin Arc. The clone occurs only inside the
   genuine participant constructor after fresh Role/OWN/native/A checks,
   through a sealed borrowed issuance object. There is no generic raw-pin
   getter or DTO pin constructor.

Before closing target export-pin acquisition, the original target independently
fetches the source's already-completed S2 Describe terminal over native-pinned
mTLS, bound to the same MF/A/source session. This prevents a still-live capture
from depending on a callback whose pin acquisition was prematurely closed.
That sealed terminal observation grants neither OLD release nor Serving.

The target closes future acquisition under its original owner transition
lock before draining the readonly reference and checking uniqueness of the
materialization reference. A strong count alone is not the proof. The short
native metadata publication guard covers only final memory checks, exact
reference takes and progress atomics. Actual pin destructors run after that
guard; the final destructor also runs outside the owner transition lock.
Original freezes and native refusal owners remain retained throughout.

Each actual take records progress before a postcheck can fail. A fresh request
may reconcile that same original phase and exact fact set; it does not revive
the expired request or issue a new native/row transaction. Acquisition is not
silently reopened on a retry.

## Focused actual controls added, not executed

`source_export_retirement_tests/remote_new_tests.rs` uses the genuine existing
S2/fifth-node native-image fixture and the production mTLS operations. It does
not pass a private local OLD/NEW owner to the requester. Five controls cover:

- actual materialization, independent callbacks and all required pin owners;
- a changed but correctly encoded fact digest refused before a take;
- cancellation of the real retained attempt before the first take;
- cancellation after actual reference removal, then a fresh same-phase retry;
- a different valid NEW majority refused after the original intent is retained.

Operation ledgers are written before invocation and retain the primary result
before assertions. Original MF/source-frame history, acknowledged seed records,
native OLD records, original target state and range-gate observations remain
available. These are source-defined tests, not passing evidence. Their source
and compiler-selected inventory must be recorded by the eventual hosted run.

## Remaining genuine producer boundaries

This handoff is **RAM-only original-owner custody**, not a durable native
DECISION journal. A cold process cannot reconstruct permission from its DTOs.
Unknown/cold continuation stays fail-closed. The remaining chain needs real
cross-process producers for complete OLD/canonical obligations and NEW quorum
decision provenance, same-body native decision publication on both OLD and NEW
majorities, committed metadata decision stages, participant preparation,
activation, Open, and phase-authorized OLD/target freeze custody retirement.

Existing local decision/phase objects consume real local service/owner Arcs.
They must not be fabricated from this remote aggregate. Likewise, this slice
does not give an existing Joined receiver an additional range by casting it
as Registered; that requires its distinct actual adapter. There is no automatic
controller driver, full 64-range cap retirement, cold recovery, Strong read,
ordinary PostgreSQL application, or stable five-node throughput result here.

The passing diagnostic-only run at `33880aea5bc4c400f70bfee9e9b0020eaf027452`
(15/0, run 43553888444) predates this producer. It does not test these changes.
Its predecessor run 43550673003 remains failed 14/1 with the original OWN-case
poststate inconclusive. Neither result is relabeled by this implementation.
