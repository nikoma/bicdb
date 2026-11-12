# Finite enrollment of an already registered native learner

This source adds the bounded step between an authenticated fifth-factory birth
observation and committing that birth in metadata. Registration, owned metadata
backend creation/catch-up, promotion, data handoff, ordinary listener release,
and an automatic operator loop remain separate and unimplemented by this step.
There is no new wire message or version bump; it uses private protocol 27.

`MetadataConsensusStore::propose_observed_native_learner_enrollment` requires
both an opaque `NativeLearnerBirthObservation` from the genuine RPC and a
SEPARATELY fresh own-leader `HostMetadataReadObservation`. A response DTO or a
configuration proposal cannot call either typed producer. Before proposing,
the store checks its actual native owner/current voter identity, actual Leader
role, requester and leader identity, term, exact committed index/read index,
committed C and restore lineage against the fresh MQ. Every old active voter
must already carry the enforced native birth contract.

The birth observation is rebased by checking only its authenticated identity
facts against CURRENT C: same cluster/bootstrap/restore, original requester
incarnation, current target Active Learner, no assigned replica, exact target
incarnation, actual presented TLS pin, schema and native birth. No old topology
is installed. The proposed topology clones current C and changes only that
registered learner's native birth field and the required generation. A different
already committed birth refuses overwrite. An identical committed birth is a
no-op; it creates no metadata backend or vote.

For a retry before commit, an exact pending proposal is eligible only when the
owned store's checksum-validated actual file contains the same entry identity,
term, body, native StoreUUID and committed C/index/restore. It re-fsyncs that
entry without allocating another index. A purely in-memory entry, changed body,
old term or conflicting pending topology is insufficient. Successful persistence
is still followed by the original cancellation/deadline checks; an error does
not delete any potentially durable proposal.

`NativeInitialConsensusLaunch::enroll_registered_native_learner` performs the
finite controller sequence: caller HostMQ, actual learner birth RPC, a new
leader HostMQ, same-service/actual-owned-store proposal, existing real-voter
heartbeat replication, and committed binding verification. Network work occurs
outside service/DB/metadata latches. One original five-second operation token
caps all children; a new MQ cannot revive an expired original birth fact. The
returned non-serialized result is an audit of committed identity binding, not
metadata vote, data readiness or serving authority. The fifth listener remains
permanently provisional and has no owned MStore.

Three new source controls use four actual protected, fsynced native metadata
stores and real mTLS listeners plus the fifth fresh factory/provisional listener:

- Commit the observed second real factory's birth, preserve local UID/no rows/
  no native floor/no writer/no metadata initialization, and verify same-birth
  idempotence. A held observation of the first real factory cannot overwrite
  it. Legitimately committed pin and incarnation changes reject stale facts.
- Cancel an original genuinely acquired birth context, and separately let a
  genuinely acquired short-deadline context expire. Retain independently live
  current leader MQ and check refusal before held service/DB latches and before
  any proposal/state change.
- Refuse a different actual pending topology proposal; then persist the genuine
  birth proposal, acquire a new MQ, retry exactly that persisted entry and let
  the finite controller commit it without another log index.

These controls are SOURCE ONLY at this checkpoint. Formatting and diff checks
are not runtime validation. The exact prior metadata ACK cohort at source 983
passed the delayed pre-binding/direct owned and real TLS controls, but does not
validate these new methods. The independently reproduced generic Raft matched-
frontier ACK issue and its separate correction are not qualified by this flow.
No HA, Azure causality, automatic joining or first-user-ACK claim follows.
