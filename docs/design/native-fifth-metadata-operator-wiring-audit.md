# Fifth metadata member: source wiring audit

Source: `58b58946c`. This is a read-only source assessment and proposed test/API
contract, not an executed operator result, automatic join or data qualification.
No Cargo, provisioning or new feature producer was used for this audit.

| Stage | Existing source / symbol | Concrete gap |
| --- | --- | --- |
| Initial four | `distribution_native_host/provisional_listener.rs`: `NativeInitialConsensusLaunch`; `metadata_startup.rs`: `elect_initial_metadata`, `heartbeat_initial_metadata`, `enroll_initial_writer` | Explicit bounded calls exist; no native runtime scheduler/controller owns the complete workflow. |
| Register fifth, unbound | `distribution_transport.rs`: `RegisterMetadataLearner` dispatcher; `distribution.rs`: `queue_metadata_learner` | Initial metadata-only listener excludes registration. The launch supplies no `metadata_distribution_store`; widening the allowlist alone would still fail. Queue success returns the previously committed C, not committed registration. |
| Drain queued mutation | `distribution.rs`: `fork_ephemeral_with_pending_metadata_mutations`; `bicdb-pgwire/checkpoint_driver.rs`: staging/propose/replicate/publication loop | A consumer exists in pgwire. The native bootstrap host has no equivalent controller loop; the current learner fixture directly proposes registration. |
| Actual fifth factory | `distribution_native_host/learner_factory.rs`: `NativeUnassignedLearnerFactory::{create,reopen_uninitialized}` | Actual learner Host MQ, fresh schema/local UIDs and strict pre-metadata continuation exist. They do not commit birth or expose a voter. |
| Actual birth observation | `distribution_native_host/learner_birth.rs`: `NativeUnassignedLearnerHost`, `NativeLearnerBirthObservation`; `distribution_transport/learner_birth.rs`: `acquire_learner_birth` | Sealed actual TLS factory facts exist. No controller consumes them into a separately current leader-side enrollment proposal. |
| Commit physical binding | `learner_factory.rs`: `enrollment_proposal`; `distribution_consensus.rs`: `propose_topology` | First method produces local configuration data; second is a low-level consensus proposal. Neither is the missing typed remote-fact/current-leader composition. |
| Same-service learner metadata | `NativeUnassignedLearnerHost::stop_and_drain`; `distribution_consensus/native_metadata_store.rs`: `initialize_native_owned`, `reopen_native_owned` | No production host transition retains that service/store, detaches the provider only after a genuine drain and attaches the actual owned metadata backend/provider to a new immutable metadata-only listener. |
| Catch-up and promote | `MetadataConsensusStore::{handle_append_request,record_append_response}`; `require_native_learner_promotions_ready`; `PromoteMetadataLearner` dispatcher | Current numeric progress guard needs post-binding actual-birth ACK provenance; see delayed-ACK schedule below. The queued promotion dispatcher is also excluded/unwired on the initial native listener. |
| Ordinary data | `distribution_data.rs`: `initial_consensus_only`; captured listener allowlists | No listener may broaden in place. Metadata membership alone does not assign a data replica, create native R0, establish Ready/Serving or produce an application ACK. |

Paths above are relative to `crates/bicdb-core/src`, except the named pgwire file.
Bootstrap discovery remains CONFIG data. Existing initial-all-C acceptance is
not permission to add a learner to an existing cluster.

## Smallest proposed enrollment method

The following API is proposed, not implemented:

`MetadataConsensusStore::propose_observed_native_learner_enrollment(actual_open,
fresh_leader_host_mq, business, sealed_birth, original_parent)`

It runs bounded local checks under the actual metadata guard, with no network
or DB-lock acquisition under that guard:

1. Require the actual same-open native owner and current eligible local actor;
   require Leader role. Fresh Host MQ must name this local requester and metadata
   leader, exact current term, committed C/restore and read index. A conservative
   first contract requires equality with the current commit index; concurrent
   metadata progress requires a new proof instead of changing old labels.
2. Keep both original cancellation chains/deadlines live. The sealed birth is
   physical evidence only. Check its actual presented peer against the CURRENT
   learner ID/incarnation and active/pending pins; check native bootstrap/birth,
   schema and restore domain against separately fresh C. Require Active Learner
   with no replica assignment, including no data-learner assignment. Reject an
   existing different birth; an identical committed binding may be an explicit
   idempotent no-op, not permission to reset a store.
3. Clone current C, change only the missing target birth, advance generation and
   call the existing native birth-transition/proposal checks. Preserve unrelated
   members, ranges, credentials and placement. A serialized proposal cannot
   enter this method as evidence in place of the sealed object.
4. Return a pending proposal/result only. Membership is committed only by actual
   Raft replication. Persistence ambiguity or cancellation does not authorize
   deleting a possibly committed proposal. Later steps read actual committed C.

Registration itself needs a similarly bounded native controller surface. Either
wire the existing queue plus an actual queue consumer, or add an explicit typed
local controller method. Do not merely permit the old queued RPC and call that
registration. A new listener must have its own immutable control allowlist after
real stop/drain of the old listener; do not flip captured provisional flags.

The learner transition consumes actual committed binding plus independently fresh
learner Host MQ, checks the same DB/birth/schema and actual drain proof, and uses
the original service/range-store owner. Initialize protected StoreUUID before
countable catch-up; preserve it on strict reopen. Prepared initialization cuts
need explicit same-identity retry/quarantine behavior, never generic missing-file
term-zero recreation or initial-all-C authority. Keep the learner nonvoting until
actual post-binding owned catch-up and a separately committed promotion.

## Delayed pre-binding ACK: required negative

`commit_entry` resets `match_index`, so simply reusing progress already recorded
before birth commitment is blocked. A distinct source-backed schedule remains:

1. An actual unbound legacy learner receives the proposed enrollment entry while
   `leader_commit` still names the prior unbound C. It logs that entry and returns
   a genuine success ACK, still under unbound committed identity. Hold the reply.
2. Other actual voters commit the enrollment, resetting learner progress.
3. Deliver that held same-term response afterward. `record_append_response` takes
   node/term/index fields but no original destination birth or owned-receiver
   proof; its numeric progress can then satisfy the promotion guard.

This is an unexecuted candidate admission gap, not consensus/data-loss or Azure
causality evidence. Preserve the existing premature-promotion negative and genuine
owned positive. A correct fix must bind counted native progress to the CURRENT
physical birth and actual owned receiver/request scope. Checking caller-supplied
installed positions or rejecting all pending own-birth logs is insufficient and
can wedge legitimate log replacement. A delayed old unbound ACK must not become
post-binding owned catch-up evidence.

## Smallest actual TLS five-member acceptance test

Start four actual native metadata stores/listeners with real votes/Appends and
fresh Host MQ; retain exact term/index/C, births and store IDs. Then:

1. Characterize the present queued registration RPC refusal; do not report guard
   success as completed join. After the controller is implemented, register the
   fifth unbound Learner over the actual authenticated surface and commit it.
2. Fifth obtains real Host MQ, creates its actual factory with fresh local UIDs,
   exposes the dedicated provisional endpoint, and controller obtains sealed
   birth facts via real TLS. Assert no birth commitment, MStore or data floor yet.
3. Obtain a NEW leader-local Host MQ and use the typed enrollment method. Test
   canceled/stale leader scope, wrong actual peer/incarnation/pin/schema/restore,
   another existing birth and plain DTO substitution. Commit the genuine binding.
4. Verify the old listener still refuses metadata/data. Stop/drain it, transition
   the SAME service/store to actual owned learner metadata, and receive a real
   birth-bound catch-up ACK. Include the held pre-binding ACK negative above.
5. Commit promotion through actual old/new metadata quorums, deliver the result
   to the learner and verify it can vote only afterward. Strict copied reopen
   must retain physical birth, local UIDs, StoreUUID, term/vote and refusal of
   pre-metadata reinitialization.

Keep all RPC/deadline/persist/drain failure receipts. The test ends at metadata
membership. Data placement, joint-data handoff, native recovery, ordinary listener
release, Ready/Serving, Strong reads and the first application ACK remain separate
gates; this sequence must never be labeled automatic join or operator completion.
