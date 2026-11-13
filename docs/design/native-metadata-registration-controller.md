# Native metadata registration controller source boundary

Source batch based on private `ba868e9ff`. The private controller and process
command regression are source only: no compile, executed registration or data
enrollment result is claimed. The existing command-path audit is retained in source `632f33a7`.

The first vertical ends when the actual owned metadata consensus store commits
the exact new Active, unassigned metadata Learner identity. Queue insertion,
bootstrap configuration, and a successful socket exchange do not establish that
result. Learner birth binding, protected metadata backend creation, catch-up,
promotion, data placement and native Serving remain separate producers.

## Owners and listener transition

The source adds a retained `NativeMetadataControlHost` around the existing
`NativeInitialConsensusLaunch`, its listener, and a bounded registration
controller. A transition object owns these inputs before its first fallible
operation. Its finite `advance(parent)` first stops and drains the actual old
listener, validates the sealed drain against the same service and RangeWriteStore
open, and revalidates the actual protected metadata owner against that service's
current DB-open guard. It then starts a **new** immutable metadata-control
listener. Bind failure or cancellation retains the stopping/drained handle,
metadata owner, controller and service for an explicit retry.

The old captured initial/learner modes do not change. The new mode is fixed for
its entire listener lifetime, including expiration of a Weak controller. Its
allowlist comprises Ping, existing consensus/read/HostMQ operations, existing
RegisterMetadataLearner and FetchBootstrapSnapshot. It refuses promotion,
schema export and all data operations. Missing or expired native controller
state refuses; it cannot fall through to generic dispatch.

Controller ingress executes after actual transport authentication and before
service/DB latches. It retains a Weak listener-side provider; the phase owner
holds the strong controller. A generic metadata store constructor is never used
to create or resume this controller.

## Registration intent and bounded queue

The chosen policy is the existing cluster-node-CA self-registration contract:
an unknown caller can register only its own ID, with the exact TLS leaf pin,
no pending pin, and no existing conflicting identity. An actual currently
registered Active metadata Voter may administratively request another identity.
This is an explicit admission policy choice, not a consequence of adding an
RPC to an allowlist. This deliberately trusts the cluster-node CA in the existing CFT threat model;
it is not application-client admission or a new security guarantee.

Both paths accept only a validated Active metadata Learner with nonzero
incarnation, pinned endpoint, no native birth and no assigned range. Existing
Active identities are exact-idempotent; a different incarnation, address, pin,
birth, capacity or labels is a conflict. This first controller refuses automatic
decommissioned-ID resurrection and any rotation/promotion side effect.

The source uses a controller-private in-memory queue bounded to 32 intents and 1 MiB of
complete serialized entries. Bound before cloning/checksumming. Queue entries
carry the actual authenticated ingress identity, original request/budget and
exact stable intended node. Same-ID identical requests coalesce; conflicting
requests do not replace one another. No raw DTO constructor mints authenticated
ingress. Expired unproposed requests are not published using a fresh controller
budget; a caller supplies a new authenticated retry. Queue state is not a vote
or a durable decision.

The existing DistributionStore join validator can be reused through a narrow
ephemeral planning constructor derived from the actual committed configuration.
Do not initialize an independent distribution topology file. A validated
DistributionConfig template is configuration data needed by the existing
BootstrapSnapshot contract, not membership authority or an instruction to open
a generic store. Its cluster/bootstrap/RF/local ID/incarnation/address/pin must
match the actual owned configuration.

## Finite CONFIG consumer

One controller operation may consume a bounded batch at a time; another
operation cannot hold the queue lock while waiting for it. The operation uses
one original capped parent deadline throughout:

1. Read the queued intent without service/DB/metadata locks across network work.
   Acquire an actual fresh **own leader** HostMetadataReadObservation.
2. Obtain the actual service DB-open guard briefly. Under the actual metadata
   guard, require the same protected owner/current DB, actual Leader role,
   exact term/read-index/committed C/restore, current ingress identity/pins, and
   the intended unassigned learner constraints. All current voters must retain
   their actual enforced native birth bindings. Plan only the one registration.
3. Propose using the actual store's normal validation and old-voter commit path.
   If an identical proposal is pending, validate the actual checksummed durable
   MStore file, StoreUUID, C, term and exact body before re-fsyncing that same
   index. Share the existing native learner-enrollment retry helper. A conflicting
   pending proposal is a retryable conflict; no override or rollback occurs.
4. Release metadata, then replicate through the accepted-per-peer frontier
   heartbeat controller. Native physical-owner ACK sealing, current membership
   rechecks, and current-term barrier requirements remain intact.
5. Obtain separately fresh own-leader HostMQ under the original deadline.
   Revalidate the actual local owner/current C/term and require the exact learner
   identity in committed C with commit index covering the actual proposal.
   Only then produce a committed registration result and remove the intent.

Timeout after persistence preserves the actual proposal. A reply loss does not
remove a decision or allocate another identity. On retry, committed exact
identity succeeds; pending exact identity resumes; conflicting state refuses.
After a process restart only unproposed queue entries are lost and resubmitted.
Actual pending entries are discovered from the retained owned metadata log.

The old wire Register response remains committed topology data. It may still
show the prior configuration if the finite attempt has not committed. A client
must poll BootstrapSnapshot and match the exact identity; an accepted queue
reply is never exposed as a completed join. No new request arms or wire version
change is needed for this first vertical.

## Command consumer and tests

The smallest actual process control adds a metadata-control transition command
to the existing four-process native startup driver. All processes construct and
retain their own actual factory, service and protected MStore. The controller
command calls the consuming host API; the parent invokes the existing actual
TcpClusterBootstrapClient to fetch, register and poll over mTLS. No test calls
propose_topology to register the fifth identity.

Required source controls are:

- Four real owned metadata processes transition through actual drain, then an
  authenticated fifth registration becomes committed on the old voter quorum.
  Exact duplicate retries allocate no new CONFIG entry; birth remains absent,
  metadata role Learner, range replicas and data WAL unchanged.
- With an unavailable quorum, Register cannot yield a joined identity. Restore
  actual peers and retry the same request; an existing pending proposal resumes
  at its real index. Conflicting incarnation/pin and arbitrary birth payloads
  refuse without changing pending/committed state.
- The old immutable listener refuses registration, and failed bind/cancel during
  handoff retains actual owners. A new control listener continues to refuse
  promotion, ordinary Get/Scan/write, schema export and raw imports.

A public Core facade is still needed to select and retain this lifecycle from
real CLI/Serve. It should expose typed finite operations and status/shutdown,
not raw DB/service/MStore Arcs. Generic CLI provisioning and PGwire's generic
MetadataConsensusStore::open remain invalid native substitutes. The process
driver is an actual command-driven local-host test, not a released CLI command,
four-to-five data result, multi-host/Azure qualification, or automatic recovery
claim.

## Implemented source scope and validation target

The private ingress consumes a receiver-only mTLS envelope seal, validates the
full node bound before cloning, and rechecks current actor incarnation/pins under
the actual owned leader. Queue entries are bounded to 32 and 1 MiB complete
serialized ingress data; each node is bounded to 32 KiB. Each incoming register
attempt consumes its own queued identity. Concurrent ingress receives only
committed CONFIG data and retries registration to drive its own identity;
there is no background queue scheduler in this checkpoint.

The controller calls the common actual pending-entry re-fsync helper shared
with observed learner birth enrollment. Node observation timestamps from a
retry do not replace the original persisted proposal body/audit. Already
committed matching identities return actual CONFIG and retain their birth,
role and placement. Data operations never dispatch through this controller.

The focused source regression is
`native_metadata_registration_four_process_commands_commit_exact_learner`.
It uses real child-process startup/transition commands and the existing TLS
bootstrap client, tests old-listener refusal, actual unavailable quorum,
post-fsync interruption/pending retry, duplicate/config-conflict checks and
continued Scan refusal. Test-only interruption occurs after the actual store
has returned its fsynced proposal and before replication; it fabricates no log,
term or acknowledgement. No Cargo has run for this source. Formatting,
`git diff --check` and source-version synchronization checks are the only local
checks authorized here. Public Core facade/CLI wiring remains to be implemented
separately; these are not released commands or a completed operator chain.
