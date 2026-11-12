# Native fifth-node enrollment: command-path delta

Read-only source audit at private Root
`f1d1398f738868accc20e3757a33752d5e62b726`. This note updates only the
missing-producer assessment in the existing
[fifth-member wiring audit](native-fifth-metadata-operator-wiring-audit.md)
and [transition audit](native-learner-metadata-transition-audit.md).
Their historical source/results remain unchanged. No runtime edits, Cargo,
command experiment, database open or cloud action were performed here.

**Finite Core enrollment and same-service metadata transition now have source;
ordinary operator commands still do not drive them.** In particular, source
`1d88047c9f1ca53588612b6c831090a25a9ea80a` adds observed enrollment; retained
owner correction `0d17ec62bd7ea13f57421c835fd6b37528b27035` is composed in Root
as `45951aabf4ec8a50a6b5e6241289c680f1562baa`. These are not an executed
command-driven join or a qualified four-to-five system. See the separate
[CI findings](native-milestone1-ci-findings-2026-11-12.md) for exact earlier
passes/failures; they do not validate this final composition by association.

## Actual CLI to host path

| Boundary | Current source and behavior | Remaining producer/wiring |
| --- | --- | --- |
| Init | [`bicdb-cli/src/main.rs`](../../crates/bicdb-cli/src/main.rs), `run_cluster_command`, `ClusterCommand::Init`: builds `DistributionConfig` with generic defaults; optional event collection uses `BicDb::open_with_config`, then ordinary distribution initialization | Native initial-plan/config lifecycle, atomic from-birth factory, complete actual schema/genesis, common initial birth preparation and owned metadata start. No live-root retrofit |
| Join | Same file, `run_remote_cluster_join`: bootstrap snapshot, unbound learner registration/poll, then `provision_cluster_member_directory`; admin-assisted Join also uses the old registration RPC | A native-mode join lifecycle preserving the empty-directory factory order, actual current HostMQ and actual schema bundle. Provisioning an ordinary directory first cannot stand in for a fresh factory |
| Serve | `run_serve_command` calls `serve_with_host_services`. [`bicdb-pgwire/src/lib.rs`](../../crates/bicdb-pgwire/src/lib.rs), `database_config_for_server`, sets distributed storage/replication options; [`checkpoint_driver.rs`](../../crates/bicdb-pgwire/src/checkpoint_driver.rs), `start_distribution_supervisor`, calls generic `MetadataConsensusStore::open` and opens the data service | A public sealed Core native-host facade selected by actual persisted mode, strict loaders and one retained DB/service/range-store/metadata owner. Existing native generic-open guards must continue refusing unsupported paths |
| API accessibility | [`bicdb-core/src/lib.rs`](../../crates/bicdb-core/src/lib.rs) declares private `mod distribution_native_host`; the launch/factory/learner-host/enrollment methods are crate-private. No use of these host types appears in CLI or pgwire | Export a bounded host lifecycle/control facade, not raw service/DB handles or DTO authority. CLI/pgwire must own its lifetime/shutdown and call it |
| Native registration surface | [`distribution_transport.rs`](../../crates/bicdb-core/src/distribution_transport.rs), initial metadata-only allowlist excludes `FetchBootstrapSnapshot`, `RegisterMetadataLearner`, `PromoteMetadataLearner` and `ExportSchemaBundle`. `start_metadata_listener` supplies no distribution mutation store | Separate authenticated native control endpoints and their actual owner/queue consumer. Widening an allowlist alone supplies neither registration nor commitment |
| Existing registration queue | Generic dispatcher checks metadata Leader, calls `queue_metadata_learner`, returns **prior committed C**. PGwire supervisor has `fork_ephemeral_with_pending_metadata_mutations` → `propose_topology` → actual replication/publication | Connect an equivalent bounded loop to the native owned host; report queued versus quorum-committed state distinctly. Do not invoke the generic metadata constructor to get a consumer |
| Background placement | [`distribution_controller.rs`](../../crates/bicdb-pgwire/src/distribution_controller.rs), `BalancedDistributionHostService`, starts the generic `ClusterSupervisor`; its relocation driver uses legacy snapshot/catch-up/promotion/cleanup | Native lifecycle/recovery scheduling and the still-missing remote joint-data handoff/cleanup chain. A configured balanced controller is not that integration |

Paths not otherwise qualified in this note are under `crates/bicdb-core/src/`.
These are source conclusions; the audit did not run Init, Join or Serve to claim
an observed command outcome.

## What the new source actually closes

`distribution_native_host/provisional_listener/learner_enrollment.rs::
NativeInitialConsensusLaunch::enroll_registered_native_learner` is a finite
controller for an **already registered, unassigned** learner. It obtains an
actual caller HostMQ, sealed learner birth RPC, then a separately fresh own-
leader HostMQ before `propose_observed_native_learner_enrollment`. It replicates
through real metadata heartbeats and verifies the committed exact birth. It
neither registers an unknown node nor creates its metadata backend.

`distribution_native_host/learner_birth/metadata_transition.rs::
NativeUnassignedLearnerHost::start_owned_metadata` consumes the learner's NEW
OWN HostMQ, not enrollment's audit. It genuinely drains the provisional listener,
retains the SAME service/range-store open, initializes and retains the actual
protected metadata owner/StoreUUID, and binds a new immutable metadata-only
listener. Cancellation/bind retry retains actual owners and drain state.
`stop_owned_metadata` and `close_stopped_owned_metadata` manage that owned
lifecycle; this is not a data listener or process supervisor.

Unresolved transition gates remain: Prepared initialization cuts quarantine;
there is no learner-purpose exact-C cold continuation when only its hash remains,
no qualified all-owner cold host loader after metadata initialization, and no
same-UUID term/vote rollback proof. The old provisional allowlist must never
broaden through Weak expiry, an enrollment result or a mode boolean.

## Smallest real command-driven metadata vertical to implement

The following are proposed extensions to existing command lifecycles, **not
currently available command syntax or passing tests**:

1. **Initial native Serve/Init.** A public Core facade owns four real processes,
   atomic factories, schema/genesis and original initial preparation. It retains
   their actual owned metadata, TLS provider and accepted-progress heartbeat
   scheduler. The CLI/pgwire entrypoint calls it; no test returns service Arcs.
   Install required writer policies before any later application ACK. Bootstrap
   configuration alone remains data, not an initial-voter capability.
2. **Administrative registration.** Initially use an operator action authenticated
   as an existing registered voter to request the fifth ID/incarnation, exact
   endpoint and certificate pin. The native control handler/queue consumer must
   produce actual committed Active/unassigned-Learner C, not acknowledge queue
   insertion as join. Route to the actual metadata leader; acquire fresh leader
   HostMQ before protected publication and revalidate after replication. Lost
   replies retry the same identity/request, never replace a committed birth.
3. **Fifth factory and observed binding.** The fifth command obtains its own
   fresh HostMQ after registration and creates `NativeUnassignedLearnerFactory`
   with empty initial ranges. Its current `create` also requires an actual
   verified `ClusterSchemaBundle` matching committed schema; wire a bounded
   native schema-export control path, not a caller fingerprint. SQL structural
   records are explicitly unsupported by this factory. Start its real provisional
   birth endpoint. The leader's control command invokes the existing finite
   enrollment method; no client/test writes `native_store_birth_v1` or submits
   an arbitrary low-level topology proposal.
4. **Owned catch-up and promotion.** After observing committed binding, the fifth
   command obtains a NEW own HostMQ and calls `start_owned_metadata`. The leader
   drives actual snapshot/Appends and counts only current-birth, protected-owned,
   matched-frontier ACKs. Implement the missing native promotion controller,
   gated on actual catch-up plus separately current leader authority and real
   old/new metadata quorum commitment. Client-reported installed counters alone
   cannot supply that gate. The fifth becomes a voter only after learning its
   committed promotion; no data floor is created by metadata membership.
5. **Persisted restart/status.** Command status identifies registration, birth
   binding, protected backend, catch-up, voting and refusal states separately.
   Restart strict Registered state with the same birth/UID/StoreUUID/term/log;
   unsupported Prepared/cold cuts remain explicit quarantine. A background owner
   repeats bounded work with actual authority and retains partial owners, rather
   than a test sequencing private methods. Test wrong identity, leader transfer,
   lost reply, shutdown/bind cuts and process restart through those commands.

### Join authorization is a separate boundary

The generic `authorize_cluster_data_caller` already permits unknown-node
bootstrap fetch and same-ID self-registration over cluster-CA-authenticated TLS;
self-registration must match the presented leaf fingerprint and cannot pre-stage
a pending pin. Existing registered callers must match current membership pins.
This is not the same as the native listener permitting those requests: its
immutable allowlist currently refuses them.

For the first native vertical, administrative pre-registration avoids silently
expanding unknown-peer admission. If self-service Join is required, define an
operator-authorized invitation bound to cluster/bootstrap, node ID/incarnation,
endpoint/pin, expiry and single-use/idempotent request identity, or explicitly
adopt and document the existing cluster-CA admission policy. A certificate or
bootstrap snapshot is not birth enrollment, metadata voting or data permission.
Keep actual destination birth/open fencing, original cancellation chains and
fresh post-observation leader MQ; cached leader hints only choose where to ask.

## Where this vertical must stop

Even a successful command-driven metadata join does not complete four-to-five
data expansion. Ordinary endpoint release requires another genuine stop/drain,
installed current policies and real recovery runtime wiring. Current initial
writer enrollment retains a coordinator/guard but intentionally leaves its
listener metadata-only. The pgwire wrapper's `RangeRecoveryRuntime::new` also
needs its actual participant transport/supervisor integration; a private API is
not automatic recovery.

The [joint cross-process audit](native-joint-cross-process-gap-audit.md) still
applies: local Arc-based OLD closure, full history/image, durable dual DATA
majority, NEW preparation/publication, activation/release and protected cleanup
need actual remote producers and coordinator crash recovery. Fresh fifth data
placement, old ACK reconciliation, new-leader ACK/Strong reads, lower-R and
missing-new-voter repair, origin-down and post-joint NoData gates remain separate.
Only an actual command-driven five-process data vertical can precede independent
VM/online-copy/convergence/capacity qualification. No private proof-component
result, source merge or metadata-voter status is substituted for those results.
