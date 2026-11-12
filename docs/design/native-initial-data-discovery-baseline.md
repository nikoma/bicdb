# INITIAL DATA discovery under service contention

Source audited: `db8ce62047618a6273808490254f3c2964cce881`.
This is a source audit and an unexecuted expected-failure test, not a diagnosis
of the earlier `3ee` CLI failure. That CLI run did not retain its complete
native roots or original per-RPC error frames; its cause remains unresolved.
No production routing, admission, deadline or listener policy changes here.
The independent observer fixture constructor correction is test-only.

## Concrete discovery condition

`distribution_native_host/provisional_listener/registration.rs`,
`NativeMetadataRegistrationController::bootstrap`, first tries the actual
service mutex to capture its listener-open guard. A busy mutex returns exactly
`native bootstrap service busy` before reading the committed metadata snapshot.
The BootstrapSnapshot dispatch is intercepted in `distribution_transport.rs`.

`distribution_events.rs::ClusterEventClient::refresh_members_until` uses the
real snapshot RPC, then real FetchTopology on error within the same member
slice/original request deadline. `distribution_transport/initial_data_listener.rs`
allows BootstrapSnapshot but rejects FetchTopology through its unchanged
unsupported-operation arm. Therefore service contention can make both routing
forms unavailable even if the metadata processes and quorum are healthy. This
is an availability condition, not evidence of a false ACK or lost value.

The new test is
`provisional_listener/tests/initial_data_tests/discovery_latch_baseline.rs::`
`native_initial_data_healthy_metadata_discovery_while_all_data_services_held`.
It uses the existing three actual from-birth factories, actual pinned TLS,
actual initial metadata election and actual INITIAL DATA owners. The n0
initial election is an explicit fixture precondition, not automatic-election
qualification. All actual policies/readiness must precede the first APP ACK.

It captures an n0-led APP ACK and exact Strong value, holds all three actual
service mutexes, and acquires a genuine fresh HostMQ WHILE those mutexes remain
held. The public client then performs its unchanged refresh and fallback. A
second genuine HostMQ must succeed while the locks remain held. A failed MQ
is recorded and fails that distinct prerequisite; a saved leader bit cannot
stand in for this proof. Both committed topologies are compared to the actual
pre-cut snapshot.

The opt-in evidence observer journals every actual public-client frame/error
with its real attempt/request IDs, destination, route generation and timing.
`rpc.jsonl` contains complete framed request/response bytes, not a search
projection; `audit.jsonl` records actual MQ context/term/index and business ACKs.
Both files fsync each record. Before the first observation, the journal
directory and all ancestor directory entries are fsynced; canonical-path
checks exclude every original `dbN` directory from that diagnostic fsync.
The assertion projection must contain the actual
BootstrapSnapshot busy response and actual FetchTopology policy refusal.
`BICDB_TEST_EVIDENCE_ROOT` retains these small files for workflow collection;
without it they are temporary. No TLS private keys or DB copies are exported.
A printed temporary path alone is not persistent custody.

After releasing the locks, the SAME public client must ACK an actual n1-led
key whose value is JSON null, Strong-read `Some(Null)`, and read the original
ACK's exact value. A new actual snapshot must equal the original topology.
Only after those checks and explicit owner stop does the final positive
healthy-discovery assertion fail on the unfixed baseline. This distinguishes
busy discovery from dead members and preserves the null-versus-absence check.
No ignored test, duration extension, synthetic handler response or policy
replica is used. This source is not ready for a production merge while its
positive acceptance remains RED.

A future correction needs an independently checked routing-HINT producer for
this immutable INITIAL DATA mode before the data latch. It must bind the
actual DB open/current Store/birth and authenticated metadata owner; it grants
no MQ, Ready, Strong, mutation or cached-schema authority. This patch does not
implement or widen that producer. Blank/registered receiver and old control
listener allowlists are unchanged. A later positive must also retain actual
APP/Strong checks and refusal of stale hints by real current admission.

## Cross-leader path audit

The source does not require metadata leader to equal the data-range leader.
`provisional_listener/metadata_startup.rs::enroll_initial_writer_retained`
uses actual HostMQ to the metadata leader before installing the same actual
coordinator. The installed writer policy is not an expiring MQ lease.
`distribution_range_consensus/initial_data.rs::recover_initial_native_genesis`
checks actual local range leadership and uses first generic MQ, actual native
read QQ, and final generic MQ under the same reservation before recording
recovered epochs.

`distribution_range_consensus.rs::admit_with_database` checks actual policy,
route/schema/current epoch and recovered state, reserves before mutation, and
then uses native intent, repair, Prepare/Commit/certification. Remote ordinary
status/apply obtains its own real role through
`distribution_range_recovery/runtime/normal_apply.rs::normal_role`.
`MetadataReadProvider::witness_recovery_role` uses local authority if this node
is the actual metadata leader and otherwise forwards to its observed leader.
No self-MQ routing defect was established. The APP dispatch releases its
service borrow before acquiring the application DB read guard. No permanent
service-held-over-origin-network lock cycle was established by this audit.
JSON null has no separate native admission branch.

The client attempt and generic server transaction have different budgeting
boundaries: EventClient slices its request deadline, while generic APP
execution does not automatically inherit the incoming special-RPC role token.
Native repair has separately bounded work. Work can therefore outlive a client
attempt; a client timeout is not a rollback or absence proof. This source
observation is not a causal explanation for `3ee`.

## Physical/domain validation complexity

`db/native_recovery_ready.rs::LocalNativeRecoveryReady::`
`require_owned_materialization_current` calls
`db/range_image_native.rs::validate_source_range_image_lineage_contract`.
This currentness check does NOT scan user rows or walk all history `0..R`.
It rereads actual birth, base anchor, base-history endpoints, certified prefix
and protected joint domain/current+archive/preparation linkage, then checks
open/schema/generations, actual namespace UIDs and portable catalogue hashes.

`db/range_image_bindings.rs::capture_range_image_bindings` visits ALL database
namespaces, sorts names, reads actual UIDs/metadata and clones/hashes the
portable catalogue. Its bounds are 65,536 namespaces and 8 MiB catalogue data;
it uses the current verified schema state rather than rescanning user rows.
Native-admission revalidation checks factory readiness/genesis receipts for ALL
initial birth ranges. Protected record reads also validate reader flags and
bounded serialized records; repeated lineage checks repeat this work.

Full `LocalNativeRecoveryReady::require_current` additionally calls
`distribution_range_consensus/native_abort.rs::`
`require_actual_abort_controls_covered`. Its protected slot inventory reads
ALL ranges' protected abort slots before filtering the selected range: 4,096
slots per range, 3 MiB per slot and 64 MiB aggregate bounds are refusal limits.
Selected control/history/promise/decision checks follow. These are metadata
and hidden-record reads, not application row scans.

`distribution_range_consensus/recovery_state.rs::`
`recovery_range_catalog_sha256_with_phase` serializes/hashes retained raw
command bodies and the selected marker/sparse/installing/abort/joint bindings;
its cost is not just a fixed-size R/H comparison. Physical attestation's
per-decision `export_native_logical_history_step_with_source_pin(index, 1)`
performs full lineage validation before AND after exporting each entry.
Thus retained decision counts multiply actual namespace/catalogue/protected
validation work. Full image row export and full-history transfer/audit are
separate operations with their own data-dependent cost.

Consequently the cost depends on namespace and initial-range inventories,
global protected-slot bytes/count, selected raw payload/catalogue/archive
bytes and repeated per-entry validation. Protocol byte/count bounds do NOT
prove completion in a four-/five-second authority window or for an 8 GiB data
range. This audit makes no measured-performance or physical qualification
claim. If one mandatory validation cannot fit, the solution needs a sound
bounded checkpoint/fresh-purpose continuation, not renewal of expired proof.

## First execution: destination-binding prerequisite failed

Source `286ca38a86cdd175b1430b9d9570456eb94b916c`, run `43276644122`,
Core job `131386760357`: **0 passed, 1 failed, 931 filtered**, 2.88 seconds.
All-target and isolated-consumer checks passed. The artifact retains the
actual first n0 APP ACK, its value, the public RPC journal and the actual
first held-MQ error: `metadata RPC destination differs from actual native
birth/incarnation`. It does NOT establish healthy MQ while held or discovery
unavailability: public refresh and the second APP/Strong lane were not reached.
The failed fixture used the original provisional transport's pre-enrollment
plan. Its destination binding correctly disagreed with the now-enrolled store.

The test-only successor constructs its real HostMQ transport using the already
authenticated committed bootstrap snapshot and the same actual n0 TLS identity.
Snapshot data supplies routing/destination intent only; the real metadata
receiver must still produce fresh opaque quorum authority. No metadata or
listener guard is changed. Original source/result remain separate.
Raw log SHA-256: `232fc5cdc1273c129d9e9c9082f41bad6212dc4302be7b0ab3c213e758ca461d`.
Artifact `13972080317` SHA-256:
`bcad7bd7bf749e4d4a65d5c95dcbaf8650a32b4f40e475135cbfe6743f14a338`.
Its six files are logs/toolchain/source plus exact audit/RPC journals. No DB
or private TLS roots were archived; a client error is not ACK reconciliation.
