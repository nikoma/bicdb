# Native operator four-to-five integration audit

This is a read-only source integration audit, not a running cluster result. It
identifies the missing production path between the private native reliability
components and an automatic four-to-five-node operator candidate. No build,
database open, process experiment, VM action or deployment was performed for
this audit.

## Exact source scope

| Source | Exact revision | Scope |
|---|---|---|
| Public main observed by this audit | `f3a063902d2fe316c29264fe20549c2fdd270242` / `1.0.478-beta` | Public operator source. Topology format 1 and cluster data protocol 19; no private native factory/enrollment/read/image implementation. The source version does not imply a new native binary or qualification. |
| Private implementation inspected | `6641e13bc2a1f95773cc416929a1977b91340ffa` / `1.0.471-beta` source identity | Private composed engine and host source. The functions described below are source components, not a delivered operator feature. This audit does not attribute later changes or tests to this revision. |
| Preserved interrupted-image baseline | `8707ffa42fc0b20ff30e0dd5ebd8989e20207a0d` / `1.0.471-beta` | Separate characterization: test PASS, exercised scenario `FAILED_UNSUPPORTED_COLD_RESUME`. One genuine ACK remains reconciled on the source; original uncertain operation 2 was resolved, not newly acknowledged. |

Private revisions are local Git objects. Their paths and symbols below identify
reviewed source; they are not claims that those files exist on public main.
Later metadata guard changes and their validation are outside this frozen audit.

## Integration findings

| Finding | Source conclusion | Operator runtime verdict |
|---|---|---|
| Native factory/bootstrap/enrollment | NOT WIRED | NOT TESTED here |
| One fully configured shared recovery runtime | NOT WIRED by the private pgwire host | NOT TESTED here |
| Explicit strong value/absence producer | NOT IMPLEMENTED | NOT TESTED here |
| Cross-process native image source/install protocol | NOT IMPLEMENTED | NOT TESTED here |
| Joint old/new **data** quorum handoff activation | NOT IMPLEMENTED | NOT TESTED here |
| Automatic native relocation and guarded cleanup | NOT WIRED | NOT TESTED here |
| Accepted-but-unfinished image cold continuation | Separate preserved baseline FAILED | No fix or recovery success claimed here |

### 1. Fresh factory and operator bootstrap

Public `crates/bicdb-cli/src/main.rs` handles `ClusterCommand::Init` with generic
`BicDb::open_with_config` for the application-events collection, followed by
`DistributionStore::initialize_at`. `DistributionConfig::default()` supplies no
native image bootstrap UUID. `run_remote_cluster_join` registers an unbound
metadata learner and then calls `provision_cluster_member_directory`.

Private `db/range_image_native.rs` provides actual
`BicDb::create_new_range_image_database`, `open_range_image_database`, durable
birth, and `initialize_range_image_genesis`. Private
`distribution_native_admission.rs` provides
`ActualNativeStoreEnrollment::from_actual_factory` and an initial-plan/learner
binding builder. These APIs remain crate-private and are not invoked by the CLI.

The smallest production addition is a Core hosted-native facade used by CLI and
pgwire. It must own atomic fresh-directory creation, schema and actual genesis
initialization, immutable bootstrap/member bindings, strict reopen and expected
birth verification. A joining node must publish an authenticated observation of
its actual local factory birth while it is an unassigned metadata learner.
A decoded birth record alone cannot grant factory or read authority. Initial
voter bindings must precede fresh metadata-store creation; generation 1 is not
permission to upgrade a live unbound voter. Existing generic roots must not be
automatically adopted as fresh native stores.

Remote join also needs a coherent ordering: the atomic factory cannot be called
after metadata provisioning has already created its supposedly fresh database
directory. Metadata-learner enrollment/promotion and data-range learner placement
are separate transitions.

### 2. Shared runtime and startup readiness

Private `bicdb-pgwire/src/checkpoint_driver.rs::start_distribution_supervisor`
constructs the metadata provider and calls
`start_cluster_data_server_with_metadata_read_provider`. In private
`distribution_transport.rs`, that wrapper creates `RangeRecoveryRuntime::new`,
whose `participant_transport` is `None`. It does not call
`with_participant_transport`, or give the host the same runtime instance for
local recovery and ownership coordination.

A production facade must construct one service, provider, configured participant
transport and runtime, then share their actual instances with the listener,
supervisor, coordinator and shutdown lifecycle. Existing authenticated recovery,
historical certification/Apply, native Ready/Complete, protected activation
decision and ordinary native Apply components can be reused behind that facade.
Recovery must run outside DB/service locks and before ordinary mutation permits;
transaction admission may verify readiness but cannot synchronously perform
network recovery while holding its caller's DB guard. Strict reopen must
reconstruct actual proof state rather than manufacture genesis or retain stale
in-memory readiness.

### 3. Explicit strong point read

Private `distribution_events.rs::execute_cluster_event` still implements Get
with ordinary `db.get`; `ClusterEventOperation` has no StrongGet variant. Private
`distribution_native_read/quorum.rs` yields `NativeReadQuorumCandidate` and an
enrolled-birth-checked result. Neither yields a successful value/absence permit.

An additive request and handler must consume actual fresh metadata and sealed
matching current-data observations, validate enrolled births, wait for the
**target-local** MVCC visibility bound outside locks, acquire a suitable actual
snapshot, and perform final native/configuration/namespace/schema revalidation
before returning a value or absence. Range index R, source DB sequence and native
page XID cannot substitute for the local visibility bound. The actual snapshot
and final fresh metadata/read validation remain missing producers. Preserve
legacy Get behavior; StrongGet must never fall back to it. A permanently
unavailable implementation would not satisfy the healthy-positive requirement.

The transport must intercept this flow before the existing ApplicationEvent
handler acquires its DB read guard. This point-read addition would not by itself
qualify arbitrary SQL, scans or all application consistency levels.

### 4. Native image transfer across processes

Private `RangeImageCapture` owns a local database handle, frozen mutation permit
and source read pin. `CompletedNativeRangeImageSource` retains that actual source.
`NativeRangeImageInstaller` consumes
`TentativeNativeRangeImageSourceAuthorization<'_>` derived from it. Private
`distribution_transport.rs` has no native image/source/capture/install/handoff
request arms. Existing local source and target objects therefore cannot simply
be moved into separate processes.

Implement an authenticated source-session protocol: actual source offer/cut,
bounded zero-through-R history export, bounded row stream, completed seal,
target staging/auditing, destructive install only after compatibility proof,
actual target-local visibility verification, native receipt and catalogue
adoption/finalization. Source-side actual objects and pins must remain owned by
the source runtime; the receiver must consume sealed, request-correlated remote
observations instead of deserializing a local authority. Session, attempt,
member birth/incarnation, schema, restore lineage, full history commitments,
extra-row detection, retry bounds and original deadlines must remain checked.

The preserved `8707` baseline adds a real restart requirement: after target clear
but before copy, its durable intent survives while ephemeral source ownership
does not. Fresh collection obtains only one of two required observations, and
the target reports no retained source freeze. A separately durable accepted-cut
and narrowly scoped same-attempt continuation protocol is still needed. The raw
intent, target `None` native floor, or stage labels cannot authorize a new cut.

### 5. Data-quorum handoff and automatic controller

Private `HandoffCutRound` and permanent old-voter promises provide a source-cut
foundation. They do not provide final membership activation.
`db/range_image_native/serving_configuration.rs` explicitly rejects handoff
configuration activation by the same-voter recovery producer;
`distribution_range_consensus/native_ready_prepare.rs` makes the same distinction.
No handoff-promise removal/completion producer was found.

The automatic `TransportClusterRelocationDriver` still uses ExportSnapshot /
ApplySnapshot and DB-sequence catch-up. `distribution_supervisor.rs` promotes
from ReadyToPromote, then begins source cleanup. That path must be replaced for
native plans by a coherent per-plan state machine:

1. Commit the exact learner plan and enrolled target identity.
2. Drain/fence the old data quorum, resolve its durable obligations, and select
   an actually qualified source image and logical cut.
3. Install and verify the target's actual native image/history and local
   visibility; repair required old/current voters safely when necessary.
4. Obtain durable matching **old and new data quorum** transition evidence,
   recover the intended leader, and activate the exact new configuration.
5. Permit source cleanup and final membership publication only after that
   evidence; then prove a healthy remaining quorum can repair and accept a new
   write after another tolerated voter failure.

Metadata quorum, placement counts, DB-sequence catch-up and a native receipt on
one target do not substitute for this decision. Existing same-voter activation
must not be broadened into a membership-change shortcut.

## Smallest executable delivery sequence

Each step should add its positive and negative controls before broad validation:

1. Three **separate node processes** use the production host facade and actual
   TLS. Exercise fresh enrolled bootstrap, genuine native ACK, replica apply,
   strict restart and a successful explicit strong point read.
2. Transfer one nonempty range across those processes through the actual image
   wire path. Reject incompatible native/catalogue history before clear, and
   preserve local UID and target-local MVCC visibility.
3. Compose joint data handoff, cleanup, a tolerated remaining voter failure,
   repair, and a new ACK/read. The retained cleanup-first witness must become a
   healthy positive through this protocol.
4. Exercise interrupted-install cold continuation, then automatic four-to-five
   growth under continuous external journaling and bounded convergence checks.

A Rust child-process harness can first exercise the same production facade while
CLI integration stabilizes, but cannot be reported as CLI/operator delivery.
For the final local operator gate, reuse the process ownership and sampling in
`scripts/platform/scale-cluster.py` with sizes `4,5`, and use the external fsynced
instrumented-events client for per-operation/RPC/ACK evidence. The existing
scale harness explicitly refuses independent qualification and uses a first-node
client with end-of-phase ledgers; that limitation must not disappear by relabeling
its output. Test-only in-process fixture constructors are not production
bootstrap or migration commands.

Physical-host qualification follows a runnable, validated candidate and renewed
infrastructure budget. This audit grants none of those states.
