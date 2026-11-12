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

## Executable operator delta at `cf91eda301` (2026-11-12)

This additive read-only audit covers exact unmerged source
`cf91eda3011f4bb441124a9707d2418d9e362430`. It preserves the historical
findings/results above. No command run, build, database open or data-serving
qualification was performed for this update. The separate stale-seed constructor
baseline/fix does not explain the earlier sixth-process exit.

Native metadata startup/registration/owned catch-up and promotion now have real
public Core/CLI paths: `cluster native-metadata serve-fresh` and `join-fresh`.
That closes the older **metadata** wiring assessment, not the data vertical.
Each process receives its own root, numeric address and TLS files; the seed
configuration's local paths must never replace them.

| Next executable boundary | Exact source at this revision |
| --- | --- |
| Native DATA open | `distribution_native_host/operator.rs` exposes metadata actions only; `status` always returns `data_serving=false`. `operator_learner.rs` likewise exposes no data-serving transition. |
| Writer policy/coordinator | `provisional_listener/metadata_startup.rs::enroll_initial_writer` exists, but its only callsite is private `tests/process_tests/startup_tests.rs`, not the public operator/CLI. A policy/hook alone is not recovered leader readiness. |
| Shared recovery/read owner | The public metadata-provider listener wrapper in `distribution_transport.rs` constructs `RangeRecoveryRuntime::new` without participant transport. `NativeStrongEventDispatcher::new` is called only in the RF3 TLS test fixture. No public native host retains/publishes that complete owner set. |
| Native data listener | The immutable metadata-control allowlist rejects application events and typed point Get before data dispatch. Neither an enrolled learner nor an expired controller may fall through to data. A new supported owned transition is required. |
| Ordinary commands | CLI `cluster init/join/serve` still uses generic initialization/provisioning and pgwire startup. `checkpoint_driver.rs::start_distribution_supervisor` uses generic MStore open and `install_commit_admission`; it does not install born2 policy/Strong ownership. These paths cannot retrofit an existing native root. |
| Client/ledger | `cluster_events_command.rs::client` expects a generic distribution configuration/store directory. Native seed creation writes its own manifest. Its workload ledger is written at phase end; its CLI Get `value: Option<Value>` JSON also loses present-null versus absence. Neither is a complete native Strong acceptance oracle. |
| Automatic range move | Native maintenance schedules metadata enrollment/promotion only. `TransportClusterRelocationDriver` still runs legacy snapshot/sequence phases. `distribution/native_joint_handoff.rs::require_legacy_relocation_mutation` correctly refuses enforced membership/cleanup. Local joint runtime/image proof components have no complete native transport/controller path. |

### First real acceptance gate

Use the existing CLI process harness, genuine common initial preparation and
real metadata elections. Initial configuration is RF3/four ranges on four
separate fresh processes, with the actual events collection and distinct local
roots/TLS. Before launching the fifth, an external trusted host client must
obtain one genuine application ACK, then `get_strong` must return its complete
value; a separately missing key must return genuine absence. No private DB,
coordinator, proof constructor or test-only setter may bypass startup.

At this revision the first APP request is expected to meet the restrictive
metadata listener. Record `DATA_SERVE_CONTRACT_MISSING` and
`fifth_data_stage_not_reached`; zero ACKs cannot make verification green. This
is a proposed executable gate, not an additional executed failure. A separate
five-metadata-process result is not a five-data-process result.

Only after that gate passes, start the fifth with one `join-fresh`, retain the
same cached gateway, and demand one **automatic** native range move, actual
joint activation/authorized cleanup, a new ACK and both complete values. Do not
supply a low-level topology proposal or invoke the legacy relocation driver to
make the move appear automatic.

The external append-only ledger must fsync full logical intents/payload hashes,
physical dispatch/replies, ACK-observed records plus durable ACK receipts,
unknown outcomes, routes/deadlines/retries and every reconciliation result.
Preserve tagged present/null/absent values and fail on successful absence of an
ACKed key. Generic end-of-phase workload output and the historical eventual-read
trace-client verdict must not be relabeled Strong evidence.

### Minimal proposed DataOpen composition (not implemented)

Reuse the actual initial launch's DB/service/StoreUUID, protected birth/genesis,
fresh OWN HostMQ writer policy, installed coordinator guard, configured
participant transport, `RangeRecoveryRuntime`, and `NativeStrongEventDispatcher`
producer checks. The TLS fixture illustrates these ownership connections; its
declared-election setup is not a production election producer and must not be
copied into the host. Keep actual supervisor elections/current-term barriers.

New seams are an owned public native data lifecycle and sealed listener
transition: retain coordinator/guard/runtime/dispatcher together; obtain and
revalidate actual current same-DB/store/birth/schema/restore/catalogue custody
before publishing; stop/drain the old listener, then expose a **new immutable**
native data mode using that same service. Do not widen the old control allowlist,
clear a mode boolean, deserialize a permit or fabricate an InitialLaunch for an
existing learner. Partial bind/cancel/recovery failures retain their real owners.

Replica admission needs actual installed current writer policy and native
materialization/refusal checks; it grants neither a leader ACK nor a read.
Leader write readiness additionally requires actual current recovery and
coordinator admission, not an installed-hook boolean or seeded recovered epoch.
Dispatcher publication is only plumbing: each Strong request still performs its
own range reservation, first fresh metadata, enrolled data quorum, actual local
MVCC snapshot and distinct final fresh metadata/currentness checks. Prepared and
CompletedFrozen remain refusals; an old Activated anchor does not prove current
R+1 materialization. Report these states separately.

Fresh initial data open can reuse actual protected genesis; strict reopened or
post-transfer data open must consume the existing genuine recovery/activation
producers. A blank learner remains data-ineligible until actual assigned-image
and joint-domain activation. Remote image sessions, joint decision/publication,
automatic placement, cleanup and interrupted-operation recovery remain separate
executable seams; opening a data listener supplies none of those authorities.

### Source-only initial DATA successor (2026-11-12)

The initial DATA source successor adds `OpenInitialData` to the public retained
`NativeMetadataHost` and the existing live CLI `step --action open-initial-data`
path. It consumes the actual INITIAL factory/DB/service/Store owners; a registered
learner cannot construct it. This is the first four-host DATA prerequisite, not
an implementation of automatic fifth-node DATA placement or movement.

It installs actual own-HostMQ writer policy and a retained coordinator, drains
the actual old metadata-control listener, then starts a **new immutable** DATA
listener. Peer native protocols become available while application ingress and
typed Gets remain unavailable. Each local leader range must pass a selected-range
original protected-genesis proof, matching actual enrolled native R0/H0 voter
observations and distinct first/final generic metadata observations under the
original finite action budget. The real coordinator range reservation spans
readiness publication and cancellation rollback; no uncancellable recovery worker,
raw counter seed, historical replay or native reset is used. Other ranges may
already have legitimate writes. Nonempty/changed-epoch/restore/joint recovery is
outside this initial-only producer.

The owner retains installed policy, listener, runtime and pending state on a
finite failure. A separately fresh action retries that same owner and rechecks
its actual bindings. Stop closes ingress, drains the actual listener, dispatcher
workers and runtime contexts, and clears only its own matching installed hook.
Replica admission, observed local leader readiness and dispatcher publication are
separate diagnostics. Publication is a capability; Strong readiness is acquired
for each request by the existing MQ/data QQ/MVCC/final-MQ protocol.

Unexecuted acceptance source is
`native_initial_data_actual_cli_four_process_first_ack_and_strong_full_value`:
four actual command processes, original all-C native startup and genuine votes,
a partial-startup no-ACK/no-absence control, finite DATA actions, and one public
client bootstrapped over real TLS. The external JSONL journal fsyncs each complete
payload before Append and the actual reply before the next phase; full value,
present JSON null, present empty object and genuinely absent key are distinct.
The test emits already-fsynced journal records to stdout. The Core
`native_initial_data_` controls add actual factory/epoch/foreign-birth/cancellation
and real nonempty-history boundaries. No compile or execution result is implied
by this source description. JoinFresh DATA remains explicitly refused; general
recovery, remote image/joint relocation, automatic range scheduling, SQL/effects,
physical fault qualification and an 8 GiB/long-run acceptance remain open.

The source successor also closes the Drop-only local ownership cycle introduced
by initial DATA installation. A single process cleanup worker is started before
any hook can be installed. Drop closes ingress and sends retained stopping owners
without waiting for I/O or locks; the worker polls actual socket/handler,
dispatcher and runtime drain, then compare-retires only the exact original hook
under the admission/hook locks. It acquires no MQ/native/recovery/serving authority
and cannot revive a canceled action. Pure revocation does not consult poisoned
health, native pages or filesystem readiness. If the cleanup worker unexpectedly
cannot receive an owner, the closed owner is explicitly retained in process
quarantine rather than dropping an unknown cut or clearing a foreign writer.
The additive actual-TLS test covers no successful Stop, canceled Stop and an
injected durability failure; all external/inner open Weak owners must end. Healthy
cases then strictly reopen the same storage roots/births and retained ACK row,
with no serving hook revived. These remain unexecuted source assertions here;
metadata cold-election recovery is a separate contract.

### Exact lifecycle runtime checkpoint

Composition `8e9642433d064d87c79fdf396194bdbf9289fadc` passed four Core
initial-data lifecycle controls at run43244356602 (zero failed, zero ignored,
7.37 seconds). [Exact scope and unchanged artifact](../evidence/native-initial-data-lifecycle-2026-11-12/README.md).
That pass precedes the final nonwaiting dispatcher-clear correction and stronger
full-record/native-history cold assertions, which require fresh validation.
The public four-process CLI target is separately running at
[run 43244401559](https://github.com/nikoma/bicdb/actions/runs/43244401559);
no public DATA or fifth-node success follows from the Core result.

The next registered-learner receiver must retain the same actual factory DB,
service, StoreUUID, protected birth and committed metadata owner. It cannot
fabricate an INITIAL launch or genesis and must leave APP/Strong unpublished
until actual image/joint recovery. Current transport has no qualified native
image/joint request-response consumers; generic snapshots are not substitutes.
Registered replica receiver, authenticated native image/joint collectors and
a native placement controller are separate unfinished executable seams. A
receiver-ready/data-serving-false status is not data relocation or scale-out.
