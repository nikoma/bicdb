# Native operator four-to-five integration audit

This is a read-only source integration audit, not a running cluster result. It
identifies the missing production path between the private native reliability
components and an automatic four-to-five-node operator candidate. No build,
database open, process experiment, VM action or deployment was performed for
this audit.

## Current source boundary at `62b602ca852a` — 2026-11-16

Exact inspected candidate: **1.0.520-beta /
`62b602ca852a4183c4aa7630e2593e10446ff35c`**, tree
`d659588bb66fb527720903064000e831c7c6c76a`. This is a source-integration
inspection, not a movement experiment. Its Core/native CI jobs failed test
compilation before any cases ran; see the [exact current execution](native-520-regression-findings.md).
No source-only finding below establishes a passing runtime scenario. Thirty-four
inspected Joined/inventory/page source files were independently compared with
this public candidate and matched byte-for-byte; comparison receipt SHA-256 is
`4df3fd63465b2190aa36eba5e2767af14b42d66050f765cba0d59dd30350c9e6`.

Adding several DATA ranges to node5 requires more than repeating FIRST. Node5
is born Registered with empty INITIAL ancestry; after its first installed range,
the actual retained transfer creates a Joined owner. Empty INITIAL ancestry is
then historical identity, not empty current DATA or permission to run FIRST again.

| Required boundary | Actual implementation and source | Finding classification and next deterministic check |
| --- | --- | --- |
| Original Joined additional-range origin and selected preflight | [Joined receive](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/joined_data/receive.rs), its row/native/catalogue scopes, and runtime N-to-A transfer are implemented. They retain actual original FIRST owner, same installed State/open/runtime and selected exclusion; do not describe the entire Joined receiver as missing | **Not tested as complete expansion.** Drive a genuine Registered-to-Joined-to-second-range scenario while target-led first-range ACK/full Strong continue; preserve original lifetimes and budgets |
| Complete accepted/Unknown selected-attempt inventory | Proposal CURRENT/archive records are keyed by opaque A in both `registered-remote-image-proposal/{A}/...` and `joined-remote-image-proposal-v2/{A}/...`. Existing row/native/catalogue absence does not enumerate them; warm A/NativeBegun can also precede visible records | **Failed source requirement: producer missing.** Complete both histories under original selected N and actual warm custody, refusing selected accepted/Unknown, orphan or malformed state while unrelated ranges progress. A new empty index or missing cache is insufficient |
| Strict bounded protected-history inspection | Generic page prefix iteration bounds values, but can map stale/dead/missing locators or malformed version headers to no visible row. A raw snapshot is not a vacuum-retention pin; syntactically decoded headers and generic transaction-status defaults are not complete history-integrity proof | **Failed source prerequisite for that inventory; runtime scenario not tested.** Distinguish legitimate invisible/aborted history from unreadable or indeterminate history with checked physical/transaction provenance. Do not change generic iterator behavior or silently certify absence |
| Joined materialized NEW and same-A joint custody | [Joined target evidence](../../crates/bicdb-core/src/distribution_data/native_joint_handoff/joined_target.rs) and [Joined joint owner](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/joined_data/receive/joint_owner.rs) exist. Repository-wide call search finds declarations/reexports and definitions, but no consumers of `ActualServiceNativeJoinedJointNewEvidence`, `take_joined_remote_joint_target`, `joined_remote_joint_owner` or `closed_publication_lease` | **Failed source integration requirement; kernels implemented.** A normal caller must consume their actual materialized image/OLD proof/fresh authority and retain the same A; a declaration or decoded role does not close this gate |
| Joined target preparation, Open, release and A retirement | [Remote preparation](../../crates/bicdb-core/src/distribution_data/native_joint_handoff/remote_serving_preparation.rs) and [remote release](../../crates/bicdb-core/src/distribution_data/native_joint_handoff/bridge/remote_release.rs) admit Registered/Survivor owners. The generic image-release bridge explicitly refuses Joined; no Joined post-activation consumer was found across host/runtime/image/native joint/handoff source. Joined runtime retires empty pre-transfer cells but has no post-consumption A-release path | **Failed source integration requirement.** Add a genuine same-owner Joined activation/release consumer and prove actual unique exclusion retirement plus target-led ACK/Strong. Generic bridge refusal alone was not used to infer repository-wide absence |
| OLD/source retirement | Original source/export retirement has separate implementations, including `remote_serving_owner/release.rs`, `reference_retirement.rs`, `joint/remote_new/participant/retirement.rs` and `initial_data/source_discovery/export_retirement.rs` | **Not tested as complete expansion by this inspection.** Missing Joined target release does not negate those existing source-retirement kernels; their full integration needs its own evidence |
| Cold DATA and automatic periodic scheduling | The normal cold DATA host, current-joint new-open rebind and complete periodic placement/resume/repair wiring remain missing. Existing learner cold reopen deliberately rejects DATA ownership | **Failed source integration requirement; physical acceptance not tested.** A metadata-only reopen or manually driven finite steps cannot substitute for automatic fault-safe DATA recovery and redistribution |

The repository-wide call-site check is reproducible at the pinned source:

```sh
rg -n 'ActualServiceNativeJoinedJointNewEvidence|take_joined_remote_joint_target|joined_remote_joint_owner|closed_publication_lease' crates --glob '*.rs'
```

The visibility/transaction observations above identify requirements for a new
protected absence producer. They are **not** a reproduced current Strong-read
failure or an attribution of the original Azure unreadable records to bit
corruption. Normal reads, native Strong and physical recovery retain their own
contracts and qualification gates. No original Azure result is changed.

## Previous complete source boundary at `4f33a8b016a9` — 2026-11-15

This additive update inspects candidate **1.0.520-beta /
`4f33a8b016a999d43410ca8698270dc01fbeffd6`**, tree
`4c87ba7caed1e5a9043eac854d7bdf15ac00b15d`. It supersedes the historical
NOT IMPLEMENTED/NOT WIRED assessments below only for the specific components
listed here. Main remains **1.0.519-beta / `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`**.
The candidate's normal [Tests43767288668](https://github.com/nikoma/bicdb/actions/runs/43767288668)
is still running at the **2026-11-15 22:54 UTC** evidence checkpoint. Its complete all-target compilation and
no-default consumer checks passed; [Rails43767288693](https://github.com/nikoma/bicdb/actions/runs/43767288693)
also passed its actual 20 runs/100 assertions, zero failures/errors/skips and
24 completed reports/cable messages. Nonserial Core passed 1,875/0/16 across
138 main-suite summaries, plus one separately isolated child pass. SQL passed
2,682/0/4 across 282 summaries. Serial Native and the remaining workspace,
including the unchanged CLI availability control, remain pending. None of
these component results establishes their outcome or physical qualification.

At the **2026-11-15 23:30 UTC** checkpoint, the serial native job
[`132975184551`](https://github.com/nikoma/bicdb/actions/runs/43767288668/job/132975184551)
completed with **387 passed, 84 failed, 1 ignored**, 989 filtered, in
2,971.93 seconds. This is a runtime failure after successful compilation.
The complete 472-case inventory assigns 67 failures to FIRST tests and 17
to learner/hot-policy tests. Observed failure sites include initial fixture
readiness/control-service contention, activation/history validation and actual
request timeouts. They are separate observations, not a demonstrated common
root cause. This candidate cannot merge on these results. The remaining
workspace job, including the original CLI availability control, is still running.

Both new registry controls executed and passed:
`native_initial_registry_real_native_sync_unknown_retains_same_owner_and_denies_app`
and `native_initial_registry_same_known_api_late_cancel_fresh_read_no_replay_and_sdk_strong`.
The original registry cancellation/old-Store control also passed. These component
passes establish neither the pending CLI outcome nor complete relocation.
The full native raw log is 206,768,590 bytes, SHA256
`b2c2ccb8bb005fbd8970824f90a962ccb02c56882571b5297aef05b485efd5c4`;
its retained gzip SHA256 is
`5e7c581c16af9f3e295202c5ec9bacd8ca163a275e639ce4ef7af641067f0db8`.
The complete outcome inventory SHA256 is
`3a1fef2bf4e14be7baeb159a083dc27df46209d4136fb5cbec5af2444ee77c07`.
Failed scenarios remain **failed** even when their prerequisite prevented them
from reaching the intended injected fault; the unreached guarantee is **not
tested** by that scenario.

The final normal run completed naturally by **2026-11-15 23:59:13 UTC**.
Remaining-workspace passed **1,062/0/6** across 129 summaries; the actual native
CLI target passed **6/0/0**. Its original healthy-existing-read/fresh-ACK/full-
readback gate after adding a fifth metadata voter passed with unchanged windows.
The complete 163,814,084-byte REST raw log SHA256 is
`8d8fe385b3751a3378e572a5a8ff733753ac476ad0360661b9ac7f306137cd5c`;
independent complete-stream/result receipt SHA256 is
`52e61381401d2dd96f14c5cce0c9b32278875655597f636f03a2c1f58e388193`.
This supersedes the pending CLI state in the earlier checkpoints, within its
bounded metadata-only scope. DATA assignments did not move. The entire run
still failed Native; no complete relocation or physical qualification follows.
The complete 96-file read-only archive manifest SHA256 is
`320733868a453f83a4aa3eb885c44126e438a12615a6e6482073016c6677d608`;
independent full-archive verification receipt SHA256 is
`3fcef4de7951cdd47f9ea857d8bb25b583c1cd68581582de0f1a6a61082ffa81`.

| Boundary | Implemented source | Actual executed evidence and classification |
| --- | --- | --- |
| Initial native DATA and explicit Strong point access | Public retained host/CLI open paths, actual coordinator/runtime/listener and request-scoped Strong protocol exist | **Passed within predecessor scope:** d3's REST CLI target passed the actual four-process APP/Strong case. This is not a passing current-candidate or movement result |
| Fifth metadata voter and original writer refresh | Automatic metadata enrollment/promotion and registry refresh exist | **Passed within current4f scope:** the unchanged CLI healthy-old-read/fresh-ACK/readback control passed after fifth metadata-voter addition. Historical d3 failed with all four writers pending; that result remains retained. Neither case moves DATA |
| Retained registry correction | Current source retains the original publisher/native outcome and separates maintenance identity from strict APP/Strong admission | **Passed within component/CLI scope:** both new actual commit-outcome controls, original cancellation/old-Store control and original CLI ten-second health gate passed. The complete native suite still failed |
| Registered fifth replica receiver | Fresh/registered retained receiver and public join path exist | **Passed within predecessor scope:** d3's CLI receiver case passed with DATA serving false. Receiver ingress or metadata voting does not establish populated DATA ownership |
| Native image/joint/Open/policy/release steps | Finite internal native host consumers exist, including actual remote-serving participants | **Failed current native acceptance:** 67 FIRST and 17 learner/hot-policy cases failed. Some intended phases were not reached. Individual passing controls do not establish complete relocation, cleanup, cold recovery or replacement |
| Multiple native ranges on the joining target | The inspected FIRST planner requires a target with no range replicas; FIRST receiver enrollment rejects an existing DATA owner or another retained receive attempt | **Not implemented by iterating FIRST:** a populated-target admission/continuation producer is still required. This does not claim a first complete handoff passed |
| Automatic native DATA placement and repair after one join | The public operator loop maintains metadata and initial writer/hot recovery; finite native relocation consumers are not a complete periodic placement controller | **Not implemented as the complete native operator path inspected here; not tested** for automatic full-range four-to-five convergence. This is a functional integration gap as well as a qualification gap |
| Complete external campaign and capacity comparison | Bounded SDK/receipt/reconciliation components exist | **Not tested:** no current full-dataset physical expansion, complete campaign-wide ledger or stable post-convergence throughput gain |

Compilation job `132975145942` full raw-log SHA256 is
`7e415d496e305e3bac5c8d282b1a042627f0168e433b5b7ad16071dbce6c0a6d`.
Rails job `132975145225` full raw-log SHA256 is
`bd9877aa33953ce0036748fe2bf8a24fc20ea3a7c46dd116eaf89c0243fedf63`;
its original 16-member ZIP/API digest is
`b8a98a1e172f507ef7e1786b74ca1e6242f27bf5c340544cf9a57f3174a674f1`.
Independent complete-log/original-ZIP review receipt SHA256
`30e45c3483eb13e8c55aac2d0e24a54dd7ae7886ab9ae77637090eb6ecac0d54`
pins actual checkout `2f42e67a11c5d8f81d79e170edd0814d4a741d2d` to the
candidate tree. The Rails artifact records its CI-built debug binary hash;
no binary file, release or deployment is produced by this document.

Core job `132975184546` full raw-log SHA256 is
`0e5b45cd82677876404904215e50895c87116f1e60aef47d5483840020a6094e`;
its 139 raw summaries include the extra child execution. Independent accounting
receipt SHA256 `7ce6ca81ae5f70e194fb477b5bc531e00840ce810d993d93ebc2bdf1fb0516d1`
separates that child from the 1,875 main-suite passes. Serial native namespaces
are excluded from this Core job. SQL job `132975184578` full 348,522,627-byte
raw-log SHA256 is `e710b5e959f0ba12763f7047d469a99b2d073f156220312b9932266eafa96610`;
complete-stream receipt SHA256
`efc3be20d387920fa3fc6a17565316500f684d833937d17046410c0d79c63e4d`
retains 2,686 result rows, including two distinct integration tests sharing one
bare function name. These counts are not physical HA or scaling measurements.

The precise automatic/native distinction matters. The existing generic
`TransportClusterRelocationDriver` and placement planner are real implementations;
the original Azure trial observed automatic movement with the older generic
runtime and still failed qualification. Their existence does not wire the
protected native path described by this audit.

Source anchors for the current native operator boundary are:

- [CLI maintenance loop](../../crates/bicdb-cli/src/native_metadata_command.rs):
  the initial-host periodic turn starts the metadata supervisor, maintains
  metadata membership, refreshes the initial writer registry and reports status.
  It does not schedule new native range placements.
- [Public host and finite internal methods](../../crates/bicdb-core/src/distribution_native_host/operator.rs):
  `advance_finite_remote_membership`, `drive_finite_remote_release`,
  `advance_finite_remote_serving_policy` and `poll_finite_remote_serving` remain
  crate-internal consumers. `status` explicitly limits its initial DATA contract
  to point access; a published dispatcher is not request-scoped Strong readiness.
- [Owned participant polling](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/remote_serving_owner.rs):
  a finite actor turn progresses retained actual participants. It does not
  periodically create placement plans. Existing worker polling after a retained
  operation starts is distinct from automatic cluster-wide placement.
- [Protected handoff enforcement](../../crates/bicdb-core/src/distribution/native_joint_handoff.rs):
  `require_legacy_relocation_mutation` refuses generic membership/cleanup for
  enforced native voters and requires the typed actual OLD/NEW data-quorum path.
  Removing that refusal would bypass the intended safety contract.
- [Unchanged CLI availability assertion](../../crates/bicdb-cli/tests/native_metadata_operator_cli/writer_registry_refresh_baseline.rs)
  and [complete d3 findings](native-520-regression-findings.md): original ACK and
  Strong value preceded the fifth voter; later reads were Unavailable and fresh
  writes had no ACK. No false absence was observed. Unavailable reads prove
  neither acknowledged-data loss nor survival.

Two further source boundaries prevent treating this as only a missing scheduler.
[The FIRST allocation planner](../../crates/bicdb-core/src/distribution_native_host/joined_data_handoff.rs)
rejects a target already holding any range replica. [FIRST receiver enrollment](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/registered_replica/first_receive.rs)
rejects an existing DATA owner or another retained attempt/image/step/target.
Those are deliberate restrictions of this initial lane. A normal expansion
that assigns several ranges to the same new node needs actual populated-target
admission and per-range continuation; deleting these checks or looping the first
control is not that producer. This does not imply all 64 ranges must move.

The [alternate remote joined-handoff backend](../../crates/bicdb-core/src/distribution_native_host/joined_data_handoff/remote_backend.rs)
also contains explicit unimplemented source transfer, decision, candidate,
activation and serving stages. It cannot substitute for the finite FIRST
consumers or complete the operator by being selected. [The actual FIRST
allocation acceptance facade](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/first_coordinator/acceptance.rs)
is a test-only module; its real metadata proposal/heartbeat/read observations
do not constitute a public automatic placement caller.

The next operator gate remains a complete automatic operation, not a manual
sequence of finite internal calls: one ordinary join, actual assigned image and
joint activation, safe source retirement, bounded resumption/diagnosis, healthy
reads and fresh ACKs, then restored replication after another tolerated failure.
All 64 ranges, the complete intended corpus, raw attempts/uncertainty and final
reads must remain in the evidence. A source-only component, passing refusal,
small smoke or generic planner test cannot close this gate. Populated cold
recovery, ordinary distributed SQL and production admission remain separate.

The original Azure result stays failed and its verified teardown unchanged.
No resources were provisioned or new spending authorization inferred. See the
[current HA audit](../ha-horizontal-scaling-audit-2026-11-10.md) and
[registry publication correction](native-registry-pending-owned-publication.md)
for their distinct source/runtime scopes. All older dated sections below retain
their original revision and evidence meaning.

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

### Source-only registered replica receiver prerequisite (2026-11-12)

A separate source successor adds public `open_replica_receiver`, the diagnostic
`open-replica-receiver` action and opt-in `join-fresh --replica-receiver`. This
first variant requires the successful JoinFresh owner's actual Registered,
locally current metadata-voter/control backend. Fresh and strict Cold owners
retain the SAME factory, DB open/birth, service, range-Store open, metadata
StoreUUID/namespaces and supervisor. An actual owned-member observation plus own
fresh HostMQ checks the transition; draining/binding failures retain that owner.
It closes/drains the old socket and starts a NEW immutable replica-only receiver.
The receiver checks both the actual Store-open lifetime and its service-slot
binding without taking the service latch. The slot privately owns its Store;
replacement revokes the previous binding before swapping objects, even when the
old Store is retained alive. Restoring that same object creates a new binding
and cannot revive the old receiver. Existing per-operation native guards remain
mandatory; this token grants no role, floor, mutation or read permission.

Opening the receiver installs NO writer hook/coordinator, native genesis,
recovered epoch or Strong dispatcher. The typed image path already obtains its
controlled row grant from actual source/full-history/role/freeze proofs rather
than ordinary writer admission. Those proofs remain mandatory and are NOT
created here. Typed Strong returns unavailable before DB latches; APP/Scan and
generic snapshot/import/repair remain refused. Metadata backend/scheduling
continue, and every allowed native operation retains its existing fresh-role,
physical-history and policy checks. `receiver_ready` means ingress ownership,
not materialization, voting promotion, source readiness or DATA serving.

Unexecuted source tests cover actual TLS same-owner rotation, UNKNOWN blank
native prefix (not zero), no installed writer and APP/Strong refusals; a public
process test first obtains four-owner full-value ACK/Strong with an external
fsynced ledger, then one fifth JoinFresh command reaches receiver-ready while
DATA remains false. This stage neither assigns a range nor moves one. The next
executable gates remain native placement/controller consensus, authenticated
native image/joint wire consumers, initial DATA writer-policy refresh after
metadata membership changes, and actual image/joint/open/writer readiness.
No generic relocation driver, manual topology proposal or DTO quorum substitutes
for those consumers. No result of the unexecuted successor is claimed here.
