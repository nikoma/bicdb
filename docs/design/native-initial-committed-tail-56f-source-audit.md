# Initial native committed-tail isolation: exact 56f source audit

This is a **source-only audit and unexecuted test specification**, not a diagnosed cause of the CLI failure or an implemented recovery controller. No runtime code, deadline, retry, recovery guard or original assertion changes here.

Audited source: `56f9a3c999fba329c349d3e6dda664dcfb64e4c6` (source `1.0.489-beta`). The retained executed CLI evidence is [FAILED-CLI-56f.md](../evidence/native-writer-registry-first-aac-2026-11-12/FAILED-CLI-56f.md), docs commit `dce0f422a` / Root import `29bfd6c9a`.

## Observed boundary and limits

The initial public client obtained full-object and present-null ACKs and exact Strong values. The subsequent empty-object Append produced **no ACK; outcome remains unknown**. Actual requests 122 and 127 each sent all 286 framed bytes to n2/range2/epoch1, then timed out without a decoded application response. All four intervening Bootstrap snapshot RPCs succeeded at Cgen1. The final aggregate client error therefore does not establish that all hosts were down.

Stopped raw catalogues retain the same range2/index1 command `01a1d186-f4ba-7af1-82d0-d4d6e63d67d1`, checksum `951258d8fbde6ce3b563605e26dbc6478b96c47b933030832a8d12c7f4fdceb1`: n2 `quorum_committed`/resolved0; n0+n3 `applied`/resolved1; n1 is not a replica. These are raw catalogue observations. They do **not** prove n2's native prefix, native Pending/materialization receipt, row-WAL replay, or where its handler stopped. There is no demonstrated `{}`-type bug, rollback, permanent loss, all-host outage or Azure cause.

Driver verified the initial stopped copy: 76 files, 3,239,725 logical bytes, 32 directory entries; manifest SHA `0e0d660b11a1fff896d532a9fdf8db173a9559b8f3f96cf25d121e762610376b`. Private receipt `56f-cli-custody-verification.json`, SHA `0f2155d893c9d93af47d28afa448e1592b22dc1be3463f80f2220fe6592722fe`, is under `/root/.local/state/bicdb/ha-scaling-2026-11-11/private/native-registry-refresh-56f9a3c9`. No engine was reopened/replayed for this audit; those original roots remain untouched.

## Exact normal write sequence

The following paths and line numbers refer to the audited 56f revision.

| Boundary | Actual source | Consequence |
| --- | --- | --- |
| Authenticated APP dispatch | `distribution_transport.rs:2272`; `distribution_events.rs:557` | Actual same-open DB and leader range are checked, then a real transaction enters commit admission. Append calls do not receive the caller's cancellation token through this function signature. |
| Local decision and certificate | `distribution_range_consensus.rs:3394`, `:3694` | Real local Prepare/Commit and counted remote decision replies precede local CQ publication. CQ is irreversible; no abort may be inferred after it. |
| Owned local reservation retained | `distribution_range_consensus.rs:3710` | Later same-range writes/point reads cannot overtake this active command. |
| Actual NativePending | `distribution_range_consensus.rs:3735` | The full native intent is persisted before authenticated replica materialization instructions. |
| Remote native Apply | `distribution_range_consensus.rs:3771`; `distribution_native_apply.rs:22` | Each actual certificate-bound Apply is synchronous before admission returns. The shared ordinary native token is independently capped at 5000 ms. Missing replies are ignored as rollback evidence. |
| Admission ticket | `distribution_range_consensus.rs:3785`; `db/spatial_paged.rs:2733` | Pending ACK evidence is retained and the real ticket returned only after the remote Apply loop. The transaction then owns cleanup/recovery release. |
| Origin local WAL/rows | `db/spatial_paged.rs:2747` onward | Origin row WAL begins after the ticket. The existing injected post-admission/pre-WAL seam is before this boundary. |
| Native materialization/prefix and raw Applied | `distribution_range_consensus.rs:3802` | The actual borrowed-DB WAL receipt must validate; native Pending completion precedes the raw applied callback/ACK result. Errors remain unknown and invalidate recovered-epoch readiness. |
| Cleanup after rejected origin apply | `db.rs:9019`; `distribution_range_consensus.rs:3983` | Volatile ticket/range ownership and recovered-epoch admission are retired after transaction cleanup. Durable CQ/Pending are retained. This is refusal/custody, not replay. |

Consequently, local CQ plus Applied peers is reachable **before the origin admission ticket/local row WAL**, while the coordinator waits for remote Apply replies. It is also compatible with later local WAL/materialization/callback failure: the raw CQ alone cannot distinguish those cases. A fully successful local Applied checkpoint followed only by response loss would ordinarily leave raw Applied; the retained raw cut does not establish that simpler explanation either.

The client's original 2-second transport timeout is not proof that the APP worker stopped. The APP path does not pass that caller token into `execute_cluster_event`/`Transaction::commit`, while replica materialization uses its own bounded native token. A disconnected client can therefore coexist with legitimate work still running. This audit does not attribute the retained cut to that possibility without actual handler phase/exit evidence.

## What the next request does

| Entry point | Existing behavior at a genuinely unresolved native tail |
| --- | --- |
| Fresh Append | `admit_with_database` requires recovered epoch and a free actual range reservation, then plans against the real native prefix. It does not call a committed-tail recovery controller. Cleanup invalidates the recovered epoch; an active original worker instead retains the reservation. |
| Fresh Strong | `NativeStrongEventDispatcher::execute` obtains fresh HostMQ and separate leader metadata authority, then the real coordinator reserves the range. An active writer can exhaust that read's original reservation budget. Once available, `native_read_catalog.rs:61/:111/:209` rejects unresolved/ahead raw history or retained CQ; it cannot legitimately mint a clean absence from that local cut. There is no replay operation in this read protocol. |
| Generic `recover_local_leadership` | It calls `recover_leader_prefix` then `recover_leader_tail`. Native mode explicitly refuses a behind prefix (`:3037`) or unresolved native tail (`:3257`) and requires actual native recovery. It is **not** a working manual native replay shortcut at this cut. |
| Initial DATA opening/action | `distribution_range_consensus/initial_data.rs:28` is a genuine protected empty-genesis producer. Nonzero/history recovery cannot enter that R0-only path. Initial host startup/registry refresh does not schedule a native committed-tail replay; no supported DATA cold-reopen path is established here. |

The audited NativeHost/Strong/runtime call graph has no `recover_local_leadership` call. The pgwire operational loop separately calls it at `checkpoint_driver.rs:1666`; that loop cannot be silently assumed for the new NativeHost, and its generic native guards still apply.

There are two importantly different outcomes: the **original still-running worker** can eventually finish and make a subsequent read succeed without repair; after a genuine post-admission error and cleanup have returned, the preserved tail instead needs a distinct actual native recovery controller. A test must distinguish these cases before claiming automatic recovery. Current source supplies fail-closed refusal, not a bounded self-healing qualification.

## Existing genuine fault seam and smallest next test

The existing `BicDb::test_reject_next_native_after_admission` (`db/range_image_native.rs:771`, consumed at `db/spatial_paged.rs:2748`) rejects only after real admission/CQ/NativePending and before local row WAL. The actual enforced fixture already uses it in `native_strong_point_tests.rs:584`, `native_strong_point_raw_cq_or_pending_native_cannot_return_old_row_or_absence`. That test preserves a genuine old row and requires Strong unavailability. It does not use the public same-client APP path or prove self-recovery.

That injection is thread-local. Arming it on the test/client thread cannot be assumed to affect an actual TLS receiver worker. The smallest additive Core regression should use a narrowly targeted, one-shot **actual receiver-thread/per-actual-DB test hook** at this same boundary, not a fake quorum/ACK or fabricated native fact. No such new hook is implemented in this note.

1. Use actual initial native writers, owned metadata, pinned TLS and the public event client. Obtain a real old ACK and exact Strong value on a selected range; retain its full value/ID/digest ledger. Use the same client and original request timeouts throughout.
2. Arm only that actual origin/range/new event for the post-admission/pre-WAL error. Require real certificate-bound peer materialization and a real client error with no ACK. Prove the origin handler/transaction cleanup actually exited, rather than treating a sender timeout or brief drain as receiver exit. A missed prerequisite remains a failed test.
3. Inspect actual native point, Pending, raw command/state, UID/history/base and the old ACK through existing real observer APIs. Record the unknown event's exact command/checksum and both peer values; do not infer native state from raw counters.
4. Issue fresh public Strong reads after the original worker has exited. For the unknown key, only its **exact full value** or structured unavailability is acceptable while unresolved; `NotFound`/`None` is forbidden. For the old acknowledged key, preserve its exact durable value and likewise forbid false absence or a wrong value. Unavailability alone is a safety result, not recovery success.
5. Under a separate original bounded test deadline, require automatic recovery to finish, a fresh Strong of the committed unknown key to return the exact value, and the next distinct same-range Append to ACK followed by exact Strong/ledger reconciliation. No direct `recover_local_leadership`, forced cache/counter change, generic repair, manual phase clear, new genesis or synthetic certificate may supply that acceptance. If current source remains unavailable, retain the actual failing witness; do not mark the final recovery gate passed.

The expected missing producer is **committed native recovery**, not NoData abort: an existing certified command must be adopted/materialized under fresh actual metadata/data quorum and retained original obligations. Existing typed native historical/recovery APIs may supply the components, followed by real Ready/Complete/Activate and a legitimate coordinator admission transition. Their automatic same-host orchestration, original-request/cold ownership and bounded completion remain separate work; this audit does not authorize a generic native guard bypass.

## Executed 721 baseline and smallest hot-origin continuation

This additive **read-only implementation proposal** inspects exact unmerged
`721c329de4196aeef96604642c439fe7d8ddb0bf`. No producer, wire arm, deadline or
controller is changed here. The [executed receipt](../evidence/native-origin-pre-wal-2026-11-12/README.md)
records one passing arm control and the failed required recovery gate, not a
recovery success. Unlike the earlier 56f unknown, this deliberate cut proves the
first origin's typed Unknown/sequenceNone, actual transaction/ticket, completed
release callback and normal APP exit before native inspection. Origin n0 is
R1/Pending2/rawCQ2; n1/n2 are genuinely native R2 with both full values. There is
no range/leader/epoch change. This does not diagnose the old 56f cause.

### Retained entry point and authority

`NativeInitialDataHost` in
`distribution_native_host/provisional_listener/registration/initial_data.rs:108`
already retains the actual initial launch, installed coordinator/guard, service,
DB, runtime, dispatcher and actual Store-slot admission. Runtime construction at
227 uses the SAME service/provider and `with_participant_transport` with the
actual initial transport. The immutable DATA listener is attached at285.
Keep those objects and their original registration/open/birth; do not install a
new writer, reset recovered epochs, recapture zero genesis or construct another
runtime to lose the original session.

Use the existing live host-maintenance path, outside engine guards. The CLI
already invokes `NativeMetadataHost::refresh_initial_data_registry` with a
separate finite five-second process-maintenance child at
`crates/bicdb-cli/src/native_metadata_command.rs:579`; that path currently
repairs only the metadata registry. A dedicated DATA-maintenance call/owner is
new work, not a property of the metadata supervisor. It should poll a bounded
number of actual local leader ranges or consume a bounded hint from actual
release, then validate actual retained Pending/command/Store state. A hint or
`NativeRangePrefixObservation` is diagnostic input only. No error-string parse
or incoming voter/counter DTO can create recovery authority.

Before side effects, a coordinator-owned entry point must take the SAME
`active_ranges` reservation used by APP/Strong (`RangeAdmissionReservation`,
`distribution_range_consensus.rs:2191,2901`). The real original release removes
that reservation and recovered epoch only after Core cleanup (`3983`); a still
running original attempt must remain busy. Keep its mutex unlocked across IO.
Take no normal mutation permit before asking the runtime to freeze/drain the
range. Across finite unsuccessful calls the runtime retains the actual Frozen
session, while recovered admission remains closed; a new tick reproves current
authority and does not reset state. Another range must retain scheduling space.

### Actual reusable sequence for committed command2

| Step | Existing actual producer at source721 | Required composition |
| --- | --- | --- |
| Fresh current domain | `MetadataReadProvider::witness_recovery_role` + `RecoveryRoleAdmission::from_witness` + `RecoveryFenceScope::from_role` | Actual current local data leader, `NormalRecovery`, unchanged voter/incarnation/pin/schema/restore/birth domain, new challenge/business; metadata routing hints are not this authority. Retain the session nonce/phase as data and reacquire actual fresh roles for later bounded phases. |
| Fence and history | `RangeRecoveryRuntime::fence_epoch` (`runtime.rs:845`), `observe_history`; `RecoveryFenceQuorumRound` | Authenticated current data quorum, actual native birth/open/Store and protected irreversible-history obligations. Freeze/drain happens outside service/DB locks; the recipient owns its actual Frozen session. Do not call NoData/Abort for already decided CQ2. |
| Re-certify command2 | `certify_historical` and private `historical_decision` (`runtime/participants.rs:213,312`) | Local retained command2 is only the candidate body. Each recipient independently collects fresh fence QQ and actual command decision evidence, then persists current-session certification. Source721's sparse path is unnecessary when this exact prefix/entry is retained. |
| Materialize origin | `apply_historical` (`participants.rs:405`), `ClusterDataNodeService::apply_native_historical_command` (`distribution_data.rs:1625`) | Collect actual current certification QQ. `BicDb::observe_known_native_recovery_prefix`/`prepare_historical_native_pending`/`apply_historical_native_command` (`certified_prefix/recovery_apply.rs:95,293,530`) revalidate exact ancestry and that the previous actual transaction scope ended; archive old Pending, mint a NEW real local transaction attempt, write actual WAL, materialization and native Applied prefix. The internal controlled replay grant is sealed, not a new generic bypass. |
| Durable raw checkpoint | `RangeWriteStore::mark_historical_applied` (`recovery_state.rs:414`) | Only after the opaque actual native receipt; persist/refsync raw Applied/resolved2 and revalidate receipt. A raw CQ/Applied label alone never substitutes for native history. Already-native-covered peers validate immutable coverage rather than replay old rows. |
| Prepared and Completed | `prepare_native_ready` (`runtime/native_ready.rs:29`), `complete_native_recovery` (`native_completion.rs:20`) | Actual closed matching native R/H0/H and separate local typed B, all mandatory participant preparations/completions, retained actual physical pins/Frozen owners. Each Complete independently obtains actual Ready/completion QQ and fresh recipient role. No catalogue-stage DTO or saved receipt becomes current authority. |
| Activate/open each recipient | `activate_native_recovery` (`runtime/native_activation.rs:5`) | It independently collects completion QQ, gets fresh role, calls `activate_local_native_recovery`, then actual `arm_local_native_activation`; exact retained pin/Frozen ownership must drain before actual release. Followers first, origin last is the simplest initial schedule. A native decision/catalogue write without actual arm/release is not an open replica. |

The manual retained-prefix test
`recovery_order_tests.rs::actual_gap_refusal_preserves_durable_observations_and_retained_prefix_recovers`
already exercises certification/replay of real R1/Pending2 while preserving the
old ACK, but it ends at native/resolved history/SourceReady rather than this
public automatic terminal. The real TLS `native_strong_point_tests/after_transfer.rs`
uses Prepare/Complete, trusted LOCAL activation, then ordinary coordinator
recovery. Neither supplies this hot original-host controller or a remote
activation endpoint.

`KnownNativeRecoveryPrefix` specifically refuses an old Pending whose previous
local scope is still active. Its recovery attempt binds old Pending checksum,
actual new local transaction/open, exact command, nonce/configuration and MQ
context; max32 physical attempts is an explicit limit, not permission to evict
old evidence. Preserve the protected writer-start/everCommit/promise inventory.
This proposal does not solve permanent-slot/archive growth or 8GiB qualification.

### Two required seams, not implemented APIs

1. **Authenticated activation delivery.** The INITIAL listener already allows
   `RecoveryEpochFence/FencedProbe`, historical certification/Apply,
   `Prepare/ObserveNativeReady`, `Complete/ObserveNativeCompletion`
   (`distribution_transport/initial_data_listener.rs:39–53`). The pre-latch
   receiver dispatches those to its actual attached runtime
   (`distribution_transport.rs:1705–1854`). There is **no**
   `ActivateNativeRecovery` request/transport arm. Add a separately fenced typed
   endpoint that forwards existing `NativeCompletionRequest` to the SAME
   runtime's actual activation producer and returns authenticated completion
   evidence. Retain all actual caller/destination/incarnation/pin/scope and fresh
   role/quorum checks. Reserve the unpublished wire version centrally before
   implementation. Do not broaden the old metadata-only listener or turn
   Complete/Observe into implicit activation.
2. **Cancellable coordinator publication from actual recovered-open state.**
   `recover_local_leadership` (`distribution_range_consensus.rs:2686`) has no
   caller cancellation, performs legacy progress/repair IO, and intentionally
   refuses a native prefix gap (`3037`) or unresolved tail (`3257`). It cannot
   be wrapped in a late-mutating thread or called to repair this Pending.
   `recover_initial_native_genesis` (`initial_data.rs:28`) is sealed R0 only and
   cannot be used at R2. Add a narrow coordinator-owned completion producer,
   analogous to its original-reservation/fresh-first-MQ/data-QQ/fresh-final-MQ
   pattern, but requiring actual same-open activated NormalRecovery and current
   native R2/H2 instead of genesis. Reuse `acquire_native_read_candidate` plus
   `check_enrolled_voter_births`; introduce an opaque recovered-open admission
   whose private producer also verifies current activated serving, no pending/
   irreversible gap, exact original writer/coordinator/registration, DB/Store
   slot/open, schema and current birth domain. Candidate facts alone are not a
   grant. Do not add naked `into_parts` or expose a numeric floor as authority.
   Final currentness must revalidate the actual local observation and all
   enrolled births against a DISTINCT final current-range MQ before inserting
   recovered admission. Use the SAME held reservation (a private checked
   under-reservation helper, no recursive `try_reserve_range`). Keep it through
   the final original-token/MQ check and rollback on cancellation, so no APP ACK
   can race a transient readiness bit. Do not toggle the global dispatcher to
   grant authority or unnecessarily stop unrelated ranges.

### Caller lifetime, errors and scope

Use a retained **process** lifecycle for the maintenance owner, distinct from
any expired original EventClient call. Each phase has an original finite child
budget capped at five seconds, preserved through local waits, every RPC, role
and mutation/publication checks. Never renew an expired QQ, relabel old votes,
replace an expired action with uncancelable work, or retroactively turn the
first Unknown into an ACK. A later genuine SDK ACK is separately journaled.

A durable Prepared pin/materialization may outlive its creating request as a
fact: `PreparedNativeRecoveryReady::require_current_under_role` (`762`) verifies
an independently fresh role, exact saved scope/catalogue, actual owned physical
pin/open and point. This is not continued authority from the old role. Keep
all existing current-request and original-writer/caller refusal contracts; a
fresh operation may only use the exact durable continuation allowed by those
producers. It may not renew a cancelled in-flight operation's deadline.

Keep the existing max4 runtime contexts/max2 exclusive collectors and leave
readonly peer headroom. Participant workers retain their real counted operation
until actual drain; a busy next phase retries boundedly, never zeroes counters.
No service/DB/schema/journal mutex crosses network IO or a drain wait. Freeze
and reservation ownership may span IO with their locks released. On stop,
cancel maintenance first and drain its actual workers before retiring the exact
host hook, using existing retained cleanup/quarantine ownership. Fsync may
finish after a caller expiry and return Unknown; it must not publish late
serving/recovered permission.

Cancellation, missing peer QQ, changed leader/epoch/voters/schema/restore,
writer/Store/DB replacement, conflicting/higher native history, missing custody
or a corrupt archive all remain explicit refusal/retry states. A source peer
failure is tolerable only with genuine remaining decision/current certification
QQ; do not adopt from one raw local CQ. Losing the ORIGINAL origin process or
retained session is outside this first hot-only continuation. Cold/higher-epoch/
image/joint/replacement recovery must keep their existing separate producers;
no reset, blank genesis, generic snapshot or RowExists workaround is proposed.

### Minimal approval-ready acceptance

- Run the unchanged source721 fault baseline with the supported automatic
  maintenance path only: first real Unknown and handler exit; actual R1/Pending2
  plus both R2 peers; then same writer/coordinator/registration/DB/Store, genuine
  replay/checkpoint and all actual Ready/Complete/Activate/open steps; SAME client
  returns old full ACK value and committed command2 value, ACKs the distinct next
  event, and returns its full Strong value within the unchanged receipt gate.
  Observe actual handler/operation drain and retain every API/RPC attempt.
- Hold the original APP scope/transaction before cleanup: no repair/recovered
  publication; release the real holder, then a separately fresh attempt proceeds.
- Drop replies after actual native WAL, raw Applied fsync and activation
  publication, separately: Unknown remains Unknown; exact fresh same-session
  reconciliation preserves command/checksum/history, avoids duplicate replay,
  and cannot ACK before actual open/coordinator publication.
- Expire/cancel the original phase while IO is held, with a separately live
  process owner: no late serving/recovered grant. A genuinely new finite attempt
  must reprove authority and exact continuation; no budget/role revival.
- Replace the actual original writer/hook or service Store slot while keeping the
  old objects alive; change real committed epoch/voter/schema/restore in separate
  controls. Captured preparations cannot publish recovered permission or erase
  the replacement. Keep old full ACK/native state and actual audit histories.
- Remove one then both decision peers, and inject a genuine conflicting/higher
  observed history: count one physical voter once, require true current QQ,
  preserve CQ/Pending/everCommit on refusal, and never call Abort/NoData.
- Stall this selected range and show an unrelated actually ready range remains
  usable. Separately kill the origin: first-scope unsupported/cold refusal is not
  relabeled bounded recovery. Full cross-process relocation, all-owner restart,
  automatic five-node DATA convergence and physical RPO/RTO remain open.

## HOT original-origin implementation candidate (not executed)

The successor of `721c329de4196aeef96604642c439fe7d8ddb0bf` implements
only the witnessed hot cut: the retained original INITIAL DATA owner has a
next unmaterialized native Pending command and matching retained raw CQ; both
peers have already materialized the committed command. This is source under
review, with no compile, runtime, throughput, fifth-node movement, cold recovery
or physical qualification result.

The public automatic-metadata owner supplies a distinct retained process
cancellation token. A DATA controller starts only when that actual process owner
and the published original DATA owner both exist. Finite DataOpen/client calls
do not start background mutators. Outside finite attempts the worker holds a
Weak owner; each attempt retains its exact objects and original token for at
most the existing five-second admission budget. Stop/Drop cancel it, and the
existing cleanup reaper retains its actual owner until the worker, listener,
read workers and runtime contexts have really drained before exact-hook
revocation. The controller has its own serialized state and does not run on or
hold up the metadata heartbeat worker. Diagnostic/status acquisition does not
wait on the controller's attempt lock.

Each selected job keeps its original nonce, command and phase on a refusal.
Those values are session data, never authority. Actual authenticated recipient
APIs acquire fresh NormalRecovery metadata/data/history evidence: fence all
members; certify the exact historical command on all members; apply with the
existing current certification quorum; Prepare, Complete and Activate all
members, with the original origin last. The new private wire-29 Activate arm
forwards the same runtime activation producer; metadata-only and registered
replica listener allowlists gain no permission. Remote completion projections
cannot publish recovered coordinator admission.

Final publication is a separate sealed path. It observes the actual same-open
local activated/refusal release and protected native activation decision,
exact native command/history and all protected abort controls, then binds that
fact to a freshly enrolled native read quorum and a distinct final range MQ.
It validates current native birth bindings, actual original writer registration,
DB incarnation, service/Store slot and exact range again. A single actual
coordinator reservation covers publication and its post-cancellation rollback;
no APP ACK can race between a transient recovered-map insertion and rollback.
There is no caller-supplied numeric floor, recovered-epoch setter or DTO grant.

A queued hot reservation shares the existing `active_ranges` map with point
reads and writes. The active-map lock always precedes the waiter-map lock;
exact-token RAII removes only the caller's waiter. A selected queued range
refuses new point-read attempts while an existing reader drains, preventing
retrying readers from overtaking the repair. This necessary scheduling refusal
grants no authority, changes no other range's readiness and does not disable
the dispatcher globally. The original role acquisition-parent cancellation
fix being prepared separately is a composition prerequisite; this candidate
also explicitly retains/checks its own original attempt and process tokens.

Focused remote validation, after independent review, should retain the two
unchanged `origin_pre_wal_fault_tests` controls and five new
`native_hot_recovery_` controls. The unchanged baseline must now reach its
existing twenty-second receipt gate through autonomous recovery, with the
same complete RPC/API ledger, first immutable CoreUnknown/sequence=None,
real TX/release/normal handler EXIT, both peers' full native command proofs,
old ACK full payload, any SDK retry ACK and every next-write/Strong outcome.
The two-second setting remains a per-RPC/attempt client setting, not a hard
logical-call wall bound. No budget or success assertion was raised.

The new controls cover a held actual read followed by queued repair and
unrelated-range public APP/Strong progress; cancellation preserving the actual
other reservation; finite DataOpen not starting a process worker; authentic
unprepared/wrong-leader Activate refusals with unchanged catalogue/native
point; and an actual replacement writer refusing the original controller
without queueing, changing native state or clearing the replacement during
old cleanup. Typecheck, no-default compilation and all execution remain
pending. Higher epochs, changed placement, cold/replaced origin, committed
suffixes beyond this selected next command, and the four-to-five image/joint
wire/controller path remain separate gates.
