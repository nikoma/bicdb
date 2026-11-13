# HOT original-origin repair: one-peer-loss and lost-reply audit

**Source review only; the proposed fault tests are NOT EXECUTED.** Exact roots:
`5cec7687196fe6b113805485672e8730c8295a56` and
`f0ebef8085ab264d7996fcab74f946d515c40857`. Their HOT/controller and recovery
runtime paths reviewed here are byte-identical. The existing
[8334/60c packet](../evidence/native-hot-origin-recovery-2026-11-12/README.md)
proves all-three-reachable success in one Core process. It does not prove RF3
one-voter-loss availability or a deliberately lost recovery reply.

## Outer orchestration requires every recipient

[The actual controller](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/hot_recovery.rs#L251)
selects every credentialed voter, sorts the origin last, and retains a
`Job {scope, command, members, phase, position}`. `run_job` advances a phase
only when `position == members.len()`. It increments position only after the
current recipient's real RPC succeeds and the original token/current service
slot pass again. `?` propagates a failed call; there is no skip, qualified
recipient subset or majority-driven outer transition.

Consequently, with origin n0 and ordered peers n1/n2, **n1 remaining unavailable
before its first Fence stalls position 0**. Later finite attempts retain the
same job and retry it; they cannot reach origin Fence, Certify or Publish.
Losing either non-origin later similarly blocks the first phase that needs its
reply. This is a source-derived all-recipient condition, not a newly executed
fault result or evidence of durable loss. Safe refusal is not availability.

| Phase | Actual producer/collector | What a missing recipient means |
| --- | --- | --- |
| Fence | `RangeRecoveryRuntime::fence_epoch` obtains a fresh recipient role, actual freeze and durable native refusal. | The outer controller still waits for every recipient, even though `RecoveryFenceQuorumRound` can later consume a distinct majority. |
| Certify | [`historical_decision`](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_range_recovery/runtime/participants.rs#L216) collects fresh fenced and historical evidence; receiver persists its actual current marker. | Its inner pool may return on sufficient authenticated evidence. The outer loop nevertheless needs a successful Certify RPC from every voter. |
| Apply | `apply_historical` independently repeats decision selection and a current-certification quorum before actual native WAL/history and raw Applied checkpoint. | Majority-capable inner selection does not bypass the outer all-voter Apply loop. NoData/Abort is not a substitute for this committed command. |
| Prepare | [`prepare_native_ready`](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_range_recovery/runtime/native_ready.rs#L29) creates or reattests the actual retained local pin/freeze/materialization. | The outer controller prepares every voter before Complete. A later Ready quorum does not prepare an absent replica. |
| Complete | [`complete_native_recovery`](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_range_recovery/runtime/native_completion.rs#L20) selects real completion decisions or a closed Ready quorum, reacquires fresh role, and persists/readbacks completion. | Inner majority/decision selection is insufficient: every Complete target must reply before the outer Activate phase. |
| Activate | [`activate_native_recovery`](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_range_recovery/runtime/native_activation.rs#L5) collects actual completion QQ, checks fresh role, persists activation and arms/releases the exact owned session. | Followers are activated before origin; one permanently absent follower blocks origin activation and final publication. |
| Publish | [`publish_hot_native_recovery`](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_range_consensus/hot_recovery.rs#L155) holds the same reservation; validates actual activated-open/command history, enrolled local+majority data QQ and distinct final MQ before original recovered admission. | The read collector can finish from a genuine local+majority and veto observed conflicts/higher facts. The current outer pipeline never reaches this path after a permanent earlier target failure. |

The historical pool has at most four workers, two attempts per remote, separate
phase IDs, bounded slices of the original deadline, and early return only when
its typed recorder reports readiness. Ready/completion collectors similarly
check actual distinct evidence, retain conflicts, use two attempts and return
on a valid quorum. Complete first spends half its remaining window on decision
selection; a fallback waits for actual worker drain before collecting Ready.
These details are not an end-to-end one-loss proof. A future fix cannot simply
ignore an error or trust a voter-list/phase DTO to replace those typed proofs.

## Lost reply is distinct from unavailable recipient

A phase request is one real transport exchange; the outer controller does not
retry it inside a longer synthetic request. An error leaves phase/position
unchanged. A later process attempt may acquire a new finite five-second token
and independently fresh role in the same durable session. That is legitimate
new work, not revival of an expired original observation or role.

- Fence/Certify have same-session persisted-refusal/marker paths; retry must
  validate actual state, original command and current scope.
- Apply must observe the exact immutable applied command and receipt if already
  materialized. It must not reapply arbitrary rows or infer success from CQ.
- A retained Prepare retries through `reattest_local_native_ready`, including
  actual refusal re-fsync/current physical checks. After durable completion,
  `refresh_after_durable_native_completion` remains a full standalone check.
- Complete has independently collected Completed/Activated decision paths and
  constrained closed-Ready fallback. Their existence does not prove bounded
  continuation after every ambiguous persistence or expired-reply cut.
- Activate's no-session path checks the actual current-open activated decision;
  it does not recreate a freeze/open permit. The controller must still obtain
  independently fresh final publication proof. An Activated DTO cannot mark
  its coordinator recovered.

Thus transient reply loss has reviewed retry entry points, **not an executed
success guarantee**. The existing baseline injects an actual origin CoreUnknown;
it does not drop a successful recovery reply after its durable effect.
Permanent peer loss stalls the current outer loop regardless of those retries.

## Lifetime and ownership boundaries remain necessary

[The process worker](https://github.com/nikoma/bicdb/blob/f0ebef8085ab264d7996fcab74f946d515c40857/crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/hot_recovery.rs#L102)
uses Weak ownership between attempts, one serialized controller state and a
round-robin cursor. Every attempt has its original five-second token; idle
cadence is at least 500ms from attempt start. No DB/service guard crosses RPCs.
The same-map queued reservation prevents selected-range retrying reads from
overtaking repair, without changing another range's readiness. Cancellation
releases only the exact reservation/waiter. Metadata heartbeat has its own
worker; retrying a dead recipient must not turn into a metadata-global lock.

Collector workers retain their actual counted `Operation` until real exit;
canceling a request is not a hard OS-I/O drain. Stop/Drop cancel the process
worker and retain runtime/listener/hook owners until actual drain. Each actual
role now retains acquisition-parent cancellation. Original service-slot,
writer, DB/open and distinct final-MQ checks remain mandatory on every retry.
None of these necessary lifetime fences permits skipping an unavailable voter.

## Smallest deterministic REQUIRED-success successor

Proposed component: `native_initial_data_actual_rpc_hot_repair_one_peer_unavailable`.
It reuses the actual original three INITIAL NativeHosts, real votes/barrier,
public EventClient, receiver fault probe and durable external attempt journal.
**No test or producer is implemented by this audit.**

1. Open genuine DATA and start all three actual automatic process owners as in
   8334. Before the fault, retain a cfg(test)-only opaque RAII hold of the
   origin controller's **existing serialization mutex**. Worker ticks already
   use `try_lock`, so this scheduling hold cannot block ordinary APP, metadata,
   DB/service access or transaction cleanup. It creates no role, permission,
   counter or native state; it prevents the repair winning the race first.
2. Obtain the old full ACK/Strong. Trigger the same actual receiver pre-WAL
   rejection. Require the immutable first CoreUnknown/sequence None, matching
   transaction/ticket, release callback and normal handler EXIT. Prove origin
   R1/Pending2/CQ2 and both real peers' exact R2/H2/typed-B2/full rows. A sender
   timeout is not handler exit or evidence of rollback.
3. Start the unchanged **20s success clock before requesting peer loss**. Stop
   n1's actual supervisor, then stop/drain its owned DATA/metadata listener under
   the same remaining parent and existing bounded stop budgets. Keep its real
   native files/objects intact; do not alter membership, pins or initial ranges.
   Record successful drain and unreachability, plus genuine fresh metadata QQ
   from surviving n0/n2 while the scheduling hold still prevents repair. An
   unrelated setup/authority failure must be classified before the health gate.
4. Release only the test scheduling hold. The retained process worker must
   repair autonomously. No explicit registry/metadata maintenance fallback,
   direct runtime RPC orchestration, manual coor recovery or counter/reset call
   is allowed. Keep client2s, phase5s and common20s limits unchanged; do not move
   the clock after drain/authority work to hide its cost.
5. Same public client must obtain exact old and committed values, a new real
   APP ACK and exact next Strong value within the common gate. Attempt both
   reads and writes before final assertions; after any ACK, every successful
   None/wrong value is immediately fatal. Errors are unsuccessful observations,
   never absence or rollback. Terminal unavailability is a **FAILED required
   acceptance**, not a passing expected-failure/refusal test.
6. Retain every API intent/outcome, actual wire attempt/route/body/error and
   original Unknown separately; record worker phases/session, peer drain,
   actual final R/H/local-B, real Ready/Complete/Activate/open and original
   publication. Retain any durable unknown until an actual successful full
   observation reconciles its value; never invent an ACK from the probe.

The scheduling hold is the smallest new fixture seam. It must expose only an
opaque guard over the actual controller mutex, never mutable job state or a
readiness setter; dropping it cannot refresh authority. Taking a point-read
reservation before APP would block the admission being tested, so is not this
cut. Starting supervisors only after loss is a simpler secondary diagnostic,
but does not test already-running automatic owners and is not the primary case.
Current source predicts the required-success test fails at n1 Fence. That
prediction still needs execution; no safe refusal may be relabeled as success.

A separate lost-reply successor should drop exactly one real recovery response
**after the selected receiver's actual producer succeeds**, e.g. origin Apply,
and retain proof of effect/handler exit before closing its TLS response. Use a
one-shot receiver/actual-Store/range/session-bound test arm; no fabricated reply,
observer-sink failure, phase setter or new permission. Attach the existing RPC
observer to the same original transport before Arc publication for internal
request/retry linkage, alongside the public client's journal. Then require a
fresh real retry, exact command materialization once, no false absence and the
same terminal next-ACK/Strong gate. Later parameterized Complete/Activate cuts
must preserve actual durable continuation/original caller semantics. These
fault controls and any majority-aware producer change require separate review;
no implementation or HA/physical qualification follows from this design.

## Historical test-only witness source checkpoint

A successor based on `ca8383ef36190c6c69cc8853cd51dd7803c7cc52` implements
only the proposed scheduling seam and required test, with no production
quorum/controller change or execution result. The guard owns the actual state
mutex privately, has no methods or Clone/Serde, and is explicitly !Send/!Sync.
Acquisition polls only `try_lock` under a finite caller deadline; Drop only
unlocks. All three actual automatic owners are started before acquisition.

Selector: `native_hot_one_peer_loss_actual_rpc_requires_autonomous_tail_recovery`.
The child uses the existing fsynced Journal and actual EventClient helpers. It
keeps the original fault's first Unknown immutable, requires a strict held
R1/Pending2 cut plus both native R2/full-value proofs, stops only n1, obtains an
actual freshly authenticated surviving HostMQ with its own attempt trace, then
releases only the scheduling hold. The original two test bodies and shared
helpers are unchanged; their parent file gains only the child module line.
The older module-name filter consequently selects three tests at this new
source, rather than the two historical tests. Source-specific counts must stay
separate; any repeat of the original pair must select their actual names.

Peer stop/drain and the fresh metadata proof consume the same 20s clock; it is
not restarted before health calls. The unchanged client2s and controller5s
budgets remain in force. There is no explicit maintenance fallback or direct
mutating recovery invocation. Every round actually attempts old+committed
reads, a next write and next read before outcome assertions. The gate checks
receipt time and preserves any earlier ACK when rejecting later false absence.
Post-gate observations are nonwaiting diagnostics and do not extend a successful
receipt. Cleanup/report errors cannot replace an earlier prerequisite or
required-acceptance error. Only ledger/log/path evidence is retained; no portable
native DB copy, cold reopen or physical fault is claimed. Current source is
predicted to fail the REQUIRED success gate, not to pass by safe refusal. No
local build or dispatch has been performed.

The direct initial Bootstrap raw-frame gap stays explicitly labelled. The fresh
surviving HostMQ uses a second real observed transport; its request IDs and
observer sequence can restart, so evidence must correlate by actual attempt UUID
and logical context, not assume global numeric request-ID uniqueness. Controller
internal calls remain phase traces, not complete independent wire capture. The
peer-loss check requires the drained endpoint's actual ConnectionRefused with
its caller budget still live; a timeout/expired probe cannot make that cut pass.

## Executed baseline and majority-orchestration implementation contract

The original stopped-peer witness at `ae58458836a070975f1643333c104d0bddce7cc2`
executed [run 43345593406](https://github.com/nikoma/bicdb/actions/runs/43345593406):
0 passed / 1 failed, all-target and isolated-consumer checks passed. The required
failure followed actual origin Unknown/release/normal handler EXIT, origin
R1/Pending2, both peers' actual native R2/full records, clean n1 listener drain,
ConnectionRefused, and a fresh surviving HostMQ. All 15 automatic attempts
remained at Fence position 0. The final health receipt was 15.333326103 seconds;
this is not a full 20-second continuous outage measurement. The complete original
attempt ledger, raw logs, artifact, source/binary identities and limits are in the
[portable failure packet](../evidence/native-hot-one-peer-loss-2026-11-13/README.md).
Safe refusal did not pass its required next-ACK/full-Strong gate.

The following is a reviewed implementation contract, **not implemented or tested
by this documentation**. It covers original-origin hot, unchanged same-config
RF3 committed-tail recovery; cold, origin loss, new domains and fifth-node data
movement remain separate gates.

1. Keep every current voter from the sealed actual `RecoveryRoleAdmission`.
   Before a phase scan/transition require a fresh NormalRecovery role matching the
   exact original session/command/range/membership and original coordinator
   reservation, DB/open, service Store slot and writer registration. A successful
   recipient observation is a per-physical-voter scheduling fact only. It is not
   a role, QQ, Activated grant, user ACK or permission to renew a deadline.
2. A phase advances only after the actual original origin and at least a real
   majority of distinct current voters successfully execute its existing typed
   recipient producer at the selected command/point. Scan the current roster;
   do not remove failed members, derive authority from a reachability list, or
   stop before checking another already received hard constraint. Origin stays
   last; it can never be skipped on the strength of follower completion.
3. Reuse `RecoveryEpochFence`, historical Certify/Apply, Prepare, Complete and
   Activate's actual authenticated packets and sealed observation constructors.
   Their real inner Fence/history/Ready/Completion QQ collectors remain intact.
   `native_ready_observation::project_prepared_native` accepts actual
   NativePrepared or NativeCompleted refusal; a mixed one-Completed/one-Prepared
   pair still needs the genuine closed Ready QQ. A completed reply by itself is
   not a completion quorum. No inferred stages, DTO counters, NoData or Abort may
   replace committed command 2.
4. Final `RangeWriteCoordinator::publish_hot_native_recovery` remains the sole
   recovered-admission producer: the SAME held reservation, actual local
   activated-open/pending obligations, exact original writer/DB/Store/slot,
   independently fresh enrolled data QQ and DISTINCT final MQ all remain
   mandatory. Completed phase counters never substitute for these facts.
5. Add sender-local, nonserializable failure provenance at the actual
   `call_with_native_destination_inner` socket boundaries. Existing connect uses
   `TcpStream::connect_timeout`; TLS/send/read failures originate inside its
   exchange; response identity and authenticated semantic rejection occur only
   after complete frame reception. Distinguish connect refused/reset/aborted,
   broken pipe/EOF and actual socket timeout (including OS WouldBlock from an
   actual bounded socket), versus decoded semantic/identity/pin/native-history
   refusal. No parsing Display strings, raw callback data, error lists or DTOs
   into either success or peer unavailability.
6. Preserve caller/role cancellation precedence. Explicit HOT-only network
   slices are capped by the original request/attempt deadline and do not replace
   or extend authority. Each genuine bounded subrequest must preserve the
   existing transmitted-budget cap: the wire/recipient budget cannot exceed its
   actual original call/transport bound or the phase parent. A silent peer can
   exhaust its network slice while the actual phase parent remains live; that
   is transport unknown-effect, never success. A complete authenticated frame
   with an incompatible identity/semantic body is not reclassified unavailable
   merely because validation finishes late. A role/caller-expired attempt stops.
   A lost origin response requires a fresh genuine reattestation before advancing.
7. Retain finite unavailable-peer backoff as a skip-only scheduling hint across
   phases of this exact command/session. Never let one blackhole consume the
   entire five-second attempt first in every phase, or block unrelated ranges'
   round-robin work. Healthy expensive Complete recipients retain their actual
   remaining budget. No detached workers, larger client/phase/health deadline,
   token revival or authority-changing cooldown. Actual incoming operations own
   their runtime contexts until they exit; cancellation cannot pretend they drained.
8. Authenticated remote errors currently share generic
   `cluster_data_operation_failed`, mapped to `BicDbError::Cluster`; they lack a
   typed busy-versus-history-conflict taxonomy. At minimum every such rejection
   aborts the current scan, cannot count toward success, and prevents its phase
   transition. Retain that physical member/phase as a semantic veto across
   attempts until an actual independently fresh successful same-peer observation
   or an explicit existing typed fresh decision proof resolves the constraint;
   a new role followed only by socket refusal cannot erase the ambiguity.
   Selective permanent conflict classification needs actual typed provenance; it
   must not be inferred from error text. Existing inner QQs' genuine higher or
   incompatible observations remain vetoes. This boundary must be resolved in
   source review before claiming general lost-reply/conflict liveness.
9. Keep retained exact durable phase progress only as scheduling. Publication
   ambiguity/expired responses may leave a real effect; retry its actual
   existing same-session re-fsync/reattest continuation with a separately fresh
   legitimate caller. Never manufacture another pin/freeze, clear Pending or
   resurrect a policy to make a cached phase look current.

Required source/test batch before qualification or main delivery:

| Control | Required result |
| --- | --- |
| Original all-three-reachable 8334 tests, unchanged bodies/helpers | Exact existing payload/Unknown/next-ACK/Strong assertions pass |
| Original ae584 stopped-peer witness | Required actual next ACK and old/committed/next full Strong pass within unchanged receipt gate; no manual maintenance |
| Actual peer socket blackhole/reset/EOF | Typed unavailable/unknown-effect, no success vote; finite original-budget attempts and unrelated range progress |
| Actual lost reply after genuine durable producer effect | First actual effect/outcome retained; fresh reattestation, no inferred ACK or duplicate user effect |
| Lost original-origin reply | Origin must actually succeed a fresh reattestation; followers alone cannot publish it |
| Authenticated semantic/higher/conflicting/identity refusal | Fatal scan/actual constraint veto; never silently skipped as network loss |
| Actual caller/role cancellation, replacement/restore Store ABA and original writer replacement | Existing no-publication/same-owner guards preserved |
| Both active receiver IO and minority unavailable | Runtime context retained to real exit; no fake drain or hard OS-IO bound |

This contract does not claim the completed source supports reset/blackhole/lost
reply yet. The original fault Unknown and all no-ACK API outcomes remain immutable;
receiver persistence or a diagnostic state is never retroactively a user ACK.

### Review resolution: actual bounded wire subrequests and semantic veto

The implementation must cap the **transmitted** request budget to its actual
bounded call child, not advertise a longer role/request budget behind a shorter
network slice. The original five-second phase parent remains unchanged. A typed
actual socket timeout is a non-success only; the sender checks the original
phase parent again before it can become a scheduling hint. Explicit cancellation,
original expiry, query-budget failure or a decoded late/incompatible response
cannot become a successful observation. Existing normal transport calls opt out.

A generic authenticated semantic/identity failure is retained per actual voter
and phase. That voter cannot be skipped when it subsequently becomes unreachable;
only an independently fresh successful same-peer/same-phase observation clears
its veto. This conservatively blocks busy-then-lost-peer scenarios too, because
there is no typed remote busy/history taxonomy. Do not claim that taxonomy or
selective restart qualified. Actual structured inner QQ constraints remain
unchanged. The first sender/controller source checkpoint uses a ten-second
per-job peer retry hint, a 500-ms initial remote Fence subrequest, and otherwise
original remaining phase time (with a 250-ms accounting margin for non-origin
calls). The origin uses its full remaining budget and can never be backoff-skipped.
These are bounded scheduling choices, not membership changes or serving proofs.
All required controls above remain unexecuted until the coherent batch is frozen
and remotely validated; the original ae584 failure is unchanged.

### Coherent source control matrix (uncompiled)

The source successor retains the original two `origin_pre_wal_fault_tests`
bodies/helpers exactly (plus their existing child-module declaration). The new
ae584 child alone is factored into a scenario runner. Its original stopped-peer
branch keeps the same actual fault prerequisites, stop/refusal/fresh-MQ sequence,
unconditional four health calls, 2-second client setting, 5-second phase limit,
20-second receipt gate and `<8s remaining` break. New scenarios re-use these
actual prerequisites/ledger/cleanup, rather than install a fixture election,
quorum, recovered epoch, freeze or counter.

- Stopped peer, actual same-endpoint TCP blackhole, and same-endpoint TCP close
  (Unix zero-linger actual socket reset) all **require** old and committed full
  Strong values plus next external ACK/full Strong. ConnectionRefused is proved
  after the genuine listener stops before the test-owned failure socket binds.
  The failure socket supplies no TLS, metadata, role or data authority.
- Separate follower and original-origin reply-loss scenarios retain all three
  real owners. A cfg(test)-only borrowed `!Send` RAII arm is weakly registered in
  the actual runtime, matching the actual selected range/index. ONLY AFTER the
  real Complete producer returns a live validated `CompletedFrozen` observation
  does the server suppress that one actual reply and end its actual connection.
  The immutable captured fact is diagnostic, not a user ACK/QQ. Each scenario
  requires that boundary fired and then the full real healthy ACK/read gate.
  The origin cannot advance on follower-only phase completion. Guard Drop only
  disarms its exact probe; no strong DB/service/coordinator ownership cycle.
- A distinct **security-negative**, not healthy-repair qualification, revokes the
  real peer's existing writer hook after actual peer native R2 is proved. It
  releases only the controller scheduling hold while that real receiver remains
  reachable, requires the actual job to retain its authenticated Fence veto,
  then stops the same receiver. The actual pending/native point must stay
  unchanged and the veto must survive later connection refusal; no next ACK may
  appear. This intentionally documents the conservative semantic-taxonomy limit.
- Two additional actual initial-host/socket controls distinguish silent-peer
  bounded child timeout from original caller cancellation/expiry, and distinguish
  a genuine authenticated follower's semantic refusal from a real reset/EOF.
  No manufactured reply/error is recorded as an authenticated observation.

Source selectors, expected declaration counts only (NOT runtime results):
`origin_pre_wal_fault_tests` now selects 8 (the original 2 plus 6 child scenarios);
`native_hot_recovery_` selects 8 (original 6 controls plus 2 socket controls).
The sets are disjoint. Run only after independent coherent source review; no
local Cargo, test execution or CI dispatch was performed for this batch. A
blackhole/reset success, lost-reply reattestation, semantic-veto control and
unchanged original success all remain required; compilation alone cannot qualify
any of them. Valid CFT observations of incompatible history remain vetoes in
existing inner collectors. Origin loss/cold, metadata/data domain changes,
member replacement and real fifth-node movement are still open.


### Retained replica debt and wire30 source successor (not executed)

The `23e34d5809d4e457e8af715fddd895c373f822e3` source removed a successful
job immediately after quorum Publish. Its `LostFollowerReply` scenario could
therefore pass the public availability gate with the remaining reachable target
still `NativeCompletedFrozen`. No scheduler subsequently owned that physical
obligation. This is a source finding, not an executed repair failure or evidence
of a lost external ACK.

The successor transfers the actual job into a bounded, retained per-range debt
record only after the existing real publication producer succeeds. It retains
original command/session, canonical data configuration, actual observed recipient
DB/Store-open identities, conservative semantic vetoes and per-recipient next
phase. Those facts choose work only: every continuation obtains a separately
fresh NormalRecovery role, checks its original acquisition cancellation, and
revalidates the exact original writer, retained DB open and revocable current
service Store-slot guard before and after IO. No recovered epoch, genesis,
policy, quorum or new serving permission comes from a saved phase or progress
counter. The original same-map range reservation remains required for origin
publication; it is **not** retained during debt peer IO.

Actual implementation boundaries:

- `initial_data/hot_recovery.rs` transfers jobs to `hot_recovery/replica_debt.rs`.
  One selected recipient per finite controller attempt and at most eight real
  steps preserve round-robin work. A ten-second transport retry hint never
  removes a voter or clears a recorded semantic veto. Successful same-peer,
  same-phase reattestation is required to clear that veto.
- A target left at Complete or Activate uses the existing exact-session real
  Complete/Activate producer, including its fresh inner quorums and actual open
  reattestation. These calls retain the original anchor even after the source
  advances. The scheduler never reconstructs a Ready/Completion QQ from a phase
  counter. The originally observed target DB/Store open must remain identical.
- `distribution_range_consensus/hot_recovery/replica_debt.rs` consumes the actual
  source native lineage and retained same-epoch Applied command, proves the
  command's immutable materialized history leaf, obtains real local Certify and
  a distinct actual target Prepare/Commit/Certify reply, then feeds these actual
  physical voter identities into the existing opaque source plan's `certify`.
  Actual ordinary NativeApply carries that certificate. The source releases all
  service/DB/topology/mutation owners before every network boundary and checks
  actual source/native configuration/Slot/writer/role again after it.
- New wire30 `RecertifyNativeOrigin` is a bounded request with the existing scope
  v2 command/bootstrap/restore/challenge and original remaining budget. The
  INITIAL DATA listener alone adds this arm. Registered replica/control
  allowlists are unchanged. Its runtime acquires an independently fresh role
  for the actual authenticated current leader; the local producer checks actual
  enforced birth, protected native Store, policy and ordinary-write eligibility
  before and after each real Prepare, everCommit-before-Commit, and Certify.
  The opaque authenticated response preserves the actual ACK, recipient
  incarnation, DB open and Store open. A decoded voter list cannot certify a
  command. A late result cannot count after original role/caller expiry.
- Mixed wire29/30 peers are explicitly refused; the source-only control uses a
  real mTLS wire29 Ping and checks the real wire30 refusal plus unchanged actual
  native prefix and catalogue bytes. No silent legacy fallback is introduced.
- Actual target NativeApply/history/raw coverage and a fresh post-IO role plus
  actual Activate/open reattestation are required before clearing its pending
  diagnostic. The record remains tracked for later suffix writes. Actual phase
  receipts/progress remain scheduling data, not current authority.

The required lost-follower-Complete test now separates two clocks. The unchanged
first public twenty-second gate requires old/committed/next full Strong and the
real next ACK. An opaque borrowed `!Send` cfg(test) hold blocks **only** the
actual debt scheduler, acquired before the fault with all automatic process
owners already running. The test requires actual target Completed/native2 while
real R3 has already ACKed, starts a separate twenty-second convergence clock
**before** dispatching real R4, requires R4 ACK/full Strong, and proves target
still Completed2 without R3/R4. Ordinary APP replay has therefore not silently
unfrozen it. Dropping only this hold must allow actual Complete/Activate, real
recertification and NativeApply3/4, exact full source/target Record and H0/H
comparison, actual immutable native leaves for commands1..4, actual current
open and controller-observed pending removal. The convergence receipt is taken
only after the final full native-history checks. No manual tick/recovery is
permitted. Earlier availability is not the replica convergence receipt or RTO.

Declaration selectors for the entire uncompiled coherent stack are
`origin_pre_wal_fault_tests` (8) and `native_hot_recovery_` (9), disjoint, 17 total.
The original two 8334 test bodies/shared helpers remain byte-identical to the
preserved base except the already-existing child module declaration. These are
source counts, **not** runtime results. No local Cargo or hosted dispatch was
performed. This batch is based on the private main491 descendant; parent must
compose actual main492/493 mutation identity/native-grant/format/SQL changes
before final runtime validation. The real fifth Assigned DATA owner requires a
common sealed necessary owner/Slot lifetime, not an INITIAL wrapper or this
NormalRecovery domain proof.

Retained work has an explicit capacity: at most 64 jobs plus tracked ranges,
including converged tracking records; each command retains the existing native
command bound (up to 3 MiB). No unsafe eviction/compaction or unbounded history
claim is introduced. Existing protected abort journal limits (4096 slots/range,
64 MiB inventory) and long-run/8-GiB workload qualification remain separate.

### Remaining native-gap debt: next real producer and required witness

Debt at Fence/Certify/Apply/Prepare is retained with an explicit refusal. A
peer with nativeR1 and real CQ2 cannot be repaired by casting the source's
Activated2 anchors into an ActiveFenceQQ or by issuing Prepare2 against a raw
counter. Existing historical-decision admission requires a genuine active
fence authority; the two healthy voters may now be Activated2/nativeR3 or R4.
The late-phase producer above deliberately supplies no such conversion. A
second origin Unknown while a previous tracked session remains is also refused
until a genuine same-domain session-dominance/retirement producer exists.
These are executable liveness gaps; late Complete recovery is not complete
self healing.

The smallest next materialization producer must independently collect a fresh
real Completed/Activated decision quorum at exact R2/H2, even if healthy peers
have advanced to R3/R4. Under fresh actual NormalRecovery/current leader and
target birth/Store/Slot/session admission, bind actual target native prefixR1
and matching locally durable CQ2/body to H0 and exact append ancestry yielding
that QQ's H2. Mint a disjoint opaque completed-decision materialization scope;
never mint ordinary recovery authority, a new Commit vote, NoData or genesis.
First scope should require actual target CQ2 already exists. Preserve all
permanent promises, everCommit, no-effect candidate audits and completed Core
cleanup. Materializing a separately proven Commit is distinct from casting a
new Commit vote after a minority NoCommitPromise. Conflicting H0/H2/command,
changed native birth/Store/slot, stale role/caller or new data domain must refuse
without new native B/rows. Controlled actual native WAL/prefix/markApplied2 under
owned freeze must precede actual full Prepare/Complete/Activate; then use the
real budgeted suffix producer above. Broader candidate-A/decided-B adoption
still needs its separately proven promise/cleanup protocol.

The next required-success baseline should produce a genuine peer CQ2/native1
cut via a one-shot actual receiver pre-effect NativeApply failure, while the
original origin reaches real CoreUnknown/release/normal handler EXIT and the
other peer really materializes2. A same-owner network-only listener stop/drain
and strict rebind may preserve that target's actual DB/Store/runtime/writer
(the public cold DATA reopen path is unsupported). Never retire its hook and
then recreate INITIAL ownership. Let real source/origin quorum publish2 and
ACK3, rebind the same retained target, and require automatic actual CQ2 native
materialization plus R4 convergence without manual protocol. Freeze this as a
separate required-RED witness until that producer exists; refusal or queued debt
cannot satisfy it. Do not weaken the already required late-phase convergence
or availability assertions while adding it. Cold/replacement/higher-domain,
compacted payloads, NoData suffix, joint/Assigned owner and automatic fifth-node
movement remain separate open producers and acceptance gates.

### Completed-decision native-gap successor (source only, not executed)

A separate successor of `0b7ffe8ad24a8d1d92bf53c180f565f4505945af` implements
the early-debt materialization family. It does not change the meaning of the
preserved baseline, canonical history hashes, old historical scope bytes or
prior runtime results. It is not a second-fault/session-retirement fix.

`completed_apply.rs::CompletedNativeCommandAdmission` owns an actual
`NativeCompletionQuorum` collected in **RequireQuorum** mode, an independently
fresh recipient NormalRecovery role, the exact requested command and original
cancellation. A single activated-decision fallback is refused. This proof is a
new disjoint `RecoveryApplyAdmission` branch; it cannot produce an ordinary
`FreshRangeRecoveryAuthority`, active fence QQ or NoData decision. Its local
recipient/context/member/domain checks remain live throughout physical work.

The incoming wire30 `MaterializeNativeCompleted` arm accepts claims only on the
immutable INITIAL DATA listener and retains the actual original admission
throughout. `runtime/completed_materialize.rs` captures the actual service Store
slot and DB-open guard before collection, repeats original writer/owner/slot/
open checks around IO, waits for actual collector contexts to exit, and obtains
a distinct fresh recipient role. Before any fence it requires the exact locally
retained CQ/Applied command/body under the protected native Store identity.
Missing or compacted bodies are refused; this branch does not insert a command,
clear a promise or cast a new Commit vote.

For an active or missing same-owner session, existing actual preflight and owned
freeze/fence producers precede controlled historical materialization. The DB
Known-prefix producer proves the actual native predecessor, H0 and deterministic
unchanged Applied-command link equal the actual completed quorum's exact R/H.
Certificate construction repeats this check against the opaque Known-prefix's
actual command/session/context binding, so a different Known object cannot mint
a certificate. This first scope requires unchanged physical INITIAL epoch and
leader. The existing controlled transaction, durable AppWAL, materialized native
receipt, immutable native history entry and raw Applied/checkpoint producers
remain mandatory. An already Prepared target is never re-fenced; existing actual
full Prepare reattestation is repeated. The returned sealed Ready point must
exactly match the still-live original completed proof, including H0/H. No engine
or mutation guard crosses collection. The scheduler proceeds to actual Complete
and Activate only after this real preparation, then recertifies suffix commands
through the real wire30 PCC producer from the preceding section.

Persisted provenance is explicitly
`CompletedDecisionMaterializationQuorum`, with the actual completion decision
SHA. The actual local attempt binding also stores that SHA. Ordinary historical
`None` bytes/checksums and its original scope tuple remain unchanged; canonical
`NativeLogicalHistoryEntryV1::Applied` is unchanged. The new scope digest adds a
separate domain only when an actual completed proof exists. These are trusted
local audit receipts, not portable voter signatures or decoded role grants.

The reserved **node-local** reader fence is
`native_completed_decision_materialization_v1`. Its format metadata is fsynced
under the actual completed admission/native journal before the first new pending
record, native transaction or new provenance can persist. It remains sticky
when later ordinary R3/R4 replaces the current prefix. Loading an extant new-kind
prefix requires that fence too. Setting the necessary flag alone never creates
completed materialization permission.

The serialization boundary audit is concrete:

| Sink or transfer | New materialization provenance and reader treatment |
| --- | --- |
| `prepare_historical_native_pending` protected `pending:range`, immutable `pending-attempt` archive | Optional new actual decision SHA; sticky format fence persists first. Original None serialization is unchanged. |
| `complete_native_range_command` protected `prefix:range` page-WAL | Distinct new evidence plus the matching actual attempt digest; ordinary R4 may replace this record, but never removes the root fence. |
| Local actual AppWAL/native materialization/receipt chain | Existing real local transaction/WAL binding and new pending/prefix audit; same owned root is fenced before any such new publication. |
| `SourceRangeImageLineage` | Opaque Debug-only local object; it has no Serde export of its private prefix. |
| `NativeLogicalHistoryEntryV1/StepV1`, `history_export.rs`, image source seal | Only unchanged canonical outcome/command checksum/previous link/H plus receipt SHA as DATA; no local certificate enum or recovery-attempt object is exported. A target constructs its own actual image/base receipts. |
| Portable schema reader features | New local flag is excluded from `REPLICATED_READER_FEATURES`; it is not represented as portable authority. |
| Full backup/capture and restore | `backup.rs` and `backup/capture.rs` retain full actual `FormatMetadata`; restore checks `check_backup_restore_compatible` before acceptance. An older capability set must reject the unknown feature. Native DATA cold reopen/restore execution remains separately unqualified. |

Tests include a **SIMULATED** old-feature decoder on an actual fresh native root
before any completed receipt and after a real format rewrite, with unchanged
native/catalogue state. The native-gap positive additionally requires that same
simulated decoder refuse the actual converted target after later ordinary R4,
and verifies the local flag did not enter portable schema features. Neither test
executes an old binary. Actual old-binary open/restore remains a separate gate;
raw copying without required format metadata is not a supported migration.

New required-success scenarios retain the original automatic owners, public
client, full attempt/Unknown/ACK ledger and unchanged five-second phases,
two-second client setting and first twenty-second health receipt. No manual
maintenance fallback is added:

- Real post-Fence and post-Prepare reply loss is armed on the actual recipient
  runtime, after the genuine producer returns. The one real socket ends without
  that reply. The same target must remain at actual Active2 or Prepared2 while
  real R3 and R4 ACK/full Strong succeed, then automatically reach native4/open
  under its separate twenty-second convergence receipt.
- A one-shot weakly registered borrowed `!Send` pre-effect hook rejects the
  actual authenticated NativeApply2 instruction before effect. After actual
  origin CoreUnknown/release/normal handler EXIT, the test independently proves
  real target CQ2/body plus native1/H1 and the full old ACK record, while the other
  peer really materializes2. It stops and drains only that target's listener;
  DB/Store/writer/coordinator/runtime/admission and metadata owner stay retained.
  With that actual listener unreachable, the original source must autonomously
  publish2 and ACK3/4. The same exact owner then rebinds a new immutable listener;
  dropping only the debt scheduler hold must cause the real completed-decision
  materialization, Prepare/Complete/Activate and native3/4/full-record convergence.
  Network rebind supplies no recovery/readiness authority or genesis.
- Actual authenticated nonleader, missing real CompletedQQ despite raw Applied,
  and pre-dispatch expired-parent controls require precise refusal and unchanged
  catalogue/native point/full ACK Record/original hook. They do not count a
  timeout as a completed-decision semantic refusal.

Expected declaration selectors are now `origin_pre_wal_fault_tests` **11** and
`native_hot_recovery_` **11**, disjoint **22**, not runtime counts. Original two
fault test bodies/helpers remain byte-identical except the existing child module
declaration. The new child scenario runner alone has documented additions.

**Still required before finished self healing:** the retained debt map currently
blocks a second origin Unknown even after first-session physical convergence.
This is a safe refusal but a real repeat-fault regression/gate, not completion.
The next mandatory witness is first Unknown2 to actual all-replica native4/open,
then second actual Unknown5/sequenceNone/cleanup EXIT through SAME original
owners/guards to real native5 and next external ACK6/full Strong/all-replica
convergence. Retirement must independently prove original session completion,
actual original opens and current canonical ancestry, and transfer all unresolved
old constraints; `pending.empty()` is never permission to erase tracking.
Partial old debt must continue as new committed work appears. A pending source
also needs a distinct read-only protected old completion/archive observation:
current serving/SourceLineage APIs correctly refuse Pending, and must not be
relaxed into new readiness. No Activated-to-Active cast, manual recovery, new
factory or fake genesis is acceptable. Compacted payloads need actual image
continuation. Lost-origin/cold/replacement/new-domain/Assigned lifecycle and real
five-node automatic movement remain separate executable gates. No source in
this section has yet been compiled or run locally or remotely.
