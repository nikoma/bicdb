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
