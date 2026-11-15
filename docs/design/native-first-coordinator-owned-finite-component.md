# Finite coordinator-owned FIRST component

Source base: `1fcfdc3adf7e38422173e072e6fec4ad56e48188`. This future branch is separate from the preserved failed PR203 head. No source version, build, runtime result, CI dispatch, release, deployment or qualification is produced by this component.

## Scope and actual owners

`NativeMetadataHost::begin_finite_remote_first` requires its actual running INITIAL DATA host, actual supervisor process, a fresh HostMQ and attached current MStore, the committed placement's existing handoff session, and the actual range leader. It creates the operation UUID privately. The UUID is correlation within a trusted crash-fault model; a persistent TLS node certificate or a serialized UUID does not prove an OS process.

The donor is an independent original OLD voter. It need not be the range or metadata leader. The finite driver invokes the existing target fence and genuine TLS ALL-OLD Constrain/Close endpoints. It never finishes an empty local OLD round or constructs an OLD certificate from reply DATA. Original constraints and Close outcomes remain owned by the actual recipients.

Only the actual JoinFresh constructor issues the sealed receiver origin. The Registered listener moves its actual authenticated peer into a queue after fresh Role/OWN/current-MStore admission. The original mutable host owner consumes that queue. Cold/generic receivers and INITIAL/Joined listeners do not acquire this ingress. No listener reconstructs a host from request selectors or MF bytes.

The receiver retains its original A before SourceStart. Start/Describe produce the actual authenticated MF; subsequent bounded calls fetch separately authenticated current frames and use the existing transfer, proposal, Installing, history, rows, verification, final-image, base, catalogue-floor and materialization APIs. These existing APIs continue to own exact native XIDs, attempted candidates, unknown results and Frozen custody. A frame or MF is never decoded into a quorum, mutation permit or Serving grant.

## Bounded steps and original deadlines

Each Step has a new authenticated exchange, original server-ingress deadline, and actual recipient Role/OWN/current-MStore point. The same admitted objects cross the queue; the owner rechecks their original lifetime and currentness before use. Queueing never creates a later deadline. A later Step requires a genuinely new request; it does not renew the expired Begin request. Client timeout/disconnect is not evidence of remote rollback or no native effect.

The original provisional request-history limits of 128/256/512 are removed. One immutable operation binding and monotonically increasing `u64` sequence replace per-frame challenge retention. Admission consumes the sequence irrevocably. There is one pending/in-flight slot, one original unresolved result, one most recent result and bounded hash/count summaries. An expired in-flight request is not assumed quiescent or removed. The coordinator retains one original unknown request and its returned DATA before later postchecks. Counter exhaustion refuses rather than resetting the operation.

`ObservePending` is a distinct readonly request using the same original operation and the lost sequence. It permits an unknown target A, uses fresh actual admission, and returns the actual retained A and returned-call DATA. A delayed lower sequence remains refused after the higher observation watermark. A genuinely returned success can be observed after a lost reply; its cached DATA is not a fresh Role or native grant. An effectful returned error remains unresolved. A specific fresh Describe may recover the original Start export under the existing donor producer; there is no generic Resume or replay of an unknown native row/final stage.

The existing native row states `AttemptedUnknown` and `WalDurablePendingApply` remain blocked. This component does not create a new transaction to bypass them. A panic or process loss can leave an in-flight operation blocked; the RAM ledger is not a cold recovery log.

## Integration boundary

The CLI learner owner thread polls receive work. No periodic coordinator initiator, capacity scheduler or automatic partial-freeze loop is enabled. The coordinator entry is an internal finite component. Materialization does not establish global DECISION quorum, Open, ALL release, writer-domain Serving, cleanup or positive SDK/Strong availability after the selected range is closed.

Production export retirement already exists in the source base, using a readonly Registered NEW export observation; that observation is not LocalNEW or a NEW vote. Later OLD retirement remains a fixture-only RAM custody transition. Source and discovery registries each retain an eight-session bound without terminal reclamation; changed same-range binding also requires genuine retirement/reclaim and stale-message fencing. The coordinator itself also retains an eight-operation map without completed-owner reclamation. The receiver admits one FIRST operation and requires its first/native attempt tables initially empty; a successful per-operation step bound is not a distinct-operation or repeated-range bound. The finite component does not remove those bounds or claim the planned 64-range 4-to-5 campaign. ALL original OLD recipients remain necessary; permanent unavailability replacement requires a separate lawful protocol.

## Evidence states and required acceptance

| Claim and evidence scope | State |
| --- | --- |
| Standalone formatting and whitespace checks of the source leaf | Passed when recorded in its source receipt |
| Real five-host-object finite FIRST success and donor independence | Not tested |
| Lost Close/Start/Enroll reply continuation on actual TCP and same original owners | Not tested |
| More than 512 actual accepted Steps, old/equal-sequence refusal after compaction | Not tested |
| Actual original server token expiry/cancellation prevents that Step's effects | Not tested |
| Actual post-WAL unknown blocks another mutation without a new XID | Not tested |
| Full automatic relocation, Serving/release, permanent-failure healing, cold recovery and registry reclamation | Not tested; implementation incomplete |
| Cause of the original PR203 failures or original physical experiment | Inconclusive |

Acceptance must use five retained normal host objects with distinct actual DB roots/TLS endpoints over real TCP in one Rust test process (not five independent processes, VMs or machines), the real range-leader factory, a donor that is neither data nor metadata leader, and a sealed Fresh receiver. It must not use `remote_old_seed`, a local OLD quorum, manufactured MF, synthetic backend or a fabricated grant. Each prerequisite failure and actual original error must be retained before assertions. Original assertions and finite budgets remain unchanged. Interleaved R1 operations are not evidence of stalled-I/O isolation. No test is passed until the relevant normal repository CI actually executes it.

## Appended acceptance source inventory

Ten new tests share the actual normal-host-object setup, and are all **not tested** until normal repository CI executes the eventual reviewed source:

- finite materialization with a genuine range leader distinct from the metadata leader and a donor that is neither;
- a successful Enroll's actual wire reply suppressed after its original result was retained, followed by fresh readonly observation of the same A;
- cancellation and deadline expiry of the actual queued server token before owner-thread effects, readonly observation, consumed exact/changed-body refusal and a genuinely new Step after proven no effect;
- actual donor SourceStart completion reply loss followed by fresh target-owned Describe and comparison of the retained donor binding/session/MF;
- actual final-image native helper return followed by cancellation of the same original server token, retained CURRENT/archive/XID/custody, and refusal of an independently authenticated fresh next mutation;
- 514 actual selected-range stock-SDK records imported with real bounded one-record row frames, proving more than 512 accepted steps only if executed; exact old/equal wire replays then use the actual source factory/transport and must refuse without another witness or native effect.

The last case is a per-operation sequence-bound control, not an 8 GiB, eight-relocation, repeated-range or 64-range campaign. The new test harness stops admitting further Steps after 120 seconds (900 for its larger sequence case); an already entered call and cleanup retain their own original bounds, so this is not a wall-clock completion/RTO bound. the existing 5-second action, 1.5-second fixture network and 2-second SDK request budgets remain unchanged. These are distinct test-harness limits, not grants to extend any original request.

The test-only placement setup wraps the existing pure allocation planner and actual host-owned native leader/MStore proposal, append and quorum-commit path. It returns DATA only and does not supply a public placement scheduler. The actual target remains on one serialized owner thread; diagnostic channels carry DATA only. The lost-Close and concurrent-admission controls are now present but uncompiled/unrun. A distinct genuine second coordinator factory/process, additional concurrent interleavings, effectful Enroll/row-unknown resolution and cold recovery remain additional open controls/dependencies. Existing OLD protocol behavior is reused, not established by these new tests. Diagnostic omissions and unavailable prerequisite cuts must remain explicit; no generic failure counts as intended-cut evidence.

## Readonly Close reconciliation and setup ownership child

An unresolved same-peer Close can no longer be reissued by `CloseAll`. `ObservePendingClose` uses the existing authenticated readonly OLD endpoint, the original retained coordinator plan/process, and a new actual OWN/MStore request. The endpoint independently checks its original retained table/predecessor/closed owner and exact native CURRENT/archive record. The client validates the same original scope/session, peer, birth and paired materialized facts/record before marking that peer known closed for sequencing. This creates no local OLD observation/quorum or mutation Role; the source export still independently obtains its actual ALL-OLD cut. A constraint-only, absent, changed or unknown closure remains pending and cannot authorize another Close. The original unknown return is retained in the resolution history and never relabeled as successful. This resolution DATA is bounded to 4 MiB around an existing 2 MiB OLD reply.

The new lost-wire control records an actual successful Close reply before suppressing its wire completion, refuses a second Close dispatch, and observes the exact same encoded native record/XID through the real readonly endpoint. The concurrent control holds an actually admitted request on the serialized target owner before effects, outside queue/service/DB/journal/version guards. Two requests are minted by the actual source factory under fresh original observations; the second must reach the actual recipient's in-flight gate. The barrier cannot lengthen the original server deadline and requires that original window still live when released. Its public test handle contains only synchronization DATA, no token/Role/Frozen/host or native permission. Both controls are **not tested**.

The earlier setup had explicit cleanup only after `Five` was completely built. A returned source/learner could therefore leave its supervisor to cancellation-on-Drop and detach instead of invoking the original join/drain API when later setup failed. Every returned owner is now retained immediately in the fixture. Actual setup errors and panics run each original stop API, retain actual cleanup results, cancel the existing process parent, and preserve the original primary error/panic. Cleanup introduces no new completion timeout. A new harness-only error/panic cut after four real supervisors checks successful original stop returns, empty supervisor slots and stopped listener state; this is **not tested** and is not an independent-process crash/recovery proof. Failed stop remains failed/incomplete, never a full-drain claim. Constructors that fail before returning an owner still depend on their own existing cleanup semantics.
