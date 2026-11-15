# FIRST same-OWN metadata service identity acquisition

Source parent is `7c77418656a41ec6e25c7601319e061c328f48e1`.
This unit is source-only: no Cargo build, Rust compilation, test execution,
manual CI, push, version assignment or HA qualification has occurred.
The separate hot Open and future Serving worktrees remain unchanged.

## Observed failure and bounded scope

Normal Tests run 43704937146, Native job 132783896405, recorded the original
catalogue-rename case failing during `actual_ALL_OLD_close`:
`Cluster("joint metadata service identity busy")`. The direct original event
is at raw line 20598 (436081us actual call); footer 22307–22308 preserves the
same error. Source is the one-shot `try_lock` in
`distribution_data/native_joint_metadata.rs:137` at the tested revision.
The intended catalogue rename fault and later assertions were not reached.
The actual lock holder and why it overlapped this call remain unproven.
This is not a native replay, lost ACK, rollback or durability finding.

Canonical raw gzip remains solely collector-owned under
`/root/bicdb-ha-private-evidence/visibility520-pr207-7c7741865-run43704937146/`.
Raw SHA256 is `d64ff32826c1acd4a59aa4819be4ea0933a9ed5c8effa4b1ae6201d99aa83172`.
Original failed evidence is not rewritten by this source change.

## Actual caller and lock ordering

`NativeFirstCoordinator::drive(CloseAll)` invokes `self.own(&token)` before
reserving the current peer's intent and before `request_remote_native_old`.
`self.own` holds only the actual original coordinator/backend/plan and the
original OWN's immutable evidence. It holds no service, database, MStore,
native journal, source-table, mutation or publication-version guard at capture.
The preceding network metadata observation has returned before this boundary.

Only this guard-free production caller selects the distinct
`capture_actual_joint_metadata_point_waiting` method. The original one-shot
API retains its immediate `try_lock` and exact busy refusal for callers that
may already hold a scope. It must not be changed to wait globally.

The waiting entry requires a finite original caller, checks that SAME OWN and
business, and creates only a child bounded by the original caller and existing
OWN deadline. It attempts the SAME actual service Arc in five-millisecond mutex
slices, checking the original caller and OWN before each slice and immediately
after obtaining the guard. No new OWN/Role/transport request is acquired.
A service-identity mutex observation is not a native or metadata grant.

The actual attached registration, database lifetime and current Store slot are
captured by the unchanged protected producer. The service guard is dropped
before actual MStore acquisition, listener/open validation, owned-member
observation, current-version validation and final request binding. All original
legacy metadata-init0 refusals/None behavior remain; FIRST still requires Some.
An attachment/slot/open/version drift or expired original window refuses.
No uncertainty flag, failed mutation result or old native outcome is cleared.

The waiting method is not callable safely while the same caller holds the
service or a lock whose owner needs that service. Its only production callsite
has been traced above. Five-millisecond lock slices are polling bounds; kernel
filesystem work and thread cleanup are not forcibly preemptible. No measured
five-second RTO follows from this source inspection.

## Genuine controls, not executed acceptance

Three definitions use the existing five normal host objects and real TCP/TLS,
actual committed allocation and original range-leader FIRST factory. They stop
before ALL-OLD effects. No service, OWN, role, lease or point escapes their
DATA-only host facade. The real coordinator obtains its original OWN once;
the contention phase reuses it without a new network authority acquisition.

A genuine worker holds the original service mutex and observes its real live
database guard and Store slot. The original one-shot helper must return the
exact busy class while that guard is held. A selected-thread callback fires
only after the SAME service's actual timed lock attempt fails; a planned sleep
alone is not reached-cut evidence.

- Success releases the real guard at that actual contention and requires a
  protected point with the exact original OWN context/term/index, binding and
  actual held Store slot.
- Cancellation cancels the actual capture parent only after real contention,
  requires QueryCanceled and no point, and proves the original OWN remains live.
- Expiry uses a shorter 100ms child inside the original five-second request;
  it does not extend either request or OWN. It requires actual contention,
  QueryTimedOut, expired capture parent, live original OWN and no point.

All controls retain the original capture result and worker cleanup result before
assertion, release the real guard before joins even on early-return/panic paths,
and compare complete original coordinator progress/unknown/intent snapshots.
Earlier setup, OWN, timeout, busy, attachment or cleanup errors fail the intended
cut. Scoped joins remain non-preemptible. The definitions have not compiled or
run; they are not proof that the historical holder was transient or that the
original full catalogue-rename acceptance now passes.

No old native request is retried: waiting occurs before its intent/RPC. All
original FIRST test function bodies, real mutation request construction, constraints,
fault assertions, ACK/Strong tails and action windows remain preserved.
Subsequent normal CI must execute the three new definitions and the unchanged
original failing case; unavailable or failed prerequisite cuts must remain
separate from actual catalogue fault and full HA acceptance.
