# Closed Ready observer: bounded source analysis

Source: `432fc9bc5e9f681bb40e3bfe6b07fd043bcdf061`. Read-only analysis;
no implementation, changed assertion, build or latency improvement is claimed.
The original full positive and its failed evidence remain unchanged.
Paths below are relative to `crates/bicdb-core/src/`.

## Executed boundary and evidence limits

Runs `43294492826`/Core `131449108302` and `43294496599`/Core
`131449126336` failed during n3 Complete, before CurrentJoint activation.
The first reports 0 passed/1 failed; the second 7 passed/1 failed, with its
idempotence continuation failing in the same original acceptance prerequisite.
Raw SHA-256 respectively:
`56fe5679f76e14c2ae90233f16fdca4eda2d5f20c2846153d693e7ab9b381184` and
`ced64391a271a9457741b1ea9619a23244fdc81831cc714dafbe5c346a8ccfaa`.

| n3 measured stage | Positive | Controls |
| --- | ---: | ---: |
| Protocol selection | 1133.244 ms | 1131.595 ms |
| Sender selection-worker drain | 5.076 ms | 5.069 ms |
| Closed Ready collection | 1803.024 ms | 1793.808 ms |
| Original request remaining at failure | 1056 ms | 1067 ms |

The last exposed peer error is n2/n1 query timeout, respectively. These are
aggregate collection times, **not measured per-peer deadlines**. Custody later
shows n1/n2 NativeCompleted and n3 NativePrepared; it is not a per-handler timing
trace. No exact peer send/receive/deadline or receiver substage spans exist in
these logs. EOF/BrokenPipe diagnostics are retained, not attributed to a physical
storage, networking or Azure cause. Libtest output timestamps are emission times;
use recorded monotonic durations rather than subtracting adjacent output lines.

## Actual collection and receiver call graph

`runtime/native_completion.rs::complete_native_recovery` gives protocol selection
at most half the remaining original request, then drains its sender workers before
Closed Ready fallback. `runtime/native_completion_quorum.rs` and
`runtime/native_ready_quorum.rs` both obtain a fresh actual collector role, perform
a **synchronous local observation first**, and only then create remote jobs.
Each local observation independently obtains its own read-only metadata role.

Remote jobs use `remaining / (waves + 2)`, with at most two attempts per peer.
For RF3/two remote workers the denominator is three; it is applied AFTER local
work. At a 2859 ms Closed Ready entry, the first remote slice is therefore at
most about 953 ms, and smaller after collector/local validation. Selection's
initial two-second budget similarly gives at most about 666 ms before its local
work. Attempt exhaustion can return unavailable with original time remaining.
This source fact does not establish which receiver operation exceeded its slice.

`distribution_transport.rs` dispatches ObserveNativeReady before the generic
service latch. The outgoing wrapper preserves the job token; TLS decoding binds
the actual expected peer/request/challenge. The receiver caps work by its IO
deadline and request budget. It acquires an observer Operation, retained actual
Prepared, fresh observer MQ, THEN the service mutex and checked database read.
It does not perform metadata network work while holding those engine guards.

The receiver subsequently runs:

1. `distribution_data/native_ready.rs::observe_local_native_ready` →
   `native_ready_observation.rs::observe_prepared_native_for_observer` →
   `native_ready_prepare.rs::require_current_for_observer`.
2. For CurrentJoint: `current_joint_recovery.rs::require_current_joint_scope_domain`
   constructs the actual protected domain, checks original writer/local closure,
   exact current binding and every archived floor.
   `serving_configuration/joint_handoff/bridge/activation/recovery.rs` loads the
   real bridge/read scope, then performs a separate full final fact-current
   validation, including another native read scope and original writer check.
3. Prepared checks actual owned physical materialization, native/open, Frozen and
   read pin; then actual Store/session/member identity and catalogue binding.
4. On an already Completed peer, `require_binding_current` invokes
   `serving_configuration.rs::observe_native_recovered_serving_for_physical`:
   full owned materialization AGAIN, current native lineage, native admission,
   then receipt-current with another admission/current-lineage validation.
5. The private projection builds actual bounded irreversible obligations and
   exact phase/point; the runtime checks its original token before returning.
   The sender still validates peer correlation/expiry and the quorum role.

Repeated proof work is source-established; its share of observed time is not.
Contention is also unresolved: sender worker drain does not prove remote handler
drain. A timed-out peer can still own a server Operation/service/native work when
a later attempt starts. Context limits, latch/journal refusal, CPU work and TLS
cost need distinct evidence. Local-observation errors are currently discarded by
`if let Ok`; final unavailable exposes only the last remote error. Preserve all
these diagnostic outcomes rather than treating them as a proven common cause.

## Possible private observer scope, subject to measurements and review

This is a candidate design, not permission to remove checks or cache authority.
Do not cast an observer role into mutation `RecoveryRoleAdmission` to reuse a
completion producer. A new scope must consume the existing actual observer role,
retained Prepared and original request/caller token. It grants no Ready/serving
permission by itself and must not escape the call or be cloneable/serializable.

The scope would retain one uninterrupted actual service mutex/exclusive Store
borrow, checked SAME database read borrow, actual Prepared/Frozen/read-pin owner,
and the real native journal. Under that domain it could derive immutable native
lineage once, then validate the protected CurrentJoint bridge/current+archive,
actual local OLD closure and permanent promise, original writer, exact saved and
archived floors, native R/H/local B and actual namespace/schema/history identities.
It must retain every existing catalogue/irreversible obligation check; counters or
a matching root cannot substitute for missing retained entries or earlier floors.

For Completed, read the actual recovered-serving pointer AND immutable archive,
validate the exact saved native receipt/R/H/B and full catalogue binding, and
project the reply while those SAME borrows remain held. Reusing facts is allowed
only within this owned guard domain; no reusable "already checked" boolean or
stored observation token. Follow existing low-level under-journal helpers; do
not call a helper that reacquires the non-reentrant journal. No network or other
participant acquisition may occur while the journal/service/DB scope is held.

The journal alone does NOT serialize all mutable authority. Before scope entry
and at exit, independently check current hook/registration generation and exact
original writer Arc/coordinator, actual DB/Store open and current service slot,
schema generation/verified digest, durability/health/uncertainty, actual Frozen
and read-pin ownership, observer role/request/challenge and ORIGINAL cancellation
and deadline. Preserve exact raw phase/current binding under the exclusive Store
borrow; any ambiguous persistence/pending policy state still refuses. A source
lineage read before acquiring the guard is not a current scoped fact. No global
application-mutation stamp may be assumed fixed across separate requests.

Current pointer/archive must be read from actual storage, not blessed by cached
JSON. Scope exit must not revive expired original Prepared issuance authority:
materialization can be historical while the observer role is independently fresh.
Any later standalone observer continues to perform full validation. A returned
reply is still only a request-bound observation admitted by an actual quorum
collector, not a freeze release, data write, snapshot or Strong permit.

## Smallest next cause-isolation controls

Add cfg(test)-only correlation to actual peer challenge/attempt: queue/send start,
original/child/request deadlines, receive/start, observer MQ, service and DB borrow,
CurrentJoint construction/final fact/local closure/archives, physical validation,
Completed binding/serving/lineage/admission, response/write/decode and Operation
exit. Preserve exact first and last errors, parent state and actual journal-busy
refusals. Enable existing detail spans only inside that real observer invocation;
do not add scans or native IO solely for telemetry.

First execute the unchanged full positive and separate idempotence prerequisite
with those spans under unchanged budgets/concurrency. A genuine RF3 Prepared
versus Completed observer control can isolate the extra Completed path using real
MQ/native production. A bounded actual receiver barrier after observer MQ can
isolate service-wait/cancel/drain behavior without manufacturing a quorum, and
must retain the real server Operation until it actually exits. Lost/late replies
and changed original writer/open/slot/schema must remain refusals.

Only measured substage evidence can justify a scoped-read implementation or a
fairer schedule under the original parent. No longer timeout, fixture idle,
serialization, omitted participant/assertion or unmeasured capacity claim follows
from this note. Full 8 GiB/online/cold/operator and physical gates remain open.
