# A second populated range on an already Serving Joined member

Source reviewed: `5a85900f571c389ff97e278a0b9555048bdeadec` (qualification
prototype, inherited source version `1.0.502-beta`). This is a source-pinned
finding, not a claim about every later main revision. The accompanying
[source index](native-joined-second-populated-range-source-index.json) retains
all 16 reviewed paths, Git blob IDs and SHA256 hashes.

**Confirmed implementation gap:** the reviewed Registered FIRST → Joined path
cannot receive a second populated range into the same already Serving database
while preserving the first range's serving owner. FIRST is a bounded transition
from a never-written Registered receiver to its first DATA policy. It is not a
general online add-range admission.

| Finding | Classification | Evidence scope |
| --- | --- | --- |
| Two distinct source ranges populated through one ordinary SDK object | Implemented fixture setup | Source inspection; not a second-movement result |
| First selected range has a Registered image/NEW/Open/Joined path | Implemented path and fixture assertions | This document imports no execution or qualification result |
| Already Serving Joined receives another populated range without replacing its database/writer | **Confirmed implementation gap** | Actual admission/host dispatch boundaries below |
| First range's APP and full Strong continue throughout second movement | **Not tested** | Required complete scenario cannot be substituted by the FIRST path |
| Joined later becomes a donor; cold restart resumes both ranges | **Separate open gates** | Neither follows from first DATA publication |
| Physical scale-out, stable throughput gain and production qualification | **Not established by this review** | Original failed Azure qualification remains unchanged |

Source inspection confirms the missing producer. It does not establish a data
loss incident, a failed runtime second move, or the cause of an earlier timeout.

## Exact source boundaries

The following paths are relative to `crates/bicdb-core/src/`. Line numbers refer
to the pinned source above. Functions, predicates and hashes remain the primary
identifiers when a later integration shifts line numbers.

| Path and function | Confirmed boundary |
| --- | --- |
| `distribution_native_host/provisional_listener/registration/registered_replica/remote_image.rs:176`, `NativeRegisteredReplicaAdmission::observe_remote_image_step_inner` | At line 238 the real target requires `require_registered_remote_image_writer_absence(&self.original_hook)` before materialization. An already installed Joined writer does not satisfy it. |
| `db/range_image_native/writer_enrollment.rs:641`, `BicDb::require_registered_remote_image_writer_absence`; private records check at line 676 | Both current and previous protected writer-policy pointers must be absent. Removing an in-memory hook does not restore this never-installed condition. |
| `distribution_native_host/provisional_listener/registration/registered_replica/remote_attempt/remote_joint/first_data_policy.rs:95`, `NativeRegisteredServingDataPolicyOrigin::require_service_identity` | Lines 121–137 require exactly **one range on which this local node is a Voter**. This is not a count of all cluster descriptors. A second Learner is not yet a second local Voter; writer absence already blocks reusing this lane for its populated receive. |
| `distribution_native_host/provisional_listener/registration/joined_data.rs:30`, `NativeJoinedDataHost`; `open_registered_joined_data` at line 417 | Retains the exact original FIRST attempt, Installed guard/coordinator and one selected range. The Assigned owner is transferred and removed at line 434. No populated second-range receive/policy continuation is supplied here. |
| `distribution_native_host/provisional_listener/registration/writer_data_domain.rs:122`, `begin_native_writer_data_domain`, and following CLOSED/lease/Serving methods | Dispatches to INITIAL or Assigned DATA only, not Joined. Retained writer-domain structures do not themselves create a Joined adapter or another range grant. |
| `distribution_native_host/learner_factory/assigned_closed.rs:4`, `require_assigned_closed_database` | Lines 50–53 explicitly refuse nonzero committed business state or retained native abort obligations. The initial Assigned CLOSED lane is not a populated-target fallback. |
| `distribution_native_host/provisional_listener/registration/initial_data.rs:107`, `NativeInitialDataAdmission::for_joined_owner`; `require_initial_source_hosting` at line 196 | Joined has empty source-writer/coordinator, source-registry and OLD weak bindings. `replace_initial_writer` at line 86 refuses using refresh to acquire donor bindings. |
| `distribution_transport/initial_data_listener.rs:35`, `require_request` | Actual source-frame, remote OLD and source-discovery ingress requires INITIAL source hosting. Sharing the APP/Get listener does not grant Joined donor authority. |

`prepare_registered_remote_install_attempt` in
`distribution_native_host/provisional_listener/registration/registered_replica/remote_attempt.rs:350`
also refuses existing INITIAL/Assigned DATA. That predicate alone is **not** a
complete Joined refusal: the Assigned option has been taken during Joined
transfer. The downstream native writer-absence check is decisive and must not
be omitted from the explanation.

The current fixture does not secretly exercise the missing transition.
`registered_image_baseline.rs:398` writes both source ranges using its original
two-second `ClusterEventClient`. `key_in` at line 164 searches the real
`cluster_event_record_id`/`range_for_key` hashes; no range ID is passed to the
SDK. Its hot branch at line 462 explicitly restores the second descriptor and
removes that relocation. In
`remote_source_tests/hot_policy_tests/data_tests.rs:312`, the original client
Strong-checks both original ACKs after the first join, but its new post-join APP
selects only the moved range. Two populated source records do not prove two
moves onto the target.

## Smallest genuine missing producer

Add a distinct **selected-range receive continuation owned by the existing
Joined host**. The description below is a required contract, not an implemented
API or a proposed serialized authority token.

1. Start from the actual retained Joined host, same service, database/open,
   native birth, Store slot, installed writer and coordinator. Retain its FIRST
   attempt and first range's native obligations. A private non-Serde producer
   must prove these identities from live owners; a path, manifest DTO, historical
   receipt, source Role or local image quorum cannot create that owner.
2. Bind the second range's genuine committed handoff, actual donor capture and
   authenticated source stream. Acquire fresh request-specific Role and OWN
   observations with the real current MStore. Apply the protected selected
   metadata-version fence at each mutation/publication sink, together with
   native epoch/promise and dual-quorum constraints. A local MStore guard is not
   a distributed lease; a still-live TTL snapshot is not sufficient authority.
3. Freeze/drain only the second range under a retained exact attempt. Copy and
   validate its filtered rows, history, namespace/schema and native/catalogue
   evidence into the existing database. Preserve exact native transaction IDs,
   known/uncertain outcomes, WAL retention and pending obligations before any
   fallible write/postcheck. Unknown outcomes remain fenced and recover using
   the same attempt; retries must not silently allocate a replacement.
4. Keep the first range's policy, request-bound read barriers and native prefix
   valid while the second is Installing. Use normal indexed/WAL application for
   incoming rows, without deleting first-range rows/index entries, resetting
   its native floors or replaying imported business side effects. Shared
   database commit sequence may legitimately advance; require monotonicity and
   exact range histories, not byte-identical global counters. Do not replace
   the whole database, its namespace identity or its active writer.
5. Publish the second range only after its actual NEW/decision, committed
   metadata/Open and required ALL-release proof. Extend the retained writer's
   selected policy through a genuine Joined transition preserving the first
   scope; do not rerun FIRST or generic re-enrollment. Publish dispatcher state
   under actual held native/hook/open proof and the short current-version gate.
   Every Strong request still needs its own current-authority/applied-prefix
   barrier; no shared `strong_ready` bit grants later reads.

Do not relax FIRST's writer-absence or one-local-voter checks, reinterpret
INITIAL genesis, fabricate a local owner for a remote fact, clear retained
obligations, or stop the first DATA owner merely to reuse a blank receiver.
Those are safety boundaries. A missing populated-owner producer is a feature
gap to implement and qualify independently.

Independent-range availability also needs a real blocking-I/O test. At
`distribution_range_image.rs:621`, export retains an actual DB read guard
through its bounded batch; other service/journal guards occur along the path.
Per-range freezes and bounded batches alone do not demonstrate that an unrelated
write can continue while an engine or disk operation is stalled.

## Required acceptance and adversarial controls

Use the same five actual native nodes and one retained original gateway SDK
object. Populate two keys that genuinely hash to different ranges and retain
all acknowledged full payloads externally. Keep the existing per-call budgets;
applications supply only namespace/key/value, never shard IDs, range leaders,
manual topology installation or a new gateway.

1. Complete the first range's real handoff through image/history/rows,
   native/catalogue, NEW/decision/Open/ALL-release, Joined policy and DATA.
   The original client must Strong-read its old full payload, APP a fresh first
   range value, and Strong-read that acknowledged value.
2. Begin the second populated range's receive on the **same** Joined database,
   using the missing genuine producer. During demonstrated transport stalls
   and separately real blocked engine/disk phases, repeatedly APP and full
   Strong-read the first range through the original client. Every ACK remains
   readable under the stated consistency contract. Unavailable authority must
   be an explicit retryable refusal, never successful false absence.
3. Release the fault within the original bounds. Require second-range
   convergence, exact full values on both ranges, correct range epoch/leader/
   applied-prefix evidence, preserved original database/hook/coordinator
   identity, no lost retained obligations and no unfinished relocation.

Retain operation intent, record IDs, expected hashes, actual ACK/uncertain/
failed/retried outcomes, actual wire routes/errors and their topology context,
replica watermarks and readiness separately. Planned descriptors are comparison
DATA, not evidence of the SDK's actual dispatch. Eventual visibility cannot
retroactively acknowledge a timed-out write.

The minimum negative controls are: actual relevant MStore/term change after
proof acquisition; source loss during second-range catch-up; target write/apply
uncertainty retaining its original transaction; and controller restart during
cleanup retaining exact progress. Each must preserve the first range's ACKs and
request-specific Strong semantics without publishing stale second-range
permission. A complete release qualification additionally requires actual
partition/new-leader fencing, not just local metadata-change injection.

Joined becoming OLD/donor for a later move is a distinct producer and test:
create genuine Joined source/capture/retention ownership rather than filling its
empty INITIAL bindings. Cold restart is also separate: restore both range
policies and reconcile exact pending row/native/WAL outcomes from durable state
before serving. A successful hot second move would not qualify either gate.

## Narrower component and qualification boundary

The separate test-only leaf
`c08fb440be78ff1b891dc9a5e4c6ec87228918cb` implements selector
`native_registered_fifth_remote_selected_rows_reply_stall_other_populated_range_original_sdk_progress`.
It holds an actual selected-range Rows transport reply **after export and after
engine/session guards are released**, while the original two-second SDK uses
another populated INITIAL source range. It retains the original OLD/Manifest
journal prefix and appends the Rows observation to that same chain. The prior
`a6cbea305`/`cb07f6047` leaves preserve a confirmed test-journal overwrite defect;
they are not qualified by the source correction.

This component can address transport independence after export. It does not
receive a second range into Joined, stall a disk under an engine guard, finish
a relocation, or exercise an automatic operator controller. Its separately
owned qualification run/result is outside this source-design document; no live
or later passing result is imported here. The compiler-listed baseline is 82
cases, including 11 macro-generated controls; the one added case expects 83,
subject to actual compiler listing. The earlier 71-name guard was incomplete,
not evidence of 71 executed tests.

The full second-move runtime scenario remains **not tested** by this document.
There is no independent-host, throughput, physical RPO/RTO, production admission
or deployment claim. Preserve the original failed Azure scale-out evidence and
require a stable converged five-node capacity comparison separately.
