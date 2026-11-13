# Genuine registered fifth: CLOSED image producer audit

Read-only audit of full prototype `8334a99e666970263c2d5a4192b5c9ce61d26677`,
inherited source `1.0.490-beta`. No new producer, local build, executed baseline,
receiver row permission or fifth-node completion is claimed.

## Actual producer chain and earliest gate

| Existing opaque producer | Actual inputs/facts | Limit relevant to a data learner |
| --- | --- | --- |
| `NativeUnassignedLearnerFactory::create`, `learner_factory.rs` | OWN fresh HostMQ, actual TLS pin/incarnation, fixed schema bundle, fresh actual DB/birth and manifest; `initial_ranges=[]` | Requires registered unbound metadata Learner and **no data assignments**. Never supplies INITIAL range genesis or writer admission. |
| `NativeLearnerBirthObservation`, `learner_birth.rs` and transport leaf | Actual same-factory/provider/open, receiver OWN MQ, authenticated TLS/requester challenge and original deadline | Identity fact only. Leader needs a separately fresh OWN MQ and actual owned Leader C/term/index to propose enrollment. |
| `propose_observed_native_learner_enrollment` / `start_owned_metadata` | Real observed birth, old-voter committed binding, actual same-service drain, protected Registered MStore UUID, actual bound Append ACK | Birth binding alone is not a metadata ACK, promotion or data proof. |
| `NativeLearnerPromotionReadiness` | Genuine receiver-owned sealed ACK and current leader MQ, exact protected receiver UUID/frontier | Promotion changes metadata membership only. Numeric position/serialized reply cannot mint it. |
| `NativeOwnedMetadataMemberObservation` + factory validation | Current local installed C equals fresh OWN MQ; actual protected Registered UUID, DB/open, birth/schema/namespace/restore | The factory validator deliberately refuses **any data placement**, so this metadata-only proof is not an assigned-receiver continuation. |
| `NativeRegisteredReplicaReceiver::prepare` | SAME factory/DB/service/Store slot, actual owned protected MStore proof, actual from-birth writer/abort contract, no installed writer; true drained new listener | Necessary receiver/open lifetime, never materialization, row grant or Serving. Open while unassigned; an assigned continuation producer is still missing. |
| Existing `RecoveryRoleAdmission` / `RecoveryFenceScope` | Independently fresh actual metadata role, exact CommittedHandoff plan/session/nonce/target and original acquisition cancellation | Configuration purpose is not local native custody, OLD materialization or image row authority. |

The immutable registered listener explicitly permits `RecoveryEpochFence`
(`distribution_transport/registered_replica_listener.rs`). The actual path is:

`RangeRecoveryRuntime::fence_epoch` → actual recipient role →
`ClusterDataNodeService::preflight_native_recovery` →
`RangeWriteStore::preflight_recovery_fence` → `require_no_image_install` →
`require_native_writer_host_range_id` → `require_native_writer_host_base`.

For a genuine enforced fifth with no writer, the last guard refuses before the
runtime creates its range freeze or persists a generic recovery fence. This is
a source prediction, not yet an executed error. Installing INITIAL policy,
clearing enforcement, treating None as permission or bypassing this guard is not
a valid registered-receiver producer.

There are later independent gates. Transfer uses the same durable recovery
scope. C1 `prepare_closed_image_original_writer` explicitly requires the actual
original writer Arc and genuine native OLD proof; it cannot represent a fresh
fifth. Both staged row construction and actual transaction authorization/commit
still require appropriate typed admission. Ordinary APP/Strong predicates must
remain unchanged.

## Smallest distinct admission contract, not an implemented API

A new assigned-receiver observation must come from the **actual** Registered
MStore/DB/open/Store slot, separately fresh OWN MQ and current Pending role. It
must bind exact Active member incarnation/pins/birth, protected metadata UUID
and native catalogue UUID/custody, schema/restore and actual namespace mapping,
plus the committed range's exact learner target/session/nonce/epoch. Existing
blank/create/Prepared/no-placement factory predicates stay strict; the new path
continues a protected Registered owner, not a fresh factory or decoded proposal.

Refusal-only fencing is distinct from row admission: a generic durable fence
may block writes, but cannot establish B/R/H or an absence proof. Before any
history/clear/copy row permission, retain the genuine original native OLD QQ,
actual source close/capture/Frozen/pin and their original cancellation, exact
staged-history proof and **all** actual target raw/CQ/pending/permanent refusal
and native slot/promise/no-effect obligations. Empty counters or no hook alone
are never eligibility. Initially unsupported nonempty domains must refuse
without clearing them. Recheck actual owner/slot/open/schema/health and exact
original BEGIN and per-step role/caller cancellation at row construction and
actual transaction commit. No future global NEW decision is required to prove
the earlier image, which would create a dependency cycle.

The capability grants only the named CLOSED image operations. It cannot enroll
an ordinary writer, arm a generation, admit APP/Strong or release the freeze.
If new durable semantics require persisted provenance, old-reader rejection
must be explicit; an optional field or stored digest alone is not authority.

## First test-only baseline and remaining goal

Use the existing four-INITIAL/RF3 actual TLS enrollment fixture and its genuine
fifth factory/owned-ACK promotion path. Open the immutable registered receiver
while unassigned, retaining the same factory, MStore UUID, birth/open/root/schema,
namespace IDs and Store slot. Open DATA only on the four actual INITIAL owners,
retain all four original writer guards, and obtain full acknowledged source
payloads. Commit real Pending DATA learner assignment to the fifth through the
actual metadata stores. Attempt a real TLS CommittedHandoff fence and record
the exact earliest error/phase and unchanged native/raw identities **before**
the required success assertion. Repeat a second range on the SAME fifth; no
factory recreation or INITIAL permission for it. This required baseline is not
a passing characterization merely because refusal is safe.

Local capture/OLD observations contain non-serializable ownership. Existing
birth/config/metadata ACK RPCs do not transfer a native image/source session.
Remote kernel/session admission and durable cold/resume reissue therefore remain
separate missing producers. The full goal still requires all-range population,
real NEW native quorum/open, target next ACK/full Strong, ordinary movement and
fault self-healing; a receiver fence or local image component cannot substitute.

## Frozen test-only baseline shape

`promotion_tests/registered_image_baseline.rs` adds the single focused selector
`native_registered_fifth_actual_closed_handoff_fence_without_initial_writer_baseline`.
It reuses `EnrollmentFixture` and its genuine birth/owned-ACK promotion producers,
then opens the existing registered receiver **before** DATA assignment. Only the
four retained actual INITIAL launches become DATA hosts; the fifth factory stays
`initial_ranges=[]`, with no installed writer and UNKNOWN per-range native point.
The four original writer guards are retained from before the two source ACKs
through both actual Pending range assignments and target fence attempts.

The test separately proves the current OWN HostMQ/protected Registered member
identity against its SAME actual DB and rechecks its store. The existing
metadata-only factory validator must still refuse data placement. This is not
permission to ignore that validator in production. A new actual target provider
wraps the SAME owned metadata backend for fresh request-scope production; the
target's actual registered listener/runtime independently acquires and validates
its own recipient witness when the genuine TLS fence request arrives.

API ACK/error and complete payload are fsynced before native-row inspection;
each actual fence error, original caller/role remaining budget, target snapshot
and owner identity are retained before the final required-success assertion.
Two safe refusals therefore remain **RED**, not a passing recovery test. Source
full ACK records are reconciled again, with original writer guards checked at
both boundaries. There is no OLD closed-data quorum or source image capture in
this earliest-fence baseline; these later actual image producers remain required.

The test attempts bounded raw custody of all five actual DB roots, including
their nested owned metadata, while all five real metadata/service/DB/native
journal guards remain held. Acquisitions are nonwaiting, credential-like files
and symlinks refuse, copies/fsyncs/hash inventories are bounded and checked, and
custody failure is explicit. Listeners are **not** claimed drained; live-owner
observed stability is not an atomic power-loss/replay proof. Original roots stay
retained even on failure. Cleanup errors are recorded without replacing the
original failure. All of this is test source only; no local compilation or
baseline execution is claimed.

The two baseline ranges are both assigned while this SAME fifth is still
unmaterialized. This does not qualify reusing it for a second image after the
first image has advanced local B or acquired permanent slot/promise history.
That continuation cannot rely on `B==0` or the old factory's no-business guard:
it must prove actual already-installed per-range native history and keep those
other-range obligations while admitting only the exact new CLOSED range. No
legitimate target writer-start/promise can currently be manufactured merely to
test a nonempty fifth domain: APP is refused and the earlier fence is missing.
Those tests need the corresponding genuine producers, not raw fixture edits.
