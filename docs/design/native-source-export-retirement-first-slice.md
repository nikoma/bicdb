# S2 export retirement: two authenticated hops, partial implementation

This source batch is based on runtime `5a85900f571c389ff97e278a0b9555048bdeadec`
(`1.0.502-beta`). It implements the first remote source-retirement component;
it does not establish a current-main release, successful relocation, Serving,
physical qualification or throughput improvement. The thirteen new actual-TCP
controls have not been compiled or executed at this document's source freeze.

The earlier qualification of the unchanged base compiled and listed **82**
tests. Its expected-name guard stopped before runtime because it expected 71
and omitted 11 macro-generated controls. Runtime execution was zero. Neither
that guard failure nor this source batch is an 82-test runtime result. Future
qualification must retain and compare the actual compiler-produced list.

## Actors and authority

The original target sends `NativeSourceExportRetirement` to its retained S2
donor. That donor independently calls the target's distinct
`NativeObserveRegisteredExportNew` endpoint. Each hop requires actual mutual
TLS and the expected native destination birth. Donor, retiring replica, range
leader and metadata leader remain separate identities. The fixture chooses a
donor different from both the range leader and retiring replica.

The donor never acquires a fabricated `CommittedHandoff` Role. Its actual OLD
owner and fresh recipient OWN/MStore checks authorize only the source operation.
The target retains a historical plan identity when it takes its actual original
image owner under genuine Role/OWN. That identity retains no Role, old parent or
deadline. Every later read-only observation independently acquires target OWN
and checks the current Pending plan, range/configuration, epoch, enrolled births,
TLS pins, schema and restore identity. Historical identity is comparison DATA,
not continuing leadership authority.

The target requires its original retained Registered owner, actual native
base/final archive, original A, original DB/Store/MStore, current receiver hook
and freeze. It retains a real target read pin before producing a reply. Existing
certificate-backed local NEW construction is unchanged. The new reply and
transport wrapper are distinct from LocalNEW, NEW quorum, a transaction grant
and Serving admission; no deserialized object constructs those authorities.

## Actual call and retention path

1. The INITIAL donor checks the authenticated original target and independently
   acquires its own current OWN and actual protected metadata-member observation.
2. It finds the existing exact S2 attempt using Describe semantics. A missing
   attempt cannot be created by retirement. The claim serializes S2 operations;
   the actual source-session latch excludes exporters at the eventual detach.
3. The donor checks the exact MF, source session, target A, plan, births and its
   own retained OLD certificate. It invokes the Registered target over mTLS;
   no MStore, service, DB, source-session or registry-map latch spans this call.
4. The target checks its genuine retained native/image/catalogue/A owner under
   fresh target OWN/MStore. It retains the actual freeze/read pin and exact
   request challenge before reply checks. The donor accepts only the sealed
   transport observation from the actual target and compares it with its own
   completed capture's root, count, bytes, B and R/H0/H.
5. After peer I/O, the donor acquires a new final recipient OWN/MStore and checks
   the original source, writer, native OLD receipts, DB/open/Store and capture.
   It retains the exact actual target observation and intended outcome before
   final checks, cancellation or snapshot removal.
6. After all native/MStore/DB checks, it acquires a nonblocking read guard from
   the actual final member's original metadata-version point. The guard spans
   only cancellation, capture detachment and the precomputed terminal outcome
   move. It drops the guard before the capture destructor or any postchecks.
   Busy/changed/dirty metadata preserves the intended observation and capture.
   This co-fences only local metadata authority, not a distributed lease or
   foreign liveness. It cancels only the original exporter session, removes its
   completed capture, and installs terminal custody before fallible reply checks.
   It keeps OLD certificates, freezes and irreversible native closures.

`Retire` and `Describe` can reconcile the exact same-process terminal outcome
after a lost reply. A new request has fresh authentication and recipient checks;
it never renews the canceled exporter. The immutable terminal outcome is
separate from the new reply's current metadata observation. This permits an
unchanged outcome to be reported after unrelated metadata advances. Challenge
histories and observations are bounded and never evicted by timeout.

## Source paths

| Boundary | Source |
| --- | --- |
| Dedicated wire DATA and source side-effect sink | `crates/bicdb-core/src/distribution_range_image/remote_source/export_retirement.rs` |
| Actual two pinned-mTLS client hops | `crates/bicdb-core/src/distribution_transport/native_source_export.rs` |
| Recipient donor consumer and fresh final authority | `crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/source_discovery/export_retirement.rs` |
| Original Registered target producer | `crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/registered_replica/remote_attempt/remote_joint/export_new.rs` |
| Retained original target and immutable listener custody | `registration/registered_replica/remote_attempt/remote_joint.rs`, `registration/registered_replica.rs` under `distribution_native_host/provisional_listener` |
| Historical plan provenance, fresh target OWN checks | `crates/bicdb-core/src/distribution_range_handoff/joint/plan.rs` |
| Dedicated before-latch dispatch and immutable listener allowlists | `distribution_transport.rs`, `distribution_transport/initial_data_listener.rs`, `distribution_transport/registered_replica_listener.rs` under `crates/bicdb-core/src` |
| Actual fixture controls | `distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/source_export_retirement_tests.rs` under `crates/bicdb-core/src` |

## Controls awaiting qualification

The new tests seed genuine remote OLD closes, let their original mutation
requests expire, and then start the source through actual S2 TCP ingress. They
do not publish a local source retainer or pass a local NEW object to the donor.
They preserve operation intents, exact outcomes, original source-frame journal,
APP acknowledgments and native OLD records before assertions.
An additional memory-only actual-gate observation distinguishes capture-object
removal from retiring its actual capture-owned reference. Successful cases retain
a private test-only Weak to the exact actual read-pin Arc and require
its strong-reference count to decrease after dropping the capture. OLD retains
the shared underlying gate pin and freeze; the target read pin also remains.
Diagnostic contention is recorded as an error, never interpreted as no pin.

| Control | Required observation | Status |
| --- | --- | --- |
| Nonleader/nonretiree donor | Actual two-hop proof; only completed export removed | Not tested |
| Lost retirement reply | Same terminal outcome; original MF/A/session and native OLD unchanged | Not tested |
| Lost S2 Start reply | Original 2-second request fails; fresh Describe finds that same capture before retirement | Not tested |
| Wrong authenticated actor | Refusal before export removal | Not tested |
| Changed MF | Refusal before export removal | Not tested |
| Changed A | Refusal before export removal | Not tested |
| Different actual enrolled node birth | Refusal before export removal; no birth DATA cast to native proof | Not tested |
| Actual target Store replacement and restoration | Revoked original slot refuses; no reconstructed owner | Not tested |
| Actual local MStore term advance at final detach | Actual term mutator invalidates original version point while original request remains live; intended observation and export retained | Not tested |
| Actual OWN expiry at final detach | Original deadline expires; intended observation and capture retained | Not tested |
| Actual original process cancellation at final detach | Source-process ancestry cancels while actual OWN remains live; intended observation and capture retained | Not tested |
| Actual original donor-process cancellation | Refusal; no replacement source/capture | Not tested |
| Actual native commit-sync-unknown prerequisite | Existing real fault control leaves no completed target NEW owner; export stays retained | Not tested |

These controls use one test process with multiple actual node/DB owners and
loopback TCP/mTLS on one CI host,
not independent-machine tests, host power-loss evidence, a physical ledger
qualification campaign, or unchanged SQL application acceptance.

## Remaining blockers

This component retires only a completed source export read-pin reference. It does not produce
remote NEW quorum, a globally durable decision, activation, ALL-Open,
OLD-custody release, target read-pin release, FIRST policy, Serving or controller
convergence. The new target read pin deliberately blocks incompatible local
mutation/release paths until a genuine later continuation consumes it. A local
NEW constructor cannot silently replace it. No NEW/decision/Open proof is
fabricated to escape that obligation.

The source/session/discovery registries still retain their cap of eight.
Removing an export capture/reference does not remove those entries. Safe registry retirement
needs actual decision/Open/cleanup producers and remains a blocker for the full
64-range four-to-five-node qualification. There is no TTL eviction.

Retries remain same-process and Pending-plan scoped. Post-policy or cold retries
can fail bounded-unavailable; no old Role, parent or native outcome is revived.
The source retirement record is retained process custody, not a claimed durable
cluster decision or cold-resumption protocol. Existing native UNKNOWN or
poisoned outcomes still fence admission.

Independent index/FTS parity, arbitrary cross-range SQL, durable multi-range SQL
transactions, source/target process replacement, full controller integration,
the populated second range after Serving, 8 GiB evidence, fault-safe 64-range
convergence and a sustained post-convergence throughput gain remain separate
required gates. No new Azure infrastructure or production deployment is part of
this component.

## Source-review correction

An initial review suspected that the metadata-version guard omitted the OWN
deadline and original RPC cancellation. Source inspection corrected that
suspicion: `owned_member.rs` seals its actual version token from the original
OWN request with the OWN deadline, and `try_publication_guard` checks that token
before and after taking the nonblocking version read guard. This was not a
measured runtime failure. Original source-process/session cancellation is a
different owner; the new sink checks that ancestry explicitly at the detach
point. The final-cut controls await execution and must retain their exact
results before any claim about those branches.

A source review after initial freeze also corrected a new diagnostic assertion:
`NativeJointOldObservation::prepare_capture` clones its actual retained read-pin
Arc into the capture. Dropping that S2 capture retires its reference, not the
OLD owner's shared underlying gate pin. Requiring the gate pin to disappear
would incorrectly demand early OLD obligation release. The successor keeps the
OLD gate/freeze intact and observes the exact Arc's before/after reference counts
through a test-only Weak. This is a source-derived test correction, not evidence
of a runtime failure or permission to release OLD custody before its real gate.
