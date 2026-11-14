# S2 source retirement: missing cross-process proof producers

This is a source-pinned design at `5a85900f571c389ff97e278a0b9555048bdeadec`, not a runtime implementation or qualification result. It extends the integration gap identified at `2ac270872cbef913f5ca0b8e4c61fffb08b2e38f`. The three-file compile fix after3b changes neither this protocol boundary nor the test bodies. The independently running71-case qualifier is outside this review.

## What actually composes

S2 Start/Describe and subsequent authenticated frames can deliver the exact manifest to the original-A Registered import path. The donor registry retains the actual source before capture and preserves uncertain outcomes. The target host genuinely retains its imported joint owner in `remote_joint_targets`. Those are useful prerequisites, but neither registry access nor a manifest checksum supplies phase authority.

The two retirement methods currently accept local opaque objects: `ActualServiceNativeRegisteredJointNewEvidence` and `NativeJointReleasePhaseQuorum`. Their only current callers are the hot fixture. That fixture obtains the donor's opaque certificate and all OLD recipient/runtime owners in the same process. The following boundaries therefore remain missing:

| Required producer | Existing local mechanism | Missing cross-process operation |
| --- | --- | --- |
| Registered NEW | `registered_target.rs::observe` checks actual target native base/final image/freeze, Role/OWN/current MStore, capture pin and the donor-owned OLD certificate | An authenticated actual target NEW producer and recipient validator; the certificate cannot be recreated at the target from MF DATA |
| OLD/survivor NEW and decision ownership | `begin_remote_actual` requires a map of actual local OLD-observation Arcs; decision owners operate on those objects | Recipient-owned OLD/NEW/decision operations and authenticated historical observations, without constructing a local quorum from replies |
| Durable decision, prepare, native bridge and catalogue activation | Existing local phase producers retain original native attempts, receipts and actual owners | Wire consumers that invoke each actual recipient producer and retain the exact attempt before native I/O |
| All-member activated/Open continuity | Release continuation stores the service/runtime/image owners of every final member; `acquire_actual_member` uses its client for MQ, then directly locks that member's service | Authenticated observation from every actual final recipient plus a distinct remote complete-phase validator |
| Source retirement | Existing primitives drop capture or obsolete certificate references after their typed local proofs | A recipient-owned source operation bound to the exact S2 discovery/session and the genuine remote phase producers above |

`ClusterDataRequest` currently provides OLD, source-discovery and source-frame operations, but no joint NEW/decision/activation/Open operations. Registered joint-owner lookup is a host-local prerequisite, not an authenticated phase endpoint. Broadening an old listener merely because a mutable field or Weak now exists would violate its immutable ingress mode.

## Retirement and Serving are different boundaries

The existing first release phase verifies all final members' committed native bridge, catalogue activation, original writer and Open continuity. It permits the actual Frozen owners to remain held. Source OLD references and survivor OLD observations must drain at that point **before** `release_actual_local` can drop the original Frozen owners. A later complete released/armed-Open phase authorizes FIRST writer-policy publication and Joined Serving. Requiring already-Serving evidence for the earlier retirement would create a dependency cycle.

Retiring the export read pin preserves OLD closure/fencing obligations. Retiring the source session's obsolete OLD-certificate references does not erase the retired node's necessary native fence or delete its physical replica. The current registry still retains discovery/session entries after these transitions; cap8 and request-history limits remain real blockers to an unbounded64-range campaign. No TTL eviction, automatic reset or physical cleanup is implied.

## Smallest genuine protocol slice

The first independently meaningful slice is **actual target NEW observation over native-pinned mTLS, consumed at the donor to retire only its exact export pin**. It needs a real target producer, a real transport wrapper and the donor consumer together. A getter or a source-only `Retire` endpoint is insufficient. This slice would still stop before global decision/Open/Serving.

The actual target producer must consume the original retained Registered owner/A and protected final/base/catalogue facts under fresh local Role/OWN and current MStore checks. It must attest the exact MF checksum/source session, target A and native birth, native committed receipt, source snapshot B, range R/H0/H, image root, schema/configuration, namespace and protected original open/Store identity. Bare `NativeJointMaterializedFacts` lacks some session/A correlation and is insufficient by itself. UNKNOWN native outcomes, incomplete images and replacement owners must refuse.

The donor must independently fetch that response through the pinned actual target endpoint. Only the actual exchange may create a distinct authenticated remote-NEW observation. The donor then checks it against its **own retained actual OLD certificate and original S2 source**, reacquires fresh recipient OWN/MStore authority after network work, and performs the exact pin transition. A fact that matches an MF does not become local `ActualService...NewEvidence`; a separate sealed recipient proof is required. If factoring the actual target validator for this producer, preserve the existing certificate-backed local validator rather than weakening its constructor.

The complete second slice requires recipient-owned joint-phase operations through durable decision, all final activations and Open continuity, then the source OLD-reference transition. It must also retire each actual survivor's obsolete local OLD observation. The later release/armed-Open and FIRST policy operations remain necessary before Serving. This is a distributed producer/consumer chain; the existing same-process map of owners cannot be transported or replaced with decoded DTOs.

Suggested wire operation names in this design are illustrative, not current APIs: `ObserveRegisteredNew`, `ObserveJointActivatedOpen` and `RetireDiscoveredSource`. Decision/prepare/activation operations must first invoke their actual local protected producers. No proposed request/response is a grant merely because it has these names.

## Binding and failure contract

Every phase binds cluster/bootstrap, plan and relocation IDs, old/new configuration digests, range/epoch, handoff session/nonce, donor/target native births, target A, source session, MF checksum and OLD-certificate digest. Decision/Open phases additionally bind the exact common durable decision and member-native/catalogue activation receipts. Membership is complete and distinct; unavailable or unknown members do not count as zero/absence or an implicit quorum.

The requester is the authenticated original target for the first slice; donor, metadata leader and retired replica may be different nodes. Each recipient checks actual TLS/native identity and its own original DB/Store slot, metadata-store identity and current selected phase. Network/MQ work occurs outside service/DB/native/session locks. Foreign observations are gathered first; fresh recipient OWN/current MStore checks authorize the sink afterwards. Unrelated metadata advancement is permitted only through real current-point checks, never by relabeling old observations.

A post-promotion retirement cannot reuse the discovery admission's Pending-only check. It needs an actual committed-decision/Open-continuity admission for the original owner. Likewise, a terminal retry after legitimate writer-policy rotation must validate the retained historical transition and current native continuity, not resurrect the old writer or expired Role.

Retain exact retirement intent/proof and original custody **before** the irreversible cancellation/drop, and record the actual completed transition before later reply/cancellation checks. Lost replies use fresh observation/Describe with the same binding; they do not create a new capture, source session, A or row transaction. Conflicting challenge bytes, changed birth/MF/plan, unavailable proof, ambiguous native outcome, original-process cancellation or changed Store refuse while retaining necessary custody. No old expired token is renewed into authority. After process death, an unimplemented cold continuation must remain explicitly unavailable; DTO reconstruction is not recovery.

Authenticated remote facts provide bounded historical observations, not continuous ownership of foreign locks. Original recipient owners and their protected records must enforce the necessary closed/materialized or activated obligations after the reply. No phase wrapper may be cast into `SourceLineage`, a local OLD quorum, Frozen, Ready or Serving.

## Acceptance cases to add before claiming Source Start to Serving

| Case | Required observation |
| --- | --- |
| Normal full path | Actual OLD Close RPCs; TCP Start/Describe; exact source fact and A; complete import; genuine remote NEW; export-pin retirement; recipient-owned decision/activation; complete activated/Open proof; source and survivor obsolete-OLD retirement; actual release; complete released/armed-Open; FIRST/Joined; original fixed-gateway SDK APP and full-value Strong |
| Lost actual Start reply | Suppress the completed real Start response; retain the first error/uncertain outcome; fresh Describe returns the same MF/session/A; finish the identical full path without local publish or a private source handle |
| Lost NEW/retirement/decision reply | Preserve original protected/native attempt and source transition; fresh observation reconciles exact identity rather than publishing a replacement |
| Stale/wrong proof | Reject wrong actor, donor/target birth, MF, A, session, decision, epoch or replaced Store/MStore; preserve source pins and OLD obligations |
| Missing or unknown final member | No complete activated/Open or released/armed-Open claim; no unsafe OLD retirement/Serving |
| Concurrent frame/cancellation | Active source callback prevents retirement or completes before the exact drop; post-drop error retains completed retirement; no resumed export from canceled original session |
| Post-Serving populated second range | Preserve first-range acknowledged records, native state and routing while using genuine new authority for the next movement; do not hide cap exhaustion with eviction |

The APP cases retain the original two-second SDK budget, gateway and assertions, with no manual route refresh or application shard hint. If a separate SDK defect prevents that APP, preserve the end-to-end failure and classify its actual journal; do not turn an import-only pass into a Serving success. Joined-as-a-future-donor admission, safe terminal registry pruning, cold reconstruction, minority/unavailable OLD support, automatic controller placement and physical throughput/HA qualification remain separate open producers/gates.

No runtime, test, qualification dispatch or new capability was implemented by this review. The source-pinned JSON records the existing boundaries and proposed acceptance work; it is not a release capability manifest.
