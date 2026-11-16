# Metadata-owner readonly catalogue scope

This isolated source component is based on `e5a662be7f61cabc933bfb0bb65da5c5ba790aef`
(inherited candidate 520). It has not been compiled or executed. It changes no
source version, wire variant, workflow, deadline or quorum requirement.

The previous certification caller ran on the real metadata leader but requested
`MetadataReadObservation`, which correctly requires the requester to be the
selected range leader. The normal FIRST fixture deliberately chooses different
metadata and range leaders and a donor different from both. Creating an empty
`NativeJointOldRound` merely to obtain its plan therefore failed that strict
requester check. The original range-leader acquisition and native Role/plan
constructors remain unchanged.

`NativeMetadataOwnerCatalogueScope` is a distinct readonly comparison object.
Its actual host factory uses fresh self HostOWN, the original service database
open and attached Store slot, and a protected actual MStore point. While the
real backend MStore is held, it validates the SAME protected member/store/open,
current metadata leadership, context, committed topology and restore state.
The added held-MStore comparison uses the captured member directly and acquires
no recursive MStore lock. The scope retains its original HostOWN, open, point
and cancellation/deadline ancestry; retaining it never renews the issuance.

The new pure configuration path uses the existing pending-handoff calculation
and existing complete native scope validation. Manifest comparison retains all
strict capture/activation/predecessor/configuration, enrollment birth, member
pin/incarnation, schema/restore and exact source OLD record/physical-prefix
checks. The readonly export-binding and OLD-record overloads use the identical
pure predicates as the untouched plan validators. The new type has no plan,
Role, local OLD/NEW quorum, Frozen, writer or activation conversion.

A separate catalogue-round entry accepts this scope. Its private backing is
limited to comparison/liveness and immutable decision-summary getters. The old
range-plan entry remains strict. All counted witnesses still come from actual
pinned recipient native-plus-catalogue exchanges: same original MF/phase/fact
set/D, distinct observer/member identity, live original requests, OLD and NEW
majorities including the activation leader, and existing member/byte bounds.
An intent or metadata record cannot become such a witness.

The existing 8 MiB collector accounting covers encoded manifest/frame witnesses;
it is not a resident-heap limit. This scope also retains its actual HostOWN and
protected point (including their committed metadata projections), subject to
the existing metadata/transport limits and the host's fixed eight-proof-context
bound. No new metadata size allowance or fixed resident-memory claim is made.

After collection the existing ephemeral topology builder and actual proposal
path still require a new self HostOWN, a new actual MStore point, SAME original
owner/open/store, and current protected metadata leadership. The actual typed
proposal return remains retained before diagnostic allocations and postchecks.
Unknown proposal/persistence outcomes, whole-candidate postcommit comparison,
and existing pending-entry reconciliation remain strict; this component does
not solve their separate unrelated-head/recovery limits. The original protected
publication version is retained across collection: a real unrelated metadata
LOAD/version advance fails this scope closed. This does not establish bounded
forward progress under ongoing metadata work, heartbeats or global-topology
drift. There is no automatic controller/convergence or Serving claim. No native request,
receipt, failed fsync or old response is relabelled successful.

## Actual control definitions

One new case extends the existing five retained normal native-host OBJECTS in
one Rust test process, with real databases and mutual TLS over actual TCP.
The original range leader, separate metadata leader and independent donor
selection are unchanged. All old FIRST test functions/assertions/budgets remain.

Before the ordinary certification call the new case requires:

* The metadata owner still receives the exact generic range-READ authority
  refusal, while the actual range leader's original plan and the new readonly
  scope have the same immutable comparison checksum.
* A real other host's protected point/open is refused; a dedicated original
  scope request's cancellation is refused. Natural coupled original-scope
  expiry runs LAST: the actual HostOWN and outer windows may expire together.
  Both actual clocks/refusal results are recorded, and the original scope must
  return `QueryTimedOut`. OWN-only expiry is not tested. No later scope,
  request, proposal or effect uses that spent token. Returned test diagnostic
  DATA is distinct from a completed production action or an expired grant.
  No shorter test TTL or replacement original deadline is introduced.
* A changed, rechecksummed MF nonce is refused against its exact original
  source OLD record.
* A genuine unrelated-range load proposal, through the actual metadata owner
  and MStore with the original scope checked under the same latch immediately
  before that mutation, invalidates the old scope. A fresh proposal check must
  report the actual version refusal while the original request is still live.
  This is load metadata, not another relocation or a fabricated history.

Actual returns/cut evidence are emitted before assertions. Target native and
catalogue snapshots must remain unchanged through these readonly/refusal
controls; the intentional metadata load proposal has its own real effects.
The existing independent recipient collection and normal certification then
remain required. Missing setup or cuts fail rather than skip. No positive
runtime result is claimed before this exact source executes in normal CI.

| Evidence scope | State |
| --- | --- |
| Old metadata-owner/range-only API compatibility | Failed, source-confirmed; captured original authority refusals preserved |
| New source predicate/owner inspection and static preservation checks | Passed within stated source-only scope |
| New controls and unchanged full normal FIRST acceptance | Not tested |
| Automatic relocation, full Serving, cold recovery and physical HA | Not tested by this component |

The earlier read-only design receipt is
`/tmp/bicdb-native-foundations-ci/metadata-owner-catalogue-scope-326-design-contract.json`,
SHA256 `a4fcbf42981537144fb991ed4da67869eb54217b5934d1388be9674a18e963ce`.
