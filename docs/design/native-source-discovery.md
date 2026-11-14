# Authenticated native donor Start/Describe discovery

This document retains design and source-review findings from the **unmerged**
runtime composition `3b4bef4eecbdf1e770bba2428cd860fda89e7dba`. Including it
in this documentation batch does not include that runtime in main. The test
statuses below describe the recorded source reviews; later qualification
results must be read with their own exact source and evidence.

This component is based on integrated source `981b7eef7447783270a1db4b8c967aa596cc132e`
(main500 plus frozen817). It adds a source-side TCP adapter; it does not complete
the automatic relocation controller or admit target rows, a writer, APP or Strong
reads. Its new tests have not yet run. Existing failed experiments and evidence
remain unchanged.

The previous source-frame client required a complete expected manifest before
requesting `Manifest`. The new `NativeSourceDiscovery` request is separate: it
names the exact committed range/epoch, relocation/plan, handoff session/nonce,
source and target births and stable target attempt. It carries a finite budget,
business identifier and challenge, but no expected manifest or zero digest.
The existing source-frame API remains strict and unchanged.

Only an actual INITIAL DATA listener serves this request. Its recipient obtains
its original admission, service, runtime, writer guard/coordinator and retained
native OLD closure. A nonleader donor does not cast the target's request or its
own metadata read into a privileged `CommittedHandoff` recovery role. A distinct
recipient-local admission combines the actual closure with fresh donor OWN,
protected same-open MStore/member/DB validation and the real authenticated target
identity. This is capture/read-only admission, never permission to Close OLD,
write rows or publish Serving.

The source reuses the production collector which fetches authenticated Observe
frames from every OLD peer and takes final fresh donor OWN after collection.
All original per-frame request checks and command/pending obligations remain.
The collector implementation is shared by the old host API and this new listener
adapter. Source metadata acquisition and network work occur outside service,
engine, registry/table and session locks. A per-attempt atomic claim serializes
work without holding its custody mutex through network operations.

The registry retains an exact per-range candidate before collection/capture.
The actual OLD certificate is consumed into an original-process source session
and retained before fallible publication. Original factory guard/coordinator
references are captured at genuine INITIAL host construction; metadata cannot
reconstruct them. The registry and the source session retain this exact ownership
on every error. Requests with a changed target attempt, birth, range, plan,
session or nonce cannot replace it. Reuse of a challenge requires the identical
bounded canonical request bytes, including operation, budget and business ID.

`Start` can begin this same retained session when capture has not been attempted.
`Describe` does not create a candidate or begin capture. A failed response after
publication is reconciled against the same session and manifest under new actual
request checks. A captured manifest retained before a postcheck error can be
revalidated; a capture marked attempted without its complete retained manifest
stays unavailable. It is never reset, evicted by timeout or silently replaced.
The discovery table is bounded to eight range owners and 64 exact request
identities per owner. Reaching either cap refuses; retirement remains a separate
proof-driven component.

After capture or read, the recipient validates the actual current MStore and
original local member/DB point outside engine/session guards before returning
its immutable reply. This is not the later local metadata-publication version
gate and makes no atomic current mutation grant. A metadata change during I/O
retains the candidate/session and can cause explicit unavailability; a stale
point is not relabeled as fresh authority. No MStore mutex spans capture I/O or
network work.

The strict client checks the actual TLS/native donor, its own original target
OWN and complete request/plan/birth correlation, then checks the returned
manifest, scope, configurations and checksum. Only that actual exchange creates
`AuthenticatedNativeSourceDiscovery`. `NativeRemoteImageFact::from_actual_discovery`
consumes it while its request is live. It does not fabricate an old Manifest
request/frame or deserialize a source capability. Later History/Rows/Finish
continue through the existing authenticated frame endpoint.

Six new source fixtures exercise actual TCP recipient/session creation, actual
reply suppression after completed publication, wrong authenticated actor before
capture, conflicting bytes under one challenge, actual committed-plan
revocation and cancellation of the genuine bound source-process parent before
capture (without claiming physical process death). They retain genuine INITIAL/Registered owners and preserve native
OLD xid/receipt comparisons and target state. These are shared-host fixtures,
not independent-machine, power-loss or production qualification. Compilation
and runtime validation of the final composed source are still required.

A target-process controller, joint Registered NEW/decision/Open/release bridge,
authenticated joint-phase adapters, source pin retirement/physical cleanup,
restart repair, populated second-range movement and a physical four-to-five-node
qualification remain separate requirements. This adapter does not add ordinary
PostgreSQL cross-range SQL or prove sustained throughput gain.

## Joined lifecycle composition

The discovery adapter is also composed onto base
`64c743225f5cdf0b25151882292c98f8cc08c318`, which includes the protected local
publication version and Joined DATA lifecycle. That integration preserves the
mutable current INITIAL writer snapshot, original database-open guard and exact
retained OLD writer/promise identity. The existing actual INITIAL policy-refresh
completion updates its captured weak guard/coordinator bindings together with
that writer snapshot; this adapter performs no policy re-enrollment. A retained
OLD observation from a replaced writer still refuses.

`for_joined_owner` leaves all INITIAL source guard/coordinator, registry/OLD,
metadata/provider/transport bindings empty. Source-specific listener arms now
require actual original INITIAL source hosting before dispatch. Sharing the
Append/Get listener never makes a Joined node an INITIAL donor. The current
source-only admission still obtains independent actual MStore checks outside
engine locks before and after capture; protected member observations also retain
the actual local version point. No metadata point is reconstructed from the
remote manifest, and this does not mint target or current publication authority.
The six discovery tests are preserved; they remain unexecuted in this port.

## Source Start to Serving remains blocked

A test-only composition review at exact source
`3b4bef4eecbdf1e770bba2428cd860fda89e7dba` found a missing production
continuation. The [source-pinned report](native-source-discovery-serving-integration-gap-3b4.json)
retains ten exact source/blob hashes. No new tests, runtime changes or runtime
result followed from that review; the frozen existing CI cohort keeps its
original source, selection and evidence scope.

Actual TCP `Start`/fresh `Describe` can supply the strict authenticated manifest
and `NativeRemoteImageFact` to the original-A target image/import path. An
independent actual donor collector can supply a historical OLD certificate;
manifest DATA cannot substitute for that certificate. The hot fixture still
obtains a local opaque source handle from `remote_old_seed`, calls
`retained.publish`, and later uses that handle for two necessary retirements:
`retire_export_after_actual_registered_new` after genuine target NEW, and
`retire_old_export_custody_after_joint_open` after genuine ALL-Open evidence.

S2 retains its source inside the original discovery registry. No existing host
or transport operation drives those two retirements for that same retained
source. `Start`/`Describe` and the source-frame operations do not provide this
consumer. Dropping the client does not release registry-owned capture pins or
OLD ownership. A new test-only getter, a manufactured local source handle or a
manifest/diagnostic DTO cast would conceal this gap.

The smallest next production slice is an exact recipient-owned source
continuation that drives the existing typed retirement primitives from genuine
NEW and ALL-Open evidence while preserving the same plan, births, A, manifest
and session on failure. A crate-local host consumer is distinct from a real
wire-phase consumer: passing an in-process actual evidence object does not
establish authenticated cross-process phase delivery or controller automation.

The two smallest acceptance cases, **not added or executed**, are:

| Case | Required complete path |
| --- | --- |
| Normal discovery to Joined | Actual OLD Close RPCs, actual TCP Start/Describe, strict fact, original-A image/import/joint/FIRST, both genuine source retirement boundaries, then original fixed-gateway SDK APP and exact full-value Strong. |
| Lost actual Start reply | Suppress the real completed Start reply; preserve its error and uncertain capture outcome; use fresh Describe to recover the same manifest/session/A; complete the same retirement and Joined path, then original SDK APP and full-value Strong. |

Both must preserve the existing assertions, two-second SDK budget and gateway,
without manual hint refresh, local publish or renewed old authority. Until that
producer and these cases exist, separate discovery and hot Joined fixtures do
not prove end-to-end Source Start to Serving. This is an integration blocker,
not a relabeling of the frozen CI run or a new production-qualification claim.
