# Authenticated native donor Start/Describe discovery

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
