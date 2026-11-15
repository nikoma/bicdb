# FIRST remote catalogue confirmation and metadata certification

This source batch is based on `f201b937e28bcc6c75364e99a793d3bfe593d265`.
It adds actual recipient catalogue publication, fresh readback of a privately
known committed native decision, a distinct authenticated dual-durability
collection, and a finite operation on the actual metadata leader. The new Rust
source and eight new normal-host control definitions have not been compiled or
executed. A source review or a passing earlier revision does not qualify them.

## Original native outcome and fresh readback

The original recipient still owns one native attempt, its issued XID, canonical
record and provenance bytes, and its actual outcome. `KnownRead` is a distinct
private admission borrowing that same attempt and independently acquired actual
Role, OWN and selected-MStore point. The native mutation entry refuses this
mode. A fresh helper takes the genuine Registered exclusion or original OLD
writer scope and verifies protected CURRENT, archive, provenance, native status
and exact original candidate against private `Committed`, which is assigned
only after the actual paged commit returns successfully. It does not persist a
format flag, acquire Controlled, begin a transaction, or replay native writes.
Its successful receipt is retained before later metadata and response checks.

This covers the source boundary where commit succeeded but the old helper's
later check failed. The original failed call and native outcome are preserved.
A private Unknown, failed fsync, aborted or unframed attempt cannot use this
producer; visible committed-looking bytes or a decoded receipt do not suffice.
The existing Unknown control remains unchanged. The new late-commit control
uses the existing actual post-commit cut 2 and compares the complete retained
native attempt before and after the fresh read, including XID and provenance.

## Catalogue ownership and uncertainty

`NativeRemoteDecisionCataloguePublication` is retained on the original recipient
before changing catalogue memory or entering IO. It owns the original and exact
candidate full catalogues, encoded candidate bytes, actual Store/open/catalogue
and participant identities, original native receipt and decision hashes, and
actual IO outcomes. It changes only the existing selected joint-decision fields
and supported existing format-14 representation. The original native remote
reader feature is already durably required before native decision publication.
No new durable record semantics or reader permission is introduced here.

Only the sealed original-candidate path may cross generic catalogue uncertainty;
generic currentness stays strict. Actual IO outcomes are recorded before any
postcheck. A failed directory sync cannot be promoted by readonly observation,
even if the renamed bytes are visible. The same retained candidate may perform
another real complete durable replacement and readback under fresh authority.
A verified readonly outcome additionally requires generic durability healthy,
current physical identity and matching selected decision in the actual latest
full catalogue. Healthy unrelated-range catalogue advancement is allowed for
that readonly path; an old whole-catalogue snapshot is never rewritten over it.

The private pending-write owner also binds the Store-local monotonic catalogue
write generation. This branch carries the same field, initializer, two methods
and generic persistence mark as policy source
`5fdb895ccc5e8bc8130196be44a90e7a89c4a32f`, plus the mark at its new actual
remote catalogue IO entry. **Composition with that source's marks at every
existing direct catalogue writer is required before qualification.** Generation
alone never owns uncertainty. An older verified owner cannot clear a later flag,
and a pending owner refuses after any intervening entered catalogue IO, even if
bytes are identical. An early refusal that performs no IO does not increment it.

The existing protected custody reader's 64 MiB per-head contract bounds each
full catalogue representation here. The separate 8 MiB wire-fact collection
bound is not a new aggregate cap on original plus candidate plus envelope.
Retaining full heads can consume several times one head's size; this is not a
memory-capacity or stalled-disk independence qualification. Physical readback
and durable replacement still perform local IO under actual service/journal
ownership. Kernel IO and fsync have no new preemption guarantee.

## Authenticated dual durability and real metadata ownership

`PublishCatalogue` and `ObserveCatalogue` are explicit new wire operations.
Replies are DATA. Only an actual existing pinned mTLS transport exchange may
construct `AuthenticatedNativeRemoteCatalogueFrame`; it retains the original
request token, exact request correlation and compact validated physical summary.
The repeated full manifest and native body are dropped only after full original
reply validation. The collection retains the manifest once, rejects duplicate
recipients, mixed expected D, different observers and expired original frames,
and counts distinct OLD majority, NEW majority and the actual activation leader.
The resulting `NativeRemoteDualDurableDecision` is a new opaque type. There is
no conversion to local `NativeJointDurableDecision`, local NEW, Role or a quorum
constructed from decoded DATA.

A finite operation on the actual `NativeMetadataControlHost` metadata leader
independently fetches these recipient outcomes using its actual transport.
Coordinator input is only immutable intent. Existing authenticated actor rules
already permit this actual metadata leader for the committed-handoff request;
no actor privilege is widened. The producer uses a fresh actual HostMQ and
exact ephemeral proposal view, retains the actual opaque collection and exact
candidate before proposal, and retains actual proposal identity and result.
It invokes the existing actual conditional proposal, heartbeat replication,
ACK/commit machinery and fresh HostMQ observation of the exact committed
candidate. An ambiguous proposal does not allocate a replacement; a readonly
fresh committed result can resolve it, while unresolved pending identity stays
owned. No engine or service lock spans outbound network replication.

This operation reaches the existing `Certified` metadata stage and leaves the
selected writers frozen. Later union/final epochs need distinct stage-bound
fresh producers for recipient Prepare/Open, ALL release and Serving. Existing
old Pending/capture checks, local durable-proof constructors and ordinary APP
or Strong authority are unchanged. The local metadata version guard remains a
local fence composed with real quorum Role/OWN and native fences; it is not a
distributed lease. This finite host method is not an automatic controller,
periodic reconciliation loop or new generic remote topology-proposal endpoint.
Old peers may refuse the new operation variants; rolling upgrade is not tested.

## New definitions and remaining acceptance gaps

All fourteen pre-existing test wrappers are byte-identical to f201. The new
normal-host definitions retain the same stock SDK two-second calls, original
Role/OWN windows, actual owner/process/transport participants and original
application assertions. They define:

- Actual recipient catalogue confirmations followed by actual metadata-owner
  independent collection and committed Certified observation.
- Actual successful catalogue response loss, then readonly same-candidate
  resolution without another native XID or catalogue IO.
- Actual directory-sync failure, readonly refusal, wrong unresolved recipient
  refusal, and same-candidate durable retry under a fresh real request.
- Actual intervening byte-identical generic catalogue IO, then refusal by the
  stale pending owner without clearing its uncertainty. This is an IO-generation
  component, not an unrelated populated-range movement test.
- Duplicate recipient rejection after two actual authenticated readonly replies.
- An actual frame rejected against a contradictory expected decision hash. This
  is not evidence of two independently produced conflicting native decisions.
- Natural expiration of an original authenticated frame, rejected by a distinct
  later real collection without renewing or rewriting the old token.
- Actual private native commit followed by cut-2 failure and fresh protected
  KnownRead of the identical original attempt before catalogue certification.

Definitions are not executed proof. Full cross-owner Store/slot ABA during this
new catalogue operation, two independently produced mixed-D corpora, a real
unrelated-range catalogue head change during uncertain publication, and a
successful large-head campaign remain untested here. The wrong-recipient check
is original coordinator custody refusal, not a fabricated Store-ABA substitute.
Existing strict physical owner/open checks remain mandatory at every sink.

RAM attempts and exact IO history do not provide cold recovery after process
loss. Persistent reconstruction, later-stage epoch producers, automatic
controller handoff, full repeated movement, Joined future-donor duties,
production deployment and physical RPO/capacity qualification remain separate
open gates. No new VM, Azure resource, build, release, deployment or CI run is
part of this source batch.
