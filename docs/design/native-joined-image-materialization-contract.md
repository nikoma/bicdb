# Existing Joined: concrete image materialization boundary

Source reviewed: `818a8f4cd125505bbb2981d7b55608eb5a663a7e` and its inherited
corrected transfer branch `d47d4edf2ace1e6b7dda4c36978525c34584eefa`.
This contract was written before the history/row successor. That successor now
adds actual source paths for Joined history and row apply, with private original
A scope and transaction grant. It remains uncompiled/unexecuted at freeze;
base/final/catalogue materialization is still absent. The a78 composition's
compile/fence run does not execute the new history/row operations.

## Existing producers that must remain distinct

`joined_data/receive/attempt.rs` already retains the exact preflight and actual
A in the original Joined host before later native work. Its proposal v2 and
catalogue-18 Installing result bind the original writer, selected preimage,
actual source Manifest/attempt/session, native/open/Store and fresh requests.
They do not import rows or admit ordinary writes for the selected range.

`registered_remote_image.rs::NativeRegisteredRemoteImageContext` and
`writer_enrollment/registered_remote_mutation.rs` require the Registered
never-writer hook and FIRST ancestry. They cannot be constructed for Joined.
The new context must remain `NativeJoinedRemoteImageContext`, borrowing the
actual Joined attempt scope and Installing fence. It may share pure wire/hash
helpers with Registered, but cannot construct or alias a Registered context,
NoDomain proof, local source lineage, OLD quorum, or ordinary writer grant.

The new context rechecks actual original State/hook, source frame, fresh
Role/OWN/member/current local version, selected A, proposal CURRENT/archive/xid,
and the actual selected Installing fence. Each operation obtains its own
finite request. Local version remains a local fence, not a metadata lease.
After a native base write, it may recognize only that exact owned base boundary;
it must not generally waive the original native lower-bound obligations.

## History and actual pending rows

The host must retain `NativeJoinedRemoteImageInstallation` before allocating a
new history generation or issuing native writes. History frames come from the
actual authenticated source and are bound to the existing Manifest and A.
The source's R/H0/H, chain, cursor and complete history audit must all agree.
A new Joined history header must carry the Joined proposal pointer directly;
casting it to NativeRegisteredRemoteProposalPointer would lose provenance.
A distinct header/version/namespace can share bounded wire validation helpers
while preserving the existing Registered header's bytes and validators.

A pending row step retains the exact source step, original logical Transaction,
mutation hash, original native intent xid and next protected progress before
any relevant WAL. Every native transaction records its issued xid at closure
entry before cancellation, framing or readback can fail, including history,
intent, progress and final publication. An incomplete begin cannot become an
unrecorded fresh transaction on the next call. Proven-abort retry, if supplied,
requires a separate actual healthy-storage/abort proof and preserves attempts.

`registered_remote_image/rows.rs::RowPending` supplies the retention pattern:
its own actual NativeWalRetentionPin survives transaction consumption until
committed protected progress. Joined must retain that same kind of actual pin
before native intent publication and share it with the original Transaction.
It cannot release it merely because logical apply returned or pending writes
became empty.

The restrictive pending-row index is DATA, not a Registered authority grant.
Its present fields bind database-open/native birth, Manifest/A/range/history,
step, logical TxID, exact mutations hash and native intent xid. Reusing this
strict DATA schema is acceptable only with exact Joined context/provenance
checks and the new reader fence. A different namespace/profile must update its
key resolver explicitly; writing an unindexed Joined intent is forbidden.
Insert debt in the SAME native transaction as the intent, and delete it in the
SAME native transaction as committed row progress. Unknown/malformed debt is
not empty, including at startup and logical-WAL cut paths.

`NativeJoinedRemoteRowTransactionGrant` must have a private constructor in the
actual Joined context. It binds the lexical weak scope lifetime, original
request and source callback, actual native proposal/Installing/history/intent,
range claim, original logical TxID and exact mutations hash. No boolean or
imported_projection flag constructs this grant. The commit paths in
`db.rs::Transaction`, `spatial_paged.rs::commit_transaction_inner`, and
`certified_prefix.rs::require_native_transaction_mutation_scope` must validate
it independently, retain WalDurablePendingApply immediately at actual logical
WAL durability, and retain AppliedKnown at actual visibility before finalizers.
A WalDurablePendingApply error is uncertainty requiring the original attempt;
it is not rollback or permission to create another Transaction.

Every imported write must hash into the selected range using the ordinary
namespace/key routing function. It maps into existing target namespaces and
uses normal record/index maintenance; it does not replace the database or
clear unrelated records/indexes. Existing global uniqueness or schema conflicts
must produce explicit errors, not be suppressed to make copying pass.

`spatial_paged.rs` already suppresses new sync/audit side effects when a genuine
import transaction has imported_projection, while maintaining actual records
and indexes. Joined needs the same narrowly granted import behavior, memory-job
suppression, and retained source audit data where the real image contract
includes it. This does not establish transparent migration of queues, broker
receipts, internal audit stores or FTS parity. Those need their actual data
contracts and independent reconciliation.

## Final image, physical base and selected catalogue

Independent bounded target scanning must reconcile full selected payloads and
count/byte/content hashes against the actual source Finished seal. It records
an actual positive target database watermark after row application, with source
and target B domains separate. Unrelated business B may advance. The selected
native image generation, schema/namespace lineage and index definitions must
remain valid. If existing unrelated writes invalidate the selected proof,
retain the failure; do not relax the proof or call one-range evidence enough.
Definition equality alone is not independent index/FTS contents parity.

The resulting private `NativeJoinedRemoteFinalImageReceipt` binds actual
history and committed row headers, source seal, Joined proposal, target scan,
namespace/index definitions and original State/open/birth/A. Publication retains
every original native xid/outcome before puts and proves CURRENT/archive and
committed status. It remains physical evidence without a read or writer grant.

`range_image_install.rs::NativeInstalledRangeImageReceipt` currently recognizes
only local profile 1 and Registered profile 2. A Joined base must have a distinct
profile with a Joined final-image pointer. Old profiles remain strict; no fake
Registered pointer or local-source finalization. Its physical reader and
history validator dispatch must explicitly recognize Joined provenance and
refuse unknown/mixed profiles. The existing node-local reader fence must be
durable before those bytes. This profile change is not implemented by the
proposal/Installing source batch.

`JoinedRemoteBaseBoundary` retains the exact original candidate/xid/bytes and
actual publication request. A fresh genuine request may reconcile that same
committed base after request expiry; it cannot renew the old request, substitute
new native bytes, or ignore an unowned prefix/pending/genesis obligation.
Only that actual result yields NativeInstalledRangeImageBase.

`NativeJoinedRemoteCataloguePublication` derives its first catalogue candidate
from current state after validating the selected Installing/base/final/native
obligations. It retains exact bytes before I/O, preserves uncertainty, and never
replays an old whole catalogue over unrelated progress. Once successful,
`NativeJoinedRemoteCatalogueMaterialization` follows the selected actual base,
log, refusal, intent and disk/current-memory equality. It does not supply
ordinary writes or Strong reads. This extends the selected Installing rule to
actual native floor adoption, not an exception to durability requirements.

## Opaque materialized owner consumed by Root's next phase

Only after complete actual history/rows, verified final, same physical base,
and durable selected catalogue may the original host retain a
`NativeJoinedRemoteMaterializedOwner`. Constructor and fields stay private.
The host must retain it infallibly before any lease transfer or later fallible
NEW work. It exposes only actual borrowed evidence:

- exact original writer-State Arc, preflight, service/runtime identity;
- original retained A/freeze and its actual runtime retention check;
- source Manifest, capture/activation descriptors and intended member births;
- protected Joined final/base/catalogue receipts and their checksums;
- require_current under an actual Joined scope and fresh request.

These are exact owner/evidence accessors, not a constructor for permission.
No Installed-domain require_range for the new range is allowed before actual
NEW, dual decision, metadata/Open and ALL release. Root's distinct
NativeJointWriterDomain::Joined and phase owner must consume that genuine
materialized owner, preserving the original dispatcher/hook/coordinator and
per-request Strong barriers. The runtime keeps its existing strong A/preflight
custody; a materialized owner containing a Runtime lease must not be added as a
strong backedge into that same runtime.

## Smallest actual acceptance controls

Run the unchanged original fixed-gateway SDK and ordinary routing/hash function.
Keep original budgets, assertions, and every ACK/uncertain/failed/retried call.
Internal copy transactions and metadata commits are not application ACKs.

1. Complete genuine first-range Serving, write and full-Strong-read R1, then
   receive the genuinely populated second range into that SAME Joined DB.
   Between bounded R2 history/row/scan/progress phases, require ordinary R1 APP
   and full Strong reads through the same gateway; reconcile original IDs and
   payload hashes plus selected range/epoch/current owner evidence.
2. Advance unrelated R1 business/WAL/index state during R2 receive. Verify its
   new ACK and full Strong payload and continue the SAME R2 A/cursor. Separately
   advance actual unrelated metadata configuration, require stale request
   refusal, then use fresh genuine paired Role/OWN/member evidence with the
   unchanged selected plan and original retained attempt.
3. Inject actual selected catalogue rename/directory-sync/readback failure.
   Preserve exact pending candidate/bytes; a changed unrelated catalogue must
   not be overwritten. Same-candidate real re-fsync can continue only after
   actual proof. Native proposal xid and source/A must remain unchanged.
4. Cancel the real request after native begin but before framing, and separately
   fail abort cleanup. Preserve the issued native xid and exact outcome/health;
   a fresh call cannot silently allocate another xid. Repeat at row-intent,
   logical-WAL durability and protected progress boundaries as those producers
   become available.
5. SIGKILL an actual child after row AppliedKnown but before protected progress;
   preserve stopped custody and exact WAL/index. Fresh open must refuse before
   generic replay/outbox/cut if unresolved debt remains. This is preservation,
   not cold self-healing, until a genuine original-outcome recovery producer is
   implemented and independently tested.

Actual stalled selected disk I/O, second Joined activation/replacement,
Joined becoming a future donor, cold self-healing, and multi-host automatic
scale-out/throughput remain separate acceptance gates. A source-defined test,
passing primitive control, or complete physical copy cannot close them.


## Actual history/row source successor

The separate Joined module now supplies BeginHistory, AppendHistory,
AuditHistory and ApplyRows through the original host's retained attempt.
Installing/proposal/current original A and fresh source/request checks precede
its distinct private context. FIRST context and grant files remain unchanged.
The Joined native scope permits only its actual controlled claim while keeping
real admission/hook/journal guards; its weak transaction lifetime dies before
those guards drop. No ordinary selected-range writer policy is installed.

The exact history generation and pending native header/entries are retained
before native begin. Issued native xids are recorded before fallible framing.
Rows retain the actual logical Transaction immediately after allocation and
before staging writes; incomplete staging stays owned, not silently reset.
The same actual WAL pin spans intent, logical apply and protected progress.
Intent/index insert and header/index removal share their native transactions.
A shared restrictive v1 index DATA key is used deliberately; no Registered
scope, proposal pointer or row grant is constructed from it. A distinct Joined
image feature and the existing index reader fence precede these writes.

The actual logical commit body now has separate Joined grant validation and
WalDurablePendingApply/AppliedKnown custody callbacks. Mixed import provenance
is refused. It suppresses recursive admission acquisition only after that
actual Joined grant validates, and does not globally skip require_range.
Native/logical import commits remain separate from external APP ACKs.

This batch has no verified target scan, final image, profile-3 base, selected
catalogue adoption, NEW/decision/Open/release or new DATA-range permission.
There is no runtime pass, second-range progress, cold repair or physical claim.
The next fixture must use these genuine host methods and original SDK, not a
constructed context or test-only Ready grant.

The current source image is copied under the real selected OLD/A exclusion.
It does not add online source writes/catch-up during the bulk copy. The moved
range can remain unavailable until the genuine later activation/release;
unrelated R1 availability and bounded selected unavailability still require
measurement. No transparent online elastic scaling follows from this source
copy path alone.


The row commit consumes the original Transaction object. After a failed
consuming call, retained custody is its exact original TxID/mutation hash,
source step/payload, indexed intent, inner logical outcome and shared WAL pin;
it is not the Rust Transaction object. Before invocation the object itself
remains retained. A consumed unknown operation cannot be reissued or replaced
by a new Transaction. AppliedKnown can advance only the same protected row
progress after actual checks. Recovery of consumed uncertainty remains an
explicit separate gate; this wording is not evidence of such a resolver.


A source-review successor retains encoded intent bytes before inserting even
the first pending-index native record, and retains the issued progress xid,
header and exact encoded candidate before clearing that index. An incomplete
framing state cannot allocate another progress transaction. Exact readback
must match the retained bytes/headers before debt and its WAL pin are released.
Optional checkpoint maintenance begins only after successful operation,
postchecks and a still-live parent/source request. It cannot preempt a kernel
I/O already in flight; bounded physical I/O progress remains unqualified.


The next source successor adds the actual Joined bounded target verifier:
BeginVerification/VerifyTarget consume the authenticated Finished frame,
committed rows and audited history under the original Joined scope. It records
actual target B, waits for durability and visibility, scans selected primary
payloads with retained cursors and reconciles full count/byte/content hashes.
It retains selected image generation, namespace/schema and index definitions.
This is source-only and unexecuted. Definition equality is not index/FTS
contents parity; no final/base/catalogue/Serving result is published yet.


## Physical scan lock correction

The initial c7 verifier source held service, global native journal, admission
and hook guards across the physical collection scan. A raw-row count bound did
not bound that I/O or the time other ranges waited for those guards. This was
a confirmed source availability gap, not an executed failure experiment.

Its successor prepares a host-retained selected read point under the genuine
native/history/rows/Installing scope, then drops ALL those global guards before
scanning. Only the actual DB read borrow, original opaque A and pinned
schema/image/index/cursor identity remain. Both actual request and authenticated
source-frame cancellation/deadlines constrain the scan; linking them cannot
extend either original request. Failed read attempts and tentative candidate
DATA stay retained. An expired postcheck does not publish the cursor or renew
the old metadata/source observation.

Publication reacquires current actual native/source/history/rows checks and
matches the original verifier revision. All hashing, I/O and allocation precede
the nonblocking native current-gate plus actual metadata-version/request check.
Only a memory state swap happens there; old state/read-point destructors run
after both short guards drop. The shared Frozen identity is observational DATA,
not a shared permit or ordinary write grant. The generic guarded VerifyTarget
operation refuses, so the actual host must use this unlocked producer.

This corrects that source locking boundary but does not prove independence of
underlying page/cache/index I/O, general PostgreSQL writer locks, or all fixed
native pre/postchecks. A genuine selected-row I/O-stall test with unchanged
R1 SDK calls is still required. The earlier transport-only hold is insufficient.

Also, auto_checkpoint_if_due returns immediately for ServerPaged. The earlier
success/live-request checkpoint guard is defensive contract hardening; it did
not fix a confirmed live checkpoint-I/O defect in this Joined deployment mode
and establishes no measured recovery benefit.


## Distinct Joined final publication source

A subsequent source-only batch adds PublishFinal and a distinct
NativeJoinedRemoteFinalImagePointer/Receipt. It consumes the original host's
actual complete audited history, archived committed row header, authenticated
Finished seal and separately published independent target verifier. The protected
v3 record additionally binds the original writer-policy hash, exact Joined
proposal/Installing intent, original DB/open/Store/birth/A, namespaces, target
watermarks/image generations/index definitions and metadata request lower bounds.
CURRENT and immutable archive share one actual native transaction.

The owner records the actual issued native xid as the first closure operation,
then an unframed native state before any fallible scope/checksum/encoding. It
retains exact candidate bytes before either put and actual commit return before
postchecks. Unframed, aborted and uncertain attempts cannot create a replacement
transaction. Readback checks the same original bytes, committed native xid,
complete history endpoints/row archive and still-current actual context. Final
receipt publication prepares allocations and native checks before the short
selected-current plus metadata-version gates, performs only the in-memory state
swap there, and drops old owners after both gates. This is physical evidence,
not a serialized capability or a new range writer/read grant.

This batch is uncompiled/unexecuted. It deliberately does not add profile-3
native base, selected catalogue adoption, materialized-owner handoff, NEW/dual
decision/Open/ALL release, APP/Strong Serving or cold recovery. No source-defined
publication result substitutes for those actual producers or their acceptance
controls. Registered FIRST final/base files remain byte-identical.
