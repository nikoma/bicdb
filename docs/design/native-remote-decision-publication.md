# Genuine remote native DECISION publication: component boundary

This source component is based on `a78c167246d5985e64164fbe3ef6dcfc243a9a87`
(source version 1.0.505-beta). Its four added runtime controls have **not been
compiled or executed** when this note was written. Formatting, version,
licensing and dependency-direction checks are separate source checks.

The earlier 4e qualification remains a failed compilation with zero runtime
cases executed. The ad8 diagnostic correction and this component do not turn
that failed run into a passing result.

## Original donor evidence, fetched independently

`NativeSourceDiscoveryOperationV1::DecisionProvenance` is a read of the actual
original S2 donor after its export capture was retired. It cannot start another
capture. The original discovery attempt/session, actual process, listener,
open/slot, writer and retained remote OLD certificate remain required. Fresh
recipient OWN/current MStore admission bounds the reply. The canceled exporter
session is historical retirement state; it is never renewed as a live grant.

The donor builds `NativeRemoteDecisionSourceData` from that actual certificate:
all retained irreversible constraints (including unresolved command hashes and
native pending bounds), exact original native close records, and the exact
MF/source-session/A binding. It compares the certificate, selected close record
and full OLD facts to the original manifest. This first lane retains the
existing **ALL-OLD certificate** requirement; it does not implement minority,
unavailable-node or constraint-only OLD continuation.

Only a real native-pinned mTLS exchange constructs
`AuthenticatedNativeRemoteDecisionSource`. The original Registered recipient
independently fetches that donor evidence and the exact selected NEW members'
actual snapshots and completed pin-handoff outcomes. All callbacks precede
fresh recipient Role/OWN/current MStore acquisition and engine locks. A donor
that differs from the metadata leader never obtains a fabricated handoff Role.

## Distinct original participant admission

`NativeRemoteDecisionAdmission` has a private producer in the original retained
remote NEW participant. It is not a conversion to `NativeJointPreparedDecision`,
a local OLD/NEW quorum or a local decision publication admission.

Publication requires the exact prior immutable fact-set intent and completed
original target export/materialization pin handoff. Every selected NEW must
report the same actual handoff intent and retained freeze. The original target
A, native base/final/catalogue, hook/journal, process and exact service/Store
continue to supply physical exclusion. Separately fresh Role/OWN/current-MStore
checks supply current authority. The participant retains the exact expected
body and every bounded authenticated callback/request context before native IO.
It admits at most eight such contexts and 8 MiB of encoded callback evidence;
exhaustion refuses without eviction or changing the attempt.

The decision body is historical DATA built from genuine remote observations.
Its full irreversible constraints are preserved rather than replaced by an
empty collection. The private remote admission, original exclusion scope and
actual current metadata point are independently required at the sink.

## Protected native body and mandatory provenance

The remote sink writes four records in **one real native transaction**:

- Existing complete native decision CURRENT and immutable archive.
- Distinct remote provenance CURRENT and immutable archive.

The common record carries an explicit optional remote-provenance pointer.
Legacy `None` is omitted from serialization, preserving its historical bytes.
Remote `Some` is a digest over the exact protected correlation, decision and
actual native xid. The provenance contains the original MF/source-session/A,
phase/fact-set, target birth/DB/Store/MStore snapshot, complete donor proof and
common-body checksum. There is no cyclic checksum dependency.

The actual native xid is retained immediately after begin, before framing or
fallible checks. Exact body/provenance records and bytes are retained before
any put. Actual commit return is recorded before every readback, metadata,
cancellation or reply check. Reconciliation reads both CURRENT/archive pairs,
checks exact retained bytes and actual committed native xid, and preserves the
same original attempt. It does not allocate another xid after an abort,
unknown outcome, incomplete framing or failed postcheck.

`native_remote_joint_decision_v1` is durably published before the first native
write and re-fsynced on continuations. Older readers must refuse this flag.
The **current** common receipt reader also validates the remote provenance
pointer, matching CURRENT/archive, complete obligations and committed native
xid; it cannot silently treat a remote body as an unchecked legacy receipt.
A remote sidecar with an erased common pointer refuses as inconsistent.

A real commit-sync failure retains `CommitOutcomeUnknown`, poisons/fences the
appropriate storage path, and cannot be converted into a fresh native attempt.
The current abort policy is intentionally restrictive: even an observed
successful abort remains retained and unavailable to publication until a
separate proven-abort resumption policy exists. This is a remaining liveness
limitation, not automatic recovery.

## Added controls and evidence scope

The four genuine fifth-target pipeline controls are:

1. Actual original donor provenance plus target native body/provenance readback.
2. Actual cancellation after native commit; a genuinely fresh same-phase request
   must reconcile the same xid, record and provenance bytes.
3. Actual cancellation before the first put followed by native abort; a fresh
   request must refuse without replacing the original native attempt.
4. Actual `page_log.sync` failure on native commit; unknown/fenced custody must
   survive a refused fresh request with no replacement xid.

They preserve the original S2, source ACK ledger, source-frame journal and
remote NEW assertions. API outcomes are retained before diagnostics and
assertions. The poisoned case uses a separate **memory-only** original-custody
observer; it does not claim readable payloads after a health fence. Actual
older-reader refusal is checked. Six additional pure-DATA refusals exercise
omitted/duplicated constraints, missing original close, wrong source session,
and future unresolved/native-materialized pending obligations; these are not
additional network or runtime qualification cases.

## Still open

This producer persists **one Registered target native DECISION fact**. It does
not persist a range catalogue acknowledgement or count a durability vote. The
OLD-only native publisher, dual-configuration durable decision quorum,
metadata conditional decision, participant PREPARE/activation/Open, final OLD
custody retirement, Joined Serving and controller integration remain separate
producers. A native receipt here grants none of those permissions.

Cold continuation, crash recovery of this new exact attempt, exhausted-custody
retirement, real independent-machine failure, the 64-range/four-to-five-node
scale-out gate, sustained throughput gain and production qualification remain
open. No existing source API or test name establishes those results.

## Current-reader negative component controls

A source-only successor adds six **injected read-observation** faults inside the
same actual current common/provenance reader. The positive pipeline first
obtains a genuine committed target/native receipt and real CURRENT/archive
records. Each scope calls `read_native_joint_decision`; it never constructs a
receipt from fixture DTOs or returns a receipt/grant from its helper. It retains
canonical hashes of the actual original body/provenance and each exact refusal,
then revalidates the untouched original native bytes after the fault scope ends.

The six observations are an erased profile-pointer observation with the actual
sidecar still present, missing provenance CURRENT, missing archive, mismatched
archive, mismatched sidecar xid, and omitted complete source obligations. Each
must refuse through its intended reader branch. These are read-observation
faults after actual reads, **not on-disk corruption, actual native transaction
aborts, cold recovery or six additional runtime tests**. They run within the
existing positive pipeline case; the four original pipeline cases and budgets
remain unchanged. All these controls remain uncompiled/unexecuted here.

A separate `not(feature = "tls-replication")` unit control uses the real native
origin fixture and actual direct/current decision reader entrypoints. A
refusal-only semantic format flag (no remote record or grant) must cause both
readers to reject the unsupported profile before looking up a record, even
without a provenance pointer. This control requires separate no-TLS execution;
compilation of a workflow consumer is not evidence it ran. It is also
**not tested** at this source-only stage.

The no-TLS successor also rejects this semantic flag in ordinary format
validation, before open/migration can silently accept its unsupported profile.
The reader-only unit checks that real format validation refusal as well; it
does not reopen a live database or construct a remote committed record.

## Source-only extension: genuine OLD native publishers

A separate successor adds the necessary OLD-only/survivor publisher, still
without catalogue voting, a dual-configuration durability quorum, Open or
Serving. Its added controls are uncompiled/unexecuted at this source stage.

`NativeDecisionSourceProvenance` is a **distinct readonly ingress**. It accepts
only `DecisionProvenance` and compares the actual mTLS observer ID/incarnation,
claimed birth, retained plan's OLD/NEW membership, and fresh source OWN/current
MStore enrollment. Original Start, Describe and capture APIs remain target-only.
A nonparticipant, wrong actual observer, wrong enrolled birth, Start or Describe
on the readonly ingress refuses without a capture or native publication.

The private transport producer now consumes the genuine original remote NEW
participant before fetching. Its enrolled observer birth must equal that
participant's actual protected native snapshot and current OWN, before and
after the real pinned exchange. The sealed result retains the actual transport
observer ID and bound birth; the publication admission checks them against the
same original participant again. Decoded request/reply DATA cannot mint it.

INITIAL `PublishDecision` independently fetches source evidence and the exact
NEW set, then obtains fresh actual recipient Role/OWN/MStore. It uses the
original OLD observation, exact native close/promise, installed writer State,
DB/open, hook/coordinator/registry, Store and original Frozen. The lexical OLD
scope checks pure stored predecessor-policy facts and original hook identity;
it does **not** require effective ordinary Serving while CLOSED. The ordinary
State, APP/Strong and generic service accessors are unchanged.

The common private native writer accepts an enum containing either a real
Registered exclusion scope or this real OLD scope. Local native facts are
selected from the corresponding actual image base or original close receipt;
there is no Registered cast, manufactured local QQ or cross-node comparison
of database sequence numbers. OLD's Frozen remains owned by its original
handoff session. The target A/source MF/session remain protected correlation
in every node's sidecar. An OLD sidecar must match its actual original native
close/birth; a target sidecar must match its real image snapshot. Both require
the same complete decision body, while their local native/provenance hashes
and xids are naturally distinct.

Four added genuine pipeline cases cover retiree publication with actual
postcommit cancellation/fresh same-xid reconciliation, survivor publication,
a nonleader donor's independent self-callback/publication, and the readonly
observer/capture-ingress refusals. Existing four Registered cases and their
budgets/assertions remain. The OLD checks preserve native candidate bytes,
source ACK/frame evidence, catalogue/rows/other-range state and the original
freeze. An actual ordinary gate attempt must still refuse before and after;
this is a **gate component check**, not an end-to-end APP/SQL admission result.

A real remote receipt is still only a native fact. No implementation here
counts OLD/new majorities or changes placement, routing, authority, ordinary
read readiness, catalogue decision state, activation/Open, final source
custody retirement or production qualification.

### Repeated movement and reply-association limits

The initial OLD publisher is explicitly limited to an actual INITIAL source
admission and the original immutable from-birth predecessor configuration in
`State.record.facts`. A subsequently received range may be absent from those
facts; even an originally owned range can acquire a later Serving-overlay
configuration. Both refuse with an explicit missing-historical-predecessor
diagnostic. The four OLD controls above cover original INITIAL configurations
only. This is not repeated movement, a Joined source producer or a complete
self-healing path. A real retained historical Serving-audit predecessor,
observed before OLD closure and independently checked afterward without an
effective-Serving fallback, is still required.

A source review also identified a reply-correlation defect: accepting any OLD
recipient in receipt DATA without tying it to the reply's authenticated snapshot
recipient. The successor adds that equality check. Each genuine successful OLD
reply supplies a pure-DATA negative control: substitute another OLD node ID and
recompute the outer checksum, then require the actual protocol validator to
refuse. This does not create a new authenticated frame or native grant. No
runtime mispublication or durable-data loss was observed; the source defect and
its unrun control remain distinct from the preserved qualification evidence.


### Retained historical predecessor producer (source successor)

The successor introduces `NativeWriterHistoricalPredecessor`, produced from the
actual original State and DB before the actual runtime range freeze. This
ordering matters: `RangeMutationGate::freeze` invalidates the prior Serving
generation. Capturing a new Serving observation after freeze is forbidden.
The actual retained OLD table owns this proof before calling the runtime; the
producer outcome and original OLD observation retain that same allocation.
Fresh retries of the exact plan recover that owner from the original table.
Missing, different-State or different-plan custody refuses instead of rebuilding
it from native DATA.

The born profile keeps the exact immutable original predecessor checks. The
Serving profile additionally requires the actual cached verified publication,
real native Serving CURRENT and immutable archive, its exact original CLOSED
record/archive, original open continuity and prior quorum audit at capture.
After freeze, only the retained historical object can check those immutable
archives and original State/plan/base/namespace/history. It does not consult
current effective Serving, renew a released generation or turn a historical
audit into APP/Strong permission. In particular, the next CLOSED CURRENT may
advance without replacing the captured predecessor's immutable archive.
The ordinary writer, APP, Strong and no-image guards are unchanged.

The existing original-INITIAL OLD controls now retain and compare their born
profile in the first/retry evidence. The actual Serving-profile path is source
implementation only; a second real handoff after a genuine first Serving
publication, archive-erasure/wrong-State refusals and the Joined OLD/discovery
adapter still require independent execution. Joined ingress is not enabled by
this producer, and it is not a repeated-movement or production qualification.
No cold reconstruction, new attempts after uncertainty, catalogue votes,
activation/Open, final OLD retirement or controller integration is supplied.


A follow-up source review of the frozen predecessor component identified a
concurrent-admission ambiguity: two still-current steps could capture distinct
owners before either inserted the original table entry. The successor checks
exact predecessor allocation at both actual retained admission sites, before
constraints, freeze or native closure. A losing duplicate capture refuses
before effects; a genuinely fresh step retrieves the table's original owner.
The original outcome also continues to reject replacement owners. The existing
`RemoteOldFrames` case adds one separately bounded component observation using
two genuine pre-freeze DB producers and that same production guard, recording
its refusal and unchanged native-close absence before assertions. It is not a
concurrent TCP schedule test, a new runtime case or proof of the Serving profile.
The original frozen source and all failed qualification evidence remain intact.


### Constraints-first continuation correction (source successor)

`observe_constraints_for_constraint_step` already performs the real range freeze
before reading raw/native safety. A constraints-first owner therefore cannot
capture new Serving provenance during a subsequent Close. Its genuine constraint
admission now obtains the same sealed pre-freeze predecessor as Close admission;
the original table retains that exact allocation before constraint observation.
Both entry points check the same original allocation before constraints/freeze.
Constraint revalidation uses that historical proof plus every original schema,
restore, member-pin/birth, actual State/Store and current authority check. The
strict ordinary predecessor helper remains available unchanged in meaning.
The joint-only readonly response also consumes the retained historical identity,
so it never requires a previously released Serving generation after freeze.

Close after successful constraint collection retains the existing original
constraint owner, complete facts, runtime, Frozen and predecessor. It allocates
the first actual producer-outcome holder before invoking Close; an existing
outcome is never replaced. Uncertain closure remains quarantined and is not a
new attempt, a rebuilt proof or a closed vote. Successful closure transfers its
actual receipts to the original OLD observation as before.

The additional actual INITIAL/born-profile test
`native_registered_fifth_remote_old_actual_constraints_then_close_preserves_original_custody`
uses pinned real RPCs for Constrain, Close and an independently fresh exact Close
replay. Before Close it requires the original closed/native-record absence and
complete constraints. After Close it compares actual retained runtime,
predecessor, constraint-owner and Frozen allocation diagnostics, full constraints
and native record bytes; replay must reuse the same actual closure. Diagnostic
addresses are JSON identity observations only, never authority or owner handles.
Every primary operation/result and custody read is retained before assertions.
The original constraint-only unresolved-minority test and all its absence
assertions remain unchanged. Neither test supplies a Joined/Serving predecessor
or qualifies repeated movement, cold recovery or physical scale-out. No result
is claimed until the coherent source actually compiles and executes in CI.
