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
