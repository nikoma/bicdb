# Remaining acceptance gaps and smallest next tests

These proposed tests are not results of this packet. Original per-request
budgets, assertions and failure evidence remain unchanged.

## TR-1 — Transport independence is not stalled native I/O

**Current component passed; disk/native-lock independence not tested.** The real
Rows handler is held after export, outside engine locks. The other populated
range continues through the same original gateway/SDK. This tests transport
coupling, not a source database lock, fsync, page read or disk stall.

The next smallest control needs a genuine selected-range native I/O cut while
range2 APP/full Strong continue under the same original2000ms budget. Retain
where the actual blocked operation holds engine/journal/session locks, exact
attempts/ACKs/uncertainties and hold-before/after snapshots. If a global lock
blocks unrelated operations, preserve the failed invariant and isolate its
scope; do not move the gateway, widen deadlines or substitute a response hold.

## TR-2 — Already-Serving Joined second-range admission is missing

**Full post-Serving second movement not tested.** Here range1 is only exported,
not installed or served on the receiver. Range2 is populated on source nodes,
not subsequently moved to the same Joined receiver. Blank FIRST admission is
not a general populated-range receiver, and an exported payload is not a writer
or Strong authority grant.

The smallest full case first completes a real range1 handoff to Joined and
proves ordinary fixed-gateway ACK/full Strong. It then receives actual range2
into that same populated Joined database, preserving range1 WAL/index/acknowledged
values and allowing range1 APP/full Strong throughout. This requires the genuine
populated-range producer with fresh actual native/Joined/current-metadata
proofs; do not relax FIRST guards, reuse INITIAL ownership, overwrite the whole
database or fabricate Role/ready DTOs. Future donor duties and cold restart are
separate gates.

## TR-3 — Routing movement and operational convergence remain open

**No retry/refresh/404 observed; changed-owner routing not tested.** Cached
SDK generation4 and response generation5 differ, but the unaffected range2
owner and epoch remain stable. That difference is insufficient evidence of
route correction. Preserve expected-descriptor DATA separately from actual
request destination/range/epoch and response barriers.

The next routing control changes the actual serving owner/epoch while using the
same original client endpoint, captures real refresh/redirect/error attempts,
and requires every successful Strong to return the acknowledged full value or
a bounded unavailable outcome. Absence cannot stand in for unavailable proof.
Use a real movement producer; do not inject SDK shard hints.

Then independently exercise bounded controller relocation and fault recovery.
A physical four-to-five-node test must measure sustained capacity only after
complete convergence, reconcile an independently durable operation ledger and
preserve every uncertain/failed/retried call. This component proves no physical
capacity, RPO/RTO, node replacement or production qualification.

## TR-4 — Original E0603 compile failure

**Failed compilation, zero runtime tests.** The new helper's original
`pub(super)` visibility excluded its grandparent caller. c968 changes visibility
only; both original failing logs and the separately passing exact component
remain in this packet. The next broad check must actually execute the complete
compiler-listed cohort. Compiling/listing83 and executing one case cannot be
reported as83 passing tests.
