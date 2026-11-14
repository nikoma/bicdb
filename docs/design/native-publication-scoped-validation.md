# Native publication validation under the original journal

Source checkpoint: `ac3e68241a77c63963015770215e9fb115333182`, an unmerged
1.0.515-beta successor to failed candidate
`7b965c6854843b7a127a5575214deddee2bc2c36`. This document describes source
behavior and required controls. Compilation, executed acceptance, physical
qualification and deployment are separate results; none is established by
this source checkpoint.

## Problem and boundary

The failed candidate's twelve hot serving scenarios time out during native
publication. Three calls complete native WAL before failing a postcondition;
eight stop after preflight, and one stops in the final foreign sweep. See
[the retained qualification report](../ha-current-source-audit-2026-11-14.md#later-terminal-follow-up-pr203).
The traces locate finite-window failures. They do not establish CPU versus I/O
causality or prove a performance improvement from this change.

An original CLOSED survivor can require the same full physical range contract
several times while one actual journal is already held. Permissionless
collection also validated materialization even though its only outputs are an
opaque comparison identity and removal of its own capture pin. Neither output
permits native publication, Serving, an application operation or a strong read.

The correction separates those necessary identity comparisons from the full
materialization proofs at their actual sinks. No request deadline, participant
set, quorum, durable format or ordinary application admission changes.

## Required validation

| Boundary | Required evidence |
| --- | --- |
| Collection or owned-pin drain | Original preparation session/cancellation, actual service and checked DB borrow, Store/State/hook/open/slot, protected original writer policy and registry, selected range, original Frozen allocation and fresh native DECISION/metadata observation. Output remains comparison identity or owned-pin removal. |
| Fresh publication service entry | Exact original actual-preparation pointer, actual held service address, original custody and collected phase owner. A temporary DB borrow ends before the final borrow. |
| Final foreign validation | The complete original foreign participant set and its unchanged strict currentness checks. Local comparison does not replace this sweep. |
| Local journal preflight | Actual journal acquisition, independently validated full physical source contract, complete catalogue/preparation binding, CURRENT/archive and retained history, original Frozen and fresh authority. |
| Repeated local CLOSED check | Exact scope DB/Store/original CLOSED allocation/Frozen; original process/runtime/service/open/slot/hook/State and registry; actual protected CLOSED counter/CURRENT/archive, native DECISION, metadata owner, promise and cut history. Only the physical comparison may use the already validated source lineage held by this scope. |
| Post-write | New actual full physical source read under the same journal, unchanged physical/history identity, selected effective membership, every retained history resolution, actual new bridge CURRENT/archive, full preparation/catalogue checks, fresh authority and final version guard. The old comparison lineage is never substituted for this poststate. |

The scope is borrowed and cannot be reconstructed from an ID, counter, digest,
serialized record or successful earlier request. The physical comparison route
is restricted to genuine original CLOSED survivors. Installed-image CLOSED
owners and other profiles keep their strict original path. Generic
`require_current` APIs remain strict.

The existing `current_native_range_source_lineage_under_journal` helper
validates one full contract plus pending/recovery state under the actual
journal. It avoids the generic reader's second validation of that same
contract. Preflight and post-write each perform a distinct actual read.

## Countercontrols and uncertainty

The source adds two actual native-fixture cases in
`hot_policy_tests/publication_scope_tests.rs`. They are not passed results at
this checkpoint:

- Delete the exact protected CLOSED archive after scope acquisition using a
  test-owned Controlled mutation under the original journal. After dropping
  that Controlled guard, the publisher must reject the missing archive before
  additional bridge WAL. The XID/LSN baseline is after the deliberate fault.
- Delete a retained middle history entry at the same cut. The publisher must
  reject the exact missing history while retaining Frozen. Detection may occur
  after bridge WAL; diagnostics preserve that distinction and never infer
  rollback from the API error.

The archive case additionally supplies a genuine other held service and another
retained preparation to the fresh service-identity boundary. Both substitutions
must be refused without native effects. These components create no synthetic
owner or authority.

The complete original positive handoff cases and all original negative
assertions remain required. The new controls deliberately require the scope
and fault to be reached; a timeout before the intended cut cannot pass them.
Test-only mutation hooks are not normal callbacks bypassing journal exclusion.
Neither these controls nor a successful CI suite establishes automatic remote
Serving, fault-safe physical rebalance, sustained capacity gain or production
qualification.

## Follow-up after failed 1fc acceptance

The [complete 1fc result](native-1fc-acceptance-result.md) is **failed**:
330 passed, 16 failed and one ignored. The protected-archive control passed;
the middle-history control failed during an earlier preparation prerequisite,
so that planned corruption remains **not tested** by this run. Supervisor and
RF3 component passes do not close the serving failures.

The source follow-up also uses the same borrowed journal scope inside a local
prepared-evidence currentness check. It retains the original capture-pin mutex
and validates that exact pin before and after the check; absent a pin, the
original quiescent-Frozen check still applies. Quiescent Frozen excludes active
and Controlled mutation; it does not itself exclude the retained source pin.
The scope independently reads the real lineage, preparation CURRENT/archive,
catalogue and every retained history entry. Original CLOSED identity, counter,
CURRENT/archive and promise checks run before and after within that scope.
Every foreign participant still runs its own complete check on its actual
service and database. No local scope supplies another participant's proof.

Registered, Installed and image profiles retain their full currentness path.
The Registered target additionally proves that its catalogue materialization
owns the exact same original preflight Arc before delegating to the existing
full catalogue check, which itself validates the supplied physical base and
its installed history. This removes a preceding duplicate base traversal; it
does not replace physical validation with an ID or a cached NoWriter claim.
Process, DB, slot, native admission, namespace, catalogue and durability checks
remain required.

These are source changes pending execution, not a measured speedup or a passed
recovery fix. Full original acceptance, intended-cut execution and independent
physical qualification remain required. No deadline or participant set changes.
