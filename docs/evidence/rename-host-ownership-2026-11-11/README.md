# Retained host ownership and rename permissions

These receipts qualify private source `dd9d199ecfb57582673d7930c0dab0870a4da31c`
for the focused scope below: seven commands and 112 tests pass. The source
version at that revision is `1.0.460-beta`. Later integrations with newer main
versions need separate validation. This evidence does not establish completed
SQL scoped admission, broad workspace CI, application acceptance, VM/crash
qualification or production readiness. Original application assertions and the
ten-second timeout are unchanged.

A retained runtime catalog scope survives consumption of its native owner at
rollback, drop or failed commit. Internal transactions capture an exact scope
and database identity; pending children refuse the owner's decision and scope
finalization. A successful owner decision cannot be resumed as undecided undo.
The trusted host remains responsible for proving the durable journal decision
before finishing a retained scope. Runtime tokens are not durable recovery
inventory and do not authorize raw catalog mutation. A restart must reconstruct
admission from the pending durable journals; that SQL integration is still open.

The initial lifecycle source `c5625c39` passes four commands and 68 tests.
Source `21e26367` adds refusal of sibling-scope finalization while overlapping
internal work remains pending. The final host controls cover both stores,
rollback/drop, real unique-commit failure, role demotion, exact database and
scope identity, policies/grants, overlapping children and 64 begin/finalize races.
The outer SQL producer still retains its conservative global checks.

The ACL control at `ece30483` actually denies `later_reader` on the original
table but permits it after rename. Renaming called the creation wrapper, which
reapplied defaults changed after the table was created. The correction saves
the journal-covered schema row while retaining the relation's existing ACL.
Its positive test checks commit and rollback, destination reuse, both stores,
reopen and creation defaults on a genuinely new table. Native PostgreSQL 16.13
passes both reference decisions with the same relation OID and role-visible
permissions. Those temporary roles and schemas were removed; no protected
application schema was changed.

The first ACL-positive batch at `21e26367` then fails on a valid migration
batch beginning with ALTER DEFAULT PRIVILEGES. The raw handler parsed later
statements as part of the grantee list. It now declines multiple statements so
the existing SQL-aware split dispatcher handles each separately. Source
`5188e977` passes all seven commands and 111 tests for the permission, parsing
and retained-ownership batch.

The demotion control at `2cbcf517` actually leaves a retained scope closed after
a refused owner commit. The standby early return skipped common error cleanup.
After that correction, `0ca35a4d` advances to a second actual failure: an internal
transaction can buffer a standby mutation. Internal host writes now check the
node's writable role before buffering. Read-only inspection remains possible;
these are local role-API controls, not network election or VM qualification.

The separate control at `97bdaf8b` demonstrates that a host transaction can
mutate an unreserved collection. The final runtime checks declared names and
immutable backing identities before common, stored and repairing writes acquire
row reservations, with a central append backstop. Ordinary clients and replica
actors retain their existing behavior. Reads keep ordinary native access rules;
policy and mutation grants still apply. This bounds native transaction row
effects, not arbitrary raw catalog operations or SQL's future metadata rows.

At `2e5566b2`, nine host tests pass and the new test correctly rejects all
unreserved writes, but its purported unrelated transaction is accidentally
created inside the ambient host context. Source `dd9d199e` drops that context
after creating the internal child. The child retains its captured authority;
the later native transaction is ordinary. All denial/progress assertions remain,
including the optimized repair path and immediate unrelated progress after a
refused write. All ten host tests and the full seven-command focused batch pass.

Remaining work includes complete SQL dependency/resource acquisition, metadata
row admission, relation/type/routine/Describe visibility, pending-journal
inventory across restart and standby, and ownership through all SQL cleanup
paths. The unrelated-writer SQL regression at `ce3dc430` remains an unresolved
producer-integration gate. These results do not justify removing global checks.
No standard binary, release or deployment was produced by this batch.

Receipts retain exact commands, revisions and outcomes. `test-observations.json`
records test summaries and original failure excerpts. File digests are in
`sha256s.json`.
