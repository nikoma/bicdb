# Native collection reservations and rename undo retry

These results cover an **unmerged foundation**, not completed SQL scoped
admission, broad workspace CI, application acceptance or production readiness.
The SQL unrelated-writer regression at `ce3dc430` is still an open integration
gate. The original application assertions and ten-second timeout are unchanged.

Native runtime source `f4ab135fef987fddd9fbc4c53c8c3d914a90005d` passes five
focused commands (29 tests), followed by six selected integration commands
(98 tests). The latter cover portable snapshot/WAL reader requirements,
replication, schema compatibility, SQL startup/profile/promotion, rename and
recycle process cuts, pending composites, views, snapshots and read-only
transactions. Process exits and reopens do not establish VM or power-loss
qualification.

Transactions publish weakly registered collection-identity bindings. A declared
catalog scope atomically checks existing bindings and fences later native
transaction access to the protected names and immutable backing identities.
The existing owner-preserving native rename permits unrelated pending work
only when its owner has reserved both names. The ordinary unscoped pending/
row-lock gate and native mutation-grant checks remain in effect.

Controls cover both stores: unrelated acknowledged writes survive the owned
rename and reopen; prior reads and retained read locks refuse reservation;
already-begun and newly started transactions cannot read, write or read-lock
either reserved name; foreign tokens cannot release a scope. Commit, rollback,
drop and exact release end runtime reservations, while a statement snapshot
refresh retains them. Sixty-four first-observation/reservation races have one
winner. A separate control drops the database before its transaction and
checks cleanup through owned shared handles. The host's normal database
execution guard is still required; these reservations do not synchronize
arbitrary raw catalog mutation or provide SQL catalog visibility.

The first candidate `2d1e2e9f` passes three of four tests but fails the old-name
admission assertion: the legacy policy lookup returns NotFound before the bind
gate. The correction checks reserved missing names while preserving existing
relations' policy/grant error order. The denial assertion is retained and later
write/read-lock assertions are added. `de64001c` passes its four commands. The
final `f4ab135f` refinement separates the registration mutex from the scope gate,
avoids collection locks under the exclusive scope gate, and adds the owned-drop
control. A local missing-brace formatting failure at `aa7d0b75` was corrected
before any remote build or tests ran on that source.

A separate tests-only source `43358d64` demonstrates a SQL undo-loop defect.
Foreign pending work refuses journal recovery, and the loop has already removed
the retry entry (actual length 0, expected 1). Source
`392688d5e38efe5b3584dee854fb43a2b0596896` retains a durable rename's session
entry until recovery succeeds. It clones only the small journal identity,
without cloning legacy table-row payloads. Both positive commands pass: all
rename units, including refusal followed by successful retry with the original
native owner, and the selected composite/view/snapshot/read-only integrations.

SQL integration must still acquire the complete native and SQL dependency
closure, authorize internal host metadata operations, keep ownership through
commit errors/full rollback/disconnect/savepoint cleanup, and fence relation,
type, routine and Describe access. The current SQL host consumes the native
transaction before full DDL rollback; simply attaching a native runtime scope
would release it too early. Durable pending-journal admission and standby
restart behavior therefore remain required. These focused results do not
justify removing the SQL producer's global checks or declaring that gate done.

The receipts and their digests are retained here. No standard server binary,
release or deployment was produced by this batch.
