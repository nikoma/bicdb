# Scoped native joint publication (unexecuted correction)

This source correction follows actual failing cohort
`398e2f2ff10049a3587bdb4884a218c65b2659c0`, run `43218446977`, Core job
`131183609612`: zero passed, three failed, 885 filtered, 34.20 seconds.
Raw log SHA-256:
`ab9dd6172736b43861b8bc11f750709021b7f779168b1a84d3ef9bdc357c744d`.
All three failed during their shared n3 native-publication setup, before the
open-adversarial hooks. These are not three executed failures of those hooks.

The foreign-writer and original-caller controls each made 15 completed
`prepared-native-current` checks (five per member) in n3 attempt zero. Their
native page-WAL spans completed in 3,967 and 6,434 microseconds, with 51 and
150 milliseconds remaining, respectively. Post-write readback exceeded the
original four-second phase. Later attempts exhausted the window in preparation
checks before reconciliation. The step-cancel control made 13/17/17 such checks
in n3 attempts zero/one/two without reaching a WAL span. These traces describe
these executions; they do not establish a physical I/O, concurrency, machine
performance or Azure cause. The original failed source and evidence remain.

A fresh phase still collects every original final member, including the new
leader, under independently fresh recipient-bound metadata QQ. It preserves the
original caller deadline and cancellation and the original quorum's explicit
cancellation. A distinct local post-quorum MQ is still required. Finish checks
complete-set membership and live actual observations. The private pin Option
can only move from Some to None; already-None skips intermediate repeated
service sweeps and confers no quiescence permission. A first actual pin drop
still follows the existing checked path. The final foreign sweep and local
kernel validation remain mandatory immediately before publication.

`NativeJointPreparedPublicationScope` holds borrowed database, preparation,
store, catalogue, image and real Frozen identities under the actual native
journal. Its constructor verifies complete catalogue/abort obligations,
quiescence, actual native lineage R/H/local B, physical base/UID/schema/
generations and the actual prepared pointer/archive. The admission then checks
the SAME service evidence and actual installed writer Arc against that scope.
No foreign service or network operation occurs while the journal is held; no
owned/serde proof or caller-provided floor is produced.

Only after that preflight may the producer acquire an owned Controlled lease.
It writes exactly the new bridge pointer and immutable archive in one actual
native page transaction. The postcondition explicitly reads and validates BOTH
stored records and independently rereads the current native source contract.
It compares the actual poststate against the prior physical base/prefix,
logical history, local B, namespace/schema identity and generations, and requires
the effective new write domain to be the protected decision's exact new members.
Every captured retained catalogue resolution is directly reread from the same
actual history generation before and after publication; a missing middle entry
or different Applied/Aborted outcome is refused even with an unchanged head and
catalogue digest. This bounded retained-map check is not a full 0..R export or
ancestry proof beyond retained entries.
It also rechecks actual preparation/decision records, catalogue obligations,
actual Controlled ownership, writer registration and current authority. The old
lineage is a comparison baseline, never a substitute for the poststate.

A pending uncounted publication now uses one scoped reconcile-or-publish
producer. An existing actual protected record retains its original quorum audit,
post-quorum context and page XID; no native write occurs during reconciliation.
A previously counted receipt disappearing remains an error. Timeout/cancellation
after WAL leaves the round pending and returns no success: a distinct fresh
scope must recover the actual stored receipt, without reviving old authority.

This batch has only formatting/static checks locally. No Cargo or runtime test
was run. Existing actual durable lost-reply/hash-preservation, expired-original-
caller, full release/ACK2/Strong, and three open-adversarial controls must run on
the coherent corrected source. The prior frozen-open/publication results apply
to their earlier exact source, not this batch. Four-second bounds, complete
participant sets and the remaining cold, repair, post-joint NoData, remote
handoff and automatic-operator gates are unchanged.

## Original writer-registration anchor (separate source correction)

Exact `e8d263bac30f59f48b7c71a478304760fc564b82`, run `43227153532`,
Core job `131213159621`, compiled and executed two passes/one failure,
887 filtered, 28.44 seconds. The actual post-fsync step-cancel recovery and
original-caller-cancel refusal passed. In the foreign reenrollment control the
old phase correctly refused (`native writer hook lifetime ended`) and the local
catalogue remained unchanged, but a NEW fresh phase accepted the replacement
writer into the OLD preparation. The retained assertion therefore failed.
Raw log SHA-256:
`7a85bbf5d91f3622ca21cceb9cb11b3c47b0360f3d7cd7cd1f567ffd658bfaca`.
This establishes erroneous fresh collection acceptance; it does not establish
subsequent false activation, a new ACK or lost acknowledged data.

The correction captures the actual installed `NativeWriterHostState` Arc at the
sole preparation constructor BEFORE the native preparation producer/WAL. It
checks that same registration again immediately before the producer and before
returning evidence. Original, fresh preparation, open and local publication
checks require exact object identity plus the existing actual database/root/
store/range/lifetime checks. Each path returning a writer to a later phase
returns this original live Arc, rather than adopting whichever writer is now
installed. A cheap phase lifetime check compares that exact original object and
its registration/schema/health lifetime without reacquiring a DB Arc lock or
native journal.

Reconciliation and image-owner capture also check this original anchor before
any durable reconciliation. Release continuation transfers only that original
writer Arc and its actual root/store binding; it retains no extra preparation,
pin or Frozen owner. Each release collection and post-quorum observation must
match the same original Arc. Thus dropping preparation objects to release a
freeze cannot reset writer identity in a later phase.

This is source-only on base `2c1b7d8be1f5672f2d26c106c8220ecf071a4971`.
The existing foreign-writer assertions and four-second/original-caller budgets
are unchanged. No new persisted format, token lifetime, permission constructor,
physical-history scan or post-joint NoData support is added. Formatting/static
checks are the only local validation; the exact three fault controls and full
terminal handoff need remote execution on the corrected source.
