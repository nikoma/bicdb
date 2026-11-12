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


## Original-TTL release writer continuity (source-only successor)

The separately confirmed `e8d263bac30f59f48b7c71a478304760fc564b82`
regression concerned fresh collection after actual writer reinstallation. The
original receipt release paths had an adjacent source-only gap: image-target
`NativeJointImageReleaseOwner::release` and surviving-voter
`NativeJointHandoffRuntime::release_activated_new_member` validated the current
host and protected activation, but their activation receipt retained no original
writer registration. No false activation or lost ACK was demonstrated on those
original release paths.

`NativeJointCatalogueActivationReceipt` now retains the original prepared writer
Arc and actual catalogue root. Both native activation persistence and exact-fact
recovery obtain it from the admission's original prepared evidence before any
write or receipt construction. Receipt liveness checks original registration,
open/health and range coverage without borrowing the DB or native journal.
Currentness also checks pointer equality with the actual Store writer and the
actual borrowed DB. Persistence checks it again before mutation, mint and return.
Thus the existing original image and survivor release paths refuse a replaced
registration before dropping their retained freeze owner. The serialized
`NativeJointCatalogueActivationFact`, activation checksum, original request
cancellation/deadline and immutable native audit do not change. A historical fact
alone still grants no release or renewed authority.

Two new unexecuted controls in `complete_joint_tests/original_release_writer_tests.rs`
use distinct typed image-target/surviving-voter continuations. Each starts with the
real same-open nonempty handoff, all-three protected native publications, a full
actual fresh three-member open phase and independent local post-quorum MQ. It
persists one actual local activation, drains duplicate preparation owners, then
installs a genuine fresh same-config Host-MQ writer policy before calling that
recipient's ORIGINAL release API. Request-only checks must pass before reinstall
and after refusal: expiry cannot satisfy the writer-identity assertion. The
control requires exact catalogue bytes, native R/H/local-B/history generation,
namespace UID and original ACK row to remain unchanged and the actual freeze to
remain held. Pre-receipt timeouts have at most three genuine bounded attempts;
no returned receipt TTL is extended. These controls do not replace the full
all-three activation/release + ACK2/Strong terminal test, and do not qualify cold
or intentional writer-replacement recovery.

This successor has only formatting and source checks locally; no Cargo or
runtime result is claimed. The preserved 87d revision and its CI scope remain
separate.


## Bounded current-scope successor after the nine-test batch (source only)

Exact `eb5b454a578f77309900e2a248fc0b779391333e` ran all nine
`complete_joint_tests` together with the unchanged four test threads and real
budgets: CI43233276416/Core131233899683, 5 passed/4 failed/890 filtered,
77.72s; compile passed. Raw log SHA-256:
`f3779145fb319f664bed47f6d27bfc52ef5e5222db53b1bffde137d9f86ba1b0`.
The surviving-voter original-release control passed. The image counterpart
returned an actual receipt with 298ms remaining, then its independent native
snapshot consumed that remainder before reinstall. The original-caller negative
failed before its intended cut at initial prepared aggregation. The full terminal
and all-Frozen-open tests expired in target final native scope after prior
collection/foreign checks. These remain failed gates, not timing qualification or
attribution to a physical host. The separate earlier ab261 three-fault pass keeps
its own exact source scope.

The successor removes an actual duplicate read: `current_read_scope` acquired the
real native journal, called `receipt.require_current` (which already derived and
validated actual SourceLineage), then derived SourceLineage again. A private
receipt helper now returns that first validated lineage only after the same
actual record, protected native anchor and exact current-pointer/archive checks.
The scope retains the same journal through its lifetime. The public currentness
API still returns unit; no cached, serialized or independently owned scope proof
is exposed. Every retained outcome/local catalogue check and pre/post publication
validation remains unchanged.

Initial prepared-round `finish` is now bounded aggregation, like the separately
reviewed fresh-phase finish. Every recorded observation was fully checked at
record time. Finish retains original caller/deadline/cancellation, each original
MQ's exact committed-domain binding and original writer lifetime checks, without
another complete physical sweep. This quorum remains materialization audit, not
publication or serving permission. The actual mutation paths retain their final
complete foreign participant sweep, local actual kernel/journal scope and fresh
post-quorum MQ checks; original and fresh publication paths are not weakened.
Explicit `require_current` still performs its full physical validation.

The image/survivor replacement controls now capture immutable R/H/local-B/history
and namespace identity before acquiring the 4s receipt, while genuine Frozen
owners are retained; exact equality is still checked after the actual reinstall
and writer-identity refusal. Exact raw catalogue bytes are still captured after
actual activation. Request-only liveness assertions before/after the cut remain,
so expiry cannot green the control. There is no extended deadline, reduced
participant set, test serialization or forced state. The same nine-test remote
batch is required to validate this successor; only static checks were run here.
