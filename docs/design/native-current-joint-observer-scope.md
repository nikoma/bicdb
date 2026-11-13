# CurrentJoint observer scope: unexecuted correction

Base `1a776947b2a13ce9fd59457da7b18d539803bc4d`, runtime through ded3.
The separate ded3 profile (`7dbb916c06f2a45e728855b9f1b00585db1e98d9`,
`docs/design/native-current-joint-ready-peer-profile-ded3.md`) measured
Completed peers' full validation exceeding their actual 810 ms first child
slices; those failed results remain separate. This source changes no deadline,
worker, retry, collector, fixture budget or original acceptance assertion.
No execution or latency improvement is claimed.

The operational Data Ready/Completion observer uses a new PRIVATE same-call
`NativeReadyObserverReadScope` ONLY for CurrentJointRecovery. NormalRecovery,
Activated observations, standalone full APIs and mutation completion scopes
retain their full paths. No observer is cast into a mutation role. The scope's
private fields, HRTB callback and explicit !Send marker prevent a scope from
escaping or crossing threads; it has no Clone/Serde/DTO constructor or write API.

The actual Data service captures its actual current RangeWriteServiceSlotGuard
while retaining its service borrow. The scope binds that slot to the SAME Store
open, checked DB, Prepared, actual observer/request, ORIGINAL caller token,
original installed writer Arc and real native journal. Entry performs full
owned materialization and derives one actual native lineage; its R/H0/H/local B
and native receipt must equal the actual Prepared point. No native Unknown or
numeric catalogue watermark can produce it.

A PRIVATE enum selects completion versus observer borrowed-journal/lineage
routes in the existing protected CurrentJoint fact factory. The observer path
reuses this actual journal/lineage, preserving protected native bridge current
and archive, exact original local OLD closure/permanent promise, original writer,
all saved and archived floor/history bindings, birth, namespace UID and actual
catalogue activation. Standalone factory/receipt checks still reconstruct their
full current lineage. No network or recursive native journal runs in this scope.

The scoped Prepared validator keeps the exact old observer identity, durable
catalogue, phase, saved native point and full cached-binding shape checks. For
Completed it reads the ACTUAL recovered-serving current pointer AND immutable
archive, validating the full record against that same lineage and pinned
R/H0/H/local B/native receipt. It does not repeat a second full physical check,
two fresh lineage constructions or mint a new native admission just to reissue
that record. The existing irreversible map projection remains unchanged.

There is NO newly invented raw-suffix/full-history equality, 1024-entry scan or
abort-slot compaction rule. Physical materialization, exact catalogue binding,
protected CurrentJoint/local closure, every saved/archived floor and original
projection retain their prior meaning; compacted/imported image facts remain
supported on that basis. This correction adds no general history qualification.

Before return, the scope performs actual physical/native/generation/schema
comparison against its retained lineage again, rechecks protected bridge pointer
and archive through the borrowed scope, original writer/local closure and all
saved/archived floors, actual phase/binding and serving current/archive. It also
rechecks actual hook/registration/DB open/Store slot, native admission, pin/Frozen,
schema/health, observer scope and ORIGINAL caller cancellation/deadline. Those
facts are not made immutable by holding a native journal. No prestate boolean or
owned "already checked" proof can replace that exit. Pure response construction
still occurs under the same Data service/exclusive Store/DB/Frozen owner borrow;
no status result becomes a Ready quorum or releases a freeze.

A separate typed full-fixture continuation, after real all-member handoff,
ACK2 and StrongR2, obtains actual CurrentJoint fences/Prepare and a real n1
Completed R2 via TLS. It checks foreign ACTUAL DB/Store rejection under the scope,
original cancellation after entry (no returned observation), a subsequent genuine
fresh TLS status, then REAL public foreign hook replacement after entry. The
last must revoke the original registration and refuse, not recursively install a
native policy under the held journal. Full raw/native/history/UID/B/slots/ACK
snapshots and actual Frozen owners remain unchanged. A scoped test hook is
thread-local, restores on unwind and carries no production authority.

The original full positive and post-joint NoData acceptance child are unchanged.
The new controls require their genuine prerequisites; an earlier Prepare timeout
is still a failed prerequisite, not an observer-cut result. Their new path, all
existing positive/authority controls and compile checks must execute before any
passing claim. This does not solve the separate Prepare timing failure, long-run
capacity, cold recovery, DATA-policy transition, remote wire/operator or physical
8 GiB qualification gates.
