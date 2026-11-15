# Cancellable native catalogue leadership validation (source, not qualified)

This source component adds a distinct selected-range entry after the local
remote-handoff poll correction. It does not replace legacy recovery, perform
native prefix repair, publish a dispatcher or provide a Native Ready/Strong
read grant. It has not been compiled or executed. Native INITIAL recovery and
Registered selected-range publication now call the bounded entries in source;
their full recovery/publication acceptance remains pending.

`recover_native_range_leadership_bounded` requires an actual finite caller,
the exact current local-leader/voter range and an actual enforced native store.
The existing native-read limit of 64 replicas bounds its peer fan-out. It uses
the same prefix and tail validation predicates as ordinary leadership recovery.
A behind prefix, unresolved/ambiguous/divergent tail, missing quorum or changed
route still refuses; the bounded entry cannot select generic replay or repair.

The new transport methods fail closed by default. TCP implementations carry the
original cancellation into the existing deadline socket/TLS path. They require
numeric addresses from the same locked dispatch snapshot and bind the request
and reply to the actual advertised enforced native birth. They do not fall back
to uncancellable transport methods or perform unbounded DNS lookup. All selected
peers are contacted concurrently in each bounded round, and joined before their
results are inspected; neither a timed-out peer nor a scheduling hint is counted
as a quorum reply. The legacy branch retains sequential call/validation order.

Topology, service, admission and recovered-marker mutex acquisition observe the
same parent. The selected mutation permit and actual admission reservation stay
held through the final current-route/parent check and recovered-epoch insertion.
A late result cannot create that marker after cancellation. The boolean reports
only whether that generic epoch marker was new; it is not permission to serve.
The caller must still satisfy actual installed policy, native readiness, current
metadata/Role/OWN and dispatcher publication checks.

The original `recover_local_leadership` API retains its existing legacy behavior.
The retained INITIAL owner uses a bounded 64-range sweep with its existing
rotating cursor and per-range current-history validation. Cancellation and
deadline errors remain typed; independently completed markers are retained when
a later range fails. The cursor advances for each range selected for an actual
attempt, before that attempt, rather than to the unattempted batch tail. Source
review caught that using the batch-tail cursor together with an early shared
deadline return could repeatedly starve a later range. The corrected scheduling
has no runtime progress result yet; a genuine multi-range deadline/fairness
control remains required. Registered publication validates only its selected locally
led range; a follower creates no leader marker. All subsequent actual policy,
metadata, native continuity and dispatcher checks remain required. The separate
generic Joined recovery path is unchanged. This is source wiring, not executed
acceptance. Filesystem validation, allocation,
cleanup and kernel calls are not forcibly preemptible. This is cancellation
propagation, not a measured wall-clock RTO or production qualification.

Required next controls use actual enrolled native hosts and authenticated peer
IO: cancel/expire during a selected progress request and tail probe; hold the
actual local service/topology/admission locks; refuse a late epoch publication;
recover after removing the fault without leaking reservations or rewriting
native history; keep another range's APP/full Strong working; retain ordinary
legacy regressions and refuse unsupported/deadline-free native transports.
Five new controls are defined on actual enrolled hosts: selected native
revalidation followed by the original SDK APP/full Strong, and progress/probe
socket cancellation/expiry at the actual selected peer destination. A real
replacement blackhole socket must accept before cancellation; expiry must
retain the original deadline. No authenticated reply, history or marker is
fabricated. The socket controls test transport primitives, not a full recovery
or late-publication path. They compare real local native observations and
existing readiness before/after. The positive case revalidates an already
recovered INITIAL epoch; it does not qualify a newly elected epoch.

Actual held-lock, late-publication, recovery-after-fault, unresolved-tail,
unrelated-range progress and unsupported-transport controls remain outstanding.
All new definitions and their results remain **not tested**. Automatic scheduling, final
metadata, repeated movement, cold recovery, cleanup, the failed Azure baseline
and the independent four-to-five production gate remain separate and open.
