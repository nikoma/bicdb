# Retain prior-session constraints through successor publication

This unmerged source-only leaf starts from exact `ff8f10fdbf6a244ca6dda0c436e3ab8e46b0f4ed`.
It has not been compiled or executed. No new wire or durable format is introduced.

The prior source used cached empty pending/veto sets to remove a prior replica
session **before** acquiring the successor's fresh role. A failure of that actual
role acquisition could therefore leave neither the old tracking record nor a
new job. This is a custody gap, not evidence of acknowledged data loss. The
existing successor's actual native/quorum/activation/read-quorum publishers can
independently re-establish safety; the cache is not itself a serving grant.

Test-only `1c5addeef` adds a separate required repeat-origin case. It retains the
genuine first recovery, full all-replica convergence, original owners/guards,
second actual pre-WAL Unknown, actual peer materialization and original API/
phase/twenty-second gates. A scoped non-Send arm is keyed to the actual original
controller/database, selected range and real pending command ID/checksum. It
cancels only the actual live maintenance-turn token immediately before fresh
role acquisition; no metadata reply, role, error, phase or authority is fabricated.
After that actual call returns, it observes the real retained map and absence of
a successor job. The original process continues. Success requires both preserved
prior custody at this failure and complete eventual native/ACK/Strong recovery.
Original repeat-case behavior is unchanged when this arm is absent. The new
required case is unexecuted and must not be described as a reproduced red result.

The corrective successor leaves the old map entry untouched until all fallible
new-job construction prerequisites succeed, then moves it into the retained job.
Every subsequent failure retains it there. Only successful real
`publish_hot_native_recovery` allows its retirement, after actual native
activation, fresh enrolled read quorum and final metadata quorum checks. The
old cached completion becomes scheduling eligibility only. This does not add
an unconditional ban on independently proved new-session recovery after a peer
open changes.

At most one prior session accompanies each active range job; this does not
create an unbounded chain. A replacement range consumes one active-range slot
and can retain two bounded tracking records while the new attempt is unfinished.
No constructor infers a new native floor or reproduces authority from diagnostics.

Still open: a second Unknown while old physical debt is **not** converged. That
case continues to retain its old session and explicitly refuse a new attempt;
it needs a genuine completed-decision/session-continuation protocol for lagging
or frozen peers. Cold reconstruction, physical failover and scale-out remain
separate qualification gates.
