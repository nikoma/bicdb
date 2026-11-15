# Registered Closed drain: retain and resume the same Installed owner

This source-only child of `70997ccaeaf66cc23f2fcb29eeefcf13200779c8`
corrects a continuation blocker found while reviewing the actual Registered
FIRST policy producer. It has not been compiled or executed. It is separate
from the public candidate, the bounded native leadership follow-up, and the
host-poll parent binding follow-up.

## Source finding and actual continuation

`prepare_registered_joined_transfer` ends the original Assigned Closed
admission before calling the real fallible `stop_and_drain`. Cancellation can
therefore leave the actual Assigned owner, Installed attempt and listener
retained with Closed `life=false`. The previous repair called `bind_closed`,
whose original liveness check correctly refuses that ended admission. No
observed runtime loss or historical failure cause is asserted.

Repair now first performs the original same-successor, Registered process,
service, range, native bridge, Installed guard, DB/open and coordinator checks.
When that validated owner retains its actual listener handle, it resumes
`open_registered_joined_data` on that same owner: the real drain creates the
real `DrainedClusterDataListener`, the original infallible transfer retains
Joined custody, and the actual replacement socket is bound. The listener's
presence selects this continuation only after the full owner checks; it is
not authority. No Closed life is revived, native installer replayed, new
writer enrolled, old Role renewed or native release uniqueness check removed.
The original no-listener repair still binds the genuine Installed Closed
owner. Stopping owners refuse. A missing listener refuses before ending a
still-bindable Closed lifetime.

The existing host-local Policy step obtains new bounded ALL-Released/self
Role/OWN/current MStore authority after the repair. A known native install
alone never publishes APP or proves Strong readiness. Partial drain/transfer
or bind failures retain their existing same-owner custody and exact outcomes.

## New control definition, not an execution result

`native_first_actual_registered_cancelled_closed_drain_resumes_same_installed_owner`
uses the existing real five-object TCP/TLS FIRST fixture and its real target
actor. The test-only one-shot callback is selected on that actual actor thread
after production has ended Closed life. It checks the original finite caller,
records its positive remaining time, and cancels that same caller. The real
`stop_and_drain` is still called and must return typed `QueryCanceled`; at
this exact cut its first cancellation check precedes `request_shutdown`.
This control does not claim to have observed a partially completed socket
shutdown or separately sampled live Role/OWN windows at that cut.

The facade records the actual failed Policy API result, actual drain API
error, retained Assigned listener, ended life and exact Installed
record/coordinator/open before assertions. A distinct real owner command
must resume the same native attempt through genuine drain and transfer into
Joined, leave APP closed, and return the first incomplete Policy transition.
It compares the original Installed evidence and actual native XID/LSN,
CURRENT/archive and bridge records, preserves the first old errors in both
helper and outer histories, and requires an actual Joined replacement socket
with original listeners absent. Separate memory/storage snapshots are
explicitly non-atomic DATA, not a grant or cold recovery proof.

The new test and the previously frozen successor-read test also join the
actual target worker's finite Serving poll Case list. The latter omission
was a source-discovered reachability gap in `70997`; all historical sources
remain immutable. Existing test function bodies, API windows and SDK
ACK/full-Strong assertions are preserved. The new component returns before
the full SDK tail and makes no claim that it ran that tail.

## Remaining boundaries

Compilation, runtime, kernel drain/join timing and actual acceptance remain
unverified. The ordinary drain joins and kernel I/O are not hard preemptible.
This child does not supply the separate bounded native leadership wiring,
automatic controller, cleanup/slot reclamation, cold reconstruction, repeated
relocation, physical RPO or capacity qualification. The eight-phase retained
custody cap remains unchanged. Original local quorum and authority APIs are
untouched.
