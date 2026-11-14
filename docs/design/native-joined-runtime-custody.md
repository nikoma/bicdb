# Joined original N-to-A runtime custody

Source implementation only. No compilation, runtime result, complete receive,
activation/Serving, cold recovery or physical qualification is established here.

The existing Joined host supplies its actual selected preflight, finite paired
Role/OWN/member request and authenticated original source frame. The runtime
requires its actual durable-fence session and reserves only that range. It does
not manufacture a selected refusal, native floor, source proof or writer.

Physical preparation holds the actual service/Store/DB/hook/journal outside the
runtime session latch. The helper consumes the temporary N Arc and drops it
before the callback. Its final point retains the actual lexical guards and a
private non-owning gate/lease identity. The callback checks current membership,
original owners and the real unique N, then rotates that same permit to A.

The actual custody cell and map node are allocated before the short version
publication guard. An empty cell never grants image operations. On a known
pre-rotation error, only the still-empty cell is removed; the original N stays
in the original session. The successful take/set/map retention performs no
filesystem/native reads, network calls or new native transaction. Native/paged
WAL and catalogue publication belong to the separate owned engine.

The runtime strongly retains the original preflight and actual A. A host view
cannot clone/extract a raw permit, thaw A or rotate it back on Drop. If that view
is lost, a fresh original request can reobtain only the same exact custody when
the runtime is its sole owner; it cannot replace A/preflight or return a second
view while the original host still owns one. A stopped empty-cell preparation
can be retried only while the same original N/session is still present.

There must be no Runtime-to-custody-to-preflight-to-Runtime ownership cycle.
The Joined origin therefore uses weak actual host/backend/factory/runtime/
coordinator/first-owner/admission edges plus thin open/slot and writer witnesses.
Full upgrade/physical checks occur outside the short publication guard. A weak
identity does not reconstruct authority; ended owners refuse. The source
implementation of that ownership correction is2fc0af4905c2f809203d04b2283e36dfe47719c9.

`NativeImageInstallFreeze::JoinedRemote` is distinct from Registered, recovery
and standalone exclusion. The generic image install and legacy release paths
explicitly refuse it. No activation/release consumer is added by this custody
slice. A genuine Joined image/base/catalogue and fresh joint release admission
must be implemented before removal of A; then the existing same-State selected
CLOSED/Serving extension can admit only the new range.

Required next evidence: actual source/target wire fixture reaching Joined with
one populated serving range, then genuine N-to-A of another populated range;
original callbacks must drain, retained cursor identity must not pin the permit
Arc, metadata/owner changes must refuse at the final point, post-transfer errors
must retain the same A, and dropped-view reconciliation must not create N or a
new attempt. Existing primitive identity tests do not exercise this runtime
transfer. Existing15-case Registered S2 passes do not qualify this Joined path.
The first range's acknowledged values, Strong contract, native WAL and indexes
must remain valid throughout. Actual disk-stall and cold/process-restart gates
remain separate and open.

## Source review correction: liveness at the actual gate point

The initial untested source a24af4d8fe3e2b79fe4b0a1b9ef91282a68f9b3b checked
liveness before `try_rotate_owner`, but that method waits on the gate mutex.
A held actual gate could therefore cross the original deadline before owner
mutation. The same issue affected a reacquired view after blocking validation.
This is a source-confirmed contract gap, not an observed data-loss finding or
an executed runtime failure. The source review is not described as qualified.

The correction acquires the actual gate publication borrow with `try_lock`
before final request/frame/Role/OWN/version checks. Point identity validation
uses that held gate without recursive acquisition; owner publication is an
infallible memory update. Reacquisition similarly holds an actual current-gate
borrow through final checks and exact custody cloning. No original deadline or
permit is renewed. A busy latch refuses without waiting or changing ownership.
Primitive and complete Joined controls remain required before qualification.
