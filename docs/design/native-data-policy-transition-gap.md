# DATA policy transition: component prerequisite versus operational handoff

Read-only audit of `aac6192c2b034e1f2e4c88a16fd84bcd94e0bf6b`.
No code, test execution, safety exploit or qualification is claimed. Paths below
are relative to `crates/bicdb-core/src/`. This refines the existing
[remote image contract](native-remote-image-session-contract.md) and
[retained metadata registry contract](native-retained-writer-registry-refresh-contract.md).

## What the full local fixture actually supplies

`db/range_image_native/rf3_fixture/native_historical_tls/`
`native_joint_old_closure_tests/complete_joint_tests.rs::refresh_hosts` (line 31)
obtains a genuine fresh per-node HostMQ, prepares actual native host enrollment,
calls `install_enrolled_native_coordinator` on the existing coordinator, and
replaces each entry in the fixture's host-guard map. Its call at line 549 follows
actual protected OLD/NEW joint decision publication and all three metadata stage
commits, and precedes NEW serving preparation/publication/open/release.

This uses real storage and authority producers, not fabricated guards. However,
it is an explicit low-level prerequisite injected by the fixture, not a call
through a production handoff controller or retained INITIAL DATA host lifecycle.
The nodes retain the same DB/Store opens, native birth, actual data and coordinator
Arcs, but the installed writer StateArc and registration generation are NEW.

`db/range_image_native/writer_enrollment.rs::install_enrolled_native_coordinator`
creates a new registration, deactivates the prior one before native policy WAL,
persists the new original policy, installs a new writer StateArc and atomically
publishes the hook. It consumes actual fresh enrollment; it does NOT consume a
`NativeJointDurableDecision`. It grants necessary installed-policy continuity,
not DATA Ready, a native membership bridge, recovery or Strong permission.
The independent native/joint checks remain required. This audit does not infer
an unsafe ACK path from that necessary-policy API alone.

## The exact original-writer window

`distribution_data/native_joint_handoff/bridge.rs::prepare_local_native_joint_serving`
requires an installed writer for the activation range, captures that writer BEFORE
native preparation/WAL, rechecks it and returns actual Prepared evidence retaining
the Arc. At this point in the full fixture, `refresh_hosts` has already run.
Fresh-phase, activation-capsule and release checks preserve THIS original prepared
writer, including pointer/generation/root/open checks; hot reinstallation cannot
be hidden by a fresh MQ. CurrentJoint recovery similarly retains its own original
operation writer. These properties do not prove that the first from-birth writer
survived the preceding DATA policy transition unchanged.

Preserving a registration forever is not the requirement. A legitimate typed
handoff may rotate it once the old domain is durably fenced and the actual
OLD/NEW decision/current authority authorize transition. Such a producer must
coordinate the new ingress/admission bindings, keep all native/history/ACK/UID
obligations, and leave errors closed. It cannot retag an already-issued Prepared
or release capsule with the replacement writer or revive its old lease.

## Production boundary still missing

`NativeWriterHostState::require_range` and `require_command` consult the immutable
original policy's DATA ranges. `HostWriterPolicyFacts::require_range` compares
the exact canonical range; command checks include original epoch and leader.
The new retained metadata-registry refresh deliberately requires `self.ranges ==
previous.ranges`. It does not authorize DATA leader, epoch, voter or placement
changes. Do not remove these refusals to make relocation pass.

`distribution_native_host/provisional_listener/metadata_startup.rs` enrolls the
initial writer. `registration/initial_data.rs::NativeInitialDataAdmission` retains
its original writer, current service-slot guard, runtime/open and ingress lifetime;
its `require_live` follows that original registration. The process maintenance tick
can refresh metadata-only registry facts on it. No typed operational DATA-policy
transition was found that consumes the real joint decision and updates these
retained host/dispatcher/admission owners. Calling the ordinary installer behind
their backs revokes their old registration; replacing a fixture map does not
update those production owners.

Actual native bridge/NEW materialization and old-fence producers exist as local
opaque components. They are necessary inputs, not the missing lifecycle publisher.
A fresh metadata membership map alone cannot select a new DATA domain. A genuine
fifth registered learner additionally lacks the full INITIAL DATA owner that the
four-member local fixture already has. Its receiver-to-DATA transition needs a
separate actual installation/ingress producer, without granting DATA from metadata
promotion or an unexposed learner role alone.

## Smallest real next regression and transition contract

Keep every existing full component assertion and failed historical result. Add a
separate typed continuation that reaches the genuine protected joint decision and
actual metadata transition with all original host owners still retained, but does
NOT call fixture-only `refresh_hosts` or substitute a host-guard map. Attempt the
real retained-host transition path. Record the precise refusal and exact original
policy/registration/coordinator/open identities, retained fences/promises, R/H,
actual per-node B/UID and complete prior ACK ledger before positive acceptance.
The baseline must fail for the missing path rather than silently fall back to
the low-level installer. The existing EnforcedNetwork owns guard maps, not real
`NativeInitialDataHost` ingress owners; a no-refresh control there establishes
only the necessary-policy refusal. Operational acceptance must retain the real
INITIAL host/dispatcher/admission owners as well. No fabricated permission or
relaxed range check.

The implementation acceptance must use an actual pending handoff, protected
quorum-durable OLD/NEW decision, exact local old closure or finalized target image,
independently fresh post-decision metadata authority and actual current owner.
A pre-serving policy transition may be necessary before NEW Prepared can be
created: it is still a closed prerequisite, not permission to write early.
Choose and prove either a typed effective-policy extension on the original owner
or an explicitly authorized registration rotation with atomic retained-owner
retargeting. The latter must keep old ingress closed and never clear a foreign
hook. A crash/timeout after policy WAL but before lifecycle publication must stay
closed and reconcile exact stored facts under new authority, not rebuild from JSON.

Require no success from metadata alone, wrong joint decision/local closure/image,
uncovered Commit/CQ, changed birth/open/slot/writer or stale epoch. Cut before/after
policy persistence and ingress publication, and preserve old Prepared/capsule
replacement negatives. Finish all mandatory NEW copies/leader, actual target-leader
ACK and Strong old/new full values, stale-old-writer refusal and authorized cleanup.
Then drive the same transition through real TLS/operator commands with a genuine
fifth process and the unchanged ordinary client; no direct child Arcs or proposal
injection. Native image/joint wire, automatic control, cold recovery, online copy
and the full 8 GiB independent-machine fault qualification remain separate gates.
