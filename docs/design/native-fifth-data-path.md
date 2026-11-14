# Registered fifth member: first positive DATA path

Scope of this document: one range moving from an original three-voter set to a
set that contains a genuine registered fifth member, in a five-host fixture
that runs in one process. It states what is exercised, how a failure is
handled, what is new source, and what is still missing. It is not a physical
four-to-five qualification, an automatic controller, or a cross-process
result.

## What the path does

For one range with voters `{L, S1, S2}` (leader `L`) among four original
members, and a fifth member `T` that registered, caught up, was promoted to
metadata voter and opened its replica-only receiver:

1. **Target admission.** `T` installs the ordinary host writer policy from a
   fresh host metadata observation and replaces its replica-only socket with a
   DATA socket. Application ingress stays closed. If this fails, nothing about
   any range has changed.
2. **Learner allocation.** The metadata leader commits `T` as a learner of the
   range (new range epoch) with a relocation that retires `L`.
3. **Source cut (freeze).** Each old voter records its closure through the
   existing old-step producer; the collector selects the cut `R/H0/H`.
4. **Transfer and verification.** The selected source captures its image; the
   complete `0..R` history is staged and audited on `T`, the range is cleared
   and copied, the target verifies the full contents against the source seal
   and finalizes its catalogue. The checks are the existing ones: history
   commitment, row audit, native receipt and catalogue adoption.
5. **Joint decision.** The new set `{S1, S2, T}` materializes the same
   `R/H0/H`; the complete decision is published on an old and a new majority.
6. **Membership change.** The three staged metadata transitions (certified,
   union while frozen, final membership awaiting activation) are built from the
   durable decision and committed by the real metadata consensus. `T` becomes a
   voter and the range leader; `L` is no longer a replica.
7. **Policy continuation.** Every owner installs the host writer policy of the
   new committed configuration.
8. **Open and release (unfreeze).** Serving preparation, bridge publication,
   catalogue activation and release run on the complete new member set through
   the existing bounded fresh phases.
9. **Serving.** `T` recovers leadership of the range and publishes application
   and Strong ingress.

Steps 3 to 6 and 8 reuse the existing joint handoff objects unchanged. No
fence, quorum rule, deadline or refusal in that machinery is modified.

## Failure handling: abort before the source cut, resume after it

The boundary is the first durable handoff promise on an old voter.

- A source close (step 3) records a permanent handoff promise on the old
  voter and advances that voter's durable range epoch to the activation
  epoch. Nothing revokes it. From that point the only continuation is to
  finish the same handoff.
- Before it, the committed learner allocation (step 2) is the only change.
  No old voter and not the target holds durable state of the new range
  epoch; the target holds a writer policy that does not cover the range and
  no image.

**Abort (steps 1 and 2).** `NativeJoinedDataHandoff::abort_before_cut` commits
the original range descriptor again through the real metadata consensus: the
original voters at the epoch the allocation expected, the learner replica and
the relocation removed. The original owners' installed policy, recovered
leadership and native write configuration are those of exactly that
descriptor, so nothing is re-enrolled, nothing is recovered and no permission
is created; writes are acknowledged by the original voters again as soon as
the commit is applied. A later run of the driver starts a new learner
allocation with a new relocation and session.

The abort is refused, and the handoff can only be resumed, when

- this driver started a source round, or
- the committed relocation is past its bare learner allocation (the metadata
  consensus itself refuses to erase a relocation that carries a joint
  decision), or
- any old voter's durable range log is already beyond the expected epoch.
  Every old voter is read; an old voter that cannot be read refuses the
  abort.

The last check is the durable truth and does not depend on the driver's
memory. It matters because this abort removes the committed relocation: on a
range with a recorded promise that would leave the promise without its
handoff.

Why the abort does not keep the allocation epoch. Keeping the range at the
allocation epoch and re-enrolling the original owners there was tried first.
The leader recovers leadership at that epoch, but the first write is refused
by both followers with "native ordinary status needs actual current
same-voter/configuration proof": a follower accepts ordinary replication at a
learner-allocation epoch only from a role that carries the committed
relocation, and the ordinary role never does. The refused write also leaves a
prepared entry on the leader that generic recovery will not resolve. Making
followers accept ordinary replication at that epoch would change a native
refusal, so it is not done here.

**Resume (steps 3 to 9).** The driver is run again. Each run derives its
position from the committed metadata (the relocation phase and the staged
joint record) and from the custody it retains, and continues from there:

- Source cut and transfer. A retained closure is bound to its original
  issuance window and cannot enter a new round, and a capture is single-use.
  Every run therefore releases the hosts' retained closures, closes the old
  voters again with fresh roles and takes a new capture. Closing again
  confirms the same durable promise and native close record; a different
  session is refused.
- Target install. A transfer that already modified the target cannot be
  returned as untouched or adopted. `RangeRecoveryRuntime::
  restart_dropped_image_transfer` restarts the dropped transfer of the same
  retained recovery session under a new attempt. The dropped attempt's freeze
  remains the only exclusion owner and is rotated to the new attempt under the
  same checks as the first transfer; the store requires the same handoff
  lineage and refuses to supersede a completed image. The new installer starts
  its bounded clear and copy again, which replaces the partial Installing
  state.
- Decision. The publication round is retained once it exists; a later run
  publishes only the members still missing.
- Membership stages. Each of the three commits is taken only when the
  committed relocation records the preceding stage, so a repeated run skips
  the stages already committed.
- Open, release and serving. Every finished part (policy continuation,
  preparation, publication, each member's Open, the release continuation,
  each member's release) is retained and skipped by a later run.
- A run after completion returns immediately and changes nothing.

**Custody.** The joint machinery issues live owners that cannot be decoded
from disk: the range freezes, the closed old quorum, the finalized target
image, the durable decision and the serving continuation. The driver object
retains them between runs. "Resume" therefore means running the same driver
object again in the process that holds it; a run that failed or returned
between stages is covered. Losing that process is a cold restart of the
collector and is not covered.

## New source

| Location | What it adds |
| --- | --- |
| `distribution_native_host/joined_data_handoff.rs` | `NativeJoinedDataHandoff`: the driver of steps 1 to 9 for one range, with `drive`, `drive_until` (stop after a named stage) and `abort_before_cut`. It sequences the existing joint objects and retains their custody between runs. The hosts are reached through `NativeJoinedDataHandoffHosts`, whose methods are existing host actions. It has no production caller yet. |
| `distribution_native_host/provisional_listener/registration/joined_data.rs` | `NativeJoinedDataHost`: the DATA owner of a registered member without initial ranges. `open_joined_data` installs the host policy and binds the DATA socket, `refresh_joined_data_policy` re-enrolls the same coordinator after the committed configuration changed, `publish_joined_data` recovers led ranges and opens application and Strong ingress, `joined_data_handoff_parts` hands the owner's own recovery runtime and DATA transport to the driver. It refuses a member that has initial ranges or another DATA owner. |
| `distribution_range_recovery/runtime.rs`, `restart_dropped_image_transfer` | Restarts a dropped image transfer of the same retained recovery session under a new attempt (see "Resume"). It cannot return the freeze to its session and grants no rows, serving or recovery authority. |
| `registration/initial_data.rs`, `refresh_data_policy` | An initial owner re-enrolls its retained coordinator at the current committed configuration. The metadata-only registry refresh still refuses any DATA change; this is a separate complete policy publication. Application ingress is closed across the replacement. It is refused once the process-owned maintenance worker is bound. |
| `registration/initial_data.rs`, `forget_old_closure` | Releases the host's retained custody of one old closure. Durable promise, native close record and joint state are untouched. |
| `NativeInitialDataAdmission` | The retained admission's writer reference can be replaced by its own owner after a policy publication, and can be constructed for a joined owner (no image-source registry, no initial genesis). Every existing check on it is unchanged. |

The joined owner reuses the immutable initial-DATA listener mode, so its
request allowlist, application gating and Strong dispatch are the existing
ones. The registered receiver and assigned-closed modes are not changed.

## What is exercised

`fifth_data_path_tests.rs` (module `registered_image_baseline`) starts four
initial owners and one registered fifth member with real mutual-TLS sockets
and drives every case through `NativeJoinedDataHandoff`. The fixture
implements the host actions; the planning view of the metadata stages and the
wait for a committed configuration are fixture wiring.

- `native_fifth_data_path_receives_image_becomes_voter_and_serves_after_one_original_stops`
  writes rows acknowledged by the four-member cluster, runs steps 1 to 9, and
  then requires: the pre-change rows are present on `T` after verification;
  Strong reads of those rows succeed with `T` as the only leader of the range;
  a new write is acknowledged, is on `T` and both surviving originals, and is
  absent on the retired source; after one surviving original is stopped, a
  further write is acknowledged, is on `T` and the remaining original, absent
  on the stopped one, and every acknowledged row is still Strong-readable.
  These requirements are the "acceptance" used by the cases below.
- `native_fifth_data_path_unavailable_target_leaves_range_writable_on_original_four`
  stops the fifth member first. Step 1 refuses, the committed configuration is
  unchanged, and a write and Strong reads on the four originals succeed.
- `native_fifth_data_path_target_stopped_after_allocation_aborts_to_original_voters`
  commits the learner allocation, shows that a write is then refused, stops
  the fifth member and aborts. The committed range is the original descriptor
  again, no relocation is left, new writes are acknowledged on a majority of
  the original voters and every acknowledged row is Strong-readable.
- `native_fifth_data_path_abort_then_new_handoff_completes` aborts with the
  fifth member running, writes on the original voters, and runs the driver
  again: a new relocation completes with the acceptance, including the rows
  written after the abort.
- `native_fifth_data_path_transfer_failure_after_source_cut_resumes_to_completion`
  fails the transfer after the source cut and the first copied step. In that
  state every acknowledged row is on every old voter, a write is refused and
  the abort is refused. Running the driver again closes the source again,
  replaces the target's partial install and completes with the acceptance.
- `native_fifth_data_path_interrupted_driver_reruns_to_completion` lets the
  driver return after the joint decision, after the certification and after
  the union membership, checks each time that the range refuses writes and
  keeps every acknowledged row, runs it again to completion with the
  acceptance, and checks that one more run changes nothing.

## What is not delivered

- **Unwritable interval.** From the learner allocation (step 2) to the end of
  step 9, or to the abort, the range does not acknowledge writes. The abort
  and the resume end that interval; they do not shorten it.
- **Custody loss.** Resume needs the driver object that holds the live joint
  owners. A lost collector process, and a restart of any participant in the
  middle of the path, are not covered. Two narrower cases inside one process
  are also not covered: if the decision cannot be prepared after the
  finalized image was handed to it, the image custody is gone and a completed
  image cannot be shipped again; and a failure while the durable decision is
  handed to the serving preparation loses the decision custody. Both leave
  the range safe and unwritable.
- **All old voters.** The source round requires every old voter to answer,
  not only a majority, and the abort requires every old voter's durable
  progress.
- **Abort of a partially cut range.** There is none by design. If some but not
  all old voters hold the promise, the handoff must be resumed.
- **In-process orchestration.** The source capture, the old and new rounds,
  the decision and the phase rounds are local objects sequenced by the driver
  in one process. The authenticated remote source stream and remote old
  collector exist only as separate, unfinished work and are not used here.
  The driver has no production caller: there is no command-driven or
  automatic controller.
- **Process maintenance.** The initial owner's policy change is refused when
  the process-owned hot maintenance worker is bound, and that worker follows
  the initial range descriptors only. A range that changed configuration is
  therefore not covered by hot origin repair after this path.
- **One range, hot only.** A second range on the same already-serving fifth
  member, cold restart of any participant during or after the path, source
  cleanup on the retired member, and any capacity or timing claim are not
  exercised.
- **Timing.** A joint phase revalidates every final member inside one metadata
  observation window, and that window is bounded by the per-request network
  budget. The fixture uses the same four-second budget as the three-member
  joint fixture; with the 1.5-second budget of the other host fixtures the
  three-member aggregation did not fit on a hosted runner.
