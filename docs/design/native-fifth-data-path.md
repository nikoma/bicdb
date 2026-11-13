# Registered fifth member: first positive DATA path

Scope of this document: one range moving from an original three-voter set to a
set that contains a genuine registered fifth member, in a five-host fixture
that runs in one process. It states what is exercised, what is new source, and
what is still missing. It is not a physical four-to-five qualification, an
automatic controller, or a cross-process result.

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

## New source

| Location | What it adds |
| --- | --- |
| `distribution_native_host/provisional_listener/registration/joined_data.rs` | `NativeJoinedDataHost`: the DATA owner of a registered member without initial ranges. `open_joined_data` installs the host policy and binds the DATA socket, `refresh_joined_data_policy` re-enrolls the same coordinator after the committed configuration changed, `publish_joined_data` recovers led ranges and opens application and Strong ingress. It refuses a member that has initial ranges or another DATA owner. |
| `registration/initial_data.rs`, `refresh_data_policy` | An initial owner re-enrolls its retained coordinator at the current committed configuration. The metadata-only registry refresh still refuses any DATA change; this is a separate complete policy publication. Application ingress is closed across the replacement. It is refused once the process-owned maintenance worker is bound. |
| `registration/initial_data.rs`, `forget_old_closure` | Releases the host's retained custody of one old closure after its collector finished. Durable promise, native close record and joint state are untouched. |
| `NativeInitialDataAdmission` | The retained admission's writer reference can be replaced by its own owner after a policy publication, and can be constructed for a joined owner (no image-source registry, no initial genesis). Every existing check on it is unchanged. |

The joined owner reuses the immutable initial-DATA listener mode, so its
request allowlist, application gating and Strong dispatch are the existing
ones. The registered receiver and assigned-closed modes are not changed.

## What is exercised

`fifth_data_path_tests.rs` (module `registered_image_baseline`) starts four
initial owners and one registered fifth member with real mutual-TLS sockets:

- `native_fifth_data_path_receives_image_becomes_voter_and_serves_after_one_original_stops`
  writes rows acknowledged by the four-member cluster, runs steps 1 to 9, and
  then requires: the pre-change rows are present on `T` after verification;
  Strong reads of those rows succeed with `T` as the only leader of the range;
  a new write is acknowledged, is on `T` and both surviving originals, and is
  absent on the retired source; after one surviving original is stopped, a
  further write is acknowledged, is on `T` and the remaining original, absent
  on the stopped one, and every acknowledged row is still Strong-readable.
- `native_fifth_data_path_unavailable_target_leaves_range_writable_on_original_four`
  stops the fifth member first. Step 1 refuses, the committed configuration is
  unchanged, and a write and Strong reads on the four originals succeed.
- `native_fifth_data_path_transfer_failure_after_source_cut_characterization`
  fails the transfer after the first copied step. It asserts only safety
  (every acknowledged row remains on every old voter; an acknowledgement, if
  returned, is on an old majority) and prints what the old configuration
  accepts. It is a characterization, not a success claim.

## What is not delivered

- **No abort after the learner allocation.** From step 2 the range is not
  writable until step 9 completes: the original owners' installed policy does
  not cover the new range epoch, and after step 3 each old voter holds a
  permanent promise. A failure in between is safe but leaves the range
  unavailable; the only continuation is to finish the same handoff. Resuming
  an interrupted transfer on the target is not implemented. This is the
  opposite of "unfreeze on the old configuration" and remains open.
- **In-process orchestration.** The source capture, the old and new rounds,
  the decision and the phase rounds are local objects driven by the test. The
  authenticated remote source stream and remote old collector exist only as
  separate, unfinished work and are not used here. There is no command-driven
  or automatic controller.
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
