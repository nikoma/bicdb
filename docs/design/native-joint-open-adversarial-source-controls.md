# Joint open adversarial controls (source only)

This test-only successor starts at `3008f6acd0e4c55b2c95072e5e9eac851ad9d019`.
The earlier scoped-open execution result remains its own cohort: run
43215307947/job131172932586 passed one test in 29.76 seconds, with all-target
compilation passing. That result does not execute the controls added here.
No local Cargo, remote test execution, release, next ACK or Strong-read result
is claimed by this source checkpoint.

The controls reuse the genuine enforced four-metadata-member/RF3 joint fixture:
actual native origin, actual image target, actual old/new data proofs and
protected native publications on every final member. The old preparation must
already be expired. Every new open phase obtains actual fresh per-node TLS Host
MQ observations for the complete three-member final cohort and independent
post-quorum local MQ. The original four-second phase cap is unchanged. At most
three separate, genuinely fresh attempts may reach a cut; original proof tokens
are never renewed. Existing complete-joint ACK/Strong assertions and existing
all-three catalogue-open control are unchanged.

| Filter (all under `open_adversarial_tests`) | Actual cut and required outcome |
| --- | --- |
| `native_joint_open_final_sweep_foreign_actual_writer_reenrollment_refuses_old_phase` | After the last foreign sweep and before acquiring the local native journal, install a genuine separately acquired, sealed host-writer enrollment on foreign member n1. This replaces the real registration while all range owners remain Frozen. The captured old phase must reject the writer lifetime/generation change before modifying n3's catalogue. Another fresh MQ phase cannot hide the changed registration. |
| `native_joint_open_post_fsync_step_cancel_recovers_exact_fact_with_original_caller_live` | After successful actual catalogue fsync, uncertainty clearing and Controlled drop, cancel only the persistence step's child token. No live receipt may escape that call. The original caller stays live. A new complete real phase must recover the exact durable activation without rewriting the catalogue bytes. |
| `native_joint_open_post_fsync_original_caller_cancel_cannot_start_fresh_phase` | At that same actual successful fsync cut, cancel the retained original caller. Persistence returns cancellation; beginning another phase of that same operation must also return cancellation. The durable closed activation and all Frozen owners remain. |

Each control checks all three native points before and after the cut: actual
range index, target-local MVCC bound B, local history generation, H0/H and native
receipt SHA (which binds the physical anchor/namespace mapping). All three gates
must still refuse ordinary entry. These checks make no activated/open/serving
claim. The first control exercises writer re-enrollment, not fabricated epoch or
generation setters and not replacement of an actual DB open.

The two injection locations in `joint_handoff_state/activation/fresh.rs` are
`cfg(test)` only. Callbacks cannot construct admissions or alter proof fields.
Their scoped wrappers clear both thread-local one-shot slots on early errors,
timeouts and unwinding, and remove the callback from its RefCell before running
it. No callback does network IO: foreign actual MQ/enrollment preparation occurs
before the local service/DB acquisition. The foreign callback invokes only the
real local enrollment producer; the fsync callback reads the actual durable
catalogue and cancels a real token. The production checks following each cut are
unchanged.

A separate operation or all-owner cold controller after original cancellation
still needs its own proper producer. This test does not add one. Missing-middle
or altered-history cold-copy controls, partial release/open-generation recovery,
new ACK and Strong-read acceptance remain outside this checkpoint. Repository
formatting, diff and version consistency checks are the only local validation;
focused remote execution is required before any dynamic claim.
