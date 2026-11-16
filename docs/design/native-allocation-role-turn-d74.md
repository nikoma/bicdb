# Allocation recovery: operation authority and retained job progress

Source: `d74d363addb35b5b97308b1afcc1c2231e021d29`, 1.0.520-beta.
This is a source correction awaiting independent review and execution. The
original native run remains failed; no latency, recovery or convergence gate
has passed because of this change.

## Evidence and source boundary

Fifty-four FIRST scenarios stopped at `actual_post_placement_registry_refresh`,
before their FIRST factory and intended later fault. Their errors comprise 43
query timeouts, eight active-relocation registry refusals and three unresolved
writer-policy publications. Those are distinct outcomes; this correction does
not establish a common cause or resolve all of them.

In `native_first_actual_released_owners_to_serving_original_sdk_ack_full_strong`,
the actual registry/maintenance call returned a timeout at raw line 299054.
Cleanup diagnostics at raw line 299064 retained an allocation job in Activate,
position 2, whose previous n3 Activate call had returned success. This proves
partial work occurred; it does not prove loss, a missed durable operation or the
reason for every timeout. Full original raw SHA256:
`0fb6aca4e8deb61f9d3fe1ff331d30041e19f4fefe7650bf9ff29f3d2e781f4f`.

[Allocation recovery](../../crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/initial_data/hot_recovery/allocation_policy.rs)
formerly bound its complete multi-member Fence/Prepare/Complete/Activate/Publish
loop to the first Role's deadline. Even its later fresh Role requests inherited
that earlier deadline. The original caller's separate five-second turn could
still have time remaining. The source also advanced a successful recipient's
cursor only after outer cancellation/currentness checks; a late failure could
therefore leave an already successful operation at the cursor. The latter is a
source defect, not an injected late-failure result from this run.

## Correction and remaining obligations

Each distinct recipient operation now obtains fresh actual authority and checks
the same retained session/nonce inside the unchanged original caller deadline.
The current Role also guards phase transition or final publication. No earlier
RPC deadline is renewed, and no new process/job/session is constructed on expiry.
After an actual successful response and original-peer-open validation, retain
cursor progress before late outer checks. Fresh final native/data-quorum and
metadata publication checks remain separate.

An error does not advance the cursor or become success. This change does not
claim to implement all uncertain-outcome recovery; original protocol restrictions
still apply. Nor does it relax metadata-only stable-audit refusal during active
relocation. Those failures need their own exact causal trace.

The next deterministic control must cancel an original turn immediately after a
genuine validated recipient success, prove the retained cursor and original
peer identity survive, then continue the next operation through fresh authority
without repeating that success. A shorter original caller deadline must remain
the outer bound. Run the unchanged FIRST/allocation assertions after the batch,
including actual selected/untouched ACK and full Strong checks. Such controls
must execute on the corrected exact source before any recovery claim is made.
