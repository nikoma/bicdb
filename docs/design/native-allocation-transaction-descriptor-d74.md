# Allocation transaction descriptor regression at d74

Inspected source: `d74d363addb35b5b97308b1afcc1c2231e021d29`,
1.0.520-beta. The automatic native job
[133048283864](https://github.com/nikoma/bicdb/actions/runs/43790716008/job/133048283864)
failed 397/93/1. The original execution remains failed. This correction is
source-only, uncompiled and unexecuted; it is not an HA qualification result.

## Observed failure and source contradiction

Eight allocation-family scenarios failed. Four activation-domain scenarios
failed in their prerequisite's selected-range SDK write before any new domain
comparison ran; three ordinary allocation scenarios failed the same SDK tail.
The generic-marker scenario reached its blocked write but got a descriptor
mismatch before its required native-readiness error. Their intended later
guarantees are not demonstrated by those failures.

In the unchanged normal allocation/SDK scenario, the original log's
`NATIVE_WRITER_RANGE_POLICY_MISMATCH` entries at raw lines 667642–667645 show
node n1 with installed epoch 2 and a requested epoch 1 for range 1. Earlier
entries show the opposite direction while the allocation was still advancing.
Those are distinct states, not proof that every error is a lagging follower.
Full original raw SHA256:
`0fb6aca4e8deb61f9d3fe1ff331d30041e19f4fefe7650bf9ff29f3d2e781f4f`.

[Transaction preflight](../../crates/bicdb-core/src/db/range_image_native/writer_enrollment.rs)
selects a mutation's range from the original installed policy. A verified joint
Serving domain already overrides that historical descriptor. A successful
SAME-State allocation audit also advances the effective descriptor, but has no
joint Serving domain. The old fallback therefore passes epoch 1 to
`require_database`, whose current verified allocation policy correctly rejects
it because it now requires epoch 2. This is a source-level contradiction aligned
with the observed error, not a demonstrated explanation for the other failures.

## Narrow correction and retained checks

Keep the existing original range/token selection and joint Serving path. In the
remaining path, select that same range ID from the already verified registry
extension when present, release its read guard, verify token containment, and
run the original `require_database` check on that descriptor. No registry record
is decoded into permission, persisted, reconciled or republished here.

The pending-publication refusal, original DB/open/hook identity, exact current
and archived native audit chain, committed allocation transaction checks and
the coordinator's independent native-readiness requirements remain unchanged.
An audit supplies policy identity; it does not supply recovered state, a read
barrier or permission to acknowledge a write. Concurrent policy advancement can
still refuse an obsolete selection through the original validation.

## Next smallest deterministic verification

Run the unchanged existing allocation-family selectors on the corrected exact
head in the normal automatic native CI job:

- `native_initial_live_allocation_preserves_state_worker_and_real_sdk_strong`
- `native_initial_live_allocation_pre_io_cancel_retains_original_candidate`
- `native_initial_live_allocation_known_native_postcheck_cancel_reconciles_without_replay`
- `native_initial_live_allocation_actual_generic_epoch_marker_cannot_replace_native_serving`
- The four existing `native_allocation_actual_activation_*` cases.

Preserve all original selected/untouched APP and full Strong assertions, exact
negative error oracles, caller budgets, native points and original operation
outcomes. The generic-marker case must reach its actual native-readiness refusal;
the four domain cases must reach their actual comparison cuts. A passing setup
or a different early rejection is insufficient. Also retain the existing
Unknown and publication-health cases. No local Rust tests or database execution
were performed for this source correction, and no new CI run was dispatched.
