//! Four passive-observer counterparts of the retained6d four-case selection.
//! Run this module with the original --test-threads=4. No synthetic start
//! barrier, Strong warmup, manual maintenance, new owner or extra deadline.
use super::*;

fn observed(case: SourceCase) {
    run_registered_image_baseline_with_observer(
        Case::Required,
        AssignmentMode::OriginalLeader,
        Some(case),
        true,
    );
}
#[test]
fn native_registered_fifth_remote_transfer_unique_runtime_owner_preserves_data() {
    observed(SourceCase::TargetTransfer);
}
#[test]
fn native_registered_fifth_remote_transfer_actual_callback_arc_blocks_then_fresh_retry() {
    observed(SourceCase::TargetTransferBusy);
}
#[test]
fn native_registered_fifth_remote_transfer_actual_cancellation_before_rotation_preserves_n() {
    observed(SourceCase::TargetTransferCanceled);
}
#[test]
fn native_registered_fifth_remote_transfer_actual_hook_aba_before_rotation_preserves_n() {
    observed(SourceCase::TargetTransferHookRevoked);
}
