//! Actual empty recovery lookup is not a publication grant. The original
//! admission is canceled at that real cut; new publication must still refuse.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn native_registered_fifth_activation_actual_absence_then_cancel_refuses_before_io() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetActivationAbsentThenCanceled),
    );
}

pub(in super::super) fn absent_then_canceled(
    published: &Arc<NativeJointPublishedServingQuorum>,
    local: &ClusterNodeId,
    admission: &NativeJointOpenAdmission,
    database: &Arc<parking_lot::RwLock<BicDb>>,
    process: &CancellationToken,
    output: &std::path::Path,
) {
    let root = database.read().data_path().to_path_buf();
    let before = published.test_owned_activation_state(local).unwrap();
    let fired = Arc::new(AtomicUsize::new(0));
    let actual_cut = Arc::new(parking_lot::Mutex::new(None::<serde_json::Value>));
    let token = admission.cancellation().clone();
    let cut_count = fired.clone();
    let cut_data = actual_cut.clone();
    let fault = bicdb_page::test_faults::arm_io_failure(
        "native_joint_activation.catalogue.rename",
        &root,
        1,
        false,
    );
    let actual =
        crate::distribution_range_consensus::RangeWriteStore::test_joint_open_after_actual_absence(
            move || {
                token.check()?;
                let remaining = token
                    .deadline()
                    .and_then(|d| d.checked_duration_since(std::time::Instant::now()))
                    .map(|d| d.as_micros());
                *cut_data.lock() = Some(serde_json::json!({
                    "same_thread_actual_no_activation_or_recovery_owner_branch":true,
                    "original_admission_remaining_us_before_cancel":remaining,
                    "original_admission_live_before_cancel":true,
                }));
                cut_count.fetch_add(1, Ordering::SeqCst);
                token.cancel();
                Ok(())
            },
            || admission.persist_local_catalogue_activation(process),
        );
    let io_fired = fault.fired();
    drop(fault);
    let after = published.test_owned_activation_state(local);
    let observed = serde_json::json!({
        "stage":"actual_absence_then_original_cancellation_before_new_publication",
        "original_returned_ok":actual.is_ok(),
        "original_error":actual.as_ref().err().map(ToString::to_string),
        "actual_cut_fired":fired.load(Ordering::SeqCst),
        "actual_cut":actual_cut.lock().clone(),
        "actual_catalogue_rename_fault_fired":io_fired,
        "original_admission_cancel_requested":admission.cancellation().is_cancel_requested(),
        "before":before,"after":after.as_ref().ok(),
        "snapshot_error":after.as_ref().err().map(ToString::to_string),
        "scope":"same-process genuine control, not executed physical qualification",
    });
    eprintln!("NATIVE_OPEN_ABSENCE_CANCELLATION_CONTROL {observed}");
    evidence(
        &output.join("open-actual-absence-cancellation-before-assertions.json"),
        &observed,
    )
    .unwrap();
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "an earlier timeout/refusal is an unreached absence cut"
    );
    assert!(
        matches!(&actual, Err(BicDbError::QueryCanceled)),
        "actual cancellation must refuse before new publication: {:?}",
        actual.as_ref().err()
    );
    assert_eq!(io_fired, 0, "absence must not authorize a catalogue write");
    assert_eq!(before["catalogue"]["retained"], false);
    assert_eq!(before["catalogue"]["uncertain_at_snapshot_entry"], false);
    let after = after.unwrap();
    assert_eq!(
        before["catalogue"], after["catalogue"],
        "canceled new-publication path must preserve original actual head/IO history"
    );
    assert_same_native(&before, &after);
    assert_eq!(before["frozen_owner"], after["frozen_owner"]);
    assert_eq!(
        before["native_preparation_sha256"],
        after["native_preparation_sha256"]
    );
}
