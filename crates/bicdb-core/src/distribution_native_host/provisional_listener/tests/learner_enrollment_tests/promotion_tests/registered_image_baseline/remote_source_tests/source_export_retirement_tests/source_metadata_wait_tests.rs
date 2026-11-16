//! Real S2 retirement on the original source, not a mirrored mutex validator.
//! Positive cases continue through the unchanged genuine NEW after-take cut.
use super::*;
use std::sync::mpsc;
use std::thread::JoinHandle;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetSourceMetadataWaitBoundary
            | SourceCase::TargetSourceMetadataWaitAfterLocal
            | SourceCase::TargetSourceMetadataWaitCallerCanceled
            | SourceCase::TargetSourceMetadataWaitScopeExpired
    )
}
pub(super) fn positive(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetSourceMetadataWaitBoundary
            | SourceCase::TargetSourceMetadataWaitAfterLocal
    )
}
macro_rules! metadata_case {
    ($name:ident, $case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
metadata_case!(
    native_registered_fifth_source_metadata_real_boundary_wait_then_same_new_after_take,
    TargetSourceMetadataWaitBoundary
);
metadata_case!(
    native_registered_fifth_source_metadata_real_after_local_wait_then_same_new_after_take,
    TargetSourceMetadataWaitAfterLocal
);
metadata_case!(
    native_registered_fifth_source_metadata_real_wait_original_server_caller_cancel_refuses,
    TargetSourceMetadataWaitCallerCanceled
);
metadata_case!(
    native_registered_fifth_source_metadata_real_wait_original_scope_expiry_refuses,
    TargetSourceMetadataWaitScopeExpired
);

enum Start {
    Hold,
    Stop,
}
pub(super) struct Control {
    trace: Arc<Mutex<Vec<serde_json::Value>>>,
    start: mpsc::Sender<Start>,
    release: mpsc::Sender<()>,
    holder: Option<JoinHandle<()>>,
    actual_metadata_pointer: usize,
    output: std::path::PathBuf,
}
impl Drop for Control {
    fn drop(&mut self) {
        let _ = self.start.send(Start::Stop);
        let _ = self.release.send(());
        if let Some(holder) = self.holder.take() {
            let _ = holder.join();
        }
        let report = serde_json::json!({"stage":"control_drop_after_holder_cleanup","trace":self.trace.lock().clone(),"actual_metadata_pointer":self.actual_metadata_pointer,"unwinding":std::thread::panicking(),"original_RPC_or_panic_not_replaced":true});
        eprintln!("NATIVE_SOURCE_METADATA_WAIT_LAST_OBSERVATION {report}");
        if let Err(error) = evidence(
            &self
                .output
                .join("source-metadata-wait-drop-observation.json"),
            &report,
        ) {
            eprintln!("NATIVE_SOURCE_METADATA_WAIT_EVIDENCE_ERROR {error}");
        }
    }
}

pub(super) fn install(
    case: SourceCase,
    donor: &NativeMetadataControlHost,
    metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    output: &std::path::Path,
) -> Control {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let actual_metadata_pointer = Arc::as_ptr(metadata) as usize;
    let metadata = Arc::clone(metadata);
    let (start, started) = mpsc::channel();
    let (held, holding) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let (dropped, drop_observed) = mpsc::channel();
    let holder = std::thread::spawn(move || {
        if !matches!(
            started.recv_timeout(Duration::from_secs(5)),
            Ok(Start::Hold)
        ) {
            return;
        }
        let guard = metadata.try_lock_for(Duration::from_millis(500));
        let acquired = guard.is_some();
        let _ = held.send(acquired);
        if acquired {
            let _ = released.recv_timeout(Duration::from_secs(6));
        }
        drop(guard);
        let _ = dropped.send(());
    });
    let before_trace = Arc::clone(&trace);
    let before_start = start.clone();
    let busy_trace = Arc::clone(&trace);
    let busy_release = release.clone();
    let failure_trace = Arc::clone(&trace);
    donor.test_arm_source_metadata_wait(
        matches!(case, SourceCase::TargetSourceMetadataWaitAfterLocal),
        Box::new(move |_, _, actual| {
            before_trace.lock().push(serde_json::json!({"event":"actual_admitted_before_lock","actual":actual}));
            before_start.send(Start::Hold).unwrap();
            let held = holding.recv_timeout(Duration::from_secs(1)).unwrap();
            before_trace.lock().push(serde_json::json!({"event":"actual_same_source_metadata_held","held":held,"actual_metadata_pointer":actual_metadata_pointer}));
            assert!(held, "actual competing source MStore holder not established");
        }),
        Box::new(move |caller, own_deadline, actual| {
            busy_trace.lock().push(serde_json::json!({"event":"actual_bounded_lock_attempt_returned_none","actual":actual}));
            let before = Instant::now();
            if matches!(case, SourceCase::TargetSourceMetadataWaitCallerCanceled) {
                caller.cancel();
            } else if matches!(case, SourceCase::TargetSourceMetadataWaitScopeExpired) {
                std::thread::sleep(own_deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1));
            }
            let now = Instant::now();
            busy_trace.lock().push(serde_json::json!({"event":"actual_fault_or_release","action_elapsed_us":before.elapsed().as_micros(),"original_own_remaining_us":own_deadline.saturating_duration_since(now).as_micros(),"original_server_caller_cancel_requested":caller.is_cancel_requested(),"original_server_caller_remaining_us":caller.deadline().map(|d|d.saturating_duration_since(now).as_micros()),"no_deadline_or_context_replacement":true}));
            busy_release.send(()).unwrap();
            drop_observed.recv_timeout(Duration::from_secs(1)).unwrap();
            busy_trace.lock().push(serde_json::json!({"event":"actual_metadata_guard_dropped"}));
        }),
        Box::new(move |class| {
            failure_trace.lock().push(serde_json::json!({"event":"actual_original_helper_error","class":class}));
        }),
    ).unwrap();
    Control {
        trace,
        start,
        release,
        holder: Some(holder),
        actual_metadata_pointer,
        output: output.to_path_buf(),
    }
}

impl Control {
    pub(super) fn observe_original_return(
        &self,
        result: &Result<crate::distribution_range_image::NativeSourceExportReplyV1>,
    ) {
        let report = serde_json::json!({"stage":"original_RPC_return_before_recipient_diagnostic_wait","trace":self.trace.lock().clone(),"original_RPC_returned_ok":result.is_ok(),"original_RPC_error":result.as_ref().err().map(ToString::to_string),"recipient_completion_not_inferred_from_RPC_return":true});
        eprintln!("NATIVE_SOURCE_METADATA_WAIT_ORIGINAL_RETURN {report}");
        if let Err(error) = evidence(
            &self
                .output
                .join("source-metadata-wait-original-return.json"),
            &report,
        ) {
            eprintln!("NATIVE_SOURCE_METADATA_WAIT_EVIDENCE_ERROR {error}");
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn finish(
        &self,
        case: SourceCase,
        result: &Result<crate::distribution_range_image::NativeSourceExportReplyV1>,
        before: &serde_json::Value,
        after: &serde_json::Value,
        native_before: &serde_json::Value,
        native_after: &serde_json::Value,
        target_before: &serde_json::Value,
        target_after: Result<serde_json::Value>,
        output: &std::path::Path,
    ) {
        let trace = self.trace.lock().clone();
        let report = serde_json::json!({"case":format!("{case:?}"),"trace":trace,"original_RPC_ok":result.is_ok(),"original_RPC_error":result.as_ref().err().map(ToString::to_string),"before":before,"after":after,"old_native_before":native_before,"old_native_after":native_after,"target_before":target_before,"target_after":target_after.as_ref().ok(),"target_observation_error":target_after.as_ref().err().map(ToString::to_string),"same_actual_metadata_pointer":self.actual_metadata_pointer,"same_observed_OWN_context_required":true,"old_native_record_equality_required":true,"after_take_NEW_positive_continuation_not_yet_run":positive(case),"physical_qualification":false});
        eprintln!("NATIVE_SOURCE_METADATA_WAIT_CONTROL {report}");
        evidence(
            &output.join("source-metadata-wait-before-assertions.json"),
            &report,
        )
        .unwrap();
        let before_lock = trace
            .iter()
            .find(|x| x["event"] == "actual_admitted_before_lock")
            .expect("genuine admitted source lock cut unreached");
        let busy = trace
            .iter()
            .find(|x| x["event"] == "actual_bounded_lock_attempt_returned_none")
            .expect("no actual bounded MStore contention");
        assert!(
            before_lock["actual"]["actual_own_remaining_us"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            busy["actual"]["actual_own_remaining_us"].as_u64().unwrap() > 0,
            "OWN expired before intended busy cut"
        );
        assert_eq!(
            before_lock["actual"]["actual_own_context"],
            busy["actual"]["actual_own_context"]
        );
        assert_eq!(busy["actual"]["actual_failed_lock_wait"], true);
        assert_eq!(busy["actual"]["actual_own_original_request_live"], true);
        assert_eq!(
            busy["actual"]["actual_server_caller_cancel_requested"],
            false
        );
        assert!(trace
            .iter()
            .any(|x| x["event"] == "actual_metadata_guard_dropped"));
        assert_eq!(native_before, native_after);
        assert_eq!(
            &target_after.expect("actual target observation unavailable"),
            target_before
        );
        let action = trace
            .iter()
            .find(|x| x["event"] == "actual_fault_or_release")
            .unwrap();
        if positive(case) {
            assert!(
                result.is_ok(),
                "actual bounded source retirement failed: {result:?}"
            );
            assert!(!trace
                .iter()
                .any(|x| x["event"] == "actual_original_helper_error"));
        } else {
            let error = trace
                .iter()
                .find(|x| x["event"] == "actual_original_helper_error")
                .expect("actual original helper error not observed");
            if matches!(case, SourceCase::TargetSourceMetadataWaitCallerCanceled) {
                assert_eq!(error["class"], "QueryCanceled");
                assert_eq!(action["original_server_caller_cancel_requested"], true);
                assert!(action["original_own_remaining_us"].as_u64().unwrap() > 0);
            } else {
                assert_eq!(error["class"], "QueryTimedOut");
                assert_eq!(action["original_own_remaining_us"], 0);
                assert_eq!(action["original_server_caller_cancel_requested"], false);
            }
            assert!(
                result.is_err(),
                "expired/canceled ORIGINAL helper cannot report retirement success"
            );
            assert_eq!(after["source"]["capture_retained"], true);
            assert!(after["source"]["export_retirement"]["completed"].is_null());
            assert_eq!(
                after["source"]["source_session"],
                before["source"]["source_session"]
            );
            assert_eq!(
                after["source"]["actual_gate"],
                before["source"]["actual_gate"]
            );
            assert_eq!(after["source"]["manifest"], before["source"]["manifest"]);
            assert_eq!(
                after["source"]["original_export_session_cancel_requested"],
                false
            );
        }
    }
}
