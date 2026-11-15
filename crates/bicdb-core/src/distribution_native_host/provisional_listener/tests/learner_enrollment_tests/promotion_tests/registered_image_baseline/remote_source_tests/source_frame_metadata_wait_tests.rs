//! Real source session, real final metadata contention, original wire4s/parent5s.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::FrameFinalMetadataWait
            | SourceCase::FrameFinalMetadataCallerCancel
            | SourceCase::FrameFinalMetadataScopeExpiry
    )
}
macro_rules! case {
    ($name:ident,$case:ident) => {
        #[test]
        fn $name() {
            run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::$case));
        }
    };
}
case!(
    native_registered_fifth_source_frame_final_metadata_real_wait_preserves_authenticated_payload,
    FrameFinalMetadataWait
);
case!(
    native_registered_fifth_source_frame_final_metadata_real_caller_cancel_own_live,
    FrameFinalMetadataCallerCancel
);
case!(
    native_registered_fifth_source_frame_final_metadata_original_scope_expiry_while_held,
    FrameFinalMetadataScopeExpiry
);
#[derive(Default)]
struct WaitState {
    entered: AtomicBool,
    holder_ready: AtomicBool,
    busy: AtomicBool,
    returned: AtomicBool,
    holder_released: AtomicBool,
    cleanup: AtomicBool,
    events: Mutex<Vec<serde_json::Value>>,
    actual_return: Mutex<Option<serde_json::Value>>,
}
struct Cleanup(Arc<WaitState>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanup.store(true, Ordering::Release);
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    retained: &crate::distribution_range_image::NativeRetainedRemoteImageSource,
    target: &TcpClusterRelocationTransport,
    manifest: &NativeRemoteImageManifestV1,
    metadata: &Arc<Mutex<crate::MetadataConsensusStore>>,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
) -> AuthenticatedNativeRemoteImageSourceFrame {
    let before = retained.test_frame_wait_source_snapshot().unwrap();
    let mf_bytes = serde_json::to_vec(manifest).unwrap();
    let state = Arc::new(WaitState::default());
    let entered = state.clone();
    let busy = state.clone();
    let returned = state.clone();
    retained
        .test_arm_frame_wait(
            Box::new(move |request, facts| {
                entered.events.lock().push(serde_json::json!({"stage":"after_actual_DB_before_FINAL_metadata_lock","facts":facts}));
                entered.entered.store(true, Ordering::Release);
                while !entered.holder_ready.load(Ordering::Acquire)
                    && !entered.cleanup.load(Ordering::Acquire)
                {
                    if request.check().is_err() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }),
            Box::new(move |request, facts| {
                busy.events.lock().push(serde_json::json!({"stage":"actual_FINAL_try_lock_for_failed","facts":facts}));
                busy.busy.store(true, Ordering::Release);
                if matches!(case, SourceCase::FrameFinalMetadataCallerCancel) {
                    request.cancel();
                    busy.events.lock().push(serde_json::json!({"stage":"cancel_actual_server_frame_child",
                        "actual_request_cancel_requested":request.is_cancel_requested()}));
                }
            }),
            Box::new(move |mut facts| {
                facts["holder_released_at_actual_server_return"] =
                    returned.holder_released.load(Ordering::Acquire).into();
                returned.events.lock().push(serde_json::json!({"stage":"actual_SERVER_result_before_wire","facts":facts}));
                *returned.actual_return.lock() = Some(facts);
                returned.returned.store(true, Ordering::Release);
            }),
        )
        .unwrap();
    let journal_index = journal.len();
    let started = Instant::now();
    let wire = std::thread::scope(|threads| {
        let cleanup = Cleanup(state.clone());
        let holder = threads.spawn(|| {
            // Test cleanup watchdog only; neither wire4s nor client5s changes.
            let until = Instant::now() + Duration::from_secs(7);
            while !state.entered.load(Ordering::Acquire) {
                if state.cleanup.load(Ordering::Acquire) {
                    return;
                }
                assert!(
                    Instant::now() < until,
                    "actual final source check not reached"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let store = metadata
                .try_lock_for(Duration::from_secs(2))
                .expect("actual source MStore holder failed to enter");
            state.holder_ready.store(true, Ordering::Release);
            while !state.busy.load(Ordering::Acquire) {
                if state.cleanup.load(Ordering::Acquire) {
                    return;
                }
                assert!(
                    Instant::now() < until,
                    "actual failed source acquisition not observed"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            if !matches!(case, SourceCase::FrameFinalMetadataWait) {
                while !state.returned.load(Ordering::Acquire)
                    && !state.cleanup.load(Ordering::Acquire)
                {
                    assert!(
                        Instant::now() < until,
                        "server did not refuse while MStore held"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            drop(store);
            state.holder_released.store(true, Ordering::Release);
        });
        // Uses the unchanged real TLS helper: original parent/OWN5s, wire4s.
        let result = frame(target, manifest, Op::Manifest, output, journal);
        // A transport timeout can arrive just before the server observes its
        // original deadline. Keep the test holder until that actual return;
        // this cleanup watchdog neither renews nor retries the server request.
        while !state.returned.load(Ordering::Acquire) && started.elapsed() < Duration::from_secs(7)
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(cleanup);
        holder.join().unwrap();
        result
    });
    let after = retained.test_frame_wait_source_snapshot();
    let actual_return = state.actual_return.lock().clone();
    let report = serde_json::json!({"case":format!("{case:?}"),"source_session":manifest.source_session,
        "events":*state.events.lock(),"entered":state.entered.load(Ordering::Acquire),
        "actual_failed_lock":state.busy.load(Ordering::Acquire),"actual_server_return":actual_return,
        "elapsed_us":started.elapsed().as_micros(),"wire_returned_ok":wire.is_ok(),"wire_error":wire.as_ref().err().map(ToString::to_string),
        "before":before,"after":after.as_ref().ok(),"diagnostic_error":after.as_ref().err().map(ToString::to_string),
        "old_result_upgraded":false,"actual_source_session_not_recreated":true});
    let evidence_result = evidence(
        &output.join(format!(
            "source-frame-final-metadata-{}.json",
            manifest.capture_range.id.get()
        )),
        &report,
    );
    eprintln!("NATIVE_SOURCE_FRAME_FINAL_METADATA_WAIT {report}");
    // The primary wire return was already persisted by frame() before this
    // diagnostic; a diagnostic failure cannot rewrite it into success.
    if let Err(error) = &evidence_result {
        eprintln!("source frame final-wait evidence failure: {error}");
    }
    assert!(state.entered.load(Ordering::Acquire));
    assert!(state.busy.load(Ordering::Acquire));
    let returned = actual_return.expect("actual server result missing");
    assert_eq!(returned["actual_request"], journal[journal_index]["intent"]);
    assert_eq!(
        returned["source_session"],
        manifest.source_session.to_string()
    );
    // check() also consults an ambient request budget. A timed-out frame must
    // not be mistaken for cancellation of the separately retained session.
    assert_eq!(returned["original_source_session_cancel_requested"], false);
    match case {
        SourceCase::FrameFinalMetadataWait => {
            assert_eq!(returned["actual_server_result"], "Ok");
            let actual = wire.as_ref().unwrap();
            actual.require_request_live().unwrap();
            assert!(matches!(&actual.reply().payload, Payload::Manifest(_)));
            assert_eq!(serde_json::to_vec(actual.manifest()).unwrap(), mf_bytes);
        }
        SourceCase::FrameFinalMetadataCallerCancel => {
            assert_eq!(returned["actual_server_result"], "QueryCanceled");
            assert_eq!(returned["holder_released_at_actual_server_return"], false);
            assert_eq!(returned["own_live"], true);
            assert!(returned["own_remaining_us"].as_u64().unwrap() > 0);
            assert!(wire.is_err(), "server cancellation became successful frame");
        }
        SourceCase::FrameFinalMetadataScopeExpiry => {
            assert_eq!(returned["actual_server_result"], "QueryTimedOut");
            assert_eq!(returned["holder_released_at_actual_server_return"], false);
            assert!(wire.is_err(), "server expiry became successful frame");
        }
        _ => unreachable!("only new final metadata controls"),
    }
    assert_eq!(
        after.unwrap(),
        before,
        "source capture/native/MF/rows changed"
    );
    evidence_result.unwrap();
    // A DISTINCT real request/OWN reads the SAME session. Keep the original
    // return above; this new success never upgrades it or replaces the owner.
    let fresh = frame(target, manifest, Op::Manifest, output, journal).unwrap();
    fresh.require_request_live().unwrap();
    assert_eq!(serde_json::to_vec(fresh.manifest()).unwrap(), mf_bytes);
    assert_ne!(
        fresh.reply().request.business.to_string(),
        journal[journal_index]["intent"]["business"]
            .as_str()
            .unwrap()
    );
    assert_ne!(
        fresh.reply().source_metadata_context.to_string(),
        returned["own_context"].as_str().unwrap()
    );
    assert_eq!(retained.test_frame_wait_source_snapshot().unwrap(), before);
    fresh
}
