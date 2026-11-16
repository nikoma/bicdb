//! Actual normal same-process hosts and held service guard. No placement,
//! native commit or fixture opening is replayed by these controls.
use super::*;
#[test]
fn native_committed_publication_waits_for_original_service_guard() {
    run(0);
}
#[test]
fn native_committed_publication_original_cancel_preserved() {
    run(1);
}
#[test]
fn native_committed_publication_original_deadline_preserved() {
    run(2);
}
fn run(mode: u8) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = Five::start(&mut report)
        .unwrap_or_else(|e| panic!("actual publication fixture prerequisite: {e}"));
    let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        let range = five.topology.ranges.values().next().unwrap().id;
        let data = report
            .data_call("actual_committed_publication_original_service_wait", || {
                five.sources[0].test_committed_publication_request_wait(range, mode, &five.lifetime)
            })?;
        assert!(data["original_control_error"].is_null());
        assert!(data["before_diagnostic_error"].is_null());
        assert!(data["after_diagnostic_error"].is_null());
        assert_eq!(data["original_process_still_live"], true);
        let control = &data["actual_control"];
        assert_eq!(control["actual_original_instant_busy"], true);
        assert_eq!(control["cut"]["actual_failed_service_slice"], true);
        assert_eq!(control["cut"]["parent_live_before_action"], true);
        assert_eq!(control["same_original_request_no_new_deadline"], true);
        assert!(control["cleanup_error"].is_null());
        assert!(control["serialization_error"].is_null());
        assert_eq!(control["actual_returned_ok"], mode == 0);
        if mode == 0 {
            assert!(control["original_error"].is_null());
        } else {
            assert_eq!(control["holder_still_held_at_original_return"], true);
            assert_eq!(
                control["original_error_class"],
                if mode == 1 {
                    "QueryCanceled"
                } else {
                    "QueryTimedOut"
                }
            );
        }
        for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
            assert!(data["before_native"][field].is_number());
            assert_eq!(data["before_native"][field], data["after_native"][field]);
        }
        Ok(())
    }));
    five.stop(&mut report);
    report.event(serde_json::json!({"publication_mode":mode,
        "actual_primary_error":match &actual {Ok(Err(error))=>Some(error.to_string()),_=>None},
        "same_process_normal_hosts_not_independent_processes":true}));
    match actual {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
