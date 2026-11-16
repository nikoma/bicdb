//! Genuine normal host bootstrap component cuts, not a process/physical trial.
use super::*;

#[test]
fn native_bootstrap_finite_original_service_wait_releases() {
    run(0);
}
#[test]
fn native_bootstrap_finite_original_service_wait_cancels() {
    run(1);
}
#[test]
fn native_bootstrap_finite_original_service_wait_expires() {
    run(2);
}
#[test]
fn native_bootstrap_unbounded_service_busy_remains_instant() {
    run(3);
}

fn run(mode: u8) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = Five::start(&mut report)
        .unwrap_or_else(|error| panic!("actual bootstrap fixture prerequisite: {error}"));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        let range = five.topology.ranges.values().next().unwrap().id;
        // Test setup is outside this finite original request. The production
        // bootstrap helper receives precisely this token, with no replacement
        // timeout or retry. This is a component budget, not a new CLI window.
        let original = five
            .lifetime
            .child_with_deadline(Instant::now() + Duration::from_millis(500));
        let deadline = original.deadline();
        let data = report.data_call("actual_bootstrap_original_service_wait", || {
            five.sources[0].test_bootstrap_request_wait(range, mode, &original)
        })?;
        assert_eq!(data["original_parent_deadline_unchanged"], true);
        assert_eq!(original.deadline(), deadline);
        assert!(data["original_control_error"].is_null());
        assert!(data["before_native_error"].is_null());
        assert!(data["before_metadata_error"].is_null());
        assert!(data["after_native_error"].is_null());
        assert!(data["after_metadata_error"].is_null());
        assert_eq!(data["bootstrap_called_once"], true);
        assert_eq!(data["routing_DATA_only"], true);
        assert_eq!(data["actual_open_live_after"], true);
        assert_eq!(data["same_bound_service_after"], true);
        assert!(data["before_metadata"]["store_id"].is_string());
        assert_eq!(
            data["before_metadata"]["store_id"],
            data["after_metadata"]["store_id"]
        );
        assert_eq!(
            data["before_metadata"]["status"]["node_id"],
            data["after_metadata"]["status"]["node_id"]
        );
        let control = &data["actual_control"];
        assert_eq!(control["actual_original_instant_busy"], true);
        assert_eq!(control["actual_returned_ok"], mode == 0);
        if mode == 3 {
            assert_eq!(control["unbounded_parent"], true);
            assert_eq!(
                control["original_error"],
                "cluster error: native host bootstrap: native bootstrap service busy"
            );
        } else {
            assert_eq!(control["cut"]["actual_failed_service_slice"], true);
            assert_eq!(control["cut"]["parent_live_before_action"], true);
            assert_eq!(control["same_original_request_no_new_deadline"], true);
            assert!(control["cleanup_error"].is_null());
            assert!(control["serialization_error"].is_null());
            assert_eq!(
                control["actual_held_store"],
                data["controller_store_open_id"]
            );
            if mode == 0 {
                assert!(control["original_error"].is_null());
                assert_eq!(data["original_parent_live_at_bootstrap_return"], true);
                let snapshot: crate::ClusterBootstrapSnapshot =
                    serde_json::from_value(control["returned_data"].clone())?;
                snapshot.validate()?;
                assert_eq!(snapshot.topology.cluster_id, five.topology.cluster_id);
                assert_eq!(snapshot.config_template.node_id, node(0));
                // The result is captured from the actual metadata read; C and
                // clocks before/after are diagnostics, not a stale fallback or
                // a promise that unrelated maintenance did not advance.
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
                if mode == 1 {
                    assert!(original.is_cancel_requested());
                } else {
                    assert!(Instant::now() >= deadline.unwrap());
                }
            }
        }
        for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
            assert!(data["before_native"][field].is_number());
            assert!(data["after_native"][field].is_number());
        }
        Ok(())
    }));
    five.stop(&mut report);
    report.event(serde_json::json!({"bootstrap_mode":mode,"same_process_normal_hosts_not_independent_processes":true,
        "actual_primary_error":match &result { Ok(Err(error))=>Some(error.to_string()),_=>None }}));
    match result {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
