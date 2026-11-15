//! Genuine normal TLS host objects and running process workers. These are
//! source definitions, not executed results or an end-to-end controller claim.
use super::*;
mod activation_domain_tests;
#[test]
fn native_initial_live_allocation_preserves_state_worker_and_real_sdk_strong() {
    run(0);
}
#[test]
fn native_initial_live_allocation_pre_io_cancel_retains_original_candidate() {
    run(1);
}
#[test]
fn native_initial_live_allocation_known_native_postcheck_cancel_reconciles_without_replay() {
    run(2);
}
#[test]
fn native_initial_live_allocation_commit_sync_unknown_never_replays_or_publishes_policy() {
    run(3);
}
#[test]
fn native_initial_live_allocation_actual_generic_epoch_marker_cannot_replace_native_serving() {
    run(4);
}
#[test]
fn native_initial_live_allocation_actual_publication_guard_refuses_injected_health_fence() {
    run(5);
}
fn run(mode: u8) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = Five::start(&mut report)
        .unwrap_or_else(|e| panic!("actual allocation fixture prerequisite: {e}"));
    let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise(&mut five, &mut report, mode)
    }));
    for host in &five.sources {
        let _ = host.test_initial_allocation_pause(false);
    }
    five.stop(&mut report);
    report.event(serde_json::json!({"actual_primary_error":match &actual {Ok(Err(e))=>Some(e.to_string()),_=>None},
        "mode":mode,"same_process_normal_hosts_not_independent_processes":true,
        "no_qualification_or_native_grant_from_report":true}));
    match actual {
        Ok(result) => result.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
fn exercise(five: &mut Five, report: &mut Report, mode: u8) -> Result<()> {
    let selected = five
        .topology
        .ranges
        .values()
        .next()
        .ok_or_else(|| invalid("allocation actual selected range absent"))?
        .clone();
    let other = five
        .topology
        .ranges
        .values()
        .find(|r| r.id != selected.id)
        .ok_or_else(|| invalid("allocation actual untouched range absent"))?
        .clone();
    let leader = (0..five.sources.len())
        .find(|i| node(*i) == selected.leader)
        .ok_or_else(|| invalid("allocation actual leader host absent"))?;
    let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?;
    sdk(
        report,
        &client,
        &key(&five.topology, selected.id, "allocation-before-selected")?,
        serde_json::json!({"full":"selected before","present_null":null}),
    )?;
    sdk(
        report,
        &client,
        &key(&five.topology, other.id, "allocation-before-untouched")?,
        serde_json::json!({"full":"untouched before"}),
    )?;
    let nonhosting = (0..five.sources.len())
        .find(|i| {
            !selected
                .replicas
                .iter()
                .any(|r| r.node_id == node(*i) && r.role == RangeReplicaRole::Voter)
        })
        .ok_or_else(|| invalid("actual allocation nonhosting original host absent"))?;
    let foreign_inventory_before = five.sources[nonhosting].test_initial_allocation_inventory()?;
    let foreign_before = five.sources[nonhosting].test_initial_allocation_native_point(selected.id);
    report.event(serde_json::json!({"actual_nonhosting_before":foreign_before.as_ref().err().map(ToString::to_string)}));
    assert!(
        matches!(&foreign_before,Err(crate::BicDbError::Cluster(message))
        if message=="native range image: no trusted native anchor; image repair required")
    );
    let before = five
        .sources
        .iter()
        .map(|h| h.test_initial_allocation_evidence())
        .collect::<Result<Vec<_>>>()?;
    if mode == 0 {
        let foreign = (leader + 1) % five.sources.len();
        let denied = report.call("allocation_actual_foreign_State_prepare_refusal", || {
            five.sources[leader]
                .test_initial_allocation_foreign_owner(&five.sources[foreign], &five.lifetime)
        });
        report.event(serde_json::json!({"actual_foreign_owner_refusal":denied.as_ref().err().map(ToString::to_string)}));
        assert!(
            denied.as_ref().err().is_some_and(|e| e
                .to_string()
                .contains("allocation replaced original State/coordinator")),
            "must reach exact real different-owner comparison: {denied:?}"
        );
    }
    if (1..=3).contains(&mode) {
        five.sources[leader].test_initial_allocation_fault(mode)?;
    }
    if mode == 5 {
        five.sources[leader].test_initial_allocation_fault(4)?;
    }
    if mode == 4 {
        five.sources[leader].test_initial_allocation_pause(true)?;
    }
    let pending = report.call("allocation_actual_committed_placement", || {
        five.sources[0].test_first_commit_allocation(selected.id, &node(4), &five.lifetime)
    })?;
    report.event(serde_json::json!({"actual_committed_allocation":pending,"original_owners_before":before,"mode":mode,
        "worker_not_replaced_or_manually_refreshed":true,"sdk_timeout_ms":2000}));
    if mode == 4 {
        let token = five
            .lifetime
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        loop {
            token.check()?;
            let all = five
                .sources
                .iter()
                .map(|h| h.test_initial_allocation_evidence())
                .collect::<Result<Vec<_>>>()?;
            if all.iter().all(|d| d["published"]["record"]["version"] == 2)
                && all[leader]["actual_live_recovery_hold_cuts"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let generic = report.call("allocation_real_generic_prefix_tail_recovery", || {
            five.sources[leader].test_initial_allocation_generic_recovery(selected.id, &token)
        })?;
        let actual_status = report.call("allocation_actual_generic_marker_status", || {
            five.sources[leader].status(&five.lifetime)
        })?;
        report.event(serde_json::json!({"actual_status_after_generic_marker_before_native_serving":actual_status}));
        let selected_status = actual_status
            .initial_data
            .as_ref()
            .and_then(|d| d.ranges.iter().find(|r| r.range_id == selected.id))
            .ok_or_else(|| invalid("actual allocation current descriptor status missing"))?;
        assert_eq!(selected_status.epoch, selected.epoch + 1);
        assert_eq!(
            selected_status.leader_admission_observed, false,
            "generic recovered marker must not advertise new native epoch readiness"
        );
        assert_eq!(
            actual_status.data_serving, false,
            "the actual incomplete led allocation remains diagnostically unavailable"
        );
        sdk(
            report,
            &client,
            &key(
                &five.topology,
                other.id,
                "allocation-unrelated-during-native-hold",
            )?,
            serde_json::json!({"actual":"untouched range remains available while selected native epoch is incomplete"}),
        )?;
        let native_before =
            five.sources[leader].test_initial_allocation_native_point(selected.id)?;
        let refused_key = key(
            &five.topology,
            selected.id,
            "allocation-generic-marker-insufficient",
        )?;
        let blocked = report.call("allocation_actual_sdk_requires_native_serving", || {
            client.append(
                "items",
                &refused_key,
                serde_json::json!({"actual":"generic epoch marker must not grant native APP"}),
            )
        });
        let native_after =
            five.sources[leader].test_initial_allocation_native_point(selected.id)?;
        report.event(serde_json::json!({"actual_generic_recovery_return":generic,"original_native_before":native_before,
            "actual_blocked_return":blocked.as_ref().err().map(ToString::to_string),"native_after":native_after}));
        five.sources[leader].test_initial_allocation_pause(false)?;
        assert!(blocked.as_ref().err().is_some_and(|e|e.to_string().contains("allocation origin requires actual completed native serving configuration")),
            "must reach the actual native configuration check after genuine generic recovery: {blocked:?}");
        assert_eq!(
            native_before, native_after,
            "refused origin changed actual selected native prefix"
        );
    }
    if mode == 5 {
        let token = five
            .lifetime
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let original = loop {
            token.check()?;
            let data = five.sources[leader].test_initial_allocation_evidence()?;
            if data["cut"]["actual_original_publication_error"].is_string() {
                break data;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        // The original actual publication Result is captured separately by
        // its typed diagnostic cut, before this distinct SDK denial attempt.
        report.event(serde_json::json!({"actual_health_fence_original":original,
            "before":before[leader],"injected_same_health_not_storage_IO_fault":true,
            "failure_detail_mutex_held_by_test":false}));
        let refused_key = key(&five.topology, selected.id, "allocation-health-refuses")?;
        let blocked = report.call("allocation_health_fence_actual_sdk_append", || {
            client.append(
                "items",
                &refused_key,
                serde_json::json!({"actual":"fenced publication must not grant an ACK"}),
            )
        });
        let after = five.sources[leader].test_initial_allocation_evidence()?;
        report.event(serde_json::json!({"actual_health_fence_sdk_error":blocked.as_ref().err().map(ToString::to_string),
            "after":after,"original":original}));
        assert_eq!(original["cut"]["mode"], 4);
        assert_eq!(original["cut"]["parent_live_before"], true);
        assert_eq!(original["cut"]["own_live_before"], true);
        assert_eq!(original["cut"]["original_deadline_recorded"], true);
        assert_eq!(original["cut"]["fault_fired"], 1);
        assert_eq!(original["cut"]["actual_typed_atomic_health_refusal"], true);
        assert_eq!(
            original["cut"]["actual_original_publication_error"],
            "PagedStorage: actual allocation atomic health refusal"
        );
        let xid = original["retained"]["record"]["native_page_xid"]
            .as_u64()
            .unwrap();
        assert!(xid > 0);
        assert_eq!(original["cut"]["original_xid"].as_u64(), Some(xid));
        assert_eq!(
            original["retained"]["actual_outcome"],
            format!("Known({xid})"),
            "the actual successful native return must precede publication refusal"
        );
        assert_eq!(original["published"], before[leader]["published"]);
        assert_eq!(original["pending"], true);
        assert!(blocked.is_err());
        assert_eq!(original["retained"]["record"], after["retained"]["record"]);
        assert_eq!(
            original["retained"]["actual_outcome"],
            after["retained"]["actual_outcome"]
        );
        assert_eq!(original["published"], after["published"]);
        assert_eq!(after["pending"], true);
        return Ok(());
    }
    if mode == 3 {
        let token = five
            .lifetime
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let original = loop {
            token.check()?;
            let data = five.sources[leader].test_initial_allocation_evidence()?;
            if data["cut"]["fault_fired"].as_u64().unwrap_or(0) > 0 {
                break data;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        report.event(serde_json::json!({"actual_unknown_original_before_followup":original}));
        // This is a separate actual APP outcome, not a repeated successful SDK
        // request and not an inference that a client error rolled back effects.
        let refused_key = key(&five.topology, selected.id, "allocation-unknown-refuses")?;
        let blocked = report.call("allocation_unknown_actual_sdk_append", || {
            client.append(
                "items",
                &refused_key,
                serde_json::json!({"actual":"must not obtain ACK"}),
            )
        });
        let after = five.sources[leader].test_initial_allocation_evidence()?;
        report.event(serde_json::json!({"actual_unknown_followup_error":blocked.as_ref().err().map(ToString::to_string),
            "original":original,"after":after}));
        assert!(
            blocked.is_err(),
            "Unknown allocation native IO must not grant an application ACK"
        );
        assert_eq!(original["cut"]["parent_live_before"], true);
        assert_eq!(original["cut"]["own_live_before"], true);
        assert_eq!(original["cut"]["original_deadline_recorded"], true);
        assert!(original["cut"]["fault_fired"].as_u64().unwrap() > 0);
        assert!(
            original["retained"]["record"]["native_page_xid"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(original["retained"]["actual_outcome"], "Unknown");
        assert_eq!(original["retained"]["record"], after["retained"]["record"]);
        assert_eq!(original["published"], after["published"]);
        assert_eq!(after["pending"], true);
        return Ok(());
    }
    for source in &five.sources {
        report.call("allocation_observe_same_worker_native_epoch", || {
            source.observe_initial_data_allocation(selected.id, &five.lifetime)
        })?;
    }
    let after = five
        .sources
        .iter()
        .map(|h| h.test_initial_allocation_evidence())
        .collect::<Result<Vec<_>>>()?;
    report.event(serde_json::json!({"actual_allocation_completed_before_assertions":after,"original":before}));
    for (original, current) in before.iter().zip(&after) {
        for field in ["registration", "coordinator", "database_open", "store_open"] {
            assert_eq!(
                original[field], current[field],
                "actual allocation replaced {field}"
            );
        }
        assert_eq!(current["pending"], false);
        assert_eq!(current["published"]["record"]["version"], 2);
    }
    let foreign_inventory_after = five.sources[nonhosting].test_initial_allocation_inventory()?;
    report.event(
        serde_json::json!({"actual_nonhosting_inventory_before":foreign_inventory_before,
        "actual_nonhosting_inventory_after":foreign_inventory_after}),
    );
    assert_eq!(
        foreign_inventory_before, foreign_inventory_after,
        "nonhosting cluster policy update changed actual physical inventory"
    );
    let foreign_after = five.sources[nonhosting].test_initial_allocation_native_point(selected.id);
    let reader = five.sources[leader].test_initial_allocation_reader_barrier();
    report.event(serde_json::json!({"actual_nonhosting_after":foreign_after.as_ref().err().map(ToString::to_string),
        "actual_old_feature_set_refusal":reader.as_ref().err().map(ToString::to_string),
        "old_binary_execution":false}));
    assert!(
        matches!(&foreign_after,Err(crate::BicDbError::Cluster(message))
        if message=="native range image: no trusted native anchor; image repair required")
    );
    assert!(
        matches!(&reader,Err(crate::BicDbError::FormatCompatibility(message))
        if message=="database requires unknown format feature flag `native_writer_allocation_policy_v1`")
    );
    if (1..=2).contains(&mode) {
        let cut = &after[leader]["cut"];
        assert_eq!(cut["mode"], mode);
        assert_eq!(
            cut["parent_live_before"], true,
            "late/expired cut cannot qualify actual cancellation"
        );
        assert_eq!(cut["own_live_before"], true);
        assert_eq!(cut["original_deadline_recorded"], true);
        if mode == 2 {
            assert!(cut["original_xid"].as_u64().unwrap() > 0);
            assert_eq!(
                cut["original_xid"], after[leader]["published"]["record"]["native_page_xid"],
                "fresh known-only reconciliation must reuse exact actual original native XID"
            );
        } else {
            assert_eq!(cut["original_xid"], 0);
        }
    }
    sdk(
        report,
        &client,
        &key(&five.topology, selected.id, "allocation-after-selected")?,
        serde_json::json!({"full":"selected after","present_null":null}),
    )?;
    sdk(
        report,
        &client,
        &key(&five.topology, other.id, "allocation-after-untouched")?,
        serde_json::json!({"full":"untouched after"}),
    )?;
    Ok(())
}
