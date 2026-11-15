//! Actual original normal INITIAL owners, unchanged SDK2s and helper5s. These
//! definitions are not executed acceptance or an independent-machine claim.
use super::*;
use std::io::Write as _;

struct ClearCut(Arc<parking_lot::RwLock<BicDb>>);
impl Drop for ClearCut {
    fn drop(&mut self) {
        self.0.read().test_registry_native_cut(None);
    }
}
fn report(value: serde_json::Value) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "REGISTRY_RETAINED_CONTROL {value}"
    );
}
// Exact allowed refusal classes from the actual pending/health producers;
// transport/timeout/service-busy errors cannot satisfy these secondary cuts.
fn actual_publication_refusal(error: &BicDbError, allow_health: bool) -> bool {
    match error {
        BicDbError::Cluster(message) | BicDbError::PagedStorage(message) => {
            message.contains("native writer policy publication unresolved")
                || (allow_health
                    && (message.contains("durable write failed at ")
                        || message
                            .contains("database refuses writes after a failed durable write at ")))
        }
        BicDbError::WriteFailed { .. } => allow_health,
        _ => false,
    }
}
#[test]
fn native_initial_registry_same_known_api_late_cancel_fresh_read_no_replay_and_sdk_strong() {
    run(false);
}
#[test]
fn native_initial_registry_real_native_sync_unknown_retains_same_owner_and_denies_app() {
    run(true);
}
fn run(unknown: bool) {
    let (f, mut controls, gateway, topology) = ready();
    let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        exercise(&f, &controls, &gateway, &topology, unknown)
    }));
    report(
        serde_json::json!({"stage":"original_control_action_before_cleanup",
        "unknown":unknown,"error":actual.as_ref().ok().and_then(|r|r.as_ref().err())
            .map(crate::distribution_native_host::remote_first::error_data),
        "panic":actual.is_err(),"scope":"same-process normal-host component only"}),
    );
    let cleanup = controls.iter_mut().enumerate().map(|(node, control)| {
        let result = control.stop(&budget(5000));
        report(serde_json::json!({"stage":"actual_owned_control_stop","node":node,
            "error":result.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
        result
    }).collect::<Vec<_>>();
    match actual {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(Err(error)) => panic!("original registry component error: {error}"),
        Ok(Ok(())) => {
            for result in cleanup {
                result.unwrap();
            }
        }
    }
}
fn exercise(
    f: &Fixture,
    controls: &[NativeMetadataControlHost],
    gateway: &crate::ClusterEventClient,
    topology: &ClusterTopology,
    unknown: bool,
) -> crate::Result<()> {
    let key = selected_key(topology);
    let expected =
        serde_json::json!({"actual":"retained v1 native registry audit", "full":[1,null,{}]});
    assert!(gateway.append("items", &key, expected.clone())?);
    assert_eq!(gateway.get_strong("items", &key)?, Some(expected.clone()));
    let id = crate::cluster_event_record_id("items", &key)?;
    let range = topology
        .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)?
        .id;
    let db = &f.hosts[0].seed.database;
    let before = point(&db.read(), range);
    let guard = controls[0].test_retained_initial_data_writer_guard()?;
    let binding = guard.binding_diagnostic();
    let parent = budget(5000);
    let facts = Arc::new(parking_lot::Mutex::new([None; 2]));
    let captured = facts.clone();
    let same_parent = parent.clone();
    db.read()
        .test_registry_native_cut(Some(Box::new(move |known, xid, parent_live, own_live| {
            // Original facts FIRST, with no assert/Display/JSON/IO under the actual
            // journal. Cancellation only at a genuinely live actual Known return.
            captured.lock()[usize::from(known)] = Some((xid, parent_live, own_live));
            if known && !unknown && parent_live && own_live {
                same_parent.cancel();
            }
        })));
    let cut_cleanup = ClearCut(db.clone());
    let fault = unknown.then(|| {
        bicdb_page::test_faults::arm_io_failure("page_log.sync", db.read().data_path(), 1, false)
    });
    let original = controls[0].test_force_initial_data_registry_refresh(&parent);
    let fired = fault.as_ref().map_or(0, |fault| fault.fired());
    drop(fault);
    drop(cut_cleanup);
    let observed = *facts.lock();
    // Preserve exact original API error and actual cuts before ANY fallible
    // projection, fresh observer or SDK probe.
    report(
        serde_json::json!({"stage":"actual_original_registry_return_before_projection",
        "unknown":unknown,"actual_error":original.as_ref().err()
            .map(crate::distribution_native_host::remote_first::error_data),
        "actual_cuts":observed,"actual_fault_fired":fired,
        "original_deadline":format!("{:?}",parent.deadline())}),
    );
    let after_original = guard.test_retained_registry_attempt_point(&db.read())?;
    // Preserve exact original API error and actual cuts before ANY fresh
    // observer/SDK probe. Late diagnostic failure cannot turn it into success.
    report(
        serde_json::json!({"stage":"actual_original_registry_return_before_fresh",
        "unknown":unknown,"actual_error":original.as_ref().err()
            .map(crate::distribution_native_host::remote_first::error_data),
        "actual_cuts":observed,"actual_fault_fired":fired,
        "retained_after_original":after_original,"original_deadline":format!("{:?}",parent.deadline())}),
    );
    let issued = observed[0].expect("actual native issued-XID cut was not reached");
    assert!(
        issued.0 > 0 && issued.1 && issued.2,
        "actual original windows were not live"
    );
    assert_eq!(after_original["retained"]["issued_xid"], issued.0);
    assert_eq!(after_original["pending"], true);
    if unknown {
        assert_eq!(fired, 1, "actual native commit sync fault not reached");
        assert!(matches!(
            &original,
            Err(BicDbError::CommitOutcomeUnknown { .. })
        ));
        assert!(
            observed[1].is_none(),
            "failed API must not emit Known-return cut"
        );
        assert_eq!(after_original["retained"]["outcome"], "unknown");
        assert!(after_original["retained"]["known_xid"].is_null());
        let fresh = budget(5000);
        let actual = controls[0].test_force_initial_data_registry_refresh(&fresh);
        report(
            serde_json::json!({"stage":"actual_fresh_unknown_return_before_projection",
            "actual_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        let after_fresh = guard.test_retained_registry_attempt_point(&db.read())?;
        report(serde_json::json!({"stage":"fresh_original_unknown_refusal",
            "actual_error":actual.as_ref().err().map(crate::distribution_native_host::remote_first::error_data),
            "same_retained_after_fresh":after_fresh,
            "reach_limit":"actual DB health may refuse before retained-Unknown match; no bypass"}));
        assert!(
            actual.is_err(),
            "actual original sync Unknown cannot publish"
        );
        assert_eq!(
            after_fresh, after_original,
            "fresh request changed retained owner/XID/LSN"
        );
        let refused = gateway.get_strong("items", &key);
        report(serde_json::json!({"stage":"actual_unknown_sdk_refusal",
            "error":refused.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}));
        assert!(
            refused
                .as_ref()
                .err()
                .is_some_and(|error| actual_publication_refusal(error, true)),
            "fenced native owner must reach actual health/pending refusal: {refused:?}"
        );
        assert!(
            db.read().commit_admission_ready(),
            "the SAME original hook stays installed; presence is not readiness"
        );
    } else {
        assert_eq!(fired, 0);
        assert!(matches!(&original, Err(BicDbError::QueryCanceled)));
        assert_eq!(
            observed[1],
            Some(issued),
            "Known cut must follow SAME actual XID/live scopes"
        );
        assert_eq!(after_original["retained"]["outcome"], "known_actual_api_ok");
        assert_eq!(after_original["retained"]["known_xid"], issued.0);
        let original_audit = db
            .read()
            .test_current_native_writer_registry_audit()?
            .unwrap();
        assert_eq!(original_audit["native_page_xid"], issued.0);
        let refused = gateway.get_strong("items", &key);
        report(
            serde_json::json!({"stage":"actual_known_pending_sdk_refusal",
            "error":refused.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        assert!(
            refused
                .as_ref()
                .err()
                .is_some_and(|error| actual_publication_refusal(error, false)),
            "known pending must reach actual pending refusal: {refused:?}"
        );
        let fresh = budget(5000);
        let actual_fresh = controls[0].test_force_initial_data_registry_refresh(&fresh);
        report(
            serde_json::json!({"stage":"actual_fresh_known_return_before_projection",
            "actual_error":actual_fresh.as_ref().err().map(crate::distribution_native_host::remote_first::error_data)}),
        );
        actual_fresh?;
        fresh.check()?;
        assert_eq!(
            db.read()
                .test_current_native_writer_registry_audit()?
                .unwrap(),
            original_audit
        );
        let after_fresh = guard.test_retained_registry_attempt_point(&db.read())?;
        for field in ["native_next_xid", "native_next_lsn", "native_durable_lsn"] {
            assert_eq!(
                after_original[field], after_fresh[field],
                "fresh Known read replayed native IO"
            );
        }
        assert!(after_fresh["retained"].is_null());
        assert_eq!(after_fresh["pending"], false);
        assert_eq!(point(&db.read(), range), before);
        let after_binding = guard.binding_diagnostic();
        for field in [
            "persisted_policy_coordinator_id",
            "persisted_policy_registration_generation",
            "persisted_policy_database_open_incarnation",
            "persisted_policy_range_store_open_id",
            "persisted_policy_checksum_sha256",
        ] {
            assert_eq!(binding[field], after_binding[field]);
        }
        assert_eq!(gateway.get_strong("items", &key)?, Some(expected));
        let fresh_key = "retained-registry-fresh-sdk-ack";
        let fresh_value = serde_json::json!({"fresh":"actual original SDK ACK after Known reconcile", "null":null});
        assert!(gateway.append("items", fresh_key, fresh_value.clone())?);
        assert_eq!(gateway.get_strong("items", fresh_key)?, Some(fresh_value));
    }
    Ok(())
}
