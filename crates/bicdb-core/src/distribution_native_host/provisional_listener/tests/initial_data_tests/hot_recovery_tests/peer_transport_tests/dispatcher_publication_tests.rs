//! Actual mutex contention and actual binding; no fabricated readiness.
use super::*;

#[test]
fn native_initial_dispatcher_actual_service_release_publishes_with_original_parent() {
    exercise(0);
}
#[test]
fn native_initial_dispatcher_actual_service_cancel_preserves_original_binding() {
    exercise(1);
}
#[test]
fn native_initial_dispatcher_actual_service_expiry_preserves_original_binding() {
    exercise(2);
}

fn exercise(mode: u8) {
    let (f, mut controls, configs) = opened();
    let (coordinator, host, runtime, _) = controls[0].test_hot_recovery_bindings().unwrap();
    let dispatcher = runtime.native_event_dispatcher().unwrap();
    let (_, range) = scope_and_range(&f, &controls);
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )
    .unwrap();
    let topology = bootstrap
        .fetch_snapshot(&node(0), &configs[0].node_address)
        .unwrap()
        .topology;
    let gateway = crate::ClusterEventClient::new(&configs[0], topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let key = (0..10_000)
        .map(|n| format!("actual-dispatch-publication-{mode}-{n}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range.id
        })
        .unwrap();
    let value = serde_json::json!({"complete":"original acknowledged payload","nested":[null,37,{"held":true}]});
    assert!(gateway.append("items", &key, value.clone()).unwrap());
    let before = f.hosts[0]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let actual = coordinator
        .test_actual_dispatch_publication_service_contention(
            &runtime,
            &host,
            &dispatcher,
            mode,
            &budget(5000),
        )
        .unwrap();
    eprintln!("NATIVE_INITIAL_DISPATCH_PUBLICATION_ACTUAL {actual}");
    assert_eq!(actual["actual_original_service_held"], true);
    assert_eq!(actual["actual_failed_service_lock_slice"], true);
    assert_eq!(actual["one_shot_still_refused"], true);
    assert_eq!(actual["original_deadline_preserved"], true);
    assert_eq!(actual["actual_holder_joined_ok"], true);
    assert!(actual["cleanup_error"].is_null());
    assert_eq!(
        actual["recovered_epochs_before"],
        actual["recovered_epochs_after"]
    );
    if mode == 0 {
        assert_eq!(actual["original_returned_ok"], true);
        assert_eq!(actual["binding_after"], actual["original_generation"]);
        assert_ne!(actual["binding_before"], actual["binding_after"]);
        assert!(actual["original_error"].is_null());
    } else {
        assert_eq!(actual["original_returned_ok"], false);
        assert_eq!(
            actual["original_error_class"],
            if mode == 1 {
                "QueryCanceled"
            } else {
                "QueryTimedOut"
            }
        );
        assert_eq!(actual["binding_before"], actual["binding_after"]);
        if mode == 1 {
            assert_eq!(actual["original_parent_cancelled"], true);
        } else {
            assert_eq!(actual["original_parent_cancelled"], false);
            assert_eq!(actual["original_parent_expired"], true);
        }
    }
    let after = f.hosts[0]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(value));
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
