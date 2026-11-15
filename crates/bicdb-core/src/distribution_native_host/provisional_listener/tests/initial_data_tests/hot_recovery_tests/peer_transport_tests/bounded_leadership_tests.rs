//! Actual born-native coordinators and original peer socket destinations.
//! These controls do not qualify relocation, new-epoch recovery or physical HA.
use super::*;
use crate::distribution_range_consensus::RangeWriteTransport;

#[test]
fn native_bounded_leadership_actual_selected_validation_preserves_sdk_ack_and_strong() {
    let (f, mut controls, configs) = opened();
    let (coordinator, _, _, _) = controls[0].test_hot_recovery_bindings().unwrap();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|range| range.leader == node(0))
        .unwrap()
        .clone();
    assert!(coordinator.initial_data_leader_admission_observed(&range));
    // The actual INITIAL producer already recovered this epoch. This only
    // revalidates its real quorum; it must not invent a different marker.
    assert!(!coordinator
        .recover_native_range_leadership_bounded(&range, &budget(5000))
        .unwrap());
    assert_eq!(
        coordinator
            .recover_native_local_leadership_bounded(64, &budget(5000))
            .unwrap(),
        0
    );
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
        .map(|n| format!("bounded-revalidation-real-sdk-{n}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range.id
        })
        .unwrap();
    let value = serde_json::json!({"original":"complete acknowledged value", "nested":[null,17,{"ok":true}]});
    assert!(gateway.append("items", &key, value.clone()).unwrap());
    assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(value));
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}

#[test]
fn native_bounded_leadership_actual_progress_socket_original_cancel() {
    socket_boundary(false, true);
}
#[test]
fn native_bounded_leadership_actual_tail_probe_socket_original_cancel() {
    socket_boundary(true, true);
}
#[test]
fn native_bounded_leadership_actual_progress_socket_original_expiry() {
    socket_boundary(false, false);
}
#[test]
fn native_bounded_leadership_actual_tail_probe_socket_original_expiry() {
    socket_boundary(true, false);
}

fn socket_boundary(probe: bool, cancel: bool) {
    let (f, mut controls, configs) = opened();
    let (coordinator, _, _, transport) = controls[0].test_hot_recovery_bindings().unwrap();
    let (_, range) = scope_and_range(&f, &controls);
    assert!(range
        .replicas
        .iter()
        .any(|replica| replica.node_id == node(1)));
    let before = f.hosts[0]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let marker_before = coordinator.initial_data_leader_admission_observed(&range);
    controls[1].stop(&budget(5000)).unwrap();
    let fault = SocketFault::bind(&configs[1].node_address, false);
    let parent = budget(if cancel { 3000 } else { 200 });
    let original_deadline = parent.deadline();
    let returned = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            if probe {
                transport
                    .probe_range_write_cancellable(&node(1), range.id, range.epoch, 1, &parent)
                    .map(drop)
            } else {
                transport
                    .range_write_progress_cancellable(&node(1), range.id, range.epoch, &parent)
                    .map(drop)
            }
        });
        if cancel {
            // Cancellation is triggered only after the real selected peer
            // TCP socket accepted. A planned sleep is not cut evidence.
            wait(|| fault.accepted.load(Ordering::Acquire) > 0, &budget(2000));
            parent.check().unwrap();
            parent.cancel();
        }
        worker.join().unwrap()
    });
    assert!(fault.accepted.load(Ordering::Acquire) > 0);
    assert_eq!(parent.deadline(), original_deadline);
    if cancel {
        assert!(matches!(returned, Err(BicDbError::QueryCanceled)));
    } else {
        assert!(matches!(returned, Err(BicDbError::QueryTimedOut)));
        assert!(!parent.is_cancel_requested());
        assert!(Instant::now() >= original_deadline.unwrap());
    }
    // A socket failure cannot fabricate history or epoch-readiness. These
    // are genuine existing INITIAL observations, never fixture-installed data.
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
    assert_eq!(
        coordinator.initial_data_leader_admission_observed(&range),
        marker_before
    );
    drop(fault);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
