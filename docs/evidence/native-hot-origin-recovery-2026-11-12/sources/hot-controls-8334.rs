//! Actual born-enforced NativeHost/control/read reservation controls. No fake
//! progress, role, voter reply or recovered permission is installed by tests.
use super::*;
use std::sync::mpsc;
fn opened() -> (
    Fixture,
    Vec<NativeMetadataControlHost>,
    Vec<DistributionConfig>,
) {
    let (f, mut controls, configs) = actual_initial_metadata_controls();
    let end = Instant::now() + Duration::from_secs(20);
    let mut ready = [false; 3];
    while !ready.iter().all(|v| *v) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !ready[i] {
                ready[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(Instant::now() < end, "actual initial DATA setup incomplete");
    }
    (f, controls, configs)
}
fn wait(mut predicate: impl FnMut() -> bool, parent: &CancellationToken) {
    while !predicate() {
        parent.check().unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
}
#[test]
fn native_hot_recovery_queued_read_handoff_and_unrelated_range_progress() {
    let (f, mut controls, configs) = opened();
    let (coordinator, host, runtime, _) = controls[0].test_hot_recovery_bindings().unwrap();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
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
    let other = topology
        .ranges
        .values()
        .find(|r| r.leader != node(0))
        .unwrap();
    let other_key = (0..10_000)
        .map(|n| format!("other-range-progress-{n}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == other.id
        })
        .unwrap();
    let held_parent = budget(5000);
    let held = coordinator
        .reserve_native_point_read(range.id, &held_parent)
        .unwrap();
    let (acquired_tx, acquired_rx) = mpsc::sync_channel(1);
    let release = Arc::new(AtomicBool::new(false));
    let worker_release = release.clone();
    let coor = coordinator.clone();
    let selected = range.clone();
    let recovery = budget(5000);
    let worker_parent = recovery.clone();
    let worker = std::thread::spawn(move || -> Result<()> {
        let reservation =
            coor.reserve_hot_native_recovery(&runtime, &host, &selected, &worker_parent)?;
        acquired_tx.send(()).unwrap();
        while !worker_release.load(Ordering::Acquire) {
            worker_parent.check()?;
            std::thread::sleep(Duration::from_millis(2));
        }
        drop(reservation);
        Ok(())
    });
    wait(
        || coordinator.test_hot_waiter_and_active(range.id).0,
        &budget(1000),
    );
    assert!(coordinator
        .reserve_native_point_read(range.id, &budget(100))
        .err()
        .expect("queued hot recovery must refuse a new read")
        .to_string()
        .contains("queued hot recovery"));
    drop(held);
    acquired_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("queued repair must get the actual slot before new retrying reads");
    let after = coordinator.test_hot_waiter_and_active(range.id);
    assert!(
        after.0
            && after
                .1
                .as_deref()
                .is_some_and(|id| id.starts_with("hot-recovery:"))
    );
    // A SAME public client still uses a different actual ready range. No
    // global dispatcher/read-ready switch is used to favor the repair.
    let value =
        serde_json::json!({"independent":"entire unaffected range value","nested":[null,17]});
    assert!(gateway.append("items", &other_key, value.clone()).unwrap());
    assert_eq!(
        gateway.get_strong("items", &other_key).unwrap(),
        Some(value)
    );
    for _ in 0..8 {
        assert!(coordinator
            .reserve_native_point_read(range.id, &budget(50))
            .err()
            .expect("queued hot recovery must refuse a new read")
            .to_string()
            .contains("queued hot recovery"));
    }
    release.store(true, Ordering::Release);
    worker.join().unwrap().unwrap();
    assert!(!coordinator.test_hot_waiter_and_active(range.id).0);
    drop(
        coordinator
            .reserve_native_point_read(range.id, &budget(200))
            .unwrap(),
    );
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
#[test]
fn native_hot_recovery_cancel_waiter_preserves_actual_other_owner() {
    let (_f, mut controls, _) = opened();
    let (coordinator, host, runtime, _) = controls[0].test_hot_recovery_bindings().unwrap();
    let range = _f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let held_parent = budget(5000);
    let held = coordinator
        .reserve_native_point_read(range.id, &held_parent)
        .unwrap();
    let original = coordinator.test_hot_waiter_and_active(range.id).1.unwrap();
    let parent = budget(5000);
    let worker_parent = parent.clone();
    let coor = coordinator.clone();
    let selected = range.clone();
    let worker = std::thread::spawn(move || {
        coor.reserve_hot_native_recovery(&runtime, &host, &selected, &worker_parent)
            .map(drop)
    });
    wait(
        || coordinator.test_hot_waiter_and_active(range.id).0,
        &budget(1000),
    );
    parent.cancel();
    assert!(worker.join().unwrap().is_err());
    let after = coordinator.test_hot_waiter_and_active(range.id);
    assert!(!after.0);
    assert_eq!(after.1.as_deref(), Some(original.as_str()));
    assert!(
        coordinator
            .reserve_native_point_read(range.id, &budget(30))
            .is_err(),
        "cancel must not clear the actual held reader"
    );
    drop(held);
    drop(
        coordinator
            .reserve_native_point_read(range.id, &budget(200))
            .unwrap(),
    );
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
#[test]
fn native_hot_recovery_finite_open_never_starts_process_worker() {
    let (_f, mut controls, _) = opened();
    assert!(controls.iter().all(|c| !c.test_hot_worker_started()));
    let canceled = budget(5000);
    canceled.cancel();
    assert!(controls[0].start_automatic_metadata(&canceled).is_err());
    assert!(!controls[0].test_hot_worker_started());
    let process = CancellationToken::uncancelable();
    let mut supervisor = controls[0].start_automatic_metadata(&process).unwrap();
    assert!(controls[0].test_hot_worker_started());
    supervisor.stop(&budget(5000)).unwrap();
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
#[test]
fn native_hot_recovery_activation_rpc_refuses_unprepared_and_wrong_actual_leader() {
    let (f, mut controls, _) = opened();
    let (_, _, _, leader) = controls[0].test_hot_recovery_bindings().unwrap();
    let (_, _, _, follower) = controls[1].test_hot_recovery_bindings().unwrap();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let scope = controls[0]
        .test_actual_hot_scope(&range, &budget(3000))
        .unwrap();
    let before = f.hosts[0]
        .seed
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let root = f.hosts[0].seed.database.read().data_path().to_owned();
    let catalog = root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let bytes = std::fs::read(&catalog).unwrap();
    let request = crate::distribution_range_recovery::NativeCompletionRequest {
        scope: scope.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 1500,
    };
    let error = leader
        .activate_native_recovery_observation(&node(0), &request, &budget(2500))
        .err()
        .expect("unprepared state cannot create actual activation");
    assert!(
        error
            .to_string()
            .contains("distinct durable completion quorum unavailable"),
        "wrong activation refusal/precondition: {error}"
    );
    let request = crate::distribution_range_recovery::NativeCompletionRequest {
        scope,
        challenge: Uuid::now_v7(),
        budget_ms: 1500,
    };
    let error = follower
        .activate_native_recovery_observation(&node(0), &request, &budget(2500))
        .err()
        .expect("actual follower is not current data leader");
    assert!(
        error.to_string().contains("metadata recovery-role"),
        "wrong authenticated-caller refusal: {error}"
    );
    assert_eq!(
        std::fs::read(catalog).unwrap(),
        bytes,
        "denied incoming activation changed catalogue"
    );
    assert_eq!(
        f.hosts[0]
            .seed
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
}
#[test]
fn native_hot_recovery_original_writer_replacement_refuses_without_reserving_or_clearing() {
    let (f, mut controls, _) = opened();
    let (coordinator, host, runtime, _) = controls[0].test_hot_recovery_bindings().unwrap();
    let range = f.hosts[0]
        .seed
        .spec
        .initial_ranges
        .iter()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let database = &f.hosts[0].seed.database;
    let before = database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let original = host.binding_diagnostic();
    let replacement = controls[0]
        .test_replace_initial_data_writer_after_identity_check(&node(0), &budget(5000))
        .unwrap();
    let current = replacement.guard.binding_diagnostic();
    assert_ne!(
        original["persisted_policy_registration_generation"],
        current["persisted_policy_registration_generation"]
    );
    assert_eq!(
        host.binding_diagnostic()["observed_bound_registration_active"],
        false
    );
    let root = database.read().data_path().to_owned();
    let catalog = root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let bytes = std::fs::read(&catalog).unwrap();
    let token = budget(1000);
    let error = coordinator
        .reserve_hot_native_recovery(&runtime, &host, &range, &token)
        .err()
        .expect("old actual original writer cannot queue recovery");
    token.check().unwrap();
    assert!(
        error
            .to_string()
            .contains("native writer hook lifetime ended")
            || error
                .to_string()
                .contains("native writer hook/open is no longer current"),
        "wrong original writer refusal: {error}"
    );
    assert_eq!(
        coordinator.test_hot_waiter_and_active(range.id),
        (false, None)
    );
    assert_eq!(std::fs::read(catalog).unwrap(), bytes);
    assert_eq!(
        database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    replacement
        .guard
        .require_for_coordinator(&database.read(), &range, &replacement.coordinator)
        .unwrap();
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    replacement
        .guard
        .require_for_coordinator(&database.read(), &range, &replacement.coordinator)
        .unwrap();
    replacement
        .guard
        .retire_if_current(&database.read(), &replacement.coordinator)
        .unwrap();
}
#[test]
fn native_hot_recovery_retained_original_store_replace_restore_revokes_held_reservation() {
    let (f, mut controls, configs) = opened();
    let (coordinator, host, runtime, _) = controls[0].test_hot_recovery_bindings().unwrap();
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
    let range = topology
        .ranges
        .values()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let key = (0..10_000)
        .map(|i| format!("actual-hot-slot-ack-{i}"))
        .find(|key| {
            topology
                .range_for_key(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", key).unwrap(),
                )
                .unwrap()
                .id
                == range.id
        })
        .unwrap();
    let gateway = crate::ClusterEventClient::new(&configs[0], topology)
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let value =
        serde_json::json!({"actual":"full old ACK survives slot refusal","nested":[null,1,{}]});
    assert!(gateway.append("items", &key, value.clone()).unwrap());
    assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(value));
    let id = crate::cluster_event_record_id("items", &key).unwrap();
    let database = &f.hosts[0].seed.database;
    let row = database
        .read()
        .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap()
        .unwrap();
    let before = database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    let root = database.read().data_path().to_owned();
    let catalog = root.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
    let bytes = std::fs::read(&catalog).unwrap();
    let token = budget(5000);
    let held = coordinator
        .reserve_hot_native_recovery(&runtime, &host, &range, &token)
        .unwrap();
    held.require_actual_store_slot().unwrap();
    let active = coordinator.test_hot_waiter_and_active(range.id);
    assert!(
        active.0
            && active
                .1
                .as_deref()
                .is_some_and(|id| id.starts_with("hot-recovery:"))
    );
    let original_open = f.hosts[0]
        ._service
        .lock()
        .native_initial_listener_store_open_id();
    let other_root = tempfile::tempdir().unwrap();
    let replacement = crate::distribution_range_consensus::RangeWriteStore::open(
        other_root.path(),
        f.plan.topology.cluster_id.clone(),
        node(0),
        true,
    )
    .unwrap();
    let old = f.hosts[0]
        ._service
        .lock()
        .test_replace_native_range_write_store(replacement);
    let old_lifetime = old.listener_open_guard();
    old_lifetime.require_live().unwrap();
    assert_eq!(old.recovery_store_open_id(), original_open);
    old.native_writer_host_state()
        .unwrap()
        .require_store(&root, original_open)
        .unwrap();
    let foreign_slot = f.hosts[0]
        ._service
        .lock()
        .native_replica_listener_store_open_guard();
    foreign_slot.require_live().unwrap();
    {
        // The necessary refusal uses the captured atomic SLOT token, even
        // while the actual service is held and the old Store/writer stay live.
        let _service = f.hosts[0]._service.lock();
        let error = held.require_actual_store_slot().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("actual service range Store slot ended or changed"),
            "wrong original slot refusal: {error}"
        );
    }
    token.check().unwrap();
    assert_eq!(coordinator.test_hot_waiter_and_active(range.id), active);
    assert_eq!(std::fs::read(&catalog).unwrap(), bytes);
    foreign_slot.require_live().unwrap();
    let foreign = f.hosts[0]
        ._service
        .lock()
        .test_replace_native_range_write_store(old);
    let foreign_lifetime = foreign.listener_open_guard();
    let restored_slot = f.hosts[0]
        ._service
        .lock()
        .native_replica_listener_store_open_guard();
    assert_eq!(restored_slot.id(), original_open);
    restored_slot.require_live().unwrap();
    old_lifetime.require_live().unwrap();
    foreign_lifetime.require_live().unwrap();
    assert!(foreign_slot.require_live().is_err());
    let error = held.require_actual_store_slot().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("actual service range Store slot ended or changed"),
        "restoring the same original Store/UUID must not revive captured slot: {error}"
    );
    token.check().unwrap();
    assert_eq!(coordinator.test_hot_waiter_and_active(range.id), active);
    assert_eq!(
        database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    assert_eq!(
        database
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .as_deref(),
        Some(row.as_ref())
    );
    assert_eq!(std::fs::read(&catalog).unwrap(), bytes);
    host.require_for_coordinator(&database.read(), &range, &coordinator)
        .unwrap();
    drop(held);
    assert_eq!(
        coordinator.test_hot_waiter_and_active(range.id),
        (false, None)
    );
    // Exact reservation cleanup never clears either actual Store slot/object
    // or the original writer hook. ABA restores objects, never old admission.
    restored_slot.require_live().unwrap();
    foreign_lifetime.require_live().unwrap();
    old_lifetime.require_live().unwrap();
    host.require_for_coordinator(&database.read(), &range, &coordinator)
        .unwrap();
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    drop(foreign);
}
