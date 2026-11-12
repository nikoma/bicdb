//! Actual INITIAL TLS/real election/APP ACK, actual native audit publication.
//! The RF3 same-registry reaffirmation controls storage/lifetime; the separate
//! public six-case CLI regression supplies actual fifth-registry addition.
use super::*;

fn ready() -> (
    Fixture,
    Vec<NativeMetadataControlHost>,
    crate::ClusterEventClient,
    ClusterTopology,
) {
    let (f, mut controls, configs) = actual_initial_metadata_controls();
    let until = Instant::now() + Duration::from_secs(20);
    let mut opened = [false; 3];
    while !opened.iter().all(|v| *v) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !opened[i] {
                opened[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(
            Instant::now() < until,
            "actual DATA owners incomplete: {opened:?}"
        );
    }
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
    (f, controls, gateway, topology)
}
fn selected_key(topology: &ClusterTopology) -> String {
    // Internal kernel fault targets actual n0. Public CLI acceptance continues
    // to use fixed ordinary keys/SAME gateway without application shard logic.
    (0..10_000)
        .map(|i| format!("registry-kernel-ack-{i}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .leader
                == node(0)
        })
        .unwrap()
}
fn point(database: &BicDb, range: crate::RangeId) -> (u64, String, String, u64) {
    let actual = database.current_native_range_source_lineage(range).unwrap();
    // B is read from the actual local visibility receipt, never cast from R.
    (
        actual.range_index(),
        actual.logical_genesis_sha256().into(),
        actual.logical_history_sha256().into(),
        actual.actual_local_visibility_sequence(),
    )
}
#[test]
fn native_initial_data_registry_actual_native_publication_error_fresh_retry_and_cold_no_grant() {
    let (f, mut controls, gateway, topology) = ready();
    let key = selected_key(&topology);
    let expected =
        serde_json::json!({"entire":"actual before registry audit", "nested":[null,17,{}]});
    assert!(gateway.append("items", &key, expected.clone()).unwrap());
    assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(expected));
    let id = crate::cluster_event_record_id("items", &key).unwrap();
    let range = topology
        .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap()
        .id;
    let database = &f.hosts[0].seed.database;
    let before = point(&database.read(), range);
    let row = database
        .read()
        .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap()
        .unwrap();
    let binding = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap()
        .writer_binding_diagnostic
        .unwrap();
    database
        .read()
        .test_fail_next_native_writer_registry_after_readback();
    let error = controls[0]
        .test_force_initial_data_registry_refresh(&budget(5000))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("after actual durable native readback"),
        "{error}"
    );
    let stored = database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .unwrap();
    assert_eq!(stored["sequence"], 1);
    let pending = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap();
    assert!(!pending.writer_policy_current);
    assert_eq!(
        pending.writer_binding_diagnostic.as_ref().unwrap()["actual_verified_registry_extension"]
            ["publication_pending"],
        true
    );
    assert_eq!(point(&database.read(), range), before);
    assert_eq!(
        database
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .as_deref(),
        Some(row.as_ref())
    );
    assert!(
        gateway.get_strong("items", &key).is_err(),
        "unpublished audit cannot admit Strong"
    );
    controls[0]
        .test_force_initial_data_registry_refresh(&budget(5000))
        .unwrap();
    assert_eq!(
        database
            .read()
            .test_current_native_writer_registry_audit()
            .unwrap()
            .unwrap(),
        stored,
        "fresh MQ retry reuses ORIGINAL durable audit, not a context rewrite/new sequence"
    );
    let after = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap();
    assert!(after.writer_policy_current);
    let current = after.writer_binding_diagnostic.unwrap();
    for field in [
        "persisted_policy_coordinator_id",
        "persisted_policy_registration_generation",
        "persisted_policy_database_open_incarnation",
        "persisted_policy_range_store_open_id",
        "persisted_policy_checksum_sha256",
        "retained_owner_coordinator_id",
    ] {
        assert_eq!(binding[field], current[field]);
    }
    assert_eq!(point(&database.read(), range), before);
    assert_eq!(
        gateway.get_strong("items", &key).unwrap(),
        Some(row.metadata.clone())
    );
    let canceled = budget(1000);
    canceled.cancel();
    assert!(controls[0]
        .test_force_initial_data_registry_refresh(&canceled)
        .is_err());
    assert_eq!(
        database
            .read()
            .test_current_native_writer_registry_audit()
            .unwrap()
            .unwrap(),
        stored
    );

    let path = database.read().data_path().to_path_buf();
    let spec = f.hosts[0].seed.spec.clone();
    let ended = Arc::downgrade(database);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    drop(gateway);
    drop(controls);
    let Fixture { hosts, _root, .. } = f;
    drop(hosts);
    let until = Instant::now() + Duration::from_secs(5);
    while ended.upgrade().is_some() {
        assert!(
            Instant::now() < until,
            "actual original DB owners did not drain"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let (database, _) = BicDb::open_range_image_database(&path, db_config(), &spec).unwrap();
    assert!(
        !database.commit_admission_ready(),
        "cold audit is not an installed writer/Ready grant"
    );
    database.schema_compatibility_fingerprint().unwrap();
    assert_eq!(
        database
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .as_deref(),
        Some(row.as_ref())
    );
    assert_eq!(point(&database, range), before);
    drop(database);
    drop(_root);
}

#[test]
fn native_initial_data_registry_original_writer_replacement_refuses_before_new_audit() {
    let (f, mut controls, gateway, topology) = ready();
    let key = selected_key(&topology);
    let expected = serde_json::json!({"old":"actual writer replacement negative", "all":[1,null]});
    assert!(gateway.append("items", &key, expected.clone()).unwrap());
    assert_eq!(gateway.get_strong("items", &key).unwrap(), Some(expected));
    let id = crate::cluster_event_record_id("items", &key).unwrap();
    let selected = topology
        .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap();
    let range = selected.id;
    let database = &f.hosts[0].seed.database;
    let before = point(&database.read(), range);
    let row = database
        .read()
        .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap()
        .unwrap();
    let old = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap()
        .writer_binding_diagnostic
        .unwrap();
    assert_eq!(old["observed_bound_registration_active"], true);
    // Produce the actual sealed preparation while original MQ/slot/writer
    // are live. Replacement must not relabel this earlier successful capture.
    let original = budget(5000);
    let captured = controls[0]
        .test_prepare_initial_data_registry_refresh(&original)
        .unwrap();
    let replacement = controls[0]
        .test_replace_initial_data_writer_after_identity_check(&node(0), &budget(5000))
        .unwrap();
    original.check().unwrap();
    let replacement_binding = replacement.guard.binding_diagnostic();
    assert_ne!(
        old["persisted_policy_registration_generation"],
        replacement_binding["persisted_policy_registration_generation"]
    );
    assert_ne!(
        old["persisted_policy_coordinator_id"],
        replacement_binding["persisted_policy_coordinator_id"]
    );
    assert_eq!(
        replacement_binding["observed_bound_registration_active"],
        true
    );
    assert_eq!(replacement_binding["observed_binding_lifetime_live"], true);
    let revoked = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap()
        .writer_binding_diagnostic
        .unwrap();
    assert_eq!(
        revoked["persisted_policy_registration_generation"],
        old["persisted_policy_registration_generation"]
    );
    assert_ne!(revoked["observed_bound_registration_active"], true);
    assert_eq!(revoked["observed_weak_directory_alive"], true);
    assert_eq!(revoked["observed_binding_lifetime_live"], false);
    assert!(database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());

    // First distinguish the ACTUAL earlier ingress fence: a new automatic
    // fresh-MQ attempt reaches the revoked old listener and must refuse there.
    let fresh = budget(5000);
    let live_error = controls[0]
        .test_force_initial_data_registry_refresh(&fresh)
        .unwrap_err();
    fresh.check().unwrap();
    assert!(
        live_error
            .to_string()
            .contains("native writer hook lifetime ended")
            || live_error
                .to_string()
                .contains("native writer hook/open is no longer current"),
        "{live_error}"
    );
    // Also exercise the kernel independently with the genuinely captured
    // preparation. No new network proof or production fence is bypassed.
    original.check().unwrap();
    let kernel_error = controls[0]
        .test_apply_initial_data_registry_refresh(captured, &original)
        .unwrap_err();
    original.check().unwrap();
    assert!(
        kernel_error
            .to_string()
            .contains("current Store/writer object changed"),
        "{kernel_error}"
    );
    assert!(database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());
    assert_eq!(point(&database.read(), range), before);
    assert_eq!(
        database
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .as_deref(),
        Some(row.as_ref())
    );
    replacement
        .guard
        .require_for_coordinator(&database.read(), selected, &replacement.coordinator)
        .unwrap();
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    // Old cleanup cannot retire the replacement. Verify the actual current
    // native hook, not only a saved generation string, before retiring it.
    replacement
        .guard
        .require_for_coordinator(&database.read(), selected, &replacement.coordinator)
        .unwrap();
    assert_eq!(
        replacement.guard.binding_diagnostic()["observed_bound_registration_active"],
        true
    );
    assert_eq!(point(&database.read(), range), before);
    assert_eq!(
        database
            .read()
            .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .as_deref(),
        Some(row.as_ref())
    );
    assert!(replacement
        .guard
        .retire_if_current(&database.read(), &replacement.coordinator)
        .unwrap());
}

#[test]
fn native_initial_data_registry_original_cancellation_and_retained_old_store_slot_refuse() {
    let (f, mut controls, gateway, _topology) = ready();
    let database = &f.hosts[0].seed.database;
    assert!(database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());
    let original = budget(5000);
    let prepared = controls[0]
        .test_prepare_initial_data_registry_refresh(&original)
        .unwrap();
    original.cancel();
    let fresh = budget(5000);
    assert!(controls[0]
        .test_apply_initial_data_registry_refresh(prepared, &fresh)
        .is_err());
    fresh.check().unwrap();
    assert!(database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());

    let original = budget(5000);
    let prepared = controls[0]
        .test_prepare_initial_data_registry_refresh(&original)
        .unwrap();
    let foreign_root = tempfile::tempdir().unwrap();
    let replacement = crate::distribution_range_consensus::RangeWriteStore::open(
        foreign_root.path(),
        f.plan.topology.cluster_id.clone(),
        node(0),
        true,
    )
    .unwrap();
    let service = &f.hosts[0]._service;
    let old = service
        .lock()
        .test_replace_native_range_write_store(replacement);
    let old_object_lifetime = old.listener_open_guard();
    old_object_lifetime.require_live().unwrap();
    original.check().unwrap();
    let error = controls[0]
        .test_apply_initial_data_registry_refresh(prepared, &original)
        .unwrap_err();
    original.check().unwrap();
    assert!(
        error
            .to_string()
            .contains("actual service range Store slot ended or changed"),
        "{error}"
    );
    old_object_lifetime.require_live().unwrap();
    assert!(database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());
    let foreign = service.lock().test_replace_native_range_write_store(old);
    // Restoration creates another actual slot, never revives captured proof.
    old_object_lifetime.require_live().unwrap();
    drop(foreign);
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    drop(gateway);
}
