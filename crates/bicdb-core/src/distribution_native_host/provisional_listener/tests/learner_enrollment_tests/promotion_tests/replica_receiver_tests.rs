//! Actual registered fifth backend; no INITIAL launch/genesis/data grant.
use super::*;
use crate::distribution_native_read::{
    NativeEventGetRequest, NativeEventGetResult, NativeEventReadMode,
};

#[test]
fn native_registered_replica_receiver_actual_tls_preserves_owner_and_refuses_blank_app_strong() {
    let f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("registered-replica-receiver");
    let token = CancellationToken::uncancelable();
    let configuration = config(&f);
    let id = start_owned(&f, &mut host);
    let readiness = ready(&f, &token);
    let (fresh, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&readiness, &fresh, business, &configuration, 61, &token)
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    let reply = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(reply.success && reply.match_index >= entry.index);
    assert_eq!(
        reply.native_receiver.as_ref().unwrap().metadata_store_id(),
        id
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &reply)
        .unwrap();
    let business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &token)
        .unwrap();
    let current = own.committed_topology(business).unwrap().clone();
    let member = &current.nodes[&node(4)];
    let mut own_config = configuration;
    own_config.node_id = member.id.clone();
    own_config.node_address = member.address.clone();
    own_config.node_incarnation = member.incarnation;
    own_config.node_capacity_bytes = member.capacity_bytes;
    own_config.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
    own_config.transport = f.networks[4].clone();
    host.enable_metadata_control(&own, business, own_config, &token)
        .unwrap();
    let service = host.test_service();
    let metadata = host.test_owned_metadata().unwrap();
    let store = service.lock().native_replica_listener_store_open_guard();
    let open = factory.database.read().incarnation_id().to_string();
    let range = current.ranges.values().next().unwrap().clone();
    let before = factory
        .database
        .read()
        .native_range_prefix_observation(range.id)
        .unwrap();
    assert_eq!(
        before.native_applied_index, None,
        "blank learner is UNKNOWN, never native0"
    );
    assert!(!factory.database.read().commit_admission_ready());
    let client = crate::TcpClusterRelocationTransport::new(
        current.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(current.clone())),
        f.networks[0].clone(),
    )
    .unwrap();
    let get = NativeEventGetRequest {
        version: 1,
        mode: NativeEventReadMode::Strong,
        business_request_id: Uuid::now_v7(),
        challenge: Uuid::now_v7(),
        range_id: range.id,
        range_epoch: range.epoch,
        namespace: "items".into(),
        key: "not-a-blank-absence-proof".into(),
        budget_ms: 1000,
    };
    assert!(
        client
            .native_event_get_cancellable(&token, &node(4), &get)
            .is_err(),
        "old immutable metadata-only listener must refuse native DATA requests"
    );
    let canceled = token.child();
    canceled.cancel();
    assert!(host
        .open_registered_replica_receiver(&own, business, &canceled)
        .is_err());
    assert!(host.replica_receiver_status(&token).unwrap().is_none());
    host.open_registered_replica_receiver(&own, business, &token)
        .unwrap();
    let status = host.replica_receiver_status(&token).unwrap().unwrap();
    assert!(status.receiver_ready);
    assert!(!status.data_serving);
    assert_eq!(status.range_store_open_id, store.id());
    assert_eq!(status.metadata_store_id, id);
    assert_eq!(status.database_incarnation, open);
    assert_eq!(status.birth_id, factory.actual_birth().birth_id());
    assert!(Arc::ptr_eq(&service, &host.test_service()));
    assert!(Arc::ptr_eq(&metadata, &host.test_owned_metadata().unwrap()));
    store.require_live().unwrap();
    assert!(
        !factory.database.read().commit_admission_ready(),
        "opening a replica receiver must not install writer authority"
    );
    let reply = client
        .native_event_get_cancellable(&token, &node(4), &get)
        .unwrap();
    assert!(
        matches!(reply.result, NativeEventGetResult::Unavailable { .. }),
        "blank receiver cannot return a successful value or absence"
    );
    for operation in [
        crate::ClusterEventOperation::Get {
            namespace: "items".into(),
            key: get.key.clone(),
        },
        crate::ClusterEventOperation::Append {
            namespace: "items".into(),
            key: get.key.clone(),
            value: serde_json::json!({"must":"refuse"}),
        },
        crate::ClusterEventOperation::Scan {
            namespace: "items".into(),
            after: None,
            limit: 1,
        },
    ] {
        assert!(client
            .application_event(
                &node(4),
                crate::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation
                }
            )
            .is_err());
    }
    assert_eq!(
        factory
            .database
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap(),
        before
    );
    assert!(!factory.database.read().commit_admission_ready());
    // Metadata bootstrap and owned voting backend continue on SAME service;
    // no backend stop/reopen or new UUID is substituted for listener rotation.
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        current.cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )
    .unwrap();
    let snapshot = bootstrap.fetch_snapshot(&node(4), &member.address).unwrap();
    assert_eq!(snapshot.topology, current);
    assert_eq!(
        metadata
            .lock()
            .registered_native_metadata_member_store_id()
            .unwrap(),
        id
    );
    host.open_registered_replica_receiver(&own, business, &token)
        .unwrap();
    assert_eq!(
        host.replica_receiver_status(&token)
            .unwrap()
            .unwrap()
            .range_store_open_id,
        store.id()
    );
    // Actual service-slot replacement while the OLD genuine object stays
    // alive. A Weak<old Store> alone would incorrectly remain live here.
    // This is a different root, so same-root DirectoryLock exclusion cannot
    // stand in for the receiver's current-slot check.
    let replacement_root = tempfile::tempdir().unwrap();
    let replacement = crate::distribution_range_consensus::RangeWriteStore::open(
        replacement_root.path(),
        current.cluster_id.clone(),
        node(4),
        true,
    )
    .unwrap();
    let retained_old = service
        .lock()
        .test_replace_native_range_write_store(replacement);
    let retained_lifetime = retained_old.listener_open_guard();
    retained_lifetime.require_live().unwrap();
    assert_eq!(retained_old.recovery_store_open_id(), store.id());
    assert!(store.require_live().is_err());
    assert!(
        !host
            .replica_receiver_status(&token)
            .unwrap()
            .unwrap()
            .receiver_ready
    );
    {
        // Rejection precedes the service latch even with the old Store live.
        let _busy_service = service.lock();
        assert!(client
            .native_event_get_cancellable(&token, &node(4), &get)
            .is_err());
    }
    // Test cleanup restores the exact old object, NOT its revoked binding.
    let replaced = service
        .lock()
        .test_replace_native_range_write_store(retained_old);
    assert!(store.require_live().is_err(), "no binding ABA on restore");
    retained_lifetime.require_live().unwrap();
    drop(replaced);
    drop(metadata);
    drop(service);
    host.stop_owned_metadata(&token).unwrap();
    host.shutdown().unwrap();
    f.shutdown();
}
