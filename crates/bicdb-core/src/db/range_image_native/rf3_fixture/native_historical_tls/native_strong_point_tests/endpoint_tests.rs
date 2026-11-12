//! Actual mTLS before-latch Get dispatch and whole-deadline host routing. The
//! election change is declared; recovery/ACKs/snapshots are actual producers.
use super::*;
use crate::distribution_native_read::{
    NativeEventGetReply, NativeEventGetRequest, NativeEventGetResult,
    NativeEventGetUnavailableCode, NativeEventReadMode,
};
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeReadyRequest};
use serde_json::{json, Value};

impl EnforcedNetwork {
    fn event_range_key(&self, prefix: &str) -> (RangeDescriptor, String) {
        let range = self.ranges()[0].clone();
        let key = (0..100_000)
            .map(|i| format!("{prefix}-{i}"))
            .find(|key| {
                self.native
                    .topology
                    .read()
                    .range_for_key(
                        crate::CLUSTER_EVENTS_COLLECTION,
                        &crate::cluster_event_record_id("tenant", key).unwrap(),
                    )
                    .unwrap()
                    .id
                    == range.id
            })
            .unwrap();
        (range, key)
    }
    fn append_event(&self, key: &str, value: Value) {
        let id = crate::cluster_event_record_id("tenant", key).unwrap();
        let range = self
            .native
            .topology
            .read()
            .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap()
            .clone();
        let response = self.clients[&node("n1")]
            .application_event(
                &range.leader,
                crate::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation: crate::ClusterEventOperation::Append {
                        namespace: "tenant".into(),
                        key: key.into(),
                        value,
                    },
                },
            )
            .unwrap();
        assert!(matches!(
            response,
            crate::ClusterEventResponse::Appended { inserted: true }
        ));
    }
    fn event_request(&self, key: &str, range: &RangeDescriptor) -> NativeEventGetRequest {
        NativeEventGetRequest {
            version: 1,
            mode: NativeEventReadMode::Strong,
            business_request_id: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            range_id: range.id,
            range_epoch: range.epoch,
            namespace: "tenant".into(),
            key: key.into(),
            budget_ms: 3500,
        }
    }
    fn wire_get(&self, destination: &str, request: &NativeEventGetRequest) -> NativeEventGetReply {
        self.clients[&node("n1")]
            .native_event_get_cancellable(
                &CancellationToken::uncancelable(),
                &node(destination),
                request,
            )
            .unwrap()
    }
    fn event_client(
        &self,
        topology: ClusterTopology,
        timeout: Duration,
    ) -> crate::ClusterEventClient {
        let id = node("n1");
        let config = crate::DistributionConfig {
            enabled: true,
            cluster_id: topology.cluster_id.clone(),
            node_id: id.clone(),
            node_address: topology.nodes[&id].address.clone(),
            transport: ClusterNetworkTransportConfig {
                connect_timeout_ms: 50,
                io_timeout_ms: 4000,
                max_frame_bytes: 4 * 1024 * 1024,
                max_inbound_connections: 16,
                dev_localhost_plaintext: false,
                tls: Some(ReplicationTlsConfig {
                    cert_path: self._keys.path().join("n1.pem"),
                    key_path: self._keys.path().join("n1.key"),
                    ca_path: self._keys.path().join("ca.pem"),
                    require_client_cert: true,
                    dev_localhost_plaintext: false,
                }),
            },
            ..Default::default()
        };
        crate::ClusterEventClient::new(&config, topology)
            .unwrap()
            .with_request_timeout(timeout)
            .unwrap()
    }
    fn refresh_endpoint_host(&mut self, id: ClusterNodeId) {
        let business = Uuid::now_v7();
        let observed = self.clients[&id]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let policy = {
            let service = self.native.services[&id].lock();
            let db = self.native.databases[&id].read();
            service
                .prepare_native_host_writer_enrollment(&db, observed, business)
                .unwrap()
        };
        let guard = self.native.databases[&id]
            .read()
            .install_enrolled_native_coordinator(self.coordinators[&id].clone(), policy)
            .unwrap();
        self.hosts.insert(id, Arc::new(guard));
    }
}
fn actual_value(
    reply: NativeEventGetReply,
) -> (
    Option<Value>,
    crate::distribution_native_read::NativeStrongPointReadAudit,
) {
    match reply.result {
        NativeEventGetResult::Value { value, audit, .. } => (value.into_option(), audit),
        other => panic!("actual Strong endpoint returned {other:?}"),
    }
}

#[test]
fn native_event_endpoint_actual_tls_value_null_object_and_absence_have_distinct_presence() {
    let fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let ranges = fixture.ranges();
    fixture
        .native
        .append(&fixture.key(ranges[1].id, "other-endpoint-a"))
        .unwrap();
    fixture
        .native
        .append(&fixture.key(ranges[1].id, "other-endpoint-b"))
        .unwrap();
    let (range, key) = fixture.event_range_key("value");
    fixture.append_event(&key, json!({"saved":true}));
    let (value, audit) = actual_value(fixture.wire_get("n0", &fixture.event_request(&key, &range)));
    assert_eq!(value, Some(json!({"saved":true})));
    assert_eq!(audit.range_index, 1);
    assert!(audit.target_local_visibility_db_sequence >= 3);
    assert_ne!(
        audit.first_metadata_context_id,
        audit.final_metadata_context_id
    );
    let (_, null_key) = fixture.event_range_key("null");
    fixture.append_event(&null_key, Value::Null);
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&null_key, &range))).0,
        Some(Value::Null)
    );
    let (_, object_key) = fixture.event_range_key("object");
    fixture.append_event(&object_key, json!({}));
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&object_key, &range))).0,
        Some(json!({}))
    );
    let (_, absent) = fixture.event_range_key("absent");
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&absent, &range))).0,
        None
    );
    let client = fixture.event_client(
        fixture.native.topology.read().clone(),
        Duration::from_secs(5),
    );
    assert_eq!(
        client.get_strong("tenant", &null_key).unwrap(),
        Some(Value::Null)
    );
    assert_eq!(client.get("tenant", &object_key).unwrap(), Some(json!({})));
    assert_eq!(client.get_strong("tenant", &absent).unwrap(), None);
    fixture.idle();
}

#[test]
fn native_event_endpoint_actual_hostmq_nonleader_and_stale_hint_are_redirect_data_only() {
    let fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let (range, key) = fixture.event_range_key("redirect");
    fixture.append_event(&key, json!({"kept":true}));
    let request = fixture.event_request(&key, &range);
    let reply = fixture.wire_get("n2", &request);
    match reply.result {
        NativeEventGetResult::Redirect {
            route,
            metadata_context_id,
            metadata_term,
            metadata_read_index,
        } => {
            assert_eq!(route.leader, node("n0"));
            assert_eq!(route.range_id, range.id);
            assert_eq!(route.range_epoch, range.epoch);
            assert!(!metadata_context_id.is_nil());
            assert!(metadata_term > 0 && metadata_read_index > 0);
        }
        other => panic!("nonleader claimed a row/absence: {other:?}"),
    }
    let mut stale = request;
    stale.challenge = Uuid::now_v7();
    stale.range_epoch += 1;
    assert!(matches!(
        fixture.wire_get("n0", &stale).result,
        NativeEventGetResult::Redirect { .. }
    ));
    fixture.idle();
}

#[test]
fn native_event_endpoint_empty_slot_refuses_before_actual_db_and_service_guards_then_publishes() {
    let fixture = EnforcedNetwork::new_endpoint_mode(true, false);
    let (range, key) = fixture.event_range_key("unpublished");
    fixture.append_event(&key, json!({"present":true}));
    {
        let _service = fixture.native.services[&node("n0")].lock();
        let _db = fixture.native.databases[&node("n0")].write();
        let start = Instant::now();
        let reply = fixture.wire_get("n0", &fixture.event_request(&key, &range));
        assert!(matches!(
            reply.result,
            NativeEventGetResult::Unavailable {
                code: NativeEventGetUnavailableCode::MissingWriter,
                ..
            }
        ));
        assert!(start.elapsed() < Duration::from_secs(1));
        let business = Uuid::now_v7();
        fixture.clients[&node("n0")]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(2),
                &CancellationToken::uncancelable(),
            )
            .unwrap()
            .require_live_request(business)
            .unwrap();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.dispatchers[&id]
            .publish(&fixture.coordinators[&id], &fixture.hosts[&id])
            .unwrap();
    }
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&key, &range))).0,
        Some(json!({"present":true}))
    );
    fixture.idle();
}

#[test]
fn native_event_endpoint_actual_hook_refresh_requires_refreshed_dispatch_publication() {
    let mut fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let (range, key) = fixture.event_range_key("refresh");
    fixture.append_event(&key, json!({"kept":true}));
    fixture.refresh_endpoint_host(node("n0"));
    assert!(matches!(
        fixture
            .wire_get("n0", &fixture.event_request(&key, &range))
            .result,
        NativeEventGetResult::Unavailable { .. }
    ));
    fixture.idle();
    fixture.dispatchers[&node("n0")]
        .publish(
            &fixture.coordinators[&node("n0")],
            &fixture.hosts[&node("n0")],
        )
        .unwrap();
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&key, &range))).0,
        Some(json!({"kept":true}))
    );
    fixture.idle();
}

#[test]
fn native_event_endpoint_real_client_cancel_retains_actual_io_context_and_never_returns_absence() {
    let fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let (range, key) = fixture.event_range_key("cancel");
    fixture.append_event(&key, json!({"kept":true}));
    let barrier = fixture.runtimes[&node("n0")].test_hold_native_read_io();
    let request = fixture.event_request(&key, &range);
    let token = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(3));
    let worker_token = token.clone();
    let client = Arc::clone(&fixture.clients[&node("n1")]);
    let worker = std::thread::spawn(move || {
        client.native_event_get_cancellable(&worker_token, &node("n0"), &request)
    });
    let until = Instant::now() + Duration::from_secs(2);
    while !barrier.entered() {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.native.services[&node("n0")].try_lock().is_some());
    assert!(fixture.native.databases[&node("n0")].try_write().is_some());
    token.cancel();
    assert!(worker.join().unwrap().is_err());
    assert!(fixture.runtimes[&node("n0")].test_active_context_count() > 0);
    barrier.release();
    fixture.idle();
    assert_eq!(
        actual_value(fixture.wire_get("n0", &fixture.event_request(&key, &range))).0,
        Some(json!({"kept":true}))
    );
    fixture.idle();
}

#[test]
fn native_event_endpoint_explicit_strong_actual_born1_refuses_without_legacy_downgrade() {
    let fixture = NetworkFixture::new_ready_network(true, true, false);
    let range = fixture
        .current
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let request = NativeEventGetRequest {
        version: 1,
        mode: NativeEventReadMode::Strong,
        business_request_id: Uuid::now_v7(),
        challenge: Uuid::now_v7(),
        range_id: range.id,
        range_epoch: range.epoch,
        namespace: "tenant".into(),
        key: "missing".into(),
        budget_ms: 1000,
    };
    let reply = fixture.clients[&node("n1")]
        .native_event_get_cancellable(&CancellationToken::uncancelable(), &node("n1"), &request)
        .unwrap();
    assert!(matches!(
        reply.result,
        NativeEventGetResult::Unavailable {
            code: NativeEventGetUnavailableCode::LegacyUnsupported,
            ..
        }
    ));
    fixture.wait_idle();
}

#[test]
fn native_event_endpoint_stale_route_retries_until_actual_new_leader_ready_under_original_budget() {
    let mut fixture = EnforcedNetwork::new_endpoint_mode(true, true);
    let (old_range, key) = fixture.event_range_key("transfer");
    fixture.append_event(&key, json!({"acked":1}));
    let stale_topology = fixture.native.topology.read().clone();
    fixture.change_committed_leader(old_range.id);
    let range = fixture
        .native
        .topology
        .read()
        .range_by_id(old_range.id)
        .unwrap()
        .clone();
    fixture.refresh_actual_writer_policies();
    // Publish actual nonleader redirect slots but keep the new leader EMPTY
    // until real recovery completes, avoiding read/phase reservation races.
    for id in [node("n0"), node("n1")] {
        fixture.dispatchers[&id]
            .publish(&fixture.coordinators[&id], &fixture.hosts[&id])
            .unwrap();
    }
    fixture.dispatchers[&node("n2")].clear();
    let client = fixture.event_client(stale_topology, Duration::from_secs(20));
    let read_key = key.clone();
    let started = Instant::now();
    let worker = std::thread::spawn(move || client.get_strong("tenant", &read_key));
    let scope = fixture.actual_current_recovery_scope(&range);
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n2")]
            .prepare_native_ready_observation(
                &id,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n2")]
            .complete_native_recovery_observation(
                &id,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.idle();
    }
    let actor = fixture.clients[&node("n2")].local_metadata_read_participant();
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 3500,
                },
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.idle();
    }
    fixture.coordinators[&node("n2")]
        .recover_local_leadership(8)
        .unwrap();
    fixture.dispatchers[&node("n2")]
        .publish(
            &fixture.coordinators[&node("n2")],
            &fixture.hosts[&node("n2")],
        )
        .unwrap();
    assert_eq!(worker.join().unwrap().unwrap(), Some(json!({"acked":1})));
    assert!(started.elapsed() < Duration::from_secs(20));
    fixture.idle();
    // Activation above is the actual LOCAL Runtime API. No incoming
    // activation endpoint, automatic election, SQL or physical-HA claim.
}

#[test]
fn native_event_endpoint_routing_retries_do_not_renew_original_deadline_or_become_absence() {
    let fixture = EnforcedNetwork::new_endpoint_mode(true, false);
    let (range, key) = fixture.event_range_key("deadline");
    fixture.append_event(&key, json!({"acked":true}));
    let client = fixture.event_client(
        fixture.native.topology.read().clone(),
        Duration::from_millis(180),
    );
    let started = Instant::now();
    assert!(client.get_strong("tenant", &key).is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(fixture.native.databases[&node("n0")]
        .read()
        .get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("tenant", &key).unwrap()
        )
        .unwrap()
        .is_some());
    assert_eq!(
        fixture.native.databases[&node("n0")]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1
    );
    fixture.idle();
}
