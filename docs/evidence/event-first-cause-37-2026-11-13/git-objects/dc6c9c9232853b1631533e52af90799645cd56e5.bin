//! Real loopback TCP with mock RPC replies: routing/error unit coverage only.
//! No database commit, actual native authority or HA guarantee is exercised.
use super::*;

#[derive(Clone, Copy)]
enum Probe {
    Append,
    AppendConflictingDiscovery,
    Get,
    RefreshOnly,
}
fn failed_operation_then_discovery(probe: Probe) -> (BicDbError, usize, usize) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = crate::DistributionConfig {
        enabled: true,
        node_address: listener.local_addr().unwrap().to_string(),
        initial_ranges: 1,
        replication_factor: 1,
        transport: ClusterNetworkTransportConfig {
            connect_timeout_ms: 100,
            io_timeout_ms: 500,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = DistributionStore::initialize_at(root.path(), config, false, 1000).unwrap();
    let client = crate::ClusterEventClient::new(store.config(), store.topology().clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let max_frame = store.config().transport.max_frame_bytes;
    let mut conflict = ClusterBootstrapSnapshot {
        topology: store.topology().clone(),
        config_template: store.config().clone(),
        metadata_leader_id: None,
    };
    conflict
        .topology
        .nodes
        .values_mut()
        .next()
        .unwrap()
        .capacity_bytes += 1;
    conflict.validate().unwrap(); // Valid shape, conflicting SAME generation.
    let operations = Arc::new(AtomicUsize::new(0));
    let discoveries = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (seen_ops, seen_discovery, stopping) =
        (operations.clone(), discoveries.clone(), stop.clone());
    let worker = thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(5);
        while !stopping.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < until,
                "mock RPC client exceeded bounded request"
            );
            let mut stream = match listener.accept() {
                Ok((s, _)) => s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(e) => panic!("mock RPC accept: {e}"),
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let request: ClusterDataRequestEnvelope =
                recv_json_frame(&mut stream, max_frame).unwrap();
            let response = if matches!(probe, Probe::AppendConflictingDiscovery)
                && matches!(&request.request, ClusterDataRequest::FetchBootstrapSnapshot)
            {
                seen_discovery.fetch_add(1, Ordering::SeqCst);
                Ok(ClusterDataResponse::BootstrapSnapshot(conflict.clone()))
            } else {
                let (code, message) = match &request.request {
                    ClusterDataRequest::ApplicationEvent(event) => {
                        assert!(matches!(
                            probe,
                            Probe::Append | Probe::AppendConflictingDiscovery
                        ));
                        let crate::ClusterEventOperation::Append {
                            namespace,
                            key,
                            value,
                        } = &event.operation
                        else {
                            panic!("expected unchanged append")
                        };
                        assert_eq!(namespace, "routing-unit");
                        assert_eq!(key, "stable-operation-key");
                        assert_eq!(value, &serde_json::json!({"same":"payload"}));
                        seen_ops.fetch_add(1, Ordering::SeqCst);
                        (
                            "first_routed_operation",
                            "actual operation refusal before discovery",
                        )
                    }
                    ClusterDataRequest::NativeEventGet(get) => {
                        assert!(matches!(probe, Probe::Get));
                        assert_eq!(get.namespace, "routing-unit");
                        assert_eq!(get.key, "stable-operation-key");
                        assert!(!get.business_request_id.is_nil());
                        seen_ops.fetch_add(1, Ordering::SeqCst);
                        (
                            "first_routed_operation",
                            "actual operation refusal before discovery",
                        )
                    }
                    ClusterDataRequest::FetchBootstrapSnapshot
                    | ClusterDataRequest::FetchTopology => {
                        seen_discovery.fetch_add(1, Ordering::SeqCst);
                        (
                            "later_discovery_failure",
                            "mock member cannot supply topology",
                        )
                    }
                    other => panic!("unexpected mock request {other:?}"),
                };
                Err(ClusterDataRemoteError {
                    code: code.into(),
                    message: message.into(),
                })
            };
            // A final discovery exchange may expire at the SAME caller deadline.
            // Its socket outcome cannot alter the already received primary error.
            let _ = send_json_frame(
                &mut stream,
                &ClusterDataResponseEnvelope {
                    actual_native_source: None,
                    protocol_version: request.protocol_version,
                    cluster_id: request.cluster_id,
                    source_node_id: request.destination_node_id,
                    request_id: request.request_id,
                    response,
                },
                max_frame,
            );
        }
    });
    let result = match probe {
        Probe::Append | Probe::AppendConflictingDiscovery => client
            .append(
                "routing-unit",
                "stable-operation-key",
                serde_json::json!({"same":"payload"}),
            )
            .map(|_| ()),
        Probe::Get => client
            .get("routing-unit", "stable-operation-key")
            .map(|_| ()),
        Probe::RefreshOnly => client.refresh(),
    };
    stop.store(true, Ordering::SeqCst);
    worker.join().unwrap();
    (
        result.expect_err("mock replies cannot acknowledge this operation"),
        operations.load(Ordering::SeqCst),
        discoveries.load(Ordering::SeqCst),
    )
}
#[test]
fn event_client_append_preserves_actual_error_when_route_discovery_fails() {
    let (error, operations, discoveries) = failed_operation_then_discovery(Probe::Append);
    assert_eq!(
        operations, 1,
        "failed discovery must not issue another application command"
    );
    assert!(discoveries > 0);
    assert!(
        error.to_string().contains("first_routed_operation"),
        "{error:?}"
    );
    assert!(!error.to_string().contains("no event cluster member"));
}
#[test]
fn event_client_get_preserves_actual_error_when_route_discovery_exhausts_deadline() {
    let (error, operations, discoveries) = failed_operation_then_discovery(Probe::Get);
    assert_eq!(operations, 1);
    assert!(discoveries > 0);
    assert!(
        error.to_string().contains("first_routed_operation"),
        "{error:?}"
    );
    assert!(!error.to_string().contains("no member reached"));
}
#[test]
fn event_client_refresh_without_operation_keeps_no_member_error() {
    let (error, operations, discoveries) = failed_operation_then_discovery(Probe::RefreshOnly);
    assert_eq!(operations, 0);
    assert!(discoveries > 0);
    assert!(
        error
            .to_string()
            .contains("no event cluster member is reachable"),
        "{error:?}"
    );
}

#[test]
fn event_client_append_preserves_primary_when_conflicting_discovery_stops_retry() {
    let (error, operations, discoveries) =
        failed_operation_then_discovery(Probe::AppendConflictingDiscovery);
    assert_eq!(operations, 1);
    assert_eq!(
        discoveries, 1,
        "conflicting topology must retain the original stop point"
    );
    assert!(
        error.to_string().contains("first_routed_operation"),
        "{error:?}"
    );
}
