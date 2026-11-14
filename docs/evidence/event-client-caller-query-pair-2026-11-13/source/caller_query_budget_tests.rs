//! Public SDK entrypoint plus real loopback TCP/mock replies. No native database
//! commit, HA authority or physical qualification is exercised.
use super::*;

struct Observation {
    outcome: Result<bool>,
    connections: usize,
    applications: usize,
}

fn append_in_caller_scope(expired: bool) -> Observation {
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
            io_timeout_ms: 1_000,
            ..Default::default()
        },
        ..Default::default()
    };
    let store = DistributionStore::initialize_at(root.path(), config, false, 1000).unwrap();
    let snapshot = ClusterBootstrapSnapshot {
        topology: store.topology().clone(),
        config_template: store.config().clone(),
        metadata_leader_id: None,
    };
    snapshot.validate().unwrap();
    let client = crate::ClusterEventClient::new(store.config(), snapshot.topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(1))
        .unwrap();
    let max_frame = store.config().transport.max_frame_bytes;
    let stop = Arc::new(AtomicBool::new(false));
    let connections = Arc::new(AtomicUsize::new(0));
    let applications = Arc::new(AtomicUsize::new(0));
    let (stopping, accepted, dispatched) =
        (stop.clone(), connections.clone(), applications.clone());
    let worker = thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(5);
        while !stopping.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < until,
                "mock client exceeded bounded request"
            );
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("mock accept: {error}"),
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let request: ClusterDataRequestEnvelope =
                recv_json_frame(&mut stream, max_frame).unwrap();
            let response = match request.request {
                ClusterDataRequest::ApplicationEvent(event) => {
                    let crate::ClusterEventOperation::Append {
                        namespace,
                        key,
                        value,
                    } = event.operation
                    else {
                        panic!("expected unchanged append")
                    };
                    assert_eq!(namespace, "caller-budget-unit");
                    assert_eq!(key, "same-operation");
                    assert_eq!(value, serde_json::json!({"original": true}));
                    dispatched.fetch_add(1, Ordering::SeqCst);
                    ClusterDataResponse::ApplicationEvent(crate::ClusterEventResponse::Appended {
                        inserted: true,
                    })
                }
                ClusterDataRequest::FetchBootstrapSnapshot => {
                    ClusterDataResponse::BootstrapSnapshot(snapshot.clone())
                }
                other => panic!("unexpected mock request {other:?}"),
            };
            send_json_frame(
                &mut stream,
                &ClusterDataResponseEnvelope {
                    actual_native_source: None,
                    protocol_version: request.protocol_version,
                    cluster_id: request.cluster_id,
                    source_node_id: request.destination_node_id,
                    request_id: request.request_id,
                    response: Ok(response),
                },
                max_frame,
            )
            .unwrap();
        }
    });
    // Real time, after all fixture setup. The expired scope has an actual
    // elapsed deadline; the live control exceeds this SAME one-second request.
    let caller_deadline = if expired {
        Instant::now()
    } else {
        Instant::now() + Duration::from_secs(2)
    };
    let outcome = {
        let _scope = crate::query_budget::QueryMemoryScope::with_deadline(0, caller_deadline);
        client.append(
            "caller-budget-unit",
            "same-operation",
            serde_json::json!({"original": true}),
        )
    };
    stop.store(true, Ordering::SeqCst);
    worker.join().unwrap();
    let observation = Observation {
        outcome,
        connections: connections.load(Ordering::SeqCst),
        applications: applications.load(Ordering::SeqCst),
    };
    // Emit actual outcome/counts BEFORE assertions, including the expected-red
    // b18 counterexample; observations are test DATA, never protocol authority.
    eprintln!(
        "event_client_caller_query_budget_observation {}",
        serde_json::json!({
            "expired_scope": expired,
            "result": format!("{:?}", observation.outcome),
            "accepted_tcp_connections": observation.connections,
            "application_frames": observation.applications,
        })
    );
    observation
}

#[test]
fn event_client_caller_query_budget_expired_refuses_before_tcp_dispatch() {
    let observed = append_in_caller_scope(true);
    assert!(
        matches!(observed.outcome, Err(BicDbError::QueryTimedOut)),
        "expired caller scope must refuse, actual {:?}",
        observed.outcome
    );
    assert_eq!(
        observed.connections, 0,
        "no TCP connection may start after caller expiry"
    );
    assert_eq!(
        observed.applications, 0,
        "no expired application frame may be sent"
    );
}

#[test]
fn event_client_caller_query_budget_live_scope_accepts_actual_mock_ack() {
    let observed = append_in_caller_scope(false);
    assert!(
        matches!(observed.outcome, Ok(true)),
        "{:?}",
        observed.outcome
    );
    assert_eq!(observed.connections, 1);
    assert_eq!(observed.applications, 1);
}
