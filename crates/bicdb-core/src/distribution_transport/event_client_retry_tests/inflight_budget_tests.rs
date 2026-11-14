//! Public SDK with actual loopback TCP and mock replies/evidence callbacks.
//! No actual native commit, filesystem sync failure or HA qualification.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Probe {
    NestedExpiry,
    WorkerMemory,
    CleanupEvidence,
    ReentryBefore,
    ReentryAfter,
    DifferentSink,
}
struct Sink {
    memory: bool,
    fail_cleanup: bool,
    injected: AtomicUsize,
    saw_limit: AtomicUsize,
    records: std::sync::Mutex<Vec<serde_json::Value>>,
    reentry_phase: Option<&'static str>,
    reentered: AtomicBool,
    client: std::sync::Mutex<Option<std::sync::Weak<crate::ClusterEventClient>>>,
    children: std::sync::Mutex<Vec<Result<bool>>>,
}
impl crate::ClusterRpcEvidenceObserver for Sink {
    fn record(&self, record: &crate::ClusterRpcEvidence) -> std::io::Result<()> {
        self.records
            .lock()
            .unwrap()
            .push(serde_json::to_value(record).unwrap());
        if self.reentry_phase == Some(record.phase)
            && record.operation == "event_append"
            && !self.reentered.swap(true, Ordering::SeqCst)
        {
            let client = self
                .client
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let outcome = client.append(
                "inflight-budget-unit",
                "reentrant-child",
                serde_json::json!({"original":true}),
            );
            self.children.lock().unwrap().push(outcome);
        }
        if self.memory
            && record.operation == "event_append"
            && record.phase == "request_frame_before_dispatch"
        {
            self.saw_limit
                .store(crate::query_budget::limit(1024), Ordering::SeqCst);
            crate::query_budget::observe(17);
        }
        if self.fail_cleanup
            && record.operation == "event_append"
            && record.phase == "partial_or_rejected_response_frame"
        {
            self.injected.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unit observer refuses sibling cancellation record",
            ));
        }
        Ok(())
    }
}
struct Observation {
    result: Result<bool>,
    applications: usize,
    connections: usize,
    child_applications: usize,
    mock_acks: usize,
    caller_peak: usize,
    api_elapsed_us: u128,
    nested_remaining_us_at_return: i128,
    sink: Arc<Sink>,
}
fn execute(probe: Probe) -> Observation {
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
            io_timeout_ms: 2_000,
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
    let sink = Arc::new(Sink {
        memory: matches!(probe, Probe::WorkerMemory),
        fail_cleanup: matches!(probe, Probe::CleanupEvidence),
        injected: AtomicUsize::new(0),
        saw_limit: AtomicUsize::new(0),
        records: std::sync::Mutex::new(Vec::new()),
        reentry_phase: match probe {
            Probe::ReentryBefore | Probe::DifferentSink => Some("request_frame_before_dispatch"),
            Probe::ReentryAfter => Some("response_frame_before_decode"),
            _ => None,
        },
        reentered: AtomicBool::new(false),
        client: std::sync::Mutex::new(None),
        children: std::sync::Mutex::new(Vec::new()),
    });
    let client = Arc::new(
        crate::ClusterEventClient::new(store.config(), snapshot.topology.clone())
            .unwrap()
            .with_request_timeout(Duration::from_secs(1))
            .unwrap()
            .with_rpc_evidence_observer(sink.clone(), crate::ClusterRpcEvidenceLimits::default())
            .unwrap(),
    );
    // Keep a different observed client alive only for the allowed-different-sink
    // control. Weak storage avoids an observer/client ownership cycle.
    let other_sink;
    let other_client;
    if matches!(probe, Probe::DifferentSink) {
        other_sink = Some(Arc::new(Sink {
            memory: false,
            fail_cleanup: false,
            injected: AtomicUsize::new(0),
            saw_limit: AtomicUsize::new(0),
            records: std::sync::Mutex::new(Vec::new()),
            reentry_phase: None,
            reentered: AtomicBool::new(false),
            client: std::sync::Mutex::new(None),
            children: std::sync::Mutex::new(Vec::new()),
        }));
        other_client = Some(Arc::new(
            crate::ClusterEventClient::new(store.config(), snapshot.topology.clone())
                .unwrap()
                .with_request_timeout(Duration::from_secs(1))
                .unwrap()
                .with_rpc_evidence_observer(
                    other_sink.as_ref().unwrap().clone(),
                    crate::ClusterRpcEvidenceLimits::default(),
                )
                .unwrap(),
        ));
        *sink.client.lock().unwrap() = Some(Arc::downgrade(other_client.as_ref().unwrap()));
    } else {
        other_sink = None;
        other_client = None;
        *sink.client.lock().unwrap() = Some(Arc::downgrade(&client));
    }
    let max_frame = store.config().transport.max_frame_bytes;
    let stop = Arc::new(AtomicBool::new(false));
    let applications = Arc::new(AtomicUsize::new(0));
    let child_applications = Arc::new(AtomicUsize::new(0));
    let connections = Arc::new(AtomicUsize::new(0));
    let mock_acks = Arc::new(AtomicUsize::new(0));
    let expiry = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let (
        stopping,
        observed_apps,
        observed_connections,
        observed_acks,
        actual_expiry,
        observed_children,
    ) = (
        stop.clone(),
        applications.clone(),
        connections.clone(),
        mock_acks.clone(),
        expiry.clone(),
        child_applications.clone(),
    );
    let server = thread::spawn(move || {
        let mut handlers = Vec::new();
        while !stopping.load(Ordering::SeqCst) {
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => panic!("mock accept: {error}"),
            };
            observed_connections.fetch_add(1, Ordering::SeqCst);
            let (snapshot, apps, acks, expiry, children) = (
                snapshot.clone(),
                observed_apps.clone(),
                observed_acks.clone(),
                actual_expiry.clone(),
                observed_children.clone(),
            );
            handlers.push(thread::spawn(move || {
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let request: ClusterDataRequestEnvelope =
                    recv_json_frame(&mut stream, max_frame).unwrap();
                let response = match request.request {
                    ClusterDataRequest::FetchBootstrapSnapshot => {
                        ClusterDataResponse::BootstrapSnapshot(snapshot)
                    }
                    ClusterDataRequest::ApplicationEvent(event) => {
                        let crate::ClusterEventOperation::Append {
                            namespace,
                            key,
                            value,
                        } = event.operation
                        else {
                            panic!("expected original append")
                        };
                        assert_eq!(namespace, "inflight-budget-unit");
                        assert!(key == "same-key" || key == "reentrant-child");
                        if key == "reentrant-child" {
                            children.fetch_add(1, Ordering::SeqCst);
                        }
                        assert_eq!(value, serde_json::json!({"original":true}));
                        let ordinal = apps.fetch_add(1, Ordering::SeqCst);
                        match probe {
                            Probe::NestedExpiry => {
                                let deadline = expiry
                                    .lock()
                                    .unwrap()
                                    .expect("caller installs actual nested deadline before append");
                                while Instant::now() <= deadline {
                                    thread::sleep(Duration::from_millis(1));
                                }
                            }
                            Probe::CleanupEvidence if ordinal == 0 => {
                                // First route remains silent after its slice.
                                // A later actual ACK triggers client cleanup;
                                // the sibling socket must close, not linger.
                                let mut byte = [0];
                                let _ = stream.read(&mut byte);
                                return;
                            }
                            _ => {}
                        }
                        ClusterDataResponse::ApplicationEvent(
                            crate::ClusterEventResponse::Appended { inserted: true },
                        )
                    }
                    other => panic!("unexpected mock request {other:?}"),
                };
                let is_ack = matches!(response, ClusterDataResponse::ApplicationEvent(_));
                let sent = send_json_frame(
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
                );
                if is_ack && sent.is_ok() {
                    acks.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for handler in handlers {
            handler.join().unwrap();
        }
    });
    let result;
    let caller_peak;
    let api_elapsed_us;
    let nested_remaining_us_at_return;
    {
        let outer = crate::query_budget::QueryMemoryScope::with_deadline(
            64,
            Instant::now() + Duration::from_secs(2),
        );
        let nested_deadline = if matches!(probe, Probe::NestedExpiry) {
            Instant::now() + Duration::from_millis(100)
        } else {
            Instant::now() + Duration::from_secs(2)
        };
        *expiry.lock().unwrap() = Some(nested_deadline);
        {
            let _inner = crate::query_budget::QueryMemoryScope::with_deadline(16, nested_deadline);
            let call_started = Instant::now();
            result = client.append(
                "inflight-budget-unit",
                "same-key",
                serde_json::json!({"original":true}),
            );
            let returned = Instant::now();
            api_elapsed_us = returned.duration_since(call_started).as_micros();
            nested_remaining_us_at_return = if nested_deadline >= returned {
                nested_deadline.duration_since(returned).as_micros() as i128
            } else {
                -(returned.duration_since(nested_deadline).as_micros() as i128)
            };
        }
        caller_peak = outer.peak_bytes();
    }
    stop.store(true, Ordering::SeqCst);
    server.join().unwrap();
    let observed = Observation {
        result,
        applications: applications.load(Ordering::SeqCst),
        connections: connections.load(Ordering::SeqCst),
        child_applications: child_applications.load(Ordering::SeqCst),
        mock_acks: mock_acks.load(Ordering::SeqCst),
        caller_peak,
        api_elapsed_us,
        nested_remaining_us_at_return,
        sink,
    };
    eprintln!(
        "event_client_inflight_budget_observation {}",
        serde_json::json!({
            "probe":format!("{:?}",probe),"result":format!("{:?}",observed.result),"applications":observed.applications,
        "configured_request_duration_ms":1000,"api_elapsed_us":observed.api_elapsed_us,
        "actual_nested_deadline_remaining_us_at_return":observed.nested_remaining_us_at_return,
        "rpc_records":*observed.sink.records.lock().unwrap(),
        "child_outcomes":format!("{:?}",*observed.sink.children.lock().unwrap()),
            "connections":observed.connections,"mock_acks":observed.mock_acks,"caller_peak":observed.caller_peak,
            "cleanup_faults":observed.sink.injected.load(Ordering::SeqCst),
        })
    );
    drop(other_client);
    drop(other_sink);
    observed
}
#[test]
fn event_client_nested_caller_deadline_expires_during_actual_mock_reply() {
    let observed = execute(Probe::NestedExpiry);
    assert!(
        matches!(observed.result, Err(BicDbError::QueryTimedOut)),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.applications, 1);
    assert_eq!(
        observed.connections, 1,
        "caller expiry must stop before discovery/retry"
    );
    let records = observed.sink.records.lock().unwrap();
    assert!(
        !records
            .iter()
            .any(|r| r["phase"] == "validated_transport_result_ok"
                && r["operation"] == "event_append"),
        "mock reply was withheld until actual tight nested deadline elapsed"
    );
}
#[test]
fn event_client_worker_memory_envelope_updates_original_caller_peak_and_refuses_io() {
    let observed = execute(Probe::WorkerMemory);
    assert!(
        matches!(observed.result, Err(BicDbError::QueryMemoryLimit(_))),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.sink.saw_limit.load(Ordering::SeqCst), 16);
    assert_eq!(observed.caller_peak, 17);
    assert_eq!(observed.connections, 0);
    assert_eq!(observed.applications, 0);
}
#[test]
fn event_client_sibling_cleanup_evidence_failure_cannot_hide_behind_actual_mock_ack() {
    let observed = execute(Probe::CleanupEvidence);
    assert!(
        matches!(&observed.result,
        Err(BicDbError::RpcEvidenceFailure { may_have_dispatched: true, detail })
            if detail.contains("unit observer refuses sibling cancellation record")),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.sink.injected.load(Ordering::SeqCst), 1);
    assert_eq!(observed.applications, 2);
    assert_eq!(
        observed.mock_acks, 1,
        "a real second TCP exchange returned an ACK before sibling cancellation"
    );
    let records = observed.sink.records.lock().unwrap();
    assert!(
        records
            .iter()
            .any(|r| r["phase"] == "validated_transport_result_ok"
                && r["operation"] == "event_append")
    );
    assert!(records
        .iter()
        .any(|r| r["phase"] == "partial_or_rejected_response_frame"
            && r["operation"] == "event_append"));
}

#[test]
fn event_client_same_sink_reentry_before_dispatch_refuses_child_and_parent_without_io() {
    let observed = execute(Probe::ReentryBefore);
    assert!(
        matches!(
            &observed.result,
            Err(BicDbError::RpcEvidenceFailure {
                may_have_dispatched: false,
                ..
            })
        ),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.connections, 0);
    assert_eq!(observed.applications, 0);
    assert_eq!(observed.child_applications, 0);
    let children = observed.sink.children.lock().unwrap();
    assert_eq!(children.len(), 1);
    assert!(
        matches!(&children[0],Err(BicDbError::RpcEvidenceFailure{may_have_dispatched:false,detail})
        if detail.contains("same-thread evidence sink reentry before worker dispatch")),
        "{:?}",
        children
    );
}
#[test]
fn event_client_same_sink_reentry_after_response_preserves_parent_dispatch_uncertainty() {
    let observed = execute(Probe::ReentryAfter);
    assert!(
        matches!(
            &observed.result,
            Err(BicDbError::RpcEvidenceFailure {
                may_have_dispatched: true,
                ..
            })
        ),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.applications, 1);
    assert_eq!(observed.child_applications, 0);
    assert_eq!(observed.mock_acks, 1);
    let children = observed.sink.children.lock().unwrap();
    assert_eq!(children.len(), 1);
    assert!(
        matches!(&children[0],Err(BicDbError::RpcEvidenceFailure{may_have_dispatched:true,detail})
        if detail.contains("same-thread evidence sink reentry before worker dispatch")),
        "{:?}",
        children
    );
}
#[test]
fn event_client_different_sink_nested_request_is_allowed_without_global_callback_lock() {
    let observed = execute(Probe::DifferentSink);
    assert!(
        matches!(&observed.result, Ok(true)),
        "{:?}",
        observed.result
    );
    assert_eq!(observed.applications, 2);
    assert_eq!(observed.child_applications, 1);
    assert_eq!(observed.mock_acks, 2);
    let children = observed.sink.children.lock().unwrap();
    assert_eq!(children.len(), 1);
    assert!(matches!(&children[0], Ok(true)), "{:?}", children);
}
