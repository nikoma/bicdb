//! Actual legacy mTLS RPCs and fsynced private journals. Not native HA qualification.
use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use std::fs::{File, OpenOptions};
use std::path::Path;

struct Journal {
    file: Mutex<File>,
    fail: Option<&'static str>,
    fail_after_sync: bool,
    cancel_after_response: Option<CancellationToken>,
}
impl Journal {
    fn new(path: &Path, fail: Option<&'static str>, fail_after_sync: bool) -> Arc<Self> {
        Arc::new(Self {
            file: Mutex::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .unwrap(),
            ),
            fail,
            fail_after_sync,
            cancel_after_response: None,
        })
    }
}
impl ClusterRpcEvidenceObserver for Journal {
    fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
        let fail = self.fail == Some(record.phase);
        if fail && !self.fail_after_sync {
            return Err(io::Error::other("injected evidence persistence failure"));
        }
        let mut file = self.file.lock();
        serde_json::to_writer(&mut *file, record)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        if record.phase == "response_frame_before_decode" {
            if let Some(parent) = &self.cancel_after_response {
                parent.cancel();
            }
        }
        if fail {
            return Err(io::Error::other(
                "injected error AFTER actual evidence sync",
            ));
        }
        Ok(())
    }
}
struct Fixture {
    root: tempfile::TempDir,
    config: crate::DistributionConfig,
    topology: Arc<RwLock<ClusterTopology>>,
    db: Arc<RwLock<crate::BicDb>>,
    server: ClusterDataServerHandle,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = params.self_signed(&key).unwrap();
        let leaf_key = KeyPair::generate().unwrap();
        let mut params =
            CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf = params.signed_by(&leaf_key, &ca, &key).unwrap();
        let cert = root.path().join("leaf.pem");
        let private = root.path().join("leaf.key");
        let trust = root.path().join("ca.pem");
        std::fs::write(&cert, leaf.pem()).unwrap();
        std::fs::write(&private, leaf_key.serialize_pem()).unwrap();
        std::fs::write(&trust, ca.pem()).unwrap();
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = crate::DistributionConfig {
            enabled: true,
            cluster_id: ClusterId::new("actual-rpc-evidence").unwrap(),
            node_id: ClusterNodeId::new("n0").unwrap(),
            node_address: reservation.local_addr().unwrap().to_string(),
            replication_factor: 1,
            initial_ranges: 1,
            node_tls_certificate_sha256: Some(
                crate::replication_transport::replication_certificate_sha256(&cert).unwrap(),
            ),
            transport: ClusterNetworkTransportConfig {
                connect_timeout_ms: 100,
                io_timeout_ms: 1000,
                max_frame_bytes: 4 * 1024 * 1024,
                dev_localhost_plaintext: false,
                tls: Some(crate::ReplicationTlsConfig {
                    cert_path: cert,
                    key_path: private,
                    ca_path: trust,
                    require_client_cert: true,
                    dev_localhost_plaintext: false,
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let store = Arc::new(Mutex::new(
            DistributionStore::initialize_at(
                root.path().join("topology"),
                config.clone(),
                false,
                1,
            )
            .unwrap(),
        ));
        let topology = Arc::new(RwLock::new(store.lock().topology().clone()));
        let db = Arc::new(RwLock::new(
            crate::BicDb::open_with_config(
                root.path().join("data"),
                crate::DbConfig::default().with_fsync(false),
            )
            .unwrap(),
        ));
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                config.cluster_id.clone(),
                config.node_id.clone(),
                db.clone(),
                false,
            )
            .unwrap(),
        ));
        let server = start_cluster_data_server_on_listener_with_metadata_read_provider(
            reservation,
            config.transport.clone(),
            topology.clone(),
            service,
            Some(store),
            None,
            None,
            None,
        )
        .unwrap();
        Self {
            root,
            config,
            topology,
            db,
            server,
        }
    }
    fn transport(&self) -> TcpClusterRelocationTransport {
        TcpClusterRelocationTransport::new(
            self.config.cluster_id.clone(),
            self.config.node_id.clone(),
            self.topology.clone(),
            self.config.transport.clone(),
        )
        .unwrap()
    }
    fn records(&self, name: &str) -> Vec<serde_json::Value> {
        std::fs::read_to_string(self.root.path().join(name))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.request_shutdown();
    }
}
fn frame(record: &serde_json::Value) -> Vec<u8> {
    serde_json::from_value(record["raw_frame"].clone()).unwrap()
}

#[test]
fn actual_rpc_evidence_mtls_exact_frames_and_discovery_errors_survive_fallback() {
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("journal.jsonl"), None, false);
    let client = crate::ClusterEventClient::new(&fixture.config, fixture.topology.read().clone())
        .unwrap()
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .unwrap();
    // No consensus backend: real snapshot discovery rejects, then real legacy
    // topology discovery succeeds. The original peer error must remain durable.
    client.refresh().unwrap();
    let records = fixture.records("journal.jsonl");
    let errors = records
        .iter()
        .filter(|r| r["phase"] == "validated_transport_result_error")
        .collect::<Vec<_>>();
    assert!(!errors.is_empty());
    assert!(errors
        .iter()
        .all(|r| r["error_chain"].as_array().is_some_and(|a| !a.is_empty())));
    assert!(records
        .iter()
        .any(|r| r["operation"] == "topology_discovery"
            && r["phase"] == "validated_transport_result_ok"));
    for request in records
        .iter()
        .filter(|r| r["phase"] == "request_frame_before_dispatch")
    {
        let bytes = frame(request);
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );
        let sent: serde_json::Value = serde_json::from_slice(&bytes[4..]).unwrap();
        assert_eq!(sent["request_id"], request["request_id"]);
        assert_eq!(sent["destination_node_id"], "n0");
        assert_eq!(
            request["topology_generation"],
            fixture.topology.read().generation
        );
        assert_eq!(request["address"], fixture.config.node_address);
        let response = records
            .iter()
            .find(|r| {
                r["attempt_id"] == request["attempt_id"]
                    && r["phase"] == "response_frame_before_decode"
            })
            .unwrap();
        let bytes = frame(response);
        let received: serde_json::Value = serde_json::from_slice(&bytes[4..]).unwrap();
        assert_eq!(received["request_id"], sent["request_id"]);
        assert_eq!(received["source_node_id"], "n0");
    }
    // A genuine peer refusal carries the complete available remote error bytes.
    let raw = records
        .iter()
        .filter(|r| r["phase"] == "response_frame_before_decode")
        .map(|r| String::from_utf8(frame(r)[4..].to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert!(raw.iter().any(|raw| raw.contains("Err")));
    // A real APP RPC with a deliberately stale epoch preserves exact owned
    // record identity/value and the server refusal, without applying a row.
    let journal = Journal::new(&fixture.root.path().join("app.jsonl"), None, false);
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .unwrap();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let value = serde_json::json!({"entire":["quotes\"",null,{"number":17}]});
    let before = fixture.db.read().current_commit_seq();
    assert!(transport
        .application_event(
            &fixture.config.node_id,
            crate::ClusterEventRequest {
                range_id: range.id,
                range_epoch: range.epoch + 1,
                operation: crate::ClusterEventOperation::Append {
                    namespace: "items".into(),
                    key: "exact-record-id".into(),
                    value: value.clone()
                }
            }
        )
        .is_err());
    let records = fixture.records("app.jsonl");
    let request = records
        .iter()
        .find(|r| r["phase"] == "request_frame_before_dispatch")
        .unwrap();
    let bytes = frame(request);
    let actual: ClusterDataRequestEnvelope = serde_json::from_slice(&bytes[4..]).unwrap();
    let ClusterDataRequest::ApplicationEvent(request) = actual.request else {
        panic!("actual APP frame required")
    };
    let crate::ClusterEventOperation::Append {
        namespace,
        key,
        value: actual_value,
    } = request.operation
    else {
        panic!("actual Append required")
    };
    assert_eq!(
        (namespace, key, actual_value),
        ("items".into(), "exact-record-id".into(), value)
    );
    assert!(records
        .iter()
        .any(|r| r["phase"] == "validated_transport_result_error"
            && !r["error_chain"].as_array().unwrap().is_empty()));
    assert_eq!(fixture.db.read().current_commit_seq(), before);
}
#[test]
fn actual_rpc_evidence_mtls_canceled_before_dispatch_records_original_cancellation() {
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("cancel.jsonl"), None, false);
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .unwrap();
    let canceled = CancellationToken::uncancelable().child();
    canceled.cancel();
    assert!(matches!(
        transport.call_cancellable(&canceled, &fixture.config.node_id, ClusterDataRequest::Ping),
        Err(BicDbError::QueryCanceled)
    ));
    let records = fixture.records("cancel.jsonl");
    assert_eq!(records.len(), 2);
    assert!(records
        .iter()
        .all(|r| r["raw_frame"].is_null() && r["may_have_dispatched"] == false));
    assert!(records[1]["error_chain"][0]
        .as_str()
        .unwrap()
        .contains("cancel"));
    assert_eq!(transport.next_request_id.load(Ordering::SeqCst), 1);
}
#[test]
fn actual_rpc_evidence_pre_dispatch_sync_failure_fences_without_application_mutation() {
    let fixture = Fixture::new();
    let journal = Journal::new(
        &fixture.root.path().join("before.jsonl"),
        Some("request_frame_before_dispatch"),
        false,
    );
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .unwrap();
    let before = fixture.db.read().current_commit_seq();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let request = ClusterDataRequest::ApplicationEvent(crate::ClusterEventRequest {
        range_id: range.id,
        range_epoch: range.epoch,
        operation: crate::ClusterEventOperation::Append {
            namespace: "items".into(),
            key: "never-dispatched".into(),
            value: serde_json::json!({"entire":[1,null,"value"]}),
        },
    });
    assert!(matches!(
        transport.call(&fixture.config.node_id, request),
        Err(BicDbError::RpcEvidenceFailure {
            may_have_dispatched: false,
            ..
        })
    ));
    assert_eq!(fixture.db.read().current_commit_seq(), before);
    let next = transport.next_request_id.load(Ordering::SeqCst);
    assert!(matches!(
        transport.call(&fixture.config.node_id, ClusterDataRequest::Ping),
        Err(BicDbError::RpcEvidenceFailure { .. })
    ));
    assert_eq!(transport.next_request_id.load(Ordering::SeqCst), next);
    assert_eq!(fixture.records("before.jsonl").len(), 1);
}
#[test]
fn actual_rpc_evidence_post_response_sync_failure_returns_unknown_and_never_rearms() {
    let fixture = Fixture::new();
    let journal = Journal::new(
        &fixture.root.path().join("after.jsonl"),
        Some("response_frame_before_decode"),
        true,
    );
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(journal.clone(), ClusterRpcEvidenceLimits::default())
        .unwrap();
    let error = transport
        .call(&fixture.config.node_id, ClusterDataRequest::Ping)
        .unwrap_err();
    assert!(matches!(
        error,
        BicDbError::RpcEvidenceFailure {
            may_have_dispatched: true,
            ..
        }
    ));
    let records = fixture.records("after.jsonl");
    let response = records
        .iter()
        .find(|r| r["phase"] == "response_frame_before_decode")
        .unwrap();
    let bytes = frame(response);
    let decoded: ClusterDataResponseEnvelope = serde_json::from_slice(&bytes[4..]).unwrap();
    assert!(matches!(decoded.response, Ok(ClusterDataResponse::Pong)));
    assert!(transport
        .call(&fixture.config.node_id, ClusterDataRequest::Ping)
        .is_err());
    assert!(transport
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .is_err());
}

#[test]
fn actual_rpc_evidence_complete_record_quota_fences_without_truncation() {
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("quota.jsonl"), None, false);
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(
            journal,
            ClusterRpcEvidenceLimits {
                max_records: 1,
                max_bytes: 1024 * 1024,
            },
        )
        .unwrap();
    assert!(matches!(
        transport.call(&fixture.config.node_id, ClusterDataRequest::Ping),
        Err(BicDbError::RpcEvidenceFailure {
            may_have_dispatched: false,
            ..
        })
    ));
    let records = fixture.records("quota.jsonl");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["phase"], "invoked");
    assert!(transport
        .call(&fixture.config.node_id, ClusterDataRequest::Ping)
        .is_err());
}

#[test]
fn actual_rpc_evidence_large_complete_frame_byte_quota_refuses_before_dispatch() {
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("bytes.jsonl"), None, false);
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(
            journal,
            ClusterRpcEvidenceLimits {
                max_records: 100,
                max_bytes: 8192,
            },
        )
        .unwrap();
    let range = fixture
        .topology
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    let before = fixture.db.read().current_commit_seq();
    let request = crate::ClusterEventRequest {
        range_id: range.id,
        range_epoch: range.epoch,
        operation: crate::ClusterEventOperation::Append {
            namespace: "items".into(),
            key: "full-frame-not-truncated".into(),
            value: serde_json::json!({"whole":"q".repeat(16*1024)}),
        },
    };
    assert!(matches!(
        transport.application_event(&fixture.config.node_id, request),
        Err(BicDbError::RpcEvidenceFailure {
            may_have_dispatched: false,
            ..
        })
    ));
    let records = fixture.records("bytes.jsonl");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["phase"], "invoked");
    assert_eq!(fixture.db.read().current_commit_seq(), before);
}

#[test]
fn actual_rpc_evidence_mtls_response_retained_before_original_cancellation_override() {
    let fixture = Fixture::new();
    let parent = CancellationToken::uncancelable().child();
    let journal = Arc::new(Journal {
        file: Mutex::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(fixture.root.path().join("late-cancel.jsonl"))
                .unwrap(),
        ),
        fail: None,
        fail_after_sync: false,
        cancel_after_response: Some(parent.clone()),
    });
    let transport = fixture
        .transport()
        .with_rpc_evidence_observer(journal, ClusterRpcEvidenceLimits::default())
        .unwrap();
    assert!(matches!(
        transport.call_cancellable(&parent, &fixture.config.node_id, ClusterDataRequest::Ping),
        Err(BicDbError::QueryCanceled)
    ));
    let records = fixture.records("late-cancel.jsonl");
    let response = records
        .iter()
        .find(|r| r["phase"] == "response_frame_before_decode")
        .unwrap();
    let bytes = frame(response);
    let decoded: ClusterDataResponseEnvelope = serde_json::from_slice(&bytes[4..]).unwrap();
    assert!(matches!(decoded.response, Ok(ClusterDataResponse::Pong)));
    assert!(records
        .iter()
        .any(|r| r["phase"] == "validated_transport_result_error"
            && r["error_chain"][0].as_str().unwrap().contains("cancel")));
    assert!(parent.is_cancel_requested());
}

#[test]
fn actual_rpc_evidence_mtls_parallel_requests_share_journal_without_false_reentrancy() {
    struct ParallelJournal {
        journal: Arc<Journal>,
        arrivals: AtomicUsize,
        until: Instant,
    }
    impl ClusterRpcEvidenceObserver for ParallelJournal {
        fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
            if record.phase == "request_frame_before_dispatch" {
                self.arrivals.fetch_add(1, Ordering::SeqCst);
                while self.arrivals.load(Ordering::SeqCst) < 2 {
                    if Instant::now() >= self.until {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "second genuine concurrent callback never arrived",
                        ));
                    }
                    thread::yield_now();
                }
            }
            self.journal.record(record)
        }
    }
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("parallel.jsonl"), None, false);
    let sink = Arc::new(ParallelJournal {
        journal,
        arrivals: AtomicUsize::new(0),
        until: Instant::now() + Duration::from_millis(fixture.config.transport.io_timeout_ms),
    });
    let transport = Arc::new(
        fixture
            .transport()
            .with_rpc_evidence_observer(sink, ClusterRpcEvidenceLimits::default())
            .unwrap(),
    );
    let mut handles = Vec::new();
    for _ in 0..2 {
        let transport = transport.clone();
        let destination = fixture.config.node_id.clone();
        handles.push(thread::spawn(move || {
            transport.call(&destination, ClusterDataRequest::Ping)
        }));
    }
    for handle in handles {
        assert!(matches!(
            handle.join().unwrap().unwrap(),
            ClusterDataResponse::Pong
        ));
    }
    let records = fixture.records("parallel.jsonl");
    let requests = records
        .iter()
        .filter(|r| r["phase"] == "request_frame_before_dispatch")
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0]["attempt_id"], requests[1]["attempt_id"]);
    assert_ne!(requests[0]["request_id"], requests[1]["request_id"]);
    let sequences = records
        .iter()
        .map(|r| r["journal_sequence"].as_u64().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(sequences, (1..=records.len() as u64).collect());
    for request in requests {
        let per_call = records
            .iter()
            .filter(|r| r["attempt_id"] == request["attempt_id"])
            .collect::<Vec<_>>();
        assert!(per_call
            .iter()
            .all(|r| r["phase_unix_ns"].as_u64().unwrap() > 0));
        assert!(per_call
            .windows(2)
            .all(|pair| pair[0]["elapsed_ns"].as_u64().unwrap()
                <= pair[1]["elapsed_ns"].as_u64().unwrap()));
        assert!(per_call
            .iter()
            .any(|r| r["phase"] == "validated_transport_result_ok"));
    }
}
#[test]
fn actual_rpc_evidence_same_thread_reentry_refuses_without_dispatch() {
    struct Reentrant {
        journal: Arc<Journal>,
        transport: Mutex<Option<std::sync::Weak<TcpClusterRelocationTransport>>>,
        rejected: AtomicBool,
    }
    impl ClusterRpcEvidenceObserver for Reentrant {
        fn record(&self, record: &ClusterRpcEvidence) -> io::Result<()> {
            if record.phase == "request_frame_before_dispatch" {
                let transport = self.transport.lock().as_ref().unwrap().upgrade().unwrap();
                let error = transport
                    .call(&ClusterNodeId::new("n0").unwrap(), ClusterDataRequest::Ping)
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("same-thread evidence sink reentry"),
                    "{error}"
                );
                self.rejected.store(true, Ordering::SeqCst);
            }
            self.journal.record(record)
        }
    }
    let fixture = Fixture::new();
    let journal = Journal::new(&fixture.root.path().join("reentry.jsonl"), None, false);
    let sink = Arc::new(Reentrant {
        journal,
        transport: Mutex::new(None),
        rejected: AtomicBool::new(false),
    });
    let transport = Arc::new(
        fixture
            .transport()
            .with_rpc_evidence_observer(sink.clone(), ClusterRpcEvidenceLimits::default())
            .unwrap(),
    );
    *sink.transport.lock() = Some(Arc::downgrade(&transport));
    assert!(matches!(
        transport.call(&fixture.config.node_id, ClusterDataRequest::Ping),
        Err(BicDbError::RpcEvidenceFailure {
            may_have_dispatched: false,
            ..
        })
    ));
    assert!(sink.rejected.load(Ordering::SeqCst));
    assert_eq!(
        transport.next_request_id.load(Ordering::SeqCst),
        2,
        "only outer real request ID allocated; neither frame dispatched"
    );
    assert!(fixture
        .records("reentry.jsonl")
        .iter()
        .all(|r| r["may_have_dispatched"] == false));
}
