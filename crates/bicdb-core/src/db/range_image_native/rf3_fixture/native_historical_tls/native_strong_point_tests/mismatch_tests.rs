//! A real legacy store under an actual enrolled identity must refuse; cached
//! membership is a necessary negative constraint and never upgrades that store.
use super::*;
use crate::distribution_native_read::{
    NativeEventGetRequest, NativeEventGetResult, NativeEventGetUnavailableCode, NativeEventReadMode,
};
use serde_json::json;

struct LegacyUnderEnrolledIdentity {
    database: Arc<RwLock<BicDb>>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    topology: Arc<RwLock<ClusterTopology>>,
    coordinator: Arc<Coordinator>,
    client: Arc<TcpClusterRelocationTransport>,
    stale_client: Arc<TcpClusterRelocationTransport>,
    _listener: ClusterDataServerHandle,
    _root: Option<tempfile::TempDir>,
}
impl LegacyUnderEnrolledIdentity {
    fn new(enrolled: &EnforcedNetwork) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("actual-legacy");
        let mut db = BicDb::open_with_config(
            &path,
            DbConfig::default()
                .with_fsync(true)
                .with_storage_mode(StorageMode::ServerPaged)
                .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        )
        .unwrap();
        db.create_collection("items").unwrap();
        db.create_collection(crate::CLUSTER_EVENTS_COLLECTION)
            .unwrap();
        assert_eq!(db.actual_native_writer_enforcement_version().unwrap(), 0);
        db.schema_compatibility_fingerprint().unwrap();
        let database = Arc::new(RwLock::new(db));
        let topology = Arc::new(RwLock::new(enrolled.native.topology.read().clone()));
        assert_eq!(
            topology.read().nodes[&node("n0")]
                .native_store_birth_v1
                .as_ref()
                .unwrap()
                .writer_enforcement_version(),
            1
        );
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                topology.read().cluster_id.clone(),
                node("n0"),
                &path,
                Arc::clone(&database),
                true,
            )
            .unwrap(),
        ));
        let config = |name: &str| ClusterNetworkTransportConfig {
            connect_timeout_ms: 50,
            io_timeout_ms: 1500,
            max_frame_bytes: 4 * 1024 * 1024,
            max_inbound_connections: 16,
            dev_localhost_plaintext: false,
            tls: Some(ReplicationTlsConfig {
                cert_path: enrolled._keys.path().join(format!("{name}.pem")),
                key_path: enrolled._keys.path().join(format!("{name}.key")),
                ca_path: enrolled._keys.path().join("ca.pem"),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            }),
        };
        let listener = start_cluster_data_server_with_recovery_runtime(
            "127.0.0.1:0",
            config("n0"),
            Arc::clone(&topology),
            Arc::clone(&service),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        topology.write().nodes.get_mut(&node("n0")).unwrap().address =
            listener.local_addr().to_string();
        let client = Arc::new(
            TcpClusterRelocationTransport::new(
                topology.read().cluster_id.clone(),
                node("n1"),
                Arc::new(RwLock::new(topology.read().clone())),
                config("n1"),
            )
            .unwrap(),
        );
        // Routing-cache staleness only; the receiver retains the actual
        // registered binding and the real mTLS certificate/pin is unchanged.
        let mut stale = topology.read().clone();
        stale
            .nodes
            .get_mut(&node("n0"))
            .unwrap()
            .native_store_birth_v1 = None;
        let stale_client = Arc::new(
            TcpClusterRelocationTransport::new(
                stale.cluster_id.clone(),
                node("n1"),
                Arc::new(RwLock::new(stale)),
                config("n1"),
            )
            .unwrap(),
        );
        let local_client = Arc::new(
            TcpClusterRelocationTransport::new(
                topology.read().cluster_id.clone(),
                node("n0"),
                Arc::clone(&topology),
                config("n0"),
            )
            .unwrap(),
        );
        let coordinator = Arc::new(
            RangeWriteCoordinator::new_schema_fenced(
                topology.read().cluster_id.clone(),
                node("n0"),
                Arc::clone(&topology),
                Arc::clone(&service),
                local_client,
            )
            .unwrap(),
        );
        Self {
            database,
            service,
            topology,
            coordinator,
            client,
            stale_client,
            _listener: listener,
            _root: Some(root),
        }
    }
    fn request(&self, key: &str) -> NativeEventGetRequest {
        let range = self
            .topology
            .read()
            .range_for_key(
                crate::CLUSTER_EVENTS_COLLECTION,
                &crate::cluster_event_record_id("tenant", key).unwrap(),
            )
            .unwrap()
            .clone();
        NativeEventGetRequest {
            version: 1,
            mode: NativeEventReadMode::DeploymentDefault,
            business_request_id: Uuid::now_v7(),
            challenge: Uuid::now_v7(),
            range_id: range.id,
            range_epoch: range.epoch,
            namespace: "tenant".into(),
            key: key.into(),
            budget_ms: 1000,
        }
    }
}
impl Drop for LegacyUnderEnrolledIdentity {
    fn drop(&mut self) {
        self.database.read().clear_commit_admission();
        if std::thread::panicking() {
            if let Some(root) = self._root.take() {
                eprintln!(
                    "retained actual mismatched legacy database: {}",
                    root.keep().display()
                );
            }
        }
    }
}

#[test]
fn native_event_endpoint_enrolled_identity_actual_legacy_refuses_all_paths_before_latches() {
    let enrolled = EnforcedNetwork::new_endpoint_mode(true, true);
    let wrong = LegacyUnderEnrolledIdentity::new(&enrolled);
    let request = wrong.request("mismatched-empty");
    // Rejection must precede these unavailable latches, including Scan/Append.
    let _service = wrong.service.lock();
    let _db = wrong.database.write();
    for client in [&wrong.client, &wrong.stale_client] {
        for mode in [
            NativeEventReadMode::DeploymentDefault,
            NativeEventReadMode::Strong,
        ] {
            let mut current = request.clone();
            current.mode = mode;
            let reply = client
                .native_event_get_cancellable(
                    &CancellationToken::uncancelable(),
                    &node("n0"),
                    &current,
                )
                .unwrap();
            assert!(matches!(
                reply.result,
                NativeEventGetResult::Unavailable {
                    code: NativeEventGetUnavailableCode::InvalidRequest,
                    ..
                }
            ));
        }
        let old_reply = client
            .test_original_event_get(
                &CancellationToken::uncancelable(),
                &node("n0"),
                request.legacy_request(),
            )
            .unwrap();
        assert!(matches!(
            old_reply.result,
            NativeEventGetResult::Unavailable { .. }
        ));
        for operation in [
            crate::ClusterEventOperation::Scan {
                namespace: "tenant".into(),
                after: None,
                limit: 4,
            },
            crate::ClusterEventOperation::Append {
                namespace: "tenant".into(),
                key: request.key.clone(),
                value: json!({"must_not_ack":true}),
            },
        ] {
            assert!(client
                .application_event(
                    &node("n0"),
                    crate::ClusterEventRequest {
                        range_id: request.range_id,
                        range_epoch: request.range_epoch,
                        operation,
                    }
                )
                .is_err());
        }
    }
    drop(_db);
    drop(_service);
    assert!(wrong
        .database
        .read()
        .get(
            crate::CLUSTER_EVENTS_COLLECTION,
            &crate::cluster_event_record_id("tenant", &request.key).unwrap()
        )
        .unwrap()
        .is_none());
    enrolled.idle();
}

#[test]
fn native_event_endpoint_enrolled_identity_legacy_coordinator_refuses_before_recovery_prepare() {
    let enrolled = EnforcedNetwork::new_endpoint_mode(true, true);
    let wrong = LegacyUnderEnrolledIdentity::new(&enrolled);
    let error = wrong.coordinator.recover_local_leadership(8).unwrap_err();
    assert!(
        error.to_string().contains("cannot recover a legacy store"),
        "{error}"
    );
    // Even an explicitly installed generic hook cannot convert the legacy DB
    // into its committed native identity. Borrowed outer DB.write stays valid.
    wrong
        .database
        .read()
        .install_commit_admission(wrong.coordinator.clone());
    let db = wrong.database.write();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new("legacy-must-not-prepare"))
        .unwrap();
    let error = tx.commit().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires its actual born-v2 database"),
        "{error}"
    );
    assert!(db
        .get("items", "legacy-must-not-prepare")
        .unwrap()
        .is_none());
    drop(db);
    wrong.database.read().clear_commit_admission();
    // Actual local and all real enrolled peer catalogues remain at genesis:
    // neither recovery nor admission reached Prepare.
    let range = wrong
        .topology
        .read()
        .range_for_key("items", "legacy-must-not-prepare")
        .unwrap()
        .clone();
    assert_eq!(
        wrong
            .service
            .lock()
            .probe_range_write_for_peer(&range, &node("n0"), 1)
            .unwrap()
            .progress
            .last_index,
        0
    );
    for service in enrolled.native.services.values() {
        assert_eq!(
            service
                .lock()
                .probe_range_write_for_peer(&range, &node("n0"), 1)
                .unwrap()
                .progress
                .last_index,
            0
        );
    }
    enrolled.idle();
}

#[test]
fn native_event_endpoint_cached_enforced_sender_promotes_default_and_actual_legacy_default_remains()
{
    let enrolled = EnforcedNetwork::new_endpoint_mode(true, true);
    let wrong = LegacyUnderEnrolledIdentity::new(&enrolled);
    // This receiver now declares the actual legacy deployment; only the first
    // sender still caches the stronger native requirement. Neither cache is MQ.
    wrong
        .topology
        .write()
        .nodes
        .get_mut(&node("n0"))
        .unwrap()
        .native_store_birth_v1 = None;
    let request = wrong.request("ordinary-legacy-absence");
    let promoted = wrong
        .client
        .native_event_get_cancellable(&CancellationToken::uncancelable(), &node("n0"), &request)
        .unwrap();
    assert!(matches!(
        promoted.result,
        NativeEventGetResult::Unavailable {
            code: NativeEventGetUnavailableCode::LegacyUnsupported,
            ..
        }
    ));
    let actual_legacy = wrong
        .stale_client
        .native_event_get_cancellable(&CancellationToken::uncancelable(), &node("n0"), &request)
        .unwrap();
    let NativeEventGetResult::LegacyValue { value } = actual_legacy.result else {
        panic!("actual legacy DeploymentDefault was not preserved")
    };
    assert_eq!(value.into_option(), None);
    enrolled.idle();
}
