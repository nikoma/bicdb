use bicdb_core::{
    distribution_key_token, provision_cluster_member_directory, start_cluster_data_server, BicDb,
    CancellationToken, ClusterDataNodeService, ClusterId, ClusterNetworkTransportConfig,
    ClusterNode, ClusterNodeId, ClusterRelocationDriver, ClusterRelocationTransportConfig,
    CollectionMeta, CommitAdmissionMutation, CommitFrame, DbConfig, DistributionConfig,
    DistributionStore, InProcessClusterRelocationTransport, MetadataConsensusStore,
    RangeLearnerStore, RangeReplica, RangeReplicaRole, RangeSnapshotBatch, RangeWriteCommand,
    RangeWriteRepairLimits, RangeWriteState, RangeWriteStore, RangeWriteTransport,
    RebalanceOptions, Record, RelocationPhase, ReplicaId, ReplicationConfig, ReplicationMode,
    ReplicationOperationType, ReplicationTlsConfig, ReplicationWrite, StorageMode,
    TcpClusterBootstrapClient, TcpClusterRelocationTransport, TransportClusterRelocationDriver,
    RANGE_WRITE_PROTOCOL_VERSION, SCHEMA_BOOTSTRAP_NODE_LABEL, SCHEMA_COMPATIBILITY_NODE_LABEL,
};
use parking_lot::{Mutex, RwLock};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn record_id_for(range: &bicdb_core::RangeDescriptor, belongs: bool) -> String {
    (0..100_000)
        .map(|number| format!("doc-{number:05}"))
        .find(|id| range.contains_token(distribution_key_token("documents", id)) == belongs)
        .expect("test token space should contain a matching id")
}

type InProcessRelocationDriver =
    TransportClusterRelocationDriver<InProcessClusterRelocationTransport>;

#[derive(Clone)]
struct TestClusterTlsIdentity {
    cert_path: PathBuf,
    key_path: PathBuf,
    ca_path: PathBuf,
}

impl TestClusterTlsIdentity {
    fn network_config(&self) -> ClusterNetworkTransportConfig {
        ClusterNetworkTransportConfig {
            connect_timeout_ms: 2_000,
            io_timeout_ms: 5_000,
            max_frame_bytes: 1024 * 1024,
            max_inbound_connections: 8,
            dev_localhost_plaintext: false,
            tls: Some(ReplicationTlsConfig {
                cert_path: self.cert_path.clone(),
                key_path: self.key_path.clone(),
                ca_path: self.ca_path.clone(),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            }),
        }
    }

    fn fingerprint(&self) -> String {
        bicdb_core::replication_transport::replication_certificate_sha256(&self.cert_path).unwrap()
    }
}

fn test_cluster_tls_identities(
    root: &Path,
    names: &[&str],
) -> BTreeMap<String, TestClusterTlsIdentity> {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca: Certificate = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = root.join("cluster-ca.pem");
    fs::write(&ca_path, ca.pem()).unwrap();

    names
        .iter()
        .map(|name| {
            let key = KeyPair::generate().unwrap();
            let mut params =
                CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                    .unwrap();
            params.is_ca = IsCa::ExplicitNoCa;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let certificate = params.signed_by(&key, &ca, &ca_key).unwrap();
            let cert_path = root.join(format!("{name}.pem"));
            let key_path = root.join(format!("{name}.key"));
            fs::write(&cert_path, certificate.pem()).unwrap();
            fs::write(&key_path, key.serialize_pem()).unwrap();
            (
                (*name).to_string(),
                TestClusterTlsIdentity {
                    cert_path,
                    key_path,
                    ca_path: ca_path.clone(),
                },
            )
        })
        .collect()
}

fn in_process_relocation_driver(
    cluster_id: &ClusterId,
    source_id: &ClusterNodeId,
    source_root: &Path,
    source: &Arc<RwLock<BicDb>>,
    target_id: &ClusterNodeId,
    target_root: &Path,
    target: &Arc<RwLock<BicDb>>,
) -> InProcessRelocationDriver {
    let mut transport = InProcessClusterRelocationTransport::new(cluster_id.clone());
    transport
        .register_node(source_id.clone(), source_root, Arc::clone(source), true)
        .unwrap();
    transport
        .register_node(target_id.clone(), target_root, Arc::clone(target), true)
        .unwrap();
    TransportClusterRelocationDriver::new(
        transport,
        ClusterRelocationTransportConfig {
            snapshot: bicdb_core::RangeSnapshotOptions {
                max_records_per_batch: 2,
                max_bytes_per_batch: 4 * 1024,
                max_record_bytes: 4 * 1024,
            },
            max_commit_frames_per_step: 1,
        },
    )
    .unwrap()
}

fn crash_reopen_database(
    database: Arc<RwLock<BicDb>>,
    root: &Path,
    config: &DbConfig,
) -> Arc<RwLock<BicDb>> {
    let strong_count = Arc::strong_count(&database);
    let lock = match Arc::try_unwrap(database) {
        Ok(lock) => lock,
        Err(_) => panic!("crashed database still has {strong_count} live references"),
    };
    // Deliberately skip BicDb::close/checkpoint_for_resume. Dropping the open
    // database models abrupt process loss; only state already made durable by
    // the normal operation may survive.
    drop(lock.into_inner());
    Arc::new(RwLock::new(
        BicDb::open_with_config(root, config.clone()).unwrap(),
    ))
}

#[test]
fn learner_snapshot_and_commit_suffix_are_durable_idempotent_and_filtered() {
    let topology_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let actor = ClusterNodeId::new("n1").unwrap();
    let cluster_id = ClusterId::new("cluster-a").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: actor.clone(),
        node_address: "127.0.0.1:9441".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 2,
        initial_ranges: 4,
        suspect_after_ms: 1_000,
        dead_after_ms: 2_000,
        ..DistributionConfig::default()
    };
    let mut topology =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    topology
        .join_node(
            ClusterNode::new(
                ClusterNodeId::new("n2").unwrap(),
                "127.0.0.1:9442",
                1,
                10_000,
                20,
            )
            .unwrap(),
            &actor,
            20,
        )
        .unwrap();
    let (_, relocations) = topology
        .start_failure_repair_cycle(
            &RebalanceOptions {
                max_replica_moves: 1,
                max_moves_per_node: 1,
                unknown_range_bytes: 1,
                ..RebalanceOptions::default()
            },
            &actor,
            30,
        )
        .unwrap();
    let relocation = topology.relocation(relocations[0]).unwrap().clone();
    let range = topology
        .topology()
        .range_by_id(relocation.range_id)
        .unwrap()
        .clone();

    let mut target =
        BicDb::open_with_config(target_root.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut learner =
        RangeLearnerStore::open(target_root.path(), cluster_id.clone(), false).unwrap();
    learner.prepare_learner(&relocation).unwrap();

    let inside_id = record_id_for(&range, true);
    let outside_id = record_id_for(&range, false);
    let record = Record::new(&inside_id).with_metadata(json!({"version": 1}));
    let batch_bytes = serde_json::to_vec(&record).unwrap().len();
    let batch = RangeSnapshotBatch {
        collection: "documents".to_string(),
        range_id: range.id,
        range_epoch: range.epoch,
        snapshot_commit_sequence: 5,
        resume_after_key: inside_id.clone(),
        serialized_record_bytes: batch_bytes,
        records: vec![record],
    };
    let meta = CollectionMeta::standard("documents");
    let progress = learner
        .apply_snapshot_batch(&mut target, &relocation, &range, None, &meta, &batch)
        .unwrap();
    assert_eq!(progress.bytes_copied, batch_bytes as u64);

    // Simulate an acknowledged data write whose topology checkpoint was lost.
    let duplicate = learner
        .apply_snapshot_batch(&mut target, &relocation, &range, None, &meta, &batch)
        .unwrap();
    assert_eq!(duplicate.bytes_copied, batch_bytes as u64);
    learner
        .finish_snapshot(&relocation, Some(&inside_id), 5)
        .unwrap();

    let updated = Record::new(&inside_id).with_metadata(json!({"version": 2}));
    let outside = Record::new(&outside_id).with_metadata(json!({"version": 99}));
    let frame = CommitFrame::new(
        cluster_id.as_str(),
        "n1",
        "range-relocation",
        6,
        60,
        1_000,
        vec![
            ReplicationWrite {
                collection: "documents".to_string(),
                record_id: inside_id.clone(),
                operation: ReplicationOperationType::Upsert,
                payload: serde_json::to_vec(&updated).unwrap(),
                schema_version: 0,
                collection_meta: Some(meta.clone()),
            },
            ReplicationWrite {
                collection: "documents".to_string(),
                record_id: outside_id.clone(),
                operation: ReplicationOperationType::Upsert,
                payload: serde_json::to_vec(&outside).unwrap(),
                schema_version: 0,
                collection_meta: Some(meta),
            },
        ],
    );
    let catch_up = learner
        .apply_commit_frames(&mut target, &relocation, &range, &[frame.clone()], 6)
        .unwrap();
    assert_eq!(catch_up.destination_durable_commit_sequence, 6);
    assert_eq!(
        target
            .get("documents", &inside_id)
            .unwrap()
            .unwrap()
            .metadata["version"],
        2
    );
    assert!(target.get("documents", &outside_id).unwrap().is_none());

    // The same source frame after restart is a no-op at the source watermark.
    drop(learner);
    let mut reopened = RangeLearnerStore::open(target_root.path(), cluster_id, false).unwrap();
    let repeated = reopened
        .apply_commit_frames(&mut target, &relocation, &range, &[frame], 6)
        .unwrap();
    assert_eq!(repeated.destination_durable_commit_sequence, 6);
    assert_eq!(
        reopened
            .state(relocation.id)
            .unwrap()
            .durable_source_commit_sequence,
        Some(6)
    );
}

#[test]
fn in_process_transport_moves_a_live_range_with_bounded_cleanup() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("moving-cluster").unwrap();
    let actor = ClusterNodeId::new("n1").unwrap();
    let target_id = ClusterNodeId::new("n2").unwrap();
    let replication = ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Primary,
        listen_addr: Some("127.0.0.1:9441".to_string()),
        advertise_addr: Some("127.0.0.1:9441".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: false,
            dev_localhost_plaintext: true,
        }),
        cluster_id: cluster_id.as_str().to_string(),
        node_id: actor.as_str().to_string(),
        ..ReplicationConfig::default()
    };
    let source_config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_replication(replication);
    let mut source = BicDb::open_with_config(source_root.path(), source_config.clone()).unwrap();
    source.create_collection("documents").unwrap();
    source
        .batch_insert(
            "documents",
            (0..100).map(|number| {
                Record::new(format!("doc-{number:05}"))
                    .with_metadata(json!({"version": 1, "number": number}))
            }),
        )
        .unwrap();
    source.close().unwrap();
    let source = Arc::new(RwLock::new(
        BicDb::open_with_config(source_root.path(), source_config).unwrap(),
    ));
    let target = Arc::new(RwLock::new(
        BicDb::open_with_config(
            target_root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap(),
    ));

    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: actor.clone(),
        node_address: "127.0.0.1:9441".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 4,
        suspect_after_ms: 10_000,
        dead_after_ms: 20_000,
        ..DistributionConfig::default()
    };
    let mut topology =
        DistributionStore::initialize_at(source_root.path(), config, false, 10).unwrap();
    topology
        .join_node(
            ClusterNode::new(target_id.clone(), "127.0.0.1:9442", 1, 10_000, 20).unwrap(),
            &actor,
            20,
        )
        .unwrap();
    let (_, relocations) = topology
        .start_failure_repair_cycle(
            &RebalanceOptions {
                max_replica_moves: 1,
                max_moves_per_node: 1,
                unknown_range_bytes: 1,
                ..RebalanceOptions::default()
            },
            &actor,
            30,
        )
        .unwrap();
    let relocation_id = relocations[0];
    let range = topology
        .topology()
        .range_by_id(topology.relocation(relocation_id).unwrap().range_id)
        .unwrap()
        .clone();
    let inside_ids = (0..100)
        .map(|number| format!("doc-{number:05}"))
        .filter(|id| range.contains_token(distribution_key_token("documents", id)))
        .collect::<Vec<_>>();
    let outside_ids = (0..100)
        .map(|number| format!("doc-{number:05}"))
        .filter(|id| !range.contains_token(distribution_key_token("documents", id)))
        .collect::<Vec<_>>();
    assert!(!inside_ids.is_empty());
    assert!(!outside_ids.is_empty());

    let mut transport = InProcessClusterRelocationTransport::new(cluster_id);
    transport
        .register_node(
            actor.clone(),
            source_root.path(),
            Arc::clone(&source),
            false,
        )
        .unwrap();
    transport
        .register_node(
            target_id.clone(),
            target_root.path(),
            Arc::clone(&target),
            false,
        )
        .unwrap();
    let mut driver = TransportClusterRelocationDriver::new(
        transport,
        ClusterRelocationTransportConfig {
            snapshot: bicdb_core::RangeSnapshotOptions {
                max_records_per_batch: 3,
                max_bytes_per_batch: 4 * 1024,
                max_record_bytes: 4 * 1024,
            },
            max_commit_frames_per_step: 2,
        },
    )
    .unwrap();
    let cancellation = CancellationToken::uncancelable();
    let mut injected_catch_up_write = false;
    for now_ms in 40..1_000 {
        if topology.relocation(relocation_id).unwrap().phase == RelocationPhase::Completed {
            break;
        }
        driver
            .drive_relocation(&mut topology, relocation_id, &actor, now_ms, &cancellation)
            .unwrap();
        if !injected_catch_up_write
            && topology.relocation(relocation_id).unwrap().phase == RelocationPhase::CatchingUp
        {
            source
                .write()
                .insert(
                    "documents",
                    Record::new(&inside_ids[0])
                        .with_metadata(json!({"version": 2, "during": "catch_up"})),
                )
                .unwrap();
            injected_catch_up_write = true;
        }
    }
    assert!(injected_catch_up_write);
    assert_eq!(
        topology.relocation(relocation_id).unwrap().phase,
        RelocationPhase::Completed
    );
    assert!(topology
        .topology()
        .range_by_id(range.id)
        .unwrap()
        .replicas
        .iter()
        .all(|replica| replica.node_id == target_id));

    let source = source.read();
    let target = target.read();
    for id in &inside_ids {
        assert!(source.get("documents", id).unwrap().is_none());
        assert!(target.get("documents", id).unwrap().is_some());
    }
    assert_eq!(
        target
            .get("documents", &inside_ids[0])
            .unwrap()
            .unwrap()
            .metadata["version"],
        2
    );
    for id in &outside_ids {
        assert!(source.get("documents", id).unwrap().is_some());
        assert!(target.get("documents", id).unwrap().is_none());
    }
}

#[test]
fn abrupt_node_and_controller_restart_resumes_every_physical_relocation_phase() {
    let crash_cases = [
        (RelocationPhase::LearnerAllocated, false),
        (RelocationPhase::SnapshotCopying, false),
        (RelocationPhase::CatchingUp, false),
        (RelocationPhase::ReadyToPromote, false),
        (RelocationPhase::Promoted, false),
        (RelocationPhase::CleaningUp, true),
    ];
    for (crash_phase, crash_source) in crash_cases {
        let source_root = tempfile::tempdir().unwrap();
        let target_root = tempfile::tempdir().unwrap();
        let topology_root = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId::new(format!("restart-{crash_phase:?}")).unwrap();
        let source_id = ClusterNodeId::new("n1").unwrap();
        let target_id = ClusterNodeId::new("n2").unwrap();
        let source_config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode: ReplicationMode::Primary,
                listen_addr: Some("127.0.0.1:1".to_string()),
                advertise_addr: Some("127.0.0.1:1".to_string()),
                tls: Some(ReplicationTlsConfig {
                    cert_path: PathBuf::new(),
                    key_path: PathBuf::new(),
                    ca_path: PathBuf::new(),
                    require_client_cert: false,
                    dev_localhost_plaintext: true,
                }),
                cluster_id: cluster_id.as_str().to_string(),
                node_id: source_id.as_str().to_string(),
                ..ReplicationConfig::default()
            });
        let target_config = DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged);
        let mut source_db =
            BicDb::open_with_config(source_root.path(), source_config.clone()).unwrap();
        source_db.create_collection("documents").unwrap();
        source_db
            .batch_insert(
                "documents",
                (0..64).map(|number| {
                    Record::new(format!("doc-{number:05}"))
                        .with_metadata(json!({"version": 1, "number": number}))
                }),
            )
            .unwrap();
        source_db.close().unwrap();
        let mut source = Arc::new(RwLock::new(
            BicDb::open_with_config(source_root.path(), source_config.clone()).unwrap(),
        ));
        let mut target = Arc::new(RwLock::new(
            BicDb::open_with_config(target_root.path(), target_config.clone()).unwrap(),
        ));

        let distribution_config = DistributionConfig {
            enabled: true,
            cluster_id: cluster_id.clone(),
            node_id: source_id.clone(),
            node_address: "127.0.0.1:1".to_string(),
            node_capacity_bytes: 10_000,
            replication_factor: 1,
            initial_ranges: 4,
            suspect_after_ms: 10_000,
            dead_after_ms: 20_000,
            ..DistributionConfig::default()
        };
        let mut topology = DistributionStore::initialize_at(
            topology_root.path(),
            distribution_config.clone(),
            true,
            10,
        )
        .unwrap();
        topology
            .join_node(
                ClusterNode::new(target_id.clone(), "127.0.0.1:2", 1, 10_000, 20).unwrap(),
                &source_id,
                20,
            )
            .unwrap();
        let (_, relocation_ids) = topology
            .start_failure_repair_cycle(
                &RebalanceOptions {
                    max_replica_moves: 1,
                    max_moves_per_node: 1,
                    unknown_range_bytes: 1,
                    ..RebalanceOptions::default()
                },
                &source_id,
                30,
            )
            .unwrap();
        let relocation_id = relocation_ids[0];
        let range = topology
            .topology()
            .range_by_id(topology.relocation(relocation_id).unwrap().range_id)
            .unwrap()
            .clone();
        let inside_ids = (0..64)
            .map(|number| format!("doc-{number:05}"))
            .filter(|id| range.contains_token(distribution_key_token("documents", id)))
            .collect::<Vec<_>>();
        let outside_ids = (0..64)
            .map(|number| format!("doc-{number:05}"))
            .filter(|id| !range.contains_token(distribution_key_token("documents", id)))
            .collect::<Vec<_>>();
        assert!(inside_ids.len() > 4, "{crash_phase:?}");
        assert!(!outside_ids.is_empty(), "{crash_phase:?}");

        let mut driver = Some(in_process_relocation_driver(
            &cluster_id,
            &source_id,
            source_root.path(),
            &source,
            &target_id,
            target_root.path(),
            &target,
        ));
        let cancellation = CancellationToken::uncancelable();
        let mut catch_up_writes_injected = false;
        let mut crashed = false;
        for now_ms in 40..2_000 {
            let relocation = topology.relocation(relocation_id).unwrap().clone();
            if relocation.phase == RelocationPhase::Completed {
                break;
            }
            if relocation.phase == RelocationPhase::CatchingUp && !catch_up_writes_injected {
                for id in inside_ids.iter().take(3) {
                    source
                        .write()
                        .insert(
                            "documents",
                            Record::new(id)
                                .with_metadata(json!({"version": 2, "during": "catch_up"})),
                        )
                        .unwrap();
                }
                catch_up_writes_injected = true;
            }
            let partial_phase_is_durable = match crash_phase {
                RelocationPhase::SnapshotCopying => relocation.snapshot_bytes_copied > 0,
                RelocationPhase::CatchingUp => {
                    relocation.destination_durable_commit_sequence
                        > relocation.snapshot_commit_sequence
                        && relocation.destination_durable_commit_sequence
                            < relocation.source_commit_sequence
                }
                RelocationPhase::CleaningUp => relocation.cleanup_records_deleted > 0,
                _ => true,
            };
            if !crashed && relocation.phase == crash_phase && partial_phase_is_durable {
                drop(driver.take());
                drop(topology);
                if crash_source {
                    source = crash_reopen_database(source, source_root.path(), &source_config);
                } else {
                    target = crash_reopen_database(target, target_root.path(), &target_config);
                }
                topology = DistributionStore::open(
                    topology_root.path(),
                    distribution_config.clone(),
                    true,
                )
                .unwrap();
                assert_eq!(
                    topology.relocation(relocation_id).unwrap().phase,
                    crash_phase,
                    "{crash_phase:?}"
                );
                driver = Some(in_process_relocation_driver(
                    &cluster_id,
                    &source_id,
                    source_root.path(),
                    &source,
                    &target_id,
                    target_root.path(),
                    &target,
                ));
                crashed = true;
                continue;
            }
            driver
                .as_mut()
                .unwrap()
                .drive_relocation(
                    &mut topology,
                    relocation_id,
                    &source_id,
                    now_ms,
                    &cancellation,
                )
                .unwrap();
        }
        assert!(crashed, "did not reach crash point {crash_phase:?}");
        assert!(catch_up_writes_injected, "{crash_phase:?}");
        assert_eq!(
            topology.relocation(relocation_id).unwrap().phase,
            RelocationPhase::Completed,
            "{crash_phase:?}"
        );
        drop(driver);
        drop(topology);
        let reopened_topology =
            DistributionStore::open(topology_root.path(), distribution_config, true).unwrap();
        assert_eq!(
            reopened_topology.relocation(relocation_id).unwrap().phase,
            RelocationPhase::Completed,
            "{crash_phase:?}"
        );
        assert!(reopened_topology
            .topology()
            .range_by_id(range.id)
            .unwrap()
            .replicas
            .iter()
            .all(|replica| replica.node_id == target_id));

        let source = source.read();
        let target = target.read();
        for id in &inside_ids {
            assert!(
                source.get("documents", id).unwrap().is_none(),
                "source retained {id} after {crash_phase:?}"
            );
            let record = target
                .get("documents", id)
                .unwrap()
                .unwrap_or_else(|| panic!("target lost {id} after {crash_phase:?}"));
            let expected_version = if inside_ids.iter().take(3).any(|updated| updated == id) {
                2
            } else {
                1
            };
            assert_eq!(
                record.metadata["version"], expected_version,
                "{id} after {crash_phase:?}"
            );
        }
        for id in &outside_ids {
            assert!(
                source.get("documents", id).unwrap().is_some(),
                "source lost out-of-range {id} after {crash_phase:?}"
            );
            assert!(
                target.get("documents", id).unwrap().is_none(),
                "target copied out-of-range {id} after {crash_phase:?}"
            );
        }
    }
}

#[test]
fn tcp_transport_moves_a_live_range_over_bounded_authenticated_node_rpcs() {
    let source_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let topology_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("tcp-moving-cluster").unwrap();
    let actor = ClusterNodeId::new("n1").unwrap();
    let target_id = ClusterNodeId::new("n2").unwrap();
    let replication = ReplicationConfig {
        enabled: true,
        mode: ReplicationMode::Primary,
        listen_addr: Some("127.0.0.1:1".to_string()),
        advertise_addr: Some("127.0.0.1:1".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            ca_path: PathBuf::new(),
            require_client_cert: false,
            dev_localhost_plaintext: true,
        }),
        cluster_id: cluster_id.as_str().to_string(),
        node_id: actor.as_str().to_string(),
        ..ReplicationConfig::default()
    };
    let source_config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_replication(replication);
    let mut source = BicDb::open_with_config(source_root.path(), source_config.clone()).unwrap();
    source.create_collection("documents").unwrap();
    source
        .batch_insert(
            "documents",
            (0..80).map(|number| {
                Record::new(format!("doc-{number:05}"))
                    .with_metadata(json!({"version": 1, "number": number}))
            }),
        )
        .unwrap();
    source.close().unwrap();
    let source = Arc::new(RwLock::new(
        BicDb::open_with_config(source_root.path(), source_config).unwrap(),
    ));
    let target = Arc::new(RwLock::new(
        BicDb::open_with_config(
            target_root.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap(),
    ));

    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: actor.clone(),
        node_address: "127.0.0.1:1".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 4,
        suspect_after_ms: 10_000,
        dead_after_ms: 20_000,
        ..DistributionConfig::default()
    };
    let mut topology =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    topology
        .join_node(
            ClusterNode::new(target_id.clone(), "127.0.0.1:2", 1, 10_000, 20).unwrap(),
            &actor,
            20,
        )
        .unwrap();
    let source_schema_sha256 = source
        .read()
        .schema_compatibility_fingerprint()
        .unwrap()
        .sha256;
    topology
        .heartbeat(
            &actor,
            1,
            1,
            10_000,
            BTreeMap::from([(
                SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                source_schema_sha256.clone(),
            )]),
            21,
        )
        .unwrap();
    topology
        .heartbeat(
            &target_id,
            1,
            1,
            10_000,
            BTreeMap::from([(SCHEMA_BOOTSTRAP_NODE_LABEL.to_string(), "true".to_string())]),
            22,
        )
        .unwrap();
    let (_, relocations) = topology
        .start_failure_repair_cycle(
            &RebalanceOptions {
                max_replica_moves: 1,
                max_moves_per_node: 1,
                unknown_range_bytes: 1,
                ..RebalanceOptions::default()
            },
            &actor,
            30,
        )
        .unwrap();
    let relocation_id = relocations[0];
    let range = topology
        .topology()
        .range_by_id(topology.relocation(relocation_id).unwrap().range_id)
        .unwrap()
        .clone();
    let inside_ids = (0..80)
        .map(|number| format!("doc-{number:05}"))
        .filter(|id| range.contains_token(distribution_key_token("documents", id)))
        .collect::<Vec<_>>();
    let outside_ids = (0..80)
        .map(|number| format!("doc-{number:05}"))
        .filter(|id| !range.contains_token(distribution_key_token("documents", id)))
        .collect::<Vec<_>>();
    assert!(!inside_ids.is_empty());
    assert!(!outside_ids.is_empty());

    let shared_topology = Arc::new(RwLock::new(topology.topology().clone()));
    let source_service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            actor.clone(),
            source_root.path(),
            Arc::clone(&source),
            false,
        )
        .unwrap(),
    ));
    let target_service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            target_id.clone(),
            target_root.path(),
            Arc::clone(&target),
            false,
        )
        .unwrap(),
    ));
    let network = ClusterNetworkTransportConfig {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 5_000,
        max_frame_bytes: 1024 * 1024,
        max_inbound_connections: 4,
        dev_localhost_plaintext: true,
        tls: None,
    };
    let source_server = start_cluster_data_server(
        "127.0.0.1:0",
        network.clone(),
        Arc::clone(&shared_topology),
        source_service,
        None,
        None,
        None,
    )
    .unwrap();
    let target_server = start_cluster_data_server(
        "127.0.0.1:0",
        network.clone(),
        Arc::clone(&shared_topology),
        target_service,
        None,
        None,
        None,
    )
    .unwrap();
    {
        let mut shared = shared_topology.write();
        shared.nodes.get_mut(&actor).unwrap().address = source_server.local_addr().to_string();
        shared.nodes.get_mut(&target_id).unwrap().address = target_server.local_addr().to_string();
    }
    let transport = TcpClusterRelocationTransport::new(
        cluster_id,
        actor.clone(),
        Arc::clone(&shared_topology),
        network,
    )
    .unwrap();
    transport.ping(&actor).unwrap();
    transport.ping(&target_id).unwrap();
    let mut driver = TransportClusterRelocationDriver::new(
        transport,
        ClusterRelocationTransportConfig {
            snapshot: bicdb_core::RangeSnapshotOptions {
                max_records_per_batch: 2,
                max_bytes_per_batch: 4 * 1024,
                max_record_bytes: 4 * 1024,
            },
            max_commit_frames_per_step: 2,
        },
    )
    .unwrap();
    let cancellation = CancellationToken::uncancelable();
    let mut injected_catch_up_write = false;
    let mut published_target_schema = false;
    for now_ms in 40..1_000 {
        if topology.relocation(relocation_id).unwrap().phase == RelocationPhase::Completed {
            break;
        }
        driver
            .drive_relocation(&mut topology, relocation_id, &actor, now_ms, &cancellation)
            .unwrap();
        if !published_target_schema
            && target
                .read()
                .schema_compatibility_fingerprint()
                .unwrap()
                .sha256
                == source_schema_sha256
        {
            let labels = BTreeMap::from([(
                SCHEMA_COMPATIBILITY_NODE_LABEL.to_string(),
                source_schema_sha256.clone(),
            )]);
            topology
                .heartbeat(&target_id, 1, 1, 10_000, labels.clone(), now_ms)
                .unwrap();
            shared_topology
                .write()
                .nodes
                .get_mut(&target_id)
                .unwrap()
                .labels = labels;
            published_target_schema = true;
        }
        if !injected_catch_up_write
            && topology.relocation(relocation_id).unwrap().phase == RelocationPhase::CatchingUp
        {
            source
                .write()
                .insert(
                    "documents",
                    Record::new(&inside_ids[0])
                        .with_metadata(json!({"version": 2, "during": "tcp_catch_up"})),
                )
                .unwrap();
            injected_catch_up_write = true;
        }
    }
    assert!(injected_catch_up_write);
    assert_eq!(
        topology.relocation(relocation_id).unwrap().phase,
        RelocationPhase::Completed
    );

    let source = source.read();
    let target = target.read();
    for id in &inside_ids {
        assert!(source.get("documents", id).unwrap().is_none());
        assert!(target.get("documents", id).unwrap().is_some());
    }
    assert_eq!(
        target
            .get("documents", &inside_ids[0])
            .unwrap()
            .unwrap()
            .metadata["version"],
        2
    );
    for id in &outside_ids {
        assert!(source.get("documents", id).unwrap().is_some());
        assert!(target.get("documents", id).unwrap().is_none());
    }
    drop(source);
    drop(target);
    source_server.join().unwrap();
    target_server.join().unwrap();
}

#[test]
fn authenticated_tcp_range_write_is_durable_and_applied_on_follower() {
    let topology_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("range-write-wire").unwrap();
    let leader_id = ClusterNodeId::new("n1").unwrap();
    let follower_id = ClusterNodeId::new("n2").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: leader_id.clone(),
        node_address: "127.0.0.1:1".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 2,
        initial_ranges: 4,
        ..DistributionConfig::default()
    };
    let mut store =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    store
        .join_node(
            ClusterNode::new(follower_id.clone(), "127.0.0.1:2", 1, 10_000, 20).unwrap(),
            &leader_id,
            20,
        )
        .unwrap();
    let mut topology = store.topology().clone();
    for range in topology.ranges.values_mut() {
        let replica_id = ReplicaId::new(topology.next_replica_id).unwrap();
        topology.next_replica_id += 1;
        range.replicas.push(RangeReplica {
            id: replica_id,
            node_id: follower_id.clone(),
            role: RangeReplicaRole::Voter,
        });
    }
    topology.validate().unwrap();
    let target = Arc::new(RwLock::new(
        BicDb::open_with_config(target_root.path(), DbConfig::default().with_fsync(false)).unwrap(),
    ));
    target.write().create_collection("documents").unwrap();
    let target_service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            follower_id.clone(),
            target_root.path(),
            Arc::clone(&target),
            false,
        )
        .unwrap(),
    ));
    let shared_topology = Arc::new(RwLock::new(topology));
    let network = ClusterNetworkTransportConfig {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 5_000,
        max_frame_bytes: 1024 * 1024,
        max_inbound_connections: 4,
        dev_localhost_plaintext: true,
        tls: None,
    };
    let target_server = start_cluster_data_server(
        "127.0.0.1:0",
        network.clone(),
        Arc::clone(&shared_topology),
        target_service,
        None,
        None,
        None,
    )
    .unwrap();
    shared_topology
        .write()
        .nodes
        .get_mut(&follower_id)
        .unwrap()
        .address = target_server.local_addr().to_string();

    let record_id = "wire-range-command";
    let range = shared_topology
        .read()
        .range_for_key("documents", record_id)
        .unwrap()
        .clone();
    let record = Record::new(record_id).with_metadata(json!({"replicated": true}));
    let mut command = RangeWriteCommand {
        protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
        cluster_id: cluster_id.clone(),
        range_id: range.id,
        range_epoch: range.epoch,
        index: 1,
        command_id: "wire-command-1".to_string(),
        leader_node_id: leader_id.clone(),
        transaction_id: 77,
        mutations: vec![CommitAdmissionMutation {
            collection: "documents".to_string(),
            record_id: record_id.to_string(),
            record: Some(record),
        }],
        checksum_sha256: String::new(),
    };
    command.checksum_sha256 = command.calculate_checksum().unwrap();
    let transport = TcpClusterRelocationTransport::new(
        cluster_id,
        leader_id,
        Arc::clone(&shared_topology),
        network,
    )
    .unwrap();
    assert_eq!(
        transport
            .prepare_range_write(&follower_id, &command)
            .unwrap()
            .state,
        RangeWriteState::Prepared
    );
    assert_eq!(
        transport
            .commit_range_write(&follower_id, &command)
            .unwrap()
            .state,
        RangeWriteState::Committed
    );
    assert!(target.read().get("documents", record_id).unwrap().is_none());
    assert_eq!(
        transport
            .certify_range_write(&follower_id, &command)
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );
    assert_eq!(
        transport
            .apply_range_write(&follower_id, &command)
            .unwrap()
            .state,
        RangeWriteState::Applied
    );
    // Network ambiguity may repeat the exact apply; it must remain safe.
    assert_eq!(
        transport
            .apply_range_write(&follower_id, &command)
            .unwrap()
            .state,
        RangeWriteState::Applied
    );
    assert_eq!(
        target
            .read()
            .get("documents", record_id)
            .unwrap()
            .unwrap()
            .metadata["replicated"],
        true
    );

    let repair_record_id = (0..100_000)
        .map(|number| format!("wire-repair-{number}"))
        .find(|record_id| range.contains_token(distribution_key_token("documents", record_id)))
        .unwrap();
    let repair_record = Record::new(&repair_record_id).with_metadata(json!({"repaired": true}));
    let mut repair_command = RangeWriteCommand {
        protocol_version: RANGE_WRITE_PROTOCOL_VERSION,
        cluster_id: command.cluster_id.clone(),
        range_id: range.id,
        range_epoch: range.epoch,
        index: 2,
        command_id: "wire-command-2".to_string(),
        leader_node_id: command.leader_node_id.clone(),
        transaction_id: 78,
        mutations: vec![CommitAdmissionMutation {
            collection: "documents".to_string(),
            record_id: repair_record_id.clone(),
            record: Some(repair_record),
        }],
        checksum_sha256: String::new(),
    };
    repair_command.checksum_sha256 = repair_command.calculate_checksum().unwrap();
    let leader_log_root = tempfile::tempdir().unwrap();
    let mut leader_log = RangeWriteStore::open(
        leader_log_root.path(),
        command.cluster_id.clone(),
        command.leader_node_id.clone(),
        false,
    )
    .unwrap();
    leader_log.prepare(repair_command.clone()).unwrap();
    leader_log.commit(&repair_command).unwrap();
    leader_log.certify(&repair_command).unwrap();
    leader_log.mark_applied(&repair_command).unwrap();
    let limits = RangeWriteRepairLimits::default();
    let batch = leader_log
        .export_repair_batch(&command.leader_node_id, range.id, range.epoch, 1, &limits)
        .unwrap();
    assert_eq!(
        transport
            .range_write_progress(&follower_id, range.id, range.epoch)
            .unwrap()
            .resolved_through,
        1
    );
    assert_eq!(
        transport
            .apply_range_write_repair(&follower_id, &batch, &limits)
            .unwrap()
            .resolved_through,
        2
    );
    assert_eq!(
        target
            .read()
            .get("documents", &repair_record_id)
            .unwrap()
            .unwrap()
            .metadata["repaired"],
        true
    );
    let fetched = transport
        .fetch_range_write_repair(&follower_id, range.id, range.epoch, 0, &limits)
        .unwrap();
    assert_eq!(fetched.source_node_id, follower_id);
    assert_eq!(fetched.previous_resolved_index, 0);
    assert_eq!(fetched.entries.len(), 2);
    assert!(fetched
        .entries
        .iter()
        .all(|entry| entry.state == RangeWriteState::Applied));
    let probe = transport
        .probe_range_write(&follower_id, range.id, range.epoch, 2)
        .unwrap();
    assert_eq!(probe.node_id, follower_id);
    assert_eq!(probe.index, 2);
    assert_eq!(probe.entry.unwrap().state, RangeWriteState::Applied);
    target_server.join().unwrap();
}

#[test]
fn member_heartbeat_updates_the_controller_and_returns_authoritative_topology() {
    let topology_root = tempfile::tempdir().unwrap();
    let data_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("heartbeat-cluster").unwrap();
    let controller_id = ClusterNodeId::new("n1").unwrap();
    let member_id = ClusterNodeId::new("n2").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: controller_id.clone(),
        node_address: "127.0.0.1:1".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 2,
        suspect_after_ms: 10_000,
        dead_after_ms: 20_000,
        ..DistributionConfig::default()
    };
    let mut store =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    store
        .join_node(
            ClusterNode::new(member_id.clone(), "127.0.0.1:2", 1, 20_000, 20).unwrap(),
            &controller_id,
            20,
        )
        .unwrap();
    let control_store = Arc::new(Mutex::new(store));
    let shared_topology = Arc::new(RwLock::new(control_store.lock().topology().clone()));
    let db = Arc::new(RwLock::new(
        BicDb::open_with_config(data_root.path(), DbConfig::default().with_fsync(false)).unwrap(),
    ));
    let service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            controller_id.clone(),
            data_root.path(),
            db,
            false,
        )
        .unwrap(),
    ));
    let network = ClusterNetworkTransportConfig {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 5_000,
        max_frame_bytes: 1024 * 1024,
        max_inbound_connections: 4,
        dev_localhost_plaintext: true,
        tls: None,
    };
    let server = start_cluster_data_server(
        "127.0.0.1:0",
        network.clone(),
        Arc::clone(&shared_topology),
        service,
        Some(Arc::clone(&control_store)),
        None,
        None,
    )
    .unwrap();
    {
        let mut topology = shared_topology.write();
        topology.nodes.get_mut(&controller_id).unwrap().address = server.local_addr().to_string();
    }
    let transport =
        TcpClusterRelocationTransport::new(cluster_id, member_id.clone(), shared_topology, network)
            .unwrap();
    let labels = BTreeMap::from([
        ("server".to_string(), "n2".to_string()),
        ("zone".to_string(), "west".to_string()),
    ]);
    let topology = transport
        .heartbeat(&controller_id, 1, 1_234, 20_000, labels.clone(), 500)
        .unwrap();
    let member = topology.nodes.get(&member_id).unwrap();
    assert_eq!(member.last_heartbeat_ms, 500);
    assert_eq!(member.used_bytes, 1_234);
    assert_eq!(member.labels, labels);
    assert_eq!(
        control_store
            .lock()
            .topology()
            .nodes
            .get(&member_id)
            .unwrap()
            .last_heartbeat_ms,
        500
    );
    server.join().unwrap();
}

#[test]
fn metadata_vote_append_and_commit_cross_the_bounded_node_transport() {
    let topology_root = tempfile::tempdir().unwrap();
    let leader_consensus_root = tempfile::tempdir().unwrap();
    let follower_consensus_root = tempfile::tempdir().unwrap();
    let follower_data_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("metadata-rpc-cluster").unwrap();
    let leader_id = ClusterNodeId::new("n1").unwrap();
    let follower_id = ClusterNodeId::new("n2").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: leader_id.clone(),
        node_address: "127.0.0.1:1".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 2,
        ..DistributionConfig::default()
    };
    let mut distribution =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    distribution
        .join_node(
            ClusterNode::new(follower_id.clone(), "127.0.0.1:2", 1, 10_000, 20).unwrap(),
            &leader_id,
            20,
        )
        .unwrap();
    let initial_topology = distribution.topology().clone();
    let mut leader_consensus = MetadataConsensusStore::open(
        leader_consensus_root.path(),
        cluster_id.clone(),
        leader_id.clone(),
        initial_topology.clone(),
        false,
    )
    .unwrap();
    let follower_consensus = Arc::new(Mutex::new(
        MetadataConsensusStore::open(
            follower_consensus_root.path(),
            cluster_id.clone(),
            follower_id.clone(),
            initial_topology.clone(),
            false,
        )
        .unwrap(),
    ));
    let shared_topology = Arc::new(RwLock::new(initial_topology));
    let follower_db = Arc::new(RwLock::new(
        BicDb::open_with_config(
            follower_data_root.path(),
            DbConfig::default().with_fsync(false),
        )
        .unwrap(),
    ));
    let follower_service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            follower_id.clone(),
            follower_data_root.path(),
            follower_db,
            false,
        )
        .unwrap(),
    ));
    let network = ClusterNetworkTransportConfig {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 5_000,
        max_frame_bytes: 1024 * 1024,
        max_inbound_connections: 4,
        dev_localhost_plaintext: true,
        tls: None,
    };
    let follower_server = start_cluster_data_server(
        "127.0.0.1:0",
        network.clone(),
        Arc::clone(&shared_topology),
        follower_service,
        None,
        Some(Arc::clone(&follower_consensus)),
        None,
    )
    .unwrap();
    shared_topology
        .write()
        .nodes
        .get_mut(&follower_id)
        .unwrap()
        .address = follower_server.local_addr().to_string();
    let transport = TcpClusterRelocationTransport::new(
        cluster_id,
        leader_id.clone(),
        Arc::clone(&shared_topology),
        network,
    )
    .unwrap();

    let vote_request = leader_consensus.start_election().unwrap();
    let vote = transport
        .request_metadata_vote(&follower_id, vote_request)
        .unwrap();
    assert!(vote.vote_granted);
    leader_consensus
        .become_leader(&BTreeSet::from([leader_id.clone(), follower_id.clone()]))
        .unwrap();
    let mut changed = shared_topology.read().clone();
    changed.generation = changed.generation.saturating_add(1);
    changed.nodes.get_mut(&follower_id).unwrap().used_bytes = 4_096;
    let entry = leader_consensus.propose_topology(changed.clone()).unwrap();
    let append = leader_consensus.append_request_from(1, 8).unwrap();
    let response = transport.append_metadata(&follower_id, append).unwrap();
    assert!(leader_consensus
        .record_append_response(&follower_id, &response)
        .unwrap());
    let commit = leader_consensus
        .append_request_from(entry.index.saturating_add(1), 1)
        .unwrap();
    assert!(
        transport
            .append_metadata(&follower_id, commit)
            .unwrap()
            .success
    );
    assert_eq!(leader_consensus.committed_topology(), &changed);
    assert_eq!(follower_consensus.lock().committed_topology(), &changed);
    follower_server.join().unwrap();
}

#[test]
fn empty_server_self_registers_and_provisions_from_remote_bootstrap_snapshot() {
    let topology_root = tempfile::tempdir().unwrap();
    let consensus_root = tempfile::tempdir().unwrap();
    let data_root = tempfile::tempdir().unwrap();
    let joined_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("remote-bootstrap-cluster").unwrap();
    let leader_id = ClusterNodeId::new("n1").unwrap();
    let learner_id = ClusterNodeId::new("n2").unwrap();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_address = probe.local_addr().unwrap();
    drop(probe);
    let network = ClusterNetworkTransportConfig {
        connect_timeout_ms: 2_000,
        io_timeout_ms: 5_000,
        max_frame_bytes: 1024 * 1024,
        max_inbound_connections: 4,
        dev_localhost_plaintext: true,
        tls: None,
    };
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: leader_id.clone(),
        node_address: listen_address.to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 2,
        transport: network.clone(),
        ..DistributionConfig::default()
    };
    let distribution =
        DistributionStore::initialize_at(topology_root.path(), config, true, 10).unwrap();
    let initial_topology = distribution.topology().clone();
    let mut consensus = MetadataConsensusStore::open(
        consensus_root.path(),
        cluster_id.clone(),
        leader_id.clone(),
        initial_topology.clone(),
        true,
    )
    .unwrap();
    consensus.start_election().unwrap();
    consensus
        .become_leader(&BTreeSet::from([leader_id.clone()]))
        .unwrap();
    let consensus = Arc::new(Mutex::new(consensus));
    let distribution = Arc::new(Mutex::new(distribution));
    let published_topology = Arc::new(RwLock::new(initial_topology));
    let db = Arc::new(RwLock::new(
        BicDb::open_with_config(data_root.path(), DbConfig::default().with_fsync(true)).unwrap(),
    ));
    let service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            leader_id.clone(),
            data_root.path(),
            db,
            true,
        )
        .unwrap(),
    ));
    let server = start_cluster_data_server(
        listen_address,
        network.clone(),
        Arc::clone(&published_topology),
        service,
        None,
        Some(Arc::clone(&consensus)),
        Some(Arc::clone(&distribution)),
    )
    .unwrap();
    let bootstrap =
        TcpClusterBootstrapClient::new(cluster_id.clone(), learner_id.clone(), network.clone())
            .unwrap();
    let initial = bootstrap
        .fetch_snapshot(&leader_id, &listen_address.to_string())
        .unwrap();
    assert_eq!(initial.metadata_leader_id, Some(leader_id.clone()));
    assert!(!initial.topology.nodes.contains_key(&learner_id));

    let learner = ClusterNode::new(learner_id.clone(), "127.0.0.1:29999", 1, 20_000, 20)
        .unwrap()
        .as_metadata_learner();
    let registration = bootstrap
        .register_metadata_learner(&leader_id, &listen_address.to_string(), learner.clone(), 20)
        .unwrap();
    assert!(!registration.nodes.contains_key(&learner_id));

    // The inbound RPC stages the learner. The normal supervisor publishes it
    // only after metadata consensus commits the new topology.
    let (staged, mutation) = distribution
        .lock()
        .fork_ephemeral_with_pending_metadata_mutations()
        .unwrap();
    consensus
        .lock()
        .propose_topology(staged.topology().clone())
        .unwrap();
    let committed = consensus.lock().committed_topology().clone();
    assert_eq!(committed.nodes.get(&learner_id), Some(&learner));
    distribution
        .lock()
        .install_authoritative_topology(committed.clone())
        .unwrap();
    distribution
        .lock()
        .acknowledge_pending_metadata_mutations(&mutation);
    *published_topology.write() = committed;

    let snapshot = bootstrap
        .fetch_snapshot(&leader_id, &listen_address.to_string())
        .unwrap();
    let provisioned = provision_cluster_member_directory(
        joined_root.path(),
        &snapshot,
        &learner_id,
        network,
        true,
    )
    .unwrap();
    assert_eq!(provisioned.cluster_id, cluster_id);
    assert_eq!(provisioned.node_id, learner_id);
    assert_eq!(provisioned.node_address, "127.0.0.1:29999");
    assert_eq!(
        DistributionStore::open(joined_root.path(), provisioned, true)
            .unwrap()
            .topology(),
        &snapshot.topology
    );
    server.join().unwrap();
}

#[test]
fn mtls_leaf_certificate_is_bound_to_node_identity_over_live_rpc() {
    let tls_root = tempfile::tempdir().unwrap();
    let identities =
        test_cluster_tls_identities(tls_root.path(), &["leader", "learner", "impostor"]);
    let leader_tls = identities["leader"].clone();
    let learner_tls = identities["learner"].clone();
    let impostor_tls = identities["impostor"].clone();

    let topology_root = tempfile::tempdir().unwrap();
    let consensus_root = tempfile::tempdir().unwrap();
    let data_root = tempfile::tempdir().unwrap();
    let cluster_id = ClusterId::new("mtls-identity-cluster").unwrap();
    let leader_id = ClusterNodeId::new("n1").unwrap();
    let learner_id = ClusterNodeId::new("n2").unwrap();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_address = probe.local_addr().unwrap();
    drop(probe);
    let leader_address = listen_address.to_string();
    let leader_network = leader_tls.network_config();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: leader_id.clone(),
        node_address: leader_address.clone(),
        node_tls_certificate_sha256: Some(leader_tls.fingerprint()),
        node_capacity_bytes: 10_000,
        replication_factor: 1,
        initial_ranges: 2,
        transport: leader_network.clone(),
        ..DistributionConfig::default()
    };
    let distribution =
        DistributionStore::initialize_at(topology_root.path(), config, true, 10).unwrap();
    let initial_topology = distribution.topology().clone();
    let mut consensus = MetadataConsensusStore::open(
        consensus_root.path(),
        cluster_id.clone(),
        leader_id.clone(),
        initial_topology.clone(),
        true,
    )
    .unwrap();
    consensus.start_election().unwrap();
    consensus
        .become_leader(&BTreeSet::from([leader_id.clone()]))
        .unwrap();
    let consensus = Arc::new(Mutex::new(consensus));
    let distribution = Arc::new(Mutex::new(distribution));
    let published_topology = Arc::new(RwLock::new(initial_topology));
    let db = Arc::new(RwLock::new(
        BicDb::open_with_config(data_root.path(), DbConfig::default().with_fsync(true)).unwrap(),
    ));
    let service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            cluster_id.clone(),
            leader_id.clone(),
            data_root.path(),
            db,
            true,
        )
        .unwrap(),
    ));
    let server = start_cluster_data_server(
        listen_address,
        leader_network,
        Arc::clone(&published_topology),
        service,
        None,
        Some(Arc::clone(&consensus)),
        Some(Arc::clone(&distribution)),
    )
    .unwrap();

    let bootstrap = TcpClusterBootstrapClient::new(
        cluster_id.clone(),
        learner_id.clone(),
        learner_tls.network_config(),
    )
    .unwrap();
    bootstrap
        .fetch_snapshot(&leader_id, &leader_address)
        .unwrap();
    let learner = ClusterNode::new(learner_id.clone(), "localhost:29999", 1, 20_000, 20)
        .unwrap()
        .with_tls_certificate_sha256(learner_tls.fingerprint())
        .unwrap()
        .as_metadata_learner();
    bootstrap
        .register_metadata_learner(&leader_id, &leader_address, learner.clone(), 20)
        .unwrap();

    let (staged, mutation) = distribution
        .lock()
        .fork_ephemeral_with_pending_metadata_mutations()
        .unwrap();
    consensus
        .lock()
        .propose_topology(staged.topology().clone())
        .unwrap();
    let committed = consensus.lock().committed_topology().clone();
    assert_eq!(committed.nodes.get(&learner_id), Some(&learner));
    distribution
        .lock()
        .install_authoritative_topology(committed.clone())
        .unwrap();
    distribution
        .lock()
        .acknowledge_pending_metadata_mutations(&mutation);
    *published_topology.write() = committed.clone();

    let legitimate = TcpClusterRelocationTransport::new(
        cluster_id.clone(),
        learner_id.clone(),
        Arc::new(RwLock::new(committed.clone())),
        learner_tls.network_config(),
    )
    .unwrap();
    legitimate.fetch_topology(&leader_id).unwrap();

    // A member also pins the destination's committed identity. CA validation
    // and a matching hostname alone are insufficient for cluster RPCs.
    let mut forged_destination_topology = committed.clone();
    forged_destination_topology
        .nodes
        .get_mut(&leader_id)
        .unwrap()
        .tls_certificate_sha256 = Some(impostor_tls.fingerprint());
    let wrong_destination = TcpClusterRelocationTransport::new(
        cluster_id.clone(),
        learner_id.clone(),
        Arc::new(RwLock::new(forged_destination_topology)),
        learner_tls.network_config(),
    )
    .unwrap();
    let error = wrong_destination.fetch_topology(&leader_id).unwrap_err();
    assert!(error.to_string().contains("destination"));
    assert!(error.to_string().contains("does not match membership"));

    // A second leaf signed by the same trusted CA can complete mTLS, but it
    // cannot put n2 in the RPC envelope. Give the malicious client a forged
    // local topology so the server-side membership check is what rejects it.
    let mut forged_topology = committed.clone();
    forged_topology
        .nodes
        .get_mut(&learner_id)
        .unwrap()
        .tls_certificate_sha256 = Some(impostor_tls.fingerprint());
    let impostor = TcpClusterRelocationTransport::new(
        cluster_id.clone(),
        learner_id.clone(),
        Arc::new(RwLock::new(forged_topology)),
        impostor_tls.network_config(),
    )
    .unwrap();
    let error = impostor.fetch_topology(&leader_id).unwrap_err();
    assert!(error.to_string().contains("does not match membership"));

    // Rotation is two-phase. The old identity stages the overlap through
    // quorum, then a heartbeat actually presented by the new identity
    // activates it through a second quorum publication.
    let next_fingerprint = impostor_tls.fingerprint();
    legitimate
        .stage_tls_certificate_rotation(&leader_id, next_fingerprint.clone(), 30)
        .unwrap();
    let (staged, mutation) = distribution
        .lock()
        .fork_ephemeral_with_pending_metadata_mutations()
        .unwrap();
    consensus
        .lock()
        .propose_topology(staged.topology().clone())
        .unwrap();
    let rotation_pending = consensus.lock().committed_topology().clone();
    assert_eq!(
        rotation_pending.nodes[&learner_id]
            .pending_tls_certificate_sha256
            .as_deref(),
        Some(next_fingerprint.as_str())
    );
    distribution
        .lock()
        .install_authoritative_topology(rotation_pending.clone())
        .unwrap();
    distribution
        .lock()
        .acknowledge_pending_metadata_mutations(&mutation);
    *published_topology.write() = rotation_pending.clone();
    legitimate
        .install_topology(rotation_pending.clone())
        .unwrap();

    let replacement = TcpClusterRelocationTransport::new(
        cluster_id,
        learner_id.clone(),
        Arc::new(RwLock::new(rotation_pending)),
        impostor_tls.network_config(),
    )
    .unwrap();
    replacement
        .heartbeat(
            &leader_id,
            learner.incarnation,
            learner.used_bytes,
            learner.capacity_bytes,
            learner.labels.clone(),
            40,
        )
        .unwrap();
    let (staged, mutation) = distribution
        .lock()
        .fork_ephemeral_with_pending_metadata_mutations()
        .unwrap();
    consensus
        .lock()
        .propose_topology(staged.topology().clone())
        .unwrap();
    let activated = consensus.lock().committed_topology().clone();
    assert_eq!(
        activated.nodes[&learner_id]
            .tls_certificate_sha256
            .as_deref(),
        Some(next_fingerprint.as_str())
    );
    assert!(activated.nodes[&learner_id]
        .pending_tls_certificate_sha256
        .is_none());
    distribution
        .lock()
        .install_authoritative_topology(activated.clone())
        .unwrap();
    distribution
        .lock()
        .acknowledge_pending_metadata_mutations(&mutation);
    *published_topology.write() = activated.clone();

    let error = legitimate.fetch_topology(&leader_id).unwrap_err();
    assert!(error.to_string().contains("does not match membership"));
    replacement.install_topology(activated).unwrap();
    replacement.fetch_topology(&leader_id).unwrap();
    server.join().unwrap();
}
