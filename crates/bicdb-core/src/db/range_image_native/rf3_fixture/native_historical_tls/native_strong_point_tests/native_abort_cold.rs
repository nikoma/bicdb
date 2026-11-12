//! Orderly full-owner cold cut after ACTUAL native Abort publication. This is
//! not SIGKILL/media durability or physical host qualification.
use super::*;

fn reopen_stopped(mut old: EnforcedNetwork) -> EnforcedNetwork {
    old.idle();
    let topology = old.native.topology.read().clone();
    let specs = old.native.specs.clone();
    let weak_databases = old
        .native
        .databases
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    let weak_services = old
        .native
        .services
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    let weak_runtimes = old
        .runtimes
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    let mut root = std::mem::replace(&mut old.native.root, tempfile::tempdir().unwrap());
    let mut keys = std::mem::replace(&mut old._keys, tempfile::tempdir().unwrap());
    root.disable_cleanup(true);
    keys.disable_cleanup(true);
    // Retain cold-cut roots on a subsequent failed assertion. Successful cases
    // clean them explicitly through their returned TempDirs.
    let root_path = root.path().to_path_buf();
    let keys_path = keys.path().to_path_buf();
    eprintln!("NATIVE_ABORT_COLD_DATABASES {}", root_path.display());
    eprintln!(
        "NATIVE_ABORT_COLD_METADATA_PRIVATE_TLS {}",
        keys_path.display()
    );
    drop(old);
    assert!(weak_databases.iter().all(|w| w.upgrade().is_none()));
    assert!(weak_services.iter().all(|w| w.upgrade().is_none()));
    assert!(weak_runtimes.iter().all(|w| w.upgrade().is_none()));
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let mut databases = BTreeMap::new();
    for (id, spec) in &specs {
        let (db, _) =
            BicDb::open_range_image_database(&root_path.join(id.as_str()), config.clone(), spec)
                .unwrap();
        assert_eq!(
            db.schema_compatibility_fingerprint().unwrap().sha256,
            topology.nodes[id].labels[crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL]
        );
        db.native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            id,
            spec.member_incarnation,
        )
        .unwrap();
        databases.insert(id.clone(), Arc::new(RwLock::new(db)));
    }
    let current = Arc::new(RwLock::new(topology.clone()));
    let services = databases
        .iter()
        .map(|(id, db)| {
            let service = ClusterDataNodeService::open(
                topology.cluster_id.clone(),
                id.clone(),
                db.read().data_path(),
                Arc::clone(db),
                true,
            )
            .unwrap();
            (id.clone(), Arc::new(Mutex::new(service)))
        })
        .collect::<BTreeMap<_, _>>();
    let identities = topology
        .nodes
        .keys()
        .map(|id| {
            (
                id.clone(),
                Identity {
                    cert: keys_path.join(format!("{id}.pem")),
                    key: keys_path.join(format!("{id}.key")),
                    ca: keys_path.join("ca.pem"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let network_config = |id: &ClusterNodeId| ClusterNetworkTransportConfig {
        connect_timeout_ms: 50,
        io_timeout_ms: 4000,
        max_frame_bytes: 4 * 1024 * 1024,
        max_inbound_connections: 16,
        dev_localhost_plaintext: false,
        tls: Some(identities[id].tls()),
    };
    let clients = topology
        .nodes
        .keys()
        .map(|id| {
            (
                id.clone(),
                Arc::new(
                    TcpClusterRelocationTransport::new(
                        topology.cluster_id.clone(),
                        id.clone(),
                        Arc::clone(&current),
                        network_config(id),
                    )
                    .unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let metadata = topology
        .nodes
        .keys()
        .map(|id| {
            (
                id.clone(),
                Arc::new(Mutex::new(
                    MetadataConsensusStore::reopen_native_owned(
                        keys_path.join(format!("strong-meta-{id}")),
                        Arc::clone(&databases[id]),
                        id.clone(),
                        topology.clone(),
                        &CancellationToken::uncancelable(),
                    )
                    .unwrap(),
                )),
            )
        })
        .collect::<BTreeMap<_, _>>();
    // Actual fresh election+append after strict reopen, preserving saved term,
    // votes, Store IDs and native owner receipts; no fresh metadata reset.
    {
        let mut leader = metadata[&node("n1")].lock();
        leader.start_election().unwrap();
        leader
            .become_leader(&topology.nodes.keys().cloned().collect())
            .unwrap();
        for id in [node("n0"), node("n2")] {
            let append = leader.append_request_from(1, 16).unwrap();
            let response = metadata[&id].lock().handle_append_request(append).unwrap();
            leader.record_append_response(&id, &response).unwrap();
        }
    }
    let providers = metadata
        .iter()
        .map(|(id, m)| {
            (
                id.clone(),
                Arc::new(MetadataReadProvider::new(Arc::clone(m), clients[id].clone()).unwrap()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let runtimes = topology
        .nodes
        .keys()
        .map(|id| {
            (
                id.clone(),
                Arc::new(
                    RangeRecoveryRuntime::new(
                        Arc::clone(&services[id]),
                        Arc::clone(&providers[id]),
                    )
                    .with_participant_transport(clients[id].clone()),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let servers = topology
        .nodes
        .keys()
        .map(|id| {
            start_cluster_data_server_with_recovery_runtime(
                &topology.nodes[id].address,
                network_config(id),
                Arc::clone(&current),
                Arc::clone(&services[id]),
                None,
                Some(Arc::clone(&metadata[id])),
                None,
                Some(Arc::clone(&providers[id])),
                Some(Arc::clone(&runtimes[id])),
            )
            .unwrap()
        })
        .collect();
    let coordinators = topology
        .nodes
        .keys()
        .map(|id| {
            (
                id.clone(),
                Arc::new(
                    RangeWriteCoordinator::new_schema_fenced(
                        topology.cluster_id.clone(),
                        id.clone(),
                        Arc::clone(&current),
                        Arc::clone(&services[id]),
                        clients[id].clone(),
                    )
                    .unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let embedded = Arc::new(ActualNativeRf3ServiceTransport {
        caller: node("n0"),
        topology: Arc::clone(&current),
        services: services.clone(),
        observations: Mutex::new(Vec::new()),
        unavailable: Mutex::new(BTreeSet::new()),
        native_delivery: AtomicBool::new(false),
        native_drop_once: Mutex::new(None),
        lose_commit_replies_after_durability: AtomicBool::new(false),
    });
    let embedded_coordinator = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            topology.cluster_id.clone(),
            node("n0"),
            Arc::clone(&current),
            Arc::clone(&services[&node("n0")]),
            Arc::clone(&embedded),
        )
        .unwrap(),
    );
    let native = ActualNativeRf3Fixture {
        root,
        topology: current,
        databases,
        services,
        specs,
        fresh_factories: BTreeMap::new(),
        transport: embedded,
        coordinator: embedded_coordinator,
    };
    let mut fixture = EnforcedNetwork {
        native,
        clients,
        metadata,
        providers,
        runtimes,
        coordinators,
        hosts: BTreeMap::new(),
        servers,
        dispatchers: BTreeMap::new(),
        _keys: keys,
    };
    // Reopened host policies are produced afresh. No recovery/epoch readiness
    // or application ACK is fabricated by constructing the fixture.
    fixture.refresh_actual_writer_policies();
    fixture
}

#[test]
fn native_abort_actual_canonical_publication_before_raw_mark_cold_reuses_exact_decision() {
    let fixture = EnforcedNetwork::new();
    let range = fixture.ranges()[0].clone();
    let key = fixture.key(range.id, "cold-abort-never-acked");
    for id in [node("n1"), node("n2")] {
        fixture.native.services[&id]
            .lock()
            .test_refuse_native_prepare_after_start(true);
    }
    assert!(fixture.native.append(&key).is_err());
    for id in [node("n1"), node("n2")] {
        fixture.native.services[&id]
            .lock()
            .test_refuse_native_prepare_after_start(false);
    }
    let scope = fixture.actual_current_recovery_scope(&range);
    let request = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        range_index: 1,
        origin_node_id: node("n0"),
        candidate_sha256: None,
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    fixture.native.services[&node("n0")]
        .lock()
        .test_fail_abort_after_native_publication();
    let error = fixture.clients[&node("n0")]
        .resolve_native_abort_at(&node("n0"), &request, &super::native_abort_tests::token())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("after actual canonical Abort native WAL"),
        "{error}"
    );
    fixture.idle();
    let (receipt, history, b) = {
        let db = fixture.native.databases[&node("n0")].read();
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.range_index(), 1);
        (
            source.native_receipt_sha256().to_owned(),
            source.logical_history_sha256().to_owned(),
            source.actual_local_visibility_sequence(),
        )
    };
    assert_eq!(
        fixture.native.services[&node("n0")]
            .lock()
            .range_write_progress(range.id)
            .resolved_through,
        0
    );
    let mut fixture = reopen_stopped(fixture);
    {
        let db = fixture.native.databases[&node("n0")].read();
        let source = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(source.native_receipt_sha256(), receipt);
        assert_eq!(source.logical_history_sha256(), history);
        assert_eq!(source.actual_local_visibility_sequence(), b);
        assert!(db.get("items", &key).unwrap().is_none());
    }
    let scope = fixture.actual_current_recovery_scope(&range);
    let fresh = crate::distribution_range_recovery::NativeAbortRequest {
        scope,
        challenge: Uuid::now_v7(),
        ..request
    };
    for id in [node("n0"), node("n1"), node("n2")] {
        let (r, _, _) = fixture.clients[&node("n0")]
            .resolve_native_abort_at(&id, &fresh, &super::native_abort_tests::token())
            .unwrap();
        assert_eq!(r, 1);
        fixture.idle();
    }
    let db = fixture.native.databases[&node("n0")].read();
    let source = db.current_native_range_source_lineage(range.id).unwrap();
    assert_eq!(
        source.native_receipt_sha256(),
        receipt,
        "cold retry must reuse original native WAL/decision"
    );
    assert_eq!(source.logical_history_sha256(), history);
    assert_eq!(source.actual_local_visibility_sequence(), b);
    assert!(db.get("items", &key).unwrap().is_none());
    drop(db);
    assert_eq!(
        fixture.native.services[&node("n0")]
            .lock()
            .range_write_progress(range.id)
            .resolved_through,
        1
    );
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .prepare_native_ready_observation(
                &id,
                &crate::distribution_range_recovery::NativeReadyRequest {
                    scope: fresh.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &super::native_abort_tests::token(),
            )
            .unwrap();
        fixture.idle();
    }
    for id in [node("n0"), node("n1"), node("n2")] {
        fixture.clients[&node("n0")]
            .complete_native_recovery_observation(
                &id,
                &crate::distribution_range_recovery::NativeCompletionRequest {
                    scope: fresh.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &super::native_abort_tests::token(),
            )
            .unwrap();
        fixture.idle();
    }
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    for id in [node("n1"), node("n2"), node("n0")] {
        fixture.runtimes[&id]
            .activate_native_recovery(
                &actor,
                &crate::distribution_range_recovery::NativeCompletionRequest {
                    scope: fresh.scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 4500,
                },
                &super::native_abort_tests::token(),
            )
            .unwrap();
        fixture.idle();
    }
    fixture.coordinators[&node("n0")]
        .recover_local_leadership(8)
        .unwrap();
    let acked = fixture.key(range.id, "actual-ack-after-cold-abort-decision-reuse");
    fixture.native.append(&acked).unwrap();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.get("items", &key).unwrap().is_none());
        assert!(db.get("items", &acked).unwrap().is_some());
        assert_eq!(
            db.current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            2
        );
    }
    fixture.native.root.disable_cleanup(false);
    fixture._keys.disable_cleanup(false);
}
